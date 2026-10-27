// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! A layered `Encoder` adapter for `Stream` transformations.
//!
//! Encodes items from a `Stream<Item = Result<T, E>>` into a stream of
//! `Result<Bytes, _>` using a tokio-util `Encoder`.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use futures::Stream;
use tokio_util::codec::Encoder;

const INITIAL_CAPACITY: usize = 8192;
const HEADROOM: usize = 512;
const HIGHWATER: usize = INITIAL_CAPACITY - HEADROOM;

pub fn encode<In, Enc, T, E>(inp: In, enc: Enc) -> LayeredEncoder<In, Enc>
where
    In: Stream<Item = Result<T, E>>,
    Enc: Encoder<T>,
    Enc::Error: From<E>,
{
    LayeredEncoder {
        inp,
        enc,
        eof: false,
        buf: BytesMut::with_capacity(INITIAL_CAPACITY),
    }
}

pub struct LayeredEncoder<In, Enc> {
    inp: In,
    enc: Enc,
    eof: bool,
    buf: BytesMut,
}

impl<In, Enc, T, E> Stream for LayeredEncoder<In, Enc>
where
    In: Stream<Item = Result<T, E>> + Unpin,
    Enc: Encoder<T> + Unpin,
    Enc::Error: From<E>,
{
    type Item = Result<Bytes, Enc::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            let mut push = false;

            if !this.eof {
                match Pin::new(&mut this.inp).poll_next(cx) {
                    Poll::Ready(None) => this.eof = true,
                    Poll::Ready(Some(Ok(item))) => {
                        if let Err(e) = this.enc.encode(item, &mut this.buf) {
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                    Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e.into()))),
                    Poll::Pending => push = true,
                }
            }

            let len = this.buf.len();
            let push = push || len > HIGHWATER || this.eof;

            if push {
                match (len == 0, this.eof) {
                    (true, true) => return Poll::Ready(None),
                    (true, false) => return Poll::Pending,
                    (false, eof) => {
                        let ret = this.buf.split_to(len);
                        if !eof {
                            this.buf.reserve(HIGHWATER);
                        }
                        return Poll::Ready(Some(Ok(ret.freeze())));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::vec;

    use bytes::{BufMut, BytesMut};
    use futures::executor::block_on;
    use futures::task::noop_waker;
    use futures::Stream;
    use quickcheck::quickcheck;
    use tokio_util::codec::Encoder;

    use super::*;

    struct EncU16;

    impl Encoder<u16> for EncU16 {
        type Error = io::Error;

        fn encode(&mut self, item: u16, dst: &mut BytesMut) -> Result<(), Self::Error> {
            dst.reserve(2);
            dst.put_u16(item);
            Ok(())
        }
    }

    struct TestStream<T> {
        iter: vec::IntoIter<Option<T>>,
    }

    impl<T> TestStream<T> {
        fn new(v: Vec<Option<T>>) -> Self {
            TestStream {
                iter: v.into_iter(),
            }
        }
    }

    impl<T: Unpin> Stream for TestStream<T> {
        type Item = Result<T, io::Error>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match self.get_mut().iter.next() {
                None => Poll::Ready(None),
                Some(None) => Poll::Pending,
                Some(Some(v)) => Poll::Ready(Some(Ok(v))),
            }
        }
    }

    #[test]
    fn simple() {
        use futures::TryStreamExt;

        let s = TestStream::new(vec![Some(0u16), Some(4), Some(5), Some(6), Some(256)]);
        let enc = encode::<_, _, _, _>(s, EncU16);
        let res: Vec<_> = block_on(enc.try_collect()).expect("collect failed");
        let res: Vec<u8> = res.into_iter().flat_map(|x| x).collect();
        assert_eq!(res.as_slice(), &[0, 0, 0, 4, 0, 5, 0, 6, 1, 0][..]);
    }

    quickcheck! {
        fn encoding(v: Vec<Option<u16>>) -> bool {
            let s = TestStream::new(v.clone());
            let mut enc = encode::<_, _, _, _>(s, EncU16);
            let waker = noop_waker();
            let mut cx = Context::from_waker(&waker);

            let mut res = Vec::new();
            loop {
                match Pin::new(&mut enc).poll_next(&mut cx) {
                    Poll::Pending => (),
                    Poll::Ready(None) => break,
                    Poll::Ready(Some(Ok(v))) => res.push(v),
                    Poll::Ready(Some(Err(e))) => panic!("encode error: {}", e),
                }
            }

            let res: Vec<u8> = res.into_iter().flat_map(|x| x).collect();
            let input: Vec<u16> = v.into_iter().filter_map(|v| v).collect();
            assert_eq!(input.len() * 2, res.len());

            let mut output = Vec::new();
            for i in 0..input.len() {
                let bytes = [res[i * 2], res[i * 2 + 1]];
                output.push(u16::from_be_bytes(bytes));
            }

            input == output
        }
    }
}
