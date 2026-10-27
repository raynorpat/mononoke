// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! A layered `Decoder` adapter for `Stream` transformations.
//!
//! Decodes a `Stream<Item = Result<Bytes, E>>` into structured frames with a
//! tokio-util `Decoder`.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use futures::Stream;
use tokio_util::codec::Decoder;

use crate::{BoxStreamWrapper, StreamWrapper};

pub fn decode<In, Dec, E>(input: In, decoder: Dec) -> LayeredDecode<In, Dec>
where
    In: Stream<Item = Result<Bytes, E>>,
    Dec: Decoder,
    Dec::Error: From<E>,
{
    LayeredDecode {
        input,
        decoder,
        buf: BytesMut::with_capacity(8 * 1024),
        eof: false,
        is_readable: false,
    }
}

#[derive(Debug)]
pub struct LayeredDecode<In, Dec> {
    input: In,
    decoder: Dec,
    buf: BytesMut,
    eof: bool,
    is_readable: bool,
}

impl<In, Dec, E> Stream for LayeredDecode<In, Dec>
where
    In: Stream<Item = Result<Bytes, E>> + Unpin,
    Dec: Decoder + Unpin,
    Dec::Error: From<E>,
{
    type Item = Result<Dec::Item, Dec::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if this.is_readable {
                if this.eof {
                    if this.buf.is_empty() {
                        return Poll::Ready(None);
                    }
                    return match this.decoder.decode_eof(&mut this.buf) {
                        Ok(frame) => Poll::Ready(frame.map(Ok)),
                        Err(e) => Poll::Ready(Some(Err(e))),
                    };
                }
                match this.decoder.decode(&mut this.buf) {
                    Ok(Some(frame)) => return Poll::Ready(Some(Ok(frame))),
                    Ok(None) => this.is_readable = false,
                    Err(e) => return Poll::Ready(Some(Err(e))),
                }
            }

            debug_assert!(!this.eof);

            match Pin::new(&mut this.input).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(v))) => this.buf.extend_from_slice(&v),
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e.into()))),
                Poll::Ready(None) => this.eof = true,
            }
            this.is_readable = true;
        }
    }
}

impl<In, Dec> StreamWrapper<In> for LayeredDecode<In, Dec> {
    fn into_inner(self) -> In {
        self.input
    }
}

impl<In, Dec> BoxStreamWrapper<In> for LayeredDecode<In, Dec> {
    fn get_ref(&self) -> &In {
        &self.input
    }

    fn get_mut(&mut self) -> &mut In {
        &mut self.input
    }

    fn into_inner(self: Box<Self>) -> In {
        self.input
    }
}

#[cfg(test)]
mod test {
    use std::io;

    use bytes::{Bytes, BytesMut};
    use futures::executor::block_on;
    use futures::stream;
    use futures::TryStreamExt;
    use tokio_util::codec::Decoder;

    use super::*;

    struct NetstringDecoder;

    impl Decoder for NetstringDecoder {
        type Item = Bytes;
        type Error = io::Error;

        fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<Bytes>> {
            crate::testutil::decode_netstring(buf)
        }
    }

    fn collect(chunks: Vec<Bytes>) -> Vec<u8> {
        let inp = stream::iter(chunks.into_iter().map(|b| Ok::<_, io::Error>(b)));
        let dec = decode(inp, NetstringDecoder);
        let out: Vec<Bytes> = block_on(dec.try_collect()).expect("decode");
        out.into_iter().flat_map(|b| b.to_vec()).collect()
    }

    #[test]
    fn simple() {
        let out = collect(vec![Bytes::from(&b"13:hello, world!,"[..])]);
        assert_eq!(out, b"hello, world!");
    }

    #[test]
    fn large() {
        let out = collect(vec![Bytes::copy_from_slice(
            "13:hello, world!,".repeat(5000).as_bytes(),
        )]);
        assert_eq!(out, "hello, world!".repeat(5000).as_bytes());
    }

    #[test]
    fn partial() {
        let out = collect(vec![
            Bytes::from(&b"13:hel"[..]),
            Bytes::from(&b"lo, world!,"[..]),
        ]);
        assert_eq!(out, b"hello, world!");
    }
}
