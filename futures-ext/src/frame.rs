// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Framed objects that are only streams, not sinks.

use std::cmp;
use std::io::{self, Read};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::BytesMut;
use futures::Stream;
use tokio::io::{AsyncRead, ReadBuf};
use tokio_util::codec::Decoder;

const BUFSIZE: usize = 8 * 1024;

#[inline]
pub fn framed_stream<T, Dec>(read: T, decoder: Dec) -> FramedStream<T, Dec>
where
    T: AsyncRead,
    Dec: Decoder,
{
    FramedStream {
        inner: read,
        decoder,
        buf: BytesMut::with_capacity(BUFSIZE),
        eof: false,
        is_readable: false,
    }
}

/// A stream of decoded frames over an `AsyncRead`.
///
/// Unlike `FramedRead`, `into_inner_leading` returns bytes that were read
/// but not consumed by the decoder.
#[derive(Debug)]
pub struct FramedStream<T, Dec> {
    inner: T,
    decoder: Dec,
    buf: BytesMut,
    eof: bool,
    is_readable: bool,
}

impl<T, Dec> FramedStream<T, Dec> {
    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    pub fn into_inner_leading(self) -> ReadLeadingBuffer<T> {
        ReadLeadingBuffer {
            inner: self.inner,
            buf: self.buf,
        }
    }
}

impl<T, Dec> Stream for FramedStream<T, Dec>
where
    T: AsyncRead + Unpin,
    Dec: Decoder + Unpin,
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

            this.buf.reserve(BUFSIZE);
            let mut tmp = [0u8; BUFSIZE];
            let mut read_buf = ReadBuf::new(&mut tmp);
            match Pin::new(&mut this.inner).poll_read(cx, &mut read_buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e.into()))),
                Poll::Ready(Ok(())) => {
                    let n = read_buf.filled().len();
                    if n == 0 {
                        this.eof = true;
                    } else {
                        this.buf.extend_from_slice(read_buf.filled());
                    }
                }
            }
            this.is_readable = true;
        }
    }
}

pub struct ReadLeadingBuffer<T> {
    inner: T,
    buf: BytesMut,
}

impl<T> ReadLeadingBuffer<T> {
    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T: Read> Read for ReadLeadingBuffer<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.buf.is_empty() {
            return self.inner.read(buf);
        }

        let len = cmp::min(buf.len(), self.buf.len());
        if len == 0 {
            return Ok(0);
        }
        buf[..len].copy_from_slice(&self.buf[..len]);
        let _ = self.buf.split_to(len);
        Ok(len)
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for ReadLeadingBuffer<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if !this.buf.is_empty() {
            let n = cmp::min(buf.remaining(), this.buf.len());
            buf.put_slice(&this.buf[..n]);
            let _ = this.buf.split_to(n);
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

#[cfg(test)]
mod test {
    use std::io::{Cursor, Write};

    use bytes::{Bytes, BytesMut};
    use futures::executor::block_on;
    use futures::StreamExt;
    use tokio_util::codec::Decoder;

    use super::*;

    struct NetstringDecoder;

    impl Decoder for NetstringDecoder {
        type Item = Bytes;
        type Error = std::io::Error;

        fn decode(&mut self, buf: &mut BytesMut) -> std::io::Result<Option<Bytes>> {
            crate::testutil::decode_netstring(buf)
        }
    }

    #[test]
    fn simple() {
        let inp = Cursor::new(b"13:hello, world!,".to_vec());
        let stream = framed_stream(inp, NetstringDecoder);
        let (res, _) = block_on(stream.into_future());
        assert_eq!(res.unwrap().unwrap().as_ref(), b"hello, world!");
    }

    #[test]
    fn leading_buffer() {
        let inp = Cursor::new(b"13:hello, world!,foo-bar-".to_vec());
        let stream = framed_stream(inp, NetstringDecoder);
        let (res, mut stream) = block_on(stream.into_future());

        assert_eq!(res.unwrap().unwrap().as_ref(), b"hello, world!");
        assert_eq!(stream.get_ref().position(), 25);

        stream.get_mut().write_all(b"baz-quux").expect("write");
        stream.get_mut().set_position(25);

        let mut read2 = stream.into_inner_leading();
        let mut out = vec![];
        assert_eq!(read2.read_to_end(&mut out).expect("read"), 16);
        assert_eq!(out, b"foo-bar-baz-quux");
    }
}
