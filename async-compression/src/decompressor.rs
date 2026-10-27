// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Non-blocking decompression over tokio `AsyncRead`.
//!
//! The codecs are synchronous `Read` adapters. Compressed bytes are pulled
//! from the inner `AsyncRead` into `Input`, and `WouldBlock` from a codec
//! means that buffer is empty and the socket must be polled.

use std::cmp;
use std::fmt::{self, Debug, Formatter};
use std::io::{self, BufReader, Read};
use std::pin::Pin;
use std::task::{Context, Poll};

use bzip2::read::BzDecoder;
use flate2::read::GzDecoder;
use tokio::io::{AsyncRead, ReadBuf};
use zstd::stream::read::Decoder as ZstdDecoder;

const READ_CHUNK: usize = 8 * 1024;

pub struct Decompressor<R> {
    d_type: DecompressorType,
    inner: Dec<R>,
}

enum Dec<R> {
    Bzip2(BzDecoder<Input<R>>),
    Gzip(GzDecoder<Input<R>>),
    Zstd(ZstdDecoder<'static, BufReader<Input<R>>>),
    Plain(R),
}

/// Bytes already pulled from `R` that a sync codec has not consumed yet.
struct Input<R> {
    inner: R,
    buf: Vec<u8>,
    pos: usize,
    eof: bool,
}

impl<R> Input<R> {
    fn new(inner: R) -> Self {
        Input {
            inner,
            buf: Vec::new(),
            pos: 0,
            eof: false,
        }
    }

    fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    fn take_reader(self) -> R {
        self.inner
    }
}

impl<R> Read for Input<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        let available = self.buf.len().saturating_sub(self.pos);
        if available == 0 {
            if self.eof {
                return Ok(0);
            }
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "compressed input exhausted",
            ));
        }
        let n = cmp::min(out.len(), available);
        out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum DecompressorType {
    Bzip2,
    Gzip,
    Zstd,
    Uncompressed,
}

impl<R> Decompressor<R> {
    pub fn new(r: R, dt: DecompressorType) -> Self {
        let inner = match dt {
            DecompressorType::Bzip2 => Dec::Bzip2(BzDecoder::new(Input::new(r))),
            DecompressorType::Gzip => Dec::Gzip(GzDecoder::new(Input::new(r))),
            // `Decoder::new` only fails if zstd cannot allocate a context.
            DecompressorType::Zstd => {
                Dec::Zstd(ZstdDecoder::new(Input::new(r)).expect("zstd decoder"))
            }
            DecompressorType::Uncompressed => Dec::Plain(r),
        };
        Decompressor { d_type: dt, inner }
    }

    pub fn get_ref(&self) -> &R {
        match &self.inner {
            Dec::Bzip2(dec) => &dec.get_ref().inner,
            Dec::Gzip(dec) => &dec.get_ref().inner,
            Dec::Zstd(dec) => &dec.get_ref().get_ref().inner,
            Dec::Plain(r) => r,
        }
    }

    pub fn get_mut(&mut self) -> &mut R {
        match &mut self.inner {
            Dec::Bzip2(dec) => &mut dec.get_mut().inner,
            Dec::Gzip(dec) => &mut dec.get_mut().inner,
            Dec::Zstd(dec) => &mut dec.get_mut().get_mut().inner,
            Dec::Plain(r) => r,
        }
    }

    pub fn into_inner(self) -> R {
        match self.inner {
            Dec::Bzip2(dec) => dec.into_inner().take_reader(),
            Dec::Gzip(dec) => dec.into_inner().take_reader(),
            Dec::Zstd(dec) => dec.finish().into_inner().take_reader(),
            Dec::Plain(r) => r,
        }
    }

    fn read_codec(&mut self, dest: &mut [u8]) -> io::Result<usize> {
        match &mut self.inner {
            Dec::Bzip2(dec) => dec.read(dest),
            Dec::Gzip(dec) => dec.read(dest),
            Dec::Zstd(dec) => dec.read(dest),
            Dec::Plain(_) => Ok(0),
        }
    }

    fn input_mut(&mut self) -> &mut Input<R> {
        match &mut self.inner {
            Dec::Bzip2(dec) => dec.get_mut(),
            Dec::Gzip(dec) => dec.get_mut(),
            Dec::Zstd(dec) => dec.get_mut().get_mut(),
            Dec::Plain(_) => unreachable!("plain reads do not use Input"),
        }
    }

    /// Pull the next chunk from the inner reader into the codec buffer.
    ///
    /// The caller only invokes this after the codec reported `WouldBlock`,
    /// which means `Input` has no unread bytes.
    fn poll_fill(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>>
    where
        R: AsyncRead + Unpin,
    {
        let input = self.input_mut();
        if !input.is_empty() || input.eof {
            return Poll::Ready(Ok(()));
        }
        input.buf.clear();
        input.pos = 0;
        input.buf.resize(READ_CHUNK, 0);
        let mut read_buf = ReadBuf::new(&mut input.buf);
        match Pin::new(&mut input.inner).poll_read(cx, &mut read_buf) {
            Poll::Pending => {
                input.buf.clear();
                Poll::Pending
            }
            Poll::Ready(Err(e)) => {
                input.buf.clear();
                Poll::Ready(Err(e))
            }
            Poll::Ready(Ok(())) => {
                let n = read_buf.filled().len();
                input.buf.truncate(n);
                if n == 0 {
                    input.eof = true;
                }
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for Decompressor<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Dec::Plain(inner) = &mut this.inner {
            return Pin::new(inner).poll_read(cx, buf);
        }

        let dest = buf.initialize_unfilled();
        if dest.is_empty() {
            return Poll::Ready(Ok(()));
        }

        loop {
            match this.read_codec(dest) {
                Ok(n) => {
                    buf.advance(n);
                    return Poll::Ready(Ok(()));
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // `Input` answers `WouldBlock` only when it is empty and
                    // the socket has not ended. Anything else is a real error;
                    // filling again would spin.
                    let input = this.input_mut();
                    if !input.is_empty() || input.eof {
                        return Poll::Ready(Err(e));
                    }
                    match this.poll_fill(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                        Poll::Ready(Ok(())) => continue,
                    }
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }
}

impl<R> Debug for Decompressor<R> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("Decompressor")
            .field("decoder_type", &self.d_type)
            .finish()
    }
}
