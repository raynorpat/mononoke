// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Non-blocking, buffered compression.
//!
//! The codecs write into an in-memory buffer. `AsyncWrite` drains that buffer
//! into the inner writer, because tokio 1 `AsyncWrite` is not `std::io::Write`.

use std::fmt::{self, Debug, Formatter};
use std::io::{self, Write};
use std::pin::Pin;
use std::task::{Context, Poll};

use bzip2::write::BzEncoder;
use flate2::write::GzEncoder;
use flate2::Compression as GzipLevel;
use tokio::io::AsyncWrite;
use zstd::stream::write::Encoder as ZstdEncoder;

use crate::decompressor::DecompressorType;

#[derive(Clone, Copy, Debug)]
pub enum CompressorType {
    Bzip2(bzip2::Compression),
    Gzip,
    Zstd { level: i32 },
    Uncompressed,
}

impl CompressorType {
    pub fn decompressor_type(&self) -> DecompressorType {
        match self {
            CompressorType::Bzip2(_) => DecompressorType::Bzip2,
            CompressorType::Gzip => DecompressorType::Gzip,
            CompressorType::Zstd { .. } => DecompressorType::Zstd,
            CompressorType::Uncompressed => DecompressorType::Uncompressed,
        }
    }
}

enum Engine {
    Bzip2(BzEncoder<Vec<u8>>),
    Gzip(GzEncoder<Vec<u8>>),
    Zstd(ZstdEncoder<'static, Vec<u8>>),
    Plain(Vec<u8>),
    Finished,
}

pub struct Compressor<W> {
    c_type: CompressorType,
    writer: W,
    engine: Engine,
    pending: Vec<u8>,
    pending_pos: usize,
    finished_enc: bool,
}

impl<W> Compressor<W> {
    pub fn new(w: W, ct: CompressorType) -> Self {
        let engine = match ct {
            CompressorType::Bzip2(level) => Engine::Bzip2(BzEncoder::new(Vec::new(), level)),
            CompressorType::Gzip => Engine::Gzip(GzEncoder::new(Vec::new(), GzipLevel::default())),
            CompressorType::Zstd { level } => {
                Engine::Zstd(ZstdEncoder::new(Vec::new(), level).expect("zstd encoder"))
            }
            CompressorType::Uncompressed => Engine::Plain(Vec::new()),
        };
        Compressor {
            c_type: ct,
            writer: w,
            engine,
            pending: Vec::new(),
            pending_pos: 0,
            finished_enc: false,
        }
    }

    fn pull(&mut self) {
        let extra = match &mut self.engine {
            Engine::Bzip2(enc) => std::mem::take(enc.get_mut()),
            Engine::Gzip(enc) => std::mem::take(enc.get_mut()),
            Engine::Zstd(enc) => std::mem::take(enc.get_mut()),
            Engine::Plain(buf) => std::mem::take(buf),
            Engine::Finished => Vec::new(),
        };
        self.pending.extend_from_slice(&extra);
    }

    fn accept(&mut self, buf: &[u8]) -> io::Result<()> {
        if self.finished_enc {
            return Err(io::Error::new(io::ErrorKind::Other, "compressor finished"));
        }
        match &mut self.engine {
            Engine::Bzip2(enc) => enc.write_all(buf)?,
            Engine::Gzip(enc) => enc.write_all(buf)?,
            Engine::Zstd(enc) => enc.write_all(buf)?,
            Engine::Plain(out) => out.extend_from_slice(buf),
            Engine::Finished => {
                return Err(io::Error::new(io::ErrorKind::Other, "compressor finished"));
            }
        }
        self.pull();
        Ok(())
    }

    fn flush_engine(&mut self) -> io::Result<()> {
        match &mut self.engine {
            Engine::Bzip2(enc) => enc.flush()?,
            Engine::Gzip(enc) => enc.flush()?,
            Engine::Zstd(enc) => enc.flush()?,
            Engine::Plain(_) | Engine::Finished => {}
        }
        self.pull();
        Ok(())
    }

    fn finish_engine(&mut self) -> io::Result<()> {
        if self.finished_enc {
            return Ok(());
        }
        self.flush_engine()?;
        let engine = std::mem::replace(&mut self.engine, Engine::Finished);
        let buf = match engine {
            Engine::Bzip2(enc) => enc.finish()?,
            Engine::Gzip(enc) => enc.finish()?,
            Engine::Zstd(enc) => enc.finish()?,
            Engine::Plain(buf) => buf,
            Engine::Finished => Vec::new(),
        };
        self.pending.extend_from_slice(&buf);
        self.finished_enc = true;
        Ok(())
    }

    /// Finish the compression frame and return the inner writer.
    ///
    /// `ErrorKind::WouldBlock` means the inner writer could not accept the
    /// trailer yet. The compressor is returned so the caller can retry.
    pub fn try_finish(mut self) -> Result<W, (Self, io::Error)>
    where
        W: AsyncWrite + Unpin,
    {
        if let Err(e) = self.finish_engine() {
            return Err((self, e));
        }
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match self.drain(&mut cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => return Err((self, e)),
            Poll::Pending => {
                return Err((
                    self,
                    io::Error::new(io::ErrorKind::WouldBlock, "compressor finish would block"),
                ));
            }
        }
        match Pin::new(&mut self.writer).poll_flush(&mut cx) {
            Poll::Ready(Ok(())) => Ok(self.writer),
            Poll::Ready(Err(e)) => Err((self, e)),
            Poll::Pending => Err((
                self,
                io::Error::new(io::ErrorKind::WouldBlock, "compressor flush would block"),
            )),
        }
    }

    fn drain(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>>
    where
        W: AsyncWrite + Unpin,
    {
        while self.pending_pos < self.pending.len() {
            match Pin::new(&mut self.writer).poll_write(cx, &self.pending[self.pending_pos..]) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write compressed data",
                    )));
                }
                Poll::Ready(Ok(n)) => self.pending_pos += n,
                Poll::Ready(Err(e)) if e.kind() == io::ErrorKind::Interrupted => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
        self.pending.clear();
        self.pending_pos = 0;
        Poll::Ready(Ok(()))
    }
}

impl<W: AsyncWrite + Unpin> Write for Compressor<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match Pin::new(&mut *self).poll_write(&mut cx, buf) {
            Poll::Ready(result) => result,
            Poll::Pending => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "compressor write would block",
            )),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match Pin::new(&mut *self).poll_flush(&mut cx) {
            Poll::Ready(result) => result,
            Poll::Pending => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "compressor flush would block",
            )),
        }
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for Compressor<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        const HIGH: usize = 64 * 1024;
        if this.pending.len().saturating_sub(this.pending_pos) > HIGH {
            match this.drain(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
        this.accept(buf)?;
        match this.drain(cx) {
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            _ => Poll::Ready(Ok(buf.len())),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.flush_engine()?;
        match this.drain(cx) {
            Poll::Ready(Ok(())) => Pin::new(&mut this.writer).poll_flush(cx),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.finish_engine()?;
        match this.drain(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        }
        match Pin::new(&mut this.writer).poll_flush(cx) {
            Poll::Ready(Ok(())) => Pin::new(&mut this.writer).poll_shutdown(cx),
            other => other,
        }
    }
}

impl<W> Debug for Compressor<W> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("Compressor")
            .field("c_type", &self.c_type)
            .finish()
    }
}

#[cfg(test)]
fn _assert_send() {
    use std::io::Cursor;

    fn _assert<T: Send>(_val: T) {}

    _assert(Compressor::new(
        Cursor::new(Vec::<u8>::new()),
        CompressorType::Uncompressed,
    ));
}

#[cfg(test)]
#[test]
fn compressor_is_send() {
    _assert_send();
}
