// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Metered read and write wrappers, to keep track of total bytes passed through
//! streams.

use std::io::{self, Read, Write};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// A reader wrapper that tracks the total number of bytes read through it.
pub struct MeteredRead<R> {
    inner: R,
    total_thru: u64,
}

impl<R> MeteredRead<R> {
    pub fn new(inner: R) -> Self {
        MeteredRead {
            inner,
            total_thru: 0,
        }
    }

    /// Total number of bytes passed through this reader.
    pub fn total_thru(&self) -> u64 {
        self.total_thru
    }

    pub fn get_ref(&self) -> &R {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: Read> Read for MeteredRead<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read_bytes = self.inner.read(buf)?;
        self.total_thru += read_bytes as u64;
        Ok(read_bytes)
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for MeteredRead<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                this.total_thru += (buf.filled().len() - before) as u64;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

/// A writer wrapper that tracks the total number of bytes written through it.
pub struct MeteredWrite<W> {
    inner: W,
    total_thru: u64,
}

impl<W> MeteredWrite<W> {
    pub fn new(inner: W) -> Self {
        MeteredWrite {
            inner,
            total_thru: 0,
        }
    }

    /// Total number of bytes written through this writer.
    ///
    /// The inner writer might be buffered. `total_thru` counts bytes accepted
    /// by `write` / `poll_write`, including data the inner writer still holds.
    pub fn total_thru(&self) -> u64 {
        self.total_thru
    }

    pub fn get_ref(&self) -> &W {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut W {
        &mut self.inner
    }

    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: Write> Write for MeteredWrite<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written_bytes = self.inner.write(buf)?;
        self.total_thru += written_bytes as u64;
        Ok(written_bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for MeteredWrite<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                this.total_thru += n as u64;
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
