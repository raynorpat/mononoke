// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! In-memory async buffers that implement `Read`.
//!
//! Sync decoders only see `std::io::Read`. When the buffer is empty they get
//! `WouldBlock`, and the waker from the current `poll_read` is stored here so
//! a later `write_buf` can wake the reader.

use std::cell::RefCell;
use std::cmp;
use std::io::{self, Read};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use bytes::BytesMut;
use tokio::io::{AsyncRead, ReadBuf};

thread_local! {
    static POLL_WAKER: RefCell<Option<Waker>> = RefCell::new(None);
}

pub(crate) fn with_poll_waker<T>(waker: &Waker, f: impl FnOnce() -> T) -> T {
    POLL_WAKER.with(|slot| {
        let prev = slot.replace(Some(waker.clone()));
        let out = f();
        slot.replace(prev);
        out
    })
}

fn current_waker() -> Option<Waker> {
    POLL_WAKER.with(|slot| slot.borrow().clone())
}

/// A fixed capacity in-memory buffer that implements asynchronous `Read`.
#[derive(Debug)]
pub struct MemBuf {
    capacity: usize,
    buf: BytesMut,
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
    eof: bool,
}

impl MemBuf {
    pub fn new(buf_size: usize) -> Self {
        MemBuf {
            capacity: buf_size,
            buf: BytesMut::with_capacity(buf_size),
            read_waker: None,
            write_waker: None,
            eof: false,
        }
    }

    pub fn write_buf(&mut self, data: &[u8]) -> io::Result<usize> {
        if self.eof || data.is_empty() {
            return Ok(0);
        }

        if self.capacity == self.buf.len() {
            self.write_waker = current_waker();
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "buffer full"));
        }

        let to_write = cmp::min(self.capacity - self.buf.len(), data.len());
        self.buf.extend_from_slice(&data[..to_write]);
        if to_write > 0 {
            self.wake_read();
        }
        Ok(to_write)
    }

    pub fn mark_eof(&mut self) {
        self.eof = true;
        self.wake_read();
    }

    fn wake_read(&mut self) {
        if let Some(waker) = self.read_waker.take() {
            waker.wake();
        }
    }

    fn wake_write(&mut self) {
        if let Some(waker) = self.write_waker.take() {
            waker.wake();
        }
    }
}

impl From<Vec<u8>> for MemBuf {
    fn from(vec: Vec<u8>) -> MemBuf {
        MemBuf {
            capacity: vec.capacity(),
            buf: BytesMut::from(vec.as_slice()),
            read_waker: None,
            write_waker: None,
            eof: false,
        }
    }
}

impl Read for MemBuf {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.buf.is_empty() {
            if self.eof {
                return Ok(0);
            }
            self.read_waker = current_waker();
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "buffer empty"));
        }

        let len = cmp::min(self.buf.len(), buf.len());
        if len == 0 {
            return Ok(0);
        }
        buf[..len].copy_from_slice(&self.buf[..len]);
        let _ = self.buf.split_to(len);
        self.wake_write();
        Ok(len)
    }
}

impl AsyncRead for MemBuf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let dest = buf.initialize_unfilled();
        let result = with_poll_waker(cx.waker(), || Read::read(this, dest));
        match result {
            Ok(n) => {
                buf.advance(n);
                Poll::Ready(Ok(()))
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Poll::Pending,
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}
