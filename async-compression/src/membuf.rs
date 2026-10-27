// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! In-memory async buffers.
//!
//! An empty buffer stores the `poll_read` waker so a later `write_buf` wakes
//! the reader. A full buffer stores the `poll_write` waker so a later read
//! wakes the writer. `write_buf` itself has no `Context`; callers that must
//! be woken use `AsyncWrite`.

use std::cell::RefCell;
use std::cmp;
use std::io::{self, Read};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

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
            // Only replace a parked writer when this call is inside a poll.
            // A bare `write_buf` has no waker and must not clear one.
            if let Some(waker) = current_waker() {
                self.write_waker = Some(waker);
            }
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

impl AsyncWrite for MemBuf {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match this.write_buf(buf) {
            Ok(n) => Poll::Ready(Ok(n)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                this.write_waker = Some(cx.waker().clone());
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().mark_eof();
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Wake, Waker};

    use super::*;

    struct Flag(AtomicBool);

    impl Wake for Flag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn full_buffer_wakes_blocked_writer() {
        let mut buf = MemBuf::new(4);
        assert_eq!(buf.write_buf(&[1, 2, 3, 4]).unwrap(), 4);

        let flag = Arc::new(Flag(AtomicBool::new(false)));
        let waker = Waker::from(flag.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut buf).poll_write(&mut cx, &[9]).is_pending());
        assert!(!flag.0.load(Ordering::SeqCst));

        let mut out = [0u8; 1];
        assert_eq!(Read::read(&mut buf, &mut out).unwrap(), 1);
        assert!(flag.0.load(Ordering::SeqCst));

        match Pin::new(&mut buf).poll_write(&mut cx, &[9]) {
            Poll::Ready(Ok(1)) => {}
            other => panic!("writer was not resumed: {:?}", other),
        }
        assert_eq!(&buf.buf[..], &[2, 3, 4, 9]);
    }
}
