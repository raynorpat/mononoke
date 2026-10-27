// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Winsock `AF_UNIX` sockets.
//!
//! Tokio 1 exports `UnixListener` and `UnixStream` only behind `cfg(unix)`.
//! On Windows the same ssh handoff uses `AF_UNIX` `SOCK_STREAM` sockets and
//! adapts the blocking handle to `AsyncRead` / `AsyncWrite`.

use std::cmp;
use std::future::Future;
use std::io::{self, Read, Write};
use std::net::Shutdown;
use std::path::Path;
use std::pin::Pin;
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread::{self, Thread};
use std::time::Duration;

use bytes::Bytes;
use socket2::{Domain, SockAddr, Socket, Type};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::oneshot;

/// In-flight read and write chunks. A full write queue returns `Pending`.
const QUEUE: usize = 8;

enum Cmd {
    Write(Bytes),
    Flush(oneshot::Sender<io::Result<()>>),
    Shutdown(oneshot::Sender<io::Result<()>>),
}

struct Shared {
    read_waker: Mutex<Option<Waker>>,
    write_waker: Mutex<Option<Waker>>,
    error: Mutex<Option<io::Error>>,
    io: Mutex<Option<Thread>>,
}

enum Wait {
    Idle,
    Flush(oneshot::Receiver<io::Result<()>>),
    Shutdown(oneshot::Receiver<io::Result<()>>),
}

pub struct UnixListener {
    socket: Socket,
}

impl UnixListener {
    pub fn bind<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let addr = SockAddr::unix(path)?;
        let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
        socket.bind(&addr)?;
        socket.listen(128)?;
        Ok(UnixListener { socket })
    }

    pub async fn accept(&self) -> io::Result<(UnixStream, String)> {
        let listener = self.socket.try_clone()?;
        let (socket, _) = tokio::task::spawn_blocking(move || listener.accept())
            .await
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))??;
        Ok((UnixStream::from_socket(socket), "unix".to_owned()))
    }
}

pub struct UnixStream {
    read_rx: Option<Receiver<io::Result<Bytes>>>,
    read_rest: Bytes,
    write_tx: Option<SyncSender<Cmd>>,
    /// Write half has been shut down. Later writes fail, reads stay open.
    write_shut: bool,
    shared: Arc<Shared>,
    io: Option<thread::JoinHandle<()>>,
    wait: Wait,
}

impl UnixStream {
    pub async fn connect<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let socket = tokio::task::spawn_blocking(move || {
            let addr = SockAddr::unix(path)?;
            let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
            socket.connect(&addr)?;
            Ok::<_, io::Error>(socket)
        })
        .await
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err))??;
        Ok(Self::from_socket(socket))
    }

    fn from_socket(socket: Socket) -> Self {
        let (read_tx, read_rx) = mpsc::sync_channel(QUEUE);
        let (write_tx, write_rx) = mpsc::sync_channel(QUEUE);
        let shared = Arc::new(Shared {
            read_waker: Mutex::new(None),
            write_waker: Mutex::new(None),
            error: Mutex::new(None),
            io: Mutex::new(None),
        });
        let shared_io = Arc::clone(&shared);
        let io = thread::spawn(move || io_loop(socket, write_rx, read_tx, shared_io));
        UnixStream {
            read_rx: Some(read_rx),
            read_rest: Bytes::new(),
            write_tx: Some(write_tx),
            write_shut: false,
            shared,
            io: Some(io),
            wait: Wait::Idle,
        }
    }

    fn enqueue(&mut self, cx: &mut Context<'_>, cmd: Cmd) -> Poll<io::Result<()>> {
        let Some(tx) = self.write_tx.clone() else {
            return Poll::Ready(Err(write_closed()));
        };
        match tx.try_send(cmd) {
            Ok(()) => {
                unpark(&self.shared);
                Poll::Ready(Ok(()))
            }
            Err(TrySendError::Disconnected(_)) => Poll::Ready(Err(write_closed())),
            Err(TrySendError::Full(cmd)) => {
                park(&self.shared.write_waker, cx);
                match tx.try_send(cmd) {
                    Ok(()) => {
                        unpark(&self.shared);
                        Poll::Ready(Ok(()))
                    }
                    Err(TrySendError::Full(_)) => Poll::Pending,
                    Err(TrySendError::Disconnected(_)) => Poll::Ready(Err(write_closed())),
                }
            }
        }
    }

    fn poll_barrier(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let polled = match &mut self.wait {
            Wait::Idle => return Poll::Ready(Ok(())),
            Wait::Flush(rx) | Wait::Shutdown(rx) => Pin::new(rx).poll(cx),
        };
        match polled {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(result)) => {
                let shutting_down = matches!(self.wait, Wait::Shutdown(_));
                self.wait = Wait::Idle;
                if shutting_down {
                    // Keep `write_tx`. Dropping it disconnects the IO loop and
                    // closes the socket, which the read half still needs.
                    self.write_shut = true;
                }
                Poll::Ready(result)
            }
            Poll::Ready(Err(_)) => {
                self.wait = Wait::Idle;
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "unix writer stopped",
                )))
            }
        }
    }
}

impl Drop for UnixStream {
    fn drop(&mut self) {
        // `FramedWrite` does not call `poll_shutdown`. Drop the command
        // sender so the IO thread finishes bytes already queued, then
        // half-closes and exits. That exit is the only time the socket and
        // the read half go away. Join before returning so `shutdown` cannot
        // run during `write`.
        drop(self.write_tx.take());
        unpark(&self.shared);
        if let Some(handle) = self.io.take() {
            let _ = handle.join();
        }
        self.read_rx.take();
    }
}

fn io_loop(
    mut socket: Socket,
    cmds: Receiver<Cmd>,
    reads: SyncSender<io::Result<Bytes>>,
    shared: Arc<Shared>,
) {
    // Blocking calls with a short timeout, on one thread. Nonblocking
    // Winsock reads do not complete unless the socket is on an IOCP, and two
    // threads blocked in recv and send on duplicated handles can stall.
    let tick = Duration::from_millis(2);
    if socket.set_read_timeout(Some(tick)).is_err() || socket.set_write_timeout(Some(tick)).is_err()
    {
        return;
    }
    *shared.io.lock().expect("io thread") = Some(thread::current());

    let mut outgoing: Option<Bytes> = None;
    let mut outgoing_pos = 0;
    let mut failed: Option<io::Error> = None;
    let mut writes_closed = false;
    let mut read_stash: Option<io::Result<Bytes>> = None;
    let mut saw_eof = false;
    let mut eof_queued = false;

    loop {
        let mut progressed = false;
        if outgoing.is_none() {
            match cmds.try_recv() {
                Ok(Cmd::Write(buf)) => {
                    if failed.is_none() && !writes_closed {
                        outgoing = Some(buf);
                        outgoing_pos = 0;
                        progressed = true;
                    }
                    wake(&shared.write_waker);
                }
                Ok(Cmd::Flush(ack)) => {
                    let result = if writes_closed && failed.is_none() {
                        Ok(())
                    } else {
                        status(&failed)
                    };
                    let _ = ack.send(result);
                    progressed = true;
                }
                Ok(Cmd::Shutdown(ack)) => {
                    let result = if writes_closed && failed.is_none() {
                        Ok(())
                    } else if let Some(err) = &failed {
                        Err(copy_err(err))
                    } else {
                        socket.shutdown(Shutdown::Write)
                    };
                    if result.is_ok() {
                        writes_closed = true;
                    }
                    let _ = ack.send(result);
                    wake(&shared.write_waker);
                    progressed = true;
                }
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => {
                    // `Drop` dropped the sender. Half-close if needed, then
                    // leave the loop so this thread can drop the socket.
                    // `Cmd::Shutdown` must not take this path: the read half
                    // is still polling.
                    if !writes_closed {
                        let _ = socket.shutdown(Shutdown::Write);
                    }
                    wake(&shared.read_waker);
                    wake(&shared.write_waker);
                    break;
                }
            }
        }

        let mut wrote_all = false;
        let mut write_err = None;
        if let Some(buf) = outgoing.as_ref() {
            match socket.write(&buf[outgoing_pos..]) {
                Ok(0) => write_err = Some(write_closed()),
                Ok(n) => {
                    outgoing_pos += n;
                    progressed = true;
                    if outgoing_pos >= buf.len() {
                        wrote_all = true;
                    }
                }
                Err(err)
                    if err.kind() == io::ErrorKind::WouldBlock
                        || err.kind() == io::ErrorKind::TimedOut => {}
                Err(err) => write_err = Some(err),
            }
        }
        if wrote_all {
            outgoing = None;
            outgoing_pos = 0;
            wake(&shared.write_waker);
        }
        if let Some(err) = write_err {
            failed = Some(copy_err(&err));
            set_error(&shared, copy_err(&err));
            outgoing = None;
            wake(&shared.write_waker);
            progressed = true;
        }

        if read_stash.is_none() && !saw_eof {
            let mut buf = [0u8; 8192];
            match socket.read(&mut buf) {
                Ok(0) => saw_eof = true,
                Ok(n) => {
                    read_stash = Some(Ok(Bytes::copy_from_slice(&buf[..n])));
                    progressed = true;
                }
                Err(err)
                    if err.kind() == io::ErrorKind::WouldBlock
                        || err.kind() == io::ErrorKind::TimedOut => {}
                Err(err) => {
                    read_stash = Some(Err(err));
                    saw_eof = true;
                    progressed = true;
                }
            }
        }
        if read_stash.is_none() && saw_eof && !eof_queued {
            read_stash = Some(Ok(Bytes::new()));
            eof_queued = true;
        }
        if let Some(msg) = read_stash.take() {
            match reads.try_send(msg) {
                Ok(()) => {
                    wake(&shared.read_waker);
                    progressed = true;
                }
                Err(TrySendError::Full(msg)) => read_stash = Some(msg),
                Err(TrySendError::Disconnected(_)) => {}
            }
        }

        if !progressed {
            thread::park_timeout(tick);
        }
    }
}

fn copy_err(err: &io::Error) -> io::Error {
    io::Error::new(err.kind(), err.to_string())
}

fn status(failed: &Option<io::Error>) -> io::Result<()> {
    match failed {
        Some(err) => Err(copy_err(err)),
        None => Ok(()),
    }
}

fn set_error(shared: &Shared, err: io::Error) {
    *shared.error.lock().expect("writer error lock") = Some(err);
}

fn take_error(shared: &Shared) -> Option<io::Error> {
    shared.error.lock().expect("writer error lock").take()
}

fn park(slot: &Mutex<Option<Waker>>, cx: &Context<'_>) {
    *slot.lock().expect("park lock") = Some(cx.waker().clone());
}

fn wake(slot: &Mutex<Option<Waker>>) {
    let waker = slot.lock().expect("park lock").take();
    if let Some(waker) = waker {
        waker.wake();
    }
}

fn unpark(shared: &Shared) {
    if let Some(io) = shared.io.lock().expect("io thread").as_ref() {
        io.unpark();
    }
}

fn write_closed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "unix socket write closed")
}

impl AsyncRead for UnixStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            if !this.read_rest.is_empty() {
                let n = cmp::min(buf.remaining(), this.read_rest.len());
                if n == 0 {
                    return Poll::Ready(Ok(()));
                }
                buf.put_slice(&this.read_rest[..n]);
                this.read_rest = this.read_rest.slice(n..);
                unpark(&this.shared);
                return Poll::Ready(Ok(()));
            }
            let Some(rx) = this.read_rx.as_ref() else {
                return Poll::Ready(Ok(()));
            };
            match rx.try_recv() {
                Ok(Ok(bytes)) if bytes.is_empty() => return Poll::Ready(Ok(())),
                Ok(Ok(bytes)) => {
                    this.read_rest = bytes;
                    unpark(&this.shared);
                }
                Ok(Err(err)) => return Poll::Ready(Err(err)),
                Err(TryRecvError::Disconnected) => return Poll::Ready(Ok(())),
                Err(TryRecvError::Empty) => {
                    park(&this.shared.read_waker, cx);
                    match rx.try_recv() {
                        Ok(Ok(bytes)) if bytes.is_empty() => return Poll::Ready(Ok(())),
                        Ok(Ok(bytes)) => this.read_rest = bytes,
                        Ok(Err(err)) => return Poll::Ready(Err(err)),
                        Err(TryRecvError::Disconnected) => return Poll::Ready(Ok(())),
                        Err(TryRecvError::Empty) => {
                            unpark(&this.shared);
                            return Poll::Pending;
                        }
                    }
                }
            }
        }
    }
}

impl AsyncWrite for UnixStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if let Some(err) = take_error(&this.shared) {
            return Poll::Ready(Err(err));
        }
        if this.write_shut || matches!(this.wait, Wait::Shutdown(_)) || this.write_tx.is_none() {
            return Poll::Ready(Err(write_closed()));
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        match this.enqueue(cx, Cmd::Write(Bytes::copy_from_slice(buf))) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(buf.len())),
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if matches!(this.wait, Wait::Flush(_) | Wait::Shutdown(_)) {
            return this.poll_barrier(cx);
        }
        if this.write_shut || this.write_tx.is_none() {
            return Poll::Ready(Ok(()));
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        match this.enqueue(cx, Cmd::Flush(ack_tx)) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Ready(Ok(())) => {
                this.wait = Wait::Flush(ack_rx);
                this.poll_barrier(cx)
            }
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if matches!(this.wait, Wait::Flush(_)) {
            match this.poll_barrier(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Ready(Ok(())) => {}
            }
        }
        if matches!(this.wait, Wait::Shutdown(_)) {
            return this.poll_barrier(cx);
        }
        if this.write_shut || this.write_tx.is_none() {
            return Poll::Ready(Ok(()));
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        match this.enqueue(cx, Cmd::Shutdown(ack_tx)) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Ready(Ok(())) => {
                this.wait = Wait::Shutdown(ack_rx);
                this.poll_barrier(cx)
            }
        }
    }
}
