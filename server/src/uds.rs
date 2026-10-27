// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Winsock `AF_UNIX` sockets.
//!
//! Tokio 1 exports `UnixListener` and `UnixStream` only behind `cfg(unix)`.
//! On Windows the same ssh handoff uses `AF_UNIX` `SOCK_STREAM` sockets and
//! adapts the blocking handles to `AsyncRead` / `AsyncWrite`.

use std::cmp;
use std::io::{self, Read, Write};
use std::net::Shutdown;
use std::path::Path;
use std::pin::Pin;
use std::sync::mpsc::{self, Receiver, Sender};
use std::task::{Context, Poll};
use std::thread;

use bytes::Bytes;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::Stream;
use socket2::{Domain, SockAddr, Socket, Type};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

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
    socket: Socket,
    read_rx: UnboundedReceiver<io::Result<Bytes>>,
    read_rest: Bytes,
    write_tx: Option<Sender<Bytes>>,
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
        let reader = socket.try_clone().expect("clone unix socket for reading");
        let writer = socket.try_clone().expect("clone unix socket for writing");
        let (read_tx, read_rx) = unbounded();
        let (write_tx, write_rx) = mpsc::channel();
        thread::spawn(move || reader_loop(reader, read_tx));
        thread::spawn(move || writer_loop(writer, write_rx));
        UnixStream {
            socket,
            read_rx,
            read_rest: Bytes::new(),
            write_tx: Some(write_tx),
        }
    }
}

impl Drop for UnixStream {
    fn drop(&mut self) {
        self.write_tx.take();
        let _ = self.socket.shutdown(Shutdown::Both);
    }
}

fn reader_loop(mut socket: Socket, tx: UnboundedSender<io::Result<Bytes>>) {
    let mut buf = [0u8; 8192];
    loop {
        match socket.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                if tx
                    .unbounded_send(Ok(Bytes::copy_from_slice(&buf[..n])))
                    .is_err()
                {
                    break;
                }
            }
            Err(err) => {
                let _ = tx.unbounded_send(Err(err));
                break;
            }
        }
    }
}

fn writer_loop(mut socket: Socket, rx: Receiver<Bytes>) {
    while let Ok(buf) = rx.recv() {
        if socket.write_all(&buf).is_err() {
            break;
        }
    }
    let _ = socket.shutdown(Shutdown::Write);
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
                return Poll::Ready(Ok(()));
            }
            match Pin::new(&mut this.read_rx).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                Poll::Ready(Some(Ok(bytes))) if bytes.is_empty() => continue,
                Poll::Ready(Some(Ok(bytes))) => this.read_rest = bytes,
            }
        }
    }
}

impl AsyncWrite for UnixStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let Some(tx) = this.write_tx.as_ref() else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "unix socket write closed",
            )));
        };
        tx.send(Bytes::copy_from_slice(buf))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "unix socket writer stopped"))?;
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Dropping the sender makes the writer thread half-close. The read
        // side stays open until the stream itself is dropped.
        self.get_mut().write_tx.take();
        Poll::Ready(Ok(()))
    }
}
