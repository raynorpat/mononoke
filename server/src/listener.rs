// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::fs;
use std::io;
use std::path::Path;

use bytes::Bytes;
use futures::channel::mpsc;
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use sshrelay::{SshDecoder, SshEncoder, SshMsg, SshStream};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{FramedRead, FramedWrite};

#[cfg(windows)]
use crate::uds::{UnixListener, UnixStream};
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};

pub struct Stdio {
    pub stdin: BoxStream<'static, Result<Bytes, io::Error>>,
    pub stdout: mpsc::Sender<Bytes>,
    pub stderr: mpsc::UnboundedSender<Bytes>,
}

/// Bind `sockname`, replacing a stale socket file when nothing is listening.
///
/// If bind fails because the path exists, connect. Connect success means
/// someone is already serving: return the bind error. Connect failure deletes
/// the file and binds again. Connecting is treated as a no-op for the peer.
pub async fn listener<P: AsRef<Path>>(sockname: P) -> io::Result<UnixListener> {
    let sockname = sockname.as_ref();
    loop {
        match UnixListener::bind(sockname) {
            Ok(bound) => return Ok(bound),
            Err(err) if bind_failed_because_path_exists(&err, sockname) => {
                match UnixStream::connect(sockname).await {
                    Ok(_conn) => return Err(err),
                    Err(_) => {
                        if sockname.exists() {
                            fs::remove_file(sockname)?;
                        } else {
                            return Err(err);
                        }
                    }
                }
            }
            Err(err) => return Err(err),
        }
    }
}

fn bind_failed_because_path_exists(err: &io::Error, path: &Path) -> bool {
    path.exists()
        || err.kind() == io::ErrorKind::AddrInUse
        || err.kind() == io::ErrorKind::AlreadyExists
        || err.raw_os_error() == Some(10048)
}

/// Split a connected client into stdin bytes and stdout/stderr senders.
///
/// Stdout and stderr are framed back onto the socket from a spawned task.
/// There is no reactor `Handle`; the caller must be inside a tokio runtime.
pub fn ssh_server_mux<S>(sock: S) -> Stdio
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (rx, tx) = tokio::io::split(sock);
    let rd = FramedRead::new(rx, SshDecoder::new());
    let wr = FramedWrite::new(tx, SshEncoder::new());

    let stdin = rd
        .filter_map(|item| async move {
            match item {
                Ok(msg) if msg.stream() == SshStream::Stdin => Some(Ok(msg.data())),
                Ok(_) => None,
                Err(err) => Some(Err(err)),
            }
        })
        .boxed();

    let (stdout, stdout_rx) = mpsc::channel(1);
    let (stderr, stderr_rx) = mpsc::unbounded();

    tokio::spawn(async move {
        let mut outgoing = stream::select(
            stdout_rx.map(|bytes| SshMsg::new(SshStream::Stdout, bytes)),
            stderr_rx.map(|bytes| SshMsg::new(SshStream::Stderr, bytes)),
        );
        let mut wr = std::pin::pin!(wr);
        while let Some(msg) = outgoing.next().await {
            if futures::SinkExt::send(&mut wr, msg).await.is_err() {
                break;
            }
        }
    });

    Stdio {
        stdin,
        stdout,
        stderr,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::future::Future;
    use std::io::Write;
    use std::path::PathBuf;
    use std::task::Poll;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::listener;

    fn sock_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("mn{label}{nanos}"));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn unix_socket_round_trip() {
        let dir = sock_dir("rt");
        let path = dir.join("mononoke.sock");
        let bound = listener(&path).await.expect("bind");

        // Tokio 1 only exports UnixStream on Unix. Windows connects with the
        // Winsock AF_UNIX stream that `listener` accepts.
        #[cfg(unix)]
        let mut client = tokio::net::UnixStream::connect(&path)
            .await
            .expect("connect");
        #[cfg(windows)]
        let mut client = crate::uds::UnixStream::connect(&path)
            .await
            .expect("connect");
        client.write_all(b"ping").await.unwrap();

        let (mut server, _) = bound.accept().await.unwrap();
        let mut buf = [0u8; 4];
        server.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        drop(server);
        drop(client);
        drop(bound);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_dir(&dir);
    }

    #[tokio::test]
    async fn flushed_write_survives_drop() {
        let dir = sock_dir("fd");
        let path = dir.join("mononoke.sock");
        let bound = listener(&path).await.expect("bind");

        #[cfg(unix)]
        let mut client = tokio::net::UnixStream::connect(&path)
            .await
            .expect("connect");
        #[cfg(windows)]
        let mut client = crate::uds::UnixStream::connect(&path)
            .await
            .expect("connect");

        let (mut server, _) = bound.accept().await.unwrap();
        let payload = b"pong-flush";
        server.write_all(payload).await.unwrap();
        server.flush().await.unwrap();
        drop(server);

        let mut got = Vec::new();
        let read = tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut got)).await;
        let n = read.expect("peer read timed out").expect("peer read");
        assert_eq!(n, payload.len());
        assert_eq!(got, payload);

        drop(client);
        drop(bound);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_dir(&dir);
    }

    #[tokio::test]
    async fn read_survives_write_shutdown() {
        let dir = sock_dir("hs");
        let path = dir.join("mononoke.sock");
        let bound = listener(&path).await.expect("bind");

        #[cfg(unix)]
        let mut client = tokio::net::UnixStream::connect(&path)
            .await
            .expect("connect");
        #[cfg(windows)]
        let mut client = crate::uds::UnixStream::connect(&path)
            .await
            .expect("connect");

        let (server, _) = bound.accept().await.unwrap();
        let (mut server_read, mut server_write) = tokio::io::split(server);
        server_write.shutdown().await.expect("shutdown write");

        let mut probe = [0u8; 1];
        let early =
            tokio::time::timeout(Duration::from_millis(50), server_read.read(&mut probe)).await;
        assert!(
            early.is_err(),
            "write shutdown looked like EOF on the read half: {early:?}"
        );

        let payload = b"after-half";
        client.write_all(payload).await.unwrap();
        client.flush().await.unwrap();

        let mut got = [0u8; 10];
        let read =
            tokio::time::timeout(Duration::from_secs(2), server_read.read_exact(&mut got)).await;
        read.expect("read after write shutdown timed out")
            .expect("read after write shutdown");
        assert_eq!(&got, payload);

        drop(server_read);
        drop(server_write);
        drop(client);
        drop(bound);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_dir(&dir);
    }

    #[tokio::test]
    async fn stale_socket_file_is_replaced() {
        let dir = sock_dir("st");
        let path = dir.join("mononoke.sock");
        let mut file = fs::File::create(&path).unwrap();
        file.write_all(b"").unwrap();
        drop(file);

        let bound = listener(&path)
            .await
            .unwrap_or_else(|err| panic!("stale socket replaced: {err}"));
        {
            // Keep an accept in flight while a second bind probes the socket.
            let accept = bound.accept();
            let mut accept = std::pin::pin!(accept);
            let _ = futures::future::poll_fn(|cx| {
                let _ = accept.as_mut().poll(cx);
                Poll::Ready(())
            })
            .await;

            let second = listener(&path).await;
            assert!(
                second.is_err(),
                "second listener should fail while the first accepts"
            );
        }
        drop(bound);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_dir(&dir);
    }
}
