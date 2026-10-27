// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

#![deny(warnings)]

use std::io::{self, Write};
use std::panic;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use bytes::{Bytes, BytesMut};
use clap::{Arg, ArgAction, Command};
use futures::{SinkExt, StreamExt};
use hgproto::sshproto::HgSshCommandDecode;
use hgproto::HgService;
use mercurial_types::NodeHash;
use mononoke::errors::{Error, Result, ResultExt};
use mononoke::listener::{self, ssh_server_mux, Stdio};
use mononoke::repo::{self, RepoClient};
use slog::{crit, error, info, o, Drain, Level, Logger, OwnedKVList, Record, KV};
use slog_term::PlainSyncDecorator;
use tokio_util::codec::Decoder;

fn init_repo<P: AsRef<Path>>(parent_logger: &Logger, repopath: P) -> Result<(PathBuf, repo::Repo)> {
    let repopath = repopath.as_ref();

    let mut sock = PathBuf::from(repopath);
    sock.push(".hg");

    let repo = repo::Repo::new(parent_logger, &sock)
        .chain_err(|| format!("Failed to initialize repo {:?}", repopath))?;

    sock.push("mononoke.sock");

    Ok((sock, repo))
}

struct SenderBytesWrite {
    chan: futures::channel::mpsc::UnboundedSender<Bytes>,
}

impl Write for SenderBytesWrite {
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.chan
            .unbounded_send(Bytes::copy_from_slice(buf))
            .map_err(|err| io::Error::new(io::ErrorKind::BrokenPipe, err))?;
        Ok(buf.len())
    }
}

/// Pass Critical records, and records explicitly marked for the remote client.
struct RemoteFilter<D> {
    inner: D,
}

impl<D> Drain for RemoteFilter<D>
where
    D: Drain<Ok = (), Err = slog::Never>,
{
    type Ok = ();
    type Err = slog::Never;

    fn log(&self, record: &Record, values: &OwnedKVList) -> std::result::Result<(), slog::Never> {
        if record.level().is_at_least(Level::Critical) || record_is_remote(record, values) {
            self.inner.log(record, values)
        } else {
            Ok(())
        }
    }
}

fn record_is_remote(record: &Record, values: &OwnedKVList) -> bool {
    struct Find(bool);

    impl slog::Serializer for Find {
        fn emit_arguments(&mut self, key: slog::Key, val: &std::fmt::Arguments) -> slog::Result {
            if key == "remote" && val.to_string() == "true" {
                self.0 = true;
            }
            Ok(())
        }
    }

    let mut find = Find(false);
    let _ = record.kv().serialize(record, &mut find);
    let _ = values.serialize(record, &mut find);
    find.0
}

fn log_causes(log: &Logger, err: &Error, remote: bool) {
    let mut cause = std::error::Error::source(err);
    while let Some(next) = cause {
        if remote {
            error!(log, "caused by: {}", next; "remote" => "true");
        } else {
            error!(log, "caused by: {}", next);
        }
        cause = next.source();
    }
}

fn command_failed(log: &Logger, err: Error) {
    error!(log, "Command failed: {}", err; "remote" => "true");
    log_causes(log, &err, true);
}

async fn read_request(
    stdin: &mut futures::stream::BoxStream<'static, std::result::Result<Bytes, io::Error>>,
    buf: &mut BytesMut,
    decoder: &mut HgSshCommandDecode,
) -> Result<Option<hgproto::Request>> {
    loop {
        if let Some(req) = decoder.decode(buf)? {
            return Ok(Some(req));
        }
        match stdin.next().await {
            Some(Ok(bytes)) => buf.extend_from_slice(&bytes),
            Some(Err(err)) => return Err(err.into()),
            None => {
                return if buf.is_empty() {
                    Ok(None)
                } else {
                    Err("stdin closed during command".into())
                };
            }
        }
    }
}

async fn read_line(
    stdin: &mut futures::stream::BoxStream<'static, std::result::Result<Bytes, io::Error>>,
    buf: &mut BytesMut,
) -> Result<Option<BytesMut>> {
    loop {
        if let Some(pos) = buf.iter().position(|byte| *byte == b'\n') {
            let mut line = buf.split_to(pos + 1);
            line.truncate(pos);
            return Ok(Some(line));
        }
        match stdin.next().await {
            Some(Ok(bytes)) => buf.extend_from_slice(&bytes),
            Some(Err(err)) => return Err(err.into()),
            None => return Ok(None),
        }
    }
}

async fn read_exact(
    stdin: &mut futures::stream::BoxStream<'static, std::result::Result<Bytes, io::Error>>,
    buf: &mut BytesMut,
    len: usize,
) -> Result<Option<Bytes>> {
    while buf.len() < len {
        match stdin.next().await {
            Some(Ok(bytes)) => buf.extend_from_slice(&bytes),
            Some(Err(err)) => return Err(err.into()),
            None => return Ok(None),
        }
    }
    Ok(Some(buf.split_to(len).freeze()))
}

fn frame_len(line: &[u8]) -> Result<usize> {
    let text = std::str::from_utf8(line).map_err(|_| Error::from("frame length is not utf-8"))?;
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("bad frame length {text:?}").into());
    }
    text.parse()
        .map_err(|_| Error::from("frame length overflows"))
}

async fn read_framed_payload(
    stdin: &mut futures::stream::BoxStream<'static, std::result::Result<Bytes, io::Error>>,
    buf: &mut BytesMut,
) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    loop {
        let line = read_line(stdin, buf)
            .await?
            .ok_or_else(|| Error::from("stdin closed during bundle"))?;
        let len = frame_len(&line)?;
        if len == 0 {
            return Ok(payload);
        }
        let chunk = read_exact(stdin, buf, len)
            .await?
            .ok_or_else(|| Error::from("stdin closed during bundle"))?;
        payload.extend_from_slice(&chunk);
    }
}

async fn accept_unbundle<Si>(
    repo: &repo::Repo,
    stdin: &mut futures::stream::BoxStream<'static, std::result::Result<Bytes, io::Error>>,
    stdout: &mut Si,
    buf: &mut BytesMut,
    heads: Vec<NodeHash>,
) -> Result<Option<hgproto::Request>>
where
    Si: futures::Sink<Bytes, Error = io::Error> + Unpin,
{
    if !repo.heads_match(&heads)? {
        stdout.send(Bytes::from_static(b"2\n0\n")).await?;
        return Ok(None);
    }
    stdout.send(Bytes::from_static(b"0\n")).await?;
    let payload = read_framed_payload(stdin, buf).await?;
    Ok(Some(hgproto::Request::Unbundle {
        heads,
        bundle: Bytes::from(payload),
    }))
}

async fn serve_connection<S>(sock: S, repo: Arc<repo::Repo>, listen_log: Logger) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
{
    let Stdio {
        stdin,
        stdout,
        stderr,
    } = ssh_server_mux(sock);

    let stderr_write = SenderBytesWrite { chan: stderr };
    let decorator = PlainSyncDecorator::new(stderr_write);
    let stderr_drain = RemoteFilter {
        inner: slog_term::FullFormat::new(decorator).build().fuse(),
    };
    let drain = slog::Duplicate::new(stderr_drain, listen_log.clone()).fuse();
    let conn_log = Logger::root(drain, o!());

    let client = RepoClient::new(Arc::clone(&repo), &conn_log);
    let service = Arc::new(HgService::new_with_logger(client, &conn_log));

    let mut stdin = stdin;
    let mut stdout = stdout.sink_map_err(|err| io::Error::new(io::ErrorKind::BrokenPipe, err));
    let mut buf = BytesMut::new();
    let mut decoder = HgSshCommandDecode;

    loop {
        let req = match read_request(&mut stdin, &mut buf, &mut decoder).await {
            Ok(Some(req)) => req,
            Ok(None) => break,
            Err(err) => {
                command_failed(&conn_log, err);
                break;
            }
        };

        let req = match req {
            hgproto::Request::Unbundle { heads, bundle } if bundle.is_empty() => {
                match accept_unbundle(&repo, &mut stdin, &mut stdout, &mut buf, heads).await {
                    Ok(Some(req)) => req,
                    Ok(None) => continue,
                    Err(err) => {
                        command_failed(&conn_log, err);
                        break;
                    }
                }
            }
            other => other,
        };

        match service.command(req).await {
            Ok(response) => {
                let mut out = BytesMut::new();
                hgproto::sshproto::response::encode(&response, &mut out);
                if stdout.send(out.freeze()).await.is_err() {
                    break;
                }
            }
            Err(err) => {
                command_failed(&conn_log, Error::from(err));
                break;
            }
        }
    }
    Ok(())
}

async fn repo_listen(sockname: &Path, repo: repo::Repo, listen_log: &Logger) -> Result<()> {
    let repo = Arc::new(repo);
    let bound = listener::listener(sockname).await?;

    loop {
        let (sock, addr) = bound.accept().await?;
        info!(listen_log, "New connection from {:?}", addr);

        let repo = Arc::clone(&repo);
        let listen_log = listen_log.clone();
        tokio::spawn(async move {
            if let Err(err) = serve_connection(sock, repo, listen_log.clone()).await {
                error!(listen_log, "connection failed: {}", err);
                log_causes(&listen_log, &err, false);
            }
        });
    }
}

fn run<'a, I>(repos: I, root_log: &Logger) -> Result<()>
where
    I: IntoIterator<Item = &'a str>,
{
    let threads = repos
        .into_iter()
        .map(|repopath| {
            init_repo(root_log, repopath).and_then(|(sockname, repo)| {
                let repopath = PathBuf::from(repopath);
                let listen_log = root_log.new(o!("repo" => format!("{:?}", repopath)));
                info!(listen_log, "Listening for connections");

                let t = thread::spawn(move || {
                    let rt = match tokio::runtime::Builder::new_current_thread()
                        .enable_io()
                        .build()
                    {
                        Ok(rt) => rt,
                        Err(err) => return Err(Error::from(err)),
                    };
                    let unw = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                        rt.block_on(repo_listen(&sockname, repo, &listen_log))
                    }));
                    match unw {
                        Err(err) => {
                            crit!(
                                listen_log,
                                "Listener thread {:?} paniced: {:?}",
                                repopath,
                                err
                            );
                            Ok(())
                        }
                        Ok(result) => result,
                    }
                });
                Ok(t)
            })
        })
        .collect::<Vec<_>>();

    for err in threads.iter().filter_map(|t| t.as_ref().err()) {
        error!(root_log, "{}", err);
        log_causes(root_log, err, false);
    }

    for thread in threads.into_iter().filter_map(Result::ok) {
        if let Err(err) = thread.join().expect("thread join failed") {
            error!(root_log, "Listener failure: {:?}", err);
        }
    }

    Ok(())
}

fn main() {
    let matches = Command::new("mononoke server")
        .version("0.0.0")
        .about("serve repos")
        .arg(
            Arg::new("debug")
                .short('d')
                .long("debug")
                .action(ArgAction::SetTrue)
                .help("print debug level output"),
        )
        .arg(
            Arg::new("REPODIR")
                .help("paths to repo dirs (parent of .hg)")
                .required(true)
                .num_args(1..)
                .action(ArgAction::Append),
        )
        .get_matches();

    let level = if matches.get_flag("debug") {
        Level::Debug
    } else {
        Level::Info
    };

    let decorator = PlainSyncDecorator::new(io::stdout());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog::LevelFilter::new(drain, level).fuse();
    let root_log = Logger::root(drain, o!());

    info!(root_log, "Starting up");

    let repos: Vec<&str> = matches
        .get_many::<String>("REPODIR")
        .into_iter()
        .flatten()
        .map(String::as_str)
        .collect();

    if let Err(ref err) = run(repos, &root_log) {
        error!(root_log, "Failed: {}", err);
        log_causes(&root_log, err, false);
        std::process::exit(1);
    }
}
