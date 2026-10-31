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
use mercurial::revlogrepo::Required;
use mercurial_types::NodeHash;
use mononoke::errors::{Error, Result, ResultExt};
use mononoke::lfs::store::{LfsStore, StoreSpec};
use mononoke::lfs::LfsServing;
use mononoke::listener::{self, ssh_server_mux, Stdio};
use mononoke::push;
use mononoke::repo::{self, RepoClient};
use slog::{crit, error, info, o, Drain, Level, Logger, OwnedKVList, Record, KV};
use slog_term::PlainSyncDecorator;
use tokio_util::codec::Decoder;

struct ServeOptions {
    lfs_store: StoreSpec,
    lfs_endpoint: Option<String>,
    public_url: Option<String>,
    max_plain_file_size: u64,
}

/// `log` receives full backend errors, so it must be the server's, never a connection's.
fn lfs_serving(log: &Logger, hg_dir: &Path, options: &ServeOptions) -> Result<LfsServing> {
    // `LfsStore::open` creates its directories, which must not appear in a path that is no repo.
    let store_dir = mercurial::RevlogRepo::open(hg_dir)?.store_path();
    let endpoint = options.lfs_endpoint.as_deref();
    let store = LfsStore::open(&options.lfs_store, endpoint, &store_dir, log)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime
        .block_on(store.check())
        .map_err(|err| format!("LFS store is not reachable: {err}"))?;
    Ok(LfsServing {
        store: Arc::new(store),
        public_url: options.public_url.clone(),
    })
}

fn init_repo<P: AsRef<Path>>(
    parent_logger: &Logger,
    repopath: P,
    first: bool,
    options: &ServeOptions,
) -> Result<(PathBuf, repo::Repo)> {
    let repopath = repopath.as_ref();

    let mut sock = PathBuf::from(repopath);
    sock.push(".hg");

    // Only the first repo has HTTP, which LFS blobs travel over.
    let lfs = if first {
        Some(lfs_serving(parent_logger, &sock, options)?)
    } else {
        None
    };
    let repo_options = repo::RepoOptions {
        lfs,
        max_plain_file_size: options.max_plain_file_size,
    };
    let repo = repo::Repo::new(parent_logger, &sock, repo_options)
        .chain_err(|| format!("Failed to initialize repo {:?}", repopath))?;
    if !first && repo.hgrepo().get_requirements().contains(&Required::Lfs) {
        return Err(format!(
            "lfs repositories need HTTP; serve {} first",
            repopath.display()
        )
        .into());
    }

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
        let mut out = BytesMut::new();
        hgproto::sshproto::response::encode(&push::heads_error(), &mut out);
        stdout.send(out.freeze()).await?;
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
            // Changegroups have no ssh framing, so each piece goes out as it is produced.
            Ok(
                hgproto::Response::Getbundle(bundle)
                | hgproto::Response::Changegroup(bundle)
                | hgproto::Response::Changegroupsubset(bundle)
                | hgproto::Response::Streamout(bundle)
                | hgproto::Response::GetCachedBundleInline(bundle),
            ) => {
                let mut body = bundle.body;
                while let Some(bytes) = body.next().await {
                    let bytes = match bytes {
                        Ok(bytes) => bytes,
                        Err(err) => {
                            command_failed(&conn_log, Error::from(err));
                            return Ok(());
                        }
                    };
                    if stdout.send(bytes).await.is_err() {
                        return Ok(());
                    }
                }
            }
            Ok(mut response) => {
                match &mut response {
                    hgproto::Response::Capabilities(caps) => caps.push("protocaps".into()),
                    hgproto::Response::Hello(map) => {
                        if let Some(caps) = map.get_mut("capabilities") {
                            caps.push("protocaps".into());
                        }
                    }
                    _ => {}
                }
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

async fn repo_listen(
    sockname: &Path,
    repo: repo::Repo,
    listen_log: &Logger,
    http: Option<(String, u16)>,
    https: Option<(String, u16, PathBuf, PathBuf)>,
) -> Result<()> {
    let repo = Arc::new(repo);
    let warm_repo = Arc::clone(&repo);
    let warm_log = listen_log.clone();
    tokio::task::spawn_blocking(move || match warm_repo.warm_up() {
        Ok(()) => info!(warm_log, "Warmed up"),
        Err(err) => error!(warm_log, "Warm-up failed: {}", err),
    });
    // Bind HTTP and HTTPS before the unix socket so a client that waits for
    // the socket file knows the TCP ports are already accepting.
    if let Some((addr, port)) = http {
        let bind_addr = format!("{addr}:{port}");
        let tcp = tokio::net::TcpListener::bind(&bind_addr)
            .await
            .chain_err(|| format!("HTTP bind {bind_addr}"))?;
        info!(listen_log, "Listening for HTTP on {}", bind_addr);
        let http_repo = Arc::clone(&repo);
        let http_log = listen_log.clone();
        tokio::spawn(async move {
            if let Err(err) = mononoke::http::serve_http(tcp, http_repo).await {
                error!(http_log, "HTTP listener failed: {}", err);
                log_causes(&http_log, &err, false);
            }
        });
    }
    if let Some((addr, port, cert, key)) = https {
        let acceptor = mononoke::http::tls_acceptor(&cert, &key)?;
        let bind_addr = format!("{addr}:{port}");
        let tcp = tokio::net::TcpListener::bind(&bind_addr)
            .await
            .chain_err(|| format!("HTTPS bind {bind_addr}"))?;
        info!(listen_log, "Listening for HTTPS on {}", bind_addr);
        let https_repo = Arc::clone(&repo);
        let https_log = listen_log.clone();
        tokio::spawn(async move {
            if let Err(err) = mononoke::http::serve_https(tcp, https_repo, acceptor).await {
                error!(https_log, "HTTPS listener failed: {}", err);
                log_causes(&https_log, &err, false);
            }
        });
    }

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

fn run<'a, I>(
    repos: I,
    http_addr: &str,
    http_port: u16,
    https: Option<(String, u16, PathBuf, PathBuf)>,
    options: &ServeOptions,
    root_log: &Logger,
) -> Result<()>
where
    I: IntoIterator<Item = &'a str>,
{
    // Every repo is checked before any listens: a repo that fails stops the server.
    let repos = repos
        .into_iter()
        .enumerate()
        .map(|(index, repopath)| {
            let (sockname, repo) = init_repo(root_log, repopath, index == 0, options)?;
            Ok((repopath, sockname, repo))
        })
        .collect::<Result<Vec<_>>>()?;

    let mut http = Some((http_addr.to_string(), http_port));
    let mut https = https;
    let threads = repos
        .into_iter()
        .map(|(repopath, sockname, repo)| {
            let http = http.take();
            let https = https.take();
            let repopath = PathBuf::from(repopath);
            let listen_log = root_log.new(o!("repo" => format!("{:?}", repopath)));
            info!(listen_log, "Listening for connections");

            thread::spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(err) => return Err(Error::from(err)),
                };
                let unw = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                    rt.block_on(repo_listen(&sockname, repo, &listen_log, http, https))
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
            })
        })
        .collect::<Vec<_>>();

    for thread in threads {
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
            Arg::new("http-addr")
                .long("http-addr")
                .value_name("ADDR")
                .default_value("127.0.0.1")
                .help("HTTP listen address"),
        )
        .arg(
            Arg::new("http-port")
                .long("http-port")
                .value_name("PORT")
                .default_value("8000")
                .help("HTTP listen port"),
        )
        .arg(
            Arg::new("https-port")
                .long("https-port")
                .value_name("PORT")
                .default_value("8443")
                .help("HTTPS listen port"),
        )
        .arg(
            Arg::new("tls-cert")
                .long("tls-cert")
                .value_name("PATH")
                .help("PEM certificate for HTTPS"),
        )
        .arg(
            Arg::new("tls-key")
                .long("tls-key")
                .value_name("PATH")
                .help("PEM private key for HTTPS"),
        )
        .arg(
            Arg::new("lfs-store")
                .long("lfs-store")
                .value_name("STORE")
                .default_value("repo")
                .help("LFS blob store: repo, s3://bucket[/prefix] or azure://account/container[/prefix]"),
        )
        .arg(
            Arg::new("lfs-endpoint")
                .long("lfs-endpoint")
                .value_name("URL")
                .help("S3-compatible endpoint, only with an s3:// --lfs-store"),
        )
        .arg(
            Arg::new("max-plain-file-size")
                .long("max-plain-file-size")
                .value_name("BYTES")
                .default_value("10485760")
                .help("largest file a push may store outside LFS, 0 for no limit"),
        )
        .arg(
            Arg::new("public-url")
                .long("public-url")
                .value_name("URL")
                .help("base URL for LFS transfers, instead of the request's Host"),
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

    let http_addr = matches
        .get_one::<String>("http-addr")
        .expect("http-addr has a default");
    let http_port_text = matches
        .get_one::<String>("http-port")
        .expect("http-port has a default");
    let http_port = match http_port_text.parse::<u16>() {
        Ok(port) => port,
        Err(_) => {
            error!(root_log, "invalid --http-port {}", http_port_text);
            std::process::exit(1);
        }
    };

    let https = match (
        matches.get_one::<String>("tls-cert"),
        matches.get_one::<String>("tls-key"),
    ) {
        (None, None) => None,
        (Some(_), None) => {
            error!(root_log, "missing --tls-key");
            std::process::exit(1);
        }
        (None, Some(_)) => {
            error!(root_log, "missing --tls-cert");
            std::process::exit(1);
        }
        (Some(cert), Some(key)) => {
            let https_port_text = matches
                .get_one::<String>("https-port")
                .expect("https-port has a default");
            let https_port = match https_port_text.parse::<u16>() {
                Ok(port) => port,
                Err(_) => {
                    error!(root_log, "invalid --https-port {}", https_port_text);
                    std::process::exit(1);
                }
            };
            Some((
                http_addr.clone(),
                https_port,
                PathBuf::from(cert),
                PathBuf::from(key),
            ))
        }
    };

    let lfs_store_text = matches
        .get_one::<String>("lfs-store")
        .expect("lfs-store has a default");
    let lfs_store = match lfs_store_text.parse::<StoreSpec>() {
        Ok(spec) => spec,
        Err(err) => {
            error!(root_log, "{}", err);
            std::process::exit(1);
        }
    };
    let max_plain_text = matches
        .get_one::<String>("max-plain-file-size")
        .expect("max-plain-file-size has a default");
    let max_plain_file_size = match max_plain_text.parse::<u64>() {
        Ok(size) => size,
        Err(_) => {
            error!(root_log, "invalid --max-plain-file-size {}", max_plain_text);
            std::process::exit(1);
        }
    };
    let options = ServeOptions {
        lfs_store,
        lfs_endpoint: matches.get_one::<String>("lfs-endpoint").cloned(),
        public_url: matches.get_one::<String>("public-url").cloned(),
        max_plain_file_size,
    };

    if let Err(ref err) = run(repos, http_addr, http_port, https, &options, &root_log) {
        error!(root_log, "Failed: {}", err);
        log_causes(&root_log, err, false);
        // The log goes to stdout, which a service manager may not show for a refused start.
        eprintln!("mononoke: {err}");
        std::process::exit(1);
    }
}
