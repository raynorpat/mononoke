// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Mercurial HTTP wire protocol version 1.
//!
//! Arguments arrive on the query string. `unbundle` carries the bundle in the
//! POST body. Response bodies are the command bytes with no SSH length prefix.

use std::collections::HashMap;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use hgproto::{BundleStream, HgService, Request, Response};
use slog::{error, info, o, Drain, Logger};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;

use crate::errors::{Error, Result};
use crate::lfs;
use crate::repo::{Repo, RepoClient};

const MAX_HEADERS: usize = 2 * 1024 * 1024;
const MAX_BODY: usize = 512 * 1024 * 1024;

pub async fn serve_http(listener: TcpListener, repo: Arc<Repo>) -> Result<()> {
    let log = http_logger();
    loop {
        let (sock, addr) = listener.accept().await?;
        let _ = sock.set_nodelay(true);
        info!(log, "HTTP connection from {:?}", addr);
        let repo = Arc::clone(&repo);
        let log = log.clone();
        tokio::spawn(async move {
            if let Err(err) = connection(sock, repo, log.clone(), "http").await {
                error!(log, "HTTP connection failed: {}", err);
            }
        });
    }
}

/// Server-authenticated TLS. Clients are not asked for a certificate.
pub fn tls_acceptor(cert_path: &Path, key_path: &Path) -> Result<TlsAcceptor> {
    install_crypto_provider();
    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;
    let config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|err| Error::from(format!("tls config: {err}")))?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Accept TLS, then run the same HTTP v1 connection handler as `serve_http`.
pub async fn serve_https(
    listener: TcpListener,
    repo: Arc<Repo>,
    acceptor: TlsAcceptor,
) -> Result<()> {
    let log = http_logger();
    loop {
        let (sock, addr) = listener.accept().await?;
        let _ = sock.set_nodelay(true);
        info!(log, "HTTPS connection from {:?}", addr);
        let repo = Arc::clone(&repo);
        let log = log.clone();
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            let sock = match acceptor.accept(sock).await {
                Ok(tls) => tls,
                Err(err) => {
                    error!(log, "TLS handshake failed: {}", err);
                    return;
                }
            };
            if let Err(err) = connection(sock, repo, log.clone(), "https").await {
                error!(log, "HTTPS connection failed: {}", err);
            }
        });
    }
}

fn install_crypto_provider() {
    // rustls 0.23 has no process-default provider until one is installed.
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    });
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(path)
        .map_err(|err| Error::from(format!("open tls cert {}: {err}", path.display())))?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|err| Error::from(format!("parse tls cert {}: {err}", path.display())))?;
    if certs.is_empty() {
        return Err(format!("tls cert {} is empty", path.display()).into());
    }
    Ok(certs)
}

fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let file = std::fs::File::open(path)
        .map_err(|err| Error::from(format!("open tls key {}: {err}", path.display())))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|err| Error::from(format!("parse tls key {}: {err}", path.display())))?
        .ok_or_else(|| Error::from(format!("tls key {} is empty", path.display())))
}

fn http_logger() -> Logger {
    let decorator = slog_term::PlainSyncDecorator::new(std::io::stderr());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog::LevelFilter::new(drain, slog::Level::Info).fuse();
    Logger::root(drain, o!("proto" => "http"))
}

pub(crate) async fn connection<S>(
    mut sock: S,
    repo: Arc<Repo>,
    log: Logger,
    scheme: &'static str,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let client = RepoClient::new(Arc::clone(&repo), &log);
    let service = HgService::new_with_logger(client, &log);
    let mut buf = Vec::new();
    loop {
        let head = match read_head(&mut sock, &mut buf).await? {
            Some(head) => head,
            None => return Ok(()),
        };
        if let Some(route) = lfs::http::route(&head.target) {
            let serving = repo.lfs();
            let close =
                lfs::http::serve(&mut sock, &mut buf, &head, route, serving, scheme, &log).await?;
            if close {
                return Ok(());
            }
            continue;
        }
        let incoming = read_incoming(&mut sock, &mut buf, head).await?;
        if incoming.method != "GET" && incoming.method != "POST" {
            let msg = format!("unsupported HTTP method {}", incoming.method);
            write_hg(&mut sock, "application/hg-error", msg.as_bytes(), true).await?;
            return Ok(());
        }
        let cmd = match command_name(&incoming) {
            Ok(cmd) => cmd,
            Err(err) => {
                let msg = err.to_string();
                write_hg(&mut sock, "application/hg-error", msg.as_bytes(), true).await?;
                return Ok(());
            }
        };
        info!(log, "HTTP {} {}", incoming.method, cmd);
        let close = incoming.close;
        let body = match dispatch(&service, &incoming, &cmd).await {
            Ok(
                Response::Getbundle(bundle)
                | Response::Changegroup(bundle)
                | Response::Changegroupsubset(bundle)
                | Response::GetCachedBundleInline(bundle),
            ) => {
                let engine = bundle_engine(&incoming.protocaps, bundle.prefer_uncompressed);
                // The 200 is already sent if the bundle fails, so the client sees a cut-off body.
                match write_hg_stream(&mut sock, bundle, engine, close).await {
                    Ok(sent) => info!(
                        log,
                        "HTTP {} -> {} bytes streamed ({})",
                        cmd,
                        sent,
                        engine.unwrap_or("0.1 zlib")
                    ),
                    Err(err) => {
                        error!(log, "HTTP {} failed while streaming: {}", cmd, err);
                        return Ok(());
                    }
                }
                if close {
                    return Ok(());
                }
                continue;
            }
            Ok(Response::Streamout(bundle)) => {
                write_legacy_stream(&mut sock, bundle, close).await?;
                if close {
                    return Ok(());
                }
                continue;
            }
            other => other.and_then(|response| response_body(&response)),
        };
        match body {
            Ok(body) => {
                info!(log, "HTTP {} -> {} bytes", cmd, body.len());
                write_hg(&mut sock, "application/mercurial-0.1", &body, close).await?;
            }
            Err(err) => {
                error!(log, "HTTP {} failed: {}", cmd, err);
                let msg = err.to_string();
                write_hg(&mut sock, "application/hg-error", msg.as_bytes(), true).await?;
                return Ok(());
            }
        }
        if close {
            return Ok(());
        }
    }
}

async fn dispatch(
    service: &HgService<RepoClient>,
    incoming: &Incoming,
    cmd: &str,
) -> Result<Response> {
    let req = frame_request(cmd, &incoming.query, &incoming.body)?;
    Ok(service.command(req).await?)
}

struct Incoming {
    method: String,
    target: String,
    query: HashMap<String, Vec<u8>>,
    body: Vec<u8>,
    close: bool,
    /// The `X-HgProto` value, such as `0.1 0.2 comp=zstd,zlib,none partial-pull`.
    protocaps: String,
}

fn command_name(incoming: &Incoming) -> Result<String> {
    let raw = incoming
        .query
        .get("cmd")
        .ok_or_else(|| Error::from(format!("missing cmd in {}", incoming.target)))?;
    let cmd = std::str::from_utf8(raw).map_err(|_| Error::from("cmd is not utf-8"))?;
    if cmd.is_empty() {
        return Err("empty cmd".into());
    }
    Ok(cmd.to_string())
}

/// A request line and its headers, with the body still unread.
pub(crate) struct Head {
    pub(crate) method: String,
    pub(crate) target: String,
    pub(crate) version: String,
    pub(crate) headers: HashMap<String, String>,
}

async fn read_head<S>(sock: &mut S, buf: &mut Vec<u8>) -> Result<Option<Head>>
where
    S: AsyncRead + Unpin,
{
    let header = loop {
        if let Some(end) = find_double_crlf(buf) {
            break buf.drain(..end).collect::<Vec<_>>();
        }
        if buf.len() > MAX_HEADERS {
            return Err("HTTP headers too large".into());
        }
        if read_some(sock, buf).await? == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            return Err("client closed during HTTP headers".into());
        }
    };
    parse_head(&header).map(Some)
}

fn parse_head(header: &[u8]) -> Result<Head> {
    let text =
        std::str::from_utf8(header).map_err(|_| Error::from("HTTP headers are not utf-8"))?;
    let mut lines = text.split("\r\n");
    let request_line = lines
        .next()
        .filter(|line| !line.is_empty())
        .ok_or_else(|| Error::from("empty HTTP request"))?;
    let mut parts = request_line.split(' ');
    let method = parts
        .next()
        .ok_or_else(|| Error::from("missing HTTP method"))?
        .to_string();
    let target = parts
        .next()
        .ok_or_else(|| Error::from("missing HTTP target"))?
        .to_string();
    let version = parts.next().unwrap_or("HTTP/1.1").to_string();

    let mut headers = HashMap::<String, String>::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if let Some((name, value)) = line.split_once(':') {
            let (name, value) = (name.trim().to_ascii_lowercase(), value.trim());
            match headers.get_mut(&name) {
                // Joined, conflicting lengths fail to parse instead of the last one winning.
                Some(length) if name == "content-length" && length != value => {
                    length.push(',');
                    length.push_str(value);
                }
                _ => {
                    headers.insert(name, value.to_string());
                }
            }
        }
    }
    Ok(Head {
        method,
        target,
        version,
        headers,
    })
}

async fn read_incoming<S>(sock: &mut S, buf: &mut Vec<u8>, head: Head) -> Result<Incoming>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let Head {
        method,
        target,
        version,
        headers,
    } = head;
    let length = match headers.get("content-length") {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| Error::from("bad content-length"))?,
        None => 0,
    };
    let body = read_body(sock, buf, &headers, length).await?;

    let query_text = match target.split_once('?') {
        Some((_, query)) => query.split('#').next().unwrap_or("").to_string(),
        None => String::new(),
    };
    let close = connection_closes(&version, headers.get("connection").map(String::as_str));
    // Mercurial splits a long `X-HgProto` value across `X-HgProto-1`, `-2`, ...
    let protocaps = (1..)
        .map_while(|n| headers.get(&format!("x-hgproto-{n}")))
        .map(String::as_str)
        .collect();
    Ok(Incoming {
        method,
        target,
        query: parse_query(&query_text)?,
        body,
        close,
        protocaps,
    })
}

/// A body held in memory, so capped at `MAX_BODY`.
pub(crate) async fn read_body<S>(
    sock: &mut S,
    buf: &mut Vec<u8>,
    headers: &HashMap<String, String>,
    length: usize,
) -> Result<Vec<u8>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if length > MAX_BODY {
        return Err("HTTP body too large".into());
    }
    if buf.len() < length {
        write_continue(sock, headers).await?;
    }
    read_exact(sock, buf, length).await
}

/// Tell a client waiting on `Expect: 100-continue` to send its body.
pub(crate) async fn write_continue<S>(sock: &mut S, headers: &HashMap<String, String>) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    let expects_continue = headers
        .get("expect")
        .map(|value| value.eq_ignore_ascii_case("100-continue"))
        .unwrap_or(false);
    if expects_continue {
        sock.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").await?;
    }
    Ok(())
}

pub(crate) fn connection_closes(version: &str, connection: Option<&str>) -> bool {
    let connection = connection.unwrap_or("").to_ascii_lowercase();
    let tokens = connection.split(',').map(str::trim);
    if tokens.clone().any(|token| token == "close") {
        return true;
    }
    if tokens.clone().any(|token| token == "keep-alive") {
        return false;
    }
    version.starts_with("HTTP/1.0")
}

fn parse_query(query: &str) -> Result<HashMap<String, Vec<u8>>> {
    let mut map = HashMap::new();
    if query.is_empty() {
        return Ok(map);
    }
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = String::from_utf8(form_decode(key)?)
            .map_err(|_| Error::from("query key is not utf-8"))?;
        let value = form_decode(value)?;
        map.entry(key).or_insert(value);
    }
    Ok(map)
}

fn form_decode(input: &str) -> Result<Vec<u8>> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut idx = 0;
    while idx < bytes.len() {
        match bytes[idx] {
            b'+' => {
                out.push(b' ');
                idx += 1;
            }
            b'%' => {
                if idx + 2 >= bytes.len() {
                    return Err("truncated percent escape".into());
                }
                let hi = hex_nibble(bytes[idx + 1])?;
                let lo = hex_nibble(bytes[idx + 2])?;
                out.push((hi << 4) | lo);
                idx += 3;
            }
            byte => {
                out.push(byte);
                idx += 1;
            }
        }
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err("bad percent escape".into()),
    }
}

fn find_double_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|idx| idx + 4)
}

async fn read_some<S>(sock: &mut S, buf: &mut Vec<u8>) -> Result<usize>
where
    S: AsyncRead + Unpin,
{
    let mut tmp = [0u8; 8192];
    let n = sock.read(&mut tmp).await?;
    buf.extend_from_slice(&tmp[..n]);
    Ok(n)
}

async fn read_exact<S>(sock: &mut S, buf: &mut Vec<u8>, len: usize) -> Result<Vec<u8>>
where
    S: AsyncRead + Unpin,
{
    while buf.len() < len {
        if read_some(sock, buf).await? == 0 {
            return Err("client closed during HTTP body".into());
        }
    }
    Ok(buf.drain(..len).collect())
}

async fn write_hg<S>(sock: &mut S, content_type: &str, body: &[u8], close: bool) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    write_status(sock, 200, "OK", content_type, body, close).await
}

pub(crate) async fn write_status<S>(
    sock: &mut S,
    status: u16,
    reason: &str,
    content_type: &str,
    body: &[u8],
    close: bool,
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    let connection = if close { "close" } else { "keep-alive" };
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: {connection}\r\n\r\n",
        body.len()
    );
    sock.write_all(header.as_bytes()).await?;
    sock.write_all(body).await?;
    Ok(())
}

/// Compression engines in server preference order, as Mercurial's server
/// ranks them. Advertised to HTTP clients as `compression=`.
const SERVER_ENGINES: [&str; 2] = ["zstd", "zlib"];
/// zstd level, Mercurial's server default.
const ZSTD_LEVEL: i32 = 3;

/// The media type 0.2 compression engine for a `getbundle`, picked as
/// Mercurial's `_httpresponsetype` does from the client's `X-HgProto` value.
/// `None` means media type 0.1, which is always zlib.
fn bundle_engine(protocaps: &str, prefer_uncompressed: bool) -> Option<&'static str> {
    let caps: Vec<&str> = protocaps.split(' ').collect();
    if !caps.contains(&"0.2") {
        return None;
    }
    // Every 0.2 client accepts uncompressed data.
    if prefer_uncompressed {
        return Some("none");
    }
    let client: Vec<&str> = caps
        .iter()
        .find_map(|cap| cap.strip_prefix("comp="))
        .map_or(vec!["zlib", "none"], |list| list.split(',').collect());
    // No engine in common falls back to 0.1.
    SERVER_ENGINES
        .into_iter()
        .find(|engine| client.contains(engine))
}

/// A `getbundle` body compressor writing into a buffer the caller drains.
enum Compressor {
    None,
    Zlib(flate2::write::ZlibEncoder<Vec<u8>>),
    Zstd(zstd::stream::write::Encoder<'static, Vec<u8>>),
}

impl Compressor {
    fn new(engine: &str) -> Result<Self> {
        Ok(match engine {
            "none" => Compressor::None,
            "zstd" => Compressor::Zstd(zstd::stream::write::Encoder::new(Vec::new(), ZSTD_LEVEL)?),
            _ => Compressor::Zlib(flate2::write::ZlibEncoder::new(
                Vec::new(),
                flate2::Compression::default(),
            )),
        })
    }

    /// Compress `data` and move whatever output is ready onto `out`.
    fn write(&mut self, data: &[u8], out: &mut Vec<u8>) -> Result<()> {
        use std::io::Write;
        match self {
            Compressor::None => out.extend_from_slice(data),
            Compressor::Zlib(enc) => {
                enc.write_all(data)?;
                out.append(enc.get_mut());
            }
            Compressor::Zstd(enc) => {
                enc.write_all(data)?;
                out.append(enc.get_mut());
            }
        }
        Ok(())
    }

    fn finish(self, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Compressor::None => {}
            Compressor::Zlib(enc) => out.extend_from_slice(&enc.finish()?),
            Compressor::Zstd(enc) => out.extend_from_slice(&enc.finish()?),
        }
        Ok(())
    }
}

/// Chunked `getbundle` body, compressed as the bundle is produced. Returns the bytes sent.
///
/// With `engine`, the body is media type 0.2: a length byte and the engine
/// name, then the compressed bundle. Without it, media type 0.1, which
/// Mercurial's HTTP client zlib-decompresses for `getbundle`
/// (`httppeer._callcompressable`).
async fn write_hg_stream<S>(
    sock: &mut S,
    bundle: BundleStream,
    engine: Option<&str>,
    close: bool,
) -> Result<u64>
where
    S: AsyncWrite + Unpin,
{
    let connection = if close { "close" } else { "keep-alive" };
    let media = if engine.is_some() { "0.2" } else { "0.1" };
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/mercurial-{media}\r\nTransfer-Encoding: chunked\r\nConnection: {connection}\r\n\r\n"
    );
    sock.write_all(header.as_bytes()).await?;
    let mut out = Vec::new();
    if let Some(name) = engine {
        out.push(name.len() as u8);
        out.extend_from_slice(name.as_bytes());
    }
    let mut compressor = Compressor::new(engine.unwrap_or("zlib"))?;
    let mut body = bundle.body;
    let mut sent = 0;
    while let Some(bytes) = body.next().await {
        compressor.write(&bytes?, &mut out)?;
        sent += write_chunk(sock, &mut out).await?;
    }
    compressor.finish(&mut out)?;
    sent += write_chunk(sock, &mut out).await?;
    sock.write_all(b"0\r\n\r\n").await?;
    Ok(sent)
}

/// `streamreslegacy` is raw even when HTTP compression has been negotiated.
async fn write_legacy_stream<S: AsyncWrite + Unpin>(
    sock: &mut S,
    bundle: BundleStream,
    close: bool,
) -> Result<()> {
    let connection = if close { "close" } else { "keep-alive" };
    sock.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/mercurial-0.1\r\nTransfer-Encoding: chunked\r\nConnection: {connection}\r\n\r\n").as_bytes()).await?;
    let mut body = bundle.body;
    while let Some(bytes) = body.next().await {
        write_chunk(sock, &mut bytes?.to_vec()).await?;
    }
    sock.write_all(b"0\r\n\r\n").await?;
    Ok(())
}

/// Write `buf` as one HTTP chunk and clear it. An empty `buf` writes nothing,
/// because an empty chunk ends the body.
async fn write_chunk<S>(sock: &mut S, buf: &mut Vec<u8>) -> Result<u64>
where
    S: AsyncWrite + Unpin,
{
    if buf.is_empty() {
        return Ok(0);
    }
    sock.write_all(format!("{:x}\r\n", buf.len()).as_bytes())
        .await?;
    sock.write_all(buf).await?;
    sock.write_all(b"\r\n").await?;
    let len = buf.len() as u64;
    buf.clear();
    Ok(len)
}

fn frame_request(cmd: &str, query: &HashMap<String, Vec<u8>>, body: &[u8]) -> Result<Request> {
    let mut args = HashMap::new();
    for (key, value) in query {
        if key != "cmd" {
            args.insert(key.clone(), value.clone());
        }
    }
    let mut buf = Vec::new();
    buf.extend_from_slice(cmd.as_bytes());
    buf.push(b'\n');
    match cmd {
        "capabilities" | "hello" | "branchmap" | "heads" | "stream_out" | "clonebundles" => {}
        "clonebundles_manifest" => write_star(&mut buf, &args),
        "protocaps" => write_kv(&mut buf, "caps", require(&args, "caps")?),
        "get_cached_bundle_inline" => write_kv(&mut buf, "path", require(&args, "path")?),
        "getbundle" => write_star(&mut buf, &args),
        "branches" => write_kv(&mut buf, "nodes", arg_or_empty(&args, "nodes")),
        "changegroup" => write_kv(&mut buf, "roots", require(&args, "roots")?),
        "changegroupsubset" => {
            write_kv(&mut buf, "bases", require(&args, "bases")?);
            write_kv(&mut buf, "heads", require(&args, "heads")?);
        }
        "known" => {
            write_kv(&mut buf, "nodes", arg_or_empty(&args, "nodes"));
            args.remove("nodes");
            write_star(&mut buf, &args);
        }
        "batch" => {
            write_kv(&mut buf, "cmds", arg_or_empty(&args, "cmds"));
            args.remove("cmds");
            write_star(&mut buf, &args);
        }
        "listkeys" => write_kv(&mut buf, "namespace", require(&args, "namespace")?),
        "lookup" => write_kv(&mut buf, "key", require(&args, "key")?),
        "pushkey" => {
            for key in ["namespace", "key", "old", "new"] {
                write_kv(&mut buf, key, arg_or_empty(&args, key));
            }
        }
        "unbundle" => write_kv(&mut buf, "heads", require(&args, "heads")?),
        other => return Err(format!("unknown HTTP command {other}").into()),
    }
    let mut req = parse_framed(&buf)?;
    if let Request::Unbundle { bundle, .. } = &mut req {
        *bundle = Bytes::copy_from_slice(body);
    }
    Ok(req)
}

fn arg_or_empty<'a>(args: &'a HashMap<String, Vec<u8>>, key: &str) -> &'a [u8] {
    args.get(key).map(Vec::as_slice).unwrap_or(b"")
}

fn require<'a>(args: &'a HashMap<String, Vec<u8>>, key: &str) -> Result<&'a [u8]> {
    args.get(key)
        .map(Vec::as_slice)
        .ok_or_else(|| Error::from(format!("missing {key}")))
}

fn write_kv(buf: &mut Vec<u8>, key: &str, value: &[u8]) {
    buf.extend_from_slice(key.as_bytes());
    buf.push(b' ');
    buf.extend_from_slice(value.len().to_string().as_bytes());
    buf.push(b'\n');
    buf.extend_from_slice(value);
}

fn write_star(buf: &mut Vec<u8>, args: &HashMap<String, Vec<u8>>) {
    buf.extend_from_slice(b"* ");
    buf.extend_from_slice(args.len().to_string().as_bytes());
    buf.push(b'\n');
    for (key, value) in args {
        write_kv(buf, key, value);
    }
}

fn parse_framed(buf: &[u8]) -> Result<Request> {
    let mut bytes = BytesMut::from(buf);
    match hgproto::sshproto::request::parse(&mut bytes) {
        Ok(Some(req)) => Ok(req),
        Ok(None) => Err(Error::from(format!(
            "incomplete wire command: {}",
            frame_preview(buf)
        ))),
        Err(err) => Err(Error::from(format!(
            "wire command parse: {err}; frame {}",
            frame_preview(buf)
        ))),
    }
}

fn frame_preview(buf: &[u8]) -> String {
    let max = 300.min(buf.len());
    let mut text = String::from_utf8_lossy(&buf[..max]).into_owned();
    if buf.len() > max {
        text.push_str("...");
    }
    text
}

fn response_body(response: &Response) -> Result<Bytes> {
    match response {
        Response::Pushres { ret, output } => {
            let mut body = format!("{ret}\n").into_bytes();
            body.extend_from_slice(output);
            Ok(Bytes::from(body))
        }
        Response::Pusherr(message) => {
            let mut body = b"0\n".to_vec();
            body.extend_from_slice(message);
            body.push(b'\n');
            Ok(Bytes::from(body))
        }
        Response::Capabilities(caps) => {
            // HTTP only: getbundle can answer as media type 0.2 in a negotiated compression.
            let mut caps = caps.clone();
            caps.push("httpmediatype=0.1rx,0.1tx,0.2tx".to_string());
            caps.push(format!("compression={}", SERVER_ENGINES.join(",")));
            Ok(Bytes::from(caps.join(" ")))
        }
        Response::Getbundle(_)
        | Response::Changegroup(_)
        | Response::Changegroupsubset(_)
        | Response::Streamout(_)
        | Response::GetCachedBundleInline(_) => {
            Err("changegroups are streamed by the connection".into())
        }
        Response::Batch(_)
        | Response::Lookup(_)
        | Response::Between(_)
        | Response::Branches(_)
        | Response::Branchmap(_)
        | Response::Debugwireargs(_)
        | Response::Hello(_)
        | Response::Heads(_)
        | Response::Known(_)
        | Response::Pushkey(_)
        | Response::Listkeys(_)
        | Response::Clonebundles(_)
        | Response::Protocaps
        | Response::Unbundle(_) => Ok(hgproto::sshproto::response::encode_cmd(response)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundle1_result_includes_http_output() {
        let response = Response::Pushres {
            ret: 1,
            output: Bytes::from_static(b"added changes\n"),
        };
        assert_eq!(
            response_body(&response).unwrap(),
            b"1\nadded changes\n".as_slice(),
        );
        let response = Response::Pushres {
            ret: 0,
            output: Bytes::new(),
        };
        assert_eq!(response_body(&response).unwrap(), b"0\n".as_slice());
    }

    #[test]
    fn bundle_engine_follows_mercurial() {
        let hg7 = "0.1 0.2 comp=zstd,zlib,bz2,none partial-pull";
        assert_eq!(bundle_engine(hg7, false), Some("zstd"));
        assert_eq!(bundle_engine(hg7, true), Some("none"));
        // Server preference wins over the client's order.
        assert_eq!(bundle_engine("0.2 comp=zlib,zstd", false), Some("zstd"));
        assert_eq!(bundle_engine("0.2 comp=zlib,none", false), Some("zlib"));
        // 0.2 without `comp=` means zlib and none.
        assert_eq!(bundle_engine("0.1 0.2", false), Some("zlib"));
        // Nothing in common, or no 0.2: media type 0.1 with zlib.
        assert_eq!(bundle_engine("0.2 comp=bz2", false), None);
        assert_eq!(bundle_engine("0.1 partial-pull", false), None);
        assert_eq!(bundle_engine("", true), None);
    }

    #[test]
    fn compressors_round_trip() {
        use std::io::Read;
        let data: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        for engine in ["none", "zlib", "zstd"] {
            let mut compressor = Compressor::new(engine).expect("compressor");
            let mut out = Vec::new();
            for piece in data.chunks(7_000) {
                compressor.write(piece, &mut out).expect("write");
            }
            compressor.finish(&mut out).expect("finish");
            let mut got = Vec::new();
            match engine {
                "none" => got = out,
                "zlib" => {
                    flate2::read::ZlibDecoder::new(&out[..])
                        .read_to_end(&mut got)
                        .expect("zlib");
                }
                _ => got = zstd::decode_all(&out[..]).expect("zstd"),
            }
            assert!(got == data, "{engine} round trip differs");
        }
    }
}
