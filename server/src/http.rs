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
            if let Err(err) = connection(sock, repo, log.clone()).await {
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
            if let Err(err) = connection(sock, repo, log.clone()).await {
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

async fn connection<S>(mut sock: S, repo: Arc<Repo>, log: Logger) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let client = RepoClient::new(repo, &log);
    let service = HgService::new_with_logger(client, &log);
    let mut buf = Vec::new();
    loop {
        let incoming = match read_request(&mut sock, &mut buf).await? {
            Some(incoming) => incoming,
            None => return Ok(()),
        };
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
            Ok(Response::Getbundle(bundle)) => {
                // The 200 is already sent if the bundle fails, so the client sees a cut-off body.
                match write_hg_stream(&mut sock, bundle, close).await {
                    Ok(sent) => info!(log, "HTTP {} -> {} bytes streamed", cmd, sent),
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

async fn read_request<S>(sock: &mut S, buf: &mut Vec<u8>) -> Result<Option<Incoming>>
where
    S: AsyncRead + AsyncWrite + Unpin,
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
    parse_request(sock, buf, &header).await
}

async fn parse_request<S>(
    sock: &mut S,
    buf: &mut Vec<u8>,
    header: &[u8],
) -> Result<Option<Incoming>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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
    let version = parts.next().unwrap_or("HTTP/1.1");

    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }

    let length = match headers.get("content-length") {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| Error::from("bad content-length"))?,
        None => 0,
    };
    if length > MAX_BODY {
        return Err("HTTP body too large".into());
    }

    let expects_continue = headers
        .get("expect")
        .map(|value| value.eq_ignore_ascii_case("100-continue"))
        .unwrap_or(false);
    if expects_continue && buf.len() < length {
        sock.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").await?;
    }
    let body = if length == 0 {
        Vec::new()
    } else {
        read_exact(sock, buf, length).await?
    };

    let query_text = match target.split_once('?') {
        Some((_, query)) => query.split('#').next().unwrap_or("").to_string(),
        None => String::new(),
    };
    let close = connection_closes(version, headers.get("connection").map(String::as_str));
    Ok(Some(Incoming {
        method,
        target,
        query: parse_query(&query_text)?,
        body,
        close,
    }))
}

fn connection_closes(version: &str, connection: Option<&str>) -> bool {
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
    let connection = if close { "close" } else { "keep-alive" };
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: {connection}\r\n\r\n",
        body.len()
    );
    sock.write_all(header.as_bytes()).await?;
    sock.write_all(body).await?;
    Ok(())
}

/// Chunked `getbundle` body, zlib-compressed as the bundle is produced. Returns the bytes sent.
///
/// Mercurial's HTTP client zlib-decompresses `getbundle` responses
/// (`httppeer._callcompressable`) even when the media type is 0.1.
async fn write_hg_stream<S>(sock: &mut S, bundle: BundleStream, close: bool) -> Result<u64>
where
    S: AsyncWrite + Unpin,
{
    use std::io::Write;
    let connection = if close { "close" } else { "keep-alive" };
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/mercurial-0.1\r\nTransfer-Encoding: chunked\r\nConnection: {connection}\r\n\r\n"
    );
    sock.write_all(header.as_bytes()).await?;
    let mut enc = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    let mut body = bundle.0;
    let mut sent = 0;
    while let Some(bytes) = body.next().await {
        enc.write_all(&bytes?)?;
        sent += write_chunk(sock, enc.get_mut()).await?;
    }
    sent += write_chunk(sock, &mut enc.finish()?).await?;
    sock.write_all(b"0\r\n\r\n").await?;
    Ok(sent)
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
        "capabilities" | "hello" | "branchmap" | "heads" => {}
        "getbundle" => write_star(&mut buf, &args),
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
        Response::Capabilities(caps) => Ok(Bytes::from(caps.join(" "))),
        Response::Getbundle(_) => Err("getbundle is streamed by the connection".into()),
        Response::Lookup(node) => Ok(Bytes::from(format!("1 {node}\n"))),
        Response::Batch(_)
        | Response::Between(_)
        | Response::Branchmap(_)
        | Response::Debugwireargs(_)
        | Response::Hello(_)
        | Response::Heads(_)
        | Response::Known(_)
        | Response::Pushkey(_)
        | Response::Listkeys(_)
        | Response::Unbundle(_) => Ok(hgproto::sshproto::response::encode_cmd(response)),
        Response::Branches(_)
        | Response::Clonebundles(_)
        | Response::Changegroup
        | Response::Changegroupsubset
        | Response::Streamout => Err("HTTP response is not implemented for this command".into()),
    }
}
