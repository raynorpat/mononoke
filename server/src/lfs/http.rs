// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! The Git LFS batch API and basic transfers, following
//! hgext/lfs/wireprotolfsserver.py.

use std::io::Cursor;
use std::time::Instant;

use futures::{stream, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use slog::{error, info, Logger};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::errors::Result;
use crate::http::{connection_closes, read_body, write_continue, write_status, Head};
use crate::lfs::pointer::Oid;
use crate::lfs::store::{Blob, LfsStore, PutOutcome};
use crate::lfs::LfsServing;

const BATCH_MEDIA_TYPE: &str = "application/vnd.git-lfs+json";
const TEXT: &str = "text/plain; charset=utf-8";
const STORAGE_ERROR: &str = "LFS storage error";
/// Each object costs at least one store call.
const MAX_BATCH_OBJECTS: usize = 1000;
const MAX_CONCURRENT_LOOKUPS: usize = 16;

#[derive(Debug, PartialEq, Eq)]
pub enum LfsRoute {
    Batch { prefix: String },
    Object { prefix: String, oid: String },
}

pub fn route(target: &str) -> Option<LfsRoute> {
    let path = target.split(['?', '#']).next().unwrap_or(target);
    if let Some(prefix) = path.strip_suffix("/.git/info/lfs/objects/batch") {
        return Some(LfsRoute::Batch {
            prefix: prefix.to_owned(),
        });
    }
    let (prefix, oid) = path.rsplit_once("/.hg/lfs/objects/")?;
    Some(LfsRoute::Object {
        prefix: prefix.to_owned(),
        oid: oid.to_owned(),
    })
}

pub fn base_url(serving: &LfsServing, scheme: &str, host: Option<&str>, prefix: &str) -> String {
    match &serving.public_url {
        Some(url) => url.trim_end_matches('/').to_owned(),
        None => format!("{scheme}://{}{prefix}", host.unwrap_or("localhost")),
    }
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        406 => "Not Acceptable",
        411 => "Length Required",
        415 => "Unsupported Media Type",
        422 => "corrupt blob",
        _ => "Internal Server Error",
    }
}

async fn reply<S>(sock: &mut S, status: u16, body: &str, close: bool) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    write_status(sock, status, reason(status), TEXT, body.as_bytes(), close).await
}

/// The request body's length, 0 when it has none. `None` when the framing is
/// unclear, because guessing wrong would parse body bytes as the next request.
fn body_length(head: &Head) -> Option<u64> {
    if head.headers.contains_key("transfer-encoding") {
        return None;
    }
    match head.headers.get("content-length") {
        // `parse` alone would take a leading `+`.
        Some(value) if value.bytes().all(|byte| byte.is_ascii_digit()) => value.parse().ok(),
        Some(_) => None,
        None => Some(0),
    }
}

/// Answers one LFS request. Returns whether to close the connection, which
/// every answer that leaves any of the request's body unread must do.
pub(crate) async fn serve<S>(
    sock: &mut S,
    buf: &mut Vec<u8>,
    head: &Head,
    route: LfsRoute,
    serving: Option<&LfsServing>,
    scheme: &str,
    log: &Logger,
) -> Result<bool>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let Some(length) = body_length(head) else {
        reply(sock, 400, "unclear request body length", true).await?;
        return Ok(true);
    };
    let connection = head.headers.get("connection").map(String::as_str);
    let close = connection_closes(&head.version, connection);
    let unread = close || length > 0;
    let Some(serving) = serving else {
        reply(sock, 404, "", unread).await?;
        return Ok(unread);
    };
    match route {
        LfsRoute::Batch { prefix } => {
            if let Some(status) = refuse_batch(head) {
                reply(sock, status, "", unread).await?;
                return Ok(unread);
            }
            let length = usize::try_from(length).unwrap_or(usize::MAX);
            let body = read_body(sock, buf, &head.headers, length).await?;
            let host = head.headers.get("host").map(String::as_str);
            let base = base_url(serving, scheme, host, &prefix);
            let auth = head.headers.get("authorization").map(String::as_str);
            match batch(&body, &base, auth, &serving.store, log).await {
                Ok(json) => write_status(sock, 200, "OK", BATCH_MEDIA_TYPE, &json, close).await?,
                Err(message) => reply(sock, 400, &message, close).await?,
            }
            Ok(close)
        }
        LfsRoute::Object { oid, .. } => {
            let method = head.method.as_str();
            if method != "GET" && method != "PUT" {
                let message = format!("Unsupported LFS transfer method: {method}");
                reply(sock, 405, &message, unread).await?;
                return Ok(unread);
            }
            let Ok(oid) = Oid::parse(&oid) else {
                reply(sock, 404, "", unread).await?;
                return Ok(unread);
            };
            let started = Instant::now();
            let store = &serving.store;
            let (status, size, closing) = if method == "GET" {
                get(sock, &oid, store, unread, log).await?
            } else if head.headers.contains_key("content-length") {
                put(sock, buf, head, &oid, length, store, close, log).await?
            } else {
                reply(sock, 411, "", unread).await?;
                (411, 0, unread)
            };
            let elapsed = started.elapsed();
            info!(
                log,
                "LFS {} {} {} bytes -> {} in {:?}", method, oid, size, status, elapsed
            );
            Ok(closing)
        }
    }
}

fn refuse_batch(head: &Head) -> Option<u16> {
    let header = |name| head.headers.get(name).map_or("", String::as_str);
    if head.method != "POST" {
        Some(405)
    } else if !header("accept").contains(BATCH_MEDIA_TYPE) {
        Some(406)
    } else if header("content-type") != BATCH_MEDIA_TYPE {
        Some(415)
    } else {
        None
    }
}

#[derive(Deserialize)]
struct BatchRequest {
    transfers: Option<Vec<String>>,
    #[serde(default)]
    operation: String,
    #[serde(default)]
    objects: Vec<BatchObject>,
}

#[derive(Deserialize)]
struct BatchObject {
    oid: String,
    #[serde(default)]
    size: u64,
}

/// The response for a batch request body, or why it is a `400`.
async fn batch(
    body: &[u8],
    base: &str,
    auth: Option<&str>,
    store: &LfsStore,
    log: &Logger,
) -> std::result::Result<Vec<u8>, String> {
    let request: BatchRequest =
        serde_json::from_slice(body).map_err(|err| format!("Invalid LFS batch request: {err}"))?;
    let transfers = request
        .transfers
        .unwrap_or_else(|| vec!["basic".to_owned()]);
    if !transfers.iter().any(|name| name == "basic") {
        return Err("Only the basic LFS transfer handler is supported".to_owned());
    }
    let operation = request.operation.as_str();
    if operation != "upload" && operation != "download" {
        return Err(format!("Unsupported LFS transfer operation: {operation}"));
    }
    let count = request.objects.len();
    if count > MAX_BATCH_OBJECTS {
        return Err(format!(
            "Too many LFS objects, the limit is {MAX_BATCH_OBJECTS}"
        ));
    }
    info!(log, "LFS batch {} {} objects", operation, count);

    // Collected first: a lazy map's closure makes the future fail the `Send` check.
    let answers: Vec<_> = request
        .objects
        .iter()
        .map(|object| batch_object(object, operation, base, auth, store, log))
        .collect();
    let objects: Vec<Value> = stream::iter(answers)
        .buffered(MAX_CONCURRENT_LOOKUPS)
        .collect()
        .await;
    let response = json!({"transfer": "basic", "objects": objects});
    Ok(response.to_string().into_bytes())
}

async fn batch_object(
    object: &BatchObject,
    operation: &str,
    base: &str,
    auth: Option<&str>,
    store: &LfsStore,
    log: &Logger,
) -> Value {
    let mut answer = json!({"oid": object.oid, "size": object.size, "authenticated": true});
    let Ok(oid) = Oid::parse(&object.oid) else {
        answer["error"] = json!({"code": 422, "message": "Invalid object id"});
        return answer;
    };
    let stored = match store.size(&oid).await {
        Ok(stored) => stored,
        Err(err) => {
            error!(log, "LFS size of {} failed: {}", oid, err);
            answer["error"] = json!({"code": 500, "message": STORAGE_ERROR});
            return answer;
        }
    };
    match (operation, stored) {
        // No actions: the client skips the upload. A wrong size is uploaded again.
        ("upload", Some(size)) if size == object.size => return answer,
        ("download", None) => {
            answer["error"] = json!({"code": 404, "message": "The object does not exist"});
            return answer;
        }
        _ => {}
    }
    let mut header = json!({"Accept": "application/vnd.git-lfs"});
    if let Some(auth) = auth.filter(|auth| auth.starts_with("Basic ")) {
        header["Authorization"] = json!(auth);
    }
    let href = format!("{base}/.hg/lfs/objects/{oid}");
    answer["actions"] = json!({operation: {"href": href, "header": header}});
    answer
}

/// Returns the status, the blob size and whether to close the connection.
async fn get<S>(
    sock: &mut S,
    oid: &Oid,
    store: &LfsStore,
    close: bool,
    log: &Logger,
) -> Result<(u16, u64, bool)>
where
    S: AsyncWrite + Unpin,
{
    match store.get(oid).await {
        Ok(Some(blob)) => {
            let size = blob.size;
            let complete = write_blob(sock, blob, close, log).await?;
            Ok((200, size, close || !complete))
        }
        Ok(None) => {
            reply(sock, 404, "", close).await?;
            Ok((404, 0, close))
        }
        Err(err) => {
            error!(log, "LFS get of {} failed: {}", oid, err);
            reply(sock, 500, STORAGE_ERROR, close).await?;
            Ok((500, 0, close))
        }
    }
}

/// Streams a blob. Once the `200` is out only closing can tell the client it
/// failed, so `false` means the body did not match its `Content-Length`.
async fn write_blob<S>(sock: &mut S, mut blob: Blob, close: bool, log: &Logger) -> Result<bool>
where
    S: AsyncWrite + Unpin,
{
    let connection = if close { "close" } else { "keep-alive" };
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: {connection}\r\n\r\n",
        blob.size
    );
    sock.write_all(header.as_bytes()).await?;
    let mut sent = 0;
    while let Some(chunk) = blob.body.next().await {
        match chunk {
            Ok(chunk) => {
                sock.write_all(&chunk).await?;
                sent += chunk.len() as u64;
            }
            Err(err) => {
                error!(log, "LFS read failed while streaming: {}", err);
                return Ok(false);
            }
        }
    }
    Ok(sent == blob.size)
}

/// Returns the status, the body size and whether to close the connection.
#[allow(clippy::too_many_arguments)]
async fn put<S>(
    sock: &mut S,
    buf: &mut Vec<u8>,
    head: &Head,
    oid: &Oid,
    length: u64,
    store: &LfsStore,
    close: bool,
    log: &Logger,
) -> Result<(u16, u64, bool)>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let buffered = length.min(buf.len() as u64);
    if buffered < length {
        write_continue(sock, &head.headers).await?;
    }
    let early: Vec<u8> = buf.drain(..buffered as usize).collect();
    // `put` reads to EOF, so the reader must end with this body, not the connection.
    let mut body = Cursor::new(early).chain((&mut *sock).take(length - buffered));
    let outcome = store.put(oid, length, &mut body).await;
    let (early, rest) = body.into_inner();
    let consumed = early.position() == buffered && rest.limit() == 0;

    let (status, message) = match outcome {
        Ok(PutOutcome::Created) => (201, ""),
        Ok(PutOutcome::Existed) => (200, ""),
        Ok(PutOutcome::Corrupt) => (422, ""),
        Err(err) => {
            error!(log, "LFS put of {} failed: {}", oid, err);
            (500, STORAGE_ERROR)
        }
    };
    let close = close || !consumed;
    reply(sock, status, message, close).await?;
    Ok((status, length, close))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::StreamExt;
    use serde_json::Value;
    use sha2::{Digest, Sha256};
    use slog::{o, Discard, Logger};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

    use super::*;
    use crate::errors::Error;
    use crate::http::connection;
    use crate::lfs::pointer::sha256_oid;
    use crate::lfs::store::{Blob, LfsStore, StoreSpec};
    use crate::lfs::LfsServing;
    use crate::repo::{Repo, RepoOptions};

    fn logger() -> Logger {
        Logger::root(Discard, o!())
    }

    fn temp_repo(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("lfs-http-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("store")).unwrap();
        std::fs::write(root.join("requires"), b"store\nrevlogv1\n").unwrap();
        root
    }

    fn serving(root: &Path, public_url: Option<&str>) -> Arc<Repo> {
        let store = LfsStore::open(&StoreSpec::Repo, None, &root.join("store"), &logger()).unwrap();
        let options = RepoOptions {
            lfs: Some(LfsServing {
                store: Arc::new(store),
                public_url: public_url.map(str::to_owned),
            }),
            ..RepoOptions::default()
        };
        Arc::new(Repo::new(&logger(), root, options).unwrap())
    }

    fn oid_of(bytes: &[u8]) -> String {
        sha256_oid(&Sha256::digest(bytes).into()).to_string()
    }

    struct Client {
        sock: DuplexStream,
        buf: Vec<u8>,
    }

    struct Reply {
        status: u16,
        headers: HashMap<String, String>,
        body: Vec<u8>,
    }

    impl Reply {
        fn json(&self) -> Value {
            serde_json::from_slice(&self.body).unwrap()
        }
    }

    fn start(repo: &Arc<Repo>) -> Client {
        let (sock, server) = tokio::io::duplex(1 << 20);
        let repo = Arc::clone(repo);
        tokio::spawn(async move {
            let _ = connection(server, repo, logger(), "http").await;
        });
        Client {
            sock,
            buf: Vec::new(),
        }
    }

    impl Client {
        async fn send(&mut self, bytes: &[u8]) {
            self.sock.write_all(bytes).await.unwrap();
        }

        async fn fill(&mut self) -> usize {
            let mut tmp = [0; 8192];
            let n = self.sock.read(&mut tmp).await.unwrap();
            self.buf.extend_from_slice(&tmp[..n]);
            n
        }

        /// The next response, or `None` once the server closed the connection.
        async fn reply(&mut self) -> Option<Reply> {
            let end = loop {
                if let Some(pos) = self.buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    break pos + 4;
                }
                if self.fill().await == 0 {
                    assert!(self.buf.is_empty(), "response cut off in its headers");
                    return None;
                }
            };
            let head: Vec<u8> = self.buf.drain(..end).collect();
            let head = String::from_utf8(head).unwrap();
            let mut lines = head.trim_end().split("\r\n");
            let status = lines.next().unwrap().split(' ').nth(1).unwrap();
            let headers: HashMap<String, String> = lines
                .map(|line| line.split_once(": ").unwrap())
                .map(|(name, value)| (name.to_ascii_lowercase(), value.to_owned()))
                .collect();
            let len = headers
                .get("content-length")
                .map_or(0, |v| v.parse().unwrap());
            while self.buf.len() < len {
                assert_ne!(self.fill().await, 0, "response cut off in its body");
            }
            let body = self.buf.drain(..len).collect();
            Some(Reply {
                status: status.parse().unwrap(),
                headers,
                body,
            })
        }

        async fn exchange(&mut self, request: &[u8]) -> Reply {
            self.send(request).await;
            self.reply().await.expect("a response")
        }
    }

    fn batch_request(json: &str, extra: &str) -> Vec<u8> {
        format!(
            "POST /.git/info/lfs/objects/batch HTTP/1.1\r\nHost: h:1\r\n\
             Accept: application/vnd.git-lfs+json\r\nContent-Type: application/vnd.git-lfs+json\r\n\
             {extra}Content-Length: {}\r\n\r\n{json}",
            json.len()
        )
        .into_bytes()
    }

    fn batch_json(operation: &str, oid: &str, size: usize) -> String {
        format!(r#"{{"operation":"{operation}","objects":[{{"oid":"{oid}","size":{size}}}]}}"#)
    }

    fn put_request(oid: &str, body: &[u8]) -> Vec<u8> {
        let mut request = format!(
            "PUT /.hg/lfs/objects/{oid} HTTP/1.1\r\nHost: h:1\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        request.extend_from_slice(body);
        request
    }

    fn get_request(oid: &str) -> Vec<u8> {
        format!("GET /.hg/lfs/objects/{oid} HTTP/1.1\r\nHost: h:1\r\n\r\n").into_bytes()
    }

    #[test]
    fn route_parses_lfs_paths() {
        let batch = |prefix: &str| LfsRoute::Batch {
            prefix: prefix.to_owned(),
        };
        assert_eq!(route("/.git/info/lfs/objects/batch"), Some(batch("")));
        assert_eq!(
            route("/a/b/.git/info/lfs/objects/batch?x=1"),
            Some(batch("/a/b"))
        );
        let oid = "a".repeat(64);
        assert_eq!(
            route(&format!("/.hg/lfs/objects/{oid}")),
            Some(LfsRoute::Object {
                prefix: String::new(),
                oid,
            })
        );
        assert_eq!(route("/?cmd=capabilities"), None);
        assert_eq!(route("/?x=/.git/info/lfs/objects/batch"), None);
    }

    #[tokio::test]
    async fn batch_upload_then_download() {
        let root = temp_repo("round-trip");
        let repo = serving(&root, None);
        let mut client = start(&repo);
        let bytes = b"some large file";
        let oid = oid_of(bytes);

        let json = batch_json("upload", &oid, bytes.len());
        let reply = client.exchange(&batch_request(&json, "")).await;
        assert_eq!(reply.status, 200);
        assert_eq!(
            reply.headers["content-type"],
            "application/vnd.git-lfs+json"
        );
        let body = reply.json();
        assert_eq!(body["transfer"], "basic");
        let object = &body["objects"][0];
        assert_eq!(object["oid"], oid.as_str());
        assert_eq!(object["size"], bytes.len());
        assert_eq!(object["authenticated"], true);
        let upload = &object["actions"]["upload"];
        assert_eq!(
            upload["href"],
            format!("http://h:1/.hg/lfs/objects/{oid}").as_str()
        );
        assert_eq!(upload["header"]["Accept"], "application/vnd.git-lfs");
        assert!(upload["header"].get("Authorization").is_none());

        let reply = client.exchange(&put_request(&oid, bytes)).await;
        assert_eq!(reply.status, 201);
        let reply = client.exchange(&put_request(&oid, bytes)).await;
        assert_eq!(reply.status, 200);

        // Already stored, so the client has nothing to send.
        let reply = client.exchange(&batch_request(&json, "")).await;
        assert!(reply.json()["objects"][0].get("actions").is_none());

        let json = batch_json("download", &oid, bytes.len());
        let reply = client.exchange(&batch_request(&json, "")).await;
        let download = &reply.json()["objects"][0]["actions"]["download"];
        assert_eq!(
            download["href"],
            format!("http://h:1/.hg/lfs/objects/{oid}").as_str()
        );

        let reply = client.exchange(&get_request(&oid)).await;
        assert_eq!(reply.status, 200);
        assert_eq!(reply.headers["content-type"], "application/octet-stream");
        assert_eq!(reply.body, bytes);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn batch_download_of_a_missing_object_is_a_per_object_404() {
        let root = temp_repo("missing");
        let repo = serving(&root, None);
        let mut client = start(&repo);
        let oid = "a".repeat(64);
        let json = batch_json("download", &oid, 1);
        let reply = client.exchange(&batch_request(&json, "")).await;
        assert_eq!(reply.status, 200);
        let body = reply.json();
        let object = &body["objects"][0];
        assert_eq!(object["error"]["code"], 404);
        assert_eq!(object["error"]["message"], "The object does not exist");
        assert!(object.get("actions").is_none());
        assert_eq!(client.exchange(&get_request(&oid)).await.status, 404);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn batch_uses_public_url_and_copies_authorization() {
        let root = temp_repo("public-url");
        let repo = serving(&root, Some("https://proxy/hg/"));
        let mut client = start(&repo);
        let oid = "a".repeat(64);
        let json = batch_json("upload", &oid, 1);
        let request = batch_request(&json, "Authorization: Basic x\r\n");
        let body = client.exchange(&request).await.json();
        let upload = &body["objects"][0]["actions"]["upload"];
        let href = upload["href"].as_str().unwrap();
        assert!(
            href.starts_with("https://proxy/hg/.hg/lfs/objects/"),
            "{href}"
        );
        assert_eq!(upload["header"]["Authorization"], "Basic x");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn batch_rejects_what_hg_rejects() {
        let root = temp_repo("batch-reject");
        let repo = serving(&root, None);
        let oid = "a".repeat(64);
        let good = batch_json("upload", &oid, 1);
        let good = String::from_utf8(batch_request(&good, "")).unwrap();

        let get = "GET /.git/info/lfs/objects/batch HTTP/1.1\r\n\r\n".to_owned();
        let accept = good.replace("Accept: application/vnd.git-lfs+json", "Accept: text/html");
        let media = good.replace("Content-Type: application/vnd", "Content-Type: text/vnd");
        for (request, status) in [(get, 405), (accept, 406), (media, 415)] {
            let reply = start(&repo).exchange(request.as_bytes()).await;
            assert_eq!(reply.status, status, "{request}");
        }

        for (json, message) in [
            (
                r#"{"operation":"upload","transfers":["tus"],"objects":[]}"#,
                "Only the basic LFS transfer handler is supported",
            ),
            (
                r#"{"operation":"be\r\nbop","objects":[]}"#,
                "Unsupported LFS transfer operation: be\r\nbop",
            ),
        ] {
            let reply = start(&repo).exchange(&batch_request(json, "")).await;
            assert_eq!(reply.status, 400, "{json}");
            assert_eq!(std::str::from_utf8(&reply.body).unwrap(), message);
        }

        let reply = start(&repo).exchange(&batch_request("not json", "")).await;
        assert_eq!(reply.status, 400);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn batch_caps_the_object_count() {
        let root = temp_repo("batch-cap");
        let repo = serving(&root, None);
        let object = format!(r#"{{"oid":"{}","size":1}}"#, "a".repeat(64));
        let json = |n: usize| {
            let objects = vec![object.as_str(); n].join(",");
            format!(r#"{{"operation":"download","objects":[{objects}]}}"#)
        };
        let reply = start(&repo).exchange(&batch_request(&json(1000), "")).await;
        assert_eq!(reply.status, 200);
        assert_eq!(reply.json()["objects"].as_array().unwrap().len(), 1000);
        let reply = start(&repo).exchange(&batch_request(&json(1001), "")).await;
        assert_eq!(reply.status, 400);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn batch_flags_an_invalid_oid_on_that_object() {
        let root = temp_repo("batch-oid");
        let repo = serving(&root, None);
        let good = "a".repeat(64);
        let json = format!(
            r#"{{"operation":"upload","objects":[{{"oid":"../x","size":1}},{{"oid":"{good}","size":1}}]}}"#
        );
        let reply = start(&repo).exchange(&batch_request(&json, "")).await;
        assert_eq!(reply.status, 200);
        let body = reply.json();
        assert_eq!(body["objects"][0]["error"]["code"], 422);
        assert!(body["objects"][1].get("actions").is_some());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn put_wrong_bytes_is_422() {
        let root = temp_repo("corrupt");
        let repo = serving(&root, None);
        let mut client = start(&repo);
        let oid = oid_of(b"right");
        let reply = client.exchange(&put_request(&oid, b"wrong")).await;
        assert_eq!(reply.status, 422);
        assert_eq!(client.exchange(&get_request(&oid)).await.status, 404);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn put_then_keepalive_request_on_same_connection() {
        let root = temp_repo("keepalive");
        let repo = serving(&root, None);
        let mut client = start(&repo);
        let oid = oid_of(b"bytes");
        let mut request = put_request(&oid, b"bytes");
        request.extend_from_slice(b"GET /?cmd=capabilities HTTP/1.1\r\nHost: h:1\r\n\r\n");
        assert_eq!(client.exchange(&request).await.status, 201);
        let reply = client.reply().await.expect("capabilities");
        assert_eq!(reply.status, 200);
        assert!(reply.body.windows(3).any(|w| w == b"lfs"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn put_waits_with_100_continue_and_streams_later_writes() {
        let root = temp_repo("continue");
        let repo = serving(&root, None);
        let mut client = start(&repo);
        let oid = oid_of(b"bytes");
        let head = format!(
            "PUT /.hg/lfs/objects/{oid} HTTP/1.1\r\nExpect: 100-continue\r\nContent-Length: 5\r\n\r\n"
        );
        assert_eq!(client.exchange(head.as_bytes()).await.status, 100);
        client.send(b"by").await;
        assert_eq!(client.exchange(b"tes").await.status, 201);
        assert_eq!(client.exchange(&get_request(&oid)).await.body, b"bytes");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn put_short_body_is_corrupt_and_closes() {
        let root = temp_repo("short");
        let repo = serving(&root, None);
        let oid = oid_of(b"bytes");
        // Both store paths: a new blob, then one already present.
        for present in [false, true] {
            let mut client = start(&repo);
            let request = put_request(&oid, b"bytes");
            client.send(&request[..request.len() - 2]).await;
            client.sock.shutdown().await.unwrap();
            assert_eq!(client.reply().await.expect("a response").status, 422);
            assert!(client.reply().await.is_none());
            if !present {
                let mut client = start(&repo);
                assert_eq!(client.exchange(&get_request(&oid)).await.status, 404);
                let reply = client.exchange(&put_request(&oid, b"bytes")).await;
                assert_eq!(reply.status, 201);
            }
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn put_store_error_is_500_and_closes_without_parsing_the_body() {
        let root = temp_repo("store-error");
        let repo = serving(&root, None);
        // A file where the blob directory should be makes every upload fail.
        let objects = root.join("store/lfs/objects");
        std::fs::remove_dir_all(&objects).unwrap();
        std::fs::write(&objects, b"").unwrap();

        let body = b"GET /?cmd=capabilities HTTP/1.1\r\n\r\n";
        let mut client = start(&repo);
        let reply = client.exchange(&put_request(&oid_of(body), body)).await;
        assert_eq!(reply.status, 500);
        assert_eq!(reply.body, b"LFS storage error");
        assert_eq!(reply.headers["connection"], "close");
        assert!(client.reply().await.is_none());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn rejected_put_does_not_parse_its_body() {
        let root = temp_repo("rejected");
        let repo = serving(&root, None);
        let mut client = start(&repo);
        let reply = client
            .exchange(&put_request(
                "nope",
                b"GET /?cmd=capabilities HTTP/1.1\r\n\r\n",
            ))
            .await;
        assert_eq!(reply.status, 404);
        assert!(client.reply().await.is_none());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn put_rejects_unclear_body_framing() {
        let root = temp_repo("framing");
        let repo = serving(&root, None);
        let oid = oid_of(b"bytes");
        let head = format!("PUT /.hg/lfs/objects/{oid} HTTP/1.1\r\n");

        for bad in [
            "Content-Length: +5\r\n",
            "Content-Length: 5, 5\r\n",
            "Content-Length: 0x5\r\n",
            "Content-Length: 99999999999999999999999\r\n",
            "Content-Length: 5\r\nContent-Length: 6\r\n",
            "Content-Length: 5\r\nTransfer-Encoding: chunked\r\n",
            "Transfer-Encoding: chunked\r\n",
        ] {
            let request = format!("{head}{bad}\r\nbytes");
            let mut client = start(&repo);
            let reply = client.exchange(request.as_bytes()).await;
            assert_eq!(reply.status, 400, "{bad}");
            assert!(client.reply().await.is_none(), "{bad}");
        }

        let reply = start(&repo)
            .exchange(format!("{head}\r\n").as_bytes())
            .await;
        assert_eq!(reply.status, 411);

        let request = format!("{head}Content-Length: 5\r\nContent-Length: 5\r\n\r\nbytes");
        assert_eq!(start(&repo).exchange(request.as_bytes()).await.status, 201);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn lfs_paths_404_when_not_serving() {
        let root = temp_repo("not-serving");
        let repo = Arc::new(Repo::new(&logger(), &root, RepoOptions::default()).unwrap());
        let json = batch_json("upload", &"a".repeat(64), 1);
        let mut client = start(&repo);
        assert_eq!(client.exchange(&batch_request(&json, "")).await.status, 404);
        // The unread body must not become the next request.
        assert!(client.reply().await.is_none());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn blob_that_fails_or_runs_short_is_not_complete() {
        let failing: Vec<crate::errors::Result<Bytes>> = vec![
            Ok(Bytes::from_static(b"ab")),
            Err(Error::from("read failed")),
        ];
        let short: Vec<crate::errors::Result<Bytes>> = vec![Ok(Bytes::from_static(b"ab"))];
        for chunks in [failing, short] {
            let blob = Blob {
                size: 10,
                body: futures::stream::iter(chunks).boxed(),
            };
            let (mut client, mut server) = tokio::io::duplex(1024);
            assert!(!write_blob(&mut server, blob, false, &logger())
                .await
                .unwrap());
            drop(server);
            let mut sent = Vec::new();
            client.read_to_end(&mut sent).await.unwrap();
            assert!(sent.starts_with(b"HTTP/1.1 200 OK\r\n"));
            assert!(sent.ends_with(b"\r\n\r\nab"));
        }
    }

    #[test]
    fn base_url_prefers_public_url() {
        let root = temp_repo("base-url");
        let store = LfsStore::open(&StoreSpec::Repo, None, &root.join("store"), &logger()).unwrap();
        let mut lfs = LfsServing {
            store: Arc::new(store),
            public_url: None,
        };
        assert_eq!(base_url(&lfs, "https", Some("h:1"), "/a"), "https://h:1/a");
        lfs.public_url = Some("https://proxy/hg/".to_owned());
        assert_eq!(
            base_url(&lfs, "https", Some("h:1"), "/a"),
            "https://proxy/hg"
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
