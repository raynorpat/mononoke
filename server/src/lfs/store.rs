// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! LFS blob storage on the repo's disk, an S3-compatible bucket or Azure Blob.

use std::collections::HashSet;
use std::path::Path as FsPath;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{GetResult, ObjectStore, WriteMultipart};
use sha2::{Digest, Sha256};
use slog::{error, warn, Logger};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::errors::{Error, Result};
use crate::lfs::pointer::{sha256_oid, Oid};

const PART_SIZE: usize = 8 * 1024 * 1024;
const MAX_PARTS_IN_FLIGHT: usize = 4;
const READ_SIZE: usize = 64 * 1024;
const BAD_SPEC: &str =
    "invalid --lfs-store: expected repo, s3://bucket[/prefix] or azure://account/container[/prefix]";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreSpec {
    Repo,
    S3 {
        bucket: String,
        prefix: String,
    },
    Azure {
        account: String,
        container: String,
        prefix: String,
    },
}

impl FromStr for StoreSpec {
    type Err = Error;

    fn from_str(s: &str) -> Result<StoreSpec> {
        let name = |part: Option<&str>| {
            part.filter(|p| !p.is_empty())
                .map(str::to_owned)
                .ok_or(BAD_SPEC)
        };
        let prefix = |part: Option<&str>| part.unwrap_or("").trim_matches('/').to_owned();

        if s == "repo" {
            Ok(StoreSpec::Repo)
        } else if let Some(rest) = s.strip_prefix("s3://") {
            let mut parts = rest.splitn(2, '/');
            Ok(StoreSpec::S3 {
                bucket: name(parts.next())?,
                prefix: prefix(parts.next()),
            })
        } else if let Some(rest) = s.strip_prefix("azure://") {
            let mut parts = rest.splitn(3, '/');
            Ok(StoreSpec::Azure {
                account: name(parts.next())?,
                container: name(parts.next())?,
                prefix: prefix(parts.next()),
            })
        } else {
            Err(BAD_SPEC.into())
        }
    }
}

pub struct Blob {
    pub size: u64,
    pub body: BoxStream<'static, Result<Bytes>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutOutcome {
    Created,
    Existed,
    Corrupt,
}

pub struct LfsStore {
    primary: Arc<dyn ObjectStore>,
    /// The repo store, read-only, for blobs `hg` wrote on the server.
    fallback: Option<Arc<dyn ObjectStore>>,
    prefix: String,
    warned: Mutex<HashSet<Oid>>,
    logger: Logger,
}

/// object_store errors can name the bucket and endpoint and quote the backend's
/// response, and returned errors can reach clients, so the detail stays in the log.
fn scrub(logger: &Logger, op: &str, err: object_store::Error) -> Error {
    error!(logger, "lfs store {} failed: {}", op, err);
    let kind = match err {
        object_store::Error::NotFound { .. } => "not found",
        object_store::Error::PermissionDenied { .. }
        | object_store::Error::Unauthenticated { .. } => "access denied",
        _ => "backend error",
    };
    format!("{} failed: {}", op, kind).into()
}

impl LfsStore {
    /// `logger` receives full backend errors, so it must not be a client-facing one.
    pub fn open(
        spec: &StoreSpec,
        s3_endpoint: Option<&str>,
        store_dir: &FsPath,
        logger: &Logger,
    ) -> Result<LfsStore> {
        if s3_endpoint.is_some() && !matches!(spec, StoreSpec::S3 { .. }) {
            return Err("--lfs-endpoint needs an s3:// --lfs-store".into());
        }
        let opened = |err| scrub(logger, "open", err);

        let repo_dir = store_dir.join("lfs").join("objects");
        std::fs::create_dir_all(&repo_dir)?;
        let repo: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&repo_dir).map_err(opened)?);

        let (primary, fallback, prefix): (Arc<dyn ObjectStore>, _, _) = match spec {
            StoreSpec::Repo => (repo, None, ""),
            StoreSpec::S3 { bucket, prefix } => {
                let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
                if let Some(endpoint) = s3_endpoint {
                    builder = builder
                        .with_endpoint(endpoint)
                        .with_virtual_hosted_style_request(false);
                }
                (
                    Arc::new(builder.build().map_err(opened)?),
                    Some(repo),
                    &prefix[..],
                )
            }
            StoreSpec::Azure {
                account,
                container,
                prefix,
            } => {
                let store = MicrosoftAzureBuilder::from_env()
                    .with_account(account)
                    .with_container_name(container)
                    .build()
                    .map_err(opened)?;
                (Arc::new(store), Some(repo), &prefix[..])
            }
        };

        Ok(LfsStore {
            primary,
            fallback,
            prefix: prefix.to_owned(),
            warned: Mutex::new(HashSet::new()),
            logger: logger.clone(),
        })
    }

    #[cfg(test)]
    fn with_stores(primary: Arc<dyn ObjectStore>, fallback: Option<Arc<dyn ObjectStore>>) -> Self {
        LfsStore {
            primary,
            fallback,
            prefix: String::new(),
            warned: Mutex::new(HashSet::new()),
            logger: Logger::root(slog::Discard, slog::o!()),
        }
    }

    /// `Path::from` drops the empty segment of an empty prefix.
    fn key(&self, oid: &Oid) -> Path {
        Path::from(format!("{}/{}", self.prefix, oid.store_path()))
    }

    /// `NotFound` is an answer, anything else is a failure.
    fn found<T>(&self, op: &str, result: object_store::Result<T>) -> Result<Option<T>> {
        match result {
            Ok(value) => Ok(Some(value)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(scrub(&self.logger, op, err)),
        }
    }

    fn blob(&self, result: GetResult) -> Blob {
        let logger = self.logger.clone();
        Blob {
            size: result.meta.size,
            body: result
                .into_stream()
                .map(move |chunk| chunk.map_err(|err| scrub(&logger, "read", err)))
                .boxed(),
        }
    }

    pub async fn check(&self) -> Result<()> {
        let prefix = Path::from(self.prefix.as_str());
        let listing = self.primary.list_with_delimiter(Some(&prefix)).await;
        listing.map_err(|err| scrub(&self.logger, "list", err))?;
        Ok(())
    }

    pub async fn size(&self, oid: &Oid) -> Result<Option<u64>> {
        let found = self.found("head", self.primary.head(&self.key(oid)).await)?;
        if let Some(meta) = found {
            return Ok(Some(meta.size));
        }
        let Some(fallback) = &self.fallback else {
            return Ok(None);
        };
        let found = fallback.head(&Path::from(oid.store_path())).await;
        Ok(self.found("head", found)?.map(|meta| meta.size))
    }

    pub async fn get(&self, oid: &Oid) -> Result<Option<Blob>> {
        let found = self.found("get", self.primary.get(&self.key(oid)).await)?;
        if let Some(result) = found {
            return Ok(Some(self.blob(result)));
        }
        let Some(fallback) = &self.fallback else {
            return Ok(None);
        };
        let found = fallback.get(&Path::from(oid.store_path())).await;
        let Some(result) = self.found("get", found)? else {
            return Ok(None);
        };
        if self.warned.lock().unwrap().insert(oid.clone()) {
            warn!(self.logger, "lfs blob served from the repo store: {}", oid);
        }
        Ok(Some(self.blob(result)))
    }

    async fn abort(&self, upload: WriteMultipart) {
        if let Err(err) = upload.abort().await {
            scrub(&self.logger, "abort", err);
        }
    }

    /// Streams `body` into the primary store. The final key never holds unverified
    /// bytes: a multipart upload is invisible until finished on all three backends.
    pub async fn put<R: AsyncRead + Unpin + Send>(
        &self,
        oid: &Oid,
        size: u64,
        mut body: R,
    ) -> Result<PutOutcome> {
        let key = self.key(oid);
        if self.found("head", self.primary.head(&key).await)?.is_some() {
            tokio::io::copy(&mut body, &mut tokio::io::sink()).await?;
            return Ok(PutOutcome::Existed);
        }

        let upload = self.primary.put_multipart(&key).await;
        let upload = upload.map_err(|err| scrub(&self.logger, "put", err))?;
        let mut upload = WriteMultipart::new_with_chunk_size(upload, PART_SIZE);
        let mut hasher = Sha256::new();
        let mut count = 0;
        let mut buf = vec![0; READ_SIZE];
        loop {
            let n = match body.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(err) => {
                    self.abort(upload).await;
                    return Err(err.into());
                }
            };
            count += n as u64;
            if count > size {
                self.abort(upload).await;
                return Ok(PutOutcome::Corrupt);
            }
            hasher.update(&buf[..n]);
            if let Err(err) = upload.wait_for_capacity(MAX_PARTS_IN_FLIGHT).await {
                self.abort(upload).await;
                return Err(scrub(&self.logger, "put", err));
            }
            upload.write(&buf[..n]);
        }

        if count != size || sha256_oid(&hasher.finalize().into()) != *oid {
            self.abort(upload).await;
            return Ok(PutOutcome::Corrupt);
        }
        let finished = upload.finish().await;
        finished.map_err(|err| scrub(&self.logger, "put", err))?;
        Ok(PutOutcome::Created)
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::path::PathBuf;
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::TryStreamExt;
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use sha2::{Digest, Sha256};
    use slog::{o, Discard, Logger};
    use tokio_util::io::StreamReader;

    use super::*;
    use crate::lfs::pointer::{sha256_oid, Oid};

    fn logger() -> Logger {
        Logger::root(Discard, o!())
    }

    fn temp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("lfs-store-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn oid_of(bytes: &[u8]) -> Oid {
        sha256_oid(&Sha256::digest(bytes).into())
    }

    fn local(dir: &PathBuf) -> Arc<dyn ObjectStore> {
        Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap())
    }

    async fn read_body(blob: Blob) -> Vec<u8> {
        let chunks: Vec<Bytes> = blob.body.try_collect().await.unwrap();
        chunks.concat()
    }

    #[tokio::test]
    async fn repo_store_round_trip_matches_hg_layout() {
        let dir = temp_dir("round-trip");
        let store = LfsStore::open(&StoreSpec::Repo, None, &dir, &logger()).unwrap();
        store.check().await.unwrap();
        let oid = oid_of(b"abc");

        let outcome = store.put(&oid, 3, &b"abc"[..]).await.unwrap();
        assert_eq!(outcome, PutOutcome::Created);
        let path = dir
            .join("lfs/objects/ba/7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
        assert_eq!(std::fs::read(path).unwrap(), b"abc");

        assert_eq!(store.size(&oid).await.unwrap(), Some(3));
        let blob = store.get(&oid).await.unwrap().unwrap();
        assert_eq!(blob.size, 3);
        assert_eq!(read_body(blob).await, b"abc");

        let outcome = store.put(&oid, 3, &b"abc"[..]).await.unwrap();
        assert_eq!(outcome, PutOutcome::Existed);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn upload_larger_than_a_part_round_trips() {
        let dir = temp_dir("large");
        let store = LfsStore::open(&StoreSpec::Repo, None, &dir, &logger()).unwrap();
        let data = vec![7u8; 20 * 1024 * 1024 + 1];
        let oid = oid_of(&data);

        let outcome = store.put(&oid, data.len() as u64, &data[..]).await.unwrap();
        assert_eq!(outcome, PutOutcome::Created);
        let blob = store.get(&oid).await.unwrap().unwrap();
        assert!(read_body(blob).await == data);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn corrupt_upload_leaves_no_blob() {
        let dir = temp_dir("corrupt");
        let store = LfsStore::open(&StoreSpec::Repo, None, &dir, &logger()).unwrap();
        let oid = oid_of(b"abc");

        // Wrong bytes, too many bytes, too few bytes.
        for body in [&b"abd"[..], b"abcd", b"ab"] {
            let outcome = store.put(&oid, 3, body).await.unwrap();
            assert_eq!(outcome, PutOutcome::Corrupt);
            assert_eq!(store.size(&oid).await.unwrap(), None);
            let path = dir.join("lfs/objects").join(oid.store_path());
            assert!(!path.exists());
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn truncated_upload_leaves_no_blob() {
        let dir = temp_dir("truncated");
        let store = LfsStore::open(&StoreSpec::Repo, None, &dir, &logger()).unwrap();
        let oid = oid_of(b"abcd");
        let body = StreamReader::new(futures::stream::iter(vec![
            Ok(Bytes::from_static(b"ab")),
            Err(io::Error::other("connection lost")),
        ]));

        assert!(store.put(&oid, 4, body).await.is_err());
        assert_eq!(store.size(&oid).await.unwrap(), None);

        let outcome = store.put(&oid, 4, &b"abcd"[..]).await.unwrap();
        assert_eq!(outcome, PutOutcome::Created);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn fallback_serves_repo_blobs() {
        let (a, b) = (temp_dir("fallback-a"), temp_dir("fallback-b"));
        let store = LfsStore::with_stores(local(&a), Some(local(&b)));

        let oid = oid_of(b"abc");
        let in_b = b.join(oid.store_path());
        std::fs::create_dir_all(in_b.parent().unwrap()).unwrap();
        std::fs::write(&in_b, b"abc").unwrap();

        assert_eq!(store.size(&oid).await.unwrap(), Some(3));
        let blob = store.get(&oid).await.unwrap().unwrap();
        assert_eq!(read_body(blob).await, b"abc");

        let new = oid_of(b"new");
        let outcome = store.put(&new, 3, &b"new"[..]).await.unwrap();
        assert_eq!(outcome, PutOutcome::Created);
        assert!(a.join(new.store_path()).exists());
        assert!(!b.join(new.store_path()).exists());

        std::fs::remove_dir_all(&a).unwrap();
        std::fs::remove_dir_all(&b).unwrap();
    }

    #[test]
    fn store_spec_parses() {
        let s3 = |bucket: &str, prefix: &str| StoreSpec::S3 {
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
        };
        let azure = |prefix: &str| StoreSpec::Azure {
            account: "acct".to_owned(),
            container: "cont".to_owned(),
            prefix: prefix.to_owned(),
        };
        assert_eq!("repo".parse::<StoreSpec>().unwrap(), StoreSpec::Repo);
        assert_eq!("s3://b".parse::<StoreSpec>().unwrap(), s3("b", ""));
        assert_eq!("s3://b/p/q".parse::<StoreSpec>().unwrap(), s3("b", "p/q"));
        assert_eq!("s3://b/p/".parse::<StoreSpec>().unwrap(), s3("b", "p"));
        assert_eq!("azure://acct/cont".parse::<StoreSpec>().unwrap(), azure(""));
        assert_eq!(
            "azure://acct/cont/p".parse::<StoreSpec>().unwrap(),
            azure("p")
        );
        for bad in ["s3://", "azure://acct", "azure://acct/", "ftp://x", ""] {
            assert!(bad.parse::<StoreSpec>().is_err(), "{bad}");
        }
    }

    #[test]
    fn endpoint_needs_an_s3_store() {
        let dir = temp_dir("endpoint");
        assert!(LfsStore::open(&StoreSpec::Repo, Some("http://x"), &dir, &logger()).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn store_errors_hide_backend_details() {
        let err = object_store::Error::Generic {
            store: "S3",
            source: "GET https://secret-bucket.example.com/key failed".into(),
        };
        let err = scrub(&logger(), "list", err);
        let text = format!("{err} {err:?}");
        assert!(!text.contains("secret-bucket"), "{text}");
        assert!(std::error::Error::source(&err).is_none());
    }
}
