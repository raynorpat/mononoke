// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! State for a single source control Repo

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::mem;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_compression::CompressorType;
use bookmarks::{Bookmarks, ListBookmarks};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{future, stream, FutureExt, Stream, StreamExt, TryStreamExt};
use hgproto::{self, BundleStream, GetbundleArgs, HgCommandRes, HgCommands};
use mercurial::changeset::RevlogChangeset;
use mercurial_bundles::part_encode::PartEncodeBuilder;
use mercurial_bundles::{parts, Bundle2EncodeBuilder};
use mercurial_types::{percent_encode, Changeset, NodeHash, Parents, NULL_HASH};
use slog::{info, o, Logger};
use tokio_util::io::ReaderStream;

use crate::bundle::history_part;
use crate::errors::Result;
use crate::stream::{list_store, stream_part, stream_requirements, StoreFile};

pub struct Repo {
    path: String,
    hgrepo: mercurial::RevlogRepo,
    /// Held while a push writes and while a stream clone lists the store, so
    /// the listed sizes are one snapshot. Keeps the last listing for later
    /// stream clones until a push clears it.
    store_listing: tokio::sync::Mutex<Option<Arc<Vec<StoreFile>>>>,
    #[allow(dead_code)]
    logger: Logger,
}

fn wireprotocaps() -> Vec<String> {
    vec![
        "lookup".to_string(),
        "known".to_string(),
        "getbundle".to_string(),
        "unbundle".to_string(),
        "pushkey".to_string(),
        "branchmap".to_string(),
        "listkeys".to_string(),
    ]
}

fn bundle2caps(stream: bool) -> String {
    let mut caps = vec![
        ("HG20", Vec::<&str>::new()),
        ("listkeys", Vec::new()),
        ("changegroup", vec!["02"]),
    ];
    if stream {
        caps.push(("stream", vec!["v2"]));
    }

    let mut encodedcaps = Vec::new();
    for (key, value) in &caps {
        if value.is_empty() {
            encodedcaps.push((*key).to_string());
        } else {
            encodedcaps.push(format!("{}={}", key, value.join(",")));
        }
    }

    percent_encode(&encodedcaps.join("\n"))
}

fn advertised_caps(repo: &mercurial::RevlogRepo) -> Vec<String> {
    let mut caps = wireprotocaps();
    let streamreqs = stream_requirements(repo);
    if let Some(reqs) = &streamreqs {
        // Clients stream-clone by default; `hg clone --pull` still gets a changegroup.
        caps.push("stream-preferred".to_string());
        caps.push(format!("streamreqs={reqs}"));
    }
    caps.push(format!("bundle2={}", bundle2caps(streamreqs.is_some())));
    caps
}

impl Repo {
    pub fn new<P: AsRef<Path>>(parent_logger: &Logger, path: P) -> Result<Self> {
        let path = path.as_ref();

        Ok(Repo {
            path: format!("{:?}", path),
            hgrepo: mercurial::RevlogRepo::open(path)?,
            store_listing: tokio::sync::Mutex::new(None),
            logger: parent_logger.new(o!("repo" => format!("{:?}", path))),
        })
    }

    /// Load what the first requests would otherwise wait for: the changelog
    /// node map that discovery's `known` lookups use, and the stream clone
    /// store listing. Blocks; run it off the runtime thread.
    pub fn warm_up(&self) -> Result<()> {
        self.hgrepo.changelog_revlog().load_node_map()?;
        if stream_requirements(&self.hgrepo).is_some() {
            let mut listing = self.store_listing.blocking_lock();
            if listing.is_none() {
                *listing = Some(Arc::new(list_store(&self.hgrepo)?));
            }
        }
        Ok(())
    }

    pub fn heads_match(&self, heads: &[NodeHash]) -> Result<bool> {
        crate::push::heads_match(&self.hgrepo, heads)
    }
}

impl Debug for Repo {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Repo({})", self.path)
    }
}

pub struct RepoClient {
    repo: Arc<Repo>,
    logger: Logger,
}

impl RepoClient {
    pub fn new(repo: Arc<Repo>, parent_logger: &Logger) -> Self {
        RepoClient {
            repo,
            logger: parent_logger.new(o!()),
        }
    }

    #[allow(dead_code)]
    pub fn get_logger(&self) -> &Logger {
        &self.logger
    }
}

fn listkeys_requested(args: &GetbundleArgs, namespace: &[u8]) -> bool {
    args.listkeys.iter().any(|key| key.as_slice() == namespace)
}

fn empty_listkeys_part(namespace: &str) -> hgproto::Result<PartEncodeBuilder> {
    // A zero-length payload chunk ends the part. `listkey_part` on an empty
    // map emits that chunk and then another terminator, which the client reads
    // as the end of the bundle and drops every following part.
    let mut part = PartEncodeBuilder::mandatory("listkeys")?;
    part.add_mparam("namespace", namespace.to_owned())?;
    Ok(part)
}

async fn bookmark_pairs(repo: &Repo) -> hgproto::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let bookmarks = repo.hgrepo.bookmarks()?;
    let names: Vec<Vec<u8>> = bookmarks
        .keys()
        .map_err(|err| hgproto::Error::from(mercurial::Error::from(err)))
        .try_collect()
        .await?;
    let mut pairs = Vec::new();
    for name in names {
        if let Some((hash, _version)) = bookmarks
            .get(&name)
            .await
            .map_err(|err| hgproto::Error::from(mercurial::Error::from(err)))?
        {
            // AsciiString doesn't currently implement AsRef<[u8]>, so switch to Vec which does.
            let hash = hash.to_hex().as_str().as_bytes().to_vec();
            pairs.push((name, hash));
        }
    }
    Ok(pairs)
}

/// Bytes buffered between the bundle encoder and the connection writing them out.
const BUNDLE_PIPE: usize = 1 << 20;

async fn create_bundle(repo: &Arc<Repo>, args: GetbundleArgs) -> hgproto::Result<BundleStream> {
    let (reader, writer) = tokio::io::duplex(BUNDLE_PIPE);
    let mut bundle = Bundle2EncodeBuilder::new(writer);
    // TODO: select an appropriate compressor. zstd appears to hang -- need to figure out
    // whether it's a framing issue or something else.
    bundle.set_compressor_type(CompressorType::Uncompressed);

    if args.stream {
        bundle.add_part(stream_clone_part(repo).await?);
    } else if args.cg {
        let history = history_part(repo.hgrepo.clone(), args.common.clone())
            .map_err(|err| hgproto::Error::from(err.to_string()))?;
        bundle.add_part(history);
    }

    if listkeys_requested(&args, b"phases") {
        bundle.add_part(empty_listkeys_part("phases")?);
    }

    if listkeys_requested(&args, b"bookmarks") {
        let pairs = bookmark_pairs(repo).await?;
        if pairs.is_empty() {
            bundle.add_part(empty_listkeys_part("bookmarks")?);
        } else {
            let items = stream::iter(pairs.into_iter().map(Ok::<_, std::io::Error>));
            bundle.add_part(parts::listkey_part("bookmarks", items)?);
        }
    }

    // Drop the writer when encoding ends so the reader sees EOF.
    let encode = tokio::spawn(async move { bundle.build().await.map(drop) });
    let body = ReaderStream::new(reader).map_err(hgproto::Error::from);
    let done = stream::once(async move {
        match encode.await {
            Ok(Ok(())) => None,
            Ok(Err(err)) => Some(Err(hgproto::Error::from(err))),
            Err(err) => Some(Err(hgproto::Error::from(err.to_string()))),
        }
    })
    .filter_map(future::ready);
    Ok(BundleStream {
        body: body.chain(done).boxed(),
        prefer_uncompressed: args.stream,
    })
}

async fn stream_clone_part(repo: &Arc<Repo>) -> hgproto::Result<PartEncodeBuilder> {
    let requirements = stream_requirements(&repo.hgrepo).ok_or_else(|| {
        hgproto::Error::from(
            "stream clone requested, but this repo is not served as one".to_string(),
        )
    })?;
    let lister = Arc::clone(repo);
    let files = tokio::task::spawn_blocking(move || -> Result<Arc<Vec<StoreFile>>> {
        let mut listing = lister.store_listing.blocking_lock();
        if let Some(files) = listing.as_ref() {
            return Ok(Arc::clone(files));
        }
        let files = Arc::new(list_store(&lister.hgrepo)?);
        *listing = Some(Arc::clone(&files));
        Ok(files)
    })
    .await
    .map_err(|err| hgproto::Error::from(err.to_string()))?
    .map_err(|err| hgproto::Error::from(err.to_string()))?;
    stream_part(files, &requirements).map_err(|err| hgproto::Error::from(err.to_string()))
}

struct ParentStream {
    repo: Arc<Repo>,
    n: NodeHash,
    bottom: NodeHash,
    wait_cs: Option<BoxFuture<'static, std::result::Result<RevlogChangeset, mercurial::Error>>>,
}

impl ParentStream {
    fn new(repo: Arc<Repo>, top: NodeHash, bottom: NodeHash) -> Self {
        ParentStream {
            repo,
            n: top,
            bottom,
            wait_cs: None,
        }
    }
}

impl Stream for ParentStream {
    type Item = std::result::Result<NodeHash, hgproto::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.n == this.bottom || this.n == NULL_HASH {
            return Poll::Ready(None);
        }

        if this.wait_cs.is_none() {
            this.wait_cs = Some(this.repo.hgrepo.get_changeset_by_nodeid(&this.n));
        }
        let cs = match this.wait_cs.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Ready(Ok(cs)) => {
                this.wait_cs = None;
                cs
            }
            Poll::Ready(Err(err)) => {
                this.wait_cs = None;
                return Poll::Ready(Some(Err(err.into())));
            }
            Poll::Pending => return Poll::Pending,
        };

        let parent = match *cs.parents() {
            Parents::None => NULL_HASH,
            Parents::One(p) => p,
            Parents::Two(p, _) => p,
        };
        let prev_n = mem::replace(&mut this.n, parent);
        Poll::Ready(Some(Ok(prev_n)))
    }
}

impl HgCommands for RepoClient {
    // @wireprotocommand('branchmap')
    fn branchmap(&self) -> HgCommandRes<HashMap<String, HashSet<NodeHash>>> {
        info!(self.logger, "branchmap");
        let repo = Arc::clone(&self.repo);
        async move {
            let heads = repo
                .hgrepo
                .get_heads()
                .map_err(hgproto::Error::from)
                .try_collect::<HashSet<_>>()
                .await?;
            let mut map = HashMap::new();
            map.insert("default".to_string(), heads);
            Ok(map)
        }
        .boxed()
    }

    // @wireprotocommand('between', 'pairs')
    fn between(&self, pairs: Vec<(NodeHash, NodeHash)>) -> HgCommandRes<Vec<Vec<NodeHash>>> {
        info!(self.logger, "between pairs {:?}", pairs);

        let repo = Arc::clone(&self.repo);
        stream::iter(pairs.into_iter().map(Ok::<_, hgproto::Error>))
            .and_then(move |(top, bottom)| {
                let mut step = 1usize;
                ParentStream::new(Arc::clone(&repo), top, bottom)
                    .enumerate()
                    .filter(move |(i, _)| {
                        let take = *i == step;
                        if take {
                            step *= 2;
                        }
                        future::ready(take)
                    })
                    .map(|(_, node)| node)
                    .try_collect::<Vec<_>>()
            })
            .try_collect::<Vec<_>>()
            .boxed()
    }

    // @wireprotocommand('changegroup', 'roots')
    fn changegroup(&self, roots: Vec<NodeHash>) -> HgCommandRes<()> {
        // TODO: streaming something
        info!(self.logger, "changegroup roots {:?}", roots);
        future::ok(()).boxed()
    }

    // @wireprotocommand('heads')
    fn heads(&self) -> HgCommandRes<HashSet<NodeHash>> {
        let repo = Arc::clone(&self.repo);
        async move {
            let heads = repo
                .hgrepo
                .get_heads()
                .map_err(hgproto::Error::from)
                .try_collect::<Vec<_>>()
                .await?;
            Ok(heads.into_iter().collect())
        }
        .boxed()
    }

    // @wireprotocommand('listkeys', 'namespace')
    fn listkeys(&self, namespace: String) -> HgCommandRes<HashMap<Vec<u8>, Vec<u8>>> {
        info!(self.logger, "listkeys {}", namespace);
        let repo = Arc::clone(&self.repo);
        async move {
            match namespace.as_str() {
                "phases" => Ok(HashMap::new()),
                "bookmarks" => Ok(bookmark_pairs(&repo).await?.into_iter().collect()),
                _ => Ok(HashMap::new()),
            }
        }
        .boxed()
    }

    // @wireprotocommand('known', 'nodes *'), but the '*' is ignored
    fn known(&self, nodes: Vec<NodeHash>) -> HgCommandRes<Vec<bool>> {
        info!(self.logger, "known: {:?}", nodes);
        let repo = Arc::clone(&self.repo);
        async move {
            let checks: Vec<_> = nodes
                .iter()
                .map(|node| repo.hgrepo.changeset_exists(node))
                .collect();
            future::try_join_all(checks)
                .await
                .map_err(hgproto::Error::from)
        }
        .boxed()
    }

    // @wireprotocommand('getbundle', '*')
    fn getbundle(&self, args: GetbundleArgs) -> HgCommandRes<BundleStream> {
        info!(self.logger, "Getbundle: {:?}", args);
        let repo = Arc::clone(&self.repo);
        async move { create_bundle(&repo, args).await }.boxed()
    }

    // @wireprotocommand('capabilities')
    fn capabilities(&self) -> HgCommandRes<Vec<String>> {
        info!(self.logger, "capabilities");
        future::ok(advertised_caps(&self.repo.hgrepo)).boxed()
    }

    // @wireprotocommand('hello')
    fn hello(&self) -> HgCommandRes<HashMap<String, Vec<String>>> {
        info!(self.logger, "Hello -> capabilities");

        let mut res = HashMap::new();
        res.insert(
            "capabilities".to_string(),
            advertised_caps(&self.repo.hgrepo),
        );

        future::ok(res).boxed()
    }

    // @wireprotocommand('pushkey', 'namespace key old new')
    fn pushkey(
        &self,
        namespace: String,
        key: String,
        old: NodeHash,
        new: NodeHash,
    ) -> HgCommandRes<bool> {
        info!(self.logger, "pushkey {} {}", namespace, key);
        if namespace != "bookmarks" {
            return future::ok(false).boxed();
        }
        let repo = Arc::clone(&self.repo);
        let key = key.into_bytes();
        async move {
            crate::push::pushkey_bookmark(&repo.hgrepo, &key, old, new)
                .map_err(|err| hgproto::Error::from(err.to_string()))
        }
        .boxed()
    }

    // @wireprotocommand('unbundle', 'heads')
    fn unbundle(&self, heads: Vec<NodeHash>, bundle: Bytes) -> HgCommandRes<Bytes> {
        info!(self.logger, "unbundle heads {:?}", heads);
        let repo = Arc::clone(&self.repo);
        async move {
            let mut listing = repo.store_listing.lock().await;
            // Cleared before writing, so even a failed push lists the store again.
            *listing = None;
            let applied = crate::push::apply_repo(&repo.hgrepo, &heads, &bundle)
                .map_err(|err| hgproto::Error::from(err.to_string()))?;
            crate::push::reply_bundle(&applied)
                .await
                .map_err(|err| hgproto::Error::from(err.to_string()))
        }
        .boxed()
    }
}
