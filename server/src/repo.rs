// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! State for a single source control Repo

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::fs;
use std::mem;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime};

use async_compression::CompressorType;
use bookmarks::{Bookmarks, ListBookmarks};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{future, stream, FutureExt, Stream, StreamExt, TryStreamExt};
use hgproto::{self, BundleStream, GetbundleArgs, HgCommandRes, HgCommands};
use mercurial::changeset::RevlogChangeset;
use mercurial::lock::RepoLock;
use mercurial::revlog::RevIdx;
use mercurial_bundles::changegroup::CgVersion;
use mercurial_bundles::part_encode::PartEncodeBuilder;
use mercurial_bundles::{parts, Bundle2EncodeBuilder};
use mercurial_types::{percent_decode, percent_encode, Changeset, NodeHash, Parents, NULL_HASH};
use slog::{info, o, warn, Logger};
use tokio_util::io::ReaderStream;

use crate::bundle::{history_part, legacy_changegroup, Selection};
use crate::errors::Result;
use crate::stream::{list_store, stream_part, stream_requirements, StoreFile};

pub struct Repo {
    path: String,
    /// Use `hgrepo()`, which reloads it after an outside change.
    revlogs: mercurial::RevlogRepo,
    /// The changelog as the server last saw it.
    changelog_seen: std::sync::Mutex<ChangelogStamp>,
    /// Held while a push writes and while a stream clone lists the store.
    /// Keeps the last listing, with the changelog it was made against, for
    /// later stream clones. A push clears it.
    store_listing: tokio::sync::Mutex<Option<(ChangelogStamp, Arc<Vec<StoreFile>>)>>,
    logger: Logger,
}

/// Size and modification time of `00changelog.i` and `00changelog.d`. Every
/// commit, push or strip changes one of them.
type ChangelogStamp = [Option<(u64, SystemTime)>; 2];

fn changelog_stamp(store: &Path) -> ChangelogStamp {
    ["00changelog.i", "00changelog.d"].map(|name| {
        let meta = fs::metadata(store.join(name)).ok()?;
        Some((meta.len(), meta.modified().ok()?))
    })
}

fn wireprotocaps() -> Vec<String> {
    vec![
        "lookup".to_string(),
        "known".to_string(),
        "getbundle".to_string(),
        "changegroupsubset".to_string(),
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
        ("changegroup", vec!["01", "02", "03"]),
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

/// Client changegroup versions, defaulting to 01 when no versions are listed.
fn client_cg_versions(bundlecaps: &[Vec<u8>]) -> Vec<CgVersion> {
    let Some(caps) = bundlecaps
        .iter()
        .find_map(|cap| cap.strip_prefix(b"bundle2="))
    else {
        return vec![CgVersion::V1];
    };
    let caps = percent_decode(&String::from_utf8_lossy(caps));
    let Some(versions) = caps
        .lines()
        .find_map(|line| line.strip_prefix("changegroup="))
        .filter(|versions| !versions.is_empty())
    else {
        return vec![CgVersion::V1];
    };
    versions
        .split(',')
        .filter_map(|value| CgVersion::from_param(value.as_bytes()))
        .collect()
}

/// The changegroup version `getbundle` sends: 03 when the client takes it,
/// else 02 or 01. A treemanifest repository can only be sent as 03.
fn send_cg_version(
    repo: &mercurial::RevlogRepo,
    args: &GetbundleArgs,
) -> hgproto::Result<CgVersion> {
    let client = client_cg_versions(&args.bundlecaps);
    if client.contains(&CgVersion::V3) {
        Ok(CgVersion::V3)
    } else if repo.is_treemanifest() {
        Err(hgproto::Error::from(
            "no common changegroup version".to_string(),
        ))
    } else if client.contains(&CgVersion::V2) {
        Ok(CgVersion::V2)
    } else if client.contains(&CgVersion::V1) {
        Ok(CgVersion::V1)
    } else {
        Err(hgproto::Error::from(
            "no common changegroup version".to_string(),
        ))
    }
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
        // Stamp before opening: a change in between only causes one extra reload.
        let changelog_seen = std::sync::Mutex::new(changelog_stamp(&path.join("store")));

        Ok(Repo {
            path: format!("{:?}", path),
            revlogs: mercurial::RevlogRepo::open(path)?,
            changelog_seen,
            store_listing: tokio::sync::Mutex::new(None),
            logger: parent_logger.new(o!("repo" => format!("{:?}", path))),
        })
    }

    /// Load what the first requests would otherwise wait for: the changelog
    /// node map that discovery's `known` lookups use, and the stream clone
    /// store listing. Blocks; run it off the runtime thread.
    pub fn warm_up(&self) -> Result<()> {
        self.hgrepo().changelog_revlog().load_node_map()?;
        if stream_requirements(self.hgrepo()).is_some() {
            self.stream_listing()?;
        }
        Ok(())
    }

    /// The repository, reloaded first if something other than this server
    /// changed its changelog since it last looked, such as `hg commit` or
    /// `hg strip`, so its cached revlogs are not stale.
    pub fn hgrepo(&self) -> &mercurial::RevlogRepo {
        let now = changelog_stamp(&self.revlogs.store_path());
        let mut seen = self.changelog_seen.lock().expect("changelog stamp lock");
        if *seen != now {
            match self.revlogs.reload() {
                Ok(()) => *seen = now,
                Err(err) => warn!(
                    self.logger,
                    "reload after an outside change failed: {}", err
                ),
            }
        }
        &self.revlogs
    }

    /// Record the changelog as this server's own push left it, so the push
    /// does not look like an outside change. Call with the write locks held.
    fn note_own_write(&self) {
        *self.changelog_seen.lock().expect("changelog stamp lock") =
            changelog_stamp(&self.revlogs.store_path());
    }

    /// The stream clone file listing. Reused while the changelog is as it was
    /// when the store was listed. Lists under the store lock so the sizes are
    /// one snapshot. Blocks.
    fn stream_listing(&self) -> Result<Arc<Vec<StoreFile>>> {
        let mut listing = self.store_listing.blocking_lock();
        let repo = self.hgrepo();
        let _lock = store_lock(repo)?;
        let stamp = changelog_stamp(&repo.store_path());
        if let Some((seen, files)) = listing.as_ref() {
            if *seen == stamp {
                return Ok(Arc::clone(files));
            }
        }
        let files = Arc::new(list_store(repo)?);
        *listing = Some((stamp, Arc::clone(&files)));
        Ok(files)
    }

    pub fn heads_match(&self, heads: &[NodeHash]) -> Result<bool> {
        crate::push::heads_match(self.hgrepo(), heads)
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
    let bookmarks = repo.hgrepo().bookmarks()?;
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
    if !args.bundlecaps.iter().any(|cap| cap == b"HG20") {
        return legacy_changegroup(
            repo.hgrepo().clone(),
            Selection::Common {
                common: args.common,
                heads: args.heads,
            },
        )
        .map_err(|err| hgproto::Error::from(err.to_string()));
    }
    let (reader, writer) = tokio::io::duplex(BUNDLE_PIPE);
    let mut bundle = Bundle2EncodeBuilder::new(writer);
    // TODO: select an appropriate compressor. zstd appears to hang -- need to figure out
    // whether it's a framing issue or something else.
    bundle.set_compressor_type(CompressorType::Uncompressed);

    if args.stream {
        bundle.add_part(stream_clone_part(repo).await?);
    } else if args.cg {
        let history = history_part(
            repo.hgrepo().clone(),
            Selection::Common {
                common: args.common.clone(),
                heads: args.heads.clone(),
            },
            send_cg_version(repo.hgrepo(), &args)?,
        )
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

/// How long a writer waits for `hg` to release a lock, Mercurial's default `ui.timeout`.
const LOCK_TIMEOUT: Duration = Duration::from_secs(600);
const LOCK_POLL: Duration = Duration::from_millis(100);

/// Take the Mercurial lock at `path`, waiting while someone else holds it.
/// Blocks; the server's runtime has no timer, so run it off the runtime thread.
fn wait_for_lock(path: &Path) -> Result<RepoLock> {
    let started = Instant::now();
    loop {
        if let Some(lock) = RepoLock::try_acquire(path)? {
            return Ok(lock);
        }
        if started.elapsed() >= LOCK_TIMEOUT {
            let holder = RepoLock::holder(path).unwrap_or_else(|| "someone".to_owned());
            return Err(format!(
                "{} is held by {holder}; if neither hg nor this server holds it, free it with `hg debuglocks --force-free-lock` or `--force-free-wlock`",
                path.display()
            )
            .into());
        }
        std::thread::sleep(LOCK_POLL);
    }
}

/// The store lock, which `hg` takes for any change under `.hg/store`.
fn store_lock(repo: &mercurial::RevlogRepo) -> Result<RepoLock> {
    wait_for_lock(&repo.store_path().join("lock"))
}

/// `.hg/wlock` then `.hg/store/lock`, in Mercurial's order, for a push or a
/// bookmark change. Waits on the blocking pool so other requests keep going.
async fn write_locks(repo: &mercurial::RevlogRepo) -> hgproto::Result<(RepoLock, RepoLock)> {
    let store = repo.store_path();
    tokio::task::spawn_blocking(move || -> Result<(RepoLock, RepoLock)> {
        let hg_dir = store.parent().expect("store is under .hg");
        let wlock = wait_for_lock(&hg_dir.join("wlock"))?;
        let lock = wait_for_lock(&store.join("lock"))?;
        Ok((wlock, lock))
    })
    .await
    .map_err(|err| hgproto::Error::from(err.to_string()))?
    .map_err(|err| hgproto::Error::from(err.to_string()))
}

async fn stream_clone_part(repo: &Arc<Repo>) -> hgproto::Result<PartEncodeBuilder> {
    let requirements = stream_requirements(repo.hgrepo()).ok_or_else(|| {
        hgproto::Error::from(
            "stream clone requested, but this repo is not served as one".to_string(),
        )
    })?;
    let lister = Arc::clone(repo);
    let files = tokio::task::spawn_blocking(move || lister.stream_listing())
        .await
        .map_err(|err| hgproto::Error::from(err.to_string()))?
        .map_err(|err| hgproto::Error::from(err.to_string()))?;
    stream_part(files, &requirements).map_err(|err| hgproto::Error::from(err.to_string()))
}

/// Resolve a `lookup` key as Mercurial's `revsymbol` does for the forms a
/// remote asks about: `null`, `tip`, a revision number (negative counts from
/// the end), a full hash, a bookmark, then a unique hash prefix. Tags, branch
/// names and `.` are not resolved.
fn resolve_key(
    repo: &mercurial::RevlogRepo,
    bookmarks: &[(Vec<u8>, Vec<u8>)],
    key: &str,
) -> std::result::Result<NodeHash, String> {
    let changelog = repo.changelog_revlog();
    let len = i64::from(changelog.len().map_err(|err| err.to_string())?);
    let node_at = |rev: i64| {
        changelog
            .get_entry(RevIdx::from(rev as u32))
            .map(|entry| entry.nodeid)
            .map_err(|err| err.to_string())
    };
    match key {
        "null" => return Ok(NULL_HASH),
        "tip" if len == 0 => return Ok(NULL_HASH),
        "tip" => return node_at(len - 1),
        _ => {}
    }
    // Only the canonical spelling is a number, so `01` is not rev 1. Out of
    // range falls through to the names below, as in Mercurial.
    if let Ok(num) = key.parse::<i64>() {
        let rev = if num < 0 { num + len } else { num };
        if num.to_string() == key && (0..len).contains(&rev) {
            return node_at(rev);
        }
    }
    if key.len() == 40 {
        if let Ok(node) = key.parse::<NodeHash>() {
            if changelog.index_of(&node).is_ok() {
                return Ok(node);
            }
        }
    }
    if let Some((_, hex)) = bookmarks.iter().find(|(name, _)| name == key.as_bytes()) {
        return std::str::from_utf8(hex)
            .ok()
            .and_then(|hex| hex.parse().ok())
            .ok_or_else(|| format!("bookmark '{key}' has a bad node"));
    }
    if !key.is_empty() && key.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        let prefix = key.to_ascii_lowercase();
        let mut found = None;
        for rev in 0..len {
            let node = node_at(rev)?;
            if node.to_hex().as_str().starts_with(&prefix) {
                if found.is_some() {
                    return Err(format!("00changelog@{key}: ambiguous identifier"));
                }
                found = Some(node);
            }
        }
        if let Some(node) = found {
            return Ok(node);
        }
    }
    Err(format!("unknown revision '{key}'"))
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
            this.wait_cs = Some(this.repo.hgrepo().get_changeset_by_nodeid(&this.n));
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
            let mut heads = repo
                .hgrepo()
                .get_heads()
                .map_err(hgproto::Error::from)
                .try_collect::<HashSet<_>>()
                .await?;
            heads.remove(&NULL_HASH);
            let mut map = HashMap::new();
            // An empty repository has no branches, and hg cannot parse a branch without heads.
            if !heads.is_empty() {
                map.insert("default".to_string(), heads);
            }
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
    fn changegroup(&self, roots: Vec<NodeHash>) -> HgCommandRes<BundleStream> {
        info!(self.logger, "changegroup roots {:?}", roots);
        let repo = Arc::clone(&self.repo);
        async move {
            let heads = repo.hgrepo().get_heads().try_collect().await?;
            legacy_changegroup(repo.hgrepo().clone(), Selection::Roots { roots, heads })
                .map_err(|err| hgproto::Error::from(err.to_string()))
        }
        .boxed()
    }

    fn changegroupsubset(
        &self,
        bases: Vec<NodeHash>,
        heads: Vec<NodeHash>,
    ) -> HgCommandRes<BundleStream> {
        let repo = Arc::clone(&self.repo);
        async move {
            legacy_changegroup(
                repo.hgrepo().clone(),
                Selection::Roots {
                    roots: bases,
                    heads,
                },
            )
            .map_err(|err| hgproto::Error::from(err.to_string()))
        }
        .boxed()
    }

    // @wireprotocommand('heads')
    fn heads(&self) -> HgCommandRes<HashSet<NodeHash>> {
        let repo = Arc::clone(&self.repo);
        async move {
            let heads = repo
                .hgrepo()
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

    // @wireprotocommand('lookup', 'key')
    fn lookup(&self, key: String) -> HgCommandRes<NodeHash> {
        info!(self.logger, "lookup {}", key);
        let repo = Arc::clone(&self.repo);
        async move {
            let bookmarks = bookmark_pairs(&repo).await?;
            // A hash prefix scans the changelog.
            tokio::task::spawn_blocking(move || resolve_key(repo.hgrepo(), &bookmarks, &key))
                .await
                .map_err(|err| hgproto::Error::from(err.to_string()))?
                .map_err(hgproto::Error::from)
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
                .map(|node| repo.hgrepo().changeset_exists(node))
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
        future::ok(advertised_caps(self.repo.hgrepo())).boxed()
    }

    // @wireprotocommand('hello')
    fn hello(&self) -> HgCommandRes<HashMap<String, Vec<String>>> {
        info!(self.logger, "Hello -> capabilities");

        let mut res = HashMap::new();
        res.insert(
            "capabilities".to_string(),
            advertised_caps(self.repo.hgrepo()),
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
            let _locks = write_locks(repo.hgrepo()).await?;
            crate::push::pushkey_bookmark(repo.hgrepo(), &key, old, new)
                .map_err(|err| hgproto::Error::from(err.to_string()))
        }
        .boxed()
    }

    // @wireprotocommand('unbundle', 'heads')
    fn unbundle(&self, heads: Vec<NodeHash>, bundle: Bytes) -> HgCommandRes<Bytes> {
        info!(self.logger, "unbundle heads {:?}", heads);
        let repo = Arc::clone(&self.repo);
        let logger = self.logger.clone();
        async move {
            let mut listing = repo.store_listing.lock().await;
            // Cleared before writing, so even a failed push lists the store again.
            *listing = None;
            let _locks = write_locks(repo.hgrepo()).await?;
            // `hgrepo()` again under the locks, to see anything hg wrote while we waited.
            let applied = crate::push::apply_repo(repo.hgrepo(), &heads, &bundle);
            repo.note_own_write();
            let reply = match applied {
                Ok(applied) => crate::push::reply_bundle(&applied).await,
                Err(err) => {
                    warn!(logger, "push failed: {}", err);
                    crate::push::abort_bundle(&err.to_string()).await
                }
            };
            reply.map_err(|err| hgproto::Error::from(err.to_string()))
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_cg_version_negotiates_legacy_and_modern_versions() {
        let root = std::env::temp_dir().join(format!("cgversions-{}", std::process::id()));
        fs::create_dir_all(root.join("store")).unwrap();
        fs::write(root.join("requires"), b"store\nrevlogv1\n").unwrap();
        let repo = mercurial::RevlogRepo::open(&root).unwrap();
        for (versions, expected) in [
            ("", Some(CgVersion::V1)),
            ("01", Some(CgVersion::V1)),
            ("01,02", Some(CgVersion::V2)),
            ("01,02,03", Some(CgVersion::V3)),
            ("04", None),
        ] {
            let args = GetbundleArgs {
                heads: Vec::new(),
                common: Vec::new(),
                listkeys: Vec::new(),
                stream: false,
                cg: true,
                bundlecaps: vec![
                    b"HG20".to_vec(),
                    format!("bundle2=HG20%0Achangegroup%3D{versions}").into_bytes(),
                ],
            };
            assert_eq!(send_cg_version(&repo, &args).ok(), expected, "{versions}");
        }
        drop(repo);
        fs::write(root.join("requires"), b"store\nrevlogv1\ntreemanifest\n").unwrap();
        let tree = mercurial::RevlogRepo::open(&root).unwrap();
        assert!(legacy_changegroup(
            tree,
            Selection::Common {
                common: Vec::new(),
                heads: Vec::new(),
            }
        )
        .is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn client_cg_versions_reads_the_bundle2_cap() {
        let caps = vec![
            b"HG20".to_vec(),
            b"bundle2=HG20%0Achangegroup%3D01%2C02%2C03%2C04%0Alistkeys".to_vec(),
        ];
        assert_eq!(
            client_cg_versions(&caps),
            vec![CgVersion::V1, CgVersion::V2, CgVersion::V3]
        );
        assert_eq!(client_cg_versions(&[b"HG20".to_vec()]), vec![CgVersion::V1]);
    }
}
