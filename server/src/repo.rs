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
use mercurial::revlogrepo::Required;
use mercurial_bundles::changegroup::CgVersion;
use mercurial_bundles::part_encode::PartEncodeBuilder;
use mercurial_bundles::{parts, Bundle2EncodeBuilder};
use mercurial_types::{percent_decode, percent_encode, Changeset, NodeHash, Parents, NULL_HASH};
use slog::{info, o, warn, Logger};
use tokio_util::io::ReaderStream;

use crate::bundle::{history_part, legacy_changegroup, Selection, LFS_REQUIRED};
use crate::errors::Result;
use crate::lfs::LfsServing;
use crate::names::{self, BranchCache};
use crate::push::{Planned, PushPolicy};
use crate::stream::{list_store, stream_part, stream_requirements, StoreFile};

pub struct RepoOptions {
    pub lfs: Option<LfsServing>,
    pub max_plain_file_size: u64,
}

impl Default for RepoOptions {
    fn default() -> Self {
        RepoOptions {
            lfs: None,
            max_plain_file_size: 10485760,
        }
    }
}

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
    branch_cache: std::sync::Mutex<BranchCache>,
    options: RepoOptions,
    logger: Logger,
}

/// Size and modification time of `00changelog.i` and `00changelog.d`, which
/// every commit, push or strip changes, and of both `requires` files: hg's lfs
/// extension adds its requirement after the commit's transaction closes.
type ChangelogStamp = [Option<(u64, SystemTime)>; 4];

fn changelog_stamp(store: &Path) -> ChangelogStamp {
    let hg_dir = store.parent().expect("store is under .hg");
    [
        store.join("00changelog.i"),
        store.join("00changelog.d"),
        store.join("requires"),
        hg_dir.join("requires"),
    ]
    .map(|path| {
        let meta = fs::metadata(path).ok()?;
        Some((meta.len(), meta.modified().ok()?))
    })
}

fn wireprotocaps() -> Vec<String> {
    vec![
        "lookup".to_string(),
        "known".to_string(),
        "getbundle".to_string(),
        "changegroupsubset".to_string(),
        "unbundle=HG10GZ,HG10BZ,HG10UN".to_string(),
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
/// else 02 or 01. A treemanifest or LFS repository can only be sent as 03.
fn send_cg_version(
    repo: &mercurial::RevlogRepo,
    args: &GetbundleArgs,
) -> hgproto::Result<CgVersion> {
    let client = client_cg_versions(&args.bundlecaps);
    if client.contains(&CgVersion::V3) {
        Ok(CgVersion::V3)
    } else if repo.get_requirements().contains(&Required::Lfs) {
        Err(hgproto::Error::from(LFS_REQUIRED.to_string()))
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

fn advertised_caps(server: &Repo) -> Vec<String> {
    let repo = server.hgrepo();
    let mut caps = wireprotocaps();
    let streamreqs = stream_requirements(repo);
    if let Some(reqs) = &streamreqs {
        // Clients stream-clone by default; `hg clone --pull` still gets a changegroup.
        caps.push("stream-preferred".to_string());
        caps.push(if reqs == "revlogv1" || reqs.is_empty() {
            "stream".to_string()
        } else {
            format!("streamreqs={reqs}")
        });
    }
    if repo
        .store_path()
        .parent()
        .expect(".hg directory")
        .join("clonebundles.manifest")
        .exists()
    {
        caps.extend(["clonebundles".into(), "clonebundles_manifest".into()]);
    }
    if server.lfs().is_some() {
        caps.push("lfs".to_string());
        if repo.get_requirements().contains(&Required::Lfs) {
            caps.push("lfs-serve".to_string());
        }
    }
    caps.push(format!("bundle2={}", bundle2caps(streamreqs.is_some())));
    caps
}

/// Mercurial normalizes manifest line endings and filters inline URLs for old clients.
fn clone_manifest(repo: &mercurial::RevlogRepo, legacy: bool) -> Result<String> {
    let path = repo
        .store_path()
        .parent()
        .expect(".hg directory")
        .join("clonebundles.manifest");
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(String::new()),
        Err(err) => return Err(err.into()),
    };
    let text = text.replace("\r\n", "\n").replace('\r', "\n");
    Ok(text
        .lines()
        .filter(|line| !legacy || !line.starts_with("peer-bundle-cache://"))
        .map(|line| format!("{line}\n"))
        .collect())
}

impl Repo {
    pub fn new<P: AsRef<Path>>(
        parent_logger: &Logger,
        path: P,
        options: RepoOptions,
    ) -> Result<Self> {
        let path = path.as_ref();
        // Stamp before opening: a change in between only causes one extra reload.
        let changelog_seen = std::sync::Mutex::new(changelog_stamp(&path.join("store")));

        Ok(Repo {
            path: format!("{:?}", path),
            revlogs: mercurial::RevlogRepo::open(path)?,
            changelog_seen,
            branch_cache: std::sync::Mutex::new(BranchCache::default()),
            store_listing: tokio::sync::Mutex::new(None),
            options,
            logger: parent_logger.new(o!("repo" => format!("{:?}", path))),
        })
    }

    pub fn lfs(&self) -> Option<&LfsServing> {
        self.options.lfs.as_ref()
    }

    pub fn options(&self) -> &RepoOptions {
        &self.options
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
                Ok(()) => {
                    *self.branch_cache.lock().expect("branch cache lock") = BranchCache::default();
                    *seen = now;
                }
                Err(err) => warn!(
                    self.logger,
                    "reload after an outside change failed: {}", err
                ),
            }
        }
        &self.revlogs
    }

    // Keep reload and branch metadata in the same lock order: stamp, then cache.
    fn with_branches<T>(
        &self,
        f: impl FnOnce(&mercurial::RevlogRepo, &mut BranchCache) -> Result<T>,
    ) -> Result<T> {
        self.hgrepo();
        let _stamp = self.changelog_seen.lock().expect("changelog stamp lock");
        let mut cache = self.branch_cache.lock().expect("branch cache lock");
        f(&self.revlogs, &mut cache)
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

fn apply_push(repo: &Repo, heads: &[NodeHash], bundle: &[u8]) -> Result<crate::push::Applied> {
    let policy = PushPolicy {
        lfs: repo.lfs().map(|lfs| lfs.store.as_ref()),
        max_plain_file_size: repo.options().max_plain_file_size,
    };
    // Validation stays outside the stamp lock: `hgrepo()` takes that lock on the
    // runtime thread, and the LFS store's IO needs that thread to make progress.
    let plan = match crate::push::plan_push(&repo.revlogs, heads, bundle, &policy)? {
        Planned::Done(applied) => return Ok(applied),
        Planned::Write(plan) => plan,
    };
    // Match with_branches' lock order. No branch reader can cache
    // changesets from a transaction that is subsequently rolled back.
    let mut stamp = repo.changelog_seen.lock().expect("changelog stamp lock");
    let applied = crate::push::write_push(&repo.revlogs, plan);
    if applied.is_err() {
        *repo.branch_cache.lock().expect("branch cache lock") = BranchCache::default();
    }
    *stamp = changelog_stamp(&repo.revlogs.store_path());
    applied
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
    fn branchmap(&self) -> HgCommandRes<HashMap<Vec<u8>, HashSet<NodeHash>>> {
        let repo = Arc::clone(&self.repo);
        async move {
            tokio::task::spawn_blocking(move || {
                repo.with_branches(|hg, cache| {
                    cache.extend(hg)?;
                    let parents = names::changelog_parents(hg)?;
                    let log = hg.changelog_revlog();
                    cache
                        .heads(&parents)
                        .into_iter()
                        .map(|(name, revisions)| {
                            let nodes = revisions
                                .into_iter()
                                .map(|r| log.get_entry(RevIdx::from(r)).map(|e| e.nodeid))
                                .collect::<std::result::Result<HashSet<_>, _>>()?;
                            Ok((name, nodes))
                        })
                        .collect()
                })
            })
            .await
            .map_err(|e| hgproto::Error::from(e.to_string()))?
            .map_err(|e| hgproto::Error::from(e.to_string()))
        }
        .boxed()
    }

    fn branches(&self, mut nodes: Vec<NodeHash>) -> HgCommandRes<Vec<hgproto::BranchRes>> {
        let repo = Arc::clone(&self.repo);
        async move {
            let log = repo.hgrepo().changelog_revlog();
            if nodes.is_empty() {
                let len = log.len()?;
                nodes.push(if len == 0 {
                    NULL_HASH
                } else {
                    log.get_entry(RevIdx::from(len - 1))?.nodeid
                });
            }
            let mut result = Vec::new();
            for top in nodes {
                let mut node = top;
                loop {
                    let (p1, p2) = if node == NULL_HASH {
                        (None, None)
                    } else {
                        let e = log.get_entry_by_nodeid(&node)?;
                        (
                            e.p1.map(|r| log.get_entry(r).map(|e| e.nodeid))
                                .transpose()?,
                            e.p2.map(|r| log.get_entry(r).map(|e| e.nodeid))
                                .transpose()?,
                        )
                    };
                    if p2.is_some() || p1.is_none() {
                        result.push(hgproto::BranchRes::new(top, node, p1, p2));
                        break;
                    }
                    node = p1.expect("linear parent");
                }
            }
            Ok(result)
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

    fn clonebundles(&self) -> HgCommandRes<String> {
        let repo = Arc::clone(&self.repo);
        async move {
            clone_manifest(repo.hgrepo(), true).map_err(|err| hgproto::Error::from(err.to_string()))
        }
        .boxed()
    }

    fn clonebundles_manifest(&self) -> HgCommandRes<String> {
        let repo = Arc::clone(&self.repo);
        async move {
            clone_manifest(repo.hgrepo(), false)
                .map_err(|err| hgproto::Error::from(err.to_string()))
        }
        .boxed()
    }

    fn get_cached_bundle_inline(&self, path: String) -> HgCommandRes<BundleStream> {
        let repo = Arc::clone(&self.repo);
        async move {
            tokio::task::spawn_blocking(move || -> Result<BundleStream> {
                let hg = repo.hgrepo();
                let root = hg
                    .hgrc_value("server", "peer-bundle-cache-root")?
                    .unwrap_or_else(|| "bundle-cache".into());
                let root = hg.store_path().parent().expect(".hg directory").join(root);
                let file = root.join(&path);
                if !file.exists() {
                    return Err(format!("clonebundle {path} does not exist").into());
                }
                if !file.canonicalize()?.starts_with(root.canonicalize()?) {
                    return Err(format!("clonebundle {path} is using an illegal path").into());
                }
                crate::stream::inline_bundle(fs::File::open(file)?)
            })
            .await
            .map_err(|err| hgproto::Error::from(err.to_string()))?
            .map_err(|err| hgproto::Error::from(err.to_string()))
        }
        .boxed()
    }

    fn stream_out(&self) -> HgCommandRes<BundleStream> {
        let repo = Arc::clone(&self.repo);
        async move {
            let files = tokio::task::spawn_blocking(move || repo.stream_listing())
                .await
                .map_err(|err| hgproto::Error::from(err.to_string()))?
                .map_err(|err| hgproto::Error::from(err.to_string()))?;
            Ok(crate::stream::stream_v1(files))
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
    fn lookup(&self, key: Vec<u8>) -> HgCommandRes<NodeHash> {
        info!(self.logger, "lookup {:?}", key);
        let repo = Arc::clone(&self.repo);
        async move {
            let bookmarks = bookmark_pairs(&repo).await?;
            // A hash prefix scans the changelog.
            tokio::task::spawn_blocking(move || {
                repo.with_branches(|hg, cache| {
                    names::resolve_key(hg, cache, &bookmarks, &key).map_err(Into::into)
                })
            })
            .await
            .map_err(|err| hgproto::Error::from(err.to_string()))?
            .map_err(|e| hgproto::Error::from(e.to_string()))
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
        future::ok(advertised_caps(&self.repo)).boxed()
    }

    // @wireprotocommand('hello')
    fn hello(&self) -> HgCommandRes<HashMap<String, Vec<String>>> {
        info!(self.logger, "Hello -> capabilities");

        let mut res = HashMap::new();
        res.insert("capabilities".to_string(), advertised_caps(&self.repo));

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
    fn unbundle(&self, heads: Vec<NodeHash>, bundle: Bytes) -> HgCommandRes<hgproto::Response> {
        info!(self.logger, "unbundle heads {:?}", heads);
        let repo = Arc::clone(&self.repo);
        let logger = self.logger.clone();
        async move {
            let mut listing = repo.store_listing.lock().await;
            // Cleared before writing, so even a failed push lists the store again.
            *listing = None;
            let locks = write_locks(repo.hgrepo()).await?;
            // `hgrepo()` again under the locks, to see anything hg wrote while we waited.
            repo.hgrepo();
            let bundle1 = crate::push::is_bundle1(&bundle);
            let pushing = Arc::clone(&repo);
            // The blocking pool, as LFS checks wait on the store. The task owns
            // the locks: it outlives this future if the client goes away.
            let applied = tokio::task::spawn_blocking(move || {
                let _locks = locks;
                apply_push(&pushing, &heads, &bundle)
            })
            .await
            .map_err(|err| hgproto::Error::from(err.to_string()))?;
            let reply = match applied {
                Ok(applied) => crate::push::reply(&applied).await,
                Err(err) => {
                    warn!(logger, "push failed: {}", err);
                    if bundle1 {
                        Ok(hgproto::Response::Pushres {
                            ret: 0,
                            output: Bytes::new(),
                        })
                    } else {
                        crate::push::abort_bundle(&err.to_string())
                            .await
                            .map(hgproto::Response::Unbundle)
                    }
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
    use crate::lfs::store::{LfsStore, StoreSpec};

    #[test]
    fn warm_up_does_not_read_changeset_text() {
        let root = std::env::temp_dir().join(format!("warmup-lazy-{}", std::process::id()));
        fs::create_dir_all(root.join("store")).unwrap();
        fs::write(root.join("requires"), b"store\nrevlogv1\n").unwrap();
        let logger = Logger::root(slog::Discard, slog::o!());
        let repo = Repo::new(&logger, &root, RepoOptions::default()).unwrap();
        repo.hgrepo().changelog_revlog().append_snapshot(
            mercurial::revlog::RevlogAppend {
                node: NodeHash::from_bytes(&[1; 20]).unwrap(), p1: None, p2: None,
                linkrev: Some(0), text: b"intentionally invalid changeset text".to_vec(),
            }, mercurial::revlog::RevlogCompression::Zlib,
        ).unwrap();
        // Startup only needs index metadata. Parsing branch metadata is deferred
        // until a command needs it, where this malformed changeset must fail.
        repo.warm_up().expect("warmup must not parse changesets");
        fs::write(root.join("dirstate"), [1u8; 20]).unwrap();
        fs::write(root.join("bookmarks"), format!("{} quick\n", "01".repeat(20))).unwrap();
        let repo = Arc::new(repo);
        let client = RepoClient::new(Arc::clone(&repo), &logger);
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        for key in ["tip", "null", ".", "0", "quick", &"01".repeat(20)] {
            runtime.block_on(client.lookup(key.as_bytes().to_vec())).expect("cheap lookup must not parse branch metadata");
        }
        assert!(repo.with_branches(|hg, cache| cache.extend(hg)).is_err());
        drop(client);
        drop(repo);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn stream_capability_uses_stream_for_revlogv1_only() {
        let root = std::env::temp_dir().join(format!("stream-caps-{}", std::process::id()));
        fs::create_dir_all(root.join("store")).unwrap();
        for (requirements, expected) in [
            ("store\nfncache\nrevlogv1\n", "stream"),
            ("store\nfncache\nrevlogv1\ngeneraldelta\n", "streamreqs=generaldelta,revlogv1"),
        ] {
            fs::write(root.join("requires"), requirements).unwrap();
            let logger = Logger::root(slog::Discard, slog::o!());
            let repo = Repo::new(&logger, &root, RepoOptions::default()).unwrap();
            let caps = advertised_caps(&repo);
            assert!(caps.iter().any(|cap| cap == expected), "{caps:?}");
            assert_eq!(caps.iter().filter(|cap| *cap == "stream" || cap.starts_with("streamreqs=")).count(), 1);
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn caps_advertise_lfs_only_when_serving() {
        fn bundle2(caps: &[String]) -> String {
            let cap = caps.iter().find_map(|cap| cap.strip_prefix("bundle2=")).unwrap();
            percent_decode(cap)
        }
        let root = std::env::temp_dir().join(format!("lfs-caps-{}", std::process::id()));
        fs::create_dir_all(root.join("store")).unwrap();
        fs::write(root.join("requires"), b"store\nrevlogv1\n").unwrap();
        let logger = Logger::root(slog::Discard, slog::o!());

        let plain = Repo::new(&logger, &root, RepoOptions::default()).unwrap();
        assert_eq!(plain.options().max_plain_file_size, 10485760);
        assert!(plain.lfs().is_none());
        let caps = advertised_caps(&plain);
        assert!(!caps.iter().any(|cap| cap == "lfs" || cap == "lfs-serve"), "{caps:?}");
        assert!(bundle2(&caps).lines().any(|line| line == "changegroup=01,02,03"));

        let store = LfsStore::open(&StoreSpec::Repo, None, &root.join("store"), &logger).unwrap();
        let options = RepoOptions {
            lfs: Some(LfsServing { store: Arc::new(store), public_url: None }),
            ..RepoOptions::default()
        };
        let serving = Repo::new(&logger, &root, options).unwrap();
        let caps = advertised_caps(&serving);
        assert!(caps.iter().any(|cap| cap == "lfs"), "{caps:?}");
        assert!(!caps.iter().any(|cap| cap == "lfs-serve"), "{caps:?}");
        assert!(bundle2(&caps).lines().any(|line| line == "changegroup=01,02,03"));

        serving.hgrepo().add_requirement(Required::Lfs).unwrap();
        let caps = advertised_caps(&serving);
        assert!(caps.iter().any(|cap| cap == "lfs") && caps.iter().any(|cap| cap == "lfs-serve"), "{caps:?}");
        drop((plain, serving));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn requirement_written_outside_the_server_is_noticed() {
        let root = std::env::temp_dir().join(format!("outside-reqs-{}", std::process::id()));
        fs::create_dir_all(root.join("store")).unwrap();
        fs::write(root.join("requires"), b"store\nrevlogv1\n").unwrap();
        let logger = Logger::root(slog::Discard, slog::o!());
        let repo = Repo::new(&logger, &root, RepoOptions::default()).unwrap();
        assert!(!repo.hgrepo().get_requirements().contains(&Required::Lfs));
        fs::write(root.join("requires"), b"store\nrevlogv1\nlfs\n").unwrap();
        assert!(repo.hgrepo().get_requirements().contains(&Required::Lfs));
        drop(repo);
        fs::remove_dir_all(root).unwrap();
    }

    fn getbundle_args(bundlecaps: Vec<Vec<u8>>) -> GetbundleArgs {
        GetbundleArgs {
            heads: Vec::new(),
            common: Vec::new(),
            listkeys: Vec::new(),
            stream: false,
            cg: true,
            bundlecaps,
        }
    }

    #[test]
    fn lfs_repo_takes_only_changegroup_03() {
        let root = std::env::temp_dir().join(format!("cg-lfs-{}", std::process::id()));
        fs::create_dir_all(root.join("store")).unwrap();
        fs::write(root.join("requires"), b"store\nrevlogv1\nlfs\n").unwrap();
        let repo = mercurial::RevlogRepo::open(&root).unwrap();
        let caps = |versions: &str| {
            getbundle_args(vec![
                b"HG20".to_vec(),
                format!("bundle2=HG20%0Achangegroup%3D{versions}").into_bytes(),
            ])
        };
        assert_eq!(send_cg_version(&repo, &caps("01,02,03")).ok(), Some(CgVersion::V3));
        for args in [caps("01,02"), caps("02"), caps("01"), caps("04"), getbundle_args(vec![b"HG20".to_vec()])] {
            let err = send_cg_version(&repo, &args).unwrap_err();
            assert!(err.to_string().contains(LFS_REQUIRED), "{err}");
        }
        drop(repo);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn unbundle_capability_lists_bundle1_types() {
        assert!(wireprotocaps()
            .iter()
            .any(|cap| cap == "unbundle=HG10GZ,HG10BZ,HG10UN"));
    }

    #[test]
    fn send_cg_version_negotiates_legacy_and_modern_versions() {
        let root = std::env::temp_dir().join(format!("cgversions-{}", std::process::id()));
        fs::create_dir_all(root.join("store")).unwrap();
        fs::write(root.join("requires"), b"store\nrevlogv1\n").unwrap();
        let repo = mercurial::RevlogRepo::open(&root).unwrap();
        let no_bundle2 = getbundle_args(vec![b"HG20".to_vec()]);
        assert_eq!(send_cg_version(&repo, &no_bundle2).ok(), Some(CgVersion::V1));
        for (versions, expected) in [
            ("", Some(CgVersion::V1)),
            ("01", Some(CgVersion::V1)),
            ("01,02", Some(CgVersion::V2)),
            ("01,02,03", Some(CgVersion::V3)),
            ("04", None),
        ] {
            let args = getbundle_args(vec![
                b"HG20".to_vec(),
                format!("bundle2=HG20%0Achangegroup%3D{versions}").into_bytes(),
            ]);
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
