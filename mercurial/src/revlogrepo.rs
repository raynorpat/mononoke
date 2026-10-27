// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::collections::HashSet;
use std::collections::hash_map::{Entry, HashMap};
use std::fmt::{self, Display};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::future::{self, BoxFuture, FutureExt};
use futures::stream::{self, BoxStream, StreamExt};
use futures::{Stream, TryFutureExt};

use asyncmemo::Filler;
use mercurial_types::{BlobNode, Changeset, Manifest, NodeHash, Path, Repo};
use stockbookmarks::StockBookmarks;

use std::result::Result as StdResult;

pub use crate::changeset::RevlogChangeset;
use crate::errors::{ChainErr, Error, ErrorKind, Result};
pub use crate::manifest::RevlogManifest;
use crate::revlog::{Revlog, RevlogIter};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum Required {
    Store,
    Fncache,
    Dotencode,
    Generaldelta,
    Treemanifest,
    Manifestv2,
    Usefncache,
    Revlogv1,
    Largefiles,
    Lz4revlog,
    SqlDirstate,
}

impl Display for Required {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            Required::Store => "store",
            Required::Fncache => "fncache",
            Required::Dotencode => "dotencode",
            Required::Generaldelta => "generaldelta",
            Required::Treemanifest => "treemanifest",
            Required::Manifestv2 => "manifestv2",
            Required::Usefncache => "usefncache",
            Required::Revlogv1 => "revlogv1",
            Required::Largefiles => "largefiles",
            Required::Lz4revlog => "lz4revlog",
            Required::SqlDirstate => "sqldirstate",
        };
        write!(fmt, "{}", s)
    }
}

impl FromStr for Required {
    type Err = Error;

    fn from_str(s: &str) -> Result<Required> {
        match s {
            "store" => Ok(Required::Store),
            "fncache" => Ok(Required::Fncache),
            "dotencode" => Ok(Required::Dotencode),
            "generaldelta" => Ok(Required::Generaldelta),
            "treemanifest" => Ok(Required::Treemanifest),
            "manifestv2" => Ok(Required::Manifestv2),
            "usefncache" => Ok(Required::Usefncache),
            "revlogv1" => Ok(Required::Revlogv1),
            "largefiles" => Ok(Required::Largefiles),
            "lz4revlog" => Ok(Required::Lz4revlog),
            "sqldirstate" => Ok(Required::SqlDirstate),
            unk => Err(ErrorKind::UnknownReq(unk.into()).into()),
        }
    }
}

/// Representation of a whole Mercurial repo
///
/// `Repo` represents a whole repo: ie, the complete history of a set of files.
/// It consists of an overall history in the form of a DAG of revisions, or changesets.
/// This DAG will typically have a single initial version (though it could have more if
/// histories are merged) and one or more heads, which are revisions which have no children.
///
/// Some revisions can be explicitly named with "bookmarks", and they're often heads as well.
///
/// At the filesystem level, the repo consists of:
///  - the changelog: .hg/store/00changelog.[di]
///  - the manifest: .hg/store/00manifest.[di]
///  - per-file histories: .hg/store/data/.../<file>.[di]
#[derive(Debug, Clone)]
pub struct RevlogRepo {
    basepath: PathBuf, // path to .hg directory
    requirements: HashSet<Required>, // requirements
    inner: Arc<Mutex<RevlogInner>>, // Inner parts
}

#[derive(Debug)]
struct RevlogInner {
    changelog: Revlog, // changes
    manifest: Revlog, // manifest
    filelogcache: HashMap<Path, Revlog>, // filelog cache
}

impl PartialEq<Self> for RevlogRepo {
    fn eq(&self, other: &Self) -> bool {
        self.basepath == other.basepath
            && self.requirements == other.requirements
            && Arc::ptr_eq(&self.inner, &other.inner)
    }
}
impl Eq for RevlogRepo {}

impl RevlogRepo {
    pub fn open<P: Into<PathBuf>>(base: P) -> Result<RevlogRepo> {
        let base = base.into();
        let store = base.as_path().join("store");

        let changelog = Revlog::from_idx_data(store.join("00changelog.i"), None as Option<String>)?;
        let manifest = Revlog::from_idx_data(store.join("00manifest.i"), None as Option<String>)?;

        let mut req = HashSet::new();
        let file = fs::File::open(base.join("requires")).chain_err(|| "Can't open `requires`")?;
        for line in BufReader::new(file).lines() {
            req.insert(line.chain_err(|| "Line read failed")?.parse()?);
        }

        Ok(RevlogRepo {
            basepath: base,
            requirements: req,
            inner: Arc::new(Mutex::new(RevlogInner {
                changelog: changelog,
                manifest: manifest,
                filelogcache: HashMap::new(),
            })),
        })
    }

    pub fn get_heads(&self) -> BoxStream<'static, StdResult<NodeHash, Error>> {
        let mut inner = self.inner.lock().expect("poisoned lock");
        match inner.changelog.get_heads() {
            Err(err) => stream::once(future::ready(Err(err))).boxed(),
            Ok(set) => stream::iter(set.into_iter().map(Ok)).boxed(),
        }
    }

    pub fn changeset_exists(
        &self,
        nodeid: &NodeHash,
    ) -> BoxFuture<'static, StdResult<bool, Error>> {
        let inner = self.inner.lock().expect("poisoned lock");
        let exists = inner.changelog.get_idx_by_nodeid(nodeid).is_ok();
        future::ready(Ok(exists)).boxed()
    }

    fn changeset_blob(&self, nodeid: &NodeHash) -> Result<BlobNode> {
        let inner = self.inner.lock().expect("poisoned lock");
        let idx = inner.changelog.get_idx_by_nodeid(nodeid)?;
        inner.changelog.get_rev(idx)
    }

    pub fn get_changeset_blob_by_nodeid(
        &self,
        nodeid: &NodeHash,
    ) -> BoxFuture<'static, StdResult<BlobNode, Error>> {
        future::ready(self.changeset_blob(nodeid)).boxed()
    }

    pub fn get_changeset_by_nodeid(
        &self,
        nodeid: &NodeHash,
    ) -> BoxFuture<'static, StdResult<RevlogChangeset, Error>> {
        // TODO: (jsgf) T17932873 distinguish between not existing vs some other error
        future::ready(self.changeset_blob(nodeid).and_then(RevlogChangeset::new)).boxed()
    }

    fn manifest_blob(&self, nodeid: &NodeHash) -> Result<BlobNode> {
        let inner = self.inner.lock().expect("poisoned lock");
        let idx = inner.manifest.get_idx_by_nodeid(nodeid)?;
        inner.manifest.get_rev(idx)
    }

    pub fn get_manifest_blob_by_nodeid(
        &self,
        nodeid: &NodeHash,
    ) -> BoxFuture<'static, StdResult<BlobNode, Error>> {
        future::ready(self.manifest_blob(nodeid)).boxed()
    }

    pub fn get_manifest_by_nodeid(
        &self,
        nodeid: &NodeHash,
    ) -> BoxFuture<'static, StdResult<RevlogManifest, Error>> {
        // TODO: (jsgf) T17932873 distinguish between not existing vs some other error
        let repo = self.clone();
        future::ready(
            self.manifest_blob(nodeid)
                .and_then(|rev| RevlogManifest::new(repo, rev)),
        )
        .boxed()
    }

    pub fn get_requirements(&self) -> &HashSet<Required> {
        &self.requirements
    }

    pub fn get_file_revlog(&self, path: &Path) -> Result<Revlog> {
        let mut inner = self.inner.lock().expect("poisoned lock");

        match inner.filelogcache.entry(path.clone()) {
            Entry::Occupied(log) => Ok(log.get().clone()),

            Entry::Vacant(missing) => {
                let dotencode = self.requirements.contains(&Required::Dotencode);
                let mut path = self
                    .basepath
                    .join("store")
                    .join("data")
                    .join(path.fsencode(dotencode));
                if let Some(ext) = path.extension().map(|ext| ext.to_string_lossy().into_owned()) {
                    path.set_extension(format!("{}.i", ext));
                } else {
                    path.set_extension("i");
                }

                let revlog = Revlog::from_idx_data(path, None as Option<String>)?;
                Ok(missing.insert(revlog).clone())
            }
        }
    }

    pub fn bookmarks(&self) -> Result<StockBookmarks> {
        StockBookmarks::read(self.basepath.clone()).map_err(Into::into)
    }

    pub fn changesets(&self) -> ChangesetStream {
        let inner = self.inner.lock().expect("poisoned lock");
        ChangesetStream::new(&inner.changelog)
    }
}

pub struct ChangesetBlobFiller(RevlogRepo);
impl ChangesetBlobFiller {
    pub fn new(revlog: &RevlogRepo) -> Self {
        ChangesetBlobFiller(revlog.clone())
    }
}

impl Filler for ChangesetBlobFiller {
    type Key = NodeHash;
    type Value = BoxFuture<'static, StdResult<Arc<BlobNode>, Error>>;

    fn fill(&self, key: &Self::Key) -> Self::Value {
        let key = *key;
        let repo = self.0.clone();
        async move { repo.changeset_blob(&key).map(Arc::new) }.boxed()
    }
}

pub struct ManifestBlobFiller(RevlogRepo);
impl ManifestBlobFiller {
    pub fn new(revlog: &RevlogRepo) -> Self {
        ManifestBlobFiller(revlog.clone())
    }
}

impl Filler for ManifestBlobFiller {
    type Key = NodeHash;
    type Value = BoxFuture<'static, StdResult<Arc<BlobNode>, Error>>;

    fn fill(&self, key: &Self::Key) -> Self::Value {
        let key = *key;
        let repo = self.0.clone();
        async move { repo.manifest_blob(&key).map(Arc::new) }.boxed()
    }
}

pub struct ChangesetStream(RevlogIter);

impl ChangesetStream {
    fn new(changelog: &Revlog) -> Self {
        ChangesetStream(changelog.into_iter())
    }
}

impl Stream for ChangesetStream {
    type Item = StdResult<NodeHash, Error>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Poll::Ready(this.0.next().map(|(_, entry)| Ok(entry.nodeid)))
    }
}

impl Repo for RevlogRepo {
    type Error = Error;

    fn get_heads(&self) -> BoxStream<'static, StdResult<NodeHash, Self::Error>> {
        RevlogRepo::get_heads(self)
    }

    fn get_changesets(&self) -> BoxStream<'static, StdResult<NodeHash, Self::Error>> {
        self.changesets().boxed()
    }

    fn changeset_exists(
        &self,
        nodeid: &NodeHash,
    ) -> BoxFuture<'static, StdResult<bool, Self::Error>> {
        RevlogRepo::changeset_exists(self, nodeid)
    }

    fn get_changeset_by_nodeid(
        &self,
        nodeid: &NodeHash,
    ) -> BoxFuture<'static, StdResult<Box<dyn Changeset>, Self::Error>> {
        RevlogRepo::get_changeset_by_nodeid(self, nodeid)
            .map_ok(|cs| cs.boxed())
            .boxed()
    }

    fn get_manifest_by_nodeid(
        &self,
        nodeid: &NodeHash,
    ) -> BoxFuture<'static, StdResult<Box<dyn Manifest<Error = Self::Error>>, Self::Error>> {
        RevlogRepo::get_manifest_by_nodeid(self, nodeid)
            .map_ok(|manifest| manifest.boxed())
            .boxed()
    }
}
