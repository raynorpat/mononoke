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
use mercurial_types::path::encodedir;
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
    RevlogCompressionZstd,
    ShareSafe,
    SparseRevlog,
    /// Working-copy format only. The server never reads a dirstate.
    DirstateV2,
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
            Required::RevlogCompressionZstd => "revlog-compression-zstd",
            Required::ShareSafe => "share-safe",
            Required::SparseRevlog => "sparserevlog",
            Required::DirstateV2 => "dirstate-v2",
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
            "revlog-compression-zstd" => Ok(Required::RevlogCompressionZstd),
            "share-safe" => Ok(Required::ShareSafe),
            "sparserevlog" => Ok(Required::SparseRevlog),
            "dirstate-v2" => Ok(Required::DirstateV2),
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
        if req.contains(&Required::ShareSafe) {
            let file = match fs::File::open(store.join("requires")) {
                Ok(file) => file,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    return Err("share-safe is set but `.hg/store/requires` is missing".into());
                }
                Err(err) => return Err(err.into()),
            };
            for line in BufReader::new(file).lines() {
                req.insert(line.chain_err(|| "Line read failed")?.parse()?);
            }
        }
        if req.contains(&Required::Treemanifest) {
            return Err("treemanifest is not served".into());
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

    /// Live changelog. Clones share the revlog the repo is already serving.
    pub fn changelog_revlog(&self) -> Revlog {
        self.inner.lock().expect("poisoned lock").changelog.clone()
    }

    /// Live manifest. Clones share the revlog the repo is already serving.
    pub fn manifest_revlog(&self) -> Revlog {
        self.inner.lock().expect("poisoned lock").manifest.clone()
    }

    /// The `.hg/store` directory.
    pub fn store_path(&self) -> PathBuf {
        self.basepath.join("store")
    }

    fn file_store_path(&self, path: &Path, ext: &[u8]) -> PathBuf {
        let dotencode = self.requirements.contains(&Required::Dotencode);
        self.basepath
            .join("store")
            .join(path.store_filelog(dotencode, ext))
    }

    pub fn get_file_revlog(&self, path: &Path) -> Result<Revlog> {
        let mut inner = self.inner.lock().expect("poisoned lock");

        match inner.filelogcache.entry(path.clone()) {
            Entry::Occupied(log) => Ok(log.get().clone()),

            Entry::Vacant(missing) => {
                let index = self.file_store_path(path, b"i");
                let data = self.file_store_path(path, b"d");
                let revlog = Revlog::from_idx_data(index, Some(data))?;
                Ok(missing.insert(revlog).clone())
            }
        }
    }

    /// Filelog for one pass over many files. Uses the cached one if there is
    /// one, but does not cache what it opens, so its maps close when dropped.
    ///
    /// The open happens outside the repo lock so callers can open in parallel.
    pub fn read_file_revlog(&self, path: &Path) -> Result<Revlog> {
        if let Some(log) = self
            .inner
            .lock()
            .expect("poisoned lock")
            .filelogcache
            .get(path)
        {
            return Ok(log.clone());
        }
        let index = self.file_store_path(path, b"i");
        let data = self.file_store_path(path, b"d");
        Revlog::from_idx_data(index, Some(data))
    }

    /// Filelog used for appends. Creates an empty index when the file is new.
    pub fn file_revlog_for_append(&self, path: &Path) -> Result<Revlog> {
        let index = self.file_store_path(path, b"i");
        if !index.exists() {
            if let Some(dir) = index.parent() {
                fs::create_dir_all(dir)?;
            }
            fs::write(&index, [])?;
            self.record_fncache(path, b"i")?;
        }
        self.get_file_revlog(path)
    }

    /// Add the filelog's `.d` to fncache once appends have split it out of the index.
    pub fn record_file_data(&self, path: &Path) -> Result<()> {
        if self.file_store_path(path, b"d").exists() {
            self.record_fncache(path, b"d")?;
        }
        Ok(())
    }

    fn record_fncache(&self, path: &Path, ext: &[u8]) -> Result<()> {
        if !self.requirements.contains(&Required::Fncache) {
            return Ok(());
        }
        // Mercurial keeps the file name with only directory encoding, not the
        // case-folded or hashed store path.
        let mut name = b"data/".to_vec();
        name.extend_from_slice(path.as_slice());
        name.push(b'.');
        name.extend_from_slice(ext);
        let rel = encodedir(&name);
        let fncache = self.basepath.join("store").join("fncache");
        let mut bytes = match fs::read(&fncache) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(err) => return Err(err.into()),
        };
        if bytes.split(|byte| *byte == b'\n').any(|line| line == rel.as_slice()) {
            return Ok(());
        }
        bytes.extend_from_slice(&rel);
        bytes.push(b'\n');
        fs::write(fncache, bytes)?;
        Ok(())
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

#[cfg(test)]
mod test {
    use std::fs;
    use std::process;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{Required, RevlogRepo};

    fn open_requires(label: &str, lines: &[&str]) -> crate::errors::Result<RevlogRepo> {
        open_split(label, lines, None)
    }

    fn open_split(
        label: &str,
        outer: &[&str],
        store: Option<&[&str]>,
    ) -> crate::errors::Result<RevlogRepo> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "mononoke-requires-{}-{}-{}",
            label,
            process::id(),
            nanos
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("store")).expect("store");
        fs::write(dir.join("requires"), format!("{}\n", outer.join("\n"))).expect("requires");
        if let Some(lines) = store {
            fs::write(
                dir.join("store").join("requires"),
                format!("{}\n", lines.join("\n")),
            )
            .expect("store requires");
        }
        let empty = include_bytes!("revlog/empty.i.bin");
        fs::write(dir.join("store").join("00changelog.i"), empty).expect("changelog");
        fs::write(dir.join("store").join("00manifest.i"), empty).expect("manifest");
        let opened = RevlogRepo::open(&dir);
        let _ = fs::remove_dir_all(&dir);
        opened
    }

    fn hg(repo: &std::path::Path, args: &[&str]) -> process::Output {
        let mut cmd = process::Command::new("hg");
        cmd.current_dir(repo)
            .args(args)
            .env("HGPLAIN", "1")
            .env("HGRCPATH", "");
        cmd.output().unwrap_or_else(|err| panic!("spawn hg {args:?}: {err}"))
    }

    fn assert_hg(output: &process::Output, what: &str) {
        assert!(
            output.status.success(),
            "{what} failed: status {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn dir_has_index(dir: &std::path::Path) -> bool {
        let Ok(entries) = fs::read_dir(dir) else {
            return false;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if dir_has_index(&path) {
                    return true;
                }
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("i") {
                return true;
            }
        }
        false
    }

    #[test]
    fn unknown_requirement_names_the_line() {
        let err = open_requires("unknown", &["store", "revlogv1", "not-a-real-requirement"])
            .expect_err("unknown requirement");
        let msg = err.to_string();
        assert!(msg.contains("not-a-real-requirement"), "{}", msg);
    }

    #[test]
    fn treemanifest_is_not_served() {
        let err = open_requires("tree", &["store", "revlogv1", "treemanifest"])
            .expect_err("treemanifest");
        let msg = err.to_string();
        assert!(msg.contains("treemanifest is not served"), "{}", msg);
    }

    #[test]
    fn zstd_requirement_is_accepted() {
        open_requires(
            "zstd",
            &[
                "store",
                "fncache",
                "dotencode",
                "generaldelta",
                "revlogv1",
                "revlog-compression-zstd",
            ],
        )
        .expect("open");
    }

    #[test]
    fn share_safe_reads_store_requires() {
        let repo = open_split(
            "share",
            &["share-safe"],
            Some(&[
                "dotencode",
                "fncache",
                "generaldelta",
                "revlog-compression-zstd",
                "revlogv1",
                "sparserevlog",
                "store",
            ]),
        )
        .expect("open");
        let names: Vec<_> = repo.get_requirements().iter().map(|r| r.to_string()).collect();
        for line in [
            "share-safe",
            "sparserevlog",
            "generaldelta",
            "revlogv1",
            "revlog-compression-zstd",
            "store",
            "fncache",
            "dotencode",
        ] {
            assert!(names.iter().any(|n| n == line), "{line} missing from {names:?}");
        }
    }

    #[test]
    fn dirstate_v2_requirement_is_accepted() {
        // A working-copy requirement, so Mercurial keeps it in `.hg/requires`.
        let repo = open_split(
            "dsv2",
            &["dirstate-v2", "share-safe"],
            Some(&["fncache", "generaldelta", "revlogv1", "store"]),
        )
        .expect("dirstate-v2 repo opens");
        assert!(repo.get_requirements().contains(&Required::DirstateV2));
    }

    #[test]
    fn share_safe_missing_store_requires_names_share_safe() {
        let err = open_split("missing", &["share-safe"], None).expect_err("missing store/requires");
        let msg = err.to_string();
        assert!(msg.contains("share-safe"), "{msg}");
        assert!(!msg.contains("Unknown requirement"), "{msg}");
    }

    #[test]
    fn same_line_in_both_requires_files_opens() {
        open_split(
            "both",
            &["share-safe", "store"],
            Some(&["store", "sparserevlog"]),
        )
        .expect("union");
    }

    #[test]
    fn treemanifest_in_store_requires_is_not_served() {
        let err = open_split(
            "tree-store",
            &["share-safe"],
            Some(&["store", "treemanifest"]),
        )
        .expect_err("treemanifest");
        assert!(
            err.to_string().contains("treemanifest is not served"),
            "{}",
            err
        );
    }

    #[test]
    fn unknown_store_requirement_names_the_line() {
        let err = open_split(
            "unk-store",
            &["share-safe"],
            Some(&["not-a-real-requirement"]),
        )
        .expect_err("unknown store line");
        assert!(
            err.to_string()
                .contains("Unknown requirement \"not-a-real-requirement\""),
            "{}",
            err
        );
    }

    #[test]
    fn long_mixed_case_path_opens_from_store_dh() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("mononoke-dh-{}-{}", process::id(), nanos));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("temp root");

        let mut init = process::Command::new("hg");
        init.current_dir(&root)
            .args(["init", "repo"])
            .args(["--config", "format.generaldelta=true"])
            .args(["--config", "format.revlog-compression=zstd"])
            .args(["--config", "format.sparse-revlog=false"])
            .args(["--config", "format.use-persistent-nodemap=false"])
            .args(["--config", "format.use-share-safe=false"])
            .args(["--config", "format.use-dirstate-v2=false"])
            .args([
                "--config",
                "format.exp-use-copies-side-data-changeset=false",
            ])
            .env("HGPLAIN", "1")
            .env("HGRCPATH", "");
        assert_hg(&init.output().expect("spawn hg init"), "hg init");

        let repo = root.join("repo");
        let name: String = std::iter::once('A')
            .chain(std::iter::repeat('b').take(113))
            .collect();
        assert_eq!(name.len(), 114);
        fs::write(repo.join(&name), b"long\n").expect("write long file");
        assert_hg(&hg(&repo, &["add", &name]), "hg add");
        assert_hg(&hg(&repo, &["commit", "-m", "long", "-u", "test"]), "hg commit");

        let requires = fs::read_to_string(repo.join(".hg").join("requires")).expect("requires");
        assert!(
            requires.lines().any(|line| line == "dotencode"),
            "requires missing dotencode:\n{requires}"
        );

        let data_index = repo
            .join(".hg")
            .join("store")
            .join("data")
            .join(format!("{name}.i"));
        assert!(
            !data_index.exists(),
            "unexpected index at {}",
            data_index.display()
        );

        let dh = repo.join(".hg").join("store").join("dh");
        assert!(dir_has_index(&dh), "no index under {}", dh.display());

        let opened = RevlogRepo::open(repo.join(".hg")).expect("open repo");
        let path = mercurial_types::Path::new(name.as_bytes()).expect("path");
        let revlog = opened.get_file_revlog(&path).expect("filelog");
        let node = revlog
            .get_rev(crate::revlog::RevIdx::from(0u32))
            .expect("rev 0");
        assert_eq!(node.as_blob().as_slice(), Some(&b"long\n"[..]));

        let _ = fs::remove_dir_all(&root);
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
