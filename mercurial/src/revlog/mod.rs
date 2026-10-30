// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::fmt::Debug;
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::result;
use std::sync::{Arc, Mutex};

use memmap2::Mmap;

use crate::errors::{ChainErr, Error, ErrorKind, Result};
use mercurial_types::{Blob, BlobNode, NodeHash};
pub use mercurial_types::bdiff::{self, Delta};
pub use mercurial_types::delta;

// Submodules
mod parser;
mod revidx;
mod lz4;
mod nodemap;

#[cfg(test)]
mod test;

use self::parser::{Header, Version};
pub use self::parser::{Entry, IdxFlags};
pub use self::nodemap::docket_uid;
pub use self::revidx::RevIdx;

#[derive(Debug)]
enum Datafile {
    Loaded(Vec<u8>),
    Mmap(Mmap),
}

impl Datafile {
    fn map<P: AsRef<Path>>(path: P) -> io::Result<Datafile> {
        let file = File::open(path)?;
        // Read-only map. Dropping `file` is safe: the mapping owns its handle.
        // A concurrent truncate can still fault a reader, same as the old mmap.
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(Datafile::Mmap(mmap))
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            Datafile::Loaded(data) => data.as_ref(),
            Datafile::Mmap(mmap) => mmap,
        }
    }
}

impl AsRef<[u8]> for Datafile {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

fn map_io<T, F, R, E>(v: Option<T>, f: &mut F) -> result::Result<Option<R>, E>
where
    F: FnMut(T) -> result::Result<R, E>,
{
    match v {
        None => Ok(None),
        Some(v) => f(v).map(Option::Some),
    }
}

/// `Revlog` represents a Mercurial revlog structure
///
/// A Mercurial revlog logicically consists of two parts: an index containing metadata about each
/// revision in the file, and data about each one. These may be stored in one or two files,
/// depending on whether the data is inlined into the index or not.
///
/// This type represents the logical revlog. It allows iteration over the entries, fetching
/// entries at random, and extracting the data for each entry.
#[derive(Debug, Clone)]
pub struct Revlog {
    inner: Arc<Mutex<RevlogInner>>,
}

#[derive(Debug)]
struct RevlogInner {
    header: Header,
    idx: Datafile,
    data: Option<Datafile>,
    idxoff: BTreeMap<RevIdx, usize>, // cache of index -> offset
    nodeidx: HashMap<NodeHash, RevIdx>, // cache of nodeid -> index
    text_cache: Option<(RevIdx, Vec<u8>)>, // last reconstructed text
    idx_path: Option<PathBuf>,
    data_path: Option<PathBuf>,
    nodemap: Option<nodemap::PersistentNodeMap>,
    want_nodemap: bool, // `reload` re-reads the nodemap
}

impl PartialEq<Self> for Revlog {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}
impl Eq for Revlog {}

/// Inline index-plus-data larger than this is rewritten as a separate `.d` file.
pub const INLINE_LIMIT: usize = 131072;
/// Longest delta chain an append extends, `sparse-revlog`'s default maximum.
const MAX_DELTA_CHAIN: usize = 1000;
/// An append does not extend a chain whose compressed size, base snapshot
/// included, would pass this many times the text, so reading stays cheap.
/// Mercurial's `LIMIT_DELTA2TEXT`.
const DELTA_CHAIN_TO_TEXT: usize = 2;

/// Compression for a full-text snapshot chunk. Neither variant adds a prefix byte.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RevlogCompression {
    Zlib,
    Zstd,
}

/// One full-text revision to append. `node` is stored as given.
#[derive(Clone, Debug)]
pub struct RevlogAppend {
    pub node: NodeHash,
    pub p1: Option<NodeHash>,
    pub p2: Option<NodeHash>,
    pub linkrev: Option<u32>,
    pub text: Vec<u8>,
}

impl Revlog {
    fn init(idx: Datafile, data: Option<Datafile>) -> Result<Self> {
        // An empty index has no header yet. The first append writes v1 inline generaldelta.
        // Any non-empty buffer still has to parse as a header.
        let (hdr, data) = if idx.as_slice().is_empty() {
            (
                Header {
                    version: Version::RevlogNG,
                    features: parser::INLINE | parser::GENERAL_DELTA,
                },
                None,
            )
        } else {
            let hdr = match parser::header(idx.as_slice()) {
                Ok((_, hdr)) => hdr,
                Err(err) => {
                    return Err(
                        ErrorKind::Revlog(format!("Header parse failed: {:?}", err)).into(),
                    )
                }
            };
            let data = if hdr.features.contains(parser::INLINE) {
                None
            } else {
                data
            };
            (hdr, data)
        };

        let mut idxoff = BTreeMap::new();
        idxoff.insert(RevIdx::zero(), 0); // prime cache - idx 0 = offset 0

        let inner = RevlogInner {
            header: hdr,
            idx: idx,
            data: data,
            idxoff: idxoff,
            nodeidx: HashMap::new(),
            text_cache: None,
            idx_path: None,
            data_path: None,
            nodemap: None,
            want_nodemap: false,
        };

        Ok(Revlog { inner: Arc::new(Mutex::new(inner)) })
    }

    /// Construct a `Revlog` using in-memory data. The index is required; the data
    /// may not be if either its inlined into the data, or not required for operations.
    pub fn new(idx: Vec<u8>, data: Option<Vec<u8>>) -> Result<Self> {
        Self::init(Datafile::Loaded(idx), data.map(Datafile::Loaded))
    }

    /// Construct a `Revlog` from an index file at the given path. Data may be inlined
    /// not not required.
    pub fn from_idx<IP>(idxpath: IP) -> Result<Revlog>
    where
        IP: AsRef<Path>,
    {
        let path = idxpath.as_ref().to_path_buf();
        let idx = Datafile::map(&path)
            .chain_err(|| format!("Can't map idxpath"))?;

        let revlog = Revlog::init(idx, None)?;
        revlog.inner.lock().expect("lock poisoned").idx_path = Some(path);

        Ok(revlog)
    }

    /// Construct a `Revlog` from an index file and data file. If `datapath` is not provided
    /// (`None`), and the index file is not inlined, then it will replace the index file's
    /// extension with `.d` and attempt to open that. The operation will fail if that file can't
    /// be opened.
    pub fn from_idx_data<IP, DP>(idxpath: IP, datapath: Option<DP>) -> Result<Revlog>
    where
        IP: AsRef<Path> + Debug,
        DP: AsRef<Path> + Debug,
    {
        let revlog = Self::from_idx(&idxpath)
            .chain_err(|| format!("Can't open index {:?}", idxpath))?;
        let datapath = datapath.as_ref().map(DP::as_ref);
        let idxpath = idxpath.as_ref();

        {
            let mut inner = revlog.inner.lock().expect("lock poisoned");

            if !inner.have_data() {
                let data_path = match datapath {
                    None => idxpath.with_extension("d"),
                    Some(path) => path.to_path_buf(),
                };
                let datafile = Datafile::map(&data_path)
                    .chain_err(|| format!("Can't open data file {:?}", data_path))?;
                inner.data = Some(datafile);
                inner.data_path = Some(data_path);
            } else if let Some(path) = datapath {
                // Inline today, but an append split must write the caller's `.d`.
                inner.data_path = Some(path.to_path_buf());
            }
        }

        Ok(revlog)
    }

    /// Return `true` if the `Revlog` has the data it requires - ie, the data is either inlined,
    /// or a data file has been provided.
    pub fn have_data(&self) -> bool {
        let inner = self.inner.lock().expect("lock poisoned");

        inner.have_data()
    }

    /// Get the `Revlog`s header.
    pub fn get_header(&self) -> Header {
        let inner = self.inner.lock().expect("lock poisoned");

        inner.header
    }

    /// Return an `Entry` entry from the `RevIdx`.
    pub fn get_entry(&self, idx: RevIdx) -> Result<Entry> {
        let mut inner = self.inner.lock().expect("lock poisoned");

        inner.get_entry(idx)
    }

    /// Return the ordinal index of an entry with the given nodeid.
    pub fn get_idx_by_nodeid(&self, nodeid: &NodeHash) -> Result<RevIdx> {
        let mut inner = self.inner.lock().expect("lock poisoned");

        inner.get_idx_by_nodeid(nodeid)
    }

    /// Return the ordinal index of an entry with the given nodeid.
    pub fn get_entry_by_nodeid(&self, nodeid: &NodeHash) -> Result<Entry> {
        let mut inner = self.inner.lock().expect("lock poisoned");

        inner.get_entry_by_nodeid(nodeid)
    }

    /// Return a `Chunk` for a revision at `RevIdx`.
    ///
    /// A `Chunk` either represents the literal
    /// text of the change, or a series of deltas against a previous version; the exact
    /// mechanism of applying the deltas depends on whether the `RevLog` has the `GENERAL_DELTA`
    /// flag set or not.
    pub fn get_chunk(&self, idx: RevIdx) -> Result<Chunk> {
        let mut inner = self.inner.lock().expect("lock poisoned");

        inner.get_chunk(idx)
    }

    pub fn get_rev(&self, tgtidx: RevIdx) -> Result<BlobNode> {
        let mut inner = self.inner.lock().expect("lock poisoned");

        inner.get_rev(tgtidx)
    }

    pub fn get_rev_by_nodeid(&self, id: &NodeHash) -> Result<BlobNode> {
        let mut inner = self.inner.lock().expect("lock poisoned");

        inner.get_rev_by_nodeid(id)
    }

    pub fn get_node_by_nodeid(&self, id: &NodeHash, with_data: bool) -> Result<BlobNode> {
        let mut inner = self.inner.lock().expect("lock poisoned");

        inner.get_node_by_nodeid(id, with_data)
    }

    /// Return the set of head revisions in a revlog
    pub fn get_heads(&mut self) -> Result<HashSet<NodeHash>> {
        let mut inner = self.inner.lock().expect("lock poisoned");

        inner.get_heads()
    }

    /// Append a generaldelta snapshot. The stored node id is `rev.node`.
    ///
    /// The base index is the new revision. `Zstd` is a zstd frame of the full
    /// text and `Zlib` is a zlib stream, with no extra prefix byte. An inline
    /// revlog whose written size would exceed `INLINE_LIMIT` is split first.
    pub fn append_snapshot(
        &mut self,
        rev: RevlogAppend,
        compression: RevlogCompression,
    ) -> Result<u32> {
        let mut inner = self.inner.lock().expect("lock poisoned");
        inner.append_revision(rev, None, compression, IdxFlags::empty())
    }

    /// Append a revision stored as `delta` against the `base` node when that
    /// pays off, otherwise as a snapshot. A changegroup carries such a delta.
    /// See `RevlogInner::delta_to_store` for when a delta is kept.
    pub fn append_revision(
        &mut self,
        rev: RevlogAppend,
        delta: Option<(NodeHash, delta::Delta)>,
        compression: RevlogCompression,
    ) -> Result<u32> {
        self.append_revision_flagged(rev, delta, compression, IdxFlags::empty())
    }

    /// `append_revision` that also stores `flags` (such as `EXTSTORED`) in the index entry.
    pub fn append_revision_flagged(
        &mut self,
        rev: RevlogAppend,
        delta: Option<(NodeHash, delta::Delta)>,
        compression: RevlogCompression,
        flags: IdxFlags,
    ) -> Result<u32> {
        let mut inner = self.inner.lock().expect("lock poisoned");
        inner.append_revision(rev, delta, compression, flags)
    }

    pub fn len(&self) -> Result<u32> {
        self.inner.lock().expect("lock poisoned").revision_count()
    }

    pub fn index_of(&self, node: &NodeHash) -> Result<u32> {
        self.get_idx_by_nodeid(node).map(|idx| idx.as_u32())
    }

    /// Re-read the index and data from disk, for a revlog that something else
    /// changed. Every clone shares the reloaded state, and cached offsets, nodes
    /// and text are dropped. An in-memory revlog is left as it is.
    pub fn reload(&self) -> Result<()> {
        self.reload_inner(false)
    }

    /// Reload after rollback, including newly created logs removed from disk.
    pub(crate) fn reload_after_rollback(&self) -> Result<()> {
        self.reload_inner(true)
    }

    fn reload_inner(&self, allow_missing: bool) -> Result<()> {
        let mut inner = self.inner.lock().expect("lock poisoned");
        let Some(idx_path) = inner.idx_path.clone() else {
            return Ok(());
        };
        let fresh = if !allow_missing || idx_path.exists() {
            Revlog::from_idx_data(&idx_path, inner.data_path.clone())?
        } else {
            // A failed transaction may have removed a newly created filelog.
            // Retained Arc handles must forget its revisions too.
            let fresh = Revlog::new(Vec::new(), None)?;
            {
                let mut empty = fresh.inner.lock().expect("lock poisoned");
                empty.idx_path = Some(idx_path);
                empty.data_path = inner.data_path.clone();
            }
            fresh
        };
        let fresh = Arc::try_unwrap(fresh.inner)
            .expect("a just-opened revlog has no other clones")
            .into_inner()
            .expect("lock poisoned");
        let want_nodemap = inner.want_nodemap;
        *inner = fresh;
        if want_nodemap {
            inner.attach_nodemap()?;
        }
        Ok(())
    }

    /// Use Mercurial's persistent nodemap for node lookups, if this revlog has
    /// a valid one. Returns whether it is in use. A later `reload` looks again.
    pub fn use_persistent_nodemap(&self) -> Result<bool> {
        self.inner.lock().expect("lock poisoned").attach_nodemap()
    }

    /// Parse every index entry so later node lookups, hits and misses, need no scan.
    pub fn load_node_map(&self) -> Result<()> {
        let mut inner = self.inner.lock().expect("lock poisoned");
        let first = inner.nodemap.as_ref().map_or(0, |nm| nm.tip_rev + 1);
        for idx in first..inner.revision_count()? {
            inner.get_entry(RevIdx::from(idx))?;
        }
        Ok(())
    }
}

impl RevlogInner {
    // Parse an entry at an offset, doing the correction for the overlap of the first
    // entry and the header.
    fn parse_entry(&self, off: usize) -> Result<Entry> {
        let res = match self.header.version {
            Version::Revlog0 => parser::index0(&self.idx.as_slice()[off..]),
            Version::RevlogNG => parser::indexng(&self.idx.as_slice()[off..]),
        };

        match res {
            Ok((_, mut res)) => {
                if off == 0 {
                    res.offset &= 0xffff;
                }
                Ok(res)
            }
            Err(err) => {
                return Err(
                    ErrorKind::Revlog(format!("failed to parse entry offset {}: {}", off, err))
                        .into(),
                )
            }
        }
    }

    fn fixed_entry_size(&self) -> usize {
        match self.header.version {
            Version::Revlog0 => parser::index0_size(),
            Version::RevlogNG => parser::indexng_size(),
        }
    }

    fn entry_size(&self, ent: Option<&Entry>) -> usize {
        let mut sz = self.fixed_entry_size();
        if self.header.features.contains(parser::INLINE) {
            sz += ent.expect("inline needs ent").compressed_len as usize;
        }
        sz
    }

    fn offset_for_idx(&self, idx: RevIdx) -> Option<usize> {
        if self.header.features.contains(parser::INLINE) {
            self.idxoff.get(&idx).cloned()
        } else {
            Some(idx * self.entry_size(None) as usize)
        }
    }

    fn have_data(&self) -> bool {
        // inline implies no data
        assert!(!self.header.features.contains(parser::INLINE) || self.data.is_none());

        // have data if inline or data is non-None
        self.header.features.contains(parser::INLINE) || self.data.is_some()
    }

    /// Return an `Entry` entry from the `RevIdx`.
    fn get_entry(&mut self, idx: RevIdx) -> Result<Entry> {
        let mut entry = if let Some(off) = self.offset_for_idx(idx) {
            // cache hit or computed
            self.parse_entry(off)?
        } else {
            // cache miss - find last cached offset and go from there
            let (last, mut off) = {
                let (last, off) = self.idxoff.iter().rev().next().expect("cache not primed?");
                (*last, *off)
            };
            assert!(
                last < idx,
                "last cached entry is after idx, but idx not in cache"
            );

            for curidx in last.range_to(idx.succ()) {
                let ent = self.parse_entry(off)?;

                self.idxoff.insert(curidx, off);
                off += self.entry_size(Some(&ent));
            }

            self.parse_entry(self.offset_for_idx(idx).expect("missing from cache?"))?
        };

        // Non-generaldelta literals are stored with baserev == self.
        // Generaldelta keeps that index: it is a snapshot, not a delta.
        if !self.is_general_delta() && entry.baserev == Some(idx) {
            entry.baserev = None;
        }

        // Update NodeHash cache
        if self.nodemap.as_ref().map_or(true, |nm| idx.as_u32() > nm.tip_rev) {
            self.nodeidx.insert(entry.nodeid, idx);
        }

        Ok(entry)
    }

    /// Return the ordinal index of an entry with the given nodeid.
    fn get_idx_by_nodeid(&mut self, nodeid: &NodeHash) -> Result<RevIdx> {
        let idx = self.nodeidx.get(nodeid).cloned();

        match idx {
            Some(idx) => Ok(idx),   // cache hit

            None => {
                // Once every revision is in the map, a miss is not in this
                // revlog. Without this, each unknown node scans the whole index.
                if self.nodeidx.len() == self.revision_count()? as usize {
                    return Err(ErrorKind::Revlog(format!("nodeid {} not found", nodeid)).into());
                }
                if let Some(nm) = &self.nodemap {
                    let (tip_rev, candidate) = (nm.tip_rev, nm.candidate(nodeid));
                    let count = self.revision_count()?;
                    if let Some(rev) = candidate.filter(|&rev| rev < count) {
                        if self.get_entry(RevIdx::from(rev))?.nodeid == *nodeid {
                            return Ok(RevIdx::from(rev));
                        }
                    }
                    // Revisions added after the nodemap was written are not in it.
                    for rev in tip_rev + 1..count {
                        if self.get_entry(RevIdx::from(rev))?.nodeid == *nodeid {
                            return Ok(RevIdx::from(rev));
                        }
                    }
                    return Err(ErrorKind::Revlog(format!("nodeid {} not found", nodeid)).into());
                }
                // search the hard way
                let mut posidx = None;
                let res = self.into_iter().position(|(i, entry)| {
                    posidx = Some(i);
                    &entry.nodeid == nodeid
                });

                match res {
                    None => Err(
                        ErrorKind::Revlog(format!("nodeid {} not found", nodeid)).into(),
                    ),
                    Some(idx) => {
                        let idx = RevIdx::from(idx);
                        assert_eq!(posidx, Some(idx));
                        self.nodeidx.insert(*nodeid, idx);
                        Ok(idx)
                    }
                }
            }
        }
    }

    fn attach_nodemap(&mut self) -> Result<bool> {
        self.want_nodemap = true;
        self.nodemap = None;
        let Some(idx_path) = self.idx_path.clone() else {
            return Ok(false);
        };
        if self.header.features.contains(parser::INLINE) {
            return Ok(false);
        }
        let Some((docket, trie)) = nodemap::load(&idx_path)? else {
            return Ok(false);
        };
        let valid = docket.tip_rev < self.revision_count()? as u64
            && trie.as_slice().len() as u64 >= docket.data_length
            && NodeHash::from_bytes(&docket.tip_node)
                .ok()
                .map_or(false, |tip| {
                    self.get_entry(RevIdx::from(docket.tip_rev as u32))
                        .map_or(false, |entry| entry.nodeid == tip)
                });
        if valid {
            self.nodemap = Some(nodemap::PersistentNodeMap {
                trie,
                data_length: docket.data_length as usize,
                tip_rev: docket.tip_rev as u32,
            });
        }
        Ok(valid)
    }

    fn get_entry_by_nodeid(&mut self, nodeid: &NodeHash) -> Result<Entry> {
        self.get_idx_by_nodeid(nodeid)
            .and_then(|idx| self.get_entry(idx))
    }

    /// Return a `Chunk` for a revision at `RevIdx`.
    ///
    /// A `Chunk` either represents the literal
    /// text of the change, or a series of deltas against a previous version; the exact
    /// mechanism of applying the deltas depends on whether the `RevLog` has the `GENERAL_DELTA`
    /// flag set or not.
    fn get_chunk(&mut self, idx: RevIdx) -> Result<Chunk> {
        if !self.have_data() {
            return Err("Can't get chunks without data".into());
        }

        let entry = self.get_entry(idx)?;

        let (chunkdata, start) = if self.header.features.contains(parser::INLINE) {
            let off = self.offset_for_idx(idx).expect("not cached?");
            let start = off + self.fixed_entry_size();

            (self.idx.as_slice(), start)
        } else {
            let start = entry.offset as usize;

            (
                self.data
                    .as_ref()
                    .expect("non-inline has no data")
                    .as_slice(),
                start,
            )
        };
        let end = start + (entry.compressed_len as usize);
        let chunkdata = &chunkdata[start..end];
        //println!("{:?}: {:?} chunk {}-{}", idx, entry, start, end);

        // If the entry has no baserev then the chunk is literal data, Otherwise
        // its 0 or more deltas against the baserev. If its general delta, then the
        // baserev itself might also be delta, otherwise its all the deltas from baserev..idx.
        // A generaldelta snapshot uses baserev == idx and stores full text.
        let snapshot = self.is_general_delta() && entry.baserev == Some(idx);
        if let Some(baserev) = entry.baserev {
            if !snapshot {
                let delta = match parser::deltachunk(chunkdata) {
                    Ok((rest, _)) if rest.len() != 0 => {
                        return Err(
                            ErrorKind::Revlog(format!(
                                "Failed to unpack details: {} remains, {:?}",
                                rest.len(),
                                &rest[..rest.len().min(16)]
                            )).into(),
                        );
                    }
                    Ok((_, deltas)) => Chunk::Deltas(baserev, deltas),
                    Err(err) => {
                        return Err(
                            ErrorKind::Revlog(format!("Failed to unpack deltas: {:?}", err)).into(),
                        )
                    }
                };
                return Ok(delta);
            }
        }
        if chunkdata.len() == 0 {
            Ok(Chunk::Literal(vec![]))
        } else {
            let literal = match parser::literal(chunkdata) {
                Ok((rest, _)) if rest.len() != 0 => {
                    return Err(
                        ErrorKind::Revlog(format!(
                            "Failed to unpack literal: {} remains, {:?}",
                            rest.len(),
                            &rest[..rest.len().min(16)]
                        )).into(),
                    );
                }
                Ok((_, literal)) => Chunk::Literal(literal),
                Err(err) => {
                    return Err(
                        ErrorKind::Revlog(format!("Failed to unpack literal: {:?}", err)).into(),
                    )
                }
            };
            Ok(literal)
        }
    }

    fn is_general_delta(&self) -> bool {
        self.header.features.contains(parser::GENERAL_DELTA)
    }

    fn construct_simple(&mut self, tgtidx: RevIdx) -> Result<Vec<u8>> {
        assert!(!self.is_general_delta());

        let entry = self.get_entry(tgtidx)?;

        // if there's no baserev, then use the target as a baserev (it should be literal)
        let baserev = entry.baserev.map(Into::into).unwrap_or(tgtidx);

        // XXX: Fix this to use delta::Delta instead of bdiff::Delta.

        // non-general delta - baserev should be literal, then we applying
        // each delta up to idx
        let mut data = Vec::new();
        let mut chain = Vec::new();
        for idx in baserev.range_to(tgtidx.succ()) {
            let chunk = self.get_chunk(idx).chain_err::<_, Error>(|| {
                format!("simple tgtidx {:?} idx {:?}", tgtidx, idx).into()
            });

            match chunk? {
                Chunk::Literal(v) => {
                    data = v;
                    chain.clear();
                }
                Chunk::Deltas(_, deltas) => {
                    chain.push(deltas);
                }
            }
        }

        delta::compat::apply_deltas(data.as_ref(), chain);

        Ok(data)
    }

    fn construct_general(&mut self, tgtidx: RevIdx) -> Result<Vec<u8>> {
        assert!(self.is_general_delta());

        let entry = self.get_entry(tgtidx)?;
        // Reuse the slot only for a delta against that revision. A snapshot
        // (`baserev == idx`) and a forward base do not.
        if let Some(baserev) = entry.baserev {
            if baserev < tgtidx {
                let hit = match &self.text_cache {
                    Some((cached_idx, _)) => *cached_idx == baserev,
                    None => false,
                };
                if hit {
                    let chunk = self.get_chunk(tgtidx).chain_err::<_, Error>(|| {
                        format!("construct_general tgtidx {:?} idx {:?}", tgtidx, tgtidx).into()
                    })?;
                    let deltas = match chunk {
                        Chunk::Deltas(_, deltas) => deltas,
                        _ => panic!("Literal text found in delta chain."),
                    };
                    let (_, cached) = self
                        .text_cache
                        .take()
                        .expect("text cache missing after hit");
                    let data =
                        delta::compat::apply_deltas(cached.as_ref(), std::iter::once(deltas));
                    self.text_cache = Some((tgtidx, data.clone()));
                    return Ok(data);
                }
            }
        }

        let mut chunks = Vec::new();
        let mut idx = tgtidx;

        // general delta - walk backwards until we hit a literal, collecting chunks on the way
        let mut data;
        loop {
            chunks.push(idx);

            let entry = self.get_entry(idx)?;
            if let Some(baserev) = entry.baserev.map(Into::into) {
                if baserev > idx {
                    return Err(ErrorKind::Revlog(format!(
                        "baserev {:?} > idx {:?}",
                        baserev, idx
                    ))
                    .into());
                }
                if baserev == idx {
                    // Snapshot: this chunk is the full text, not a delta.
                    let idx = chunks.pop().unwrap();
                    let chunk = self.get_chunk(idx).chain_err::<_, Error>(|| {
                        format!("construct_general tgtidx {:?} idx {:?}", tgtidx, idx).into()
                    })?;
                    match chunk {
                        Chunk::Literal(v) => data = v,
                        _ => panic!("snapshot chunk is not literal"),
                    }
                    break;
                }
                idx = baserev;
            } else {
                let idx = chunks.pop().unwrap();
                let chunk = self.get_chunk(idx)
                    .chain_err::<_, Error>(|| {
                        format!("construct_general tgtidx {:?} idx {:?}", tgtidx, idx).into()
                    })?;
                match chunk {
                    Chunk::Literal(v) => data = v,
                    _ => panic!("Non-literal chunk with no baserev."),
                }
                break;
            }
        }

        // XXX: Fix this to use delta::Delta instead of bdiff::Delta.
        let chain = chunks.into_iter().rev().map(|idx| {
            let chunk = self.get_chunk(idx);

            match chunk {
                Ok(Chunk::Deltas(_, deltas)) => deltas,
                _ => panic!("Literal text found in delta chain."),
            }
        });

        data = delta::compat::apply_deltas(data.as_ref(), chain);

        self.text_cache = Some((tgtidx, data.clone()));
        Ok(data)
    }

    fn make_node<T>(&mut self, entry: &Entry, blob: Blob<T>) -> Result<BlobNode<T>>
    where
        T: AsRef<[u8]>,
    {
        let mut pnodeid = |p| {
            let pn = self.get_entry(p);
            pn.map(|n| n.nodeid)
        };
        let p1 = map_io(entry.p1, &mut pnodeid)?;
        let p2 = map_io(entry.p2, &mut pnodeid)?;

        Ok(BlobNode::new(blob, p1.as_ref(), p2.as_ref()))
    }

    fn get_rev(&mut self, tgtidx: RevIdx) -> Result<BlobNode> {
        if !self.have_data() {
            return Err("Need data to assemble revision".into());
        }

        let entry = self.get_entry(tgtidx)?;

        let data = if self.is_general_delta() {
            self.construct_general(tgtidx)?
        } else {
            self.construct_simple(tgtidx)?
        };

        self.make_node(&entry, Blob::from(data))
    }

    fn get_rev_by_nodeid(&mut self, id: &NodeHash) -> Result<BlobNode> {
        self.get_idx_by_nodeid(id).and_then(|idx| {
            self.get_rev(idx)
                .chain_err::<_, Error>(|| format!("can't get rev for id {}", id).into())
        })
    }

    fn get_node_by_nodeid(&mut self, id: &NodeHash, with_data: bool) -> Result<BlobNode> {
        if with_data {
            self.get_idx_by_nodeid(id).and_then(|idx| self.get_rev(idx))
        } else {
            let entry = self.get_entry_by_nodeid(id)?;
            let blob = Blob::from(entry.nodeid);

            self.make_node(&entry, blob)
        }
    }

    /// Return the set of head revisions in a revlog
    fn get_heads(&mut self) -> Result<HashSet<NodeHash>> {
        // Current set of candidate heads
        let mut heads = HashMap::new();

        for (idx, entry) in self.into_iter() {
            // New entry could be a head
            heads.insert(idx, entry);

            // This entry's parent(s) are non-heads
            if let Some(p1) = entry.p1 {
                let _ = heads.remove(&p1);
            }

            if let Some(p2) = entry.p2 {
                let _ = heads.remove(&p2);
            }
        }

        // Mercurial reports the null node as the head of an empty revlog.
        if heads.is_empty() {
            return Ok(HashSet::from([mercurial_types::NULL_HASH]));
        }
        // Convert to a set of nodeids
        Ok(heads.values().map(|n| n.nodeid).collect())
    }

    fn append_revision(
        &mut self,
        rev: RevlogAppend,
        delta: Option<(NodeHash, delta::Delta)>,
        compression: RevlogCompression,
        flags: IdxFlags,
    ) -> Result<u32> {
        if self.header.version != Version::RevlogNG {
            return Err(ErrorKind::Revlog("append_snapshot requires revlog v1".into()).into());
        }
        if rev.text.len() > u32::MAX as usize {
            return Err(ErrorKind::Revlog("revision text exceeds u32 length".into()).into());
        }
        let snapshot = compress_snapshot(&rev.text, compression)?;
        let stored = match delta {
            Some((base, delta)) => {
                self.delta_to_store(&base, &delta, rev.text.len(), snapshot.len(), compression)?
            }
            None => None,
        };
        let (baserev, compressed) = match stored {
            Some((base_idx, bytes)) => (Some(base_idx), bytes),
            None => (None, snapshot),
        };
        if compressed.len() > u32::MAX as usize {
            return Err(ErrorKind::Revlog("compressed revision exceeds u32 length".into()).into());
        }

        let p1 = self.parent_rev(rev.p1)?;
        let p2 = self.parent_rev(rev.p2)?;
        let linkrev = rev.linkrev.unwrap_or(u32::MAX);
        // Disk rename can fail after this snapshot. Roll the bytes back so a retry
        // does not append a second revision.
        let saved = self.snapshot();
        let appended = (|| -> Result<u32> {
            let rev_idx = self.revision_count()?;
            if self.header.features.contains(parser::INLINE) {
                let new_size =
                    self.idx.as_slice().len() + self.fixed_entry_size() + compressed.len();
                if new_size > INLINE_LIMIT {
                    self.split_inline()?;
                }
            } else if self.data.is_none() && rev_idx > 0 {
                return Err("Can't append without data".into());
            }

            self.write_revision(
                rev_idx,
                baserev.unwrap_or(rev_idx),
                &rev.node,
                p1,
                p2,
                linkrev,
                rev.text.len() as u32,
                &compressed,
                flags,
            )?;
            self.persist()?;
            Ok(rev_idx)
        })();
        if appended.is_err() {
            self.restore(saved);
        }
        appended
    }

    /// The base index and compressed bytes to store `delta` with, or `None` when
    /// a snapshot is better: the revlog is not generaldelta, the base is not in
    /// it, the delta does not compress smaller than the snapshot, or the chain
    /// would pass `MAX_DELTA_CHAIN` deltas or `DELTA_CHAIN_TO_TEXT` times the text.
    fn delta_to_store(
        &mut self,
        base: &NodeHash,
        delta: &delta::Delta,
        text_len: usize,
        snapshot_len: usize,
        compression: RevlogCompression,
    ) -> Result<Option<(u32, Vec<u8>)>> {
        if !self.header.features.contains(parser::GENERAL_DELTA) {
            return Ok(None);
        }
        let Ok(base_idx) = self.get_idx_by_nodeid(base) else {
            return Ok(None);
        };
        let compressed = compress_snapshot(&encode_delta(delta), compression)?;
        if compressed.len() >= snapshot_len {
            return Ok(None);
        }
        // Walk from the base down to its snapshot, counting the new delta too.
        let mut chain_len = 1;
        let mut chain_bytes = compressed.len();
        let mut idx = base_idx;
        loop {
            let entry = self.get_entry(idx)?;
            chain_bytes += entry.compressed_len as usize;
            match entry.baserev {
                Some(next) if next != idx => {
                    idx = next;
                    chain_len += 1;
                    if chain_len > MAX_DELTA_CHAIN {
                        return Ok(None);
                    }
                }
                _ => break,
            }
        }
        if chain_bytes > DELTA_CHAIN_TO_TEXT * text_len {
            return Ok(None);
        }
        Ok(Some((base_idx.as_u32(), compressed)))
    }

    /// `None` is a null parent (`u32::MAX`). `Some` must already be in this revlog.
    fn parent_rev(&mut self, parent: Option<NodeHash>) -> Result<u32> {
        match parent {
            None => Ok(u32::MAX),
            Some(node) => self.get_idx_by_nodeid(&node).map(|idx| idx.as_u32()),
        }
    }

    fn revision_count(&self) -> Result<u32> {
        let bytes_len = self.idx.as_slice().len();
        if bytes_len == 0 {
            return Ok(0);
        }
        if self.header.features.contains(parser::INLINE) {
            let mut off = 0usize;
            let mut count = 0u32;
            let fixed = self.fixed_entry_size();
            while off < bytes_len {
                let ent = self.parse_entry(off)?;
                let next = off + fixed + ent.compressed_len as usize;
                if next > bytes_len {
                    return Err(ErrorKind::Revlog(format!(
                        "inline chunk for rev {} exceeds index",
                        count
                    ))
                    .into());
                }
                off = next;
                count += 1;
            }
            Ok(count)
        } else {
            let size = self.fixed_entry_size();
            if size == 0 || bytes_len % size != 0 {
                return Err(ErrorKind::Revlog(format!(
                    "index length {} is not a multiple of {}",
                    bytes_len, size
                ))
                .into());
            }
            Ok((bytes_len / size) as u32)
        }
    }

    fn split_inline(&mut self) -> Result<()> {
        if !self.header.features.contains(parser::INLINE) {
            return Ok(());
        }
        let fixed = self.fixed_entry_size();
        let bytes = self.idx.as_slice().to_vec();
        let mut data = Vec::new();
        let mut index = Vec::new();
        let mut off = 0usize;
        let mut rev = 0u32;
        while off < bytes.len() {
            let (_, ent) = match parser::indexng(&bytes[off..]) {
                Ok(parsed) => parsed,
                Err(err) => {
                    return Err(ErrorKind::Revlog(format!(
                        "failed to parse entry offset {}: {}",
                        off, err
                    ))
                    .into())
                }
            };
            let start = off + fixed;
            let end = start + ent.compressed_len as usize;
            if end > bytes.len() {
                return Err(ErrorKind::Revlog(format!(
                    "inline chunk for rev {} exceeds index",
                    rev
                ))
                .into());
            }
            let data_off = data.len() as u64;
            let packed = pack_indexng(
                data_off,
                ent.flags.bits(),
                ent.compressed_len,
                ent.len.unwrap_or(0),
                opt_rev(ent.baserev),
                opt_rev(ent.linkrev),
                opt_rev(ent.p1),
                opt_rev(ent.p2),
                ent.nodeid.as_ref(),
            );
            data.extend_from_slice(&bytes[start..end]);
            index.extend_from_slice(&packed);
            off = end;
            rev += 1;
        }

        self.header.features.remove(parser::INLINE);
        if !index.is_empty() {
            overlay_header(&mut index, self.header.features, self.header.version);
        }
        self.idx = Datafile::Loaded(index);
        self.data = Some(Datafile::Loaded(data));
        self.idxoff.clear();
        if rev > 0 {
            self.idxoff.insert(RevIdx::zero(), 0);
        }
        if self.data_path.is_none() {
            if let Some(path) = self.idx_path.clone() {
                self.data_path = Some(path.with_extension("d"));
            }
        }
        Ok(())
    }

    /// Write one index entry and its stored bytes. `baserev` is `rev_idx` for a snapshot.
    fn write_revision(
        &mut self,
        rev_idx: u32,
        baserev: u32,
        node: &NodeHash,
        p1: u32,
        p2: u32,
        linkrev: u32,
        uncompressed_len: u32,
        compressed: &[u8],
        flags: IdxFlags,
    ) -> Result<()> {
        let inline = self.header.features.contains(parser::INLINE);
        let fixed = self.fixed_entry_size();
        let entry_off = if inline {
            self.idx.as_slice().len()
        } else {
            rev_idx as usize * fixed
        };
        let data_off = if inline {
            // Mercurial stores the offset into the logical data stream, the
            // same value a `.d` file would use. Its reader adds
            // `(rev + 1) * entry_size` to reach the bytes after this entry.
            let logical = entry_off.checked_sub(rev_idx as usize * fixed).ok_or_else(|| {
                ErrorKind::Revlog(format!(
                    "inline offset {} is shorter than {} entries",
                    entry_off, rev_idx
                ))
            })?;
            logical as u64
        } else {
            self.data.as_ref().map(|d| d.as_slice().len()).unwrap_or(0) as u64
        };
        let mut packed = pack_indexng(
            data_off,
            flags.bits(),
            compressed.len() as u32,
            uncompressed_len,
            baserev,
            linkrev,
            p1,
            p2,
            node.as_ref(),
        );
        if rev_idx == 0 {
            overlay_header(&mut packed, self.header.features, self.header.version);
        }

        self.idx_vec().extend_from_slice(&packed);
        if inline {
            self.idx_vec().extend_from_slice(compressed);
        } else {
            if self.data.is_none() {
                self.data = Some(Datafile::Loaded(Vec::new()));
            }
            self.data_vec()?.extend_from_slice(compressed);
        }
        // A hole above the cached prefix makes `get_entry` assert. Leave the
        // map unchanged so the walker can still start at the previous high-water mark.
        self.note_inline_offset(rev_idx, entry_off);
        self.nodeidx.insert(*node, RevIdx::from(rev_idx as usize));
        Ok(())
    }

    fn note_inline_offset(&mut self, rev_idx: u32, entry_off: usize) {
        if !self.header.features.contains(parser::INLINE) {
            return;
        }
        let Some(&high) = self.idxoff.keys().next_back() else {
            return;
        };
        if high.as_u32().checked_add(1) == Some(rev_idx) {
            self.idxoff
                .insert(RevIdx::from(rev_idx as usize), entry_off);
        }
    }

    fn snapshot(&self) -> RevlogSnapshot {
        RevlogSnapshot {
            header: self.header,
            idx: self.idx.as_slice().to_vec(),
            data: self.data.as_ref().map(|data| data.as_slice().to_vec()),
            idxoff: self.idxoff.clone(),
            nodeidx: self.nodeidx.clone(),
            data_path: self.data_path.clone(),
        }
    }

    fn restore(&mut self, saved: RevlogSnapshot) {
        self.header = saved.header;
        self.idx = Datafile::Loaded(saved.idx);
        self.data = saved.data.map(Datafile::Loaded);
        self.idxoff = saved.idxoff;
        self.nodeidx = saved.nodeidx;
        self.data_path = saved.data_path;
    }

    fn idx_vec(&mut self) -> &mut Vec<u8> {
        if matches!(&self.idx, Datafile::Mmap(_)) {
            let owned = self.idx.as_slice().to_vec();
            self.idx = Datafile::Loaded(owned);
        }
        match &mut self.idx {
            Datafile::Loaded(data) => data,
            Datafile::Mmap(_) => unreachable!("mmap copied to a vec"),
        }
    }

    fn data_vec(&mut self) -> Result<&mut Vec<u8>> {
        if let Some(Datafile::Mmap(_)) = &self.data {
            let owned = self
                .data
                .as_ref()
                .expect("data mmap")
                .as_slice()
                .to_vec();
            self.data = Some(Datafile::Loaded(owned));
        }
        match &mut self.data {
            Some(Datafile::Loaded(data)) => Ok(data),
            _ => Err("Can't append without data".into()),
        }
    }

    fn persist(&self) -> Result<()> {
        // Data file first. A replaced index must not point at a `.d` that is still old or empty.
        if let (Some(path), Some(data)) = (&self.data_path, &self.data) {
            replace_file(path, data.as_slice())?;
        }
        if let Some(path) = &self.idx_path {
            replace_file(path, self.idx.as_slice())?;
        }
        Ok(())
    }
}

struct RevlogSnapshot {
    header: Header,
    idx: Vec<u8>,
    data: Option<Vec<u8>>,
    idxoff: BTreeMap<RevIdx, usize>,
    nodeidx: HashMap<NodeHash, RevIdx>,
    data_path: Option<PathBuf>,
}

/// Write `bytes` to a same-directory temp file, then rename it over `path`.
///
/// `std::fs::write` truncates the destination first. A crash there leaves an empty
/// index, which reopens as an empty revlog.
pub(crate) fn replace_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let (tmp, mut file) = loop {
        let tmp = sibling_tmp(path);
        match std::fs::OpenOptions::new().write(true).create_new(true).open(&tmp) {
            Ok(file) => break (tmp, file),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err.into()),
        }
    };
    if let Err(err) = file.write_all(bytes) {
        drop(file);
        let _ = std::fs::remove_file(&tmp);
        return Err(err.into());
    }
    drop(file);
    if let Err(err) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(err.into());
    }
    Ok(())
}

fn sibling_tmp(path: &Path) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut tmp = path.to_path_buf();
    let name = match path.file_name() {
        Some(name) => name.to_string_lossy().into_owned(),
        None => "revlog".to_string(),
    };
    tmp.set_file_name(format!(".{name}.append-tmp-{}-{id}", std::process::id()));
    tmp
}

fn opt_rev(idx: Option<RevIdx>) -> u32 {
    idx.map(RevIdx::as_u32).unwrap_or(u32::MAX)
}

fn pack_indexng(
    offset: u64,
    flags: u16,
    compressed_len: u32,
    uncompressed_len: u32,
    baserev: u32,
    linkrev: u32,
    p1: u32,
    p2: u32,
    node: &[u8],
) -> [u8; 64] {
    let mut entry = [0u8; 64];
    entry[0] = ((offset >> 40) & 0xff) as u8;
    entry[1] = ((offset >> 32) & 0xff) as u8;
    entry[2] = ((offset >> 24) & 0xff) as u8;
    entry[3] = ((offset >> 16) & 0xff) as u8;
    entry[4] = ((offset >> 8) & 0xff) as u8;
    entry[5] = (offset & 0xff) as u8;
    entry[6..8].copy_from_slice(&flags.to_be_bytes());
    entry[8..12].copy_from_slice(&compressed_len.to_be_bytes());
    entry[12..16].copy_from_slice(&uncompressed_len.to_be_bytes());
    entry[16..20].copy_from_slice(&baserev.to_be_bytes());
    entry[20..24].copy_from_slice(&linkrev.to_be_bytes());
    entry[24..28].copy_from_slice(&p1.to_be_bytes());
    entry[28..32].copy_from_slice(&p2.to_be_bytes());
    entry[32..52].copy_from_slice(&node[..20]);
    entry
}

/// Rev 0 stores features and version in the high 4 bytes of its offset field.
fn overlay_header(entry: &mut [u8], features: parser::Features, version: Version) {
    entry[0..2].copy_from_slice(&features.bits().to_be_bytes());
    entry[2..4].copy_from_slice(&(version as u16).to_be_bytes());
}

/// The revlog encoding of `delta`: per fragment, the start, end and content
/// length as big-endian `u32`s, then the content.
fn encode_delta(delta: &delta::Delta) -> Vec<u8> {
    let mut out = Vec::new();
    for frag in delta.fragments() {
        out.extend_from_slice(&(frag.start as u32).to_be_bytes());
        out.extend_from_slice(&(frag.end as u32).to_be_bytes());
        out.extend_from_slice(&(frag.content.len() as u32).to_be_bytes());
        out.extend_from_slice(&frag.content);
    }
    out
}

fn compress_snapshot(text: &[u8], compression: RevlogCompression) -> Result<Vec<u8>> {
    match compression {
        RevlogCompression::Zstd => Ok(zstd::encode_all(text, 0)?),
        RevlogCompression::Zlib => {
            let mut enc =
                flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
            enc.write_all(text)?;
            Ok(enc.finish()?)
        }
    }
}

/// Data associated with a revision.
///
/// XXX internal detail?
#[derive(Debug)]
pub enum Chunk {
    /// Literal text of the revision
    Literal(Vec<u8>),
    /// Vector of `Delta`s against a previous version
    Deltas(RevIdx, Vec<Delta>),
}

struct RevlogInnerIter<'a>(&'a mut RevlogInner, RevIdx);

impl<'a> IntoIterator for &'a mut RevlogInner {
    type Item = <Self::IntoIter as Iterator>::Item;
    type IntoIter = RevlogInnerIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        RevlogInnerIter(self, RevIdx::zero())
    }
}

impl<'a> Iterator for RevlogInnerIter<'a> {
    type Item = (RevIdx, Entry);

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.1;
        let ret = self.0.get_entry(idx).ok();
        self.1 = idx.succ();
        ret.map(|r| (idx, r))
    }
}

#[derive(Debug)]
pub struct RevlogIter(Arc<Mutex<RevlogInner>>, RevIdx);

impl IntoIterator for Revlog {
    type Item = <Self::IntoIter as Iterator>::Item;
    type IntoIter = RevlogIter;

    fn into_iter(self) -> Self::IntoIter {
        RevlogIter(self.inner.clone(), RevIdx::zero())
    }
}

impl<'a> IntoIterator for &'a Revlog {
    type Item = <Self::IntoIter as Iterator>::Item;
    type IntoIter = RevlogIter;

    fn into_iter(self) -> Self::IntoIter {
        RevlogIter(self.inner.clone(), RevIdx::zero())
    }
}

impl RevlogIter {
    pub fn seek(&mut self, idx: RevIdx) {
        self.1 = idx;
    }
}

impl Iterator for RevlogIter {
    type Item = (RevIdx, Entry);

    fn next(&mut self) -> Option<Self::Item> {
        let mut revlog = self.0.lock().expect("locked poisoned");

        let idx = self.1;
        let ret = revlog.get_entry(idx).ok();
        self.1 = idx.succ();
        ret.map(|r| (idx, r))
    }
}
