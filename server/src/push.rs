// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::pin::Pin;
use std::task::{Context, Poll};

use async_compression::{CompressorType, Decompressor, DecompressorType};
use bookmarks::{Bookmarks, BookmarksMut, Version};
use bytes::{Bytes, BytesMut};
use futures::{FutureExt, Stream};
use mercurial::revlog::{IdxFlags, Revlog, RevlogAppend, RevlogCompression};
use mercurial::revlogrepo::Required;
use mercurial::RevlogRepo;
use mercurial_bundles::bundle2::Bundle2Stream;
use mercurial_bundles::changegroup::{
    unpacker::Cg2Unpacker, CgDeltaChunk, CgVersion, Part, Section,
};
use mercurial_bundles::part_encode::PartEncodeBuilder;
use mercurial_bundles::{Bundle2Item, InnerPart, PartHeader};
use mercurial_types::delta::{self, Delta};
use mercurial_types::{NodeHash, Path as MPath, NULL_HASH};
use tokio_util::codec::Decoder;

use crate::errors::{Error, Result};
use crate::lfs::pointer::Pointer;
use crate::lfs::store::LfsStore;

/// Hg pointers are about 130 bytes; a longer EXTSTORED revision is not a pointer.
const MAX_POINTER_LEN: usize = 4096;

pub(crate) struct PushPolicy<'a> {
    pub lfs: Option<&'a LfsStore>,
    /// `0` disables the limit.
    pub max_plain_file_size: u64,
}

pub(crate) struct Applied {
    pub(crate) body: Vec<u8>,
    pub(crate) changegroup_ids: Vec<u32>,
    pub(crate) push_raced: bool,
    bundle1: bool,
}

pub fn apply_unbundle(repo_path: &std::path::Path, heads: &[NodeHash], bundle: &[u8]) -> Result<Vec<u8>> {
    let repo = RevlogRepo::open(repo_path)?;
    let policy = PushPolicy { lfs: None, max_plain_file_size: 0 };
    Ok(apply_repo(&repo, heads, bundle, &policy)?.body)
}

pub(crate) fn heads_match(repo: &RevlogRepo, heads: &[NodeHash]) -> Result<bool> {
    if hgproto::sshproto::request::unbundle_heads_are_force(heads) {
        return Ok(true);
    }
    let mut changelog = repo.changelog_revlog();
    let current = changelog.get_heads()?;
    let offered: std::collections::HashSet<NodeHash> = heads.iter().copied().collect();
    Ok(current == offered)
}

pub(crate) fn apply_repo(
    repo: &RevlogRepo,
    heads: &[NodeHash],
    bundle: &[u8],
    policy: &PushPolicy,
) -> Result<Applied> {
    match plan_push(repo, heads, bundle, policy)? {
        Planned::Done(applied) => Ok(applied),
        Planned::Write(plan) => write_push(repo, plan),
    }
}

pub(crate) enum Planned {
    /// Nothing to write: the push lost a race.
    Done(Applied),
    Write(PushPlan),
}

/// A bundle that passed every check.
pub(crate) struct PushPlan {
    groups: Vec<(Section, Vec<CgDeltaChunk>)>,
    pushkeys: Vec<PartHeader>,
    changegroup_ids: Vec<u32>,
    tree: bool,
    adds_lfs: bool,
    bundle1: bool,
}

/// Parse and validate the entire input, writing nothing. With `policy.lfs` set
/// this must run on the blocking pool, which is where `RepoClient::unbundle` puts it.
pub(crate) fn plan_push(
    repo: &RevlogRepo,
    heads: &[NodeHash],
    bundle: &[u8],
    policy: &PushPolicy,
) -> Result<Planned> {
    let bundle1 = is_bundle1(bundle);
    if !heads_match(repo, heads)? {
        return Ok(Planned::Done(Applied {
            body: b"0\n".to_vec(),
            changegroup_ids: Vec::new(),
            push_raced: false,
            bundle1,
        }));
    }
    if !bundle1 && !check_heads_agree(repo, bundle)? {
        return Ok(Planned::Done(Applied {
            body: Vec::new(),
            changegroup_ids: Vec::new(),
            push_raced: true,
            bundle1,
        }));
    }

    let mut groups: Vec<(Section, Vec<CgDeltaChunk>)> = Vec::new();
    let mut changegroup_ids = Vec::new();
    let mut cg_part = false;
    let mut tree = false;
    let mut pushkeys = Vec::new();

    if bundle1 {
        drain_bundle1(bundle, |part| {
            if let Part::CgChunk(section, chunk) = part {
                push_chunk(&mut groups, section, chunk);
            }
        })?;
    } else {
        drain_bundle(bundle, |item| {
            match item {
                Bundle2Item::Start(_) => {}
                Bundle2Item::Header(header) => {
                    let kind = header.part_type_lower().as_str();
                    cg_part = kind == "changegroup";
                    if cg_part {
                        changegroup_ids.push(header.part_id());
                        if param(&header, "treemanifest").is_ok() {
                            tree = true;
                        }
                    } else if kind == "pushkey" {
                        pushkeys.push(header);
                    }
                }
                Bundle2Item::Inner(inner) if cg_part => match inner.cg2_part() {
                    Part::CgChunk(section, chunk) => push_chunk(&mut groups, section, chunk),
                    Part::SectionEnd(_) | Part::End => {}
                },
                Bundle2Item::Inner(_) => {}
            }
            Ok(())
        })?;
    }

    // Validate the entire input before requirements, bookmarks or history change.
    let adds_lfs = check_groups(repo, &groups, policy)?;
    Ok(Planned::Write(PushPlan { groups, pushkeys, changegroup_ids, tree, adds_lfs, bundle1 }))
}

fn unsupported_flags(chunk: &CgDeltaChunk) -> Error {
    format!("unsupported revision flags {:#06x} for {}", chunk.flags, chunk.node).into()
}

/// Result length of `delta` on a base of `base_len` bytes. The range check is
/// what makes `delta::apply` safe on client deltas; `Delta::new` already keeps
/// fragments sorted and apart, so the removed lengths cannot exceed `base_len`.
fn delta_result_len(base_len: usize, delta: &Delta) -> Result<usize> {
    let mut len = base_len;
    for frag in delta.fragments() {
        if frag.start > frag.end || frag.end > base_len {
            return Err("changegroup delta is outside its base".into());
        }
        len = (len - (frag.end - frag.start)) + frag.content.len();
    }
    Ok(len)
}

/// Check every revision the push would add. Returns whether any is EXTSTORED.
fn check_groups(
    repo: &RevlogRepo,
    groups: &[(Section, Vec<CgDeltaChunk>)],
    policy: &PushPolicy,
) -> Result<bool> {
    let dotencode = repo.get_requirements().contains(&Required::Dotencode);
    // By path as well as node: a bundle can repeat a file's section.
    let mut lens: HashMap<(&MPath, NodeHash), usize> = HashMap::new();
    let mut pointers: HashMap<(&MPath, NodeHash), Vec<u8>> = HashMap::new();
    let mut adds_lfs = false;
    for (section, chunks) in groups {
        let Section::Filelog(path) = section else {
            if let Some(chunk) = chunks.iter().find(|chunk| chunk.flags != 0) {
                return Err(unsupported_flags(chunk));
            }
            continue;
        };
        let name = String::from_utf8_lossy(path.as_slice());
        let exists = repo.store_path().join(path.store_filelog(dotencode, b"i")).exists();
        let stored = if exists { Some(repo.read_file_revlog(path)?) } else { None };
        for chunk in chunks {
            if chunk.flags & !IdxFlags::EXTSTORED.bits() != 0 {
                return Err(unsupported_flags(chunk));
            }
            // `append_group` skips revisions the repository has. One flagged here
            // still counts: a crash can leave it there without the requirement.
            if let Some(entry) = stored.as_ref().and_then(|log| log.get_entry_by_nodeid(&chunk.node).ok()) {
                adds_lfs |= entry.flags.contains(IdxFlags::EXTSTORED);
                continue;
            }
            let base = (path, chunk.base);
            let base_len = if chunk.base == NULL_HASH {
                0
            } else if let Some(len) = lens.get(&base) {
                *len
            } else {
                // Raw stored length, a pointer's own for an EXTSTORED base.
                let entry = stored.as_ref().and_then(|log| log.get_entry_by_nodeid(&chunk.base).ok());
                let len = entry.and_then(|entry| entry.len);
                len.ok_or_else(|| Error::from(format!("{name}: delta base {} is not known", chunk.base)))? as usize
            };
            let len = delta_result_len(base_len, &chunk.delta)?;
            // Appends can reorder chunks, so a repeat could differ from what was measured.
            if lens.insert((path, chunk.node), len).is_some() {
                return Err(format!("{name}: duplicate revision {}", chunk.node).into());
            }
            if chunk.flags != 0 {
                let Some(store) = policy.lfs else {
                    return Err("this repository does not serve LFS".into());
                };
                let malformed = || Error::from(format!("{name}: malformed LFS pointer"));
                if len > MAX_POINTER_LEN {
                    return Err(malformed());
                }
                let text = if chunk.base == NULL_HASH {
                    delta::apply(&[], chunk.delta.clone())
                } else if let Some(text) = pointers.get(&base) {
                    delta::apply(text, chunk.delta.clone())
                } else if let (false, Some(log)) = (lens.contains_key(&base), &stored) {
                    full_text(log, chunk.base, chunk.delta.clone())?
                } else {
                    // Hg never deltas across the flag, and plain texts of this push are not kept.
                    return Err(malformed());
                };
                let pointer = Pointer::parse(&text).map_err(|_| malformed())?;
                // `size` has logged the detail, which clients must not see.
                let size = tokio::runtime::Handle::current()
                    .block_on(store.size(&pointer.oid))
                    .map_err(|_| Error::from("LFS storage error"))?;
                if size != Some(pointer.size) {
                    return Err(format!(
                        "{name}: LFS blob {} is not on the server; push again so hg uploads it",
                        pointer.oid
                    )
                    .into());
                }
                pointers.insert((path, chunk.node), text);
                adds_lfs = true;
            } else if policy.max_plain_file_size > 0 && len as u64 > policy.max_plain_file_size {
                return Err(format!(
                    "{name} is {} MB, over the server's {} MB limit for files not stored in LFS; track it in .hglfs",
                    len as u64 / 1_000_000,
                    policy.max_plain_file_size / 1_000_000
                )
                .into());
            }
        }
    }
    Ok(adds_lfs)
}

pub(crate) fn write_push(repo: &RevlogRepo, plan: PushPlan) -> Result<Applied> {
    let PushPlan { groups, pushkeys, changegroup_ids, tree, adds_lfs, bundle1 } = plan;
    let compression = compression_of(repo);
    let mut transaction = PushTransaction::new(repo)?;
    let applied = (|| -> Result<()> {
        if tree { use_treemanifest(repo)?; }
        for (section, chunks) in groups {
            transaction.watch_group(repo, &section)?;
            append_group(repo, compression, &section, chunks)?;
        }
        for header in pushkeys { apply_pushkey(repo, &header)?; }
        // Last, so no step can fail with it set; a rollback restores `requires` anyway.
        if adds_lfs { repo.add_requirement(Required::Lfs)?; }
        Ok(())
    })();
    if let Err(err) = applied {
        transaction.rollback(repo).map_err(|rollback| Error::from(format!("{err}; rollback failed: {rollback}")))?;
        return Err(err);
    }

    Ok(Applied {
        body: b"1\n".to_vec(),
        changegroup_ids,
        push_raced: false,
        bundle1,
    })
}

/// Revlog writes replace files; hard links retain their old bytes without copying
/// large histories. Small metadata files are copied because their writers may
/// truncate in place. Only paths this push can change are recorded.
struct PushTransaction {
    backup: std::path::PathBuf,
    files: Vec<(std::path::PathBuf, Option<std::path::PathBuf>)>,
    watched: std::collections::HashSet<std::path::PathBuf>,
    dirs: std::collections::BTreeSet<std::path::PathBuf>,
    keep_backup: bool,
}

impl PushTransaction {
    fn new(repo: &RevlogRepo) -> Result<Self> {
        let store = repo.store_path();
        let base = store.parent().expect("hg directory");
        let nonce = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
            .map_err(|err| Error::from(err.to_string()))?.as_nanos();
        let backup = store.join(format!("push-backup-{}-{nonce}", std::process::id()));
        std::fs::create_dir(&backup)?;
        let mut tx = Self { backup, files: Vec::new(), watched: Default::default(), dirs: Default::default(), keep_backup: false };
        for path in [base.join("requires"), store.join("requires"), store.join("fncache"), base.join("bookmarks")] {
            tx.watch(path, false)?;
        }
        Ok(tx)
    }

    fn watch(&mut self, path: std::path::PathBuf, revlog: bool) -> Result<()> {
        if self.watched.contains(&path) { return Ok(()); }
        let saved = match std::fs::metadata(&path) {
            Ok(meta) if meta.is_file() => {
                let backup = self.backup.join(self.files.len().to_string());
                if revlog { std::fs::hard_link(&path, &backup)?; }
                else { std::fs::copy(&path, &backup)?; }
                Some(backup)
            }
            Ok(_) => return Err(format!("push path is not a file: {}", path.display()).into()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => return Err(err.into()),
        };
        let mut parent = path.parent();
        while let Some(dir) = parent {
            if dir.exists() { break; }
            self.dirs.insert(dir.to_path_buf());
            parent = dir.parent();
        }
        self.watched.insert(path.clone());
        self.files.push((path, saved));
        Ok(())
    }

    fn watch_group(&mut self, repo: &RevlogRepo, section: &Section) -> Result<()> {
        let store = repo.store_path();
        let dotencode = repo.get_requirements().contains(&Required::Dotencode);
        for ext in [b"i".as_slice(), b"d".as_slice()] {
            let path = match section {
                Section::Changeset => store.join(format!("00changelog.{}", ext[0] as char)),
                Section::Manifest => store.join(format!("00manifest.{}", ext[0] as char)),
                Section::Filelog(path) => store.join(path.store_filelog(dotencode, ext)),
                Section::Treemanifest(dir) => store.join(dir.store_dirlog(dotencode, ext)),
            };
            self.watch(path, true)?;
        }
        Ok(())
    }

    fn rollback(&mut self, repo: &RevlogRepo) -> Result<()> {
        // Never truncate a mapped file (Windows rejects that). Replacing its
        // name also preserves any independent reader's original file mapping.
        self.keep_backup = true;
        for (path, saved) in self.files.iter().rev() {
            if let Some(saved) = saved {
                std::fs::rename(saved, path)?;
            } else {
                match std::fs::remove_file(path) {
                    Ok(()) => {},
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {},
                    Err(err) => return Err(err.into()),
                }
            }
        }
        for dir in self.dirs.iter().rev() {
            // Remove only directories we created, and only if empty.
            // A concurrent unrelated file in a new directory must survive.
            let _ = std::fs::remove_dir(dir);
        }
        repo.reload_after_rollback()?;
        self.keep_backup = false;
        Ok(())
    }
}

impl Drop for PushTransaction {
    fn drop(&mut self) {
        if !self.keep_backup { let _ = std::fs::remove_dir_all(&self.backup); }
    }
}

pub(crate) fn is_bundle1(bundle: &[u8]) -> bool {
    // Legacy wire peers send the unpacked changegroup without an HG10 header.
    bundle.starts_with(b"HG10") || bundle.first() == Some(&0)
}

pub fn heads_error() -> hgproto::Response {
    hgproto::Response::Pusherr(Bytes::from_static(
        b"repository changed while preparing changes - please try again",
    ))
}

pub(crate) async fn reply(applied: &Applied) -> Result<hgproto::Response> {
    if applied.body == b"0\n" {
        return Ok(heads_error());
    }
    if applied.bundle1 {
        return Ok(hgproto::Response::Pushres {
            ret: 1,
            output: Bytes::new(),
        });
    }
    reply_bundle(applied).await.map(hgproto::Response::Unbundle)
}

fn drain_bundle1(bundle: &[u8], mut on_part: impl FnMut(Part)) -> Result<()> {
    let mut raw = Vec::new();
    if bundle.first() == Some(&0) {
        raw.extend_from_slice(bundle);
    } else {
        match bundle.get(4..6) {
            Some(b"UN") => raw.extend_from_slice(&bundle[6..]),
            Some(b"GZ") => {
                flate2::read::ZlibDecoder::new(&bundle[6..]).read_to_end(&mut raw)?;
            }
            Some(b"BZ") => {
                // Bundle1 omits the BZ bytes at the start of a bzip2 stream.
                let compressed = [b"BZ".as_slice(), &bundle[6..]].concat();
                let mut decoder = Decompressor::new(Cursor::new(compressed), DecompressorType::Bzip2);
                ready(tokio::io::AsyncReadExt::read_to_end(&mut decoder, &mut raw))?;
            }
            _ => return Err("unknown bundle1 compression type".into()),
        }
    }
    let logger = slog::Logger::root(slog::Discard, slog::o!());
    let mut decoder = Cg2Unpacker::with_version(logger, CgVersion::V1);
    let mut raw = BytesMut::from(raw.as_slice());
    while let Some(inner) = decoder.decode_eof(&mut raw).map_err(bundle_err)? {
        on_part(inner.cg2_part());
    }
    Ok(())
}

pub(crate) async fn reply_bundle(applied: &Applied) -> Result<Bytes> {
    if applied.push_raced {
        return push_raced_bundle().await;
    }
    let body = applied.body.as_slice();
    let ids = if applied.changegroup_ids.is_empty() {
        vec![0]
    } else {
        applied.changegroup_ids.clone()
    };
    let ret = if body == b"0\n" { "0" } else { "1" };
    let mut bundle = mercurial_bundles::Bundle2EncodeBuilder::new(Cursor::new(Vec::new()));
    bundle.set_compressor_type(CompressorType::Uncompressed);
    for id in ids {
        let mut part = PartEncodeBuilder::mandatory("reply:changegroup")
            .map_err(bundle_err)?;
        part.add_aparam("in-reply-to", id.to_string())
            .map_err(bundle_err)?;
        part.add_aparam("return", ret)
            .map_err(bundle_err)?;
        part.set_data_bytes(Bytes::copy_from_slice(body))
            .map_err(bundle_err)?;
        bundle.add_part(part);
    }
    let cursor = bundle.build().await.map_err(bundle_err)?;
    Ok(Bytes::from(cursor.into_inner()))
}

async fn push_raced_bundle() -> Result<Bytes> {
    error_bundle(
        "error:pushraced",
        "remote repository changed while pushing - please try again",
    )
    .await
}

/// Reply to a push that failed, so the client shows `message`.
pub(crate) async fn abort_bundle(message: &str) -> Result<Bytes> {
    error_bundle("error:abort", &bound_message(message)).await
}

/// Fits `message` in a bundle2 parameter value. A long one names a long path,
/// so the middle goes; the last bytes hold the advice of every rejection.
fn bound_message(message: &str) -> String {
    const MAX_LEN: usize = u8::MAX as usize;
    const TAIL: usize = 200;
    const ELLIPSIS: &str = "...";
    if message.len() <= MAX_LEN {
        return message.to_string();
    }
    let head = message.floor_char_boundary(MAX_LEN - TAIL - ELLIPSIS.len());
    let tail = message.ceil_char_boundary(message.len() - TAIL);
    format!("{}{ELLIPSIS}{}", &message[..head], &message[tail..])
}

async fn error_bundle(kind: &str, message: &str) -> Result<Bytes> {
    let mut bundle = mercurial_bundles::Bundle2EncodeBuilder::new(Cursor::new(Vec::new()));
    bundle.set_compressor_type(CompressorType::Uncompressed);
    let mut part = PartEncodeBuilder::mandatory(kind).map_err(bundle_err)?;
    part.add_mparam("message", message.to_string())
        .map_err(bundle_err)?;
    bundle.add_part(part);
    let cursor = bundle.build().await.map_err(bundle_err)?;
    Ok(Bytes::from(cursor.into_inner()))
}

fn check_heads_agree(repo: &RevlogRepo, bundle: &[u8]) -> Result<bool> {
    let current = repo.changelog_revlog().get_heads()?;
    let mut in_part = false;
    let mut buf = Vec::new();
    let mut raced = false;
    drain_bundle(bundle, |item| {
        match item {
            Bundle2Item::Start(_) => {}
            Bundle2Item::Header(header) => {
                finish_check_heads(&mut in_part, &mut buf, &current, &mut raced)?;
                in_part = header.part_type_lower().as_str() == "check:heads";
            }
            Bundle2Item::Inner(InnerPart::CheckHeads(bytes)) if in_part => {
                buf.extend_from_slice(&bytes);
            }
            Bundle2Item::Inner(_) => {}
        }
        Ok(())
    })?;
    finish_check_heads(&mut in_part, &mut buf, &current, &mut raced)?;
    Ok(!raced)
}

fn finish_check_heads(
    in_part: &mut bool,
    buf: &mut Vec<u8>,
    current: &std::collections::HashSet<NodeHash>,
    raced: &mut bool,
) -> Result<()> {
    if !*in_part {
        return Ok(());
    }
    *in_part = false;
    if buf.len() % 20 != 0 {
        return Err("check:heads payload is not a multiple of 20 bytes".into());
    }
    let mut offered = std::collections::HashSet::new();
    for chunk in buf.chunks(20) {
        let node = NodeHash::from_bytes(chunk).map_err(|err| Error::from(err.to_string()))?;
        offered.insert(node);
    }
    buf.clear();
    if offered != *current {
        *raced = true;
    }
    Ok(())
}

fn bundle_err(err: impl std::fmt::Display) -> Error {
    Error::from(err.to_string())
}

fn compression_of(repo: &RevlogRepo) -> RevlogCompression {
    if repo
        .get_requirements()
        .contains(&Required::RevlogCompressionZstd)
    {
        RevlogCompression::Zstd
    } else {
        RevlogCompression::Zlib
    }
}

fn drain_bundle(bundle: &[u8], mut on_item: impl FnMut(Bundle2Item) -> Result<()>) -> Result<()> {
    let logger = slog::Logger::root(slog::Discard, slog::o!());
    let mut stream = Bundle2Stream::new(Cursor::new(bundle), logger);
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    loop {
        match Pin::new(&mut stream).poll_next(&mut cx) {
            Poll::Ready(Some(item)) => on_item(item.map_err(bundle_err)?)?,
            Poll::Ready(None) => return Ok(()),
            Poll::Pending => return Err("bundle2 read blocked".into()),
        }
    }
}

fn push_chunk(groups: &mut Vec<(Section, Vec<CgDeltaChunk>)>, section: Section, chunk: CgDeltaChunk) {
    if let Some((last, chunks)) = groups.last_mut() {
        if last == &section {
            chunks.push(chunk);
            return;
        }
    }
    groups.push((section, vec![chunk]));
}

fn append_group(
    repo: &RevlogRepo,
    compression: RevlogCompression,
    section: &Section,
    chunks: Vec<CgDeltaChunk>,
) -> Result<()> {
    let mut revlog = revlog_for(repo, section)?;
    let mut left = chunks;
    while !left.is_empty() {
        let batch = std::mem::take(&mut left);
        let mut progress = false;
        for chunk in batch {
            if revlog.index_of(&chunk.node).is_ok() {
                progress = true;
                continue;
            }
            if !chunk_ready(&revlog, &chunk) {
                left.push(chunk);
                continue;
            }
            let next = revlog.len()?;
            let linkrev = changelog_linkrev(repo, section, &chunk, next)?;
            let CgDeltaChunk {
                node,
                p1,
                p2,
                base,
                linknode: _,
                flags,
                delta,
            } = chunk;
            // Keep the client's delta when it is against a revision we have.
            let stored_delta = (base != NULL_HASH).then(|| (base, delta.clone()));
            let text = full_text(&revlog, base, delta)?;
            revlog.append_revision_flagged(
                RevlogAppend {
                    node,
                    p1: parent_opt(p1),
                    p2: parent_opt(p2),
                    linkrev: Some(linkrev),
                    text,
                },
                stored_delta,
                compression,
                // `check_groups` let only known flags through.
                IdxFlags::from_bits_truncate(flags),
            )?;
            progress = true;
        }
        if !progress {
            return Err("changegroup revision has no parent in the revlog".into());
        }
    }
    match section {
        Section::Filelog(path) => repo.record_file_data(path)?,
        Section::Treemanifest(dir) => repo.record_dir_data(dir)?,
        Section::Changeset | Section::Manifest => {}
    }
    Ok(())
}

/// A changegroup with directory manifests needs a treemanifest repository.
/// An empty flat one becomes one, as Mercurial does.
fn use_treemanifest(repo: &RevlogRepo) -> Result<()> {
    if repo.is_treemanifest() {
        return Ok(());
    }
    if repo.changelog_revlog().len()? != 0 {
        return Err("bundle contains tree manifests, but local repo is non-empty and does not use tree manifests".into());
    }
    Ok(repo.add_requirement(Required::Treemanifest)?)
}

fn revlog_for(repo: &RevlogRepo, section: &Section) -> Result<Revlog> {
    match section {
        Section::Changeset => Ok(repo.changelog_revlog()),
        Section::Manifest => Ok(repo.manifest_revlog()),
        Section::Filelog(path) => Ok(repo.file_revlog_for_append(path)?),
        Section::Treemanifest(dir) => Ok(repo.dir_revlog_for_append(dir)?),
    }
}

fn chunk_ready(revlog: &Revlog, chunk: &CgDeltaChunk) -> bool {
    parent_ready(revlog, chunk.p1)
        && parent_ready(revlog, chunk.p2)
        && (chunk.base == NULL_HASH || revlog.index_of(&chunk.base).is_ok())
}

fn parent_ready(revlog: &Revlog, parent: NodeHash) -> bool {
    parent == NULL_HASH || revlog.index_of(&parent).is_ok()
}

fn parent_opt(parent: NodeHash) -> Option<NodeHash> {
    if parent == NULL_HASH {
        None
    } else {
        Some(parent)
    }
}

fn changelog_linkrev(
    repo: &RevlogRepo,
    section: &Section,
    chunk: &CgDeltaChunk,
    next_index: u32,
) -> Result<u32> {
    let changelog = repo.changelog_revlog();
    if let Ok(idx) = changelog.index_of(&chunk.linknode) {
        return Ok(idx);
    }
    if matches!(section, Section::Changeset) && chunk.linknode == chunk.node {
        return Ok(next_index);
    }
    Err(format!("linknode {} is not in the changelog", chunk.linknode).into())
}

fn full_text(revlog: &Revlog, base: NodeHash, delta: Delta) -> Result<Vec<u8>> {
    let base_text = if base == NULL_HASH {
        Vec::new()
    } else {
        let node = revlog.get_rev_by_nodeid(&base)?;
        node.as_blob()
            .as_slice()
            .ok_or_else(|| Error::from("revision is missing text"))?
            .to_vec()
    };
    delta_result_len(base_text.len(), &delta)?;
    Ok(delta::apply(&base_text, delta))
}

fn apply_pushkey(repo: &RevlogRepo, header: &PartHeader) -> Result<()> {
    let namespace = param(header, "namespace")?;
    if namespace != b"bookmarks" {
        return Ok(());
    }
    let key = param(header, "key")?;
    let old = parse_hex_node(param(header, "old")?)?;
    let new = parse_hex_node(param(header, "new")?)?;
    // A missed compare-and-swap leaves the file unchanged and must not abort
    // the changegroup that shares this bundle.
    let _applied = pushkey_bookmark(repo, key, old, new)?;
    Ok(())
}

pub(crate) fn pushkey_bookmark(
    repo: &RevlogRepo,
    key: &[u8],
    old: NodeHash,
    new: NodeHash,
) -> Result<bool> {
    if new != NULL_HASH && repo.changelog_revlog().index_of(&new).is_err() {
        return Ok(false);
    }
    let marks = repo.bookmarks()?;
    let current = ready(marks.get(&key))?;
    let matches = match current {
        Some((hash, _)) => hash == old,
        None => old == NULL_HASH,
    };
    if !matches {
        return Ok(false);
    }
    let version = if current.is_some() {
        Version::from(1)
    } else {
        Version::absent()
    };
    let updated = if new == NULL_HASH {
        ready(marks.delete(&key, &version))?
    } else {
        ready(marks.set(&key, &new, &version))?
    };
    Ok(updated.is_some())
}

fn param<'a>(header: &'a PartHeader, name: &str) -> Result<&'a [u8]> {
    header
        .mparams()
        .get(name)
        .or_else(|| header.aparams().get(name))
        .map(Bytes::as_ref)
        .ok_or_else(|| Error::from(format!("pushkey missing {name}")))
}

fn parse_hex_node(bytes: &[u8]) -> Result<NodeHash> {
    if bytes.is_empty() {
        return Ok(NULL_HASH);
    }
    if bytes.len() != 40 {
        return Err(format!("node hash length {}", bytes.len()).into());
    }
    let mut raw = [0u8; 20];
    for i in 0..20 {
        raw[i] = (hex_val(bytes[i * 2])? << 4) | hex_val(bytes[i * 2 + 1])?;
    }
    NodeHash::from_bytes(&raw).map_err(|err| Error::from(err.to_string()))
}

fn hex_val(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err("node hash is not hex".into()),
    }
}

fn ready<T, E, F>(fut: F) -> Result<T>
where
    E: std::fmt::Display,
    F: std::future::Future<Output = std::result::Result<T, E>>,
{
    match fut.now_or_never() {
        Some(Ok(value)) => Ok(value),
        Some(Err(err)) => Err(err.to_string().into()),
        None => Err("bookmark update blocked".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lfs::pointer::{sha256_oid, Oid};
    use crate::lfs::store::{LfsStore, StoreSpec};
    use crate::lfs::LfsServing;
    use crate::repo::{Repo, RepoClient, RepoOptions};
    use hgproto::HgCommands;
    use mercurial_types::delta::Fragment;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn abort_message_keeps_its_start_and_advice_within_the_limit() {
        let advice = "; track it in .hglfs";
        let message = format!("{}{advice}", "\u{e9}".repeat(150));
        let bounded = bound_message(&message);
        assert!(bounded.len() <= 255, "{}", bounded.len());
        assert!(bounded.starts_with('\u{e9}') && bounded.contains("..."), "{bounded}");
        assert!(bounded.ends_with(advice), "{bounded}");
        assert_eq!(bound_message("short"), "short");
    }

    struct Fixture(PathBuf);

    fn hg(path: &Path, args: &[&str]) -> Vec<u8> {
        let out = Command::new("hg")
            .current_dir(path)
            .args(args)
            .env("HGPLAIN", "1")
            .env("HGRCPATH", "")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "hg {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        out.stdout
    }

    impl Fixture {
        fn new() -> Self {
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let root = std::env::temp_dir().join(format!("push01-{stamp}"));
            fs::create_dir_all(&root).unwrap();
            hg(
                &root,
                &[
                    "init",
                    "source",
                    "--config",
                    "format.use-persistent-nodemap=false",
                ],
            );
            let source = root.join("source");
            fs::write(source.join("a"), b"first\n").unwrap();
            hg(&source, &["commit", "-Aqm", "first", "-u", "test"]);
            hg(&root, &["clone", "-q", "source", "target"]);
            fs::write(source.join("a"), b"second\n").unwrap();
            hg(&source, &["commit", "-qm", "second", "-u", "test"]);
            Self(root)
        }

        fn bundle(&self, kind: &str) -> Vec<u8> {
            let source = self.0.join("source");
            let output = source.join("out.hg");
            let _ = fs::remove_file(&output);
            hg(
                &source,
                &["bundle", "-t", kind, "-r", "tip", "--base", "0", "out.hg"],
            );
            fs::read(output).unwrap()
        }

        fn repo(&self) -> RevlogRepo {
            RevlogRepo::open(&self.0.join("target/.hg")).unwrap()
        }

        fn apply(&self, bundle: &[u8]) {
            let repo = self.repo();
            let heads: Vec<_> = repo
                .changelog_revlog()
                .get_heads()
                .unwrap()
                .into_iter()
                .collect();
            let applied = apply_repo(&repo, &heads, bundle, &NO_POLICY).unwrap();
            assert_eq!(applied.body, b"1\n");
            assert_eq!(repo.changelog_revlog().len().unwrap(), 2);
            hg(&self.0.join("target"), &["verify"]);
            assert_eq!(
                hg(
                    &self.0.join("target"),
                    &["log", "-r", "tip", "-T", "{node}"]
                ),
                hg(
                    &self.0.join("source"),
                    &["log", "-r", "tip", "-T", "{node}"]
                )
            );
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn failed_push_restores_disk_and_shared_revlogs() {
        let fixture = Fixture::new();
        let source = fixture.0.join("source");
        fs::create_dir_all(source.join("newdir")).unwrap();
        fs::write(source.join("newdir/b"), b"late\n").unwrap();
        // Force an inline-to-split transition before the later file obstruction.
        let mut value = 42u32;
        let large: Vec<u8> = (0..200_000).map(|_| {
            value ^= value << 13; value ^= value >> 17; value ^= value << 5;
            value as u8
        }).collect();
        fs::write(source.join("a"), large).unwrap();
        hg(&source, &["commit", "-Aqm", "late", "-u", "test"]);
        let target = fixture.0.join("target");
        let obstruction = target.join(".hg/store/data/newdir");
        fs::write(&obstruction, b"keep me").unwrap();
        let unrelated = target.join(".hg/store/.00changelog.i.append-tmp");
        fs::write(&unrelated, b"unrelated temporary file").unwrap();
        let before = fs::read(target.join(".hg/store/fncache")).unwrap();
        let repo = fixture.repo();
        let log = repo.changelog_revlog();
        let file = repo.get_file_revlog(&mercurial_types::Path::new(b"a").unwrap()).unwrap();
        let heads: Vec<_> = log.clone().get_heads().unwrap().into_iter().collect();
        let bundle = fixture.bundle("none-v2");
        assert!(apply_repo(&repo, &heads, &bundle, &NO_POLICY).is_err());
        assert_eq!(log.len().unwrap(), 1, "failed push remained in shared changelog");
        assert_eq!(file.len().unwrap(), 1, "failed push remained in shared filelog");
        assert!(!target.join(".hg/store/data/a.d").exists(), "split data file survived rollback");
        assert_eq!(file.get_rev(mercurial::revlog::RevIdx::from(0u32)).unwrap().as_blob().as_slice(), Some(b"first\n".as_slice()));
        assert_eq!(fs::read(target.join(".hg/store/fncache")).unwrap(), before);
        assert_eq!(fs::read(&obstruction).unwrap(), b"keep me");
        assert_eq!(fs::read(&unrelated).unwrap(), b"unrelated temporary file");
        hg(&target, &["verify"]);
        fs::remove_file(obstruction).unwrap();
        apply_repo(&repo, &heads, &bundle, &NO_POLICY).unwrap();
        assert_eq!(log.len().unwrap(), 3);
        hg(&target, &["verify"]);
    }

    #[test]
    fn failed_tree_push_restores_requirements_fncache_and_new_files() {
        let fixture = Fixture::new();
        let source = fixture.0.join("source");
        let reqpath = source.join(".hg/store/requires");
        let mut reqs = fs::read(&reqpath).unwrap();
        reqs.extend_from_slice(b"treemanifest\n");
        fs::write(reqpath, reqs).unwrap();
        fs::create_dir_all(source.join("newdir")).unwrap();
        fs::write(source.join("newdir/b"), b"tree\n").unwrap();
        hg(&source, &["commit", "-Aqm", "tree", "-u", "test"]);
        hg(&source, &["bundle", "--all", "-t", "none-v2;cg.version=03", "tree.hg"]);
        hg(&fixture.0, &["init", "empty"]);
        let target = fixture.0.join("empty");
        fs::create_dir_all(target.join(".hg/store/data")).unwrap();
        fs::write(target.join(".hg/store/data/newdir"), b"unrelated obstruction").unwrap();
        fs::write(target.join(".hg/store/requires.tmp"), b"unrelated metadata file").unwrap();
        let repo = RevlogRepo::open(target.join(".hg")).unwrap();
        let clone = repo.clone();
        let requirements = repo.get_requirements();
        let before = fs::read(target.join(".hg/store/requires")).unwrap();
        let raw = fs::read(source.join("tree.hg")).unwrap();
        let mut parts = Vec::new();
        drain_bundle(&raw, |item| {
            if let Bundle2Item::Inner(inner) = item { parts.push(inner.cg2_part()); }
            Ok(())
        }).unwrap();
        let mut part = mercurial_bundles::parts::changegroup_part_version(
            futures::stream::iter(parts.into_iter().map(Ok::<_, mercurial_bundles::Error>)), CgVersion::V3,
        ).unwrap();
        part.add_mparam("treemanifest", "1").unwrap();
        let mut builder = mercurial_bundles::Bundle2EncodeBuilder::new(Cursor::new(Vec::new()));
        builder.add_part(part);
        let bytes = futures::executor::block_on(builder.build()).unwrap().into_inner();
        assert!(apply_repo(&repo, &[NULL_HASH], &bytes, &NO_POLICY).is_err());
        assert_eq!(clone.get_requirements(), requirements);
        assert_eq!(fs::read(target.join(".hg/store/requires")).unwrap(), before);
        assert_eq!(clone.changelog_revlog().len().unwrap(), 0);
        assert_eq!(clone.manifest_revlog().len().unwrap(), 0);
        assert!(!target.join(".hg/store/fncache").exists());
        assert!(!target.join(".hg/store/meta").exists());
        assert!(!target.join(".hg/store/data/a.i").exists());
        assert_eq!(fs::read(target.join(".hg/store/data/newdir")).unwrap(), b"unrelated obstruction");
        assert_eq!(fs::read(target.join(".hg/store/requires.tmp")).unwrap(), b"unrelated metadata file");
        hg(&target, &["verify"]);
    }

    #[test]
    fn failed_bookmark_part_restores_prior_bookmark_and_history() {
        let fixture = Fixture::new();
        let target = fixture.0.join("target");
        hg(&target, &["bookmark", "keep"]);
        let before = fs::read(target.join(".hg/bookmarks")).unwrap();
        let raw = fixture.bundle("none-v1");
        let mut builder = mercurial_bundles::Bundle2EncodeBuilder::new(Cursor::new(Vec::new()));
        let mut cg = PartEncodeBuilder::mandatory("changegroup").unwrap();
        cg.add_mparam("version", "01").unwrap();
        cg.set_data_bytes(Bytes::copy_from_slice(&raw[6..])).unwrap();
        builder.add_part(cg);
        let old = String::from_utf8(hg(&target, &["log", "-r", "tip", "-T", "{node}"])).unwrap();
        let new = String::from_utf8(hg(&fixture.0.join("source"), &["log", "-r", "tip", "-T", "{node}"])).unwrap();
        let mut mark = PartEncodeBuilder::mandatory("pushkey").unwrap();
        for (key, value) in [("namespace", "bookmarks"), ("key", "keep"), ("old", &old), ("new", &new)] {
            mark.add_mparam(key, value.to_owned()).unwrap();
        }
        builder.add_part(mark);
        let mut bad = PartEncodeBuilder::mandatory("pushkey").unwrap();
        bad.add_mparam("namespace", "bookmarks").unwrap();
        builder.add_part(bad);
        let bytes = futures::executor::block_on(builder.build()).unwrap().into_inner();
        let repo = fixture.repo();
        let heads: Vec<_> = repo.changelog_revlog().get_heads().unwrap().into_iter().collect();
        let err = apply_repo(&repo, &heads, &bytes, &NO_POLICY).err().expect("invalid second bookmark part");
        assert!(err.to_string().contains("pushkey missing key"), "{err}");
        assert_eq!(fs::read(target.join(".hg/bookmarks")).unwrap(), before);
        assert_eq!(repo.changelog_revlog().len().unwrap(), 1);
        hg(&target, &["verify"]);
    }

    #[test]
    fn flagged_cg03_is_rejected_before_any_write() {
        let fixture = Fixture::new();
        let raw = fixture.bundle("none-v2;cg.version=03");
        let mut parts = Vec::new();
        drain_bundle(&raw, |item| {
            if let Bundle2Item::Inner(inner) = item {
                parts.push(inner.cg2_part());
            }
            Ok(())
        }).unwrap();
        for part in &mut parts {
            if let Part::CgChunk(Section::Filelog(_), chunk) = part {
                chunk.flags = 0x8000;
            }
        }
        let part = mercurial_bundles::parts::changegroup_part_version(
            futures::stream::iter(parts.into_iter().map(Ok::<_, mercurial_bundles::Error>)),
            CgVersion::V3,
        ).unwrap();
        let mut builder = mercurial_bundles::Bundle2EncodeBuilder::new(Cursor::new(Vec::new()));
        builder.add_part(part);
        let bytes = futures::executor::block_on(builder.build()).unwrap().into_inner();
        let repo = fixture.repo();
        let heads: Vec<_> = repo.changelog_revlog().get_heads().unwrap().into_iter().collect();
        let result = apply_repo(&repo, &heads, &bytes, &NO_POLICY);
        assert!(result.is_err(), "unsupported flags were silently stripped");
        assert!(result.err().unwrap().to_string().contains("flags"));
        assert_eq!(repo.changelog_revlog().len().unwrap(), 1);
        hg(&fixture.0.join("target"), &["verify"]);
    }

    const NO_POLICY: PushPolicy = PushPolicy { lfs: None, max_plain_file_size: 0 };
    const EXT: u16 = IdxFlags::EXTSTORED.bits();

    fn oid_of(data: &[u8]) -> Oid {
        use sha2::{Digest, Sha256};
        sha256_oid(&Sha256::digest(data).into())
    }

    fn pointer(data: &[u8]) -> Vec<u8> {
        format!(
            "version https://git-lfs.github.com/spec/v1\noid sha256:{}\nsize {}\n",
            oid_of(data),
            data.len()
        )
        .into_bytes()
    }

    fn insert(text: &[u8]) -> Delta {
        Delta::new(vec![Fragment { start: 0, end: 0, content: text.to_vec() }]).unwrap()
    }

    fn replace_all(base_len: usize, text: &[u8]) -> Delta {
        Delta::new(vec![Fragment { start: 0, end: base_len, content: text.to_vec() }]).unwrap()
    }

    fn cg_parts(raw: &[u8]) -> Vec<Part> {
        let mut parts = Vec::new();
        drain_bundle(raw, |item| {
            if let Bundle2Item::Inner(inner) = item {
                parts.push(inner.cg2_part());
            }
            Ok(())
        }).unwrap();
        parts
    }

    fn cg03_builder(parts: Vec<Part>) -> mercurial_bundles::Bundle2EncodeBuilder<Cursor<Vec<u8>>> {
        let part = mercurial_bundles::parts::changegroup_part_version(
            futures::stream::iter(parts.into_iter().map(Ok::<_, mercurial_bundles::Error>)),
            CgVersion::V3,
        ).unwrap();
        let mut builder = mercurial_bundles::Bundle2EncodeBuilder::new(Cursor::new(Vec::new()));
        builder.add_part(part);
        builder
    }

    /// The second file revision on top of `chunk`, with its own node.
    fn next_rev(chunk: &CgDeltaChunk, flags: u16, delta: Delta) -> CgDeltaChunk {
        CgDeltaChunk {
            node: NodeHash::from_bytes(&[7; 20]).unwrap(),
            p1: chunk.node,
            base: chunk.node,
            flags,
            delta,
            ..chunk.clone()
        }
    }

    fn as_pointer(mut chunk: CgDeltaChunk, data: &[u8]) -> CgDeltaChunk {
        chunk.flags = EXT;
        chunk.base = NULL_HASH;
        chunk.delta = insert(&pointer(data));
        chunk
    }

    impl Fixture {
        fn store(&self) -> Arc<LfsStore> {
            let logger = slog::Logger::root(slog::Discard, slog::o!());
            let dir = self.0.join("target/.hg/store");
            Arc::new(LfsStore::open(&StoreSpec::Repo, None, &dir, &logger).unwrap())
        }

        /// The cg03 bundle of the source's second commit, with the filelog
        /// chunk replaced by what `edit` makes of it.
        fn cg03_parts(&self, edit: impl FnOnce(CgDeltaChunk) -> Vec<CgDeltaChunk>) -> Vec<Part> {
            let mut parts = cg_parts(&self.bundle("none-v2;cg.version=03"));
            let at = parts.iter().position(|part| matches!(part, Part::CgChunk(Section::Filelog(_), _))).unwrap();
            let Part::CgChunk(section, chunk) = parts.remove(at) else { unreachable!() };
            for (i, chunk) in edit(chunk).into_iter().enumerate() {
                parts.insert(at + i, Part::CgChunk(section.clone(), chunk));
            }
            parts
        }

        fn cg03(&self, edit: impl FnOnce(CgDeltaChunk) -> Vec<CgDeltaChunk>) -> Vec<u8> {
            let builder = cg03_builder(self.cg03_parts(edit));
            futures::executor::block_on(builder.build()).unwrap().into_inner()
        }

        /// Push the way `unbundle` does: on the blocking pool of a current-thread runtime.
        fn push(&self, bundle: &[u8], store: Option<&Arc<LfsStore>>, limit: u64) -> Result<Applied> {
            let path = self.0.join("target/.hg");
            let bundle = bundle.to_vec();
            let store = store.cloned();
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            runtime.block_on(async move {
                tokio::task::spawn_blocking(move || {
                    let repo = RevlogRepo::open(path)?;
                    let heads: Vec<_> = repo.changelog_revlog().get_heads()?.into_iter().collect();
                    let policy = PushPolicy { lfs: store.as_deref(), max_plain_file_size: limit };
                    apply_repo(&repo, &heads, &bundle, &policy)
                }).await
            }).unwrap()
        }

        fn put(&self, store: &LfsStore, data: &[u8]) {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            runtime.block_on(store.put(&oid_of(data), data.len() as u64, data)).unwrap();
        }

        fn assert_rejected(&self, result: Result<Applied>, needle: &str) {
            let err = result.err().expect("push must be rejected").to_string();
            assert!(err.contains(needle), "{err}");
            let repo = self.repo();
            assert_eq!(repo.changelog_revlog().len().unwrap(), 1, "nothing is written");
            assert_eq!(repo.manifest_revlog().len().unwrap(), 1, "nothing is written");
            assert!(!repo.get_requirements().contains(&Required::Lfs));
            let backups = fs::read_dir(self.0.join("target/.hg/store")).unwrap()
                .filter(|entry| entry.as_ref().unwrap().file_name().to_string_lossy().starts_with("push-backup"))
                .count();
            assert_eq!(backups, 0);
            hg(&self.0.join("target"), &["verify"]);
        }
    }

    #[test]
    fn plain_limit_rejects_large_file() {
        let fixture = Fixture::new();
        let bundle = fixture.cg03(|mut chunk| {
            chunk.base = NULL_HASH;
            chunk.delta = insert(b"12345678901");
            vec![chunk]
        });
        fixture.assert_rejected(
            fixture.push(&bundle, None, 10),
            "a is 0 MB, over the server's 0 MB limit for files not stored in LFS; track it in .hglfs",
        );
    }

    #[test]
    fn plain_limit_counts_delta_against_base_in_same_push() {
        let fixture = Fixture::new();
        let bundle = fixture.cg03(|mut chunk| {
            chunk.base = NULL_HASH;
            chunk.delta = insert(b"123456");
            let next = next_rev(&chunk, 0, insert(b"abcde"));
            vec![chunk, next]
        });
        fixture.assert_rejected(fixture.push(&bundle, None, 10), "over the server's");
        fixture.push(&bundle, None, 11).map(drop).unwrap();
    }

    #[test]
    fn plain_limit_counts_delta_against_stored_base() {
        let fixture = Fixture::new();
        // "first\n" is stored; growing it by five is 11 bytes.
        let bundle = fixture.cg03(|mut chunk| {
            chunk.base = chunk.p1;
            chunk.delta = insert(b"12345");
            vec![chunk]
        });
        fixture.assert_rejected(fixture.push(&bundle, None, 10), "over the server's");
    }

    #[test]
    fn plain_limit_applies_to_every_changegroup_format() {
        // "second\n" is 7 bytes.
        let fixture = Fixture::new();
        let v1 = fixture.bundle("none-v1");
        let bundles = [
            ("bundle1", v1.clone()),
            ("headerless", v1[6..].to_vec()),
            ("bundle2 cg01", bundle2_v1(&v1, true)),
            ("cg02", fixture.bundle("none-v2;cg.version=02")),
            ("cg03", fixture.bundle("none-v2;cg.version=03")),
        ];
        for (kind, bundle) in bundles {
            let err = fixture.push(&bundle, None, 6).err().unwrap_or_else(|| panic!("{kind} passed the limit"));
            assert!(err.to_string().contains("over the server's"), "{kind}: {err}");
            assert_eq!(fixture.repo().changelog_revlog().len().unwrap(), 1, "{kind}");
        }
    }

    #[test]
    fn revisions_already_stored_are_not_checked() {
        let fixture = Fixture::new();
        let bundle = fixture.cg03(|chunk| vec![chunk]);
        fixture.push(&bundle, None, 0).map(drop).unwrap();
        fixture.push(&bundle, None, 1).map(drop).unwrap();
    }

    #[test]
    fn duplicate_revision_in_a_push_is_rejected() {
        let fixture = Fixture::new();
        let bundle = fixture.cg03(|chunk| vec![chunk.clone(), chunk]);
        fixture.assert_rejected(fixture.push(&bundle, None, 0), "a: duplicate revision");
    }

    #[test]
    fn delta_result_len_rejects_hostile_ranges() {
        let frag = |start, end, content: &[u8]| Fragment { start, end, content: content.to_vec() };
        let delta = Delta::new(vec![frag(1, 3, b"xyz"), frag(4, 5, b"")]).unwrap();
        assert_eq!(delta_result_len(6, &delta).unwrap(), 6);
        assert_eq!(delta_result_len(0, &Delta::default()).unwrap(), 0);
        assert!(delta_result_len(4, &delta).is_err(), "end past the base");
        let huge = Delta::new(vec![frag(usize::MAX - 1, usize::MAX, b"")]).unwrap();
        assert!(delta_result_len(10, &huge).is_err());
    }

    #[test]
    fn delta_past_its_base_is_rejected_without_a_panic() {
        let fixture = Fixture::new();
        let bundle = fixture.cg03(|mut chunk| {
            chunk.base = chunk.p1;
            chunk.delta = Delta::new(vec![Fragment { start: 100, end: 200, content: Vec::new() }]).unwrap();
            vec![chunk]
        });
        fixture.assert_rejected(fixture.push(&bundle, None, 0), "outside its base");
    }

    #[test]
    fn unknown_delta_base_is_rejected() {
        let fixture = Fixture::new();
        let bundle = fixture.cg03(|mut chunk| {
            chunk.base = NodeHash::from_bytes(&[9; 20]).unwrap();
            vec![chunk]
        });
        fixture.assert_rejected(fixture.push(&bundle, None, 0), "a: delta base");
    }

    #[test]
    fn push_rejects_missing_blob() {
        let fixture = Fixture::new();
        let store = fixture.store();
        let bundle = fixture.cg03(|chunk| vec![as_pointer(chunk, b"blob")]);
        let expected = format!("a: LFS blob {} is not on the server; push again so hg uploads it", oid_of(b"blob"));
        fixture.assert_rejected(fixture.push(&bundle, Some(&store), 0), &expected);
    }

    #[test]
    fn push_rejects_blob_of_the_wrong_size() {
        let fixture = Fixture::new();
        let store = fixture.store();
        fixture.put(&store, b"blob");
        let text = String::from_utf8(pointer(b"blob")).unwrap().replace("size 4", "size 5");
        let bundle = fixture.cg03(|chunk| {
            let mut chunk = as_pointer(chunk, b"blob");
            chunk.delta = insert(text.as_bytes());
            vec![chunk]
        });
        fixture.assert_rejected(fixture.push(&bundle, Some(&store), 0), "is not on the server");
    }

    #[test]
    fn push_accepts_present_blob_and_adds_requirement() {
        let fixture = Fixture::new();
        let store = fixture.store();
        fixture.put(&store, b"blob");
        let mut node = NULL_HASH;
        // The chunk keeps the node hg gave it, a hash of other text: an
        // LFS node hashes the real content, never the pointer.
        let bundle = fixture.cg03(|chunk| {
            node = chunk.node;
            vec![as_pointer(chunk, b"blob")]
        });
        // The plain limit does not apply to the pointer.
        assert_eq!(fixture.push(&bundle, Some(&store), 1).unwrap().body, b"1\n");
        let repo = fixture.repo();
        let log = repo.get_file_revlog(&mercurial_types::Path::new(b"a").unwrap()).unwrap();
        assert_eq!(log.get_entry_by_nodeid(&node).unwrap().flags, IdxFlags::EXTSTORED);
        assert!(repo.get_requirements().contains(&Required::Lfs));
        let requires = fs::read_to_string(fixture.0.join("target/.hg/store/requires")).unwrap()
            + &fs::read_to_string(fixture.0.join("target/.hg/requires")).unwrap();
        assert!(requires.lines().any(|line| line == "lfs"), "{requires}");
    }

    #[test]
    fn pointer_deltas_chain_inside_a_push() {
        let fixture = Fixture::new();
        let store = fixture.store();
        fixture.put(&store, b"blob");
        fixture.put(&store, b"blob two");
        let (first, second) = (pointer(b"blob"), pointer(b"blob two"));
        let bundle = fixture.cg03(|chunk| {
            let chunk = as_pointer(chunk, b"blob");
            let next = next_rev(&chunk, EXT, replace_all(first.len(), &second));
            // A plain revision measures a pointer base by its raw length.
            let mut plain = next_rev(&next, 0, replace_all(second.len(), b"x"));
            plain.node = NodeHash::from_bytes(&[8; 20]).unwrap();
            vec![chunk, next, plain]
        });
        assert_eq!(fixture.push(&bundle, Some(&store), 1).unwrap().body, b"1\n");
    }

    #[test]
    fn pointer_delta_against_plain_revision_in_the_push_is_rejected() {
        let fixture = Fixture::new();
        let store = fixture.store();
        let bundle = fixture.cg03(|chunk| {
            let next = next_rev(&chunk, EXT, Delta::default());
            vec![chunk, next]
        });
        fixture.assert_rejected(fixture.push(&bundle, Some(&store), 0), "malformed LFS pointer");
    }

    #[test]
    fn push_rejects_malformed_pointer() {
        let fixture = Fixture::new();
        let store = fixture.store();
        let bundle = fixture.cg03(|chunk| {
            let mut chunk = as_pointer(chunk, b"blob");
            chunk.delta = insert(b"hello");
            vec![chunk]
        });
        fixture.assert_rejected(fixture.push(&bundle, Some(&store), 0), "a: malformed LFS pointer");
    }

    #[test]
    fn oversized_pointer_is_rejected_before_building_its_text() {
        let fixture = Fixture::new();
        let store = fixture.store();
        fixture.put(&store, b"blob");
        // A valid pointer but for its length.
        let mut text = pointer(b"blob");
        text.extend_from_slice(format!("x-pad {}\n", "x".repeat(MAX_POINTER_LEN)).as_bytes());
        let bundle = fixture.cg03(|chunk| {
            let mut chunk = as_pointer(chunk, b"blob");
            chunk.delta = insert(&text);
            vec![chunk]
        });
        fixture.assert_rejected(fixture.push(&bundle, Some(&store), 0), "malformed LFS pointer");
    }

    #[test]
    fn push_rejects_lfs_without_lfs_serving() {
        let fixture = Fixture::new();
        let bundle = fixture.cg03(|chunk| vec![as_pointer(chunk, b"blob")]);
        fixture.assert_rejected(fixture.push(&bundle, None, 0), "this repository does not serve LFS");
    }

    #[test]
    fn extstored_outside_filelogs_is_rejected() {
        let fixture = Fixture::new();
        let store = fixture.store();
        let mut parts = cg_parts(&fixture.bundle("none-v2;cg.version=03"));
        for part in &mut parts {
            if let Part::CgChunk(Section::Manifest, chunk) = part {
                chunk.flags = EXT;
            }
        }
        let bytes = futures::executor::block_on(cg03_builder(parts).build()).unwrap().into_inner();
        fixture.assert_rejected(fixture.push(&bytes, Some(&store), 0), "unsupported revision flags");
    }

    #[test]
    fn censored_and_mixed_flags_on_a_filelog_are_rejected() {
        for flags in [IdxFlags::CENSORED.bits(), EXT | 1] {
            let fixture = Fixture::new();
            let store = fixture.store();
            let bundle = fixture.cg03(|mut chunk| {
                chunk.flags = flags;
                vec![chunk]
            });
            fixture.assert_rejected(fixture.push(&bundle, Some(&store), 0), "unsupported revision flags");
        }
    }

    #[test]
    fn failure_after_lfs_append_leaves_no_requirement() {
        let fixture = Fixture::new();
        let store = fixture.store();
        fixture.put(&store, b"blob");
        let mut builder = cg03_builder(fixture.cg03_parts(|chunk| vec![as_pointer(chunk, b"blob")]));
        let mut bad = PartEncodeBuilder::mandatory("pushkey").unwrap();
        bad.add_mparam("namespace", "bookmarks").unwrap();
        builder.add_part(bad);
        let bytes = futures::executor::block_on(builder.build()).unwrap().into_inner();
        let requires = [".hg/requires", ".hg/store/requires"].map(|file| fixture.0.join("target").join(file));
        let before = requires.clone().map(|path| fs::read(path).unwrap());
        let err = fixture.push(&bytes, Some(&store), 0).err().expect("bad pushkey part");
        assert!(err.to_string().contains("pushkey missing key"), "{err}");
        assert_eq!(requires.map(|path| fs::read(path).unwrap()), before);
        let repo = fixture.repo();
        assert!(!repo.get_requirements().contains(&Required::Lfs));
        assert_eq!(repo.changelog_revlog().len().unwrap(), 1);
        hg(&fixture.0.join("target"), &["verify"]);
    }

    /// The real entry point on the server's kind of runtime: every wire
    /// protocol ends in `RepoClient::unbundle`.
    #[test]
    fn unbundle_checks_blobs_from_a_current_thread_runtime() {
        let fixture = Fixture::new();
        let store = fixture.store();
        let bundle = fixture.cg03(|chunk| vec![as_pointer(chunk, b"blob")]);
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let options = RepoOptions {
            lfs: Some(LfsServing { store: Arc::clone(&store), public_url: None }),
            ..RepoOptions::default()
        };
        let repo = Arc::new(Repo::new(&logger, &fixture.0.join("target/.hg"), options).unwrap());
        let heads: Vec<_> = repo.hgrepo().changelog_revlog().get_heads().unwrap().into_iter().collect();
        let client = RepoClient::new(Arc::clone(&repo), &logger);
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

        let refused = runtime.block_on(client.unbundle(heads.clone(), Bytes::from(bundle.clone()))).unwrap();
        let hgproto::Response::Unbundle(reply) = refused else { panic!("not a bundle2 reply") };
        assert!(reply.windows(14).any(|bytes| bytes == b"is not on the "), "{reply:?}");
        assert_eq!(repo.hgrepo().changelog_revlog().len().unwrap(), 1);

        fixture.put(&store, b"blob");
        let pushed = runtime.block_on(client.unbundle(heads, Bytes::from(bundle))).unwrap();
        assert!(matches!(pushed, hgproto::Response::Unbundle(_)));
        assert_eq!(repo.hgrepo().changelog_revlog().len().unwrap(), 2);
        assert!(repo.hgrepo().get_requirements().contains(&Required::Lfs));
    }

    #[test]
    fn hostile_manifest_delta_rolls_back() {
        let fixture = Fixture::new();
        let mut parts = cg_parts(&fixture.bundle("none-v2;cg.version=03"));
        for part in &mut parts {
            if let Part::CgChunk(Section::Manifest, chunk) = part {
                chunk.base = chunk.p1;
                chunk.delta = Delta::new(vec![Fragment { start: 1000, end: 2000, content: Vec::new() }]).unwrap();
            }
        }
        let bytes = futures::executor::block_on(cg03_builder(parts).build()).unwrap().into_inner();
        fixture.assert_rejected(fixture.push(&bytes, None, 0), "outside its base");
    }

    #[test]
    fn store_failure_reaches_the_client_as_a_fixed_message() {
        let fixture = Fixture::new();
        // Nothing listens there; this also runs a real HTTP client under the
        // blocking pool's `block_on`.
        let s3 = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name("bucket")
            .with_region("region")
            .with_endpoint("http://127.0.0.1:1")
            .with_allow_http(true)
            .with_skip_signature(true)
            .with_retry(object_store::RetryConfig { max_retries: 0, ..Default::default() })
            .build()
            .unwrap();
        let store = Arc::new(LfsStore::with_stores(Arc::new(s3), None));
        let bundle = fixture.cg03(|chunk| vec![as_pointer(chunk, b"blob")]);
        fixture.assert_rejected(fixture.push(&bundle, Some(&store), 0), "LFS storage error");
    }

    #[test]
    fn retry_after_crash_adds_the_missing_requirement() {
        let fixture = Fixture::new();
        let store = fixture.store();
        fixture.put(&store, b"blob");
        let bundle = fixture.cg03(|chunk| vec![as_pointer(chunk, b"blob")]);
        fixture.push(&bundle, Some(&store), 0).map(drop).unwrap();
        // The state a crash between the appends and `add_requirement` leaves.
        for file in [".hg/requires", ".hg/store/requires"] {
            let path = fixture.0.join("target").join(file);
            let kept: Vec<_> = fs::read_to_string(&path).unwrap().lines()
                .filter(|line| *line != "lfs").map(|line| format!("{line}\n")).collect();
            fs::write(path, kept.concat()).unwrap();
        }
        assert!(!fixture.repo().get_requirements().contains(&Required::Lfs));
        fixture.push(&bundle, Some(&store), 0).map(drop).unwrap();
        assert!(fixture.repo().get_requirements().contains(&Required::Lfs));
    }

    #[test]
    fn hg10_compressions_decode() {
        for (kind, magic) in [
            ("gzip-v1", b"HG10GZ"),
            ("bzip2-v1", b"HG10BZ"),
            ("none-v1", b"HG10UN"),
        ] {
            let fixture = Fixture::new();
            let bundle = fixture.bundle(kind);
            assert!(bundle.starts_with(magic));
            fixture.apply(&bundle);
        }
    }

    #[test]
    fn headerless_cg01_is_applied() {
        let fixture = Fixture::new();
        let bundle = fixture.bundle("none-v1");
        fixture.apply(&bundle[6..]);
    }

    #[test]
    fn bundle2_cg01_part_is_applied() {
        let fixture = Fixture::new();
        // Mercurial's bundle command selects HG10 for cg.version=01 even
        // with none-v2. Wrap its stock V1 payload in an actual bundle2 part.
        let raw = fixture.bundle("none-v2;cg.version=01");
        fixture.apply(&bundle2_v1(&raw, true));
    }

    #[test]
    fn bundle2_cg01_missing_version_is_applied() {
        let fixture = Fixture::new();
        let raw = fixture.bundle("none-v1");
        fixture.apply(&bundle2_v1(&raw, false));
    }

    fn bundle2_v1(raw: &[u8], version: bool) -> Vec<u8> {
        assert!(raw.starts_with(b"HG10UN"));
        let mut builder = mercurial_bundles::Bundle2EncodeBuilder::new(Cursor::new(Vec::new()));
        builder.set_compressor_type(CompressorType::Uncompressed);
        let mut part = PartEncodeBuilder::mandatory("changegroup").unwrap();
        if version {
            part.add_mparam("version", "01").unwrap();
        }
        part.set_data_bytes(Bytes::copy_from_slice(&raw[6..]))
            .unwrap();
        builder.add_part(part);
        futures::executor::block_on(builder.build())
            .unwrap()
            .into_inner()
    }

    #[test]
    fn malformed_hg10_compression_does_not_write() {
        let fixture = Fixture::new();
        for kind in ["gzip-v1", "bzip2-v1", "none-v1"] {
            let mut bundle = fixture.bundle(kind);
            bundle.truncate(bundle.len() / 2);
            let repo = fixture.repo();
            let heads: Vec<_> = repo
                .changelog_revlog()
                .get_heads()
                .unwrap()
                .into_iter()
                .collect();
            assert!(apply_repo(&repo, &heads, &bundle, &NO_POLICY).is_err(), "{kind}");
            assert_eq!(repo.changelog_revlog().len().unwrap(), 1);
        }
    }
}
