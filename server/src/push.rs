// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::io::Cursor;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_compression::CompressorType;
use bookmarks::{Bookmarks, BookmarksMut, Version};
use bytes::Bytes;
use futures::{FutureExt, Stream};
use mercurial::revlog::{Revlog, RevlogAppend, RevlogCompression};
use mercurial::revlogrepo::Required;
use mercurial::RevlogRepo;
use mercurial_bundles::bundle2::Bundle2Stream;
use mercurial_bundles::changegroup::{CgDeltaChunk, Part, Section};
use mercurial_bundles::part_encode::PartEncodeBuilder;
use mercurial_bundles::{Bundle2Item, InnerPart, PartHeader};
use mercurial_types::delta::Delta;
use mercurial_types::{NodeHash, NULL_HASH};

use crate::errors::{Error, Result};

pub(crate) struct Applied {
    pub(crate) body: Vec<u8>,
    pub(crate) changegroup_ids: Vec<u32>,
    pub(crate) push_raced: bool,
}

pub fn apply_unbundle(repo_path: &std::path::Path, heads: &[NodeHash], bundle: &[u8]) -> Result<Vec<u8>> {
    let repo = RevlogRepo::open(repo_path)?;
    Ok(apply_repo(&repo, heads, bundle)?.body)
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

pub(crate) fn apply_repo(repo: &RevlogRepo, heads: &[NodeHash], bundle: &[u8]) -> Result<Applied> {
    if !heads_match(repo, heads)? {
        return Ok(Applied {
            body: b"0\n".to_vec(),
            changegroup_ids: Vec::new(),
            push_raced: false,
        });
    }
    if !check_heads_agree(repo, bundle)? {
        return Ok(Applied {
            body: Vec::new(),
            changegroup_ids: Vec::new(),
            push_raced: true,
        });
    }

    let compression = compression_of(repo);
    let mut groups: Vec<(Section, Vec<CgDeltaChunk>)> = Vec::new();
    let mut changegroup_ids = Vec::new();
    let mut cg_part = false;

    drain_bundle(bundle, |item| {
        match item {
            Bundle2Item::Start(_) => {}
            Bundle2Item::Header(header) => {
                let kind = header.part_type_lower().as_str();
                cg_part = kind == "changegroup";
                if cg_part {
                    changegroup_ids.push(header.part_id());
                } else if kind == "pushkey" {
                    apply_pushkey(repo, &header)?;
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

    for (section, chunks) in groups {
        append_group(repo, compression, &section, chunks)?;
    }

    Ok(Applied {
        body: b"1\n".to_vec(),
        changegroup_ids,
        push_raced: false,
    })
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
    let mut bundle = mercurial_bundles::Bundle2EncodeBuilder::new(Cursor::new(Vec::new()));
    bundle.set_compressor_type(CompressorType::Uncompressed);
    let mut part = PartEncodeBuilder::mandatory("error:pushraced").map_err(bundle_err)?;
    part.add_mparam(
        "message",
        "remote repository changed while pushing - please try again",
    )
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
    let mut left = chunks;
    while !left.is_empty() {
        let batch = std::mem::take(&mut left);
        let mut progress = false;
        for chunk in batch {
            let mut revlog = revlog_for(repo, section)?;
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
                delta,
            } = chunk;
            // Keep the client's delta when it is against a revision we have.
            let stored_delta = (base != NULL_HASH).then(|| (base, delta.clone()));
            let text = full_text(&revlog, base, delta)?;
            revlog.append_revision(
                RevlogAppend {
                    node,
                    p1: parent_opt(p1),
                    p2: parent_opt(p2),
                    linkrev: Some(linkrev),
                    text,
                },
                stored_delta,
                compression,
            )?;
            progress = true;
        }
        if !progress {
            return Err("changegroup revision has no parent in the revlog".into());
        }
    }
    if let Section::Filelog(path) = section {
        repo.record_file_data(path)?;
    }
    Ok(())
}

fn revlog_for(repo: &RevlogRepo, section: &Section) -> Result<Revlog> {
    match section {
        Section::Changeset => Ok(repo.changelog_revlog()),
        Section::Manifest => Ok(repo.manifest_revlog()),
        Section::Filelog(path) => Ok(repo.file_revlog_for_append(path)?),
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
    Ok(mercurial_types::delta::apply(&base_text, delta))
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
