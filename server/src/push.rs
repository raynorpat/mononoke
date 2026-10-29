// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::io::{Cursor, Read};
use std::pin::Pin;
use std::task::{Context, Poll};

use async_compression::{CompressorType, Decompressor, DecompressorType};
use bookmarks::{Bookmarks, BookmarksMut, Version};
use bytes::{Bytes, BytesMut};
use futures::{FutureExt, Stream};
use mercurial::revlog::{Revlog, RevlogAppend, RevlogCompression};
use mercurial::revlogrepo::Required;
use mercurial::RevlogRepo;
use mercurial_bundles::bundle2::Bundle2Stream;
use mercurial_bundles::changegroup::{
    unpacker::Cg2Unpacker, CgDeltaChunk, CgVersion, Part, Section,
};
use mercurial_bundles::part_encode::PartEncodeBuilder;
use mercurial_bundles::{Bundle2Item, InnerPart, PartHeader};
use mercurial_types::delta::Delta;
use mercurial_types::{NodeHash, NULL_HASH};
use tokio_util::codec::Decoder;

use crate::errors::{Error, Result};

pub(crate) struct Applied {
    pub(crate) body: Vec<u8>,
    pub(crate) changegroup_ids: Vec<u32>,
    pub(crate) push_raced: bool,
    bundle1: bool,
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
    let bundle1 = is_bundle1(bundle);
    if !heads_match(repo, heads)? {
        return Ok(Applied {
            body: b"0\n".to_vec(),
            changegroup_ids: Vec::new(),
            push_raced: false,
            bundle1,
        });
    }
    if !bundle1 && !check_heads_agree(repo, bundle)? {
        return Ok(Applied {
            body: Vec::new(),
            changegroup_ids: Vec::new(),
            push_raced: true,
            bundle1,
        });
    }

    let compression = compression_of(repo);
    let mut groups: Vec<(Section, Vec<CgDeltaChunk>)> = Vec::new();
    let mut changegroup_ids = Vec::new();
    let mut cg_part = false;

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
                            use_treemanifest(repo)?;
                        }
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
    }

    for (section, chunks) in groups {
        append_group(repo, compression, &section, chunks)?;
    }

    Ok(Applied {
        body: b"1\n".to_vec(),
        changegroup_ids,
        push_raced: false,
        bundle1,
    })
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
    error_bundle("error:abort", message).await
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
                flags: _,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

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
            let applied = apply_repo(&repo, &heads, bundle).unwrap();
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
            assert!(apply_repo(&repo, &heads, &bundle).is_err(), "{kind}");
            assert_eq!(repo.changelog_revlog().len().unwrap(), 1);
        }
    }
}
