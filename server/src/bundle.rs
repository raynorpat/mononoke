// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Changegroup history for `getbundle`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Future, Stream};
use mercurial::revlog::{Chunk, RevIdx, Revlog};
use mercurial::RevlogRepo;
use mercurial_bundles::changegroup::{CgDeltaChunk, Part, Section};
use mercurial_bundles::part_encode::PartEncodeBuilder;
use mercurial_bundles::parts::changegroup_part;
use mercurial_types::delta::{Delta, Fragment};
use mercurial_types::{BlobNode, Changeset, NodeHash, Parents, Path, NULL_HASH};

use crate::errors::{Error, Result};

pub fn missing_nodes(repo: &RevlogRepo, common: &[NodeHash]) -> Result<Vec<NodeHash>> {
    let revs = revisions(repo)?;
    let mut index = HashMap::with_capacity(revs.len());
    for (idx, rev) in revs.iter().enumerate() {
        index.insert(rev.node, idx);
    }

    let mut excluded = vec![false; revs.len()];
    for node in common {
        // NULL_HASH is not a changelog node and does not exclude real revisions.
        if *node == NULL_HASH {
            continue;
        }
        let Some(&start) = index.get(node) else {
            continue;
        };
        let mut stack = vec![start];
        while let Some(idx) = stack.pop() {
            if excluded[idx] {
                continue;
            }
            excluded[idx] = true;
            for parent in &revs[idx].parents {
                if let Some(&parent_idx) = index.get(parent) {
                    stack.push(parent_idx);
                }
            }
        }
    }

    Ok(revs
        .into_iter()
        .enumerate()
        .filter(|(idx, _)| !excluded[*idx])
        .map(|(_, rev)| rev.node)
        .collect())
}

pub fn history_part(repo: &RevlogRepo, common: &[NodeHash]) -> Result<PartEncodeBuilder> {
    let parts = history_parts(repo, common)?;
    changegroup_part(parts).map_err(|err| Error::from(err.to_string()))
}

fn history_parts(repo: &RevlogRepo, common: &[NodeHash]) -> Result<Vec<Part>> {
    let missing = missing_nodes(repo, common)?;
    let missing_set: HashSet<NodeHash> = missing.iter().copied().collect();
    let nodes = changelog_nodes(repo)?;
    let mut available: HashSet<NodeHash> = common
        .iter()
        .copied()
        .filter(|node| *node != NULL_HASH)
        .collect();

    let mut parts = Vec::new();
    let mut seen_manifests = HashSet::new();
    let mut manifest_revs = Vec::new();
    let mut seen_files: HashMap<Path, HashSet<NodeHash>> = HashMap::new();
    let mut file_revs: BTreeMap<Path, Vec<(NodeHash, NodeHash)>> = BTreeMap::new();

    for node in nodes {
        let cs = ready(repo.get_changeset_by_nodeid(&node))??;
        let manifest_id = *cs.manifestid();
        let missing_cs = missing_set.contains(&node);
        if !missing_cs {
            available.insert(manifest_id);
        }
        if seen_manifests.insert(manifest_id) && missing_cs {
            manifest_revs.push((manifest_id, node));
        }

        if !cs.files().is_empty() {
            let manifest = ready(repo.get_manifest_by_nodeid(&manifest_id))??;
            if !missing_cs {
                for (_, details) in manifest.manifest() {
                    available.insert(*details.nodeid());
                }
            }
            for file in cs.files() {
                let Some(details) = manifest.lookup(file) else {
                    continue;
                };
                let filenode = *details.nodeid();
                let seen = seen_files.entry(file.clone()).or_default();
                if seen.insert(filenode) && missing_cs {
                    file_revs
                        .entry(file.clone())
                        .or_default()
                        .push((filenode, node));
                }
            }
        }

        if missing_cs {
            let blob = ready(repo.get_changeset_blob_by_nodeid(&node))??;
            let renamed = blob.maybe_renamed();
            let parents = *blob.parents();
            let text = full_text(&blob)?;
            parts.push(fulltext_chunk(
                Section::Changeset,
                node,
                &parents,
                renamed,
                node,
                text,
            )?);
        }
    }
    parts.push(Part::SectionEnd(Section::Changeset));

    let manifest_log = repo.manifest_revlog();
    let mut ordered_manifests = Vec::with_capacity(manifest_revs.len());
    for (manifest_id, linknode) in manifest_revs {
        ordered_manifests.push((manifest_log.index_of(&manifest_id)?, manifest_id, linknode));
    }
    ordered_manifests.sort_by_key(|(idx, _, _)| *idx);
    for (_, manifest_id, linknode) in ordered_manifests {
        parts.push(stored_revision(
            &manifest_log,
            Section::Manifest,
            manifest_id,
            linknode,
            &available,
        )?);
        available.insert(manifest_id);
    }
    parts.push(Part::SectionEnd(Section::Manifest));

    for (path, revs) in file_revs {
        let revlog = repo.get_file_revlog(&path)?;
        let mut ordered = Vec::with_capacity(revs.len());
        for (filenode, linknode) in revs {
            ordered.push((revlog.index_of(&filenode)?, filenode, linknode));
        }
        ordered.sort_by_key(|(idx, _, _)| *idx);
        for (_, filenode, linknode) in ordered {
            parts.push(stored_revision(
                &revlog,
                Section::Filelog(path.clone()),
                filenode,
                linknode,
                &available,
            )?);
            available.insert(filenode);
        }
        parts.push(Part::SectionEnd(Section::Filelog(path)));
    }
    parts.push(Part::End);

    Ok(parts)
}

fn indexed_parents(revlog: &Revlog, idx: RevIdx) -> Result<(Parents, bool)> {
    let entry = revlog.get_entry(idx)?;
    let p1 = match entry.p1 {
        Some(parent) => Some(revlog.get_entry(parent)?.nodeid),
        None => None,
    };
    let p2 = match entry.p2 {
        Some(parent) => Some(revlog.get_entry(parent)?.nodeid),
        None => None,
    };
    let blob = BlobNode::new(Vec::<u8>::new(), p1.as_ref(), p2.as_ref());
    Ok((*blob.parents(), blob.maybe_renamed()))
}

fn stored_revision(
    revlog: &Revlog,
    section: Section,
    node: NodeHash,
    linknode: NodeHash,
    available: &HashSet<NodeHash>,
) -> Result<Part> {
    let idx = RevIdx::from(revlog.index_of(&node)?);
    let (parents, renamed) = indexed_parents(revlog, idx)?;
    match revlog.get_chunk(idx)? {
        Chunk::Literal(text) => fulltext_chunk(section, node, &parents, renamed, linknode, text),
        Chunk::Deltas(base_idx, deltas) => {
            let base = revlog.get_entry(base_idx)?.nodeid;
            if available.contains(&base) {
                let (p1, p2) = parent_nodes(&parents, renamed);
                let fragments = deltas
                    .into_iter()
                    .map(|delta| Fragment {
                        start: delta.start,
                        end: delta.end,
                        content: delta.content,
                    })
                    .collect();
                Ok(Part::CgChunk(
                    section,
                    CgDeltaChunk {
                        node,
                        p1,
                        p2,
                        base,
                        linknode,
                        delta: Delta::new(fragments).map_err(|err| Error::from(err.to_string()))?,
                    },
                ))
            } else {
                let blob = revlog.get_rev_by_nodeid(&node)?;
                let text = full_text(&blob)?;
                fulltext_chunk(section, node, &parents, renamed, linknode, text)
            }
        }
    }
}

struct Revision {
    node: NodeHash,
    parents: Vec<NodeHash>,
}

fn revisions(repo: &RevlogRepo) -> Result<Vec<Revision>> {
    let nodes = changelog_nodes(repo)?;
    let mut revs = Vec::with_capacity(nodes.len());
    for node in nodes {
        let cs = ready(repo.get_changeset_by_nodeid(&node))??;
        let parents = cs
            .parents()
            .into_iter()
            .filter(|parent| *parent != NULL_HASH)
            .collect();
        revs.push(Revision { node, parents });
    }
    Ok(revs)
}

fn changelog_nodes(repo: &RevlogRepo) -> Result<Vec<NodeHash>> {
    let mut stream = repo.changesets();
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut nodes = Vec::new();
    loop {
        match Pin::new(&mut stream).poll_next(&mut cx) {
            Poll::Ready(Some(item)) => nodes.push(item?),
            Poll::Ready(None) => return Ok(nodes),
            Poll::Pending => return Err("changelog walk would block".into()),
        }
    }
}

fn ready<T, F>(fut: F) -> Result<T>
where
    F: Future<Output = T> + Unpin,
{
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut fut = fut;
    match Pin::new(&mut fut).poll(&mut cx) {
        Poll::Ready(value) => Ok(value),
        Poll::Pending => Err("revlog future would block".into()),
    }
}

fn full_text(node: &BlobNode) -> Result<Vec<u8>> {
    node.as_blob()
        .as_slice()
        .map(|text| text.to_vec())
        .ok_or_else(|| Error::from("revision has no data"))
}

fn fulltext_delta(text: Vec<u8>) -> Result<Delta> {
    Delta::new(vec![Fragment {
        start: 0,
        end: 0,
        content: text,
    }])
    .map_err(|err| Error::from(err.to_string()))
}

fn parent_nodes(parents: &Parents, renamed: bool) -> (NodeHash, NodeHash) {
    match *parents {
        Parents::None => (NULL_HASH, NULL_HASH),
        // BlobNode stores a p2-only filelog parent as Parents::One with renamed set.
        Parents::One(p) if renamed => (NULL_HASH, p),
        Parents::One(p) => (p, NULL_HASH),
        Parents::Two(p1, p2) => (p1, p2),
    }
}

fn fulltext_chunk(
    section: Section,
    node: NodeHash,
    parents: &Parents,
    renamed: bool,
    linknode: NodeHash,
    text: Vec<u8>,
) -> Result<Part> {
    let (p1, p2) = parent_nodes(parents, renamed);
    Ok(Part::CgChunk(
        section,
        CgDeltaChunk {
            node,
            p1,
            p2,
            base: NULL_HASH,
            linknode,
            delta: fulltext_delta(text)?,
        },
    ))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use mercurial::revlog::{RevIdx, Revlog};

    use super::*;

    fn sample_parent() -> NodeHash {
        NodeHash::from_bytes(&[0x11; 20]).expect("parent")
    }

    fn file_chunk(blob: &BlobNode) -> CgDeltaChunk {
        let text = full_text(blob).expect("text");
        match fulltext_chunk(
            Section::Filelog(Path::new(b"a.txt").expect("path")),
            blob.nodeid().expect("filenode"),
            blob.parents(),
            blob.maybe_renamed(),
            NULL_HASH,
            text,
        )
        .expect("chunk")
        {
            Part::CgChunk(_, chunk) => chunk,
            Part::SectionEnd(_) | Part::End => panic!("expected a filelog chunk"),
        }
    }

    #[test]
    fn renamed_filelog_parent_stays_in_p2() {
        let parent = sample_parent();
        let renamed = BlobNode::new(b"copied\n".to_vec(), None, Some(&parent));
        assert!(renamed.maybe_renamed());
        assert_eq!(*renamed.parents(), Parents::One(parent));

        let chunk = file_chunk(&renamed);
        assert_eq!(
            (chunk.p1, chunk.p2),
            (NULL_HASH, parent),
            "p2-only parent encoded as ({:?}, {:?})",
            chunk.p1,
            chunk.p2
        );

        let plain = BlobNode::new(b"plain\n".to_vec(), Some(&parent), None);
        assert!(!plain.maybe_renamed());
        let chunk = file_chunk(&plain);
        assert_eq!((chunk.p1, chunk.p2), (parent, NULL_HASH));
    }

    struct TempRepo {
        root: std::path::PathBuf,
        repo: std::path::PathBuf,
    }

    impl Drop for TempRepo {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn hg(repo: &std::path::Path, args: &[&str]) -> std::process::Output {
        let mut cmd = std::process::Command::new("hg");
        cmd.current_dir(repo)
            .args(args)
            .env("HGPLAIN", "1")
            .env("HGRCPATH", "");
        cmd.output()
            .unwrap_or_else(|err| panic!("spawn hg {args:?}: {err}"))
    }

    fn assert_hg(output: &std::process::Output, what: &str) {
        assert!(
            output.status.success(),
            "{what} failed: status {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn read_u32(bytes: &[u8]) -> u32 {
        u32::from_be_bytes(bytes.try_into().expect("u32"))
    }

    fn pack_index_entry(
        offset: u64,
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
        entry[8..12].copy_from_slice(&compressed_len.to_be_bytes());
        entry[12..16].copy_from_slice(&uncompressed_len.to_be_bytes());
        entry[16..20].copy_from_slice(&baserev.to_be_bytes());
        entry[20..24].copy_from_slice(&linkrev.to_be_bytes());
        entry[24..28].copy_from_slice(&p1.to_be_bytes());
        entry[28..32].copy_from_slice(&p2.to_be_bytes());
        entry[32..52].copy_from_slice(&node[..20]);
        entry
    }

    /// `u` + one bdiff fragment replacing `old_len` bytes at offset 0.
    fn uncompressed_replace(old_len: u32, text: &[u8]) -> Vec<u8> {
        let mut chunk = Vec::with_capacity(13 + text.len());
        chunk.push(b'u');
        chunk.extend_from_slice(&0u32.to_be_bytes());
        chunk.extend_from_slice(&old_len.to_be_bytes());
        chunk.extend_from_slice(&(text.len() as u32).to_be_bytes());
        chunk.extend_from_slice(text);
        chunk
    }

    /// Two commits of `a.txt` in a share-safe generaldelta repo.
    ///
    /// Mercurial 7.0.3 writes the share-safe requires split and an inline
    /// generaldelta filelog (`00 03`), but stores rev 1 as a snapshot
    /// (`baserev == self`) because the delta is larger than `beta\n`.
    /// The index is rewritten so rev 1 is a delta against rev 0. The node id
    /// hg recorded is kept.
    fn init_share_safe_repo() -> TempRepo {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "mononoke-bundle-delta-{}-{}",
            std::process::id(),
            nanos
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("temp root");

        let mut init = std::process::Command::new("hg");
        init.current_dir(&root)
            .args(["init", "repo"])
            .args(["--config", "format.use-share-safe=true"])
            .args(["--config", "format.generaldelta=true"])
            .args(["--config", "format.revlog-compression=zstd"])
            .args(["--config", "format.sparse-revlog=true"])
            .args(["--config", "format.use-persistent-nodemap=false"])
            .args(["--config", "format.use-dirstate-v2=false"])
            .args([
                "--config",
                "format.exp-use-copies-side-data-changeset=false",
            ])
            .env("HGPLAIN", "1")
            .env("HGRCPATH", "");
        assert_hg(&init.output().expect("spawn hg init"), "hg init");

        let repo = root.join("repo");
        std::fs::write(repo.join("a.txt"), b"alpha\n").expect("write a.txt");
        assert_hg(&hg(&repo, &["add", "a.txt"]), "hg add");
        assert_hg(
            &hg(&repo, &["commit", "-m", "first", "-u", "test"]),
            "hg commit",
        );
        std::fs::write(repo.join("a.txt"), b"beta\n").expect("rewrite a.txt");
        assert_hg(
            &hg(&repo, &["commit", "-m", "second", "-u", "test"]),
            "hg commit",
        );

        let requires =
            std::fs::read_to_string(repo.join(".hg").join("requires")).expect("requires");
        assert!(
            requires.lines().any(|line| line == "share-safe"),
            "requires missing share-safe:\n{requires}"
        );
        let store_requires =
            std::fs::read_to_string(repo.join(".hg").join("store").join("requires"))
                .expect("store requires");
        for line in [
            "sparserevlog",
            "generaldelta",
            "revlogv1",
            "revlog-compression-zstd",
        ] {
            assert!(
                store_requires.lines().any(|got| got == line),
                "store/requires missing {line}:\n{store_requires}"
            );
        }

        let index_path = repo.join(".hg").join("store").join("data").join("a.txt.i");
        let original = std::fs::read(&index_path).expect("filelog index");
        assert_eq!(&original[..2], &[0x00, 0x03], "filelog feature bytes");
        let rev0_clen = read_u32(&original[8..12]) as usize;
        let rev1_off = 64 + rev0_clen;
        let baserev = read_u32(&original[rev1_off + 16..rev1_off + 20]);
        if baserev != 0 {
            let node = &original[rev1_off + 32..rev1_off + 52];
            let old_len = read_u32(&original[12..16]);
            let chunk = uncompressed_replace(old_len, b"beta\n");
            let entry = pack_index_entry(
                rev0_clen as u64,
                chunk.len() as u32,
                b"beta\n".len() as u32,
                0,
                1,
                0,
                u32::MAX,
                node,
            );
            let mut rebuilt = original[..rev1_off].to_vec();
            rebuilt.extend_from_slice(&entry);
            rebuilt.extend_from_slice(&chunk);
            std::fs::write(&index_path, rebuilt).expect("rewrite filelog");
        }

        let revlog = Revlog::from_idx_data(&index_path, None::<String>).expect("filelog");
        let entry = revlog.get_entry(RevIdx::from(1u32)).expect("rev 1");
        assert_eq!(entry.baserev, Some(RevIdx::from(0u32)));
        assert_eq!(
            revlog
                .get_rev(RevIdx::from(0u32))
                .expect("rev 0")
                .as_blob()
                .as_slice(),
            Some(&b"alpha\n"[..])
        );
        assert_eq!(
            revlog
                .get_rev(RevIdx::from(1u32))
                .expect("rev 1")
                .as_blob()
                .as_slice(),
            Some(&b"beta\n"[..])
        );

        TempRepo { root, repo }
    }

    fn filelog_chunks(parts: &[Part]) -> Vec<&CgDeltaChunk> {
        parts
            .iter()
            .filter_map(|part| match part {
                Part::CgChunk(Section::Filelog(path), chunk) if path.as_slice() == b"a.txt" => {
                    Some(chunk)
                }
                _ => None,
            })
            .collect()
    }

    fn open_repo(repo: &std::path::Path) -> RevlogRepo {
        RevlogRepo::open(repo.join(".hg")).expect("open repo")
    }

    #[test]
    fn second_file_revision_uses_stored_base() {
        let temp = init_share_safe_repo();
        let repo = open_repo(&temp.repo);
        let parts = history_parts(&repo, &[]).expect("history");
        let chunks = filelog_chunks(&parts);
        assert_eq!(chunks.len(), 2, "filelog chunks");
        assert_eq!(chunks[0].base, NULL_HASH, "first file base");
        assert_eq!(
            chunks[1].base, chunks[0].node,
            "second file chunk base is NULL_HASH"
        );
        assert_ne!(chunks[1].base, NULL_HASH);
        assert_eq!(
            mercurial_types::delta::apply(b"alpha\n", chunks[1].delta.clone()),
            b"beta\n"
        );
        for part in &parts {
            if let Part::CgChunk(Section::Changeset, chunk) = part {
                assert_eq!(chunk.base, NULL_HASH);
            }
        }
    }

    #[test]
    fn second_file_revision_keeps_its_base_when_client_has_it() {
        let temp = init_share_safe_repo();
        let logged = hg(&temp.repo, &["log", "-r", "0", "-T", "{node}"]);
        assert_hg(&logged, "hg log");
        let common = NodeHash::from_str(std::str::from_utf8(&logged.stdout).expect("utf8").trim())
            .expect("changeset node");
        let repo = open_repo(&temp.repo);
        let parts = history_parts(&repo, &[common]).expect("history");
        let chunks = filelog_chunks(&parts);
        assert_eq!(chunks.len(), 1, "filelog chunks");

        let path = Path::new(b"a.txt").expect("path");
        let filelog = repo.get_file_revlog(&path).expect("filelog");
        let base = filelog.get_entry(RevIdx::from(0u32)).expect("rev 0").nodeid;
        let rev1 = filelog.get_entry(RevIdx::from(1u32)).expect("rev 1").nodeid;
        assert_eq!(chunks[0].base, base);
        assert_eq!(chunks[0].node, rev1);
        assert!(parts.iter().all(|part| match part {
            Part::CgChunk(_, chunk) => chunk.node != base,
            _ => true,
        }));
    }
}
