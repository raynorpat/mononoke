// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Changegroup history for `getbundle`.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use futures::{Future, Stream};
use mercurial::revlog::{Chunk, RevIdx, Revlog};
use mercurial::RevlogRepo;
use mercurial_bundles::changegroup::{CgDeltaChunk, Part, Section};
use mercurial_bundles::part_encode::PartEncodeBuilder;
use mercurial_bundles::Error as BundleError;
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

/// Parts queued between the revlog reader and the bundle writer.
const PART_QUEUE: usize = 1024;

/// Changegroup part whose chunks are read from the revlogs while it is written.
///
/// Revlog reads block, so they run on tokio's blocking pool instead of the
/// connection's runtime thread. A dropped part stops the reader at its next send.
pub fn history_part(repo: RevlogRepo, common: Vec<NodeHash>) -> Result<PartEncodeBuilder> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(PART_QUEUE);
    tokio::task::spawn_blocking(move || {
        let sent = history_parts(&repo, &common, &mut |part| {
            tx.blocking_send(Ok(part))
                .map_err(|_| Error::from("getbundle writer went away"))
        });
        if let Err(err) = sent {
            let _ = tx.blocking_send(Err(BundleError::from(err.to_string())));
        }
    });
    let parts = futures::stream::poll_fn(move |cx| rx.poll_recv(cx));
    changegroup_part(parts).map_err(|err| Error::from(err.to_string()))
}

fn history_parts(
    repo: &RevlogRepo,
    common: &[NodeHash],
    emit: &mut dyn FnMut(Part) -> Result<()>,
) -> Result<()> {
    let missing_set: HashSet<NodeHash> = missing_nodes(repo, common)?.into_iter().collect();
    let nodes = changelog_nodes(repo)?;
    // Indexed by changelog rev: is this changeset going to the client.
    let missing: Vec<bool> = nodes
        .iter()
        .map(|node| missing_set.contains(node))
        .collect();

    let mut files = BTreeSet::new();
    for (rev, node) in nodes.iter().enumerate() {
        if !missing[rev] {
            continue;
        }
        let cs = ready(repo.get_changeset_by_nodeid(node))??;
        files.extend(cs.files().iter().cloned());
        let blob = ready(repo.get_changeset_blob_by_nodeid(node))??;
        let renamed = blob.maybe_renamed();
        let parents = *blob.parents();
        let text = full_text(&blob)?;
        emit(fulltext_chunk(
            Section::Changeset,
            *node,
            &parents,
            renamed,
            *node,
            text,
        )?)?;
    }
    emit(Part::SectionEnd(Section::Changeset))?;

    linked_revisions(
        &repo.manifest_revlog(),
        Section::Manifest,
        &nodes,
        &missing,
        emit,
    )?;
    emit(Part::SectionEnd(Section::Manifest))?;

    let files: Vec<Path> = files.into_iter().collect();
    read_in_order(
        &files,
        |path| file_parts(repo, path, &nodes, &missing),
        &mut |parts: Vec<Part>| parts.into_iter().try_for_each(&mut *emit),
    )?;
    emit(Part::End)
}

/// Threads reading files at once. The first read of a file waits on the
/// filesystem (about 6 ms on Windows) far longer than it uses the CPU or disk,
/// so many reads in flight keep the disk busy.
pub(crate) const READ_THREADS: usize = 16;
/// Items read per batch. Only one batch of results is held at a time.
const READ_BATCH: usize = 256;

/// `read` every item on `READ_THREADS` threads and pass the results to `emit`
/// in item order. Items are read a batch at a time, so a slow `emit` bounds memory.
pub(crate) fn read_in_order<T, R>(
    items: &[T],
    read: impl Fn(&T) -> Result<R> + Sync,
    emit: &mut dyn FnMut(R) -> Result<()>,
) -> Result<()>
where
    T: Sync,
    R: Send,
{
    for batch in items.chunks(READ_BATCH) {
        let next = AtomicUsize::new(0);
        let mut done: Vec<(usize, Result<R>)> = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..READ_THREADS.min(batch.len()))
                .map(|_| {
                    scope.spawn(|| {
                        let mut out = Vec::new();
                        loop {
                            let i = next.fetch_add(1, Ordering::Relaxed);
                            let Some(item) = batch.get(i) else {
                                return out;
                            };
                            out.push((i, read(item)));
                        }
                    })
                })
                .collect();
            workers
                .into_iter()
                .flat_map(|worker| worker.join().expect("read thread panicked"))
                .collect()
        });
        done.sort_unstable_by_key(|(i, _)| *i);
        for (_, result) in done {
            emit(result?)?;
        }
    }
    Ok(())
}

/// The parts for one filelog's missing revisions, ending with its `SectionEnd`,
/// or nothing when the client has every revision.
fn file_parts(
    repo: &RevlogRepo,
    path: &Path,
    nodes: &[NodeHash],
    missing: &[bool],
) -> Result<Vec<Part>> {
    let revlog = repo.read_file_revlog(path)?;
    let mut parts = Vec::new();
    if linked_revisions(
        &revlog,
        Section::Filelog(path.clone()),
        nodes,
        missing,
        &mut |part| {
            parts.push(part);
            Ok(())
        },
    )? {
        parts.push(Part::SectionEnd(Section::Filelog(path.clone())));
    }
    Ok(parts)
}

/// Emit every revision of `revlog` whose linkrev is a missing changeset, in revision order.
/// Returns whether any revision was emitted.
///
/// A revision linked to a common changeset is already on the client, and every other
/// earlier revision is sent before this one, so a stored delta base is always available.
fn linked_revisions(
    revlog: &Revlog,
    section: Section,
    nodes: &[NodeHash],
    missing: &[bool],
    emit: &mut dyn FnMut(Part) -> Result<()>,
) -> Result<bool> {
    let mut any = false;
    for idx in 0..revlog.len()? {
        let idx = RevIdx::from(idx);
        let entry = revlog.get_entry(idx)?;
        let linkrev = entry
            .linkrev
            .ok_or_else(|| Error::from("revision has no linkrev"))?
            .as_u32() as usize;
        // A linkrev past `missing` is a push that landed after the changelog was read.
        if !missing.get(linkrev).copied().unwrap_or(false) {
            continue;
        }
        emit(stored_revision(
            revlog,
            section.clone(),
            idx,
            entry.nodeid,
            nodes[linkrev],
        )?)?;
        any = true;
    }
    Ok(any)
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
    idx: RevIdx,
    node: NodeHash,
    linknode: NodeHash,
) -> Result<Part> {
    let (parents, renamed) = indexed_parents(revlog, idx)?;
    match revlog.get_chunk(idx)? {
        Chunk::Literal(text) => fulltext_chunk(section, node, &parents, renamed, linknode, text),
        Chunk::Deltas(base_idx, deltas) => {
            let base = revlog.get_entry(base_idx)?.nodeid;
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
    fn read_in_order_keeps_order_and_stops_at_an_error() {
        let items: Vec<usize> = (0..READ_BATCH * 2 + 7).collect();
        let mut got = Vec::new();
        read_in_order(&items, |i| Ok(*i * 2), &mut |r| {
            got.push(r);
            Ok(())
        })
        .expect("read");
        assert_eq!(got, items.iter().map(|i| i * 2).collect::<Vec<_>>());

        let mut got = Vec::new();
        let err = read_in_order(
            &items,
            |i| {
                if *i == READ_BATCH + 3 {
                    Err(Error::from("bad item"))
                } else {
                    Ok(*i)
                }
            },
            &mut |r| {
                got.push(r);
                Ok(())
            },
        )
        .expect_err("error item");
        assert_eq!(err.to_string(), "bad item");
        assert_eq!(got, (0..READ_BATCH + 3).collect::<Vec<_>>());
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

    fn collect_parts(repo: &RevlogRepo, common: &[NodeHash]) -> Vec<Part> {
        let mut parts = Vec::new();
        history_parts(repo, common, &mut |part| {
            parts.push(part);
            Ok(())
        })
        .expect("history");
        parts
    }

    fn open_repo(repo: &std::path::Path) -> RevlogRepo {
        RevlogRepo::open(repo.join(".hg")).expect("open repo")
    }

    #[test]
    fn second_file_revision_uses_stored_base() {
        let temp = init_share_safe_repo();
        let repo = open_repo(&temp.repo);
        let parts = collect_parts(&repo, &[]);
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
        let parts = collect_parts(&repo, &[common]);
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
