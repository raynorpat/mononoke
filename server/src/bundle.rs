// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Changegroup history for `getbundle`.

use std::collections::{BTreeSet, HashMap};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use futures::Future;
use futures::{StreamExt, TryStreamExt};
use mercurial::revlog::{Chunk, IdxFlags, RevIdx, Revlog};
use mercurial::revlogrepo::Required;
use mercurial::RevlogRepo;
use mercurial_bundles::changegroup::packer::Cg2Packer;
use mercurial_bundles::changegroup::{CgDeltaChunk, CgVersion, Part, Section};
use mercurial_bundles::part_encode::PartEncodeBuilder;
use mercurial_bundles::parts::changegroup_part_version;
use mercurial_bundles::Error as BundleError;
use mercurial_types::delta::{Delta, Fragment};
use mercurial_types::textdiff::textdiff;
use mercurial_types::{BlobNode, Changeset, NodeHash, Parents, Path, NULL_HASH};

use crate::errors::{Error, Result};

/// Why changegroup 01 or 02 cannot carry an LFS repository: only 03 has revision flags.
pub const LFS_REQUIRED: &str =
    "this repository uses LFS; enable the lfs extension (hg --config extensions.lfs= ...)";

/// Changelog nodes in revision order, and for each whether to send it: an
/// ancestor of a `heads` node (every changeset when `heads` is empty) that is
/// not an ancestor of a `common` node.
///
/// Parents come from the changelog index, so no changeset text is read.
fn missing_revs(
    repo: &RevlogRepo,
    common: &[NodeHash],
    heads: &[NodeHash],
) -> Result<(Vec<NodeHash>, Vec<bool>)> {
    let changelog = repo.changelog_revlog();
    let len = changelog.len()? as usize;
    let mut nodes = Vec::with_capacity(len);
    let mut parents = Vec::with_capacity(len);
    for idx in 0..len {
        let entry = changelog.get_entry(RevIdx::from(idx))?;
        nodes.push(entry.nodeid);
        parents.push([entry.p1, entry.p2]);
    }
    // Mark `start` and its ancestors with `to`, stopping where already marked.
    let mark = |flags: &mut Vec<bool>, start: usize, to: bool| {
        let mut stack = vec![start];
        while let Some(idx) = stack.pop() {
            if flags[idx] == to {
                continue;
            }
            flags[idx] = to;
            stack.extend(parents[idx].iter().flatten().map(|p| p.as_u32() as usize));
        }
    };

    // NULL_HASH is not a changelog node, so it neither adds nor excludes revisions.
    let mut missing = vec![heads.is_empty(); len];
    for node in heads.iter().filter(|node| **node != NULL_HASH) {
        let start = changelog
            .index_of(node)
            .map_err(|_| Error::from(format!("unknown revision '{node}'")))?;
        mark(&mut missing, start as usize, true);
    }
    for node in common {
        if *node == NULL_HASH {
            continue;
        }
        // The client may have commits this server does not.
        let Ok(start) = changelog.index_of(node) else {
            continue;
        };
        mark(&mut missing, start as usize, false);
    }
    Ok((nodes, missing))
}

pub enum Selection {
    Common {
        common: Vec<NodeHash>,
        heads: Vec<NodeHash>,
    },
    Roots {
        roots: Vec<NodeHash>,
        heads: Vec<NodeHash>,
    },
}

/// Descendants of roots that are also ancestors of heads, including the roots.
fn missing_from_roots(
    repo: &RevlogRepo,
    roots: &[NodeHash],
    heads: &[NodeHash],
) -> Result<(Vec<NodeHash>, Vec<bool>)> {
    let (nodes, mut missing) = missing_revs(repo, &[], heads)?;
    // A subset with no heads selects nothing, unlike omitted getbundle heads.
    if heads.is_empty() {
        missing.fill(false);
        return Ok((nodes, missing));
    }
    if roots.contains(&NULL_HASH) {
        return Ok((nodes, missing));
    }
    let changelog = repo.changelog_revlog();
    let mut descendants = vec![false; nodes.len()];
    for root in roots {
        descendants[changelog.index_of(root)? as usize] = true;
    }
    for rev in 0..nodes.len() {
        let entry = changelog.get_entry(RevIdx::from(rev))?;
        descendants[rev] |= [entry.p1, entry.p2]
            .iter()
            .flatten()
            .any(|p| descendants[p.as_u32() as usize]);
        missing[rev] &= descendants[rev];
    }
    Ok((nodes, missing))
}

/// Parts queued between the revlog reader and the bundle writer.
const PART_QUEUE: usize = 1024;

/// Changegroup part whose chunks are read from the revlogs while it is written.
///
/// Revlog reads block, so they run on tokio's blocking pool instead of the
/// connection's runtime thread. A dropped part stops the reader at its next send.
pub fn history_part(
    repo: RevlogRepo,
    selection: Selection,
    version: CgVersion,
) -> Result<PartEncodeBuilder> {
    let treemanifest = repo.is_treemanifest();
    let parts = history_stream(repo, selection, version);
    let mut part =
        changegroup_part_version(parts, version).map_err(|err| Error::from(err.to_string()))?;
    if treemanifest {
        part.add_mparam("treemanifest", "1")
            .map_err(|err| Error::from(err.to_string()))?;
    }
    Ok(part)
}

fn history_stream(
    repo: RevlogRepo,
    selection: Selection,
    version: CgVersion,
) -> futures::stream::BoxStream<'static, std::result::Result<Part, BundleError>> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(PART_QUEUE);
    tokio::task::spawn_blocking(move || {
        let sent = history_parts(&repo, &selection, version, &mut |part| {
            tx.blocking_send(Ok(part))
                .map_err(|_| Error::from("getbundle writer went away"))
        });
        if let Err(err) = sent {
            let _ = tx.blocking_send(Err(BundleError::from(err.to_string())));
        }
    });
    futures::stream::poll_fn(move |cx| rx.poll_recv(cx)).boxed()
}

/// Raw changegroup 01, without a bundle header or bundle2 framing.
pub fn legacy_changegroup(repo: RevlogRepo, selection: Selection) -> Result<hgproto::BundleStream> {
    if repo.is_treemanifest() {
        return Err("cannot produce changegroup 01 for a treemanifest repository".into());
    }
    if repo.get_requirements().contains(&Required::Lfs) {
        return Err(LFS_REQUIRED.into());
    }
    let parts = history_stream(repo, selection, CgVersion::V1);
    let body = Cg2Packer::with_version(parts, CgVersion::V1)
        .and_then(|chunk| futures::future::ready(chunk.into_bytes()))
        .map_err(hgproto::Error::from)
        .boxed();
    Ok(hgproto::BundleStream {
        body,
        prefer_uncompressed: false,
    })
}

fn history_parts(
    repo: &RevlogRepo,
    selection: &Selection,
    version: CgVersion,
    emit: &mut dyn FnMut(Part) -> Result<()>,
) -> Result<()> {
    let (nodes, missing) = match selection {
        Selection::Common { common, heads } => missing_revs(repo, common, heads)?,
        Selection::Roots { roots, heads } => missing_from_roots(repo, roots, heads)?,
    };

    // A restricted pull may refer to nodes first stored on an unselected sibling.
    // In that case use the requested changesets' manifests, as Mercurial's
    // non-fastpath packer does, rather than selecting by stored linkrev alone.
    let heads = match selection {
        Selection::Common { heads, .. } | Selection::Roots { heads, .. } => heads,
    };
    let restricted = repo.is_treemanifest()
        || missing_revs(repo, &[], heads)?
            .1
            .iter()
            .any(|selected| !selected);
    let mut manifest_links = HashMap::new();

    let mut files = BTreeSet::new();
    for (rev, node) in nodes.iter().enumerate() {
        if !missing[rev] {
            continue;
        }
        check_flags(&repo.changelog_revlog(), &Section::Changeset, RevIdx::from(rev), version)?;
        let cs = ready(repo.get_changeset_by_nodeid(node))??;
        files.extend(cs.files().iter().cloned());
        if restricted && *cs.manifestid() != NULL_HASH {
            manifest_links.entry(*cs.manifestid()).or_insert(*node);
        }
        if version == CgVersion::V1 {
            continue;
        }
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
            0,
            text,
        )?)?;
    }
    if version == CgVersion::V1 {
        linked_revisions(
            &repo.changelog_revlog(),
            Section::Changeset,
            &nodes,
            &missing,
            version,
            emit,
        )?;
    }
    emit(Part::SectionEnd(Section::Changeset))?;

    let mut file_links: HashMap<Path, HashMap<NodeHash, NodeHash>> = HashMap::new();
    let mut tree_links: std::collections::BTreeMap<Path, HashMap<NodeHash, NodeHash>> = Default::default();
    let common = if restricted {
        let common_heads = match selection {
            Selection::Common { common, .. } => common.clone(),
            Selection::Roots { .. } => {
                let mut common = Vec::new();
                let changelog = repo.changelog_revlog();
                for (rev, selected) in missing.iter().enumerate() {
                    if *selected {
                        let entry = changelog.get_entry(RevIdx::from(rev))?;
                        for parent in [entry.p1, entry.p2].into_iter().flatten() {
                            let parent = parent.as_u32() as usize;
                            if !missing[parent] {
                                common.push(nodes[parent]);
                            }
                        }
                    }
                }
                common
            }
        };
        let common: Vec<bool> = missing_revs(repo, &common_heads, &[])?
            .1
            .into_iter()
            .map(|missing| !missing)
            .collect();
        // Iterate manifests in changeset order so each filenode gets its first
        // outgoing changeset as linknode.
        let mut visited_trees = std::collections::HashSet::new();
        for (rev, selected) in missing.iter().enumerate() {
            if !selected {
                continue;
            }
            let cs = ready(repo.get_changeset_by_nodeid(&nodes[rev]))??;
            if repo.is_treemanifest() {
                // Walk references, not storage linkrevs: identical manifests and
                // files may first have been stored on an unselected sibling.
                // A tree repository can also contain flat root manifests.
                let mut pending = vec![(Vec::new(), *cs.manifestid())];
                while let Some((prefix, node)) = pending.pop() {
                    if node == NULL_HASH || !visited_trees.insert((prefix.clone(), node)) { continue; }
                    let log = if prefix.is_empty() {
                        repo.manifest_revlog()
                    } else {
                        repo.read_dir_revlog(&Path::new(&prefix).map_err(|err| Error::from(err.to_string()))?)?
                    };
                    let text = full_text(&log.get_rev_by_nodeid(&node)?)?;
                    for (path, details) in mercurial::manifest::revlog::parse(&text)? {
                        let mut full = prefix.clone();
                        if !full.is_empty() { full.push(b'/'); }
                        full.extend_from_slice(path.as_slice());
                        let path = Path::new(&full).map_err(|err| Error::from(err.to_string()))?;
                        if details.is_tree() {
                            tree_links.entry(path).or_default().entry(*details.nodeid()).or_insert(nodes[rev]);
                            pending.push((full, *details.nodeid()));
                        } else if files.contains(&path) {
                            file_links.entry(path).or_default().entry(*details.nodeid()).or_insert(nodes[rev]);
                        }
                    }
                }
                continue;
            }
            let manifest = repo.manifest_revlog();
            let idx = manifest.get_idx_by_nodeid(cs.manifestid())?;
            let entry = manifest.get_entry(idx)?;
            // read_delta_parents(exact=False): when storage deltas against a
            // parent, only its inserted/replaced manifest lines supply files.
            let text = match manifest.get_chunk(idx)? {
                Chunk::Deltas(base, deltas) if [entry.p1, entry.p2].contains(&Some(base)) => {
                    deltas.into_iter().flat_map(|delta| delta.content).collect()
                }
                _ => full_text(&manifest.get_rev(idx)?)?,
            };
            for (path, details) in mercurial::manifest::revlog::parse(&text)? {
                if files.contains(&path) {
                    file_links
                        .entry(path)
                        .or_default()
                        .entry(*details.nodeid())
                        .or_insert(nodes[rev]);
                }
            }
        }
        Some(common)
    } else {
        None
    };
    if restricted {
        referenced_revisions(
            &repo.manifest_revlog(),
            Section::Manifest,
            &manifest_links,
            None,
            version,
            emit,
        )?;
    } else {
        linked_revisions(
            &repo.manifest_revlog(),
            Section::Manifest,
            &nodes,
            &missing,
            version,
            emit,
        )?;
    }
    emit(Part::SectionEnd(Section::Manifest))?;

    if version == CgVersion::V3 && repo.is_treemanifest() {
        let dirs: Vec<_> = tree_links.keys().cloned().collect();
        read_in_order(
            &dirs,
            |dir| {
                let mut parts = Vec::new();
                if referenced_revisions(
                    &repo.read_dir_revlog(dir)?,
                    Section::Treemanifest(dir.clone()),
                    &tree_links[dir],
                    common.as_deref(),
                    version,
                    &mut |part| { parts.push(part); Ok(()) },
                )? {
                    parts.push(Part::SectionEnd(Section::Treemanifest(dir.clone())));
                }
                Ok(parts)
            },
            &mut |parts: Vec<Part>| parts.into_iter().try_for_each(&mut *emit),
        )?;
    }

    let files: Vec<Path> = files.into_iter().collect();
    read_in_order(
        &files,
        |path| {
            if restricted {
                let mut parts = Vec::new();
                if let Some(links) = file_links.get(path) {
                    if referenced_revisions(
                        &repo.read_file_revlog(path)?,
                        Section::Filelog(path.clone()),
                        links,
                        common.as_deref(),
                        version,
                        &mut |part| {
                            parts.push(part);
                            Ok(())
                        },
                    )? {
                        parts.push(Part::SectionEnd(Section::Filelog(path.clone())));
                    }
                }
                return Ok(parts);
            }
            revlog_parts(
                &repo.read_file_revlog(path)?,
                Section::Filelog(path.clone()),
                &nodes,
                &missing,
                version,
            )
        },
        &mut |parts: Vec<Part>| parts.into_iter().try_for_each(&mut *emit),
    )?;
    emit(Part::End)
}

fn referenced_revisions(
    revlog: &Revlog,
    section: Section,
    links: &HashMap<NodeHash, NodeHash>,
    common: Option<&[bool]>,
    version: CgVersion,
    emit: &mut dyn FnMut(Part) -> Result<()>,
) -> Result<bool> {
    let mut revisions: Vec<_> = links
        .iter()
        .map(|(node, link)| Ok((revlog.get_idx_by_nodeid(node)?, *link)))
        .collect::<Result<_>>()?;
    revisions.sort_unstable_by_key(|(idx, _)| *idx);
    let mut previous = None;
    for (idx, linknode) in revisions {
        let entry = revlog.get_entry(idx)?;
        if common.is_some_and(|common| {
            entry
                .linkrev
                .is_some_and(|link| common.get(link.as_u32() as usize).copied().unwrap_or(false))
        }) {
            continue;
        }
        let part = if version == CgVersion::V1 {
            v1_revision(
                revlog,
                section.clone(),
                idx,
                previous.or(entry.p1),
                linknode,
            )?
        } else {
            // A stored base may itself belong to the unselected sibling.
            let flags = check_flags(revlog, &section, idx, version)?;
            let (parents, renamed) = indexed_parents(revlog, idx)?;
            fulltext_chunk(
                section.clone(),
                entry.nodeid,
                &parents,
                renamed,
                linknode,
                flags,
                full_text(&revlog.get_rev(idx)?)?,
            )?
        };
        emit(part)?;
        previous = Some(idx);
    }
    Ok(previous.is_some())
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

/// The parts for one filelog's or directory manifest's missing revisions,
/// ending with its `SectionEnd`, or nothing when the client has every revision.
fn revlog_parts(
    revlog: &Revlog,
    section: Section,
    nodes: &[NodeHash],
    missing: &[bool],
    version: CgVersion,
) -> Result<Vec<Part>> {
    let mut parts = Vec::new();
    if linked_revisions(
        revlog,
        section.clone(),
        nodes,
        missing,
        version,
        &mut |part| {
            parts.push(part);
            Ok(())
        },
    )? {
        parts.push(Part::SectionEnd(section));
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
    version: CgVersion,
    emit: &mut dyn FnMut(Part) -> Result<()>,
) -> Result<bool> {
    let mut any = false;
    let mut previous = None;
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
        let part = if version == CgVersion::V1 {
            let base = previous.or(entry.p1);
            v1_revision(revlog, section.clone(), idx, base, nodes[linkrev])?
        } else {
            stored_revision(revlog, section.clone(), idx, entry.nodeid, nodes[linkrev], version)?
        };
        emit(part)?;
        previous = Some(idx);
        any = true;
    }
    Ok(any)
}

fn v1_revision(
    revlog: &Revlog,
    section: Section,
    idx: RevIdx,
    base_idx: Option<RevIdx>,
    linknode: NodeHash,
) -> Result<Part> {
    check_flags(revlog, &section, idx, CgVersion::V1)?;
    if let Chunk::Deltas(stored_base, _) = revlog.get_chunk(idx)? {
        if Some(stored_base) == base_idx && revlog.get_entry(stored_base)?.flags.is_empty() {
            return stored_revision(
                revlog,
                section,
                idx,
                revlog.get_entry(idx)?.nodeid,
                linknode,
                CgVersion::V1,
            );
        }
    }
    let (parents, renamed) = indexed_parents(revlog, idx)?;
    let (p1, p2) = parent_nodes(&parents, renamed);
    let (base, base_text) = match base_idx {
        Some(base) => (
            revlog.get_entry(base)?.nodeid,
            full_text(&revlog.get_rev(base)?)?,
        ),
        None => (NULL_HASH, Vec::new()),
    };
    let text = full_text(&revlog.get_rev(idx)?)?;
    Ok(Part::CgChunk(
        section,
        CgDeltaChunk {
            node: revlog.get_entry(idx)?.nodeid,
            p1,
            p2,
            base,
            linknode,
            flags: 0,
            delta: textdiff(&base_text, &text),
        },
    ))
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

/// The flags to send for a revision. Only EXTSTORED on a filelog revision is
/// served, and only changegroup 03 can carry it.
fn check_flags(revlog: &Revlog, section: &Section, idx: RevIdx, version: CgVersion) -> Result<u16> {
    let entry = revlog.get_entry(idx)?;
    if entry.flags.is_empty() {
        return Ok(0);
    }
    if entry.flags != IdxFlags::EXTSTORED || !matches!(section, Section::Filelog(_)) {
        return Err(format!("unsupported revision flags {:#06x} for {}", entry.flags.bits(), entry.nodeid).into());
    }
    if version != CgVersion::V3 {
        return Err(LFS_REQUIRED.into());
    }
    Ok(entry.flags.bits())
}

fn stored_revision(
    revlog: &Revlog,
    section: Section,
    idx: RevIdx,
    node: NodeHash,
    linknode: NodeHash,
    version: CgVersion,
) -> Result<Part> {
    let flags = check_flags(revlog, &section, idx, version)?;
    let (parents, renamed) = indexed_parents(revlog, idx)?;
    match revlog.get_chunk(idx)? {
        Chunk::Literal(text) => {
            fulltext_chunk(section, node, &parents, renamed, linknode, flags, text)
        }
        // Like hg's `_candelta`: clients may hold a flagged revision's node with
        // other raw text, so no delta goes out to or from one. The stored text
        // is sent as it is, and for EXTSTORED that is the pointer.
        Chunk::Deltas(base_idx, _)
            if flags != 0 || !revlog.get_entry(base_idx)?.flags.is_empty() =>
        {
            let text = full_text(&revlog.get_rev(idx)?)?;
            fulltext_chunk(section, node, &parents, renamed, linknode, flags, text)
        }
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
                    flags: 0,
                    delta: Delta::new(fragments).map_err(|err| Error::from(err.to_string()))?,
                },
            ))
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
    flags: u16,
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
            flags,
            delta: fulltext_delta(text)?,
        },
    ))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use mercurial::revlog::{RevIdx, Revlog, RevlogAppend, RevlogCompression};

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
            0,
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
        collect_version(repo, common, CgVersion::V2)
    }

    fn collect_version(repo: &RevlogRepo, common: &[NodeHash], version: CgVersion) -> Vec<Part> {
        let selection = Selection::Common {
            common: common.to_vec(),
            heads: Vec::new(),
        };
        let mut parts = Vec::new();
        history_parts(repo, &selection, version, &mut |part| {
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
    fn missing_revs_skip_ancestors_of_common() {
        // Revs 0 and 1 are linear. Rev 2 branches from rev 0.
        let temp = init_share_safe_repo();
        assert_hg(&hg(&temp.repo, &["update", "-r", "0"]), "hg update 0");
        std::fs::write(temp.repo.join("b.txt"), b"branch\n").expect("write b.txt");
        assert_hg(&hg(&temp.repo, &["add", "b.txt"]), "hg add b.txt");
        assert_hg(
            &hg(&temp.repo, &["commit", "-m", "branch", "-u", "test"]),
            "hg commit branch",
        );
        let node = |rev: &str| {
            let out = hg(&temp.repo, &["log", "-r", rev, "-T", "{node}"]);
            assert_hg(&out, "hg log");
            NodeHash::from_str(std::str::from_utf8(&out.stdout).expect("utf8").trim())
                .expect("node")
        };
        let repo = open_repo(&temp.repo);

        let (nodes, missing) = missing_revs(&repo, &[], &[]).expect("no common");
        assert_eq!(nodes, vec![node("0"), node("1"), node("2")]);
        assert_eq!(missing, vec![true, true, true]);

        let (_, missing) = missing_revs(&repo, &[NULL_HASH], &[]).expect("null common");
        assert_eq!(missing, vec![true, true, true]);

        let (_, missing) = missing_revs(&repo, &[node("1")], &[]).expect("common 1");
        assert_eq!(missing, vec![false, false, true]);

        let (_, missing) = missing_revs(&repo, &[node("0")], &[]).expect("common 0");
        assert_eq!(missing, vec![false, true, true]);

        let (_, missing) = missing_revs(&repo, &[node("1"), node("2")], &[]).expect("both heads");
        assert_eq!(missing, vec![false, false, false]);

        // A client may have commits this server never saw.
        let unknown = NodeHash::from_bytes(&[0x42; 20]).expect("unknown node");
        let (_, missing) = missing_revs(&repo, &[unknown, node("2")], &[]).expect("unknown");
        assert_eq!(missing, vec![false, true, false]);

        // `heads` limits what is sent to their ancestors.
        let (_, missing) = missing_revs(&repo, &[], &[node("1")]).expect("heads 1");
        assert_eq!(missing, vec![true, true, false]);
        let (_, missing) = missing_revs(&repo, &[node("0")], &[node("2")]).expect("heads 2");
        assert_eq!(missing, vec![false, false, true]);
        let (_, missing) = missing_revs(&repo, &[node("1")], &[node("1")]).expect("have it");
        assert_eq!(missing, vec![false, false, false]);
        assert!(
            missing_revs(&repo, &[], &[unknown]).is_err(),
            "unknown head"
        );
    }

    #[test]
    fn flagged_output_is_rejected_in_every_version() {
        let temp = init_share_safe_repo();
        let path = temp.repo.join(".hg/store/data/a.txt.i");
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[6..8].copy_from_slice(&0x8000u16.to_be_bytes());
        std::fs::write(&path, bytes).unwrap();
        let repo = open_repo(&temp.repo);
        for version in [CgVersion::V1, CgVersion::V2, CgVersion::V3] {
            let result = history_parts(&repo, &Selection::Common { common: vec![], heads: vec![] }, version, &mut |_| Ok(()));
            assert!(result.is_err(), "nonzero flags lost for {version:?}");
            assert!(result.unwrap_err().to_string().contains("flags"));
        }
    }

    #[test]
    fn flags_on_changelog_and_manifest_revisions_are_rejected() {
        for file in ["00changelog.i", "00manifest.i"] {
            let temp = init_share_safe_repo();
            let path = temp.repo.join(".hg/store").join(file);
            let mut bytes = std::fs::read(&path).unwrap();
            bytes[6..8].copy_from_slice(&IdxFlags::EXTSTORED.bits().to_be_bytes());
            std::fs::write(&path, bytes).unwrap();
            let repo = open_repo(&temp.repo);
            let selection = Selection::Common { common: vec![], heads: vec![] };
            let err = history_parts(&repo, &selection, CgVersion::V3, &mut |_| Ok(())).unwrap_err();
            assert!(err.to_string().contains("unsupported revision flags"), "{file}: {err}");
        }
    }

    /// `a.txt` gains rev 2 (EXTSTORED, snapshot), rev 3 (plain, stored as a delta
    /// on rev 2) and rev 4 (EXTSTORED, stored as a delta on rev 3).
    fn append_flagged_revisions(repo: &std::path::Path) -> [Vec<u8>; 3] {
        let index = repo.join(".hg/store/data/a.txt.i");
        let mut log = Revlog::from_idx_data(&index, None::<String>).expect("filelog");
        let tip = log.get_entry(RevIdx::from(1u32)).expect("rev 1").nodeid;
        let first: Vec<u8> = (1..400u64)
            .flat_map(|i| format!("{:016x}\n", i.wrapping_mul(0x9E3779B97F4A7C15)).into_bytes())
            .collect();
        let second = [&first[..], b"second\n"].concat();
        let third = [&second[..], b"third\n"].concat();
        let mut parent = tip;
        let mut base: Option<(NodeHash, &[u8])> = None;
        for (fill, text, flags) in [
            (0xa1, &first, IdxFlags::EXTSTORED),
            (0xa2, &second, IdxFlags::empty()),
            (0xa3, &third, IdxFlags::EXTSTORED),
        ] {
            let node = NodeHash::from_bytes(&[fill; 20]).expect("node");
            let idx = log
                .append_revision_flagged(
                    RevlogAppend { node, p1: Some(parent), p2: None, linkrev: Some(1), text: text.clone() },
                    base.map(|(base, old)| (base, textdiff(old, text))),
                    RevlogCompression::Zstd,
                    flags,
                )
                .expect("append");
            if base.is_some() {
                let chunk = log.get_chunk(RevIdx::from(idx)).expect("chunk");
                assert!(matches!(chunk, Chunk::Deltas(..)), "rev {idx} is not a stored delta");
            }
            parent = node;
            base = Some((node, text));
        }
        [first, second, third]
    }

    #[test]
    fn flagged_filelog_revision_keeps_flag_in_cg3() {
        let temp = init_share_safe_repo();
        let [first, second, third] = append_flagged_revisions(&temp.repo);
        let repo = open_repo(&temp.repo);
        let parts = collect_version(&repo, &[], CgVersion::V3);
        let chunks = filelog_chunks(&parts);
        assert_eq!(chunks.len(), 5, "filelog chunks");
        // As hg's `_candelta`, nothing deltas across EXTSTORED: every revision
        // goes out whole, even the plain one stored as a delta on a flagged base.
        for (chunk, flags, text) in [
            (chunks[2], 1 << 13, &first),
            (chunks[3], 0, &second),
            (chunks[4], 1 << 13, &third),
        ] {
            assert_eq!(chunk.flags, flags);
            assert_eq!(chunk.base, NULL_HASH);
            assert_eq!(&mercurial_types::delta::apply(b"", chunk.delta.clone()), text);
        }
        assert!(chunks[..2].iter().all(|chunk| chunk.flags == 0));

        for version in [CgVersion::V1, CgVersion::V2] {
            let selection = Selection::Common { common: vec![], heads: vec![] };
            let err = history_parts(&repo, &selection, version, &mut |_| Ok(())).unwrap_err();
            assert_eq!(err.to_string(), LFS_REQUIRED, "{version:?}");
        }
    }

    #[test]
    fn plain_revision_on_a_flagged_base_keeps_cg1_deltas_valid() {
        let temp = init_share_safe_repo();
        let [_, second, _] = append_flagged_revisions(&temp.repo);
        let repo = open_repo(&temp.repo);
        let log = repo.read_file_revlog(&Path::new(b"a.txt").expect("path")).expect("filelog");
        // cg1 has no base field, so a whole text would be applied to the base.
        let base = log.get_rev(RevIdx::from(2u32)).expect("rev 2");
        let section = Section::Filelog(Path::new(b"a.txt").expect("path"));
        let idx = RevIdx::from(3u32);
        match v1_revision(&log, section, idx, Some(RevIdx::from(2u32)), NULL_HASH).expect("part") {
            Part::CgChunk(_, chunk) => assert_eq!(
                mercurial_types::delta::apply(&full_text(&base).expect("text"), chunk.delta),
                second
            ),
            _ => panic!("expected a chunk"),
        }
    }

    #[test]
    fn lfs_repo_refuses_legacy_changegroups() {
        let temp = init_share_safe_repo();
        let repo = open_repo(&temp.repo);
        repo.add_requirement(Required::Lfs).expect("requirement");
        let selection = Selection::Common { common: vec![], heads: vec![] };
        let err = legacy_changegroup(repo, selection).err().expect("refused");
        assert_eq!(err.to_string(), LFS_REQUIRED);
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

    #[test]
    fn v1_deltas_reuse_stored_prev_delta() {
        let temp = init_share_safe_repo();
        std::fs::write(temp.repo.join("a.txt"), b"beta\ngamma\n").expect("rewrite a.txt");
        assert_hg(
            &hg(&temp.repo, &["commit", "-m", "third", "-u", "test"]),
            "hg commit",
        );
        // Store rev 2 as a delta against rev 1 that replaces all of it, which
        // `textdiff` would not produce, so only a reused delta matches.
        let index_path = temp
            .repo
            .join(".hg")
            .join("store")
            .join("data")
            .join("a.txt.i");
        let original = std::fs::read(&index_path).expect("filelog index");
        let rev0_clen = read_u32(&original[8..12]) as usize;
        let rev1_off = 64 + rev0_clen;
        let rev1_clen = read_u32(&original[rev1_off + 8..rev1_off + 12]) as usize;
        let rev2_off = rev1_off + 64 + rev1_clen;
        let node = &original[rev2_off + 32..rev2_off + 52];
        let text = b"beta\ngamma\n";
        let chunk = uncompressed_replace(5, text);
        let entry = pack_index_entry(
            (rev0_clen + rev1_clen) as u64,
            chunk.len() as u32,
            text.len() as u32,
            1,
            2,
            1,
            u32::MAX,
            node,
        );
        let mut rebuilt = original[..rev2_off].to_vec();
        rebuilt.extend_from_slice(&entry);
        rebuilt.extend_from_slice(&chunk);
        std::fs::write(&index_path, rebuilt).expect("rewrite filelog");

        let repo = open_repo(&temp.repo);
        let parts = collect_version(&repo, &[], CgVersion::V1);
        let chunks = filelog_chunks(&parts);
        assert_eq!(chunks.len(), 3, "filelog chunks");
        assert_eq!(chunks[0].base, NULL_HASH);
        assert_eq!(chunks[2].base, chunks[1].node);
        assert_eq!(
            chunks[2].delta.fragments(),
            &[Fragment {
                start: 0,
                end: 5,
                content: text.to_vec(),
            }]
        );
        // Every chunk is against the one before it in its group.
        let mut prev: Option<(Section, NodeHash)> = None;
        for part in &parts {
            if let Part::CgChunk(section, chunk) = part {
                if let Some((prev_section, prev_node)) = &prev {
                    if prev_section == section {
                        assert_eq!(chunk.base, *prev_node, "{section:?}");
                    }
                }
                prev = Some((section.clone(), chunk.node));
            }
        }
    }

    #[test]
    fn tree_history_sends_directory_groups() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "mononoke-bundle-tree-{}-{}",
            std::process::id(),
            nanos
        ));
        std::fs::create_dir_all(&root).expect("temp root");
        let temp = TempRepo {
            repo: root.join("repo"),
            root,
        };
        assert_hg(
            &hg(
                &temp.root,
                &["init", "repo", "--config", "experimental.treemanifest=1"],
            ),
            "hg init",
        );
        for dir in ["a", "b"] {
            std::fs::create_dir_all(temp.repo.join(dir)).expect("mkdir");
        }
        for (rev, text) in [("first", "1\n"), ("second", "2\n")] {
            std::fs::write(temp.repo.join("a").join("x"), text).expect("write a/x");
            std::fs::write(temp.repo.join("b").join("y"), text).expect("write b/y");
            assert_hg(
                &hg(&temp.repo, &["commit", "-Aqm", rev, "-u", "test"]),
                "hg commit",
            );
        }

        let repo = open_repo(&temp.repo);
        assert!(repo.is_treemanifest());
        let parts = collect_version(&repo, &[], CgVersion::V3);
        for dir in ["a", "b"] {
            let dir = Path::new(dir).expect("dir");
            let sent: Vec<NodeHash> = parts
                .iter()
                .filter_map(|part| match part {
                    Part::CgChunk(Section::Treemanifest(got), chunk) if *got == dir => {
                        Some(chunk.node)
                    }
                    _ => None,
                })
                .collect();
            let revlog = repo.read_dir_revlog(&dir).expect("dirlog");
            let stored: Vec<NodeHash> = (0..revlog.len().expect("len"))
                .map(|idx| revlog.get_entry(RevIdx::from(idx)).expect("entry").nodeid)
                .collect();
            assert_eq!(stored.len(), 2, "{dir} revisions");
            assert_eq!(sent, stored, "{dir} group");
            assert!(parts.contains(&Part::SectionEnd(Section::Treemanifest(dir))));
        }

        let part = changegroup_part_version(
            futures::stream::iter(parts.into_iter().map(Ok::<_, BundleError>)),
            CgVersion::V3,
        )
        .expect("part");
        let mut bundle =
            mercurial_bundles::Bundle2EncodeBuilder::new(std::io::Cursor::new(Vec::new()));
        bundle.add_part(part);
        let bytes = futures::executor::block_on(bundle.build())
            .expect("encode")
            .into_inner();
        let file = temp.root.join("tree.hg");
        std::fs::write(&file, bytes).expect("write bundle");
        let out = hg(&temp.root, &["debugbundle", "--all", "tree.hg"]);
        assert_hg(&out, "hg debugbundle");
        let listed = String::from_utf8_lossy(&out.stdout);
        let lines: Vec<&str> = listed.lines().map(str::trim).collect();
        for dir in ["a/", "b/"] {
            assert!(lines.contains(&dir), "{dir} missing:\n{listed}");
        }
    }
}
