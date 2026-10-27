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
    let missing = missing_nodes(repo, common)?;
    let missing_set: HashSet<NodeHash> = missing.iter().copied().collect();
    let nodes = changelog_nodes(repo)?;

    let mut parts = Vec::new();
    let mut seen_manifests = HashSet::new();
    let mut manifest_revs = Vec::new();
    let mut seen_files: HashMap<Path, HashSet<NodeHash>> = HashMap::new();
    let mut file_revs: BTreeMap<Path, Vec<(NodeHash, NodeHash)>> = BTreeMap::new();

    for node in nodes {
        let cs = ready(repo.get_changeset_by_nodeid(&node))??;
        let manifest_id = *cs.manifestid();
        if seen_manifests.insert(manifest_id) && missing_set.contains(&node) {
            manifest_revs.push((manifest_id, node));
        }

        if !cs.files().is_empty() {
            let manifest = ready(repo.get_manifest_by_nodeid(&manifest_id))??;
            let introduced = missing_set.contains(&node);
            for file in cs.files() {
                let Some(details) = manifest.lookup(file) else {
                    continue;
                };
                let filenode = *details.nodeid();
                let seen = seen_files.entry(file.clone()).or_default();
                if seen.insert(filenode) && introduced {
                    file_revs
                        .entry(file.clone())
                        .or_default()
                        .push((filenode, node));
                }
            }
        }

        if missing_set.contains(&node) {
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

    for (manifest_id, linknode) in manifest_revs {
        let blob = ready(repo.get_manifest_blob_by_nodeid(&manifest_id))??;
        let renamed = blob.maybe_renamed();
        let parents = *blob.parents();
        let text = full_text(&blob)?;
        parts.push(fulltext_chunk(
            Section::Manifest,
            manifest_id,
            &parents,
            renamed,
            linknode,
            text,
        )?);
    }
    parts.push(Part::SectionEnd(Section::Manifest));

    for (path, revs) in file_revs {
        for (filenode, linknode) in revs {
            let revlog = repo.get_file_revlog(&path)?;
            let blob = revlog.get_rev_by_nodeid(&filenode)?;
            let renamed = blob.maybe_renamed();
            let parents = *blob.parents();
            let text = full_text(&blob)?;
            parts.push(fulltext_chunk(
                Section::Filelog(path.clone()),
                filenode,
                &parents,
                renamed,
                linknode,
                text,
            )?);
        }
        parts.push(Part::SectionEnd(Section::Filelog(path)));
    }
    parts.push(Part::End);

    changegroup_part(parts).map_err(|err| Error::from(err.to_string()))
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
}
