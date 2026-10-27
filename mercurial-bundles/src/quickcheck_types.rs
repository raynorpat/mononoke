// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Quickcheck support for a few types that don't have support upstream,
//! and for a few other test types.

use std::iter;

use bytes::Bytes;
use futures::stream;
use futures::Stream;
use mercurial_types::delta::Fragment;
use mercurial_types::{Delta, NodeHash, Path};
use quickcheck::{empty_shrinker, single_shrinker, Arbitrary, Gen};

use crate::changegroup;
use crate::errors::*;

#[derive(Clone, Debug)]
pub struct QCBytes(Bytes);

impl From<QCBytes> for Bytes {
    fn from(qcbytes: QCBytes) -> Bytes {
        qcbytes.0
    }
}

impl Arbitrary for QCBytes {
    fn arbitrary(g: &mut Gen) -> Self {
        // Just use the Vec<u8> quickcheck underneath.
        let v: Vec<u8> = Vec::arbitrary(g);
        QCBytes(v.into())
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.0.to_vec().shrink().map(|v| QCBytes(v.into())))
    }
}

#[derive(Clone, Debug)]
pub struct Cg2PartSequence {
    // Storing the ends in here bypasses a number of lifetime issues.
    changesets: Vec<changegroup::Part>,
    changesets_end: changegroup::Part,
    manifests: Vec<changegroup::Part>,
    manifests_end: changegroup::Part,
    filelogs: Vec<(Vec<changegroup::Part>, changegroup::Part)>,
    end: changegroup::Part,
}

impl Cg2PartSequence {
    /// Combine all the changesets, manifests and filelogs into a single iterator.
    pub fn as_iter(&self) -> Box<dyn Iterator<Item = &changegroup::Part> + '_> {
        Box::new(
            self.changesets
                .iter()
                .chain(iter::once(&self.changesets_end))
                .chain(self.manifests.iter())
                .chain(iter::once(&self.manifests_end))
                .chain(
                    self.filelogs
                        .iter()
                        .filter(|entry| {
                            // If there are no filelog parts, it isn't valid to return a
                            // SectionEnd since that won't be referring to anything. So
                            // just skip the whole filelog.
                            !entry.0.is_empty()
                        })
                        .flat_map(|entry| entry.0.iter().chain(iter::once(&entry.1))),
                )
                .chain(iter::once(&self.end)),
        )
    }

    /// Combine all the changesets, manifests and filelogs into a single stream.
    ///
    /// This returns a clone of everything because streams can't really return
    /// references at the moment.
    pub fn to_stream(&self) -> impl Stream<Item = Result<changegroup::Part>> + Unpin + use<'_> {
        let part_results: Vec<_> = self.as_iter().cloned().map(Ok).collect();
        stream::iter(part_results)
    }
}

impl PartialEq<[changegroup::Part]> for Cg2PartSequence {
    fn eq(&self, other: &[changegroup::Part]) -> bool {
        self.as_iter().eq(other.iter())
    }
}

impl Arbitrary for Cg2PartSequence {
    fn arbitrary(g: &mut Gen) -> Self {
        use changegroup::*;

        // Generate a valid part sequence (changegroup, then manifest, then filelogs).
        let size = g.size();

        let changesets = gen_parts(Section::Changeset, g);
        let manifests = gen_parts(Section::Manifest, g);

        let nfilelogs = gen_range(g, 0, size);
        let mut filelogs = Vec::with_capacity(nfilelogs);

        for _ in 0..nfilelogs {
            let path = arbitrary_path(g);
            let section_end = Part::SectionEnd(Section::Filelog(path.clone()));
            filelogs.push((gen_parts(Section::Filelog(path), g), section_end));
        }

        Cg2PartSequence {
            changesets,
            changesets_end: Part::SectionEnd(Section::Changeset),
            manifests,
            manifests_end: Part::SectionEnd(Section::Manifest),
            filelogs,
            end: Part::End,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        use changegroup::*;

        // All the parts can be shrinked independently as long as the section
        // remains the same (ensured in the impl of Arbitrary for
        // changegroup::Part).
        Box::new(
            (
                self.changesets.clone(),
                self.manifests.clone(),
                self.filelogs.clone(),
            )
                .shrink()
                .map(|(c, m, f)| Cg2PartSequence {
                    changesets: c,
                    changesets_end: Part::SectionEnd(Section::Changeset),
                    manifests: m,
                    manifests_end: Part::SectionEnd(Section::Manifest),
                    filelogs: f,
                    end: Part::End,
                }),
        )
    }
}

fn gen_parts(section: changegroup::Section, g: &mut Gen) -> Vec<changegroup::Part> {
    let size = g.size();
    (0..gen_range(g, 0, size))
        .map(|_| {
            changegroup::Part::CgChunk(section.clone(), changegroup::CgDeltaChunk::arbitrary(g))
        })
        .collect()
}

impl Arbitrary for changegroup::Part {
    fn arbitrary(_g: &mut Gen) -> Self {
        unimplemented!()
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        use changegroup::Part::CgChunk;

        match self {
            CgChunk(section, delta_chunk) => {
                // Keep the section the same, but shrink the delta chunk.
                let section = section.clone();
                Box::new(
                    delta_chunk
                        .shrink()
                        .map(move |chunk| CgChunk(section.clone(), chunk)),
                )
            }
            _ => empty_shrinker(),
        }
    }
}

impl Arbitrary for changegroup::CgDeltaChunk {
    fn arbitrary(g: &mut Gen) -> Self {
        // TODO: should these be more structured? e.g. base = p1 some of the time
        changegroup::CgDeltaChunk {
            node: arbitrary_node(g),
            p1: arbitrary_node(g),
            p2: arbitrary_node(g),
            base: arbitrary_node(g),
            linknode: arbitrary_node(g),
            delta: arbitrary_delta(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        // Don't bother trying to shrink node hashes -- the meat is in the delta.
        let clone = self.clone();
        Box::new(
            shrink_delta(&self.delta).map(move |delta| changegroup::CgDeltaChunk {
                node: clone.node,
                p1: clone.p1,
                p2: clone.p2,
                base: clone.base,
                linknode: clone.linknode,
                delta,
            }),
        )
    }
}

fn gen_range(g: &mut Gen, low: usize, high: usize) -> usize {
    if low >= high {
        return low;
    }
    low + usize::arbitrary(g) % (high - low)
}

fn arbitrary_node(g: &mut Gen) -> NodeHash {
    let mut bytes = [0u8; 20];
    // The null hash is special, so give it a 5% chance of happening.
    if usize::arbitrary(g) % 20 != 0 {
        for byte in &mut bytes {
            *byte = u8::arbitrary(g);
        }
    }
    NodeHash::from_bytes(&bytes).unwrap()
}

fn arbitrary_path(g: &mut Gen) -> Path {
    let size = g.size().max(1);
    let size_sqrt = ((size as f64).sqrt() as usize).max(2);
    let chars: Vec<u8> = (1..b'/').chain((b'/' + 1)..255).collect();
    let mut path = Vec::new();
    let ncomp = gen_range(g, 1, size_sqrt);
    for i in 0..ncomp {
        if i > 0 {
            path.push(b'/');
        }
        let n = gen_range(g, 1, 2 * size_sqrt);
        for _ in 0..n {
            path.push(*g.choose(&chars).unwrap());
        }
    }
    Path::new(path).unwrap()
}

fn lognormal_sample(g: &mut Gen, mean: f64, std_dev: f64) -> f64 {
    let u1 = ((u64::arbitrary(g) >> 11) | 1) as f64 / ((1u64 << 53) as f64);
    let u2 = (u64::arbitrary(g) >> 11) as f64 / ((1u64 << 53) as f64);
    let z = (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos();
    (mean + std_dev * z).exp()
}

fn arbitrary_frag_content(g: &mut Gen) -> Vec<u8> {
    let size = g.size();
    let sample = lognormal_sample(g, -3.0, 2.0);
    let raw = (size as f64) * sample;
    let content_len = if raw.is_finite() && raw > 0.0 {
        (raw as usize).min(size.max(1))
    } else {
        0
    };

    let mut v = vec![0u8; content_len];
    for byte in &mut v {
        *byte = u8::arbitrary(g);
    }
    v
}

fn arbitrary_delta(g: &mut Gen) -> Delta {
    let size = g.size().max(1);
    let nfrags = gen_range(g, 0, size);
    let mut end = 0usize;
    let mut frags = Vec::with_capacity(nfrags);
    for _ in 0..nfrags {
        let start = end + gen_range(g, 0, size);
        end = start + gen_range(g, 0, size);
        frags.push(Fragment {
            start,
            end,
            content: arbitrary_frag_content(g),
        });
    }
    Delta::new(frags).unwrap()
}

fn shrink_delta(delta: &Delta) -> Box<dyn Iterator<Item = Delta>> {
    let mut frags = delta.fragments().to_vec();
    if frags.is_empty() {
        return empty_shrinker();
    }
    frags.pop();
    match Delta::new(frags) {
        Ok(delta) => single_shrinker(delta),
        Err(_) => empty_shrinker(),
    }
}
