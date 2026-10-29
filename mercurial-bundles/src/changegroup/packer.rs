// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::pin::Pin;
use std::task::{Context, Poll};

use byteorder::{BigEndian, ByteOrder};
use bytes::BufMut;
use futures::Stream;

use crate::chunk::Chunk;
use crate::errors::*;

use super::{CgDeltaChunk, CgVersion, Part, Section};

pub struct Cg2Packer<S> {
    delta_stream: S,
    last_seen: Section,
    version: CgVersion,
    /// Version 03: whether the empty chunk ending the directory manifests has
    /// been written.
    trees_closed: bool,
}

impl<S> Cg2Packer<S> {
    pub fn new(delta_stream: S) -> Self {
        Self::with_version(delta_stream, CgVersion::V2)
    }

    pub fn with_version(delta_stream: S, version: CgVersion) -> Self {
        Cg2Packer {
            delta_stream,
            last_seen: Section::Changeset,
            version,
            trees_closed: version != CgVersion::V3,
        }
    }
}

impl<S, E> Stream for Cg2Packer<S>
where
    S: Stream<Item = std::result::Result<Part, E>> + Unpin,
    Error: From<E>,
{
    type Item = Result<Chunk>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.delta_stream).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err.into()))),
            Poll::Ready(Some(Ok(Part::CgChunk(section, delta_chunk)))) => {
                if matches!(section, Section::Treemanifest(_)) && this.version != CgVersion::V3 {
                    return Poll::Ready(Some(Err(ErrorKind::Bundle2Encode(
                        "directory manifests need changegroup 03".into(),
                    ))));
                }
                let mut builder = ChunkBuilder::new(this.version);
                if matches!(section, Section::Filelog(_)) && !this.trees_closed {
                    builder.close_trees();
                    this.trees_closed = true;
                }
                if this.last_seen != section {
                    builder.encode_section(&section);
                    this.last_seen = section;
                }
                builder.encode_delta_chunk(delta_chunk);
                Poll::Ready(Some(builder.build()))
            }
            Poll::Ready(Some(Ok(Part::SectionEnd(_section)))) => {
                Poll::Ready(Some(Ok(empty_cg_chunk())))
            }
            Poll::Ready(Some(Ok(Part::End))) => {
                if this.trees_closed {
                    Poll::Ready(Some(Ok(empty_cg_chunk())))
                } else {
                    this.trees_closed = true;
                    // The end of the directory manifests, then the end of the files.
                    Poll::Ready(Some(Chunk::new(vec![0; 8])))
                }
            }
        }
    }
}

/// Produce an empty changegroup chunk.
///
/// Note that this is distinct from Chunk::empty() -- this is an actual chunk
/// with a 4-byte payload.
fn empty_cg_chunk() -> Chunk {
    Chunk::new(vec![0, 0, 0, 0]).expect("Chunk::new should not fail for a 4-byte chunk")
}

struct ChunkBuilder {
    version: CgVersion,
    inner: Vec<u8>,
    len_offset: usize,
}

impl ChunkBuilder {
    pub fn new(version: CgVersion) -> Self {
        ChunkBuilder {
            version,
            // Reserve four bytes in the beginning for the length.
            inner: vec![0, 0, 0, 0],
            len_offset: 0,
        }
    }

    /// Put the empty chunk that ends the directory manifests in front of the
    /// first file section. Must be called before anything else.
    pub fn close_trees(&mut self) -> &mut Self {
        self.inner = vec![0; 8];
        self.len_offset = 4;
        self
    }

    /// Encode the beginning of a section. This should always happen before any
    /// delta chunks are encoded.
    pub fn encode_section(&mut self, section: &Section) -> &mut Self {
        // Changeset and manifest sections are implicitly encoded, so we don't
        // need to do anything there.
        let (name, suffix): (_, &[u8]) = match section {
            Section::Filelog(f) => (f, b""),
            Section::Treemanifest(d) => (d, b"/"),
            _ => return self,
        };
        // Note that the filename length must include the four bytes for itself.
        let at = self.len_offset;
        BigEndian::write_i32(
            &mut self.inner[at..],
            (name.len() + suffix.len() + 4) as i32,
        );
        self.inner.put_slice(name.as_slice());
        self.inner.put_slice(suffix);
        // Add four more bytes for the start of the section.
        self.len_offset = self.inner.len();
        self.inner.put_slice(&[0, 0, 0, 0]);
        self
    }

    pub fn encode_delta_chunk(&mut self, chunk: CgDeltaChunk) -> &mut Self {
        self.inner.put_slice(chunk.node.as_ref());
        self.inner.put_slice(chunk.p1.as_ref());
        self.inner.put_slice(chunk.p2.as_ref());
        if self.version != CgVersion::V1 {
            self.inner.put_slice(chunk.base.as_ref());
        }
        self.inner.put_slice(chunk.linknode.as_ref());
        if self.version == CgVersion::V3 {
            self.inner.put_u16(chunk.flags);
        }

        for fragment in chunk.delta.fragments() {
            self.inner.put_i32(fragment.start as i32);
            self.inner.put_i32(fragment.end as i32);
            self.inner.put_i32(fragment.content.len() as i32);
            self.inner.put_slice(&fragment.content[..]);
        }

        self
    }

    pub fn build(self) -> Result<Chunk> {
        let len = self.inner.len() - self.len_offset;
        let mut inner = self.inner;
        BigEndian::write_i32(&mut inner[self.len_offset..], len as i32);
        Chunk::new(inner)
    }
}
