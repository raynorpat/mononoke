// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Unpacking changegroups.
//! See https://www.mercurial-scm.org/repo/hg/file/@/mercurial/help/internals/changegroups.txt.

use std::cmp;
use std::mem;

use bytes::BytesMut;
use slog;
use tokio_util::codec::Decoder;

use mercurial_types::delta::Fragment;
use mercurial_types::{Delta, NodeHash, Path};

use super::{CgDeltaChunk, CgVersion, Part, Section};
use crate::bail;
use crate::errors::*;
use crate::utils::BytesExt;
use crate::InnerPart;

#[derive(Debug)]
pub struct Cg2Unpacker {
    #[allow(dead_code)]
    logger: slog::Logger,
    version: CgVersion,
    state: State,
    /// Version 01 chunks have no base: it is the previous node of the group,
    /// or p1 for the first.
    prev_node: Option<NodeHash>,
}

impl Part {
    /// Gets the section this part is from.
    ///
    /// # Panics
    ///
    /// When self does not contain a Section.
    pub fn section(&self) -> &Section {
        match self {
            Part::CgChunk(section, _) => section,
            Part::SectionEnd(section) => section,
            _ => panic!("this Part does not contain a Section"),
        }
    }

    /// Gets the chunk inside this part.
    ///
    /// # Panics
    ///
    /// When self does not contain a CgDeltaChunk (it is a SectionEnd or End).
    pub fn chunk(&self) -> &CgDeltaChunk {
        match self {
            Part::CgChunk(_, chunk) => chunk,
            _ => panic!("this Part does not contain a CgDeltaChunk"),
        }
    }
}

impl Decoder for Cg2Unpacker {
    type Item = InnerPart;
    type Error = Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<InnerPart>> {
        match self.decode_next(buf) {
            Err(e) => {
                self.state = State::Invalid;
                Err(e)
            }
            Ok((ret, state)) => {
                self.state = state;
                match ret {
                    None => Ok(None),
                    Some(v) => Ok(Some(InnerPart::Cg2(v))),
                }
            }
        }
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<InnerPart>> {
        match self.decode(buf)? {
            None => {
                if !buf.is_empty() {
                    let len = buf.len();
                    let bytes = &buf[..cmp::min(len, 128)];
                    let msg = format!(
                        "incomplete changegroup: {} bytes remaining in \
                         buffer. State: {:?}, First 128 bytes: {:?}",
                        len, self.state, bytes,
                    );
                    bail!(ErrorKind::Cg2Decode(msg));
                }
                if self.state != State::End {
                    let msg = format!(
                        "incomplete changegroup: expected state End, found {:?}",
                        self.state
                    );
                    bail!(ErrorKind::Cg2Decode(msg));
                }
                Ok(None)
            }
            Some(v) => Ok(Some(v)),
        }
    }
}

impl Cg2Unpacker {
    pub fn new(logger: slog::Logger) -> Self {
        Self::with_version(logger, CgVersion::V2)
    }

    pub fn with_version(logger: slog::Logger, version: CgVersion) -> Self {
        Cg2Unpacker {
            logger,
            version,
            state: State::Changeset,
            prev_node: None,
        }
    }

    fn decode_next(&mut self, buf: &mut BytesMut) -> Result<(Option<Part>, State)> {
        let state = self.state.take();
        match state {
            State::Changeset => match self.decode_chunk(buf)? {
                None => Ok((None, State::Changeset)),
                Some(CgChunk::Empty) => {
                    Ok((Some(Part::SectionEnd(Section::Changeset)), State::Manifest))
                }
                Some(CgChunk::Delta(chunk)) => Ok((
                    Some(Part::CgChunk(Section::Changeset, chunk)),
                    State::Changeset,
                )),
            },
            State::Manifest => match self.decode_chunk(buf)? {
                None => Ok((None, State::Manifest)),
                Some(CgChunk::Empty) => {
                    let next = if self.version == CgVersion::V3 {
                        State::Treemanifest
                    } else {
                        State::Filename
                    };
                    Ok((Some(Part::SectionEnd(Section::Manifest)), next))
                }
                Some(CgChunk::Delta(chunk)) => Ok((
                    Some(Part::CgChunk(Section::Manifest, chunk)),
                    State::Manifest,
                )),
            },
            State::Treemanifest => match Self::decode_filename(buf)? {
                DecodeRes::None => Ok((None, State::Treemanifest)),
                DecodeRes::Some(dir) => {
                    let dir = match dir.as_slice().strip_suffix(b"/") {
                        Some(dir) => Path::new(dir)?,
                        None => bail!(ErrorKind::Cg2Decode(
                            "directory manifest name has no trailing '/'".into()
                        )),
                    };
                    self.decode_tree_chunk(buf, dir)
                }
                // The empty chunk ending the directory manifests; files follow.
                DecodeRes::End => {
                    self.state = State::Filename;
                    self.decode_next(buf)
                }
            },
            State::Tree(dir) => self.decode_tree_chunk(buf, dir),
            State::Filename => {
                let filename = Self::decode_filename(buf)?;
                match filename {
                    DecodeRes::None => Ok((None, State::Filename)),
                    DecodeRes::Some(f) => self.decode_filelog_chunk(buf, f),
                    DecodeRes::End => Ok((Some(Part::End), State::End)),
                }
            }
            State::Filelog(filename) => self.decode_filelog_chunk(buf, filename),
            State::End => Ok((None, State::End)),
            State::Invalid => Err(ErrorKind::Cg2Decode("byte stream corrupt".into())),
        }
    }

    fn decode_tree_chunk(
        &mut self,
        buf: &mut BytesMut,
        dir: Path,
    ) -> Result<(Option<Part>, State)> {
        match self.decode_chunk(buf)? {
            None => Ok((None, State::Tree(dir))),
            Some(CgChunk::Empty) => Ok((
                Some(Part::SectionEnd(Section::Treemanifest(dir))),
                State::Treemanifest,
            )),
            Some(CgChunk::Delta(chunk)) => Ok((
                Some(Part::CgChunk(Section::Treemanifest(dir.clone()), chunk)),
                State::Tree(dir),
            )),
        }
    }

    fn decode_filelog_chunk(
        &mut self,
        buf: &mut BytesMut,
        f: Path,
    ) -> Result<(Option<Part>, State)> {
        match self.decode_chunk(buf)? {
            None => Ok((None, State::Filelog(f))),
            Some(CgChunk::Empty) => {
                Ok((Some(Part::SectionEnd(Section::Filelog(f))), State::Filename))
            }
            Some(CgChunk::Delta(chunk)) => Ok((
                Some(Part::CgChunk(Section::Filelog(f.clone()), chunk)),
                State::Filelog(f),
            )),
        }
    }

    fn decode_chunk(&mut self, buf: &mut BytesMut) -> Result<Option<CgChunk>> {
        if buf.len() < 4 {
            return Ok(None);
        }

        let chunk_len = buf.peek_i32();
        // Note that chunk_len includes the 4 bytes consumed by itself
        // TODO: chunk_len < 0 = error
        let chunk_len = chunk_len as usize;
        if chunk_len == 0 {
            let _ = buf.drain_i32();
            self.prev_node = None;
            return Ok(Some(CgChunk::Empty));
        }
        // The last 4 is for the length field itself.
        let header_len = self.version.header_len() + 4;
        if chunk_len < header_len {
            let msg = format!(
                "invalid chunk: length >= {} required, found {}",
                header_len, chunk_len
            );
            bail!(ErrorKind::Cg2Decode(msg));
        }

        if buf.len() < chunk_len {
            return Ok(None);
        }
        let _ = buf.drain_i32();

        // A chunk header has:
        // ---
        // node: NodeHash (20 bytes)
        // p1: NodeHash (20 bytes)
        // p2: NodeHash (20 bytes) -- NULL_HASH if only 1 parent
        // base node: NodeHash (20 bytes) (new in changegroup2)
        // link node: NodeHash (20 bytes)
        // flags: u16 (new in changegroup3)
        // ---

        let node = buf.drain_node();
        let p1 = buf.drain_node();
        let p2 = buf.drain_node();
        let base = match self.version {
            CgVersion::V1 => self.prev_node.unwrap_or(p1),
            _ => buf.drain_node(),
        };
        let linknode = buf.drain_node();
        let flags = if self.version == CgVersion::V3 {
            buf.drain_u16()
        } else {
            0
        };
        self.prev_node = Some(node);

        let delta = Self::decode_delta(buf, chunk_len - header_len)?;
        Ok(Some(CgChunk::Delta(CgDeltaChunk {
            node,
            p1,
            p2,
            base,
            linknode,
            flags,
            delta,
        })))
    }

    fn decode_delta(buf: &mut BytesMut, mut remaining: usize) -> Result<Delta> {
        let mut frags = vec![];

        while remaining > 0 {
            // Each delta fragment has:
            // ---
            // start offset: i32
            // end offset: i32
            // new length: i32
            // content (new length bytes)
            // ---
            let start = buf.drain_i32();
            let end = buf.drain_i32();
            let new_len = buf.drain_i32();
            // TODO: handle negative values for all the above

            let delta_len = (new_len + 12) as usize;
            if remaining < delta_len {
                bail!(ErrorKind::Cg2Decode(format!(
                    "expected {} bytes, {} remaining",
                    delta_len, remaining
                )));
            }

            frags.push(Fragment {
                start: start as usize,
                end: end as usize,
                // TODO: avoid copies here by switching this to Bytes
                content: buf.split_to(new_len as usize).to_vec(),
            });

            remaining -= delta_len;
        }

        Delta::new(frags).chain_err(|| ErrorKind::Cg2Decode("invalid delta".into()))
    }

    fn decode_filename(buf: &mut BytesMut) -> Result<DecodeRes<Path>> {
        if buf.len() < 4 {
            return Ok(DecodeRes::None);
        }
        let filename_len = buf.peek_i32();
        // TODO: filename_len < 0 == error
        if filename_len == 0 {
            let _ = buf.split_to(4);
            return Ok(DecodeRes::End);
        }
        let filename_len = filename_len as usize;
        // filename_len includes the 4 bytes for the length field.
        if buf.len() < filename_len {
            return Ok(DecodeRes::None);
        }
        let _ = buf.split_to(4);
        let filename = buf.drain_path(filename_len - 4).chain_err(|| {
            let msg = format!("invalid filename of length {}", filename_len);
            ErrorKind::Cg2Decode(msg)
        })?;
        Ok(DecodeRes::Some(filename))
    }
}

enum DecodeRes<T> {
    None,
    Some(T),
    End,
}

enum CgChunk {
    Delta(CgDeltaChunk),
    Empty,
}

#[derive(Debug, Eq, PartialEq)]
enum State {
    Changeset,
    Manifest,
    /// Version 03: expecting a directory manifest header or the empty chunk after them.
    Treemanifest,
    Tree(Path),
    Filename,
    Filelog(Path),
    End,
    Invalid,
}

impl State {
    pub fn take(&mut self) -> Self {
        mem::replace(self, State::Invalid)
    }
}
