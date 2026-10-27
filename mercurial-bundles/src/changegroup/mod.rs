// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use mercurial_types::{Delta, NodeHash, Path};

pub mod packer;
pub mod unpacker;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Section {
    Changeset,
    Manifest,
    Filelog(Path),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Part {
    CgChunk(Section, CgDeltaChunk),
    SectionEnd(Section),
    End,
}

impl Part {
    pub fn is_section_end(&self) -> bool {
        match self {
            Part::SectionEnd(_) => true,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CgDeltaChunk {
    pub node: NodeHash,
    pub p1: NodeHash,
    pub p2: NodeHash,
    pub base: NodeHash,
    pub linknode: NodeHash,
    pub delta: Delta,
}

#[cfg(test)]
mod test {
    use std::io::{self, Cursor};

    use futures::executor::block_on;
    use futures::{SinkExt, StreamExt, TryStreamExt};
    use futures_ext::StreamLayeredExt;
    use partial_io::quickcheck_types::{GenWouldBlock, PartialWithErrors};
    use partial_io::{PartialAsyncRead, PartialAsyncWrite};
    use quickcheck::{Gen, QuickCheck, TestResult};
    use slog::{o, Drain, Logger};
    use slog_term;
    use tokio_util::codec::{FramedRead, FramedWrite};

    use super::*;
    use crate::chunk::{ChunkDecoder, ChunkEncoder};
    use crate::quickcheck_types::Cg2PartSequence;

    #[test]
    fn test_roundtrip() {
        // Each test case gets pretty big (O(size**2) parts (because of
        // filelogs), each part with O(size) deltas), so reduce the size a bit
        // and generate a smaller number of test cases.
        //
        // WouldBlock is turned into Poll::Pending and the waker is woken, so
        // block_on can drive partial writes and reads.
        let mut quickcheck = QuickCheck::new().rng(Gen::new(50)).tests(50);
        quickcheck.quickcheck(
            roundtrip
                as fn(
                    Cg2PartSequence,
                    PartialWithErrors<GenWouldBlock>,
                    PartialWithErrors<GenWouldBlock>,
                ) -> TestResult,
        );
    }

    #[test]
    fn test_roundtrip_giant() {
        // Test a smaller number of cases with much larger inputs.
        let mut quickcheck = QuickCheck::new().rng(Gen::new(200)).tests(1);
        quickcheck.quickcheck(
            roundtrip
                as fn(
                    Cg2PartSequence,
                    PartialWithErrors<GenWouldBlock>,
                    PartialWithErrors<GenWouldBlock>,
                ) -> TestResult,
        );
    }

    fn roundtrip(
        seq: Cg2PartSequence,
        write_ops: PartialWithErrors<GenWouldBlock>,
        read_ops: PartialWithErrors<GenWouldBlock>,
    ) -> TestResult {
        let cursor = Cursor::new(Vec::with_capacity(32 * 1024));
        let partial_write = PartialAsyncWrite::new(cursor, write_ops);
        let mut packer = packer::Cg2Packer::new(seq.to_stream());
        let mut sink = FramedWrite::new(partial_write, ChunkEncoder);
        block_on(sink.send_all(&mut packer)).unwrap();
        let mut cursor = sink.into_inner().into_inner();

        cursor.set_position(0);

        let partial_read = PartialAsyncRead::new(cursor, read_ops);
        let chunks = FramedRead::new(partial_read, ChunkDecoder)
            .map(|chunk| chunk.and_then(|chunk| chunk.into_bytes().map_err(|err| err)));

        let logger = make_root_logger();
        let unpacker = unpacker::Cg2Unpacker::new(logger);
        let part_stream = chunks.decode(unpacker);

        let parts: Vec<Part> = block_on(part_stream.map_ok(|part| part.cg2_part()).try_collect())
            .unwrap_or_else(|err| panic!("unexpected error: {}", err));

        if seq != parts[..] {
            return TestResult::failed();
        }

        TestResult::passed()
    }

    fn make_root_logger() -> Logger {
        let plain = slog_term::PlainSyncDecorator::new(io::stdout());
        Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!())
    }
}
