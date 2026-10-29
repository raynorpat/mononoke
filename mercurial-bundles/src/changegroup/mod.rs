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
    /// A directory manifest (changegroup 03), without the trailing `/` of its
    /// header chunk.
    Treemanifest(Path),
    Filelog(Path),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CgVersion {
    V1,
    V2,
    V3,
}

impl CgVersion {
    pub fn param(self) -> &'static str {
        match self {
            CgVersion::V1 => "01",
            CgVersion::V2 => "02",
            CgVersion::V3 => "03",
        }
    }

    pub fn from_param(param: &[u8]) -> Option<CgVersion> {
        match param {
            b"01" => Some(CgVersion::V1),
            b"02" => Some(CgVersion::V2),
            b"03" => Some(CgVersion::V3),
            _ => None,
        }
    }

    /// Bytes of a chunk header (without the length field).
    pub(crate) fn header_len(self) -> usize {
        match self {
            CgVersion::V1 => 80,
            CgVersion::V2 => 100,
            CgVersion::V3 => 102,
        }
    }
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
    /// Revlog flags. Only carried by version 03, 0 otherwise.
    pub flags: u16,
    pub delta: Delta,
}

#[cfg(test)]
mod test {
    use std::io::{self, Cursor};

    use futures::executor::block_on;
    use futures::{SinkExt, StreamExt, TryStreamExt};
    use futures_ext::StreamLayeredExt;
    use mercurial_types::NULL_HASH;
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
    fn round_trip_every_version() {
        // Each test case gets pretty big (O(size**2) parts (because of
        // filelogs), each part with O(size) deltas), so reduce the size a bit
        // and generate a smaller number of test cases.
        //
        // WouldBlock is turned into Poll::Pending and the waker is woken, so
        // block_on can drive partial writes and reads.
        quickcheck_version(roundtrip_v1);
        quickcheck_version(roundtrip_v2);
        quickcheck_version(roundtrip_v3);
    }

    type RoundtripFn = fn(
        Cg2PartSequence,
        PartialWithErrors<GenWouldBlock>,
        PartialWithErrors<GenWouldBlock>,
    ) -> TestResult;

    fn quickcheck_version(test: RoundtripFn) {
        QuickCheck::new()
            .rng(Gen::new(50))
            .tests(50)
            .quickcheck(test);
    }

    fn roundtrip_v1(
        seq: Cg2PartSequence,
        write_ops: PartialWithErrors<GenWouldBlock>,
        read_ops: PartialWithErrors<GenWouldBlock>,
    ) -> TestResult {
        roundtrip(CgVersion::V1, seq, write_ops, read_ops)
    }

    fn roundtrip_v2(
        seq: Cg2PartSequence,
        write_ops: PartialWithErrors<GenWouldBlock>,
        read_ops: PartialWithErrors<GenWouldBlock>,
    ) -> TestResult {
        roundtrip(CgVersion::V2, seq, write_ops, read_ops)
    }

    fn roundtrip_v3(
        seq: Cg2PartSequence,
        write_ops: PartialWithErrors<GenWouldBlock>,
        read_ops: PartialWithErrors<GenWouldBlock>,
    ) -> TestResult {
        roundtrip(CgVersion::V3, seq, write_ops, read_ops)
    }

    #[test]
    fn test_roundtrip_giant() {
        // Test a smaller number of cases with much larger inputs.
        QuickCheck::new()
            .rng(Gen::new(200))
            .tests(1)
            .quickcheck(roundtrip_v3 as RoundtripFn);
    }

    fn roundtrip(
        version: CgVersion,
        seq: Cg2PartSequence,
        write_ops: PartialWithErrors<GenWouldBlock>,
        read_ops: PartialWithErrors<GenWouldBlock>,
    ) -> TestResult {
        let seq = seq.for_version(version);
        let cursor = Cursor::new(Vec::with_capacity(32 * 1024));
        let partial_write = PartialAsyncWrite::new(cursor, write_ops);
        let mut packer = packer::Cg2Packer::with_version(seq.to_stream(), version);
        let mut sink = FramedWrite::new(partial_write, ChunkEncoder);
        block_on(sink.send_all(&mut packer)).unwrap();
        let mut cursor = sink.into_inner().into_inner();

        cursor.set_position(0);

        let partial_read = PartialAsyncRead::new(cursor, read_ops);
        let chunks = FramedRead::new(partial_read, ChunkDecoder)
            .map(|chunk| chunk.and_then(|chunk| chunk.into_bytes().map_err(|err| err)));

        let logger = make_root_logger();
        let unpacker = unpacker::Cg2Unpacker::with_version(logger, version);
        let part_stream = chunks.decode(unpacker);

        let parts: Vec<Part> = block_on(part_stream.map_ok(|part| part.cg2_part()).try_collect())
            .unwrap_or_else(|err| panic!("unexpected error: {}", err));

        if seq != parts[..] {
            return TestResult::failed();
        }

        TestResult::passed()
    }

    fn hg(dir: &std::path::Path, args: &[&str]) {
        let out = std::process::Command::new("hg")
            .current_dir(dir)
            .args(args)
            .env("HGPLAIN", "1")
            .env("HGRCPATH", "")
            .output()
            .expect("run hg");
        assert!(
            out.status.success(),
            "hg {:?} failed: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Make a repo with files in nested directories and two commits, and
    /// `hg bundle --all` it with `bundlespec`.
    fn hg_bundle(name: &str, config: &[&str], bundlespec: &str) -> Vec<u8> {
        let dir = std::env::temp_dir().join(format!("mononoke-cg-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let repo = dir.join("repo");
        std::fs::create_dir_all(repo.join("a/b")).unwrap();
        std::fs::create_dir_all(repo.join("c")).unwrap();
        hg(&repo, &[&["init"][..], config].concat());
        std::fs::write(repo.join("top.txt"), "one\n").unwrap();
        std::fs::write(repo.join("a/b/x.txt"), "x1\nx2\n").unwrap();
        std::fs::write(repo.join("c/y.txt"), "y1\n").unwrap();
        let commit = |msg| {
            hg(
                &repo,
                &[
                    &["commit", "-A", "-u", "t", "-d", "0 0", "-m", msg][..],
                    config,
                ]
                .concat(),
            )
        };
        commit("one");
        std::fs::write(repo.join("a/b/x.txt"), "x1\nx2\nx3\n").unwrap();
        std::fs::write(repo.join("c/z.txt"), "z1\n").unwrap();
        commit("two");
        hg(
            &repo,
            &[
                &["bundle", "--all", "-t", bundlespec][..],
                config,
                &["../out.hg"],
            ]
            .concat(),
        );
        let bytes = std::fs::read(dir.join("out.hg")).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        bytes
    }

    /// Pack `parts` and concatenate the chunk payloads.
    fn pack(parts: Vec<Part>, version: CgVersion) -> Vec<u8> {
        let packer = packer::Cg2Packer::with_version(
            futures::stream::iter(parts.into_iter().map(Ok::<_, crate::errors::Error>)),
            version,
        );
        let chunks: Vec<crate::chunk::Chunk> = block_on(packer.try_collect()).unwrap();
        chunks
            .into_iter()
            .flat_map(|chunk| chunk.into_bytes().unwrap())
            .collect()
    }

    #[test]
    fn cg3_bytes_match_mercurial() {
        let bundle = hg_bundle(
            "cg3",
            &["--config", "experimental.treemanifest=1"],
            "none-v2;cg.version=03",
        );

        // Raw payload of the changegroup part: skip "HG20" and the stream
        // params, then walk the parts (header size, header, payload chunks).
        let rd = |b: &[u8]| i32::from_be_bytes(b[..4].try_into().unwrap()) as usize;
        let mut pos = 4 + 4 + rd(&bundle[4..]);
        let mut payload = Vec::new();
        loop {
            let hlen = rd(&bundle[pos..]);
            assert_ne!(hlen, 0, "no changegroup part");
            let is_cg = bundle[pos + 5..pos + 4 + hlen]
                .to_ascii_lowercase()
                .starts_with(b"changegroup");
            pos += 4 + hlen;
            loop {
                let clen = rd(&bundle[pos..]);
                pos += 4;
                if clen == 0 {
                    break;
                }
                if is_cg {
                    payload.extend_from_slice(&bundle[pos..pos + clen]);
                }
                pos += clen;
            }
            if is_cg {
                break;
            }
        }

        let mut stream =
            crate::bundle2::Bundle2Stream::new(Cursor::new(bundle), make_root_logger());
        let mut parts = Vec::new();
        let mut in_cg = false;
        while let Some(item) = block_on(stream.next()) {
            match item.unwrap() {
                crate::Bundle2Item::Header(h) => {
                    in_cg = h.part_type_lower().as_str() == "changegroup";
                    if in_cg {
                        assert_eq!(h.mparams().get("version").map(|v| &v[..]), Some(&b"03"[..]));
                    }
                }
                crate::Bundle2Item::Inner(p) if in_cg => parts.push(p.cg2_part()),
                _ => {}
            }
        }
        assert!(parts
            .iter()
            .any(|p| matches!(p.section(), Section::Treemanifest(_))));
        assert_eq!(pack(parts, CgVersion::V3), payload);
    }

    #[test]
    fn cg1_bytes_match_mercurial() {
        let bundle = hg_bundle("cg1", &[], "none-v1");
        assert_eq!(&bundle[..6], b"HG10UN");
        let raw = &bundle[6..];

        let mut unpacker = unpacker::Cg2Unpacker::with_version(make_root_logger(), CgVersion::V1);
        let mut buf = bytes::BytesMut::from(raw);
        let mut parts = Vec::new();
        while let Some(p) = tokio_util::codec::Decoder::decode_eof(&mut unpacker, &mut buf).unwrap()
        {
            parts.push(p.cg2_part());
        }
        assert!(parts.len() > 3);
        assert_eq!(pack(parts, CgVersion::V1), raw);
    }

    #[test]
    fn missing_version_param_means_01() {
        let node = NodeHash::from_bytes(&[7; 20]).unwrap();
        let p1 = NodeHash::from_bytes(&[8; 20]).unwrap();
        let seq = vec![
            Part::CgChunk(
                Section::Changeset,
                CgDeltaChunk {
                    node,
                    p1,
                    p2: NULL_HASH,
                    base: p1,
                    linknode: node,
                    flags: 0,
                    delta: Delta::new(vec![]).unwrap(),
                },
            ),
            Part::SectionEnd(Section::Changeset),
            Part::SectionEnd(Section::Manifest),
            Part::End,
        ];
        let payload = pack(seq.clone(), CgVersion::V1);

        let mut builder = crate::part_encode::PartEncodeBuilder::mandatory("changegroup").unwrap();
        builder.set_data_generated(futures::stream::iter(vec![Ok::<_, crate::errors::Error>(
            crate::chunk::Chunk::new(payload).unwrap(),
        )]));
        let mut enc = crate::bundle2_encode::Bundle2EncodeBuilder::new(Cursor::new(Vec::new()));
        enc.set_compressor_type(async_compression::CompressorType::Uncompressed);
        enc.add_part(builder);
        let mut buf = block_on(enc.build()).unwrap();
        buf.set_position(0);

        let mut stream = crate::bundle2::Bundle2Stream::new(buf, make_root_logger());
        let mut parts = Vec::new();
        while let Some(item) = block_on(stream.next()) {
            if let crate::Bundle2Item::Inner(p) = item.unwrap() {
                parts.push(p.cg2_part());
            }
        }
        assert_eq!(parts, seq);
    }

    fn make_root_logger() -> Logger {
        let plain = slog_term::PlainSyncDecorator::new(io::stdout());
        Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!())
    }
}
