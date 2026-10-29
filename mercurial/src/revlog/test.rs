// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use super::parser;
use super::*;

static EMPTY: &[u8] = include_bytes!("empty.i.bin");

fn ng_entry(offset: u64, compressed_len: u32, baserev: u32) -> [u8; 64] {
    let mut entry = [0u8; 64];
    entry[0..6].copy_from_slice(&[
        ((offset >> 40) & 0xff) as u8,
        ((offset >> 32) & 0xff) as u8,
        ((offset >> 24) & 0xff) as u8,
        ((offset >> 16) & 0xff) as u8,
        ((offset >> 8) & 0xff) as u8,
        (offset & 0xff) as u8,
    ]);
    entry[8..12].copy_from_slice(&compressed_len.to_be_bytes());
    entry[16..20].copy_from_slice(&baserev.to_be_bytes());
    entry[20..24].copy_from_slice(&u32::MAX.to_be_bytes());
    entry[24..28].copy_from_slice(&u32::MAX.to_be_bytes());
    entry[28..32].copy_from_slice(&u32::MAX.to_be_bytes());
    entry
}

#[test]
fn short_delta_remainder_is_error() {
    let mut entry0 = ng_entry(0, 0, u32::MAX);
    // The first entry overlaps the header: features 0, version NG.
    entry0[0] = 0;
    entry0[1] = 0;
    entry0[2] = 0;
    entry0[3] = 1;
    let entry1 = ng_entry(0, 3, 0);
    let mut idx = Vec::with_capacity(128);
    idx.extend_from_slice(&entry0);
    idx.extend_from_slice(&entry1);

    let revlog = Revlog::new(idx, Some(b"abc".to_vec())).expect("revlog");
    let err = revlog
        .get_chunk(RevIdx::from(1u32))
        .expect_err("short remainder");
    let msg = err.to_string();
    assert!(msg.contains("Failed to unpack details"), "{}", msg);
    assert!(msg.contains("3 remains"), "{}", msg);
}

fn one_rev_generaldelta(chunk: &[u8], baserev: u32) -> Revlog {
    let mut entry = ng_entry(0, chunk.len() as u32, baserev);
    // Features GENERAL_DELTA, version revlog NG. The header occupies the
    // first four bytes of entry 0; the data offset stays 0.
    entry[0] = 0x00;
    entry[1] = 0x02;
    entry[2] = 0x00;
    entry[3] = 0x01;
    Revlog::new(entry.to_vec(), Some(chunk.to_vec())).expect("revlog")
}

#[test]
fn zstd_magic_chunk_decompresses() {
    let frame = zstd::encode_all(&b"hello revlog"[..], 0).expect("zstd");
    assert_eq!(&frame[..4], &[0x28, 0xb5, 0x2f, 0xfd]);
    let revlog = one_rev_generaldelta(&frame, 0);
    let node = revlog.get_rev(RevIdx::from(0u32)).expect("rev");
    assert_eq!(node.as_blob().as_slice(), Some(&b"hello revlog"[..]));
}

#[test]
fn revlog_snapshot_base_equals_own_index_returns_full_text() {
    let mut chunk = vec![b'u'];
    chunk.extend_from_slice(b"snapshot-text");
    let revlog = one_rev_generaldelta(&chunk, 0);
    let node = revlog.get_rev(RevIdx::from(0u32)).expect("rev");
    assert_eq!(node.as_blob().as_slice(), Some(&b"snapshot-text"[..]));
}

#[test]
fn emptyrev() {
    let revlog = Revlog::new(EMPTY.to_vec(), None).expect("construction failed");
    let node = revlog
        .get_rev(RevIdx::from(0u32))
        .expect("failed to get rev");

    assert_eq!(node.size(), Some(0));
}

fn node_with_tail(tail: u8) -> NodeHash {
    let mut bytes = [0u8; 20];
    bytes[19] = tail;
    NodeHash::from_bytes(&bytes).expect("node")
}

/// Bytes that zstd level 0 (default) does not shrink under the inline limit.
fn incompressible(len: usize) -> Vec<u8> {
    let mut s: u64 = 0x1234_5678_9abc_def0;
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
        out.extend_from_slice(&s.to_le_bytes());
    }
    out.truncate(len);
    out
}

#[test]
fn append_snapshot_round_trips_zstd() {
    let mut revlog = Revlog::new(Vec::new(), None).expect("empty revlog");
    let node = node_with_tail(1);
    let idx = revlog
        .append_snapshot(
            RevlogAppend {
                node,
                p1: None,
                p2: None,
                linkrev: None,
                text: b"alpha\n".to_vec(),
            },
            RevlogCompression::Zstd,
        )
        .expect("append");

    assert_eq!(idx, 0);
    assert_eq!(
        revlog.get_header(),
        parser::Header {
            version: parser::Version::RevlogNG,
            features: parser::INLINE | parser::GENERAL_DELTA,
        }
    );

    let got = revlog.get_rev(RevIdx::from(idx)).expect("rev");
    assert_eq!(got.as_blob().as_slice(), Some(&b"alpha\n"[..]));

    let entry = revlog.get_entry(RevIdx::from(idx)).expect("entry");
    assert_eq!(entry.baserev, Some(RevIdx::from(idx)));
    // Rev 0's offset field is the logical data offset. The header covers its
    // high bytes, and the chunk itself still follows the 64-byte entry.
    assert_eq!(entry.offset, 0);
    assert_eq!(entry.nodeid, node);
    assert_eq!(entry.p1, None);
    assert_eq!(entry.p2, None);
    assert_eq!(entry.linkrev, None);

    let inner = revlog.inner.lock().expect("lock poisoned");
    let bytes = inner.idx.as_slice();
    assert_eq!(&bytes[0..4], &[0x00, 0x03, 0x00, 0x01]);
    assert_eq!(&bytes[64..68], &[0x28, 0xb5, 0x2f, 0xfd]);
}

#[test]
fn append_snapshot_converts_past_inline_limit() {
    let mut revlog = Revlog::new(Vec::new(), None).expect("empty revlog");
    let first = node_with_tail(1);
    revlog
        .append_snapshot(
            RevlogAppend {
                node: first,
                p1: None,
                p2: None,
                linkrev: None,
                text: b"alpha\n".to_vec(),
            },
            RevlogCompression::Zstd,
        )
        .expect("seed");
    assert!(revlog.get_header().features.contains(parser::INLINE));
    assert!(revlog
        .get_header()
        .features
        .contains(parser::GENERAL_DELTA));

    let overhead = parser::indexng_size();
    let mut len = INLINE_LIMIT - overhead + 1;
    let text = loop {
        let text = incompressible(len);
        let frame = zstd::encode_all(text.as_slice(), 0).expect("zstd");
        if overhead + frame.len() > INLINE_LIMIT {
            break text;
        }
        len += INLINE_LIMIT / 4;
    };
    let second = node_with_tail(2);
    let idx = revlog
        .append_snapshot(
            RevlogAppend {
                node: second,
                p1: Some(first),
                p2: None,
                linkrev: None,
                text: text.clone(),
            },
            RevlogCompression::Zstd,
        )
        .expect("large append");

    let got = revlog.get_rev(RevIdx::from(idx)).expect("large rev");
    assert_eq!(got.as_blob().as_slice(), Some(text.as_slice()));
    let kept = revlog.get_rev(RevIdx::from(0u32)).expect("first rev");
    assert_eq!(kept.as_blob().as_slice(), Some(&b"alpha\n"[..]));

    let header = revlog.get_header();
    assert!(!header.features.contains(parser::INLINE));
    assert!(header.features.contains(parser::GENERAL_DELTA));
    assert_eq!(header.version, parser::Version::RevlogNG);

    let entry = revlog.get_entry(RevIdx::from(idx)).expect("large entry");
    assert_eq!(entry.baserev, Some(RevIdx::from(idx)));
    assert_eq!(entry.nodeid, second);
    assert_eq!(entry.p1, Some(RevIdx::from(0u32)));
    assert_eq!(entry.linkrev, None);

    let third = node_with_tail(3);
    let idx2 = revlog
        .append_snapshot(
            RevlogAppend {
                node: third,
                p1: Some(second),
                p2: None,
                linkrev: Some(7),
                text: b"beta\n".to_vec(),
            },
            RevlogCompression::Zstd,
        )
        .expect("second append");
    let got2 = revlog.get_rev(RevIdx::from(idx2)).expect("beta");
    assert_eq!(got2.as_blob().as_slice(), Some(&b"beta\n"[..]));
    assert!(!revlog.get_header().features.contains(parser::INLINE));
    let entry2 = revlog.get_entry(RevIdx::from(idx2)).expect("beta entry");
    assert_eq!(entry2.baserev, Some(RevIdx::from(idx2)));
    assert_eq!(entry2.p1, Some(RevIdx::from(idx)));
    assert_eq!(entry2.linkrev, Some(RevIdx::from(7u32)));
    assert_eq!(entry2.nodeid, third);
}

#[test]
fn unknown_node_lookups_stay_misses_after_the_map_is_full() {
    let mut revlog = Revlog::new(Vec::new(), None).expect("empty revlog");
    let mut nodes = Vec::new();
    for tail in 1..=3u8 {
        let node = node_with_tail(tail);
        revlog
            .append_snapshot(
                RevlogAppend {
                    node,
                    p1: nodes.last().copied(),
                    p2: None,
                    linkrev: None,
                    text: vec![tail; 8],
                },
                RevlogCompression::Zstd,
            )
            .expect("append");
        nodes.push(node);
    }
    let unknown = node_with_tail(0x42);
    revlog.load_node_map().expect("load node map");
    // With the map full, a miss answers without a scan.
    assert!(revlog.index_of(&unknown).is_err());
    assert!(revlog.index_of(&unknown).is_err());
    for (idx, node) in nodes.iter().enumerate() {
        assert_eq!(revlog.index_of(node).expect("known node"), idx as u32);
    }

    let later = node_with_tail(4);
    assert!(revlog.index_of(&later).is_err());
    revlog
        .append_snapshot(
            RevlogAppend {
                node: later,
                p1: Some(nodes[2]),
                p2: None,
                linkrev: None,
                text: vec![4; 8],
            },
            RevlogCompression::Zstd,
        )
        .expect("append later");
    assert_eq!(revlog.index_of(&later).expect("appended node"), 3);
    assert!(revlog.index_of(&unknown).is_err());
}

fn replace_delta(start: usize, end: usize, content: &[u8]) -> delta::Delta {
    delta::Delta::new(vec![delta::Fragment {
        start,
        end,
        content: content.to_vec(),
    }])
    .expect("delta")
}

fn append_on(
    revlog: &mut Revlog,
    tail: u8,
    base: Option<(NodeHash, delta::Delta)>,
    text: Vec<u8>,
) -> u32 {
    let p1 = base.as_ref().map(|(node, _)| *node);
    revlog
        .append_revision(
            RevlogAppend {
                node: node_with_tail(tail),
                p1,
                p2: None,
                linkrev: None,
                text,
            },
            base,
            RevlogCompression::Zstd,
        )
        .expect("append")
}

#[test]
fn append_revision_stores_a_small_delta() {
    let mut revlog = Revlog::new(Vec::new(), None).expect("empty revlog");
    let text0 = incompressible(50_000);
    append_on(&mut revlog, 1, None, text0.clone());

    let mut text1 = text0.clone();
    text1[100..105].copy_from_slice(b"HELLO");
    let idx = append_on(
        &mut revlog,
        2,
        Some((node_with_tail(1), replace_delta(100, 105, b"HELLO"))),
        text1.clone(),
    );
    let entry = revlog.get_entry(RevIdx::from(idx)).expect("entry");
    assert_eq!(entry.baserev, Some(RevIdx::from(0u32)), "stored as a delta");
    assert!(
        entry.compressed_len < 1_000,
        "delta is {} bytes",
        entry.compressed_len
    );
    assert_eq!(entry.len, Some(text1.len() as u32));
    let got = revlog.get_rev(RevIdx::from(idx)).expect("rev 1");
    assert_eq!(got.as_blob().as_slice(), Some(text1.as_slice()));

    // A delta larger than the text it makes is stored as a snapshot.
    let idx = append_on(
        &mut revlog,
        3,
        Some((node_with_tail(2), replace_delta(0, text1.len(), b"tiny"))),
        b"tiny".to_vec(),
    );
    let entry = revlog.get_entry(RevIdx::from(idx)).expect("tiny entry");
    assert_eq!(entry.baserev, Some(RevIdx::from(idx)), "tiny is a snapshot");
    let got = revlog.get_rev(RevIdx::from(idx)).expect("tiny");
    assert_eq!(got.as_blob().as_slice(), Some(&b"tiny"[..]));
}

#[test]
fn append_revision_starts_a_snapshot_at_the_chain_limit() {
    let mut revlog = Revlog::new(Vec::new(), None).expect("empty revlog");
    let mut text = incompressible(200_000);
    append_on(&mut revlog, 0, None, text.clone());
    let mut base = node_with_tail(0);
    let mut last = 0;
    for rev in 1..=(MAX_DELTA_CHAIN as u32 + 1) {
        let mark = (rev % 251) as u8;
        text[10] = mark;
        let mut bytes = [0u8; 20];
        bytes[..4].copy_from_slice(&rev.to_be_bytes());
        bytes[19] = 0xee;
        let node = NodeHash::from_bytes(&bytes).expect("node");
        last = revlog
            .append_revision(
                RevlogAppend {
                    node,
                    p1: Some(base),
                    p2: None,
                    linkrev: None,
                    text: text.clone(),
                },
                Some((base, replace_delta(10, 11, &[mark]))),
                RevlogCompression::Zstd,
            )
            .expect("append");
        base = node;
    }
    let deltas = (1..=MAX_DELTA_CHAIN as u32)
        .filter(|rev| {
            let entry = revlog.get_entry(RevIdx::from(*rev)).expect("entry");
            entry.baserev == Some(RevIdx::from(*rev - 1))
        })
        .count();
    assert_eq!(
        deltas, MAX_DELTA_CHAIN,
        "every revision up to the limit is a delta"
    );
    let entry = revlog.get_entry(RevIdx::from(last)).expect("last");
    assert_eq!(
        entry.baserev,
        Some(RevIdx::from(last)),
        "past the limit is a snapshot"
    );
    let got = revlog.get_rev(RevIdx::from(last)).expect("last rev");
    assert_eq!(got.as_blob().as_slice(), Some(text.as_slice()));
}

#[test]
fn reload_sees_another_writers_append() {
    let dir = std::env::temp_dir().join(format!("mononoke-revlog-{}-reload", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp dir");
    let idx_path = dir.join("log.i");
    std::fs::write(&idx_path, b"").expect("empty index");
    let append = |revlog: &mut Revlog, tail: u8, p1: Option<NodeHash>| {
        revlog
            .append_snapshot(
                RevlogAppend {
                    node: node_with_tail(tail),
                    p1,
                    p2: None,
                    linkrev: None,
                    text: vec![tail; 16],
                },
                RevlogCompression::Zstd,
            )
            .expect("append");
    };
    let mut writer = Revlog::from_idx(&idx_path).expect("writer");
    append(&mut writer, 1, None);

    let reader = Revlog::from_idx(&idx_path).expect("reader");
    let shared = reader.clone();
    assert_eq!(reader.len().expect("len"), 1);
    append(&mut writer, 2, Some(node_with_tail(1)));
    assert!(
        reader.index_of(&node_with_tail(2)).is_err(),
        "seen before reload"
    );

    reader.reload().expect("reload");
    // Clones share the reloaded state.
    assert_eq!(shared.len().expect("len after reload"), 2);
    assert_eq!(shared.index_of(&node_with_tail(2)).expect("node 2"), 1);
    let got = shared.get_rev(RevIdx::from(1u32)).expect("rev 1");
    assert_eq!(got.as_blob().as_slice(), Some(&[2u8; 16][..]));
    drop((writer, reader, shared));
    std::fs::remove_dir_all(&dir).expect("cleanup");
}

#[test]
fn append_snapshot_round_trips_zlib() {
    let mut revlog = Revlog::new(Vec::new(), None).expect("empty revlog");
    let idx = revlog
        .append_snapshot(
            RevlogAppend {
                node: node_with_tail(4),
                p1: None,
                p2: None,
                linkrev: Some(3),
                text: b"alpha\n".to_vec(),
            },
            RevlogCompression::Zlib,
        )
        .expect("append");
    let got = revlog.get_rev(RevIdx::from(idx)).expect("rev");
    assert_eq!(got.as_blob().as_slice(), Some(&b"alpha\n"[..]));
    let inner = revlog.inner.lock().expect("lock poisoned");
    assert_eq!(inner.idx.as_slice()[64], b'x');
}

#[test]
fn append_snapshot_split_writes_data_file() {
    let dir = std::env::temp_dir().join(format!("mononoke-revlog-{}-split", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp dir");
    let idx_path = dir.join("log.i");
    std::fs::write(&idx_path, b"").expect("empty index");

    let mut revlog = Revlog::from_idx(&idx_path).expect("open");
    revlog
        .append_snapshot(
            RevlogAppend {
                node: node_with_tail(1),
                p1: None,
                p2: None,
                linkrev: None,
                text: b"alpha\n".to_vec(),
            },
            RevlogCompression::Zstd,
        )
        .expect("seed");
    let on_disk = std::fs::read(&idx_path).expect("index bytes");
    assert_eq!(&on_disk[0..4], &[0x00, 0x03, 0x00, 0x01]);

    let text = incompressible(INLINE_LIMIT);
    revlog
        .append_snapshot(
            RevlogAppend {
                node: node_with_tail(2),
                p1: Some(node_with_tail(1)),
                p2: None,
                linkrev: None,
                text: text.clone(),
            },
            RevlogCompression::Zstd,
        )
        .expect("split");

    let data_path = dir.join("log.d");
    let data = std::fs::read(&data_path).expect("data file");
    let index = std::fs::read(&idx_path).expect("split index");
    assert_eq!(&index[0..4], &[0x00, 0x02, 0x00, 0x01]);
    assert_eq!(index.len(), parser::indexng_size() * 2);
    assert!(data.len() > INLINE_LIMIT - parser::indexng_size());

    let reopened = Revlog::from_idx_data(&idx_path, Some(&data_path)).expect("reopen");
    let kept = reopened.get_rev(RevIdx::from(0u32)).expect("first");
    assert_eq!(kept.as_blob().as_slice(), Some(&b"alpha\n"[..]));
    let got = reopened.get_rev(RevIdx::from(1u32)).expect("second");
    assert_eq!(got.as_blob().as_slice(), Some(text.as_slice()));
    drop(reopened);
    drop(revlog);
    assert!(!dir.join(".log.i.append-tmp").exists());
    assert!(!dir.join(".log.d.append-tmp").exists());
    std::fs::remove_dir_all(&dir).expect("cleanup");
}

#[test]
fn append_snapshot_split_uses_given_data_path() {
    // A hashed `dh/` filelog's `.d` has its own hash, not the `.i` name with `d`.
    let dir = std::env::temp_dir().join(format!(
        "mononoke-revlog-{}-split-named",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp dir");
    let idx_path = dir.join("log1111.i");
    let data_path = dir.join("log2222.d");
    std::fs::write(&idx_path, b"").expect("empty index");

    let mut revlog = Revlog::from_idx_data(&idx_path, Some(&data_path)).expect("open");
    for (tail, text) in [(1, b"alpha\n".to_vec()), (2, incompressible(INLINE_LIMIT))] {
        revlog
            .append_snapshot(
                RevlogAppend {
                    node: node_with_tail(tail),
                    p1: None,
                    p2: None,
                    linkrev: None,
                    text,
                },
                RevlogCompression::Zstd,
            )
            .expect("append");
    }
    drop(revlog);

    assert!(data_path.exists(), "split did not write the given data path");
    assert!(!dir.join("log1111.d").exists());
    let reopened = Revlog::from_idx_data(&idx_path, Some(&data_path)).expect("reopen");
    let kept = reopened.get_rev(RevIdx::from(0u32)).expect("first");
    assert_eq!(kept.as_blob().as_slice(), Some(&b"alpha\n"[..]));
    drop(reopened);
    std::fs::remove_dir_all(&dir).expect("cleanup");
}

#[test]
fn append_snapshot_inline_middle_revision_stays_readable() {
    let mut built = Revlog::new(Vec::new(), None).expect("empty revlog");
    for (tail, text) in [(1u8, &b"one\n"[..]), (2, &b"two\n"[..])] {
        built
            .append_snapshot(
                RevlogAppend {
                    node: node_with_tail(tail),
                    p1: None,
                    p2: None,
                    linkrev: None,
                    text: text.to_vec(),
                },
                RevlogCompression::Zstd,
            )
            .expect("build");
    }
    let bytes = built.inner.lock().expect("lock poisoned").idx.as_slice().to_vec();
    drop(built);

    // Fresh open primes only revision 0, leaving a gap before the tip.
    let mut revlog = Revlog::new(bytes, None).expect("reload");
    let idx = revlog
        .append_snapshot(
            RevlogAppend {
                node: node_with_tail(3),
                p1: None,
                p2: None,
                linkrev: None,
                text: b"three\n".to_vec(),
            },
            RevlogCompression::Zstd,
        )
        .expect("append");
    assert_eq!(idx, 2);
    let middle = revlog.get_rev(RevIdx::from(1u32)).expect("middle");
    assert_eq!(middle.as_blob().as_slice(), Some(&b"two\n"[..]));
    let first = revlog.get_rev(RevIdx::from(0u32)).expect("first");
    assert_eq!(first.as_blob().as_slice(), Some(&b"one\n"[..]));
    let tip = revlog.get_rev(RevIdx::from(idx)).expect("tip");
    assert_eq!(tip.as_blob().as_slice(), Some(&b"three\n"[..]));
}

#[test]
fn append_snapshot_rejects_unknown_parent() {
    let mut revlog = Revlog::new(Vec::new(), None).expect("empty revlog");
    revlog
        .append_snapshot(
            RevlogAppend {
                node: node_with_tail(1),
                p1: None,
                p2: None,
                linkrev: None,
                text: b"alpha\n".to_vec(),
            },
            RevlogCompression::Zstd,
        )
        .expect("seed");

    let err = revlog
        .append_snapshot(
            RevlogAppend {
                node: node_with_tail(2),
                p1: Some(node_with_tail(9)),
                p2: None,
                linkrev: None,
                text: b"beta\n".to_vec(),
            },
            RevlogCompression::Zstd,
        )
        .expect_err("unknown parent");
    let msg = err.to_string();
    assert!(msg.contains("not found"), "{}", msg);
    assert!(revlog.get_rev(RevIdx::from(1u32)).is_err());
    let kept = revlog.get_rev(RevIdx::from(0u32)).expect("unchanged");
    assert_eq!(kept.as_blob().as_slice(), Some(&b"alpha\n"[..]));
}

/// Entry 0 carries the feature word and version 1 in its first four bytes.
/// Parents and linkrev stay `u32::MAX` from `ng_entry`.
fn stamp_separate_header(entry: &mut [u8; 64], features: u16, node_byte: u8) {
    entry[0..2].copy_from_slice(&features.to_be_bytes());
    entry[2..4].copy_from_slice(&1u16.to_be_bytes());
    entry[51] = node_byte;
}

fn zlib_frame(text: &[u8]) -> Vec<u8> {
    use std::io::Write;

    let mut enc = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(text).expect("zlib");
    let frame = enc.finish().expect("zlib");
    assert_eq!(frame[0], b'x');
    frame
}

#[test]
fn changelog_self_base_zstd_reads_full_text() {
    let chunk = zstd::encode_all(&b"hello changelog"[..], 0).expect("zstd");
    let mut entry = ng_entry(0, chunk.len() as u32, 0);
    stamp_separate_header(&mut entry, 0x0000, 1);
    let revlog = Revlog::new(entry.to_vec(), Some(chunk)).expect("revlog");
    let node = revlog.get_rev(RevIdx::from(0u32)).expect("rev");
    assert_eq!(node.as_blob().as_slice(), Some(&b"hello changelog"[..]));
}

#[test]
fn non_generaldelta_self_base_zlib_and_raw_are_literals() {
    let mut raw = vec![b'u'];
    raw.extend_from_slice(b"raw-text");
    let zlib = zlib_frame(b"zlib-text");

    for (chunk, text) in [(raw, &b"raw-text"[..]), (zlib, &b"zlib-text"[..])] {
        let mut entry = ng_entry(0, chunk.len() as u32, 0);
        stamp_separate_header(&mut entry, 0x0000, 1);
        let revlog = Revlog::new(entry.to_vec(), Some(chunk)).expect("revlog");
        let node = revlog.get_rev(RevIdx::from(0u32)).expect("rev");
        assert_eq!(node.as_blob().as_slice(), Some(text));
    }
}

#[test]
fn sparse_chain_skips_unrelated_revision() {
    let mut rev0 = vec![b'u'];
    rev0.extend_from_slice(b"aaaa");
    let mut rev1 = vec![b'u'];
    rev1.extend_from_slice(b"zzzz");
    let mut rev2 = vec![b'u'];
    // One fragment: start=0, end=1, len=1, content=b.
    rev2.extend_from_slice(&0u32.to_be_bytes());
    rev2.extend_from_slice(&1u32.to_be_bytes());
    rev2.extend_from_slice(&1u32.to_be_bytes());
    rev2.push(b'b');

    let mut entry0 = ng_entry(0, rev0.len() as u32, 0);
    stamp_separate_header(&mut entry0, 0x0002, 1);
    let mut entry1 = ng_entry(rev0.len() as u64, rev1.len() as u32, 1);
    entry1[51] = 2;
    let mut entry2 = ng_entry((rev0.len() + rev1.len()) as u64, rev2.len() as u32, 0);
    entry2[51] = 3;

    let mut idx = Vec::with_capacity(192);
    idx.extend_from_slice(&entry0);
    idx.extend_from_slice(&entry1);
    idx.extend_from_slice(&entry2);
    let mut data = Vec::new();
    data.extend_from_slice(&rev0);
    data.extend_from_slice(&rev1);
    data.extend_from_slice(&rev2);

    let revlog = Revlog::new(idx, Some(data)).expect("revlog");
    let node = revlog.get_rev(RevIdx::from(2u32)).expect("rev");
    assert_eq!(node.as_blob().as_slice(), Some(&b"baaa"[..]));
}

/// Generaldelta, not inline. Rev 0 is the literal `hello` (`baserev` 0).
/// Rev 1 and rev 2 each delta against rev 0, not against each other.
fn sparse_fixture() -> Revlog {
    let mut rev0 = vec![b'u'];
    rev0.extend_from_slice(b"hello");
    // Insert `!` at offset 5: start=5, end=5, len=1.
    let mut rev1 = vec![b'u'];
    rev1.extend_from_slice(&5u32.to_be_bytes());
    rev1.extend_from_slice(&5u32.to_be_bytes());
    rev1.extend_from_slice(&1u32.to_be_bytes());
    rev1.push(b'!');
    // Insert `?` at offset 5: start=5, end=5, len=1.
    let mut rev2 = vec![b'u'];
    rev2.extend_from_slice(&5u32.to_be_bytes());
    rev2.extend_from_slice(&5u32.to_be_bytes());
    rev2.extend_from_slice(&1u32.to_be_bytes());
    rev2.push(b'?');

    let mut entry0 = ng_entry(0, rev0.len() as u32, 0);
    stamp_separate_header(&mut entry0, 0x0002, 1);
    let mut entry1 = ng_entry(rev0.len() as u64, rev1.len() as u32, 0);
    entry1[51] = 2;
    let mut entry2 = ng_entry((rev0.len() + rev1.len()) as u64, rev2.len() as u32, 0);
    entry2[51] = 3;

    let mut idx = Vec::with_capacity(192);
    idx.extend_from_slice(&entry0);
    idx.extend_from_slice(&entry1);
    idx.extend_from_slice(&entry2);
    let mut data = Vec::new();
    data.extend_from_slice(&rev0);
    data.extend_from_slice(&rev1);
    data.extend_from_slice(&rev2);

    Revlog::new(idx, Some(data)).expect("revlog")
}

#[test]
fn reconstruction_ignores_cache_when_base_is_not_cached_rev() {
    let revlog = sparse_fixture(); // the three revisions above
    let first = revlog.get_rev(RevIdx::from(1u32)).expect("rev 1");
    assert_eq!(first.as_blob().as_slice(), Some(&b"hello!"[..]));
    let skipped = revlog.get_rev(RevIdx::from(2u32)).expect("rev 2");
    assert_eq!(skipped.as_blob().as_slice(), Some(&b"hello?"[..]));
}

fn hg_in(repo: &std::path::Path, args: &[&str]) -> String {
    let out = std::process::Command::new("hg")
        .current_dir(repo)
        .args(["--config", "storage.revlog.persistent-nodemap.slow-path=allow"])
        .args(args)
        .env("HGPLAIN", "1")
        .env("HGRCPATH", "")
        .output()
        .unwrap_or_else(|err| panic!("spawn hg {args:?}: {err}"));
    assert!(
        out.status.success(),
        "hg {args:?} failed\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// A repo whose changelog is non-inline and has a persistent nodemap.
fn nodemap_repo(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("mononoke-nodemap-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp dir");
    hg_in(&dir, &["init", "--config", "format.use-persistent-nodemap=true", "."]);
    hg_in(&dir, &["debugbuilddag", "+3000"]);
    assert!(hg_in(&dir, &["debugnodemap", "--metadata"]).contains("tip-rev"));
    dir
}

fn changelog_path(repo: &std::path::Path) -> std::path::PathBuf {
    repo.join(".hg/store/00changelog.i")
}

fn repo_nodes(repo: &std::path::Path) -> Vec<NodeHash> {
    hg_in(repo, &["log", "-T", "{node}\n"])
        .lines()
        .map(|hex| hex.parse().expect("node"))
        .collect()
}

#[test]
fn nodemap_resolves_every_node() {
    let repo = nodemap_repo("every");
    let revlog = Revlog::from_idx(changelog_path(&repo)).expect("open");
    assert!(revlog.use_persistent_nodemap().expect("nodemap"));
    let nodes = repo_nodes(&repo);
    assert_eq!(nodes.len(), 3000);
    // `hg log` lists newest first.
    for (i, node) in nodes.iter().rev().enumerate() {
        assert_eq!(revlog.index_of(node).expect("resolve"), i as u32);
    }
    drop(revlog);
    let _ = std::fs::remove_dir_all(&repo);
}

#[test]
fn unknown_node_sharing_prefix_is_not_found() {
    let repo = nodemap_repo("prefix");
    let revlog = Revlog::from_idx(changelog_path(&repo)).expect("open");
    assert!(revlog.use_persistent_nodemap().expect("nodemap"));
    let mut bytes = revlog.get_entry(RevIdx::from(5u32)).expect("entry").nodeid.sha1().as_ref().to_vec();
    bytes[19] ^= 0x01;
    let unknown = NodeHash::from_bytes(&bytes).expect("node");
    assert!(revlog.index_of(&unknown).is_err());
    drop(revlog);
    let _ = std::fs::remove_dir_all(&repo);
}

#[test]
fn nodemap_load_parses_only_the_tail() {
    let repo = nodemap_repo("tail");
    let revlog = Revlog::from_idx(changelog_path(&repo)).expect("open");
    assert!(revlog.use_persistent_nodemap().expect("nodemap"));
    revlog.load_node_map().expect("load");
    let cached = revlog.inner.lock().expect("lock").nodeidx.len();
    assert!(cached < 10, "{cached} entries parsed");
    drop(revlog);
    let _ = std::fs::remove_dir_all(&repo);
}

#[test]
fn revisions_after_docket_tip_resolve() {
    let repo = nodemap_repo("append");
    let mut revlog = Revlog::from_idx_data(changelog_path(&repo), None::<&std::path::Path>).expect("open");
    assert!(revlog.use_persistent_nodemap().expect("nodemap"));
    revlog.load_node_map().expect("load");
    let tip = revlog.get_entry(RevIdx::from(2999u32)).expect("tip");
    let manifest = hg_in(&repo, &["log", "--debug", "-r", "tip", "-T", "{manifest}"]);
    let manifest = manifest.split(':').nth(1).expect("manifest node").to_string();
    let mut parent = tip.nodeid;
    let mut added = Vec::new();
    for n in 0..2u32 {
        let text = format!("{manifest}\nuser\n{} 0\n\nextra {n}", 1_700_000_000 + n).into_bytes();
        let node = BlobNode::new(text.clone(), Some(&parent), None)
            .nodeid()
            .expect("node hash");
        revlog
            .append_snapshot(
                RevlogAppend { node, p1: Some(parent), p2: None, linkrev: Some(3000 + n), text },
                RevlogCompression::Zlib,
            )
            .expect("append");
        added.push(node);
        parent = node;
    }
    for (i, node) in added.iter().enumerate() {
        assert_eq!(revlog.index_of(node).expect("resolve"), 3000 + i as u32);
    }
    // A fresh handle finds them by scanning past the docket's tip.
    let reopened = Revlog::from_idx_data(changelog_path(&repo), None::<&std::path::Path>).expect("reopen");
    assert!(reopened.use_persistent_nodemap().expect("nodemap"));
    assert_eq!(reopened.index_of(&added[1]).expect("resolve"), 3001);
    // The server never rewrites the nodemap, so let hg catch it up.
    hg_in(&repo, &["debugupdatecaches"]);
    hg_in(&repo, &["debugnodemap", "--check"]);
    hg_in(&repo, &["verify"]);
    drop((revlog, reopened));
    let _ = std::fs::remove_dir_all(&repo);
}

#[test]
fn stale_docket_after_strip_falls_back() {
    let repo = nodemap_repo("stale");
    let docket_path = repo.join(".hg/store/00changelog.n");
    let mut docket = std::fs::read(&docket_path).expect("docket");
    let parsed = nodemap::parse_docket(&docket).expect("parse");
    let at = docket.len() - parsed.tip_node.len();
    docket[at..].fill(0);
    std::fs::write(&docket_path, &docket).expect("write docket");
    let revlog = Revlog::from_idx(changelog_path(&repo)).expect("open");
    assert!(!revlog.use_persistent_nodemap().expect("nodemap"));
    for (i, node) in repo_nodes(&repo).iter().rev().enumerate().step_by(97) {
        assert_eq!(revlog.index_of(node).expect("resolve"), i as u32);
    }
    drop(revlog);
    let _ = std::fs::remove_dir_all(&repo);
}

#[test]
fn docket_parses_mercurial_bytes() {
    let repo = nodemap_repo("docket");
    let bytes = std::fs::read(repo.join(".hg/store/00changelog.n")).expect("docket");
    let docket = nodemap::parse_docket(&bytes).expect("parse");
    let uid = String::from_utf8(docket.uid).expect("uid");
    assert!(repo.join(format!(".hg/store/00changelog-{uid}.nd")).exists());
    assert_eq!(docket.tip_rev, 2999);
    assert!(nodemap::parse_docket(&bytes[..5]).is_none());
    let _ = std::fs::remove_dir_all(&repo);
}
