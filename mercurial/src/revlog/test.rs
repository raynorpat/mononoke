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
    let unknown = node_with_tail(9);
    let idx = revlog
        .append_snapshot(
            RevlogAppend {
                node,
                p1: None,
                p2: Some(unknown),
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
    // Header occupies the high bytes of rev 0's offset; the data follows the 64-byte entry.
    assert_eq!(entry.offset, 64);
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
    std::fs::remove_dir_all(&dir).expect("cleanup");
}
