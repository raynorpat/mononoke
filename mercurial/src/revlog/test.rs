// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

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
