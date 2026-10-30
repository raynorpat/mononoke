// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

// Parsers for Mercurial revlog index entries and chunk payloads.
use std::io::{Cursor, Read};

use bitflags::bitflags;
use flate2::read::ZlibDecoder;
use zstd::stream::read::Decoder as ZstdDecoder;

use mercurial_types::bdiff::Delta;
use mercurial_types::NodeHash;

use super::lz4;
use super::revidx::RevIdx;

// hack until I work out how to propagate proper E type
#[allow(non_upper_case_globals, non_snake_case, dead_code)]
pub mod Badness {
    pub const IO: u32 = 0;
    pub const Version: u32 = 1;
    pub const Features: u32 = 2;
    pub const BadZlib: u32 = 3;
    pub const BadLZ4: u32 = 4;
}

/// Failure while reading a revlog index entry or chunk.
#[derive(Debug)]
#[allow(dead_code)] // payloads are only formatted, by Debug or Display
pub enum ParseError {
    Incomplete,
    NoMatch,
    BadZlib(String),
    BadLz4(String),
    BadZstd(String),
    BadFlags(u16),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::BadFlags(bits) => write!(f, "unknown revision flags {:#06x}", bits),
            other => write!(f, "{:?}", other),
        }
    }
}

pub type ParseResult<'a, T> = Result<(&'a [u8], T), ParseError>;

bitflags! {
    /// `Revlog` features
    #[derive(Copy, Clone, Debug, Eq, PartialEq)]
    pub struct Features: u16 {
        const INLINE        = 1 << 0;
        const GENERAL_DELTA = 1 << 1;
    }
}

pub const INLINE: Features = Features::INLINE;
pub const GENERAL_DELTA: Features = Features::GENERAL_DELTA;

bitflags! {
    /// Per-revision flags
    #[derive(Copy, Clone, Debug, Eq, PartialEq)]
    pub struct IdxFlags: u16 {
        const EXTSTORED = 1 << 13;
        const CENSORED = 1 << 15;
    }
}

/// Revlog version number
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Version {
    Revlog0 = 0,
    RevlogNG = 1,
}

/// Revlog header
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Header {
    pub version: Version,
    pub features: Features,
}

/// Entry entry for a revision
#[derive(Copy, Clone, Debug)]
pub struct Entry {
    pub offset: u64, // offset of content (delta/literal) in datafile (or inlined)
    pub flags: IdxFlags, // unused?
    pub compressed_len: u32, // compressed content size
    pub len: Option<u32>, // size of final file (after applying deltas)
    pub baserev: Option<RevIdx>, // base/previous rev for deltas (None if literal)
    pub linkrev: Option<RevIdx>, // changeset id
    pub p1: Option<RevIdx>, // parent p1
    pub p2: Option<RevIdx>, // parent p2
    pub nodeid: NodeHash, // nodeid
}

impl Entry {
    pub fn nodeid(&self) -> &NodeHash {
        &self.nodeid
    }
}

fn read_u16(input: &[u8]) -> u16 {
    u16::from_be_bytes([input[0], input[1]])
}

fn read_u32(input: &[u8]) -> u32 {
    u32::from_be_bytes([input[0], input[1], input[2], input[3]])
}

fn read_u48(input: &[u8]) -> u64 {
    ((input[0] as u64) << 40)
        + ((input[1] as u64) << 32)
        + ((input[2] as u64) << 24)
        + ((input[3] as u64) << 16)
        + ((input[4] as u64) << 8)
        + ((input[5] as u64) << 0)
}

fn optional_idx(v: u32) -> Option<RevIdx> {
    if v == u32::MAX {
        None
    } else {
        Some(v.into())
    }
}

/// Parse the revlog header.
///
/// Layout is a 2-byte big-endian feature word, then a 2-byte big-endian
/// version (`0` revlog0, `1` revlog NG).
pub fn header(input: &[u8]) -> ParseResult<'_, Header> {
    if input.len() < 4 {
        return Err(ParseError::Incomplete);
    }
    let features = read_u16(&input[0..2]);
    let version = read_u16(&input[2..4]);
    let vers = match version {
        0 => Version::Revlog0,
        1 => Version::RevlogNG,
        _ => panic!("bad version"),
    };
    let features = match Features::from_bits(features) {
        Some(features) => features,
        None => panic!("bad features"),
    };
    Ok((
        &input[4..],
        Header {
            version: vers,
            features,
        },
    ))
}

pub fn indexng_size() -> usize {
    6 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 32
}

/// Parse an "NG" revlog entry.
///
/// 64 bytes: 6-byte offset, 2-byte flags, four `u32`s (compressed length,
/// uncompressed length, base, link), two parent `u32`s, then a 32-byte node
/// slot of which the first 20 bytes are the node id. `u32::MAX` is `None`.
pub fn indexng(input: &[u8]) -> ParseResult<'_, Entry> {
    let size = indexng_size();
    if input.len() < size {
        return Err(ParseError::Incomplete);
    }
    let hash = &input[32..64];
    let bits = read_u16(&input[6..8]);
    let flags = IdxFlags::from_bits(bits).ok_or(ParseError::BadFlags(bits))?;
    let entry = Entry {
        offset: read_u48(&input[0..6]),
        flags,
        compressed_len: read_u32(&input[8..12]),
        len: Some(read_u32(&input[12..16])),
        baserev: optional_idx(read_u32(&input[16..20])),
        linkrev: optional_idx(read_u32(&input[20..24])),
        p1: optional_idx(read_u32(&input[24..28])),
        p2: optional_idx(read_u32(&input[28..32])),
        nodeid: NodeHash::from_bytes(&hash[..20]).expect("bad bytes for sha"),
    };
    Ok((&input[size..], entry))
}

pub fn index0_size() -> usize {
    4 + 4 + 4 + 4 + 4 + 4 + 4 + 20
}

/// Parse an original revlog entry.
pub fn index0(input: &[u8]) -> ParseResult<'_, Entry> {
    let size = index0_size();
    if input.len() < size {
        return Err(ParseError::Incomplete);
    }
    // The v0 entry starts with a header; the fields used below follow it.
    let _ = header(input)?;
    let hash = &input[28..48];
    let entry = Entry {
        offset: read_u32(&input[4..8]) as u64,
        flags: IdxFlags::empty(),
        compressed_len: read_u32(&input[8..12]),
        len: None,
        baserev: optional_idx(read_u32(&input[12..16])),
        linkrev: optional_idx(read_u32(&input[16..20])),
        p1: optional_idx(read_u32(&input[20..24])),
        p2: optional_idx(read_u32(&input[24..28])),
        nodeid: NodeHash::from_bytes(&hash[..20]).expect("bad bytes for sha"),
    };
    Ok((&input[size..], entry))
}

/// Parse a single delta fragment: start, end, and a length-prefixed payload.
pub fn delta(input: &[u8]) -> ParseResult<'_, Delta> {
    if input.len() < 12 {
        return Err(ParseError::Incomplete);
    }
    let len = read_u32(&input[8..12]) as usize;
    if input.len() < 12 + len {
        return Err(ParseError::Incomplete);
    }
    let content = input[12..12 + len].to_vec();
    Ok((
        &input[12 + len..],
        Delta {
            start: read_u32(&input[0..4]) as usize,
            end: read_u32(&input[4..8]) as usize,
            content,
        },
    ))
}

/// Parse zero or more deltas.
///
/// `many0` stops on a hard mismatch and fails when a fragment is `Incomplete`.
/// A short tail is not a successful end.
fn deltas(input: &[u8]) -> Result<(&[u8], Vec<Delta>), ParseError> {
    let mut rest = input;
    let mut out = Vec::new();
    loop {
        if rest.is_empty() {
            return Ok((rest, out));
        }
        match delta(rest) {
            Ok((next, one)) => {
                if next.len() == rest.len() {
                    return Err(ParseError::Incomplete);
                }
                out.push(one);
                rest = next;
            }
            Err(ParseError::Incomplete) => return Err(ParseError::Incomplete),
            Err(err) => return Err(err),
        }
    }
}

fn zlib_decompress(input: &[u8]) -> Result<(Vec<u8>, usize), ParseError> {
    let mut decoder = ZlibDecoder::new(input);
    let mut data = Vec::new();
    match decoder.read_to_end(&mut data) {
        Ok(_) => Ok((data, decoder.total_in() as usize)),
        Err(err) => Err(ParseError::BadZlib(err.to_string())),
    }
}

/// Zstd frame magic (`28 b5 2f fd`). The payload is raw literal or delta bytes.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];

fn is_zstd(input: &[u8]) -> bool {
    input.starts_with(&ZSTD_MAGIC)
}

/// Decompress one zstd frame. `used` is the number of input bytes consumed.
fn zstd_decompress(input: &[u8]) -> Result<(Vec<u8>, usize), ParseError> {
    let mut cursor = Cursor::new(input);
    let mut data = Vec::new();
    let mut decoder = ZstdDecoder::with_buffer(cursor.by_ref())
        .map_err(|err| ParseError::BadZstd(err.to_string()))?
        .single_frame();
    decoder
        .read_to_end(&mut data)
        .map_err(|err| ParseError::BadZstd(err.to_string()))?;
    drop(decoder);
    Ok((data, cursor.position() as usize))
}

// One framed chunk of deltas. `None` means this byte is not a frame marker,
// so the caller leaves those bytes unconsumed.
fn delta_frame(input: &[u8]) -> Result<Option<(&[u8], Vec<Delta>)>, ParseError> {
    if is_zstd(input) {
        let (data, used) = zstd_decompress(input)?;
        let (rest, parsed) = deltas(&data)?;
        if !rest.is_empty() {
            return Err(ParseError::Incomplete);
        }
        return Ok(Some((&input[used..], parsed)));
    }
    match input[0] {
        b'u' => {
            let (rest, parsed) = deltas(&input[1..])?;
            Ok(Some((rest, parsed)))
        }
        0 => {
            let (rest, parsed) = deltas(input)?;
            Ok(Some((rest, parsed)))
        }
        b'x' => {
            let (data, used) = zlib_decompress(input)?;
            let (rest, parsed) = deltas(&data)?;
            if !rest.is_empty() {
                return Err(ParseError::Incomplete);
            }
            Ok(Some((&input[used..], parsed)))
        }
        b'4' => {
            let data = lz4::decompress(&input[1..]).map_err(ParseError::BadLz4)?;
            let (rest, parsed) = deltas(&data)?;
            if !rest.is_empty() {
                return Err(ParseError::Incomplete);
            }
            Ok(Some((&[], parsed)))
        }
        _ => Ok(None),
    }
}

/// Parse a chunk of data that contains some deltas.
pub fn deltachunk(input: &[u8]) -> ParseResult<'_, Vec<Delta>> {
    let mut rest = input;
    let mut out = Vec::new();
    while !rest.is_empty() {
        let Some((next, parsed)) = delta_frame(rest)? else {
            break;
        };
        if next.len() == rest.len() {
            break;
        }
        out.extend(parsed);
        rest = next;
    }
    Ok((rest, out))
}

/// Parse some literal data, possibly compressed.
pub fn literal(input: &[u8]) -> ParseResult<'_, Vec<u8>> {
    if input.is_empty() {
        return Err(ParseError::NoMatch);
    }
    // The frame payload is the literal text. It is not wrapped in `u`/`x`.
    if is_zstd(input) {
        let (data, used) = zstd_decompress(input)?;
        return Ok((&input[used..], data));
    }
    match input[0] {
        0 => Ok((&[], input.to_vec())),
        b'x' => {
            let (data, used) = zlib_decompress(input)?;
            Ok((&input[used..], data))
        }
        b'4' => {
            let data = lz4::decompress(&input[1..]).map_err(ParseError::BadLz4)?;
            Ok((&[], data))
        }
        b'u' => Ok((&[], input[1..].to_vec())),
        _ => Err(ParseError::NoMatch),
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;

    use flate2::Compression;
    use flate2::write::ZlibEncoder;

    use super::{Features, GENERAL_DELTA, Header, INLINE, Version, deltachunk, header};

    /// One complete empty fragment, then four bytes that start another fragment.
    fn truncated_delta_payload() -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&0u32.to_be_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes());
        payload.extend_from_slice(&[0x01, 0x02, 0x03, 0x04]);
        payload
    }

    #[test]
    fn truncated_zlib_delta_is_error() {
        let mut enc = ZlibEncoder::new(Vec::new(), Compression::default());
        enc.write_all(&truncated_delta_payload()).unwrap();
        let frame = enc.finish().unwrap();
        assert_eq!(frame[0], b'x');
        assert!(deltachunk(&frame).is_err());
    }

    #[test]
    fn truncated_lz4_delta_is_error() {
        let compressed = lz4_flex::block::compress_prepend_size(&truncated_delta_payload());
        let mut frame = vec![b'4'];
        frame.extend_from_slice(&compressed);
        assert!(deltachunk(&frame).is_err());
    }

    #[test]
    fn test_header_0() {
        let d = [0x00, 0x00, 0x00, 0x00];
        assert_eq!(
            header(&d[..]).unwrap(),
            (
                &b""[..],
                Header {
                    version: Version::Revlog0,
                    features: Features::empty(),
                }
            )
        )
    }

    #[test]
    fn test_header_1() {
        let d = [0x00, 0x00, 0x00, 0x01];
        assert_eq!(
            header(&d[..]).unwrap(),
            (
                &b""[..],
                Header {
                    version: Version::RevlogNG,
                    features: Features::empty(),
                }
            )
        )
    }

    #[test]
    fn test_header_feat_1() {
        let d = [0x00, 0x01, 0x00, 0x01];
        assert_eq!(
            header(&d[..]).unwrap(),
            (
                &b""[..],
                Header {
                    version: Version::RevlogNG,
                    features: INLINE,
                }
            )
        )
    }

    #[test]
    fn test_header_feat_2() {
        let d = [0x00, 0x02, 0x00, 0x01];
        assert_eq!(
            header(&d[..]).unwrap(),
            (
                &b""[..],
                Header {
                    version: Version::RevlogNG,
                    features: GENERAL_DELTA,
                }
            )
        )
    }

    #[test]
    fn test_header_feat_3() {
        let d = [0x00, 0x03, 0x00, 0x01];
        assert_eq!(
            header(&d[..]).unwrap(),
            (
                &b""[..],
                Header {
                    version: Version::RevlogNG,
                    features: INLINE | GENERAL_DELTA,
                }
            )
        )
    }
}
