// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

const HEX: &[u8] = b"0123456789ABCDEF";

// The old `url` crate's USERINFO_ENCODE_SET, plus `,` (Python urllib encodes it).
// This does not exactly match urllib, but it encodes `=`, which bundle2 needs.
fn hg_should_encode(byte: u8) -> bool {
    if byte < 0x20 || byte > 0x7E {
        return true;
    }
    matches!(
        byte,
        b' ' | b'"'
            | b'#'
            | b'<'
            | b'>'
            | b'`'
            | b'?'
            | b'{'
            | b'}'
            | b'/'
            | b':'
            | b';'
            | b'='
            | b'@'
            | b'['
            | b'\\'
            | b']'
            | b'^'
            | b'|'
            | b','
    )
}

pub fn percent_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for &byte in input.as_bytes() {
        if hg_should_encode(byte) {
            out.push('%');
            out.push(HEX[(byte >> 4) as usize] as char);
            out.push(HEX[(byte & 0xf) as usize] as char);
        } else {
            out.push(byte as char);
        }
    }
    out
}

/// Undo URL percent-encoding; `+` is a space.
pub fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut idx = 0;
    while idx < bytes.len() {
        match bytes[idx] {
            b'+' => {
                out.push(b' ');
                idx += 1;
            }
            b'%' if idx + 2 < bytes.len() => {
                let hex = &input[idx + 1..idx + 3];
                if let Ok(value) = u8::from_str_radix(hex, 16) {
                    out.push(value);
                    idx += 3;
                } else {
                    out.push(b'%');
                    idx += 1;
                }
            }
            byte => {
                out.push(byte);
                idx += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}
