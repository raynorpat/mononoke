// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Netstring framing shared by crate tests. `<ascii-len>:<data>,`.

use std::io;

use bytes::{Bytes, BytesMut};

pub fn decode_netstring(buf: &mut BytesMut) -> io::Result<Option<Bytes>> {
    let colon = match buf.iter().position(|b| *b == b':') {
        Some(i) => i,
        None => {
            if buf.len() > 10 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "netstring length too long",
                ));
            }
            return Ok(None);
        }
    };
    if colon == 0 || !buf[..colon].iter().all(|b| b.is_ascii_digit()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid netstring length",
        ));
    }
    let len_str = std::str::from_utf8(&buf[..colon])
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let len: usize = len_str
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let header = colon + 1;
    let total = header
        .checked_add(len)
        .and_then(|n| n.checked_add(1))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "netstring length overflow"))?;
    if buf.len() < total {
        return Ok(None);
    }
    if buf[header + len] != b',' {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "netstring missing comma",
        ));
    }
    let frame = buf.split_to(total);
    Ok(Some(frame.freeze().slice(header..header + len)))
}
