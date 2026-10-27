// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Retry `ErrorKind::Interrupted` around a `Write`.

use std::io::{self, Write};

#[inline]
pub fn retry_write<T: Write>(writer: &mut T, buf: &[u8]) -> io::Result<usize> {
    loop {
        match writer.write(buf) {
            Ok(n) => return Ok(n),
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
}
