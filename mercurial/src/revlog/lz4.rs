// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

// Mercurial's lz4revlog prefix stores the raw size as a little-endian u32
// followed by one raw lz4 block. `lz4_flex` uses that same layout.

pub fn decompress(input: &[u8]) -> Result<Vec<u8>, String> {
    lz4_flex::block::decompress_size_prepended(input).map_err(|err| err.to_string())
}
