#![deny(warnings)]
// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

mod compressor;
mod decompressor;
pub mod membuf;
pub mod metered;
#[cfg(test)]
mod retry;

#[cfg(test)]
mod test;

pub use compressor::{Compressor, CompressorType};
pub use decompressor::{Decompressor, DecompressorType};

pub const ZSTD_DEFAULT_LEVEL: i32 = 1;
