#![deny(warnings)]
// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

pub mod bundle2;
pub mod bundle2_encode;
pub mod changegroup;
mod chunk;
pub mod part_encode;
mod part_header;
mod part_inner;
mod part_outer;
pub mod parts;
#[cfg(test)]
mod quickcheck_types;
mod stream_start;
mod types;

#[cfg(test)]
mod test;

mod errors;
pub use errors::*;
mod utils;

pub use bundle2_encode::Bundle2EncodeBuilder;
pub use chunk::Chunk;
pub use part_header::PartHeader;
pub use part_inner::InnerPart;
pub use types::StreamHeader;

#[derive(Debug, Eq, PartialEq)]
pub enum Bundle2Item {
    Start(StreamHeader),
    Header(PartHeader),
    Inner(InnerPart),
}

impl Bundle2Item {
    pub fn is_start(&self) -> bool {
        match self {
            &Bundle2Item::Start(_) => true,
            _ => false,
        }
    }

    pub fn is_inner(&self) -> bool {
        match self {
            &Bundle2Item::Inner(_) => true,
            _ => false,
        }
    }

    pub fn stream_header(self) -> StreamHeader {
        match self {
            Bundle2Item::Start(stream_header) => stream_header,
            _ => panic!("stream_header called on a Bundle2Item that isn't Start!"),
        }
    }

    pub fn inner_part(self) -> InnerPart {
        match self {
            Bundle2Item::Inner(inner_part) => inner_part,
            _ => panic!("inner_part called on a Bundle2Item that isn't Inner!"),
        }
    }
}
