// Copyright (c) 2017-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::mem;
use std::sync::Arc;

use mercurial_types::{BlobNode, NodeHash};

/// Return the "weight" of a type.
///
/// This is an abstract value which can mean anything, but it typically
/// relates to memory consumption. The expectation is that calling `get_weight()`
/// fairly cheap - ideally O(1).
pub trait Weight {
    fn get_weight(&self) -> usize;
}

impl Weight for String {
    #[inline]
    fn get_weight(&self) -> usize {
        mem::size_of::<String>() + self.capacity()
    }
}

impl<T> Weight for Arc<T>
where
    T: Weight + ?Sized,
{
    #[inline]
    fn get_weight(&self) -> usize {
        mem::size_of::<Arc<T>>() + (**self).get_weight()
    }
}

impl Weight for NodeHash {
    #[inline]
    fn get_weight(&self) -> usize {
        mem::size_of::<Self>()
    }
}

impl<T> Weight for BlobNode<T>
where
    T: AsRef<[u8]>,
{
    #[inline]
    fn get_weight(&self) -> usize {
        mem::size_of::<Self>() + self.size().unwrap_or(0)
    }
}
