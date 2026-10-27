// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use serde::{Deserialize, Serialize};

use crate::hash::Sha1;
use crate::NodeHash;

/// Representation of a blob of data.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub enum Blob<T> {
    /// Modified data with no corresponding hash
    Dirty(T),
    /// Clean data paired with its hash
    Clean(T, BlobHash),
    /// External data; we only have its hash
    Extern(BlobHash),
    /// External data; we only have its nodeid
    NodeId(NodeHash),
}

/// Hash of a blob.
///
/// This is implemented as a `Sha1` hash.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub struct BlobHash(Sha1);

/// Compute the hash of a byte slice, resulting in a `BlobHash`
impl From<&[u8]> for BlobHash {
    fn from(data: &[u8]) -> Self {
        BlobHash(Sha1::from(data))
    }
}

impl<T> Blob<T>
where
    T: AsRef<[u8]>,
{
    /// Clean a `Blob` by computing its hash. Leaves non-`Dirty` blobs unchanged.
    pub fn clean(self) -> Self {
        match self {
            Blob::Dirty(data) => {
                let hash = BlobHash::from(data.as_ref());
                Blob::Clean(data, hash)
            }
            b @ (Blob::Clean(..) | Blob::Extern(..) | Blob::NodeId(..)) => b,
        }
    }

    pub fn size(&self) -> Option<usize> {
        match self {
            Blob::Dirty(data) => Some(data.as_ref().len()),
            Blob::Clean(_, data) => Some(data.as_ref().len()),
            Blob::Extern(..) | Blob::NodeId(..) => None,
        }
    }

    pub fn as_inner(&self) -> Option<&T> {
        match self {
            Blob::Dirty(data) => Some(data),
            Blob::Clean(data, _) => Some(data),
            Blob::Extern(..) | Blob::NodeId(..) => None,
        }
    }

    pub fn into_inner(self) -> Option<T> {
        match self {
            Blob::Dirty(data) => Some(data),
            Blob::Clean(data, _) => Some(data),
            Blob::Extern(..) | Blob::NodeId(..) => None,
        }
    }

    pub fn as_slice(&self) -> Option<&[u8]> {
        match self {
            Blob::Dirty(data) => Some(data.as_ref()),
            Blob::Clean(data, _) => Some(data.as_ref()),
            Blob::Extern(..) | Blob::NodeId(..) => None,
        }
    }
}

/// Construct a dirty blob from raw data.
impl From<&[u8]> for Blob<Vec<u8>> {
    fn from(data: &[u8]) -> Self {
        Blob::Dirty(Vec::from(data))
    }
}

impl From<Vec<u8>> for Blob<Vec<u8>> {
    fn from(data: Vec<u8>) -> Self {
        Blob::Dirty(data)
    }
}

/// Get a reference to the `Blob`'s data, if it has some (ie, not `Extern`)
impl<'a, T> From<&'a Blob<T>> for Option<&'a [u8]>
where
    T: AsRef<[u8]>,
{
    fn from(blob: &'a Blob<T>) -> Option<&'a [u8]> {
        match blob {
            Blob::Clean(data, _) => Some(data.as_ref()),
            Blob::Dirty(data) => Some(data.as_ref()),
            Blob::Extern(..) | Blob::NodeId(..) => None,
        }
    }
}

/// Construct an `Extern` `Blob` from a `BlobHash`
impl<T> From<BlobHash> for Blob<T> {
    fn from(bh: BlobHash) -> Self {
        Blob::Extern(bh)
    }
}

/// Construct a `NodeId` `Blob` from a `NodeHash`
impl<T> From<NodeHash> for Blob<T> {
    fn from(id: NodeHash) -> Self {
        Blob::NodeId(id)
    }
}

/// Get a reference for the underlying Sha1 bytes of a `BlobHash`
impl AsRef<[u8]> for BlobHash {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}
