// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::error::Error as StdError;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};

/// Versions are used to ensure consistency of state across all users of the bookmark store.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Default, Serialize, Deserialize)]
pub struct Version(Option<u64>);

impl Version {
    pub fn absent() -> Self {
        Version::default()
    }
}

impl From<u64> for Version {
    fn from(v: u64) -> Self {
        Version(Some(v))
    }
}

/// Trait representing read-only operations on a bookmark store, which maintains a global mapping
/// of names to commit identifiers. Consistency is maintained using versioning.
pub trait Bookmarks: Send + 'static {
    type Value: Send + 'static;
    type Error: StdError + Send + 'static;

    /// Current value and version of the bookmark, if it exists.
    fn get(
        &self,
        key: &dyn AsRef<[u8]>,
    ) -> BoxFuture<'static, Result<Option<(Self::Value, Version)>, Self::Error>>;
}

/// Trait representing write operations on a bookmark store. Consistency is maintained using
/// versioning.
pub trait BookmarksMut: Bookmarks {
    /// A new version, or `None` if the operation could not be completed due to a version mismatch.
    fn set(
        &self,
        key: &dyn AsRef<[u8]>,
        value: &Self::Value,
        version: &Version,
    ) -> BoxFuture<'static, Result<Option<Version>, Self::Error>>;

    fn delete(
        &self,
        key: &dyn AsRef<[u8]>,
        version: &Version,
    ) -> BoxFuture<'static, Result<Option<Version>, Self::Error>>;

    /// Create a bookmark. The initial version is always [`Version::absent`].
    fn create(
        &self,
        key: &dyn AsRef<[u8]>,
        value: &Self::Value,
    ) -> BoxFuture<'static, Result<Option<Version>, Self::Error>> {
        self.set(key, value, &Version::absent())
    }
}

/// Bookmark stores that implement this trait support efficiently enumerating all of
/// the stored bookmarks. Bookmarks are not guaranteed to be returned in any particular order.
pub trait ListBookmarks: Bookmarks {
    fn keys(&self) -> BoxStream<'static, Result<Vec<u8>, Self::Error>>;
}

/// Convenience trait to capture listing bookmarks and write operations.
pub trait ListBookmarksMut: ListBookmarks + BookmarksMut {}

/// Ensure that trait objects can be created from the traits here.
#[allow(dead_code)]
fn _assert_objects() {
    use std::io;

    let _: Box<dyn Bookmarks<Value = Vec<u8>, Error = io::Error>>;
    let _: Box<dyn BookmarksMut<Value = Vec<u8>, Error = io::Error>>;
    let _: Box<dyn ListBookmarks<Value = Vec<u8>, Error = io::Error>>;
    let _: Box<dyn ListBookmarksMut<Value = Vec<u8>, Error = io::Error>>;
}
