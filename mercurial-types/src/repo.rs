// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::BoxStream;

use crate::changeset::Changeset;
use crate::manifest::Manifest;
use crate::nodehash::NodeHash;

pub trait Repo: 'static {
    type Error: Send + 'static;

    /// Return a stream of all changeset ids
    ///
    /// This returns a Stream which produces each changeset that's reachable from a
    /// head exactly once. This does not guarantee any particular order, but something
    /// approximating a BFS traversal from the heads would be ideal.
    ///
    /// XXX Is "exactly once" too strong? This probably requires a "has seen" structure which
    /// will be O(changesets) in size. Probably OK up to 10-100M changesets.
    fn get_changesets(&self) -> BoxStream<'static, Result<NodeHash, Self::Error>>;

    fn get_heads(&self) -> BoxStream<'static, Result<NodeHash, Self::Error>>;
    fn changeset_exists(&self, nodeid: &NodeHash) -> BoxFuture<'static, Result<bool, Self::Error>>;
    fn get_changeset_by_nodeid(
        &self,
        nodeid: &NodeHash,
    ) -> BoxFuture<'static, Result<Box<dyn Changeset>, Self::Error>>;
    fn get_manifest_by_nodeid(
        &self,
        nodeid: &NodeHash,
    ) -> BoxFuture<'static, Result<Box<dyn Manifest<Error = Self::Error>>, Self::Error>>;

    fn boxed(self) -> Box<dyn Repo<Error = Self::Error>>
    where
        Self: Sized,
    {
        Box::new(self)
    }
}

impl<RE> Repo for Box<dyn Repo<Error = RE>>
where
    RE: Send + 'static,
{
    type Error = RE;

    fn get_changesets(&self) -> BoxStream<'static, Result<NodeHash, Self::Error>> {
        (**self).get_changesets()
    }

    fn get_heads(&self) -> BoxStream<'static, Result<NodeHash, Self::Error>> {
        (**self).get_heads()
    }

    fn changeset_exists(
        &self,
        nodeid: &NodeHash,
    ) -> BoxFuture<'static, Result<bool, Self::Error>> {
        (**self).changeset_exists(nodeid)
    }

    fn get_changeset_by_nodeid(
        &self,
        nodeid: &NodeHash,
    ) -> BoxFuture<'static, Result<Box<dyn Changeset>, Self::Error>> {
        (**self).get_changeset_by_nodeid(nodeid)
    }

    fn get_manifest_by_nodeid(
        &self,
        nodeid: &NodeHash,
    ) -> BoxFuture<'static, Result<Box<dyn Manifest<Error = Self::Error>>, Self::Error>> {
        (**self).get_manifest_by_nodeid(nodeid)
    }
}

impl<RE> Repo for Arc<dyn Repo<Error = RE>>
where
    RE: Send + 'static,
{
    type Error = RE;

    fn get_changesets(&self) -> BoxStream<'static, Result<NodeHash, Self::Error>> {
        (**self).get_changesets()
    }

    fn get_heads(&self) -> BoxStream<'static, Result<NodeHash, Self::Error>> {
        (**self).get_heads()
    }

    fn changeset_exists(
        &self,
        nodeid: &NodeHash,
    ) -> BoxFuture<'static, Result<bool, Self::Error>> {
        (**self).changeset_exists(nodeid)
    }

    fn get_changeset_by_nodeid(
        &self,
        nodeid: &NodeHash,
    ) -> BoxFuture<'static, Result<Box<dyn Changeset>, Self::Error>> {
        (**self).get_changeset_by_nodeid(nodeid)
    }

    fn get_manifest_by_nodeid(
        &self,
        nodeid: &NodeHash,
    ) -> BoxFuture<'static, Result<Box<dyn Manifest<Error = Self::Error>>, Self::Error>> {
        (**self).get_manifest_by_nodeid(nodeid)
    }
}
