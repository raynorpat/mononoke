// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use juniper::graphql_object;

use mercurial_types::{Changeset, NodeHash, Time};

use crate::graphql_int;
use crate::manifest::{GQLManifest, GQLPath};
use crate::node::GQLNodeId;
use crate::ready;
use crate::repo::RepoCtx;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct GQLChangeset(GQLNodeId);

impl From<GQLNodeId> for GQLChangeset {
    fn from(csid: GQLNodeId) -> Self {
        GQLChangeset(csid)
    }
}

impl From<NodeHash> for GQLChangeset {
    fn from(csid: NodeHash) -> Self {
        GQLChangeset(GQLNodeId::from(csid))
    }
}

pub struct GQLTime(Time);

/// Time a commit was made
#[graphql_object(name = "Time", context = RepoCtx)]
impl GQLTime {
    /// UTC time in seconds since Unix Epoch
    fn time(&self) -> Result<i32, String> {
        graphql_int(self.0.time)
    }

    /// Timezone offset in seconds from UTC
    fn tz(&self) -> i32 {
        self.0.tz
    }
}

/// A single atomic change
#[graphql_object(name = "Changeset", context = RepoCtx)]
impl GQLChangeset {
    /// changeset identifier
    fn id(&self) -> &GQLNodeId {
        &self.0
    }

    /// manifest identifier
    fn manifest(&self, ctx: &RepoCtx) -> Result<GQLManifest, String> {
        let repo = ctx.repo();
        ready(repo.get_changeset_by_nodeid(&self.0))
            .map(|cs| GQLManifest::from(*cs.manifestid()))
            .map_err(|err| err.to_string())
    }

    /// user
    fn user(&self, ctx: &RepoCtx) -> Result<String, String> {
        let repo = ctx.repo();
        ready(repo.get_changeset_by_nodeid(&self.0))
            .map(|cs| String::from_utf8_lossy(cs.user()).into_owned())
            .map_err(|err| err.to_string())
    }

    /// commit comments
    fn comments(&self, ctx: &RepoCtx) -> Result<String, String> {
        let repo = ctx.repo();
        ready(repo.get_changeset_by_nodeid(&self.0))
            .map(|cs| String::from_utf8_lossy(cs.comments()).into_owned())
            .map_err(|err| err.to_string())
    }

    /// commit time
    fn time(&self, ctx: &RepoCtx) -> Result<GQLTime, String> {
        let repo = ctx.repo();
        ready(repo.get_changeset_by_nodeid(&self.0))
            .map(|cs| GQLTime(*cs.time()))
            .map_err(|err| err.to_string())
    }

    /// paths affected by commit
    fn paths(&self, ctx: &RepoCtx) -> Result<Vec<GQLPath>, String> {
        let repo = ctx.repo();
        ready(repo.get_changeset_by_nodeid(&self.0))
            .map(|cs| cs.files().iter().map(GQLPath::from).collect())
            .map_err(|err| err.to_string())
    }

    /// get changeset's parents
    fn parents(&self, ctx: &RepoCtx) -> Result<Vec<GQLChangeset>, String> {
        let repo = ctx.repo();
        ready(repo.get_changeset_by_nodeid(&self.0))
            .map(|cs| cs.parents().into_iter().map(GQLChangeset::from).collect())
            .map_err(|err| err.to_string())
    }
}
