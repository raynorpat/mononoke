// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use juniper::graphql_object;

use mercurial::symlink::Symlink;
use mercurial_types::{NodeHash, Path};

use crate::graphql_int;
use crate::manifest::GQLPath;
use crate::manifestobj::{ManifestObj, ManifestObjValue};
use crate::node::GQLNodeId;
use crate::repo::RepoCtx;

pub struct GQLSymlink(GQLPath, GQLNodeId);

impl GQLSymlink {
    pub fn new(path: &Path, id: &NodeHash) -> Self {
        GQLSymlink(GQLPath::from(path), GQLNodeId::from(id))
    }
}

impl ManifestObj for GQLSymlink {
    fn path(&self) -> &GQLPath {
        &self.0
    }

    fn nodeid(&self) -> &GQLNodeId {
        &self.1
    }
}

/// A file in the repo
#[graphql_object(
    name = "Symlink",
    context = RepoCtx,
    impl = ManifestObjValue
)]
impl GQLSymlink {
    fn path(&self) -> &GQLPath {
        &self.0
    }

    fn id(&self) -> &GQLNodeId {
        &self.1
    }

    fn target(&self, ctx: &RepoCtx) -> Result<GQLPath, String> {
        let repo = ctx.repo();
        let filelog = repo
            .get_file_revlog(&self.0)
            .map_err(|err| format!("open {:?}: {:?}", self.0, err))?;
        let node = filelog
            .get_rev_by_nodeid(&self.1)
            .map_err(|err| err.to_string())?;
        let symlink = Symlink::new(node);

        symlink
            .path()
            .map_err(|err| err.to_string())?
            .ok_or_else(|| "no path content".to_string())
            .map(GQLPath::new)
    }

    fn size(&self, ctx: &RepoCtx) -> Result<i32, String> {
        let repo = ctx.repo();
        let entry = repo
            .get_file_revlog(&self.0)
            .map_err(|err| format!("open {:?}: {:?}", self.0, err))?
            .get_entry_by_nodeid(&self.1)
            .map_err(|err| err.to_string())?;

        match entry.len {
            Some(sz) => graphql_int(sz),
            None => Err("File has no size".into()),
        }
    }

    /// get changeset's parents
    fn parents(&self, ctx: &RepoCtx) -> Result<Vec<GQLSymlink>, String> {
        let repo = ctx.repo();
        let filelog = repo
            .get_file_revlog(&self.0)
            .map_err(|err| format!("open {:?}: {:?}", self.0, err))?;
        let node = filelog
            .get_rev_by_nodeid(&self.1)
            .map_err(|err| err.to_string())?;

        Ok(node
            .parents()
            .into_iter()
            .map(|parent| GQLSymlink::new(&self.0, &parent))
            .collect())
    }
}
