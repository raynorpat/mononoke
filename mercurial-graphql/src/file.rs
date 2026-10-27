// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use juniper::graphql_object;

use mercurial::file::File;
use mercurial_types::{NodeHash, Path};

use crate::graphql_int;
use crate::manifest::GQLPath;
use crate::manifestobj::{ManifestObj, ManifestObjValue};
use crate::node::GQLNodeId;
use crate::repo::RepoCtx;

pub struct GQLFile(GQLPath, GQLNodeId);

impl GQLFile {
    pub fn new(path: &Path, id: &NodeHash) -> Self {
        GQLFile(GQLPath::from(path), GQLNodeId::from(id))
    }
}

impl ManifestObj for GQLFile {
    fn path(&self) -> &GQLPath {
        &self.0
    }

    fn nodeid(&self) -> &GQLNodeId {
        &self.1
    }
}

/// A file in the repo
#[graphql_object(
    name = "File",
    context = RepoCtx,
    impl = ManifestObjValue
)]
impl GQLFile {
    fn path(&self) -> &GQLPath {
        &self.0
    }

    fn id(&self) -> &GQLNodeId {
        &self.1
    }

    // XXX can't return raw binary data, so coerce to text
    fn contents(&self, ctx: &RepoCtx) -> Result<String, String> {
        let repo = ctx.repo();
        let filelog = repo
            .get_file_revlog(&self.0)
            .map_err(|err| format!("open {:?}: {:?}", self.0, err))?;
        let file = File::new(
            filelog
                .get_rev_by_nodeid(&self.1)
                .map_err(|err| err.to_string())?,
        );

        file.content()
            .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
            .ok_or_else(|| "no file content".to_string())
    }

    fn size(&self, ctx: &RepoCtx) -> Result<i32, String> {
        let repo = ctx.repo();
        let filelog = repo
            .get_file_revlog(&self.0)
            .map_err(|err| format!("open {:?}: {:?}", self.0, err))?;

        // Try getting the size without loading/constructing the file content, but
        // if that fails (likely because it has rename metadata or multiple parents),
        // then actually load the real data and try again.
        let size = match File::new(
            filelog
                .get_node_by_nodeid(&self.1, false)
                .map_err(|err| err.to_string())?,
        )
        .size()
        {
            Some(sz) => sz,
            None => File::new(
                filelog
                    .get_node_by_nodeid(&self.1, true)
                    .map_err(|err| err.to_string())?,
            )
            .size()
            .expect("File is missing data despite loading it?"),
        };

        graphql_int(size)
    }

    /// get changeset's parents
    fn parents(&self, ctx: &RepoCtx) -> Result<Vec<GQLFile>, String> {
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
            .map(|parent| GQLFile::new(&self.0, &parent))
            .collect())
    }
}
