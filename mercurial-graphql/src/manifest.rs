// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::ops::Deref;

use juniper::{graphql_object, GraphQLScalar};

use mercurial_types::{NodeHash, Path};

use crate::manifestobj::{GQLManifestObj, ManifestObjValue};
use crate::node::GQLNodeId;
use crate::ready;
use crate::repo::RepoCtx;

#[derive(Clone, Debug, Eq, Hash, PartialEq, GraphQLScalar)]
#[graphql(
    name = "GQLPath",
    description = "A path in the manifest",
    to_output_with = Self::to_output,
    from_input_with = Self::from_input,
    parse_token(String)
)]
pub struct GQLPath(Path);

impl GQLPath {
    pub fn new(path: Path) -> GQLPath {
        GQLPath(path)
    }

    fn to_output(&self) -> String {
        String::from_utf8_lossy(self.0.as_slice()).into_owned()
    }

    fn from_input(input: &str) -> Result<Self, String> {
        Path::new(input.as_bytes())
            .map(GQLPath::new)
            .map_err(|err| err.to_string())
    }
}

impl<'a> From<&'a Path> for GQLPath {
    fn from(path: &'a Path) -> Self {
        GQLPath::new(path.clone())
    }
}

impl Deref for GQLPath {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.0
    }
}

pub struct GQLManifest(GQLNodeId);

impl From<NodeHash> for GQLManifest {
    fn from(id: NodeHash) -> Self {
        GQLManifest(GQLNodeId::from(id))
    }
}

/// The manifest for a specific changeset
#[graphql_object(name = "Manifest", context = RepoCtx)]
impl GQLManifest {
    /// Get manifest id
    fn id(&self) -> &GQLNodeId {
        &self.0
    }

    /// Get paths
    fn paths(&self, ctx: &RepoCtx) -> Result<Vec<GQLPath>, String> {
        let repo = ctx.repo();
        ready(repo.get_manifest_by_nodeid(&self.0))
            .map(|manifest| {
                manifest
                    .manifest()
                    .into_iter()
                    .map(|(path, _)| GQLPath::from(path))
                    .collect()
            })
            .map_err(|err| err.to_string())
    }

    /// Get entries
    fn entries(&self, ctx: &RepoCtx) -> Result<Vec<ManifestObjValue>, String> {
        let repo = ctx.repo();
        ready(repo.get_manifest_by_nodeid(&self.0))
            .map(|manifest| {
                manifest
                    .manifest()
                    .into_iter()
                    .map(|(path, details)| {
                        GQLManifestObj::new(&GQLPath::from(path), details).into_value()
                    })
                    .collect()
            })
            .map_err(|err| err.to_string())
    }

    /// Lookup entry
    fn lookup(&self, ctx: &RepoCtx, path: GQLPath) -> Result<ManifestObjValue, String> {
        let repo = ctx.repo();
        let manifest =
            ready(repo.get_manifest_by_nodeid(&self.0)).map_err(|err| err.to_string())?;
        let details = manifest
            .lookup(&path)
            .ok_or_else(|| format!("\"{}\" not present", &*path))?;
        Ok(GQLManifestObj::new(&path, details).into_value())
    }
}
