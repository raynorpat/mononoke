// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use juniper::graphql_interface;

use mercurial::manifest::Details;

use crate::file::GQLFile;
use crate::manifest::GQLPath;
use crate::node::GQLNodeId;
use crate::repo::RepoCtx;
use crate::symlink::GQLSymlink;

/// An object in the manifest
#[graphql_interface(for = [GQLFile, GQLSymlink], context = RepoCtx, name = "ManifestObj")]
pub trait ManifestObj {
    fn path(&self) -> &GQLPath;

    #[graphql(name = "id")]
    fn nodeid(&self) -> &GQLNodeId;
}

pub enum GQLManifestObj {
    File(GQLFile),
    Symlink(GQLSymlink),
}

impl GQLManifestObj {
    pub fn new(path: &GQLPath, details: &Details) -> Self {
        if details.is_file() {
            GQLManifestObj::File(GQLFile::new(path, details.nodeid()))
        } else if details.is_symlink() {
            GQLManifestObj::Symlink(GQLSymlink::new(path, details.nodeid()))
        } else {
            // treenodes, something else?
            unimplemented!()
        }
    }

    pub fn into_value(self) -> ManifestObjValue {
        match self {
            GQLManifestObj::File(file) => file.into(),
            GQLManifestObj::Symlink(symlink) => symlink.into(),
        }
    }
}

impl From<GQLFile> for GQLManifestObj {
    fn from(file: GQLFile) -> Self {
        GQLManifestObj::File(file)
    }
}

impl From<GQLSymlink> for GQLManifestObj {
    fn from(symlink: GQLSymlink) -> Self {
        GQLManifestObj::Symlink(symlink)
    }
}

impl ManifestObj for GQLManifestObj {
    fn path(&self) -> &GQLPath {
        match self {
            GQLManifestObj::File(file) => file.path(),
            GQLManifestObj::Symlink(symlink) => symlink.path(),
        }
    }

    fn nodeid(&self) -> &GQLNodeId {
        match self {
            GQLManifestObj::File(file) => file.nodeid(),
            GQLManifestObj::Symlink(symlink) => symlink.nodeid(),
        }
    }
}
