// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::sync::{Arc, Mutex, MutexGuard};

use futures::TryStreamExt;
use juniper::graphql_object;

use mercurial::RevlogRepo;

use crate::changeset::GQLChangeset;
use crate::node::GQLNodeId;
use crate::ready;

#[derive(Debug)]
pub struct GQLRepo;

impl GQLRepo {
    pub fn new() -> GQLRepo {
        GQLRepo
    }
}

#[derive(Clone)]
pub struct RepoCtx {
    repo: Arc<Mutex<RevlogRepo>>,
}

impl RepoCtx {
    pub fn new(repo: RevlogRepo) -> Self {
        RepoCtx {
            repo: Arc::new(Mutex::new(repo)),
        }
    }

    pub fn repo(&self) -> MutexGuard<'_, RevlogRepo> {
        self.repo.lock().expect("lock failed")
    }
}

impl juniper::Context for RepoCtx {}

/// A source control repository
#[graphql_object(name = "Repo", context = RepoCtx)]
impl GQLRepo {
    fn required(ctx: &RepoCtx) -> Vec<String> {
        let repo = ctx.repo();
        repo.get_requirements()
            .iter()
            .map(|req| format!("{}", req))
            .collect()
    }

    /// Set of head revisions in the repo
    fn heads(ctx: &RepoCtx) -> Result<Vec<GQLChangeset>, String> {
        let repo = ctx.repo();
        let set = ready(repo.get_heads().try_collect::<Vec<_>>()).map_err(|err| err.to_string())?;
        Ok(set.into_iter().map(GQLChangeset::from).collect())
    }

    /// Fetch a specific changeset
    fn changeset(ctx: &RepoCtx, id: GQLNodeId) -> Result<GQLChangeset, String> {
        let repo = ctx.repo();
        // Check the id exists, but we don't need the result now
        match ready(repo.changeset_exists(&id)).map_err(|err| err.to_string())? {
            true => Ok(GQLChangeset::from(id)),
            false => Err(format!("changeset {} does not exist", *id)),
        }
    }

    /// Fetch all changesets
    fn changesets(ctx: &RepoCtx) -> Vec<GQLChangeset> {
        let repo = ctx.repo();
        ready(repo.changesets().try_collect::<Vec<_>>())
            .unwrap()
            .into_iter()
            .map(GQLChangeset::from)
            .collect()
    }
}
