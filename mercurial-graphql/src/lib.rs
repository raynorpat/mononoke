#![deny(warnings)]
// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

pub mod changeset;
pub mod file;
pub mod manifest;
pub mod manifestobj;
pub mod node;
pub mod repo;
pub mod symlink;

pub(crate) fn ready<T>(fut: impl std::future::Future<Output = T>) -> T {
    use futures::future::FutureExt;

    fut.now_or_never()
        .expect("revlog future is immediately ready")
}

pub(crate) fn graphql_int(
    value: impl Copy + std::fmt::Display + TryInto<i32>,
) -> Result<i32, String> {
    value
        .try_into()
        .map_err(|_| format!("value {value} does not fit in GraphQL Int"))
}
