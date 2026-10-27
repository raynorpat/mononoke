// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid sha-1 input: {0}")]
    InvalidSha1Input(String),
    #[error("invalid path: {0}")]
    InvalidPath(String),
    #[error("invalid fragment list: {0}")]
    InvalidFragmentList(String),
    #[error("invalid fragment: start {start} > end {end}")]
    InvalidFragment { start: usize, end: usize },
    #[error(transparent)]
    Utf8(#[from] std::str::Utf8Error),
}

pub type ErrorKind = Error;

pub type Result<T> = std::result::Result<T, Error>;
