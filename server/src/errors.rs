// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::fmt;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Msg(String),
    #[error("{context}")]
    WithChain {
        context: String,
        #[source]
        cause: Box<Error>,
    },
    #[error(transparent)]
    Mercurial(#[from] mercurial::Error),
    #[error(transparent)]
    HgProto(#[from] hgproto::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type ErrorKind = Error;

pub type Result<T> = std::result::Result<T, Error>;

impl From<&str> for Error {
    fn from(msg: &str) -> Self {
        Error::Msg(msg.to_owned())
    }
}

impl From<String> for Error {
    fn from(msg: String) -> Self {
        Error::Msg(msg)
    }
}

pub trait ResultExt<T> {
    fn chain_err<F, D>(self, op: F) -> Result<T>
    where
        F: FnOnce() -> D,
        D: fmt::Display;
}

impl<T, E> ResultExt<T> for std::result::Result<T, E>
where
    E: Into<Error>,
{
    fn chain_err<F, D>(self, op: F) -> Result<T>
    where
        F: FnOnce() -> D,
        D: fmt::Display,
    {
        self.map_err(|err| Error::WithChain {
            context: op().to_string(),
            cause: Box::new(err.into()),
        })
    }
}
