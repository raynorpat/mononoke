// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Msg(String),
    #[error("{0}")]
    Bundle2Decode(String),
    #[error("{0}")]
    Revlog(String),
    #[error("{0}")]
    Repo(String),
    #[error("Unknown requirement \"{0}\"")]
    UnknownReq(String),
    #[error("{context}")]
    WithChain {
        context: String,
        #[source]
        cause: Box<Error>,
    },
    #[error(transparent)]
    MercurialTypes(#[from] mercurial_types::Error),
    #[error(transparent)]
    StockBookmarks(#[from] stockbookmarks::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Utf8(#[from] std::str::Utf8Error),
    #[error(transparent)]
    Utf8String(#[from] std::string::FromUtf8Error),
    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),
}

pub type ErrorKind = Error;

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn with_chain<E, C>(error: E, context: C) -> Self
    where
        E: Into<Error>,
        C: std::fmt::Display,
    {
        Error::WithChain {
            context: context.to_string(),
            cause: Box::new(error.into()),
        }
    }
}

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

impl From<Error> for String {
    fn from(err: Error) -> String {
        format!("{}", err)
    }
}

impl From<Error> for io::Error {
    fn from(err: Error) -> io::Error {
        io::Error::new(io::ErrorKind::Other, err)
    }
}

pub trait ChainErr<T> {
    fn chain_err<F, D>(self, op: F) -> Result<T>
    where
        F: FnOnce() -> D,
        D: Into<Error>;
}

impl<T, E> ChainErr<T> for std::result::Result<T, E>
where
    E: Into<Error>,
{
    fn chain_err<F, D>(self, op: F) -> Result<T>
    where
        F: FnOnce() -> D,
        D: Into<Error>,
    {
        self.map_err(|err| {
            let context = op().into().to_string();
            Error::with_chain(err, context)
        })
    }
}
