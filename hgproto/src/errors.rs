// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::io;

use thiserror::Error;

#[macro_export]
macro_rules! bail {
    ($e:expr) => {
        return Err($crate::Error::from($e))
    };
    ($fmt:expr, $($arg:tt)+) => {
        return Err($crate::Error::from(format!($fmt, $($arg)+)))
    };
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Msg(String),
    #[error("unimplemented operation '{0}'")]
    Unimplemented(String),
    #[error("command parse failed for \"{0}\"")]
    CommandParse(String),
    #[error("malformed batch command '{0}'")]
    BatchInvalid(String),
    #[error("unknown escape character in batch command '{0}'")]
    BatchEscape(u8),
    #[error("{context}")]
    WithChain {
        context: Box<Error>,
        #[source]
        cause: Box<Error>,
    },
    #[error(transparent)]
    Mercurial(#[from] mercurial::Error),
    #[error(transparent)]
    MercurialBundles(#[from] mercurial_bundles::Error),
    #[error(transparent)]
    Fmt(#[from] std::fmt::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Utf8(#[from] std::str::Utf8Error),
}

pub type ErrorKind = Error;

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn with_chain<E, C>(error: E, context: C) -> Self
    where
        E: Into<Error>,
        C: Into<Error>,
    {
        Error::WithChain {
            context: Box::new(context.into()),
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

impl From<Error> for io::Error {
    fn from(err: Error) -> io::Error {
        io::Error::new(io::ErrorKind::Other, format!("Error: {}", err))
    }
}

pub trait ResultExt<T> {
    fn chain_err<F, D>(self, op: F) -> Result<T>
    where
        F: FnOnce() -> D,
        D: Into<Error>;
}

impl<T, E> ResultExt<T> for std::result::Result<T, E>
where
    E: Into<Error>,
{
    fn chain_err<F, D>(self, op: F) -> Result<T>
    where
        F: FnOnce() -> D,
        D: Into<Error>,
    {
        self.map_err(|err| Error::with_chain(err, op()))
    }
}
