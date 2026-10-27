// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::io;

use ascii::AsciiString;
use thiserror::Error;

use crate::part_header::PartHeader;

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
    #[error("{0}")]
    Bundle2Decode(String),
    #[error("changegroup2 decode error: {0}")]
    Cg2Decode(String),
    #[error("{0}")]
    Bundle2Encode(String),
    #[error("bundle2 chunk error: {0}")]
    Bundle2Chunk(String),
    #[error("unknown part type: {}", .0.part_type())]
    BundleUnknownPart(PartHeader),
    #[error("unknown params for bundle2 part '{}': {}", .0, .1.join(", "))]
    BundleUnknownPartParams(AsciiString, Vec<String>),
    #[error("error while generating listkey part")]
    ListkeyGeneration,
    #[error("{context}")]
    WithChain {
        context: Box<Error>,
        #[source]
        cause: Box<Error>,
    },
    #[error(transparent)]
    MercurialTypes(#[from] mercurial_types::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Utf8(#[from] std::str::Utf8Error),
}

pub type ErrorKind = Error;

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn kind(&self) -> &Self {
        match self {
            Error::WithChain { context, .. } => context.kind(),
            _ => self,
        }
    }

    pub fn is_app_error(&self) -> bool {
        match self.kind() {
            Error::BundleUnknownPart(_) | Error::BundleUnknownPartParams(_, _) => true,
            _ => false,
        }
    }

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
        io::Error::new(io::ErrorKind::InvalidData, err)
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
        self.map_err(|err| Error::with_chain(err, op()))
    }
}
