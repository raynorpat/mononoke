// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use thiserror::Error;

#[macro_export]
macro_rules! bail {
    ($e:expr) => {
        return Err($crate::errors::Error::from($e))
    };
    ($fmt:expr, $($arg:tt)+) => {
        return Err($crate::errors::Error::from(format!($fmt, $($arg)+)))
    };
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Msg(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("send failed")]
    Send,
}

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

impl From<futures::channel::mpsc::SendError> for Error {
    fn from(_err: futures::channel::mpsc::SendError) -> Self {
        Error::Send
    }
}
