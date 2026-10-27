// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::fmt::{self, Display};

use futures::future::BoxFuture;
use futures::stream::BoxStream;

use crate::blob::Blob;
use crate::blobnode::Parents;
use crate::nodehash::NodeHash;
use crate::path::Path;

/// Interface for a manifest
pub trait Manifest: Send + 'static {
    type Error: Send + 'static;

    fn lookup(
        &self,
        path: &Path,
    ) -> BoxFuture<'static, Result<Option<Box<dyn Entry<Error = Self::Error>>>, Self::Error>>;
    fn list(&self) -> BoxStream<'static, Result<Box<dyn Entry<Error = Self::Error>>, Self::Error>>;

    fn boxed(self) -> Box<dyn Manifest<Error = Self::Error>>
    where
        Self: Sized,
    {
        Box::new(self)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum Type {
    File,
    Symlink,
    Tree,
    Executable,
}

pub enum Content<E> {
    File(Blob<Vec<u8>>),
    Executable(Blob<Vec<u8>>),
    Symlink(Path),
    Tree(Box<dyn Manifest<Error = E>>),
}

pub trait Entry: Send + 'static {
    type Error: Send + 'static;

    fn get_type(&self) -> Type;
    fn get_parents(&self) -> BoxFuture<'static, Result<Parents, Self::Error>>;
    fn get_content(&self) -> BoxFuture<'static, Result<Content<Self::Error>, Self::Error>>;
    fn get_hash(&self) -> &NodeHash;
    fn get_path(&self) -> &Path;

    fn boxed(self) -> Box<dyn Entry<Error = Self::Error>>
    where
        Self: Sized,
    {
        Box::new(self)
    }
}

impl<E> Entry for Box<dyn Entry<Error = E>>
where
    E: Send + 'static,
{
    type Error = E;

    fn get_type(&self) -> Type {
        (**self).get_type()
    }

    fn get_parents(&self) -> BoxFuture<'static, Result<Parents, Self::Error>> {
        (**self).get_parents()
    }

    fn get_content(&self) -> BoxFuture<'static, Result<Content<Self::Error>, Self::Error>> {
        (**self).get_content()
    }

    fn get_hash(&self) -> &NodeHash {
        (**self).get_hash()
    }

    fn get_path(&self) -> &Path {
        (**self).get_path()
    }
}

impl Display for Type {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            Type::Symlink => "l",
            Type::Executable => "x",
            Type::Tree => "t",
            Type::File => "",
        };
        write!(fmt, "{}", s)
    }
}
