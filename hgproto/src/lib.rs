// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Mercurial command protocol
//!
//! Mercurial has a set of commands which are implemented across at least two protocols:
//! SSH and HTTP. This module defines enums representing requests and responses for those
//! protocols, and a service framework for them via `HgCommands`.

#![deny(warnings)]

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};

use bytes::Bytes;

use mercurial_types::NodeHash;

mod batch;
mod errors;
mod service;
pub mod sshproto;

pub use crate::errors::{Error, ErrorKind, Result, ResultExt};
pub use crate::service::{HgCommandRes, HgCommands, HgService};

/// `getbundle`, `changegroup` or `changegroupsubset` bytes, produced while they are sent.
pub struct BundleStream {
    pub body: futures::stream::BoxStream<'static, Result<Bytes>>,
    /// Stream clone data barely compresses, so HTTP sends it uncompressed
    /// when the client allows, as Mercurial's server does.
    pub prefer_uncompressed: bool,
}

impl Debug for BundleStream {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str("BundleStream")
    }
}

// result from `branches()`
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct BranchRes {
    top: NodeHash,
    node: NodeHash,
    p0: Option<NodeHash>,
    p1: Option<NodeHash>,
}

impl BranchRes {
    pub fn new(top: NodeHash, node: NodeHash, p0: Option<NodeHash>, p1: Option<NodeHash>) -> Self {
        Self { top, node, p0, p1 }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Request {
    Batch {
        cmds: Vec<(Vec<u8>, Vec<u8>)>,
    },
    Between {
        pairs: Vec<(NodeHash, NodeHash)>,
    },
    Branchmap,
    Branches {
        nodes: Vec<NodeHash>,
    },
    Clonebundles,
    Capabilities,
    Changegroup {
        roots: Vec<NodeHash>,
    },
    Changegroupsubset {
        bases: Vec<NodeHash>,
        heads: Vec<NodeHash>,
    },
    Debugwireargs {
        one: Vec<u8>,
        two: Vec<u8>,
        all_args: HashMap<Vec<u8>, Vec<u8>>,
    },
    Getbundle(GetbundleArgs),
    Heads,
    Hello,
    Listkeys {
        namespace: String,
    },
    Lookup {
        key: Vec<u8>,
    },
    Known {
        nodes: Vec<NodeHash>,
    },
    Pushkey {
        namespace: String,
        key: String,
        old: NodeHash,
        new: NodeHash,
    },
    Streamout,
    Unbundle {
        heads: Vec<NodeHash>,
        bundle: Bytes,
    },
}

/// The arguments that `getbundle` accepts, in a separate struct for
/// the convenience of callers.
#[derive(Eq, PartialEq)]
pub struct GetbundleArgs {
    pub heads: Vec<NodeHash>,
    pub common: Vec<NodeHash>,
    pub bundlecaps: Vec<Vec<u8>>,
    pub listkeys: Vec<Vec<u8>>,
    /// The client asks for a stream clone instead of a changegroup.
    pub stream: bool,
    /// The client wants a changegroup. Mercurial sends `cg=0` with `stream=1`.
    pub cg: bool,
}

impl Debug for GetbundleArgs {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let bcaps: Vec<_> = self
            .bundlecaps
            .iter()
            .map(|s| String::from_utf8_lossy(s))
            .collect();
        let listkeys: Vec<_> = self
            .listkeys
            .iter()
            .map(|s| String::from_utf8_lossy(s))
            .collect();
        fmt.debug_struct("GetbundleArgs")
            .field("heads", &self.heads)
            .field("common", &self.common)
            .field("bundlecaps", &bcaps)
            .field("listkeys", &listkeys)
            .field("stream", &self.stream)
            .field("cg", &self.cg)
            .finish()
    }
}

#[derive(Debug)]
pub enum Response {
    Batch(Vec<Bytes>),
    Between(Vec<Vec<NodeHash>>),
    Branchmap(HashMap<Vec<u8>, HashSet<NodeHash>>),
    Branches(Vec<BranchRes>),
    Clonebundles(String),
    Capabilities(Vec<String>),
    Changegroup(BundleStream),
    Changegroupsubset(BundleStream),
    Debugwireargs(Bytes),
    Getbundle(BundleStream),
    Heads(HashSet<NodeHash>),
    Hello(HashMap<String, Vec<String>>),
    Listkeys(HashMap<Vec<u8>, Vec<u8>>),
    /// The node, or why `key` did not resolve. Mercurial answers both on the wire.
    Lookup(std::result::Result<NodeHash, String>),
    Known(Vec<bool>),
    Pushkey(bool),
    Pushres {
        ret: i64,
        output: Bytes,
    },
    Pusherr(Bytes),
    Streamout,
    Unbundle(Bytes),
}

impl Response {
    /// Whether this represents a streaming response. Streaming responses don't have any framing.
    pub fn is_stream(&self) -> bool {
        match self {
            Response::Getbundle(_)
            | Response::Changegroup(_)
            | Response::Changegroupsubset(_)
            | Response::Unbundle(_) => true,
            _ => false,
        }
    }
}
