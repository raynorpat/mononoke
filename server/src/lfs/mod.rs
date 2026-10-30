// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::sync::Arc;

pub mod http;
pub mod pointer;
pub mod store;

/// How a repository serves LFS.
pub struct LfsServing {
    pub store: Arc<store::LfsStore>,
    pub public_url: Option<String>,
}
