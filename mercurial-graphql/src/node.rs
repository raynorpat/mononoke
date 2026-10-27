// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::ops::Deref;

use juniper::GraphQLScalar;

use mercurial_types::NodeHash;

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq, GraphQLScalar)]
#[graphql(
    name = "NodeId",
    description = "Unique ID of a particular version of a file.",
    to_output_with = Self::to_output,
    from_input_with = Self::from_input,
    parse_token(String)
)]
pub struct GQLNodeId(NodeHash);

impl GQLNodeId {
    fn to_output(&self) -> String {
        self.0.to_string()
    }

    fn from_input(input: &str) -> Result<Self, String> {
        input.parse().map(GQLNodeId).map_err(|err| err.to_string())
    }
}

impl From<NodeHash> for GQLNodeId {
    fn from(nodehash: NodeHash) -> Self {
        GQLNodeId(nodehash)
    }
}

impl<'a> From<&'a NodeHash> for GQLNodeId {
    fn from(nodehash: &'a NodeHash) -> Self {
        GQLNodeId(*nodehash)
    }
}

impl Deref for GQLNodeId {
    type Target = NodeHash;

    fn deref(&self) -> &NodeHash {
        &self.0
    }
}
