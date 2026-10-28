// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Mercurial protocol service framework
//!
//! To implement a Mercurial service, implement `HgCommands` and then use it to handle incoming
//! connections.
use std::collections::{HashMap, HashSet};

use bytes::{Bytes, BytesMut};
use futures::future::{self, FutureExt};
use futures::TryStreamExt;
use slog::{debug, info, o, Logger};

use futures_ext::futures_ordered;
use mercurial_types::NodeHash;

use crate::errors::Error;
use crate::sshproto;
use crate::{BranchRes, BundleStream, GetbundleArgs, Request, Response};

pub struct HgService<H> {
    commands: H,
    logger: Option<Logger>,
}

impl<H: HgCommands> HgService<H> {
    pub fn new(hgcmds: H) -> Self {
        HgService {
            commands: hgcmds,
            logger: None,
        }
    }

    pub fn new_with_logger(hgcmds: H, logger: &Logger) -> Self {
        HgService {
            commands: hgcmds,
            logger: Some(logger.new(o!())),
        }
    }

    pub fn call(&self, req: Request) -> HgCommandRes<Response> {
        self.command(req)
    }

    pub fn command(&self, req: Request) -> HgCommandRes<Response> {
        if let Some(logger) = &self.logger {
            if !matches!(req, Request::Batch { .. }) {
                debug!(logger, "Got request: {:?}", req);
            }
        }
        let hgcmds = &self.commands;

        match req {
            Request::Batch { cmds } => map_cmd(self.batch(cmds), Response::Batch),
            Request::Between { pairs } => map_cmd(hgcmds.between(pairs), Response::Between),
            Request::Branchmap => map_cmd(hgcmds.branchmap(), Response::Branchmap),
            Request::Branches { nodes } => map_cmd(hgcmds.branches(nodes), Response::Branches),
            Request::Clonebundles => map_cmd(hgcmds.clonebundles(), Response::Clonebundles),
            Request::Capabilities => map_cmd(hgcmds.capabilities(), Response::Capabilities),
            Request::Changegroup { roots } => {
                map_cmd(hgcmds.changegroup(roots), |_| Response::Changegroup)
            }
            Request::Changegroupsubset { bases, heads } => {
                map_cmd(hgcmds.changegroupsubset(bases, heads), |_| {
                    Response::Changegroupsubset
                })
            }
            Request::Debugwireargs { one, two, all_args } => map_cmd(
                self.debugwireargs(one, two, all_args),
                Response::Debugwireargs,
            ),
            Request::Getbundle(args) => map_cmd(hgcmds.getbundle(args), Response::Getbundle),
            Request::Heads => map_cmd(hgcmds.heads(), Response::Heads),
            Request::Hello => map_cmd(hgcmds.hello(), Response::Hello),
            Request::Listkeys { namespace } => {
                map_cmd(hgcmds.listkeys(namespace), Response::Listkeys)
            }
            Request::Lookup { key } => map_cmd(hgcmds.lookup(key), Response::Lookup),
            Request::Known { nodes } => map_cmd(hgcmds.known(nodes), Response::Known),
            Request::Pushkey {
                namespace,
                key,
                old,
                new,
            } => map_cmd(hgcmds.pushkey(namespace, key, old, new), Response::Pushkey),
            Request::Streamout => map_cmd(hgcmds.stream_out(), |_| Response::Streamout),
            Request::Unbundle { heads, bundle } => {
                map_cmd(hgcmds.unbundle(heads, bundle), Response::Unbundle)
            }
        }
    }

    // @wireprotocommand('batch', 'cmds *'), but the '*' is ignored.
    // This is handled here because it needs to spin off additional commands.
    fn batch(&self, cmds: Vec<(Vec<u8>, Vec<u8>)>) -> HgCommandRes<Vec<Bytes>> {
        let mut parsed_cmds = Vec::with_capacity(cmds.len());
        for cmd in cmds {
            // XXX This is somewhat ugly -- we're using the SSH protocol's rules
            // to handle this even though this is actually somewhat
            // protocol-agnostic.
            //
            // Ideally, the parser in sshproto/request.rs would be split up into
            // separate ssh-specific and general wireproto command parsers, but
            // we want to rewrite it soon anyway so it's not really worth doing
            // at the moment.
            let mut full_cmd = BytesMut::with_capacity(cmd.0.len() + 1 + cmd.1.len());
            full_cmd.extend_from_slice(&cmd.0);
            full_cmd.extend_from_slice(b"\n");
            full_cmd.extend_from_slice(&cmd.1);
            let parsed = match sshproto::request::parse_batch(&mut full_cmd) {
                // TODO: collect all parsing errors, not just the first one?
                Err(err) => return box_ready(Err(err)),
                Ok(None) => {
                    return box_ready(Err(Error::BatchInvalid(
                        String::from_utf8_lossy(&full_cmd).into_owned(),
                    )));
                }
                // Mercurial does not batch getbundle, and its stream has no batch encoding.
                Ok(Some(Request::Getbundle(_))) => {
                    return box_ready(Err(Error::BatchInvalid("getbundle".into())));
                }
                Ok(Some(cmd)) => cmd,
            };
            if let Some(logger) = &self.logger {
                info!(logger, "batch command: {:?}", parsed);
            }
            parsed_cmds.push(parsed);
        }

        // Spin up all the individual commands. We must force evaluation of the
        // iterator because otherwise the closure will have captured self for
        // too long.
        let response_futures: Vec<_> = parsed_cmds
            .into_iter()
            .map(|cmd| self.command(cmd))
            .collect();

        let encoded_futures = response_futures.into_iter().map(|cmd| {
            cmd.map(|res| res.map(|response| sshproto::response::encode_cmd(&response)))
        });
        futures_ordered(encoded_futures)
            .try_collect::<Vec<Bytes>>()
            .boxed()
    }

    // @wireprotocommand('debugwireargs', 'one two *')
    // Handled here because this is a meta debugging command that isn't
    // specific to a repo.
    fn debugwireargs(
        &self,
        one: Vec<u8>,
        two: Vec<u8>,
        all_args: HashMap<Vec<u8>, Vec<u8>>,
    ) -> HgCommandRes<Bytes> {
        let mut out = Vec::<u8>::new();
        out.extend_from_slice(&one[..]);
        out.push(b' ');
        out.extend_from_slice(&two[..]);
        out.push(b' ');
        out.extend_from_slice(get_or_none(&all_args, b"three"));
        out.push(b' ');
        out.extend_from_slice(get_or_none(&all_args, b"four"));
        out.push(b' ');
        // Note that "five" isn't actually read off the wire -- instead, the
        // default value "None" is used.
        out.extend_from_slice(NONE);

        box_ready(Ok(out.into()))
    }
}

const NONE: &[u8] = b"None";

#[inline]
fn get_or_none<'a>(map: &'a HashMap<Vec<u8>, Vec<u8>>, key: &'a [u8]) -> &'a [u8] {
    match map.get(key) {
        Some(val) => val,
        None => NONE,
    }
}

fn map_cmd<T, F>(fut: HgCommandRes<T>, f: F) -> HgCommandRes<Response>
where
    T: Send + 'static,
    F: FnOnce(T) -> Response + Send + 'static,
{
    fut.map(move |res| res.map(f)).boxed()
}

fn box_ready<T: Send + 'static>(res: Result<T, Error>) -> HgCommandRes<T> {
    future::ready(res).boxed()
}

#[inline]
fn unimplemented<S, T>(op: S) -> HgCommandRes<T>
where
    S: Into<String>,
    T: Send + 'static,
{
    box_ready(Err(Error::Unimplemented(op.into())))
}

// Async response from an Hg command.
pub type HgCommandRes<T> = futures::future::BoxFuture<'static, Result<T, Error>>;

// Trait representing Mercurial protocol operations, generic across protocols
// Derived from hg/mercurial/wireprotocol.py, functions with the `@wireprotocommand`
// decorator.
//
// XXX Do we need to do all of these? Are some historical/obsolete and can be ignored?
//
// TODO: placeholder types are generally `()`
pub trait HgCommands {
    // @wireprotocommand('between', 'pairs')
    fn between(&self, _pairs: Vec<(NodeHash, NodeHash)>) -> HgCommandRes<Vec<Vec<NodeHash>>> {
        unimplemented("between")
    }

    // @wireprotocommand('branchmap')
    fn branchmap(&self) -> HgCommandRes<HashMap<String, HashSet<NodeHash>>> {
        unimplemented("branchmap")
    }

    // @wireprotocommand('branches', 'nodes')
    fn branches(&self, _nodes: Vec<NodeHash>) -> HgCommandRes<Vec<BranchRes>> {
        unimplemented("branches")
    }

    // @wireprotocommand('clonebundles', '')
    fn clonebundles(&self) -> HgCommandRes<String> {
        unimplemented("clonebundles")
    }

    // @wireprotocommand('capabilities')
    fn capabilities(&self) -> HgCommandRes<Vec<String>> {
        unimplemented("capabilities")
    }

    // @wireprotocommand('changegroup', 'roots')
    fn changegroup(&self, _roots: Vec<NodeHash>) -> HgCommandRes<()> {
        // TODO: streaming something
        unimplemented("changegroup")
    }

    // @wireprotocommand('changegroupsubset', 'bases heads')
    fn changegroupsubset(&self, _bases: Vec<NodeHash>, _heads: Vec<NodeHash>) -> HgCommandRes<()> {
        unimplemented("changegroupsubset")
    }

    // @wireprotocommand('getbundle', '*')
    fn getbundle(&self, _args: GetbundleArgs) -> HgCommandRes<BundleStream> {
        unimplemented("getbundle")
    }

    // @wireprotocommand('heads')
    fn heads(&self) -> HgCommandRes<HashSet<NodeHash>> {
        unimplemented("heads")
    }

    // @wireprotocommand('hello')
    fn hello(&self) -> HgCommandRes<HashMap<String, Vec<String>>> {
        unimplemented("hello")
    }

    // @wireprotocommand('listkeys', 'namespace')
    fn listkeys(&self, _namespace: String) -> HgCommandRes<HashMap<Vec<u8>, Vec<u8>>> {
        unimplemented("listkeys")
    }

    // @wireprotocommand('lookup', 'key')
    fn lookup(&self, _key: String) -> HgCommandRes<NodeHash> {
        unimplemented("lookup")
    }

    // @wireprotocommand('known', 'nodes *')
    fn known(&self, _nodes: Vec<NodeHash>) -> HgCommandRes<Vec<bool>> {
        unimplemented("known")
    }

    // @wireprotocommand('pushkey', 'namespace key old new')
    fn pushkey(
        &self,
        _namespace: String,
        _key: String,
        _old: NodeHash,
        _new: NodeHash,
    ) -> HgCommandRes<bool> {
        unimplemented("pushkey")
    }

    // @wireprotocommand('stream_out')
    fn stream_out(
        &self,
    ) -> HgCommandRes<futures::stream::BoxStream<'static, std::result::Result<Vec<u8>, Error>>>
    {
        // XXX raw streaming?
        unimplemented("stream_out")
    }

    // @wireprotocommand('unbundle', 'heads')
    fn unbundle(&self, _heads: Vec<NodeHash>, _bundle: Bytes) -> HgCommandRes<Bytes> {
        unimplemented("unbundle")
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use futures::executor::block_on;
    use futures::future::FutureExt;

    use super::*;
    use crate::{Request, Response};

    struct Dummy;
    impl HgCommands for Dummy {
        fn hello(&self) -> HgCommandRes<HashMap<String, Vec<String>>> {
            let mut res = HashMap::new();
            res.insert("capabilities".into(), vec!["something".into()]);

            future::ready(Ok(res)).boxed()
        }
    }

    #[test]
    fn hello() {
        let service = HgService::new(Dummy);

        let r = block_on(service.call(Request::Hello));
        println!("hello r = {:?}", r);
        let mut res: HashMap<String, Vec<String>> = HashMap::new();
        res.insert("capabilities".into(), vec!["something".into()]);

        match r {
            Ok(Response::Hello(ref r)) if r == &res => (),
            bad => panic!("Bad result {:?}", bad),
        }
    }

    #[test]
    fn batch_rejects_getbundle() {
        let service = HgService::new(Dummy);

        let r = block_on(service.call(Request::Batch {
            cmds: vec![(b"getbundle".to_vec(), b"heads=".to_vec())],
        }));

        match r {
            Err(Error::BatchInvalid(cmd)) => assert_eq!(cmd, "getbundle"),
            bad => panic!("Bad result {:?}", bad),
        }
    }

    #[test]
    fn unimpl() {
        let service = HgService::new(Dummy);

        let r = block_on(service.call(Request::Heads));
        println!("heads r = {:?}", r);

        match r {
            Err(err) => {
                let msg = err.to_string();
                assert!(
                    msg.contains("heads"),
                    "error display should name the command, got {}",
                    msg
                );
            }
            bad => panic!("Bad result {:?}", bad),
        }
    }
}
