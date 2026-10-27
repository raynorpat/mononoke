// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::path::{Path, PathBuf};

use clap::ArgMatches;
use futures::{SinkExt, StreamExt, TryFutureExt};
use futures_ext::StreamExt as _;
use sshrelay::{SshDecoder, SshEncoder, SshMsg, SshStream};
use tokio_util::codec::{FramedRead, FramedWrite};

#[cfg(windows)]
use mononoke::uds::UnixStream;
#[cfg(unix)]
use tokio::net::UnixStream;

use crate::bail;
use crate::errors::{Error, Result};

mod fdio;

pub fn cmd(main: &ArgMatches, sub: &ArgMatches) -> Result<()> {
    if sub.get_flag("stdio") {
        if let Some(repo) = main.get_one::<String>("repository") {
            let mut path = PathBuf::from(repo);
            path.push(".hg");
            path.push("mononoke.sock");
            return ssh_relay(path);
        }
        return Err("Missing repository".into());
    }
    Err("Only stdio server is supported".into())
}

fn ssh_relay<P: AsRef<Path>>(path: P) -> Result<()> {
    let path = path.as_ref().to_path_buf();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()?;
    rt.block_on(relay(path))
}

async fn relay(path: PathBuf) -> Result<()> {
    let stdin = fdio::stdin();
    let stdout = fdio::stdout();
    let stderr = fdio::stderr();

    let socket = UnixStream::connect(&path).await?;
    let (socket_read, socket_write) = tokio::io::split(socket);
    let rx = FramedRead::new(socket_read, SshDecoder::new());
    let tx = FramedWrite::new(socket_write, SshEncoder::new());

    let stdin_future = stdin
        .map(|chunk| chunk.map(|buf| SshMsg::new(SshStream::Stdin, buf)))
        .forward(tx)
        .map_err(Error::from)
        .map_ok(|_| ());

    let stdout_future = rx
        .streamfork(
            stdout.with(|msg: SshMsg| futures::future::ready(Ok::<_, Error>(msg.data()))),
            stderr.with(|msg: SshMsg| futures::future::ready(Ok::<_, Error>(msg.data()))),
            |msg| -> Result<bool> {
                match msg.stream() {
                    SshStream::Stdout => Ok(false),
                    SshStream::Stderr => Ok(true),
                    bad => bail!("Bad stream: {:?}", bad),
                }
            },
        )
        .map_ok(|_| ());

    match futures::future::select(stdout_future, stdin_future).await {
        futures::future::Either::Left((Ok(_), _)) | futures::future::Either::Right((Ok(_), _)) => {
            Ok(())
        }
        futures::future::Either::Left((Err(err), _)) => Err(err),
        futures::future::Either::Right((Err(err), _)) => Err(err),
    }
}
