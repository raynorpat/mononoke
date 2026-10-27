// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::str::FromStr;

use clap::{Arg, Command};

use mercurial::revlog::{RevIdx, Revlog};
use mercurial::Result;

fn run() -> Result<()> {
    let matches = Command::new("idxdump")
        .version("0.0.0")
        .about("dump index entries")
        .arg(
            Arg::new("IDXFILE")
                .help("index file")
                .required(true)
                .index(1),
        )
        .arg(Arg::new("REV").help("revision index").index(2))
        .get_matches();

    let idxpath = matches.get_one::<String>("IDXFILE").unwrap();
    let revidx: Option<RevIdx> = match matches
        .get_one::<String>("REV")
        .map(|rev| FromStr::from_str(rev))
    {
        Some(Ok(idx)) => Some(idx),
        Some(Err(err)) => return Err(format!("idx malformed: {:?}", err).into()),
        None => None,
    };

    let revlog = match Revlog::from_idx(idxpath) {
        Ok(revlog) => revlog,
        Err(err) => return Err(format!("failed to load idx {}: {:?}", idxpath, err).into()),
    };

    println!("Header: {:?}", revlog.get_header());

    let mut iter = revlog.into_iter();
    if let Some(revidx) = revidx {
        iter.seek(revidx);
    }

    for (idx, entry) in iter {
        println!("{:?}: {:?}", idx, entry)
    }

    Ok(())
}

fn main() {
    if let Err(ref err) = run() {
        println!("Failed: {}", err);

        let mut cause = std::error::Error::source(err);
        while let Some(next) = cause {
            println!("caused by: {}", next);
            cause = std::error::Error::source(next);
        }

        std::process::exit(1);
    }
}
