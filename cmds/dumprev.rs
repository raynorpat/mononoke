// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::fs::File;
use std::io::Write;
use std::str::FromStr;

use clap::{Arg, Command};

use mercurial::revlog::Revlog;
use mercurial::{ChainErr, Error, Result};

fn run() -> Result<()> {
    let matches = Command::new("dumprev")
        .version("0.0.0")
        .about("extract a revision from a revlog")
        .arg(
            Arg::new("data")
                .short('d')
                .long("data")
                .value_name("DATAFILE")
                .help("Data file if not inline"),
        )
        .arg(
            Arg::new("write")
                .short('w')
                .long("write")
                .value_name("DUMPFILE")
                .help("Write data to file"),
        )
        .arg(
            Arg::new("IDXFILE")
                .help("index file")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("REV")
                .help("revision index")
                .required(true)
                .index(2),
        )
        .get_matches();

    let idxpath = matches.get_one::<String>("IDXFILE").unwrap();
    let datapath = matches.get_one::<String>("data");
    let dumpfile = matches.get_one::<String>("write");
    let revidx = FromStr::from_str(matches.get_one::<String>("REV").unwrap())
        .map_err(Error::from)
        .chain_err(|| "idx malformed")?;

    let revlog =
        Revlog::from_idx_data(idxpath, datapath).chain_err(|| "failed to load idx and data")?;
    println!("made revlog {:?}", revlog.get_header());

    let entry = revlog
        .get_entry(revidx)
        .chain_err(|| "failed to get entry")?;

    println!("Revlog[{:?}] = {:?}", revidx, entry);
    match revlog.get_rev(revidx) {
        Ok(ref rev) if rev.nodeid().is_some() => {
            if entry.nodeid() != &rev.nodeid().unwrap() {
                println!(
                    "NOTE: hash mismatch: expected {}, got {}",
                    entry.nodeid(),
                    rev.nodeid().unwrap()
                )
            }
            if let Some(revdata) = rev.as_blob().as_slice() {
                if let Some(dumpfile) = dumpfile {
                    let mut file = match File::create(dumpfile) {
                        Ok(file) => file,
                        Err(err) => {
                            return Err(
                                format!("Failed to create file {}: {:?}", dumpfile, err).into()
                            )
                        }
                    };
                    println!(
                        "Writing rev {:?} to {}",
                        rev.nodeid().expect("no id"),
                        dumpfile
                    );
                    if let Err(err) = file.write_all(revdata) {
                        return Err(format!("Failed to write {}: {:?}", dumpfile, err).into());
                    }
                } else {
                    println!(
                        "rev {:?}:\n{}",
                        rev.nodeid().expect("no id"),
                        String::from_utf8_lossy(revdata)
                    );
                }
            } else {
                println!("Dataless rev {:?}", rev.nodeid().expect("no id"));
            }
        }
        Ok(rev) => {
            return Err(format!(
                "Nodeid missing: got {:?} expected {:?}",
                rev.nodeid(),
                entry.nodeid()
            )
            .into())
        }
        Err(err) => return Err(format!("failed to get chunk {:?}: {}", revidx, err).into()),
    };

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
