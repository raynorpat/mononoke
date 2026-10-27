// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use clap::{Arg, ArgAction, Command};

mod errors;
mod serve;

fn main() {
    let matches = Command::new("Mononoke CLI")
        .about("Provide minimally compatible CLI to Mononoke server")
        .arg(
            Arg::new("repository")
                .short('R')
                .long("repository")
                .value_name("REPO")
                .help("repository name"),
        )
        .subcommand(
            Command::new("serve")
                .about("start server")
                .arg(
                    Arg::new("accesslog")
                        .short('A')
                        .long("accesslog")
                        .value_name("FILE")
                        .num_args(0..=1)
                        .help("name of access log file"),
                )
                .arg(
                    Arg::new("daemon")
                        .short('d')
                        .long("daemon")
                        .action(ArgAction::SetTrue)
                        .help("run server in background"),
                )
                .arg(
                    Arg::new("errorlog")
                        .short('E')
                        .long("errorlog")
                        .value_name("FILE")
                        .num_args(0..=1)
                        .help("name of error log file to write to"),
                )
                .arg(
                    Arg::new("stdio")
                        .long("stdio")
                        .action(ArgAction::SetTrue)
                        .help("for remote clients"),
                )
                .arg(
                    Arg::new("cmdserver")
                        .long("cmdserver")
                        .value_name("MODE")
                        .value_parser(["pipe", "unix"])
                        .help("for remote clients"),
                ),
        )
        .get_matches();

    let res = if let Some(subcmd) = matches.subcommand_matches("serve") {
        serve::cmd(&matches, subcmd)
    } else {
        Err("unexpected or missing subcommand".into())
    };

    if let Err(err) = res {
        println!("Subcommand failed: {:?}", err);
    }
}
