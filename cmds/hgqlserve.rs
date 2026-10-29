// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

#[cfg(fbcode_build)]
extern crate hgqlserve_build_info;

use std::collections::HashMap;
use std::io::Cursor;
use std::net::ToSocketAddrs;
use std::path::{Component, PathBuf};

use clap::{Arg, ArgAction, Command};
use juniper::http::GraphQLRequest;
use juniper::{EmptyMutation, EmptySubscription, RootNode};
use mercurial::Result;
use mercurial::RevlogRepo;
use mercurial_graphql::repo::{GQLRepo, RepoCtx};
use mercurial_types::percent_decode;
use tiny_http::{Header, Method, Request, Response, Server, StatusCode};

#[allow(unused)]
#[cfg(fbcode_build)]
use hgqlserve_build_info::BUILDINFO;

type Schema = RootNode<GQLRepo, EmptyMutation<RepoCtx>, EmptySubscription<RepoCtx>>;

struct Mount {
    ctx: RepoCtx,
    schema: Schema,
}

fn find_repos<I, S>(repos: I) -> HashMap<String, RevlogRepo>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut repomap = HashMap::new();

    for path in repos.into_iter() {
        let path = path.as_ref();
        let mut path = PathBuf::from(path);

        let name = match path.components().rev().take(1).next() {
            Some(Component::Normal(ref name)) => name.to_string_lossy().into_owned(),
            None | Some(_) => {
                println!("path {:?} has no name?", path);
                continue;
            }
        };

        path.push(".hg");

        if !path.exists() || !path.is_dir() {
            println!("path {:?} doesn't exist or isn't a dir", path);
            continue;
        }

        let repo = match RevlogRepo::open(&path) {
            Ok(repo) => repo,
            Err(err) => {
                println!("Failed to open repo {:?}: {}", path, err);
                continue;
            }
        };

        repomap.insert(name, repo);
    }

    repomap
}

fn init_mounts(repomap: HashMap<String, RevlogRepo>) -> HashMap<String, Mount> {
    repomap
        .into_iter()
        .map(|(name, repo)| {
            let mount = Mount {
                ctx: RepoCtx::new(repo),
                schema: RootNode::new(
                    GQLRepo::new(),
                    EmptyMutation::<RepoCtx>::new(),
                    EmptySubscription::<RepoCtx>::new(),
                ),
            };
            (name, mount)
        })
        .collect()
}

fn graphql_from_query(query: &str) -> std::result::Result<GraphQLRequest, String> {
    let mut gql = None;
    let mut operation = None;
    let mut variables = None;
    if !query.is_empty() {
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let value = percent_decode(value);
            match key {
                "query" => gql = Some(value),
                "operationName" => operation = Some(value),
                "variables" => variables = Some(value),
                _ => {}
            }
        }
    }
    let Some(query) = gql else {
        return Err("missing query".into());
    };
    let mut body = serde_json::Map::new();
    body.insert("query".into(), serde_json::Value::String(query));
    if let Some(operation) = operation {
        if !operation.is_empty() {
            body.insert("operationName".into(), serde_json::Value::String(operation));
        }
    }
    if let Some(variables) = variables {
        if !variables.is_empty() {
            let parsed = serde_json::from_str(&variables).map_err(|err| err.to_string())?;
            body.insert("variables".into(), parsed);
        }
    }
    serde_json::from_value(serde_json::Value::Object(body)).map_err(|err| err.to_string())
}

fn content_type(value: &'static str) -> Header {
    Header::from_bytes(&b"Content-Type"[..], value.as_bytes()).expect("content-type")
}

fn text_response(status: u16, kind: &'static str, body: String) -> Response<Cursor<Vec<u8>>> {
    Response::from_string(body)
        .with_status_code(StatusCode(status))
        .with_header(content_type(kind))
}

fn json_error(message: &str) -> Response<Cursor<Vec<u8>>> {
    text_response(
        400,
        "application/json",
        serde_json::json!({"errors": [{"message": message}]}).to_string(),
    )
}

fn execute(mount: &Mount, request: GraphQLRequest) -> Response<Cursor<Vec<u8>>> {
    let response = request.execute_sync(&mount.schema, &mount.ctx);
    let status = if response.is_ok() { 200 } else { 400 };
    match serde_json::to_string(&response) {
        Ok(body) => text_response(status, "application/json", body),
        Err(err) => json_error(&err.to_string()),
    }
}

fn handle(mut request: Request, mounts: &HashMap<String, Mount>) {
    let url = request.url().to_string();
    let is_get = request.method() == &Method::Get;
    let is_post = request.method() == &Method::Post;
    let (path, query) = match url.split_once('?') {
        Some((path, query)) => (path.to_string(), query.to_string()),
        None => (url, String::new()),
    };
    let path = path.strip_prefix('/').unwrap_or(path.as_str()).to_string();

    let response = match path.split_once('/') {
        Some((name, rest)) if !rest.contains('/') => match mounts.get(&percent_decode(name)) {
            Some(_mount) if rest == "query" && is_get => {
                let html = juniper::http::graphiql::graphiql_source(
                    &format!("/{}/gql", percent_decode(name)),
                    None,
                );
                text_response(200, "text/html; charset=utf-8", html)
            }
            Some(mount) if rest == "gql" && is_get => match graphql_from_query(&query) {
                Ok(gql) => execute(mount, gql),
                Err(err) => json_error(&err),
            },
            Some(mount) if rest == "gql" && is_post => {
                let mut body = Vec::new();
                match request.as_reader().read_to_end(&mut body) {
                    Err(err) => json_error(&err.to_string()),
                    Ok(_) => match serde_json::from_slice(&body) {
                        Ok(gql) => execute(mount, gql),
                        Err(err) => json_error(&err.to_string()),
                    },
                }
            }
            _ => text_response(404, "text/plain", "not found".into()),
        },
        _ => text_response(404, "text/plain", "not found".into()),
    };
    let _ = request.respond(response);
}

fn run() -> Result<()> {
    let matches = Command::new("hgqlserve")
        .version("0.0.0")
        .about("browse a repo")
        .arg(
            Arg::new("listen")
                .short('l')
                .long("listen")
                .value_name("LISTEN")
                .help("if/port to listen on"),
        )
        .arg(
            Arg::new("REPODIR")
                .help("paths to repo dirs (parent of .hg)")
                .required(true)
                .num_args(1..)
                .action(ArgAction::Append),
        )
        .get_matches();

    let listen = matches
        .get_one::<String>("listen")
        .map(String::as_str)
        .unwrap_or("localhost:8080");
    let sa = match listen.to_socket_addrs().map(|mut addrs| addrs.next()) {
        Ok(Some(sa)) => sa,
        Ok(None) => return Err(format!("{} maps to no addresses", listen).into()),
        Err(err) => return Err(format!("Malformed listen address \"{}\": {}", listen, err).into()),
    };

    let repos: Vec<&str> = matches
        .get_many::<String>("REPODIR")
        .into_iter()
        .flatten()
        .map(String::as_str)
        .collect();
    let repomap = find_repos(repos);
    if repomap.is_empty() {
        return Err("No repos successfully opened".into());
    }
    for name in repomap.keys() {
        println!("Repo \"{}\" at http://{}/{}/query", name, listen, name);
    }

    let mounts = init_mounts(repomap);
    match Server::http(sa) {
        Ok(server) => {
            println!("OK");
            for request in server.incoming_requests() {
                handle(request, &mounts);
            }
        }
        Err(err) => println!("Bad! {}", err),
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
