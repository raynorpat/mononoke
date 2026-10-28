# Mononoke

A [Mercurial](https://www.mercurial-scm.org/) server written in
[Rust](https://www.rust-lang.org/). It serves ordinary Mercurial repositories
(revlogs in `.hg/store`) to stock Mercurial clients over ssh, HTTP and HTTPS,
with stream clones and streamed changegroups for large repositories.

This tree started from an early snapshot of Facebook's Mononoke. It now has its
own revlog-backed server, and is not the blobstore-based Mononoke in
[Sapling](https://github.com/meta/sapling/tree/main/eden/mononoke).

## Quick start

```sh
cargo build --release

# Serve a repository. REPODIR is the directory that contains .hg.
target/release/mononoke --http-addr 0.0.0.0 /srv/hg/myrepo

# From a client:
hg clone http://server:8000/ myrepo
hg clone --remotecmd /path/to/hgcli ssh://user@server//srv/hg/myrepo
```

HTTPS is enabled when both `--tls-cert` and `--tls-key` are given, on
`--https-port` (default `8443`). The server has no authentication. See
[docs/running.md](docs/running.md) before exposing it.

## Documentation

- [Differences from `hg serve`](docs/hg-serve-differences.md): what is
  supported, what is not, and repository format requirements.
- [Building](docs/building.md): toolchain, build and tests.
- [Running](docs/running.md): starting the server, HTTP and HTTPS, ssh through
  `hgcli`, several repositories, pushing.
- [Source layout](docs/source-layout.md): what each crate and file does.
- [GraphQL browser](docs/graphql.md): `hgqlserve` and its schema.

## License

GPL-2.0-or-later. See [LICENSE](LICENSE). Contributions: see
[CONTRIBUTING.md](CONTRIBUTING.md).
