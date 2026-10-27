# Mononoke

Mononoke is a next-generation server for the [Mercurial source control
system](https://www.mercurial-scm.org/), meant to scale up to accepting
thousands of commits every hour across millions of files. It is primarily
written in the [Rust programming language](https://www.rust-lang.org/en-US/).

## Serve

`REPODIR` is the parent of `.hg`.

```
mononoke [--http-addr ADDR] [--http-port PORT] <REPODIR>
```

`--http-addr` defaults to `127.0.0.1` and `--http-port` defaults to `8000`.

HTTPS is optional. Pass `--tls-cert` and `--tls-key` together. The same process then also serves HTTPS on `--https-port` (default `8443`).

```
hgcli -R <REPODIR> serve --stdio
```

`hgcli serve --stdio` relays to `<REPODIR>/.hg/mononoke.sock`.
