# Running the server

See [building.md](building.md) to build the binaries, and
[hg-serve-differences.md](hg-serve-differences.md) for what is and is not
supported.

## Prepare a repository

Mononoke serves an existing Mercurial repository. `REPODIR` is the directory
that contains `.hg`. Any repository made by `hg init` or `hg clone` works if
its requirements are supported. The main ones that are not are `treemanifest`
and `persistent-nodemap`. Check the requirements with:

```sh
cat REPODIR/.hg/requires REPODIR/.hg/store/requires
```

To make a new repository that serves well, including stream clones:

```sh
hg init REPODIR --config format.use-persistent-nodemap=false
```

The server writes its ssh socket to `REPODIR/.hg/mononoke.sock`. Unix socket
paths are limited to about 100 characters, so keep `REPODIR` short.

## Start the server

```sh
mononoke [OPTIONS] REPODIR [REPODIR...]
```

| Option | Default | Meaning |
|---|---|---|
| `--http-addr ADDR` | `127.0.0.1` | Address for HTTP and HTTPS. Use `0.0.0.0` to listen on every interface. |
| `--http-port PORT` | `8000` | HTTP port. |
| `--https-port PORT` | `8443` | HTTPS port. Used only with `--tls-cert` and `--tls-key`. |
| `--tls-cert PATH` | | PEM certificate chain. Give it together with `--tls-key` to enable HTTPS. |
| `--tls-key PATH` | | PEM private key. |
| `-d`, `--debug` | | Debug-level logging. |

Example:

```sh
mononoke --http-addr 0.0.0.0 --http-port 8000 \
         --tls-cert server.pem --tls-key server.key \
         /srv/hg/myrepo
```

The server logs to stdout. Startup looks like this:

```
INFO Starting up
INFO Listening for connections, repo: "/srv/hg/myrepo"
INFO Listening for HTTP on 0.0.0.0:8000, repo: "/srv/hg/myrepo"
INFO Listening for HTTPS on 0.0.0.0:8443, repo: "/srv/hg/myrepo"
INFO Warmed up, repo: "/srv/hg/myrepo"
```

"Warmed up" appears once the changelog node map and the stream-clone file
listing are built. That takes about 30 seconds on a repository the size of
mozilla-central. The server answers requests before then, and pushes wait
until it finishes.

### Several repositories

Pass several `REPODIR`s to serve them from one process. Each gets its own
`.hg/mononoke.sock`, so all of them are reachable over ssh. HTTP and HTTPS
serve only the first `REPODIR`. To serve several repositories over HTTP, run
one `mononoke` per repository on different ports.

### Running as a service

The server runs in the foreground until stopped. Use your init system (systemd,
a Windows service wrapper, and so on) to keep it running and to capture stdout.
Stop the server before changing a served repository with `hg`, then start it
again (see [hg-serve-differences.md](hg-serve-differences.md#writing-to-a-served-repository)).

## Clients over HTTP and HTTPS

```sh
hg clone http://server:8000/ myrepo
hg clone https://server:8443/ myrepo
```

The URL path is ignored. Full clones are stream clones by default. Add
`--pull` to get a changegroup clone instead.

For a self-signed certificate, tell the client to trust it:

```sh
hg clone https://server:8443/ myrepo --config web.cacerts=/path/to/server.pem
```

Mercurial checks the certificate's common name against the host in the URL, so
make the certificate for the name clients use.

There is no authentication. Anyone who can reach the port can clone and push.
Restrict the port with a firewall, or put an authenticating proxy in front.

## Clients over ssh

Over ssh, the client runs `hg -R <path> serve --stdio` on the server. On the
server that command must run `hgcli`, which relays to the running `mononoke`
through `REPODIR/.hg/mononoke.sock`. `mononoke` must already be running for
that repository, on the same machine as sshd.

Either tell clients where `hgcli` is:

```sh
hg clone --remotecmd /opt/mononoke/hgcli ssh://user@server//srv/hg/myrepo
```

or set it once in the client's `hgrc`:

```ini
[ui]
remotecmd = /opt/mononoke/hgcli
```

Or install `hgcli` on the server under the name `hg`, earlier on `PATH` than
any real Mercurial.

sshd handles authentication. Mercurial's classic ssh protocol has no
compression of its own, so turn on ssh compression for slow links (`ssh -C`, or
`Compression yes` in the client's ssh config).

`hgcli` only supports `serve --stdio`. Other `hg` commands run through it fail.

## Pushing

```sh
hg push                      # to the default path
hg push -B mybookmark        # push and move a bookmark
```

A push is rejected if the server's heads changed since the client last pulled,
as with `hg serve`. Pull, merge or rebase, and push again.

## Checking a served repository

`hg verify` can run against a served repository. It only reads, but stop the
server first if you want a result that reflects no in-flight push.

```sh
hg -R REPODIR verify
```
