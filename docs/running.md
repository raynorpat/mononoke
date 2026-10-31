# Running the server

See [building.md](building.md) to build the binaries, and
[hg-serve-differences.md](hg-serve-differences.md) for what is and is not
supported.

## Prepare a repository

Mononoke serves an existing Mercurial repository. `REPODIR` is the directory
that contains `.hg`. Any repository made by `hg init` or `hg clone` works if
its requirements are supported. Stock Mercurial 7.0 repositories, including
`persistent-nodemap`, and treemanifest repositories are supported. Experimental
revlog v2/changelog v2 and narrow repositories are not. Check requirements with:

```sh
cat REPODIR/.hg/requires REPODIR/.hg/store/requires
```

To make a new repository that serves well, including stream clones:

```sh
hg init REPODIR
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
| `--lfs-store STORE` | `repo` | Where LFS blobs live: `repo`, `s3://bucket[/prefix]` or `azure://account/container[/prefix]`. See [Large files](#large-files-lfs). |
| `--lfs-endpoint URL` | | S3-compatible endpoint (MinIO, R2). Only with an `s3://` store. |
| `--max-plain-file-size BYTES` | `10485760` | Largest file a push may store outside LFS. `0` means no limit. |
| `--public-url URL` | | Base URL clients use for LFS transfers, instead of the request's `Host`. |
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

"Warmed up" appears once the node map and stream-clone
listing are ready. A valid persistent nodemap avoids rebuilding its covered
node index. Branch metadata is loaded on the first command that needs it.
The server answers requests during warm-up; pushes wait for any
store listing already in progress.

### Several repositories

Pass several `REPODIR`s to serve them from one process. Each gets its own
`.hg/mononoke.sock`, so all of them are reachable over ssh. HTTP and HTTPS
serve only the first `REPODIR`. To serve several repositories over HTTP, run
one `mononoke` per repository on different ports.

### Running as a service

The server runs in the foreground until stopped. Use your init system (systemd,
a Windows service wrapper, and so on) to keep it running and to capture stdout.
`hg` can still work in a served repository while the server runs (see
[hg-serve-differences.md](hg-serve-differences.md#writing-to-a-served-repository)).

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
If the proxy changes the host or scheme clients see, set `--public-url` to
the address clients use (see [Behind a proxy](#behind-a-proxy)).

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

To clone or pull part of the history, name a revision. The server resolves
hashes, hash prefixes, revision numbers, `.`, `tip`, bookmarks, tags and named
branches:

```sh
hg clone -r mybookmark http://server:8000/ partial
hg clone -r v1.0 http://server:8000/ tagged
hg pull -r stable
hg pull -r 1a2b3c4d
```

## Large files (LFS)

The server speaks Mercurial's `lfs` extension. Clients send large files to
the server's LFS endpoint, and the repository holds only small pointers. The
first push of a pointer adds `lfs` to the repository's requirements.

**Upload is exactly as open as push.** There is no authentication: anyone who
can push can upload blobs, and anyone who can reach the port can download
them. Restrict the port or authenticate at a proxy, as for pushing.

### Client setup

Enable the extension and say which files are large. Commit a `.hglfs` at the
root of the working copy, with a size catch-all so that nothing big slips
through as a plain file:

```ini
[track]
**.psd = all()
** = size(">10MB")
```

```ini
[extensions]
lfs =
```

Clients over HTTP and HTTPS need nothing else; the LFS address is derived
from the repository URL. Clients over ssh must point `lfs.url` at the HTTP or
HTTPS address, because blobs only travel over HTTP:

```ini
[lfs]
url = http://server:8000/.git/info/lfs
```

A push of a plain file larger than `--max-plain-file-size` is rejected over
every transport, so a missing rule shows up as an error and not as a huge
revlog. `--max-plain-file-size 0` switches the limit off.

The limit also applies to history. Pushing a plain repository with large files
in its history to a fresh server, or pulling from one server and pushing to
another, fails until the receiving server runs with `--max-plain-file-size 0`.
Revisions already on the server are not rechecked.

### Where blobs are stored

`--lfs-store` takes three forms.

```sh
mononoke /srv/hg/myrepo                                   # repo (default)
mononoke --lfs-store s3://my-bucket/lfs /srv/hg/myrepo
mononoke --lfs-store azure://myaccount/mycontainer/lfs /srv/hg/myrepo
```

- `repo` keeps blobs in `.hg/store/lfs/objects`, where `hg` itself keeps
  them. The first repository gets that directory at startup, so it must not
  be on a read-only mount. After a crash, files can remain in
  `.hg/store/lfs/objects/tmp`. They are safe to delete. Stream clones do not
  send anything under `.hg/store/lfs`; clients fetch blobs through the LFS
  endpoint when they update.
- `s3://bucket[/prefix]` uses an S3 bucket. Credentials come from the
  environment (the names `object_store` reads): `AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` (or `AWS_DEFAULT_REGION`) and, for
  temporary credentials, `AWS_SESSION_TOKEN`.
- `azure://account/container[/prefix]` uses Azure Blob Storage. Credentials
  come from `AZURE_STORAGE_ACCOUNT_KEY`, or `AZURE_STORAGE_SAS_KEY`, or
  `AZURE_STORAGE_CLIENT_ID`, `AZURE_STORAGE_CLIENT_SECRET` and
  `AZURE_STORAGE_TENANT_ID`.

For MinIO, Cloudflare R2 and other S3-compatible services, add the endpoint:

```sh
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_REGION=us-east-1 \
  mononoke --lfs-store s3://lfs --lfs-endpoint http://minio:9000 /srv/hg/myrepo
```

A plain `http://` endpoint works without further settings. `https://`
endpoints have their certificates verified.

Only the first repository on the command line serves LFS, because only the
first has HTTP. LFS store and repository refusals stop the server at startup:
an invalid `--lfs-store`, `--lfs-endpoint` without an `s3://` store, a store
that cannot be reached, a blob directory that cannot be created, or `lfs` in
the requirements of a repository other than the first. Any other repository
that fails to open is logged, and the others keep serving.

Notes on bucket stores:

- If `hg` writes a blob on the server (a commit there), the blob goes to
  `.hg/store/lfs/objects`. The server reads it from there when the bucket has
  no copy, and logs a warning naming the oid once.
- A failed upload to S3 can leave an unfinished multipart upload, which the
  bucket listing does not show. Add a lifecycle rule that aborts incomplete
  multipart uploads after a few days.
- One blob can be about 80 GB on S3 (8 MiB parts, at most 10,000 of them).
- Store errors reach clients only as `LFS storage error`. The details are in
  the server log, which can contain bucket URLs and parts of the store's
  responses, so treat the log accordingly.

### Behind a proxy

The server tells clients where to send blobs using the request's `Host`. If
a proxy changes the host or scheme, give the address clients use:

```sh
mononoke --public-url https://hg.example.com/ /srv/hg/myrepo
```

### Converting an existing repository

Conversion rewrites history. **Every changeset hash changes, so existing
clones cannot pull from or push to the converted repository. Every user must
re-clone.**

1. Write a `.hglfs` with what goes to LFS, as above.
2. Convert with the same rules, enable `lfs` in `new`, update to the tip
   and commit the `.hglfs`. Without the update, `hg convert` leaves `new`
   at the null revision and the `.hglfs` commit becomes a second root:

   ```sh
   hg convert --config extensions.convert= --config extensions.lfs= \
       --config 'lfs.track=**.psd or size(">10MB")' old new
   printf '[extensions]\nlfs =\n' >> new/.hg/hgrc
   hg -R new update
   cp .hglfs new/.hglfs
   hg -R new commit -Am "track large files"
   ```

   The push in step 4 aborts if `lfs` is not enabled in `new`.

3. Create an empty repository (`hg init`) and start the server on it with
   the `--lfs-store` you chose.
4. `hg push` from `new` to the server. The client uploads every blob first,
   then pushes the changesets.
5. Tell every user to clone again.

## Clone bundles

Put a clone-bundle manifest in `REPODIR/.hg/clonebundles.manifest`. For a
bundle hosted separately, a line can be:

```text
https://example.com/full.hg BUNDLESPEC=zstd-v2
```

To serve a bundle over the repository's existing connection, create it below
the configured cache root:

```sh
mkdir -p REPODIR/.hg/bundles
hg -R REPODIR bundle --all -t zstd-v2 REPODIR/.hg/bundles/full.hg
```

Set the root in `REPODIR/.hg/hgrc`:

```ini
[server]
peer-bundle-cache-root = bundles
```

Then add this line to `REPODIR/.hg/clonebundles.manifest`:

```text
peer-bundle-cache://full.hg BUNDLESPEC=zstd-v2
```

Without an override the root is `.hg/bundle-cache`. Clients that support
inline clone bundles fetch the file over ssh or HTTP and pull any remaining
revisions normally. Older clone-bundle clients receive external URLs only.
Cached paths must resolve inside the configured root. Manifest and root
setting changes apply without restarting Mononoke. Regenerate the bundle
when updating the cached history.

## Checking a served repository

`hg verify` can run against a served repository while the server runs. It takes
the store lock, so a push waits for it to finish.

```sh
hg -R REPODIR verify
```

Mononoke never rewrites persistent nodemap files after a push. Mercurial
refreshes them on its next write, or explicitly with `hg debugupdatecaches`.
Stop Mononoke before an in-place `hg strip` on Windows, where mapped revlogs
cannot be truncated while the server is running.
