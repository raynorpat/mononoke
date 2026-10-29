# Differences from `hg serve`

Mononoke serves Mercurial repositories over the same wire protocol as
`hg serve`, so a stock Mercurial client can clone, pull and push. It is a
separate server written in Rust, and it supports a narrower set of features.
This page lists what differs. See [running.md](running.md) for how to start it.

## What is the same

- The client is stock Mercurial. Tested with Mercurial 7.0.3.
- Repositories are ordinary Mercurial repositories on disk (`.hg/store`
  revlogs). There is no import step, and `hg` can still read the repository.
- Transports are ssh, HTTP and HTTPS, using wire protocol version 1 with
  bundle2.
- Clone, pull and push work, including bookmarks (`listkeys` and `pushkey` in
  the `bookmarks` namespace).
- Full clones are stream clones (`stream=v2`). The server advertises
  `stream-preferred`, so clients that support stream v2 stream by default.
  `hg clone --pull` and older clients get a changegroup instead.
- Over HTTP, `getbundle` negotiates compression as Mercurial's server does.
  Stream clones are sent uncompressed and changegroups as zstd. Clients that do
  not negotiate get zlib.

## What is different

### Serving

| | `hg serve` | Mononoke |
|---|---|---|
| Web UI (hgweb) | Yes | No. HTTP serves the wire protocol only. For browsing, see [graphql.md](graphql.md). |
| Several repositories | `hgweb.config` or `--web-conf` | Every `REPODIR` gets an ssh socket. Only the first one gets HTTP and HTTPS. |
| URL path | Picks the repository | Ignored. Any path on the HTTP port is the first repository. |
| ssh | `hg -R repo serve --stdio` | `hgcli -R repo serve --stdio`, which relays to the running server through `<repo>/.hg/mononoke.sock`. |
| Process model | One process per ssh connection | One long-running server process. ssh connections go through a small relay. |
| Config, hooks, extensions | `hgrc` | None. The server reads no `hgrc`, and runs no hooks or extensions. |

### Access control

Nothing restricts reading or pushing. `web.allow-push`, `web.deny-read` and
`push_ssl` have no equivalent. Anyone who can reach the HTTP port can push.

HTTPS authenticates the server only. There are no client certificates, and the
server does not check passwords. Put Mononoke behind something that
authenticates, or only expose it on a trusted network. Over ssh, sshd does the
authentication.

### Repository formats

Requirements accepted in `.hg/requires` and `.hg/store/requires`:

- `store`, `fncache`, `dotencode`, `usefncache`
- `revlogv1`, `generaldelta`, `sparserevlog`
- `revlog-compression-zstd`, `lz4revlog`
- `share-safe`
- `dirstate-v2`, `dirstate-tracked-key-v1`. These are working-copy formats,
  and the server never reads a dirstate. They are not passed on to stream
  clones, so each client keeps its own format.
- `largefiles`, `manifestv2`, `sqldirstate`. These are accepted, but nothing
  handles them specially. Largefiles blobs are not served.

Refused when the server starts:

- `treemanifest` ("treemanifest is not served").
- Anything else, for example `persistent-nodemap`, `revlogv2`, `changelogv2`
  or narrow repositories ("Unknown requirement").

Stream clones need `store` and `fncache`. A repository without them is still
served, but only by changegroup.

### Wire protocol commands

Implemented: `capabilities`, `hello`, `batch`, `heads`, `known`, `branchmap`,
`between`, `getbundle`, `listkeys`, `lookup`, `pushkey` and `unbundle`.

`getbundle` honours `heads`, so `hg clone -r` and `hg pull -r` get only the
requested changesets and their ancestors.

Not implemented or partial:

- `lookup` resolves what a remote usually asks for: `null`, `tip`, revision
  numbers (negative ones count from the end), full hashes, unique hash
  prefixes and bookmarks. It does not resolve tags, branch names or `.`, so
  `hg clone -r v1.0` or `hg pull -r stable` fail with "unknown revision".
- `branchmap` puts every head under `default`. Named branches are not
  reported.
- `changegroup` and `changegroupsubset` (pre-bundle2 clients) return nothing.
- `stream_out` (legacy stream v1), `clonebundles` and `branches` are not
  implemented.
- Largefiles (`statlfile`, `getlfile`, `putlfile`) and LFS are not served.

### Phases and obsolescence

- The server reports no phase roots and is not marked publishing. Clients
  treat everything they pull as public.
- Phase data sent with a push is ignored.
- Obsolescence markers are neither sent nor accepted.

### Pushes

- A push is checked against `check:heads` and applied as one unit. Pushes to
  one repository take turns, and each takes Mercurial's locks (see below).
- A pushed revision keeps the client's delta when that delta is against a
  revision the server has, compresses smaller than the full text, and keeps
  the delta chain within 1,000 deltas and twice the text's size. Otherwise it
  is stored as a compressed full text, as Mercurial does in the same cases.
  Unlike Mercurial, the server does not search for a better delta base than
  the one the client used.
- New filelogs get Mercurial fncache entries, so `hg verify` passes on a pushed
  repository.
- There is no pushrebase. A push that is not a fast-forward of the server's
  heads is rejected, as with `hg serve`.

### Writing to a served repository

`hg` can commit, push, strip or tag in a served repository while the server
runs:

- The server takes Mercurial's own locks: `.hg/wlock` and then
  `.hg/store/lock` for a push or a bookmark change, and `.hg/store/lock` while
  it lists the store for a stream clone. While `hg` holds a lock the server
  waits, up to Mercurial's default 600 seconds. `hg` waits for the server in
  the same way.
- Before each request the server checks the size and modification time of
  the changelog files. If something else changed them, it reloads the
  repository and lists the store again for the next stream clone.

If the server is killed while it holds a lock, the lock file stays behind and
pushes and `hg` commands wait for it. Remove it with
`hg debuglocks --force-free-lock` (or `--force-free-wlock`) once no `hg` or server
process is writing. A clone or pull that is running while `hg strip` removes
revisions can fail; run it again.

### Performance behaviour

- A full clone streams the store files with no per-revision work. On
  mozilla-central (887k changesets, 1M files), a stream clone took 24 minutes
  and a `--pull` changegroup clone 52 minutes.
- The changegroup path finds what the client is missing from the changelog
  index, and reads filelogs on 16 threads.
- At startup the server builds the changelog node map and the stream-clone
  file listing in the background (about 30 seconds on mozilla-central). Pushes
  that arrive during that time wait.
- Responses are streamed, so memory use stays flat during large clones.
