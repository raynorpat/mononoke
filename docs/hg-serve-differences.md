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
  bundle1 and bundle2. Changegroups 01, 02 and 03 are supported; treemanifest
  repositories send changegroup 03.
- Clone, pull and push work, including bookmarks (`listkeys` and `pushkey` in
  the `bookmarks` namespace).
- Full clones are stream clones (`stream=v2`). The server advertises
  `stream-preferred`, so clients that support stream v2 stream by default.
  `hg clone --pull` and older clients get a changegroup instead.
- Over HTTP, `getbundle`, `changegroup` and `changegroupsubset` negotiate
  compression as Mercurial's server does.
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
| Config, hooks, extensions | `hgrc` | Reads only `[server] peer-bundle-cache-root` from the repository's `.hg/hgrc`. No user/system config, includes, hooks or extensions. |

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
- `persistent-nodemap`, `treemanifest`
- `dirstate-v2`, `dirstate-tracked-key-v1`. These are working-copy formats,
  and the server reads the dirstate parent for `lookup .`. They are not
  passed on to stream clones, so each client keeps its own format.
- `largefiles`, `manifestv2`, `sqldirstate`. These are accepted, but nothing
  handles them specially. Largefiles blobs are not served.

Refused when the server starts:

- Experimental revlog v2 (`exp-revlogv2.2`), changelog v2 (`exp-changelog-v2`),
  narrow repositories and other unknown requirements ("Unknown requirement").

The server reads valid persistent nodemaps instead of rebuilding their covered
node indexes. It never writes `.n` or `.nd` files. Mercurial brings them up to
date on its next write or `hg debugupdatecaches`; a lagging nodemap after a
Mononoke push is expected. Stream clones include the current nodemap files.

Stream clones need `store` and `fncache`. A repository without them is still
served, but only by changegroup.

### Wire protocol commands

Implemented: `capabilities`, `hello`, `batch`, `heads`, `known`, `branchmap`,
`branches`, `between`, `getbundle`, `changegroup`, `changegroupsubset`,
`stream_out`, `clonebundles`, `clonebundles_manifest`,
`get_cached_bundle_inline`, `protocaps`, `listkeys`, `lookup`, `pushkey` and
`unbundle`.

`getbundle` honours `heads`, so `hg clone -r` and `hg pull -r` get only the
requested changesets and their ancestors.

`lookup` resolves `.`, `null`, `tip`, revision numbers (negative ones count
from the end), hashes and prefixes, bookmarks, tags and named branches.
Branch maps include closed heads, and branch lookup prefers an open head.
Legacy changegroup commands are unavailable for treemanifest repositories.

Clone-bundle capabilities are advertised when `.hg/clonebundles.manifest`
exists. Inline bundles are read below `peer-bundle-cache-root`, which defaults
to `.hg/bundle-cache`. Changes to the manifest and repository setting apply
without restarting. See [running.md](running.md#clone-bundles) for setup.

Largefiles (`statlfile`, `getlfile`, `putlfile`) and LFS are not served.

### Phases and obsolescence

- The server reports no phase roots and is not marked publishing. Clients
  treat everything they pull as public.
- Phase data sent with a push is ignored.
- Obsolescence markers are neither sent nor accepted.

### Pushes

- Pushes check the client's view of the heads. Pushes to one repository take
  turns, and each takes Mercurial's locks (see below). A failed push restores
  revlogs, fncache, requirements and bookmarks, including shared reader state.
  Rollback retains original revlogs with hard links; the repository filesystem
  must support them. This handles reported write failures, not crash recovery.
- Pushes containing nonzero revision flags are rejected before any changes.
  Changegroup pulls of such revisions also fail explicitly. Censored revisions
  and extension-backed flag semantics are not implemented.
- A pushed revision keeps the client's delta when that delta is against a
  revision the server has, compresses smaller than the full text, and keeps
  the delta chain within 1,000 deltas and twice the text's size. Otherwise it
  is stored as a compressed full text, as Mercurial does in the same cases.
  Unlike Mercurial, the server does not search for a better delta base than
  the one the client used.
- New filelogs get Mercurial fncache entries, so `hg verify` passes on a pushed
  repository.
- There is no pushrebase. Stock clients require `hg push --force` to create
  additional heads, as with `hg serve`.

### Writing to a served repository

`hg` can commit, push or tag in a served repository while the server runs:

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
process is writing. Stop Mononoke before an in-place `hg strip` on Windows:
Windows does not allow truncating the revlogs while they are memory-mapped.
On systems that allow it, a clone or pull running during a strip can fail;
run it again.

### Performance behaviour

- A full clone streams the store files with no per-revision work. On
  mozilla-central (887k changesets, 1M files), a stream clone took 24 minutes
  and a `--pull` changegroup clone 52 minutes.
- The changegroup path finds what the client is missing from the changelog
  index, and reads filelogs on 16 threads.
- At startup the server loads a valid persistent nodemap, or builds a node
  map when none is available, and warms the stream-clone file listing in the
  background. Branch metadata is loaded lazily when a command needs it, then
  extended or invalidated after history changes. Historical timings above predate these
  format changes. Pushes wait for any store listing already in progress.
- Responses are streamed, so memory use stays flat during large clones.
