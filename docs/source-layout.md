# Source layout

The repository is one Cargo workspace. Each directory below is a crate. The
list goes from the lowest layer up.

## Binaries

| Binary | Crate | What it does |
|---|---|---|
| `mononoke` | `server` | The server: ssh (through `hgcli`), HTTP and HTTPS. |
| `hgcli` | `hgcli` | The ssh endpoint, run by sshd. Relays stdin, stdout and stderr to the server's socket. |
| `hgqlserve` | `cmds` | Read-only GraphQL browser for repositories. See [graphql.md](graphql.md). |
| `dumprev` | `cmds` | Debugging tool: prints one revision from a revlog. |
| `idxdump` | `cmds` | Debugging tool: prints revlog index entries. |

## Crates

### Mercurial data

- **`mercurial-types`**: the basic Mercurial types. Node hashes (`NodeHash`),
  file paths and their store encoding (`Path::store_filelog`, `encodedir`,
  `decodedir`), blobs, deltas, and bdiff. No I/O.
- **`mercurial`**: reads and writes repositories on disk.
  - `revlog/`: revlog index and data parsing, reconstruction from delta
    chains, zlib, zstd and lz4 decompression, appending revisions as deltas or
    snapshots (`append_revision`), and reloading after an outside change.
  - `revlogrepo.rs`: `RevlogRepo`, a whole repository. It checks
    requirements, and opens the changelog, the manifest and filelogs
    (including hashed `dh/` paths). It also records fncache entries.
  - `lock.rs`: Mercurial's lock files (`.hg/wlock`, `.hg/store/lock`), in the
    format `hg` reads.
  - `changeset/`, `manifest/`, `file.rs`, `symlink.rs`: parse changesets,
    manifests and file revisions.
- **`bookmarks`**, **`bookmarks/stockbookmarks`**: bookmark traits, and the
  reader and writer for Mercurial's `.hg/bookmarks` file.

### Wire protocol

- **`mercurial-bundles`**: bundle2 encoding and decoding, and changegroup
  version 02.
  - `bundle2_encode.rs`, `part_encode.rs`: write bundle2 streams. A part's
    payload can be generated as it is written (`set_data_generated`).
  - `bundle2.rs`, `part_outer.rs`, `part_inner.rs`: read bundle2 streams, as
    used by push.
  - `changegroup/`: pack and unpack changegroup chunks.
  - `parts/`: helpers for the `changegroup` and `listkeys` parts.
- **`hgproto`**: Mercurial wire protocol commands, independent of the
  transport.
  - `lib.rs`: `Request`, `Response`, `GetbundleArgs` and `BundleStream`.
  - `service.rs`: the `HgCommands` trait, and `HgService`, which dispatches
    requests (including `batch`).
  - `sshproto/`: parsing requests and encoding responses in the ssh framing,
    also reused by HTTP.
- **`sshrelay`**: the framing that multiplexes stdin, stdout and stderr between
  `hgcli` and the server.

### Server

- **`server`** (binary `mononoke`):
  - `main.rs`: command-line options, one thread per repository, the ssh
    socket loop and the HTTP listeners.
  - `repo.rs`: `Repo` and `RepoClient`. Implements `HgCommands`:
    capabilities, heads, known, lookup, getbundle, unbundle, bookmarks. Also
    takes Mercurial's locks for writes, reloads the repository when `hg`
    changes it, keeps the cached stream-clone listing, and warms up at start.
  - `bundle.rs`: changegroup generation for `getbundle`. Finds the missing
    changesets from the changelog index, sends manifest and filelog revisions
    by linkrev, and reads filelogs in parallel (`read_in_order`).
  - `stream.rs`: stream clones (the bundle2 `stream2` part). Lists the store
    and sends the files as they are on disk.
  - `push.rs`: applies `unbundle`. Checks heads, appends changegroup
    revisions, records fncache entries and updates bookmarks.
  - `http.rs`: the HTTP/1.1 wire protocol server, also used for HTTPS, and
    compression negotiation for `getbundle`.
  - `lfs/`: LFS serving. `pointer.rs` parses pointers and oids, `store.rs`
    stores blobs (repo directory, S3 or Azure), `http.rs` serves the batch
    API and blob transfers.
  - `listener.rs`, `uds.rs`: the `.hg/mononoke.sock` listener. `uds.rs` is the
    Windows `AF_UNIX` implementation, since tokio only has Unix sockets on Unix.
- **`hgcli`**: the ssh endpoint. `serve --stdio` connects to
  `<repo>/.hg/mononoke.sock` and relays through `sshrelay`.

### GraphQL

- **`mercurial-graphql`**: the GraphQL schema (juniper) on top of `mercurial`.
- **`cmds`**: `hgqlserve`, `dumprev` and `idxdump`.

### Shared utilities

- **`futures-ext`**: stream and future helpers: ordered futures, stream fork,
  framing, and decoders.
- **`async-compression`**: async compressor and decompressor wrappers used by
  bundle2 (bzip2, gzip, zstd).
- **`asyncmemo`**: an async memoizing cache.

## Other directories

- `docs/`: this documentation. `docs/superpowers/` holds design plans and
  specs from development.
- `server/tests/serve.rs`: end-to-end tests. They start the server and drive
  it with stock `hg` over ssh, HTTP and HTTPS.
- `mercurial-bundles/src/fixtures/`: sample bundles used by the bundle tests.

## How a request flows

ssh: the client runs `ssh host hg -R repo serve --stdio`, which runs `hgcli`.
`hgcli` relays to `.hg/mononoke.sock`. `main.rs` decodes each command with
`hgproto::sshproto` and hands it to `HgService`, which calls `RepoClient` in
`repo.rs`.

HTTP: `http.rs` turns `?cmd=...` and its arguments into the same `Request`, and
then follows the same path.

`getbundle` returns a `BundleStream`. The bundle2 encoder writes into a pipe
while `bundle.rs` or `stream.rs` read the store on the blocking thread pool.
The transport sends bytes as they arrive.
