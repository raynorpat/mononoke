# Serve a local Mercurial repo from Mononoke

## Intent

This tree is the early public Mononoke drop. It has no Cargo project, and the README says it does not build. The point of making it build is to serve a local Mercurial repository to a stock Mercurial client.

The repository that matters is mozilla-central, cloned first with ordinary Mercurial from `https://hg-edge.mozilla.org/mozilla-central`. Mononoke then serves that local `.hg`. A second, empty Mercurial clones from Mononoke and gets history and a working copy. Push from that client writes back into the same store.

## Goals

- `cargo build --workspace` succeeds on this Windows machine with current stable Rust.
- One `mononoke` process serves a local revlog repository (the parent of `.hg`).
- A stock `hg` on this machine can clone that repo over `ssh://`, `http://`, and `https://`, and the clone has history and a working copy.
- A stock `hg push` over those same three transports writes new changesets into the served revlog store. Ordinary `hg` pointed at the served repo can see them afterwards.
- On Windows, the ssh handoff socket is opened with Winsock. On Unix, the existing Unix-domain socket stays.
- The revlog reader and writer handle repositories whose `.hg/requires` includes `generaldelta`, `revlogv1`, and `revlog-compression-zstd`.
- `eden-server` and `blobimport` are removed, along with crates that only they use.

## Non-goals

- Mononoke does not clone mozilla-central itself. A person clones it with ordinary Mercurial first, then points Mononoke at that directory.
- No login, accounts, or authorization. The machine is the trust boundary.
- No Eden REST API, no blob store, no `blobimport`.
- `hgqlserve` and `mercurial-graphql` stay. They are not part of this serve path and they are not deleted.
- Requirement names other than the three above, plus names `RevlogRepo` already accepts, are not implemented. Opening a repo that lists anything else fails, and the error names the unknown requirement.
- No work whose only purpose is to make a full mozilla-central clone fast.

## Anti-goals

- A client that receives bookmarks and an empty changelog is not a successful clone.
- A second HTTP server inside `hgcli` is not the design. `hgcli` stays the stdio relay.
- Rewriting the served repo into a different store format on push is not acceptable. Push appends to the revlog store the repo already uses.

## What exists today

There is no `Cargo.toml`. The crates are 2017-era Rust (`extern crate`, `futures` 0.1, `tokio-core`, `error-chain`).

`server` (`mononoke`) takes repo directory paths, opens each as `mercurial::RevlogRepo` on the `.hg` directory, and listens on `<repo>/.hg/mononoke.sock`. Connections speak the Mercurial ssh wire protocol through `HgService` and `RepoClient`.

`hgcli serve` implements only `--stdio`. It connects to `<repo>/.hg/mononoke.sock` and copies stdin, stdout, and stderr. Its port and address flags are not a listening server. The command bails on any other mode.

`RepoClient::getbundle` builds a bundle2 that contains bookmarks. It does not contain changeset, manifest, or file revisions. `changegroup` and `unbundle` return without sending or writing revisions. The integration test `tests/integration/test-init.t` matches that behavior: an ssh pull reports no changes, then learns a bookmark.

`RevlogRepo::open` reads `.hg/requires` and rejects any line it cannot parse (`UnknownReq`). It already recognizes `store`, `fncache`, `dotencode`, `generaldelta`, `treemanifest`, `manifestv2`, `usefncache`, `revlogv1`, `largefiles`, `lz4revlog`, and `sqldirstate`. It does not recognize `revlog-compression-zstd`. Recognizing a name is not the same as reading and writing that format. `generaldelta` and `revlogv1` are already in the enum. Zstd compression of revlog chunks, and correct full-text reconstruction for generaldelta bases, have to actually work for clone and push.

mozilla-central's published bundles advertise `generaldelta`, `revlogv1`, and `revlog-compression-zstd`. A fresh clone may also list requirements this parser does not know. Those still fail open.

`eden-server` is a small HTTP service that returns a root-tree manifest id from a blob repo. `cmds/blobimport.rs` copies a revlog repo into that blob repo. `tests/integration/test-eden-server.t` is the only test of that path. `blobrepo`, `blobstore` (`fileblob`, `memblob`, `rocksblob`), `heads` (`fileheads`, `memheads`), and `bookmarks/filebookmarks` are used from that path. `bookmarks` and `bookmarks/stockbookmarks` are used by `server` and stay. `cmds/hgqlserve.rs` reads a `RevlogRepo` directly and stays. Its `hgqlserve_build_info` dependency is already behind `cfg(fbcode_build)`.

## How it works

### Build

A Cargo workspace at the repo root builds every crate we keep, on current stable Rust, on Windows. `cargo build --workspace` is the build. Internal choices (edition, `tokio` and `futures` versions, how far the 2017 style is rewritten) are the builder's, as long as that command succeeds and the behavior in this document holds.

### One process

`mononoke <repopath>` opens `<repopath>/.hg` and serves that one repo. The same process owns:

- the ssh socket at `<repopath>/.hg/mononoke.sock`
- an HTTP listener
- an HTTPS listener when a certificate path and a private-key path are passed

Default bind address is loopback. The HTTP port is a flag with a fixed default the builder chooses. HTTPS uses the same HTTP wire protocol on its own port and is enabled only when both the certificate path and the key path are present.

Repo operations live in one place (`RepoClient` / `HgCommands` or its replacement). The ssh framing and the HTTP framing both call that place. Clone and push must not drift between transports.

### SSH handoff

Unchanged shape. Mercurial runs `hgcli -R <repo> serve --stdio`. That process connects to `mononoke.sock` and relays the ssh wire protocol.

On Unix the socket stays a Unix-domain socket, including the current replace-if-stale bind behavior.

On Windows both ends open that same filesystem path with Winsock Unix-domain sockets (`AF_UNIX`). `hgcli` does not use `nix` for this. The path Mononoke listens on and the path `hgcli` connects to are the same string.

The ssh test does not need a real ssh daemon. It uses a local stand-in that execs `hgcli`, in the spirit of `tests/integration/third_party/dummyssh.py` and `test-init.t`.

### HTTP and HTTPS

The listener speaks the Mercurial HTTP wire protocol that stock `hg clone http://127.0.0.1:<port>/` and `hg clone https://127.0.0.1:<port>/` speak, and the commands those clients send for clone and push. Which framing version that is (HTTP protocol v1, v2, or both) is the builder's call after checking what the installed `hg` actually sends. The acceptance test is that installed client, not a hand-written subset of commands.

The HTTPS test trusts the certificate that was passed to Mononoke. It does not turn TLS verification off for every host.

There is no authentication.

### Clone

`getbundle` (and any other command the client uses to fetch history) returns a bundle the stock client can apply: changesets, manifests, and file revisions, plus the bookmarks the repo has. After clone, `hg log` in the new working copy shows the source history, and files at the working-copy parent match the source.

The existing bookmark-only bundle is not enough.

### Push

`unbundle` and the bookmark updates a stock `hg push` sends are applied to the served revlog store. New revisions are stored in the format that repo already requires (`revlogv1`, `generaldelta`, `revlog-compression-zstd` when those are in `.hg/requires`). A later `hg log` in the served directory shows the pushed commit. A later clone from Mononoke includes it.

Push of obsolescence markers, phases, or other side data is in scope only when the stock client sends it as part of a normal `hg push` of a regular commit. Anything the client does not send is left alone.

### Revlog formats

These three are real read and write support, not just accepted names in `.hg/requires`:

- `revlogv1`
- `generaldelta`
- `revlog-compression-zstd`

Full text reconstruction follows generaldelta base revisions. Chunks compressed with zstd decompress. New chunks written on push use zstd when the repo requires `revlog-compression-zstd`, and record generaldelta bases when the repo requires `generaldelta`.

Names `RevlogRepo::open` already accepts stay accepted. `treemanifest` is one of those names. Accepting the name is not a promise to implement tree manifests. If a tree-manifest repo cannot be cloned correctly, opening it fails with an error that says tree manifests are not served, rather than producing a bundle stock `hg` will mis-apply. mozilla-central is only in scope when its `.hg/requires` is the three formats above plus names already accepted that this server can actually read. If the clone lists anything else, open fails and names the requirement. That failure is the stop sign from the design conversation. Do not silently ignore unknown requirements.

### What is deleted

- the `eden-server` crate
- `cmds/blobimport.rs`
- `tests/integration/test-eden-server.t` and the `blobimport` / `edenserver` helpers in `tests/integration/library.sh`
- any crate that, after those deletions, has no caller outside itself and its own tests

`dumprev`, `idxdump`, `hgqlserve`, and `mercurial-graphql` stay and are members of the workspace. A missing Facebook-only build-info crate must not break the default build.

## Tests

Automated, on Windows:

1. `hg init` a tiny repo with `generaldelta`, `revlogv1`, and `revlog-compression-zstd` enabled, and commit a file.
2. Start `mononoke` on it.
3. Clone over the fake-ssh handoff, over HTTP, and over HTTPS.
4. In each clone, check `hg log` and the file contents.
5. Commit in one clone and `hg push` to Mononoke. Check the served repo with ordinary `hg log`, then clone again and see the new commit.
6. A repo whose `.hg/requires` contains an unknown line fails open, and the error includes that line.

The mozilla-central loop is the manual acceptance test, run once the automated test passes. It is not part of the automated suite: clone mozilla-central with ordinary Mercurial from hg-edge, serve that directory, clone it from Mononoke over ssh and over http (and https when a cert is supplied), and confirm history and a working copy. Push of a new public commit back to the mozilla-central server is not part of the test. Push is tested against the local served clone only.

## Calls made here

These were not spelled out by the person who asked. They can be rejected.

- Current stable Rust, and `cargo build` on Windows, is the toolchain. The 2017 compiler is not kept.
- One `mononoke` process owns the socket and the HTTP(S) ports. `hgcli` stays stdio-only.
- Windows uses Winsock `AF_UNIX` on the existing `mononoke.sock` path. The protocol is not switched to TCP for the ssh handoff.
- HTTPS is configured with a certificate path and a key path. There is no login.
- Crates that only `eden-server` and `blobimport` use are deleted with them. GraphQL stays.
- Automated coverage is a tiny repo with those three requirements. mozilla-central is the manual acceptance test.
- The ssh test uses a local fake ssh that execs `hgcli`, not a real sshd.
- Tree-manifest repos fail open with an explicit error even though `treemanifest` is already a recognized requirement name, unless the builder finds that the flat reader already produces a correct clone for them.

## Left to the builder

- Cargo package layout and dependency versions, given the build constraint above.
- HTTP wire-protocol framing version, given the stock-`hg` constraint above.
- Whether `Revlog::get_rev` already walks generaldelta bases and which compression bytes it already understands. Implement what is missing.
- The default HTTP port, flag names, and how the test certificate is generated.
- How far `test-init.t` is rewritten versus replaced. Bookmark pull behavior it covers must still hold once history is sent: a client that already has the changesets still learns new bookmarks.
