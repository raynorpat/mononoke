# Building

## Requirements

- Rust stable. `rust-toolchain.toml` pins the `stable` channel, and the crates
  use edition 2021. Developed with Rust 1.99 on `x86_64-pc-windows-msvc`.
- A C toolchain for the crates that build native code (zstd, ring). On Windows
  that is the MSVC build tools.
- Windows 10 version 1803 or later, for `AF_UNIX` sockets. Linux and macOS use
  ordinary Unix sockets.
- Mercurial (`hg`) on `PATH`, for the tests only. Tested with Mercurial 7.0.3.

## Build

```sh
cargo build --release
```

The binaries go to `target/release/`: `mononoke`, `hgcli`, `hgqlserve`,
`dumprev` and `idxdump` (with `.exe` on Windows).

To build one binary:

```sh
cargo build --release -p mononoke   # the server
cargo build --release -p hgcli      # the ssh endpoint
cargo build --release -p cmds       # hgqlserve, dumprev, idxdump
```

`Cargo.lock` is not committed. The first build resolves dependencies.

If the drive holding the checkout is short of space, put the build output
elsewhere:

```sh
CARGO_TARGET_DIR=/path/with/space cargo build --release
```

## Test

```sh
cargo test --workspace --exclude mononoke
cargo test -p mononoke --lib
cargo test -p mononoke --test serve -- --test-threads=1
```

The `serve` tests start real servers on local ports and drive them with `hg`,
so they run one at a time. They create repositories under the system temp
directory, and need `hg` on `PATH`.

On Windows, a running `mononoke.exe` or `hgcli.exe` locks its file. Stop any
server started from `target/` before rebuilding.

## Formatting

The crates are formatted with `rustfmt` (edition 2021). The checked-in
`rustfmt.toml` has old options that current rustfmt warns about. Format only
the files you change, because a whole-crate `cargo fmt` rewrites unrelated
code.
