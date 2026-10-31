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

### LFS bucket tests

`s3_lfs_push_and_clone` and `azure_lfs_push_and_clone` print `skipped` and pass
unless their variables are set. To run them, start the emulators and create the
bucket and container first:

```sh
# MinIO: a directory under /data is a bucket
docker run -d --name minio -p 127.0.0.1:9000:9000 --entrypoint sh minio/minio \
  -c 'mkdir -p /data/mononoke-lfs && minio server /data'
export MONONOKE_TEST_S3_ENDPOINT=http://127.0.0.1:9000
export MONONOKE_TEST_S3_BUCKET=mononoke-lfs
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1

# Azurite
docker run -d --name azurite -p 127.0.0.1:10000:10000 \
  mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0
az storage container create -n mononoke-lfs --connection-string \
  'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;'
export MONONOKE_TEST_AZURE_ACCOUNT=devstoreaccount1
export MONONOKE_TEST_AZURE_CONTAINER=mononoke-lfs
export AZURE_STORAGE_USE_EMULATOR=true

cargo test -p mononoke --test serve lfs -- --test-threads=1
docker rm -f minio azurite
```

Use a bucket and container name that does not occur in a URL path such as
`.hg/lfs`, because the tests check that batch responses do not name them.

On Windows, a running `mononoke.exe` or `hgcli.exe` locks its file. Stop any
server started from `target/` before rebuilding.

## Formatting

The crates are formatted with `rustfmt` (edition 2021). The checked-in
`rustfmt.toml` has old options that current rustfmt warns about. Format only
the files you change, because a whole-crate `cargo fmt` rewrites unrelated
code.
