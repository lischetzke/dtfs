# Building from Source

DTFS is a Cargo workspace (Rust edition 2024). The supported toolchain is Linux: rocksdb and fuse3 do not build on a Windows host. The canonical, reproducible way to build and test is the Docker build image; building directly on Linux works too.

## Docker build image (canonical)

The image `dtfs-buildenv:latest` (`docker/buildenv.Dockerfile`) carries the pinned Rust toolchain plus `protoc`, `clang` and `libfuse3-dev`. Build it once:

```bash
docker build -t dtfs-buildenv:latest -f docker/buildenv.Dockerfile .
```

Run cargo inside it with the checkout bind-mounted at `/build` and the build cache in two named volumes. From Git Bash on Windows `MSYS_NO_PATHCONV=1` is required, and do not use `bash -lc` inside the image (it drops cargo from `PATH`):

```bash
MSYS_NO_PATHCONV=1 docker run --rm -v "<path-to-checkout>:/build" \
  -v dtfs-target:/target -v dtfs-registry:/usr/local/cargo/registry \
  -w /build dtfs-buildenv:latest cargo test -p dtfs-common --release
```

Always pass `--release`; the cache volumes hold release artifacts. Parallel containers share the `dtfs-target` volume, so cargo may wait on the build lock. The exact command and conventions are the contract in `CLAUDE.md` section A.4; this page only explains them.

## Building natively on Linux

Install the dependencies, then use cargo directly.

| Dependency | Purpose | Debian/Ubuntu | Fedora/RHEL |
|------------|---------|---------------|-------------|
| `protoc` | gRPC code generation | `protobuf-compiler` | `protobuf-compiler` |
| `libclang` | RocksDB build | `libclang-dev` | `clang-devel` |
| `pkg-config` | build helper | `pkg-config` | `pkgconf` |
| FUSE 3 headers | `dtfs-fuse` | `libfuse3-dev` | `fuse3-devel` |

```bash
sudo apt install -y build-essential protobuf-compiler libclang-dev pkg-config libfuse3-dev
cargo build --release
```

Individual crates: `cargo build --release -p dtfs-manager` (likewise `dtfs-data`, `dtfs-fuse`, `dtfs-cli`). Binaries land in `target/release/`.

## Workspace crates

See the table in the [Introduction](../introduction.md#components). `dtfs-proto` regenerates its Rust code from `proto/*.proto` automatically through `build.rs`; after editing a `.proto`, rebuild with `cargo clean -p dtfs-proto && cargo build -p dtfs-proto`.

## Deploy and release builds: use the wrappers

Every build carries a `BUILD_DATE` stamp, the only field that tells two dev builds apart. A bare `cargo build --release` can reuse a stale stamp, so builds that go to a cluster must use a wrapper:

| Script | Use |
|--------|-----|
| `scripts/build-linux-amd64.sh`, `scripts/dtfs-build.sh` | Dev/deploy build with a fresh `BUILD_DATE` |
| `scripts/dtfs-release.sh <tag>` | Release build: also stamps `DTFS_VERSION`, builds, writes `target/release-artifacts/<tag>/` |

Confirm what is running with `dtfs-<binary> --version`. Guards: `scripts/tests/test-build-date-fresh.sh` and `scripts/tests/test-release-version-stamp.sh`. Mechanism: [Invariants](../internals/invariants.md#a35--build-date--version-stamp-mechanism-bug-bs-az-5).

### Release artifacts

`dtfs-release.sh` produces `dtfs-linux-amd64.tar.gz` (`dtfs/bin/*`, `dtfs/systemd/*`, `dtfs/MANIFEST` with per-binary sha256), `SHA256SUMS` and release notes. Releases are cut by CI: a commit whose subject starts with `release vX.Y.Z` publishes that version, one starting with `deploy` publishes an alpha `v<base>_<YYYYMMDDHHmm>`; both then run the rolling update. See [CI Pipeline](../operations/ci-pipeline.md).

### `dtfs-cli update`

The updater downloads the tarball from GitHub releases. It checks every binary against the tarball's `MANIFEST` and refuses to install anything if one hash differs. When a minisign public key is configured (`[updater].minisign_public_key` or `DTFS_MINISIGN_PUBKEY`) it also downloads `SHA256SUMS` and `SHA256SUMS.minisig`, verifies the signature, and checks the tarball's sha256 against the signed list; without a key it prints a warning that authenticity is not verified. Redirects are followed by hand: the access token header is sent only on the first request (never to a redirect target on another host), later hops are HTTPS-only. Token precedence: `--token`, then `DTFS_REPO_TOKEN`, then `[updater].token`. The installed binary is swapped atomically and the matching service is restarted.

## Troubleshooting

- `failed to run custom build command for dtfs-proto`: install `protoc`.
- `rocksdb-sys` build failure: install `libclang-dev`.
- `unresolved import fuse3`: install `libfuse3-dev`.
- Dead-code warnings: no crate carries `allow(dead_code)` or `allow(unused_imports)` (rustc 1.94.1 builds the release and test profiles without the old dead-code ICE). Delete the unused item, or mark it `#[cfg(test)]` when only tests use it.
