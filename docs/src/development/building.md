# Building from Source

DTFS is a Rust project organized as a Cargo workspace. This page covers how to set up the build environment and compile all components.

## Prerequisites

### Rust Toolchain

DTFS requires the Rust 2024 edition (Rust 1.85 or later). Install Rust using `rustup`:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Verify the installation:

```bash
rustc --version
cargo --version
```

### System Dependencies

The following system libraries are required at build time:

| Dependency | Purpose | Package (Debian/Ubuntu) | Package (Fedora/RHEL) |
|------------|---------|------------------------|----------------------|
| `protoc` | Protocol Buffers compiler (for gRPC codegen) | `protobuf-compiler` | `protobuf-compiler` |
| `libclang` | Required by RocksDB build | `libclang-dev` | `clang-devel` |
| `pkg-config` | Build system helper | `pkg-config` | `pkgconf` |
| FUSE headers | Required for `dtfs-fuse` | `libfuse3-dev` | `fuse3-devel` |

**Debian/Ubuntu:**

```bash
sudo apt update
sudo apt install -y build-essential protobuf-compiler libclang-dev \
  pkg-config libfuse3-dev
```

**Fedora/RHEL:**

```bash
sudo dnf install -y gcc protobuf-compiler clang-devel pkgconf fuse3-devel
```

**macOS (for development only -- FUSE client requires Linux):**

```bash
brew install protobuf llvm pkg-config
```

## Building

### Full Workspace Build

From the repository root:

```bash
cargo build
```

This compiles all crates in the workspace in debug mode. Binaries are placed in `target/debug/`.

### Release Build

For production use, build with optimizations:

```bash
cargo build --release
```

Release binaries are placed in `target/release/`.

### Building Individual Crates

To build a specific component:

```bash
cargo build -p dtfs-manager
cargo build -p dtfs-data
cargo build -p dtfs-fuse
cargo build -p dtfs-cli
```

### Workspace Crate Structure

| Crate | Type | Description |
|-------|------|-------------|
| `dtfs-manager` | Binary | Manager node (Raft, metadata, placement, tiering, WebUI) |
| `dtfs-data` | Binary | Data node (chunk storage, replication, scrubbing) |
| `dtfs-fuse` | Binary | FUSE client (POSIX mount, read-ahead, write buffering) |
| `dtfs-cli` | Binary | Command-line administration tool |
| `dtfs-client` | Library | Shared gRPC client used by `dtfs-fuse` and `dtfs-cli` |
| `dtfs-common` | Library | Shared types, configuration, topology, hash ring, checksums |
| `dtfs-proto` | Library | Protobuf definitions and generated gRPC code |
| `dtfs-raft` | Library | Raft integration (OpenRaft state machine, log store, networking) |

## Building on Windows (WSL)

The FUSE client (`dtfs-fuse`) requires Linux kernel FUSE support. On Windows, use WSL 2 (Windows Subsystem for Linux) for development:

1. **Install WSL 2** if not already available:

```powershell
wsl --install -d Ubuntu
```

2. **Install dependencies** inside WSL:

```bash
sudo apt update
sudo apt install -y build-essential protobuf-compiler libclang-dev \
  pkg-config libfuse3-dev curl
```

3. **Install Rust** inside WSL:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
```

4. **Clone and build** inside WSL:

```bash
cd /path/to/dtfs2
cargo build
```

The manager, data node, and CLI can be compiled natively on Windows, but the FUSE client is Linux-only. For Windows-native builds of non-FUSE components:

```powershell
cargo build -p dtfs-manager -p dtfs-data -p dtfs-cli -p dtfs-client -p dtfs-common -p dtfs-proto -p dtfs-raft
```

## Proto Code Generation

The `dtfs-proto` crate uses `tonic-build` to generate Rust code from the `.proto` files in the `proto/` directory. Code generation runs automatically as part of `cargo build` via a `build.rs` script. No manual `protoc` invocation is needed beyond having the `protoc` binary on your PATH.

If you modify `.proto` files, a clean rebuild of `dtfs-proto` ensures regeneration:

```bash
cargo clean -p dtfs-proto
cargo build -p dtfs-proto
```

## Installed Binaries

After a release build, the following binaries are available:

| Binary | Path |
|--------|------|
| `dtfs-manager` | `target/release/dtfs-manager` |
| `dtfs-data` | `target/release/dtfs-data` |
| `dtfs-fuse` | `target/release/dtfs-fuse` |
| `dtfs-cli` | `target/release/dtfs-cli` |

These are statically linked against `rustls` (no OpenSSL dependency at runtime). The only runtime dependency is FUSE 3 for `dtfs-fuse`.

## Troubleshooting

### `protoc` not found

```
error: failed to run custom build command for `dtfs-proto`
```

Install the Protocol Buffers compiler (`protobuf-compiler` on Debian/Ubuntu, `protobuf` on macOS via Homebrew).

### RocksDB build failure

```
error: failed to run custom build command for `rocksdb-sys`
```

Ensure `libclang` is installed. On Debian/Ubuntu: `sudo apt install libclang-dev`.

### FUSE headers not found

```
error[E0432]: unresolved import `fuse3`
```

Install FUSE 3 development headers: `sudo apt install libfuse3-dev` (Debian/Ubuntu) or `sudo dnf install fuse3-devel` (Fedora).
