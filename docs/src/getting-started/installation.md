# Installation

This guide covers building DTFS from source. DTFS is written in Rust and uses Cargo as its build system.

## Prerequisites

### Rust Toolchain

DTFS requires Rust **edition 2024** (Rust 1.85+). Install or update via [rustup](https://rustup.rs/):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup update stable
```

Verify your Rust version:

```bash
rustc --version
# rustc 1.85.0 (... ) or newer
```

### System Dependencies

DTFS depends on a few system libraries that must be installed before building.

#### Debian / Ubuntu

```bash
sudo apt update
sudo apt install -y \
    build-essential \
    pkg-config \
    libfuse3-dev \
    librocksdb-dev \
    clang \
    protobuf-compiler
```

#### Fedora / RHEL

```bash
sudo dnf install -y \
    gcc gcc-c++ \
    pkg-config \
    fuse3-devel \
    rocksdb-devel \
    clang \
    protobuf-compiler
```

#### Arch Linux

```bash
sudo pacman -S --needed \
    base-devel \
    pkg-config \
    fuse3 \
    rocksdb \
    clang \
    protobuf
```

#### macOS (Development Only)

FUSE mounting is Linux-only, but you can build the manager and data node crates on macOS for development:

```bash
brew install protobuf rocksdb llvm
```

> **Note:** The `dtfs-fuse` binary compiles on macOS but cannot mount a filesystem. Use Linux or WSL for end-to-end testing.

#### Windows (WSL)

DTFS is designed for Linux. On Windows, use WSL 2 with an Ubuntu distribution:

```powershell
wsl --install -d Ubuntu
```

Then follow the Debian/Ubuntu instructions above inside your WSL environment.

### XFS Filesystem (Production)

Data nodes store chunks on XFS-formatted disks. While any filesystem works for development, XFS is recommended for production due to its efficient handling of large files and `O_DIRECT` support:

```bash
# Format a disk as XFS (DESTRUCTIVE -- wipes the disk)
sudo mkfs.xfs /dev/sdX

# Create a mount point and mount
sudo mkdir -p /mnt/dtfs-disk-01
sudo mount /dev/sdX /mnt/dtfs-disk-01

# Add to /etc/fstab for persistence
echo '/dev/sdX /mnt/dtfs-disk-01 xfs defaults 0 0' | sudo tee -a /etc/fstab
```

## Building from Source

### Clone the Repository

```bash
git clone https://github.com/leduc/dtfs2.git
cd dtfs2
```

### Build All Binaries

Build the entire workspace in release mode:

```bash
cargo build --release
```

This produces three binaries in `target/release/`:

| Binary | Description |
|--------|-------------|
| `dtfs-manager` | Manager node (Raft consensus, metadata, placement, WebUI) |
| `dtfs-data` | Data node (chunk storage, replication, scrubbing) |
| `dtfs-fuse` | FUSE client (POSIX mount point) |
| `dtfs-cli` | Command-line administration tool |

### Build Individual Crates

If you only need a specific binary:

```bash
# Manager only
cargo build --release -p dtfs-manager

# Data node only
cargo build --release -p dtfs-data

# FUSE client only (Linux)
cargo build --release -p dtfs-fuse

# CLI tool
cargo build --release -p dtfs-cli
```

### Verify the Build

```bash
./target/release/dtfs-manager --help
./target/release/dtfs-data --help
./target/release/dtfs-fuse --help
```

Expected output for `dtfs-manager`:

```text
DTFS Manager Node

Usage: dtfs-manager [OPTIONS]

Options:
  -c, --config <CONFIG>  Path to configuration file [default: /etc/dtfs/manager.toml]
  -h, --help             Print help
```

## Running Tests

Run the full test suite:

```bash
cargo test --workspace
```

Run tests for a specific crate:

```bash
cargo test -p dtfs-common
cargo test -p dtfs-data
cargo test -p dtfs-raft
```

## Installation Paths

For a production deployment, install the binaries and create the default configuration directory:

```bash
# Install binaries
sudo install -m 0755 target/release/dtfs-manager /usr/local/bin/
sudo install -m 0755 target/release/dtfs-data /usr/local/bin/
sudo install -m 0755 target/release/dtfs-fuse /usr/local/bin/
sudo install -m 0755 target/release/dtfs-cli /usr/local/bin/

# Create configuration directory
sudo mkdir -p /etc/dtfs

# Create default data directory
sudo mkdir -p /var/lib/dtfs
```

## Workspace Structure

The repository is organized as a Cargo workspace:

```text
dtfs2/
+-- Cargo.toml              # Workspace root
+-- crates/
|   +-- dtfs-common/        # Shared types, config, topology, hash ring
|   +-- dtfs-proto/         # Protobuf definitions and generated code
|   +-- dtfs-raft/          # OpenRaft integration (state machine, log store)
|   +-- dtfs-manager/       # Manager node binary
|   +-- dtfs-data/          # Data node binary
|   +-- dtfs-client/        # Shared gRPC client library
|   +-- dtfs-fuse/          # FUSE client binary
|   +-- dtfs-cli/           # CLI administration tool
+-- proto/                   # .proto source files
+-- deploy/                  # Deployment configurations
+-- tests/                   # Integration tests
+-- webui/                   # WebUI frontend assets
+-- docs/                    # This documentation (mdBook)
```

## Next Steps

- [Quick Start](./quickstart.md) -- run a single-node DTFS instance in 5 minutes
- [First Cluster](./first-cluster.md) -- deploy a multi-node cluster
- [Manager Configuration](../configuration/manager.md) -- full configuration reference
