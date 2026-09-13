# DTFS build/test image (CLAUDE.md § A.4). Reproduces the local
# `dtfs-buildenv:latest`: rust 1.94.1 on Debian bookworm plus the native
# deps rocksdb, tonic/prost and fuse3 need. The checkout is bind-mounted
# at /build and the cargo cache lives in two named volumes:
#
#   docker build -t dtfs-buildenv:latest -f docker/buildenv.Dockerfile .
#   MSYS_NO_PATHCONV=1 docker run --rm -v "<checkout>:/build" \
#     -v dtfs-target:/target -v dtfs-registry:/usr/local/cargo/registry \
#     -w /build dtfs-buildenv:latest cargo test --workspace --release
#
# rustc 1.94 is pinned: dtfs-manager/dtfs-data carry a crate-level
# #![allow(dead_code)] that works around a dead-code ICE on this version.
FROM rust:1.94.1-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        clang libclang-dev cmake protobuf-compiler pkg-config \
        libfuse3-dev fuse3 libssl-dev \
    && rm -rf /var/lib/apt/lists/*

ENV CARGO_TARGET_DIR=/target
WORKDIR /build
