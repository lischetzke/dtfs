# Introduction

DTFS (Distributed Tiered Filesystem) is a Raft-managed, tiered, chunked distributed filesystem written in Rust. It exposes a POSIX-style FUSE mount, stores every chunk with an end-to-end BLAKE3 checksum, and keeps long-term copies on large slow disks while serving the working set from nearer, faster ones.

## What it is built for

DTFS targets one household-scale media library (Plex) served from several datacenters with asymmetric bandwidth. Reads dominate and most chunks are read once or twice; writes are bursty (a finished download lands as a whole season in minutes). The reference deployment has three sites:

| Site | Link | Role |
|------|------|------|
| LELI | up to 40 Mbps | Primary archive. Holds the anchor copy of every chunk on a large HDD node |
| TORO | about 400 Mbps | Working set and read cache on SSD |
| NRB | about 1 Gbps | Landing zone for fresh downloads, replicated to LELI |

Everything else follows from that: the anchor rule, cache replicas that appear on read demand, bounded memory, and integrity checks on every byte path. Non-goals are multi-tenant isolation and full POSIX (xattrs, mmap and open-unlink races are best effort).

## The model in one picture

```text
  file    = ordered list of chunks (default 4 MB each)
  chunk   = immutable bytes, id = BLAKE3 hash of the bytes
  replica = one copy of a chunk on one (node, disk)

  Mandatory replicas  maintained by the repair scheduler, never evicted
        +-- always include the ANCHOR copy (LELI, cold HDD)
  Cache replicas      created on read demand by the tiering engine,
                      evictable, never counted as durability
```

Metadata (files, chunk locations, nodes, volumes, leases) lives in a Raft state machine on 3 or 5 manager nodes. Chunk bytes live only on data nodes and never pass through a manager. See the [Architecture Overview](./architecture/overview.md) for the data model and safety invariants, and [Data Flow](./architecture/data-flow.md) for step-by-step walkthroughs.

## Key properties

- **Single mutation point.** Every metadata change is a Raft command, and applying a command is deterministic (no wall clock, no randomness), so all managers and every replay agree. See [Consensus](./architecture/consensus.md).
- **Integrity first.** BLAKE3 is computed on the client, re-checked by the data node on write, on every read and on every scrub, and checked again by the client on the fetched bytes. A faulty chunk is quarantined by renaming, never deleted. See [Scrubbing](./storage/scrubbing.md).
- **Anchor invariant.** Every chunk keeps at least one live replica in the volume's anchor datacenter. Tiering may move or drop any other replica. See [Replication](./storage/replication.md).
- **Tiering by demand.** Cache replicas are promoted per datacenter when reads cross a threshold inside a window and demoted after the window goes idle. See [Tiering](./storage/tiering.md).
- **Bounded memory.** Streaming I/O plus a per-role memory ladder (warn, shrink, shed) so DTFS does not starve a co-resident application. See [Caching](./storage/caching.md).
- **Location-aware reads.** Operator priority, then physical distance, then tier. See [FUSE Mount](./access/fuse-mount.md).

## Components

| Crate | Description |
|-------|-------------|
| `dtfs-manager` | Manager binary: Raft peer, metadata gRPC, REST API and WebUI, placement, repair, tiering, orphan reaper |
| `dtfs-data` | Data node binary: chunk store, replication, scrub, quarantine |
| `dtfs-fuse` | FUSE client binary: POSIX mount, write buffer, read-ahead, local cache |
| `dtfs-client` | Shared gRPC client library used by `dtfs-fuse` and `dtfs-cli` |
| `dtfs-cli` | Administration CLI and updater |
| `dtfs-common` | Shared types, configuration, hash ring, checksums, crypto, memory budget |
| `dtfs-proto` | Protobuf definitions and generated gRPC code |
| `dtfs-raft` | OpenRaft state machine, log store, network transport |

## Next steps

- [Installation](./getting-started/installation.md): build DTFS from source
- [Quick Start](./getting-started/quickstart.md): run a single-node cluster
- [First Cluster](./getting-started/first-cluster.md): multi-node setup
- [Architecture Overview](./architecture/overview.md): how the pieces fit together
- [Building from Source](./development/building.md): building and testing with the Docker build image
