# Introduction

DTFS (Distributed Tiered Filesystem) is a Rust-based distributed storage system designed for organizations that need to manage large volumes of data across heterogeneous storage media with automatic hot/cold tiering, strong consistency, and POSIX-compatible access.

## What is DTFS?

DTFS is a clustered filesystem that automatically places and migrates data across storage tiers -- NVMe SSDs, SATA SSDs, and spinning HDDs -- based on real-time access patterns. It exposes a standard POSIX mount point via FUSE, so existing applications can read and write files without modification.

Under the hood, files are split into large chunks (default 128 MB), replicated across failure domains, and tracked by a Raft-based metadata service that guarantees consistency even during node failures.

```text
+------------------+     +------------------+     +------------------+
|   Application    |     |   Application    |     |   Application    |
+--------+---------+     +--------+---------+     +--------+---------+
         |                        |                        |
   +-----v------+          +-----v------+          +-----v------+
   | dtfs-fuse  |          | dtfs-fuse  |          | dtfs-fuse  |
   | (FUSE      |          | (FUSE      |          | (FUSE      |
   |  client)   |          |  client)   |          |  client)   |
   +-----+------+          +-----+------+          +-----+------+
         |                        |                        |
         +------------+-----------+------------------------+
                      |
              +-------v--------+
              |  dtfs-manager  |  (Raft consensus cluster)
              |  dtfs-manager  |  (3 or 5 nodes)
              |  dtfs-manager  |
              +-------+--------+
                      |
         +------------+-----------+
         |            |           |
   +-----v------+  +-v--------+  +-----v------+
   | dtfs-data  |  | dtfs-data|  | dtfs-data  |
   | (SSD hot)  |  | (SSD     |  | (HDD cold) |
   |            |  |  warm)   |  |            |
   +------------+  +----------+  +------------+
```

## Key Features

### Automatic Tiering

Data is classified into four temperature levels based on access frequency:

| Temperature | Criteria | Default Target Tier |
|-------------|----------|---------------------|
| Hot | >= 5 reads/hour | NVMe / SSD |
| Warm | >= 2 reads/day but below hot threshold | SATA SSD |
| Cold | No access for 14+ days | HDD |
| Frozen | No access for 60+ days | HDD (reduced replicas) |

The [Tiering Engine](./storage/tiering.md) runs on the manager and periodically evaluates every chunk, scheduling migrations when a chunk's temperature no longer matches its current storage tier.

### Per-Tier Replication

Replication factors are configured independently for each tier, so hot data that must be served at low latency can keep 3 replicas while cold archival data keeps only 1:

```toml
[defaults.replication]
hot  = 3
warm = 2
cold = 1
```

See [Replication](./storage/replication.md) for details on the replication protocol.

### Write Safety

Every write must be acknowledged by a configurable minimum number of replicas before the client receives a success response. The default requires 2 out of 3 replicas to acknowledge:

```toml
[defaults.write_safety]
min_replicas_before_ack = 2
write_timeout_secs = 30
```

This prevents data loss if a single node crashes immediately after a write. See [Data Flow](./architecture/data-flow.md) for the full write path.

### Location-Aware Placement

DTFS models your infrastructure as a 4-level topology hierarchy:

```text
Region > Datacenter > Rack > Host
```

The [placement engine](./architecture/data-distribution.md) uses this hierarchy to spread replicas across failure domains. By default, replicas are placed on different racks within the same datacenter. For geo-distributed clusters, replicas can be spread across datacenters or even regions.

### Raft Consensus

All metadata operations -- file creation, chunk placement, node registration, configuration changes -- are replicated through a [Raft consensus](./architecture/consensus.md) cluster of manager nodes. This provides linearizable reads and writes with automatic leader election and failover.

### POSIX Access via FUSE

The `dtfs-fuse` client mounts the distributed filesystem as a local directory. Applications use standard `open()`, `read()`, `write()`, and `close()` system calls. The FUSE client handles:

- Chunk-level read/write translation
- Metadata caching with configurable TTL
- [Read-ahead prefetching](./access/read-ahead.md) with multiple strategies
- Write buffering with automatic chunk-sized flushes
- Nearest-node selection based on client location

### Integrity Scrubbing

Data nodes run periodic [scrub jobs](./storage/scrubbing.md) to detect corruption:

- **Quick scrubs** (every 6 hours): verify file existence and size metadata
- **Deep scrubs** (weekly): full BLAKE3 checksum verification of every chunk

Corrupt chunks are automatically re-replicated from healthy replicas.

### WebUI

A built-in web interface on the manager node provides a dashboard for cluster health, node status, volume management, and configuration changes. See [REST API](./api-reference/rest-api.md) for the underlying API.

## Project Layout

DTFS is structured as a Cargo workspace with the following crates:

| Crate | Description |
|-------|-------------|
| `dtfs-manager` | Manager node binary -- Raft, metadata, placement, tiering, WebUI |
| `dtfs-data` | Data node binary -- chunk storage, replication, scrubbing |
| `dtfs-fuse` | FUSE client binary -- POSIX mount, read-ahead, write buffering |
| `dtfs-client` | Shared gRPC client library used by `dtfs-fuse` and `dtfs-cli` |
| `dtfs-cli` | Command-line administration tool |
| `dtfs-common` | Shared types, configuration, topology, hash ring, checksums |
| `dtfs-proto` | Protobuf definitions and generated gRPC code |
| `dtfs-raft` | Raft integration layer (OpenRaft state machine, log store, networking) |

## Next Steps

- [Installation](./getting-started/installation.md) -- build DTFS from source
- [Quick Start](./getting-started/quickstart.md) -- run a single-node cluster in 5 minutes
- [First Cluster](./getting-started/first-cluster.md) -- set up a multi-node production cluster
- [Architecture Overview](./architecture/overview.md) -- understand how the pieces fit together
