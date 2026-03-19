# Architecture Overview

DTFS is composed of three binary components that collaborate to provide a distributed, tiered filesystem. This page describes each component's role, their interactions, and the key subsystems that make them work together.

## The Three Binaries

```text
+------------------------------------------------------------------+
|                        DTFS Cluster                              |
|                                                                  |
|  +-------------------+    +-------------------+                  |
|  |   dtfs-manager    |    |   dtfs-manager    |  (Raft cluster)  |
|  |   (leader)        |<-->|   (follower)      |                  |
|  +--------+----------+    +-------------------+                  |
|           |                                                      |
|           |  gRPC: metadata, placement, heartbeats               |
|           |                                                      |
|  +--------v----------+    +-------------------+                  |
|  |   dtfs-data       |    |   dtfs-data       |  (chunk storage) |
|  |   (SSD / hot)     |    |   (HDD / cold)    |                  |
|  +--------+----------+    +--------+----------+                  |
|           |                        |                             |
|           +--- gRPC: replication --+                             |
|                                                                  |
+------------------------------------------------------------------+
        |                        |
  +-----v------+          +-----v------+
  | dtfs-fuse  |          | dtfs-fuse  |   (FUSE clients)
  | /mnt/dtfs  |          | /mnt/dtfs  |
  +------------+          +------------+
```

### dtfs-manager

The **manager node** is the brain of the cluster. A group of 3 or 5 manager nodes forms a Raft consensus cluster, electing a leader that handles all write operations. Manager responsibilities include:

| Subsystem | Module | Purpose |
|-----------|--------|---------|
| Raft Consensus | `dtfs-raft` | Replicated state machine for all metadata |
| Metadata Service | `metadata.rs` | File and chunk location tracking |
| Placement Engine | `placement.rs` | Decides where to store chunk replicas |
| Cluster Manager | `cluster.rs` | Node registration, join protocol |
| Health Monitor | `monitor.rs` | Heartbeat processing, failure detection |
| Tiering Engine | `tiering.rs` | Access pattern analysis, tier migration |
| Task Scheduler | `scheduler.rs` | Background task queue (replication, migration) |
| Audit Log | `audit.rs` | Immutable record of cluster operations |
| REST API | `api.rs` | HTTP API for WebUI and external integrations |
| WebUI | `webui.rs` | Embedded web interface (served via Axum) |

**Ports (defaults):**

| Port | Protocol | Purpose |
|------|----------|---------|
| 9400 | gRPC | Raft internal communication |
| 9401 | gRPC | Client and data node RPCs |
| 9402 | HTTP | REST API and WebUI |

**State model:** All metadata is stored in the Raft state machine (`StateMachineData`), which holds:

- **File metadata**: path, volume ID, chunk size, chunk location list
- **Node registry**: hostname, location, disk inventory, health status
- **Cluster configuration**: versioned JSON blob, updated via Raft
- **Audit log**: append-only operation history

See [Consensus](./consensus.md) for details on the Raft implementation.

### dtfs-data

The **data node** stores and serves chunk data on local disks. Each data node manages one or more XFS-formatted disks, each assigned to a storage tier (hot, warm, or cold).

| Subsystem | Module | Purpose |
|-----------|--------|---------|
| Chunk Store | `store.rs` | WAL-protected chunk I/O with BLAKE3 checksums |
| Disk Manager | `disk.rs` | Multi-disk management, tier-based disk selection |
| ARC Cache | `cache.rs` | In-memory Adaptive Replacement Cache |
| Replication | `replication.rs` | Push/pull chunk replication between data nodes |
| Scrub Scheduler | `scrub.rs` | Periodic integrity checking (quick and deep) |
| Metrics | `metrics.rs` | I/O statistics exposed via Prometheus |
| gRPC Server | `server.rs` | PutChunk / GetChunk / DeleteChunk RPCs |

**Port (default):** 9500 (gRPC)

**Disk layout on each managed disk:**

```text
/mnt/dtfs-disk-{uuid}/
+-- .dtfs/
|   +-- disk.meta             # Disk identity and tier
|   +-- wal/                  # Write-ahead log entries
|       +-- {chunk_id}.wal.json
+-- chunks/
|   +-- {00-ff}/              # First byte of chunk hash
|       +-- {00-ff}/          # Second byte of chunk hash
|           +-- {hash}.dat    # Chunk data
|           +-- {hash}.meta   # Chunk metadata (size, checksum, timestamps)
+-- tmp/                       # Staging area for atomic writes
```

Chunk writes follow a crash-safe sequence: WAL entry, write to `tmp/`, atomic rename to `chunks/`, remove WAL entry. On startup, the data node runs WAL recovery to clean up any incomplete writes from a previous crash.

### dtfs-fuse

The **FUSE client** mounts the distributed filesystem as a local directory and translates POSIX operations into gRPC calls against the manager and data nodes.

| Subsystem | Module | Purpose |
|-----------|--------|---------|
| Filesystem | `filesystem.rs` | POSIX operation handlers (lookup, open, read, write, readdir) |
| Read-Ahead Engine | `readahead.rs` | Background chunk prefetching |
| Write Buffer | `writebuf.rs` | Coalesces small writes into chunk-sized flushes |
| Migration Trigger | `migration.rs` | Requests closer replicas for remote reads |
| Client Library | `dtfs-client` | Shared gRPC client for manager and data node communication |

**Key behaviors:**

- **Metadata caching**: File locations are cached with a configurable TTL (default 5 seconds) to reduce manager round-trips.
- **Nearest-node selection**: When a chunk has multiple replicas, the client reads from the replica in the closest failure domain based on its own `[location]` config.
- **Read-ahead**: After each chunk read, the engine speculatively prefetches upcoming chunks in background tasks. See [Read-Ahead Modes](../access/read-ahead.md).
- **Write buffering**: Small writes are accumulated until a full chunk is ready, then flushed to the cluster in a single operation.

## Communication Patterns

All inter-component communication uses gRPC with Protocol Buffers. The `.proto` definitions live in the `proto/` directory.

### Client-to-Manager RPCs

Defined in `manager.proto` via the `ManagerService`:

| RPC | Direction | Purpose |
|-----|-----------|---------|
| `CreateFile` | Client -> Manager | Create a new file in the namespace |
| `GetFileLocations` | Client -> Manager | Resolve file path to chunk locations |
| `DeleteFile` | Client -> Manager | Remove a file |
| `ListDirectory` | Client -> Manager | List directory contents |
| `AllocateChunks` | Client -> Manager | Request placement decisions for new chunks |

### Data-Node-to-Manager RPCs

| RPC | Direction | Purpose |
|-----|-----------|---------|
| `RegisterNode` | Data -> Manager | Register a new data node |
| `Heartbeat` | Data -> Manager | Periodic liveness signal with capacity stats |
| `ReportChunkStatus` | Data -> Manager | Report chunk health after scrubbing |

### Data-Node-to-Data-Node RPCs

Defined in `data.proto` via the `DataService`:

| RPC | Direction | Purpose |
|-----|-----------|---------|
| `PutChunk` | Node -> Node | Stream a chunk for replication |
| `GetChunk` | Node -> Node | Stream a chunk for reading or pull replication |
| `DeleteChunk` | Node -> Node | Remove a chunk replica |

### Client-to-Data-Node RPCs

FUSE clients also call `GetChunk` and `PutChunk` directly on data nodes for actual data transfer. The manager only provides metadata and placement decisions -- bulk data never flows through the manager.

```text
                    1. GetFileLocations(path)
  dtfs-fuse  -------------------------------->  dtfs-manager
                    2. Response: [chunk_id -> [node-a, node-b, node-c]]
  dtfs-fuse  <--------------------------------  dtfs-manager

                    3. GetChunk(chunk_id)
  dtfs-fuse  -------------------------------->  dtfs-data (node-a)
                    4. Streamed chunk data
  dtfs-fuse  <--------------------------------  dtfs-data (node-a)
```

## Subsystem Interactions

### Health Monitoring Flow

```text
  dtfs-data                dtfs-manager
     |                         |
     |--- Heartbeat ---------->|
     |    (uptime, chunks,     |
     |     bytes_used,         |  HealthMonitor processes heartbeats
     |     config_version)     |  and updates node health status.
     |                         |
     |<-- HeartbeatResponse ---|  Includes config_version; if stale,
     |    (config_update?)     |  the response includes a full
     |                         |  ConfigUpdate for live reload.
     |                         |
     |  (no heartbeat for      |
     |   15+ seconds)          |  HealthMonitor marks node "suspect"
     |                         |  after 3 missed heartbeats (15s),
     |                         |  then "down" after 6 (30s).
     |                         |
     |                         |  TaskScheduler triggers re-replication
     |                         |  of chunks from the failed node.
```

### Tiering Flow

The tiering engine runs on the manager leader and periodically evaluates chunk access statistics reported by data nodes:

1. Data nodes report per-chunk read counts in heartbeats
2. The tiering engine classifies each chunk's temperature (hot/warm/cold/frozen)
3. If a chunk's temperature no longer matches its storage tier, a migration task is scheduled
4. The task scheduler coordinates the migration: copy chunk to a node with the target tier, update metadata via Raft, remove the old copy

See [Data Flow](./data-flow.md) for complete read and write paths, and [Data Distribution](./data-distribution.md) for placement algorithm details.

## Security Model

- **Shared secret**: Data nodes authenticate with the manager using a shared secret during registration
- **TLS**: All gRPC connections support TLS with auto-generated or user-provided certificates
- **JWT**: The REST API uses JWT tokens for authentication (see [Authentication](../security/authentication.md))
- **Audit log**: All state-mutating operations are recorded in the Raft-replicated audit log

## Configuration Model

Each component loads its configuration from a TOML file at startup:

| Component | Default Config Path | Config Struct |
|-----------|-------------------|---------------|
| dtfs-manager | `/etc/dtfs/manager.toml` | `ManagerConfig` |
| dtfs-data | `/etc/dtfs/data.toml` | `DataNodeConfig` |
| dtfs-fuse | `/etc/dtfs/fuse.toml` | `FuseConfig` |

Cluster-wide configuration (replication policies, tiering thresholds) is stored in the Raft state machine and propagated to data nodes via heartbeat responses. This enables [Live Reload](../configuration/live-reload.md) without restarting nodes.

## Next Steps

- [Data Flow](./data-flow.md) -- trace read and write operations end to end
- [Consensus](./consensus.md) -- understand the Raft implementation
- [Data Distribution](./data-distribution.md) -- learn how chunks are placed across nodes
