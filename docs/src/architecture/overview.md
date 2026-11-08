# Architecture Overview

DTFS has three runtime binaries (manager, data node, FUSE client). This page describes their roles, the data model they share, and the safety invariants every change must preserve.

## The three binaries

```text
  dtfs-fuse (/mnt/dtfs)            dtfs-fuse
        |  metadata (gRPC)             |
        v                              v
  +-----------------------------------------+
  | dtfs-manager x3 or x5  (Raft cluster)   |
  | leader + followers (+ optional learners)|
  +-------------------+---------------------+
                      | heartbeats, repair / verify / list RPCs
                      v
  +----------+   +----------+   +----------+
  | dtfs-data|   | dtfs-data|   | dtfs-data|   chunk bytes
  +----------+   +----------+   +----------+
        ^ chunk data (gRPC streams) straight from/to dtfs-fuse
```

Bulk data never flows through a manager. Clients talk to managers for metadata and to data nodes for bytes.

### dtfs-manager

A Raft peer and the REST/gRPC front end. It owns all metadata: files, chunk locations, node and disk registry, volumes, write leases, scrub and migration state.

| Subsystem | Module | Purpose |
|-----------|--------|---------|
| Raft state machine | `dtfs-raft` | The only place state changes; the log is the source of truth |
| Metadata service | `metadata.rs` | ManagerService gRPC; every mutation is a Raft write; followers forward writes to the leader |
| Placement | `placement.rs`, `placement_state.rs` | Hash-ring placement; anchor, goal and misplacement views |
| Health monitor | `monitor.rs` | Heartbeat-based Healthy/Suspect/Down detection only; never schedules repair |
| Task scheduler | `scheduler.rs` and `scheduler/*` | Sole owner of replica repair, migration, flush and the orphan reaper; leader-gated and bounded |
| Tiering engine | `tiering.rs` | Demand-driven cache promotion and demotion |
| Audit log | `audit.rs` | Local ring plus JSONL archive; optional Raft replication |
| REST API and WebUI | `api.rs`, `api/*`, `webui.rs` | Operator surface |

Default ports: 9400 Raft transport, 9401 client and data-node gRPC, 9402 REST and WebUI. See [Consensus](./consensus.md).

### dtfs-data

Stores and serves chunks on local disks, each assigned a tier (hot, warm, cold, tape).

| Subsystem | Module | Purpose |
|-----------|--------|---------|
| Chunk store | `store.rs` | WAL-protected, checksummed chunk I/O; streaming put and get |
| Disk manager | `disk.rs` | Multi-disk layout; persistent disk ids |
| Replication | `replication.rs` | Push replication used by repair and promotion |
| Scrub | `scrub.rs`, `integrity.rs` | Quick and deep scrub, quarantine, fault reporting |
| Memory ladder | `memory.rs` | Per-role pressure response (`[memory]`) |
| gRPC server | `server.rs` | PutChunk, GetChunk, DeleteChunk, ReplicateChunk, VerifyChunk, ListChunks, RecordCacheHit |

There is no in-process chunk cache on the data node; the operating system page cache serves hot chunks. Default port 9500.

On-disk layout of one data disk:

```text
<mount>/
  .dtfs-disk-id          persistent numeric disk id, written once
  .dtfs/wal/             write-ahead entries (<id>.put.wal.json, <id>.delete.wal.json)
  .dtfs/quarantine/      <id>.dat.bad  (renamed, never deleted by scrub)
  chunks/<aa>/<bb>/      <id>.dat  and  <id>.meta
  tmp/                   staging for atomic writes
```

### dtfs-fuse

Mounts the filesystem and translates POSIX calls into gRPC. It holds a metadata cache (5 s TTL, bounded), a write buffer, a read-ahead engine with a bounded memory cache, and an optional local disk cache. Details in [FUSE Mount](../access/fuse-mount.md) and [Read-Ahead](../access/read-ahead.md).

## Data model

```text
  Volume ------ policy: chunk size, mandatory_replicas per tier,
     |                  cache_replicas per tier, anchor datacenter,
     |                  write_safety (min_replicas_before_ack)
     v
  File (path) -- ordered chunk list: [chunk_0, chunk_1, ...]
     |
     v
  Chunk ------- id = BLAKE3(content), size, checksum
     |
     v
  Replica ----- (node_id, disk_id) -> tier, datacenter, live or not
```

Key points:

- A chunk id is the hash of its bytes, so a chunk is immutable. Overwriting part of a file writes a new chunk and rebinds the file's chunk list; the old chunk becomes an orphan and is reclaimed later.
- A replica is identified by the pair `(node_id, disk_id)`, never by node alone. Disk ids are persistent across restarts.
- Replicas fall into two independent classes that are never conflated:

| Class | Configured by | Created by | Removed by |
|-------|---------------|------------|------------|
| Mandatory | `mandatory_replicas.<tier>` | Under-replication scheduler only | Operator reconfiguration or file deletion |
| Cache | `cache_replicas.<tier>.{min,max}` | Tiering engine only, on read demand | Tiering demotion or operator flush |

- The **anchor** is a volume setting (`anchor.datacenter`, optionally a required tier). At least one live replica of every chunk must sit in that datacenter. Datacenter names compare case-insensitively everywhere.
- All metadata above is in the Raft state machine. Chunk bytes and `.meta` files are on data disks. If the two disagree, the orphan reaper and scrub reconcile them (see the invariants below).

## Safety invariants

Every change to a byte-touching path must preserve these. The detailed checklist lives in `.claude/rules/integrity.md`; history is in [Invariants and Bug Archaeology](../internals/invariants.md).

1. **One mutation point.** Metadata changes only through a Raft command. Apply is a pure function of the state and the log entry: timestamps come from the proposer's `now_ms` or the log index, never a replica-local clock.
2. **Checksum end to end.** BLAKE3 is computed incrementally, stored in the chunk meta, and verified on every read and scrub. A mismatch is `data_loss`; the client fails over to another replica.
3. **Durable before acknowledged.** `.dat` and `.meta` are written to a temp file, `sync_all`, then atomically renamed. A WAL entry precedes the operation and is cleared only after fsync.
4. **Remove from metadata first, bytes second.** Every replica removal goes through Raft first. If the Raft write fails or the state machine refuses it, no byte is deleted. A failed byte delete is never rolled back; the orphan reaper finishes it.
5. **Never delete the last live replica or the anchor.** The state machine refuses a `RemoveChunkLocation` that would strand the anchor or remove the last live replica. A replica that justifies dropping another must itself be live.
6. **Quarantine, do not delete.** A faulty chunk is renamed into `.dtfs/quarantine/`. A missing or torn `.meta` is repaired from a verified `.dat` before anything is quarantined.
7. **Unlink does not delete bytes.** Removing a file drops its metadata. The orphan reaper reclaims unreferenced chunks after a grace period measured on the `.dat` mtime. The reaper is keyed by `(node_id, disk_id)`, re-reads the node view after listing, and never removes a chunk recorded under another disk of the same node.
8. **Snapshots never lose state.** A snapshot that cannot be persisted is a fatal Raft error (no silent log purge). A node whose snapshot is older than its purged log refuses to start as a voter.
9. **Replication is never silently lowered.** A write must reach `min_replicas_before_ack` distinct `(node, disk)` replicas or the commit is rejected.
10. **Mutations are authenticated.** With `cluster.shared_secret` set, every mutating RPC on every plane carries an HMAC stamp. See [Authentication](../security/authentication.md).

## Failure and health model

- A data node heartbeats every 5 s. After 3 missed beats it is `Suspect`, after 6 it is `Down` (configurable under `[monitoring]`). The monitor only sets health; it never starts repair.
- `Maintenance`, `Draining` and `Quarantined` are operator or integrity states. The monitor never overwrites them with `Down`.
- The scheduler (leader only) counts only live replicas when deciding whether a chunk is under-replicated, then copies from a live source to a new disk. See the walkthroughs in [Data Flow](./data-flow.md).
- Metadata reads omit replicas on `Down` or `Failed` nodes and disks unless a chunk has no live replica at all, in which case every row is returned as a last resort.

## Memory model

Streaming I/O everywhere: no hot path accumulates a full chunk in a `Vec`. An optional `[memory]` block on each role sets `hard_cap_mb` and warn and shed thresholds. The ladder goes: stream to disk, shrink caches, drop unconsumed read-ahead, pause background tasks, TCP backpressure, and only as an opt-in last resort reject writes with `RESOURCE_EXHAUSTED`. Composite hosts (several roles on one box) set a cap per role so the total stays below host RAM minus the co-resident application.

## Communication

All traffic is gRPC. Manager API (`ManagerService`): file operations, `AllocateChunks`, `CommitChunkLocations`, write leases, `RegisterNode`, `Heartbeat`, `ReportChunkStatus`, `ForwardClientWrite`. Data API (`DataService`): `PutChunk`, `GetChunk`, `DeleteChunk`, `ReplicateChunk`, `VerifyChunk`, `ListChunks`, `GetDiskStatus`, `ReportStats`, `RecordCacheHit`. The POSIX lock RPCs exist on the manager but the FUSE client does not use them; POSIX locks are process-local. Full list: [gRPC API](../api-reference/grpc-api.md).

## Configuration model

Each binary reads a TOML file (`/etc/dtfs/manager.toml`, `data.toml`, `fuse.toml`). Cluster policy (replication, tiering, scrub) lives in Raft and reaches data nodes in heartbeat responses; see [Live Reload](../configuration/live-reload.md).

## Next steps

- [Data Flow](./data-flow.md): write, read, failure and tiering walkthroughs
- [Consensus](./consensus.md): the Raft implementation
- [Data Distribution](./data-distribution.md): how chunks are placed
