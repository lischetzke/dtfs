# gRPC API

All inter-component traffic uses gRPC with Protocol Buffers. Definitions: `proto/*.proto`; generated Rust code: the `dtfs-proto` crate. This page follows the current `.proto` files.

## Services

| Service | Proto | Served by | Port (default) | Status |
|---|---|---|---|---|
| `dtfs.manager.ManagerService` | `manager.proto` | manager | `[grpc].listen_addr` (9401) | in use |
| `dtfs.manager.RaftTransport` | not a proto service | manager | same port as ManagerService | in use, see [Raft transport](#raft-transport) |
| `dtfs.data.DataService` | `data.proto` | data node | `[grpc].listen_addr` (9500) | in use |

`common.proto` holds shared messages (`Location`, `DiskInfo`, `NodeInfo`). With `[tls]` enabled all connections use mutual TLS ([TLS](../security/tls.md)). Message size caps come from `[grpc].max_message_size_mb` (servers) and `[client].max_message_size_mb` (FUSE).

## Authentication

When `cluster.shared_secret` is non-empty, mutating RPCs must carry two gRPC metadata entries:

| Metadata key | Value |
|---|---|
| `x-dtfs-rpc-ts` | Unix seconds when the stamp was made. Rejected when more than 300 s from the receiver's clock (keep clocks synced, see [Time Sync](../operations/time-sync.md)). |
| `x-dtfs-rpc-mac` | Lowercase hex HMAC-SHA256 (key = shared secret) of the canonical message below. |

Canonical message (newline-terminated fields):

```
<plane>\nv1\n<Method>\n<ts>\n<binding>\n
```

There are two planes so a stamp for one cannot be replayed on the other: `dtfs-data-rpc` for DataService and `dtfs-mgr-rpc` for ManagerService and the Raft transport. The `binding` ties the stamp to the object. A missing or invalid stamp returns `PERMISSION_DENIED`. With an empty secret (lab mode) stamping and verification are skipped.

| Service | RPC | Binding |
|---|---|---|
| DataService | `PutChunk`, `ListChunks`, `GetDiskStatus`, `ReportStats` | empty |
| DataService | `GetChunk`, `VerifyChunk`, `RecordCacheHit` | `chunk_id` |
| DataService | `DeleteChunk` | `chunk_id\|disk_id\|if_mtime_le_ms` (unset = empty); legacy `chunk_id` accepted while `require_delete_binding = false` |
| DataService | `ReplicateChunk` | `chunk_id`, a literal `\|`, then `target_address` |
| ManagerService | `CreateFile`, `DeleteFile`, `AllocateChunks`, `CommitChunkLocations`, `SetFileSize`, `CreateSymlink`, `AcquireWriteLease`, `RenewWriteLease`, `ReleaseWriteLease` | `path` |
| ManagerService | `RenameFile` | `from\nto` |
| ManagerService | `CreateLink` | `source\ndest` |
| ManagerService | `Heartbeat`, `ReportChunkStatus` | `node_id` |
| ManagerService | `ReleaseLeasesByHost` | `hostname` |
| ManagerService | `ForwardClientWrite` | hex `blake3(client_request_json)`; while `cluster.accept_legacy_forward_secret = true` (default) the follower also fills the legacy raw `shared_secret` field (for pre-HMAC leaders) and the leader accepts an unstamped forward on it; with `false` the field is sent empty and an unstamped forward is refused |
| ManagerService | `RegisterNode` | no stamp; the request carries `shared_secret` instead |
| ManagerService | `GetFileLocations`, `ListDirectory`, `ReadSymlink`, `ClusterCapacity` | not authenticated (read-only) |

### Raft transport

Managers exchange Raft traffic (AppendEntries, Vote, InstallSnapshot) over the service `dtfs.manager.RaftTransport` on the manager gRPC port, with JSON payloads. Each call is stamped on the `dtfs-mgr-rpc` plane with method `RaftRpc` and the binding `<from>\n<to>\n<blake3(body)>` (sender id in the `x-dtfs-raft-from` header, receiver's own id). Receiver behaviour is set by `cluster.require_raft_auth`:

| `require_raft_auth` | Valid stamp | No stamp | Invalid or incomplete stamp |
|---|---|---|---|
| `false` (default, rolling-update safe) | accepted | accepted, rate-limited warning | rejected |
| `true` | accepted | rejected | rejected |

Enable `true` once every manager runs a build that signs Raft RPCs. An empty shared secret skips Raft authentication entirely.

## ManagerService (`manager.proto`)

Used by FUSE clients (metadata, leases, locks), data nodes (registration, heartbeat) and peer managers (`ForwardClientWrite`). Any manager serves these; writes are applied through Raft, and a follower forwards them to the leader.

### Namespace

| RPC | Request | Response |
|---|---|---|
| `CreateFile` | `path`, optional `volume_id` | `success`, `chunk_size`, `volume_id` |
| `GetFileLocations` | `path` | `path`, `total_size`, `chunks` (`ChunkLocationInfo`), `chunk_size`, `created_at_ms`, `modified_at_ms`, `is_directory`, `volume_id`, `is_symlink` |
| `DeleteFile` | `path` | `success` |
| `ListDirectory` | `path` | `entries` (`DirectoryEntry`) |
| `RenameFile` | `from`, `to` | `success`, `error` |
| `SetFileSize` | `path`, `new_size` | `success`, `error` |
| `CreateSymlink` | `path`, `target` | `success`, `error` |
| `ReadSymlink` | `path` | `target`, `found` |
| `CreateLink` | `source`, `dest` | `success`, `error` |
| `ClusterCapacity` | none | `total_bytes`, `used_bytes`, `file_count` |

`ChunkLocationInfo`: `chunk_id` (hex BLAKE3), `size`, `node_addresses` (all live replicas), `node_tiers` and `node_locations` (positional with `node_addresses`; empty from an older manager), `chunk_index`. Replicas on Down or Failed nodes or Failed/Removed disks are omitted unless the chunk has no live replica at all, in which case every row is returned. `volume_id` is the file's own volume; clients must not substitute `default`.

`DirectoryEntry`: `name`, `size`, `is_directory`, `created_at`, `modified_at`, `is_symlink`. `is_symlink` (and `GetFileLocationsResponse.is_symlink`) is `false` from an older manager.

### Write path

Order used by the FUSE client: `AcquireWriteLease`, `AllocateChunks`, stream `PutChunk` to the data nodes, `CommitChunkLocations`, `ReleaseWriteLease`.

| RPC | Request | Response |
|---|---|---|
| `AllocateChunks` | `path`, `num_chunks`, `volume_id`, `preferred_tier`, `exclude_addrs`, `requester_location` (the FUSE client's full `[location]`), optional `defer_anchor` | `placements`: per chunk `chunk_index`, `primary_address`, `replica_addresses`, `tier`. Placement follows the file's own volume. `defer_anchor = true` from outside the anchor datacenter skips the forced anchor slot (FUSE `write.anchor_sync = false`). |
| `CommitChunkLocations` | `path`, `volume_id`, `lease_id`, `chunks` (`ChunkLocationCommit`: `chunk_index`, `chunk_id`, `primary_address`, `size_bytes`, `disk_id`) | `success`, `lease_conflict`, `error`. Rejected when a chunk landed on fewer distinct `(node_id, disk_id)` replicas than the volume's `write_safety.min_replicas_before_ack` (after clamping). |

`ChunkLocationCommit.disk_id` must be the `disk_id` returned by `PutChunkResponse`; the manager resolves the replica's tier from it.

### Leases and locks

| RPC | Request | Response |
|---|---|---|
| `AcquireWriteLease` | `path`, `owner_id`, `lease_ms` | `granted`, `lease_id`, `expires_at_ms`, `conflict_owner`, `conflict_expires_at_ms` |
| `RenewWriteLease` | `path`, `lease_id`, `extension_ms` | `granted`, `new_expires_at_ms` |
| `ReleaseWriteLease` | `path`, `lease_id` | `ok` |
| `ReleaseLeasesByHost` | `hostname`, `except_pid` | `released_count` (drops the host's already-expired leases; never one of `except_pid`, never a live one) |

### Node lifecycle

| RPC | Request | Response |
|---|---|---|
| `RegisterNode` | `hostname`, `role` (`data`, `client`), `grpc_port`, `location`, `disks`, `shared_secret` (optional), `version`, `build_date` | `accepted`, `node_id`, `config_version` |
| `Heartbeat` | `node_id`, `uptime_secs`, `total_chunks`, `total_bytes_used`, `config_version`, `chunk_reads` (per-chunk read counters by datacenter), `total_bytes_read`, `total_bytes_written`, `disk_usage` (`disk_id`, `total_bytes`, `used_bytes`), `integrity_faults`, `scrub_results` | `accepted`, `config_version`, `config_update`, `self_quarantine_cleared`, `scrub_request` |
| `ReportChunkStatus` | `node_id`, `chunks` (`chunk_id`, `size`, `status`: `healthy`, `corrupt`, `missing`) | `accepted` |
| `ForwardClientWrite` | `client_request_json`, `shared_secret` | `success`, `response_json`, `error`. Peer-manager use only: a follower forwards a Raft command to the leader. Only an allow-listed set of commands is accepted. |

A heartbeat whose liveness write cannot commit fails the RPC; `NOT_FOUND` tells the data node to re-register. `HeartbeatResponse.config_update.scrub_override` (`ScrubConfigOverride`) carries the cluster scrub policy from the manager's `[scrub]` block on every heartbeat: `quick_interval_hours`, `deep_interval_hours`, `quick_interval_secs`, `deep_interval_secs` (exact seconds, `0` from an older manager), `orphan_interval_secs`, `orphan_grace_secs`, `max_io_percent`, `verify_on_read`, `quarantine_on_fault`, `auto_unquarantine_secs`. `scrub_request` relays a manual scrub trigger (`id`, `level`, `requested_at_ms`); data nodes deduplicate by `id`. `config_update.config_json` is empty in current builds.

## DataService (`data.proto`)

Chunk I/O on data nodes. Chunks are streamed, never buffered whole; the BLAKE3 checksum is computed incrementally and verified on every read.

| RPC | Request | Response |
|---|---|---|
| `PutChunk` (client stream) | first message sets `chunk_id`, `volume_id`, `tier`; every message carries `data` | `chunk_id`, `size`, `checksum` (hex BLAKE3), `success`, `disk_id` |
| `GetChunk` (server stream) | `chunk_id`, `requester_dc`, `requester_host` | stream of `data`, `offset`, `total_size` |
| `DeleteChunk` | `chunk_id`, optional `disk_id`, `tier`, optional `if_mtime_le_ms` | `success`, `was_present`, `freed_bytes` |
| `ReplicateChunk` | `chunk_id`, `target_address`, `target_tier` | `success`, `error`, optional `disk_id` |
| `VerifyChunk` | `chunk_id`, optional `disk_id` | `valid`, `error` |
| `ListChunks` | `page_token`, `limit` | `chunk_ids`, `next_page_token`, `chunk_mtimes_ms`, `chunk_disk_ids` |
| `GetDiskStatus` | none | `hostname`, `disks` (`DiskInfo`) |
| `ReportStats` | none | `total_chunks`, `total_bytes`, `cache_hits`, `cache_misses` |
| `RecordCacheHit` | `chunk_id`, `requester_dc`, `requester_host` | empty |

Notes:

- `PutChunkResponse.disk_id` and `ReplicateChunkResponse.disk_id` report the disk the data actually landed on. Callers must pass the `PutChunk` value into `CommitChunkLocations`; for replication, an unset `disk_id` (older data node) means the manager must not guess among several disks.
- `DeleteChunkRequest.disk_id` limits the delete to that disk's copy. Replica identity is `(node_id, disk_id)`; unset means first-found (legacy).
- `DeleteChunkRequest.if_mtime_le_ms` is the orphan reaper's guard: the copy is deleted only if its `.dat` mtime (Unix ms) is at or below this value, checked under the chunk lock. A copy re-uploaded since the reaper listed it is skipped and the call returns `was_present = false`. Unset means an unconditional delete.
- `VerifyChunkRequest.disk_id` checks only that disk's `.dat` and `.meta` (an unknown disk is reported invalid); unset means first-found (legacy). The scheduler passes the disk the target reported at `ReplicateChunk`.
- `ListChunks` arrays `chunk_mtimes_ms` and `chunk_disk_ids` are index-aligned with `chunk_ids`; empty from an older node. The manager uses them to reap orphans per disk after a grace period.
- A `PutChunk` onto a disk below its free-space reserve, or while the node sheds load, fails with `RESOURCE_EXHAUSTED`; clients reroute or retry with backoff.
- A checksum mismatch on read returns `DATA_LOSS`; the FUSE client fails over to another replica.
- `ReplicateChunk` pushes the chunk to `target_address`, which receives it as a `PutChunk`; the call timeout scales with the chunk size.

Nodes join via `ManagerService.RegisterNode` ([Cluster Join](../security/cluster-join.md)). Admin operations (scrub, maintenance, audit log) use the [REST API](./rest-api.md); there is no admin gRPC service.

## Common types (`common.proto`)

| Message | Fields |
|---|---|
| `Location` | `region`, `datacenter`, `rack`, `host` |
| `DiskInfo` | `disk_id`, `uuid`, `mount_path`, `tier` (`hot`, `warm`, `cold`, `tape`), `total_bytes`, `used_bytes`, `status` (`healthy`, `degraded`, `draining`, `failed`, `removed`) |
| `NodeInfo` | `id`, `role` (`manager`, `data`, `client`), `hostname`, `grpc_port`, `location`, `disks`, `total_capacity`, `used_capacity`, `health_status` |
