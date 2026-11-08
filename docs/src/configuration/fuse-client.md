# FUSE Client Configuration

File: `/etc/dtfs/fuse.toml`. Source of truth: `crates/dtfs-common/src/config.rs` (`FuseConfig`). A complete, parse-tested example is `deploy/examples/fuse.toml`.

## Conventions

- **Scope**: every client key is `node` (per host).
- **Reload**: no option is re-read while the process runs; change then restart the unit ([Live Reload](./live-reload.md)).
- **Validated**: the process refuses to start when the check fails. Under systemd a failing start becomes a restart loop; after editing, check `journalctl -u dtfs-fuse@<mount> -n 20`.
- Required tables: `[client]`, `[location]`, `[mount]`, `[readahead]`, `[cache]`, `[partition_safety]`. They may be empty when every key has a default, except `[client]` (`manager_addr`), `[location]` and `[mount]` (`mountpoint`). Optional: `[write]`, `[memory]`, `[tls]`, `[updater]` (CLI only).
- Unknown keys are ignored silently; check spelling against this page.
- Startup checks: `[memory]`, `client.max_message_size_mb`, `cache.prune_interval_secs`, `partition_safety.quorum_loss_timeout_secs`, `[tls]` resolution.

## Top-level key

Must precede the first `[table]` header.

| Key | Type | Default | Notes |
|---|---|---|---|
| `read_location_priority` | list of string | `[]` | Ordered datacenter or host names (case-insensitive). A read picks the first replica whose datacenter or host matches the earliest entry, ahead of physical distance. No match falls through to distance, then to a faster-tier tie-break at equal distance. Empty: distance only. |

Replica selection order: (1) `read_location_priority`; (2) physical distance (`SameHost` < `SameRack` < `SameDatacenter` < `SameRegion` < `CrossRegion`); (3) tier (warm before cold) only at equal distance.

```toml
read_location_priority = ["dc1", "dc3"]
```

## `[client]`

| Key | Type | Default | Constraint |
|---|---|---|---|
| `manager_addr` | string | required | Primary manager gRPC `host:port`. |
| `manager_addrs` | list of string | `[]` | Fallback managers, walked in order. `manager_addr` plus `manager_addrs` must include at least one Raft **voter**: reads work against a learner, but client mutations are not leader-forwarded, so writes fail with EIO otherwise. |
| `shared_secret` | string | `""` | Cluster secret. Needed to self-register (the client then appears in the node table) and to sign mutating manager RPCs. Empty is accepted by the client, but a manager with a secret rejects the client's mutations. |
| `heartbeat_interval_secs` | u64 | `10` | Keepalive re-registration cadence. Keep well below the manager's `raft.node_liveness_secs` (30) or the client flaps `Down`. `0` registers once only. |
| `chunk_size_mb` | u32 | `4` | Chunk size of the write buffer. Must equal the chunk size of the volume being written. A write to a file whose chunk size differs is refused with an error that names this key. |
| `chunk_read_timeout_secs` | u64 | `60` | Per-replica chunk-fetch timeout; on expiry the read fails over to the next replica. `0` disables the timeout (a hung node then stalls the read). Raise for very large chunks on very slow links. |
| `max_message_size_mb` | u32 | `256` | **Validated**: > 0. Must be at least the managers' and data nodes' `grpc.max_message_size_mb`. |

## `[location]`

| Key | Type | Default | Constraint |
|---|---|---|---|
| `region`, `datacenter`, `rack` | string | required | Real topology of the client; drives nearest-replica routing. |
| `host` | string | required | Also the client's **registry identity** (the FUSE client has no `[node].hostname`). Unique cluster-wide: never reuse a data node's or manager's hostname. Convention for a client on a data-node box: `<data-hostname>_cln`. |

The `_cln` suffix loses the `SameHost` tie-break; give each physical box its own `rack` so `SameRack` keeps reads local.

## `[mount]`

| Key | Type | Default | Notes |
|---|---|---|---|
| `mountpoint` | path | required | Must exist. Can be overridden on the command line. |
| `allow_other` | bool | `false` | Mount with FUSE `allow_other` so other UIDs (for example Plex) can use the mount. May require `user_allow_other` in `/etc/fuse.conf`. |

## `[readahead]`

| Key | Type | Default | Notes |
|---|---|---|---|
| `mode` | string | `"folder-sequential"` | `disabled`, `file-only`, `file-sequential`, `folder-sequential` (also prefetches the next files in the directory). An unknown value falls back to `folder-sequential` with a warning. |
| `prefetch_chunks` | u32 | `8` | Chunks prefetched ahead on sequential access. `0` disables chunk prefetch. |
| `prefetch_next_files` | u32 | `3` | Files prefetched ahead (`folder-sequential` only). |
| `cache_mb` | u64 | `64` | Byte budget of the in-memory prefetch cache; evicts to stay under it regardless of chunk size. |
| `prefetch_max_inflight` | u32 | `2` | Concurrent prefetch `GetChunk` streams (`0` acts as 1). Keep `cache_mb >= (prefetch_chunks + prefetch_max_inflight) x chunk_size_mb`. |

See [Read-Ahead](../access/read-ahead.md).

## `[cache]`

On-disk chunk cache on the client.

| Key | Type | Default | Constraint |
|---|---|---|---|
| `local_cache_dir` | path | unset | Unset disables the disk cache. |
| `local_cache_max_gb` | u64 | `50` | Eviction starts above the limit and stops at 90% of it. `0` = zero budget. |
| `max_age_days` | u32 | unset | Evicts chunks not accessed for this long. Unset: size-based eviction only. |
| `prune_interval_secs` | u64 | `3600` | **Validated**: > 0. |

## `[write]` (optional)

| Key | Type | Default | Notes |
|---|---|---|---|
| `first_write_tier` | string | `"cold"` | `cold`: no tier hint, the manager uses the volume's own policy. `nearest-warm`: asks for the warm tier; honoured when a healthy warm disk exists, replica count clamped up to `write_safety.min_replicas_before_ack`. No datacenter affinity, and the volume anchor still forces one replica into the anchor datacenter. Unknown values act as `cold`. See [FUSE mount](../access/fuse-mount.md#writes). |
| `flush_batch_chunks` | u32 | `4` | Complete chunks held back before one `AllocateChunks` plus `CommitChunkLocations` round trip. Clamped to 1..=32 and further so that `batch x chunk_size_mb <= 64`. `1` flushes per chunk. `flush`/close always drains held chunks. |

## `[partition_safety]`

Parsed but **not read** by the client in this build; partition behaviour is not configurable here. See [Partition Behavior](../access/partition-behavior.md). `quorum_loss_timeout_secs` is still range-checked.

| Key | Type | Default | Constraint |
|---|---|---|---|
| `mode` | string | `"strict"` | `strict`, `localreadonly`, `fulllock` (lowercase, no hyphens). Not read. |
| `quorum_loss_timeout_secs` | u64 | `10` | **Validated**: > 0. Not otherwise read. |
| `show_warning` | bool | `true` | Not read. |

## `[tls]` (optional)

Same keys and semantics as the manager (`mode`, `cert_file`, `key_file`, `ca_file`; default `auto`), for connections to managers and data nodes. `cert_file`/`key_file` are the client certificate and key. **Validated** as on the manager. See [Manager `[tls]`](./manager.md#tls) and [TLS](../security/tls.md).

## `[memory]` (optional)

`hard_cap_mb = 0` (default, or block absent) disables the ladder. With a cap: at the warn level the `[readahead] cache_mb` budget is halved; at the shed level all unconsumed prefetch is dropped (the disk cache survives). On a composite host the sum of all roles' caps must stay below host RAM.

| Key | Type | Default | Constraint |
|---|---|---|---|
| `hard_cap_mb` | u64 | `0` | **Validated**: `0` or at least 128. |
| `warn_threshold_pct` | u8 | `80` | **Validated**: 1..=100 and below `shed_threshold_pct`. |
| `shed_threshold_pct` | u8 | `92` | **Validated**: at most 100. |
| `sample_interval_secs` | u64 | `5` | `0` is treated as 1. |

## `[updater]` (optional, CLI only)

Read by `dtfs-cli update fuse` (alias `client`); the daemon ignores it. Keys and precedence: [Manager `[updater]`](./manager.md#updater-optional-cli-only).
