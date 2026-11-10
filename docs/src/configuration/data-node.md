# Data Node Configuration

File: `/etc/dtfs/data.toml`. Source of truth: `crates/dtfs-common/src/config.rs` (`DataNodeConfig`). A complete, parse-tested example is `deploy/examples/data.toml`.

## Conventions

- **Scope**: every data-node key is `node` (per host). The cluster-wide scrub policy comes from the manager and overrides `[scrub]` at runtime (see below).
- **Reload**: no option is re-read while the process runs; change then restart ([Live Reload](./live-reload.md)).
- **Validated**: the process refuses to start when the check fails.
- Required tables: `[node]`, `[location]`, `[cluster]`, `[grpc]`, `[tls]`, `[scrub]` and at least one `[[disks]]`. `[tls]` and `[scrub]` may be empty. Optional: `[memory]`, `[placement]`, `[[tape_devices]]`, `[updater]` (CLI only).
- Unknown keys are ignored silently; check spelling against this page.
- Startup checks: `cluster.shared_secret` non-empty, `[memory]`, `[grpc]`, `[tls]` resolution, persistent disk ids.

## `[node]`

| Key | Type | Default | Constraint |
|---|---|---|---|
| `hostname` | string | required | Identity: `node_id = hash(hostname)`. Unique across every daemon. Must differ from a co-located manager's `[node].hostname` and from a co-located FUSE client's `[location].host`. |
| `id` | string | `"auto"` | Leave `auto`. |
| `data_dir` | path | `/var/lib/dtfs` | Node-local state. Chunk bytes live on the `[[disks]]` paths. |

## `[location]`

Required strings `region`, `datacenter`, `rack`, `host`. Use the same values as other daemons on the same box so reads prefer the local replica.

## `[cluster]`

| Key | Type | Default | Constraint |
|---|---|---|---|
| `manager_addr` | string | required | Primary manager gRPC `host:port` (registration and heartbeat). |
| `manager_addrs` | list of string | `[]` | Fallback managers, walked in order after repeated heartbeat failures. List every manager so a rolling manager restart never strands the node. |
| `shared_secret` | string | required | **Validated**: empty is refused unless `allow_insecure_empty_secret = true`. Same value as the managers. Used to register, to sign heartbeats and to verify the `x-dtfs-rpc-*` stamp on data-plane RPCs. |
| `allow_insecure_empty_secret` | bool | `false` | Lab only. With an empty secret, data-plane RPC authentication is skipped. |
| `require_delete_binding` | bool | `false` | `true`: a `DeleteChunk` stamp must cover `chunk_id\|disk_id\|if_mtime_le_ms`. `false`: a legacy `chunk_id`-only stamp from an older manager is also accepted, with a rate-limited warning. Set `true` once every manager is updated. No effect with an empty secret. |

## `[grpc]`

| Key | Type | Default | Constraint |
|---|---|---|---|
| `listen_addr` | string | required | Chunk-streaming gRPC. Use the routable address other nodes dial, not `0.0.0.0`, on a multi-interface host. |
| `max_message_size_mb` | u32 | `256` | **Validated**: > 0. |
| `max_concurrent_chunk_writes` | usize | `8` | In-flight `PutChunk` operations (floor 1, no upper clamp). Puts stream to disk, so each in-flight put holds about 1 MiB; the bound mainly protects disk and fsync queueing. Pair it with the FUSE clients' `write.upload_concurrency` (puts above it queue here, inside the client's 60 s put deadline), e.g. 64 for a slow-fsync NAS-backed disk. With `[memory] hard_cap_mb` set this value is ignored: slots = `(hard_cap_mb - 256) / 16` (clamped 1..=256), so 64 slots need `hard_cap_mb >= 1280`. A put whose sender sends no piece for 60 s fails with `DEADLINE_EXCEEDED` and frees its slot and tmp; the gRPC server also sends HTTP/2 keepalives (10 s, 20 s timeout) and TCP keepalive (30 s) to drop dead peers. Not configurable. |

## `[tls]`

Same keys and semantics as the manager (`mode`, `cert_file`, `key_file`, `ca_file`; default `auto`). **Validated**: unknown `mode`, `require` without all three files, or a partial `auto` configuration refuses startup. See [Manager `[tls]`](./manager.md#tls) and [TLS](../security/tls.md).

## `[[disks]]`

One table per local disk or mount point. At least one is required.

| Key | Type | Default | Constraint |
|---|---|---|---|
| `path` | path | required | Dedicated directory or mount point, writable by the daemon. |
| `tier` | string | required | `hot`, `warm`, `cold` or `tape` (lowercase). |

Disk ids are positions: the first `[[disks]]` entry is disk 0, the next disk 1, and so on. Replica identity is `(node_id, disk_id)`. The id is persisted in `<path>/.dtfs-disk-id` on first start, and **the daemon refuses to start** when a disk now sits at a different position. Therefore never remove or reorder entries in the middle of the list. Append new disks at the end; replace a failed or drained disk in place (same position, same path).

```toml
[[disks]]          # disk 0
path = "/mnt/hdd0"
tier = "cold"
[[disks]]          # disk 1
path = "/mnt/ssd0"
tier = "warm"
```

## `[cache]` (removed)

The data node has no server-side chunk cache (reads stream from disk). An old `[cache]` table (`max_memory_mb`, `policy`) is still accepted and ignored. Client-side caching is the FUSE `[cache]` disk cache.

## `[scrub]`

Local fallback used until the first heartbeat response arrives. From then on the manager's cluster `[scrub]` policy ([Manager `[scrub]`](./manager.md#scrub-optional)) overrides the intervals, I/O throttle, `verify_on_read` and `quarantine_on_fault` at runtime.

| Key | Type | Default | Notes |
|---|---|---|---|
| `quick_interval_hours` | u64 | `6` | Stat, size and `.meta` presence. |
| `deep_interval_hours` | u64 | `168` | Full BLAKE3 verify. |
| `max_io_percent` | u32 | `10` | Clamped to 1..=100 when applied. |
| `verify_on_read` | bool | `true` | Hash every read before bytes leave the node. |
| `quarantine_on_fault` | bool | `true` | `true`: a single integrity fault self-quarantines the node. `false`: faults are recorded and reported, the node does not self-quarantine. The manager applies the same switch on its side. |
| `quick_interval_secs`, `deep_interval_secs` | u64 | `0` | Internal: filled from the manager's heartbeat for sub-hour cadences. Do not set by hand. |

Quarantined chunks are moved to `.dtfs/quarantine/<id>.dat.bad`, never deleted.

Intervals count from the last finished pass, persisted per disk in `.dtfs/last_{quick,deep}_scrub_ms` (wall-clock ms), so a restart does not reset them; the oldest disk counts. A disk without the file starts its interval at first startup. An interrupted pass is not recorded and reruns from the beginning once due.

## `[memory]` (optional)

`hard_cap_mb = 0` (default, or block absent) disables the ladder. Set a non-zero cap on every production deploy. With a cap, the node sizes `PutChunk` concurrency to it and, at the shed level, rejects new `PutChunk` with `RESOURCE_EXHAUSTED` (clients back off and retry) instead of risking an OOM kill. On a composite host the sum of all roles' caps must stay below host RAM.

| Key | Type | Default | Constraint |
|---|---|---|---|
| `hard_cap_mb` | u64 | `0` | **Validated**: `0` or at least 128. |
| `warn_threshold_pct` | u8 | `80` | **Validated**: 1..=100 and below `shed_threshold_pct`. |
| `shed_threshold_pct` | u8 | `92` | **Validated**: at most 100. |
| `sample_interval_secs` | u64 | `5` | `0` is treated as 1. |

## `[placement]` (optional)

`PutChunk` rejects a write to a disk whose free space is below `max(min_free_bytes, total x min_free_pct%)` with a typed `RESOURCE_EXHAUSTED`, so the client reroutes instead of streaming into ENOSPC. Same keys as the manager's [`[placement]`](./manager.md#placement-optional) (`min_free_bytes` default `1073741824`, `min_free_pct` default `5.0`, `allow_capacity_spill` default `true`). The range check on `min_free_pct` runs on the manager only; use identical values on both.

## `[[tape_devices]]`

Parsed (`ltfs_path`, `device`, `library_device`, `label`, `capacity_gb`, `max_write_mbps`) but **reserved, not yet read** by the daemon. A tape tier is declared through a `[[disks]]` entry with `tier = "tape"` on a pre-mounted LTFS path. See [Tape](../storage/tape.md).

## `[updater]` (optional, CLI only)

Read by `dtfs-cli update data`; the daemon ignores it. Keys and precedence: [Manager `[updater]`](./manager.md#updater-optional-cli-only).
