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
| `chunk_size_mb` | u32 | `4` | Chunk size of the write buffer, 1 to 64 (startup refuses other values). Must equal the chunk size of the volume being written. A write to a file whose chunk size differs is refused with an error that names this key. |
| `volume_id` | string | empty | Volume in which this mount creates new files and directories. Empty sends none and the manager uses `default`. Only `A-Z a-z 0-9 . _ -` (startup refuses others). A volume the manager does not know fails the create with `NotFound` only on managers newer than v0.1.13 (the release that added this key or later); managers at v0.1.13 or older create the file in the unknown volume with the default chunk size, so upgrade the managers first. Pair with `chunk_size_mb` set to that volume's chunk size; files of another chunk size are refused for writes through this mount. |
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
| `first_write_tier` | string | `"cold"` | `cold`: no tier hint, the manager uses the volume's own policy. `nearest-warm`: asks for the warm tier; honoured when a healthy warm disk exists, replica count clamped up to `write_safety.min_replicas_before_ack`. No datacenter affinity, and the volume anchor still forces one replica into the anchor datacenter (unless `anchor_sync = false`). Unknown values act as `cold`. See [FUSE mount](../access/fuse-mount.md#writes). |
| `flush_batch_chunks` | u32 | `4` | Complete chunks held back before one `AllocateChunks` plus `CommitChunkLocations` round trip. Clamped to 1..=64. It sets the batch byte limit, `max(64 MB, batch x 4 MiB)` capped at 256 MB, counted against a 4 MiB reference chunk so a large-chunk volume with the default batch keeps 64 MB (fewer chunks per batch), and the per-file dirty cap, `max(128 MB, 2 x batch limit)`: the default 4 x 4 MiB gives 64 MB / 128 MB, 16 x 4 MiB the same, 64 x 4 MiB 256 MB / 512 MB. `1` flushes per chunk. `flush`/close always drains held chunks. Memory: the dirty cap applies per open writer, so `dirty cap x open writers + readahead.cache_mb` must fit the client's `[memory]` budget (64 x 4 MiB with two files written at once: 1 GB of write buffer). |
| `upload_concurrency` | u32 | `2` | Chunks of one flush batch put concurrently, each to all its targets. Clamped to 1..=64; a batch holds `flush_batch_chunks` chunks, so a value above it gains nothing for a batch flush (raise both together); `1` puts one chunk at a time. Values above 8 only help a data disk with slow fsync (for example a NAS-backed virtual disk, where one chunk's fsync takes about 200 ms): raise the data node's `grpc.max_concurrent_chunk_writes` (default 8) to match, or the extra puts queue there. Memory: each in-flight put copies about 3 MiB per target (two queued 1 MiB pieces plus the one being sent), so about `upload_concurrency x replicas x 3 MiB` per flushing file (default 2 x 3 x 3 = 18 MiB worst case), on top of the batch itself. Every put has a fixed 60 s RPC deadline that includes queueing on the data node; on a slow shared WAN link keep this low (1 or 2), since many files flushing at once can push puts past it and fail the flush with `EIO`. |
| `anchor_sync` | bool | `true` | `true`: allocation forces one target into the volume's anchor datacenter, so every write uploads to it before the ack. `false`: `AllocateChunks` sends `defer_anchor`; the manager skips the hash ring and places each chunk on `min_replicas_before_ack` distinct healthy nodes with room on any maintained tier (mandatory count above 0) outside the anchor datacenter, whatever the volume's write tier or `first_write_tier`, ranked fastest tier first and then nearest. Anchor-datacenter nodes are used only for slots the other nodes cannot fill, so the floor is never lowered. The scheduler's repair then copies the anchor replica, and any further mandatory copies, data node to data node. During that window the chunk has at least floor copies but no anchor copy ([Anchor pending](../glossary.md)). No effect for a client inside the anchor datacenter; managers without the field keep forcing the anchor. See [Replication](../storage/replication.md#anchor-invariant). |
| `async_commit` | bool | `false` | `false`: `fsync` and every `close(2)` commit all buffered bytes, including a partial tail chunk. `true`: a write that completes a batch of `flush_batch_chunks` chunks hands it to a background upload (up to `max_inflight_batches` per file at a time, default 1; buffered plus uploading bytes count against the per-file dirty cap (128 MB by default, see `flush_batch_chunks`), and a write that would reach it waits for the upload) and returns without waiting; `fsync` commits nothing, does not wait for background uploads (knfsd sends one per NFS COMMIT) and reports a pending background upload error once; a partial chunk is committed when it fills, when its oldest buffered byte is `async_commit_secs` old (checked once per second), when the per-file dirty cap would be exceeded, or at the last writer's `close(2)` (that close commits everything and returns the commit error; a close while another writable handle of the file stays open in this `dtfs-fuse` commits nothing, as knfsd does for every NFS COMMIT). **Risk: an acknowledged write (even an fsynced one) can live only in client memory until the file is closed, for up to `async_commit_secs` (or up to the dirty cap, 128 MB by default, per open file), and is lost if `dtfs-fuse` or the host crashes.** A failed background commit keeps the bytes for retry and makes the next `fsync`/`close` on that file return `EIO` once. Recommended only for media and [NFS re-export](../access/fuse-mount.md#nfs-re-export), never for databases. Logged at warn on startup. |
| `async_commit_secs` | u64 | `30` | Max age of an uncommitted buffered byte with `async_commit = true`. Must be in 1..=60 when `async_commit = true` (startup refuses otherwise): at most half the 120 s write lease, which renews only on a write or a commit. Ignored when off. |
| `max_inflight_batches` | u32 | `1` | `async_commit` only (ignored when off): background batches of one file that upload at the same time. Clamped to 1..=16; `1` keeps one batch per file. With one batch, every batch end waits for its slowest put and the put pipeline drains to zero; with more, the next batch starts while earlier ones finish. Puts in flight per file: up to `max_inflight_batches x min(upload_concurrency, flush_batch_chunks)`, each holding about 3 MiB of put buffers per target (two queued 1 MiB pieces plus the one being sent). These put buffers sit outside the dirty cap and outside the `[memory]` ladder: `max_inflight_batches x min(upload_concurrency, flush_batch_chunks) x replicas x 3 MiB` per writing file, e.g. 4 x 16 puts x 3 targets x 3 MiB = 576 MiB (192 MiB on a 1-replica volume). Batches of one file never share a chunk (a write into an uploading chunk waits for its batch), so their commits merge into the open handles without a full location re-fetch. The uploads overlap, but the batches of one file commit in the order they started: a batch whose puts finish first waits until every earlier batch has committed. If an earlier batch fails, every later batch of the file still in flight is not committed either: its bytes go back to the buffer, its chunks count as failed (`EIO`, see below) and the next flush or close retries them in order, so other clients never see a later in-flight batch committed over the gap a failed batch left. Such a batch stops uploading (no further puts) and no new batch of the file starts until it ended, so the restored bytes go out once, in one batch. It raises the per-file dirty cap (buffered plus uploading bytes) to `max(128 MB, (n + 1) x batch limit)`: 16 x 4 MiB batches with `n = 4` give 320 MB per open writer; size `[memory]` for it. A failed batch keeps its bytes for retry and its `EIO` stays pending until those chunks commit, even if a later batch succeeds. |

## Tuning recommendations

Defaults suit a LAN client with one or two readers. Two cases worth changing (chunk size 4 MiB assumed):

- **WAN writers** (client far from its data nodes, e.g. a high-RTT link to LELI): `write.flush_batch_chunks = 16`. Each batch costs one `AllocateChunks` plus one `CommitChunkLocations` round trip to the manager, so 16 cuts those round trips four-fold versus the default; 16 x 4 MiB is exactly the 64 MB batch byte floor. Cost: up to 64 MB of complete chunks held per open writer before a flush (default: 16 MB; the 128 MB per-file dirty cap is unchanged). Puts inside the batch stay bounded by `upload_concurrency`; leave that at its default on a slow link.
- **Data disk with slow fsync** (NAS-backed virtual disk, about 200 ms per 4 MiB put; throughput scales with puts in flight), typically with `async_commit` for an NFS re-export: `write.flush_batch_chunks = 64` and `write.upload_concurrency = 64`, and on the data node `grpc.max_concurrent_chunk_writes = 64` (see [data node](data-node.md)). Cost: 256 MB batch and 512 MB dirty cap per open writer, plus up to 64 x replicas x 3 MiB of put buffers while it flushes; size `[memory]` for dirty cap x concurrent writers + readahead. With one batch per file the puts in flight drop to zero at every batch end (the batch ends with its slowest put); to keep about 64 puts busy for one file use `write.flush_batch_chunks = 16`, `write.upload_concurrency = 16` and `write.max_inflight_batches = 4` (64 MB batches, 320 MB dirty cap per open writer, plus 64 puts x replicas x 3 MiB of put buffers outside the dirty cap and `[memory]`: about 576 MiB with 3 targets, 192 MiB on a 1-replica volume).
- **Clients serving about 5 parallel streams** (a Plex host): `readahead.cache_mb = 192` and `readahead.prefetch_max_inflight = 4`. With the default `prefetch_chunks = 8`, five streams want 5 x 8 x 4 MiB = 160 MiB of prefetched chunks, which the 64 MB default evicts before they are read. Cost: about 128 MB more client RAM than the default (the cache budget), plus two more 4 MiB in-flight prefetch buffers; with a `[memory]` cap the warn level still halves it. Size `hard_cap_mb` to include it.

**Upgrade note (from v0.1.13 or older):** those clients clamped `upload_concurrency` to 16 and the batch to 16 chunks (64 MB with 4 MiB chunks). A config that already sets `flush_batch_chunks` in 17..=64 or `upload_concurrency` above 16 gets larger batches (up to 256 MB), a larger dirty cap (up to 512 MB per open writer) and more puts in flight (each about 3 MiB per target) after the upgrade, without any config change. Check `[memory]` against the new numbers before upgrading such a client.

## `[partition_safety]`

Parsed but **not read** by the client in this build; partition behaviour is not configurable here. See [Partition Behavior](../access/partition-behavior.md). `quorum_loss_timeout_secs` is still range-checked.

| Key | Type | Default | Constraint |
|---|---|---|---|
| `quorum_loss_timeout_secs` | u64 | `10` | **Validated**: > 0. Not otherwise read at runtime. |

The former `mode` and `show_warning` keys were removed (never read); configs that still set them keep loading.

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
