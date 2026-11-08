# Caching

DTFS has no cache on the data node and two caches on the FUSE client. A third thing that is often called a cache, the cache replica, is a placement-layer concept and lives in [Tiering](./tiering.md).

| Layer | Where | What it holds | Bounded by |
|-------|-------|---------------|------------|
| Metadata cache | FUSE client | Chunk locations per path, 5 s TTL | 4096 entries, swept on the TTL cadence |
| Read-ahead memory cache | FUSE client | Prefetched and recently read chunks | `readahead.cache_mb` (default 64) |
| Local disk cache | FUSE client (optional) | Whole chunks on local disk | `cache.local_cache_max_gb` (default 50) |
| Cache replicas | Data nodes in other datacenters | Full replica copies | Per-volume policy, see [Tiering](./tiering.md) |

The data node keeps no chunk cache of its own. It streams from disk in pieces (one in memory at a time) and relies on the operating system page cache. Its memory is governed by the `[memory]` ladder (see the [Architecture Overview](../architecture/overview.md#memory-model)).

## Read-ahead memory cache

Holds prefetched chunks until the application consumes them. It is bounded by `readahead.cache_mb` and evicts the oldest-inserted entry first, draining entry by entry rather than clearing wholesale. Prefetch fan-out is limited by `readahead.prefetch_max_inflight` (default 2), and the candidate list is the next `prefetch_chunks` chunks after the one being read. Under memory pressure the ladder halves `cache_mb` at Warn and drops all unconsumed prefetch at Shed. See [Read-Ahead](../access/read-ahead.md).

## Local disk cache

```toml
[cache]                          # fuse.toml
local_cache_dir     = "/var/cache/dtfs"   # unset = disabled
local_cache_max_gb  = 50
max_age_days        = 30                  # optional
prune_interval_secs = 3600
```

- Entries are whole chunks named by chunk id. Because a chunk id is the hash of its content, a cached chunk can never be stale: if the file changes, it points at a different chunk id. There is no invalidation protocol and none is needed.
- Eviction runs when the cache exceeds the limit and trims to 90 percent of it (hysteresis). Entries older than `max_age_days` are pruned on the prune interval. All removals go through one path so the size accounting stays exact.
- A disk-cache hit is also inserted into the read-ahead memory cache, so a repeated read of the same chunk is served from RAM.
- Whether a read counts as "viewer" access (for the access-time hint used by eviction) is decided from the bytes read on the handle and whether read-ahead is active, not from one kernel read's length.

## Metadata cache

`GetFileLocations` results are cached per path for 5 seconds. The entry is dropped after a flush or unlink, and a post-commit refresh replaces it so reads on the same handle see new chunks. Expired entries are removed, never served stale. See [FUSE Mount](../access/fuse-mount.md).

## Monitoring

The client logs cache activity; there is no client Prometheus endpoint. Data node read counts that drive cache-replica promotion are visible through the tiering metrics in [Monitoring](../operations/monitoring.md).
