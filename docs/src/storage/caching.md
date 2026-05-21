# Caching

DTFS uses two layers of caching to reduce read latency and network traffic: an in-memory cache on each data node and an optional local disk cache on each FUSE client.

> **Not on this page:** the volume-policy cache layer (Bug AV
> `cache_replicas` — opportunistic full-replica copies on warm/hot
> disks, created on read demand). That is a different system — it
> lives at the *placement* layer (which nodes hold replicas of a
> chunk), not the *data-node* layer (whose bytes are kept hot in
> RAM). See [Tiering → Cache tier semantics (Bug AV)](./tiering.md#cache-tier-semantics-bug-av)
> and [Volume Configuration → Replication classes](../configuration/volume-config.md#replication-classes).

## Data Node ARC Cache

Each data node maintains an in-memory Adaptive Replacement Cache (ARC) that stores recently and frequently accessed chunk data. ARC automatically balances between recency and frequency, adapting to workload patterns without manual tuning.

### How ARC Works

ARC maintains four internal lists:

- **T1**: pages seen recently (recency-biased).
- **T2**: pages seen at least twice recently (frequency-biased).
- **B1**: ghost entries evicted from T1 (metadata only, no data).
- **B2**: ghost entries evicted from T2 (metadata only, no data).

When a cache miss hits a ghost entry in B1, ARC increases the size of T1 (favoring recency). When a miss hits B2, ARC increases T2 (favoring frequency). This self-tuning behavior makes ARC effective for mixed workloads without requiring operators to choose between LRU and LFU policies.

### Configuration

```toml
[cache.arc]
# Maximum memory for the ARC cache. Accepts human-readable sizes.
max_size = "4 GiB"

# Minimum chunk size eligible for caching. Chunks smaller than this
# are always cached; larger chunks are cached only if space permits.
min_chunk_size = "1 MiB"

# Whether to cache chunks on write path (in addition to read path)
cache_on_write = false
```

The `max_size` should be set based on available system memory. A reasonable starting point is 25-50% of free memory after accounting for the operating system and other processes.

### Cache Bypass

Certain operations bypass the ARC cache:

- **Scrub reads**: deep scrub reads go directly to disk to verify on-disk integrity.
- **Migration reads**: reads for chunk migration skip the cache to avoid polluting it with data that is being moved away.
- **Large sequential scans**: when a client issues a sequential read spanning more than `scan_threshold` chunks (default: 32), subsequent reads in the sequence bypass the cache to prevent scan pollution.

```toml
[cache.arc]
scan_threshold = 32
```

## FUSE Client Local Disk Cache

The `dtfs-fuse` client can maintain a local disk cache that stores copies of recently accessed chunk data on the client machine. This reduces round trips to data nodes for repeated reads of the same data.

### Configuration

```toml
[cache.local]
enabled = true

# Path to the local cache directory. Should be on a fast local disk.
path = "/var/cache/dtfs"

# Maximum size of the local disk cache.
max_size = "50 GiB"

# Cache entry TTL. Entries older than this are eligible for eviction
# even if the cache is not full.
ttl_secs = 3600
```

### LRU Eviction

The local disk cache uses an LRU (Least Recently Used) eviction policy. When the cache reaches `max_size`, the least recently accessed entries are evicted to make room for new data. Eviction proceeds until the cache is at 90% of `max_size` to provide a buffer before the next eviction cycle.

Cache entries are stored as individual files named by chunk ID. A lightweight index file tracks access timestamps and is persisted periodically to survive client restarts.

### Cache Coherence

The local disk cache must stay consistent with the authoritative data on the data nodes. DTFS uses the following mechanisms:

- **Version stamps**: each chunk has a monotonically increasing version number. When the client reads a chunk from cache, it sends the cached version to the data node in a conditional request. If the version matches, the data node responds with a short "not modified" message. If the version has changed, the full chunk data is returned and the cache is updated.
- **TTL expiration**: cached entries expire after `ttl_secs`. Expired entries are re-validated on next access using the version stamp mechanism.
- **Push invalidation**: when a chunk is written, the manager broadcasts an invalidation notice to all FUSE clients that have recently accessed that chunk. Clients that receive the notice evict the affected cache entry immediately.

```toml
[cache.local]
# Enable push invalidation. Requires a persistent connection to the manager.
push_invalidation = true
```

### Write-Through Behavior

The local cache operates in write-through mode. Writes are always sent to the data nodes and are never served from the local cache. After a successful write, the local cache entry for the affected chunk is invalidated. This ensures that the cache never contains stale write data.

## Cache Warm-Up

When a FUSE client starts with a cold cache, initial reads incur full network round trips. DTFS supports optional cache warm-up to pre-populate the local cache:

```toml
[cache.local]
warmup_paths = ["/data/models", "/data/config"]
warmup_depth = 2    # directory traversal depth
```

During warm-up, the client issues background reads for chunks in the specified paths. Warm-up reads run at low priority and do not block foreground I/O.

## Monitoring

Cache metrics are exported by both data nodes and FUSE clients:

**Data node ARC metrics:**

- `dtfs_arc_hit_total` -- cache hits.
- `dtfs_arc_miss_total` -- cache misses.
- `dtfs_arc_eviction_total` -- entries evicted.
- `dtfs_arc_size_bytes` -- current cache size in bytes.

**FUSE client local cache metrics:**

- `dtfs_local_cache_hit_total` -- local cache hits.
- `dtfs_local_cache_miss_total` -- local cache misses.
- `dtfs_local_cache_size_bytes` -- current local cache size.
- `dtfs_local_cache_invalidation_total{source="ttl|push|write"}` -- invalidations by source.
