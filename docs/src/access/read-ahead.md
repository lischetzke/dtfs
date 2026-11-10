# Read-Ahead

The FUSE client prefetches the chunks that follow the one being read, so sequential playback finds its next chunk already in memory.

## Configuration

```toml
[readahead]                      # fuse.toml
mode                   = "folder-sequential"   # default
prefetch_chunks        = 8
prefetch_next_files    = 3
cache_mb               = 64
prefetch_max_inflight  = 2
```

| Key | Default | Meaning |
|-----|---------|---------|
| `mode` | `folder-sequential` | `file-sequential`, `folder-sequential`, `file-only` or `disabled`. An unknown value falls back to `folder-sequential` with a warning |
| `prefetch_chunks` | 8 | Chunks fetched ahead of the read position (`disabled` makes it 0) |
| `prefetch_next_files` | 3 | Parsed for `folder-sequential`; see the note below |
| `cache_mb` | 64 | Byte budget of the prefetch memory cache |
| `prefetch_max_inflight` | 2 | Prefetch `GetChunk` streams on the wire at once |

> **All three non-disabled modes currently behave the same.** After each chunk read the engine prefetches the next `prefetch_chunks` chunks of the same file. Cross-file prefetch for `folder-sequential` (`prefetch_next_files`) and any `fadvise`-driven behaviour for `file-only` are not implemented. There are no per-directory overrides and no read-ahead CLI. Cross-file prefetch is done by the manager instead: the volume's [pre-caching](../storage/tiering.md#pre-caching) copies the next episodes or files into the reader's datacenter cache tier.

## How it works

1. After every chunk transition (hit or miss) the engine computes the window `chunk_idx+1 ..= chunk_idx+prefetch_chunks`, capped at the end of the file, and skips chunks already cached or in flight.
2. Each missing chunk becomes a background fetch. Fetches are admitted in ascending chunk order through a semaphore of `prefetch_max_inflight` permits, so the nearest chunk is always on the wire first. The replica is chosen with the same ranking as foreground reads, and each fetch is bounded by `client.chunk_read_timeout_secs`.
3. A foreground read that misses first waits briefly (up to 2 s) for an in-flight prefetch of the same first candidate chunk, but never for one that has not yet obtained a slot; otherwise it fetches itself. Concurrent foreground misses for one chunk are single-flighted.
4. Fetched bytes are verified against the chunk id before they are cached, like any other read.
5. Cached chunks are consumed by reads. The cache evicts oldest-inserted first to stay under `cache_mb` and drains entry by entry. Keep `cache_mb` at least `(prefetch_chunks + prefetch_max_inflight) * chunk_size_mb` or the window evicts itself.

There is deliberately no "cache is full, stop prefetching" gate: in-flight bytes are bounded by the semaphore and stored bytes by the eviction budget.

## Memory pressure

With `[memory]` configured: at Warn, `cache_mb` is halved; at Shed, unconsumed prefetch is dropped and prefetching is switched off until pressure subsides. Foreground reads are never blocked by prefetch.

## Choosing a mode

| Workload | Mode |
|----------|------|
| Plex playback, large sequential files | any of the three (identical today); default is fine |
| Random access (databases, indexers) | `disabled` |

## Monitoring

There are no read-ahead metrics. Prefetch activity is visible at `RUST_LOG=dtfs_fuse=debug` ("readahead triggered").
