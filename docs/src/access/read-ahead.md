# Read-Ahead Modes

The DTFS FUSE client supports configurable read-ahead to prefetch chunk data before the application requests it, reducing perceived latency for sequential access patterns.

## Modes

The read-ahead engine operates in one of four modes, selected per mount or per directory:

### file-sequential

**Default mode.** The client detects sequential read patterns within a single file and prefetches subsequent chunks.

When the client observes two consecutive chunk reads from the same file in ascending order, it begins prefetching the next N chunks in the background. The prefetch depth starts at 1 and doubles with each confirmed sequential access, up to `max_prefetch_chunks`.

This mode is well suited for large file reads such as media playback, log processing, and data analytics where a single file is read from beginning to end.

```toml
[read_ahead]
mode = "file-sequential"
max_prefetch_chunks = 8
```

### folder-sequential

The client prefetches not only subsequent chunks of the current file but also the first chunks of the next files in the same directory, sorted by filename.

When a file read reaches the final chunk, the client begins prefetching the first chunk of the next file in directory order. This continues up to `max_prefetch_files` files ahead.

This mode is designed for workloads that process every file in a directory in order, such as batch image processing, sequential log ingestion, or training data pipelines.

```toml
[read_ahead]
mode = "folder-sequential"
max_prefetch_chunks = 8
max_prefetch_files = 4
```

### file-only

The client prefetches chunks within the current file only when the application issues an explicit `posix_fadvise(FADV_SEQUENTIAL)` or `readahead()` system call. No automatic detection is performed.

This mode gives the application full control over prefetching behavior. It is useful when the application knows its own access pattern and can issue advisory calls, or when automatic detection would waste bandwidth on random-access workloads.

```toml
[read_ahead]
mode = "file-only"
max_prefetch_chunks = 4
```

### disabled

Read-ahead is completely turned off. Every chunk is fetched on demand when the application reads it.

Use this mode for workloads with purely random access patterns where prefetching would waste network bandwidth and cache space.

```toml
[read_ahead]
mode = "disabled"
```

## Configuration

### Global Settings

Read-ahead is configured in the FUSE client configuration file:

```toml
[read_ahead]
# Mode selection: "file-sequential", "folder-sequential", "file-only", "disabled"
mode = "file-sequential"

# Maximum number of chunks to prefetch ahead of the read position
max_prefetch_chunks = 8

# Maximum number of files to prefetch ahead in folder-sequential mode
max_prefetch_files = 4

# Maximum memory for prefetch buffers
max_prefetch_buffer = "512 MiB"

# Prefetch priority relative to foreground reads (0.0-1.0)
# Lower values give more bandwidth to foreground reads
prefetch_priority = 0.5
```

### Per-Directory Overrides

Different directories can use different read-ahead modes via extended attributes:

```bash
# Set a directory to use folder-sequential mode
dtfs-cli read-ahead set /mnt/dtfs/training-data --mode folder-sequential

# Disable read-ahead for a random-access directory
dtfs-cli read-ahead set /mnt/dtfs/database --mode disabled

# View current read-ahead mode for a path
dtfs-cli read-ahead get /mnt/dtfs/training-data
```

Per-directory settings override the global mode. They are stored as metadata in the manager and persist across client restarts.

## How Prefetching Works

The read-ahead engine maintains a prefetch queue per open file. When a prefetch decision is made:

1. The engine enqueues chunk IDs into the prefetch queue.
2. Background workers dequeue chunks and fetch them from data nodes using the same location-aware selection as foreground reads.
3. Fetched chunks are placed in the local disk cache (if enabled) or held in the prefetch memory buffer.
4. When the application reads a prefetched chunk, it is served from memory or local cache with no network round trip.

If the application's access pattern changes (for example, it seeks to a distant position), the prefetch queue is flushed and detection restarts. This avoids wasting bandwidth on speculative fetches that are no longer relevant.

### Prefetch Buffer Pressure

If the prefetch buffer reaches `max_prefetch_buffer`, new prefetch requests are dropped until buffer space is freed by application reads consuming buffered data. Foreground reads are never blocked by prefetch operations.

## Use Cases

| Workload | Recommended Mode | Rationale |
|----------|-----------------|-----------|
| Video/audio streaming | file-sequential | Single large files read start to end |
| Log processing | file-sequential | Sequential reads within individual log files |
| Batch image processing | folder-sequential | Directory of images processed in order |
| ML training data loading | folder-sequential | Iterating over datasets stored as files in a directory |
| Database storage engine | disabled or file-only | Random access patterns, prefetching wastes I/O |
| Interactive file browsing | disabled | Unpredictable access patterns |
| Scientific simulation checkpoints | file-only | Application knows when it will read sequentially |

## Monitoring

Read-ahead metrics are exported by the FUSE client:

- `dtfs_readahead_prefetch_total` -- total chunks prefetched.
- `dtfs_readahead_prefetch_hit_total` -- prefetched chunks that were actually read by the application.
- `dtfs_readahead_prefetch_waste_total` -- prefetched chunks evicted before being read.
- `dtfs_readahead_hit_ratio` -- ratio of hits to total prefetches (higher is better).
- `dtfs_readahead_buffer_bytes` -- current prefetch buffer usage.
