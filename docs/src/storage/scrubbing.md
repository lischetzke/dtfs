# Scrubbing

DTFS data nodes run periodic scrub jobs to detect data corruption, bit rot, and metadata inconsistencies. Corrupt chunks are automatically flagged for re-replication from a healthy replica, ensuring that silent data corruption does not go unnoticed.

## Scrub Types

DTFS supports two levels of integrity verification, each with different performance characteristics.

### Quick Scrub

A quick scrub performs lightweight checks without reading full chunk data:

- **Existence**: verifies that the chunk file exists on disk at the expected path.
- **Size**: confirms that the file size matches the size recorded in the metadata.
- **Modification time (mtime)**: checks that the file's mtime has not changed unexpectedly since the last known write.

Quick scrubs are fast because they only require `stat()` system calls, not full reads. They catch common failure modes such as accidental deletion, truncation, and filesystem corruption that alters file metadata.

**Default interval**: every 6 hours.

### Deep Scrub

A deep scrub reads the full content of every chunk and recomputes its BLAKE3 checksum:

- **Full read**: the entire chunk (default 128 MB) is read sequentially from disk.
- **Checksum verification**: a BLAKE3 hash is computed over the read data and compared to the checksum stored in the metadata.
- **Header validation**: chunk headers (version, flags, compression type) are validated against the expected format.

Deep scrubs detect bit rot, silent disk corruption, and any form of data modification that leaves file size and mtime intact. They are I/O-intensive and are therefore run less frequently than quick scrubs.

**Default interval**: every 7 days (weekly).

## Configuration

Scrub intervals and behavior are configured in the data node's configuration file:

```toml
[scrubbing]
quick_interval_secs = 21600     # 6 hours
deep_interval_secs  = 604800    # 7 days

# Throttle scrub I/O to limit impact on production reads
max_scrub_bandwidth_mbps = 100

# Maximum number of chunks to scrub concurrently
max_concurrent_scrubs = 2

# Time window during which deep scrubs are allowed to run
# (24-hour format, local time). Outside this window, deep scrubs
# are paused and resume when the window reopens.
deep_scrub_window_start = "02:00"
deep_scrub_window_end   = "06:00"
```

### Bandwidth Throttling

The `max_scrub_bandwidth_mbps` setting limits the disk read rate for scrub operations. This prevents scrubbing from competing with production workloads for disk I/O bandwidth. The limit applies to the aggregate scrub throughput across all concurrent scrub threads on the node.

### Scheduling Window

For deep scrubs, operators can define a time window during which scrubbing is permitted. This is useful for clusters with predictable load patterns where off-peak hours provide spare I/O capacity. Outside the window, any in-progress deep scrub is paused (not cancelled) and resumes when the window reopens.

If the deep scrub cannot complete within its window over multiple cycles, the data node logs a warning and the manager raises an alert. In this case, consider widening the window or increasing `max_scrub_bandwidth_mbps`.

## Scrub Lifecycle

Each scrub run proceeds through the following stages:

1. **Enumerate**: the data node lists all chunks on the target disk.
2. **Check**: each chunk is verified according to the scrub type (quick or deep).
3. **Report**: results are sent to the manager in the next heartbeat. Healthy chunks update their last-scrub timestamp. Failed chunks are flagged.
4. **Repair**: the manager schedules re-replication for any chunk that failed verification, using a healthy replica as the source.

If no healthy replica exists (all copies are corrupt), the manager raises a critical alert. DTFS cannot repair chunks that have no healthy copy.

## Manual Triggers

### CLI

Operators can trigger a scrub on demand using the CLI:

```bash
# Quick scrub a specific disk
dtfs-cli scrub quick --node data-01 --disk /mnt/ssd0

# Deep scrub all disks on a node
dtfs-cli scrub deep --node data-01

# Deep scrub a specific chunk by ID
dtfs-cli scrub deep --chunk abc123def456

# Check scrub status
dtfs-cli scrub status --node data-01
```

### REST API

The same operations are available through the manager's REST API:

```
POST /api/v1/scrub
{
  "type": "deep",
  "target": {
    "node": "data-01",
    "disk": "/mnt/ssd0"
  }
}

GET /api/v1/scrub/status?node=data-01
```

Manual scrubs bypass the scheduling window and start immediately. They respect the bandwidth and concurrency limits.

## Corrupt Chunk Handling

When a scrub detects a corrupt chunk, the following sequence occurs:

1. The data node marks the local replica as `CORRUPT` and stops serving reads from it.
2. The data node reports the corruption to the manager on the next heartbeat.
3. The manager redirects any reads for the affected chunk to a healthy replica.
4. The manager schedules re-replication to restore the target replication factor.
5. Once the new replica is verified, the corrupt copy is deleted.

Clients experience no errors as long as at least one healthy replica exists. Read requests are transparently rerouted.

## Monitoring

Scrub-related metrics are exported by each data node:

- `dtfs_scrub_quick_last_completed_timestamp` -- Unix timestamp of the last completed quick scrub.
- `dtfs_scrub_deep_last_completed_timestamp` -- Unix timestamp of the last completed deep scrub.
- `dtfs_scrub_chunks_checked_total{type="quick|deep"}` -- total chunks checked.
- `dtfs_scrub_chunks_corrupt_total{type="quick|deep"}` -- total corrupt chunks detected.
- `dtfs_scrub_duration_seconds{type="quick|deep"}` -- histogram of scrub duration.
- `dtfs_scrub_bandwidth_bytes_per_second` -- current scrub I/O rate.
