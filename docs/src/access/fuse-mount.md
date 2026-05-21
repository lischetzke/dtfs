# FUSE Mount

DTFS exposes the distributed filesystem as a local mount point using FUSE (Filesystem in Userspace). Applications read and write files through standard POSIX system calls without any awareness of the underlying distributed storage.

## Overview

The `dtfs-fuse` binary is the FUSE client. It connects to the manager cluster for metadata operations and directly to data nodes for chunk reads and writes. From the application's perspective, the mount point behaves like a local filesystem.

```text
Application
    |
    v  (POSIX: open, read, write, stat, readdir, ...)
+-----------+
| dtfs-fuse |
+-----------+
    |               |
    v               v
dtfs-manager    dtfs-data
(metadata)      (chunk I/O)
```

## Mounting

### Basic Mount

```bash
dtfs-fuse mount \
  --manager-addrs 10.0.1.1:9100,10.0.1.2:9100,10.0.1.3:9100 \
  --volume default \
  --mountpoint /mnt/dtfs
```

The `--manager-addrs` flag accepts a comma-separated list of manager addresses. The client discovers the current Raft leader automatically and reconnects on failover.

### Configuration File

For production deployments, use a configuration file instead of command-line flags:

```toml
# /etc/dtfs/fuse-client.toml

[manager]
addrs = ["10.0.1.1:9100", "10.0.1.2:9100", "10.0.1.3:9100"]

[mount]
volume = "default"
mountpoint = "/mnt/dtfs"

# Allow non-root users to access the mount
allow_other = true

# Set the filesystem name shown in mount and df output
fsname = "dtfs"
```

```bash
dtfs-fuse mount --config /etc/dtfs/fuse-client.toml
```

### Unmounting

```bash
dtfs-fuse unmount /mnt/dtfs
```

Or use the standard `fusermount` command:

```bash
fusermount -u /mnt/dtfs
```

A graceful unmount flushes all pending writes and closes all open file handles before detaching the mount. If applications hold open handles, the unmount blocks until they are closed (or use `--force` to detach immediately, which may lose unflushed writes).

## Transparent Remote Access

When a FUSE client reads a file, the following sequence occurs transparently:

1. The client resolves the file path to a file ID and chunk list via the manager's metadata service.
2. For each chunk needed, the client selects the best replica based on network proximity (see location-aware selection below).
3. The client opens a gRPC stream to the selected data node and reads the chunk data.
4. The data is returned to the application through the FUSE interface.

### Location-Aware Node Selection

The FUSE client is configured with its own location in the topology hierarchy:

```toml
[client.location]
region     = "us-east"
datacenter = "dc1"
rack       = "rack-05"
```

When multiple replicas of a chunk are available, the client selects the one with the smallest topological distance:

1. Same rack (lowest latency).
2. Same datacenter, different rack.
3. Same region, different datacenter.
4. Different region (highest latency).

If two replicas are equidistant, the client picks one at random to distribute load.

## Performance Tuning

### Metadata Caching

The FUSE client caches directory entries and file attributes to reduce round trips to the manager:

```toml
[cache.metadata]
# Time-to-live for cached directory entries
entry_ttl_secs = 5

# Time-to-live for cached file attributes (size, mtime, permissions)
attr_ttl_secs = 5

# Maximum number of cached metadata entries
max_entries = 100000
```

Lower TTL values provide stronger consistency but increase metadata traffic. For workloads where files are rarely modified, increasing the TTL reduces latency.

### First-write tier preference (Bug AV)

The FUSE client can request a specific tier for the first chunk of a write via the `[write] first_write_tier` config option. Two modes:

* **`"cold"` (default)** — first-chunk placement targets the volume's effective cold tier (the mandatory anchor). The write ACK blocks until the cold replica confirms. Safer and no anchor-lag window, but slower when the client's DC has a slow uplink to the anchor DC.
* **`"nearest-warm"`** — first-chunk placement targets a warm-tier node in the client's own DC. The write ACK comes back as soon as the warm replica confirms; the manager's anchor-lag-repair pass (Bug AV AV-3-3) schedules the mandatory cold copy asynchronously within one tiering tick (default 5 min).

Configure in `fuse.toml`:

```toml
[write]
first_write_tier = "nearest-warm"
```

See [FUSE Client Configuration → `[write]`](../configuration/fuse-client.md#write----write-path-preferences-bug-av) for the full reference and trade-off discussion.

> The mode interacts with the volume's `cache_replicas` policy at the manager. A volume that doesn't enable warm caching (`cache_replicas.warm.max = 0`) ignores the `nearest-warm` request — the manager falls back to the volume default tier. This is intentional so a client misconfiguration can't bypass a volume's durability requirements.

### Write Buffering

Writes are buffered on the client and flushed to data nodes in chunk-sized batches (default 128 MB). This converts many small writes into fewer large sequential writes:

```toml
[write_buffer]
# Maximum memory for write buffers across all open files
max_total_size = "1 GiB"

# Flush a chunk when the buffer for that chunk reaches this size
flush_threshold = "128 MiB"

# Maximum time a buffer can remain unflushed
flush_timeout_secs = 30
```

The `flush_timeout_secs` ensures that data is not held in memory indefinitely. Even if a chunk buffer has not reached the flush threshold, it is written to the data nodes after this timeout.

### Read-Ahead

The FUSE client supports configurable read-ahead to prefetch data before the application requests it. See [Read-Ahead Modes](./read-ahead.md) for details.

### Concurrency

```toml
[io]
# Maximum concurrent chunk reads
max_read_concurrency = 16

# Maximum concurrent chunk writes
max_write_concurrency = 8

# Number of FUSE worker threads
fuse_threads = 4
```

Increasing `max_read_concurrency` improves throughput for workloads with many parallel readers. The `fuse_threads` setting controls how many FUSE requests are processed in parallel by the kernel-to-userspace bridge.

## systemd Integration

For systems using systemd, install the provided unit file:

```ini
# /etc/systemd/system/dtfs-fuse.service
[Unit]
Description=DTFS FUSE Client
After=network-online.target
Wants=network-online.target

[Service]
Type=notify
ExecStart=/usr/local/bin/dtfs-fuse mount --config /etc/dtfs/fuse-client.toml
ExecStop=/usr/local/bin/dtfs-fuse unmount /mnt/dtfs
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
systemctl enable --now dtfs-fuse
```

## Monitoring

The FUSE client exports metrics on a local Prometheus endpoint:

- `dtfs_fuse_ops_total{op="read|write|open|stat|readdir|..."}` -- total FUSE operations by type.
- `dtfs_fuse_op_duration_seconds{op="..."}` -- histogram of operation latency.
- `dtfs_fuse_read_bytes_total` -- total bytes read through the mount.
- `dtfs_fuse_write_bytes_total` -- total bytes written through the mount.
- `dtfs_fuse_open_handles` -- currently open file handles.
- `dtfs_fuse_write_buffer_bytes` -- current write buffer usage.
