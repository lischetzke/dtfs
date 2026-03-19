# FUSE Client Configuration

The FUSE client is configured via `/etc/dtfs/fuse.toml`. This page documents every available option.

## `[client]` -- Manager Connection

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `manager_addr` | `client.manager_addr` | `String` | *(required)* | `host:port` | No | Address of a manager node for metadata operations. The FUSE client contacts this address to resolve file paths to chunk locations. For high availability, this can point to a load balancer in front of multiple managers. |

### Example

```toml
[client]
manager_addr = "mgr-01.example.com:9401"
```

---

## `[location]` -- Client Topology Location

The location block places this client in the four-level topology hierarchy. The cluster uses this to route reads to the nearest data node that holds the requested chunk.

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `region` | `location.region` | `String` | *(required)* | Any non-empty string | No | Geographic region where this client runs. |
| `datacenter` | `location.datacenter` | `String` | *(required)* | Any non-empty string | No | Datacenter within the region. |
| `rack` | `location.rack` | `String` | *(required)* | Any non-empty string | No | Rack or failure zone. |
| `host` | `location.host` | `String` | *(required)* | Any non-empty string | No | Host-level identifier. |

### Example

```toml
[location]
region = "eu"
datacenter = "frankfurt"
rack = "rack-b2"
host = "app-server-01"
```

---

## `[mount]` -- FUSE Mount Point

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `mountpoint` | `mount.mountpoint` | `PathBuf` | *(required)* | Absolute filesystem path | No | Directory where the DTFS filesystem is mounted. This directory must exist and be empty before the FUSE client starts. Applications access DTFS files through this path. |

### Example

```toml
[mount]
mountpoint = "/mnt/dtfs"
```

---

## `[readahead]` -- Read-Ahead Prefetching

Controls speculative prefetching of chunks and files to reduce read latency for sequential and predictable workloads.

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `mode` | `readahead.mode` | `String` | `"folder-sequential"` | `"disabled"`, `"sequential"`, `"folder-sequential"` | Yes | Read-ahead strategy. `"disabled"` turns off all prefetching. `"sequential"` prefetches subsequent chunks of the current file. `"folder-sequential"` additionally prefetches the next files in the same directory (sorted by name), which is useful for media streaming and log processing workloads. |
| `prefetch_chunks` | `readahead.prefetch_chunks` | `u32` | `8` | 0--64 | Yes | Number of subsequent chunks to prefetch when sequential access is detected. Set to `0` to disable chunk-level prefetching while keeping file-level prefetching active. |
| `prefetch_next_files` | `readahead.prefetch_next_files` | `u32` | `3` | 0--20 | Yes | Number of subsequent files in the same directory to prefetch. Only applies when `mode` is `"folder-sequential"`. |

### Example

```toml
[readahead]
mode = "folder-sequential"
prefetch_chunks = 8
prefetch_next_files = 3
```

---

## `[cache]` -- Local Disk Cache

Controls an optional on-disk cache on the client machine that stores recently accessed chunks. This reduces network round trips for repeated reads.

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `local_cache_dir` | `cache.local_cache_dir` | `Option<PathBuf>` | `None` | Absolute filesystem path | No | Directory for the local chunk cache. When set to `None` (omitted), the local disk cache is disabled and all reads go to the network. |
| `local_cache_max_gb` | `cache.local_cache_max_gb` | `u64` | `50` | 1--10000 | Yes | Maximum size of the local disk cache in gigabytes. When the cache exceeds this limit, the least recently used entries are evicted. |

### Example

```toml
[cache]
local_cache_dir = "/var/cache/dtfs"
local_cache_max_gb = 100
```

---

## `[partition_safety]` -- Network Partition Behavior

Controls how the FUSE client behaves when it loses contact with the manager cluster (network partition or quorum loss).

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `mode` | `partition_safety.mode` | `PartitionMode` | `"strict"` | `"strict"`, `"local-readonly"`, `"full-lock"` | Yes | Behavior when the client cannot reach the manager quorum. `"strict"` returns I/O errors for all operations after `quorum_loss_timeout_secs`. `"local-readonly"` allows reads from the local cache but blocks all writes. `"full-lock"` freezes the mount entirely, causing all I/O to block until connectivity is restored. |
| `quorum_loss_timeout_secs` | `partition_safety.quorum_loss_timeout_secs` | `u64` | `10` | 1--3600 | Yes | Number of seconds to wait after losing manager contact before the partition safety `mode` takes effect. During this grace period, cached metadata is used for reads and writes are buffered. |
| `show_warning` | `partition_safety.show_warning` | `bool` | `true` | `true` or `false` | Yes | When `true`, the FUSE client logs a warning and writes a marker file at the mount root (`.dtfs-partition-warning`) when a partition is detected. Applications can watch for this file to implement their own partition-aware logic. |

### Example

```toml
[partition_safety]
mode = "strict"
quorum_loss_timeout_secs = 10
show_warning = true
```

---

## Complete Example

```toml
[client]
manager_addr = "mgr-01.example.com:9401"

[location]
region = "eu"
datacenter = "frankfurt"
rack = "rack-b2"
host = "app-server-01"

[mount]
mountpoint = "/mnt/dtfs"

[readahead]
mode = "folder-sequential"
prefetch_chunks = 8
prefetch_next_files = 3

[cache]
local_cache_dir = "/var/cache/dtfs"
local_cache_max_gb = 50

[partition_safety]
mode = "strict"
quorum_loss_timeout_secs = 10
show_warning = true
```
