# FUSE Client Configuration

The FUSE client is configured via `/etc/dtfs/fuse.toml`. This page documents every available option.

> **Malformed config = restart loop.** A `fuse.toml` that fails to parse
> (syntax error, missing required field such as `manager_addr`) makes the
> service exit at startup; under systemd it restart-loops every few seconds.
> After editing, check `journalctl -u dtfs-fuse@<mount> -n 20`.

## Top-level options

These keys sit at the root of `fuse.toml` (before any `[table]` header).

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `read_location_priority` | `read_location_priority` | `Vec<String>` | `[]` | Datacenter or host names | No | Ordered read-location preference (U4). When set, a read picks the first replica whose `datacenter` **or** `host` matches the earliest-listed entry (case-insensitive), taking precedence over raw physical distance — the operator's explicit cross-DC routing intent. When no entry matches, selection falls through to physical distance and then a warm-over-cold tier tie-break. Empty (the default) = distance-only behaviour, unchanged. |

Read replica selection order (U4): **(1)** `read_location_priority` (if any entry matches a replica's DC/host); **(2)** physical distance (`SameHost` < `SameRack` < `SameDatacenter` < `SameRegion` < `CrossRegion`); **(3)** when distance is *equal*, prefer the faster tier (warm over the cold anchor HDD). The tier is only a tie-break — a strictly-nearer replica always wins, so a local cold copy still beats a remote warm one.

### Example

```toml
# A TORO client that should read from Falkenstein first, then Limburg,
# before considering any other replica by distance.
read_location_priority = ["falkenstein", "limburg"]
```

---

## `[client]` -- Manager Connection

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `manager_addr` | `client.manager_addr` | `String` | *(required)* | `host:port` | No | Primary manager gRPC endpoint for metadata operations. |
| `manager_addrs` | `client.manager_addrs` | `Vec<String>` | `[]` | List of `host:port` | No | Additional manager endpoints, walked in order on failover (F-024). List every manager. |
| `shared_secret` | `client.shared_secret` | `String` | `""` | Cluster secret | No | Required for the client to self-register and appear in the node table (Bug AA). Empty = the client stays invisible except while holding a write lease. |
| `heartbeat_interval_secs` | `client.heartbeat_interval_secs` | `u64` | `10` | 0--liveness window | No | Keepalive re-register cadence (Bug BQ-1). Keep well under the manager's `raft.node_liveness_secs` (default 30) or the client flaps `Down`. `0` = register once. |
| `chunk_size_mb` | `client.chunk_size_mb` | `u32` | `4` | Must match volume | No | Chunk size the write buffer splits files into (Bug BW). **Must equal the volume's chunk size** (manager `defaults.chunk_size_mb`); a mismatch corrupts multi-chunk reads. |
| `chunk_read_timeout_secs` | `client.chunk_read_timeout_secs` | `u64` | `60` | `0` = disabled | No | Per-replica chunk-fetch timeout (FUSE-HOL). A hung/dead data node that never returns a transport error would otherwise stall the read — and the app's FUSE op — indefinitely; on timeout the read fails over to the next replica. Generous vs a 4 MB chunk at 40 Mbps (~0.8 s); raise for very large chunks on very slow links. `0` keeps the legacy unbounded await. |

### Example

```toml
[client]
manager_addr = "mgr-01.example.com:9401"
manager_addrs = ["mgr-02.example.com:9401", "mgr-03.example.com:9401"]
shared_secret = "a-strong-random-secret"
```

---

## `[location]` -- Client Topology Location

The location block places this client in the four-level topology hierarchy. The cluster uses this to route reads to the nearest data node that holds the requested chunk.

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `region` | `location.region` | `String` | *(required)* | Any non-empty string | No | Geographic region where this client runs. |
| `datacenter` | `location.datacenter` | `String` | *(required)* | Any non-empty string | No | Datacenter within the region. |
| `rack` | `location.rack` | `String` | *(required)* | Any non-empty string | No | Rack or failure zone. |
| `host` | `location.host` | `String` | *(required)* | Any non-empty string | No | Host-level identifier — **and this client's registry identity**. The FUSE client has no `[node].hostname`; it registers (and holds write leases) under this string. It must be unique cluster-wide: never reuse a data node's or manager's hostname, or the two daemons overwrite each other's registry entry. Convention for a client co-located with a data node: `<data-hostname>_cln`. |

> The `_cln` suffix costs the `SameHost` distance tie-break for read routing
> (the strings no longer match). Give each physical box its own `rack` value
> so `SameRack` keeps reads local.

### Example

```toml
[location]
region = "eu"
datacenter = "frankfurt"
rack = "rack-b2"
host = "app-server-01_cln"
```

---

## `[mount]` -- FUSE Mount Point

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `mountpoint` | `mount.mountpoint` | `PathBuf` | *(required)* | Absolute filesystem path | No | Directory where the DTFS filesystem is mounted. This directory must exist and be empty before the FUSE client starts. Applications access DTFS files through this path. |
| `allow_other` | `mount.allow_other` | `bool` | `false` | `true` or `false` | No | Mount with FUSE `allow_other` so non-root users (e.g. a media-server service under its own UID) can read and write through the mount (F-028). |

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
| `mode` | `readahead.mode` | `String` | `"folder-sequential"` | `"disabled"`, `"file-only"`, `"file-sequential"`, `"folder-sequential"` | Yes | Read-ahead strategy. `"disabled"` turns off all prefetching. `"file-only"` / `"file-sequential"` prefetch subsequent chunks of the current file. `"folder-sequential"` additionally prefetches the next files in the same directory (sorted by name) — useful for media streaming. Unknown values fall back to `"folder-sequential"` with a warning. |
| `prefetch_chunks` | `readahead.prefetch_chunks` | `u32` | `8` | 0--64 | Yes | Number of subsequent chunks to prefetch when sequential access is detected. Set to `0` to disable chunk-level prefetching while keeping file-level prefetching active. |
| `prefetch_next_files` | `readahead.prefetch_next_files` | `u32` | `3` | 0--20 | Yes | Number of subsequent files in the same directory to prefetch. Only applies when `mode` is `"folder-sequential"`. |
| `cache_mb` | `readahead.cache_mb` | `u64` | `64` | >=1 | Yes | Byte budget (MB) for the in-memory prefetch cache (Bug CE). The cache evicts to stay under this regardless of chunk size, so whole-file reads cannot OOM the FUSE process. |

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
| `local_cache_max_gb` | `cache.local_cache_max_gb` | `u64` | `50` | 0--10000 | Yes | Maximum size of the local disk cache in gigabytes. When the cache exceeds this limit, the least recently used entries are evicted. `0` = zero byte budget (cache effectively disabled even when `local_cache_dir` is set). |
| `max_age_days` | `cache.max_age_days` | `Option<u32>` | `None` | >=1 | Yes | Maximum age of a cached chunk before the pruner evicts it (atime-based, P0-11). Unset = size-based eviction only. |
| `prune_interval_secs` | `cache.prune_interval_secs` | `u64` | `3600` | >=60 | Yes | Interval between background pruner sweeps. |

### Example

```toml
[cache]
local_cache_dir = "/var/cache/dtfs"
local_cache_max_gb = 100
```

---

## `[write]` -- Write-Path Preferences (Bug AV)

Controls how the FUSE client requests chunk placement at write time. Combined with the manager's anchor-lag-repair pass (AV-3-3), this lets operators trade durability latency for throughput on slow uplinks.

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `first_write_tier` | `write.first_write_tier` | `string` | `"cold"` | `"cold"`, `"nearest-warm"` | No (restart FUSE) | Tier preference for first-chunk placement. `"cold"` (default) blocks the ACK on the mandatory cold (anchor) copy — slower in DCs with slow uplinks to the anchor DC but no anchor-lag window. `"nearest-warm"` places the first chunk on a warm-tier node in the client's own DC and ACKs immediately; the manager's anchor-lag-repair pass (AV-3-3) schedules the mandatory cold copy asynchronously within one tiering tick (default 5 min). Any unrecognised value falls back to `"cold"` for forward-compat safety. |

### Example

```toml
[write]
first_write_tier = "nearest-warm"
```

### When to use `"nearest-warm"`

* **Fresh-download landing zones** — the NRB Plexmedia DC has a 1 Gbps uplink and the operator wants torrent finishes to ACK in seconds, not block on the 40 Mbps LELI anchor link. The anchor copy lands asynchronously.
* **Office writes** — small frequent edits where the warm-tier SSD in the user's own DC is the fastest target; the cold anchor copy can lag by a tick.

### When to keep the default `"cold"`

* **Compliance / DR workloads** — the write must be durably on the anchor before the application sees ACK. The anchor-lag window is a real durability gap; `"cold"` closes it.
* **Tiny clusters without a fast warm tier** — picking the wrong tier just adds a hop.

> See [Tiering → Promotion math](../storage/tiering.md#promotion-math) for what happens *after* the first chunk lands. The anchor-lag repair fires from the manager's normal tiering tick, so the cold copy typically lands within one tick interval.

---

## `[partition_safety]` -- Network Partition Behavior

Controls how the FUSE client behaves when it loses contact with the manager cluster (network partition or quorum loss).

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `mode` | `partition_safety.mode` | `PartitionMode` | `"strict"` | `"strict"`, `"localreadonly"`, `"fulllock"` | Yes | Behavior when the client cannot reach the manager quorum. `"strict"` returns I/O errors for all operations after `quorum_loss_timeout_secs`. `"localreadonly"` allows reads from the local cache but blocks all writes. `"fulllock"` freezes the mount entirely, causing all I/O to block until connectivity is restored. (TOML values are the lowercase variant names, no hyphens.) |
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

## `[tls]` -- TLS Settings

Optional block (CF-6). gRPC transport security toward managers and data
nodes: mutual TLS verified against the cluster CA. The block may be omitted
entirely (mode `"auto"` with no certificate paths resolves to plaintext, so
legacy configs keep working). Enable only after every manager and data node
runs a TLS-capable build -- see [TLS](../security/tls.md).

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `mode` | `tls.mode` | `String` | `"auto"` | `"auto"`, `"require"`, `"disabled"` | No | `"auto"` enables mTLS only when all three files are configured AND exist on disk, else plaintext. `"require"` fails startup unless all three load. `"disabled"` forces plaintext. |
| `cert_file` | `tls.cert_file` | `Option<PathBuf>` | `None` | Absolute path to PEM file | No | Client certificate presented to servers (mutual TLS). |
| `key_file` | `tls.key_file` | `Option<PathBuf>` | `None` | Absolute path to PEM file | No | Private key for `cert_file`. |
| `ca_file` | `tls.ca_file` | `Option<PathBuf>` | `None` | Absolute path to PEM file | No | Cluster CA used to verify manager and data-node certificates. |

---

## `[memory]` -- Process Memory Budget

Optional block (CF-5). `hard_cap_mb = 0` (the default, and the behaviour when
the block is absent) means unbounded -- no pressure ladder. When non-zero, a
background sampler watches RSS and, at the warn level, halves the
[`[readahead]`](#readahead--read-ahead-prefetching) `cache_mb` prefetch budget;
at the shed level it drops all unconsumed prefetch (the on-disk `[cache]`
survives). systemd `MemoryHigh`/`MemoryMax` remain the hard backstop; on a
composite host the sum of all roles' caps must stay under host RAM.

| Option | TOML Path | Type | Default | Description |
|--------|-----------|------|---------|-------------|
| `hard_cap_mb` | `memory.hard_cap_mb` | `u64` | `0` | Total RAM budget for the FUSE client process in MB. `0` disables the ladder. |
| `warn_threshold_pct` | `memory.warn_threshold_pct` | `u8` | `80` | At/above this fraction of the cap the readahead `cache_mb` budget is halved. |
| `shed_threshold_pct` | `memory.shed_threshold_pct` | `u8` | `92` | At/above this all unconsumed prefetch is dropped from the in-memory cache. |
| `sample_interval_secs` | `memory.sample_interval_secs` | `u64` | `5` | RSS sampling cadence. |

---

## `[updater]` -- Self-Updater Source

Read by [`dtfs-cli update`](../api-reference/cli-reference.md#update) when
run on this node (e.g. `dtfs-cli update fuse`, alias `client`); the FUSE
client daemon itself ignores the block. Both keys are optional and
default to the canonical DTFS repo and `/usr/local/bin`. See the
`[updater]` section of the [Manager Configuration](./manager.md) page for
the full option table.

```toml
[updater]
repo_url    = "https://github.com/lischetzke/dtfs"
install_dir = "/usr/local/bin"
```

---

## Complete Example

The canonical, exhaustive example (every option, optional ones commented out
with their defaults) is checked in at `deploy/examples/fuse.toml`. It is
parse-tested against the config structs (`dtfs-common`
`example_fuse_toml_parses`), so it cannot silently drift from the code.
