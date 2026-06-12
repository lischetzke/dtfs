# Data Node Configuration

The data node is configured via `/etc/dtfs/data.toml`. This page documents every available option.

## `[node]` -- Node Identity

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `id` | `node.id` | `String` | `"auto"` | Any non-empty string or `"auto"` | No | Unique identifier for this data node. When set to `"auto"`, a UUID v4 is generated on first startup and persisted to the data directory. |
| `hostname` | `node.hostname` | `String` | *(required)* | Any non-empty string | No | This data node's **identity**: the registry `node_id` is a hash of this string. Must be unique across every daemon in the cluster — in particular it must differ from a co-located manager's `[node].hostname` and from a co-located FUSE client's `[location].host`. A collision makes the two daemons overwrite each other's registry entry on every heartbeat. |
| `data_dir` | `node.data_dir` | `PathBuf` | `"/var/lib/dtfs"` | Absolute filesystem path | No | Directory where the data node stores local metadata and state. Must exist and be writable. Chunk data itself is stored on the configured disks (see `[[disks]]`). |

### Example

```toml
[node]
id = "auto"
hostname = "data-01.example.com"
data_dir = "/var/lib/dtfs/data"
```

---

## `[location]` -- Topology Location

Places this node in the four-level topology hierarchy used for replica placement and proximity-aware read routing.

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `region` | `location.region` | `String` | *(required)* | Any non-empty string | No | Geographic region (e.g., `"eu"`, `"us-east"`). |
| `datacenter` | `location.datacenter` | `String` | *(required)* | Any non-empty string | No | Datacenter within the region. |
| `rack` | `location.rack` | `String` | *(required)* | Any non-empty string | No | Rack or failure zone within the datacenter. |
| `host` | `location.host` | `String` | *(required)* | Any non-empty string | No | Host-level identifier, typically matching the hostname. |

### Example

```toml
[location]
region = "eu"
datacenter = "frankfurt"
rack = "rack-a1"
host = "data-01"
```

---

## `[cluster]` -- Cluster Connection

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `manager_addr` | `cluster.manager_addr` | `String` | *(required)* | `host:port` | No | Primary manager gRPC endpoint to register with on startup. |
| `manager_addrs` | `cluster.manager_addrs` | `Vec<String>` | `[]` | List of `host:port` | No | Additional manager endpoints, walked in order when the primary is unreachable (F-024). List every manager so a rolling manager restart never strands the node. |
| `shared_secret` | `cluster.shared_secret` | `String` | *(required)* | Any non-empty string | No | Pre-shared key used to authenticate with the manager cluster. Must match the `cluster.shared_secret` in the manager configuration. |

### Example

```toml
[cluster]
manager_addr = "mgr-01.example.com:9401"
manager_addrs = ["mgr-02.example.com:9401", "mgr-03.example.com:9401"]
shared_secret = "a-strong-random-secret"
```

---

## `[grpc]` -- gRPC Server

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `listen_addr` | `grpc.listen_addr` | `String` | *(required)* | `host:port` | No | Address and port for the gRPC server that handles chunk read/write requests from clients and replication traffic from peers. On a multi-interface host, prefer the routable address other nodes dial over `0.0.0.0`. |
| `max_message_size_mb` | `grpc.max_message_size_mb` | `u32` | `256` | 1--1024 | No | Maximum gRPC message size in megabytes. Must be at least as large as the maximum chunk size used in the cluster. |
| `max_concurrent_chunk_writes` | `grpc.max_concurrent_chunk_writes` | `usize` | `8` | 1--32 | No | Maximum in-flight `PutChunk` operations (Bug D). Bounds peak write memory: N in-flight writes x chunk size. |

### Example

```toml
[grpc]
listen_addr = "0.0.0.0:9500"
max_message_size_mb = 256
```

---

## `[tls]` -- TLS Settings

Mutual TLS (CF-6) for the data gRPC listener plus outbound replication and
manager connections. Roll out only after every node runs a TLS-capable
build -- see [TLS](../security/tls.md).

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `mode` | `tls.mode` | `String` | `"auto"` | `"auto"`, `"require"`, `"disabled"` | No | `"auto"` enables mTLS only when all three files are configured AND exist on disk, else plaintext (back-compat). `"require"` fails startup unless all three load. `"disabled"` forces plaintext. |
| `cert_file` | `tls.cert_file` | `Option<PathBuf>` | `None` | Absolute path to PEM file | No | Path to the TLS certificate file in PEM format. |
| `key_file` | `tls.key_file` | `Option<PathBuf>` | `None` | Absolute path to PEM file | No | Path to the TLS private key file in PEM format. |
| `ca_file` | `tls.ca_file` | `Option<PathBuf>` | `None` | Absolute path to PEM file | No | Path to the CA certificate bundle for verifying peer certificates. |

### Example

```toml
[tls]
mode = "require"
cert_file = "/etc/dtfs/tls/data-node.crt"
key_file = "/etc/dtfs/tls/data-node.key"
ca_file = "/etc/dtfs/tls/ca.crt"
```

---

## `[[disks]]` -- Storage Disks

The `[[disks]]` section is an array of tables. Each entry declares a disk (or mount point) that the data node uses to store chunk data. You must configure at least one disk.

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `path` | `disks[].path` | `PathBuf` | *(required)* | Absolute filesystem path | No | Mount point or directory path where chunk data is stored. Must be a dedicated mount point or directory that the DTFS process has full read/write access to. |
| `tier` | `disks[].tier` | `DiskTier` | *(required)* | `"hot"`, `"warm"`, `"cold"`, `"tape"` | No | Storage tier classification for this disk. The tiering engine uses this to decide where to place chunks based on their access temperature. Typically, `"hot"` is NVMe/SSD, `"warm"` is SSD/fast HDD, `"cold"` is high-capacity HDD; `"tape"` is for LTFS mounts declared via `[[tape_devices]]`. |

### Example

```toml
[[disks]]
path = "/mnt/nvme-a"
tier = "hot"

[[disks]]
path = "/mnt/ssd-a"
tier = "warm"

[[disks]]
path = "/mnt/hdd-a"
tier = "cold"

[[disks]]
path = "/mnt/hdd-b"
tier = "cold"
```

---

## `[cache]` -- Read Cache

Controls the in-memory read cache on the data node. This cache stores recently and frequently accessed chunks to reduce disk I/O.

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `max_memory_mb` | `cache.max_memory_mb` | `u64` | `8192` | 256--1048576 | Yes | Maximum memory in megabytes allocated to the read cache. Size this based on available system RAM minus what the OS and other processes need. |
| `policy` | `cache.policy` | `String` | `"arc"` | `"arc"`, `"lru"`, `"lfu"` | Yes | Cache eviction policy. `"arc"` (Adaptive Replacement Cache) automatically balances between recency and frequency. `"lru"` evicts least recently used entries. `"lfu"` evicts least frequently used entries. |

### Example

```toml
[cache]
max_memory_mb = 16384
policy = "arc"
```

---

## `[scrub]` -- Data Integrity Scrubbing

Controls the background scrubbing process that verifies chunk integrity by recalculating and comparing checksums.

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `quick_interval_hours` | `scrub.quick_interval_hours` | `u64` | `6` | 1--720 | Yes | Interval in hours between quick scrub cycles. Quick scrubs verify chunk metadata and sizes without reading full chunk contents. |
| `deep_interval_hours` | `scrub.deep_interval_hours` | `u64` | `168` | 1--8760 | Yes | Interval in hours between deep scrub cycles. Deep scrubs read entire chunk contents and recompute checksums. The default of 168 hours equals one week. |
| `quick_cron` | `scrub.quick_cron` | `Option<String>` | `None` | Valid cron expression | Yes | Optional cron expression that overrides `quick_interval_hours` for scheduling quick scrubs. When set, `quick_interval_hours` is ignored. |
| `deep_cron` | `scrub.deep_cron` | `Option<String>` | `None` | Valid cron expression | Yes | Optional cron expression that overrides `deep_interval_hours` for scheduling deep scrubs. When set, `deep_interval_hours` is ignored. |
| `max_io_percent` | `scrub.max_io_percent` | `u32` | `10` | 1--100 | Yes | Maximum percentage of disk I/O bandwidth that scrubbing is allowed to consume. Keeps scrubbing from impacting normal read/write performance. |
| `verify_on_read` | `scrub.verify_on_read` | `bool` | `true` | `true` or `false` | Yes | Startup fallback for hash-on-read before the first manager heartbeat arrives. |
| `quarantine_on_fault` | `scrub.quarantine_on_fault` | `bool` | `true` | `true` or `false` | Yes | Startup fallback for fault-quarantine policy. |

> This local block is only a **startup fallback** — once connected, the
> manager's cluster-wide `[scrub]` policy (see the manager page) overrides it
> via heartbeat.

### Example

```toml
[scrub]
quick_interval_hours = 6
deep_interval_hours = 168
max_io_percent = 10
```

With cron overrides:

```toml
[scrub]
quick_cron = "0 */6 * * *"
deep_cron = "0 2 * * 0"
max_io_percent = 15
```

---

## `[updater]` -- Self-Updater Source

Read by [`dtfs-cli update`](../api-reference/cli-reference.md#update) when
run on this node (e.g. `dtfs-cli update data`); the data-node daemon
itself ignores the block. Both keys are optional and default to the
canonical DTFS repo and `/usr/local/bin`. See the `[updater]` section of
the [Manager Configuration](./manager.md) page for the full option table.

```toml
[updater]
repo_url    = "https://github.com/lischetzke/dtfs"
install_dir = "/usr/local/bin"
```

---

## `[memory]` -- Process Memory Budget

Optional block (Bug G). `hard_cap_mb = 0` (the default, and the behaviour when
the block is absent) means unbounded — no backpressure ladder. **Set a
non-zero cap on every production deploy**; on a composite host the sum of all
roles' caps must stay under host RAM.

| Option | TOML Path | Type | Default | Description |
|--------|-----------|------|---------|-------------|
| `hard_cap_mb` | `memory.hard_cap_mb` | `u64` | `0` | Total RAM budget for the data-node process in MB. `0` disables the ladder. |
| `warn_threshold_pct` | `memory.warn_threshold_pct` | `u8` | `80` | Log a warning when RSS crosses this fraction of the cap. |
| `shed_threshold_pct` | `memory.shed_threshold_pct` | `u8` | `92` | Above this, new `put_chunk` requests get `RESOURCE_EXHAUSTED` (clients retry with backoff) instead of risking an OOM kill. |
| `sample_interval_secs` | `memory.sample_interval_secs` | `u64` | `5` | RSS sampling cadence. |

---

## `[placement]` -- Free-Space Headroom

Optional block (Bug BU). `put_chunk` rejects writes to a disk whose free space
is below `max(min_free_bytes, total * min_free_pct%)` with a typed
`RESOURCE_EXHAUSTED`, so the client reroutes instead of streaming into ENOSPC.

| Option | TOML Path | Type | Default |
|--------|-----------|------|---------|
| `min_free_bytes` | `placement.min_free_bytes` | `u64` | `1073741824` (1 GiB) |
| `min_free_pct` | `placement.min_free_pct` | `f64` | `5.0` |
| `allow_capacity_spill` | `placement.allow_capacity_spill` | `bool` | `true` |

---

## `[[tape_devices]]` -- LTFS Tape Devices

Optional array; omit if the node has no tape drives. Each entry exposes an
LTFS mount as `"tape"`-tier storage (write-once; only the tiering engine
writes to tape). The LTFS filesystem must be mounted before `dtfs-data`
starts.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `ltfs_path` | `PathBuf` | *(required)* | LTFS mount point. |
| `device` | `Option<String>` | `None` | Raw drive node (e.g. `/dev/nst0`) for status/rewind. |
| `library_device` | `Option<String>` | `None` | Robotic changer (e.g. `/dev/sg2`); omit for standalone drives. |
| `label` | `String` | *(required)* | Human-readable label for WebUI/CLI. |
| `capacity_gb` | `Option<u64>` | `None` | Informational capacity hint per cartridge. |
| `max_write_mbps` | `u32` | `0` | Tiering write bandwidth cap; `0` = unlimited. |

---

## Complete Example

The canonical, exhaustive example (every option, optional ones commented out
with their defaults) is checked in at `deploy/examples/data.toml`. It is
parse-tested against the config structs (`dtfs-common`
`example_data_toml_parses`), so it cannot silently drift from the code.
