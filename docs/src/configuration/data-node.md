# Data Node Configuration

The data node is configured via `/etc/dtfs/data.toml`. This page documents every available option.

## `[node]` -- Node Identity

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `id` | `node.id` | `String` | `"auto"` | Any non-empty string or `"auto"` | No | Unique identifier for this data node. When set to `"auto"`, a UUID v4 is generated on first startup and persisted to the data directory. |
| `hostname` | `node.hostname` | `String` | *(required)* | Valid hostname or FQDN | No | The hostname that the manager and other nodes use to reach this data node. Must be resolvable by all cluster members. |
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
| `manager_addr` | `cluster.manager_addr` | `String` | *(required)* | `host:port` | No | Address of a manager node to connect to on startup. The data node uses this to register itself and receive cluster state. For high availability, this can point to a load balancer in front of multiple managers. |
| `shared_secret` | `cluster.shared_secret` | `String` | *(required)* | Any non-empty string | No | Pre-shared key used to authenticate with the manager cluster. Must match the `cluster.shared_secret` in the manager configuration. |

### Example

```toml
[cluster]
manager_addr = "mgr-01.example.com:9401"
shared_secret = "a-strong-random-secret"
```

---

## `[grpc]` -- gRPC Server

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `listen_addr` | `grpc.listen_addr` | `String` | *(required)* | `host:port` | No | Address and port for the gRPC server that handles chunk read/write requests from clients and replication traffic from peers. |
| `max_message_size_mb` | `grpc.max_message_size_mb` | `u32` | `256` | 1--1024 | No | Maximum gRPC message size in megabytes. Must be at least as large as the maximum chunk size used in the cluster. |

### Example

```toml
[grpc]
listen_addr = "0.0.0.0:9500"
max_message_size_mb = 256
```

---

## `[tls]` -- TLS Settings

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `mode` | `tls.mode` | `String` | `"auto"` | `"auto"`, `"require"`, `"disable"` | No | TLS mode. `"auto"` enables TLS if cert/key files are present, `"require"` fails startup without them, `"disable"` turns off TLS entirely. |
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
| `tier` | `disks[].tier` | `DiskTier` | *(required)* | `"hot"`, `"warm"`, `"cold"` | No | Storage tier classification for this disk. The tiering engine uses this to decide where to place chunks based on their access temperature. Typically, `"hot"` is NVMe/SSD, `"warm"` is SSD/fast HDD, and `"cold"` is high-capacity HDD. |

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

## Complete Example

```toml
[node]
id = "auto"
hostname = "data-01.example.com"
data_dir = "/var/lib/dtfs/data"

[location]
region = "eu"
datacenter = "frankfurt"
rack = "rack-a1"
host = "data-01"

[cluster]
manager_addr = "mgr-01.example.com:9401"
shared_secret = "change-me-to-a-strong-secret"

[grpc]
listen_addr = "0.0.0.0:9500"
max_message_size_mb = 256

[tls]
mode = "auto"

[[disks]]
path = "/mnt/nvme-a"
tier = "hot"

[[disks]]
path = "/mnt/hdd-a"
tier = "cold"

[cache]
max_memory_mb = 8192
policy = "arc"

[scrub]
quick_interval_hours = 6
deep_interval_hours = 168
max_io_percent = 10
```
