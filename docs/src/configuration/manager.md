# Manager Configuration

The manager node is configured via `/etc/dtfs/manager.toml`. This page documents every available option.

## `[node]` -- Node Identity

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `id` | `node.id` | `String` | `"auto"` | Any non-empty string or `"auto"` | No | Unique identifier for this manager node. When set to `"auto"`, a UUID v4 is generated on first startup and persisted to `data_dir`. |
| `hostname` | `node.hostname` | `String` | *(required)* | Valid hostname or FQDN | No | The hostname that other nodes use to reach this manager. Must be resolvable by all cluster members. |
| `data_dir` | `node.data_dir` | `PathBuf` | `"/var/lib/dtfs"` | Absolute filesystem path | No | Directory where the manager stores Raft logs, snapshots, and metadata. Must exist and be writable by the DTFS process. |

### Example

```toml
[node]
id = "auto"
hostname = "mgr-01.example.com"
data_dir = "/var/lib/dtfs/manager"
```

---

## `[location]` -- Topology Location

The location block places this node in the four-level topology hierarchy used for replica placement and proximity-aware routing.

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `region` | `location.region` | `String` | *(required)* | Any non-empty string | No | Geographic region (e.g., `"eu"`, `"us-east"`). |
| `datacenter` | `location.datacenter` | `String` | *(required)* | Any non-empty string | No | Datacenter within the region (e.g., `"frankfurt"`, `"us-east-1a"`). |
| `rack` | `location.rack` | `String` | *(required)* | Any non-empty string | No | Rack or failure zone within the datacenter. |
| `host` | `location.host` | `String` | *(required)* | Any non-empty string | No | Host-level identifier, typically matching the hostname. |

### Example

```toml
[location]
region = "eu"
datacenter = "frankfurt"
rack = "rack-a1"
host = "mgr-01"
```

---

## `[cluster]` -- Cluster Membership

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `name` | `cluster.name` | `String` | *(required)* | Any non-empty string | No | Human-readable cluster name. All managers in a cluster must share the same name. |
| `shared_secret` | `cluster.shared_secret` | `String` | *(required)* | Any non-empty string | No | Pre-shared key used to authenticate nodes joining the cluster. Must be identical on all nodes. |
| `bootstrap` | `cluster.bootstrap` | `bool` | `false` | `true` or `false` | No | Set to `true` on exactly one manager during initial cluster creation. This node forms a single-node Raft cluster and elects itself leader. Must be `false` on all subsequent joins. |

### Example

```toml
[cluster]
name = "production"
shared_secret = "a-strong-random-secret"
bootstrap = true
```

---

## `[raft]` -- Raft Consensus

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `listen_addr` | `raft.listen_addr` | `String` | `"0.0.0.0:9400"` | `host:port` | No | Address and port for Raft peer-to-peer communication. |
| `election_timeout_ms` | `raft.election_timeout_ms` | `u64` | `400` | 100--10000 | No | Maximum time in milliseconds a follower waits before starting an election. Higher values reduce spurious elections on slow networks but increase failover latency. |
| `heartbeat_interval_ms` | `raft.heartbeat_interval_ms` | `u64` | `150` | 50--5000 | No | Interval in milliseconds at which the leader sends heartbeats to followers. Must be significantly less than `election_timeout_ms`. |
| `snapshot_threshold` | `raft.snapshot_threshold` | `u64` | `10000` | 100--1000000 | No | Number of applied Raft log entries before a snapshot is triggered. Lower values use more disk I/O; higher values increase recovery time after restart. |

### Example

```toml
[raft]
listen_addr = "0.0.0.0:9400"
election_timeout_ms = 400
heartbeat_interval_ms = 150
snapshot_threshold = 10000
```

---

## `[grpc]` -- gRPC Server

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `listen_addr` | `grpc.listen_addr` | `String` | *(required)* | `host:port` | No | Address and port for the gRPC server that data nodes and clients connect to. |
| `max_message_size_mb` | `grpc.max_message_size_mb` | `u32` | `256` | 1--1024 | No | Maximum gRPC message size in megabytes. Increase if large metadata responses are expected. |

### Example

```toml
[grpc]
listen_addr = "0.0.0.0:9401"
max_message_size_mb = 256
```

---

## `[api]` -- REST API / Web UI

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `listen_addr` | `api.listen_addr` | `String` | `"0.0.0.0:9402"` | `host:port` | No | Address and port for the HTTP REST API and Web UI. |
| `cors_origins` | `api.cors_origins` | `Vec<String>` | `["*"]` | List of origin URLs or `"*"` | Yes | Allowed CORS origins for the REST API. Use `["*"]` to allow all origins, or specify exact origins for production environments. |

### Example

```toml
[api]
listen_addr = "0.0.0.0:9402"
cors_origins = ["https://dashboard.example.com"]
```

---

## `[tls]` -- TLS Settings

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `mode` | `tls.mode` | `String` | `"auto"` | `"auto"`, `"require"`, `"disable"` | No | TLS mode. `"auto"` enables TLS if cert/key files are present, `"require"` fails startup without them, `"disable"` turns off TLS entirely. |
| `cert_file` | `tls.cert_file` | `Option<PathBuf>` | `None` | Absolute path to PEM file | No | Path to the TLS certificate file in PEM format. |
| `key_file` | `tls.key_file` | `Option<PathBuf>` | `None` | Absolute path to PEM file | No | Path to the TLS private key file in PEM format. |
| `ca_file` | `tls.ca_file` | `Option<PathBuf>` | `None` | Absolute path to PEM file | No | Path to the CA certificate bundle for verifying client certificates (mutual TLS). |

### Example

```toml
[tls]
mode = "require"
cert_file = "/etc/dtfs/tls/server.crt"
key_file = "/etc/dtfs/tls/server.key"
ca_file = "/etc/dtfs/tls/ca.crt"
```

---

## `[defaults]` -- Default Volume Settings

These defaults apply to newly created volumes unless overridden at volume creation time.

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `chunk_size_mb` | `defaults.chunk_size_mb` | `u32` | `128` | 1--1024 | Yes | Default chunk size in megabytes for new volumes. Larger chunks reduce metadata overhead; smaller chunks reduce write amplification. |

### `[defaults.write_safety]`

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `min_replicas_before_ack` | `defaults.write_safety.min_replicas_before_ack` | `u32` | `2` | 1--replication factor | Yes | Minimum number of replicas that must acknowledge a write before the client receives success. Setting this to 1 gives lower latency but risks data loss on node failure. |
| `write_timeout_secs` | `defaults.write_safety.write_timeout_secs` | `u32` | `30` | 1--3600 | Yes | Maximum time in seconds to wait for write acknowledgements before returning an error to the client. |

### `[defaults.replication]`

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `hot` | `defaults.replication.hot` | `u32` | `3` | 1--9 | Yes | Default replication factor for hot-tier data. |
| `warm` | `defaults.replication.warm` | `u32` | `2` | 1--9 | Yes | Default replication factor for warm-tier data. |
| `cold` | `defaults.replication.cold` | `u32` | `1` | 1--9 | Yes | Default replication factor for cold-tier data. |

### Example

```toml
[defaults]
chunk_size_mb = 128

[defaults.write_safety]
min_replicas_before_ack = 2
write_timeout_secs = 30

[defaults.replication]
hot = 3
warm = 2
cold = 1
```

---

## `[tiering]` -- Automatic Data Tiering

Controls the automatic data tiering engine that moves chunks between hot, warm, cold, and frozen storage based on access patterns.

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `enabled` | `tiering.enabled` | `bool` | `true` | `true` or `false` | Yes | Enable or disable the automatic tiering engine. |
| `hot_threshold_reads_per_hour` | `tiering.hot_threshold_reads_per_hour` | `u32` | `5` | 1--10000 | Yes | Minimum reads per hour for data to be classified as hot. Data below this threshold is considered warm. |
| `cold_threshold_days` | `tiering.cold_threshold_days` | `u32` | `14` | 1--3650 | Yes | Days without access before data is classified as cold. |
| `frozen_threshold_days` | `tiering.frozen_threshold_days` | `u32` | `60` | 1--3650 | Yes | Days without access before data is classified as frozen. Must be greater than `cold_threshold_days`. |
| `migration_bandwidth_limit_mbps` | `tiering.migration_bandwidth_limit_mbps` | `u32` | `200` | 0--10000 | Yes | Maximum bandwidth in megabits per second allocated to tier migration. Set to `0` for unlimited. |
| `evaluation_interval_secs` | `tiering.evaluation_interval_secs` | `u64` | `3600` | 60--86400 | Yes | How often, in seconds, the tiering engine evaluates data access patterns and triggers migrations. |

### Example

```toml
[tiering]
enabled = true
hot_threshold_reads_per_hour = 5
cold_threshold_days = 14
frozen_threshold_days = 60
migration_bandwidth_limit_mbps = 200
evaluation_interval_secs = 3600
```

---

## `[monitoring]` -- Node Health Monitoring

Controls how the manager monitors the health of data nodes through heartbeats.

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `heartbeat_interval_secs` | `monitoring.heartbeat_interval_secs` | `u64` | `5` | 1--300 | Yes | Expected interval in seconds between data node heartbeats. |
| `suspect_after_missed` | `monitoring.suspect_after_missed` | `u32` | `3` | 1--100 | Yes | Number of consecutive missed heartbeats before a node is marked as suspect. A suspect node is still considered available but may be experiencing issues. |
| `down_after_missed` | `monitoring.down_after_missed` | `u32` | `6` | 1--100 | Yes | Number of consecutive missed heartbeats before a node is marked as down. A down node triggers re-replication of its chunks. Must be greater than `suspect_after_missed`. |

### Example

```toml
[monitoring]
heartbeat_interval_secs = 5
suspect_after_missed = 3
down_after_missed = 6
```

---

## Complete Example

```toml
[node]
id = "auto"
hostname = "mgr-01.example.com"
data_dir = "/var/lib/dtfs/manager"

[location]
region = "eu"
datacenter = "frankfurt"
rack = "rack-a1"
host = "mgr-01"

[cluster]
name = "production"
shared_secret = "change-me-to-a-strong-secret"
bootstrap = false

[raft]
listen_addr = "0.0.0.0:9400"
election_timeout_ms = 400
heartbeat_interval_ms = 150
snapshot_threshold = 10000

[grpc]
listen_addr = "0.0.0.0:9401"
max_message_size_mb = 256

[api]
listen_addr = "0.0.0.0:9402"
cors_origins = ["*"]

[tls]
mode = "auto"

[defaults]
chunk_size_mb = 128

[defaults.write_safety]
min_replicas_before_ack = 2
write_timeout_secs = 30

[defaults.replication]
hot = 3
warm = 2
cold = 1

[tiering]
enabled = true
hot_threshold_reads_per_hour = 5
cold_threshold_days = 14
frozen_threshold_days = 60
migration_bandwidth_limit_mbps = 200
evaluation_interval_secs = 3600

[monitoring]
heartbeat_interval_secs = 5
suspect_after_missed = 3
down_after_missed = 6
```
