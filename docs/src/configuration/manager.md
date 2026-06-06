# Manager Configuration

The manager node is configured via `/etc/dtfs/manager.toml`. This page documents every available option.

## `[node]` -- Node Identity

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `id` | `node.id` | `String` | `"auto"` | Any non-empty string or `"auto"` | No | Unique identifier for this manager node. When set to `"auto"`, a UUID v4 is generated on first startup and persisted to `data_dir`. |
| `hostname` | `node.hostname` | `String` | *(required)* | Any non-empty string | No | This manager's **identity**: the Raft/registry `node_id` is a hash of this string. It must be unique across every daemon in the cluster and must exactly equal this node's `[[cluster.initial_voters]]` entry (see below). It does not have to be DNS-resolvable. |
| `data_dir` | `node.data_dir` | `PathBuf` | `"/var/lib/dtfs"` | Absolute filesystem path | No | Directory where the manager stores Raft logs, snapshots, and metadata. Must exist and be writable by the DTFS process. |

> **Identity uniqueness.** `node_id = hash(hostname)`. Two daemons that register
> with the same hostname share one registry slot and overwrite each other on
> every heartbeat (role flapping, vanishing `grpc_addr`). On a composite host,
> give every role its own name: manager `dtfs-fsn-mgr01`, data node
> `dtfs-fsn-data01`, FUSE client `dtfs-fsn-data01_cln` (the FUSE client
> registers under its `[location].host` — see the FUSE client page).

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
| `bootstrap` | `cluster.bootstrap` | `bool` | `false` | `true` or `false` | No | Set to `true` on exactly one manager during initial cluster creation, and only for the very first start. **Never** set it on a node meant to join an existing cluster: it initializes a second, divergent Raft group that never federates (metadata split-brain). |
| `initial_voters` | `[[cluster.initial_voters]]` | array of tables | `[]` | — | No | The full expected voter set. See below. |

### `[[cluster.initial_voters]]` -- Voter Set

One entry per manager. The identity contract:

- `hostname` must **exactly equal** that manager's `[node].hostname` — the
  Raft `node_id` is derived from this string. A manager that finds no entry
  matching its own hostname cannot bootstrap-join and logs
  `Bug AC bootstrap: no matching initial_voters entry for self`.
- The **same list** must appear in every manager's `manager.toml`.
- `addr` is the peer-routable Raft gRPC `host:port` (the peer's
  `grpc.listen_addr` as other nodes reach it). Never `0.0.0.0` — peers dial
  this exact string.
- `region` / `datacenter` / `rack` are optional WebUI display hints.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `hostname` | `String` | *(required)* | Must equal the peer's `[node].hostname`. |
| `addr` | `String` | *(required)* | Routable `host:port` of the peer's manager gRPC. |
| `region`, `datacenter`, `rack` | `String` | `""` | Optional topology hints for the node table. |

Adding a manager to an **existing** cluster: editing `initial_voters` alone
does not join an already-initialized cluster — see
[Cluster Management](../operations/cluster-management.md) for the late-join
procedure.

### Example

```toml
[cluster]
name = "production"
shared_secret = "a-strong-random-secret"
bootstrap = true            # first manager, first start only

[[cluster.initial_voters]]
hostname = "mgr-01"          # == that node's [node].hostname
addr = "10.0.0.1:9401"

[[cluster.initial_voters]]
hostname = "mgr-02"
addr = "10.0.0.2:9401"

[[cluster.initial_voters]]
hostname = "mgr-03"
addr = "10.0.0.3:9401"
```

---

## `[raft]` -- Raft Consensus

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `listen_addr` | `raft.listen_addr` | `String` | `"0.0.0.0:9400"` | `host:port` | No | Address and port for Raft peer-to-peer communication. |
| `election_timeout_ms` | `raft.election_timeout_ms` | `u64` | `400` | 100--10000 | No | Maximum time in milliseconds a follower waits before starting an election. Higher values reduce spurious elections on slow networks but increase failover latency. |
| `heartbeat_interval_ms` | `raft.heartbeat_interval_ms` | `u64` | `150` | 50--5000 | No | Interval in milliseconds at which the leader sends heartbeats to followers. Must be significantly less than `election_timeout_ms`. |
| `snapshot_threshold` | `raft.snapshot_threshold` | `u64` | `10000` | 100--1000000 | No | Number of applied Raft log entries before a snapshot is triggered. Lower values use more disk I/O; higher values increase recovery time after restart. |
| `rocksdb_keep_log_file_num` | `raft.rocksdb_keep_log_file_num` | `u64` | `4` | >=1 | No | RocksDB info-LOG files kept for the Raft log store (Bug BP-1; unbounded logs once filled a manager partition). |
| `rocksdb_max_manifest_bytes` | `raft.rocksdb_max_manifest_bytes` | `u64` | `67108864` | >=1 MiB | No | RocksDB MANIFEST size before rotation (Bug BP-1). |
| `max_in_snapshot_log_to_keep` | `raft.max_in_snapshot_log_to_keep` | `u64` | `1000` | >=0 | No | Committed Raft log entries kept after a snapshot (Bug BP-1; keeps the on-disk log small). |
| `node_liveness_secs` | `raft.node_liveness_secs` | `u64` | `30` | >=5 | No | A node whose `last_seen` is older than this is projected `Suspect` (and `Down` past 4x) in the read API, regardless of stored health (Bug BP-3). |

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
| `chunk_size_mb` | `defaults.chunk_size_mb` | `u32` | `4` | 1--1024 | Yes | Default chunk size in megabytes for new volumes. 4 MB is the workspace default (streaming put + memory budget make small chunks win: faster migrations, granular retries, finer cache). FUSE clients must use the same value (`client.chunk_size_mb`). |
| `anchor_datacenter` | `defaults.anchor_datacenter` | `Option<String>` | unset | DC name | Yes | When set, every chunk placement guarantees at least one replica in this datacenter (the anchor invariant). Unset = no anchor enforcement. |

### `[defaults.write_safety]`

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `min_replicas_before_ack` | `defaults.write_safety.min_replicas_before_ack` | `u32` | `2` | 1--replication factor | Yes | Minimum number of replicas that must acknowledge a write before the client receives success. Setting this to 1 gives lower latency but risks data loss on node failure. |
| `write_timeout_secs` | `defaults.write_safety.write_timeout_secs` | `u32` | `30` | 1--3600 | Yes | Maximum time in seconds to wait for write acknowledgements before returning an error to the client. |

> **Topology prerequisite.** `min_replicas_before_ack = N` is only a durable
> guarantee if the cluster can *keep* N replicas: each counted replica needs a
> tier with disks AND a maintainer (`mandatory_replicas` or a firing cache
> promotion — see [Replication](../storage/replication.md)). If the Nth
> replica lands on a tier nothing maintains, the ack is satisfied by a copy
> that is later dropped and the file settles below N.

### `[defaults.replication]`

| Option | TOML Path | Type | Default | Valid Range | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|-------------|
| `hot` | `defaults.replication.hot` | `u32` | `3` | 0--9 | Yes | Default replication factor for hot-tier data. |
| `warm` | `defaults.replication.warm` | `u32` | `2` | 0--9 | Yes | Default replication factor for warm-tier data. |
| `cold` | `defaults.replication.cold` | `u32` | `1` | 0--9 | Yes | Default replication factor for cold-tier data. |
| `tape` | `defaults.replication.tape` | `u32` | `1` | 0--9 | Yes | Default replication factor for tape-tier data. |

> **A replication count alone creates nothing.** A tier's copies exist only if
> something maintains them: the under-replication scheduler (driven by the
> volume's `mandatory_replicas.<tier>`) or the tiering engine (cache replicas,
> only when `promote_reads > 0` fires). `replication.warm = 1` with
> `mandatory_replicas.warm = 0` and `promote_reads = 0` yields **zero** warm
> copies. Details: [Replication](../storage/replication.md).

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
| `enable_cache_redundancy_escalation` | `tiering.enable_cache_redundancy_escalation` | `bool` | `false` | `true` or `false` | Yes | Opt-in (Bug AV-4-5): let the cache planner ramp a chunk's cache replicas from `min` toward `max` once reads reach `2 x promote_reads`. |

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

## `[updater]` -- Self-Updater Source

Read by [`dtfs-cli update`](../api-reference/cli-reference.md#update), not
by the manager daemon (the daemon ignores the block). Both keys are
optional; the defaults apply when the block -- or the whole file -- is
absent. The same block is honoured in `data.toml` and `fuse.toml`;
`dtfs-cli update <target>` reads `/etc/dtfs/<role>.toml` by default
(override with `--config`).

| Option | TOML Path | Type | Default | Live Reload | Description |
|--------|-----------|------|---------|-------------|-------------|
| `repo_url` | `updater.repo_url` | `String` | the canonical DTFS repo | No | GitHub repo the release tarball is pulled from (`<repo_url>/releases/{latest/download,download/<tag>}/dtfs-linux-amd64.tar.gz`). Overridden by `--repo-url` and the `DTFS_REPO_URL` env var. |
| `install_dir` | `updater.install_dir` | `String` | `/usr/local/bin` | No | Directory binaries are installed into. Overridden by `--install-dir`. |

### Example

```toml
[updater]
repo_url    = "https://github.com/lischetzke/dtfs"
install_dir = "/usr/local/bin"
```

---

## `[scrub]` -- Cluster-Wide Scrub & Integrity Policy

Optional block (Bug BC). Propagated to every data node via heartbeat,
overriding their local `[scrub]` fallback.

| Option | TOML Path | Type | Default | Description |
|--------|-----------|------|---------|-------------|
| `quick_interval_secs` | `scrub.quick_interval_secs` | `u64` | `21600` (6 h) | Quick scrub cadence (stat + size + meta presence). |
| `deep_interval_secs` | `scrub.deep_interval_secs` | `u64` | `604800` (7 d) | Deep scrub cadence (full BLAKE3 verify). |
| `orphan_interval_secs` | `scrub.orphan_interval_secs` | `u64` | `86400` (1 d) | Orphan cross-check (data-node `ListChunks` vs metadata). |
| `orphan_grace_secs` | `scrub.orphan_grace_secs` | `u64` | `86400` (1 d) | Grace before reaping an orphan, measured against the chunk's on-disk mtime (Bug CB). |
| `max_io_percent` | `scrub.max_io_percent` | `u32` | `10` | Deep-scrub I/O throttle. |
| `verify_on_read` | `scrub.verify_on_read` | `bool` | `true` | Hash-verify every read before bytes leave the data node. Disabling requires an explicit API acknowledgement and is the only mode where reads can return corrupt bytes. |
| `quarantine_on_fault` | `scrub.quarantine_on_fault` | `bool` | `true` | A single integrity fault quarantines the faulting node immediately. |
| `auto_unquarantine_secs` | `scrub.auto_unquarantine_secs` | `u64` | `0` | Cooldown for automatic unquarantine. `0` = manual only. |

---

## `[alerts]`, `[audit]`, `[placement]` -- Optional Blocks

| Block | Option | Default | Description |
|-------|--------|---------|-------------|
| `[[alerts.webhooks]]` | `url`, `secret`, `kinds`, `dedup_secs` | — / `""` / `[]` (all) / `300` | HTTP alert sinks (P1-8). |
| `[audit]` | `replicate_via_raft` | `false` | Replicate audit entries via Raft so every manager serves the same `/api/v1/audit` view (Bug AH). Enable only after **all** managers run a binary that understands it — see the rollout recipe in `crates/dtfs-common/src/config.rs`. |
| `[placement]` | `min_free_bytes` | `1073741824` (1 GiB) | Per-disk free-space reserve (Bug BU). |
| | `min_free_pct` | `5.0` | Reserve = `max(min_free_bytes, total * pct%)`. |
| | `allow_capacity_spill` | `true` | Spill a write to the fastest tier with room when the mandated tier is full, instead of failing it (Bug BX; the anchor copy is never spilled). `false` = fail-loud. |

---

## Complete Example

The canonical, exhaustive example (every option, optional ones commented out
with their defaults) is checked in at `deploy/examples/manager.toml`. It is
parse-tested against the config structs (`dtfs-common`
`example_manager_toml_parses`), so it cannot silently drift from the code.
