# Live Reload

DTFS supports live configuration reload for a subset of options. Changes to these options take effect without restarting any process. This page explains which options support live reload, how the mechanism works, and how to implement the `Reconfigurable` trait for new modules.

## Options That Support Live Reload

The following tables summarize every option that can be changed at runtime. Options not listed here require a process restart.

### Manager Node (`manager.toml`)

| Section | Option | TOML Path |
|---------|--------|-----------|
| `[api]` | `cors_origins` | `api.cors_origins` |
| `[defaults]` | `chunk_size_mb` | `defaults.chunk_size_mb` |
| `[defaults.write_safety]` | `min_replicas_before_ack` | `defaults.write_safety.min_replicas_before_ack` |
| `[defaults.write_safety]` | `write_timeout_secs` | `defaults.write_safety.write_timeout_secs` |
| `[defaults.replication]` | `hot` | `defaults.replication.hot` |
| `[defaults.replication]` | `warm` | `defaults.replication.warm` |
| `[defaults.replication]` | `cold` | `defaults.replication.cold` |
| `[tiering]` | `enabled` | `tiering.enabled` |
| `[tiering]` | `hot_threshold_reads_per_hour` | `tiering.hot_threshold_reads_per_hour` |
| `[tiering]` | `cold_threshold_days` | `tiering.cold_threshold_days` |
| `[tiering]` | `frozen_threshold_days` | `tiering.frozen_threshold_days` |
| `[tiering]` | `migration_bandwidth_limit_mbps` | `tiering.migration_bandwidth_limit_mbps` |
| `[tiering]` | `evaluation_interval_secs` | `tiering.evaluation_interval_secs` |
| `[monitoring]` | `heartbeat_interval_secs` | `monitoring.heartbeat_interval_secs` |
| `[monitoring]` | `suspect_after_missed` | `monitoring.suspect_after_missed` |
| `[monitoring]` | `down_after_missed` | `monitoring.down_after_missed` |

### Data Node (`data.toml`)

| Section | Option | TOML Path |
|---------|--------|-----------|
| `[cache]` | `max_memory_mb` | `cache.max_memory_mb` |
| `[cache]` | `policy` | `cache.policy` |
| `[scrub]` | `quick_interval_hours` | `scrub.quick_interval_hours` |
| `[scrub]` | `deep_interval_hours` | `scrub.deep_interval_hours` |
| `[scrub]` | `quick_cron` | `scrub.quick_cron` |
| `[scrub]` | `deep_cron` | `scrub.deep_cron` |
| `[scrub]` | `max_io_percent` | `scrub.max_io_percent` |

### FUSE Client (`fuse.toml`)

| Section | Option | TOML Path |
|---------|--------|-----------|
| `[readahead]` | `mode` | `readahead.mode` |
| `[readahead]` | `prefetch_chunks` | `readahead.prefetch_chunks` |
| `[readahead]` | `prefetch_next_files` | `readahead.prefetch_next_files` |
| `[cache]` | `local_cache_max_gb` | `cache.local_cache_max_gb` |
| `[partition_safety]` | `mode` | `partition_safety.mode` |
| `[partition_safety]` | `quorum_loss_timeout_secs` | `partition_safety.quorum_loss_timeout_secs` |
| `[partition_safety]` | `show_warning` | `partition_safety.show_warning` |

### Options That Do NOT Support Live Reload

The following categories of options require a process restart to take effect:

- **Node identity** -- `[node]` section (id, hostname, data_dir)
- **Topology location** -- `[location]` section
- **Cluster membership** -- `[cluster]` section (name, shared_secret, bootstrap, manager_addr)
- **Network listeners** -- `[raft]`, `[grpc]`, and `[api]` listen addresses
- **TLS** -- `[tls]` section (mode, cert_file, key_file, ca_file)
- **Disk layout** -- `[[disks]]` section (path, tier)
- **Mount point** -- `[mount]` section
- **Cache directory** -- `cache.local_cache_dir` (the path itself; the size limit is reloadable)

## How Live Reload Works

DTFS uses two mechanisms for live configuration reload, depending on where the setting lives.

### Cluster-wide Settings (via Raft)

Cluster-wide settings (defaults, tiering, monitoring, and per-volume configuration) are stored in the Raft-replicated state machine as a `VersionedConfig<T>`. Each update increments a monotonic `ConfigVersion`.

The reload flow is:

1. An administrator changes a setting via the REST API or Web UI.
2. The leader manager proposes the change as a Raft log entry.
3. Once committed, the new configuration is stored in the state machine.
4. The manager pushes the new configuration to affected nodes over gRPC.
5. Each node calls `apply_config()` on the relevant module.
6. The node reports its applied `ConfigVersion` back to the manager.
7. The `ConfigVersionTracker` records per-node versions so operators can monitor rollout progress.

### Local Settings (via File Watch)

Some local TOML settings (such as cache policy on a data node or readahead mode on a FUSE client) are reloadable by sending `SIGHUP` to the process or by editing the TOML file while file-watching is active. On receiving the signal, the process re-reads the TOML file and calls `apply_config()` on each affected module.

## The `Reconfigurable` Trait

Every module that supports live reload implements the `Reconfigurable` trait, defined in `dtfs-common::reconfig`:

```rust
#[async_trait]
pub trait Reconfigurable: Send + Sync {
    /// The specific configuration type this module uses.
    type Config: Send + Sync;

    /// Apply new configuration without restart.
    async fn apply_config(&self, config: &Self::Config) -> Result<()>;

    /// Get the current configuration.
    async fn current_config(&self) -> Self::Config;
}
```

### Contract

- **Non-blocking**: `apply_config()` must not block the calling task. If applying the configuration requires heavy work (such as resizing a cache), that work should be spawned as a background task.
- **Safe under load**: `apply_config()` may be called while the module is actively serving requests. Implementations must handle concurrent access, typically by using interior mutability (`RwLock`, `watch` channel, or atomic swap).
- **Idempotent**: Applying the same configuration twice must have no adverse effect.
- **Atomic rollback**: If `apply_config()` fails, the module must remain in its previous configuration. The error is propagated to the caller so the operator is informed.

### `VersionedConfig<T>`

Cluster-wide configuration is wrapped in `VersionedConfig<T>`, which pairs a `ConfigVersion` (monotonic `u64`) with the configuration payload:

```rust
pub struct VersionedConfig<T> {
    pub version: ConfigVersion,
    pub config: T,
}
```

Nodes compare the incoming version against their local version to detect stale or duplicate pushes. A node only calls `apply_config()` when the incoming version is strictly greater than its current version.

### `ConfigVersionTracker`

The manager maintains a `ConfigVersionTracker` that records which `ConfigVersion` each node has applied:

```rust
pub struct ConfigVersionTracker {
    pub node_versions: BTreeMap<NodeId, ConfigVersion>,
}
```

This allows operators to query the REST API (`GET /api/v1/config/version`) to see whether all nodes have converged on the latest configuration, and which nodes are still behind.

## Triggering a Reload

There are three ways to trigger a live reload:

1. **REST API / Web UI** -- Change a cluster-wide setting. The manager handles distribution automatically.
2. **SIGHUP** -- Send `SIGHUP` to a manager, data node, or FUSE client process to re-read the local TOML file and apply any changed reloadable options.
3. **CLI** -- Use `dtfsctl config reload` to send a reload signal to a local DTFS process.

## Verifying a Reload

After triggering a reload, verify that the new configuration has been applied:

- **Cluster-wide**: Query `GET /api/v1/config/version` and confirm all nodes report the latest version.
- **Local**: Check the process logs for a confirmation message, or query the node's status endpoint.
