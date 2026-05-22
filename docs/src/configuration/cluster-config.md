# Cluster-wide Configuration

Cluster-wide settings are managed through the Web UI or REST API and stored in the Raft-replicated state machine. Unlike per-node TOML files, these settings are centrally managed and automatically distributed to all nodes in the cluster.

All cluster-wide settings support [live reload](./live-reload.md). When a setting is changed, the manager increments the `ConfigVersion`, replicates the new configuration through Raft, and pushes it to all affected nodes.

## Default Volume Settings

These settings control the defaults applied when new volumes are created. They mirror the `[defaults]` section of the manager TOML file but take precedence over it once the cluster is running.

| Setting | Type | Default | Valid Range | Description |
|---------|------|---------|-------------|-------------|
| `chunk_size_mb` | `u32` | `128` | 1--1024 | Default chunk size in megabytes for newly created volumes. |
| `write_safety.min_replicas_before_ack` | `u32` | `2` | 1--replication factor | Minimum replicas that must acknowledge a write before the client receives success. |
| `write_safety.write_timeout_secs` | `u32` | `30` | 1--3600 | Maximum seconds to wait for write acknowledgements. |
| `replication.hot` | `u32` | `3` | 1--9 | Default replication factor for hot-tier data. |
| `replication.warm` | `u32` | `2` | 1--9 | Default replication factor for warm-tier data. |
| `replication.cold` | `u32` | `1` | 1--9 | Default replication factor for cold-tier data. |

Changing these defaults does not retroactively modify existing volumes. Use the per-volume configuration to update individual volumes.

## Tiering Settings

Controls the cluster-wide automatic data tiering engine.

| Setting | Type | Default | Valid Range | Description |
|---------|------|---------|-------------|-------------|
| `tiering.enabled` | `bool` | `true` | `true` or `false` | Enable or disable the automatic tiering engine across the cluster. |
| `tiering.hot_threshold_reads_per_hour` | `u32` | `5` | 1--10000 | Minimum reads per hour for data to remain classified as hot. |
| `tiering.cold_threshold_days` | `u32` | `14` | 1--3650 | Days without access before data is reclassified as cold. |
| `tiering.frozen_threshold_days` | `u32` | `60` | 1--3650 | Days without access before data is reclassified as frozen. |
| `tiering.migration_bandwidth_limit_mbps` | `u32` | `200` | 0--10000 | Maximum bandwidth in Mbps for tier migration traffic. `0` means unlimited. |
| `tiering.evaluation_interval_secs` | `u64` | `3600` | 60--86400 | Seconds between tiering evaluation cycles. |
| `tiering.enable_cache_redundancy_escalation` | `bool` | `false` | `true` or `false` | Bug AV (AV-4-5): opt-in for the cache-tier redundancy-escalation pass. When `true` the planner may ramp cache replicas past `cache_replicas.<tier>.min` toward `.max` for chunks with sustained read pressure (>= `2 * promote_reads`). Leave `false` unless per-DC HA on warm needs the extra copy. |

## Monitoring Settings

Controls health monitoring thresholds for data nodes.

| Setting | Type | Default | Valid Range | Description |
|---------|------|---------|-------------|-------------|
| `monitoring.heartbeat_interval_secs` | `u64` | `5` | 1--300 | Expected heartbeat interval in seconds. |
| `monitoring.suspect_after_missed` | `u32` | `3` | 1--100 | Missed heartbeats before a node is marked suspect. |
| `monitoring.down_after_missed` | `u32` | `6` | 1--100 | Missed heartbeats before a node is marked down and re-replication begins. |

## How Cluster Config Is Applied

1. An administrator changes a setting via the Web UI or REST API.
2. The leader manager proposes the change as a Raft log entry.
3. Once committed, the new `VersionedConfig` is stored in the replicated state machine.
4. The manager pushes the updated configuration to each affected node via gRPC.
5. Each node calls `apply_config()` on the relevant module (see [Live Reload](./live-reload.md)).
6. Nodes report their applied `ConfigVersion` back to the manager.
7. The `ConfigVersionTracker` records which version each node has applied, allowing the Web UI to show rollout progress.

## REST API

Cluster configuration can be read and modified through the REST API:

- `GET /api/v1/config` -- Returns the current cluster-wide configuration and its version number.
- `PUT /api/v1/config` -- Submits a partial or full configuration update. Only the fields included in the request body are changed; omitted fields retain their current values.
- `GET /api/v1/config/version` -- Returns the current `ConfigVersion` and a map of each node's applied version.

## Relationship to TOML Files

The manager TOML file (`manager.toml`) provides initial seed values for cluster-wide settings. Once the cluster is bootstrapped and running:

- Cluster-wide settings stored in Raft take precedence over the TOML file values.
- The TOML file is only re-read on process startup and is used to populate the initial Raft state if no existing state is found.
- Per-node settings (such as `[node]`, `[location]`, `[raft]`, `[grpc]`, and `[tls]`) always come from the local TOML file and are never overridden by cluster config.
