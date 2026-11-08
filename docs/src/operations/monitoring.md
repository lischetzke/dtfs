# Monitoring

DTFS exposes metrics and health endpoints for integration with monitoring systems such as Prometheus, Grafana, and custom dashboards.

## Health Endpoints

All on the manager REST port (default 9402), no authentication for reads.

| Endpoint | Meaning |
|----------|---------|
| `GET /healthz` | Liveness: `200 ok` whenever the process answers. |
| `GET /readyz` | Readiness: `200` only when the state machine has applied at least one log entry **and** at least one node is healthy; otherwise `503` with the reason. A fresh cluster with no data node yet is not ready. |
| `GET /api/v1/health` | `{"status":"healthy"}`, same liveness meaning. `dtfs-cli health` wraps it. |
| `GET /api/v1/raft/state` | Raft view of this manager: `running_state` (`ok` or `fatal: ...`), `state`, `is_leader`, `current_leader` (null = no quorum), `voters`, `learners`, `membership`, `applied_log_lag`, `millis_since_quorum_ack`, and on the leader per-peer `replication` lag. |
| `GET /api/v1/cluster/status` | Cluster summary, see below. |
| `GET /api/v1/nodes` | Array of nodes with `health`, `role`, `version`, `build_date`, `disk_count`, capacity and `last_seen_ms`. |

`/api/v1/cluster/status`:

```json
{
  "status": "ok",
  "cluster_name": "production",
  "node": "mgr-01",
  "data_nodes": 12,
  "data_nodes_healthy": 12,
  "files": 48293,
  "files_missing_chunks": 0,
  "anchor_pending": 0,
  "total_capacity": 107374182400,
  "used_capacity": 53687091200,
  "config_version": 42
}
```

`status` is `degraded` when any file has lost its committed data
(`files_missing_chunks > 0`) or fewer data nodes are healthy than registered.
Empty files and sparse files that were never written have no chunks and do
not count ([Glossary](../glossary.md)).
`anchor_pending` counts files whose anchor copy has not landed yet (asynchronous
convergence, not data loss, does not degrade `status`). The post-deploy gate
`scripts/dtfs-cluster-healthcheck.sh` asserts exactly these fields.

## Prometheus Metrics

Only the manager exposes `GET /metrics` (port 9402, text exposition format). Data
nodes and FUSE clients have no metrics endpoint in this revision; monitor them
through the manager's node table, `journalctl`, and host-level exporters
(node_exporter, SMART).

```yaml
scrape_configs:
  - job_name: 'dtfs-managers'
    static_configs:
      - targets: ['mgr-01:9402', 'mgr-02:9402', 'mgr-03:9402']
```

Metrics are computed from the local state machine, so every manager serves the same
cluster-wide values; scrape all of them to also detect a manager that is down or
lagging.

| Metric | Type | Description |
|--------|------|-------------|
| `dtfs_files_total` | gauge | Files tracked |
| `dtfs_chunks_total` | gauge | Chunk-location entries (replicas), not unique chunks |
| `dtfs_volumes_total` | gauge | Configured volumes |
| `dtfs_nodes_healthy` | gauge | Nodes in `Healthy` state (data nodes, managers registered in the node table, clients) |
| `dtfs_nodes_suspect` | gauge | Nodes `Suspect` |
| `dtfs_nodes_down` | gauge | Nodes `Down` |
| `dtfs_nodes_quarantined` | gauge | Nodes `Quarantined` after an integrity fault |
| `dtfs_cluster_capacity_bytes` | gauge | Total capacity reported by nodes |
| `dtfs_cluster_used_bytes` | gauge | Used capacity |
| `dtfs_write_leases_active` | gauge | Held single-writer leases |
| `dtfs_anchor_protected_total` | counter | Tiering decisions refused to protect the anchor |
| `dtfs_tier_migrations_completed_total` | counter | Promote and demote events in the in-memory audit ring |
| `dtfs_migration_failed_total` | counter | Migration tasks that exhausted retries (audit ring) |
| `dtfs_audit_log_in_memory` | gauge | Entries in the capped audit ring |

The last three counters are derived from a bounded audit ring, so they can drop
when old entries are evicted; alert on `increase()` over short windows, not on
absolute values. A node in `Maintenance` is counted in none of the node gauges.
Anything else (Raft lag, per-disk usage, scrub results) comes from the REST
endpoints above or `GET /api/v1/stats`.

## Node Health Monitoring

The manager tracks data node health through periodic heartbeats:

| Configuration | Default | Description |
|---------------|---------|-------------|
| `monitoring.heartbeat_interval_secs` | 5 | Heartbeat interval |
| `monitoring.suspect_after_missed` | 3 | Missed heartbeats before a node becomes suspect (15 s with defaults) |
| `monitoring.down_after_missed` | 6 | Missed heartbeats before node is marked down (30s default) |

### Health State Transitions

```
Healthy -> Suspect (after 3 missed heartbeats)
Suspect -> Healthy (heartbeat received)
Suspect -> Down    (after 6 total missed heartbeats)
Down    -> Healthy (node restarts and heartbeats resume)
```

Operator-set `Maintenance` and `Quarantined` (and `Draining`) are sticky: the monitor never marks such a node `Down`, and a heartbeat does not clear them. Only data nodes are tracked by the monitor.

When a node transitions to `Down`, the manager begins re-replicating its chunks to maintain the configured replication factor.

## Key Dashboards

Grafana panels worth building on the manager metrics and a small JSON poller of the
REST endpoints:

- **Cluster:** capacity used vs total, node health gauges, files and replicas,
  `anchor_pending`, `files_missing_chunks`.
- **Per node** (from `/api/v1/nodes` or `/api/v1/stats`): disk usage per disk,
  bytes read and written, last scrub result per level.
- **Raft:** which manager is leader (`/api/v1/raft/state`), `applied_log_lag`,
  term changes (rare in a healthy cluster), per-peer replication lag.
- **Host level (not DTFS):** disk SMART, free space, memory vs the systemd
  `MemoryMax` of each role, clock offset.

## Alerting Integration

See [Alerting](./alerting.md) for configuring alerts based on these metrics. Common alert conditions include:

- Node health transitions (Healthy -> Suspect -> Down).
- Disk capacity exceeding 80% or 90%.
- Scrub discovering corrupt chunks (a quarantined node: `dtfs_nodes_quarantined > 0`).
- Raft leader changes (flapping).

## WebUI

The manager includes a built-in WebUI served on the same port as the REST API (default: `9402`). The WebUI provides a visual cluster dashboard with:

- Real-time node and disk status.
- File browser.
- Configuration viewer.
- Audit log viewer.

Access the WebUI at `http://<manager>:9402/` in a web browser.

## Log Monitoring

The daemons log through `tracing` in the default text format to stdout, so under
systemd the logs are in the journal (`journalctl -u dtfs-manager -u dtfs-data`).
Verbosity is set with `RUST_LOG` (units default to `dtfs=info`; override in a
drop-in). JSON output is not implemented in this revision, see
[Log Shipping](./log-shipping.md).
