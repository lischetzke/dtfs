# Monitoring

DTFS exposes metrics and health endpoints for integration with monitoring systems such as Prometheus, Grafana, and custom dashboards.

## Health Endpoints

### Manager Health

```
GET /api/v1/health
```

Returns a simple health check response:

```json
{"status": "healthy"}
```

Returns HTTP 200 when the manager is operational. Use this endpoint for load balancer health checks and uptime monitors.

### Cluster Status

```
GET /api/v1/cluster/status
```

Returns a comprehensive cluster overview:

```json
{
  "status": "ok",
  "cluster_name": "production",
  "node": "mgr-01.example.com",
  "data_nodes": 12,
  "data_nodes_healthy": 12,
  "files": 48293,
  "total_capacity": 107374182400,
  "used_capacity": 53687091200,
  "config_version": 42
}
```

### CLI Health Check

```bash
dtfs-cli health
```

Exits with code 0 if the manager is healthy, code 1 otherwise. Suitable for use in monitoring scripts.

## Prometheus Metrics

DTFS uses the `prometheus-client` library to expose metrics in the OpenMetrics/Prometheus exposition format.

### Metrics Endpoint

Each node type exposes metrics on its HTTP port:

- **Manager:** `http://<manager>:9402/metrics`
- **Data node:** `http://<data-node>:9500/metrics` (if the data node HTTP endpoint is enabled)

### Prometheus Scrape Configuration

```yaml
scrape_configs:
  - job_name: 'dtfs-managers'
    static_configs:
      - targets:
        - 'mgr-01:9402'
        - 'mgr-02:9402'
        - 'mgr-03:9402'

  - job_name: 'dtfs-data-nodes'
    static_configs:
      - targets:
        - 'data-01:9500'
        - 'data-02:9500'
        - 'data-03:9500'
```

For dynamic environments, use service discovery instead of static targets.

### Manager Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `dtfs_cluster_data_nodes_total` | Gauge | Total registered data nodes |
| `dtfs_cluster_data_nodes_healthy` | Gauge | Data nodes in healthy state |
| `dtfs_cluster_files_total` | Gauge | Total files tracked |
| `dtfs_cluster_capacity_total_bytes` | Gauge | Aggregate cluster capacity |
| `dtfs_cluster_capacity_used_bytes` | Gauge | Aggregate cluster usage |
| `dtfs_raft_term` | Gauge | Current Raft term |
| `dtfs_raft_committed_index` | Gauge | Last committed log index |
| `dtfs_raft_applied_index` | Gauge | Last applied log index |
| `dtfs_raft_leader` | Gauge | 1 if this node is leader, 0 otherwise |
| `dtfs_api_requests_total` | Counter | REST API requests by endpoint and status |
| `dtfs_api_request_duration_seconds` | Histogram | REST API request latency |
| `dtfs_tiering_promotions_total` | Counter | Chunks promoted to a hotter tier |
| `dtfs_tiering_demotions_total` | Counter | Chunks demoted to a colder tier |
| `dtfs_config_version` | Gauge | Current cluster configuration version |

### Data Node Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `dtfs_data_reads_total` | Counter | Total read operations |
| `dtfs_data_writes_total` | Counter | Total write operations |
| `dtfs_data_bytes_read_total` | Counter | Total bytes read |
| `dtfs_data_bytes_written_total` | Counter | Total bytes written |
| `dtfs_data_cache_hits_total` | Counter | ARC cache hits |
| `dtfs_data_cache_misses_total` | Counter | ARC cache misses |
| `dtfs_data_cache_hit_ratio` | Gauge | Cache hit ratio (hits / (hits + misses)) |
| `dtfs_data_chunks_total` | Gauge | Total chunks stored |
| `dtfs_data_disk_total_bytes` | Gauge | Per-disk total capacity (labels: `disk_id`, `tier`, `mount_path`) |
| `dtfs_data_disk_used_bytes` | Gauge | Per-disk used capacity |
| `dtfs_data_disk_status` | Gauge | Per-disk status (1=healthy, 0=other) |
| `dtfs_scrub_last_duration_seconds` | Gauge | Duration of last scrub |
| `dtfs_scrub_chunks_checked_total` | Counter | Chunks checked across all scrubs |
| `dtfs_scrub_chunks_corrupt_total` | Counter | Corrupt chunks found |
| `dtfs_scrub_running` | Gauge | 1 if a scrub is currently running |

## Node Health Monitoring

The manager tracks data node health through periodic heartbeats:

| Configuration | Default | Description |
|---------------|---------|-------------|
| `monitoring.heartbeat_interval_secs` | 5 | Heartbeat interval |
| `monitoring.suspect_after_missed` | 3 | Missed heartbeats before node becomes suspect (15s default) |
| `monitoring.down_after_missed` | 6 | Missed heartbeats before node is marked down (30s default) |

### Health State Transitions

```
Healthy -> Suspect (after 3 missed heartbeats)
Suspect -> Healthy (heartbeat received)
Suspect -> Down    (after 6 total missed heartbeats)
Down    -> Healthy (node restarts and heartbeats resume)
```

When a node transitions to `Down`, the manager begins re-replicating its chunks to maintain the configured replication factor.

## Key Dashboards

### Cluster Overview Dashboard

Recommended panels for a Grafana cluster overview:

- **Cluster capacity** -- Stacked gauge of total vs. used capacity.
- **Node health** -- Table of all nodes with color-coded health status.
- **File count** -- Time series of total files.
- **Throughput** -- Aggregate read/write bytes per second across all data nodes.
- **Cache hit ratio** -- Per-node cache effectiveness.

### Per-Node Dashboard

- **Disk utilization** -- Per-disk capacity bars.
- **I/O rates** -- Reads/writes per second.
- **Cache performance** -- Hit ratio, T1/T2 sizes.
- **Scrub status** -- One card per node and scrub level (quick / deep):
  completion time, duration, and chunks checked / healthy / corrupt /
  missing. Quick and deep results are listed separately so the latest of
  each level is always visible.

### Raft Dashboard

- **Leader status** -- Which node is the current leader.
- **Log lag** -- Difference between committed and applied index.
- **Term changes** -- Rate of elections (should be rare in a healthy cluster).

## Alerting Integration

See [Alerting](./alerting.md) for configuring alerts based on these metrics. Common alert conditions include:

- Node health transitions (Healthy -> Suspect -> Down).
- Disk capacity exceeding 80% or 90%.
- Scrub discovering corrupt chunks.
- Cache hit ratio dropping below a threshold.
- Raft leader changes (flapping).

## WebUI

The manager includes a built-in WebUI served on the same port as the REST API (default: `9402`). The WebUI provides a visual cluster dashboard with:

- Real-time node and disk status.
- File browser.
- Configuration viewer.
- Audit log viewer.

Access the WebUI at `http://<manager>:9402/` in a web browser.

## Log Monitoring

DTFS uses structured logging via the `tracing` framework. Logs are output in JSON format when configured:

```toml
# In manager.toml or data.toml (environment variable)
RUST_LOG=dtfs_manager=info,dtfs_raft=info
```

JSON log output can be ingested by log aggregation systems such as Elasticsearch, Loki, or Splunk for centralized monitoring.
