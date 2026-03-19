# Alerting

This chapter covers configuring alerts for DTFS cluster events and integrating with external notification systems.

## Alert Sources

DTFS alerts can be triggered from two sources:

1. **Prometheus alerting rules** -- Based on the metrics exposed by manager and data nodes (see [Monitoring](./monitoring.md)).
2. **Webhook notifications** -- Built-in event-driven notifications from the manager for critical cluster events.

## Prometheus Alerting Rules

### Recommended Alert Rules

The following Prometheus alerting rules cover the most important failure scenarios. Add these to your Prometheus alerting configuration.

#### Node Health

```yaml
groups:
  - name: dtfs-node-health
    rules:
      - alert: DtfsNodeSuspect
        expr: dtfs_cluster_data_nodes_healthy < dtfs_cluster_data_nodes_total
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "One or more DTFS data nodes are not healthy"
          description: "{{ $value }} of {{ $labels.total }} nodes are unhealthy"

      - alert: DtfsNodeDown
        expr: dtfs_cluster_data_nodes_healthy < dtfs_cluster_data_nodes_total
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "DTFS data node has been down for more than 5 minutes"
```

#### Disk Capacity

```yaml
      - alert: DtfsDiskAlmostFull
        expr: (dtfs_data_disk_used_bytes / dtfs_data_disk_total_bytes) > 0.80
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "DTFS disk {{ $labels.mount_path }} is above 80% capacity"

      - alert: DtfsDiskCriticallyFull
        expr: (dtfs_data_disk_used_bytes / dtfs_data_disk_total_bytes) > 0.90
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "DTFS disk {{ $labels.mount_path }} is above 90% capacity"
```

#### Data Integrity

```yaml
      - alert: DtfsScrubCorruptChunks
        expr: increase(dtfs_scrub_chunks_corrupt_total[1h]) > 0
        labels:
          severity: critical
        annotations:
          summary: "DTFS scrub found corrupt chunks on {{ $labels.instance }}"

      - alert: DtfsScrubNotRunning
        expr: time() - dtfs_scrub_last_timestamp > 86400
        for: 1h
        labels:
          severity: warning
        annotations:
          summary: "DTFS scrub has not run in over 24 hours on {{ $labels.instance }}"
```

#### Cache Performance

```yaml
      - alert: DtfsCacheHitRatioLow
        expr: dtfs_data_cache_hit_ratio < 0.50
        for: 30m
        labels:
          severity: warning
        annotations:
          summary: "DTFS cache hit ratio below 50% on {{ $labels.instance }}"
```

#### Raft Consensus

```yaml
      - alert: DtfsRaftLeaderFlapping
        expr: changes(dtfs_raft_leader[10m]) > 2
        labels:
          severity: warning
        annotations:
          summary: "DTFS Raft leadership is unstable"

      - alert: DtfsRaftLogLag
        expr: (dtfs_raft_committed_index - dtfs_raft_applied_index) > 1000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "DTFS Raft log application is lagging behind"
```

#### Cluster Capacity

```yaml
      - alert: DtfsClusterCapacityWarning
        expr: (dtfs_cluster_capacity_used_bytes / dtfs_cluster_capacity_total_bytes) > 0.75
        for: 1h
        labels:
          severity: warning
        annotations:
          summary: "DTFS cluster is above 75% total capacity"

      - alert: DtfsClusterCapacityCritical
        expr: (dtfs_cluster_capacity_used_bytes / dtfs_cluster_capacity_total_bytes) > 0.90
        for: 30m
        labels:
          severity: critical
        annotations:
          summary: "DTFS cluster is above 90% total capacity"
```

## Webhook Notifications

The manager can send HTTP webhook notifications for critical cluster events. Configure webhooks in the manager configuration:

```toml
[monitoring.webhooks]
enabled = true
endpoints = [
    "https://hooks.example.com/dtfs-alerts",
    "https://slack.example.com/webhook/dtfs",
]
timeout_secs = 10
retry_count = 3
```

### Webhook Payload Format

Webhook notifications are sent as HTTP POST requests with a JSON payload:

```json
{
  "event": "node.down",
  "timestamp": "2025-01-15T10:30:00Z",
  "cluster": "production",
  "severity": "critical",
  "details": {
    "node_id": "abc123",
    "hostname": "data-03.example.com",
    "last_heartbeat": "2025-01-15T10:29:30Z"
  },
  "message": "Data node data-03.example.com is down (missed 6 heartbeats)"
}
```

### Supported Webhook Events

| Event | Severity | Description |
|-------|----------|-------------|
| `node.suspect` | warning | Node missed heartbeats, marked suspect |
| `node.down` | critical | Node confirmed down |
| `node.recovered` | info | Previously down node has recovered |
| `node.joined` | info | New node joined the cluster |
| `node.maintenance` | info | Node entered/exited maintenance mode |
| `disk.failed` | critical | Disk failure detected |
| `disk.degraded` | warning | Disk showing errors but still functional |
| `scrub.corrupt` | critical | Scrub found corrupt chunks |
| `scrub.completed` | info | Scrub completed successfully |
| `raft.leader_change` | info | Raft leadership transferred |
| `capacity.warning` | warning | Disk or cluster capacity threshold exceeded |
| `config.updated` | info | Cluster configuration changed |

## Integration Examples

### Slack

Use a Slack incoming webhook URL as a webhook endpoint. The JSON payload will appear as a message in the configured channel.

For richer formatting, use a middleware service that transforms the DTFS webhook payload into Slack Block Kit format.

### PagerDuty

Route critical alerts through Prometheus Alertmanager to PagerDuty:

```yaml
# alertmanager.yml
receivers:
  - name: 'pagerduty-critical'
    pagerduty_configs:
      - routing_key: '<your-pagerduty-key>'
        severity: '{{ .CommonLabels.severity }}'

route:
  receiver: 'pagerduty-critical'
  routes:
    - match:
        severity: critical
      receiver: 'pagerduty-critical'
```

### Email

Configure Prometheus Alertmanager for email delivery:

```yaml
receivers:
  - name: 'email-team'
    email_configs:
      - to: 'storage-team@example.com'
        from: 'dtfs-alerts@example.com'
        smarthost: 'smtp.example.com:587'
```

### Custom Integration

For custom alerting pipelines, consume the webhook endpoint with any HTTP server and process events programmatically. The JSON payload is stable and documented above.

## Alert Severity Guidelines

| Severity | Action | Response Time |
|----------|--------|---------------|
| `info` | Log for awareness | No immediate action required |
| `warning` | Investigate during business hours | Within 4 hours |
| `critical` | Immediate response required | Within 15 minutes |

## Silencing and Maintenance Windows

During planned maintenance, silence alerts to avoid false positives:

1. **Prometheus Alertmanager:** Create a silence in the Alertmanager UI or via API for the duration of the maintenance window.
2. **Webhook filtering:** Add maintenance window awareness to your webhook consumer to suppress notifications during known windows.
3. **Node-level:** Placing a node in maintenance mode (see [Maintenance Mode](./maintenance-mode.md)) is the correct way to signal planned downtime. Alert rules should account for the `Maintenance` health state.

## Best Practices

- Set up alerts before deploying to production. Do not wait for the first incident.
- Use the two-tier approach: Prometheus for metric-based alerts, webhooks for event-based alerts.
- Test alert delivery regularly by triggering a manual scrub or briefly placing a test node in maintenance mode.
- Keep alert noise low. Tune thresholds and durations to avoid alert fatigue.
- Include the cluster name in alert labels to distinguish between staging and production clusters.
