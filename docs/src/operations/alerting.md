# Alerting

This chapter covers alerts for DTFS cluster events and how to wire them to
notification systems.

## Alert Sources

1. **Webhook sinks** built into the manager: HMAC-signed JSON POSTs for a small
   set of critical events (below).
2. **Prometheus rules** over the manager's `/metrics` and, via a blackbox or JSON
   exporter, the REST health endpoints ([Monitoring](./monitoring.md)). Data nodes
   have no metrics endpoint, so per-disk and per-node conditions come from the
   manager's node table or from host exporters.

## Webhook Sinks

Configured under `[alerts]` in `manager.toml` (one `[[alerts.webhooks]]` per
sink; the block may be omitted):

```toml
[alerts]
[[alerts.webhooks]]
url = "https://hooks.example.com/dtfs"
secret = "per-sink-hmac-secret"      # signs the body
kinds = []                           # empty = all kinds, or e.g. ["node.down", "anchor.violated"]
dedup_secs = 300                     # suppress repeats of one kind to this sink
```

Each event is a POST with `content-type: application/json` and the header
`x-dtfs-signature: <hex HMAC-SHA256 of the raw body keyed by the sink secret>`;
verify it in the receiver. Delivery is best effort with a 5 s timeout: there is no
retry queue in this revision.

Payload:

```json
{
  "kind": "node.down",
  "cluster": "production",
  "manager": "mgr-01",
  "timestamp_ms": 1736937000000,
  "summary": "Data node data-03 is down",
  "details": {}
}
```

### Alert kinds

| `kind` | Meaning | Emitted by this revision |
|--------|---------|--------------------------|
| `node.down` | A data node was marked `Down` | yes (monitor) |
| `anchor.violated` | The tiering or flush planner refused or detected a change that would strand a chunk's anchor copy | yes (tiering, flush) |
| `migration.failed` | A migration task exhausted its retries | yes (scheduler) |
| `disk.failed` | Disk failure | defined, no emitter yet |
| `scrub.corruption` | Scrub found corruption | defined, no emitter yet |
| `quorum.lost` | Raft quorum lost | defined, no emitter yet |
| `lease.gc.spike` | Unusual lease garbage collection | defined, no emitter yet |

Only the first three fire today. For the rest use the metrics and REST checks
below. Only the leader runs the scheduler and monitor, so a sink receives events
from whichever manager is leader at the time; configure the same sinks on every
manager.

## Prometheus Rules

Rules over the metrics that exist (`dtfs_nodes_*`, `dtfs_cluster_*_bytes`,
`dtfs_anchor_protected_total`, `dtfs_migration_failed_total`). Scrape every
manager.

```yaml
groups:
  - name: dtfs
    rules:
      - alert: DtfsNodeDown
        expr: max(dtfs_nodes_down) > 0
        for: 2m
        labels: {severity: critical}
        annotations: {summary: "A DTFS node is Down"}

      - alert: DtfsNodeSuspect
        expr: max(dtfs_nodes_suspect) > 0
        for: 5m
        labels: {severity: warning}

      - alert: DtfsNodeQuarantined
        expr: max(dtfs_nodes_quarantined) > 0
        labels: {severity: critical}
        annotations: {summary: "A node self-quarantined after an integrity fault"}

      - alert: DtfsClusterCapacityHigh
        expr: max(dtfs_cluster_used_bytes) / max(dtfs_cluster_capacity_bytes) > 0.80
        for: 1h
        labels: {severity: warning}

      - alert: DtfsClusterCapacityCritical
        expr: max(dtfs_cluster_used_bytes) / max(dtfs_cluster_capacity_bytes) > 0.90
        for: 30m
        labels: {severity: critical}

      - alert: DtfsMigrationFailures
        expr: increase(dtfs_migration_failed_total[1h]) > 0
        labels: {severity: warning}

      - alert: DtfsManagerUnreachable
        expr: up{job="dtfs-managers"} == 0
        for: 2m
        labels: {severity: critical}
```

Conditions that have no metric are checked against the REST API (for example with
a blackbox or JSON exporter or a cron job):

| Condition | Check |
|-----------|-------|
| No Raft leader | `GET /api/v1/raft/state`: `current_leader == null` on any manager |
| Raft lag | `applied_log_lag` above a few hundred, or a peer's `replication[].lag` |
| Data loss signal | `GET /api/v1/cluster/status`: `files_missing_chunks > 0` (files whose committed data is gone; empty files do not count), or `status != "ok"` |
| Not ready | `GET /readyz` returns 503 |
| Anchor copy missing | `anchor_pending` stays above zero for long |
| Auth probing | audit rows `auth.denied` (see below) |

Auth failures are written to the audit log as `auth.denied`, coalesced to one row
per (source, path) per window with a count of the suppressed repeats, so a flood of
bad requests cannot evict real history; alert on the presence of such rows from
unexpected sources, not on their count.

## Integration Examples

- **Slack / chat:** point a sink at a small relay that verifies `x-dtfs-signature`
  and reformats `summary` for the chat API.
- **PagerDuty / email:** route Prometheus alerts with a `severity` label through
  Alertmanager receivers (`pagerduty_configs`, `email_configs`).
- **Custom:** any HTTP server that checks the signature and acts on `kind`.

## Severity Guidelines

| Severity | Action |
|----------|--------|
| `info` | Log for awareness |
| `warning` | Investigate during business hours |
| `critical` | Respond immediately (Down node, quarantine, capacity above 90 %, no leader) |

## Silencing During Maintenance

A planned data-node stop is `node.down` after 30 s unless the node is in
maintenance mode ([Maintenance Mode](./maintenance-mode.md)). Maintenance mode
prevents the `Down` marking and therefore the event and the re-replication, but
there is no `node.maintenance` event and maintenance nodes are counted in no
`dtfs_nodes_*` gauge; silence Alertmanager rules for the window as well.

## Best Practices

- Set up alerts before production use; test a sink with a signed manual POST to
  your receiver.
- Keep the same sinks on all managers.
- Tune `dedup_secs` so a flapping node does not page repeatedly.
- Include the cluster name in alert labels to separate staging from production.
