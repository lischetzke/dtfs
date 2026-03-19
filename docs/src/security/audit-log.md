# Audit Log

DTFS maintains an append-only audit log that records security-relevant and administrative events. The log is used for compliance, debugging, and integration with external monitoring systems.

## Architecture

The audit log is implemented as an in-memory ring buffer on the manager node. When the buffer reaches its maximum capacity (default: 10,000 entries), the oldest entries are evicted to make room for new ones.

All state-mutating operations that pass through the manager -- node joins, configuration changes, maintenance mode toggles, scrub triggers -- are automatically recorded.

## Entry Format

Each audit log entry contains five fields:

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | Unix epoch (seconds) | When the event occurred (UTC) |
| `actor` | String | Who initiated the action: a username, node hostname, or `"system"` for automated operations |
| `action` | String | Dot-delimited action type (e.g., `node.join.request`, `config.update`) |
| `target` | String | What was acted upon: a node ID, file path, configuration key, etc. |
| `result` | String | Outcome: `"success"`, `"accepted"`, or an error description |

### Example Entry (JSON)

```json
{
  "timestamp": 1710842400,
  "actor": "data-node-03.example.com",
  "action": "node.join.request",
  "target": "data-node-03.example.com",
  "result": "accepted"
}
```

### Action Types

The following action types are recorded:

| Action | Trigger |
|--------|---------|
| `node.join.request` | A node attempts to join the cluster (Phase 1) |
| `node.join.complete` | A node completes the join handshake (Phase 2) |
| `maintenance.toggle` | Maintenance mode enabled or disabled on a node |
| `config.update` | Cluster configuration updated via REST API |
| `scrub.trigger.quick` | Quick scrub triggered via API |
| `scrub.trigger.deep` | Deep scrub triggered via API |
| `tier.promote` | Chunk migrated to a higher-performance tier |
| `tier.demote` | Chunk migrated to a lower-performance tier |
| `file.create` | A new file created in the namespace |
| `file.delete` | A file deleted from the namespace |

## Retention

The in-memory ring buffer has a configurable maximum size. The default capacity is 10,000 entries. At typical event rates, this provides approximately 90 days of retention for a moderately active cluster.

### Configuring Retention

The maximum entry count can be set in the manager configuration:

```toml
[audit]
max_entries = 10000
```

Increasing this value consumes more memory on the manager node. Each entry is approximately 200-500 bytes depending on field lengths, so 10,000 entries uses roughly 2-5 MB of memory.

### Retention Policy

Since the audit log uses a ring buffer, entries are evicted on a first-in-first-out basis when the buffer is full. To preserve entries beyond the ring buffer's capacity, export them to an external system before they are evicted (see JSON Export below).

## Querying the Audit Log

### Via CLI

```bash
# Show the 20 most recent entries
dtfs-cli audit --limit 20

# Filter by action type
dtfs-cli audit --action node.join --limit 50

# Output as JSON
dtfs-cli audit --limit 100 --format json
```

### Via REST API

```bash
# Default query (50 entries)
curl http://<manager>:9402/api/v1/audit

# With filters
curl "http://<manager>:9402/api/v1/audit?limit=100&action=config.update"
```

Query parameters:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | Integer | 50 | Maximum number of entries to return |
| `action` | String | (none) | Filter by action prefix (e.g., `node.join` matches both `node.join.request` and `node.join.complete`) |

### Via gRPC

The `AdminService.GetAuditLog` RPC provides programmatic access:

```
GetAuditLog(since_timestamp, limit, action_filter) -> [AuditLogEntry]
```

## JSON Export for SIEM Integration

The audit log can be exported as a JSON array for ingestion into Security Information and Event Management (SIEM) systems such as Splunk, Elastic SIEM, or similar platforms.

### Export via CLI

```bash
# Export all current entries as JSON
dtfs-cli audit --limit 10000 --format json > /var/log/dtfs/audit-export.json
```

### Export via REST API

```bash
curl -s "http://<manager>:9402/api/v1/audit?limit=10000" \
  -H "Authorization: Bearer <token>" \
  > /var/log/dtfs/audit-export.json
```

### Automated Export

For continuous SIEM integration, set up a periodic export job that fetches new entries since the last export. The `since_timestamp` parameter on the gRPC API and the timestamp-based filtering allow incremental exports:

```bash
#!/bin/bash
# Example: export new audit entries every 5 minutes
LAST_TS=$(cat /var/lib/dtfs/last-audit-ts 2>/dev/null || echo 0)
ENTRIES=$(curl -s "http://manager:9402/api/v1/audit?limit=10000")
echo "$ENTRIES" >> /var/log/dtfs/audit-stream.jsonl
# Update timestamp marker for next run
echo "$ENTRIES" | jq -r '.[-1].timestamp // empty' > /var/lib/dtfs/last-audit-ts
```

### SIEM Field Mapping

The audit entry fields map naturally to common SIEM schemas:

| DTFS Field | Common SIEM Field |
|------------|-------------------|
| `timestamp` | `@timestamp` / `event.created` |
| `actor` | `user.name` / `source.user` |
| `action` | `event.action` / `event.category` |
| `target` | `event.target` / `object.name` |
| `result` | `event.outcome` |

## Audit Log and Raft

Audit entries are written locally on the manager leader. They are not replicated through Raft to avoid adding high-frequency writes to the consensus log. If the leader changes, the new leader starts with an empty audit buffer. For durable audit history, use the JSON export mechanism to persist entries to external storage.
