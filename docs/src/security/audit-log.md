# Audit Log

DTFS maintains an append-only audit log that records security-relevant and administrative events. The log is used for compliance, debugging, and integration with external monitoring systems.

## Architecture

The audit log is an in-memory ring buffer on each manager, optionally mirrored to a daily-rotated JSONL file (`archive_dir`) and, with `replicate_via_raft`, to the Raft state machine. When the buffer reaches its maximum capacity (default: 10,000 entries), the oldest entries are evicted to make room for new ones.

All state-mutating operations that pass through the manager -- node joins, configuration changes, maintenance mode toggles, scrub triggers -- are automatically recorded.

Audit rows written by the state machine take their time from the `now_ms` the proposing manager stamped into the Raft entry (`DeleteFile`, `RegisterNode`, `DeregisterNode`, `UpdateNodeStatus`, `QuarantineNode`, `UnquarantineNode`, `UpdateConfig`, `UpsertVolume`, `DeleteVolume`, `ReportScrubResult`, `ReleaseWriteLease`, and the chunk-location variants), so every manager shows the same timestamp. An entry from an older build without `now_ms` uses its Raft log index as a stand-in; such a legacy row is ordered correctly but its timestamp is not a wall-clock time (it shows as a date near 1970).

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

The in-memory log is capped at 10,000 entries (`MAX_AUDIT_LOG_ENTRIES`,
compile-time constant — not configurable). At typical event rates this
provides days-to-months of retention, but a bulk operation (e.g. a
re-replication wave emitting one `replication.complete` per chunk) can
churn through the whole cap in minutes — export to JSONL if you need
history.

### `[audit]` Configuration

The manager's `[audit]` block has a single option:

```toml
[audit]
replicate_via_raft = false   # default
```

When `false`, audit entries stay in a per-manager local ring (+ JSONL
archive) — each manager returns only events it observed itself. When `true`,
entries replicate through Raft so every manager serves the same view. Enable
only after **all** managers run a binary that understands `AppendAudit` (see
the rollout recipe in `crates/dtfs-common/src/config.rs`).

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

> **`limit` ordering.** `limit=N` returns the **newest** N matching
> entries, newest-first, regardless of `replicate_via_raft` (CF-4a — the
> default local ring and the replicated log now agree).

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

With `replicate_via_raft = false` (default) events live only in the observing manager's local ring and JSONL archive. With `true`, each event is also proposed as an `AppendAudit` Raft write (spawn-and-forget, 5 s timeout; a failed proposal never blocks the request) and every manager's state machine holds the same capped log.

Rows the state machine itself writes while applying a command (node registration, chunk-location changes, and so on) carry no wall-clock time. Their timestamp is the proposer's `now_ms` where the command has one, otherwise the committed Raft log index, so every replica and every replay produces the identical entry. `AppendAudit` rows carry the originating manager's timestamp. Treat a state-machine-written timestamp that looks tiny as a log index, not a date.

## Rejected REST mutations (`auth.denied`)

A REST mutation refused by the shared-secret gate writes one `auth.denied` row per (source IP, path) per 60 seconds. Further denials inside the window are counted and reported as `suppressed=N` on the next row, so an unauthenticated flood cannot evict real audit history.
