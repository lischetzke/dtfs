# REST API

The DTFS manager exposes a REST API on the HTTP port (default 9402). This API is consumed by the WebUI dashboard, the `dtfs-cli` tool, and external integrations.

All endpoints return JSON. State-mutating endpoints require [JWT authentication](../security/authentication.md) when auth is enabled.

## Base URL

```
http://<manager-host>:9402/api/v1
```

## Endpoints

### GET /api/v1/cluster/status

Returns overall cluster status including node counts, capacity, and configuration version.

**Response:**

```json
{
  "status": "ok",
  "cluster_name": "production",
  "node": "manager-01",
  "data_nodes": 6,
  "data_nodes_healthy": 6,
  "files": 1423,
  "total_capacity": 12884901888000,
  "used_capacity": 4294967296000,
  "config_version": 3
}
```

| Field | Type | Description |
|-------|------|-------------|
| `status` | String | `"ok"` when the cluster is operating normally |
| `cluster_name` | String | Name from the manager configuration |
| `node` | String | Hostname of the responding manager |
| `data_nodes` | Integer | Total registered data nodes |
| `data_nodes_healthy` | Integer | Data nodes with `Healthy` status |
| `files` | Integer | Total files tracked in metadata |
| `total_capacity` | Integer | Aggregate capacity in bytes across all nodes |
| `used_capacity` | Integer | Aggregate used space in bytes |
| `config_version` | Integer | Current cluster configuration version |

---

### GET /api/v1/health

Simple health check endpoint. Returns HTTP 200 if the manager is running.

**Response:**

```json
{
  "status": "healthy"
}
```

Useful for load balancer health probes and monitoring systems.

---

### GET /api/v1/nodes

Lists all registered data nodes with summary information.

**Response:**

```json
[
  {
    "node_id": "1",
    "hostname": "data-01.example.com",
    "grpc_addr": "data-01.example.com:9500",
    "location": "us-east/dc1/rack-a/data-01",
    "disk_count": 4,
    "total_capacity": 4294967296000,
    "used_capacity": 1073741824000,
    "health": "Healthy",
    "role": "Data"
  }
]
```

---

### GET /api/v1/nodes/{id}

Returns detailed information for a specific node, including its full disk inventory.

**Path parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | String | Node ID |

**Response:**

```json
{
  "node_id": "1",
  "hostname": "data-01.example.com",
  "grpc_addr": "data-01.example.com:9500",
  "location": {
    "region": "us-east",
    "datacenter": "dc1",
    "rack": "rack-a",
    "host": "data-01"
  },
  "disks": [
    {
      "disk_id": 0,
      "uuid": "a1b2c3d4-...",
      "mount_path": "/mnt/dtfs-disk-a1b2c3d4",
      "tier": "Hot",
      "total_bytes": 1073741824000,
      "used_bytes": 268435456000,
      "status": "Healthy"
    }
  ],
  "total_capacity": 4294967296000,
  "used_capacity": 1073741824000,
  "health": "Healthy",
  "role": "Data"
}
```

**Error responses:**

| Status | Condition |
|--------|-----------|
| 400 | Invalid node ID format |
| 404 | Node not found |

---

### POST /api/v1/nodes/{id}/maintenance

Enable or disable maintenance mode on a node. See [Maintenance Mode](../operations/maintenance-mode.md) for the full draining process.

**Path parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | String | Node ID |

**Request body:**

```json
{
  "enable": true
}
```

**Response:**

```json
{
  "node_id": "1",
  "maintenance": true
}
```

**Error responses:**

| Status | Condition |
|--------|-----------|
| 400 | Invalid node ID format |
| 503 | Raft leader unavailable |

---

### GET /api/v1/files

Lists files tracked in the cluster metadata. Results are capped at 1,000 entries.

**Response:**

```json
{
  "count": 1423,
  "files": [
    {
      "path": "/data/reports/2024-q1.parquet",
      "volume_id": "default",
      "chunk_count": 3,
      "total_size": 402653184,
      "created_at": 1710842400000,
      "updated_at": 1710928800000
    }
  ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `count` | Integer | Total file count (may exceed the returned list length) |
| `files` | Array | Up to 1,000 file entries |

---

### GET /api/v1/volumes

Lists volumes aggregated from file metadata.

**Response:**

```json
{
  "count": 2,
  "volumes": [
    {
      "volume_id": "default",
      "file_count": 1200,
      "total_size": 549755813888
    },
    {
      "volume_id": "archive",
      "file_count": 223,
      "total_size": 107374182400
    }
  ]
}
```

> **Bug AV note:** the JSON shape currently emits only the
> legacy `replication` block. The new `mandatory_replicas`,
> `cache_replicas`, and `anchor` fields are stored on the
> state machine (`POST /api/v1/volumes/{id}/migrate-policy` is
> idempotent thanks to this) but the read-side projection has
> not yet been broadened. Tracked as a follow-up under
> [Plan AV-1-12 / AV-1-13](https://github.com/lischetzke/dtfs/blob/master/plans/active/PLAN-BUG-AV-TIER-LIFECYCLE.md).

---

### POST /api/v1/volumes/{id}/migrate-policy

Bug AV — idempotently copy a volume's legacy `replication` block
into the new `mandatory_replicas` field, leaving the legacy field
intact for rollback safety. No-op if the migration has already
been applied.

**Path parameters:**

| Name | Description |
|------|-------------|
| `id` | Volume ID. |

**Response (changed):**

```json
{
  "volume_id": "default",
  "changed": true,
  "mandatory_replicas": { "hot": 0, "warm": 2, "cold": 1, "tape": 1 }
}
```

**Response (no-op):**

```json
{
  "volume_id": "default",
  "changed": false,
  "mandatory_replicas": { "hot": 0, "warm": 2, "cold": 1, "tape": 1 }
}
```

**Errors:**

| Status | Reason |
|--------|--------|
| `404 Not Found` | Volume does not exist. |
| `503 Service Unavailable` | Raft write failed (cluster lost quorum or follower without forwarding). |

**Example:**

```bash
$ curl -X POST http://mgr-01:9402/api/v1/volumes/default/migrate-policy
{"changed":true,"mandatory_replicas":{...},"volume_id":"default"}
```

CLI equivalent: `dtfs-cli volumes migrate-policy default` (see
[CLI reference](./cli-reference.md#volumes-migrate-policy)).

---

### POST /api/v1/files/flush_cache

Bug AV — operator-driven button to drop **cache** replicas of every
chunk under a path prefix on the listed tiers, refusing any drop
that would strand the mandatory replica or anchor. Sets a
per-chunk cooldown so the tiering engine doesn't immediately
re-promote.

**Request body:**

```json
{
  "path_prefix": "/movies/Wicked.mkv",
  "cache_tiers": ["warm"],
  "cooldown_secs": 300,
  "max_tasks": 10000,
  "allow_empty_prefix": false
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path_prefix` | string | *(required)* | Prefix to match. `/` matches every file in the cluster but is rejected unless `allow_empty_prefix=true`. |
| `cache_tiers` | array of `"hot"`/`"warm"`/`"cold"`/`"tape"` | `["warm","hot"]` | Tiers eligible to be flushed. The planner additionally filters out any tier that's also `mandatory_replicas.<tier> > 0` for the file's volume — so e.g. flushing warm on a volume with `mandatory_replicas.warm = 1` is a safe no-op. |
| `cooldown_secs` | integer | `300` | How long after the flush the tiering engine refuses to re-promote affected chunks. Set to `0` to disable the cooldown. |
| `max_tasks` | integer | `10000` | Maximum replica deletions to enqueue in a single call. Operators re-run the call to pick up remaining work; the `truncated` counter in the response reports the unseen tail. |
| `allow_empty_prefix` | bool | `false` | Safety gate against an empty/`"/"` prefix accidentally flushing the entire cluster. |

**Response:**

```json
{
  "status": "accepted",
  "path_prefix": "/movies/Wicked.mkv",
  "cache_tiers": ["warm"],
  "cooldown_secs": 300,
  "files_matched": 1,
  "chunks_examined": 128,
  "replicas_dropped": 254,
  "mandatory_refusals": 0,
  "truncated": 0,
  "unreachable_nodes": 0,
  "rpc_failures": 0,
  "raft_failures": 0
}
```

| Field | Meaning |
|-------|---------|
| `files_matched` | Files whose path begins with `path_prefix`. |
| `chunks_examined` | Distinct `chunk_index`es considered across all matched files. |
| `replicas_dropped` | Successful per-replica deletes (counts replicas, not chunks). |
| `mandatory_refusals` | Chunks where the planner refused to flush because doing so would have stranded the mandatory/anchor copy. Operator should investigate (each refusal emits a `cache.flush_refused` audit row + fires one deduped `AnchorViolated` alert per call). |
| `truncated` | Replicas the planner skipped because `max_tasks` was hit. |
| `unreachable_nodes` | Per-action failures: the node hosting the replica was unreachable at delete time. |
| `rpc_failures` | Per-action failures: the data node's `DeleteChunkReplica` RPC errored. |
| `raft_failures` | Per-action failures: the manager's `RemoveChunkLocation` Raft write errored. |

**Audit and alerting:**

* `cache.flush_replica` audit row per successful per-replica delete.
* `cache.flush_refused` audit row per refused chunk (reason
  `mandatory_strand`).
* `cache.flush_complete` audit row at the end of the call (always
  fires, even on early-return paths).
* `AlertKind::AnchorViolated` fires once per call (deduped) when
  any refusals occurred, with `subsystem: "cache_flush"` and a
  sampled first-20 refusals in the details JSON.

**Errors:**

| Status | Reason |
|--------|--------|
| `400 Bad Request` | Empty `path_prefix` without `allow_empty_prefix=true`; invalid `cache_tiers` entry. |
| `503 Service Unavailable` | Raft writes during the dispatch failed; partial progress is committed and reflected in counters. |

**Example:**

```bash
$ curl -X POST http://mgr-01:9402/api/v1/files/flush_cache \
       -H 'content-type: application/json' \
       -d '{"path_prefix":"/movies/Wicked.mkv","cache_tiers":["warm"],"cooldown_secs":300}'
{"status":"accepted","replicas_dropped":254,"mandatory_refusals":0,...}
```

CLI equivalent: planned `dtfs-cli files flush-cache` under AV-3-5
(see [CLI reference](./cli-reference.md#files-flush-cache-planned)).

---

### GET /api/v1/audit

Query the audit log. See [Audit Log](../security/audit-log.md) for details on entry format and retention.

**Query parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | Integer | 50 | Maximum entries to return |
| `action` | String | (none) | Filter by action prefix |

**Example request:**

```
GET /api/v1/audit?limit=10&action=node.join
```

**Response:**

```json
[
  {
    "timestamp": 1710842400,
    "actor": "data-node-03.example.com",
    "action": "node.join.request",
    "target": "data-node-03.example.com",
    "result": "accepted"
  },
  {
    "timestamp": 1710842401,
    "actor": "data-node-03.example.com",
    "action": "node.join.complete",
    "target": "3",
    "result": "success"
  }
]
```

---

### GET /api/v1/config

Returns the current cluster configuration, including defaults, replication settings, and tiering parameters.

**Response:**

```json
{
  "version": 3,
  "config": { },
  "defaults": {
    "chunk_size_mb": 128,
    "write_safety": {
      "min_replicas_before_ack": 2,
      "write_timeout_secs": 30
    },
    "replication": {
      "hot": 3,
      "warm": 2,
      "cold": 1
    }
  },
  "tiering": {
    "enabled": true,
    "hot_threshold_reads_per_hour": 5,
    "cold_threshold_days": 14,
    "migration_bandwidth_limit_mbps": 200
  }
}
```

---

### POST /api/v1/config

Update the cluster-wide configuration. The new configuration is written to the Raft state machine and propagated to data nodes via heartbeat responses.

**Request body:**

```json
{
  "config_json": "{\"replication\":{\"hot\":3,\"warm\":2,\"cold\":2}}"
}
```

**Response:**

```json
{
  "version": 4,
  "status": "applied"
}
```

**Error responses:**

| Status | Condition |
|--------|-----------|
| 503 | Raft leader unavailable |

---

### POST /api/v1/scrub

Trigger a scrub operation. See [Scrubbing](../storage/scrubbing.md) for details on scrub types and scheduling.

**Request body:**

```json
{
  "target_type": "disk",
  "target": "/mnt/dtfs-disk-a1b2c3d4",
  "level": "deep"
}
```

| Field | Type | Values | Description |
|-------|------|--------|-------------|
| `target_type` | String | `"disk"`, `"file"`, `"chunk"`, `"all"` | What to scrub |
| `target` | String | Path, chunk ID, or `"*"` | Specific target identifier |
| `level` | String | `"quick"`, `"deep"` | Scrub depth |

**Response:**

```json
{
  "status": "accepted",
  "target_type": "disk",
  "target": "/mnt/dtfs-disk-a1b2c3d4",
  "level": "deep"
}
```

## Error Format

When an error occurs, the API returns an appropriate HTTP status code. Common error codes:

| Status | Meaning |
|--------|---------|
| 400 | Bad request (invalid parameters) |
| 401 | Unauthorized (missing or invalid JWT token) |
| 403 | Forbidden (insufficient role) |
| 404 | Resource not found |
| 503 | Service unavailable (Raft leader not available) |
