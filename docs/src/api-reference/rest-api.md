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
