# gRPC API

All inter-component communication in DTFS uses gRPC with Protocol Buffers. The `.proto` definitions are in the `proto/` directory, and generated Rust code lives in the `dtfs-proto` crate.

## Services Overview

| Service | Proto File | Port (Default) | Purpose |
|---------|-----------|----------------|---------|
| `ManagerService` | `manager.proto` | 9401 | Client and data node operations against the manager |
| `DataService` | `data.proto` | 9500 | Chunk storage operations on data nodes |
| `ClusterJoin` | `cluster.proto` | 9401 | Node join handshake |
| `AdminService` | `admin.proto` | 9401 | Administrative operations (scrub, maintenance, audit) |

All connections use mTLS when TLS is enabled (see [TLS](../security/tls.md)).

---

## ManagerService

Defined in `manager.proto` under the `dtfs.manager` package. This service handles metadata operations from FUSE clients and registration/heartbeat traffic from data nodes.

### CreateFile

Creates a new file in the namespace.

```protobuf
rpc CreateFile(CreateFileRequest) returns (CreateFileResponse);
```

| Request Field | Type | Description |
|---------------|------|-------------|
| `path` | string | Absolute file path |
| `volume_id` | string (optional) | Volume to create the file in; defaults to the cluster default volume |

| Response Field | Type | Description |
|----------------|------|-------------|
| `success` | bool | Whether the file was created |
| `chunk_size` | uint64 | Chunk size in bytes for this file |
| `volume_id` | string | Assigned volume ID |

### GetFileLocations

Resolves a file path to its chunk locations across data nodes.

```protobuf
rpc GetFileLocations(GetFileLocationsRequest) returns (GetFileLocationsResponse);
```

| Request Field | Type | Description |
|---------------|------|-------------|
| `path` | string | Absolute file path |

| Response Field | Type | Description |
|----------------|------|-------------|
| `path` | string | Echoed file path |
| `total_size` | uint64 | Total file size in bytes |
| `chunks` | repeated ChunkLocationInfo | List of chunks with replica addresses |

Each `ChunkLocationInfo` contains:

| Field | Type | Description |
|-------|------|-------------|
| `chunk_id` | string | Hex-encoded chunk ID |
| `size` | uint64 | Chunk size in bytes |
| `node_addresses` | repeated string | Addresses of all nodes holding replicas |

### DeleteFile

Removes a file from the namespace and schedules chunk cleanup.

```protobuf
rpc DeleteFile(DeleteFileRequest) returns (DeleteFileResponse);
```

| Request Field | Type | Description |
|---------------|------|-------------|
| `path` | string | Absolute file path |

| Response Field | Type | Description |
|----------------|------|-------------|
| `success` | bool | Whether the file was deleted |

### ListDirectory

Lists entries in a directory.

```protobuf
rpc ListDirectory(ListDirectoryRequest) returns (ListDirectoryResponse);
```

| Request Field | Type | Description |
|---------------|------|-------------|
| `path` | string | Directory path |

Each `DirectoryEntry` in the response contains:

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Entry name |
| `size` | uint64 | Size in bytes (0 for directories) |
| `is_directory` | bool | True if this entry is a directory |
| `created_at` | int64 | Creation timestamp (Unix epoch) |
| `modified_at` | int64 | Last modification timestamp |

### AllocateChunks

Requests the placement engine to decide where new chunks should be stored.

```protobuf
rpc AllocateChunks(AllocateChunksRequest) returns (AllocateChunksResponse);
```

| Request Field | Type | Description |
|---------------|------|-------------|
| `path` | string | File path being written |
| `num_chunks` | uint32 | Number of chunks to allocate |
| `volume_id` | string | Volume ID for placement decisions |

Each `ChunkPlacement` in the response contains:

| Field | Type | Description |
|-------|------|-------------|
| `chunk_index` | uint32 | Index of this chunk in the file |
| `primary_address` | string | Address of the primary data node |
| `replica_addresses` | repeated string | Addresses of replica data nodes |

### RegisterNode

Registers a new data node with the cluster. Called during the join process after the handshake completes.

```protobuf
rpc RegisterNode(RegisterNodeRequest) returns (RegisterNodeResponse);
```

| Request Field | Type | Description |
|---------------|------|-------------|
| `hostname` | string | Node hostname |
| `role` | string | Node role (`"data"`) |
| `grpc_port` | uint32 | gRPC listen port |
| `location` | Location | Topology location (region/datacenter/rack/host) |
| `disks` | repeated DiskInfo | Disk inventory |

| Response Field | Type | Description |
|----------------|------|-------------|
| `accepted` | bool | Whether registration was accepted |
| `node_id` | string | Assigned node ID |
| `config_version` | uint64 | Current cluster configuration version |

### Heartbeat

Periodic liveness signal from data nodes. Carries capacity statistics and the node's current config version.

```protobuf
rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
```

| Request Field | Type | Description |
|---------------|------|-------------|
| `node_id` | string | Sending node's ID |
| `uptime_secs` | uint64 | Node uptime in seconds |
| `total_chunks` | uint64 | Number of chunks stored |
| `total_bytes_used` | uint64 | Bytes used across all disks |
| `config_version` | uint64 | Node's current config version |

| Response Field | Type | Description |
|----------------|------|-------------|
| `accepted` | bool | Whether the heartbeat was processed |
| `config_version` | uint64 | Manager's current config version |
| `config_update` | ConfigUpdate (optional) | Full config if the node's version is stale |

### ReportChunkStatus

Data nodes report chunk health after scrubbing.

```protobuf
rpc ReportChunkStatus(ChunkStatusReport) returns (ChunkStatusAck);
```

| Request Field | Type | Description |
|---------------|------|-------------|
| `node_id` | string | Reporting node's ID |
| `chunks` | repeated ChunkStatus | List of chunk health reports |

Each `ChunkStatus` contains:

| Field | Type | Description |
|-------|------|-------------|
| `chunk_id` | string | Chunk identifier |
| `size` | uint64 | Chunk size in bytes |
| `status` | string | `"healthy"`, `"corrupt"`, or `"missing"` |

---

## DataService

Defined in `data.proto` under the `dtfs.data` package. This service runs on every data node and handles chunk-level I/O.

### PutChunk

Stores a chunk using client-side streaming. The first message carries metadata (chunk ID, volume, target tier), and subsequent messages carry data payloads.

```protobuf
rpc PutChunk(stream PutChunkRequest) returns (PutChunkResponse);
```

| Request Field | Type | Description |
|---------------|------|-------------|
| `chunk_id` | string | Hex-encoded chunk ID (first message) |
| `volume_id` | string | Volume ID (first message) |
| `tier` | string | Target tier: `"hot"`, `"warm"`, `"cold"` (first message) |
| `data` | bytes | Chunk data payload (all messages) |

| Response Field | Type | Description |
|----------------|------|-------------|
| `chunk_id` | string | Echoed chunk ID |
| `size` | uint64 | Total bytes written |
| `checksum` | string | Hex-encoded BLAKE3 checksum |
| `success` | bool | Whether the write succeeded |

### GetChunk

Retrieves a chunk using server-side streaming. Data is returned in multiple response messages.

```protobuf
rpc GetChunk(GetChunkRequest) returns (stream GetChunkResponse);
```

| Request Field | Type | Description |
|---------------|------|-------------|
| `chunk_id` | string | Hex-encoded chunk ID |

| Response Field | Type | Description |
|----------------|------|-------------|
| `data` | bytes | Chunk data payload |
| `offset` | uint64 | Byte offset within the chunk |
| `total_size` | uint64 | Total chunk size |

### DeleteChunk

Removes a chunk from the data node.

```protobuf
rpc DeleteChunk(DeleteChunkRequest) returns (DeleteChunkResponse);
```

| Request Field | Type | Description |
|---------------|------|-------------|
| `chunk_id` | string | Chunk to delete |

| Response Field | Type | Description |
|----------------|------|-------------|
| `success` | bool | Whether the chunk was deleted |

### ReplicateChunk

Instructs the data node to copy a chunk it holds to another data node.

```protobuf
rpc ReplicateChunk(ReplicateChunkRequest) returns (ReplicateChunkResponse);
```

| Request Field | Type | Description |
|---------------|------|-------------|
| `chunk_id` | string | Chunk to replicate |
| `target_address` | string | Address of the destination data node |

| Response Field | Type | Description |
|----------------|------|-------------|
| `success` | bool | Whether replication completed |
| `error` | string | Error message if failed |

### VerifyChunk

Checks the integrity of a chunk on the data node.

```protobuf
rpc VerifyChunk(VerifyChunkRequest) returns (VerifyChunkResponse);
```

| Request Field | Type | Description |
|---------------|------|-------------|
| `chunk_id` | string | Chunk to verify |

| Response Field | Type | Description |
|----------------|------|-------------|
| `valid` | bool | Whether the chunk passes integrity check |
| `error` | string | Error description if invalid |

---

## ClusterJoin

Defined in `cluster.proto` under the `dtfs.cluster` package. Implements the two-phase node join handshake (see [Cluster Join Protocol](../security/cluster-join.md)).

### RequestJoin

Phase 1: The joining node proves possession of the shared secret via HMAC.

```protobuf
rpc RequestJoin(JoinRequest) returns (JoinResponse);
```

| Request Field | Type | Description |
|---------------|------|-------------|
| `secret_hmac` | bytes | HMAC-SHA256(shared_secret, nonce) |
| `nonce` | string | Random nonce generated by the joining node |
| `node_info` | NodeInfo | Node metadata (ID, role, hostname, disks, location) |

| Response Field | Type | Description |
|----------------|------|-------------|
| `accepted` | bool | Whether the HMAC was valid |
| `challenge` | string | Challenge nonce for Phase 2 |
| `cluster_ca_cert` | bytes | PEM-encoded cluster CA certificate |
| `managers` | repeated NodeInfo | List of manager nodes |
| `rejection_reason` | string | Reason if rejected |

### CompleteJoin

Phase 2: The joining node completes the handshake and is registered in Raft.

```protobuf
rpc CompleteJoin(JoinCompletionRequest) returns (JoinCompletionResponse);
```

| Request Field | Type | Description |
|---------------|------|-------------|
| `node_id` | string | Node's ID |
| `challenge_response` | bytes | Response to the manager's challenge |

| Response Field | Type | Description |
|----------------|------|-------------|
| `success` | bool | Whether registration succeeded |
| `config_version` | uint64 | Current cluster configuration version |
| `config_json` | string | Full cluster configuration as JSON |

---

## AdminService

Defined in `admin.proto` under the `dtfs.admin` package. Provides administrative operations used by `dtfs-cli` and automation tooling.

### TriggerScrub

Initiates a scrub operation targeting a disk, file, chunk, or the entire cluster.

```protobuf
rpc TriggerScrub(TriggerScrubRequest) returns (TriggerScrubResponse);
```

| Request Field | Type | Description |
|---------------|------|-------------|
| `target_type` | string | `"disk"`, `"file"`, `"chunk"`, or `"all"` |
| `target_id` | string | Disk path, file path, chunk ID, or empty for `"all"` |
| `level` | string | `"quick"` or `"deep"` |

| Response Field | Type | Description |
|----------------|------|-------------|
| `accepted` | bool | Whether the scrub was scheduled |
| `task_id` | string | Task identifier for status queries |

### GetScrubResults

Retrieves the results of a previously triggered scrub.

```protobuf
rpc GetScrubResults(GetScrubResultsRequest) returns (GetScrubResultsResponse);
```

| Request Field | Type | Description |
|---------------|------|-------------|
| `task_id` | string | Task ID from TriggerScrub |

| Response Field | Type | Description |
|----------------|------|-------------|
| `status` | string | `"running"`, `"completed"`, or `"failed"` |
| `chunks_checked` | uint64 | Total chunks examined |
| `chunks_healthy` | uint64 | Chunks that passed verification |
| `chunks_corrupt` | uint64 | Chunks with checksum mismatches |
| `chunks_missing` | uint64 | Chunks that could not be found |
| `chunks_size_mismatch` | uint64 | Chunks with incorrect sizes |
| `duration_secs` | double | Elapsed time in seconds |

### SetMaintenanceMode

Enables or disables maintenance mode on a data node.

```protobuf
rpc SetMaintenanceMode(MaintenanceModeRequest) returns (MaintenanceModeResponse);
```

| Request Field | Type | Description |
|---------------|------|-------------|
| `node_id` | string | Target node ID |
| `enable` | bool | `true` to enter maintenance, `false` to exit |

| Response Field | Type | Description |
|----------------|------|-------------|
| `success` | bool | Whether the mode change was applied |
| `status` | string | Current node status: `"draining"`, `"maintenance"`, or `"active"` |

### GetAuditLog

Queries the audit log with optional filters.

```protobuf
rpc GetAuditLog(AuditLogRequest) returns (AuditLogResponse);
```

| Request Field | Type | Description |
|---------------|------|-------------|
| `since_timestamp` | int64 | Return entries at or after this Unix timestamp |
| `limit` | uint32 | Maximum entries to return |
| `action_filter` | string | Filter by action prefix |

Each `AuditLogEntry` in the response contains:

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | int64 | Unix epoch timestamp |
| `actor` | string | Who performed the action |
| `action` | string | Action type |
| `target` | string | What was acted upon |
| `result` | string | Outcome |
| `source_location` | string | Source location information |

---

## Common Types

Defined in `common.proto` under the `dtfs.common` package. These types are shared across all services.

### Location

```protobuf
message Location {
  string region = 1;
  string datacenter = 2;
  string rack = 3;
  string host = 4;
}
```

### DiskInfo

```protobuf
message DiskInfo {
  uint32 disk_id = 1;
  string uuid = 2;
  string mount_path = 3;
  string tier = 4;        // "hot", "warm", "cold"
  uint64 total_bytes = 5;
  uint64 used_bytes = 6;
  string status = 7;      // "healthy", "degraded", "draining", "failed", "removed"
}
```

### NodeInfo

```protobuf
message NodeInfo {
  string id = 1;
  string role = 2;          // "manager" or "data"
  string hostname = 3;
  uint32 grpc_port = 4;
  Location location = 5;
  repeated DiskInfo disks = 6;
  uint64 total_capacity = 7;
  uint64 used_capacity = 8;
  string health_status = 9;
}
```
