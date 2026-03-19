# Volume Configuration

Each volume in DTFS has its own configuration that controls chunk sizing, replication, and write safety. Volume settings are managed through the Web UI or REST API and stored in the Raft-replicated state machine.

All volume settings support [live reload](./live-reload.md).

## Volume Settings Reference

| Setting | Type | Default | Valid Range | Description |
|---------|------|---------|-------------|-------------|
| `id` | `VolumeId` | *(auto-generated)* | Any non-empty string | Unique identifier for the volume. Set at creation time and cannot be changed. |
| `name` | `String` | `"default"` | Any non-empty string | Human-readable name for the volume. Can be changed after creation. |
| `chunk_size_bytes` | `u64` | `134217728` (128 MiB) | 1048576--1073741824 (1 MiB--1 GiB) | Size in bytes of each chunk. Files are split into chunks of this size for distribution across data nodes. Larger chunks reduce metadata overhead; smaller chunks reduce write amplification and improve parallel read performance. This value is set at volume creation and cannot be changed afterward. |
| `replication.hot` | `u32` | `3` | 1--9 | Number of replicas maintained for hot-tier chunks in this volume. |
| `replication.warm` | `u32` | `2` | 1--9 | Number of replicas maintained for warm-tier chunks in this volume. |
| `replication.cold` | `u32` | `1` | 1--9 | Number of replicas maintained for cold-tier chunks in this volume. |
| `write_safety.min_replicas_before_ack` | `u32` | `2` | 1--replication factor | Minimum replicas that must acknowledge a write before the client receives success. Cannot exceed the replication factor for the relevant tier. |
| `write_safety.write_timeout_secs` | `u32` | `30` | 1--3600 | Maximum seconds to wait for write acknowledgements before returning an error. |

## Creating a Volume

Volumes are created through the REST API or Web UI. At creation time, you can override any of the cluster-wide defaults:

```
POST /api/v1/volumes
```

```json
{
  "name": "media-archive",
  "chunk_size_bytes": 268435456,
  "replication": {
    "hot": 2,
    "warm": 2,
    "cold": 1
  },
  "write_safety": {
    "min_replicas_before_ack": 1,
    "write_timeout_secs": 60
  }
}
```

Fields omitted from the request body inherit their values from the [cluster-wide defaults](./cluster-config.md).

## Modifying a Volume

Replication factors and write safety settings can be changed on a live volume:

```
PUT /api/v1/volumes/{volume_id}
```

```json
{
  "replication": {
    "hot": 3
  },
  "write_safety": {
    "min_replicas_before_ack": 2
  }
}
```

Only the fields included in the request body are changed. Omitted fields retain their current values.

### Immutable Settings

The following settings cannot be changed after volume creation:

- `id` -- The volume identifier.
- `chunk_size_bytes` -- Changing the chunk size would require re-chunking all existing data. Create a new volume with the desired chunk size and migrate data if needed.

### Effects of Changing Replication

When the replication factor is increased, the cluster begins creating additional replicas in the background. The volume remains fully available during this process. When the replication factor is decreased, excess replicas are garbage-collected during the next tiering evaluation cycle.

### Effects of Changing Write Safety

Changes to `min_replicas_before_ack` and `write_timeout_secs` take effect immediately for new writes. In-flight writes that were started before the change complete under the old settings.

## Default Volume

Every cluster has a volume named `"default"` that is created automatically during cluster bootstrap. It uses the cluster-wide default settings. The default volume cannot be deleted, but its replication and write safety settings can be modified like any other volume.
