# Partition Behavior

When a network partition splits a DTFS cluster, some nodes lose contact with the Raft leader and cannot commit metadata operations. DTFS provides configurable behavior modes that control what operations clients can perform during quorum loss, allowing operators to choose the right trade-off between availability and consistency.

## Background: Quorum and Partitions

The manager cluster uses Raft consensus, which requires a majority quorum (more than half the nodes) to commit operations. In a 3-node manager cluster, 2 nodes form a quorum. In a 5-node cluster, 3 nodes form a quorum.

When a network partition occurs:

- The **majority side** retains quorum and continues normal operation.
- The **minority side** loses quorum and cannot commit metadata changes (file creation, deletion, rename, chunk placement updates).

DTFS clients connected to the minority side are affected. The partition behavior mode determines what these clients can still do.

## Modes

The partition behavior mode is configured on the FUSE client:

```toml
[partition]
mode = "strict"
```

### strict (default)

The strictest consistency mode. During quorum loss:

- **Local reads**: permitted. Clients can read chunks that are cached locally or stored on data nodes reachable from the client. Metadata is served from the client's local metadata cache (subject to TTL).
- **Cross-datacenter reads**: blocked. Reads that would require contacting a data node in another datacenter are rejected, because the client cannot verify that the chunk's placement metadata is current without quorum.
- **All writes**: blocked. No file creation, modification, deletion, or rename is permitted. Write operations return `EIO` (I/O error) to the application.

This mode guarantees that clients never read stale data from a remote datacenter that may have been updated by the majority side. It is the recommended mode for workloads that require strong consistency.

```text
Partition occurs (client on minority side):
  read local chunk  --> OK (served from local data node / cache)
  read remote chunk --> EIO (cross-DC read blocked)
  write any file    --> EIO (no quorum for metadata commit)
  mkdir / rename    --> EIO (no quorum for metadata commit)
```

### local-readonly

A relaxed mode that allows reads from any reachable data node, even across datacenter boundaries:

- **All reads**: permitted, from any reachable data node. Metadata is served from the local cache. Reads may return stale data if the majority side has processed writes that the minority side has not seen.
- **All writes**: blocked.

This mode trades consistency for read availability. It is appropriate for workloads that can tolerate stale reads during a partition, such as serving static assets or read-heavy analytics.

```text
Partition occurs (client on minority side):
  read local chunk  --> OK
  read remote chunk --> OK (may be stale)
  write any file    --> EIO
  mkdir / rename    --> EIO
```

**Stale read risk**: in local-readonly mode, a file that was deleted or modified on the majority side may still be readable from the minority side until the partition heals and the metadata cache refreshes. Applications must tolerate this possibility.

### full-lock

The most conservative mode. During quorum loss, all operations are blocked:

- **All reads**: blocked, including local and cached reads.
- **All writes**: blocked.

This mode is intended for workloads where reading stale data is worse than being unavailable. Financial transaction logs, audit records, and other compliance-critical data may warrant this setting.

```text
Partition occurs (client on minority side):
  read local chunk  --> EIO
  read remote chunk --> EIO
  write any file    --> EIO
  mkdir / rename    --> EIO
```

## Configuration

### FUSE Client

```toml
[partition]
# Mode: "strict", "local-readonly", "full-lock"
mode = "strict"

# How quickly the client detects quorum loss.
# The client considers quorum lost if it cannot reach
# the manager leader for this duration.
detection_timeout_secs = 10

# Grace period after quorum is regained before resuming
# full operations. Allows metadata caches to refresh.
recovery_grace_secs = 5
```

### Per-Volume Override

Different volumes can use different partition modes:

```toml
[volume.financial-records.partition]
mode = "full-lock"

[volume.static-assets.partition]
mode = "local-readonly"
```

This allows a single cluster to serve workloads with different consistency requirements.

## Quorum Loss Detection

The FUSE client maintains a heartbeat connection to the manager leader. When the leader becomes unreachable, the client attempts to discover a new leader by contacting the other manager nodes. If no leader is found within `detection_timeout_secs`, the client enters the partitioned state and applies the configured behavior mode.

When the partition heals and a leader becomes reachable again, the client waits `recovery_grace_secs` before resuming normal operations. During this grace period:

1. The metadata cache is invalidated and refreshed from the leader.
2. Any writes that were queued (in modes that support write queuing, which is not the default) are flushed.
3. The client verifies that its local view of the filesystem is consistent with the leader's state.

## Behavior Summary

| Operation | strict | local-readonly | full-lock |
|-----------|--------|----------------|-----------|
| Read (local/cached data) | Allowed | Allowed | Blocked |
| Read (remote data node) | Blocked | Allowed (may be stale) | Blocked |
| Write (any) | Blocked | Blocked | Blocked |
| Metadata mutation (mkdir, rename, delete) | Blocked | Blocked | Blocked |
| Stat / readdir (cached) | Allowed | Allowed | Blocked |

## Monitoring

Partition-related metrics and events:

- `dtfs_partition_state{mode="normal|partitioned"}` -- current partition state (gauge, 1 = active).
- `dtfs_partition_events_total{type="detected|recovered"}` -- total partition events.
- `dtfs_partition_duration_seconds` -- histogram of partition duration.
- `dtfs_partition_blocked_ops_total{op="read|write|metadata"}` -- operations blocked due to partition mode.

The manager also logs partition events at `WARN` level when it detects that a client has entered the partitioned state.
