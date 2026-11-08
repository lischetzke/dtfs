# Consensus

DTFS uses [OpenRaft](https://github.com/databendlabs/openraft), a Rust implementation of the Raft consensus algorithm, to replicate all cluster metadata across manager nodes. This page covers leader election, log replication, snapshots, quorum behavior, and partition handling as implemented in the `dtfs-raft` crate.

## Why Raft for DTFS

The manager cluster must agree on a single, consistent view of:

- File metadata (paths, chunk sizes, chunk locations)
- Node registry (which data nodes exist, their health, disk inventory)
- Cluster configuration (replication policies, tiering thresholds)
- Audit log (immutable operation history)

Raft provides strong consistency (linearizable reads/writes on the leader), automatic leader election, and a well-understood failure model. DTFS deploys 3 or 5 manager nodes to tolerate 1 or 2 failures respectively.

## Type Configuration

DTFS parameterizes OpenRaft with application-specific types via the `TypeConfig` declaration in `dtfs-raft/src/lib.rs`:

```rust
openraft::declare_raft_types!(
    pub TypeConfig:
        D            = ClientRequest,    // Application commands
        R            = ClientResponse,   // Command results
        NodeId       = u64,              // Raft node IDs
        Node         = DtfsNode,         // Node metadata (addr, hostname)
        SnapshotData = Cursor<Vec<u8>>,  // Snapshot transport format
);
```

- **ClientRequest**: An enum of every state-mutating command: namespace (create, delete, rename, symlink, link, size), chunk locations (commit, update, remove), nodes and disks, volumes and policy, write leases, cache cooldowns, scrub and migration state, config and audit. A new mutation is a new variant plus an arm in `apply_request`. Fields that land in replicated state and need a time carry `#[serde(default)] now_ms`, stamped by the proposer.
- **ClientResponse**: The result returned after applying a command (Success, FileCreated, Error). A command the state machine refuses (for example removing the last live replica) comes back as an error response, not a Raft failure; callers use the shared response checker so a refusal is never treated as success.
- **DtfsNode**: Carries the gRPC address and human-readable hostname for each Raft peer, stored inside the Raft membership.

## Cluster Topology

A typical 3-node manager cluster:

```
  +------------------+       +------------------+       +------------------+
  |  Manager Node 1  |       |  Manager Node 2  |       |  Manager Node 3  |
  |  Raft ID: 1      |       |  Raft ID: 2      |       |  Raft ID: 3      |
  |  Role: Leader     | <--> |  Role: Follower   | <--> |  Role: Follower   |
  |                  |       |                  |       |                  |
  |  LogStore(RocksDB)|       |  LogStore(RocksDB)|       |  LogStore(RocksDB)|
  |  StateMachine    |       |  StateMachine    |       |  StateMachine    |
  +------------------+       +------------------+       +------------------+
         ^
         |  All client writes go to the leader
         |
  +------+------+
  |  dtfs-fuse  |
  |  dtfs-data  |
  +-------------+
```

All write operations (file creation, node registration, metadata updates) must go through the Raft leader. The leader proposes the command as a log entry, replicates it to followers, and applies it to the state machine once a quorum acknowledges.

## Leader Election

### Election Overview

```
  Normal operation:
  +--------+     heartbeats      +----------+
  | Leader | ==================> | Follower |
  +--------+     (AppendEntries  +----------+
                  with no new
                  entries)

  Leader failure detected (election timeout):
  +----------+                    +----------+
  | Follower |  "I'm a candidate" | Follower |
  | (now     |  RequestVote ----->|          |
  | Candidate)|                   |          |
  +----------+  <-- VoteGranted   +----------+
       |
       v
  +--------+
  | Leader |   (won majority)
  +--------+
```

### Election Process

1. **Timeout**: Each follower has a randomized election timeout. If a follower receives no `AppendEntries` from the leader within this timeout, it assumes the leader has failed.

2. **Candidate transition**: The follower increments its term, votes for itself, and sends `VoteRequest` RPCs to all other nodes.

3. **Vote persistence**: Before granting a vote, each node persists it to RocksDB (`save_vote` in `LogStore`) with a synced write, so a node never votes twice in the same term, even after a crash.

4. **Majority wins**: A candidate becomes leader once it receives votes from a majority of nodes (quorum). For a 3-node cluster, quorum is 2; for 5 nodes, quorum is 3.

5. **Split vote**: If two candidates split the vote, neither achieves a majority. Both revert to followers with incremented terms and try again after a new randomized timeout.

### Term and Vote Semantics

Each Raft node tracks:

- **Current term**: Monotonically increasing integer. A term represents a logical time period during which at most one leader exists.
- **Voted for**: The node ID this node voted for in the current term. Persisted to `RocksDB` in the `meta` column family under the key `vote`.

A node only grants its vote if:

- The candidate's term is >= the node's current term.
- The node has not already voted in this term (or voted for the same candidate).
- The candidate's log is at least as up-to-date as the node's log (compared by last log term, then last log index).

## Log Replication

### How Commands Are Replicated

```
  Client                    Leader (Node 1)           Follower (Node 2)     Follower (Node 3)
    |                            |                          |                      |
    |  ClientRequest             |                          |                      |
    |  (e.g., CreateFile)        |                          |                      |
    |  =========================>|                          |                      |
    |                            |                          |                      |
    |                            |  1. Append to local log  |                      |
    |                            |     (RocksDB, "logs" CF) |                      |
    |                            |     key: big-endian u64  |                      |
    |                            |     val: bincode(Entry)  |                      |
    |                            |                          |                      |
    |                            |  2. AppendEntries RPC    |                      |
    |                            |  ----------------------->|                      |
    |                            |  ----------------------->|--------------------->|
    |                            |                          |                      |
    |                            |  3. Followers append     |  3. Followers append |
    |                            |     to their local logs  |     to their local   |
    |                            |                          |     logs             |
    |                            |                          |                      |
    |                            |  4. ACK                  |  4. ACK              |
    |                            |  <-----------------------|                      |
    |                            |  <------------------------|----------------------|
    |                            |                          |                      |
    |                            |  5. Quorum reached (2/3) |                      |
    |                            |     Commit the entry     |                      |
    |                            |     Apply to state       |                      |
    |                            |     machine              |                      |
    |                            |                          |                      |
    |  6. ClientResponse         |                          |                      |
    |  (FileCreated)             |                          |                      |
    |  <=========================|                          |                      |
    |                            |                          |                      |
    |                            |  7. Next AppendEntries   |                      |
    |                            |     includes updated     |                      |
    |                            |     commit_index         |                      |
    |                            |  ----------------------->|--------------------->|
    |                            |                          |                      |
    |                            |                          |  8. Followers apply  |
    |                            |                          |     committed entries|
    |                            |                          |     to their state   |
    |                            |                          |     machines         |
```

### Log Storage (RocksDB)

The `LogStore` in `dtfs-raft/src/log_store.rs` uses RocksDB with two column families:

| Column Family | Key | Value | Purpose |
|---------------|-----|-------|---------|
| `logs` | Big-endian `u64` (log index) | bincode-encoded `Entry` | Raft log entries |
| `meta` | `b"vote"` | bincode-encoded `Vote` | Current vote (synced write) |
| `meta` | `b"last_purged"` | bincode-encoded `Option<LogId>` | Last purged log index |

Key operations:

- **Append**: Write entries to the `logs` CF with a synced write. The `IOFlushed` callback notifies OpenRaft when the write is stable on disk.
- **Truncate after**: Delete all entries with index > the given log ID with a range delete in a synced `WriteBatch`. Used when a follower discards conflicting entries from a deposed leader.
- **Purge**: Delete entries up to and including the given index (after a snapshot) with `delete_range_cf`, and persist `last_purged` in the same batch so a crash cannot leave one without the other.

### Entry Application

When an entry is committed (acknowledged by a quorum), the state machine applies it:

```rust
// Simplified from state_machine.rs
match &entry.payload {
    EntryPayload::Blank => ClientResponse::Success,
    EntryPayload::Membership(mem) => {
        sm.last_membership = StoredMembership::new(Some(log_id), mem.clone());
        ClientResponse::Success
    }
    EntryPayload::Normal(req) => apply_request(&mut sm, &log_id, req),
}
```

The `apply_request` function handles each `ClientRequest` variant. It is deterministic: its result depends only on the current state and the entry. It never reads the wall clock or a random source. Timestamps written during apply (audit rows, lease expiry, creation times) come from the proposer's `now_ms`, or from the committed `log_id.index` for legacy entries where `now_ms` is `0`. Replicas and replays therefore always agree.

Notable apply rules:

| Rule | Effect |
|------|--------|
| Lease-less `UpdateChunkLocations` | Union only: it adds the rows it names and never erases other replicas. It skips any chunk index that has no existing row carrying the same hash (it cannot introduce a new index), never grows `logical_size`, and returns `Error` when anything was skipped (the valid rows are still merged; callers treat it as a retry). Writes that hold a lease replace the chunk's rows |
| `RemoveCorruptChunkReplicas` | Drops the `(node, disk)` rows of the reported hashes, but never the last row of a `(file, chunk_index)`: the bytes are quarantined so reads fail with EIO instead of FUSE serving zeros, and an audit row records the kept index |
| `RemoveChunkLocation` | Refused if it would strand the anchor or remove the last live replica |
| `RegisterNode` | Sticky states (`Maintenance`, `Draining`, `Quarantined`) survive re-registration |
| `DeregisterNode` | Refused while the node still holds replicas |
| `RenameFile` | Directory renames re-key all children; directory-onto-file and file-onto-directory are refused |
| `CreateSymlink` / `CreateLink` | Refuse an occupied destination |

If the applying node is the leader and a client is waiting, the response is sent back through the `responder` channel.

## Snapshots

Over time, the Raft log grows unboundedly. Snapshots compact the state machine into a point-in-time image so that old log entries can be purged.

### Snapshot Format

A snapshot is the serialized `StateMachineData` (JSON) wrapped in a `Cursor<Vec<u8>>`, plus the last applied log id and membership. Every field added to `StateMachineData` must carry `#[serde(default)]` so older snapshots still load, and each new field gets a regression test.

### Snapshot Lifecycle

```
  Leader
    |
    |  Log grows beyond threshold
    |
    v
  1. DtfsSnapshotBuilder::build_snapshot()
    |  - Clone the entire StateMachineData (under read lock)
    |  - Serialize to JSON
    |  - Generate snapshot_id from last applied log
    |
    v
  2. Persist the snapshot to disk (temp file, rename). A failure here is
    |  returned to Raft as a fatal storage error: the snapshot is NOT
    |  published and the log is NOT purged
    v
  3. Cache it in memory (StoredSnapshot)
    |
    v
  4. Purge log entries up to snapshot's last_applied_log
    |  - delete_range_cf on the "logs" column family
    |  - last_purged written in the SAME batch as the range delete
    |
    v
  5. If a follower is too far behind (missing purged entries):
    |  - Send full snapshot via InstallSnapshot RPC
    |
    v
```

### Snapshot Installation on Followers

When a follower is too far behind to catch up via log entries (because those entries have been purged), the leader sends the entire snapshot:

```
  Leader                                    Lagging Follower
    |                                              |
    |  full_snapshot() RPC                         |
    |  (vote, meta, base64-encoded JSON blob)      |
    |  ------------------------------------------->|
    |                                              |
    |                                              |  1. begin_receiving_snapshot()
    |                                              |     Allocate empty Cursor
    |                                              |
    |                                              |  2. install_snapshot()
    |                                              |     - Deserialize JSON blob
    |                                              |     - Persist it to disk FIRST;
    |                                              |       on failure state is unchanged
    |                                              |     - Replace the whole state
    |                                              |       machine (one restore path
    |                                              |       shared with snapshot build)
    |                                              |     - Update last_applied_log
    |                                              |       and last_membership
    |                                              |
    |  SnapshotResponse                            |  3. Store snapshot blob
    |  <-------------------------------------------|     for future retrieval
    |                                              |
```

The snapshot data is transferred over gRPC as a base64-encoded JSON payload. The `full_snapshot` method on `DtfsNetwork` supports cancellation via `tokio::select!` so that replication can be aborted if leadership changes mid-transfer.

### Snapshot Trigger Policies

OpenRaft manages snapshot triggers automatically based on configuration. Common policies:

- **Log count threshold**: Take a snapshot when the log grows beyond N uncommitted entries.
- **Periodic**: Take a snapshot every N seconds regardless of log size.

After a snapshot is taken, all log entries up to the snapshot's `last_applied_log` can be purged, freeing RocksDB disk space.

## Quorum Behavior

### Quorum Sizes

| Cluster Size | Quorum | Tolerated Failures |
|-------------|--------|-------------------|
| 1 | 1 | 0 |
| 3 | 2 | 1 |
| 5 | 3 | 2 |
| 7 | 4 | 3 |

DTFS recommends 3-node or 5-node manager clusters. Single-node mode is supported for development but provides no fault tolerance.

### Write Quorum

A write is committed when the leader plus enough followers have persisted the log entry such that a strict majority of nodes have the entry on disk. For a 3-node cluster:

```
  Node 1 (Leader): log entry persisted  [X]
  Node 2 (Follower): log entry persisted [X]  <-- quorum reached (2 of 3)
  Node 3 (Follower): still replicating   [ ]

  --> Entry is committed and applied on the leader.
  --> Node 3 will eventually receive and apply it.
```

### Read Consistency

Writes are linearized through the leader (followers forward client mutations to it, authenticated with the shared secret). Metadata reads are served from the local state machine of whichever manager the client asked, including learners, so a read can lag the leader by a replication delay. Clients tolerate this: file locations are cached for 5 s anyway, and a stale location only costs a failover to another replica. Client mutations are not forwarded by learners, so a FUSE client's manager list must include at least one voter.

## Membership Changes

Raft supports dynamic membership changes to add or remove manager nodes without downtime. OpenRaft implements joint consensus for safe transitions:

```
  Current membership: {1, 2, 3}

  Add node 4:
    1. Propose joint config: {1,2,3} + {1,2,3,4}
       Quorum requires majority of BOTH sets
    2. Commit joint config
    3. Propose final config: {1,2,3,4}
    4. Commit final config

  Remove node 2:
    1. Propose joint config: {1,2,3} + {1,3}
    2. Commit joint config
    3. Propose final config: {1,3}
    4. Commit final config
```

During the joint configuration phase, entries must be committed by a majority of both the old and new configurations. This prevents split-brain scenarios during the transition.

## Network Partition Behavior

### Partition Scenarios

#### Scenario 1: Leader isolated from majority

```
  +-------+          PARTITION          +-------+   +-------+
  | Node1 |  X X X X X X X X X X X X  | Node2 |---| Node3 |
  | Leader |                            | Foll. |   | Foll. |
  +-------+                            +-------+   +-------+

  Result:
  - Node 1 can no longer reach a quorum. Its writes will time out.
  - Nodes 2 and 3 elect a new leader (say Node 2).
  - New writes succeed on {Node2, Node3}.
  - Node 1 eventually steps down when it sees a higher term.
  - When the partition heals:
    - Node 1 discovers the new leader with a higher term.
    - Node 1 truncates any uncommitted entries from its log.
    - Node 1 catches up from the new leader's log (or via snapshot).
```

#### Scenario 2: One follower isolated

```
  +-------+   +-------+          PARTITION          +-------+
  | Node1 |---| Node2 |  X X X X X X X X X X X X  | Node3 |
  | Leader |   | Foll. |                            | Foll. |
  +-------+   +-------+                            +-------+

  Result:
  - Leader (Node 1) can still reach Node 2. Quorum: {1, 2} = majority.
  - Writes continue to succeed normally.
  - Node 3 falls behind but does not disrupt the cluster.
  - When the partition heals:
    - Node 3 catches up via AppendEntries from the leader.
    - If too far behind, the leader sends a snapshot.
```

#### Scenario 3: Three-way partition (no majority possible)

```
  +-------+          +-------+          +-------+
  | Node1 |  X X X  | Node2 |  X X X  | Node3 |
  +-------+          +-------+          +-------+

  Result:
  - No node can reach a majority. The cluster is unavailable for writes.
  - All pending writes time out.
  - The cluster automatically recovers when any two nodes can communicate.
```

### Impact on DTFS Operations During Partitions

| Operation | Behavior During Partition |
|-----------|-------------------------|
| File creation | Fails if leader cannot reach quorum. Client receives timeout error. |
| File reads (cached) | Succeeds if FUSE client has cached metadata and data node is reachable. |
| File reads (uncached) | Fails if metadata lookup cannot reach a functioning leader. |
| Chunk reads | Succeeds as long as at least one replica data node is reachable (data path does not go through Raft). |
| Chunk writes | Allocation, write leases and the final location commit all go through the leader, so a write cannot complete without one. Bytes already streamed to data nodes are not lost; they become orphans if the commit never happens and are reaped after the grace period. |
| Node registration | Fails without a functioning leader. |
| Health monitoring | Heartbeats continue to the accessible manager nodes. Unreachable data nodes are marked suspect/down. |

### Data Node Behavior During Manager Partition

Data nodes operate independently of the manager for serving existing chunks. If a data node cannot reach any manager:

- **GetChunk** requests from FUSE clients continue to work for chunks already stored locally.
- **PutChunk** requests can still land bytes, but a client cannot commit their locations until it reaches a leader.
- **Heartbeats** fail, so the data node appears unhealthy to any manager that can still reach it.

### FUSE Client Behavior During Partition

The FUSE client handles manager unavailability through:

1. **Metadata cache**: Reads of recently-accessed files keep working from cached chunk locations for the 5-second TTL. After that the client must reach a manager again; an expired entry is dropped, not served stale.
2. **Manager walk**: `DtfsClient` walks its manager list and, when no usable manager answers (election, rolling restart), re-walks with backoff for up to 15 s before surfacing an error (EIO on a write). See [Partition Behavior](../access/partition-behavior.md).
3. **Direct data node access**: Once the client knows which data nodes hold a chunk, it communicates directly with them. The data path is independent of the manager.

## Raft Transport

The gRPC transport is implemented in `dtfs-raft/src/network.rs`. All Raft messages are serialized as JSON and sent over a single generic gRPC method:

```
  Service: dtfs.manager.RaftTransport
  Method:  RaftRpc (unary)
  Request: JSON-encoded RaftMessage { kind, payload }
  Response: JSON-encoded RaftMessage { kind, payload }
```

The `RaftRpcKind` enum identifies the message type:

| Kind | OpenRaft Method | Purpose |
|------|----------------|---------|
| `AppendEntries` | `append_entries()` | Log replication and heartbeats |
| `Vote` | `vote()` | Leader election |
| `Snapshot` | `full_snapshot()` | Snapshot transfer to lagging followers |

The `DtfsNetwork` struct lazily creates a tonic `Channel` on first use and caches it. If a connection fails, the channel is cleared so the next attempt reconnects.

When `cluster.shared_secret` is set, every Raft RPC carries the `x-dtfs-rpc-ts` and `x-dtfs-rpc-mac` stamp. The MAC is bound to `"<from>\n<to>\n<blake3(body)>"` (sender id in the `x-dtfs-raft-from` header, receiver's own id, hash of the payload), so it is checked after the size-bounded body read; unsigned RPCs are accepted only while `cluster.require_raft_auth` is false (rolling-update window). The request body is capped at the manager gRPC message limit. See [Authentication](../security/authentication.md#raft-transport).

Snapshot data is base64-encoded within the JSON payload. While not the most efficient encoding, it simplifies the transport layer by keeping all Raft messages in a uniform format. For production clusters with large state machines, a streaming snapshot transfer would be more appropriate.

## State Machine Recovery

On startup, each manager node:

1. Opens the RocksDB log store and reads the persisted vote and last purged log id.
2. Loads the newest persisted snapshot, if any, and restores the state machine from it.
3. Applies the safety check: a node whose state machine is blank (or whose last applied index is below `last_purged`) while the log has been purged **refuses to start as a voter**. Starting would silently serve an empty or stale state; the operator must restore the snapshot or rejoin the node as a learner.
4. Lets OpenRaft replay log entries after the snapshot's `last_applied_log`.

Two refusals protect against silent data loss: a **blank** state machine (for example an unreadable, discarded snapshot) next to a purged log, and a **stale** state machine whose last applied index is below the purged index (for example an old snapshot restored beside a newer log). In both cases the recovery is to restore `raft-sm` from a recent backup, or wipe `raft-log` and `raft-sm` and re-join the node as a learner from a healthy peer.
