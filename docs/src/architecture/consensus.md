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

- **ClientRequest**: An enum of all state-mutating commands (CreateFile, DeleteFile, UpdateChunkLocations, RegisterNode, DeregisterNode, UpdateNodeStatus, UpdateConfig).
- **ClientResponse**: The result returned after applying a command (Success, FileCreated, Error).
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

3. **Vote persistence**: Before granting a vote, each node persists the vote to RocksDB (`save_vote` in `LogStore`). This ensures that a node never votes twice in the same term, even after a crash.

   ```rust
   // From log_store.rs -- vote persistence with fsync
   async fn save_vote(&mut self, vote: &Vote) -> Result<(), io::Error> {
       let cf = self.db.cf_handle(CF_META).expect("meta CF");
       let bytes = bincode::serialize(vote)?;
       self.db.put_cf(&cf, META_KEY_VOTE, bytes)?;
       self.db.flush_cf(&cf)?;  // Flush to guarantee durability
       Ok(())
   }
   ```

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
| `meta` | `b"vote"` | bincode-encoded `Vote` | Current vote (persisted with fsync) |
| `meta` | `b"last_purged"` | bincode-encoded `Option<LogId>` | Last purged log index |

Key operations:

- **Append**: Write entries to the `logs` CF, then `flush_cf` to ensure durability. The `IOFlushed` callback notifies OpenRaft when the write is stable on disk.
- **Truncate after**: Delete all entries with index > the given log ID. Used when a follower needs to discard conflicting entries from a deposed leader.
- **Purge**: Delete entries up to and including the given index (after a snapshot). Uses RocksDB's `delete_range_cf` for efficiency.

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

The `apply_request` function handles each `ClientRequest` variant:

| Command | State Machine Effect |
|---------|---------------------|
| `CreateFile` | Insert into `files` BTreeMap, append audit log entry |
| `DeleteFile` | Remove from `files` BTreeMap, append audit log entry |
| `UpdateChunkLocations` | Update chunk list on existing file entry |
| `RegisterNode` | Insert into `nodes` BTreeMap |
| `DeregisterNode` | Remove from `nodes` BTreeMap |
| `UpdateNodeStatus` | Update health field on existing node entry |
| `UpdateConfig` | Replace `cluster_config` with new versioned JSON |

If the applying node is the leader and a client is waiting for the response, the response is sent back via the `responder` channel.

## Snapshots

Over time, the Raft log grows unboundedly. Snapshots compact the state machine into a point-in-time image so that old log entries can be purged.

### Snapshot Format

Snapshots are serialized as JSON via the `StateMachineSnapshot` struct:

```rust
pub struct StateMachineSnapshot {
    pub last_applied_log: Option<(u64, u64)>,  // (term, index)
    pub last_membership: Option<serde_json::Value>,
    pub files: BTreeMap<String, FileMetadata>,
    pub nodes: BTreeMap<u64, NodeInfo>,
    pub cluster_config: ClusterConfig,
    pub audit_log: Vec<AuditEntry>,
}
```

The snapshot is wrapped in a `Cursor<Vec<u8>>` for transport. The `last_applied_log` is stored as a simple `(term, index)` tuple to decouple the snapshot format from OpenRaft internal types.

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
  2. Store snapshot in memory (StoredSnapshot)
    |
    v
  3. Purge log entries up to snapshot's last_applied_log
    |  - delete_range_cf on the "logs" column family
    |  - Update last_purged in "meta" CF
    |
    v
  4. If a follower is too far behind (missing purged entries):
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
    |                                              |     - Replace state machine:
    |                                              |       files, nodes, config,
    |                                              |       audit_log
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

All reads in DTFS go to the Raft leader (or are forwarded to the leader by followers). This provides linearizable read consistency -- every read sees the result of the most recently committed write.

However, for metadata reads that can tolerate slightly stale data (such as file location lookups that the FUSE client will cache for 5 seconds anyway), a follower read optimization is possible. The FUSE client's metadata cache TTL already provides a natural staleness window.

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
| Chunk writes | Placement allocation fails without a leader. Writes that are already placed succeed if data nodes are reachable. |
| Node registration | Fails without a functioning leader. |
| Health monitoring | Heartbeats continue to the accessible manager nodes. Unreachable data nodes are marked suspect/down. |

### Data Node Behavior During Manager Partition

Data nodes operate independently of the manager for serving existing chunks. If a data node cannot reach any manager:

- **GetChunk** requests from FUSE clients continue to work for chunks already stored locally.
- **PutChunk** requests succeed for local storage, but the manager cannot update chunk locations in the Raft state machine until the partition heals.
- **Heartbeats** fail, so the data node appears unhealthy to any manager that can still reach it.

### FUSE Client Behavior During Partition

The FUSE client handles manager unavailability through:

1. **Metadata cache**: Reads for recently-accessed files continue to work using cached chunk locations (default 5-second TTL, but the data remains valid beyond the TTL as long as data nodes have not changed).
2. **Retry with backoff**: The `DtfsClient` retries failed RPCs with exponential backoff.
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

Snapshot data is base64-encoded within the JSON payload. While not the most efficient encoding, it simplifies the transport layer by keeping all Raft messages in a uniform format. For production clusters with large state machines, a streaming snapshot transfer would be more appropriate.

## State Machine Recovery

On startup, each manager node:

1. Opens the RocksDB log store and reads the persisted vote and last purged log ID.
2. Initializes an empty `StateMachineData`.
3. OpenRaft replays all log entries from the log store (from `last_purged + 1` to the latest entry), applying them to the state machine.
4. If a snapshot exists, it is loaded first, and only entries after the snapshot's `last_applied_log` are replayed.

This ensures that the state machine is always consistent with the committed log, even after a crash.

```
  Startup sequence:

  1. Open RocksDB at configured path
         |
  2. Read meta CF: vote, last_purged
         |
  3. Read logs CF: iterate all entries
         |
  4. If snapshot exists: install_snapshot()
         |  - Deserialize state machine from JSON
         |  - Set last_applied_log, last_membership
         |
  5. Replay log entries after snapshot
         |  - For each entry: apply to state machine
         |
  6. State machine is now up to date
         |
  7. Join Raft cluster (or start election if single-node)
```
