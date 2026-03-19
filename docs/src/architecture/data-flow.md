# Data Flow

This page traces the complete read and write data paths through the DTFS cluster, covering the interactions between FUSE clients, manager nodes, and data nodes. It also covers replication, write safety, transparent remote-DC fetch, and background migration.

## High-Level Architecture Recap

Bulk data never flows through the manager. The manager provides metadata and placement decisions only; all chunk data travels directly between FUSE clients and data nodes (or between data nodes during replication).

```
+-------------+       metadata (gRPC)       +----------------+
|  dtfs-fuse  | <-------------------------> |  dtfs-manager   |
|  /mnt/dtfs  |                             |  (Raft leader)  |
+------+------+                             +-------+--------+
       |                                            |
       |  chunk data (gRPC streaming)               |  placement,
       |                                            |  replication
       v                                            |  commands
+-------------+       replication (gRPC)    +-------v--------+
|  dtfs-data  | <------------------------> |   dtfs-data     |
|  (node-a)   |                             |   (node-b)     |
+-------------+                             +----------------+
```

## Read Path

A FUSE `read()` call traverses the following stages:

```
  Application
      |
      | read(fd, buf, 4096)
      v
  +-----------+
  | dtfs-fuse |  1. Translate offset to chunk index
  |           |  2. Check metadata cache (TTL 5s)
  |           |  3. If miss: GetFileLocations(path) -> manager
  |           |  4. Select nearest node from replica list
  |           |  5. Check read-ahead cache
  |           |  6. If miss: GetChunk(chunk_id) -> data node
  |           |  7. Trigger read-ahead for next chunks
  |           |  8. Trigger migration check if remote read
  |           |  9. Extract byte range, return to caller
  +-----------+
```

### Step-by-Step Read Sequence

```
  dtfs-fuse                dtfs-manager              dtfs-data (node-a)
      |                         |                          |
      |  1. GetFileLocations    |                          |
      |  ---------------------->|                          |
      |                         |                          |
      |  2. Response:           |                          |
      |  path, total_size,      |                          |
      |  chunks: [              |                          |
      |    { chunk_id: "abc",   |                          |
      |      size: 128MB,       |                          |
      |      nodes: [           |                          |
      |        "node-a:9500",   |                          |
      |        "node-b:9500",   |                          |
      |        "node-c:9500"    |                          |
      |      ]                  |                          |
      |    }, ...               |                          |
      |  ]                      |                          |
      |  <----------------------|                          |
      |                         |                          |
      |  3. GetChunk("abc")     |                          |
      |  ------------------------------------------------>|
      |                         |                          |
      |  4. Streamed chunk data |                          |
      |  <-------------------------------------------------|
      |                         |                          |
```

### Metadata Caching

The FUSE client caches `FileLocations` in a `DashMap` with a configurable TTL (default 5 seconds). On each `lookup()` or `open()`:

1. Check the cache by path. If the entry exists and `cached_at.elapsed() < metadata_ttl`, return the cached result.
2. Otherwise, call `GetFileLocations` on the manager, store the result, and return it.

Cache invalidation happens automatically on TTL expiry and explicitly after a `flush()` or `unlink()`.

### Nearest-Node Selection

When a chunk has multiple replicas across different nodes, the FUSE client selects the nearest one based on topology distance. The client compares its own configured `Location` (region/datacenter/rack/host) against each replica node's location:

```
  Priority order (closest first):

  1. SameHost       -- co-located on the same machine
  2. SameRack       -- same rack, different host
  3. SameDatacenter -- same DC, different rack
  4. SameRegion     -- same region, different DC
  5. CrossRegion    -- different region entirely
```

The selection function is in `dtfs_client::location::select_nearest_node`. If location data is unavailable for a node, the first address in the list is used as a fallback.

### Read-Ahead Prefetching

After every successful chunk read, the `ReadAheadEngine` speculatively prefetches upcoming chunks in background tokio tasks. This improves throughput for sequential reads because by the time the application requests the next chunk, it is already in the prefetch cache.

The read-ahead engine supports multiple modes configured via `fuse.toml`:

| Mode | Behavior |
|------|----------|
| `disabled` | No prefetching |
| `sequential` | Prefetch the next N chunks after each read |
| `aggressive` | Prefetch N chunks ahead plus adjacent files |

Prefetched data is stored in a bounded in-memory cache (default 256 entries). Cache hits are logged at DEBUG level.

## Write Path

A FUSE `write()` call follows this sequence:

```
  Application
      |
      | write(fd, buf, len)
      v
  +-----------+
  | dtfs-fuse |  1. Buffer data in WriteBuffer (chunk-aligned)
  |           |  2. When a full chunk is ready:
  |           |     a. AllocateChunks(path, count, volume_id) -> manager
  |           |     b. PutChunk(chunk_id, data) -> primary data node
  |           |  3. On flush/close: flush_all (including partial chunks)
  +-----------+
```

### Write Buffering

The `WriteBuffer` accumulates writes in memory and produces chunk-aligned segments. Small writes (e.g., 4KB application writes into a 128MB chunk) are coalesced until a full chunk boundary is reached. This avoids sending many small RPCs to data nodes.

```
  Application writes:   [4KB] [4KB] [4KB] ... [4KB]
                         |     |     |          |
                         v     v     v          v
  WriteBuffer:          [========= 128MB chunk =========]
                                     |
                                     v  (chunk complete)
                         flush_segments_to_cluster()
```

Key behaviors:

- **Aligned flush**: `flush()` only extracts segments where `chunk_size` bytes are complete. Partial trailing data remains in the buffer.
- **Full flush**: `flush_all()` extracts all data, including partial final chunks. This is called on `close()` and explicit `fsync()`.
- **Overlapping writes**: If the application overwrites a region already in the buffer, the new data replaces the old data in place.
- **Gap filling**: Disjoint writes to the same file are joined with zero-filled gaps (optimized for sequential patterns).

### Write Sequence Diagram

```
  dtfs-fuse               dtfs-manager              dtfs-data (primary)    dtfs-data (replica)
      |                        |                          |                       |
      |  1. AllocateChunks     |                          |                       |
      |  --------------------->|                          |                       |
      |                        |                          |                       |
      |  2. Response:          |                          |                       |
      |  placements: [         |                          |                       |
      |    { primary: node-a,  |                          |                       |
      |      replicas: [       |                          |                       |
      |        node-b, node-c  |                          |                       |
      |      ]                 |                          |                       |
      |    }                   |                          |                       |
      |  ]                     |                          |                       |
      |  <---------------------|                          |                       |
      |                        |                          |                       |
      |  3. PutChunk(data)     |                          |                       |
      |  ------------------------------------------------>|                       |
      |                        |                          |                       |
      |                        |                          |  4. replicate_to()    |
      |                        |                          |  --------------------->|
      |                        |                          |                       |
      |  5. PutChunk ACK       |                          |  6. Replication ACK   |
      |  <-------------------------------------------------|  <-------------------|
      |                        |                          |                       |
      |                        |  7. UpdateChunkLocations |                       |
      |                        |  <-----------------------|                       |
      |                        |  (via Raft commit)       |                       |
      |                        |                          |                       |
```

### Chunk Storage on Data Nodes

When a data node receives a `PutChunk` request, it follows a crash-safe write sequence:

```
  PutChunk RPC arrives
      |
      v
  1. Write WAL entry       -->  .dtfs/wal/{chunk_id}.wal.json
      |
      v
  2. Compute BLAKE3 checksum of data
      |
      v
  3. Write data to tmp     -->  tmp/{chunk_id}.dat.tmp
      |
      v
  4. fsync the tmp file
      |
      v
  5. Atomic rename          -->  chunks/{ab}/{cd}/{hash}.dat
      |
      v
  6. Write chunk metadata   -->  chunks/{ab}/{cd}/{hash}.meta
      |
      v
  7. fsync metadata file
      |
      v
  8. Remove WAL entry       -->  .dtfs/wal/{chunk_id}.wal.json
      |
      v
  9. ACK to caller
```

The two-level directory structure (`{first_byte}/{second_byte}/`) under `chunks/` prevents any single directory from accumulating too many files, which is important for XFS performance.

On crash recovery, the data node replays all WAL entries:

- **Incomplete PutChunk**: Remove orphaned tmp files.
- **Incomplete DeleteChunk**: Complete the deletion by removing data and metadata files.

### Write Safety

Write safety is configured per-volume via `WriteSafety`:

```toml
[volume.write_safety]
min_replicas_before_ack = 2   # ACK after 2 replicas confirmed
write_timeout_secs = 30       # Timeout for the entire write
```

The primary data node receives the chunk from the FUSE client, writes it locally, then pushes replicas to the other nodes in the placement list. The write is acknowledged back to the FUSE client after `min_replicas_before_ack` replicas (including the primary) have confirmed successful storage. Remaining replicas complete asynchronously.

```
  Timeline for min_replicas_before_ack = 2, replication_factor = 3:

  t0: FUSE client sends PutChunk to primary (node-a)
  t1: node-a stores chunk locally (WAL + fsync)       --> replica 1 done
  t2: node-a starts replication to node-b and node-c
  t3: node-b confirms storage                          --> replica 2 done
  t4: node-a ACKs PutChunk to FUSE client              --> WRITE COMPLETE
  t5: node-c confirms storage (async, not blocking)    --> replica 3 done
```

## Replication

Replication is handled by the `ReplicationManager` on each data node. It supports two modes:

### Push Replication (Primary to Replicas)

After the primary stores a chunk, it pushes copies to all designated replica nodes. The data is streamed in 1MB pieces over gRPC:

```
  node-a (primary)                     node-b (replica)
      |                                      |
      |  PutChunk (streaming, 1MB pieces)    |
      |  ----------------------------------->|
      |                                      |
      |     piece 1: chunk_id + volume_id    |
      |     piece 2: data[0..1MB]            |
      |     piece 3: data[1MB..2MB]          |
      |     ...                              |
      |     piece N: data[(N-1)MB..end]      |
      |                                      |
      |  PutChunkResponse (success, size)    |
      |  <-----------------------------------|
      |                                      |
```

The first piece in the stream carries metadata (chunk ID, volume ID, tier). Subsequent pieces carry raw data. This avoids loading the entire chunk (up to 128MB) into memory at once.

### Pull Replication (Replica from Source)

Used when a data node needs to fetch a chunk it does not have, for example during re-replication after a node failure:

```
  node-c (new replica)                 node-a (source)
      |                                      |
      |  GetChunk(chunk_id)                  |
      |  ----------------------------------->|
      |                                      |
      |  Streamed response                   |
      |  <-----------------------------------|
      |                                      |
      |  Store locally via ChunkStore        |
      |                                      |
```

### Per-Tier Replication Factors

Each volume defines independent replication factors per storage tier:

```toml
[volume.replication]
hot  = 3    # 3 replicas on NVMe/SSD
warm = 2    # 2 replicas on SSD
cold = 1    # 1 replica on HDD (erasure coding planned)
```

The placement engine uses the appropriate replication factor when computing placements for each tier. When data is migrated between tiers (e.g., hot -> warm), the replication factor changes accordingly, and excess replicas are garbage-collected.

## Transparent Remote-DC Fetch

When a FUSE client in one datacenter reads a chunk whose replicas are all in another datacenter, the read still succeeds transparently. The process:

```
  DC-EAST (client)                     DC-WEST (data nodes)
  +-------------+                      +-------------+
  | dtfs-fuse   |                      | dtfs-data   |
  | location:   |   GetChunk(chunk_id) | (node-w1)   |
  |  dc=east    |  ------------------->|             |
  |             |  <-------------------|  (streamed) |
  +------+------+                      +-------------+
         |
         |  Remote read detected!
         |  distance >= SameDatacenter
         v
  +------+------+
  | Migration   |  allocate_chunks(path, 1, volume_id)
  | Trigger     |  ----------------------------------> dtfs-manager
  +-------------+
         |
         |  Manager schedules background migration
         |  to place a replica in DC-EAST
         v
  (Future reads served from local DC)
```

### Migration Trigger Details

The `MigrationTrigger` in `dtfs-fuse` fires after every chunk read. It computes the topology distance between the FUSE client's location and the data node's location:

1. If distance < `SameDatacenter` (i.e., SameHost or SameRack), the read is local -- no action.
2. If distance >= `SameDatacenter`, the read is considered remote.
3. A migration request is sent to the manager, rate-limited to once per chunk per 5 minutes (`MIGRATION_COOLDOWN`).

The manager receives the allocation request and uses the placement engine's `place_with_affinity()` method, which respects `prefer_client_locations` to favor placing a new replica in the requesting client's datacenter.

### Multi-DC Read Path Example

Consider a three-DC deployment:

```
  +--DC-EAST--+     +--DC-CENTRAL--+     +--DC-WEST--+
  |           |     |              |     |           |
  | dtfs-fuse |     | dtfs-manager |     | dtfs-data |
  | (client)  |     | (leader)     |     | (node-w1) |
  |           |     |              |     | (node-w2) |
  | dtfs-data |     | dtfs-data    |     | (node-w3) |
  | (node-e1) |     | (node-c1)   |     |           |
  +-----------+     +--------------+     +-----------+

  1. Client opens file, gets chunk locations from manager.
     Chunk "abc" replicas: [node-w1, node-w2]

  2. Client selects nearest: both are in DC-WEST.
     Picks node-w1 (first in list).

  3. Client reads chunk "abc" from node-w1 (cross-DC).
     Read succeeds, data returned to application.

  4. MigrationTrigger detects: client=DC-EAST, node=DC-WEST
     distance = SameRegion (or CrossRegion)
     Sends migration request to manager.

  5. Manager runs placement with client affinity for DC-EAST.
     Selects node-e1 (in client's DC) for new replica.
     Schedules replication: node-w1 -> node-e1.

  6. Background: node-e1 pulls chunk "abc" from node-w1.
     Manager updates chunk locations via Raft.

  7. Next read: client's metadata cache expires (5s TTL).
     Fresh GetFileLocations returns: [node-w1, node-w2, node-e1]
     Client selects node-e1 (SameDatacenter). Local read!
```

## Background Migration (Tiering)

The tiering engine on the manager leader periodically evaluates data temperature and moves chunks between storage tiers:

```
  dtfs-manager (leader)
      |
      |  Tiering engine evaluates access stats
      |  from data node heartbeats
      |
      |  Chunk "xyz" classified as "cold"
      |  Currently on hot tier (SSD)
      |  Policy says cold data -> cold tier (HDD)
      |
      v
  +------------------+
  | Task Scheduler   |  Creates migration task:
  |                  |    source: node-a (hot/SSD)
  |                  |    target: node-d (cold/HDD)
  |                  |    chunk: "xyz"
  +--------+---------+
           |
           v
  Migration task lifecycle:

  1. Pending    -- queued in scheduler
  2. Copying    -- target node pulls chunk from source
  3. Verifying  -- target node validates checksum
  4. Completing -- manager updates metadata via Raft
  5. Done       -- source copy scheduled for GC
```

The migration status transitions:

```
  Pending --> Copying --> Verifying --> Completing --> Done
      |          |            |             |
      +----------+------------+-------------+---> Failed
                     (on any error)
```

Key points:

- Migrations are non-blocking: reads continue to be served from the current location while the copy is in progress.
- The Raft state machine is only updated after the new copy is verified, ensuring no metadata points to incomplete data.
- Replication factor changes when crossing tiers: hot (3 replicas) -> cold (1 replica) means two excess replicas are eventually garbage-collected.
- Tiering decisions are based on configurable thresholds for access frequency and recency.

## End-to-End Write Flow Summary

The complete lifecycle of a write, from application to durable storage:

```
  1. Application calls write(fd, buf, 4096)
         |
  2. FUSE layer calls DtfsFilesystem::write(fh, offset, data)
         |
  3. WriteBuffer accumulates data
         |
  4. When chunk boundary reached: flush_segments_to_cluster()
         |
  5. AllocateChunks RPC to manager
         |  Returns: primary node + replica list
         |
  6. PutChunk RPC to primary data node
         |  Primary writes: WAL -> tmp -> fsync -> rename -> meta -> fsync -> WAL remove
         |
  7. Primary replicates to replica nodes
         |  Each replica: WAL -> tmp -> fsync -> rename -> meta -> fsync -> WAL remove
         |
  8. After min_replicas_before_ack confirmed: ACK to FUSE client
         |
  9. Primary reports chunk locations to manager
         |  Manager commits UpdateChunkLocations via Raft
         |
  10. FUSE client receives success, write() returns
```

## End-to-End Read Flow Summary

```
  1. Application calls read(fd, buf, 4096)
         |
  2. FUSE layer calls DtfsFilesystem::read(fh, offset, size)
         |
  3. Check metadata cache for chunk locations
         |  Hit: use cached locations
         |  Miss: GetFileLocations RPC to manager
         |
  4. Compute which chunks span [offset, offset+size)
         |
  5. For each chunk:
         |  a. Select nearest data node from replica list
         |  b. Check read-ahead cache
         |     Hit: use cached chunk data
         |     Miss: GetChunk RPC to data node
         |  c. Data node reads chunk from XFS disk, verifies BLAKE3 checksum
         |  d. Trigger read-ahead for upcoming chunks
         |  e. Trigger migration check if read was remote
         |
  6. Extract byte range from chunk data
         |
  7. Return bytes to application
```
