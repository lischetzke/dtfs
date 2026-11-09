# Data Flow

This page traces what happens, step by step, for the common operations and for the common failures. Terminology is in the [Architecture Overview](./overview.md).

## Write

```text
  app write()                dtfs-fuse                 manager (leader)        data nodes
      |                         |                            |                     |
      |-- bytes --------------->| 1. buffer by chunk         |                     |
      |                         | 2. AcquireWriteLease ----->| (Raft)              |
      |                         |    (120 s, renewed at 60 s)|                     |
      |                         | 3. AllocateChunks -------->| placement           |
      |                         |<-- targets per chunk ------|                     |
      |                         | 4. hash chunk (BLAKE3)     |                     |
      |                         | 5. PutChunk (stream) ------------------------->  all targets
      |                         |    in parallel             |        tmp, fsync, rename, .meta
      |                         | 6. need ceil(N/2) acks     |                     |
      |                         | 7. CommitChunkLocations -->| (Raft) checks       |
      |                         |    one row per acked replica   min_replicas_before_ack
      |<-- ok ------------------|                            |                     |
```

1. **Buffer.** The write buffer groups bytes by chunk (default chunk size 4 MB; the size must match the volume). A partial overwrite of an existing chunk first reads the old chunk once to seed the buffer (read-modify-write), then merges the new bytes. A write whose chunk size does not match the file's is refused with an error rather than committed misaligned.
2. **Lease.** The client holds a write lease on the path (120 s, renewed after 60 s). A lapsed lease is re-acquired once under the same owner id; a lease held by another writer aborts the write. The local deadline is tracked on the handle so a renew is sent before a long flush, not after.
3. **Allocate.** The manager places each chunk by the file's own registered volume, using the hash ring over healthy nodes with room, forcing one replica into the anchor datacenter (see [Data Distribution](./data-distribution.md)).
4. **Hash.** The client hashes the chunk once. The hash is the chunk id and is sent as the declared id, so the data node can reject a bit flip between the client buffer and its own incremental hash.
5. **Put.** The client streams the chunk to every target in parallel. Each data node writes `tmp/<uuid>.dat.tmp` incrementally while hashing, fsyncs, renames to `chunks/<aa>/<bb>/<id>.dat`, then writes `.meta` the same way, fsyncs the chunk directory once for both renames, then clears the WAL entry. If a disk is full (`RESOURCE_EXHAUSTED`), the client re-allocates that chunk excluding the full node (bounded retries).
6. **Quorum on the client.** The write proceeds when at least `ceil(N/2)` targets acknowledged with a matching chunk id. A node that returns a different id is treated as failed.
7. **Commit.** `CommitChunkLocations` is one Raft command. The manager rejects it when any chunk has fewer distinct `(node_id, disk_id)` replicas than the volume's `write_safety.min_replicas_before_ack` (counting only replicas on maintained tiers; allocation fills the floor across such tiers, and the floor is clamped only to what the cluster can hold). Replicas that missed the quorum are created later by the repair scheduler.

Large writes flush in batches of complete chunks (`write.flush_batch_chunks`, default 4) so memory stays bounded. The chunks of one batch are put up to `write.upload_concurrency` (default 2) at a time, each to all its targets; the batch's single `CommitChunkLocations` is sent only after every chunk reached its quorum, so one failed chunk commits nothing of the batch. If a flush fails, the drained bytes are restored to the buffer. On `release`, a file that never committed a chunk (a create that failed) is unlinked (the "ghost" case); a file that has ever committed is never ghost-unlinked.

Other write-side operations:

- **rename** is one Raft command; chunk ids do not change. The client moves its buffers, inode mappings and mode overrides with the path, and descendants of a renamed directory are re-keyed.
- **truncate** to a smaller unaligned size rewrites the tail chunk through the normal lease and commit path. A bare path `truncate` (no open writer) opens a temporary internal writer for that.
- **unlink** removes the file's metadata only. Bytes are reclaimed later by the orphan reaper (see below).

## Read

```text
  app read()        dtfs-fuse                                 manager     data node
     |                 |  1. metadata cache (5 s)                |            |
     |                 |  2. miss: GetFileLocations ------------>|            |
     |                 |<-- chunk list, replicas (live only) ----|            |
     |                 |  3. readahead memory cache? disk cache? |            |
     |                 |  4. order replicas, GetChunk (stream) ------------->|
     |                 |<-- bytes, verified server side ---------------------|
     |                 |  5. BLAKE3 of bytes == chunk id ?       |            |
     |<-- bytes -------|  6. cache, prefetch ahead, RecordCacheHit ---------->|
```

1. **Metadata.** Locations are cached per path for 5 s (bounded to 4096 entries). The response lists only live replicas unless none are live. It carries each replica's tier and location so the client can rank them.
2. **Replica order.** `select_read_replica` ranks candidates: configured `read_location_priority` first, then physical distance (same host, rack, datacenter, region), then tier only on equal distance (prefer warm over the cold anchor HDD). A strictly nearer replica always wins.
3. **Caches.** The read-ahead memory cache is checked first, then the optional local disk cache. A disk cache hit is also inserted into the memory cache. Concurrent misses for the same chunk are single-flighted: one fetch, the others wait. A fetched chunk is written to the disk cache in the background (at most 2 such writes in flight; beyond that the write is skipped), so the reader does not wait for the disk.
4. **Fetch and fail over.** The client tries candidates in order, each bounded by `client.chunk_read_timeout_secs` (default 60 s). The data node streams the file in pieces. With `verify_on_read` (default on) it hashes the file first and answers `data_loss` on a mismatch, so the client moves to the next replica. The read fails only after every candidate failed.
5. **Client verify.** The client hashes the streamed bytes and compares them with the chunk id. A mismatch is an integrity fault and also fails over.
6. **After the read.** The client stores the chunk in its caches, prefetches the following chunks, and tells a data node about the read (`RecordCacheHit`, at most once per file handle and chunk transition). Those read counts feed tiering.

`readdir` takes one snapshot of the directory per `opendir` and serves pages from it. The kernel's lookup and forget counts are tracked so an inode is dropped only when the kernel has forgotten it.

## A data node goes down

```text
  t=0     last heartbeat from node D
  t=15 s  3 missed: D is Suspect       (monitor)
  t=30 s  6 missed: D is Down          (monitor, writes the state through Raft)
  next    GetFileLocations stops returning D's replicas (unless a chunk has no
  tick    live replica); reads fail over to other replicas
  next    scheduler (leader): chunks whose LIVE replica count is below
  tick    mandatory_replicas  -->  copy from a live source to a new disk,
          verify the copy, then register ONLY the new row via Raft
```

- The monitor never marks `Maintenance`, `Draining` or `Quarantined` nodes `Down`, so a planned reboot or a quarantine is not turned into a repair storm.
- The scheduler registers the new copy under the disk id the target actually reported. A lease-less location update is a union, so it can add a row to a chunk index that already has one but never erase other replicas, add a new index or grow the file size; if any index was skipped the update returns `Error` and the scheduler retries.
- Cache replicas that were on D are not repaired; the tiering engine recreates them if reads continue.
- If D comes back, its replicas count again. Surplus copies are trimmed by the excess-replica pass, which never goes below the mandatory count plus the cache allowance and never removes the anchor.

## A disk fails

1. The disk is marked `failed` (reported status or operator action, see [Disk Management](../operations/disk-management.md)). Reads that hit the bad disk meanwhile fail over on the client.
2. Replicas on that disk stop counting as live. The repair path is the same as for a down node, but only for that disk's chunks.
3. When the repair copy is verified, the old row is removed through the one de-location path: Raft `RemoveChunkLocation` first, byte delete after. If Raft refuses (for example it would remove the last live replica), no byte is deleted.
4. Disk ids persist in `.dtfs-disk-id`, so a restarted node does not look like a new disk, and a replaced disk gets a new id.

## A scrub finds a fault

```text
  quick scrub: stat .dat, compare size with .meta           (every 6 h)
  deep scrub : re-hash .dat with BLAKE3, compare checksum   (every 7 d)
        |
        v
  .meta missing or torn?  re-hash .dat; digest matches the chunk id?
        |-- yes: rewrite .meta atomically, count as repaired, healthy
        |-- no / .dat unreadable / digest mismatch:
        v
  rename .dat to .dtfs/quarantine/<id>.dat.bad   (never remove_file)
  queue the id; the next heartbeat reports it to the manager
        |
        v
  manager drops that (node, chunk) row through Raft; the repair scheduler
  copies the chunk from a healthy replica
```

- A failed attempt to write the repaired `.meta` still counts the chunk as healthy; only a real digest mismatch or unreadable data quarantines.
- Quarantined ids that were not yet reported are queued again on data node startup, so a restart cannot lose the report. An id that has a live `.dat` again is skipped.
- Scrub I/O is throttled per chunk to `max_io_percent` of disk time.
- With `quarantine_on_fault` (default on), one fault also puts the node into `Quarantined`, which an operator clears (or `auto_unquarantine_secs` if set). With it off, faults are recorded and repaired but the node keeps serving.

## Orphan reaping (after unlink or a failed write)

Chunks no file references accumulate after an unlink, an overwrite or an aborted write. The orphan reaper runs on the manager leader every `orphan_interval_secs` (default 24 h):

1. List chunks on a data node (`ListChunks`).
2. Re-read the node's view of the state machine after listing, so a chunk registered while the listing ran is not mistaken for an orphan.
3. For chunks still unreferenced, wait out the grace period (`orphan_grace_secs`, default 24 h, minimum 6 h: a lower value fails manager startup) measured on the `.dat` mtime of that disk. An unknown mtime defers the reap.
4. De-locate through Raft first, then delete bytes. The `DeleteChunk` carries `if_mtime_le_ms` set to the mtime from the listing, and the data node refuses the delete (`was_present = false`) if the `.dat` is newer by then, so a re-upload of the same content between listing and delete survives. A chunk recorded under a different disk of the same node is never reaped by a listing of this disk.

See the [Orphan Cleanup Runbook](../operations/orphan-cleanup-runbook.md).

## Cache promotion and demotion

```text
  read on TORO --> data node counts it --> heartbeat --> manager read window
                                                         (per chunk, per DC)
  every tiering tick (leader only):
    window reads in DC >= promote_reads, no cooldown, DC has < min cache copies
        --> schedule ONE copy to a healthy node with a disk of that tier in that DC
            (anchor check: an additive copy never removes the source)
    window idle for promote_window_secs, cache tier, a LIVE survivor satisfies
    the anchor
        --> remove location via Raft, then delete bytes
```

- Promotions are planned for all chunks, then dispatched as one batch capped at 50 new tasks per tick so anchor and mandatory repair are not starved; the rest are re-planned next tick.
- `cache_replicas.per_dc` (default true) counts reads and copies per datacenter; the excess-replica pass allows one cache allowance per datacenter.
- An operator flush sets a cooldown so the next read does not instantly re-promote. An engine-driven demotion clears an expired cooldown but never a future one.
- Demotion requires a live replica that still satisfies the anchor. The anchor datacenter compares case-insensitively.

Details and a worked example are in [Tiering](../storage/tiering.md).

## Replication and migration

Repair, promotion, flush and operator migrations share the scheduler's task state machine (`Pending, Copying, Verifying, Completing, Done, Failed`). A task is marked `Done` only after `VerifyChunk` on the target, and the source is removed only after the new row is committed. See [Migration](../storage/migration.md).
