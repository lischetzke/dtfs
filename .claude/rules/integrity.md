---
paths:
  - "crates/dtfs-data/**"
  - "crates/dtfs-manager/src/scheduler.rs"
  - "crates/dtfs-manager/src/scheduler/**"
  - "crates/dtfs-raft/**"
---

# Data-integrity checklist (CLAUDE.md § A.3.3 / § A.6)

Check every item that applies to the path you touch. Narrative and bug
history: `docs/src/internals/invariants.md` § A.6 and the archived rule
text there.

## Bytes on disk

1. BLAKE3 via `IncrementalHasher`; final hash lands in `ChunkMeta.checksum`.
2. `.dat`: write `tmp/<uuid>.dat.tmp`, `sync_all()`, atomic rename to
   `chunks/<aa>/<bb>/<id>.dat`.
3. `.meta`: only via `write_meta_atomic` (tmp, `sync_all`, rename, dir
   fsync). Never `File::create` at the final path. An unparseable `.meta`
   is treated as missing. The put commit alone uses
   `write_meta_atomic_no_dir_sync` and fsyncs the chunk dir ONCE after
   both the `.dat` and `.meta` renames, before the WAL delete (5 syncs per
   new chunk; the WAL delete itself is not fsynced).
4. Counters (`bytes_stored`, `chunk_count`) update only after `.meta` is
   durable, and only for a genuinely new chunk.
5. Any put error removes the tmp; an error AFTER the `.dat` rename removes
   `.dat`, `.meta` and the WAL entry (`cleanup_after_dat_committed`),
   except a `.dat` that existed before the rename (only the WAL goes).
   Put-WAL replay rebuilds a missing/torn `.meta` from a `.dat` that hashes
   to its id and rolls back only a mismatch, removes a `.meta` whose
   `.dat` is missing, and is a no-op for a complete chunk. Commit stamps the `.dat` mtime
   to commit time (the `if_mtime_le_ms` delete guard reads it).
   Put and delete of the same id serialize on the 256-way striped chunk
   lock; WAL names are op-keyed (`.put.wal.json` / `.delete.wal.json`).
   The PutChunk commit and the DeleteChunk unlink run in a spawned task
   (`server::detached`): a cancelled RPC cannot stop them midway. A panic
   in the put commit task removes only the tmp (`detached_commit`).

## Replica lifecycle

6. Migration marks Done only after `VerifyChunk` on the target (disk-targeted
   via the landed disk id). `RemoveCorruptChunkReplicas` never removes the last
   row of a (file, chunk_index). Every
   de-location goes through `scheduler::delocate_replica`: Raft
   `RemoveChunkLocation` FIRST, byte delete AFTER; a failed OR
   state-machine-refused Raft remove (`raft_write_checked` /
   `client_response_ok`, the one Raft-response checker) issues no byte
   delete; a failed byte delete is never rolled back (the orphan reaper
   reclaims it). After the remove it re-reads the state and skips the byte
   delete while any file still has a row with that hash on the same
   (node, disk) (content-addressed sharing), and the delete carries
   `if_mtime_le_ms` taken before that re-read (a concurrent same-hash put
   stamps a newer mtime at commit; known limit: manager vs data-node clock
   skew). Registration of a finished copy uses the disk id the
   target reported (`ReplicateChunkResponse.disk_id`) and sends only the
   new row (apply is a union for lease-less writes). Data disk ids persist
   in `.dtfs-disk-id`.
7. Scrub hashes `.dat` vs `checksum`; on a `.meta` mismatch it re-hashes vs the
   chunk id and repairs `.meta` (only an id mismatch is corruption;
   `verify_on_read` also compares to the id and rebuilds a missing or
   unreadable `.meta` from a `.dat` that hashes to its id, no fault). Quarantine runs per bucket under
   the chunk lock after a fresh re-hash (`confirm_and_quarantine`). Missing/unparseable `.meta` is first
   repaired from the `.dat` when the re-hash matches the id; only a
   digest mismatch or unreadable `.dat` quarantines, by RENAMING to
   `.dtfs/quarantine/<id>.dat.bad`, never `remove_file`.
8. Unlink removes `FileMetadata` first and does NOT fan out deletes; the
   orphan reaper reclaims bytes, passing the listed `.dat` mtime as
   `DeleteChunkRequest.if_mtime_le_ms` (data node skips a newer, re-uploaded copy). The reaper re-reads the node view after
   `ListChunks` and never de-locates a chunk recorded under another disk
   of the same node. Orphan/de-located reap grace is measured on the on-disk `.dat` mtime; the unreferenced verdict is
   node-wide (no row names the node on any disk), the delete carries the
   reported disk id; unknown mtime defers the reap. Grace floors:
   config `orphan_grace_secs` >= 6 h, one-shot manual reap
   (`POST /api/v1/scrub/orphans`) >= `MANUAL_ORPHAN_GRACE_FLOOR_SECS`
   (1 h); timer and manual share one single-flight token, and a dry run
   issues no delete and no Raft write.
9. Rename is one Raft command; chunk_id bindings unchanged.
10. Reads fall back replica by replica; fail only after all candidates.

## Replica classes (Bug AV)

- `mandatory_replicas.<tier>`: only `check_under_replicated` maintains
  them, counting only LIVE replicas (`live_replica_tier`).
- `cache_replicas.<tier>.{min,max}`: only the tiering engine creates them;
  the under-replication scheduler never backfills them.
- Write ack floor counts only distinct (node_id, disk_id) replicas on RF>0
  tiers; allocation fills it across maintained tiers; the only lowering is
  the capacity clamp (healthy maintained nodes, same eligibility rule as the
  fill, never 0, warn + audit `write_safety.floor_clamped`).
- A survivor that authorises dropping another replica must pass
  `survivor_replica_tier` (live, node not Maintenance/Quarantined, no
  quarantine record); `apply_remove_chunk_location` refuses to remove the last live replica.
- Every promote/demote/flush checks the anchor against the destination
  (`would_violate_anchor_for`, `demote_preserves_anchor`, `enforce_anchor` on
  apply). Anchor DC compares case-insensitively.
- Anchor-pending (FUSE write.anchor_sync=false, writer outside the anchor
  DC): acked on the unchanged ack floor without an anchor copy;
  check_anchor_lag repairs it. Until the anchor copy is live, every removal
  path refuses (goal_met / demote_preserves_anchor / enforce_anchor),
  including dead or stale rows.
- Presence and removal keyed by `(node_id, disk_id)`; removals carry the
  disk id. The orphan reaper is disk-keyed: it never removes a row
  recorded under a different disk of the same node.

## RPC auth

- Data plane (`shared_secret` set): every RPC incl. `RecordCacheHit`
  needs `x-dtfs-rpc-ts` + `x-dtfs-rpc-mac` = HMAC-SHA256 over
  `dtfs-data-rpc\nv1\n{method}\n{ts}\n{binding}\n` (bindings:
  chunk_id; ReplicateChunk `chunk_id|target_address`; DeleteChunk
  `chunk_id|disk_id|if_mtime_le_ms` via `data_rpc_delete_binding`, legacy
  chunk_id-only only while data-node `require_delete_binding=false`;
  PutChunk/ListChunks empty). Empty secret = legacy skip.
- Manager plane: mutating ManagerService RPCs incl. `Heartbeat` and
  `ReportChunkStatus` need the same shape over `dtfs-mgr-rpc`.
- Follower forwards: `forward_secret_ok` checks who, `forwardable()`
  allow-lists what; `write_via_leader` must read
  `ForwardWriteResponse.success`.

New code that creates or removes replicas names the class explicitly.
