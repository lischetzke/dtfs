---
paths:
  - "crates/dtfs-data/**"
  - "crates/dtfs-manager/src/scheduler.rs"
  - "crates/dtfs-raft/**"
---

# Data-integrity checklist (AGENTS.md § A.3.3 / § A.6)

Check every item that applies to the path you touch. Narrative and bug
history: `docs/src/internals/invariants.md` § A.6 and the archived rule
text there.

## Bytes on disk

1. BLAKE3 via `IncrementalHasher`; final hash lands in `ChunkMeta.checksum`.
2. `.dat`: write `tmp/<uuid>.dat.tmp`, `sync_all()`, atomic rename to
   `chunks/<aa>/<bb>/<id>.dat`.
3. `.meta`: only via `write_meta_atomic` (tmp, `sync_all`, rename, dir
   fsync). Never `File::create` at the final path. An unparseable `.meta`
   is treated as missing.
4. Counters (`bytes_stored`, `chunk_count`) update only after `.meta` is
   durable, and only for a genuinely new chunk.
5. Any put error removes the tmp; an error AFTER the `.dat` rename removes
   `.dat`, `.meta` and the WAL entry (`cleanup_after_dat_committed`).
   Put and delete of the same id serialize on the 256-way striped chunk
   lock; WAL names are op-keyed (`.put.wal.json` / `.delete.wal.json`).

## Replica lifecycle

6. Migration marks Done only after `VerifyChunk` on the target. Every
   de-location goes through `scheduler::delocate_replica`: Raft
   `RemoveChunkLocation` FIRST, byte delete AFTER; a failed Raft remove
   issues no byte delete; a failed byte delete is never rolled back (the
   orphan reaper reclaims it).
7. Scrub hashes `.dat` vs `checksum`. Missing/unparseable `.meta` is first
   repaired from the `.dat` when the re-hash matches the id; only a
   digest mismatch or unreadable `.dat` quarantines, by RENAMING to
   `.dtfs/quarantine/<id>.dat.bad`, never `remove_file`.
8. Unlink removes `FileMetadata` first, bytes async. Orphan/de-located
   reap grace is measured on the on-disk `.dat` mtime, keyed by
   `(node_id, disk_id)` when the data node reports disk ids (node-wide
   fallback otherwise); unknown mtime defers the reap.
9. Rename is one Raft command; chunk_id bindings unchanged.
10. Reads fall back replica by replica; fail only after all candidates.

## Replica classes (Bug AV)

- `mandatory_replicas.<tier>`: only `check_under_replicated` maintains
  them, counting only LIVE replicas (`live_replica_tier`).
- `cache_replicas.<tier>.{min,max}`: only the tiering engine creates them;
  the under-replication scheduler never backfills them.
- A survivor that authorises dropping another replica must be live;
  `apply_remove_chunk_location` refuses to remove the last live replica.
- Every promote/demote/flush checks the anchor against the destination
  (`would_violate_anchor`, `demote_preserves_anchor`, `enforce_anchor` on
  apply). Anchor DC compares case-insensitively.
- Presence and removal keyed by `(node_id, disk_id)`; removals carry the
  disk id. Whole-chunk reclaimers (unlink fan-out, orphan reaper) stay
  node-wide on purpose.

## RPC auth

- Data plane (`shared_secret` set): every RPC incl. `RecordCacheHit`
  needs `x-dtfs-rpc-ts` + `x-dtfs-rpc-mac` = HMAC-SHA256 over
  `dtfs-data-rpc\nv1\n{method}\n{ts}\n{binding}\n` (bindings:
  chunk_id; ReplicateChunk `chunk_id|target_address`; PutChunk/ListChunks
  empty). Empty secret = legacy skip.
- Manager plane: mutating ManagerService RPCs incl. `Heartbeat` and
  `ReportChunkStatus` need the same shape over `dtfs-mgr-rpc`.
- Follower forwards: `forward_secret_ok` checks who, `forwardable()`
  allow-lists what; `write_via_leader` must read
  `ForwardWriteResponse.success`.

New code that creates or removes replicas names the class explicitly.
