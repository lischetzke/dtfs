---
paths:
  - "crates/dtfs-data/**"
  - "crates/dtfs-manager/src/scheduler.rs"
  - "crates/dtfs-raft/**"
---

# Data-integrity audit checklist (AGENTS.md § A.6)

Run this whenever you touch a write/migrate/delete/scrub path; mark each
"Verified by `<test>`" in the commit. Full narrative context (why each
item exists, the bugs that motivated it) is inline below — this is the
long form of AGENTS.md A.3.3/A.6, loaded only for the crates it governs.

1. BLAKE3 via `IncrementalHasher`; final hash in `ChunkMeta.checksum`.
2. On-disk `.dat` written to `tmp/<uuid>.dat.tmp`, atomic-renamed to
   `chunks/<aa>/<bb>/<id>.dat` only after `sync_all()`.
3. `.meta` JSON written with `sync_all()` before the put's WAL entry is
   cleared.
4. Counter updates (`bytes_stored`, `chunk_count`) happen after meta is
   durable, so a crash mid-put leaves a reconcilable tmp+wal residue.
5. `commit_chunk_from_tmp` removes the tmp on any error path.
6. Migration marks a task Done only after `VerifyChunk` on the target
   succeeds; the scheduler never removes the source (only tier-demote /
   unlink delete a chunk). **CO-1-3** — every de-location-with-byte-delete
   path (tiering demote dispatch, scheduler cache-flush dispatch) issues
   the Raft `RemoveChunkLocation` (with `enforce_anchor`) FIRST and the
   data-plane `DeleteChunkReplica` byte delete AFTER, via the shared
   `scheduler::delocate_replica` (over the `DelocateOps` seam). On a
   Raft-remove failure NO byte delete is issued (the replica stays live in
   the SM + on disk; retry next tick). A byte-delete RPC that fails after
   a committed remove is NEVER rolled back — the leftover on-disk bytes
   are now a chunk de-located on that node, which the **CO-3-2** reaper
   reclaims (item 8). This makes de-location atomic at the metadata layer:
   no phantom replica (bytes gone but still advertised live) and no
   stranded anchor can result from a crash/RPC failure in the window.
7. Scrub BLAKE3s the disk file vs `chunk.meta.checksum`; mismatch →
   `chunks_corrupt` + `ScrubCorruption` alert; auto-repair via
   `replicate_chunk`.
8. Unlink removes manager-side `FileMetadata` first, then GC deletes
   chunks async; a crash between leaves orphans (reclaimed by scrub),
   never dangling pointers. **Bug CB** — the orphan reaper measures its
   grace against each chunk's durable on-disk `.dat` mtime
   (`scheduler::orphan_reap_due`, fed by `ListChunks.chunk_mtimes_ms`),
   NOT a manager-local first-observed clock, so reclamation survives
   manager restarts / leadership changes (pre-CB the in-memory clock
   reset every restart and deleted-file disk was never freed under
   rolling-restart churn). An unknown mtime (pre-CB data node) defers
   the reap rather than dropping a possibly in-flight chunk. **CO-3-2**
   generalised the reaper to also reclaim a chunk present on a node's disk
   for which the SM records NO location entry naming that node
   ("de-located"; `scheduler::chunk_delocated_on_node`), after the same
   grace — this is what mops the byte-delete-failed leftovers produced by
   the CO-1-3 Raft-remove-first ordering (item 6).
9. Rename is a single Raft command; open handles keep reading the same
   chunks (path updated, chunk_id binding unchanged).
10. Reads use replica-fallback (P0-2): try the next replica on error, fail
    only after all candidates fail.

## Data-plane RPC auth (LIS-19/55/83, AGENTS.md § A.3.3 #7)

When `cluster.shared_secret` is non-empty, `PutChunk` / `DeleteChunk` /
`ReplicateChunk` / `GetChunk` / `ListChunks` / `VerifyChunk` require gRPC
metadata `x-dtfs-rpc-ts` + `x-dtfs-rpc-mac` (HMAC-SHA256 over
`dtfs-data-rpc\nv1\n{method}\n{ts}\n{binding}\n`; DeleteChunk/GetChunk/
VerifyChunk bind `chunk_id`; ReplicateChunk binds
`chunk_id|target_address`; PutChunk/ListChunks empty binding). Missing or
invalid → `PermissionDenied`. Empty secret = legacy skip; data-node
startup refuses empty secret unless `allow_insecure_empty_secret`.
Callers that stamp: FUSE/client put+get, data→data replication put,
manager scheduler ReplicateChunk/VerifyChunk/ListChunks, manager unlink
fan-out + scheduler flush.

## Mandatory vs cache replicas (Bug AV invariant, AGENTS.md § A.3.4)

Two independent, never-mixed replica classes:
- `mandatory_replicas.<tier>` — protected; only
  `scheduler::check_under_replicated` maintains them, reading
  `effective_mandatory_replicas()`, counting only serviceable replicas
  (`live_replica_tier`, Bug CH) so node/disk loss surfaces as a deficit.
- `cache_replicas.<tier>.{min,max}` — demand-driven, evictable; only the
  tiering engine creates them, only on the promotion threshold. The
  under-replication scheduler MUST NOT backfill cache replicas.
- `scheduler::plan_flush_cache` refuses any flush that would strand the
  mandatory copy → `cache.flush_refused` audit + `AnchorViolated` alert.
- Every promote/demote checks the anchor against the destination state:
  legacy path via `would_violate_anchor`; AV idle-demote via
  `tiering::demote_preserves_anchor` (checks another placement still
  satisfies `effective_anchor`, records `TickPlan.anchor_protected`); the
  apply path (`RemoveChunkLocation` with `enforce_anchor`) re-checks as
  defence-in-depth. The mandatory-tier survival check ALONE is not
  enough: a chunk's only anchor-DC replica can be a cache copy while the
  mandatory copy lives elsewhere.
- Replica presence is keyed by `(node_id, disk_id)`, never `node_id`
  alone (Bug BH) — a mandatory and a cache replica of the same chunk may
  co-locate on one node on different disks/tiers.
- Removal is disk-targeted (CO-1-1): every replica-drop path carries the
  specific `disk_id` so it removes exactly the `(node_id, disk_id)` copy,
  never a co-located anchor on another disk of the same node. Whole-chunk
  reclaimers (unlink fan-out, orphan reaper) intentionally stay node-wide.

When a new code path creates OR removes replicas, name the class
explicitly and consult one of these checks.
