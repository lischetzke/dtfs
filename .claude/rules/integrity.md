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
   cleared. **CR-4-2** — the write itself is tmp+rename (`write_meta_
   atomic`: tmp file in the disk's `tmp/` dir, `sync_all`, rename over
   the final `.meta` path, fsync the directory), never a direct
   `File::create` at the final path, so a crash mid-write can never leave
   a torn (partial/0-byte) `.meta` where readers look for it.
   `recover_from_wal`'s `PutChunk` arm treats a present-but-unparseable
   `.meta` the same as a missing one (CO-1-4b) and rolls the half-
   committed chunk back.
4. Counter updates (`bytes_stored`, `chunk_count`) happen after meta is
   durable, so a crash mid-put leaves a reconcilable tmp+wal residue.
5. `commit_chunk_from_tmp` removes the tmp on any error path. **CR-4-3**
   — a failure AFTER the `.dat` rename (typically the `.meta` write) is
   cleaned up by `cleanup_after_dat_committed`, which removes the `.dat`,
   any `.meta`, and the WAL entry before returning the error, so a
   committed `.dat` never survives without its `.meta` companion.
   **CR-4-4** — `commit_chunk_from_tmp_inner` and
   `delete_chunk_idempotent_on_disk` each hold a 256-way striped
   per-chunk `tokio::sync::Mutex<()>` (indexed by the id's first byte)
   for their whole operation, so a concurrent put and delete of the SAME
   chunk id can never interleave into a torn state; the WAL filename is
   op-keyed (`<id>.put.wal.json` / `<id>.delete.wal.json`, plus the
   legacy `<id>.wal.json` still replayed) so they never contend over one
   WAL file either.
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
   `replicate_chunk`. **CR-4-1** — before recording a `meta_read` /
   `meta_parse` / missing-`.meta` fault as corrupt, the scrub re-hashes
   the already-stat'd `.dat` and, on a match against the id encoded in
   the filename, rewrites `.meta` from the verified `.dat`
   (`try_repair_meta_from_dat` + `write_meta_atomic`) and counts the
   chunk healthy (`ScrubResult.chunks_meta_repaired`) instead of
   quarantining it — only a digest mismatch or an unreadable `.dat`
   still quarantines. `quarantine_chunk_on_disk` itself now RENAMES the
   `.dat` to `.dtfs/quarantine/<id>.dat.bad` (overwriting an earlier
   quarantine of the same id) instead of `remove_file`ing it, so a
   genuinely corrupt chunk's bytes are preserved for forensics rather
   than destroyed.
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
`ReplicateChunk` / `GetChunk` / `ListChunks` / `VerifyChunk` / **`Record-
CacheHit`** (CR-5-5/DATA-11) require gRPC metadata `x-dtfs-rpc-ts` +
`x-dtfs-rpc-mac` (HMAC-SHA256 over
`dtfs-data-rpc\nv1\n{method}\n{ts}\n{binding}\n`; DeleteChunk/GetChunk/
VerifyChunk/RecordCacheHit bind `chunk_id`; ReplicateChunk binds
`chunk_id|target_address`; PutChunk/ListChunks empty binding). Missing or
invalid → `PermissionDenied`. Empty secret = legacy skip; data-node
startup refuses empty secret unless `allow_insecure_empty_secret`.
Callers that stamp: FUSE/client put+get+record_cache_hit, data→data
replication put, manager scheduler ReplicateChunk/VerifyChunk/
ListChunks, manager unlink fan-out + scheduler flush. `RecordCacheHit`
was the last mutating data-plane RPC left ungated pre-CR-5-5 — an
unauthenticated caller could inflate a chunk's read counter and its
tiering promotion score.

## Manager-plane RPC auth (LIS-56, AGENTS.md § A.3.3 #8)

When `cluster.shared_secret` is non-empty, mutating `ManagerService`
gRPC RPCs (`CreateFile`/`DeleteFile`/leases/…, and since **CR-5-1**
also `Heartbeat` and `ReportChunkStatus`) require the same header shape
over the `dtfs-mgr-rpc` plane (`metadata::require_mgr_rpc_auth`); reads
stay open. Pre-CR-5-1, Heartbeat/ReportChunkStatus were the only two
mutating RPCs not gated — any caller who knew a node id could poison
node health or drop replica entries without the secret. Data nodes
stamp both from `[cluster].shared_secret` (`dtfs-data/src/main.rs::
stamp_mgr_rpc_request`); deploy manager + data together.

A follower's `ForwardClientWrite` (used by the heartbeat/health-monitor
forward path when a write lands on a non-leader) is authenticated by
`forward_secret_ok` (CO-2-2, constant-time) but that only checks WHO is
forwarding. **CR-5-2 (MGR-11)** added a second gate on WHAT is
forwarded: `forwardable(&ClientRequest)` allow-lists exactly the
variants a follower legitimately proposes today (`UpdateNodeUsage`,
`UpdateNodeDiskUsage`, `UpdateNodeStatus`, `AppendAudit`,
`QuarantineNode`, `RemoveCorruptChunkReplicas`, `ReportScrubResult`,
`ReportChunkReads`); anything else is denied + audited
(`auth.denied`) before it ever reaches `raft.client_write`. **CR-5-3
(MGR-12)** — the caller side (`write_via_leader`) now inspects
`ForwardWriteResponse.success`/`.error` via `forward_result` instead of
discarding the response body, so a forward rejected by the leader (bad
secret, a disallowed MGR-11 variant, or a failed Raft apply) is no
longer silently treated as a successful write.

## Mandatory vs cache replicas (Bug AV invariant, AGENTS.md § A.3.4)

Two independent, never-mixed replica classes:
- `mandatory_replicas.<tier>` — protected; only
  `scheduler::check_under_replicated` maintains them, reading
  `effective_mandatory_replicas()`, counting only serviceable replicas
  (`live_replica_tier`, Bug CH) so node/disk loss surfaces as a deficit.
- `cache_replicas.<tier>.{min,max}` — demand-driven, evictable; only the
  tiering engine creates them, only on the promotion threshold. The
  under-replication scheduler MUST NOT backfill cache replicas.
- **CR-3-1 (MGR-2)** — a "surviving" mandatory/anchor replica must be
  LIVE to count: `plan_chunk_demotions`'s `mandatory_still_present` and
  `plan_flush_cache`'s `mandatory_survives` resolve every placement's
  liveness through `live_replica_tier` before treating it as the
  survivor that authorises dropping another replica. A replica on a
  Down/Failed node or Failed/Removed disk is not servable and must
  never be the reason a demote/flush is allowed to proceed.
  **CR-3-2** — independent of anchor policy, `apply_remove_chunk_location`
  also refuses to remove the last LIVE replica of a `(path,
  chunk_index)`; a dead row can always be removed, but the removal of a
  live row is refused when no other live row of that chunk survives.
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
