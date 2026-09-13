# Archive: contract text removed on 2026-09-13

Moved here verbatim during the token-efficiency pass so the load-bearing files stay short. Nothing below is a live rule on its own; the condensed rules in CLAUDE.md and `.claude/rules/` are authoritative.

## CLAUDE.md A.2 Read paragraph (pre-2026-09-13)

**Read:** lookup metadata (cached ENTRY_TTL) → pick the read replica (`location.rs::select_read_replica`: configured `read_location_priority` beats physical distance beats a tier tie-break on equal distance only) → stream GetChunk → optionally cache locally (P0-11) → readahead prefetches. Full U4/CO-3-10 detail (how tier + topology reach the FUSE client's `known_locations`): `docs/src/internals/invariants.md` § A.2. CR-6-4 (FUSE-6): `GetFileLocationsResponse` carries the file's `volume_id` on the wire; the FUSE client's `fetch_file_locations` keeps whatever volume_id it already believes when an older manager sends `""`, never silently downgrading to `"default"` — and `allocate_chunks` always resolves placement from the file's OWN registered volume when it already exists, ignoring a stale/wrong request value. CR-6-6 (FUSE-9): a foreground chunk-miss is single-flighted by chunk_id (content-addressed) — the first miss becomes the fetch Owner, a concurrent miss on the same chunk_id joins the Owner's result instead of issuing its own `GetChunk`.

## CLAUDE.md A.3.2 (pre-2026-09-13)

**A.3.2 Memory discipline.** No `Vec<u8>` accumulation of full chunks on the hot path, read or write — stream to/off disk, hash incrementally (Bug I, U2). Memory caps are best-effort, never reject by default (§ A.5). `[memory]` (CF-5) runs on all three roles, shared core `dtfs-common/src/memory.rs`, each with its own ladder rung (data sheds put_chunk; manager pauses scheduler ticks at Shed; fuse halves readahead at Warn, stops prefetch at Shed). `MemoryConfig::validate()` (CR-6-1/CMN-1) is called by all three `main.rs` right after config load, rejecting `warn_threshold_pct >= shed_threshold_pct` or either `> 100` at startup instead of flapping the ladder at runtime; the manager also calls the new `PlacementConfig::validate()` (CMN-8) rejecting a `min_free_pct` outside `0.0..=100.0`. systemd `MemoryMax` backstops all roles. FUSE detail: `.claude/rules/fuse.md`. CR-6-5 (FUSE-5): the FUSE client's `metadata_cache` is bounded (`METADATA_CACHE_MAX_ENTRIES = 4096`, FIFO eviction via a DashMap + insertion-order queue) and periodically swept for expired entries on the `metadata_ttl` cadence — before this, one full chunk-location list per path ever stat'ed was retained forever, pinning hundreds of MB during a Plex library scan on a 1 GB client. CR2-8-3/CR2-8-4 (FUSE-11/12): `local_cache.rs`'s disk cache now classifies a viewer read from cumulative per-fh consumption (`OpenFile.bytes_read_on_fh`) or active readahead rather than one 128 KB kernel call's length, and `evict_to_size` targets 90% of `max_bytes` (hysteresis) instead of a full O(N) stat sweep on almost every `put()`.

## CLAUDE.md A.3.3 (pre-2026-09-13)

**A.3.3 Integrity is the prime directive.** Every path that touches bytes must: (1) carry a BLAKE3 checksum end-to-end into `ChunkMeta.checksum`; (2) write new chunks to tmp + atomic rename after `sync_all()`; (3) WAL the op before performing it, clear only after fsync; (4) never delete a chunk until a replacement is verified or the referencing file is unlinked and no other live file references the id (Bug CI `fanout_delete_allowed`) — a scrub-detected fault quarantines by RENAMING the `.dat` to `.dtfs/quarantine/<id>.dat.bad` (CR-4-1), never `remove_file`, and only after a re-hash of the `.dat` against its own id fails to repair the `.meta` in place; (5) never silently downgrade replication factor; (6) verify on every read and on scrub (Bug BC-8) — a mismatch is `Status::data_loss`, FUSE falls over (P0-2), disabling `verify_on_read` needs an explicit API-boundary ack; (7) LIS-19/55/83 — when `cluster.shared_secret` is set, every mutating data-plane RPC requires an HMAC-SHA256 `x-dtfs-rpc-ts`+`x-dtfs-rpc-mac` pair (empty secret = legacy skip), including `RecordCacheHit` (DATA-11 — previously ungated, letting any caller inflate a chunk's tiering promotion score); (8) LIS-56 — mutating ManagerService RPCs require the same HMAC shape over `dtfs-mgr-rpc`, including CR-5-1's `Heartbeat` and `ReportChunkStatus` (previously ungated: any node id could poison node status or drop replica entries without the cluster secret) — data nodes stamp both from `[cluster].shared_secret`; deploy manager+data together; REST mutations stay on LIS-18. Full checklist + HMAC shape: § A.6 / `.claude/rules/integrity.md`.

## CLAUDE.md A.3.4 (pre-2026-09-13)

**A.3.4 Mandatory vs cache replicas (Bug AV invariant).** Two independent classes, never conflated: `mandatory_replicas.<tier>` (protected, only the under-replication scheduler maintains them) and `cache_replicas.<tier>.{min,max}` (demand-driven, evictable, only the tiering engine creates them). Every promote/demote checks the anchor against the destination first (CO-1-2). Replica presence/removal is keyed by `(node_id, disk_id)`, never `node_id` alone (Bug BH, CO-1-1). CR-3-1 (MGR-2) — every planner that decides whether a "surviving" mandatory/anchor replica justifies dropping another one (`plan_chunk_demotions`'s `mandatory_still_present`, `plan_flush_cache`'s `mandatory_survives`) must resolve that survivor's liveness through `live_replica_tier` first; a replica on a Down/Failed node or Failed/Removed disk can never be the thing that authorises the drop, and the state-machine's `apply_remove_chunk_location` (CR-3-2) independently refuses to remove the last live replica of a chunk regardless of anchor policy. CR-3-7 (CMN-5) — an anchor `datacenter` string and a node's registered `datacenter` are compared with `eq_ignore_ascii_case` (`AnchorPolicy::is_satisfied_by`, the two `hash_ring.rs` anchor-DC compares), matching `SpillPolicy`/`read_location_priority`: a casing mismatch must never make the anchor permanently unsatisfiable. New code that creates/removes replicas: consult `.claude/rules/integrity.md` / `docs/src/internals/invariants.md` § A.3.4.

## CLAUDE.md A.5 tail (pre-2026-09-13)

CR2-8-1 (CMN-3): `[grpc].max_message_size_mb` (manager/data) and the mirrored `[client].max_message_size_mb` (fuse) now actually govern the tonic `max_decoding/encoding_message_size` caps via the shared `dtfs_common::config::max_message_bytes` helper — previously a hardcoded 32 MB literal ignored the parsed config value — and each role's `main.rs` calls the matching `validate()` (reject `0`) alongside `memory.validate()`.

## CLAUDE.md A.6 (pre-2026-09-13)

## A.6 Data-integrity audit checklist

Run mentally when you touch a write/migrate/delete/scrub path; name the covering test in the commit body only where it isn't obvious. Full checklist with narrative context lives in `.claude/rules/integrity.md`, loaded automatically for `dtfs-data/**`, `dtfs-manager/src/scheduler.rs`, `dtfs-raft/**`. Summary: (1) BLAKE3 checksum end-to-end into `ChunkMeta.checksum`; (2-3) tmp+atomic-rename+`sync_all()` for both `.dat` and `.meta` — `.meta` is written via a `write_meta_atomic` tmp+rename helper (CR-4-2), never a direct create at the final path, so a crash never leaves a torn sidecar; (4) counters update only after meta is durable, and only for a genuinely new chunk (a re-upload of an already-complete chunk is not double-counted, CR-4-6); (5) tmp removed on any put error path, and any failure AFTER the `.dat` rename also removes the `.dat`/`.meta`/WAL entry it just committed (CR-4-3) — a 256-way striped per-chunk lock plus op-keyed WAL names (`.put.wal.json`/`.delete.wal.json`, legacy `.wal.json` still replayed) keep a same-id put and delete from interleaving into a torn state (CR-4-4); (6) migration verifies before marking Done, de-location is Raft-remove-then-byte-delete (the CO-3-2 reaper mops a failed byte-delete); (7) scrub BLAKE3-checks vs `checksum`, auto-repairs via `replicate_chunk` — but first tries to repair a missing/unparseable `.meta` by re-hashing the `.dat` against its own id (CR-4-1); only a genuine mismatch or unreadable `.dat` quarantines; (8) unlink deletes metadata before bytes, orphan reap keys off durable on-disk mtime (Bug CB), not a manager-local clock — CR2-7-2 (MGR-10): when the data node's `ListChunks` reply carries `chunk_disk_ids`, the reap is keyed by `(node_id, disk_id)` instead of node-wide, so a de-located leftover on one disk of a node is reaped correctly even when another disk of the SAME node still holds a live replica of the same chunk; an older data node reporting no disk ids falls back to the prior node-wide behaviour; (9) rename is one Raft command, chunk_id binding unchanged; (10) reads use replica-fallback (P0-2).

## CLAUDE.md A.8 table (pre-2026-09-13)

| File | Responsibility |
|---|---|
| `dtfs-raft/src/state_machine.rs` | `apply_request`, the authoritative state-mutation point. CR-2-1..2-6 (RAFT-1/2/3/4/8/9): `RegisterNode` carries forward sticky Quarantined/Maintenance/Draining state instead of resetting to Healthy, `RenameFile` re-keys every child under a renamed directory (not just the exact key), `UpdateChunkLocations` grows `logical_size` to cover chunks committed past a truncate, and the seven `now_ms`-carrying variants apply deterministically — see `docs/src/internals/invariants.md` § A.8. CR2-7-6 (RAFT-6): `open_at`'s BP-2 corrupt-snapshot discard (blank SM) is checked at manager startup against the log store's `last_purged` — a blank SM with an already-purged log refuses to start as a voter (`log_store::blank_sm_startup_allowed`, called from `main.rs`) since the discarded snapshot was the only surviving copy of the pre-purge history |
| `dtfs-raft/src/lib.rs::ClientRequest` | Command enum; new mutation = new variant + arm |
| `dtfs-raft/src/network.rs::full_snapshot` | Snapshot-stream RPC; unreachable-vs-storage error mapping (Bug CL), bounded in-flight sends (CNF-4) |
| `dtfs-raft/src/log_store.rs` | RocksDB-backed Raft log/vote/last-purged store. CR2-7-5 (RAFT-5): `append`/`save_vote`/`persist_last_purged` use a synced `WriteOptions` (`set_sync(true)`) instead of `put_cf` + `flush_cf` (which only flushed the memtable, no fsync, and created one L0 SST per commit); `truncate_after` is one `delete_range_cf` instead of a delete-key-by-key loop (atomic, no torn-log hole on a crash mid-loop); `purge` commits its range delete and the `last_purged` meta write as one synced `WriteBatch` so both are durable together or neither is |
| `dtfs-manager/src/scheduler.rs` | Migration/re-replication loop, SOLE owner of replica repair; leader-gated, bounded, Raft-remove-before-byte-delete. CR-3-1..3-6 (MGR-1/2/4/5/6/7/8/9): `plan_flush_cache`/`plan_chunk_demotions` (tiering.rs) resolve survivor liveness via `live_replica_tier`; `register_replica` (`MigrationOps`) returns `Result` so a failed Completing registration retries instead of advancing to Done; `register_replica_location` collapsed to one SM lock via the pure `plan_replica_registration`, registering every carrying chunk_index in every referencing file; `resolve_disk_id_for_tier` returns `Option<u32>`, filtering Failed/Removed disks; `check_under_replicated`/`schedule_replication` dedup via `task_blocks_requeue`, source/target picks via the pure `pick_live_source`/`pick_backfill_target` (the latter requiring `placement_cfg.has_room`); `check_anchor_lag` now honours the REV-3-4 task budget and updates its dedup set inline; the heartbeat integrity-fault forwards (metadata.rs) surface `accepted: false` on either write's failure — see `docs/src/internals/invariants.md` § A.8. CR2-7-3 (MGR-13): `orphan_scrub_tick` re-checks `is_leader()` + `orphan_sweep_allowed` at the top of every per-node iteration AND immediately before every delete batch (not just once before the loop), auditing `orphan.sweep_aborted_not_leader` and stopping the sweep on either failing. CR2-7-2 (MGR-10): see § A.6 item 8. |
| `dtfs-manager/src/tiering.rs` | AV cache-tick planner + legacy temperature migration. CR2-7-1 (MGR-3): `build_av_cache_snapshot` collects the SM-derived per-chunk inputs under `state_data.read()`, drops that lock, then takes `read_windows.write()` ONCE for the whole batch via the pure `window_counts_for_chunk` — pre-fix it held the SM read lock across a per-chunk × per-DC `read_windows.write()` await, and a fair `tokio::sync::RwLock` queued every gRPC/REST reader behind one snapshot. |
| `dtfs-manager/src/monitor.rs` | Heartbeat health detection only (Bug CH); MUST NOT schedule repair; leader-gated (Bug CM) |
| `dtfs-manager/src/metadata.rs` | ManagerService gRPC over `raft_write`; follower writes forward with HMAC + a `forwardable()` variant allow-list (MGR-11); `write_via_leader` checks `ForwardWriteResponse.success` (MGR-12); LIS-56 auth incl. Heartbeat/ReportChunkStatus. CR-6-2 (CMN-2): `clamp_ack_floor_to_rf` clamps `write_safety.min_replicas_before_ack` down to the effective mandatory RF of the tier chunks actually landed on, warning once per volume + auditing `write_safety.floor_clamped`, so a floor above a tier's own RF (e.g. default 2 above cold's default RF 1) no longer rejects every commit to that tier forever. CR-6-4 (FUSE-6): `get_file_locations` fills `GetFileLocationsResponse.volume_id`; `allocate_chunks` resolves placement via `resolve_placement_volume_id` — the file's OWN registered volume when it exists, never a stale/wrong request value. |
| `dtfs-manager/src/api.rs` | REST endpoints; LIS-18 auth; effective-config merge; decommission refuses while replicas remain |
| `dtfs-manager/src/rest_auth.rs` | LIS-18 mutation-auth check + axum middleware |
| `dtfs-manager/src/membership.rs` | CF-2 online raft membership validation |
| `dtfs-manager/src/join.rs` | Two-phase data-node join (HMAC handshake); node_id derived server-side |
| `dtfs-manager/src/webui.rs` | Embedded SPA; LIS-35 mutating POSTs carry the shared secret. UIP (2026-09): covers every `api.rs` mutation the CLI has — volume create/edit (mandatory, anchor, spill, cache, write-safety, migrate-policy), scrub-all, raft membership (`/ui/raft`), maintenance, unquarantine, decommission — all through `mutateFetch`; a webui test forbids any inline `method: 'POST'` |
| `dtfs-data/src/server.rs::put_chunk` | Streaming put path; LIS-19/55/83 auth gates all data-plane RPCs |
| `dtfs-data/src/replication.rs::replicate_to` | Push-replication engine; timeout sized from chunk bytes |
| `dtfs-common/src/replication.rs` | Shared manager/data-node replication-deadline helpers |
| `dtfs-common/src/crypto.rs` | Join HMAC + data-rpc/mgr-rpc stamp/verify |
| `dtfs-data/src/store.rs::commit_chunk_from_tmp` | Atomic commit (§ A.6) |
| `dtfs-common/src/memory.rs` | CF-5 role-agnostic memory budget core |
| `dtfs-data/src/memory.rs` | Data-node rung; `[cache]` is parsed but inert (REV-4-5) |
| `dtfs-fuse/src/filesystem.rs` | FUSE↔cluster bridge; await-guard + write-batching rules in `.claude/rules/fuse.md`. CR-6-5 (FUSE-5): bounded, periodically-swept `metadata_cache`. CR-6-6 (FUSE-8/9/10): per-(fh,chunk)-transition `record_cache_hit` notify (semaphore-bounded), single-flighted foreground chunk-miss fetches (`foreground_inflight`), `on_chunk_read` called on every transition (hit or miss) — full detail in `.claude/rules/fuse.md`. |
| `dtfs-fuse/src/readahead.rs` | `ReadAheadEngine`; prefetch bound + Shed-rung, `.claude/rules/fuse.md` |
| `dtfs-fuse/src/fuse_adapter.rs` | fuse3 trait impl; every op error routes through `cluster_err_to_errno` |
| `dtfs-common/build.rs` + `src/version.rs` | `VERSION`/`BUILD_DATE` stamps (§ A.3.5) |
| `dtfs-cli/src/main.rs` | REST client over `api.rs` (clap). UIP (2026-09): feature-complete against the webui — `volumes {create,show,migrate-policy,set-cache,set-mandatory,set-write-safety,set-anchor,set-spill,set-schedules}`, `files {list,inspect,flush-cache}`, `migrations create`, `unquarantine`, `decommission`, `config {set,effective}`, `integrity faults`, `stats`, `scrub`, `raft {members,add-learner,promote,remove}`. Mutations via `post_with_status` print the server error body and exit non-zero. Full list: `docs/src/api-reference/cli-reference.md` |
| `dtfs-cli/src/updater.rs` | `dtfs-cli update` — download, verify, atomic install, restart |
| `scripts/dtfs-release.sh` | Release builder; stamps version, assembles tarball, prints `gh release create` |
| `dtfs-common/src/tls.rs` | CF-6 `TlsContext`; all gRPC routes through it; enable only cluster-wide |
| `plans/active/` | Optional short plans for multi-session work (`plans/README.md`) |
| `tests/release/`, `tests/proxmox/` | Cluster-only harnesses, dormant without a lab |

## .claude/rules/integrity.md (pre-2026-09-13, full narrative)

---
paths:
  - "crates/dtfs-data/**"
  - "crates/dtfs-manager/src/scheduler.rs"
  - "crates/dtfs-raft/**"
---

# Data-integrity audit checklist (CLAUDE.md § A.6)

Run this whenever you touch a write/migrate/delete/scrub path; mark each
"Verified by `<test>`" in the commit. Full narrative context (why each
item exists, the bugs that motivated it) is inline below — this is the
long form of CLAUDE.md A.3.3/A.6, loaded only for the crates it governs.

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
   the CO-1-3 Raft-remove-first ordering (item 6). **CR2-7-2 (MGR-10)** —
   when the data node's `ListChunks` reply carries `chunk_disk_ids`, the
   de-located check and the reap delete are keyed by `(node_id, disk_id)`
   instead of node-wide, so a leftover on ONE disk of a node is reaped
   correctly even when another disk of the SAME node still holds a live
   replica of the same chunk (the cold-anchor + warm-cache co-location);
   an older data node reporting no disk ids falls back to the prior
   node-wide behaviour exactly.
9. Rename is a single Raft command; open handles keep reading the same
   chunks (path updated, chunk_id binding unchanged).
10. Reads use replica-fallback (P0-2): try the next replica on error, fail
    only after all candidates fail.

## Data-plane RPC auth (LIS-19/55/83, CLAUDE.md § A.3.3 #7)

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

## Manager-plane RPC auth (LIS-56, CLAUDE.md § A.3.3 #8)

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

## Mandatory vs cache replicas (Bug AV invariant, CLAUDE.md § A.3.4)

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

## .claude/rules/fuse.md (pre-2026-09-13, full narrative)

---
paths:
  - "crates/dtfs-fuse/**"
---

# dtfs-fuse memory ladder + await-guard rules (CLAUDE.md § A.3.2, § A.8)

## Never hold a DashMap guard across an `.await`

`filesystem.rs::read()` clones the per-call data (path/chunk_size/
volume_id/locations) out of the `open_files` entry and drops the DashMap
guard BEFORE the per-chunk network loop (same idiom as `write()`/
`flush()`). Holding an `open_files` guard across an `.await` serializes
concurrent opens of the same file. The same rule covers
`known_locations`: the post-fetch migration-hint lookup
(`known_locations.get(&node_addr)`) uses `.map(|r| r.clone())` so the
`Ref` guard drops at the end of that statement, before the migration
trigger runs, never across an `.await`/spawn.

`migration` (`MigrationTrigger`) is fire-and-forget: `read()` calls
`spawn_on_remote_read` (`tokio::spawn`, never `.await`s `on_remote_read`
inline) because the RPC it issues (`DtfsClient::allocate_chunks`) sets no
per-call deadline and retries `Unavailable` for up to the 15s manager
walk deadline — awaiting it inline after the chunk bytes are already in
hand could stall a completed read for that long.

## Write batching (WI-01)

The write buffer (`writebuf.rs`) holds back complete chunks until
`write.flush_batch_chunks` (default 4, clamp `1..=32`) are available
before `flush()` fires, so `flush_segments_to_cluster` amortizes one
`AllocateChunks` + `CommitChunkLocations` round trip over a batch instead
of paying it per chunk. `flush_all` (fsync/close) always drains held-back
chunks regardless of batch state. The effective batch is additionally
byte-clamped (`writebuf::effective_batch_chunks`,
`FLUSH_BATCH_MAX_BYTES` = 64 MB) so batching never holds more than 64 MB
of un-committed data per open file — the memory ladder below sheds
readahead, not write buffers.

R1 (FUSE-1 memory half) — `writebuf::DIRTY_BYTES_CAP` (2x
`FLUSH_BATCH_MAX_BYTES`) governs total dirty bytes per open file: the
buffer is force-drained via `WriteBuffer::flush_complete()` whenever a
sparse-hole zero-fill or an incoming write would meet or exceed the cap,
so a write far past EOF (e.g. a torrent client's first piece landing at
offset 100 MB) can no longer materialise the whole hole into heap before
committing anything.

## Foreground-read prefetch join (WI-05 / B2)

`read()`'s foreground miss tries `readahead.join_pending()` against ONLY
the FIRST candidate (prefetch targets `read_candidates`' head), bounded
by the fixed `readahead::PREFETCH_JOIN_TIMEOUT` (2s, deliberately NOT
`client.chunk_read_timeout_secs`), and refuses to wait on a prefetch that
has not yet acquired its `prefetch_slots` permit (still queued), instead
returning `None` immediately. A chunk genuinely on the wire is JOINED
rather than re-fetched with a second `GetChunk` over the same link;
`None` falls through to the unchanged candidate loop, preserving the
P0-2 fallover budget (`tests/release/cases/69-read-hung-node-fallover.sh`,
`ELAPSED <= TMO*3`).

## Prefetch fan-out bound (WI-05)

`readahead.rs::ReadAheadEngine` bounds prefetch fan-out with a
`prefetch_slots` semaphore sized from `readahead.prefetch_max_inflight`
(default 2) — the prefetch WINDOW stays `prefetch_chunks` deep (default
8), but fetches acquire their slot chained in ascending order
(`spawn_prefetch`'s `prev_acquired` oneshot) so at most
`prefetch_max_inflight` `GetChunk` streams are ever on the wire at once,
nearest chunk first. Before this an uncapped fan-out shared one pooled
HTTP/2 channel per node N ways and starved the nearest chunk instead of
adding bounded heap. Do NOT re-add the old `cache_bytes() >=
max_cache_bytes` dormancy gate — it only ever saw stored bytes, never
in-flight ones, and could go permanently true under the default sizing,
silently killing all further prefetch.

## Memory ladder (CF-5, shared core in `dtfs-common/src/memory.rs`)

FUSE halves the readahead `cache_mb` at Warn, drops unconsumed prefetch
AND stops spawning new prefetch at Shed, via
`ReadAheadEngine.prefetch_enabled: Arc<AtomicBool>` — `on_chunk_read`
early-returns while it is false, so `drop_unconsumed()` is not undone by
the very next read; `ReadAheadCacheController::set_prefetch_enabled()` is
pinned false at Shed and true at Normal/Warn by
`main.rs::run_readahead_pressure_ladder`. `pending` holds
`PendingPrefetch { handle, done, started }`, where `started` is set true
the instant a prefetch task acquires its `prefetch_slots` permit,
letting `join_pending()` above distinguish a fetch genuinely in flight
from one still queued. systemd `MemoryMax` remains the hard backstop.

## Per-chunk cache-hit notify, single-flight misses, continuous prefetch (CR-6-6)

`read()` tracks `OpenFile.last_read_chunk: Option<u32>` per fh; the pure
`should_notify(last, current)` decides whether the CURRENT chunk index is
a transition from the last one this fh touched. Three things gate on the
same transition signal (computed once, right after building read
candidates, before any cache lookup):

- **FUSE-8** — `record_cache_hit` fires at most once per (fh, chunk)
  transition, not once per 128 KB kernel read (a 4 MB chunk read in
  128 KB pieces used to fire ~32 of these RPCs). In-flight notify tasks
  are additionally bounded by a `Semaphore(CACHE_HIT_NOTIFY_MAX_INFLIGHT
  = 8)`; a permit-less notify is simply skipped (a missed metrics ping
  is acceptable, unlike a stalled read).
- **FUSE-10** — `readahead.on_chunk_read` fires on every transition, hit
  or miss (previously only on a miss), so the prefetch window slides
  continuously instead of stalling for one synchronous foreground fetch
  every `prefetch_chunks+1` chunks.
- **FUSE-9** — a foreground cache MISS is single-flighted by chunk_id
  (content-addressed, so any candidate node is interchangeable) via
  `claim_foreground_fetch`/`complete_foreground_fetch` over a
  `foreground_inflight: DashMap<String, watch::Receiver<Option<Bytes>>>`.
  The first miss becomes the `Owner` (does the real `GetChunk` via the
  extracted `fetch_chunk_from_candidates`, then publishes the result and
  clears the entry); a concurrent miss on the SAME chunk_id becomes a
  `Waiter` that awaits the Owner's `watch::Receiver` instead of issuing
  its own fetch. A `Waiter` whose Owner's fetch failed (published `None`)
  falls back to fetching it itself rather than failing the read. This
  keeps N concurrent kernel reads into one cold chunk down to exactly one
  `GetChunk` over the link instead of N.

New WI-05/B2/H2 test coverage is unaffected: the join-pending path (a
chunk already ON THE WIRE as a *prefetch*) is untouched and still tried
before the miss branch; `foreground_inflight` only covers the foreground
fetch loop itself.

## Disk-cache viewer-read hint + eviction hysteresis (CR2-8-3/CR2-8-4)

`local_cache.rs`'s viewer/scanner atime hint can no longer be driven by
one kernel read's length (FUSE-11): FUSE reads arrive in ~128 KB pieces,
always under `ATIME_UPDATE_MIN_BYTES` (1 MiB), so `read()` now tracks
`OpenFile.bytes_read_on_fh` and combines it with
`ReadAheadEngine::is_active()` via the pure `filesystem::is_viewer_read`
helper, passed into the new `LocalChunkCache::get_with_hint(id,
is_viewer)` (the old `get(id, bytes_hint)` delegates to it). FUSE-12:
`evict_to_size` targets `EVICT_TARGET_PCT` (90%) of `max_bytes` instead
of exactly `max_bytes`, so a cache at steady state pays a full O(N) stat
sweep roughly once per 10% of the cap's worth of new data instead of on
almost every single `put()`; the dead `lru: BTreeMap` index (inserted,
never read) is gone.

## FUSE error mapping (CO-4-16)

`fuse_adapter.rs` metadata ops map cluster errors via
`cluster_err_to_errno` — ENOENT ONLY for a genuine gRPC `NotFound`, EIO
for transient/unknown (manager outage, election, timeout). A blip mapped
to ENOENT makes Plex drop the library entry; route every new
error-returning op through the helper, never a bare `Err(_) => ENOENT`.
