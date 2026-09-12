# CORE-REVIEW-2026-09-12 — core-module code review (common, proto, raft, data, manager, fuse)

Method: code review only, no lab, no build (builder unreachable). One reviewer
agent per crate, run sequentially against `master` (`cca10ac`, post PR #104),
each instructed to re-read every claimed line before reporting. Every surviving
finding below was then re-checked by hand against the source; refuted or
cosmetic items were dropped. Findings are numbered per crate so they can be
lifted 1:1 into plan tasks.

Severity: HIGH = data loss / silent corruption / invariant violation / outage /
OOM / non-terminating repair / auth bypass. MEDIUM = slow-healing wrong state,
wasted bandwidth on the 40 Mbps link, operator-misleading observability,
resource leak. LOW = worthwhile optimisation or robustness hardening.

Pre-review housekeeping (same day): merged PR #104 (PERF-SWEEP-2026-08) to
master; deleted 65 stale local branches (62 already merged via PR; 3 never had a
PR and were dropped: `claude/determined-saha-3ba85d` AZ-6 per-disk
`usable_bytes` cap — unfinished June feature, redo from scratch if wanted;
`worktree-agent-ac4b1a35a71cf296c` PLAN-CN-FOLLOWUPS plan file only;
`worktree-curious-yawning-yeti` no-op plan deletions); removed 9 agent
worktrees.

---

## 1. dtfs-common + dtfs-proto

### PROTO-1 — Heartbeat and ReportChunkStatus are unauthenticated Raft-mutating RPCs (HIGH)
- `crates/dtfs-manager/src/metadata.rs:1061` (`heartbeat`), `:1393` (`report_chunk_status`); `proto/manager.proto` `HeartbeatRequest` / `ChunkStatusReport` carry no secret field.
- Both handlers start with `request.into_inner()` and never call `require_mgr_rpc_auth` (every other mutating handler does, LIS-56). `ChunkStatusReport{status:"corrupt"}` drives `RemoveCorruptChunkReplicas`; `HeartbeatRequest.integrity_faults` drives quarantine; `disk_usage`/`chunk_reads` steer placement and tiering. `node_id` is `hash(hostname)`, public.
- Impact: anyone reaching 9401 can de-locate every anchor replica in the SM (A.1 anchor invariant), quarantine any node, or forge load. Contradicts AGENTS.md A.3.3 #8.
- Fix: gate both with `require_mgr_rpc_auth(secret, "Heartbeat"|"ReportChunkStatus", node_id, metadata)`; stamp from the dtfs-data heartbeat loop with `stamp_mgr_rpc_auth`; deploy manager+data together; list them in A.3.3 #8. Release case required.

### CMN-1 — `MemoryConfig::validate` never called; inverted warn/shed flaps every sample (MEDIUM)
- `crates/dtfs-common/src/config.rs:237` (doc claims "called by the role config validate()"), no caller in any `main.rs`; `memory.rs:172-191` `record_sample`.
- With `warn >= shed`, `Shed` sets on `rss >= shed` and clears on `rss < warn` (above shed) → toggles each sample; data node alternates RESOURCE_EXHAUSTED/accept, log spam.
- Fix: call `config.memory.validate()` in all three `main.rs` right after config load (also `PlacementConfig::validate`, see CMN-8).

### CMN-2 — Default `min_replicas_before_ack = 2` exceeds default cold RF = 1 (MEDIUM)
- `crates/dtfs-common/src/types.rs:514` (WriteSafety default 2), `:299` (TierReplication cold 1), `config.rs:979` (FUSE `first_write_tier = "cold"`); `metadata.rs:816-834` `resolve_ack_floor` returns the volume floor verbatim, never clamps to RF; `:2130` default floor from `config.defaults.write_safety`.
- Cluster on compiled-in defaults (example manager.toml has `[defaults.write_safety]` and `[defaults.replication]` commented out) places 1 cold replica and CF-3b then rejects every `CommitChunkLocations`. Docs row (manager.md:216) says valid range `1--RF`; the default violates it for cold. Confidence medium: depends on effective tier picked for the first write; the floor-vs-RF inconsistency is confirmed.
- Fix: clamp `resolve_ack_floor` to `min(floor, effective_mandatory_replicas of the placed tier)` with a warn log, or default the floor to 1; add a config-load check `floor <= max(effective RF)`.

### CMN-3 — `grpc.max_message_size_mb` parsed + documented, never wired (LOW)
- `config.rs:544,1063`; `dtfs-data/src/server.rs:211` and `dtfs-client/src/client.rs:944` hardcode `max_decoding_message_size(32 MiB)`. Docs (manager.md:149, data-node.md:72) promise an effect. B.6 config-surface violation.
- Fix: wire into the tonic builders or delete field + example + docs row.

### CMN-4 — `hex_decode` byte-slices a `&str`, panics on multibyte input (LOW)
- `crates/dtfs-common/src/crypto.rs:125-133`; same class as CO-5-12 (`ChunkId::from_hex`). Currently unreachable over gRPC (tonic `MetadataValue<Ascii>` rejects non-ASCII) but the helper is `pub`.
- Fix: `if !s.is_ascii() { return None; }` before the loop.

### CMN-5 — Anchor datacenter compare is case-sensitive, siblings are not (LOW, operationally HIGH if hit)
- `types.rs:494` (`dc != self.datacenter`), `hash_ring.rs:255,282`; contrast `types.rs:611-617` SpillPolicy and `config.rs:331` read_location_priority (both `eq_ignore_ascii_case`).
- `anchor_datacenter = "leli"` vs node `datacenter = "LELI"` → anchor permanently unsatisfiable: placement never swaps in the anchor, `anchor_pending` stuck, `check_anchor_lag` re-queues forever, `enforce_anchor` refuses every de-location.
- Fix: normalise once (`eq_ignore_ascii_case` in `AnchorPolicy::is_satisfied_by` + the two ring sites, or lowercase at config/registration boundary).

### CMN-6 — Memory-ladder docs say manager pauses at Warn; code pauses at Shed (LOW)
- `memory.rs:40-44` table, `config.rs:38-43`, `docs/src/configuration/manager.md:313-323` vs `scheduler.rs:232` (`== PressureLevel::Shed`, REV-3-5). Fix the three doc sites.

### CMN-7 — `unique_dc_count_for_tier` / `unique_rack_count_for_tier` recomputed per visited vnode (LOW, perf)
- `hash_ring.rs:172-173,187-188,338-361`: full `ring.values()` scan + sort + dedup per skipped vnode, per placement, on the AllocateChunks path (~1500 vnodes/disk at weight 10). Hoist both counts before the strict-pass loop; ring is immutable during the walk.

### CMN-8 — `PlacementConfig.min_free_pct` unvalidated (LOW)
- `config.rs:59-60,84-87`: `>100` makes `headroom_bytes > total` → every disk excluded from the ring; negative/NaN saturate to 0 and silently disable the reserve. Add `validate()` for `0.0..=100.0`, call with CMN-1.

### PROTO-2 — `ScrubConfigOverride` quick/deep cadence hours-only; sub-hour secs ceiled to 1 h silently (LOW)
- `proto/manager.proto:581-592`; `metadata.rs:1105-1112` (`(secs+3599)/3600 .max(1)`); `dtfs-data/src/scrub.rs:101`. BC-2 added secs fields for orphan only. `quick_interval_secs = 600` (accepted by BC-1 test) becomes 3600 on every data node.
- Fix: add `quick_interval_secs = 9`, `deep_interval_secs = 10`; data node prefers non-zero secs; warn at manager when rounding changes the value.

### PROTO-3 — Dead/drifted messages in common.proto (LOW)
- `proto/common.proto:4-10,41-67`: `TierReplication` lacks `tape`, `VolumeConfig` lacks `mandatory_replicas`/`cache_replicas`/`anchor`/`spill`; `NodeId`/`ChunkId`/`ChunkMeta`/`WriteSafety` have zero Rust users. `DiskInfo.tier` comment omits `tape`, `NodeInfo.role` omits `client`. Delete or mark UNUSED.

### Noted, not a finding
- Data-plane `PutChunk`/`ListChunks` MAC has an empty binding (documented A.3.3 #7) → a captured stamp replays against any data node for ±300 s (content-addressing limits it to disk-fill). Consider binding `chunk_id` for PutChunk if the trade-off was not deliberate.

Clean: `replication.rs` B4 arithmetic verified (inner clamp(bytes/MiB s, 30, 270), outer = inner+30 ∈ [60,300], strictly above at the cap); `memory.rs` hysteresis + `hard_cap_mb=0` guard; `topology.rs` distance symmetric; `crypto.rs` planes domain-separated, constant-time compare; `tls.rs`; all AV/BC/BZ fields in `types.rs` carry `serde(default)`; `data.proto` `DeleteChunkRequest.disk_id` optional (CO-1-1).

## 2. dtfs-raft

### RAFT-1 — RegisterNode wipes sticky Quarantined/Maintenance/Draining state on re-register (HIGH)
- `crates/dtfs-raft/src/state_machine.rs:1189` (`sm.nodes.insert(*node_id, info.clone())`, `prior` used only for audit dedup); sole producer `metadata.rs:1028-1041` builds `health: Healthy, quarantine: None` unconditionally.
- A data node quarantined for an integrity fault (Bug BC) re-registers on every restart and comes back Healthy with the same corrupt disk, re-entering read/allocate/replication pools. Bypasses BF-6 (`UpdateNodeStatus` refuses to clear Quarantined) and CO-3-8 (sticky states in `heartbeat_may_mark_healthy`). Operator Maintenance/Draining lost the same way. Contradicts lib.rs:346-349 "only UnquarantineNode clears".
- Fix: in the apply, carry forward from `prior`: `quarantine` + `Quarantined` health when set; keep `Maintenance`/`Draining`. SM regression test: quarantine, re-register, assert still Quarantined. Release case.

### RAFT-2 — RenameFile moves only the exact key; directory rename strands children (HIGH)
- `state_machine.rs:1466-1506`; FUSE `filesystem.rs:1572` issues one `rename_file(from, to)`.
- `mv /Movies/Old /Movies/New`: only the mkdir marker moves; children stay under the old prefix (still visible as a synthetic dir), new dir is empty. Synthetic-dir source (no marker) → `Error("source not found")` → EIO. Renaming onto an existing directory with children replaces it with a file entry. Routine Plex/Sonarr library reorganisation fails or appears to lose files.
- Fix: in the apply, after moving `from`, re-key every `files.range("{from}/"..)` entry (plus `write_leases`, `posix_locks`) to `to + suffix`; treat synthetic-dir source with children as success; refuse when `to` is an existing non-empty directory. Single Raft entry keeps it atomic. SM test + release case.

### RAFT-3 — `logical_size` set by SetFileSize is never revisited; truncate-then-rewrite reports the truncated size forever (HIGH)
- `state_machine.rs:1508-1516` sets `logical_size = Some(new_size)`; `:1122-1124` `UpdateChunkLocations` merges chunks only; `metadata.rs:937-939` `total_size = logical_size.unwrap_or_else(sum chunks)`. FUSE calls `set_file_size` only from `truncate()`/`set_size()` (`filesystem.rs:1532,1549`); `flush_segments_to_cluster` never sends a size.
- `open(O_TRUNC)` on an existing file (cp, rsync --inplace, shell `>`, SQLite journal recycle) → `SetFileSize(0)` → subsequent writes commit chunks but `total_size` stays 0 for every other client and for this client after cache expiry. Truncate to N then append past N: size stuck at N. Also `:1516` `|| *new_size == 0 && false` and `.saturating_sub(0)` are dead code.
- Fix: in `UpdateChunkLocations`, after merge compute `extent = max(idx*chunk_size + size)` and set `logical_size = logical_size.map(|l| l.max(extent))`. SM test: truncate to 0, commit chunk, assert size == chunk size. Delete the dead code.

### RAFT-4 — Apply still stamps replica-local `SystemTime::now()` into replicated state (MEDIUM)
- `state_machine.rs:919-922` `now_ms` used at 1124, 1166, 1473, 1511, 1537-1538, 1559-1560, 2063-2066 and every SM-internal `audit()` timestamp. CO-3-1 fixed only CreateFile.
- Replicas diverge byte-wise; leader change / snapshot install flips every file mtime (Plex change detection) and audit timestamp. Violates lib.rs:617 "apply is a pure function of req + state".
- Fix: add `#[serde(default)] now_ms: u64` to UpdateChunkLocations, RemoveCorruptChunkReplicas, RenameFile, SetFileSize, CreateSymlink, CreateLink, RemoveChunkLocation (proposer-stamped, like AcquireWriteLease); apply uses `if now_ms == 0 { log_id.index } else { now_ms }`; route audit timestamps through it.

### RAFT-5 — Log store: per-append `flush_cf` instead of synced WAL writes; truncate/purge non-atomic (MEDIUM)
- `log_store.rs:384,413` `flush_cf` after each append/vote (no `WriteOptions` anywhere); `:421-454` `truncate_after` deletes key-by-key; `:456-477` `purge` = `delete_range_cf` + separate unsynced `put_cf(META_KEY_LAST_PURGED)`.
- (a) every Raft commit (heartbeat usage updates, 1 s lease sweep, per-tick migration state) creates an L0 SST → continuous compaction (BP-1 already fought this). (b) crash mid-`truncate_after` leaves a hole → openraft fatal storage inconsistency. (c) delete durable but `last_purged` not → `last_purged = None` for a log starting at N+1.
- Fix: `WriteOptions::set_sync(true)` on append/save_vote instead of `flush_cf`; `truncate_after` as one `delete_range_cf(start, u64::MAX)`; `purge` as one synced `WriteBatch`.

### RAFT-6 — BP-2 corrupt-snapshot discard can boot a voter with a blank SM whose log is purged past the snapshot (MEDIUM, confidence medium)
- `state_machine.rs:481-523` `open_at` deletes `snapshot.bin` on decode failure and returns `StateMachineData::default()`; never consults `LogStore::last_purged`.
- If that node wins an election (openraft elects on log freshness), it applies only the surviving suffix onto an empty SM → every file created before the purge point is gone cluster-wide, propagated by its next snapshot. Unverified offline whether openraft 0.10.0-alpha.17 refuses `last_applied < last_purged` at startup (would turn this into a crash-loop instead).
- Fix: after discarding, if `last_purged.is_some()` refuse to start as a voter (reuse the recovery-runbook message at `log_store.rs:140-145`) or wipe the log and rejoin as learner.

### RAFT-7 — DeleteFile leaks lease + posix locks; rmdir of non-empty dir succeeds (LOW)
- `state_machine.rs:1051-1055` removes only `files[path]`; `write_leases`/`posix_locks`/children untouched; FUSE rmdir → `unlink`. Re-create within `lease_ms` → `WriteLeaseConflict` → EIO on first flush; `rm -r` reports success while the tree remains.
- Fix: also remove lease + locks; return `Error` when a child exists (FUSE maps to ENOTEMPTY).

### RAFT-8 — UpdateConfig CAS does not pin the new version; `version` can regress (LOW)
- `state_machine.rs:1386-1400`: checks `expected_version` only, then stores proposer-supplied `version`. Hardening: set `version = current + 1` in the apply, ignore wire value.

### RAFT-9 — Non-finite `duration_secs` in a ScrubRecord poisons every snapshot (LOW)
- `state_machine.rs:1429-1449` stores `f64` verbatim; serde_json writes NaN/inf as `null`; `snapshot.rs:81-93` deserialize then fails → every `install_snapshot` fails and BP-2 discards each manager's snapshot on restart (→ RAFT-6). Data node always sends finite, but the RPC is CLI/REST-injectable. Fix: clamp non-finite to 0.0 in the apply.

### Noted, unconfirmed
- After `RemoveCorruptChunkReplicas` drops every row of a chunk index, `scheduler.rs:1343-1350` `register_replica_location_for_path` can no longer recover `chunk_index` from `chunk_hash` and falls back to `unwrap_or(0)` → repaired copy re-registered at index 0. Not verified whether a task can still exist for such a chunk. Check in the manager section.

Clean: A.3.1 snapshot compat (every post-v1 field `serde(default)`, externally-tagged enums, idempotent `migrate_scrub_results_by_level`); snapshot persist tmp+fsync+rename+dir-fsync; CO-1-1 `(chunk_index, node_id, disk_id)` match with `u32::MAX` sentinel; DeregisterNode TOCTOU re-check; lease arithmetic saturating with proposer `now_ms`; Bug CL mapping (only local `serde_json::to_string` → StorageError); CO-3-6 timeout arithmetic; CNF-4 permit RAII across send; base64 round-trip; no lock across `.await` in apply/snapshot; CO-4-12 error propagation; `audit_log` capped at 10k.

## 3. dtfs-data

### DATA-1 — Scrub quarantine unlinks the `.dat` on ANY fault class, no replacement check (HIGH)
- `crates/dtfs-data/src/store.rs:1502-1526` `quarantine_chunk_on_disk` → `remove_file(dat)` for every id in `corrupt_chunk_ids`; `record_scrub_fault` (`:1345-1361`) pushes ids for detectors `stat` (non-NotFound error), `meta_read` (EIO), `meta_parse` (torn `.meta`), `data_read`; meta-less case (`:1249-1251`) too. None hashed the `.dat`. Dispatched from `scrub.rs:196-215`.
- A.3.3 #4 forbids deleting before a verified replacement exists. A transient EACCES/EIO or a torn `.meta` (DATA-2) destroys the bytes immediately; if this was the sole LELI anchor copy, permanent loss of never-proven-bad data.
- Fix: `rename` the `.dat` into `.dtfs/quarantine/<id>.dat.bad` instead of `remove_file` (reads still fall over, bytes survive for repair). For meta-side faults, re-hash the `.dat` first; digest == id → regenerate `.meta`, count healthy.

### DATA-2 — `.meta` written non-atomically (`File::create` + write); torn `.meta` = false corruption + node self-quarantine (MEDIUM)
- `store.rs:566-574`; recovery CO-1-4b fires only on `dat.exists() && !meta.exists()` (`:1007`); `server.rs:665-688` maps any meta parse error → `verify_on_read_meta_absent` → `data_loss` → `self_quarantined` (blocks all writes, `server.rs:386`).
- Crash between create and sync → 0-byte `.meta` with good `.dat`; next read self-quarantines, next scrub deletes the good `.dat` (DATA-1). Duplicate put of an existing id truncates the live `.meta` in place for the create→write window; concurrent get/verify/scrub sees a parse error.
- Fix: write `.meta` to `tmp/<uuid>.meta.tmp`, `sync_all`, `rename` over final (same as `.dat`). In `recover_from_wal` PutChunk arm treat "WAL present + `.meta` unparseable" like meta-missing.

### DATA-3 — No per-chunk serialisation; PutChunk racing DeleteChunk on the same id shares one WAL filename (MEDIUM, confidence medium)
- `store.rs:538` and `:723-724` both `wal_dir.join("{id}.wal.json")`; no chunk-keyed lock in `ChunkStore`; handlers run concurrently.
- Interleaving delete-unlink-dat / put-rename / put-meta / put-ack / delete-unlink-meta → `.dat` present, `.meta` gone, WAL gone, `success:true` already returned and committed as a location. Every read → `data_loss` + self-quarantine; next scrub deletes `.dat` (DATA-1). Reverse order → phantom location. Realistic trigger: unlink fan-out for file A while identical-content file B streams to the same node (Bug CI dispatch check cannot see B's not-yet-committed location).
- Fix: striped per-chunk-id `tokio::sync::Mutex` in `ChunkStore` held across `commit_chunk_from_tmp_inner` and `delete_chunk_idempotent_on_disk`; key WAL files by op (`<id>.put.wal.json` / `<id>.delete.wal.json`).

### DATA-4 — Commit failure after the rename leaves a full-size meta-less `.dat` until restart (ENOSPC self-sustaining) (MEDIUM)
- `store.rs:471-488` wrapper removes only `tmp_path` on error; inner `:544` renames tmp→final first, then `.meta` create/write/sync/fsync_dir each `?`. After the rename `tmp_path` is gone; a `.meta` ENOSPC leaves the `.dat` in `chunks/` with a live PutChunk WAL: scrub skips it (WAL present), `list_chunks` hides it (no `.meta`), reaper never sees it. Bug BU-6 class via another path; reads of the id on this node → `data_loss` self-quarantine (DATA-2).
- Fix: on error after the rename, remove `final_dat_path`, `final_meta_path`, `wal_path` in-process (CO-1-4b equivalent).

### DATA-5 — PutChunk never compares the caller-declared `chunk_id` with the computed hash; write path has no end-to-end checksum (MEDIUM)
- `server.rs:428-451` reads `first`, never uses `first.chunk_id`; `:503-508` commits `ChunkId::from_bytes(hasher.finalize())`. FUSE sends `""` (`filesystem.rs:1927`) and commits `put_resp.chunk_id` verbatim; replication sends `chunk_hex` but only the client (CO-5-4) compares.
- A.3.3 #1 requires BLAKE3 end-to-end. Bit-flip between the writer's buffer and the receiver's hasher (plaintext link, NIC offload, sender memory fault) is stored under a self-consistent id and committed as truth; two replicas of one `chunk_index` can even land under different ids unnoticed.
- Fix: server: `if !first.chunk_id.is_empty() && first.chunk_id != computed_hex → Status::data_loss` (tmp_guard cleans up). FUSE (see FUSE section): hash the segment and populate `chunk_id`.

### DATA-6 — Heartbeat / ChunkStatusReport / RegisterNode RPCs have no deadline; channel has no keepalive (MEDIUM)
- `main.rs:458,602,636-638` `Request::new(..)` without `set_timeout`; `tls.rs:182-205` sets only `connect_timeout`, no `http2_keep_alive_interval`/`tcp_keepalive`. Failure counting + manager walk (`:775-833`) run only on `Err`.
- Established connection to a wedged/partitioned manager parks the heartbeat loop until the TCP retransmit budget expires (~15 min) or forever on a half-open path; node marked Down cluster-wide; F-024 endpoint walk never runs. CO-3-6 gave Raft RPCs per-request timeouts; the data control plane did not get it.
- Fix: `req.set_timeout(10s)` on the three control RPCs, and/or `http2_keep_alive_interval(10s).keep_alive_timeout(5s)` in `TlsContext::endpoint` (benefits every role).

### DATA-7 — Streaming commit always bumps `chunk_count`/`bytes_stored`; CO-5-5 duplicate guard only on the test-only `put_chunk` (LOW)
- `store.rs:585-587` unconditional `fetch_add`; `already_present` check exists only in `ChunkStore::put_chunk` (`:333-334`), which has no non-test callers. Duplicate streaming puts inflate heartbeat `total_chunks`/`total_bytes_used` until restart rescan.
- Fix: check `final_dat && final_meta exist` before the rename in `commit_chunk_from_tmp_inner`, gate step 5; repoint the CO-5-5 test.

### DATA-8 — Corrupt-id report is one unbounded `ChunkStatusReport`; above ~49k ids it can never be delivered (LOW)
- `main.rs:589-634` drains all ids into one RPC, re-enqueues the whole batch on failure; manager `server.rs:351` uses the tonic default 4 MB decode limit. A partially failing disk (REV-4-4 counts every per-chunk I/O error) → report rejected every 5 s and grows; manager never learns of replicas scrub already deleted. Fix: batches of ~1000, re-enqueue only the remainder.

### DATA-9 — `VerifyChunk` hashes only `meta.size` bytes; over-long `.dat` verifies `valid:true` while `GetChunk` rejects it (LOW)
- `server.rs:816-827` ignores the stat size; `get_chunk` (`:641-644,690`) hashes the full file; scrub (`store.rs:1270`) checks length first. Migration marked Done on a copy every read rejects. Fix: size check before hashing, mirroring scrub.

### DATA-10 — `chunk_hex::decode` byte-slices `&str`, panics on non-ASCII (LOW)
- `server.rs:964-976`; same class as CMN-4/CO-5-12. Fix: use `ChunkId::from_hex` or `s.get(i..i+2)`.

### DATA-11 — `RecordCacheHit` unauthenticated, feeds tiering promotion counters (LOW)
- `server.rs:940-961` no `require_data_rpc_auth`; feeds the same `record_chunk_read` table heartbeat ships as `chunk_reads` for `promote_reads`. Anyone on 9500 can fabricate per-DC demand → cross-DC promotions over the 40 Mbps link, displace real entries from the 10k table. Fix: gate + stamp from `dtfs-client`.

### Noted, not filed
- `shutdown.rs:12` doc promises a final "draining" heartbeat; `main.rs` never sends one (doc drift).
- Multi-disk `get_chunk` takes `.dat` and `.meta` from independently-first-matching disks; no realistic divergence found.

Clean: all six documented RPCs gated with correct method + binding before any I/O; put path (incremental BLAKE3, tmp flush+sync before rename, fsync_dir after rename and after `.meta`, WAL before rename / cleared after `.meta` fsync, `TmpCleanup` RAII, CO-5-3 ENOSPC → RESOURCE_EXHAUSTED, shed check before slot acquire); get path (8 MB `read_exact` streaming, channel depth 2, correct `data_loss`/`not_found` split); delete `disk_id: Option<u32>` threading with unknown-disk no-op; replication WI-04 timeout from on-disk size + CO-5-4 check; ListChunks REV-3-2 incremental walk + positional `chunk_mtimes_ms`; scrub P0-5 WAL skip correct; heartbeat CO-3-7/BF-5/CO-5-6 re-enqueue; `cache.rs` confirmed inert; no mutex across `.await`.

## 4. dtfs-manager

### MGR-1 — Leader tiering engine fed `file_path=""` / fake `Hot` tier; legacy temperature path runs on AV-cache volumes, defeating CO-4-6 (HIGH)
- `crates/dtfs-manager/src/metadata.rs:1500-1520`: the ONLY `report_access` caller (ReportChunkReads intercept in `forward_client_write`) passes `""`, `total, total`, `DiskTier::Hot`. `tiering.rs:327-331` CO-4-6 skip is `if let Some(vid) = path_to_volume.get(&profile.file_path)` → `None` for `""` → never skips. `tiering.rs:933-935` `would_violate_anchor_for("")` → file not found → `false` → proceeds to `schedule_replication` (`:442,:560`).
- `tiering.enabled` defaults true, `hot_threshold` 5. A chunk with 2..4 reads in one heartbeat classifies Warm; `Warm != current_tier(Hot)` → a "demote" to Warm is planned and dispatched even on AV-cache volumes: bypasses `promote_reads`, the operator-flush cooldown (the CO-4-6 bug reintroduced), `select_promotion_target` liveness/capacity, per-DC cap; up to `MAX_LEGACY_MIGRATIONS_PER_TICK` per tick. Chunks with `total == 0` never set `last_access_ms` → Frozen → Tape target → one "No suitable target node" warn per chunk per tick. Every profile field except `reads_by_dc` is garbage on the leader.
- Fix: resolve `file_path` + the replica's real tier from `sm_data` in the intercept, or make `evaluate` `continue` when `path_to_volume.get(file_path)` is `None`. Regression test: AV-volume chunk via `ReportChunkReads` yields zero legacy decisions.

### MGR-2 — Cache demote / flush planners count a replica on a Down node or Failed disk as the surviving mandatory copy (HIGH)
- `tiering.rs:853-862` placements built via `n.disk(e.disk_id).map(|d| d.tier)` with no health filter; `:1953-1959` `mandatory_still_present` = any other placement on a mandatory tier. `scheduler.rs:2933` `FlushSnapshot` `disk_tiers` from every disk regardless of `DiskStatus`; `:3108-3116` `mandatory_survives` same. Neither uses `live_replica_tier`.
- A.3.4/A.6: coverage counts only serviceable replicas. Cold anchor node Down → warm cache copy is the only live replica → both planners still drop it. Apply-side `enforce_anchor` (REV-3-7) saves it only when the volume has an anchor policy AND the dead copy is the anchor; otherwise `apply_remove_chunk_location` has no last-live-replica guard → zero serviceable replicas, and if the disk is genuinely Failed, permanent loss with no backfill source.
- Fix: resolve placement tiers through `scheduler::live_replica_tier` in both snapshot builders (unresolvable → non-surviving); require the surviving mandatory replica to be live in `plan_chunk_demotions` / `plan_flush_cache`. Consider a last-live-replica guard in `apply_remove_chunk_location` independent of anchor.

### MGR-3 — `build_av_cache_snapshot` holds the SM read lock across O(chunks × DCs) awaits on `read_windows.write()` (MEDIUM)
- `tiering.rs:765-907`: `state_data.read()` at 765, per-chunk per-DC `read_window_count(...).await` (takes `read_windows.write()`), `drop(sm)` at 907. Tokio RwLock is fair: once the Raft apply loop queues a writer, every gRPC/REST reader blocks for the whole snapshot (~100k lock acquisitions at tens of thousands of chunks × 3 DCs).
- Fix: collect `(chunk_id, idx, placements, cooldown, volume_id, updated_at)` under the SM lock, drop it, take `read_windows.write()` once, compute all windows.

### MGR-4 — Completing marks a task Done even when the `UpdateChunkLocations` Raft write failed (MEDIUM)
- `scheduler.rs:1385-1398` `register_replica_location_for_path` swallows `client_write` `Err` with a warn; `:2814-2830` `ops.register_replica(...)` returns `()` and the task is unconditionally `Done` + `replication.complete` audited.
- Leader change mid-tick, `WriteLeaseConflict` (CO-4-15) or file-not-found → bytes on target, no row. Task terminal so nothing retries; bytes become CO-3-2 de-located leftovers (reaped), `check_under_replicated` re-copies next tick: silent copy/reap loop over the 40 Mbps link for any chunk of a file under an active write lease.
- Fix: `register_replica` returns `Result`; on `Err` stay `Completing`, bump `attempts`, retry; fail+alert at the cap.

### MGR-5 — `check_under_replicated` re-queues a deterministically failing chunk every tick; Failed backlog unbounded (MEDIUM)
- `scheduler.rs:491-497` `live_index` excludes Done AND Failed; `task_blocks_requeue` (`:3574`) used only by `check_anchor_lag` (`:890-895`, Bug BO-2). Source selection `:570-580` picks any Healthy node holding a row without `live_replica_tier` (copy on a Failed disk is a valid source); target = first healthy node with the tier, no capacity check.
- Source-disk-Failed → "Chunk not found" → Bug AF instant Failed → fresh task every 30 s, each retained 30 min → up to 60 Failed rows per chunk; `MAX_LIVE_TASKS` counts only non-terminal → multi-MB `UpdateMigrationState` proposals again (Bug CN / REV-3-4 class).
- Fix: apply `task_blocks_requeue` in `check_under_replicated` dedup + `schedule_replication`; source via `live_replica_tier`; target via `placement_cfg.has_room`.

### MGR-6 — `check_anchor_lag` bypasses the REV-3-4 task budget; dedup set not updated within the pass (MEDIUM)
- `scheduler.rs:867-1000`: inserts a task per un-anchored chunk with no `task_budget_available` (helper called only at `:612`); `existing_tasks` built once at `:890`, never updated on insert → two files sharing a hash double-schedule `(chunk, target)`.
- Fix: thread `created`/`live_tasks` + `task_budget_available` into `check_anchor_lag`; insert into `existing_tasks` after scheduling.

### MGR-7 — Heartbeat integrity faults dropped silently when the forwarded Raft writes fail (MEDIUM)
- `metadata.rs:1250-1295`: `let _ = self.raft_write_or_forward(QuarantineNode{..})` and `let _ = ...RemoveCorruptChunkReplicas{..}`; response `:1385` `accepted: true` unconditionally. CO-3-7 fixed this for `report_chunk_status` only; verify_on_read faults travel on the heartbeat.
- During an election the node is neither quarantined nor de-located, the data node's collector is already drained → corrupt replica stays advertised healthy until the next scrub.
- Fix: propagate both write failures into `accepted = false` (data node already re-enqueues on that, CO-3-7).

### MGR-8 — Replica registration picks the first disk of the tier even if Failed/Removed, and only the first chunk index carrying the hash (MEDIUM)
- `scheduler.rs:4206-4218` `resolve_disk_id_for_tier` = `disks.iter().find(|d| d.tier == tier).unwrap_or(0)`, no status filter (`validate_task_target` requires a LIVE disk); `ReplicateChunkResponse` (proto/data.proto:172) carries no `disk_id`. `:1343-1350` resolves ONE `chunk_index` per hash.
- (a) node with Failed cold disk 0 + live cold disk 1: copy lands on 1, row says 0 → `live_replica_tier` None → coverage short → task re-created every tick, `replica_already_present(node,0)` makes each completion a no-op → infinite copy loop. (b) file with the same hash at two indexes (zero-filled padding): second index stays short and re-copies forever (intra-file REV-4-6).
- Fix: filter Failed/Removed in `resolve_disk_id_for_tier` (better: return `disk_id` from ReplicateChunk); register for EVERY `chunk_index` carrying the hash.

### MGR-9 — `unwrap_or(0)` in `register_replica_location_for_path` reachable; mis-registers a foreign hash at chunk index 0 (MEDIUM, confidence medium)
- `scheduler.rs:1309-1325` (`find_files_for_chunk` under one SM read lock) → lock released → `:1338-1375` new read lock, `chunk_index = ...find(hash).unwrap_or(0)`, entries = all index-0 rows + pushed `{chunk_index: 0, chunk_hash: <this hash>, node_id: target}` → `UpdateChunkLocations` replaces index 0 wholesale.
- Bug AE orphan guard runs only in Copying; a Verifying/Completing task skips it. If `RemoveCorruptChunkReplicas` (node-wide sweep) or a lease-holder rewrite lands between the two lock acquisitions, the file is returned, the hash is gone, index defaults to 0 → a row claiming chunk 0 is `<other hash>` on target. Reader picking it fetches the wrong chunk's bytes; verify_on_read passes (hashes against the row's own id) → silent wrong content. Small window, corruption-class consequence.
- Fix: path + index lookup under ONE lock; hash not found in the file → skip + warn, never default to 0.

### MGR-10 — CO-3-2 reaper never reclaims a de-located leftover on a node still holding another-tier replica of the same chunk (LOW)
- `scheduler.rs:2137-2145, 3457-3475` `node_expected` = union of hashes across ALL disks of the node; `:2178` reap is node-wide first-found (`disk_id None`). Documented co-located layout (cold anchor + warm cache on one node): failed warm byte-delete leaves warm bytes never reaped; even if reapable, first-found could pick the cold copy. Leak on the 1 TB warm nodes.
- Fix: ListChunks reports `(chunk_id, disk_id, mtime)`; key `expected_by_node` on `(node, disk)`; pass `Some(disk_id)` to the reap.

### MGR-11 — `ForwardClientWrite` applies ANY `ClientRequest` with only the cluster secret (LOW)
- `metadata.rs:1479-1546`: after `forward_secret_ok`, deserialise + `client_write` unconditionally. Secret is held by every data node and FUSE client. Any of them can commit `UpdateConfig` (skips CO-4-3 + CO-5-10), `UpsertVolume` with zero mandatory (skips `apply_set_mandatory`), `DeregisterNode`, `RemoveChunkLocation` without `enforce_anchor`, `UnquarantineNode`.
- Fix: allow-list the variants forwarders actually send (UpdateNodeUsage, UpdateNodeDiskUsage, UpdateNodeStatus, AppendAudit, QuarantineNode, RemoveCorruptChunkReplicas, ReportScrubResult, ReportChunkReads); `permission_denied` for the rest.

### MGR-12 — `write_via_leader` reports success when the leader rejected the forwarded write (LOW)
- `metadata.rs:578-590`: `forward_client_write(..).await?; Ok(())`, `ForwardWriteResponse.success/error` never inspected (contrast `proxy_to_leader`). Monitor logs "Committed UpdateNodeStatus" for a declined write. Fix: check `resp.success`, return `Err(resp.error)`.

### MGR-13 — Orphan/de-located reap runs to completion after leadership is lost (LOW)
- `scheduler.rs:1935-1942, 2038-2050, 2178`: `is_leader()` + `orphan_sweep_allowed` evaluated once before the per-node loop; a long sweep (50 TB anchor node) can span an election. 24 h mtime grace makes it benign in practice. Fix: re-check both per node iteration and before each delete batch.

### MGR-14 — Audit fan-out spawns one unbounded task + Raft proposal per `log()` (LOW)
- `audit.rs:214-232`: `tokio::spawn(fanout.dispatch(..))` per entry, no semaphore; `entries.remove(0)` on a 10k `Vec`. With `audit.replicate_via_raft = true`, a 10k-replica flush spawns thousands of concurrent `client_write`s over the 40 Mbps link. Fix: bounded `Semaphore` / single-drainer mpsc; `VecDeque` for the ring.

### Noted, unconfirmed
- `heartbeat` proposes `UpdateNodeStatus("healthy")` every 5 s per Healthy data node (Raft entry per node per 5 s) — cost over the 40 Mbps link not measured.
- `check_under_replicated` always targets the FIRST healthy node with the tier (BTreeMap order) → all repair copies for a lost node pile onto one target with no capacity check (interacts with MGR-5).
- `create_volume` accepts `replication_{hot,warm,cold} = 0` with tape hard-coded 1; whether a later `set-mandatory` + `normalize_tape_replication` can reach an all-zero effective RF (→ `plan_excess_spill_removal` strips everything) was not constructible.
- `join_complete_api` accepts a client-supplied `grpc_addr` (RegisterNode derives it from the peer IP); a secret-holder can point manager ReplicateChunk/DeleteChunk/ListChunks at an arbitrary address.

Clean: monitor leader-gated + never schedules repair (CH/CM/CO-4-8); every `/api/v1/*` POST behind `rest_auth` `route_layer`, webui GET-only, constant-time compare; webui escapes all untrusted strings, CSP set, secret in sessionStorage only; join CO-2-3 TTL/single-use/server-derived id; CO-1-3 ordering via `delocate_replica` for both flush and demote, `check_misplaced_rebalance` Raft-only; CO-3-5 lock coverage; WI-04 per-target permits owned, no leak; Bug CN prune-first + bounded RPC; REV-3-10 reconcile never drops the last live row; `count_anchor_pending_files` / `resolve_dc_tier` / `check_under_replicated` / `check_misplaced_rebalance` all use `live_replica_tier`; CO-4-3 / CO-5-9 / CO-5-10 / CO-2-1 present; decommission pre-check + apply re-check; `raft_write` maps ForwardToLeader → Unavailable.

## 5. dtfs-fuse

### FUSE-1 — Non-sequential writes re-emit zero-filled gaps as real chunk data; write buffer grows to file size (HIGH)
- `crates/dtfs-fuse/src/writebuf.rs:372-417` `merge_write`: disjoint-before branch `vec![0u8; ..]` + `dirty_offset = new_start`; write past `buf_end` → `data.resize(new_len, 0)`; `:445-458` `extract_complete_chunks` emits every aligned chunk from `dirty_offset`, zeros included. `FileWriteBuffer` is ONE contiguous `Vec<u8>`.
- After `flush()` drained chunks 0..k, any write below `dirty_offset` (ffmpeg/mkvmerge header patch, rsync --inplace, `dd conv=notrunc`, torrent client writing pieces out of order on the NRB landing client) prepends a zero gap covering the committed chunks; next flush re-commits 0..k-1 as zeros with valid checksums: silent destruction of committed data. `need_seed` false so no RMW rescue. Same mechanism: piece at 100 MB then 4 MB → ~96 MB in-heap Vec, nothing in the CF-5 ladder sheds write buffers, H1 bounds batching only. Contradicts A.3.2.
- Fix: sparse per-chunk buffer (`BTreeMap<u32, ChunkBuf>` with dirty ranges); write into a non-resident chunk triggers RMW seed of that chunk, never gap fill; cap total dirty bytes per file.

### FUSE-2 — `flush()`/`release()` on a read-only fh drains and deletes the writer's path-keyed buffer (HIGH)
- `filesystem.rs:1234-1249` `flush(fh)` has no `flags.is_writable()` check (the only flag check is in `write()`, `:1042`), calls `write_buffer.flush_all(&path)`; `:1357-1388` `release` calls `flush` then unconditionally `write_buffer.remove(&file.path)`.
- Kernel issues FUSE flush on every close(2) incl. O_RDONLY. Plex scanner / any header-read tool closing a file a downloader is writing: (a) forces mid-stream commit of the writer's partial tail (then full RMW on the next write), (b) `remove()` drops bytes the writer buffered between the reader's `flush_all` and `remove`, or bytes `restore_segments` just put back. Writer sees no error; file ends with a hole. `was_fresh` + lease renewal computed from the READER's OpenFile (→ FUSE-3).
- Fix: early-return in `flush()`/`release()` for non-writable handles; longer term key the buffer by fh or restrict to lease-holding fhs.

### FUSE-3 — F-013 ghost-unlink fires on files with committed chunks and never restores drained bytes (HIGH)
- `filesystem.rs:1241-1245` `was_fresh = locations.map(|l| total_size == 0 && chunks.is_empty())`; `OpenFile.locations` refreshed only by `flush()` (`:1333`), never by the write()-driven batch flushes (`:1158`). On error with `was_fresh`: `client.delete_file(&path)`, no `restore_segments` (`:1296-1320`).
- Create, stream 20 MB (four chunks committed via write-driven flushes), write tail, fsync → transient failure (election > 15 s walk) → stale `was_fresh` true → file with 16 MB committed is unlinked, tail dropped. App gets EIO once; retried fsync finds empty buffer → Ok; close Ok; file gone. With FUSE-2 a reader's close can trigger this against a writer's file.
- Fix: `committed_any: bool` on OpenFile set on `flush_segments_to_cluster` success; always `restore_segments` on failure; ghost-unlink only in `release()` when nothing was ever committed.

### FUSE-4 — readdir/readdirplus map every cluster error to ENOENT (CO-4-16 violation) (HIGH)
- `fuse_adapter.rs:793-797` and `:865-869` `.map_err(|_| Errno::from(libc::ENOENT))`; also bare EIO at `:659-662, :687-690, :707-710, :1031-1034` (unlink/rmdir/mkdir/rename).
- Manager blip during a Plex library scan → directory "does not exist" → Plex drops the library items (with empty-trash-on-scan). Exactly what CO-4-16 fixed for lookup/getattr; A.8 says route every op through `cluster_err_to_errno`. Secondary: `rm -rf` on a stale dentry gets EIO instead of ENOENT.
- Fix: `cluster_err_to_errno` in both readdir paths and in unlink/rmdir/mkdir/rename/link/symlink.

### FUSE-5 — `metadata_cache` never swept: one full chunk-location list retained per path ever stat'ed (HIGH)
- `filesystem.rs:281, :449-472, :1704-1722`: `cache_metadata` stores full `FileLocations` (every `ChunkInfo` with id, addrs, tiers, `Vec<Location>`); expiry only when the same path is re-looked-up after `metadata_ttl`; all other removes are per-path on mutation. `lookup()` fetches + caches the whole list on every uncached getattr.
- Plex scan stats every file once. 30 GB file at 4 MB chunks ≈ 7500 ChunkInfo × ~300 B × RF = several MB; 10k-title library pins hundreds of MB to > 1 GB on the 1 GB client (A.3.2). getattr needs size/mtime only but pulls the full location list over the wire.
- Fix: bounded LRU (entries or bytes) or periodic expiry sweep; cache attrs only for lookup, fetch locations at open.

### FUSE-6 — `volume_id` hardcoded to `"default"` after every location refresh; non-default-volume files get default-volume placement (HIGH)
- `filesystem.rs:1692-1696` `fetch_file_locations` sets `volume_id: "default"` (proto `GetFileLocationsResponse` has no volume_id, `proto/manager.proto:325-341`); `:1333-1339` `flush()` overwrites every OpenFile's `locations` with that; `:2039-2049` `find_volume_id_for_path` reads it → `allocate_chunks_with_hint(.., volume_id)`; manager `metadata.rs:1822` `data.volumes.get(&req.volume_id)` drives tier, RF, anchor DC.
- File created in volume "strict": correct only for chunks committed before the first flush; every later batch and every write via `open()` of an existing file is placed with the DEFAULT volume's RF/tier/anchor. CO-4-13 protects the ack floor only. Violates A.3.3 #5 / anchor for non-default volumes.
- Fix: add `volume_id` to `GetFileLocationsResponse` and carry it into `FileLocations`; manager side, resolve placement from `sm.files[path].volume_id` when the file exists, ignoring the request value.

### FUSE-7 — RMW seeding incomplete and non-fatal; partial writes truncate or shift committed chunks (HIGH)
- `filesystem.rs:1091-1129` seed gated by `effective_offset > 0`, seeds only `chunk_at(offset / chunk_size)`, on `Err` only `warn!` then continues; `:536-561` O_APPEND pre-seed uses positional `chunks[len-1]` and `last_idx * chunk_size` (REV-2-1: never positional), runs BEFORE the lease acquire; `writebuf.rs:509-530` mid-chunk branch emits `data[..first_end]` as the whole chunk.
- (a) patch at offset 0 (mkvpropedit, `dd conv=notrunc`) never seeded → chunk 0 truncated to n bytes; (b) write straddling into a persisted next chunk seeds only the first → chunk 1 committed as the straddle tail; (c) seed read failure → segment stored at chunk position 0 → bytes shifted by offset-in-chunk, prefix lost; (d) sparse file with single entry at index 25 seeded at offset 0; (e) second O_APPEND opener that will get EBUSY first overwrites the live writer's buffered region with the stale cluster copy.
- Fix: seed every touched chunk not fully covered by buffered data (incl. index 0); seed failure → EIO; `chunk_at(max index)` + `index * chunk_size` for append; lease before touching the buffer.

### FUSE-8 — `record_cache_hit` RPC spawned per 128 KB readahead-cache hit (MEDIUM)
- `filesystem.rs:725-737, :805-821`: `tokio::spawn(client.record_cache_hit(..))` on every `read()` hit → ~32 RPCs per 4 MB chunk per stream, unbounded tasks; slow data node accumulates thousands of outstanding RPCs. AV-4-1c intends one signal per chunk read. Fix: notify once per (fh, chunk_id) transition; semaphore-bound in-flight notifies.

### FUSE-9 — Foreground chunk fetches not single-flighted; concurrent kernel reads of one cold chunk fetch it N times (MEDIUM)
- `filesystem.rs:779-797, :837-892`: `join_pending` joins only `spawn_prefetch` entries; the foreground miss loop calls `client.read_chunk` directly, never registers. Kernel parallel 128 KB reads into one 4 MB chunk → N × `GetChunk` over the 40 Mbps LELI path for the same bytes, N × 4 MB heap; first chunk of every cold file fetched several times. Fix: per-chunk in-flight map (or reuse `pending` with `started=true`) so later readers await the same future.

### FUSE-10 — Prefetch window re-armed only on a miss → stall every `prefetch_chunks+1` chunks (MEDIUM, confidence medium)
- `filesystem.rs:949` `on_chunk_read` sits inside the miss `else`; hits spawn nothing. Miss at 0 prefetches 1..8; 9 is a synchronous miss (~0.8 s at 40 Mbps) before 10..17 are requested → periodic playback stall instead of a sliding window. Fix: call `on_chunk_read` on every chunk transition (track last index per fh); `pending`/`cache` already dedup.

### FUSE-11 — Disk-cache "viewer read" atime refresh can never trigger from FUSE reads (MEDIUM, confidence medium)
- `filesystem.rs:749-751` passes the kernel per-call read length to `lc.get`; `local_cache.rs:38` `ATIME_UPDATE_MIN_BYTES = 1 MiB`; kernel FUSE reads are 128 KB (no max_pages/max_read set) → every playback read classified as scanner → LRU degrades to put-order, watched files evicted like scanned ones. Fix: hint from per-fh consumption (≥ 1 MiB read or readahead active), or refresh atime on the first in-memory miss.

### FUSE-12 — Local cache: O(N) stat sweep on every put at steady state; `lru` index dead but leaks (LOW)
- `local_cache.rs:340-372` `evict_to_size` stats every tracked id whenever `total_bytes > max_bytes`, called from every `put` → at a full 1 TB/4 MB cache ~260k stats per foreground miss on the read path. `lru: BTreeMap<u64,String>` (`:53, :311-318`) inserted per put, never used for eviction, not removed by `prune_by_age`. Fix: evict with hysteresis (to ~90 %), use `lru` as the atime index or delete it.

### Noted, unconfirmed
- Same-process second writable open of one path gets EBUSY (owner_id includes fh) — may be intended P0-1 semantics; breaks apps that dup-open for write.
- Lease renewed only from write()/flush(); idle writer > 120 s gets "write lease lost" EIO (documented P2-x follow-up in code).
- `write()`'s lease renew runs AFTER the batch flush; protected only by the manager's Bug P lease check.
- `readdir` re-lists the whole directory per kernel readdir call; cost on very large dirs not measured.

Clean: `select_read_replica` U4 order + `learn_locations`; P0-2 fallover uniform across transport/timeout/DataLoss; CO-3-12 no DashMap guard across await; WI-05 permits + chain + B2 + Bug CE + H1/H2/H4; data-plane HMAC stamped per attempt on Get/Put, every mutating manager RPC via `stamp_mgr_mutation`; CO-1-8 truncate; sparse-hole reads zero-filled (REV-2-1/2-3); readdir/readdirplus cookie parity (Bug F); REV-2-4 disk-cache verify-on-read; RenameFile migrates leases (CO-4-10); inodes monotonic within a mount session.

---

## Summary

| Crate | HIGH | MEDIUM | LOW |
|---|---|---|---|
| common + proto | 1 | 2 | 8 |
| raft | 3 | 3 | 3 |
| data | 1 | 5 | 5 |
| manager | 2 | 7 | 5 |
| fuse | 7 | 4 | 1 |
| **Total** | **14** | **21** | **22** |

### Suggested plan grouping (for delegation)

1. **Write-path data safety (FUSE)** — FUSE-1, FUSE-2, FUSE-3, FUSE-7, DATA-5 (+ FUSE half). Sparse per-chunk write buffer, fh-gated flush/release, committed_any ghost-unlink guard, complete RMW seeding, end-to-end chunk_id. Single plan; touches writebuf.rs + filesystem.rs + data server.rs.
2. **Metadata SM correctness (raft)** — RAFT-1, RAFT-2, RAFT-3, RAFT-7, RAFT-8, RAFT-9, RAFT-4. All in `apply_request`; each an SM unit test + release case.
3. **Anchor/replica liveness (manager + tiering)** — MGR-2, MGR-1, MGR-4, MGR-8, MGR-9, MGR-5, MGR-6, MGR-10, CMN-5. Planners through `live_replica_tier`, tiering intercept fix, register_replica Result + per-index + live-disk, task budget.
4. **Data-node durability** — DATA-1, DATA-2, DATA-3, DATA-4, DATA-7, DATA-9. Quarantine-by-rename, atomic `.meta`, per-chunk lock + op-keyed WAL, post-rename cleanup.
5. **Auth + control-plane hardening** — PROTO-1, MGR-7, MGR-11, MGR-12, DATA-6, DATA-11, DATA-8, CMN-4, DATA-10. Heartbeat/ReportChunkStatus HMAC, forward allow-list, request deadlines + keepalive.
6. **Config/docs sync + perf** — CMN-1, CMN-2, CMN-3, CMN-6, CMN-8, PROTO-2, PROTO-3, CMN-7, RAFT-5, RAFT-6, MGR-3, MGR-13, MGR-14, FUSE-5, FUSE-6, FUSE-8..12.

FUSE-6 (volume_id) needs a proto field and belongs with group 3 or its own small task; FUSE-5 (metadata_cache) and FUSE-8/9/10 are the client-side perf items most visible to Plex playback.

---

## Status after PLAN-CR-2026-09 (re-verified against `master` 57ae8ed, 2026-09-12)

Every HIGH/MEDIUM finding was re-checked against master by locating the fix
in source after all six group PRs (#105-#110) merged. Workspace suite at
57ae8ed: manager 382, data 121, fuse 110, common 109, raft 105, client 31,
cli 12 — all green (Docker `dtfs-buildenv`). Release cases 100-109 authored,
`bash -n` clean, NOT executed (no lab).

| Finding | Status | Landed in |
|---|---|---|
| PROTO-1 | FIXED | #109 CR-5-1 |
| CMN-1, CMN-8 | FIXED | #110 CR-6-1 |
| CMN-2 | FIXED (clamp to highest placed-tier RF) | #110 CR-6-2 |
| CMN-4 | FIXED | #109 CR-5-7 |
| CMN-5 | FIXED | #107 CR-3-7 |
| PROTO-2 | FIXED | #110 CR-6-3 |
| RAFT-1, RAFT-2, RAFT-3, RAFT-4, RAFT-7, RAFT-8, RAFT-9 | FIXED | #106 CR-2-1..2-6 |
| DATA-1, DATA-2, DATA-3, DATA-4, DATA-7, DATA-9, DATA-10 | FIXED | #108 CR-4-1..4-7 |
| DATA-5 | FIXED (FUSE half #105 CR-1-5, server half #108 CR-4-5) | #105, #108 |
| DATA-6, DATA-8, DATA-11 | FIXED | #109 CR-5-4..5-6 |
| MGR-1, MGR-2 (+ SM last-live-replica guard), MGR-4, MGR-5, MGR-6, MGR-7, MGR-8, MGR-9 | FIXED | #107 CR-3-1..3-6 |
| MGR-11, MGR-12 | FIXED | #109 CR-5-2, 5-3 |
| FUSE-1, FUSE-2, FUSE-3, FUSE-4, FUSE-7 | FIXED (incl. review follow-ups R1 dirty-bytes cap, R2 partial-segment refusal) | #105 |
| FUSE-5, FUSE-6, FUSE-8, FUSE-9, FUSE-10 | FIXED | #110 CR-6-4..6-6 |
| **MGR-3** | **OPEN** — `build_av_cache_snapshot` still holds the SM read lock across the per-chunk `read_window_count().await` loop. Slipped from the plan's task tables. Reserved as the AIWF-5-1 roster dry-run fix (next session; project agents register at session start). | — |
| **MGR-10** | **OPEN** — deferred (optional CR-3-8: `ListChunksResponse.chunk_disk_ids` + disk-keyed reaper). | — |
| CMN-3, CMN-6, CMN-7, PROTO-3, RAFT-5, RAFT-6, MGR-13, MGR-14, FUSE-11, FUSE-12 | OPEN (LOW, deferred by plan decision) | — |

Review follow-ups raised by the lead during the group reviews and fixed before
merge: Group 1 R1/R2 (hole-fill memory cap, unseeded partial segment), Group 3
R1/R2 (intercept lock scope + batch index, no `Hot` tier fallback), Group 4 R1
(scrub P0-5 skip recognises op-keyed WAL names), Group 6 R1 (ack-floor clamp
`max` not `min`).

Rollout note for the next deploy: manager + data + fuse together (LIS-56 HMAC
now covers Heartbeat/ReportChunkStatus; data-plane RecordCacheHit gated;
`GetFileLocationsResponse.volume_id` and `ScrubConfigOverride` secs fields are
additive). Plan file `PLAN-CR-2026-09-CORE-REVIEW-FIXES.md` retired (git
history keeps it); this table is the record.
