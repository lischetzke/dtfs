# PLAN-CO — Codebase audit: fix-guide for the implementing model

Generated 2026-06-12 from a 6-agent parallel audit; each finding was
spot-verified by reading the cited code. **Failing tests are already
written and committed to the working tree** (red on the lab builder).
Your job (the implementing model): make them green with the smallest
correct fix, following the § B.1 procedure, then handle the
`needs-fix-seam` findings (write the helper + its test + the fix
together) and the `no-test-yet` findings (fix + add a release case).

## How to work (read first)

- You CANNOT build on the Windows host (no rocksdb/fuse3). Build + test
  on the lab builder. Sync the working tree first (autocrlf-safe):
  ```bash
  # from the worktree root on the operator host
  STASH=$(git stash create)
  git -c core.autocrlf=false -c core.eol=lf archive "${STASH:-HEAD}" \
    | ssh -i ~/.ssh/dtfs-lab/id_ed25519 debian@10.150.0.150 \
        'rm -rf ~/dtfs2-audit && mkdir -p ~/dtfs2-audit && tar xf - -C ~/dtfs2-audit'
  ```
- Run ONE finding's test, watch it fail, fix, watch it pass:
  ```bash
  ssh -i ~/.ssh/dtfs-lab/id_ed25519 debian@10.150.0.150 \
    'source ~/.cargo/env && cd ~/dtfs2-audit \
     && export CARGO_TARGET_DIR=~/dtfs2-audit/target RUSTFLAGS="-C link-arg=-fuse-ld=bfd" \
     && cargo test -p <crate> --release <test_fn_substring> 2>&1 | tail -30'
  ```
  The builder is 2-core / 4 GB: use `-fuse-ld=bfd` (the default lld
  OOM-bus-errors), build one crate at a time, `cargo clean` if the 32 GB
  disk fills. Re-sync after every local edit (archive overwrites
  `~/dtfs2-audit`).
- All red tests live in a `#[cfg(test)] mod audit_co_tests { ... }` block
  appended to each file (except where noted they reuse an existing
  module). Test fns are prefixed with the finding id.
- One commit per finding, § B.4 format, `Plan:` + `Co-Authored-By:`
  trailers. Co-locate AGENTS.md / docs / example-config updates in the
  same commit (§ B.7, § B.6). Delete this plan when every task is ✅.
- **Snapshot/Raft compat:** any new `ClientRequest` field or struct field
  that reaches a snapshot needs `#[serde(default)]` + a regression test
  (§ A.3.1). Several fixes below add Raft-variant fields — do not skip.
- The audit also surfaced security holes (Phase 2). Treat the redaction /
  auth fixes as security changes: write the code normally, no shortcuts.

Status keys: ⬜ todo 🟦 wip ✅ done 🟥 blocked.

---

## GROUP A — red test already written; make it green

Each row: finding, file the test is in, test fn, the fix. Severity in
the heading. Fuller bug context is in GROUP-A-DETAIL below the table.

| ID | Sev | Test file | Test fn | Fix in one line |
|----|-----|-----------|---------|-----------------|
| CO-1-5 | HIGH | dtfs-data/src/server.rs | `co_1_5a_verify_chunk_detects_flipped_byte`, `co_1_5b_verify_chunk_detects_missing_meta` | `verify_chunk` must load `.meta` + `hash_file_incrementally` vs `meta.checksum`; `valid=false` on mismatch or meta-absent |
| CO-1-4a | HIGH | dtfs-data/src/store.rs | `co_1_4a_delete_wal_replay_must_not_drop_live_chunk` | clear the DeleteChunk WAL on the error path too, AND skip DeleteChunk replay when on-disk `.dat` mtime > wal `timestamp_ms` |
| CO-1-4b | HIGH | dtfs-data/src/store.rs | `co_1_4b_put_wal_replay_cleans_meta_less_committed_dat` | PutChunk replay: if `.dat` exists but `.meta` does not, remove the `.dat` before clearing the WAL |
| CO-1-7 | HIGH | dtfs-fuse/src/writebuf.rs | `co_1_7_midchunk_flush_does_not_drop_leading_partial_bytes` | mid-chunk branch must retain the un-emitted leading partial (drain `bytes_to_complete_first..pos`, adjust `dirty_offset`), not `drain(..pos)` |
| CO-3-1 | HIGH | dtfs-raft/src/state_machine.rs | `co_3_1_create_file_apply_is_deterministic_across_replicas` | stamp a proposer-supplied `now_ms` (carried in the command) into replicated fields instead of `SystemTime::now()` inside apply |
| CO-3-4 | MED | dtfs-manager/src/scheduler.rs | `co_3_4_persistent_verify_mismatch_eventually_fails` | increment `attempts` on verify `Ok(false)` and `Err`; don't reset after a mismatch; fail at `MAX_MIGRATION_ATTEMPTS` |
| CO-2-1 | HIGH(sec) | dtfs-manager/src/api.rs | `co_2_1_shared_secret_redacted_in_flattened_config` | redact `cluster.shared_secret` (→ `"***"`) in the config read path; editor treats `***` as unchanged |
| CO-4-2 | MED | dtfs-data/src/store.rs | `co_4_2_deep_scrub_survives_one_bad_meta` | per-chunk error handling in `scrub_disk` (count corrupt/missing + continue, don't `?`); hash incrementally not `fs::read` |
| CO-4-8 | MED | dtfs-manager/src/monitor.rs | `co_4_8_already_down_node_emits_no_transition` (+ green guard `co_4_8_healthy_stale_node_still_transitions_down`) | `liveness_changes` must skip nodes whose `info.health` already equals the computed status |
| CO-4-9 | MED | dtfs-raft/src/state_machine.rs | `co_4_9_create_file_existing_path_preserves_chunks` | CreateFile on an existing non-empty regular file preserves `chunks`/`created_at_ms` |
| CO-4-10 | MED | dtfs-raft/src/state_machine.rs | `co_4_10_rename_file_migrates_write_lease` | RenameFile moves the `write_leases` entry from→to (mirror posix_locks) |
| CO-4-11 | MED | dtfs-raft/src/state_machine.rs | `co_4_11_pending_scrub_survives_snapshot_install` | add `#[serde(default)] pending_scrub` to `StateMachineSnapshot`; copy in build/install/open_at (assign even when None) |
| CO-4-15 | MED | dtfs-raft/src/state_machine.rs | `co_4_15_empty_lease_id_does_not_bypass_held_lease` | reject empty `lease_id` on commit when a lease is currently held for the path |
| CO-5-1 | LOW | dtfs-raft/src/state_machine.rs | `co_5_1_acquire_write_lease_huge_lease_ms_does_not_overflow` | `saturating_add` for all `now_ms + lease_ms`/`+ extension_ms` |
| CO-5-5 | LOW | dtfs-data/src/store.rs | `co_5_5_duplicate_put_must_not_double_count` | skip the `chunk_count`/`bytes_stored` increment when the rename overwrote an already-present chunk |
| CO-5-11 | LOW | dtfs-cli/src/main.rs | `co_5_11_truncate_handles_multibyte_boundary` | truncate on char boundaries (`char_indices`/`chars().take`), not byte slicing |
| CO-5-12 | LOW | dtfs-common/src/types.rs | `co_5_12_from_hex_rejects_multibyte_64_bytes` | guard `!s.is_ascii()` (or iterate `s.as_bytes()`) in `ChunkId::from_hex` → return Err, never panic |
| CO-5-13 | LOW | dtfs-cli/src/main.rs | `co_5_13_maintenance_requires_explicit_flag` | require an explicit `--enable`/`--disable` (clap `ArgGroup`) for `maintenance` |
| CO-5-19 | LOW | dtfs-client/src/retry.rs | `co_5_19_backoff_does_not_collapse_at_attempt_32` | use a saturating/checked shift so backoff is monotonic past attempt 31 (or delete the dead module) |

### GROUP-A-DETAIL (only the non-obvious ones)

- **CO-1-5** `server.rs:785-794` — `valid:true` iff `get_chunk()` Ok; no
  checksum. Migration marks tasks Done on this then tier-demote can drop
  the source → bit-rot propagates. Reuse `hash_file_incrementally`
  (server.rs:227). § A.6 #6 invariant.
- **CO-1-4a** `store.rs:668-679` — `remove_file(...)?` returns before the
  WAL clear (reachable via two concurrent deletes → NotFound). Replay
  (`recover_from_wal` :840-861) is unconditional → boot deletes a
  re-uploaded live chunk (content-addressed, Bug CI). Fix both: clear WAL
  on error path AND make replay mtime-aware.
- **CO-1-4b** `store.rs:498-546` + replay arm `:825-839` — crash between
  rename and meta-write leaves `.dat` without `.meta`; replay only
  removes `{id}.dat.tmp` (never matches the real `streaming-{pid}-{nanos}`
  tmp). The survivor later DATA_LOSSes on read and self-quarantines the
  node's write path. § A.6 #3/#5.
- **CO-3-1** `state_machine.rs:911-914,1857-1860` — `SystemTime::now()`
  inside `apply_request` writes created/updated_at_ms + audit timestamps
  into replicated state; replicas diverge; replay re-stamps mtimes. Add
  `#[serde(default)] now_ms: u64` to the affected variants (CreateFile,
  UpdateChunkLocations, RenameFile, SetFileSize, RemoveChunkLocation,
  RemoveCorruptChunkReplicas, CreateSymlink/CreateLink); fallback to
  `now()` only when 0 (legacy entries). Compat test required.
- **CO-4-2** `store.rs:1004-1023` — `?` on metadata/read/parse aborts the
  whole disk scan exactly when corruption exists; also `fs::read` loads
  each chunk whole (A.3.2). Count + continue; hash incrementally.
- **CO-2-1** `api.rs:3231` `effective_config_value` serializes the full
  `ManagerConfig`; the unauthenticated `get_config_effective` flattens
  every leaf, emitting `manager.cluster.shared_secret` cleartext — the
  secret that gates raft membership. Redact after the merge; also redact
  TLS key material. Editor sends `***` to mean "unchanged".

---

## GROUP B — needs-fix-seam: create helper + test + fix in one commit

No clean unit seam exists today. For each: (1) add the helper with the
given signature, (2) add a unit test asserting the listed cases (it fails
against a naive/no-op impl), (3) implement the fix and route the
production code through the helper. Where a Raft variant grows a field,
add `#[serde(default)]` + a snapshot-compat test.

### CO-1-1 ✅ done — disk-targeted replica removal [CRITICAL]
- DONE 2026-06-13 (branch fix/co-1-1-disk-targeted). Added
  `legacy_disk_id()` + `#[serde(default = "legacy_disk_id")] disk_id: u32`
  on `RemoveChunkLocation` + `RemoveCorruptChunkReplicas`; apply matches
  `(chunk_index, node_id, disk_id)` unless the `u32::MAX` sentinel (then
  node-wide). `DeleteChunkRequest` gained `optional uint32 disk_id` +
  `string tier`; `store::delete_chunk_idempotent_on_disk(Some(id))` unlinks
  only the matching disk (None = legacy first-found). Threaded the disk
  through excess-spill (`plan_excess_spill_removal` → `(node,disk)`), flush
  (`FlushAction.disk_id/tier`), and demote (`TickDemotion.disk_id`). Whole-
  chunk reclaimers (unlink fan-out, orphan reaper) stay node-wide (None).
  Tests: state_machine `co_1_1_remove_chunk_location_overbroad_drops_colocated_entry`
  (un-ignored, now drops disk-1 only), `_legacy_sentinel_is_node_wide`,
  `_disk_id_defaults_to_legacy_sentinel` (serde compat); scheduler
  `drops_colocated_spill_not_anchor` + cnf1 flush disk assertion; tiering
  demote test now BH-co-located on one node asserting disk_id=1; store
  `co_1_1_disk_targeted_delete_preserves_cold_anchor`, `_disk_none_is_legacy_first_found`,
  `_unknown_disk_id_is_noop`. Release case
  `tests/release/cases/62-demote-colocated-anchor-survives.sh`. AGENTS.md
  § A.3.4 updated. (Build+test gate run by the coordinator.)
- Bug: `state_machine.rs:1854` retains by `(chunk_index, node_id)` →
  drops BOTH entries when a chunk has a mandatory + cache replica
  co-located on one node (legal, § A.3.4 / Bug BH). `DeleteChunkRequest`
  carries no disk either (`store.rs::delete_chunk_idempotent` deletes the
  first copy across `all_disks()`); demote passes `enforce_anchor: None`.
  Idle-demote of the warm copy can delete the cold anchor bytes.
- A characterization test `co_1_1_remove_chunk_location_overbroad_drops_colocated_entry`
  is already red (trips the `debug_assert_eq!(.., 1)` at :1856). Keep it.
- Fix: add `#[serde(default = "legacy_disk_id")] disk_id: u32` (sentinel
  `u32::MAX` = legacy node-wide) to `RemoveChunkLocation` (lib.rs:532) and
  `RemoveCorruptChunkReplicas` (lib.rs:258); apply matches
  `(chunk_index, node_id, disk_id)` unless sentinel. Add `disk_id`/tier to
  `DeleteChunkRequest` (proto/data.proto); data node deletes only the
  matching disk (sentinel = legacy first-found). Thread `disk_id` through
  the three callers: excess-spill `scheduler.rs:613`, flush `:1403`,
  demote `tiering.rs:535`.
- New post-fix test to add: two entries `(node 10, disk 0 cold)` +
  `(node 10, disk 1 warm)`; `RemoveChunkLocation{node_id:10, disk_id:1}`
  → only disk-0 survives. Plus a snapshot-compat test: a legacy entry
  (sentinel) still applies node-wide. Plus data-node test: chunk on two
  disks, delete with warm disk_id, cold `.dat` survives.
- Release case (§ B.5): demote on the BH topology leaves the cold anchor.

### CO-1-6 — FUSE read u32 truncation [HIGH]
- Bug: `filesystem.rs:567` `size.min((file_size - offset) as u32)`
  truncates mod 2^32 → reads of files ≥ 4 GiB return 0/short → kernel
  sees premature EOF (large-remux playback truncates ~4 GiB early).
- Helper: `fn effective_read_len(file_size: u64, offset: u64, size: u32)
  -> u32` = `size.min((file_size - offset).min(u32::MAX as u64) as u32)`.
  Route `read()` through it.
- Test: `effective_read_len(4*GiB, 0, 131072) == 131072` (today 0);
  `effective_read_len(5*GiB, 4*GiB-64, 131072) == 131072`.

### CO-1-8 — FUSE flush/truncate/release data safety [MED-HIGH]
- (b) `filesystem.rs:1296-1301` truncate removes the WHOLE buffer when
  `buffered_eof > new_size`, losing buffered bytes below new EOF. Add
  `WriteBuffer::truncate(&self, path, new_size)` keeping `[0..new_size]`.
  Test: buffer 100 B @0, `truncate("/f",50)`, `flush_all` still emits the
  first 50 B.
- (a) `filesystem.rs:1053-1073` (+ write() :923-925): `flush_all` removes
  bytes before `flush_segments_to_cluster`; on error they're dropped, a
  retried fsync returns Ok with data lost. (c) `release()` :1104-1133
  propagates the flush error before removing the handle/lease/buffer → fh
  leak + stale buffer overlays future reads. Both need a mock data/manager
  client injected into `DtfsFilesystem` — introduce a `trait ChunkIo`
  seam. (a) on flush error, write segments back into the buffer; (c)
  release always cleans up, returns the error after. Tests via the mock.

### CO-3-8 — maintenance mode self-clears [HIGH]
- Bug: `metadata.rs:824-831` heartbeat raft-writes
  `UpdateNodeStatus(healthy)`; apply (state_machine.rs:1121) guards only
  quarantine → Maintenance/Draining flips back ≤5 s.
- Helper: `fn heartbeat_may_mark_healthy(current: HealthStatus) -> bool`
  (false for Maintenance/Draining/Quarantined; true for Healthy/Suspect/
  Down). Gate the heartbeat healthy write on it. Test the table.

### CO-3-9 — leader-addr resolution misses promoted leaders [MED]
- Bug: `metadata.rs:248-253,367-368` resolve the leader gRPC addr only
  from `config.cluster.initial_voters`; a CF-2-promoted leader isn't
  there → heartbeat-originated writes on that follower drop.
- Helper: `resolve_leader_addr(voters, membership_nodes: &BTreeMap<u64,
  NodeInfo>, leader_id) -> Option<String>` preferring membership, falling
  back to voters. Test: leader present only in membership resolves.

### CO-3-10 — FUSE known_locations never populated [HIGH]
- Bug: `filesystem.rs:222,288` the map is only ever `.get()` (no `.insert`
  anywhere in dtfs-fuse) → distance + `read_location_priority` dead,
  `migration.on_remote_read` never fires. `location.rs::select_read_replica`
  is correct in isolation (already unit-tested) — the defect is wiring.
- Fix (needs a small design call — flag at review): extend
  `GetFileLocations`/`ChunkLocationInfo` to carry replica locations
  (positional, like `node_tiers`) and populate `known_locations` on
  metadata refresh, OR pull the node registry once and refresh on TTL.
  Then a characterization test: two replicas where a non-first one matches
  `read_location_priority` → `read_candidates` returns it first.
- Release case: TORO client with `read_location_priority=["toro"]` reads
  the TORO replica.

### CO-3-11 — hash_ring relaxed pass [HIGH]
- Bug: `hash_ring.rs:203-205` relaxed fill pushes `selected`+
  `selected_hosts` but NOT `selected_dcs` → the P0-10 anchor `zip` check
  goes blind and the push branch can return a DUPLICATE NodeId (silent RF
  downgrade). `:150` FailureDomain::Rack compares bare rack names while
  `:298` counts `dc:rack` → cross-DC rack-name collisions mis-skip. Both
  latent (brute force over small topologies didn't surface them as a
  black-box dup — they need an inspectable seam).
- Seam: have placement expose the parallel datacenters/racks, e.g.
  `find_placement_for_pg(...) -> Placement { nodes: Vec<NodeId>,
  datacenters: Vec<String>, racks: Vec<String> }`. Fix: push dcs/racks in
  the relaxed pass; dedup against full `selected`; store `"{dc}:{rack}"`.
  Tests: `placement.nodes.len() == placement.datacenters.len()`; no
  duplicate NodeId when the relaxed pass fires; two same-named racks in
  different DCs both selected in the strict pass.

### CO-4-5 — promotion target ignores tier + health [HIGH]
- Bug: `tiering.rs:594-609` `build_av_cache_snapshot` sets
  `has_target_tier_disk: true` per-disk unconditionally and excludes only
  Draining → promotions target cold-only/dead nodes, fast-fail forever,
  cache replica never created.
- Helper: `fn node_selection_view(info: &NodeInfo, target_tier: DiskTier)
  -> NodeSelectionView` with `has_target_tier_disk = info.disks.iter()
  .any(|d| d.tier == target_tier && d.is_serviceable())` and exclude
  Down/Failed/Quarantined. Test: cold-only big node A + warm small node B,
  tier=Warm → A's view has `has_target_tier_disk=false` (so B wins).

### CO-4-7 — would_violate_anchor models additive copy as a move [MED]
- Bug: `tiering.rs:784-793` subtracts 1 for the source DC even though
  `schedule_replication` is additive (§ A.6 #6) → sole-anchor LELI→TORO
  promote computes anchor_count 0 → refused + `AnchorViolated` spam; reads
  raw `v.anchor_datacenter` not `effective_anchor()`.
- Helper: `fn anchor_violated_after_copy(replicas, anchor_dc,
  source_dc, target_dc, is_additive) -> bool`. Fix: no source subtraction
  on additive copies; use `effective_anchor`. Test: replicas {LELI cold},
  target TORO, additive → false.

### CO-4-13 — ack floor from request volume, not file's volume [MED]
- Bug: `metadata.rs:1639-1643` resolves the floor from `req.volume_id`; a
  wrong/unknown id falls back to default → CF-3b bypass (silent RF
  downgrade).
- Helper: `fn resolve_ack_floor(sm, path, req_volume_id, default_floor)
  -> u32` = stricter of the file's stored-volume floor and the request
  floor. Test: file in floor=2 volume, request claims "default", single
  replica → reject.

### CO-4-14 — create_volume overwrites tuning [MED]
- Bug: `api.rs:1689-1732` blind `UpsertVolume`; re-POST wipes
  set-mandatory/cache/anchor tuning (apply :1264 is a blind insert).
- Fix: 409 when `sm.volumes.contains_key(id)`. Test: create → tune →
  create again → 409, policy preserved.

### CO-4-18 — retry classifies non-idempotent mutations as retryable [LOW-MED]
- Bug: `client.rs:794-799` `matches!(code, Unavailable|DeadlineExceeded|
  Unknown)` with no idempotency input → rename/lease retried after a
  possibly-applied write.
- Helper: `fn is_retryable(code: tonic::Code, idempotent: bool) -> bool`
  (DeadlineExceeded/Unknown only when idempotent; Unavailable always —
  RPC never reached the server). Test: `is_retryable(DeadlineExceeded,
  false) == false`, `== true` when idempotent.

### CO-4-1 — GetChunk channel can buffer ~512 MB [MED]
- Bug: `server.rs:254,301` `mpsc::channel(64)` × 8 MB pieces from a
  detached feeder. Introduce `const GET_CHANNEL_DEPTH: usize = 2..4` and
  use it both places. Test: `GET_CHANNEL_DEPTH * GET_PIECE_SIZE <= ceiling`.

### CO-5-2 — streaming tmp name collision [LOW]
- Bug: `store.rs:379-387` tmp = `streaming-{pid}-{nanos}` collides on a
  coarse clock → two puts interleave into one tmp → checksum mismatch
  acked success. Use `uuid::Uuid::new_v4()` (already a dep). If you want a
  test, extract `fn streaming_tmp_name(...) -> String` and assert two
  calls with the same simulated clock differ.

### CO-5-4 — replicate_to ignores returned chunk_id [LOW]
- Bug: `replication.rs:131-141` checks only `resp.success`. Extract
  `fn validate_put_response(expected_hex, resp) -> Result<()>` asserting
  `resp.chunk_id == expected_hex`; call it in `replicate_to`. Test the
  helper with a wrong id → Err.

### CO-5-10 — update_config version TOCTOU [LOW]
- Bug: `api.rs:2924-2932` read-then-write `version+1`; apply (:1252)
  blind-overwrites → concurrent saves lose updates. Add
  `expected_version: u64` to `ClientRequest::UpdateConfig` (dtfs-raft;
  `#[serde(default)]` + compat test); apply rejects on mismatch. Test:
  two applies from the same base version, second rejected.

### CO-5-15 — update --check compares nothing [LOW]
- Bug: `updater.rs:269-286` prints versions, never compares; naive string
  compare orders `v0.1.10 < v0.1.9`. Helper: `fn version_is_newer(installed:
  &str, latest: &str) -> bool` (semver-aware). Test:
  `version_is_newer("v0.1.9","v0.1.10")` true; reverse false.

### CO-5-17 — readahead pending-map key leak [LOW]
- Bug: `readahead.rs:310-356` `pending.insert` runs after `tokio::spawn`;
  a fast task removes the key first → key leaks → that chunk never
  prefetched again. Reserve the key (entry guard / placeholder) before
  spawn. Test: drive `prefetch` with an immediate mock `read_chunk`,
  assert `pending` empty after completion.

### CO-5-18 — fallocate ignores mode [LOW]
- Bug: `fuse_adapter.rs:962-985` always `set_size(offset+length)` →
  FALLOC_FL_KEEP_SIZE grows the file; `offset+length` can overflow.
  Helper: `fn fallocate_target_size(offset, length, mode) ->
  Result<Option<u64>, Errno>` (None for KEEP_SIZE, Err(EINVAL) on
  overflow, else Some). Tests: KEEP_SIZE → None; `(u64::MAX,1,0)` → EINVAL.

### CO-5-20 — MemoryConfig has no validation [LOW]
- Bug: `config.rs` MemoryConfig accepts `warn >= shed` (Warn unreachable).
  Add `fn validate(&self) -> Result<(), String>` (warn < shed); call it on
  config load. Test: `{warn:90, shed:80}` → Err; default `(80,92)` → Ok.

### CO-4-12 — log_store swallows decode errors [MED]
- Bug: `log_store.rs:319-338` etc. — the `_ =>` catch-all turns a
  `Some(Err(rocksdb_iter_error))` into a fabricated `last_purged`/empty
  log (committed-entry loss); `truncate_after` (:403-413) `filter_map`s
  errored items so a conflicting suffix can survive. Add explicit
  `Some(Err(e)) => return Err(...)` arms and `?` in truncate. Testing the
  swallow needs a fault-injectable iterator/DB wrapper — add that seam,
  then assert a forced iterator error propagates as `Err`.

---

## GROUP C — no test yet: fix + add a release-suite case (§ B.5)

These need cross-process / live-cluster verification (security surface,
multi-tick races, ordering). Fix the code, add a
`tests/release/cases/NN-name.sh` (and `tests/proxmox/abuse/` where
noted). Verify on the lab minicluster (FUSE client `10.150.0.176`, jump
via builder `10.150.0.150`); use `wait_for`, last line `STATUS: …`.

| ID | Sev | Where | Fix | Release case asserts |
|----|-----|-------|-----|----------------------|
| CO-1-2 | HIGH | tiering.rs:1498 | demote planner skips a placement unless another satisfies `effective_anchor`; dispatcher passes the anchor as `enforce_anchor` | demote that would strand the LELI anchor is refused (`tier.anchor_protected` audit) |
| CO-1-3 | HIGH | scheduler.rs:1380, tiering.rs:497 | reorder: Raft `RemoveChunkLocation` (enforce_anchor) FIRST, byte delete after; rely on CO-3-2 reap for RPC-failed leftovers | on a forced Raft-write failure, no `DeleteChunkReplica` RPC issued; no phantom replica |
| CO-2-2 | HIGH(sec) | metadata.rs:1059 | add `shared_secret` to `ForwardWriteRequest` (proto), verify (constant-time) in `forward_client_write` | unauthorized `ForwardClientWrite(DeleteFile)` → PERMISSION_DENIED, file intact |
| CO-2-3 | HIGH(sec) | join.rs:137, api.rs:914 | persist the phase-1 challenge (TTL), require `HMAC(secret, challenge)` in complete, derive `node_id = node_id_from_hostname(hostname)` server-side | forged `/join/complete` (no prior request / mismatched node_id) rejected; existing node's grpc_addr unchanged |
| CO-2-4 | HIGH(sec) | dtfs-cli updater.rs:414,339 | `tempfile::tempdir()` (bail EEXIST) for staging; install via copy-to-`.new` + `rename` | (unit) pre-created dir errors; install leaves no truncated dest window |
| CO-3-2 | HIGH | scheduler.rs:611 | rebalance must delete bytes after the Raft remove, OR teach the reaper a per-node "on disk, no location entry for this node + grace" rule (also mops CO-1-3) | over-replicated chunk: after rebalance the de-located on-disk copy is reaped |
| CO-3-3 | HIGH | scheduler.rs:1623 | re-check current SM after detecting `missing` (or require 2 consecutive ticks) before `QuarantineNode` | demote mid-orphan-scan does NOT quarantine the healthy node |
| CO-3-5 | MED | scheduler.rs:249 vs 304 | `tokio::Mutex` around migration-state load-modify-save | concurrent `schedule_replication` during a tick: task survives, `next_task_id` monotonic |
| CO-3-6 | HIGH | network.rs:120,141,289 | per-request snapshot RPC timeout (`Request::set_timeout` from `option.hard_ttl()` / size-scaled), not the 5 s channel default | `tests/proxmox/abuse/`: >25 MB snapshot installs to a learner behind the 40 Mbps shape |
| CO-3-7 | HIGH | metadata.rs:1024,1041 | `raft_write_or_forward` + `accepted:false` on failure (don't `let _ =`) | corrupt-chunk report to a FOLLOWER manager still drops the replica + queues re-replication |
| CO-3-12 | MED | filesystem.rs:545 | clone path/chunk_size/volume_id/locations, drop the DashMap guard before the chunk loop (match write()/flush()) | (loom/integration) stalled read + 4 concurrent opens all complete |
| CO-4-3 | MED-HIGH | api.rs:2889,2951 | strict-validate the override (deep-merge onto effective, require it deserializes as `ManagerConfig`); persist `toml::to_string_pretty` of the merge, never raw JSON into `.toml` | sparse override round-trips through `load_config`; bad-type override → 400 |
| CO-4-4 | MED | tiering.rs:221,585 | collect decisions under the lock, drop it, then dispatch/query (no lock across await) | (timed) heartbeat latency during a large `evaluate` stays bounded |
| CO-4-6 | MED | tiering.rs:227 | skip legacy-temperature profiles whose volume has an active AV cache policy | after operator flush (future cooldown), reads do NOT re-create the warm replica during cooldown |
| CO-4-16 | MED | fuse_adapter.rs:311,329,369,759,1078 | map transient cluster errors to EIO, ENOENT only for genuine `NotFound` | lookup during manager outage does not report ENOENT |
| CO-4-17 | MED | tiering.rs:193, metrics.rs:102 | prune `read_windows` in `apply_heartbeat_reads`; evict `per_chunk_last_access` alongside `per_chunk_reads` | (unit) 10k samples bounded; MAX+k chunks → len ≤ MAX |
| CO-5-3 | LOW | server.rs:515 | surface `DtfsError::Io` from `commit_chunk_from_tmp` and map via `put_io_status` (ENOSPC → RESOURCE_EXHAUSTED) | (unit) raw_os_error(28) → RESOURCE_EXHAUSTED |
| CO-5-6 | LOW | dtfs-data main.rs:515,548 | re-enqueue scrub results + per-chunk read counters on heartbeat failure (Bug O pattern) | (helper unit) re-enqueue restores counters |
| CO-5-7 | LOW | scheduler.rs:2356 | resolve `FlushSnapshot.anchor` per the file's volume, not the default | (unit) non-default-volume flush enforces its own anchor |
| CO-5-8 | LOW | scheduler.rs:2540 | flush "remaining" counter = count of remaining would-drop entries, not `examined - actions` | (unit) counter correct |
| CO-5-9 | LOW-MED | api.rs:2847 | derive the `defaults` block of GET /config from `effective_config_value`, not raw `state.config` | (unit) a `defaults.*` cluster override is visible on read-back |

---

## Locked-in decisions needing a human nod before CO-1-1 / CO-1-3 / CO-2-1 land

1. `disk_id` field on `RemoveChunkLocation` / `RemoveCorruptChunkReplicas`
   / `DeleteChunkRequest`, `#[serde(default)]` sentinel `u32::MAX` =
   legacy node-wide.
2. Demote/flush ordering flips to Raft-remove-first; RPC-failed leftover
   bytes handled by the CO-3-2 per-node reap rule.
3. `cluster.shared_secret` redacted as `"***"`; editor treats `***` =
   unchanged.
4. CO-3-10 wire-field vs registry-pull for replica locations.

## Cross-cutting invariants (apply to every task)

- New Raft-variant/struct fields reaching a snapshot → `#[serde(default)]`
  + compat regression test (§ A.3.1).
- Phase touching write/migrate/delete/scrub paths → run the § A.6
  checklist, mark "Verified by <test>" in the commit.
- Release registration (§ B.5) for every cluster-visible fix (all of
  GROUP C; CO-1-1/1-5 also warrant one).
- AGENTS.md updates co-located in the same commit when behaviour it
  documents changes: § A.3.4 keying (CO-1-1), demote ordering (CO-1-3),
  config read endpoint (CO-2-1/4-3/5-9), read path wiring (CO-3-10).

## Deferred — UNCONFIRMED (reproduce before tasking)

rmdir trailing-slash mismatch (fuse U1); read zero-padding masking short
chunks (fuse U2); FolderSequential.prefetch_next_files unimplemented
(fuse U3); ScrubRecord.duration_secs NaN → poison snapshot; UpdateNodeStatus
on quarantined node vs "Some iff Quarantined"; disk_id = positional config
index vs BN-1 UUID remap (data); BF-8 whole-node quarantine on single disk
fail; directory rename child decomposition; 5 s metadata TTL vs 200 ms
kernel TTL; setlk middle-range unlock split (fuse U4); list_chunks O(pages×n)
rescan; ArcCache never populated (wire-with-byte-accounting vs remove).

## Verification log

- 2026-06-12: 22 audit_co tests written across 10 files, confirmed RED on
  builder 10.150.0.150 (per-crate `cargo test --no-run` compiles; named
  tests fail by panic/assertion as documented). Full-workspace
  `cargo test --workspace --no-run` gate: see commit / build log.

## Deviations

- CO-1-1 unit test is a characterization test (proves the current
  overbroad drop via the production `debug_assert`); the surgical
  "drop exactly the right disk" test is added with the fix (needs the new
  `disk_id` field). CO-4-12 / CO-5-2 / CO-5-4 and the GROUP-C items had no
  pure seam — listed with the exact helper to extract.
