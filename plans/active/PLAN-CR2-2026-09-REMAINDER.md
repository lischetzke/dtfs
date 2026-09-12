# PLAN-CR2-2026-09 — core-review remainder (MGR-3, MGR-10, deferred LOWs)

Findings: `plans/reports/CORE-REVIEW-2026-09-12.md` (status table at the end
lists exactly these as OPEN). Same rules, build environment and commit format
as the retired PLAN-CR-2026-09 (git history: `git show 21ca1a7^:plans/active/PLAN-CR-2026-09-CORE-REVIEW-FIXES.md`),
summarised here so this file is self-contained.

## Goals / non-goals

- Close every remaining code finding: MGR-3, MGR-10, CMN-3, CMN-6, CMN-7,
  PROTO-3, RAFT-5, RAFT-6, MGR-13, MGR-14, FUSE-11, FUSE-12.
- Non-goals: anything lab-gated (executing release cases, PERF-SWEEP-5-7,
  REL/REV 🟨 items); the AIWF-5-1 roster dry run (fresh session).

## Build / verify

```bash
MSYS_NO_PATHCONV=1 docker run --rm -v "C:/Users/<user>/git_github.com/dtfs2:/build" \
  -v dtfs-target:/target -v dtfs-registry:/usr/local/cargo/registry \
  -w /build dtfs-buildenv:latest cargo test -p <crate> --release 2>&1 | tail -30
```
Baseline (master 6f949a8): manager 382, data 121, fuse 110, raft 105,
common 109, client 31, cli 12. CRLF sources, Edit tool only; rustc 1.94 ICE
workaround = constructor fn; never `bash -lc` in the container.

Commit: `CR2-<g>-<n>: <summary>` + body + `Release-case:` trailer (path form
or `none (<reason>)`) + `Plan: plans/active/PLAN-CR2-2026-09-REMAINDER.md (CR2-<g>-<n> ✅)`
+ `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`
+ `Claude-Session: https://claude.ai/code/session_01Jpihnx3rznxgvt2mZxq8JB`.
Branch per group (both cross the wire/on-disk threshold), PR, lead review, merge.

## Tasks

Status: ⬜ / 🟦 / ✅ / 🟥

### Group 7 — manager + raft + proto (branch `bug/cr2-7-manager-raft`)

| ID | St | Finding | Files | Sketch | Tests | Done when |
|---|---|---|---|---|---|---|
| CR2-7-1 | ✅ | MGR-3 | `crates/dtfs-manager/src/tiering.rs::build_av_cache_snapshot` | Under the SM read lock collect per-chunk inputs (`chunk_id, idx, placements, cooldown, volume_id, updated_at, unique_dcs`) into a Vec, `drop(sm)`, then take `read_windows.write()` ONCE and compute every `reads_in_window_by_dc`; no `.await` while `sm` is held. | unit: a pure `fn window_counts_for(...)` over a prebuilt read-window map, plus an existing `build_av_cache_snapshot` test still green; grep proves no `read_window_count(...).await` between `state_data.read()` and `drop(sm)`. | Tests pass; grep clean. |
| CR2-7-2 | ✅ | MGR-10 | `proto/data.proto` `ListChunksResponse`, `dtfs-data/src/server.rs::list_chunks`, `dtfs-manager/src/scheduler.rs` orphan/de-located reap | Add `repeated uint32 chunk_disk_ids = N;` positional with `chunk_ids`; data node fills it; reaper keys `expected_by_node` on `(node, disk)` (`chunk_delocated_on_node` → per-disk) and passes `Some(disk_id)` to the reap delete. Empty field (old data node) → legacy node-wide behaviour. | scheduler unit: warm leftover + cold live row on one node → reaped on the warm disk only; legacy empty field → node-wide as before. | Tests pass; compat path covered. |
| CR2-7-3 | ✅ | MGR-13 | `scheduler.rs` orphan sweep loop | Re-check `is_leader()` + `orphan_sweep_allowed` at the top of each per-node iteration and before each delete batch; stop the sweep (audit `orphan.sweep_aborted_not_leader`) when either fails. | unit on the pure gate helper; mock-driven test that a leadership loss mid-sweep stops further deletes. | Tests pass. |
| CR2-7-4 | ✅ | MGR-14 | `dtfs-manager/src/audit.rs` | Bound fan-out with a `Semaphore(16)` (skip + counter when exhausted, never block the caller) and switch the ring to `VecDeque`. | unit: 100 concurrent `log()` calls → at most 16 in flight (counting mock fanout); ring eviction order unchanged. | Tests pass. |
| CR2-7-5 | ✅ | RAFT-5 | `crates/dtfs-raft/src/log_store.rs` | `WriteOptions::set_sync(true)` on append/save_vote instead of `flush_cf`; `truncate_after` = one `delete_range_cf(start, u64::MAX key)`; `purge` = one synced `WriteBatch` (delete_range + last_purged put). Keep the test helpers that call `flush_cf` if they exist (tests only). | existing log_store tests green; new: truncate_after leaves no hole (indices contiguous), purge persists `last_purged` atomically (reopen store → value present). | Tests pass. |
| CR2-7-6 | ✅ | RAFT-6 | `state_machine.rs::open_at` corrupt-snapshot path + `dtfs-manager/src/main.rs` startup | After discarding a corrupt snapshot, if `LogStore::last_purged` is `Some`, refuse to start (error with the recovery-runbook message already used at `log_store.rs` ~140-145) instead of booting a blank voter. | unit: blank SM + `last_purged = Some(..)` → startup helper returns Err; `last_purged = None` → Ok. | Tests pass; docs/src recovery page gets one sentence. |
| CR2-7-7 | ✅ | PROTO-3 | `proto/common.proto` | Delete the unused messages (`TierReplication`, `VolumeConfig`, `NodeId`, `ChunkId`, `ChunkMeta`, `WriteSafety` — confirm zero Rust users first with grep across crates) or mark `// UNUSED — do not add fields`; fix the `DiskInfo.tier` (add `tape`) and `NodeInfo.role` (add `client`) comments. | workspace builds. | Builds; grep shows no user. |
| CR2-7-8 | ✅ | CMN-7 | `crates/dtfs-common/src/hash_ring.rs` | Hoist `unique_dc_count_for_tier` / `unique_rack_count_for_tier` out of the strict-pass loop (compute once per placement). | existing ring tests green; one test asserting placement result identical before/after on a fixed ring. | Tests pass. |
| CR2-7-9 | ✅ | — | `AGENTS.md`, `.claude/rules/integrity.md`, `docs/src/internals/invariants.md` | One sentence each for MGR-10 disk-keyed reap, RAFT-5 synced writes, RAFT-6 refusal, MGR-3 lock scope. | — | Same commit as the code. |

### Group 8 — common + fuse (branch `bug/cr2-8-common-fuse`)

| ID | St | Finding | Files | Sketch | Tests | Done when |
|---|---|---|---|---|---|---|
| CR2-8-1 | ✅ | CMN-3 | `dtfs-common/src/config.rs`, `dtfs-data/src/server.rs`, `dtfs-client/src/client.rs`, `deploy/examples/*.toml`, `docs/src/configuration/*.md` | Wire `grpc.max_message_size_mb` into the tonic builders (`max_decoding_message_size` / `max_encoding_message_size`) everywhere a `32 * 1024 * 1024` literal is used today. | config unit: value propagates; `example_*_toml_parses` green. | Tests pass; docs row true. |
| CR2-8-2 | ✅ | CMN-6 | `dtfs-common/src/memory.rs` table, `config.rs` docs, `docs/src/configuration/manager.md` | Manager rung: Warn = no action; Shed = pause scheduler ticks (self-limiting). | — | Doc sites agree with `scheduler.rs`. |
| CR2-8-3 | ✅ | FUSE-11 | `dtfs-fuse/src/filesystem.rs` read path, `local_cache.rs` | Viewer-read hint from per-fh consumption (≥ 1 MiB read on this fh, or readahead active) instead of the kernel's per-call length; keep `ATIME_UPDATE_MIN_BYTES` semantics for the hint. | unit on the pure hint helper. | Tests pass. |
| CR2-8-4 | ✅ | FUSE-12 | `dtfs-fuse/src/local_cache.rs` | Evict with hysteresis (down to 90 % of `max_bytes`) so the stat sweep is rare; either make `lru` the atime index (update on viewer get, remove on prune) or delete it. | unit: 1000 puts at a full cache → sweeps ≤ 1000/10; `lru` never exceeds `sizes.len()`. | Tests pass. |
| CR2-8-5 | ✅ | — | `AGENTS.md`, `.claude/rules/fuse.md`, `.claude/rules/config-surface.md` | One sentence each. | — | Same commit. |

## Verification log

- Baseline 2026-09-12 master 6f949a8: manager 382, data 121, fuse 110,
  raft 105, common 109, client 31, cli 12 (Docker `dtfs-buildenv`).
- Group 7 complete (branch `bug/cr2-7-manager-raft`), 2026-09-12,
  `cargo test -p <crate> --release` via Docker `dtfs-buildenv`:
  - CR2-7-1 (MGR-3): manager 382 -> 384 (+2). grep proof of no
    `.await` between `state_data.read()` and `drop(sm)` in
    `build_av_cache_snapshot` other than the acquire itself.
  - CR2-7-8 (CMN-7): common 109 -> 110 (+1).
  - CR2-7-7 (PROTO-3): workspace `cargo build --release --workspace`
    clean; `grep -rn "dtfs::common::\(TierReplication\|VolumeConfig\|
    NodeId\|ChunkId\|ChunkMeta\|WriteSafety\)" crates/` empty before
    deletion.
  - CR2-7-4 (MGR-14): manager 384 -> 386 (+2).
  - CR2-7-3 (MGR-13): manager 386 -> 389 (+3).
  - CR2-7-2 (MGR-10): data 121 -> 122 (+1), manager 389 -> 390 (+1).
  - CR2-7-5 (RAFT-5): raft 105 -> 108 (+3); rocksdb 0.22.0 API
    (`WriteOptions::set_sync`, `DB::put_cf_opt`/`write_opt`,
    `WriteBatch::{put_cf,delete_range_cf}`) verified against the
    vendored crate source before writing.
  - CR2-7-6 (RAFT-6): raft 108 -> 111 (+3); `dtfs-manager` builds
    clean with the new `main.rs` startup check wired in (no dedicated
    manager-side unit test — the decision logic is tested in
    dtfs-raft; the integration point is a straight-line call).
  - CR2-7-9: doc-only, no test count change.
  - Final full-workspace counts: manager 390, data 122, fuse 110
    (untouched), raft 111, common 110, client 31 (untouched), cli 12
    (untouched). `cargo build --release --workspace` clean throughout.

- Group 8 (branch `bug/cr2-8-common-fuse`), 2026-09-12,
  `cargo test -p <crate> --release` via Docker `dtfs-buildenv`:
  - CR2-8-2 (CMN-6): doc-only (memory.rs table, config.rs doc comment,
    docs/src/configuration/manager.md); no test count change.
  - CR2-8-1 (CMN-3): common 110 -> 113 (+3: `max_message_bytes`
    conversion, `GrpcConfig::validate` rejects 0,
    `FuseClientConfig::max_message_size_mb` default+validate).
    data 122 -> 122 (wiring only, no new test), client 31 -> 31, fuse
    110 -> 110. `example_*_toml_parses` green.
    `cargo build --release --workspace` clean.
  - CR2-8-4 (FUSE-12): fuse 110 -> 111 (+1: `cr2_8_4_hysteresis_bounds_
    sweep_frequency_at_steady_state`, 1000 puts at a full 10 KB cache ->
    <= 100 sweeps). Deleted the dead `lru: BTreeMap<u64, String>` index
    entirely (smaller diff than wiring it up as the real atime index —
    `evict_to_size` already re-derives recency from on-disk atime, so the
    index had no reader); added `EVICT_TARGET_PCT = 90` hysteresis to
    `evict_to_size`. Updated `rev_3_12_evict_to_size_sweeps_once_for_
    many_victims`'s hardcoded victim count (3 -> 4) to match the new 90%
    target on the same fixture.
  - CR2-8-3 (FUSE-11): fuse 111 -> 114 (+3: `cr2_8_3_is_viewer_read_tests`).
    Added `OpenFile.bytes_read_on_fh`, the pure helper
    `filesystem::is_viewer_read(bytes_on_fh, readahead_active)`, and
    `ReadAheadEngine::is_active()`; `read()` accumulates
    `bytes_read_on_fh` once per call (no `.await` while the `open_files`
    guard is held, CO-3-12) and passes the resulting hint to the new
    `LocalChunkCache::get_with_hint`, which the old `get(id, bytes_hint)`
    now delegates to.
  - CR2-8-5: doc-only. One-sentence additions to AGENTS.md (A.3.2 for
    FUSE-11/12, A.5 for the CMN-3 message-size wiring),
    `.claude/rules/fuse.md`, `.claude/rules/config-surface.md`. No
    matching section in docs/src/internals/invariants.md for these
    finding ids, so left untouched.

## Deviations

- CR2-7-3: `orphan_scrub_tick` is tightly coupled to live gRPC calls
  (`list_chunks_on_node`, `delete_chunk_replica_on_node`), so the
  "mock-driven test that a leadership loss mid-sweep stops further
  deletes" is a pure model (`first_sweep_abort_index`) of the repeated
  `orphan_sweep_allowed` checkpoint calls the real loop makes, rather
  than a full async mock of the loop itself. The production loop was
  changed to match exactly (re-check + `break 'nodes` at the same two
  checkpoints the model represents).
- CR2-7-4: the plan's "counting mock fanout" is `CountingSink`
  implementing a new `FanoutSink` trait extracted from the Raft
  dispatch, rather than a mock of `RaftFanout` itself (which held the
  dispatch inline pre-fix).
- CR2-7-6: added one sentence to
  `docs/src/operations/backup-recovery.md` (not listed in the task's
  file list) pointing an operator at the new startup refusal in the
  single-manager recovery flow, since that is the doc page an operator
  would actually be reading when this fires.
- CR2-7-9: also updated `docs/src/internals/invariants.md` for RAFT-6
  (in addition to the three files the task names) since that page
  already carried a forward-reference to RAFT-6 from the RAFT-9 entry
  that needed the corresponding fix noted.
