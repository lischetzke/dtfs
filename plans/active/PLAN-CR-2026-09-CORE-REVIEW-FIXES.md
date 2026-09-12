# PLAN-CR-2026-09 — core-review fixes (common, proto, raft, data, manager, fuse)

Findings: `plans/reports/CORE-REVIEW-2026-09-12.md` (57 findings, 14 HIGH /
21 MEDIUM / 22 LOW). Every task below cites its finding id; read the finding
before starting the task — it carries the exact file:line evidence and the
trigger scenario the regression test must reproduce.

## Goals

Close every HIGH and MEDIUM finding, plus the LOW findings that are cheap
and sit in a file a HIGH/MEDIUM task already touches. Each fix lands with a
failing-then-passing test in the purest layer (§ B.5) and, where the
behaviour is cluster-visible, a release case (authored + `bash -n` clean;
cannot be executed — no lab).

## Non-goals / locked-in decisions

- No lab, no live verification, no release. Dev builds only; unit + crate
  suites are the gate.
- Implementation is delegated to Sonnet agents, **one agent per group,
  groups run sequentially** (groups share `metadata.rs`, `scheduler.rs`,
  `config.rs`, `AGENTS.md`; parallel agents would conflict). Each group is
  one branch `bug/cr-<group-id>` off current `master`, one PR, reviewed by
  the lead before merge; the next group branches from the merged result.
- Fix the smallest thing that closes the finding. No refactors beyond what
  the fix needs. No new abstractions for single-use code.
- LOW findings not listed below are deferred, not dropped: CMN-3, CMN-6,
  CMN-7, PROTO-3, RAFT-5, RAFT-6, MGR-13, MGR-14, FUSE-11, FUSE-12. Re-file
  from the report when picking them up.
- Wire changes (proto) are additive only, `#[serde(default)]` on every new
  persisted field (§ A.3.1), old managers/clients must interoperate.

## Build / verify environment (deviation from § A.4)

`dtfs-builder` (10.20.0.150) is unreachable. Operator decision: build in
**Docker** with the local `dtfs-buildenv:latest` image (rust toolchain at
`/usr/local/cargo/bin`, protoc, clang/libclang, libfuse3-dev; `CARGO_HOME`
`/usr/local/cargo`, `CARGO_TARGET_DIR=/target`). Two named volumes persist
the build cache across runs: `dtfs-target` (→ `/target`) and `dtfs-registry`
(→ `/usr/local/cargo/registry`). The Windows checkout is bind-mounted at
`/build`, so edits made on the host are built in place — no sync step.

```bash
# from Git Bash: MSYS_NO_PATHCONV=1 is REQUIRED, otherwise `-w /build` is
# rewritten to `C:/Program Files/Git/build` and docker refuses to start.
MSYS_NO_PATHCONV=1 docker run --rm -v "C:/Users/<user>/git_github.com/dtfs2:/build" \
  -v dtfs-target:/target -v dtfs-registry:/usr/local/cargo/registry \
  -w /build dtfs-buildenv:latest cargo test -p <crate> --release 2>&1 | tail -30
```

Verified 2026-09-12: `cargo test -p dtfs-common --release` → 104 passed
(baseline for the other crates is in the first verification-log entry).

Do NOT use `bash -lc` inside the image (the login profile drops
`/usr/local/cargo/bin` from `PATH`); call `cargo` directly as above or via
`bash -c`. `--release` is required: the cached `/target` has no debug
artefacts for this tree and a debug build of dtfs-manager takes far longer.

Rules for the implementer:
- `cargo test -p <crate> --release` for every crate you touched, before
  every commit. Quote the failing-then-passing output in the commit body.
- Full workspace `cargo test --release --workspace` once per group before
  opening the PR.
- Source files are **CRLF** in the Windows checkout. Use the Edit tool for
  edits; do not pipe files through `sed -i` (it strips/mangles CRLF).
- rustc 1.94 can ICE on struct-literal edits in dtfs-manager/dtfs-data
  (`query stack [typeck]`). Reformulate (move the literal into a free
  function) — do not remove the crate-level `#![allow(dead_code)]`.
- Release case numbering continues from `100-`. Every case: `trap
  case_cleanup EXIT`, last stdout line `STATUS: PASS|FAIL|SKIP`, no operator
  interaction, `bash -n` clean. Do not add to `QUICK_CASES`.
- Run `cargo test -p dtfs-common --release` after any config/example change
  (`example_*_toml_parses` gates § B.6 config-surface sync).

## Data-model changes

| Change | Shape | Compat |
|---|---|---|
| `GetFileLocationsResponse.volume_id` | `string volume_id = 8;` (next free tag in proto/manager.proto — verify) | old manager sends `""` → client keeps `"default"` |
| `HeartbeatRequest` / `ChunkStatusReport` auth | no proto change; gRPC metadata `x-dtfs-rpc-ts` + `x-dtfs-rpc-mac` over `dtfs-mgr-rpc` plane, binding = `node_id` string | empty secret = legacy skip |
| `ClientRequest::{UpdateChunkLocations, RemoveCorruptChunkReplicas, RenameFile, SetFileSize, CreateSymlink, CreateLink, RemoveChunkLocation}` | add `#[serde(default)] now_ms: u64` | `0` → apply uses `log_id.index` (CO-3-1 fallback) |
| `ScrubConfigOverride` | `uint64 quick_interval_secs = 9; uint64 deep_interval_secs = 10;` (verify free tags) | `0` → fall back to `*_hours` |
| `ListChunksResponse` | `repeated uint32 chunk_disk_ids = N;` positional with `chunk_ids` | empty → node-wide legacy (MGR-10, optional, see CR-3-8) |
| `OpenFile.committed_any: bool` (dtfs-fuse, in-memory) | set on first successful `flush_segments_to_cluster` | n/a |
| `FileWriteBuffer` | contiguous `Vec<u8>` → `BTreeMap<u32, ChunkBuf>` (per chunk index, `data: Vec<u8>`, `dirty: Range<usize>`, `seeded: bool`) | in-memory only |
| `ChunkStore.chunk_locks` | `[tokio::sync::Mutex<()>; 256]` striped by first `.dat` byte pair | in-memory only |
| WAL file name | `<id>.put.wal.json` / `<id>.delete.wal.json`; recovery still reads legacy `<id>.wal.json` | on-disk, additive |
| `MemoryConfig::validate`, `PlacementConfig::validate` | called from all three `main.rs` after `load_config` | rejects previously-accepted bad configs (warn>=shed, min_free_pct out of range) — document |

## Task list

Status: ⬜ todo / 🟦 wip / ✅ done / 🟥 blocked / 🟨 review

### Group 1 — FUSE write-path data safety (branch `bug/cr-1-fuse-write-path`)

Order matters: CR-1-1 first (buffer shape), then 1-2..1-5 build on it.

| ID | St | Finding | Files | Sketch | Tests | Done when |
|---|---|---|---|---|---|---|
| CR-1-1 | ✅ | FUSE-1 | `crates/dtfs-fuse/src/writebuf.rs`, `filesystem.rs` | Replace the contiguous `FileWriteBuffer` (`data: Vec<u8>` + `dirty_offset`) with a per-chunk map `BTreeMap<u32, ChunkBuf { data: Vec<u8>, dirty: Range<usize>, seeded: bool }>`. `merge_write` writes into the affected chunk(s) only; never zero-fills a gap to another chunk. A write into a chunk that is not resident and does not cover the whole chunk sets `needs_seed` (returned to the caller) instead of gap-filling. `extract_complete_chunks` / `extract_all_chunks` / `restore_segments` / `logical_size` / `truncate_at` / `effective_batch_chunks` keep their signatures and semantics (CO-1-7, CO-1-8, H1 byte clamp). Total dirty bytes per file capped at `FLUSH_BATCH_MAX_BYTES * 2`; a write that would exceed it forces a flush first (caller side). | writebuf unit: (a) seq write 0..3 chunks, flush, write 100 B at offset 0 → extract yields ONLY chunk 0 marked `needs_seed`, chunks 1..2 NOT re-emitted; (b) write at 100 MB then at 4 MB → buffer holds two chunk entries, `dirty_bytes() < 2*chunk_size`; (c) all existing writebuf tests pass unchanged (or updated with a one-line reason in the commit). | Tests (a),(b) fail on current code, pass after; `cargo test -p dtfs-fuse --release` green. |
| CR-1-2 | ✅ | FUSE-7 | `filesystem.rs` (`write()` seed block ~1091-1129, O_APPEND pre-seed ~536-561), `writebuf.rs` | Seed EVERY chunk the write touches that is not fully covered by buffered data, including index 0 (drop the `effective_offset > 0` gate). Seed read failure returns `Err` (→ EIO), never `warn!` + continue. O_APPEND pre-seed: use `chunk_at(max chunk_index)` and `index * chunk_size`, and run it AFTER the lease is acquired. | fuse unit with a mock client: (a) write 100 B at offset 0 of a 1-chunk existing file → flushed chunk 0 is `chunk_size` bytes with the patched prefix; (b) write straddling chunk 0→1 seeds both; (c) seed error → `write()` returns Err. | Tests fail before, pass after; `cargo test -p dtfs-fuse --release` green. |
| CR-1-3 | ✅ | FUSE-2 | `filesystem.rs` `flush()` ~1234, `release()` ~1357 | `flush(fh)`: if `!file.flags.is_writable()` return `Ok(())` before touching `write_buffer`. `release(fh)`: only `write_buffer.remove(&path)` when this fh is writable AND no other open writable fh on the same path remains (scan `open_files`). | unit: open RO + RW on same path, write via RW, flush RO → buffer untouched; release RO → buffer still present; release RW → removed. | Test fails before, passes after. |
| CR-1-4 | ✅ | FUSE-3 | `filesystem.rs` (`OpenFile`, `flush()` error branch ~1296-1320, `flush_segments_to_cluster`) | Add `committed_any: bool` to `OpenFile`, set true on first successful `flush_segments_to_cluster` for that path (set on every OpenFile of the path). `was_fresh` = `!committed_any && locations empty`. On flush failure ALWAYS `restore_segments` first; ghost-unlink only from `release()` when `!committed_any`. | unit: create, write 2 chunks (batch flush commits them), write tail, force flush error → file NOT deleted, segments restored, next flush succeeds and commits the tail. | Test fails before, passes after. |
| CR-1-5 | ✅ | DATA-5 (FUSE half) | `filesystem.rs` `write_chunk_bytes(&addr, "", ..)` ~1927, `crates/dtfs-client/src/client.rs` put path | FUSE hashes each segment (BLAKE3, `dtfs_common::checksum`) before the put and sends the hex in `PutChunkRequest.chunk_id` of the first piece. After the put, compare `put_resp.chunk_id` to the local hex; mismatch → treat as put failure (retry next replica / return Err), never commit the location. | unit: mock data node returning a different id → segment not committed, error surfaced. | Test fails before, passes after. |
| CR-1-6 | ✅ | FUSE-4 | `crates/dtfs-fuse/src/fuse_adapter.rs` readdir ~793, readdirplus ~865, unlink/rmdir/mkdir/rename/link/symlink bare EIO/ENOENT sites | Route every `map_err` through `cluster_err_to_errno`. | fuse_adapter unit: `cluster_err_to_errno` already tested; add a test that readdir on a mock returning `Unavailable` yields EIO not ENOENT. | grep shows zero `Errno::from(libc::ENOENT)` in error-mapping closures outside the helper. |
| CR-1-7 | ✅ | — | `tests/release/cases/100-fuse-rewrite-offset-zero.sh`, `101-fuse-reader-close-does-not-drain-writer.sh` | Case 100: write 12 MB file, `dd conv=notrunc` 1 KB at offset 0 and at offset 4 MB−512, `cmp` against a local reference. Case 101: writer streams 20 MB with `pv -L`, reader `cat file > /dev/null` mid-stream, `cmp` after writer closes. | `bash -n` clean. | Cases present; STATUS line; documented as unexecuted in the verification log. |
| CR-1-8 | ✅ | — | `AGENTS.md` A.8 `filesystem.rs`/`writebuf.rs` entries | Document the per-chunk buffer, fh-gated flush/release, `committed_any`, end-to-end `chunk_id`. | — | Same commit as the code it describes. |

### Group 2 — Raft state-machine correctness (branch `bug/cr-2-raft-sm`)

| ID | St | Finding | Files | Sketch | Tests | Done when |
|---|---|---|---|---|---|---|
| CR-2-1 | ✅ | RAFT-1 | `crates/dtfs-raft/src/state_machine.rs` `RegisterNode` arm ~1189 | Carry forward from `prior`: if `prior.quarantine.is_some()` → `info.quarantine = prior.quarantine.clone(); info.health = Quarantined`; if `prior.health ∈ {Maintenance, Draining}` → keep `prior.health`. Audit `RegisterNode: <id> (sticky state preserved: <state>)` when it fires. | SM unit (`posix_path_regression` or new `sticky_state_tests`): quarantine node → RegisterNode → still Quarantined with same `QuarantineInfo`; Maintenance → RegisterNode → still Maintenance; Healthy → RegisterNode → Healthy. | Tests fail before, pass after. |
| CR-2-2 | ✅ | RAFT-2 | `state_machine.rs` `RenameFile` arm ~1466 | After moving `from` (or when `from` has no exact key but `files.range("{from}/"..)` has children → synthetic dir, success), re-key every child `k` with `k.starts_with("{from}/")` to `to + &k[from.len()..]`, updating `meta.path`; move matching `write_leases` and `posix_locks` too. Refuse (`Error`) when `to` exists as a directory with children. Collect keys first, then mutate (BTreeMap). | SM unit: (a) mkdir /a, create /a/x, /a/b/y; rename /a→/c → /c/x, /c/b/y exist, /a/* gone, `meta.path` updated; (b) synthetic dir rename works; (c) rename onto non-empty dir → Error; (d) lease on /a/x follows to /c/x. | Tests fail before, pass after. |
| CR-2-3 | ✅ | RAFT-3 | `state_machine.rs` `UpdateChunkLocations` arm ~1122, `SetFileSize` arm ~1508 | After merge: `extent = chunks.iter().map(|c| c.chunk_index*chunk_size + c.size_bytes).max()`; `file.logical_size = file.logical_size.map(|l| l.max(extent))` (None stays None — derived). Delete dead code at ~1516 (`|| *new_size == 0 && false`, `.saturating_sub(0)`). | SM unit: create, commit 1 chunk (4 MB), SetFileSize(0), commit chunk → `logical_size == Some(4 MB)`; SetFileSize(6 MB) then commit chunk idx 0 → stays 6 MB (sparse-grow preserved). | Tests fail before, pass after. |
| CR-2-4 | ✅ | RAFT-7 | `state_machine.rs` `DeleteFile` arm ~1051; `crates/dtfs-fuse/src/fuse_adapter.rs` rmdir | `DeleteFile`: also `write_leases.remove(path)`, `posix_locks.remove(path)`; if `files.range("{path}/"..)` has a child → `Error("directory not empty")`. FUSE rmdir maps that error to ENOTEMPTY (extend `cluster_err_to_errno` or match the message via a dedicated gRPC code `FailedPrecondition`). | SM unit: delete with live lease → lease gone; delete dir with child → Error, child intact. | Tests fail before, pass after. |
| CR-2-5 | ✅ | RAFT-4 | `crates/dtfs-raft/src/lib.rs` `ClientRequest`, `state_machine.rs`, proposers in `dtfs-manager/src/metadata.rs`, `scheduler.rs`, `tiering.rs` | Add `#[serde(default)] now_ms: u64` to the seven variants listed in Data-model changes. In apply: `let now_ms = if req.now_ms == 0 { log_id.index /* CO-3-1 fallback */ } else { req.now_ms }`; pass it into `audit()` for those arms. Every proposer stamps `now_ms` from `SystemTime::now()`. Remove the top-of-apply `SystemTime::now()` once no arm needs it (leave it if any arm not in scope still does; note in Deviations). | SM unit: apply the same `RenameFile{now_ms: 1234}` on two fresh SMs → `serde_json::to_vec` identical; old-format entry (no `now_ms`) still deserialises. Snapshot compat test per § A.3.1. | Tests pass; `grep -n "SystemTime::now" state_machine.rs` shows only the fallback-free sites documented in Deviations. |
| CR-2-6 | ✅ | RAFT-8, RAFT-9 | `state_machine.rs` `UpdateConfig` ~1386, `ReportScrubResult` ~1429 | UpdateConfig: `version = sm.cluster_config.version + 1`, ignore wire value (keep CAS check). ScrubRecord: `duration_secs = if x.is_finite() { x } else { 0.0 }`. | SM unit: UpdateConfig with wire version 1 after current 5 → stored 6; ScrubResult with NaN → snapshot round-trips. | Tests fail before, pass after. |
| CR-2-7 | ✅ | — | `tests/release/cases/102-rename-directory-moves-children.sh`, `103-truncate-rewrite-reports-size.sh`, `104-quarantined-node-restart-stays-quarantined.sh` | Shell cases for RAFT-2/3/1 via FUSE + REST. | `bash -n` clean. | Present, unexecuted. |
| CR-2-8 | ✅ | — | `AGENTS.md` A.8 `state_machine.rs` entry, A.3.1 | Document `now_ms` convention, RenameFile prefix semantics, sticky-state carry-forward. | — | Same commit. |

### Group 3 — Anchor / replica liveness in manager + tiering (branch `bug/cr-3-anchor-liveness`)

| ID | St | Finding | Files | Sketch | Tests | Done when |
|---|---|---|---|---|---|---|
| CR-3-1 | ✅ | MGR-2 | `crates/dtfs-manager/src/tiering.rs` `build_av_cache_snapshot` ~853, `plan_chunk_demotions` ~1953; `scheduler.rs` `FlushSnapshot::from_sm` ~2933, `plan_flush_cache` ~3108 | Resolve every placement tier through `scheduler::live_replica_tier(sm, node_id, disk_id)` (returns `None` for Down/Failed node or Failed/Removed disk). `None` → placement excluded from `mandatory_still_present` / `mandatory_survives` (still listed so it can be dropped itself). | planner unit tests (existing `co_1_2_*`, `plan_flush_cache` tests as templates): cold anchor on Down node + warm cache → demote/flush REFUSED with `anchor_protected` / `mandatory_refusals`; same with node Healthy → allowed. | Tests fail before, pass after. |
| CR-3-2 | ✅ | MGR-2 (defence) | `crates/dtfs-raft/src/state_machine.rs` `apply_remove_chunk_location` ~2040 | Independent of anchor: refuse (`Error("last live replica")`) when the row being removed is the last live replica of `(path, chunk_index)` (`live` = node not Down/Failed, disk not Failed/Removed, using the existing survivor scan). Keep `enforce_anchor` logic. | SM unit: two rows, one on Down node; remove the live one → Error; remove the Down one → ok. | Test fails before, passes after. |
| CR-3-3 | ✅ | MGR-1 | `crates/dtfs-manager/src/metadata.rs` ReportChunkReads intercept ~1500; `tiering.rs` `evaluate` ~327 | Intercept: look up `sm_data` for the file(s) referencing `chunk_id` and the replica tier on `node_id` (`live_replica_tier`); pass real `file_path` + tier; skip `report_access` when the chunk is unknown. `evaluate`: `continue` when `path_to_volume.get(&profile.file_path).is_none()` (defence). | tiering unit: profile with `file_path: ""` yields zero legacy decisions; AV-volume chunk reported through the intercept yields zero legacy decisions; non-AV volume chunk still classifies. | Tests fail before, pass after. |
| CR-3-4 | ✅ | MGR-4, MGR-8, MGR-9 | `scheduler.rs` `register_replica_location_for_path` ~1338, `register_replica` (`MigrationOps`), Completing transition ~2814, `resolve_disk_id_for_tier` ~4206 | (a) `register_replica` returns `Result<(), String>`; on `Err` task stays `Completing`, `attempts += 1`, retry next tick, `Failed` + alert at the existing cap. (b) path lookup + index lookup under ONE SM read lock; hash not found in the file → skip + warn, never `unwrap_or(0)`. (c) register the new row for EVERY `chunk_index` in the file whose rows carry the hash. (d) `resolve_disk_id_for_tier` filters `DiskStatus::{Failed, Removed}`; none live → return `Err`, task not Done. | `advance_round_tests` mock: register error → task stays Completing, Done after retry succeeds; unit for (b): hash absent → no `UpdateChunkLocations` proposed; unit for (c): two indexes same hash → both rows added; unit for (d). | Tests fail before, pass after. |
| CR-3-5 | ✅ | MGR-5, MGR-6 | `scheduler.rs` `check_under_replicated` ~491/570, `schedule_replication`, `check_anchor_lag` ~867 | Use `task_blocks_requeue` in `check_under_replicated`'s dedup index and in `schedule_replication`. Source via `live_replica_tier`; target must satisfy `placement_cfg.has_room`. `check_anchor_lag`: honour `task_budget_available` (thread `created`/`live_tasks`), insert `(chunk, target)` into `existing_tasks` after each schedule. | scheduler unit: chunk with a recent Failed task → not re-queued; source on Failed disk not chosen; anchor-lag over N > budget chunks creates exactly budget tasks; two files same hash → one task. | Tests fail before, pass after. |
| CR-3-6 | ✅ | MGR-7 | `metadata.rs` heartbeat ~1250-1300, ~1385 | Capture `Err` from the `QuarantineNode` and `RemoveCorruptChunkReplicas` forwards; any failure → `accepted: false` in `HeartbeatResponse` (data node already re-enqueues on that, CO-3-7). Keep `AppendAudit` best-effort. | metadata unit (existing CO-3-7 test as template): forced forward failure → `accepted == false`. | Test fails before, passes after. |
| CR-3-7 | ✅ | CMN-5 | `crates/dtfs-common/src/types.rs` `AnchorPolicy::is_satisfied_by` ~494, `hash_ring.rs` ~255/282 | `eq_ignore_ascii_case` at the three sites. | types unit: `anchor "leli"` satisfied by `"LELI"`; ring test with mixed case. | Tests fail before, pass after. |
| CR-3-8 | 🟥 | MGR-10 (optional, do if time) | `proto/data.proto` `ListChunksResponse`, `dtfs-data/src/server.rs` list_chunks, `scheduler.rs` orphan reap | Add positional `chunk_disk_ids`; key `expected_by_node` on `(node, disk)`; pass `Some(disk_id)` to the reap delete. Empty field → legacy node-wide. | scheduler unit: warm leftover + cold live row on one node → reaped on warm disk only. | Test passes; old-manager compat path covered. |
| CR-3-9 | ✅ | — | `tests/release/cases/105-demote-refused-when-anchor-node-down.sh`, `106-legacy-tiering-skips-av-volume.sh` | Shell cases. | `bash -n` clean. | Present, unexecuted. |
| CR-3-10 | ✅ | — | `AGENTS.md` A.3.4, A.8 scheduler/tiering entries | Document liveness in planners, register_replica Result, per-index registration, budget in anchor-lag, case-insensitive DC. | — | Same commit. |

### Group 4 — Data-node durability (branch `bug/cr-4-data-durability`)

| ID | St | Finding | Files | Sketch | Tests | Done when |
|---|---|---|---|---|---|---|
| CR-4-1 | ⬜ | DATA-1 | `crates/dtfs-data/src/store.rs` `quarantine_chunk_on_disk` ~1502, scrub fault classification ~1226-1301 | `quarantine_chunk_on_disk`: `rename` `.dat` → `.dtfs/quarantine/<id>.dat.bad` (never `remove_file`). Before quarantining a `stat`/`meta_read`/`meta_parse`/missing-meta fault: hash the `.dat` incrementally; digest == id → rewrite `.meta` (tmp+rename, see CR-4-2), count healthy, do NOT push to `corrupt_chunk_ids`. Only a hash mismatch or unreadable `.dat` quarantines. Quarantine dir size is reported in the heartbeat disk usage (best effort; note if skipped). | store unit: (a) good `.dat` + 0-byte `.meta` → scrub repairs meta, no corrupt id; (b) bad `.dat` → quarantined, `.dat.bad` exists with original bytes; (c) `.dat` missing → corrupt id (unchanged). | Tests fail before, pass after. |
| CR-4-2 | ⬜ | DATA-2 | `store.rs` `commit_chunk_from_tmp_inner` meta write ~566, `recover_from_wal` PutChunk arm ~1007 | Write `.meta` to `tmp/<uuid>.meta.tmp`, `sync_all`, `rename` over final, `fsync_dir`. Recovery: WAL(Put) present + `.meta` unparseable → same as meta-missing (rebuild from `.dat` hash or roll back per CO-1-4b). | store unit: inject a 0-byte `.meta` + WAL → recovery repairs/rolls back; concurrent duplicate put while `get_chunk_meta` loops → never a parse error. | Tests fail before, pass after. |
| CR-4-3 | ⬜ | DATA-4 | `store.rs` `commit_chunk_from_tmp_inner` ~544-579 | On any error AFTER the `rename(tmp → final_dat)`: remove `final_dat`, `final_meta` (if any), `wal_path`, then return the error. | store unit: fail the meta write (inject via a read-only chunk dir or a size-limited tmpfs shim) → no `.dat`, no WAL left. | Test fails before, passes after. |
| CR-4-4 | ⬜ | DATA-3 | `store.rs` `ChunkStore` (new `chunk_locks`), `commit_chunk_from_tmp_inner`, `delete_chunk_idempotent_on_disk`, WAL naming ~538/~723, `recover_from_wal` | Striped per-chunk `tokio::sync::Mutex<()>` (256 stripes by first byte of the id) held across commit and delete. WAL files `<id>.put.wal.json` / `<id>.delete.wal.json`; recovery reads both plus legacy `<id>.wal.json`. | store unit: interleaved put/delete of the same id under a barrier → final state is either (dat+meta) or (neither), never one without the other; recovery test with both WAL kinds present. | Tests pass; property-style loop (≥ 200 iterations) never observes a torn state. |
| CR-4-5 | ⬜ | DATA-5 (server half) | `crates/dtfs-data/src/server.rs` `put_chunk` ~428-508 | After `finalize()`: `if !first.chunk_id.is_empty() && first.chunk_id != computed_hex → Status::data_loss("declared chunk_id does not match received bytes")`; tmp guard cleans up, nothing committed. | server unit: declared id mismatch → `data_loss`, no `.dat`; empty declared id → accepted (compat). | Tests fail before, pass after. |
| CR-4-6 | ⬜ | DATA-7, DATA-9 | `store.rs` ~585 counters, `server.rs` `verify_chunk` ~816 | Counters: check `final_dat && final_meta` exist before the rename, skip step 5 when already present (repoint the CO-5-5 test at `commit_chunk_from_tmp`). VerifyChunk: `if stat_size != meta.size → valid:false, error:"size mismatch"` before hashing. | store unit: duplicate commit leaves counters unchanged; server unit: `.dat` with trailing garbage → `valid:false`. | Tests fail before, pass after. |
| CR-4-7 | ⬜ | DATA-10 | `server.rs` `chunk_hex::decode` ~964 | Replace with `ChunkId::from_hex` (or `s.get(i..i+2)`); non-ASCII → `invalid_argument`. | unit: `"a\u{e9}a"` → Err, no panic. | Test fails (panics) before, passes after. |
| CR-4-8 | ⬜ | — | `tests/release/cases/107-scrub-repairs-torn-meta.sh` | Truncate a `.meta` on a data node, trigger scrub, assert chunk still readable and `.dat.bad` absent. | `bash -n` clean. | Present, unexecuted. |
| CR-4-9 | ⬜ | — | `AGENTS.md` A.3.3 #4, A.6 items 2-5 | Quarantine-by-rename, atomic `.meta`, post-rename cleanup, per-chunk lock + op-keyed WAL. | — | Same commit. |

### Group 5 — Auth + control-plane hardening (branch `bug/cr-5-control-plane`)

| ID | St | Finding | Files | Sketch | Tests | Done when |
|---|---|---|---|---|---|---|
| CR-5-1 | ⬜ | PROTO-1 | `crates/dtfs-manager/src/metadata.rs` `heartbeat` ~1061, `report_chunk_status` ~1393; `crates/dtfs-data/src/main.rs` heartbeat loop ~458/602/636 | Gate both handlers with `require_mgr_rpc_auth(secret, "Heartbeat"|"ReportChunkStatus", &req.node_id, request.metadata())` BEFORE `into_inner()`. Data node stamps via `dtfs_common::crypto::stamp_mgr_rpc_auth` with `[cluster].shared_secret` (data config already has it). Also stamp `RegisterNode`? No — it carries its own `shared_secret` field; leave. | metadata unit (LIS-56 tests as template): secret set + no header → `PermissionDenied`; valid header → ok; empty secret → ok. | Tests fail before, pass after; AGENTS.md A.3.3 #8 lists both RPCs + "deploy manager+data together". |
| CR-5-2 | ⬜ | MGR-11 | `metadata.rs` `forward_client_write` ~1479 | Allow-list: `UpdateNodeUsage, UpdateNodeDiskUsage, UpdateNodeStatus, AppendAudit, QuarantineNode, RemoveCorruptChunkReplicas, ReportScrubResult, ReportChunkReads` + any variant `grep -n "raft_write_or_forward\|write_via_leader"` proves a follower proposes today (list them in the commit). Anything else → `permission_denied` + `auth.denied` audit. | unit: `UpdateConfig` via forward → denied; `UpdateNodeUsage` → applied. | Tests fail before, pass after. |
| CR-5-3 | ⬜ | MGR-12 | `metadata.rs` `write_via_leader` ~578 | Inspect `ForwardWriteResponse.success`; false → `Err(resp.error)`. | unit with mock leader returning `success:false`. | Test fails before, passes after. |
| CR-5-4 | ⬜ | DATA-6 | `crates/dtfs-data/src/main.rs` control RPCs, `crates/dtfs-common/src/tls.rs` `endpoint()` ~182 | `req.set_timeout(Duration::from_secs(10))` on heartbeat / report_chunk_status / register_node. `TlsContext::endpoint`: `.http2_keep_alive_interval(10s).keep_alive_timeout(5s).keep_alive_while_idle(true)`. | tls unit: endpoint builder sets keepalive (inspect via `Endpoint` debug or a builder-recording shim); data main: a mock manager that accepts then never answers → heartbeat loop advances within 15 s. | Tests pass. |
| CR-5-5 | ⬜ | DATA-11 | `crates/dtfs-data/src/server.rs` `record_cache_hit` ~940; `crates/dtfs-client/src/client.rs` | Gate with `require_data_rpc_auth(secret, "RecordCacheHit", &req.chunk_id, md)`; client stamps. | server unit: secret set + no header → denied. | Test fails before, passes after. |
| CR-5-6 | ⬜ | DATA-8 | `dtfs-data/src/main.rs` ~589-634 | Send `ChunkStatusReport` in batches of 1000 ids; re-enqueue only the unsent remainder + the failed batch. | unit on the batching helper: 2500 ids → 3 calls; failure on call 2 → 1500 re-enqueued. | Test passes. |
| CR-5-7 | ⬜ | CMN-4 | `crates/dtfs-common/src/crypto.rs` `hex_decode` ~125 | `if !s.is_ascii() { return None; }`. | unit: `"€€"` → None, no panic. | Test fails (panics) before, passes after. |
| CR-5-8 | ⬜ | — | `tests/release/cases/108-heartbeat-requires-mgr-rpc-auth.sh` | grpcurl (or dtfs-cli debug) Heartbeat without headers → PermissionDenied when secret set. | `bash -n` clean. | Present, unexecuted. |
| CR-5-9 | ⬜ | — | `AGENTS.md` A.3.3 #7/#8, A.8 metadata entry | Heartbeat/ReportChunkStatus/RecordCacheHit gated; forward allow-list; control-plane deadlines + keepalive. | — | Same commit. |

### Group 6 — Config/proto sync + client perf (branch `bug/cr-6-config-client-perf`)

| ID | St | Finding | Files | Sketch | Tests | Done when |
|---|---|---|---|---|---|---|
| CR-6-1 | ⬜ | CMN-1, CMN-8 | `crates/dtfs-common/src/config.rs` (`PlacementConfig::validate` new), `dtfs-manager/src/main.rs`, `dtfs-data/src/main.rs`, `dtfs-fuse/src/main.rs` | `PlacementConfig::validate` rejects `!(0.0..=100.0).contains(&min_free_pct)`. All three mains call `config.memory.validate()` (and manager `config.placement.validate()`) right after load; error → exit non-zero with the message. Docs: note the new startup rejection in `docs/src/configuration/{manager,data-node,fuse-client}.md` memory rows. | config unit: `warn 95 / shed 90` → Err; `min_free_pct 150` → Err. | Tests fail before, pass after; `example_*_toml_parses` green. |
| CR-6-2 | ⬜ | CMN-2 | `crates/dtfs-manager/src/metadata.rs` `resolve_ack_floor` ~816 | Clamp: `min(floor, effective_mandatory_replicas of the tier the chunks were placed on)`; when clamped, `warn!` once per volume (dedup by volume id) and audit `write_safety.floor_clamped`. Docs row manager.md:216: "clamped to the effective RF". | metadata unit (`resolve_ack_floor` tests at ~3756 as template): volume floor 2, cold RF 1, cold placement → floor 1. | Test fails before, passes after. |
| CR-6-3 | ⬜ | PROTO-2 | `proto/manager.proto` `ScrubConfigOverride`, `metadata.rs` ~1105, `dtfs-data/src/scrub.rs` ~101 | Add `quick_interval_secs = 9`, `deep_interval_secs = 10`; manager fills both (secs exact, hours as before); data node prefers non-zero secs; manager `warn!` when hour rounding changes the value and the peer is old (cannot know → log once at startup instead: "sub-hour scrub cadence requires data nodes ≥ this build"). | data scrub unit: override with `quick_interval_secs=600` → 600 s cadence; secs=0 → hours×3600. | Tests fail before, pass after. |
| CR-6-4 | ⬜ | FUSE-6 | `proto/manager.proto` `GetFileLocationsResponse`, `metadata.rs` `get_file_locations` ~940, `allocate_chunks` ~1822, `dtfs-fuse/src/filesystem.rs` `fetch_file_locations` ~1692 | Add `volume_id` to the response; FUSE stores it in `FileLocations.volume_id` (`""` → keep current value, not `"default"`). Manager `allocate_chunks`: when `sm.files[req.path]` exists, use ITS `volume_id` for placement and ignore the request value (log at debug when they differ). | metadata unit: file in volume "strict", request says "default" → placement uses strict RF/anchor; fuse unit: refresh keeps volume id. | Tests fail before, pass after. |
| CR-6-5 | ⬜ | FUSE-5 | `dtfs-fuse/src/filesystem.rs` `metadata_cache` ~281/449 | Bound the cache: `max_entries` (default 4096) with LRU eviction on insert (simple: `VecDeque` of keys + DashMap, or the `lru` crate if already a dependency — check Cargo.lock; do NOT add a new dependency without noting it). Expired entries also swept every `metadata_ttl` by the existing readahead pressure ladder task or a tiny interval task. | unit: insert 5000 paths → `len() <= 4096`; expired entry gone after sweep. | Test fails before, passes after. |
| CR-6-6 | ⬜ | FUSE-8, FUSE-9, FUSE-10 | `filesystem.rs` `read()` ~725-980, `readahead.rs` | (FUSE-8) notify `record_cache_hit` once per `(fh, chunk_id)` transition (store `last_notified_chunk` in `OpenFile`), in-flight bounded by a `Semaphore(8)`. (FUSE-9) foreground miss registers itself in a per-chunk in-flight `DashMap<String, watch::Receiver>` so concurrent readers of the same chunk await one fetch. (FUSE-10) call `readahead.on_chunk_read` on every chunk-index transition per fh (hit or miss); `pending`/cache dedup unchanged. | fuse unit: 32 sequential 128 KB reads of one cached chunk → exactly 1 notify; two concurrent reads of an uncached chunk with a counting mock client → 1 `GetChunk`; sequential hits keep `pending.len()` at the window depth. | Tests fail before, pass after; existing WI-05/B2 tests unchanged. |
| CR-6-7 | ⬜ | — | `tests/release/cases/109-non-default-volume-placement-after-reopen.sh` | Create file in a non-default volume, close, reopen, append, assert every chunk's replicas match that volume's RF/anchor via `/api/v1/files/inspect`. | `bash -n` clean. | Present, unexecuted. |
| CR-6-8 | ⬜ | — | `AGENTS.md` A.2 read path, A.3.2, A.8 fuse entries; `deploy/examples/*.toml` + `docs/src/configuration/*` for any new key | Document volume_id on the wire, metadata cache bound, single-flight reads, per-chunk cache-hit notify, config validation at startup. | `cargo test -p dtfs-common` (`example_*_toml_parses`). | Same commit. |

## Cross-cutting invariants (every group)

1. No `Vec<u8>` of a whole chunk on any hot path that did not already have one (A.3.2).
2. Every new persisted field: `#[serde(default)]` + snapshot-compat test (A.3.1).
3. Every replica-removing path consults liveness (`live_replica_tier`) AND anchor (A.3.4).
4. Byte delete never before the Raft remove (A.6 #6); no new delete-by-id path without a live-reference re-check (A.6 #4).
5. Old manager ↔ new client and new manager ↔ old client must keep working for every proto change in this plan.
6. `cargo test --release --workspace` green on WSL before each PR.

## Delegation protocol (lead ↔ implementer)

1. Lead creates branch `bug/cr-<g>-...` from `master`, hands the implementer this file + the report section for the group.
2. Implementer works task by task (§ B.3): mark 🟦, failing test, fix, passing test, crate suite, commit (§ B.4 format with `Plan:` + `Co-Authored-By:` trailers, task marked ✅ in the same commit with a verification-log entry below). Never `--no-verify`, never amend.
3. Implementer finishes with workspace suite green, pushes, opens a PR titled `bug: <group summary ≤72 chars>` labelled `bug`, and reports: tasks done, tasks 🟥 with reason, test counts before/after per crate, deviations.
4. Lead reviews the diff against each finding's "Done when", runs the crate suites once more, merges (merge commit), and starts the next group from the updated `master`.
5. After Group 6: lead re-reads every HIGH/MEDIUM finding against `master` and records OPEN/FIXED in the report's summary table; then deletes this plan file (§ B.2 retention).

## Deviations (Group 3)

- CR-3-8 (MGR-10, optional): deferred — proto change (`ListChunksResponse`
  positional `chunk_disk_ids`) + data-node fill + reaper re-keying is a
  multi-file, cross-crate change (proto/dtfs-data/dtfs-manager) explicitly
  marked "optional, do if time" in the task brief; skipped to land the
  required CR-3-1..3-7, 3-9, 3-10 tasks. MGR-10 stays open in the report.

## Verification log

- **Baseline** 2026-09-12, `master` `1a946ec`, Docker `dtfs-buildenv:latest`,
  `cargo test --release --workspace`: all green — dtfs-manager 346,
  dtfs-data 98, dtfs-raft 88, dtfs-fuse 76, dtfs-common 104, dtfs-client 31,
  dtfs-cli 12 passed, 0 failed. Every task entry below states its crate's
  count after the change (must be >= baseline + new tests).

## Deviations

- CR-1-1: `ChunkBuf`'s coverage flag is named `is_complete`/tracked via the
  `needs_seed` field on `ChunkSegment` (not a `seeded: bool` on `ChunkBuf`
  as the data-model table sketched) — the consumer (filesystem.rs, CR-1-2)
  only ever needs the derived per-segment signal, not per-chunk mutable
  state, so a stored bool would just be recomputed from `dirty` anyway.
  `complete_chunks_available(dirty_offset, len, chunk_size)` is kept
  unchanged (still directly unit-tested) but is no longer called by the
  new sparse `flush()`/`extract_complete_chunks`, which count complete
  `ChunkBuf` entries directly — noted as the "one-line justification" for
  an unchanged-but-now-orphaned pure helper per the task instructions.
  Added `WriteBuffer::dirty_bytes()` and `chunk_is_fully_buffered()` (not
  in the original method list) because CR-1-2/CR-1-3 need them; both are
  additive, no existing signature changed.

## Verification log

- **CR-1-1** 2026-09-12, Docker `dtfs-buildenv:latest`,
  `cargo test -p dtfs-fuse --release`: 76 -> 78 passed (2 new:
  `cr_1_1_rewrite_offset_zero_marks_only_that_chunk_needs_seed`,
  `cr_1_1_scattered_writes_hold_only_touched_chunks`), 0 failed. Rewrote
  `FileWriteBuffer` from a contiguous `Vec<u8>` to `BTreeMap<u32, ChunkBuf>`
  (`crates/dtfs-fuse/src/writebuf.rs`); every existing test passed
  unchanged (no semantic rewrite needed) once `write()` splits at chunk
  boundaries before merging into the per-chunk buffer.

- **CR-1-2** 2026-09-12, Docker `dtfs-buildenv:latest`,
  `cargo test -p dtfs-fuse --release`: 78 -> 82 passed (4 new, module
  `cr_1_2_seed_tests`: `offset_zero_is_flagged_for_seeding`,
  `straddling_write_flags_both_chunks`, `fully_buffered_chunk_is_not_flagged`,
  `only_incomplete_chunks_are_flagged`), 0 failed. `write()`'s RMW seed
  block now calls the extracted `chunks_needing_seed()` helper over EVERY
  chunk index the write touches (dropped the `effective_offset > 0` gate)
  and a seed-read failure propagates via `?` (EIO) instead of `warn!` +
  continue. The O_APPEND pre-seed in `open_with_append` now runs AFTER the
  write lease is granted and resolves the tail chunk via
  `chunk_at(max_idx)` (REV-2-1 index resolution) instead of positional
  `chunks[len-1]`.
  Deviation: (c) "seed error -> write() returns Err" from the task's test
  list is enforced structurally by the `?` on `read_chunk(...)` in
  `write()` (a compile-time control-flow guarantee — any `Err` from the
  seed read now aborts the write before the caller's bytes are merged),
  but is not exercised by a unit test: dtfs-fuse has no `DtfsClient` mock/
  trait double, so a real network-error-path test would need either a
  mock gRPC data-node server or a client trait abstraction, both out of
  scope for "smallest fix" here. (a)/(b) are covered at the exact decision
  boundary (`chunks_needing_seed`), which is the only new logic `write()`
  added — the actual seed I/O (`read_chunk` + `write_buffer.write`) is
  unchanged, already-tested plumbing.

- **CR-1-3** 2026-09-12, Docker `dtfs-buildenv:latest`,
  `cargo test -p dtfs-fuse --release`: 82 -> 85 passed (3 new, module
  `cr_1_3_flush_release_gating_tests`), 0 failed. `flush()` now returns
  `Ok(())` immediately for a non-writable fh, before touching the
  path-keyed write buffer or the lease. `release()`'s buffer-drop is now
  gated on `file.flags.is_writable()` AND no other writable fh remaining
  open on the same path (extracted as the pure, directly-tested
  `other_writer_open()` helper, since standing up a live `DtfsFilesystem`/
  `DtfsClient` in a unit test isn't practical here — no mock/trait double
  exists for the client, and `FuseConfig` has no `Default` impl to build
  one cheaply).

- **CR-1-4** 2026-09-12, Docker `dtfs-buildenv:latest`,
  `cargo test -p dtfs-fuse --release`: 85 -> 87 passed (2 new, module
  `cr_1_4_ghost_unlink_tests`), 0 failed. Added `OpenFile.committed_any`,
  set true on every `OpenFile` sharing a path the moment
  `flush_segments_to_cluster` first succeeds for it. `flush()` now
  UNCONDITIONALLY calls `restore_segments` on a cluster-write failure
  (the old `was_fresh` branch that skipped the restore and unlinked
  instead is gone from `flush()` entirely). The ghost-unlink moved to
  `release()`, gated by the extracted `should_ghost_unlink(flush_failed,
  committed_any)` and firing only when nothing was EVER committed for the
  path. Deviation: the full end-to-end scenario in the task's test
  column (create, batch-commit 2 chunks, write a tail, force a flush
  error, assert not-deleted + tail retried) needs a live
  `flush_segments_to_cluster` failure, which needs a `DtfsClient`/network
  mock this crate doesn't have (same gap noted under CR-1-2/CR-1-3);
  covered instead at the exact decision boundary the fix introduces
  (`should_ghost_unlink`), which is the only new control-flow `release()`
  gained.

- **CR-1-5** 2026-09-12, Docker `dtfs-buildenv:latest`,
  `cargo test -p dtfs-fuse --release`: 87 -> 90 passed (3 new, module
  `cr_1_5_chunk_id_tests`), 0 failed. `flush_segments_to_cluster` now
  hashes each segment once (`blake3_hex`, extracted for testability) and
  sends the hex digest as the declared `chunk_id` on every
  `write_chunk_bytes` call (previously always `""`); a reply whose
  `chunk_id` does not match is treated as a failed replica (excluded from
  `successes`, contributes to `last_err`) so a mismatched write is never
  committed as a location. No dtfs-data change in this group (Group 4
  does the server-side compare per the plan). Deviation: same client-mock
  gap as CR-1-2/3/4 — the task's "mock data node returning a different
  id" test is covered at the hashing/comparison boundary
  (`blake3_hex` + the `!=` check that feeds the mismatch branch), not via
  a live RPC round trip.

- **CR-1-6** 2026-09-12, Docker `dtfs-buildenv:latest`,
  `cargo test -p dtfs-fuse --release`: 90 -> 91 passed (1 new,
  `cr_1_6_transient_manager_error_is_not_enoent`), 0 failed. Routed
  `readdir`, `readdirplus`, `mkdir`, `unlink`, `rmdir`, `create`,
  `rename`, `symlink`, and `link` through `cluster_err_to_errno` (was a
  bare `Errno::from(libc::ENOENT)`/`EIO)` in each `map_err`/`Err(e) =>`
  arm). `grep -c "Errno::from(libc::ENOENT)"` shows 6 remaining hits, all
  inside the helper itself, `child_path`/`path_for`'s unrelated
  `ok_or_else`, and tests — zero in a `map_err` closure outside the
  helper. No fuse3 test harness exists to drive `readdir`/`readdirplus`
  directly (pre-existing comment on `fuse_readdir_regression`), so the
  new test locks the mapping function both now call instead.

- **CR-1-7** 2026-09-12. Added
  `tests/release/cases/100-fuse-rewrite-offset-zero.sh` (patches offset 0
  and the chunk-0/chunk-1 boundary of an already-flushed file, cmp against
  a locally-patched reference) and
  `tests/release/cases/101-fuse-reader-close-does-not-drain-writer.sh`
  (paces a background streaming write, repeatedly open-read-closes the
  same path read-only while it's in flight, then checks the writer still
  finishes with the byte-correct content). Both modeled on
  `96-write-batch-flush.sh` / `85-unaligned-write-integrity.sh` /
  `90-lease-release-spares-live-writer.sh`; `trap case_cleanup EXIT`,
  last line `pass` (-> `STATUS: PASS`), not in `QUICK_CASES`.
  `bash -n` clean on both (verified: `bash -n
  tests/release/cases/100-fuse-rewrite-offset-zero.sh` and `...101-...sh`
  -> no output, exit 0). NOT executed — no lab, per the plan's Build/verify
  environment section.

- **CR-1-8** 2026-09-12. Updated `AGENTS.md` A.8's `filesystem.rs` entry
  (added CR-1-1..CR-1-5 paragraphs: per-chunk `ChunkBuf` map, RMW seed
  scope + O_APPEND ordering, flush/release writable-fh gating,
  `committed_any` + relocated ghost-unlink, end-to-end BLAKE3 `chunk_id`)
  and the `fuse_adapter.rs` entry (CR-1-6: `readdir`/`readdirplus`/
  `mkdir`/`unlink`/`rmdir`/`create`/`rename`/`symlink`/`link` now route
  through `cluster_err_to_errno`). Own commit per the task instructions.

- **R1 (PR #105 review, FUSE-1 memory half)** 2026-09-12,
  `cargo test -p dtfs-fuse --release`: 91 -> 95 passed (4 new:
  `writebuf::tests::r1_flush_complete_ignores_batch_threshold`,
  `filesystem::r1_dirty_cap_tests::under_cap_is_not_exceeded`,
  `filesystem::r1_dirty_cap_tests::meeting_or_crossing_cap_is_exceeded`,
  `filesystem::r1_dirty_cap_tests::saturates_instead_of_overflowing`),
  0 failed. Added `WriteBuffer::flush_complete()` (drains every complete
  chunk immediately, ignoring `flush_batch_chunks`) and wired
  `DIRTY_BYTES_CAP` into `write()`'s F-022 sparse-hole fill loop (checked
  after each 64 MiB slice) and once more before merging the caller's own
  bytes, via the extracted pure `dirty_cap_exceeded(dirty, incoming)`
  predicate. On a cap hit, `flush_complete()` + `flush_segments_to_cluster`
  runs immediately; a failure there `restore_segments`s and returns Err
  (same CO-1-8(a) contract as the batch flush path). AGENTS.md A.3.2
  documents the cap next to the WI-01/H1 sentence.

- **R2 (PR #105 review, FUSE-7(c) still reachable)** 2026-09-12,
  `cargo test -p dtfs-fuse --release`: 95 -> 98 passed (3 new,
  `filesystem::r2_segments_committable_tests::*`), 0 failed.
  `flush_segments_to_cluster` now refuses (before `allocate_chunks_with_hint`)
  to commit any segment with `offset_in_chunk > 0` via the extracted pure
  `segments_committable()` helper, returning an `Err` naming the
  offending chunk; every caller already `restore_segments`s on `Err`, so
  the bytes survive for a retry instead of being stored shifted. Also
  added a `warn!` at each of the three `continue` sites in `write()`'s
  RMW seed loop (no cached `locations_snapshot`, no `chunk_at(idx)`
  entry, empty `node_addresses`) noting the seed was skipped and that the
  commit-time guard above is the backstop — left as a `warn!` rather than
  a hard error there because `write()` cannot cheaply determine whether
  the chunk genuinely doesn't exist yet (legitimate) vs. the snapshot
  being stale (needs the guard), so the decision is deferred to
  `segments_committable()` at commit time, per the review note.

- **CR-2-1** 2026-09-12, Docker `dtfs-buildenv:latest`,
  `cargo test -p dtfs-raft --release`: 88 -> 91 passed (3 new:
  `raft_1_reregister_keeps_quarantined`, `raft_1_reregister_keeps_maintenance`,
  `raft_1_reregister_healthy_stays_healthy`), 0 failed. `RegisterNode`'s apply
  now reads `prior = sm.nodes.get(node_id)` before inserting and carries
  `quarantine`/`Quarantined` or `Maintenance`/`Draining` forward onto the
  incoming `info` before it overwrites `sm.nodes`; `node_info_material_eq`
  already excludes health/quarantine so audit dedup is unaffected. Emits an
  extra `RegisterNode: <id> (sticky state preserved: <state>)` audit row
  when carry-forward fires.

- **CR-2-2** 2026-09-12, Docker `dtfs-buildenv:latest`,
  `cargo test -p dtfs-raft --release`: 91 -> 95 passed (4 new:
  `raft_2_rename_directory_moves_children`,
  `raft_2_rename_synthetic_directory_moves_children`,
  `raft_2_rename_onto_nonempty_directory_is_refused`,
  `raft_2_rename_lease_follows_child`), 0 failed. `RenameFile`'s apply now
  collects every `from/...` child key (BTreeMap range, collected before any
  mutation) and re-keys each to `to/...`, moving its `posix_locks`/
  `write_leases` entry too; a synthetic-dir source (no exact key, but
  children exist) now succeeds instead of erroring; refuses when `to`
  already has children (real or synthetic) or is inside `from`'s own
  subtree.

- **CR-2-3** 2026-09-12, Docker `dtfs-buildenv:latest`,
  `cargo test -p dtfs-raft --release`: 95 -> 97 passed (2 new:
  `raft_3_truncate_zero_then_commit_grows_logical_size`,
  `raft_3_sparse_grow_size_is_preserved_over_a_smaller_chunk_extent`),
  0 failed. `UpdateChunkLocations`'s apply now computes
  `extent = max(chunk_index*chunk_size + size_bytes)` over the merged
  chunks and grows (never shrinks) an already-set `logical_size` to cover
  it; `None` stays `None`. Deleted the dead `|| *new_size == 0 && false`
  branch and the no-op `.saturating_sub(0)` in `SetFileSize`.

- **CR-2-4** 2026-09-12, Docker `dtfs-buildenv:latest`. dtfs-raft
  `cargo test -p dtfs-raft --release`: 97 -> 99 passed (2 new:
  `raft_7_delete_file_removes_lease_and_locks`,
  `raft_7_delete_nonempty_directory_is_refused`); dtfs-fuse
  `cargo test -p dtfs-fuse --release`: 98 -> 99 passed (1 new:
  `cr_2_4_failed_precondition_maps_to_enotempty`); dtfs-manager
  `cargo test -p dtfs-manager --release`: 346 passed (no regression, no
  new unit -- delete_file is a gRPC handler with no test scaffold, same
  gap as the CR-1 group). 0 failed across all three. `DeleteFile`'s
  apply now drops `write_leases`/`posix_locks` for the path and refuses
  (`Error`) when a child key exists under `path/`. `cluster_err_to_errno`
  maps gRPC `FailedPrecondition` to `ENOTEMPTY`. `metadata.rs::delete_file`
  now checks the Raft response BEFORE computing/spawning the chunk
  fan-out (previously the fan-out was scheduled unconditionally even on a
  refusal) and maps `ClientResponse::Error` to `Status::failed_precondition`.

- **CR-2-6** 2026-09-12, Docker `dtfs-buildenv:latest`,
  `cargo test -p dtfs-raft --release`: 99 -> 101 passed (2 new:
  `raft_8_update_config_version_always_current_plus_one`,
  `raft_9_non_finite_duration_is_clamped_to_zero`), 0 failed. `UpdateConfig`
  now always stores `sm.cluster_config.version + 1` (ignoring the wire
  `version`, CAS check unchanged); `ReportScrubResult` clamps a non-finite
  `duration_secs` to `0.0` before it reaches `ScrubRecord`. Updated the
  pre-existing `co_5_10_update_config_legacy_no_expected_version_applies`
  (asserted the wire value 7 was stored verbatim for a legacy/no-CAS
  entry -- exactly the regression path this fix closes) to expect
  `current + 1 = 5` instead.

- **CR-2-5** 2026-09-12, Docker `dtfs-buildenv:latest`. dtfs-raft
  `cargo test -p dtfs-raft --release`: 101 -> 104 passed (3 new:
  `raft_4_rename_file_apply_is_deterministic_across_replicas`,
  `raft_4_legacy_rename_file_without_now_ms_falls_back_to_log_index`,
  `raft_4_update_chunk_locations_apply_is_deterministic_across_replicas`);
  dtfs-manager `cargo test -p dtfs-manager --release`: 346 passed
  (unchanged, no unit test scaffold touches the proposer call sites).
  0 failed. Added `#[serde(default)] now_ms: u64` to `UpdateChunkLocations`,
  `RemoveCorruptChunkReplicas`, `RenameFile`, `SetFileSize`,
  `CreateSymlink`, `CreateLink`, `RemoveChunkLocation`. The apply for each
  now computes `eff_now_ms = if now_ms == 0 { log_id.index } else { now_ms }`
  (CO-3-1 fallback) and routes both `updated_at_ms` and that arm's own
  audit row through a new `audit_at(summary, ts)` closure (the shared
  `audit` closure still captures the top-of-apply wall clock for every
  arm NOT in this list). `apply_remove_chunk_location` (factored out of
  the `RemoveChunkLocation` arm) took an explicit `now_ms: u64` parameter
  in place of its own inline `SystemTime::now()`. Every proposer
  (`dtfs-manager/src/metadata.rs` via a new `stamp_now_ms()` helper;
  `scheduler.rs`/`tiering.rs` via the pre-existing `now_millis()`) now
  stamps `now_ms` instead of leaving it at the serde default. Left the
  top-of-apply `SystemTime::now()` in place (see Deviations) since arms
  outside the seven-variant scope (DeleteFile, RegisterNode,
  QuarantineNode, ...) still call the wall-clock `audit` closure;
  `grep -n "SystemTime::now" state_machine.rs` shows it only at the
  top-of-apply definition, the read-only `lookup_path` synthetic-dir
  helper (not persisted, out of scope), and in comments.

- **CR-2-7** 2026-09-12. Added three release cases, `bash -n` clean on
  all three, none executed (no lab, per the Build/verify environment
  section): `tests/release/cases/102-rename-directory-moves-children.sh`
  (mkdir + nested child, `mv` through the FUSE mount, asserts children
  and content land under the new path and the old path is fully gone,
  CR-2-2/RAFT-2); `103-truncate-rewrite-reports-size.sh` (write 8 MiB,
  shell `>` truncate to 0, rewrite 3 MiB, asserts `stat` reports the
  rewritten size rather than staying pinned at 0, CR-2-3/RAFT-3);
  `104-quarantined-node-restart-stays-quarantined.sh` (corrupt a live
  replica, read it through FUSE so verify_on_read's heartbeat
  IntegrityFault auto-quarantines the node, `systemctl restart dtfs-data`,
  asserts health is still `quarantined` after the re-register rather than
  reverted to `healthy`, CR-2-1/RAFT-1). Modeled on `20-quarantine-flow.sh`
  and `22-register-audit-idempotent.sh` for the corrupt/restart idiom and
  `100-fuse-rewrite-offset-zero.sh` for the FUSE probe shape; none added
  to `QUICK_CASES`.

- **CR-3-7** 2026-09-12, Docker `dtfs-buildenv:latest`,
  `cargo test -p dtfs-common --release`: 104 -> 106 passed (2 new:
  `types::bug_av_types_tests::cr_3_7_anchor_policy_dc_compare_is_case_insensitive`,
  `hash_ring::tests::cr_3_7_anchor_dc_swap_is_case_insensitive`), 0 failed.
  `AnchorPolicy::is_satisfied_by` and the two anchor-DC compares in
  `hash_ring.rs` (`already_in_anchor`'s zip and the swap-candidate scan)
  now use `eq_ignore_ascii_case` instead of `==`/`!=`, matching the
  existing `SpillPolicy`/`read_location_priority` convention.

- **CR-3-1** 2026-09-12, Docker `dtfs-buildenv:latest`,
  `cargo test -p dtfs-manager --release`: 346 -> 348 passed (2 new:
  `scheduler::bug_av_flush_cache_tests::cr_3_1_flush_refused_when_mandatory_replica_is_on_dead_node`,
  `tiering::audit_co_tests::cr_3_1_demote_refused_when_only_surviving_mandatory_replica_is_dead`),
  0 failed. `FlushNodeSummary.disk_tiers` is now `Vec<(u32, DiskTier,
  bool)>` — the bool is `live_replica_tier(node, disk_id).is_some()`
  resolved once in `FlushSnapshot::from_sm`; `plan_flush_cache`'s
  `mandatory_survives` now requires the surviving entry to also be
  live (`entry_live`). `tiering::TickPlacement` gained a `live: bool`
  field, resolved the same way in `build_av_cache_snapshot`;
  `plan_chunk_demotions`'s `mandatory_still_present` now requires the
  other placement to be `live`. Test helpers `node_summary` (scheduler)
  and `placement` (tiering) default to live; added
  `node_summary_with_liveness` / `dead_placement` for the dead-replica
  cases. `demote_preserves_anchor` (the separate anchor check) is
  untouched — MGR-2's finding and fix scope are the two named
  "mandatory" functions only.

- **CR-3-2** 2026-09-12, Docker `dtfs-buildenv:latest`,
  `cargo test -p dtfs-raft --release`: 104 -> 105 passed (1 new:
  `state_machine::posix_path_regression::cr_3_2_last_live_replica_guard_is_independent_of_anchor`),
  0 failed. `apply_remove_chunk_location` now refuses (`Error("...would
  remove the last live replica...")`) when the row being removed is
  live and no other live replica of the same `(path, chunk_index)`
  survives, regardless of whether `enforce_anchor` is set. The check
  runs AFTER the existing anchor survivor scan (not before) so a
  volume with an anchor policy keeps the more specific "would strand
  anchor" message when both checks would refuse the same removal —
  needed to keep `rev_3_7_a_dead_replica_cannot_satisfy_the_anchor`'s
  message assertions passing unchanged (it failed on first pass with
  the check ordered before the anchor block; reordering, not the test,
  was the fix, since the anchor-specific message is more informative
  for an anchor volume).

- **CR-3-3** 2026-09-12, Docker `dtfs-buildenv:latest`,
  `cargo test -p dtfs-manager --release`: 348 -> 352 passed (4 new:
  `metadata::tests::cr_3_3_unknown_chunk_resolves_to_none`,
  `metadata::tests::cr_3_3_known_chunk_resolves_real_path_and_tier`,
  `metadata::tests::cr_3_3_dead_replica_falls_back_to_hot_tier`,
  `tiering::audit_co_tests::cr_3_3_legacy_path_should_skip_truth_table`),
  0 failed. The `ReportChunkReads` intercept in
  `forward_client_write` now resolves the real `file_path` (via
  `scheduler::files_referencing_chunk`) and the replica's real tier on
  `node_id` (via `scheduler::live_replica_tier`) through a new pure
  `metadata::resolve_chunk_read_context` helper, instead of always
  passing `""` / `DiskTier::Hot`; an unknown chunk is skipped entirely.
  `tiering::evaluate` now runs the CO-4-6 AV-volume skip through a new
  pure `legacy_path_should_skip(file_path, path_to_volume,
  av_cache_volumes)` helper that also skips when `file_path` resolves
  to no known file (closing the case an unresolvable path used to fall
  through to the legacy path). No test stands up a full `TieringEngine`
  (no scaffold exists in this crate for that, same gap noted in Group 1
  for `DtfsClient`); both fixes are covered at the exact new decision
  boundary each introduced, which is what actually changed.

- **CR-3-4** 2026-09-12, Docker `dtfs-buildenv:latest`,
  `cargo test -p dtfs-manager --release`: 352 -> 359 passed (7 new:
  `scheduler::advance_round_tests::cr_3_4_register_failure_keeps_task_completing_then_succeeds`,
  `scheduler::advance_round_tests::cr_3_4_register_failure_fails_task_at_attempt_cap`,
  `scheduler::bug_ch_live_replica_tests::dead_disk_of_the_tier_is_not_resolved`,
  `scheduler::cr_3_4_plan_replica_registration_tests::{unresolvable_hash_yields_no_writes,
  duplicate_hash_within_one_file_registers_every_index,
  already_present_replica_is_not_duplicated,
  shared_hash_registers_against_every_file}`), 0 failed.
  (a) `MigrationOps::register_replica` now returns `Result<(), String>`;
  the Completing dispatch routes `Err` through the existing
  `record_copy_failure` helper (bumps `attempts`, stays Completing,
  Failed+alert only at `MAX_MIGRATION_ATTEMPTS` — message text still
  says "Copying retry queued", a cosmetic mismatch noted rather than
  forking the helper). (b)/(c) `register_replica_location` collapsed to
  ONE SM read lock and delegates to a new pure
  `plan_replica_registration(files, chunk_hash, target_node,
  resolved_disk_id)`, which registers the new replica at EVERY
  chunk_index in EVERY referencing file that carries the hash (was:
  first file, first index, under two separately-acquired locks) and
  returns no writes at all when the hash is unresolvable (was:
  `unwrap_or(0)`, silently mis-registering at index 0). (d)
  `resolve_disk_id_for_tier` now returns `Option<u32>`, filtering
  Failed/Removed disks; `None` propagates as an `Err` from
  `register_replica_location` before any write is attempted. Existing
  callers/tests of `resolve_disk_id_for_tier` updated for the new
  `Option` return.

- **CR-3-5** 2026-09-12, Docker `dtfs-buildenv:latest`,
  `cargo test -p dtfs-manager --release`: 359 -> 364 passed (5 new:
  `scheduler::bug_ch_live_replica_tests::{pick_live_source_skips_dead_disk,
  pick_live_source_none_when_all_dead, pick_backfill_target_skips_full_disk,
  pick_backfill_target_respects_exclude}`,
  `scheduler::bug_av_anchor_lag_tests::cr_3_5_anchor_lag_dedup_within_one_pass`),
  0 failed. `check_under_replicated`'s dedup index and
  `schedule_replication`'s `already_scheduled` check now both use
  `task_blocks_requeue` (covers the pre-existing
  `task_blocks_requeue(Failed) == true` unit test) instead of a bespoke
  `!= Done && != Failed`, so a lingering Failed task's retention window
  also blocks an immediate re-queue. Source selection now goes through
  a new pure `pick_live_source` (first entry whose `live_replica_tier`
  resolves, not "any row on a healthy node"); target selection through
  a new pure `pick_backfill_target`, requiring `placement_cfg.has_room`
  on the candidate disk — `TaskScheduler` gained a `placement_cfg`
  field (constructor parameter, wired from `config.placement.clone()`
  in `main.rs`; the sole call site). `check_anchor_lag` now threads a
  `created`/`live_tasks` pair through `task_budget_available` (same
  bound as `check_under_replicated`) and inserts the just-scheduled
  `(chunk_id, target_node)` into `existing_tasks` immediately after
  each schedule, not only once at the top of the pass — proven by the
  new `cr_3_5_anchor_lag_dedup_within_one_pass` test, which shows a
  second file sharing the hash is refused once the set is updated
  inline. No test exercises the full async `check_anchor_lag`/
  `check_under_replicated` methods directly (no `TaskScheduler`+`DtfsRaft`
  test harness exists in this crate); each new decision point is
  covered at the pure-function boundary it now goes through instead.

- **CR-3-6** 2026-09-12, Docker `dtfs-buildenv:latest`,
  `cargo test -p dtfs-manager --release`: 364 -> 365 passed (1 new:
  `metadata::tests::cr_3_6_integrity_fault_batch_accepted_truth_table`),
  0 failed. The heartbeat handler's integrity-fault-batch block now
  captures `Err` from both the `QuarantineNode` and (when the batch
  carries chunk ids) `RemoveCorruptChunkReplicas` forwards and combines
  them through a new pure `integrity_fault_batch_accepted(quarantine_ok,
  remove_corrupt_ok)`; either failure sets the outer `accepted` (now a
  `let mut` seeded true, returned as `HeartbeatResponse.accepted`
  instead of the previous hardcoded `true`) to false. The data node
  already re-enqueues drained faults on `!accepted` (Bug BF-5), so this
  closes the same class of election-window drop that CO-3-7 fixed for
  `report_chunk_status`. `AppendAudit` stays best-effort (`let _ =`),
  unchanged. No test exercises the full `heartbeat` gRPC handler (no
  `MetadataService`+`DtfsRaft` test harness exists in this crate, same
  gap noted for CR-3-3); the new decision point
  (`integrity_fault_batch_accepted`) is covered directly.

- **CR-3-9** 2026-09-12. Added
  `tests/release/cases/105-demote-refused-when-anchor-node-down.sh`
  (CR-3-1/CR-3-2/MGR-2: anchor cold=1 dc-a + warm cache, promote a warm
  copy, stop the cold anchor's data node, assert `flush_cache(warm)` is
  refused — warm copy survives — while the anchor is Down, then
  succeeds once the node is healthy again) and
  `tests/release/cases/106-legacy-tiering-skips-av-volume.sh`
  (CR-3-3/MGR-1: active AV cache policy, write+read a file to generate
  `ReportChunkReads`, assert the audit log carries no legacy
  `Bug BX-4`/Tape-migration rows for that file's chunks). Both modeled
  on `62-demote-colocated-anchor-survives.sh` (anchor+cache setup,
  `set-anchor`/`set-mandatory`/`set-cache`, `count_replicas_in_dc_tier`/
  `count_replicas_in_tier`) and `30-stale-liveness-downgrade.sh`
  (stop/restart a data node, poll for health transition); `trap
  case_cleanup EXIT`, last line `pass` (-> `STATUS: PASS`), `known_issue`
  fallbacks for lab-topology gaps (no dc-a cold node, promotion not
  landing in the poll window), not in `QUICK_CASES`. `bash -n` clean on
  both. NOT executed — no lab, per the plan's Build/verify environment
  section.

- **CR-3-10** 2026-09-12. AGENTS.md A.3.4 gained one sentence on
  liveness-gated planner survivors (CR-3-1/CR-3-2) and one on the
  case-insensitive anchor-DC compare (CR-3-7); the A.8 scheduler.rs row
  gained a summary sentence covering CR-3-1..3-6. `.claude/rules/
  integrity.md`'s Mandatory-vs-cache-replicas section gained the
  CR-3-1/CR-3-2 liveness-planner paragraph. `docs/src/internals/
  invariants.md` gained the full narrative: a CR-3-1/CR-3-2 block under
  § A.3.4, a CR-3-1..3-6 block under the `scheduler.rs` A.8 entry, and a
  CR-3-3/CR-3-6 block under the `metadata.rs` A.8 entry. No code change;
  docs-only commit.

- **CR-2-8** 2026-09-12. Added one sentence each to AGENTS.md A.3.1
  (the `now_ms` convention for a `ClientRequest` field that lands in
  replicated state) and the A.8 `state_machine.rs` table row (sticky-
  state carry-forward, prefix rename, `logical_size` extent), both
  pointing at a new `### dtfs-raft/src/state_machine.rs` section in
  `docs/src/internals/invariants.md` § A.8 carrying the full CR-2-1..2-6
  narrative.
