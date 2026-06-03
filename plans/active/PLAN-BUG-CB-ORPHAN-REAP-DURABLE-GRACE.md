# PLAN-BUG-CB — Orphan reap uses a durable grace clock

Status: code complete, build + live-verify pending (Windows host cannot
build; run on the Linux builder per AGENTS.md A.4.1).

## Background

User report: files deleted via the client still consume disk on the
data nodes. Analysis (read-only) traced the delete path:

- `unlink` -> manager `DeleteFile { path }` -> `sm.files.remove(path)`
  only. Frees zero physical bytes; chunks become orphans (unreferenced
  by any `FileMetadata`). AGENTS.md A.6 #8.
- Physical bytes are reclaimed solely by the `orphan_scrub` reaper
  (`scheduler.rs`, Bug BC-7).
- Pre-fix, the reaper measured its reap grace against an in-memory
  `orphan_seen` map (`(node_id, chunk_id) -> first_observed_ms`). That
  map is NOT persisted and is wiped on every manager restart /
  leadership change. The loop also consumed a full `orphan_interval_secs`
  (default 86400 = 24h) before its first scan after every (re)start.
- Net effect: a manager had to run ~48h uninterrupted (skip-first-tick
  interval + 24h grace) to reap anything. On a cluster doing rolling
  deploys or soak chaos, no manager survived that long, so `first_seen`
  kept resetting and deleted-file chunks were never freed. Disk usage of
  deleted media climbed without bound.

## Goals

- Reap an unlinked file's chunks reliably, independent of manager
  restart / leadership churn.
- Keep the in-flight put/promote race protected (never reap a chunk that
  is mid-write or mid-promote).
- No new Raft writes, no new persisted state machine field.

## Non-goals

- Changing the default `orphan_interval_secs` / `orphan_grace_secs`
  (operator-tunable; out of scope).
- Synchronous chunk deletion on `unlink` (async reclamation is the
  intended design; we only fix its reliability).
- Content-dedup retention: a deleted file whose chunks are shared with a
  surviving file correctly keeps those bytes. Unchanged.

## Locked-in decisions

- Anchor the reap grace to each chunk's DURABLE on-disk `.dat` mtime
  instead of a manager-local first-observed clock. The mtime is set at
  `commit_chunk_from_tmp` (atomic rename) and survives restarts.
- A chunk whose mtime is unknown (0) DEFERS the reap rather than reaping
  with no grace. This is the mixed-version safety valve: a pre-CB data
  node sends an empty `chunk_mtimes_ms`, so reaping pauses on that node
  until it is upgraded (data nodes roll after managers, A.4.2).

## Data-model changes

- `proto/data.proto` `ListChunksResponse`: new
  `repeated uint64 chunk_mtimes_ms = 3;`, index-aligned with
  `chunk_ids`. Additive, backward compatible (old field tags unchanged).

## Task list

- **CB-1** ✅ proto: add `chunk_mtimes_ms` to `ListChunksResponse`.
  - Files: `proto/data.proto`.
  - Done when: field present; `dtfs-proto` regenerates on build.
- **CB-2** ✅ data store: `list_all_chunk_ids` ->
  `list_all_chunks_with_mtime() -> Vec<(String, u64)>`; stat each `.dat`
  mtime during the existing walk; stat failure yields 0.
  - Files: `crates/dtfs-data/src/store.rs`.
- **CB-3** ✅ data server `list_chunks` handler: sort by id, page, emit
  parallel `chunk_ids` + `chunk_mtimes_ms`.
  - Files: `crates/dtfs-data/src/server.rs`.
- **CB-4** ✅ scheduler: `list_chunks_on_node` returns
  `HashMap<String, u64>` (id -> mtime); default 0 when the node omits
  mtimes.
  - Files: `crates/dtfs-manager/src/scheduler.rs`.
- **CB-5** ✅ scheduler: add pure `orphan_reap_due(is_orphan,
  chunk_mtime_ms, now_ms, grace_secs)`; reap loop uses it; remove the
  `orphan_seen` field, its init, the per-tick `first_seen` bookkeeping,
  and the end-of-tick `retain`. In-state-missing check uses
  `contains_key`.
  - Files: `crates/dtfs-manager/src/scheduler.rs`.
- **CB-6** ✅ scheduler: replace skip-first-full-interval with a short
  (30s) warmup so the first post-(re)start scan is prompt.
  - Files: `crates/dtfs-manager/src/scheduler.rs`.
- **CB-7** ✅ unit tests: `cb_orphan_reap_due_uses_durable_mtime`
  (referenced/recent/aged/boundary/unknown) and
  `cb_orphan_reap_survives_simulated_restart` (verdict carries no state
  across calls).
  - Files: `crates/dtfs-manager/src/scheduler.rs`.
- **CB-8** ✅ docs: AGENTS.md A.6 #8, orphan-cleanup runbook safety
  notes, changelog `UNRELEASED.md` (Bug CB entry).
- **CB-9** 🟦 build + test on the Linux builder
  (`cargo test -p dtfs-data -p dtfs-manager --release`). BLOCKED on
  Windows host (rocksdb/fuse3). Failing-first evidence: the new helper
  did not exist, so the test module referenced an undefined symbol
  (compile error = the failing test per B.5).
- **CB-10** 🟦 live-verify on the lab + release case
  `tests/release/cases/14-orphan-reap.sh` (still valid; recommend
  augmenting with a manager-restart-mid-grace assertion as a follow-up).
  BLOCKED: no cluster access from this session.

## Cross-cutting invariants preserved

- A chunk a `FileMetadata` still references is NEVER reaped
  (`chunk_is_orphan`, Bug BP-4 / A.6 #8). Unchanged.
- No silent replication-factor downgrade; reaper only deletes
  unreferenced chunks. Unchanged.
- Anchor invariant: orphan reap acts only on files already removed from
  metadata, so the anchor copy is gone with the file. Anchor checks
  (flush/demote) are unaffected.

## Verification log

- (pending CB-9 / CB-10) — to be filled in from the Linux builder and
  the lab cluster.

## Deviations from plan

- Bug letter: CA was already taken (WebUI bug-ID leak, in
  `changelog/UNRELEASED.md`); this work uses CB.
