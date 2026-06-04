# PLAN-BUG-CD — release_leases_by_host must not drop live concurrent leases

Status: complete — built + tested on dtfs-builder, deployed (managers
leader-last) and integration-smoke-verified on the Minicluster (2026-06-04).
All tasks ✅.

## Background

`ReleaseLeasesByHost` (FUSE startup/shutdown orphan cleanup, P0-6) drops
every write lease whose `owner_id` starts with `<host>:` except the one
matching the caller's own `except_pid`. The in-code comment claims it
"keeps leases from healthy concurrent mounts on the same host" — it does
NOT: any second live FUSE pid on the host loses its leases whenever any
other FUSE process on that host (re)starts. On the Minicluster this nuked
a healthy writer's in-flight lease (a duplicate crash-looping systemd unit
re-ran the cleanup every ~5s), so the writer's next `UpdateChunkLocations`
hit `WriteLeaseConflict` → `commit_chunk_locations failed` → EIO. Integrity
is the prime directive (§ A.3.3); silently dropping a live writer's lease
is the sharp edge to remove.

`AcquireWriteLease` already lazily evicts an expired lease on the contended
path (`state_machine.rs` ~1480), so a restarting FUSE self-heals its own
orphan on acquire without needing the blanket release. The only job left
for `ReleaseLeasesByHost` is reaping *expired* host leases promptly.

## Goals
- A live (renewing) concurrent FUSE lease on the same host is NEVER dropped
  by `ReleaseLeasesByHost`.
- Expired/orphan host leases are still reaped.

## Non-goals
- No pid-liveness registry (the lease's own `expires_at_ms` is the liveness
  signal: live writers renew at half-life, orphans stop renewing and lapse).
- No owner_id format change.

## Data-model changes
- `crates/dtfs-raft/src/lib.rs` `ClientRequest::ReleaseLeasesByHost` gains
  `now_ms: u64` (manager-supplied at propose time, deterministic across
  replicas — same pattern as `SweepExpiredLeases { now_ms }` /
  `AcquireWriteLease { now_ms }`). New tagged-enum field; never persisted, no
  snapshot-compat impact.

## Task list
- **CD-1** ✅ Failing test `bug_cd_release_by_host_spares_live_peer` in
  `state_machine.rs`: seed two same-host leases — one unexpired (live peer,
  different pid), one expired (orphan) — apply `ReleaseLeasesByHost { now_ms }`;
  assert only the expired one is dropped and the live one survives.
- **CD-2** ✅ Added `now_ms: u64` to the variant + `Display` arm (`lib.rs`).
- **CD-3** ✅ apply arm: added `&& l.expires_at_ms <= *now_ms` to the drop
  filter; rewrote the stale comment (`state_machine.rs`).
- **CD-4** ✅ `metadata.rs::release_leases_by_host`: computes `now_ms`
  (SystemTime, as the other lease ops do) and passes it.
- **CD-5** ✅ changelog `UNRELEASED.md` entry.
- **CD-6** ✅ build + `cargo test -p dtfs-raft -p dtfs-manager --release` on
  dtfs-builder; observed fail→pass.
- **CD-7** ✅ release-gate case
  `tests/release/cases/41-lease-release-spares-live-writer.sh` (SKIP-guarded:
  the host-scoped, pid-discriminated bug needs two co-located FUSE pids, which
  the single-FUSE Minicluster can't stage; runs the real scenario when
  CLIENT2+MOUNT2 are set).

## Cross-cutting invariants preserved
- `ReleaseLeasesByHost` goes through Raft; `now_ms` is computed by the
  proposing manager before `raft_write` so all replicas apply identically.
- Still removes expired orphans, so P0-6's intent (don't let a dead writer's
  lease linger) holds; the restart self-heal via `AcquireWriteLease` covers
  the not-yet-expired same-path case.

## Verification log
- **2026-06-04 — CD-6 fail→pass (dtfs-builder).** Behavioral fail-first: with
  the expiry clause sed-removed from the apply filter, the new test reported
  `WriteLeasesReleased { count: 2 }` (the live peer was wrongly dropped) →
  FAILED. With the fix restored: `cargo test -p dtfs-raft --release` 58 passed
  (was 57; +`bug_cd_release_by_host_spares_live_peer`), `dtfs-manager` 181
  passed, `dtfs-common` 47 passed, 0 failed.
- **2026-06-04 — deploy + integration smoke (Minicluster).** Built
  `dtfs-manager` with fresh `SOURCE_DATE_EPOCH`; rolled all three managers
  leader-last (`20260604-095050`), cluster re-elected a leader and all nodes
  Healthy. The FUSE restart during the client roll exercised the new
  `ReleaseLeasesByHost { now_ms }` startup-cleanup path (count=0, no expired
  orphans; mount came up healthy), and a write/read/delete lease lifecycle
  succeeded on the new build. The full two-pid "live peer survives a peer
  restart" scenario is not stageable on the single-FUSE lab; it is covered by
  the unit test (purest layer, per § B.5) and the SKIP-guarded release case.

## Deviations from plan
- CD-6's failing observation was made behaviorally (sed-neutralizing the
  expiry clause in the deployed-for-test copy, observing `count: 2`) rather
  than as a bare compile error, to prove the wrong behavior directly.
