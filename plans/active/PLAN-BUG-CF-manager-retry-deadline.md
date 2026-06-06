# PLAN-BUG-CF — call_manager retry-with-deadline (ride through elections)

## Incident

2026-06-06 ~16:00 CEST, dtfs-lm cluster: WinSCP transfer to the FUSE mount
on dtfs-lm-data01 failed with EIO while a rolling manager restart ran.
Timeline (journals on dtfs-lm-mgr02/03 + dtfs-fuse on data01):

- 15:59:58 mgr03 restarted (follower), 16:00:07 mgr02 restarted (follower),
  ~16:00:14 mgr01 (leader) restarted last — procedure correct (leader-last).
- 16:00:15.009–.013: FUSE `commit_chunk_locations` walked the pool once:
  mgr01 dead (channel-level), mgr02+mgr03 replied
  `Unavailable: manager is not the leader → forward to mgr01` (stale
  leader hint; election had not fired yet). All 3 attempts burned in 4 ms
  → `all managers exhausted` → flush failed → EIO → WinSCP abort.
- 16:00:17.6: mgr03 leader. Total no-leader window ≈ 2.5 s.

## Root cause

- `DtfsClient::call_manager` (crates/dtfs-client/src/client.rs:705) walks
  the manager pool exactly once: `attempts = manager_addrs.len()`, no
  backoff, no second round, no overall deadline. Three instant
  `Unavailable` replies = total failure in milliseconds.
- `manager_client()` (client.rs:662) connect-level failure of ALL addrs
  propagates through `?` at client.rs:713 and aborts `call_manager`
  immediately — bypasses even the single walk.
- Bug AC's server-side 3 s leader-wait (dtfs-manager/src/metadata.rs:157)
  only covers `ForwardToLeader(None)`. Followers that still *name* a dead
  leader return instantly, so nothing on the server slows the walk either.

## Goals

- A FUSE write in flight while a leader election runs (≤ ~15 s without a
  reachable leader) completes instead of returning EIO.
- All manager RPCs (commit, allocate, lease acquire/renew/release, delete,
  register) get the same ride-through behaviour via `call_manager`.

## Non-goals

- No server-side change (Bug AC wait stays as-is).
- No change to data-node retry policy (`put_chunk_retry_backoff`).
- No config surface — constants in client.rs (mirror Bug G constants).
- No request dedup / idempotency work (separate known issue, lease_id
  guard already covers commit).

## Locked-in decisions

- Client-side fix in `call_manager` only; full B.1 procedure (user,
  2026-06-06).
- Overall deadline 15 s (election ≈ 2.5 s observed; rolling-restart gaps
  a few seconds; keep FUSE ops from hanging forever).
- Backoff between full-pool walks: 250 ms → 500 ms → 1 s → 2 s (cap), no
  jitter — mirrors `put_chunk_retry_backoff`.
- Application-level errors (NotFound, FailedPrecondition, …) still
  surface immediately. Empty address list still fails immediately.

## Data-model changes

None. New consts + pure helper in crates/dtfs-client/src/client.rs:

```rust
pub(crate) const MANAGER_WALK_DEADLINE_MS: u64 = 15_000;
const MANAGER_WALK_BASE_BACKOFF_MS: u64 = 250;
const MANAGER_WALK_MAX_BACKOFF_MS: u64 = 2_000;

/// Bug CF — Some(backoff) for the next full-pool walk, None when the
/// overall deadline is spent.
pub(crate) fn manager_walk_retry_backoff(
    walk_round: u32,
    elapsed: std::time::Duration,
) -> Option<std::time::Duration>;
```

## Tasks

### CF-1-1 ✅ pure backoff policy + unit tests
- Files: crates/dtfs-client/src/client.rs
- Tests (failing first — compile error counts):
  `bug_cf_walk_backoff_doubles_and_caps` (round 0→250 ms, 1→500 ms,
  2→1000 ms, 3..→2000 ms), `bug_cf_walk_backoff_deadline_exhausts`
  (elapsed ≥ 15 s → None), `bug_cf_walk_backoff_zero_elapsed_retries`.
- Done when: tests green in `cargo test -p dtfs-client --release`.

### CF-1-2 ✅ call_manager outer walk-loop
- Files: crates/dtfs-client/src/client.rs (`call_manager`)
- Sketch: pre-check empty addr list (bail). Outer `loop`: inner walk as
  today, but a `manager_client()` error breaks the walk (retryable)
  instead of `?`-aborting. On walk exhaustion consult
  `manager_walk_retry_backoff(walk_round, started.elapsed())`:
  Some(d) → warn + sleep(d) + next round; None → return last error with
  "(all managers exhausted after Ns)" context.
- Test (failing first):
  `bug_cf_call_manager_retries_walks_until_managers_recover` — client via
  `DtfsClient::new()`, one addr, lazy channel pre-inserted into
  `manager_channels` (test module has private access); closure returns
  `Status::unavailable` 3 times then Ok(42); today fails after 1 call,
  post-fix returns 42 after ~1.75 s of backoff rounds.
- Done when: new test + full dtfs-client suite green on builder.

### CF-1-3 ✅ release case 49-write-during-leader-restart.sh
- Files: tests/release/cases/49-write-during-leader-restart.sh
- Sketch: needs ≥3 managers (skip otherwise). Start a multi-second FUSE
  write (dd from /dev/urandom through the mount), stop the leader manager
  mid-write, wait for new leader, write must complete rc=0 and the file
  must read back with matching checksum; restart old leader (cleanup
  trap). `STATUS: PASS|FAIL|SKIP` last line; use `wait_for` from
  lib/common.sh.
- Done when: case passes against a lab cluster running the fixed binary.

### CF-1-4 ✅ changelog + AGENTS.md check + live verify
- Files: changelog/UNRELEASED.md (bug-fix entry); AGENTS.md (expect "no
  change needed" — behaviour is client-internal resilience, no operator
  surface).
- Live verify on dtfs-lm: repeat incident — continuous write via dd to
  /mnt/dtfs while rolling-restarting mgr03→mgr02→mgr01(leader-last);
  write completes, no EIO; quote journal lines in the verification log.
- Done when: verification log below has the entry; plan deleted on merge.

## Cross-cutting invariants

- Never retry application-level errors (lease_conflict, NotFound, …).
- Deadline must stay well under WRITE_LEASE_MS renewal slack (60 s) so a
  riding-through writer cannot silently outlive its lease.
- Heartbeat/register paths share `call_manager`; 15 s worst-case blocking
  there is acceptable (loops tolerate skew).

## Verification log

- CF-1-1 / CF-1-2 (2026-06-06): failing test observed first —
  `cargo test -p dtfs-client --release bug_cf` on dtfs-builder:
  `error[E0425]: cannot find function manager_walk_retry_backoff` (x9,
  compile error counts per B.5). After implementing:
  `test result: ok. 22 passed; 0 failed` (dtfs-client, includes the 4
  new bug_cf tests; incident-regression test
  `bug_cf_call_manager_retries_walks_until_managers_recover` exercises
  3 all-Unavailable walks then recovery). Full workspace:
  `cargo test --release --workspace` — all crates green, 0 failed.
- CF-1-3 / CF-1-4 live verify on Minicluster (2026-06-06): pre-fix run of
  `tests/release/cases/49-write-during-leader-restart.sh` against
  dtfs-fuse built 20260604-095050 (no CF) on mini-client01:
  `FAIL: file size after write through leader restart: expected
  '1073741824', got '658505728'` — 1 GiB write died when leader
  mini-mgr03 restarted mid-write (incident reproduced). Deployed
  dtfs-fuse with the fix; post-fix run of case 49: `STATUS: PASS` —
  1 GiB write through a mid-write restart of leader mini-mgr02
  completed at full size and read back. FUSE journal confirms the new
  path engaged:
  `manager pool exhausted — backing off before next walk … walk_round=0
  delay_ms=250` then `walk_round=1 delay_ms=500`, after which the commit
  landed on the new leader (15:05:24-25Z, binary built 20260606-150359).
- Deploy hiccup (2026-06-06): the first "fixed" deploy (binary built
  20260606-145524) did NOT contain the fix — the tar-over-ssh sync
  preserves source mtimes, so cargo considered the cached PRE-fix
  dtfs-client release rlib fresh and linked it into dtfs-fuse (the
  dtfs-client lib-TEST binary had no cached artifact, so it rebuilt
  from fixed source and its tests passed — masking the stale rlib).
  Case 49 failed against that binary (864026624/1073741824 bytes,
  old log format, no walk_round field). `touch crates/dtfs-client/src/
  client.rs` + rebuild produced a binary containing the new log strings
  (verified via `grep -ac` before deploy). AGENTS.md § A.4 gains a
  stale-artifact warning.
- Observed during pre-fix runs, follow-up candidate (Bug CG, not in
  scope here): both failing dd runs exited rc=0 with conv=fsync while
  the file committed short (658/864 MB of 1024 MB) — commit failures on
  buffered segments did not propagate to the writer's write()/fsync()
  as an error. The dtfs-lm incident DID surface an error to WinSCP, so
  propagation appears path-dependent (fresh-file flush vs mid-write
  async segment flush).

## Deviations

- Live-verify target moved from dtfs-lm (incident cluster) to the
  Minicluster lab — user choice 2026-06-06; the auto-mode classifier
  also blocked an unattended binary install on dtfs-lm. dtfs-lm gets
  the fix via the next release (AZ-5 updater).
- Release case throttles via a single sustained 1 GiB dd stream instead
  of pv (not installed on lab clients).
