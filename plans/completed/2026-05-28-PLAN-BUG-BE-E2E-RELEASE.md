# Bug BE — End-to-end release test suite

**Status legend:** ⬜ todo · 🟦 in progress · ✅ done · 🟥 blocked · 🟨 needs review

> **Reconciled 2026-05-28 (BG-2):** the harness (`run.sh`, all 20 cases,
> `lib/common.sh`, `release-testing.md`) was written but never
> committed; landed in commit `3469ac5` (BG-1). BE-2..BE-21 ✅. Two
> items stay open and are tracked in `PLAN-BUG-BG-CONSOLIDATION.md`:
> **BG-5** (BE-1 remainder: `lib/fixtures.sh` + `lib/health.sh`) and
> **BG-10** (BE-22 optional pre-commit hook). This plan stays active
> until both close.

The repo has accumulated dozens of `tests/proxmox/abuse/NN-name.sh`
scripts written reactively (one per bug). There is no curated
release-gate suite that exercises the cluster end-to-end. This plan
builds one: a single entry-point harness, a fixture set, and a
catalogued sequence of scenarios covering the bug history + the
real-world Plexmedia workload (writes, reads, scrubs, migration,
cache, restart, integrity faults, recovery).

## 0. Goals + non-goals

### Goals
- One harness command: `bash tests/release/run.sh [--quick]`.
- `--quick` ≈ 15 min subset for branch-level smoke.
- Full run ≈ 60–90 min, covering every documented invariant.
- Every test exits with a status: `PASS`, `FAIL`, `SKIP`,
  `KNOWN_ISSUE`. KNOWN_ISSUE rows pin to a Bug ID — when that bug
  closes, the row flips to FAIL until the test recognises the new
  green state, then to PASS.
- Tests can run against the lab minicluster (default) or any other
  cluster via env vars (`MGRS=<csv>`, `DATA=<csv>`,
  `CLIENT=<host>`, `MOUNT=<path>`).
- Suite output: console summary + machine-readable JSON
  (`plans/reports/RELEASE-<date>-<duration>.json`) + per-test logs.

### Non-goals
- Performance benchmarking (separate plan, future).
- Multi-cluster federation tests.
- Chaos engineering beyond planned restarts (no random fault
  injection — every fault is named).

## 1. Harness shape

```
tests/release/
  run.sh                       # entry point; orchestrates + summarises
  lib/
    common.sh                  # mgr_get / mgr_post / ssh_run / assert helpers
    fixtures.sh                # produce + verify sha256 test files
    health.sh                  # cluster-state probes
  cases/
    00-prereqs.sh              # ssh ok, mgrs leader elected, FUSE mounted
    01-basic-write-read.sh     # write, read, sha256 match
    02-replica-placement.sh    # mandatory + cache replica counts match policy
    03-scrub-quick.sh          # quick scrub fires + reports clean disk
    04-scrub-deep.sh           # deep scrub fires + reports clean disk
    05-migration.sh            # operator migrate; source preserved; target verified
    06-flush-cache.sh          # mandatory preserved; cache dropped
    07-set-schedules.sh        # set scrub_interval; verify heartbeat override
    08-set-anchor.sh           # AnchorPolicy applied
    09-set-mandatory.sh        # change RF; new chunks honour; existing trigger backfill
    10-leader-rotation.sh      # stop leader; quorum holds; new leader elected
    11-data-node-restart.sh    # stop data node; reads fall over to other replica
    12-fuse-restart.sh         # kill FUSE; remount; reads continue
    13-integrity-corruption.sh # full integrity sequence (see § 3)
    14-orphan-reap.sh          # unlink + verify chunk reap (Bug BC orphan_scrub)
    15-soak-15min.sh           # condensed workload
    16-version-report.sh       # every node reports version + build_date
    17-stats-correctness.sh    # raw ≈ net × RF; logical ≤ net; 6/6 nodes; tier sums
    18-api-shape.sh            # every endpoint returns expected schema keys
    19-config-effective.sh     # /api/v1/config/effective returns ≥ 40 entries + scope_kind (Bug BD)
    20-quarantine-flow.sh      # corrupt → quarantine → routing skip → unquarantine (Bug BC)
```

`run.sh --quick` runs: 00, 01, 02, 05, 06, 13, 15, 17, 18.
`run.sh` (full) runs every case in numeric order.

## 2. Test result protocol

Each case script:
- Sources `lib/common.sh`.
- Prints `STATUS: PASS|FAIL|SKIP|KNOWN_ISSUE [<bug-id>]` as the
  LAST line of stdout.
- Exits 0 on PASS/SKIP/KNOWN_ISSUE; exits 1 on FAIL.

Harness `run.sh`:
- Iterates cases, captures stdout to a per-case log.
- Tallies counts.
- Final exit = 0 iff zero FAIL.
- Writes JSON summary.

## 3. Integrity test sequence (case 13)

This is the canonical case the user asked for. End-to-end exercise
of write → read → migrate → corrupt → scrub → quarantine →
auto-correction → unquarantine.

```
A) Write a 10 MiB random file via FUSE → /soak/integrity-A.bin.
B) Read it back, sha256 must match.
C) Inspect: file has expected mandatory + cache replicas.
D) Migrate via operator API (e.g. /api/v1/migrate path-prefix
   target_tier=cold). Verify new replica lands; existing source
   preserved.
E) sha256 still matches after migrate.
F) Pick one data node holding a replica; SSH; `dd conv=notrunc
   bs=1 count=1 seek=4096` → flip a byte in the .dat file.
G) Read the file via FUSE — sha256 still matches (must fall over
   to another replica). Once BC-8 (verify_on_read) lands:
   integrity-fault trailer fires; FUSE retries; healthy bytes
   returned.
H) Once BC-7 (orphan_scrub) lands: mgr cross-check detects the
   in-state-not-on-disk-correctly mismatch via the deep_scrub
   path; quarantines the faulty node.
I) Verify routing: /api/v1/files/<path> read NOT routed to the
   quarantined node.
J) Verify auto-correction: scheduler re-replicates from a healthy
   node to a non-quarantined target (`replication.complete`
   audit row with src ≠ faulty_node).
K) Operator unquarantine via POST /api/v1/nodes/{id}/unquarantine
   with operator_id + reason_acknowledged.
L) Verify node back to Healthy; writes/reads resume on it.
M) Delete the file; verify all replicas GC'd within
   orphan_scrub interval + grace (BC-7).
```

Today (pre-BC), G/H/J/K/L/M produce `KNOWN_ISSUE [BC]`. A/B/C/D/E/F/I
work today. Test still RUNS the whole sequence so partial failures
are recorded; pre-BC the case exits with `KNOWN_ISSUE [BC]` overall
because some steps are blocked.

## 4. Tasks

### Phase 1 — Harness + helpers
#### 🟨 BE-1 — `lib/common.sh` + `lib/fixtures.sh` + `lib/health.sh`  [PARTIAL → BG-5]
> `lib/common.sh` shipped (commit `3469ac5`). `lib/fixtures.sh` +
> `lib/health.sh` NOT yet written — migrated to **BG-5** in
> `PLAN-BUG-BG-CONSOLIDATION.md`.
* Helper fns: `mgr_get`, `mgr_post`, `mgr_post_leader`, `ssh_run`,
  `client_run`, `wait_for`, `assert_eq`, `assert_match`,
  `chunk_ids_for_path`, `chunks_on_node`, `sha256_via_fuse`.
* Status protocol: `pass`, `fail <msg>`, `skip <reason>`,
  `known_issue <bug>`.
* **Done when:** sourcing the libs prints nothing; helpers run
  from a smoke test.

#### ✅ BE-2 — `run.sh` entry point
* Iterates `cases/NN-*.sh` (sorted).
* `--quick` flag filters to the quick subset.
* Captures per-case stdout to
  `plans/reports/RELEASE-<date>-<run-id>/<case>.log`.
* Prints aggregated table at end; writes JSON summary.
* **Done when:** `bash tests/release/run.sh --quick` works
  against an empty cases dir (no failures, "0 cases run").

### Phase 2 — Quick subset
#### ✅ BE-3 — `00-prereqs.sh`
#### ✅ BE-4 — `01-basic-write-read.sh`
#### ✅ BE-5 — `02-replica-placement.sh`
#### ✅ BE-6 — `05-migration.sh`
#### ✅ BE-7 — `06-flush-cache.sh`
#### ✅ BE-8 — `13-integrity-corruption.sh` (see § 3)
#### ✅ BE-9 — `15-soak-15min.sh`
#### ✅ BE-10 — `17-stats-correctness.sh`
#### ✅ BE-11 — `18-api-shape.sh`

### Phase 3 — Full extension
#### ✅ BE-12 — `03-scrub-quick.sh` / `04-scrub-deep.sh`
#### ✅ BE-13 — `07-set-schedules.sh` / `08-set-anchor.sh` / `09-set-mandatory.sh`
#### ✅ BE-14 — `10-leader-rotation.sh`
#### ✅ BE-15 — `11-data-node-restart.sh`
#### ✅ BE-16 — `12-fuse-restart.sh`
#### ✅ BE-17 — `14-orphan-reap.sh`
#### ✅ BE-18 — `16-version-report.sh`
#### ✅ BE-19 — `19-config-effective.sh`
#### ✅ BE-20 — `20-quarantine-flow.sh`

### Phase 4 — Docs + CI hook
#### ✅ BE-21 — `docs/src/operations/release-testing.md`
* Document: how to run, what each case checks, how to interpret
  KNOWN_ISSUE rows, how to add a new case.

#### ✅ BE-22 — Make `--quick` part of pre-commit (optional, not
  enforced) + add a release checklist row in CHANGELOG template.  [done under BG-10]
> Done 2026-05-28 (BG-10). Added the release-suite gate to
> `changelog/README.md` "Cutting a release". The pre-commit-hook half
> was deliberately NOT implemented: the suite needs a live cluster, so a
> local git hook would fail every offline commit — wrong tradeoff for a
> hermetic pre-commit. The gate lives in the release runbook instead.

## 5. Invariants every test must preserve

- **A failing case never modifies cluster state in a way the next
  case can't recover from.** Each case is responsible for its own
  cleanup; the suite must be re-runnable without manual reset.
- **Integrity-fault injection is reversible.** The
  `13-integrity-corruption.sh` script restores the original bytes
  + unquarantines the node + re-replicates if its actions left
  the cluster reduced.
- **No case requires operator interaction.** No prompts, no
  passwords, no manual confirmation.
- **All assertions are first-class.** No "soft" warnings that
  silently degrade to PASS.

## 6. Commit-message convention

```
BE-<n>: <one-line summary, <72 chars>>

<body>

Plan: plans/active/PLAN-BUG-BE-E2E-RELEASE.md (BE-<n> done)
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```
