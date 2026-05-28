# Bug AV — Phase 5 follow-ups

**Status legend:** ⬜ todo · 🟦 in progress · ✅ done · 🟥 blocked · 🟨 needs review

Follow-up plan after Bug AV Phase 4 close-out
(`plans/completed/2026-05-22-PLAN-BUG-AV-TIER-LIFECYCLE.md`).
Captures items deferred out of Phase 4 + new bugs surfaced during the
2026-05-22 deploy + live verification.

---

## 0. Background

Phase 4 closed 2026-05-22 with all 9 tasks ✅ live-verified (S1 demo
green: cold mandatory + warm cache promote). Two items deferred by
design (AV-4-1b multi-DC infra) and several smaller items surfaced
during deploy. This plan catalogs them with the same schema as the
Phase 4 plan so a fresh agent (or operator) can resume the work.

### 0.1 Non-goals

- No new Phase 4 task scope. Items here are clean follow-ups, not
  Bug AV regression patches.
- Push to `origin/master` stays a manual operator step (per
  AGENTS.md § B.4 hygiene "never push to master mid-plan"); not a
  task in this plan.

---

## 1. Data-model changes

None.

---

## 2. Task list (ordered by effort ASC)

IDs stay stable (commits + audit reference them); only the display
order shifts.

| Order | ID | Effort |
|-------|-----|--------|
| 1 | AV-5-3 | 5 min |
| 2 | AV-5-4 | 30 min manual |
| 3 | AV-5-5 | 30 min live |
| 4 | AV-5-7 | 0.5 d |
| 5 | AV-5-2 | 0.5-1 d |
| 6 | AV-5-6 | 1 d |
| 7 | AV-5-1 | 1-2 d infra |

### ✅ AV-5-3 — Sweep remaining CLAUDE.md § N refs in archived plan (~5 min)
> Done 2026-05-28 under **BG-9**. `grep -rnE "CLAUDE.md\s*§" plans/completed/` clean.

* **Files:**
  `plans/completed/2026-05-22-PLAN-BUG-AV-TIER-LIFECYCLE.md`.
* **Implementation:** the archived plan has 10+ historical
  `CLAUDE.md § N` references inside task notes that describe
  past commits to pre-consolidation CLAUDE.md. Factually correct
  for that moment but confusing for a new reader. Replace with
  `AGENTS.md § A.x / B.x` mappings (§ 1.1 → § A.1.1, § 3.4 →
  § A.3.4, § 4 → § B.5, § 9 → § A.7, § 10 → § A.8, § 11 → § B.9).
* **Tests:** `grep -nE "CLAUDE.md\s*§" plans/completed/` returns
  empty.
* **Done when:** grep clean.

### ⬜ AV-5-4 — Browser click-test AV-4-3 / AV-4-4 (~30 min manual)

* **Files:** none (live verification only).
* **Implementation:** open
  `http://mini-mgr01:9402/ui/volumes` in browser, click
  **Details** on `default`/`av-demo` row (AV-4-3 expand card),
  click **Edit policy…** (AV-4-4 modal), change `warm_max` from
  1 → 2, submit, verify result panel reports new policy AND
  `dtfs-cli volumes show default` shows `cache.warm.max=2`.
  Reset to `1` via same modal.
* **Tests:** live only (no JS unit framework in repo).
* **Done when:** screenshot/notes captured in
  `plans/reports/WEBUI-VERIFICATION-<date>.md`; AV-4-3 + AV-4-4
  status flipped from "markup smoke; live deferred" to "✅
  live-verified" in Phase 4 plan § 7.4.

### ⬜ AV-5-5 — Live S2 + S3 demos (~30 min live)

* **Files:** `tests/proxmox/abuse/15-bug-av-cache-lifecycle.sh`
  (already written, S2 + S3 blocks unchanged).
* **Implementation:** with S1 now green (Phase 4 close-out + AV-4-8
  fix), re-run the script end-to-end. S2 = demote-on-idle (no
  reads in window → engine drops warm replica + clears
  cooldown). S3 = cooldown asymmetry (operator flush sets
  cooldown, immediate re-reads do NOT re-promote; engine demote
  cleared cooldown so re-promote was instant — verify the
  asymmetry).
* **Tests:** script exit 0 for S1+S2+S3 (S4 still pending
  AV-5-1).
* **Done when:** script run logged in
  `plans/reports/MINICLUSTER-VERIFICATION-<date>.md`; Phase 4
  plan § 7.4 updated to include S2/S3.

### ✅ AV-5-7 — Audit heartbeat `chunk_reads` count inflation (~0.5 d)
> Done (already implemented 2026-05-22/23; marker reconciled under BG-7
> 2026-05-28). `metrics.rs::record_chunk_read` dedups a (chunk,dc) to
> one increment per 1s window, collapsing the ~16 kernel-page reads of a
> single playback chunk to count=1. Tests `bug_av_5_7_*` (2) green on
> builder.

* **Files:** `crates/dtfs-data/src/metrics.rs::record_chunk_read`,
  `crates/dtfs-data/src/server.rs::get_chunk`.
* **Implementation:** during AV-4-8 diagnosis, 2 `cat` reads
  through FUSE produced `count=16` per heartbeat. Suspicious-high:
  the read pipeline may count per-chunk-byte or per-piece-stream
  rather than per-RPC. Audit. If true, `promote_reads` thresholds
  shift in meaning. Fix to "1 read = 1 increment" semantics, or
  document the actual unit.
* **Tests:** unit `bug_av_5_7_record_chunk_read_unit` — 1
  `record_chunk_read` call → `harvest_per_chunk` reports count=1.
* **Done when:** unit test green AND live: 2 `cat` reads through
  FUSE → heartbeat ships count=2 (not 16).

### ⬜ AV-5-2 — Raft log "unexpected EOF" RCA (~0.5-1 d)

* **Files:** investigate `crates/dtfs-raft/src/log_store.rs` (if
  exists) + the openraft-rt-tokio adapter at the pinned version
  (`Cargo.lock`: `openraft 0.10.0-alpha.17`,
  `openraft-rt-tokio 1.70.2`).
* **Implementation:** during the 2026-05-22 Phase 4 deploy,
  every manager failed `Error: when Read Logs:
  std::io::error::Error: io error: unexpected end of file` against
  the existing log written by the prior binary. Cargo.lock was
  unchanged, all schema additions were `#[serde(default)]`. The
  fix-of-last-resort wiped `/var/lib/dtfs/manager/raft-log` +
  `raft-sm` and re-bootstrapped. Investigate: did the *old*
  binary leave a partial-write tolerated by its own reader but
  rejected by the new? Is there a corrupt sstable detection
  path that should self-heal? Capture a regression test
  (synthetic partial sst + assert new binary either repairs or
  errors with an actionable message instead of crash-loop).
* **Tests:** unit / integration that exercises a truncated log
  file. Live: cycle deploy on a fresh test cluster without
  wiping state.
* **Done when:** root cause documented in
  `plans/incidents/SOAK-RECOVERY-<date>.md`; either a code fix
  lands or a clear runbook entry exists for the wipe-and-rebuild
  recovery in `docs/src/operations/`.

### ⬜ AV-5-6 — Validate `enable_cache_redundancy_escalation` on per-DC HA workload (~1 d)

* **Files:** none (config + observation).
* **Implementation:** set `[tiering]
  enable_cache_redundancy_escalation = true` on a volume with
  `cache_replicas.warm = { min: 1, max: 2 }` and ≥2 warm-tier
  nodes per DC. Drive sustained reads above
  `2 * promote_reads`. Confirm planner emits `promote_redundancy`
  audit rows (not `promote_threshold_met`) and 2 warm replicas
  appear in the DC.
* **Tests:** live (the AV-4-5 unit tests cover the planner
  decision; this validates real placement).
* **Done when:** observed in audit + on-disk; recorded in
  `plans/reports/REDUNDANCY-ESCALATION-<date>.md`.

### ⬜ AV-5-1 — Multi-DC topology for S4 anchor-lag-repair demo (~1-2 d infra)

* **Files:** `tests/proxmox/ansible/inventory.yml`,
  `tests/proxmox/ansible/roles/dtfs-netshape/defaults/main.yml`,
  `tests/proxmox/abuse/15-bug-av-cache-lifecycle.sh` S4 block.
* **Implementation:** provision a 2nd lab DC (TORO or NRB per
  Plexmedia spec) on the Proxmox host, run `apply-shaping.yml`
  with the correct class for each DC, re-deploy data + client
  nodes with `[location] datacenter = "dc-b"` (and a 3rd if
  using full LELI/TORO/NRB). S4 expects a warm-first write in
  `dc-b` → manager schedules cold anchor in `dc-a` within one
  tick.
* **Tests:** `tests/proxmox/abuse/15-bug-av-cache-lifecycle.sh`
  S4 exits 0 + recorded in
  `MINICLUSTER-VERIFICATION-<date>.md` under `plans/reports/`.
* **Done when:** ≥2-DC minicluster up, S4 passes, plan section
  ✅ + RESOLVED note appended to Phase 4 plan's § 7.2.

### Phase 5 Done When

- AV-5-1 .. AV-5-7 all ✅.
- `plans/active/PLAN-BUG-AV-FOLLOWUPS.md` →
  `plans/completed/<date>-PLAN-BUG-AV-FOLLOWUPS.md`.
- Phase 4 plan § 7.2 carryover list updated to mark each
  follow-up RESOLVED.

---

## 3. Per-task procedure

Standard AGENTS.md § B.3 loop: ⬜ → 🟦 → implement → run tests →
deploy → live verify → ✅ + commit (`Plan:` trailer pointing here).

---

## 4. Verification log

> Append per-task. Empty at plan authorship.

---

## 5. Deviations from plan

> Append per-task. Empty at plan authorship.

---

## 6. Commit-message convention

```
AV-5-<n>: <one-line summary, <72 chars>>

<2-3 sentence body.>

Plan: plans/active/PLAN-BUG-AV-FOLLOWUPS.md (AV-5-<n> ✅)
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```
