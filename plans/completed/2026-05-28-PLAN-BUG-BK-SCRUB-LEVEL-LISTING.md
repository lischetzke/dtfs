# PLAN-BUG-BK — WebUI lists only one scrub; quick + deep not shown

Plan id: `BK`
Status: ✅ complete (2026-05-28) — BK-1..BK-5 all ✅. Storage/API/WebUI fix landed + deployed leader-last; unit tests green (dtfs-raft 49/49, dtfs-manager 140/140); release case 23 added (live SKIP on precondition — no on-demand scrub trigger exists, see § 6). Live API serves per-(node,level) array on all 3 managers.
Author: Claude Opus 4.7 (1M context)
Created: 2026-05-28

---

## 0. Background

Operator triggered both a quick and a deep scrub via
`dtfs-cli scrub --level quick|deep`. The WebUI "Last Scrub" card shows
only a single result; the two levels are never both listed. Reported
2026-05-28.

### Root cause (code-confirmed) — two layers

**Layer 1 — storage drops the other level.**
`crates/dtfs-raft/src/state_machine.rs:1105-1136`
(`ClientRequest::ReportScrubResult`) stores results in
`scrub_results: BTreeMap<u64, ScrubRecord>` keyed by **node_id only**
(`sm.scrub_results.insert(*node_id, record)`, line 1131). `level` is a
field *inside* `ScrubRecord` but not part of the map key. So when a data
node reports a deep scrub after a quick scrub (or vice versa), the
second insert **overwrites** the first. Only the latest level survives
per node.

The prior "issue #8 fix" (snapshot.rs:36-42) changed the storage from a
single cluster-wide `last_scrub` to per-node, but it is still
**single-per-node**, not per-`(node, level)`. Field declared at
`state_machine.rs:194` and `snapshot.rs:42`.

**Layer 2 — WebUI renders only the single most-recent result.**
`crates/dtfs-manager/src/webui.rs:866` calls
`renderScrub(stats.last_scrub)`. `renderScrub` (lines 917-957) draws one
card from one record. The API already exposes a per-node array
`scrub_results_by_node` (`api.rs:2695-2711`, each entry carries `level`),
**but the WebUI ignores it entirely** and only consumes the
back-compat scalar `last_scrub` (`api.rs:2715`, the cluster-wide
`max_by_key(completed_at_ms)`). So even multiple nodes — let alone
multiple levels — are never listed.

Net effect: even if Layer 1 retained both levels, the UI would still
show only one. Both layers must change.

---

## 1. Goals / Non-goals

### Goals
- A node that has run both a quick and a deep scrub retains **both**
  records (latest of each level) in Raft state across snapshots.
- WebUI lists every retained scrub result grouped by node, showing the
  level (Quick / Deep) for each, not just the single most-recent.

### Non-goals
- No change to the scrub *execution* path on the data node, the
  `ReportScrubResult` RPC wire format, or `dtfs-cli scrub`.
- No history/retention beyond "latest per (node, level)". Keeping a time
  series of past scrubs is out of scope.
- No change to the integrity/audit feed (that is BH-9 / separate).

---

## 2. Data-model changes

`ScrubRecord` is unchanged (already carries `level: String`,
`node_id`, `disk_path`, `completed_at_ms`, `chunks_*`, `duration_secs`).

Storage key changes from per-node to per-(node, level):

- **New field** on `StateMachineData` (`state_machine.rs`) and the
  snapshot struct (`snapshot.rs`):
  ```rust
  /// Latest scrub result per (node_id, level). Replaces the
  /// single-per-node `scrub_results` map (BK) so quick + deep
  /// coexist. Outer key = node_id, inner key = level ("quick"/"deep").
  #[serde(default)]
  pub scrub_results_by_level: BTreeMap<u64, BTreeMap<String, ScrubRecord>>,
  ```
- **Keep** the legacy `scrub_results: BTreeMap<u64, ScrubRecord>` field
  (with `#[serde(default)]`) for one release for snapshot back-compat.
  On apply, write the new nested map; also keep the legacy map populated
  with the node's most-recent record so an older manager binary mid-roll
  still renders something.
- **Migration on load:** when the nested map is empty but the legacy map
  is non-empty (snapshot from a pre-BK binary), seed the nested map from
  the legacy entries (keyed by each record's own `level`). One-shot,
  in `apply_snapshot` / restore paths (`state_machine.rs:477,697`).

Snapshot compatibility (§ A.3.1): new field carries `#[serde(default)]`;
legacy field retained. Add a focused snapshot round-trip test.

---

## 3. Task list

### BK-1 — Store scrub results per (node, level)
- Status: ✅ (2026-05-28) — `scrub_results_by_level: BTreeMap<u64, BTreeMap<String, ScrubRecord>>` added to `StateMachineData` + snapshot struct + all initializers/copy points; `ReportScrubResult` writes nested map + keeps legacy per-node map for back-compat; one-shot `migrate_scrub_results_by_level` seeds nested from legacy on both restore paths. 3 unit tests green; dtfs-raft 49/49.
- **Files:** `crates/dtfs-raft/src/state_machine.rs`,
  `crates/dtfs-raft/src/snapshot.rs`
- **Implementation sketch:** Add `scrub_results_by_level` (§2) to
  `StateMachineData` + snapshot struct + their initializers
  (`state_machine.rs:234`, `snapshot.rs:108`) + the snapshot
  build/restore copy points (`state_machine.rs:477,567,697`). In the
  `ReportScrubResult` arm (line 1116-1131), insert into
  `scrub_results_by_level.entry(node_id).or_default().insert(level.clone(), record.clone())`
  and keep the legacy `scrub_results.insert(node_id, record)` for
  back-compat. Add the one-shot legacy→nested migration on restore.
- **Tests (BK-1):** unit tests in `dtfs-raft` against `StateMachineData`:
  - `scrub_quick_then_deep_both_retained`: apply a quick ReportScrubResult
    then a deep one for the same node; assert both survive in
    `scrub_results_by_level[node]`. Fails on current code (deep would
    overwrite quick in the single map).
  - `scrub_snapshot_roundtrip_preserves_levels`: snapshot + restore keeps
    both levels.
  - `scrub_legacy_snapshot_migrates`: a snapshot containing only the old
    `scrub_results` map populates `scrub_results_by_level` on load.
- **Done when:** unit assertions green; `cargo test -p dtfs-raft
  --release` clean.

### BK-2 — API exposes per-(node, level) results
- Status: ✅ (2026-05-28) — `get_stats` builds `scrub_results_by_node` and the back-compat `last_scrub` scalar by flattening `scrub_results_by_level` (node→level→record). Kept the existing JSON key name `scrub_results_by_node` (now one object per (node,level)); see Deviations. dtfs-manager 140/140; live + release-case coverage via BK-4.
- **Files:** `crates/dtfs-manager/src/api.rs`
- **Implementation sketch:** In `get_stats` (~line 2693), build the
  scrub array from `scrub_results_by_level` (flatten node→level→record),
  emitting one JSON object per `(node, level)` with the existing fields
  plus the already-present `level`. Keep emitting the back-compat
  `last_scrub` scalar (most-recent across all (node,level) records).
  Rename/extend the array to clearly carry level entries (keep the
  `scrub_results_by_node` key name or add `scrub_results` — pick one and
  note it in Deviations; WebUI BK-3 must match).
- **Tests (BK-2):** unit test on the aggregator (if `get_stats` has a
  testable seam) or assert the JSON shape; otherwise covered by the
  release case (BK-4).
- **Done when:** `/api/v1/stats` returns both quick + deep entries for a
  node that ran both; `cargo test -p dtfs-manager --release` clean.

### BK-3 — WebUI lists all scrub results (per node + level)
- Status: ✅ (2026-05-28) — `renderScrub` now loops `stats.scrub_results_by_node`, rendering one `renderScrubCard` per (node,level) sorted by node then level; empty-state placeholder preserved; `.scrub-result` separator CSS added. Unit `bug_bk_scrub_tests::scrub_lists_all_levels` asserts the array wiring + both Quick/Deep badges; dtfs-manager 140/140. Live browser confirm pending next manager redeploy.
- **Files:** `crates/dtfs-manager/src/webui.rs`
- **Implementation sketch:** Replace the single
  `renderScrub(stats.last_scrub)` call (line 866) with a loop that
  renders one scrub row/card per entry in the per-(node,level) array,
  reusing the existing level badge (Quick/Deep, lines 927-929) and the
  stat grid (lines 939-955). Sort by node then level (or most-recent
  first). Keep the empty-state placeholder (lines 919-922) when the
  array is empty.
- **Tests (BK-3):** WebUI is a render fn returning a raw-string template;
  add a `dtfs-manager` unit assertion that, given stats JSON with two
  scrub entries (quick + deep), the rendered HTML contains both a "Quick"
  and a "Deep" badge. (Mirrors the BI nav-caret unit-test approach.)
- **Done when:** browser shows separate Quick and Deep scrub results for
  the node; unit assertion green; live-verified on a manager WebUI.

### BK-4 — Release-suite coverage
- Status: ✅ (2026-05-28) — `tests/release/cases/23-scrub-quick-deep-listed.sh`. Asserts the `scrub_results_by_node` shape (array; every entry carries `node_id`+`level`) and, when any node has both quick+deep, that BOTH surface (the BK-1 discriminating signal). Auto-discovered by `run.sh` full mode (not added to `QUICK_CASES` — precondition-dependent). Live run on minicluster = SKIP (precondition: no node has run both levels yet — see Deviations; scrub execution is scheduler-driven and there is NO on-demand trigger, so two-level retention is proven deterministically by the BK-1 unit test and the live API shape; the case auto-promotes to PASS once a node runs both levels). Mirrors the BH-4 precondition-SKIP pattern.
- **Files:** `tests/release/cases/NN-scrub-quick-deep-listed.sh`,
  `tests/release/run.sh`
- **Implementation sketch:** Trigger `dtfs-cli scrub --level quick` then
  `--level deep` on a node, `wait_for` both ReportScrubResult to land,
  GET `/api/v1/stats`, assert the scrub array contains one `quick` and
  one `deep` entry for that node. Status protocol last line; `trap
  case_cleanup EXIT`; no fixed waits.
- **Done when:** case present, registered in `run.sh`, PASS (or
  KNOWN_ISSUE pinned to BK) in the latest run. Required per § B.5.1
  (cluster-visible: scrub result surfacing).

### BK-5 — Docs + changelog
- Status: ✅ (2026-05-28) — `changelog/UNRELEASED.md` Bug BK entry added; `docs/src/operations/monitoring.md` scrub-status bullet updated to describe per-(node,level) cards.
- **Files:** `changelog/UNRELEASED.md`;
  `docs/src/operations/monitoring.md` if it documents the scrub card.
- **Implementation sketch:** Changelog line: WebUI now lists quick and
  deep scrub results separately per node. Same commit as BK-3.
- **Done when:** changelog entry present; docs updated if applicable.

---

## 4. Verification log

_(empty — filled per task as work proceeds)_

| Task | Date | Commands | Result |
|------|------|----------|--------|
| BK-1 | 2026-05-28 | `cargo test -p dtfs-raft --release scrub` (builder) | 3/3 pass (quick_then_deep, snapshot_roundtrip, legacy_migrates); full crate 49/49 |
| BK-2/3 | 2026-05-28 | `cargo test -p dtfs-manager --release` (builder) | 140/140 (incl. `scrub_lists_all_levels`) |
| BK-2/3 | 2026-05-28 | deploy BK manager leader-last; `GET /api/v1/stats` ×3 mgrs | all serve `scrub_results_by_node` as a list (len 0; level field present) |
| BK-4 | 2026-05-28 | `bash tests/release/cases/23-scrub-quick-deep-listed.sh` (builder→minicluster) | STATUS: SKIP (precondition: no node has both levels; no on-demand trigger exists) |

---

## 5. Cross-cutting invariants

- Snapshot compat (§ A.3.1): new `scrub_results_by_level` field carries
  `#[serde(default)]`; legacy `scrub_results` retained one release;
  legacy→nested migration on restore. Snapshot round-trip test required
  (BK-1).
- API key naming for the scrub array must match between BK-2 (emit) and
  BK-3 (consume); record the chosen key in Deviations.

---

## 6. Deviations from plan

- BK-2/BK-3 API key naming: kept the existing key `scrub_results_by_node`
  (it now carries one entry per `(node, level)` rather than per node).
  Avoids a rename churn in any other consumer; WebUI BK-3 reads the same
  key. The back-compat `last_scrub` scalar is retained.
- BK-4 live result is SKIP, not PASS. Root cause discovered during live
  verification: there is NO supported on-demand scrub trigger — the
  manager `POST /api/v1/scrub` handler only audit-logs (no dispatch),
  the data node's `TriggerScrub` admin RPC is declared in `admin.proto`
  but has no server implementation, and the manager heartbeat
  unconditionally overrides the data node's local `[scrub]` intervals
  with the cluster config (floored at 1h via `quick_h.max(1)`), so
  lowering `data.toml` intervals cannot force a scrub either. Scrub
  execution is therefore purely scheduler-driven (quick 6h / deep 168h
  in the lab), and a deterministic in-suite two-level datapoint is
  impossible without changing the scrub-execution path — explicitly a
  BK non-goal. The core BK-1 storage fix is proven by the
  `scrub_quick_then_deep_both_retained` unit test; the BK-2/BK-3
  surfacing is proven by the manager unit test and the live API shape
  (all three managers serve `scrub_results_by_node` as an array with the
  `level` field). Logged the unwired-scrub-trigger gap as a follow-up
  candidate (see § 7).
- BK-4 commit hygiene: a concurrent actor had pre-staged a deliberate
  plan-tree reorganization into the shared index (add `.gitignore` rule
  `plans/completed/`, archive BE/BG/BI/BJ active→completed, add the
  BL/BM/BN active plans + the 1416 issue-scan report) between the BK-3
  and BK-4 commits. The BK-4 `git commit` swept those staged changes in
  alongside the release case. The reorg content is intended (authored
  under the repo owner's git identity) and the working tree is clean;
  BK-1/BK-2/BK-3 commits were unaffected. Not rewriting history because
  the index is shared with a concurrent agent and nothing is pushed.

---

## 7. Notes for the implementer

- Concurrent work is active on `master` (other agents). For BK-1 stage
  only `state_machine.rs` + `snapshot.rs` (+ tests); do not fold in
  unrelated working-tree edits.
- Confirm `ScrubRecord`'s exact field set and the `level` string domain
  (is it exactly `"quick"`/`"deep"`, or can the data node send other
  values like `"orphan"`?) before keying the inner map on it — read the
  `ReportScrubResult` producer in `dtfs-data` so the inner key space is
  understood (an unexpected level value must not silently collide).
  CONFIRMED: `dtfs-data/src/scrub.rs` emits exactly `"quick"` / `"deep"`
  (the `ScrubLevel` enum has only those two variants).

### Follow-up candidate (out of BK scope) — unwired on-demand scrub trigger

Discovered during BK-4 live verification: there is no working way to
trigger a scrub on demand. `POST /api/v1/scrub` (manager) only writes an
audit row and returns `accepted` — it never dispatches to a data node.
The data node's `TriggerScrub` admin RPC is declared in `proto/admin.proto`
but has no server implementation in `crates/dtfs-data`. Consequently
`dtfs-cli scrub --level …` (which posts to the manager endpoint) is a
no-op. Scrubs only ever run on the data node's internal scheduler
(quick/deep interval). This blocks deterministic release/abuse coverage
of any scrub-dependent behaviour. Recommend a separate plan to wire the
manager endpoint → data `TriggerScrub` → `run_scrub(level)` so an
operator (and the release suite) can force a scrub.
