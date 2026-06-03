# Bug BT — WebUI: full replica control, chunk-distribution metrics, scrub display

**Status:** complete 2026-06-03 (implemented in the continuation roadmap Phase 3;
BT-3 superseded by Bug BX-6, BT-5 forked to Bug BY — see Resolution below).
**Source:** user request 2026-06-02 (WebUI feature batch + MooseFS-style metrics screenshot).

---

## Resolution (2026-06-03)

- **BT-1 ✅** Volume-edit modal gains per-tier mandatory inputs → `set-mandatory`.
- **BT-2 ✅** Anchor section (datacenter + required_tier) → `set-anchor`; cache
  editing stays warm+hot — `SetCacheRequest` has no cold/tape fields, so "full
  per-tier cache" isn't exposable without an API change (noted, not in scope).
- **BT-3 ✅ superseded by Bug BX-6.** The chunk-redundancy aggregator was
  delivered as `/api/v1/stats.chunk_placement`
  (missing/undergoal/misplaced/stable/overgoal/pending_deletion). No separate
  `chunk_redundancy` aggregator was built; BT-4 consumes `chunk_placement`.
- **BT-4 ✅** Home "Chunk Placement" panel — stacked bar + legend over
  `stats.chunk_placement`.
- **BT-5 → Bug BY (filed).** Investigation found the WebUI render + empty-state
  are correct, but `scrub_results_by_node` is permanently empty because data
  nodes never report scrub RESULTS to the manager: the `ReportScrubResult` Raft
  command exists + is applied + surfaced by the API, but nothing produces it
  (no RPC, not in the heartbeat). Forked to Bug BY (data-node scrub-result
  reporting) — out of BT's WebUI scope.
- **BT-6 ✅** Release case `tests/release/cases/39-webui-replica-and-placement.sh`
  (PASS live) + case 36 covers the metric.
- **BT-7 ✅** Changelog landed.

Verification: webui units `chunk_placement_panel_present`,
`volume_modal_edits_mandatory_and_anchor` PASS; manager suite green; deployed
to the minicluster, case 39 PASS (all BT markers present in the served HTML).

---

## 0. Background

Three WebUI workstreams requested:

1. **Full control over mandatory / cache / anchor replicas in the WebUI.**
   The backend already exposes all four POST endpoints (atomic via Raft
   `UpsertVolume`): `/api/v1/volumes/{id}/set-mandatory`, `set-cache`,
   `set-anchor`, `set-schedules` (`api.rs:156-166`). But the WebUI volume-edit
   modal (Bug AV AV-4-4, `webui.rs:585-635`) edits **only `cache_replicas`**
   (warm/hot min/max + cooldown); its own help text says "Mandatory replicas and
   the legacy `replication` ... unchanged." So mandatory and anchor are not
   editable from the UI despite working APIs. This is a UI-wiring gap.

2. **MooseFS-style chunk-distribution metrics** (per the screenshot: a matrix /
   legend of chunk health — missing, endangered, undergoal, stable, overgoal,
   pending-deletion). DTFS uses replica copies + tiers (no EC), so the DTFS
   analogue is: classify every chunk by its **actual replica count vs its goal**
   (mandatory + configured cache target for the volume), bucketed into
   missing / endangered / undergoal / stable / overgoal, plus pending-deletion
   (orphans queued for GC). Today `/api/v1/stats` exposes only per-node
   `chunks_healthy` / `chunks_missing` (`api.rs:3051-3075`) — no cluster-wide
   redundancy distribution. Needs a backend aggregator + a WebUI panel.

3. **Verify last-scrub display.** Live `/api/v1/stats` returns
   `last_scrub:null` and `scrub_results_by_node:[]` even though scrubs run
   (the minicluster self-check loop completed 14:01-14:56). WebUI calls
   `renderScrub(stats.scrub_results_by_node || [])` (`webui.rs:867`). Either the
   scrub→heartbeat→Raft (`scrub_results_by_level`, Bug BK)→stats→webui path is
   not populating, or no scrub result is recorded on this cluster. Investigate
   before assuming a code fix.

---

## 1. Goals / non-goals

**Goals:**
- WebUI can set mandatory replicas (per tier) and the anchor policy, in addition
  to cache replicas (BT-1, BT-2).
- A cluster chunk-redundancy distribution metric (backend) + a WebUI panel that
  renders the buckets with a legend (BT-3, BT-4).
- Confirm last-scrub data renders; fix the path if broken (BT-5).

**Non-goals:**
- New replica *semantics* — only expose the existing
  mandatory/cache/anchor model (AGENTS.md §A.3.4) through the UI. The anchor
  invariant + AV class boundaries are unchanged.
- Erasure coding / the literal MooseFS RL matrix (DTFS has no EC). The DTFS
  metric is replica-count-vs-goal buckets.
- Per-file drill-down UI (cluster-level aggregate only for now).

---

## 2. Locked-in decisions

- **BT-1/BT-2 reuse existing endpoints.** No new Raft commands. The volume-edit
  modal gains a mandatory section (per-tier counts: hot/warm/cold/tape) wired to
  `set-mandatory`, and an anchor section (datacenter + required_tier) wired to
  `set-anchor`, alongside the existing cache fields (extend to all tiers, not
  just warm+hot). Each section submits to its own endpoint; show the server's
  validation errors inline (e.g. BM-3 unsatisfiable-tape rejection, anchor
  guards).
- **BT-3 aggregator is read-path, computed from `StateMachineData`.** For each
  chunk (group file chunk entries by chunk-hash / (file, index)), count distinct
  healthy replicas and compare to the volume goal (mandatory total; classify vs
  cache target for over/undergoal). Buckets:
  - `missing` (0 replicas), `endangered` (mandatory met but no redundancy
    margin — define as exactly 1 where goal > 1), `undergoal` (< goal),
    `stable` (== goal), `overgoal` (> goal), `pending_deletion` (orphan/GC
    queued). Exact thresholds finalized in BT-3 against the AV model; documented
    in the task. Counted once per logical chunk, not per replica.
- **BT-3 cost guard:** the walk is O(chunks). For large state, compute it in the
  same pass as other stats aggregation and cache within the existing stats
  response; do not add a separate full scan. If it proves heavy, gate behind a
  query param.
- **BT-5 is investigate-first.** Determine whether scrub results reach Raft
  state on the live cluster (trigger a scrub via CLI, re-query stats). If the
  path is broken, fix; if it is just empty (no scrub recorded yet), document the
  expected-populated shape and add the render/empty-state test. Split a separate
  bug only if a real defect in the BK path surfaces.

---

## 3. Data-model changes

- BT-3: new `/api/v1/stats` field, e.g. `chunk_redundancy`:
  `{ missing, endangered, undergoal, stable, overgoal, pending_deletion }`
  (computed; no `StateMachineData` field, no snapshot change).
- BT-1/BT-2/BT-5: none (reuse existing endpoints + existing
  `scrub_results_by_node` / `scrub_results_by_level`).

---

## 4. Task list

### BT-1: WebUI — edit mandatory replicas

- Status: ⬜
- Files: `crates/dtfs-manager/src/webui.rs` (volume-edit modal + submit JS).
- Sketch: add a "Mandatory replicas" section (per-tier number inputs:
  hot/warm/cold/tape) to the volume-edit modal; on Apply, POST to
  `/api/v1/volumes/{id}/set-mandatory`; surface server validation errors inline
  (e.g. unsatisfiable tape — BM-3). Pre-fill from `GET /api/v1/volumes/{id}`.
- Tests: webui.rs unit asserting the modal markup includes the mandatory inputs
  and the submit targets `set-mandatory`; live: change mandatory via UI, confirm
  `GET /api/v1/volumes/{id}` reflects it.
- Done when: mandatory replicas editable from the UI and persisted via Raft.

### BT-2: WebUI — edit anchor policy + full cache tiers

- Status: ⬜
- Files: `crates/dtfs-manager/src/webui.rs`.
- Sketch: add an "Anchor" section (datacenter + required_tier select) wired to
  `set-anchor`; extend the cache section to all tiers (cold/tape too), wired to
  `set-cache`. Inline validation errors (anchor guards).
- Tests: webui.rs unit for the new controls + submit targets; live: set anchor +
  cache from UI, confirm via `GET /api/v1/volumes/{id}`.
- Done when: anchor + full per-tier cache editable from the UI.

### BT-3: Backend — chunk-redundancy distribution aggregator

- Status: ⬜
- Files: `crates/dtfs-manager/src/api.rs` (`get_stats` + a pure helper, tested).
- Sketch: pure fn over `StateMachineData` that classifies each logical chunk by
  healthy-replica-count vs volume goal into the buckets in §2; add
  `chunk_redundancy` to the stats JSON. Reuse healthy-node determination
  consistent with `effective_health` / placement.
- Tests: unit with a constructed `StateMachineData` covering each bucket
  (0 → missing, 1<goal → undergoal/endangered, ==goal → stable, >goal →
  overgoal, orphan → pending_deletion).
- Done when: `/api/v1/stats.chunk_redundancy` returns correct counts on the unit
  fixtures and on the live cluster.

### BT-4: WebUI — chunk-distribution panel

- Status: ⬜
- Files: `crates/dtfs-manager/src/webui.rs`.
- Sketch: render `chunk_redundancy` as a summary panel + legend (missing /
  endangered / undergoal / stable / overgoal / pending-deletion), colour-coded
  like the reference (red/orange/yellow/green/blue/grey). Cluster-level totals;
  no per-file table for now.
- Tests: webui.rs unit asserting the panel consumes `chunk_redundancy` and
  renders each bucket; live: panel shows non-zero stable on the cluster.
- Done when: the panel renders the live distribution.

### BT-5: Verify (and fix if broken) last-scrub display

- Status: ⬜
- Files: investigate `crates/dtfs-manager/src/api.rs` (stats scrub aggregation),
  `crates/dtfs-data` (scrub → heartbeat), `crates/dtfs-raft` (BK
  `scrub_results_by_level`), `webui.rs` (`renderScrub`).
- Sketch:
  1. Trigger a scrub via CLI on the lab; re-query `/api/v1/stats`. Confirm
     whether `scrub_results_by_node` / `last_scrub` populate.
  2. If empty after a real scrub → trace the gap (data node records result? ships
     in heartbeat? applied to Raft? surfaced in stats?). Fix the broken hop.
  3. If populated → confirm WebUI renders it; add a render + empty-state unit
     test. Document the populated shape.
- Tests: unit for the stats scrub aggregation (populated → rendered);
  live: a freshly-triggered scrub shows in the WebUI scrub card.
- Done when: a real scrub is visible in the WebUI; or, if a defect is found, a
  separate bug is filed and BT-5 records the handoff.

### BT-6: Release cases

- Status: ⬜
- Files: `tests/release/cases/NN-*.sh`.
- Cases: (a) set mandatory+anchor via the volume endpoints and confirm
  persistence (covers BT-1/BT-2 backend contract); (b)
  `/api/v1/stats.chunk_redundancy` present and self-consistent (sum == total
  chunks); (c) triggered scrub appears in stats (BT-5).
- Done when: cases PASS or `KNOWN_ISSUE [BT]` pinned.

### BT-7: Changelog + docs

- Status: ⬜
- Files: `changelog/UNRELEASED.md`, `docs/src/...` (WebUI/ops reference).
- Entries: UI replica control (mandatory/cache/anchor), chunk-distribution
  metric, scrub-display fix. Same commit as the code.

---

## 5. Cross-cutting invariants

- UI replica edits go through the existing Raft `UpsertVolume` endpoints — never
  a side path that skips the AV class boundaries or anchor guards (AGENTS.md
  §A.3.4). Server-side validation (BM-3 tape, anchor) stays authoritative; UI
  only surfaces errors.
- `chunk_redundancy` counts each logical chunk once (by chunk-hash / file+index),
  not per replica; buckets are mutually exclusive and sum to total chunks.
- Stats aggregation must not add a second full state-machine scan (fold into the
  existing pass).

---

## 6. Verification log

> Append per-task.

---

## 7. Deviations from plan

> Append if reality diverges (esp. BT-5: whether it stays a verify task or forks
> into a separate scrub-path bug).
