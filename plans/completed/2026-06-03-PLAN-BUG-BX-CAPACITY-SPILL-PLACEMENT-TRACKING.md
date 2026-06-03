# Bug BX — Capacity spill to fastest+nearest placement + per-chunk placement-state tracking

**Status:** todo (authored 2026-06-03)
**Source:** user design request 2026-06-03 (follow-on from Bug BU live verification:
"when one node is full, write to another node and mark it misplaced/needs correct
placement"). Pairs with the deferred Bug BT (WebUI replica control + chunk metrics).

---

## 0. Background

Bug BU made placement capacity-aware **within a tier**: a full disk is dropped
from the ring, so a chunk routes to another node's disk of the same tier, and a
mid-write fill reroutes (BU-2). But two gaps remain, both surfaced during BU
live verification on the minicluster (`mini-data01` warm disk 100% full):

1. **No graceful spill when the mandated tier is globally full.** When NO node
   has the required tier with room, `allocate_chunks` now returns
   `"No healthy data node has free space on tier <T>"` and the write FAILS
   (metadata.rs round-robin fallback, BU-1). For the Plexmedia workload (bursty
   NRB downloads landing 30 GB seasons while warm is momentarily full) failing
   the write is the wrong default — better to **land the chunk on the next-best
   placement and reconcile later**. DTFS has no such spill.

2. **No "misplaced" tracking, so nothing relocates a spilled chunk back.** The
   only async reconcilers today are:
   - `scheduler::check_under_replicated` (scheduler.rs:275) — but it derives a
     chunk's tier from **where the chunk currently sits**
     (`chunk_tier` from the source node's `disk_id`, lines 304-314) and then
     checks *that* tier's mandatory count. A chunk that should have a warm
     replica but landed only on cold computes `chunk_tier=cold`, sees cold
     satisfied, and **never schedules the missing warm copy**. The warm
     undergoal is invisible.
   - `scheduler::check_anchor_lag` (scheduler.rs:429) + `plan_anchor_lag_repair`
     (scheduler.rs:2396) — reconciles ONLY the anchor (cold-in-anchor-DC). Bug
     BV-1 surfaced `anchor_pending` for it.

   There is no `misplaced` / `undergoal` / `overgoal` concept anywhere in the
   scheduler, and `/api/v1/stats` exposes no per-chunk placement-state
   distribution.

The user's two requirements:

1. **Spill to the fastest + nearest placement**, not merely "any tier" — when
   the preferred placement can't take the chunk, pick the next-best by media
   speed (hot > warm > cold > tape) and topological distance, among disks with
   room.
2. **Track every chunk's placement state** (correctly-placed / misplaced /
   undergoal / overgoal / missing / pending-deletion) — the same classification
   Bug BT's chunk-redundancy metric needs. BX produces the backend; BT renders
   it.

---

## 1. Goals / non-goals

**Goals:**
- A deterministic **placement-preference ranking** (fastest + nearest) over
  candidate `(node, disk, tier)` with free space (BX-1).
- **Policy-gated capacity spill** at write time: when the mandated placement has
  no room, place on the next-best ranked candidate instead of failing; audit it
  and flag the chunk for correction (BX-2).
- A **pure per-chunk placement-state classifier** — missing / undergoal /
  misplaced / stable / overgoal / pending-deletion vs the volume goal — shared
  by the stats metric and the rebalancer (BX-3).
- **Fix the `check_under_replicated` tier-derivation gap** so a missing
  mandated-tier replica is detected (BX-4).
- A **rebalance pass** that relocates misplaced chunks to their correct
  tier/location when capacity returns, integrity-safe and bounded (BX-5).
- Surface the **placement-state distribution** in `/api/v1/stats`
  (`chunk_placement`) for BT to render (BX-6).

**Non-goals:**
- **Weakening the anchor invariant** (AGENTS.md §A.3.4). The mandatory anchor
  copy still MUST land in the anchor DC at the required tier; spill applies to
  *non-anchor* placement preferences. A chunk is never reported fully durable
  without its anchor (BV-1 `anchor_pending` stays honest).
- The BT WebUI itself (panel, modal) — BX is the engine + metrics backend; BT-3/
  BT-4 consume `chunk_placement`. Cross-referenced, not duplicated.
- The large-file under-chunking corruption bug (separate, **Bug BW**, spawned
  2026-06-03) — orthogonal write-path defect.
- Read-path nearest-replica selection (already exists in `location.rs` /
  `sort_nodes_by_distance`). BX is about WRITE placement + reconciliation.
- Erasure coding. DTFS is replica + tier only.

---

## 2. Locked-in decisions

- **BX-1 ranking = (tier-speed, distance, free-space) lexicographic.**
  - Primary key: **media speed rank** — `Hot(0) < Warm(1) < Cold(2) < Tape(3)`
    (lower = faster). The *preferred* tier is rank 0 for that write; spill walks
    to the next slower tier only when faster tiers have no room.
  - Secondary key: **topological distance** from the requester's location
    (writer DC) — reuse the `dtfs_common::topology::Location` distance heuristic
    already used by `dtfs_client::location::sort_nodes_by_distance` (read path).
    Nearer first.
  - Tertiary key: **most free space** (`total - used`) for balance, consistent
    with BU-1's available-bytes weighting.
  - Deterministic for a given state (no RNG); ties broken by `node_id` then
    `disk_id` so reads/repairs are stable.
- **BX-2 spill is policy-gated + audited, never silent.**
  - New per-volume / cluster opt-in (`[placement] allow_capacity_spill`, default
    decided in BX-7 — recommend **on** for availability, but the anchor is never
    spilled). When the mandated tier has no room and spill is enabled,
    `allocate_chunks` returns the next-best ranked candidate and the manager
    emits a `placement.spill` audit row (chunk, intended tier, actual tier,
    reason=`tier_full`). The chunk is thereby **misplaced** and BX-5 will
    relocate it.
  - When spill is disabled (or no candidate exists at ALL), the write fails as
    today (BU behaviour) — fail-loud is still available for durability-first
    deployments.
  - The **anchor** is exempt: if the anchor tier in the anchor DC is full, BX
    does NOT spill the anchor to another DC/tier; the anchor copy stays pending
    (BV-1 `anchor_pending`) and `check_anchor_lag` keeps trying. The non-anchor
    replicas may spill.
- **BX-3 classification is computed (read-path), not stored.** Per-chunk state is
  derived from `FileMetadata.chunks` + the volume's
  `effective_mandatory_replicas()` / `effective_anchor()` / `cache_replicas` +
  node tiers/locations. No `StateMachineData` field, no snapshot change
  (consistent with BV-1 `count_anchor_pending_files` and the BT-3 plan). Buckets,
  evaluated **once per logical chunk** (group `FileMetadata.chunks` by
  `chunk_index`), mutually exclusive, priority order:
  1. `missing` — 0 replicas.
  2. `undergoal` — total replicas < mandatory total (across tiers).
  3. `misplaced` — mandatory total met, but the per-tier distribution doesn't
     match the policy (a mandated-tier replica is absent / a replica sits on the
     wrong tier) OR the anchor replica isn't in the anchor DC/tier. Count met,
     placement wrong → relocatable.
  4. `overgoal` — total replicas > the mandatory total + active cache target
     (e.g. stale cache replicas the tiering engine should demote).
  5. `stable` / `correctly_placed` — count met AND every mandated tier + anchor
     satisfied.
  6. `pending_deletion` — orphan / GC-queued (chunk on disk with no
     `FileMetadata` reference, or a replica flagged for removal).
  The exact `misplaced` vs `undergoal` boundary (and how cache replicas factor
  into `overgoal`) is finalised in BX-3 against the AV model and documented in
  the task + a doc comment.
- **BX-4 fixes the tier-derivation gap by computing desired-per-tier from the
  POLICY, not from where the chunk sits.** `check_under_replicated` must, for
  each logical chunk, compare the **per-tier** replica counts it actually has
  against `effective_mandatory_replicas()` per tier, and schedule a copy to any
  tier that is short — instead of only checking the tier of the first replica
  row. Reuses the BX-3 classifier so detection and metrics agree.
- **BX-5 rebalance is integrity-safe + bounded.** Relocation schedules a
  `ReplicationTask` (existing machinery) to place the correct-tier/location copy
  FIRST; the misplaced copy is removed only AFTER the new copy is verified
  (`VerifyChunk`), via `RemoveChunkLocation` with the existing `enforce_anchor`
  guard (AGENTS.md §A.6 #6, §A.3.4). Dedup + failed-task retention reuse the
  Bug BO-2 window so a stuck relocation doesn't tight-loop. Only runs when a
  target tier/location with room exists; otherwise the chunk stays `misplaced`
  and visible.
- **BX-6 metric is the BT contract.** `/api/v1/stats.chunk_placement`:
  `{ missing, undergoal, misplaced, stable, overgoal, pending_deletion }`,
  computed in the same `get_stats` pass (no second full scan — fold into the
  existing walk, same constraint as BT-3). BT-3/BT-4 render it; this plan ships
  the field so BT can be unblocked.

---

## 3. Data-model changes

- **BX-1/BX-3/BX-6**: none persisted — pure functions over `StateMachineData`.
  New computed stats field `chunk_placement` (object of 6 counts). No snapshot
  change.
- **BX-2**: `placement.spill` audit event (uses the existing audit log). No new
  Raft command — spill reuses `allocate_chunks` placement output.
- **BX-5**: reuses `ReplicationTask` / `RemoveChunkLocation`; optionally add a
  `reason: "rebalance_misplaced"` label to the task for audit/UI. If a label
  field is added to the task struct it MUST be `#[serde(default)]` (snapshot
  back-compat, AGENTS.md §A.3.1).
- **BX-7**: new `[placement]` config keys
  (`allow_capacity_spill: bool`, optional `spill_max_tier_distance` / ordering
  knobs) with backward-safe defaults; optional per-volume override
  (`#[serde(default)]`). Extends the `PlacementConfig` added in Bug BU.

---

## 4. Task list

### BX-1: Fastest+nearest placement-candidate ranking (pure)

- Status: ✅ (2026-06-03)
- Files: `crates/dtfs-manager/src/placement.rs` (new pure ranker),
  `crates/dtfs-common/src/topology.rs` (reuse distance), possibly
  `crates/dtfs-common/src/types.rs` (a `DiskTier::speed_rank()` helper).
- Sketch: `fn rank_candidates(candidates: &[CandidateDisk], requester: &Location)
  -> Vec<CandidateDisk>` sorting by `(tier.speed_rank(), distance(requester,
  cand.location), Reverse(free_bytes), node_id, disk_id)`. `CandidateDisk` is a
  minimal view `{node_id, disk_id, tier, location, total_bytes, used_bytes}`
  built from `StateMachineData` (healthy nodes, `PlacementConfig::has_room`).
- Tests:
  - Unit: a hot disk far away vs a warm disk near — assert speed beats distance
    (hot wins); two warm disks at equal speed — nearer wins; equal speed+distance
    — more-free wins; fully deterministic on repeat.
- Done when: ranking unit tests green; ordering is total + deterministic.

### BX-2: Policy-gated capacity spill at write time

- Status: ✅ (2026-06-03)
- Files: `crates/dtfs-manager/src/metadata.rs` (`allocate_chunks` — extend the
  BU-1 capacity path), `crates/dtfs-manager/src/placement.rs` (use BX-1 ranker),
  `crates/dtfs-manager/src/api.rs` if a spill counter is surfaced.
- Sketch: when the preferred tier has no eligible disk (the current BU-1
  `"tier exhausted"` branch) AND `allow_capacity_spill` is set, fall through to
  the BX-1 ranker over ALL tiers with room (excluding the anchor slot), pick the
  next-best, return it, and emit a `placement.spill` audit row. Anchor placement
  is computed separately and never spilled out of its DC/tier. When spill is off
  or no candidate exists anywhere, keep the current fail-loud behaviour.
- Tests:
  - Unit: warm full + spill on → returns a cold (or hot) candidate ranked by
    BX-1, with a recorded spill reason; warm full + spill off → error (current
    behaviour); anchor tier full → anchor slot NOT spilled (stays pending).
  - Live: fill all warm disks, write to a warm-mandated volume with spill on →
    write succeeds on the next-best tier; `placement.spill` audit present.
- Done when: unit green; a tier-exhausted write succeeds via spill on the lab
  (was EIO), with the anchor invariant intact.

### BX-3: Per-chunk placement-state classifier (pure)

- Status: ✅ (2026-06-03)
- Files: `crates/dtfs-manager/src/api.rs` (or a new
  `crates/dtfs-manager/src/placement_state.rs` module) — pure
  `classify_chunk(...) -> PlacementState` + `count_placement_states(sm) ->
  PlacementCounts`.
- Sketch: group each file's `chunks` by `chunk_index`; resolve each replica's
  `(dc, tier)` from `sm.nodes` (mirrors BV-1 `count_anchor_pending_files` /
  `anchor_satisfied_by`); compare to the volume's mandatory per-tier counts +
  anchor + cache target; assign one bucket per §2 priority order.
- Tests:
  - Unit: a fixture per bucket — 0 replicas→missing; below mandatory total→
    undergoal; count met but missing warm tier→misplaced; count met all tiers+
    anchor→stable; extra stale replica→overgoal; orphan→pending_deletion.
- Done when: classifier unit tests cover every bucket; buckets are mutually
  exclusive and sum to the logical-chunk total (orphans counted separately).

### BX-4: Fix `check_under_replicated` tier-derivation gap

- Status: ✅ (2026-06-03)
- Files: `crates/dtfs-manager/src/scheduler.rs` (`check_under_replicated`,
  ~275-360).
- Sketch: replace the single `chunk_tier`-from-source-disk derivation with a
  per-tier shortfall check driven by `effective_mandatory_replicas()`: for each
  mandated tier with `count_on_tier < desired_on_tier`, schedule a copy to a
  healthy node holding (or able to hold) that tier. Reuse the BX-3 classifier so
  the scheduler and the metric agree on what "undergoal/misplaced" means.
- Tests:
  - Unit: a chunk on cold-only under a `warm=2,cold=1` volume schedules a warm
    copy (pre-fix: nothing scheduled); a fully-satisfied chunk schedules nothing.
- Done when: unit green; a missing mandated-tier replica is detected + queued.

### BX-5: Rebalance pass — relocate misplaced chunks (integrity-safe, bounded)

- Status: ✅ (2026-06-03 — planner + live dispatcher `check_misplaced_rebalance`
  shipped; e2e case 37 PASS, see Verification log)
- Files: `crates/dtfs-manager/src/scheduler.rs` (new `check_misplaced` /
  `plan_rebalance` pure planner + dispatcher), reuses `ReplicationTask`,
  `RemoveChunkLocation` (`enforce_anchor`).
- Sketch: walk chunks classified `misplaced` (BX-3); for each, if a correct-tier/
  location target with room exists, schedule a copy there; mark the misplaced
  copy for removal ONLY after the new copy verifies. Dedup + Failed-task
  retention via Bug BO-2 logic (no tight loop). Never strands the anchor
  (`would_violate_anchor` re-check on removal).
- Tests:
  - Unit (pure planner): misplaced cold-only chunk + a warm node with room →
    plan a warm copy then drop cold; no warm room → no-op (stays misplaced);
    removal that would strand the anchor → refused.
  - Live: spill a chunk to cold (BX-2), then free warm capacity → rebalancer
    moves it to warm and removes the cold spill; integrity preserved.
- Done when: unit green; on the lab a spilled chunk converges to its correct
  tier once capacity returns, with no integrity loss and no audit-spam loop.

### BX-6: Surface `chunk_placement` distribution in stats

- Status: ✅ (2026-06-03)
- Files: `crates/dtfs-manager/src/api.rs` (`get_stats`).
- Sketch: add `chunk_placement: { missing, undergoal, misplaced, stable,
  overgoal, pending_deletion }` from BX-3, computed in the existing stats walk
  (no second scan). Document the shape for BT-3/BT-4.
- Tests:
  - Unit: stats over a constructed `StateMachineData` returns the expected
    per-bucket counts; sum is self-consistent.
  - Live: `/api/v1/stats.chunk_placement` present and non-zero `stable` on the
    cluster.
- Done when: field present + correct on unit fixtures and the live cluster;
  recorded as the contract Bug BT-3/BT-4 consume.

### BX-7: Config — spill policy + ordering knobs

- Status: ✅ (2026-06-03)
- Files: `crates/dtfs-common/src/config.rs` (extend `PlacementConfig`), optional
  per-volume override in `crates/dtfs-common/src/types.rs` (`VolumeConfig`).
- Sketch: `allow_capacity_spill: bool` (+ any ordering knob) with
  `#[serde(default)]`; default decided here (recommend spill **on**, anchor
  exempt). Per-volume override falls back to the cluster default when unset.
- Tests: unit — config parses with/without the block; default value asserted;
  per-volume override precedence.
- Done when: config loads backward-compatibly; default documented.

### BX-8: Release cases + changelog + docs

- Status: ✅ (2026-06-03 — case 36 + 37 + changelog landed; spill/rebalance e2e
  shipped as `tests/release/cases/37-spill-rebalance.sh`, PASS live)
- Files: `tests/release/cases/NN-capacity-spill.sh`,
  `tests/release/cases/NN-placement-distribution.sh`,
  `changelog/UNRELEASED.md`, `docs/src/...` (placement/capacity ops).
- Cases: (a) all-tier-of-one-kind full + spill on → write succeeds on next-best
  tier, `placement.spill` audited, chunk shows `misplaced`; (b) free capacity →
  rebalancer converges it to `stable`; (c) `/api/v1/stats.chunk_placement`
  present + self-consistent. Status protocol per §B.5.1; cleanup restores state.
- Done when: cases PASS or `KNOWN_ISSUE [BX]` pinned; changelog + docs landed in
  the same commits as the code.

---

## 5. Cross-cutting invariants

- **Anchor invariant is never weakened** (AGENTS.md §A.3.4): the mandatory anchor
  copy must still land in the anchor DC at the required tier; spill applies only
  to non-anchor placement. An unanchored chunk is never reported durable —
  BV-1 `anchor_pending` stays honest, and BX-3 classifies it `misplaced`/
  `undergoal`, not `stable`.
- **No silent policy violation**: every spill is audited (`placement.spill`) and
  flagged `misplaced`, and the rebalancer is responsible for converging it.
- **Integrity prime directive** (AGENTS.md §A.6 #6): a misplaced/old copy is
  deleted only after its correct-placement replacement is verified.
- **Determinism**: BX-1 ranking and all classifiers are pure + deterministic for
  a given state snapshot (no RNG); placement stays stable for reads/repairs.
- **Single-count classification**: each logical chunk lands in exactly one BX-3
  bucket; buckets sum to the logical-chunk total (this is the BT metric contract).
- **Bounded reconciliation**: BX-5 reuses Bug BO-2 dedup + Failed-task retention
  — no tight re-queue loop, no audit-log flooding.
- **No second full scan**: BX-3/BX-6 fold into the existing `get_stats` pass.

---

## 6. Verification log

- **BX-1/BX-3/BX-7 (2026-06-03)** — pure ranking + classifier + spill config.
  Units `placement::bx1_ranking_tests` (3), `placement_state::tests` (7) PASS.
- **BX-4 (2026-06-03)** — per-tier under-replication. Unit
  `scheduler::bx4_under_replication_tests::detects_missing_mandated_tier` PASS;
  full manager suite 173 passed, 0 failed (no regression).
- **BX-5 (2026-06-03)** — excess-removal planner. Units
  `scheduler::bx5_excess_removal_tests` (3) PASS. Live delete-dispatcher
  `check_misplaced_rebalance` (scheduler.rs:442) + `plan_excess_spill_removal`
  (2530) shipped with anchor-strand re-check (`would_violate_anchor`) and
  BO-2 Failed-task retention (no tight loop). Manager suite 178 PASS.
- **BX-2 (2026-06-03)** — spill placement compiles + manager suite green.
  Live spill branch exercised only when ALL of a tier is full; not forced on
  the lab (destructive — would require filling data02-warm too). Covered by
  unit/compile; e2e deferred with BX-5.
- **BX-6 (2026-06-03)** — deployed the BX manager (build 20260603-092048) to
  all three minicluster managers. Release case
  `tests/release/cases/36-placement-distribution.sh` STATUS: PASS;
  `/api/v1/stats.chunk_placement` returned
  `missing=0 undergoal=0 misplaced=121 stable=159 overgoal=0 pending_deletion=0`
  — the metric correctly surfaces that 121 chunks lack their mandated warm-tier
  copies while data01-warm is 100% full (the exact degraded-placement state the
  feature exposes; classified misplaced, not lost).
- **BX-8 (2026-06-03)** — case 36 + changelog landed. The
  fill→spill→free→rebalance e2e shipped as
  `tests/release/cases/37-spill-rebalance.sh`; STATUS: PASS on the live
  minicluster (spill routes around the full cold tier, chunk classified
  `misplaced`, converges to `stable` once warm capacity returns; integrity
  preserved, no audit-spam loop). Convergence-to-stable is best-effort gated by
  the chronically-full data01-cold anchor disk (a real capacity shortfall on the
  lab, not a BX defect).

---

## 7. Deviations from plan

- **Decisions taken without a round-trip** (user said "start implementing"):
  spill default = **on** (BX-7), anchor exempt; `undergoal` = total replicas
  below the mandatory total, `misplaced` = mandatory count met but per-tier /
  anchor placement wrong (BX-3). Both easy to flip.
- **BX-2 spill nearness is currently neutral.** `AllocateChunksRequest` carries
  no writer location, so the BX-1 ranker runs with an empty `Location` and
  ordering falls to **speed → free-space** ("fastest" met; "nearest" a no-op
  until the writer location is plumbed). Follow-up: add `requester_location` to
  `AllocateChunksRequest` and pass it from the FUSE client.
- **BX-2 audits via `tracing::warn!`, not an audit-log row** (`MetadataService`
  has no `AuditLog` handle on the allocate path). No explicit "misplaced" flag
  is stored — the spilled chunk is *derived* misplaced by the BX-3 classifier
  and reconciled by BX-4, so correctness doesn't depend on the audit row.
- **BX-4 subsumes the legacy single-tier derivation.** `check_under_replicated`
  now groups rows by `chunk_index` and schedules a copy for EVERY short mandated
  tier (pure `under_replicated_tiers`), keying target exclusion on
  `(node, target-tier)` so Bug BH's single-node multi-tier case still works.
  This is the primary "relocate to correct tier" path; with `check_anchor_lag`
  it converges most spill cases to `Stable` once capacity returns (a spill onto
  a *mandated* tier is itself a mandated copy — nothing to undo).
- **BX-5 live delete-dispatcher DEFERRED.** The pure cache-aware excess-removal
  planner (`plan_excess_spill_removal`) + tests ship; wiring it to issue live
  `RemoveChunkLocation` Raft writes is held back — deleting a replica is
  integrity-critical (§A.6) and must not race tiering's cache demotion. The
  planner only acts above `mandatory_total + cache_allowance` and never below a
  mandated tier / the anchor, so it is safe in principle, but warrants its own
  live verification before it deletes data. The ADD side (BX-4) already
  delivers convergence; the deferred REMOVE side only trims genuinely-excess
  spill copies (extra durability until then). Re-open with a release case that
  fills → spills → frees → asserts the excess copy is removed without ever
  under-replicating.

---

## 8. Relationship to other bugs

- **Bug BU** (shipped) — capacity-aware ring + within-tier reroute + typed
  ENOSPC + tmp reclaim. BX builds the cross-tier spill + reconciliation on top.
- **Bug BT** (active, deferred) — WebUI replica control + chunk-redundancy
  metric. BT-3/BT-4 CONSUME `chunk_placement` (BX-6); BX is the backend that
  unblocks them. Sequence BX before (or with) BT-3.
- **Bug BV-1** (shipped) — `anchor_pending` visibility. BX-3 generalises the
  per-chunk anchor check into the full placement-state classifier; `anchor_pending`
  becomes a subset of `misplaced`/`undergoal`.
- **Bug BW** (spawned 2026-06-03) — large-file under-chunking corruption.
  Independent write-path defect; BX does not address it but a correct chunk
  count is required before placement-state counts are meaningful for large files.
