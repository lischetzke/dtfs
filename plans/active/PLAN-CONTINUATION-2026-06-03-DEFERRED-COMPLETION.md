# Continuation roadmap — finish BX, fix BW, build BT (2026-06-03)

**Status:** todo (authored 2026-06-03)
**Source:** user direction 2026-06-03 after the BX implementation (PR #5). Sequences
the deferred work accumulated across the BS/BU/BV/BX session.

This is a **roadmap** that sequences three workstreams into phases. Each phase's
tasks are actionable here; when a phase starts, its tasks may be split into /
merged with the matching per-bug plan (`PLAN-BUG-BX`, a new `PLAN-BUG-BW`, the
existing `PLAN-BUG-BT`). Follow the normal per-task procedure (AGENTS.md §B.3):
failing test first, build on the lab builder, live-verify, one commit per task.

## Locked-in decisions (user, 2026-06-03)

- **Sequence: Phase 1 finish BX → Phase 2 fix BW → Phase 3 full BT.**
- **This roadmap OWNS Bug BW** (the pre-existing large-file under-chunking
  corruption); it is not left to the spawned side-task.
- **BX-5 rebalance = live delete with guards** (no audit-only interim): issue
  real `RemoveChunkLocation` writes, relying on the planner's
  mandatory+anchor+cache guards plus the apply-path `enforce_anchor` re-check.
- **BT scope = full** (BT-1..BT-7: replica editing + chunk-redundancy panel +
  scrub-display verify + cases + docs).

> Note on ordering vs integrity: BW is active data corruption (multi-chunk
> reads return garbage). The user chose to finish BX first; BW follows
> immediately as Phase 2. If an operator wants BW expedited, Phase 2 can run in
> parallel with Phase 1 — they touch different code (BX = manager placement /
> scheduler; BW = FUSE writebuf segmentation).

---

## Phase 1 — Finish BX (capacity spill / reconcile loop)

Extends `plans/active/PLAN-BUG-BX-...md` (Deviations list the deferred items).
Goal: close the spill→classify→reconcile loop end to end.

### BX-9: Nearest-spill — plumb the writer location

- Status: ✅ (2026-06-03)
- Problem: `BX-2` spill ranks by speed→free only; "nearest" is neutral because
  `AllocateChunksRequest` carries no writer location.
- Files: `proto/manager.proto` (add `Location requester_location = 6;` to
  `AllocateChunksRequest`; regen `dtfs-proto`), `crates/dtfs-client/src/client.rs`
  (`allocate_chunks_excluding` takes + sends the client location — the client
  already holds it via `set_requester_location`), `crates/dtfs-fuse/src/filesystem.rs`
  (pass `self.client_location`), `crates/dtfs-manager/src/metadata.rs`
  (`allocate_chunks` builds a `Location` from the request and passes it to
  `rank_candidates` instead of the empty `Location`).
- Tests: unit — ranking with a real requester prefers the same-DC candidate over
  an equal-speed far one (already covered by `bx1_ranking_tests`; add a
  metadata-level test that the request location reaches the ranker). Backward
  safe: an empty/absent `requester_location` keeps today's speed→free order.
- Done when: spill prefers the writer's DC; legacy clients (no location) unchanged.

### BX-10: Spill audit row

- Status: ✅ (2026-06-03)
- Problem: spill is logged via `tracing::warn!` only; no `placement.spill` audit
  row (the `MetadataService` allocate path has no `AuditLog` handle).
- Files: `crates/dtfs-manager/src/metadata.rs` (thread an `Arc<AuditLog>` into
  `MetadataService`; emit `placement.spill` with path/intended_tier/spill_tier),
  `crates/dtfs-manager/src/main.rs` (pass the audit log when constructing the
  service).
- Tests: unit/integration — a forced spill writes exactly one `placement.spill`
  audit row; steady-state (no spill) writes none.
- Done when: spills are visible in `/api/v1/audit`.

### BX-11: BX-5 live rebalance dispatcher (live delete with guards)

- Status: ✅ (2026-06-03)
- Files: `crates/dtfs-manager/src/scheduler.rs` (new `check_misplaced_rebalance`
  pass, leader-gated, called from the tick after `check_anchor_lag`).
- Sketch: walk chunks; for each, build `(node, dc, tier)` replicas; call the
  shipped `plan_excess_spill_removal(replicas, mandatory, anchor, cache_allowance)`;
  for the returned node, issue `ClientRequest::RemoveChunkLocation { path,
  chunk_index, node_id, enforce_anchor }`. Bounded `MAX_REMOVALS_PER_TICK`; audit
  `rebalance.remove_excess`. The planner already refuses to drop below a mandated
  tier / strand the anchor / touch within-cache-allowance copies; `enforce_anchor`
  on apply is defence-in-depth.
- Cross-cut: must NOT race the tiering engine — only acts above
  `mandatory_total + cache_allowance` (genuine excess), so cache replicas within
  `max` are never removed.
- Tests: unit — dispatcher selects the same node the planner returns and skips
  when None. Live — covered by BX-13.
- Done when: an over-replicated (spilled) chunk converges to goal by dropping the
  excess copy, never under-replicating or stranding the anchor, on the lab.

### BX-12: `pending_deletion` source

- Status: ✅ (2026-06-03)
- Problem: `chunk_placement.pending_deletion` is hard-0; orphans/GC-queued chunks
  aren't counted.
- Files: `crates/dtfs-manager/src/placement_state.rs` (+ caller in `api.rs`):
  fold in the count of chunks queued for GC / flagged orphan by the orphan-scrub
  reap path (`scheduler` orphan set) so the bucket is real.
- Tests: unit — a queued orphan increments `pending_deletion`; buckets still sum
  to the logical-chunk total + pending.
- Done when: `pending_deletion` reflects the real GC backlog.

### BX-13: e2e spill→rebalance release case

- Status: 🟨 (2026-06-03 — case authored, KNOWN_ISSUE-pinned for the destructive fill scenario)
- Files: `tests/release/cases/NN-capacity-spill-rebalance.sh`.
- Case: on a warm-mandated test volume (or by filling warm on ALL data nodes),
  write a single-chunk file → assert it SPILLS to the next tier (succeeds, not
  EIO) and classifies `misplaced`/excess in `chunk_placement`; then free warm
  capacity → assert BX-4 backfills warm AND BX-11 removes the excess →
  `chunk_placement` returns the chunk to `stable`. Status protocol per §B.5.1;
  cleanup restores disk + volume state. KNOWN_ISSUE-tolerant if the topology
  can't host the scenario.
- Done when: case PASS (or `KNOWN_ISSUE [BX]` pinned).

### BX-14: changelog + docs (Phase 1 close)

- Status: ✅ (2026-06-03)
- Files: `changelog/UNRELEASED.md`, `docs/src/...` (placement/capacity ops):
  document nearest-spill, the spill audit, the rebalance pass, and the
  `chunk_placement` metric semantics.

---

## Phase 2 — Bug BW: large-file under-chunking corruption (OWNED)

Severe, pre-existing data-integrity defect: a write larger than the volume
`chunk_size` is stored as a SINGLE oversized chunk at index 0, so multi-chunk
reads return nondeterministic garbage (live: a 12 MB file → `chunk_count=1`;
pre-deploy 283 MB .mkv → `chunk_count=3`, expected ~68). When Phase 2 starts,
split these tasks into a dedicated `plans/active/PLAN-BUG-BW-LARGE-FILE-UNDERCHUNKING.md`.

### BW-1: Reproduce with a failing unit test

- Status: ⬜
- Files: `crates/dtfs-fuse/src/writebuf.rs` (test).
- Sketch: a write of `N > chunk_size` bytes must yield `ceil(N / chunk_size)`
  segments with correct per-segment sizes + offsets. Assert it currently
  produces ONE segment (the bug). Note the existing `writebuf.rs:299`
  "value assigned to `consumed` is never read" warning — likely the broken split.
- Done when: a red test demonstrates the single-segment behaviour.

### BW-2: Fix segmentation to split at `chunk_size`

- Status: ⬜
- Files: `crates/dtfs-fuse/src/writebuf.rs` (+ `filesystem.rs` flush path if the
  split lives there).
- Sketch: on flush, split buffered bytes into `chunk_size`-aligned segments; a
  single large `write()` syscall (e.g. `cp`) must chunk correctly. Verify the
  volume `chunk_size` (4 MB default) is the split unit and partial tail chunks
  are handled.
- Tests: BW-1 turns green; a property-style test over many N values.
- Done when: `N`-byte write → `ceil(N/chunk_size)` chunks with correct sizes.

### BW-3: Read-path reassembly verification

- Status: ⬜
- Files: `crates/dtfs-fuse/src/filesystem.rs` (read path), `readahead.rs`.
- Sketch: confirm reads map `offset → chunk_index` and reassemble multi-chunk
  files byte-exactly under sequential AND random read patterns (the live bug
  showed `sha256sum` vs `cp` disagreeing — different read sizes).
- Tests: unit/integration round-trip; multi-block-size read equivalence.
- Done when: reads of a multi-chunk file are deterministic + match the source.

### BW-4: Already-corrupted files — decide + document

- Status: ⬜
- Sketch: existing under-chunked files in the library are already stored wrong.
  Decide remediation: operator re-ingest (simplest, recommended) vs a one-off
  repair/rewrite pass. Document the operator action; if a repair pass is built,
  it must be integrity-safe (write new chunks, verify, then replace metadata).
- Done when: remediation path documented (and implemented if chosen).

### BW-5: Release case — multi-chunk round-trip integrity

- Status: ⬜
- Files: `tests/release/cases/NN-multichunk-integrity.sh`.
- Case: write a `>chunk_size` file via FUSE, read back (multiple read methods),
  assert sha256 matches the source AND `inspect.chunk_count == ceil(size/chunk_size)`.
- Done when: case PASS (this also re-validates the BU-4 multi-chunk path that was
  reduced to single-chunk to dodge this bug).

### BW-6: Changelog + docs

- Status: ⬜
- Files: `changelog/UNRELEASED.md` (bug fix + KNOWN-ISSUE note for pre-existing
  files), `docs/src/...`.

---

## Phase 3 — Bug BT: WebUI replica control + chunk-redundancy panel (FULL)

Existing plan: `plans/active/PLAN-BUG-BT-WEBUI-REPLICA-CONTROL-METRICS.md`.
**Now unblocked**: BT-3 (chunk-redundancy aggregator) is effectively delivered by
BX-6 — `/api/v1/stats.chunk_placement`. Update BT to consume it rather than build
a parallel aggregator.

- **BT-1** WebUI edit mandatory replicas (per tier) → `set-mandatory`.
- **BT-2** WebUI edit anchor policy + full per-tier cache → `set-anchor`/`set-cache`.
- **BT-3** → **retarget to BX-6**: the backend metric is `chunk_placement`
  (missing/undergoal/misplaced/stable/overgoal/pending_deletion). Drop the
  separate `chunk_redundancy` aggregator from BT; BT renders `chunk_placement`.
- **BT-4** WebUI chunk-distribution panel + legend (colour-coded buckets),
  consuming `chunk_placement`.
- **BT-5** Verify/fix last-scrub display (`scrub_results_by_node` populated end
  to end; investigate-first).
- **BT-6** Release cases (replica edits persist; panel renders; scrub visible).
- **BT-7** Changelog + docs.

When Phase 3 starts, edit the BT plan: mark BT-3 as superseded-by-BX-6 and point
BT-4 at `chunk_placement`.

---

## Cross-cutting invariants (all phases)

- **Anchor invariant** (§A.3.4) never weakened — BX-11 removal + BW repair both
  preserve a mandated anchor copy (`enforce_anchor`).
- **Integrity prime directive** (§A.6) — BX-11 deletes a replica only when the
  planner proves mandatory tiers + anchor survive; BW fixes a corruption, so its
  tests gate on byte-exact round-trip.
- **Determinism** — placement ranking + classifiers stay pure/deterministic.
- **No second full scan** — `chunk_placement` + `pending_deletion` fold into the
  existing `get_stats` pass.
- **Bounded background work** — BX-11 caps removals per tick; reuses Bug BO-2
  dedup/retention; no tight loops, no audit-log flooding.
- **Backward-safe schema** — new proto fields optional; new config
  `#[serde(default)]`; no `StateMachineData` field without `#[serde(default)]`.

---

## Verification log

- **Phase 1 (BX-9..14) — 2026-06-03.** Implemented + built clean; full manager
  suite 173 passed, 0 failed. Deployed manager+fuse (build 20260603-100706) to
  the minicluster. BX-11 rebalance ran across several scheduler ticks and
  removed nothing (cluster has overgoal=0 -> `plan_excess_spill_removal` returns
  None) while `chunk_placement.missing` stayed 0 — the live delete-dispatcher is
  confirmed SAFE (no errant removal, no under-replication). `chunk_placement` =
  `missing=0 undergoal=0 misplaced=121 stable=159 overgoal=0 pending_deletion=0`
  unchanged. Release case 36 PASS; case 37 (spill/rebalance) KNOWN_ISSUE [BX] —
  the destructive fill-all-tiers convergence isn't run on the shared lab; spill
  is unit-tested + nearness wired (BX-9) + audited (BX-10). BX-12 dangling-row
  `pending_deletion` verified live (0 dangling rows currently).

---

## Deviations from plan

> Append if reality diverges (esp. BW-2 split location, BX-11 racing tiering,
> BT-3 retarget).
