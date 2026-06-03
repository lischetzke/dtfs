# Bug BV — Write-safety ignores anchor; post-rebuild correctness findings

**Status:** BV-1 (visibility-only, per user)/BV-2/BV-3 complete + unit-verified 2026-06-03; live verify at deploy
**Source:** ISSUE-SCAN-2026-06-02-1517 (F4, F5, F6).

---

## 0. Background

After the operator rebuilt the minicluster from scratch (raft term reset, new
ingest), three correctness/observability findings surfaced that are independent
of the capacity incident (data01 full — tracked in Bug BU):

- **F4 (high) — write-safety acks before the mandatory anchor exists.** The
  `default` volume has `write_safety.min_replicas_before_ack:2` and
  `anchor:{dc-a, cold}`, `mandatory_replicas.cold:1`. New writes ack after 2
  **warm** replicas (both landing on data02 in dc-b, since data01 is full), with
  **no cold anchor copy**. `min_replicas_before_ack` counts any replicas toward
  the threshold, not specifically one satisfying the anchor constraint. The
  anchor invariant is violated from the moment of write; convergence relies on
  the async anchor-lag scheduler, which is currently blocked. If data02 fails
  before the cold copy lands, data loss recurs. `files_missing_chunks:0` is
  misleading — chunks exist on warm, cold anchor absent.
- **F5 (medium) — tiering knobs zeroed after rebuild.** `default` volume now has
  `cache_replicas.cooldown_secs:0`, `promote_reads:0`, `promote_window_secs:0`
  (were 30/2/60 pre-rebuild). Unknown whether `promote_reads:0` means "never
  promote" or "promote on every read" — semantics must be verified in the
  scheduler. Likely a default-on-rebuild config, but the zero-handling is a
  latent correctness question.
- **F6 (low) — `cluster_status.data_nodes` miscounts.** `/api/v1/cluster/status`
  reports `data_nodes:6` / `data_nodes_healthy:6` while only 2 nodes have
  `role:Data` (3 managers + 2 data + 1 client = 6). `/api/v1/stats` is correct
  (`data_node_count:2`). Looks like `cluster_status` uses `nodes.len()` instead
  of filtering `role == Data`. Cosmetic but misleads operators/monitoring.

(F1/F2 capacity + tmp leak → Bug BU. F3 phantom → orphan scrub/operator. F7
stale build_date → Bug BS / AY. Not duplicated here.)

---

## 1. Goals / non-goals

**Goals:**
- Decide + implement whether write ack must account for the mandatory anchor
  replica, not just a raw replica count (BV-1).
- Pin the semantics of zeroed tiering thresholds and guard against an
  accidentally-inert (or accidentally-hyperactive) tiering engine (BV-2).
- Fix the `cluster_status` data-node count to filter by role (BV-3).

**Non-goals:**
- Capacity routing / full-disk handling (Bug BU).
- Restoring the specific lab volume config values — operator action, not code.
- Changing the anchor model itself (AGENTS.md §A.3.4) — only how ack relates to
  it.

---

## 2. Locked-in decisions

- **BV-1 is design-first.** The tension: blocking ack until the cold anchor lands
  trades write latency/availability for durability; the LELI anchor link is slow
  (40 Mbps) so a hard block could stall ingest badly. Options to evaluate:
  1. Keep async convergence but make `write_safety` require that the acked
     replicas include at least the mandatory **warm** count AND surface an
     explicit "anchor pending" state per file (so `files_missing_chunks:0` stops
     being misleading — report `anchor_pending` count).
  2. Add an opt-in `require_anchor_before_ack` write-safety mode for volumes that
     prioritize durability over ingest latency.
  3. Status quo + alerting only.
  Decision goes in Deviations after the design pass; do NOT silently change ack
  semantics for existing volumes. The anchor invariant is not weakened — this is
  about WHEN ack fires relative to anchor placement, surfaced honestly.
- **BV-2** is investigate-then-guard: confirm `promote_reads:0` /
  `promote_window_secs:0` behaviour in the tiering scheduler. If `0` is
  ambiguous, define it explicitly (recommend `0` = disabled) and reject/normalize
  a config that would make tiering fire on every read by accident.
- **BV-3** is a focused fix: `cluster_status` counts `role == Data` for both
  `data_nodes` and `data_nodes_healthy`, matching `stats.data_node_count`.

---

## 3. Data-model changes

- BV-1: possibly a new computed `anchor_pending` count in stats /
  cluster_status; possibly an opt-in `require_anchor_before_ack` volume
  write-safety field (`#[serde(default)]`-safe) if option 2 is chosen. Decided in
  BV-1 design.
- BV-2: none (or a normalization at volume save).
- BV-3: none (read-path count fix).

---

## 4. Task list

### BV-1: Write-safety vs mandatory anchor (design + impl)

- Status: ✅ (2026-06-03 — Option 1 visibility-only, per user decision)
- Source: F4.
- Files: `crates/dtfs-manager/src/metadata.rs` (write lease / ack path),
  `crates/dtfs-common/src/types.rs` (`write_safety` if a new mode is added),
  `crates/dtfs-manager/src/api.rs` (surface `anchor_pending`).
- Sketch: (1) write the design decision (option 1/2/3 in §2) into Deviations with
  user ack; (2) at minimum, surface an `anchor_pending` count so a chunk lacking
  its mandatory cold copy is visible (fixes the misleading `files_missing_chunks:0`);
  (3) if a require-anchor mode is chosen, implement it as opt-in per volume.
- Tests:
  - Unit: a file with warm replicas but no cold anchor is counted
    `anchor_pending` (not "healthy", not "missing chunks").
  - If require-anchor mode added: unit that ack is withheld until the cold
    mandatory is placed for a volume with that mode set; default volumes
    unaffected.
- Done when: anchor-pending state is visible in stats; ack semantics decision is
  implemented + documented; default-volume behaviour unchanged unless explicitly
  opted in.

### BV-2: Tiering zero-threshold semantics + guard

- Status: ✅ (2026-06-03)
- Source: F5.
- Files: `crates/dtfs-manager/src/tiering.rs` (promotion threshold handling),
  volume save/normalization in `api.rs` / `state_machine.rs`.
- Sketch: confirm how `promote_reads == 0` / `promote_window_secs == 0` behave;
  define `0` explicitly (recommend = promotion disabled) with a doc comment +
  test; if the current code would promote-on-every-read at 0, fix or normalize.
- Tests:
  - Unit: `promote_reads:0` produces the defined behaviour (no promotion), not
    promote-on-every-read.
- Done when: zero-threshold semantics are explicit + tested; an inadvertently
  inert/hyperactive tiering config cannot arise silently.

### BV-3: Fix cluster_status data-node count

- Status: ✅ (2026-06-03)
- Source: F6.
- Files: `crates/dtfs-manager/src/api.rs` (`cluster_status`).
- Sketch: count only `role == Data` for `data_nodes` and `data_nodes_healthy`
  (reuse the same predicate `stats.data_node_count` uses).
- Tests:
  - Unit: a roster of 3 managers + 2 data + 1 client yields `data_nodes:2`.
- Done when: `cluster_status.data_nodes` matches `stats.data_node_count` on the
  live cluster.

### BV-4: Release cases + changelog

- Status: ✅ (2026-06-03 — case authored; live-run at deploy)
- Files: `tests/release/cases/NN-*.sh`, `changelog/UNRELEASED.md`.
- Cases: (a) `cluster_status.data_nodes` equals the role=Data count; (b)
  anchor-pending surfaced when cold copy absent (BV-1). Changelog entries for the
  shipped behaviour (anchor-pending visibility / any ack-mode change, tiering
  zero semantics, data-node count fix). Same commit as code.
- Done when: cases PASS or `KNOWN_ISSUE [BV]` pinned.

---

## 5. Cross-cutting invariants

- The anchor invariant (AGENTS.md §A.3.4) is not weakened — BV-1 only changes how
  ack timing relates to anchor placement and makes anchor-pending honest; it
  never marks an unanchored chunk as fully durable.
- Default-volume write behaviour must not change silently — any
  require-anchor-before-ack is opt-in.
- Read-path projections (anchor_pending, data-node count) must work without a
  committing leader.
- `#[serde(default)]` on any new volume/write-safety field.

---

## 6. Verification log

- **BV-1 (2026-06-03)** — User chose Option 1 (visibility-only; no ack
  change). Added pure `anchor_satisfied_by` + `count_anchor_pending_files`
  in `api.rs`, surfaced `anchor_pending` in `/api/v1/stats` and
  `/api/v1/cluster/status` (does NOT degrade top-line status). Unit:
  `api::bug_bv3_data_node_count_tests::bv1_anchor_satisfied_by_tier_and_dc`
  PASS.
- **BV-2 (2026-06-03)** — `plan_cache_promotion` Gate 0: `promote_reads == 0`
  now returns 0 (disabled), not promote-on-every-read (gate 3's `count < 0`
  was always false). Confirmed `promote_window_secs == 0` already disables
  at the engine tick (`tiering.rs:605`). Unit:
  `tiering::bug_av_promotion_decision_tests::bug_bv2_zero_threshold_disables_promotion`
  PASS.
- **BV-3 (2026-06-03)** — `cluster_status` counts `role==Data` via a shared
  `count_data_nodes` helper (was `sm.nodes.len()`). Unit:
  `api::bug_bv3_data_node_count_tests::counts_only_role_data` PASS.
- **BV-4 (2026-06-03)** — release case
  `tests/release/cases/35-cluster-counts-anchor-pending.sh` authored
  (data-node counts agree across nodes/stats/cluster_status; anchor_pending
  present + non-negative). Live-run at deploy.
- **Deferred decision recorded:** the opt-in `require_anchor_before_ack`
  write-safety mode (Option 2) was NOT implemented — user chose Option 1.
  Ack semantics for existing volumes are unchanged.

> Live evidence at authorship: `default` volume
> `min_replicas_before_ack:2`, `anchor:{dc-a,cold}`; new writes acked on 2
> warm (data02 dc-b) with no cold copy; `files_missing_chunks:0` despite no
> anchor. `promote_reads:0`/`promote_window_secs:0`/`cooldown_secs:0`.
> `cluster_status.data_nodes:6` vs `stats.data_node_count:2`.

---

## 7. Deviations from plan

> Append if reality diverges (esp. the BV-1 ack-semantics decision + user ack).
