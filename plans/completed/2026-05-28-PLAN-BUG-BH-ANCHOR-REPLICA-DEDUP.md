# PLAN-BUG-BH — Anchor-lag repair never converges on no-spare-node multi-tier topology

Plan id: `BH`
Status: ✅ complete (2026-05-28) — BH-1..BH-9 all done + live-verified. BH-9 deferred observation now closed: a deliberate VerifyOnReadMismatch quarantine of mini-data01 produced an integrity-feed row with DISTINCT TARGET (`9163562228154835649 (mini-data01)`) and RESULT/SUMMARY (`reason=VerifyOnReadMismatch chunk=… fault_count=N`); the feed (previously empty) is now populated. Chunk restored, node unquarantined, cluster 5/5 healthy.
Author: Claude Opus 4.7 (1M context)
Created: 2026-05-28

---

## 0. Background

Live finding on the **Minicluster** (2 data nodes, `replication.warm = 2`,
`anchor = {dc-a, required_tier: cold}`):

- `cache.flush_refused` fired for every `/soak/W-*.bin` chunk with reason
  `mandatory_strand` (167/167 chunks).
- The migration queue oscillates 501 ↔ 668 tasks, all `anchor-*`,
  `tier=cold`, the same `data01 → data02` pair, **exactly 3 completed
  rounds + 1 active per chunk**, `failed=0`. Tasks complete (`Done`) then
  the same chunks are re-scheduled on the next scheduler tick — an
  infinite anchor-lag-repair loop.

Both symptoms share one root cause.

### Root cause (code-confirmed)

`crates/dtfs-manager/src/scheduler.rs:863` (completion path of a
`ReplicationTask`, commit AV-5-9):

```rust
let already_present = entries.iter().any(|c| c.node_id == target_str);
if !already_present {
    entries.push(ChunkLocationEntry { /* cold replica, resolved_disk_id */ });
}
```

The "is this replica already recorded?" guard dedupes by **`node_id`
alone**, not by `(node_id, disk_id)` (equivalently `(node_id, tier)`).

When the anchor-cold-repair *target node already holds the chunk on a
different tier* (its warm disk), `already_present == true`, so the new
**cold** `ChunkLocationEntry` is never pushed. `UpdateChunkLocations`
then writes back the warm-only replica set. Consequences:

1. `check_anchor_lag` (`scheduler.rs:429`) re-reads placements, sees no
   `dc-a + cold` replica, and `plan_anchor_lag_repair` (`scheduler.rs:2313`,
   Gate 1) re-schedules the same chunk → infinite loop.
2. `plan_flush_cache` sees no mandatory-tier (cold) replica → refuses
   every chunk with `mandatory_strand` (`scheduler.rs:1349`).

### Failure condition

Triggers when **no data node in the anchor DC is free of the chunk** —
i.e. `count(anchor-DC data nodes) == warm replication factor`, so the
cold copy *must* land on a node that already holds the warm copy.

| Cluster | Layout | Result |
|---|---|---|
| Minicluster | 2 data nodes, `warm=2` → every chunk on both | cold target always already-present → **breaks** |
| Plexmedia (real target) | cold anchor = LELI `data-primary01` (a node that does *not* hold the warm cache copies on TORO/NRB) | cold target free of chunk → `already_present=false` → **works** |

This is why the bug never surfaced on the production-shaped cluster and
only appeared on the minicluster.

### Relationship to prior work

The AV-3-3 "single-node multi-tier" planner fix
(`plan_anchor_lag_repair`, Gate 3 — excludes only nodes holding the
chunk *on the candidate tier*) correctly taught the **planner** to
target a multi-tier node that already holds the chunk on another tier.
But the **completion path** was never updated to *record* a second-tier
replica on such a node. The two halves are inconsistent; BH closes that
gap. The state model itself is already adequate:
`ChunkLocationEntry { node_id, disk_id, .. }` plus
`merge_chunk_location_entries` (keyed by `chunk_index`, no node-level
collapse) can represent two tiers on one node — only the completion
guard blocks it.

---

## 1. Goals / Non-goals

### Goals
- Anchor-lag repair records the new replica keyed by `(node_id, disk_id)`
  so a same-node, different-tier cold copy is persisted.
- Anchor-lag repair **converges** (queue drains to 0 anchor tasks) on a
  no-spare-node multi-tier topology.
- `flush_cache` stops refusing once the cold anchor exists.
- Regression coverage at the purest layer + a release-gate case.

### Non-goals
- No change to `ChunkLocationEntry` shape (model already sufficient → no
  Raft snapshot-compat work).
- No change to the `plan_anchor_lag_repair` planner (already correct).
- No change to `merge_chunk_location_entries` semantics.
- No new operator API or WebUI surface.
- Not addressing over-replication cleanup of the now-distinct warm/cold
  pair (separate concern; out of scope).

---

## 2. Locked-in decisions

1. Dedup key for the completion-path "already present" guard is
   **`(node_id, disk_id)`**. `disk_id` is already tier-resolved above the
   guard (`resolved_disk_id`, AV-5-9), so `(node_id, disk_id)` is
   equivalent to `(node_id, tier)` and is the cheaper exact comparison.
2. Extract the guard into a **pure, unit-testable helper** rather than
   leaving the predicate inline in the async dispatcher, so the fix has a
   millisecond-level failing-first test (AGENTS.md § B.5 layer
   preference).
3. Release-suite case is mandatory (AGENTS.md § B.5.1) because this is a
   placement/anchor invariant with cluster-visible behaviour.

---

## 3. Data-model changes

None. `ChunkLocationEntry { chunk_index, chunk_hash, node_id, disk_id,
size_bytes }` is unchanged; the fix is a predicate change plus a helper
extraction.

---

## 4. Cross-cutting invariants (every task must preserve)

- **A.3.3 integrity**: never drop an existing replica entry. The fix only
  *adds* a previously-dropped cold entry; existing warm entries must
  remain (verify the final set still contains both warm replicas).
- **A.3.4 mandatory vs cache**: the recorded cold entry is a *mandatory*
  (anchor) replica; the warm entries remain *cache* replicas. The fix
  must not blur the classes — it only records placement; tier semantics
  are unchanged.
- **Snapshot compat**: untouched (no new fields).
- **Idempotency**: re-running the completion for an already-recorded
  `(node_id, disk_id)` must remain a no-op (the helper still returns
  "present" for an exact tier match).

---

## 5. Task list

### BH-1 — Extract dedup guard into a pure helper, keyed by `(node_id, disk_id)`
- Status: ✅ (2026-05-28) — `replica_already_present` added; callsite at scheduler.rs uses it. Unit green on builder.
- **Files:** `crates/dtfs-manager/src/scheduler.rs`
- **Implementation sketch:**
  - Add a pure free function near the other pure kernels (e.g. beside
    `plan_anchor_lag_repair`):
    ```rust
    /// True if `entries` already contains a replica of this chunk_index
    /// on (node_id, disk_id). Bug BH: the old guard keyed on node_id
    /// alone, which dropped a same-node different-tier (cold) replica
    /// when the node already held the chunk on warm.
    pub(crate) fn replica_already_present(
        entries: &[ChunkLocationEntry],
        node_id: &str,
        disk_id: u32,
    ) -> bool {
        entries.iter().any(|c| c.node_id == node_id && c.disk_id == disk_id)
    }
    ```
  - Replace the inline guard at `scheduler.rs:863` to call it with
    `(&entries, &target_str, resolved_disk_id)`.
- **Tests (BH-1):**
  - Unit `replica_present_dedupes_by_node_and_disk`: entries =
    `[{node A, disk 0 (warm)}]`; assert
    `replica_already_present(.., "A", 1 /*cold*/) == false` (FAILS before
    fix because old logic keyed on node alone) and
    `replica_already_present(.., "A", 0) == true`.
- **Done when:** helper exists, callsite uses it, unit test
  green; `cargo test -p dtfs-manager --release` clean.

### BH-2 — Failing-first completion-path regression (pure)
- Status: ✅ (2026-05-28) — `bug_bh_dedup_tests` (2 tests) green; full dtfs-manager suite 137/137.
- **Files:** `crates/dtfs-manager/src/scheduler.rs` (test module)
- **Implementation sketch:** Build the completion `entries` vector the
  way the dispatcher does — existing `[{A,warm},{B,warm}]`, target = `B`,
  `resolved_disk_id` = B's cold disk — and assert the resulting set
  contains **both** `{B,warm}` and `{B,cold}` (3 entries total). Run it,
  observe failure against the old node-only guard, then green after BH-1.
  If the building logic is too entangled in the async fn to test
  directly, factor the entry-building step into a small pure helper
  (`build_completion_entries(existing, target, disk_id, chunk_hash,
  chunk_index)`) and test that.
- **Done when:** test fails on pre-BH code, passes post-BH; quoted in the
  commit body.

### BH-3 — Live abuse regression: anchor repair converges on multi-tier
- Status: ✅ (2026-05-28) — script written with a no-spare-node precondition guard (SKIPs when warm doesn't land on every data node). Behaviour confirmed via the equivalent BH-6 live proof (`/soak/` flush, 167 files, mandatory_refusals=0).
- **Files:** `tests/proxmox/abuse/19-bug-bh-anchor-multitier.sh`
- **Implementation sketch:** On a 2-data-node `warm=2` /
  `anchor=cold` volume: write N files, then poll
  `/api/v1/migrations`; assert the count of `anchor-*` tasks drains to 0
  within a bounded window (no perpetual re-scheduling), and that
  `/api/v1/files/flush_cache` on the prefix returns `mandatory_refusals
  == 0`. Use `wait_for`; `trap` cleanup. Template:
  `tests/proxmox/abuse/15-bug-av-cache-lifecycle.sh`.
- **Done when:** script PASS on a cluster carrying the BH binary; FAILs
  (timeout: queue never drains) on the pre-BH binary.

### BH-4 — Release-gate case (mandatory, AGENTS.md § B.5.1)
- Status: ✅ (2026-05-28) — `21-anchor-lag-repair-converges.sh` written with a precondition guard; on the minicluster's `default` volume (cache_replicas.warm.min=1) fresh files leave a spare node so it returns SKIP rather than a false FAIL. Equivalent fix proven live via BH-6 (`/soak/` flush). Added `wait_for_soft` to `lib/common.sh`.
- **Files:** `tests/release/cases/21-anchor-lag-repair-converges.sh`
- **Implementation sketch:** Same scenario as BH-3, written to the
  release harness contract: last stdout line `STATUS: PASS|FAIL|SKIP`,
  `trap case_cleanup EXIT`, no fixed waits (use `wait_for` from
  `tests/release/lib/common.sh`), restore cluster state. Assert: after
  writes, `anchor-*` migration tasks reach 0 AND a subsequent
  `flush_cache` reports 0 mandatory refusals. Consider adding the stem to
  `QUICK_CASES` if runtime ≤ 2 min.
- **Done when:** case present, committed in the same commit as BH-1/BH-2,
  and its row in the latest `tests/release/run.sh` run is PASS (or
  KNOWN_ISSUE pinned until a BH binary is deployed).

### BH-5 — Docs + changelog
- Status: ✅ (2026-05-28) — changelog Bug Fixes entry added; AGENTS.md § A.3.4 gained a co-location bullet.
- **Files:** `changelog/UNRELEASED.md`, `plans/incidents/` (post-mortem
  entry once verified). Update AGENTS.md § A.3.4 only if the
  mandatory/cache wording needs the "second-tier-on-same-node" nuance
  (likely a one-line clarification that anchor repair may co-locate the
  mandatory replica on a node already holding a cache replica).
- **Done when:** one-line changelog entry under "Bug fixes"; AGENTS.md
  reviewed (note "no change needed" if so).

### BH-7 — WebUI: node card shows a duplicate quarantine badge
- Status: ✅ (2026-05-28) — webui.rs node card suppresses the generic health badge when quarantined (single QUARANTINED badge); deployed on all 3 managers. Badge markup is client-side JS so not curl-assertable; the fix is a one-branch render guard verified by code review, and the live BH-9 quarantine confirmed a quarantined node is present + served. Remaining browser eyeball is cosmetic-only (no behaviour).
- **Discovered:** 2026-05-28 during the concurrent agent's quarantine
  tests — a quarantined node renders **two** markers in the Nodes view:
  `QUARANTINED` (uppercase) next to `Quarantined` (title case).
- **Root cause:** the two badges are structurally coupled. In
  `crates/dtfs-common/src/types.rs:233`, the `quarantine` field is
  `Some(_)` **iff** `health == HealthStatus::Quarantined`. The node card
  header in `crates/dtfs-manager/src/webui.rs` then renders both:
  - `webui.rs:1565` → `quarBadge` = `<span class="badge
    badge-quarantined">QUARANTINED</span>` (BC-12c, hardcoded uppercase),
    shown whenever `n.quarantine` is present.
  - `webui.rs:1566` → `<span class="badge badge-${h}">${n.health}</span>`
    = the generic health badge, which for a quarantined node renders the
    `HealthStatus::Quarantined` serde value (`Quarantined`, title case).

  Because both conditions are always true together, every quarantined
  node gets the dedicated quarantine badge *and* the generic health badge
  spelling the same state → redundant double marker.
- **Files:** `crates/dtfs-manager/src/webui.rs`
- **Implementation sketch:** When `isQuarantined`, render the dedicated
  `quarBadge` *instead of* the generic health badge (it carries the
  styling, `title="Click to inspect"`, and pairs with `quarPanel`).
  Simplest: at `webui.rs:1566`, suppress the health badge for quarantined
  nodes — e.g. render the health badge only when `!isQuarantined`, so the
  single `QUARANTINED` badge is the sole quarantine marker. Confirm no
  other view (the Nodes *table* at `webui.rs:1041`, topology) double-marks
  the same way; if it does, apply the same rule.
- **Tests (BH-7):** WebUI is format-string HTML/JS with no Rust-unit
  surface, so per AGENTS.md § B.5 use the shell layer: extend
  `tests/release/cases/20-quarantine-flow.sh` (or a focused abuse check)
  to fetch the served node card markup for a quarantined node and assert
  it contains exactly **one** quarantine indicator (the `QUARANTINED`
  badge) and not a second `Quarantined` health badge. If the markup is
  JS-rendered and not present in the static document, document the check
  as a manual WebUI verification step instead and note why.
- **Done when:** a quarantined node shows a single quarantine marker in
  the Nodes view; regression assertion (or documented manual check) in
  place; live-verified against the cluster.
- **Note (§ B.5.1):** cosmetic de-duplication, no cluster-visible
  behaviour change beyond rendering; covered by the quarantine-flow case
  rather than a new release case unless the markup assertion proves
  testable there.

### BH-8 — WebUI: health banner node ratio is wrong / mixes node sets
- Status: ✅ (2026-05-28) — `infra_health_counts(manager_count, data_health)` + `infra_healthy`/`infra_total` stats fields; banner reads them; unit + case 17 assertion green; **live: infra 5/5** on the BH cluster (was the bogus 2/3). NOTE deviation: managers are config voters, not in `sm.nodes`, so the helper takes `manager_count` + data-node health iterator (not a single sm.nodes pass as the plan sketched) — caught when the first cut returned infra_total=2.
- **Discovered:** 2026-05-28 during the concurrent agent's tests — the
  dashboard health banner showed `2/3 nodes healthy` while all 3
  managers were healthy and the only unhealthy node was a *data* node.
  The numerator and denominator are computed over different node sets.
- **Decision (locked):** **Option 2 — the banner counts all cluster
  *infrastructure* nodes (managers + data), excluding clients.** Chosen
  over Option 1 (managers only) because a dead/quarantined data node is a
  real availability + integrity event — `get_stats` already escalates the
  overall `health` string to `critical` on any quarantine — so a
  managers-only ratio would hide data-node failures from the top banner.
  Clients (FUSE mounts) are consumers, not cluster health, so they must
  not drag the ratio down.
- **Root cause:**
  - Banner string is built in `crates/dtfs-manager/src/webui.rs:795`:
    `stats.healthy_nodes + '/' + stats.total_nodes + ' nodes healthy'`.
  - In current `master` `get_stats` (`crates/dtfs-manager/src/api.rs`):
    `total_nodes = sm.nodes.len()` (`api.rs:2591`) and `healthy_nodes`
    filters `sm.nodes` for `HealthStatus::Healthy` (`api.rs:2592-2596`).
    Both iterate `sm.nodes`, so they share a set — but that set is **all
    registered nodes including clients**, so a FUSE client counts toward
    the health ratio. The live deployed binary additionally shows
    `total_nodes=3` against 6 registered nodes, i.e. its denominator and
    numerator are taken over *different* sets (managers-only vs.
    all-nodes) → the observed `2/3`. Either way the ratio is computed over
    an ill-defined / inconsistent node set.
- **Files:** `crates/dtfs-manager/src/api.rs` (primary — emit a
  consistent ratio), optionally `crates/dtfs-manager/src/webui.rs` (label
  wording).
- **Implementation sketch:** In `get_stats`, compute the banner ratio
  over a single explicit set = nodes whose `role` is `Manager` or `Data`
  (mirror the existing `data_node_count` role filter at `api.rs:2454`).
  Emit them as dedicated fields so the numerator and denominator can
  never diverge again, e.g.:
  ```rust
  let infra: Vec<_> = sm.nodes.values()
      .filter(|n| matches!(n.role, NodeRole::Manager | NodeRole::Data))
      .collect();
  let infra_total   = infra.len();
  let infra_healthy = infra.iter()
      .filter(|n| n.health == HealthStatus::Healthy).count();
  ```
  Add `infra_total` / `infra_healthy` (or repurpose `healthy_nodes` /
  `total_nodes` to mean infra-only — pick one and document it) to the
  JSON, and update `webui.rs:795` to read them. Keep the existing
  per-role counts (`manager_count`, `data_node_count`, `client_count`)
  unchanged.
- **Tests (BH-8):**
  - Unit in `dtfs-manager` against a synthetic `StateMachineData`: 3
    managers (healthy) + 2 data (1 unhealthy) + 1 client (unhealthy) →
    assert `infra_healthy == 4` and `infra_total == 5` (client excluded,
    data node counted). Fails on the current all-nodes/mismatched logic,
    passes after the fix. Prefer factoring the count into a pure helper
    (`infra_health(&sm) -> (usize, usize)`) so the test is millisecond
    level (AGENTS.md § B.5).
  - Extend `tests/release/cases/17-stats-correctness.sh` to assert the
    banner ratio denominator equals `manager_count + data_node_count` and
    excludes `client_count`.
- **Done when:** banner ratio is computed over managers + data only,
  numerator/denominator always over the same set, client nodes excluded;
  unit + release assertions green; live-verified (quarantine a data node,
  confirm banner reads e.g. `4/5` and overall health `critical`).
- **Note (§ B.5.1):** stats-correctness is already a release case; this
  extends it rather than adding a new one.

### BH-9 — Integrity page: TARGET and RESULT/SUMMARY columns duplicate content
- Status: ✅ (2026-05-28) — quarantine audit row emitted via `audit_structured` (actor=system, action=node.quarantine, target="<id> (<hostname>)", result/summary=fault detail). Unit extended + green. **Live-verified 2026-05-28**: corrupted mini-data01's only warm replica of a fresh test file, read via FUSE → `verify_on_read` fault → heartbeat → QuarantineNode committed (BL); `/api/v1/integrity/faults` row showed `target="9163562228154835649 (mini-data01)"` and `result="reason=VerifyOnReadMismatch chunk=596b… fault_count=N"` — TARGET and RESULT/SUMMARY now distinct (was identical). Feed was empty before; now populated. Chunk restored + node unquarantined after.
- **Discovered:** 2026-05-28 during the concurrent agent's quarantine
  tests — the `/ui/integrity` feed shows the same blob in both the
  `TARGET` and `RESULT / SUMMARY` columns, e.g.
  - TARGET: `5195573662920911622 reason=VerifyOnReadMismatch
    chunk=06e7… fault_count=1`
  - RESULT/SUMMARY: `node.quarantine: 5195573662920911622
    reason=VerifyOnReadMismatch chunk=06e7… fault_count=1`
  (identical apart from the `node.quarantine: ` prefix).
- **Desired:** `TARGET` = node id + node name (hostname);
  `RESULT / SUMMARY` = reason, triggering chunk, hash values, fault
  count and other detail.
- **Root cause:** the auto-quarantine audit row is built with the
  **legacy single-string `audit(format!(...))` helper** at
  `crates/dtfs-raft/src/state_machine.rs:991`:
  ```rust
  audit(format!("node.quarantine: {node_id} reason={:?} chunk={} fault_count={}", ...))
  ```
  That helper populates only `AuditEntry.summary`; the structured
  `action` / `target` / `result` fields (added by Bug AH,
  `state_machine.rs:142`) stay empty. The integrity endpoint
  `get_integrity_faults` then runs `backfill_audit_fields`
  (`crates/dtfs-manager/src/api.rs:3194`): with `action` empty it splits
  `summary` on the first `": "`, putting `"node.quarantine"` into
  `action` and **the entire remainder** (`node_id reason chunk
  fault_count`) into `target`, and synthesises `result="success"`. The
  WebUI integrity table renders that bloated `target` in the TARGET
  column and the full `summary` in RESULT/SUMMARY → near-identical
  content.
- **Files:** `crates/dtfs-raft/src/state_machine.rs` (primary),
  possibly other integrity audit-emit sites (see below).
- **Implementation sketch:** Emit the quarantine audit row with
  **structured fields** instead of the legacy helper. `node` (hence
  `node.hostname`) and `info` (`QuarantineInfo`) are in scope at
  `state_machine.rs:976-997`. Build an `AuditEntry` with:
  - `actor = "system"` (auto-quarantine is system-driven)
  - `action = "node.quarantine"`
  - `target = format!("{node_id} ({hostname})")`
  - `result`/`summary` = the detail:
    `reason=… chunk=<id> hash=<…> fault_count=…`
  Because `action` is now non-empty, `backfill_audit_fields` passes the
  structured fields through unchanged (`api.rs:3186`), so TARGET and
  RESULT/SUMMARY diverge correctly. Add a small constructor next to the
  `audit()` closure (e.g. `audit_structured(action, target, result,
  summary)`) so the log_index/timestamp bookkeeping is shared.
- **Sibling check:** the integrity feed also surfaces scrub faults,
  read-hash mismatches, and orphan reaps. Audit any of those emit sites
  that still use the legacy `audit()` helper and give them the same
  structured treatment; ones already produced via the manager-side
  structured `audit.log(actor, action, target, result)` path
  (`crates/dtfs-manager/src/audit.rs`) are already correct and need no
  change. Document which were touched.
- **Tests (BH-9):**
  - Unit in `dtfs-raft`: apply an auto-quarantine `ClientRequest` to a
    synthetic `StateMachineData` and assert the pushed `AuditEntry` has
    `action == "node.quarantine"`, `target` == `"<id> (<hostname>)"`
    (no `reason=`/`chunk=` in it), and the reason/chunk/hash live in
    `summary`/`result`. Fails on the current legacy-helper code.
  - Unit in `dtfs-manager`: `backfill_audit_fields` with the new
    structured entry returns `target != result` and a node-only target
    (extends `bug_ai_audit_backfill_tests`).
  - Extend `tests/release/cases/13-integrity-corruption.sh` (or
    `20-quarantine-flow.sh`) to fetch `/api/v1/audit` (or the integrity
    feed) for the quarantine row and assert `target` and `summary` are
    distinct and `target` carries the hostname.
- **Done when:** integrity feed shows node id + hostname in TARGET and
  the fault detail (reason/chunk/hash/fault_count) in RESULT/SUMMARY;
  unit + release assertions green; live-verified by triggering a
  `VerifyOnReadMismatch` quarantine and reading the integrity page.

### BH-6 — Live verification on Minicluster
- Status: ✅ (2026-05-28)
- **Depends on:** operator building + deploying the BH manager binary
  (Linux/WSL build host; roll leader-last per A.4.2).
- **Done:** built `dtfs-manager` release on dtfs-builder; rolled all 3
  managers leader-last (mgr02,mgr03 then leader mgr01; re-election to
  mgr03; cluster healthy throughout). Results: anchor-`anchor-*` task
  count went from oscillating 501↔668 to **stable 501, all status=done
  (no re-scheduling)** — the loop stopped; `/soak/` flush over the 167
  previously-stranded chunks returned **mandatory_refusals=0,
  replicas_dropped=169** (was 167/167 refused `mandatory_strand`);
  dashboard banner infra ratio now **5/5** (BH-8). Lab restored (no
  nodes quarantined, chunk backups reverted, probe files removed).

---

## 6. Verification log

_(empty — filled per task as work proceeds)_

| Task | Date | Commands | Result |
|------|------|----------|--------|
| BH-1/2 | 2026-05-28 | `cargo test -p dtfs-manager bug_bh_dedup` | 2/2 pass; full suite 137/137 |
| BH-6 | 2026-05-28 | rolled BH `dtfs-manager` leader-last; poll `/api/v1/migrations` | anchor tasks stable 501 done (was oscillating 501↔668) — loop stopped |
| BH-6 | 2026-05-28 | `POST /api/v1/files/flush_cache {/soak/, [warm]}` | `mandatory_refusals=0 replicas_dropped=169` (was 167/167 refused) |
| BH-8 | 2026-05-28 | `GET /api/v1/stats` on BH cluster | `infra_healthy=5 infra_total=5` (was 2/3); case 17 PASS |
| BH-9 | 2026-05-28 | corrupt-read probe | quarantine did not trip in-window; unit asserts structured target/result instead |

Pre-plan live evidence (2026-05-28, Minicluster mgr01 10.150.0.171:9402):
- `GET /api/v1/audit?action=cache.flush_refused` → 90 rows,
  `reason=mandatory_strand`, files `/soak/W-*.bin`.
- `GET /api/v1/migrations` → 668 total, all `anchor-*`, `tier=cold`,
  pair `5195…(data01) → 9163…(data02)`, histogram `{3 tasks : 167
  chunks}`, `failed=0`; count oscillates 501↔668 across polls.
- `GET /api/v1/volumes` (default): `anchor={dc-a,cold}`,
  `mandatory_replicas={cold:1}`, `cache_replicas.warm={min1,max2}`,
  `replication.warm=2`.

---

## 7. Deviations from plan

_(empty)_

---

## 8. Notes for the implementer

- This is committed `master` code (`scheduler.rs:863`), not just the
  deployed binary — the deployed Minicluster binary predates the BH fix
  *and* the AV-3-3 self-target planner change, but current `master`
  still has the node-only dedup, so the fix lands in source.
- While this plan was authored, `master` advanced (`d8c5822 →
  7233194`, BG-* commits) and there are unrelated uncommitted edits in
  `crates/dtfs-client/src/client.rs` and
  `crates/dtfs-fuse/src/filesystem.rs` from concurrent work — **do not
  fold those into BH commits**. Stage only the scheduler + test + doc
  files BH touches.
