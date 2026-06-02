# Bug BB — per-tier disk usage fidelity (get_stats apportionment lie)

**Status:** code complete (2026-06-02) — BB-1/BB-2 done; dtfs-manager 154/154; deploy + live-verify in progress. Finding: the post-AU per-disk preference already largely fixed BA-2 (live tier_breakdown matched real disk sums); the residual bug was the PER-DISK fallback decision fabricating usage onto an empty disk of an otherwise-used node, now fixed by deciding per-NODE.
**Sources:** PLAN-BUG-BA catalog entry BA-2 (P2). Split out of BA so the BA
soak plan can close; this is the one BA finding still needing a code fix.

---

## 0. Background

`get_stats` (`crates/dtfs-manager/src/api.rs`) computes per-tier `used_bytes`
by **apportioning** each node's single `used_capacity` across its disks by
`disk.total_bytes / node_total_bytes`, regardless of which tier actually
holds the bytes. On a node with equal-size warm + cold disks this forces the
tier breakdown to a fabricated 50/50, and the RF divisor then compounds the
lie in `net_used`.

Evidence (BA 2026-05-26 stat audit): mgr `/api/v1/stats` reported warm
`used_bytes`=13303025664 and cold `used_bytes`=13303025664 — identical to the
byte — while `du -sh` on the data nodes showed warm 12.6 GB vs cold 12.3 GB.

Root: the manager only receives a per-NODE `used_capacity`, not per-DISK
usage, so it cannot attribute bytes to the right tier and guesses by capacity
ratio.

Note: BN-era heartbeat already carries per-disk usage (`UpdateNodeDiskUsage`,
`DiskInfo.used_bytes` is populated and surfaced in `/api/v1/nodes`). So the
real per-disk numbers are now ON the node entries — `get_stats` just needs to
sum the real `disk.used_bytes` per tier instead of apportioning.

---

## 1. Goals / non-goals

**Goals:**
- `get_stats` per-tier `used_bytes` reflects real on-disk usage summed from
  each disk's reported `used_bytes`, not a capacity-ratio apportionment.
- `net_used` (RF-divided) derives from the corrected per-tier sums.

**Non-goals:**
- No heartbeat wire change (per-disk `used_bytes` already ships — confirm).
- No change to `/api/v1/nodes` (already correct per-disk).

---

## 2. Locked-in decisions

- Sum `disk.used_bytes` grouped by `disk.tier` across all data nodes for the
  per-tier `used_bytes`. Fall back to the legacy apportionment ONLY for a node
  whose disks report all-zero `used_bytes` (pre-BN/aged data node), so older
  data binaries degrade gracefully rather than reporting 0.
- Extract the per-tier rollup into a pure helper so it is unit-testable
  against a synthetic node set (mirrors BH-8 / BP-4 helper style).

---

## 3. Task list

### BB-1: per-tier used_bytes from real disk usage
- Status: ✅ (2026-06-02) — pure `node_tier_used(disks, used_capacity)`: trusts per-disk `used_bytes` (incl 0) when the node reports any; apportions only a legacy all-zero node. Wired into get_stats tier_breakdown. Units bb_1_real_per_disk_no_fabrication / bb_1_legacy_node_apportions / bb_1_zero_capacity_node_empty; dtfs-manager 154/154.
- Files: `crates/dtfs-manager/src/api.rs` (`get_stats` tier breakdown).
- Sketch:
  1. Add pure `tier_used_bytes(nodes) -> BTreeMap<DiskTier, u64>` summing
     `disk.used_bytes` by tier; per-node fallback to apportionment when that
     node's disks are all-zero.
  2. Replace the apportionment block in `get_stats` with the helper; feed the
     RF divisor for `net_used` from the corrected sums.
- Tests:
  - Unit `bb_1_tier_used_sums_real_disk_bytes`: a node with warm=12.6 GB /
    cold=12.3 GB reports those exact per-tier sums (not 50/50).
  - Unit `bb_1_all_zero_disks_falls_back`: a node whose disks report 0 falls
    back to apportionment so it doesn't read 0.
- Done when: unit green; live `/api/v1/stats` per-tier `used_bytes` matches
  `du` within rounding on the minicluster.

### BB-2: release case + changelog
- Status: ✅ (2026-06-02) — tests/release/cases/31-stats-tier-usage-fidelity.sh (asserts stats tier used_bytes matches /nodes per-disk sums within 1 MiB). changelog/UNRELEASED.md Bug BB entry. Auto-discovered by run.sh.
- Files: `tests/release/cases/NN-stats-tier-usage.sh`, `changelog/UNRELEASED.md`.
- Sketch: assert per-tier `used_bytes` from `/api/v1/stats` is not an exact
  cross-tier tie on a node with asymmetric tier usage (or matches
  `/api/v1/nodes` disk sums). Changelog entry: per-tier usage no longer
  fabricated by capacity apportionment.
- Done when: case PASS or `KNOWN_ISSUE [BB]`; changelog present.

---

## 4. Cross-cutting invariants

- Read-path only; no raft write, works on a follower.
- Graceful degradation for pre-BN data binaries (all-zero per-disk usage).

## 5. Verification log

> Append per-task.

## 6. Deviations from plan

> Append if reality diverges.
