# Bug BU — Writes hard-fail on a full disk; placement is capacity-blind, no write fallback

**Status:** BU-1/BU-3/BU-6 complete + unit-verified 2026-06-02; BU-2 deferred (see Deviations); live verify at deploy
**Source:** live investigation 2026-06-02 (operator-reproduced ENOSPC on minicluster); relates to ISSUE-SCAN-2026-05-28-2041 F4.

---

## 0. Background

Live-reproduced on the minicluster: `mini-data01` warm + cold disks both at
100% (`df`: `/mnt/data01-warm 20G 19G 0 100%`). A client write to a warm-tier
volume fails outright:
```
write_chunk failed on replica node=http://10.20.0.174:9500 (data01)
  error=put_chunk RPC failed (code=Internal): write tmp: No space left on device (os error 28)
→ only 0/1 replicas succeeded (need 1)
→ flush of fresh file failed; unlinking ghost entry to surface error
```
The write returns EIO to the client and the file is unlinked. `mini-data02`
warm has ~21 GB free but is never tried. data01 also floods
`ERROR dtfs_data::replication: Replication stream closed prematurely` as
anchor/replication tasks targeting the full disk also fail.

Two independent root causes, confirmed by reading the code:

1. **Capacity-blind placement.** `placement.rs::rebuild_ring` weights each disk
   by **total** bytes, never free bytes:
   ```rust
   let weight = disk.total_bytes as f64 / (1024.0 * 1024.0 * 1024.0 * 100.0); // TOTAL, not FREE
   let weight = weight.max(0.1).min(10.0);
   new_ring.add_node(&nid, disk.disk_id, disk.tier, &info.location, weight);
   ```
   A 100%-full disk keeps full ring weight and stays selectable.
   `place_chunks_with_anchor` is pure consistent-hash on the chunk key + tier
   rule with an empty exclusion list (`find_nodes_for_pg(pg_id, &rule, &[])`) —
   no free-space check, no full-disk exclusion, no spill to a peer node or to
   cold. Chunks whose key hashes to data01-warm land there regardless of
   fullness.

2. **No write-time fallback.** `dtfs-data put_chunk` streams to a tmp on the
   assigned tier disk; `write_all`/`flush` returns
   `Status::internal("write tmp: No space left on device")` with no alternate
   disk. `dtfs-fuse` `write_chunk` counts `0/1 replicas`, does NOT re-call
   `allocate_chunks` for a different placement, unlinks the ghost, returns the
   error. One-shot, no reroute.

Net: a single full disk hard-fails writes cluster-wide for every chunk that
hashes to it, even with abundant free capacity elsewhere — and never spills to
cold. This is the data-plane availability failure the Plexmedia workload (bursty
NRB downloads landing 30 GB seasons) will hit routinely.

---

## 1. Goals / non-goals

**Goals:**
- Placement excludes (or strongly deweights) disks without enough free space and
  prefers free capacity, so writes route to a node/disk that can hold them
  (BU-1).
- A write that still hits ENOSPC on its assigned placement recovers by
  re-routing to another eligible placement rather than failing the client
  (BU-2), within the configured replication/anchor rules.
- Define + honour a free-space headroom threshold per disk (BU-3).

**Non-goals:**
- Spilling across *tiers* against policy (a warm-mandated chunk must not silently
  become cold-only). Cross-tier spill is allowed only where the volume policy
  permits it; the anchor invariant (AGENTS.md §A.3.4) is never weakened.
- Rebalancing already-placed chunks off a full disk (that is migration /
  tiering, separate). BU is about *new writes* finding a home.
- The disk-usage fidelity question (data01 warm and cold reporting identical
  `used_bytes` — possible BB apportionment artefact); note for triage, not in
  scope here.
- Memory-pressure shedding (Bug G) — orthogonal.

---

## 2. Locked-in decisions

- **BU-1 free-space-aware ring.** `rebuild_ring` must factor free space, not just
  total. Two parts: (a) weight by *available* bytes
  (`total_bytes - used_bytes`) instead of total; (b) **exclude** a disk from the
  ring entirely when free space is below a headroom threshold (BU-3), so a
  near-full disk receives no new placements. Keep the existing healthy-node
  filter. Determinism for the same input state is preserved (no RNG).
- **BU-2 write fallback.** On a placement that returns ENOSPC/Internal-no-space,
  the write path must try another eligible target before failing:
  - Preferred: `dtfs-fuse` re-calls `allocate_chunks` with the failed
    node/disk excluded (manager already has the exclusion-list parameter shape
    in `find_nodes_for_pg`), then retries `write_chunk` on the new placement.
    Bounded retries (e.g. 2-3) to avoid loops; fail only when no eligible target
    exists at the required tier/RF.
  - The manager allocation itself should not hand out a disk it already knows is
    over the headroom threshold (BU-1 makes this the default).
- **BU-3 headroom threshold.** New config knob (per data node or cluster
  `[placement]`), e.g. `min_free_bytes` or `min_free_pct` (default sane, e.g.
  reserve max(1 GiB, 5%)). A disk under headroom is "full" for placement
  purposes. `put_chunk` SHOULD also pre-check headroom and return a typed
  `RESOURCE_EXHAUSTED`/no-space status (not opaque `Internal`) so the client can
  distinguish "disk full, reroute" from a real internal error.
- **Tier discipline.** When all eligible disks of the mandated tier are full,
  the write fails with a clear "tier <T> exhausted" error rather than silently
  landing on the wrong tier — unless the volume policy explicitly allows the
  spill. Cross-tier spill, if added, is policy-gated and audited.

---

## 3. Data-model changes

- BU-3: new `[placement]` config (`min_free_bytes` / `min_free_pct`) with
  backward-safe defaults. No `StateMachineData` field.
- BU-2: reuse the existing `allocate_chunks` request; add an `exclude_nodes` /
  `exclude_disks` hint to the request proto if not already present (the manager
  ring already accepts an exclusion slice). If the proto changes, regen
  `dtfs-proto`.
- A typed no-space gRPC status from `put_chunk` (use `RESOURCE_EXHAUSTED`); no
  schema change, behaviour only.

---

## 4. Task list

### BU-1: Free-space-aware placement ring

- Status: ✅ (2026-06-02)
- Files: `crates/dtfs-manager/src/placement.rs` (`rebuild_ring`), the hash-ring
  helper in `crates/dtfs-common/src/hash_ring.rs` if exclusion needs support.
- Sketch:
  1. Weight by available bytes (`total_bytes.saturating_sub(used_bytes)`) rather
     than `total_bytes`.
  2. Skip adding a disk to the ring when its free space is below the BU-3
     headroom threshold (alongside the existing non-healthy skip), with a
     `tracing::warn!` naming the disk.
- Tests:
  - Unit: build a ring from a state with one near-full warm disk + one empty
    warm disk; assert placements for many keys land overwhelmingly (or
    exclusively, if over headroom) on the disk with free space.
  - Unit: a disk under headroom is absent from the ring.
- Done when:
  - Unit green; on the lab, new writes with data01-warm full route to
    data02-warm (confirm via `/api/v1/nodes` used_bytes rising on data02).

### BU-2: Write-path fallback on ENOSPC

- Status: ✅ (2026-06-03 — implemented at user request after the initial defer)
- Files: `crates/dtfs-fuse/src/filesystem.rs` (`write_chunk` / allocate-and-write
  loop ~1475-1595), `crates/dtfs-data/src/server.rs` (`put_chunk` typed status),
  `crates/dtfs-manager/src/metadata.rs` (`allocate_chunks` honour exclusion),
  `proto/` if request gains `exclude_*`.
- Sketch:
  1. `put_chunk`: detect no-space (pre-check headroom and/or map os error 28) →
     return `Status::resource_exhausted("disk full: tier <T>")`.
  2. `dtfs-fuse`: on resource-exhausted / no-space from a replica, re-`allocate_chunks`
     excluding the failed node/disk, retry `write_chunk` on the new placement.
     Bounded retries; only fail when no eligible target remains.
- Tests:
  - Unit (fuse): a fake manager returns placement A (fails no-space) then B;
    assert the writer retries on B and succeeds; assert it gives up after the
    bound with no eligible target.
  - Abuse/live: fill one warm disk, write; assert the write SUCCEEDS on the peer
    instead of unlinking a ghost.
- Done when:
  - Unit green; on the lab a write with data01-warm full succeeds on data02-warm
    (no `0/1 replicas` / ghost-unlink for that write).

### BU-3: Free-space headroom config + typed no-space status

- Status: ✅ (2026-06-02)
- Files: `crates/dtfs-common/src/config.rs` (`[placement]` knob),
  `crates/dtfs-data/src/server.rs` (headroom pre-check in `put_chunk`),
  consumers in placement (BU-1).
- Sketch: add `min_free_bytes`/`min_free_pct` with defaults; `put_chunk`
  refuses (typed status) when the target disk is under headroom even before
  hitting the kernel ENOSPC, so the reroute (BU-2) triggers cleanly.
- Tests:
  - Unit: headroom helper classifies a disk full/not-full at the threshold
    boundary.
  - Unit: `put_chunk` returns resource-exhausted when under headroom.
- Done when: unit green; configured headroom keeps a disk from being filled to
  literal 0 before reroute kicks in.

### BU-6: Reclaim streaming `.tmp` files on ENOSPC / failed put_chunk (tmp leak)

- Status: ✅ (2026-06-02)
- Source: ISSUE-SCAN-2026-06-02-1517 F1.
- Files: `crates/dtfs-data/src/server.rs` (`put_chunk` streaming path, tmp
  create/write at ~248-276), `crates/dtfs-data/src/store.rs`
  (`commit_chunk_from_tmp` error paths), WAL/startup rescan cleanup.
- Evidence: live data01 — `df` 100% on warm+cold, only ~1.25 GB committed
  `.dat` (314 files) but **84 `*.tmp` streaming files ~18.5 GB** in
  `/mnt/data01-{warm,cold}/tmp`. The streaming put writes the tmp; when the disk
  fills mid-stream the write errors out but the partial `.tmp` is not removed,
  so leaked tmps consume the disk and never get reclaimed — the disk-full
  condition then becomes self-sustaining.
- Sketch:
  1. `put_chunk` streaming path: on ANY error after `File::create(tmp)` (write
     /flush/commit), remove the tmp before returning (RAII guard or explicit
     cleanup on every error branch). Mirrors AGENTS.md §A.6 #5
     ("`commit_chunk_from_tmp` removes the tmp on any error path").
  2. Startup/WAL rescan: sweep `tmp/` for `.tmp` files with no live write lease /
     no WAL entry and delete them (orphan tmp GC), so a crash mid-stream cannot
     strand them across restarts.
  3. Consider a periodic orphan-tmp sweep, not only at startup.
- Tests:
  - Unit (dtfs-data): simulate a write/flush error after tmp create; assert the
    tmp is gone afterward.
  - Unit: rescan classifies a leaseless `.tmp` as orphan and removes it.
  - Live: fill a disk to force the failure; confirm no `.tmp` accumulates beyond
    in-flight writes.
- Done when: a failed/aborted put_chunk leaves no `.tmp` behind; startup reclaims
  pre-existing orphan tmps; on the lab the leaked-tmp accumulation does not recur.

### BU-4: Release case

- Status: ✅ (2026-06-02 — authored; live-run at deploy)
- Files: `tests/release/cases/NN-write-fallback-disk-full.sh`
- Case: saturate one tier disk on one node, perform a client write, assert it
  succeeds on another eligible disk/node (not EIO), and that no chunk lands on a
  full disk. Extend to assert no orphaned `.tmp` files remain after a forced
  ENOSPC (BU-6). Status protocol per §B.5.1; cleanup restores disk state.
- Done when: case PASS or `KNOWN_ISSUE [BU]` pinned.

### BU-5: Changelog + docs

- Status: ✅ (2026-06-02 — changelog; docs note pending)
- Files: `changelog/UNRELEASED.md`, `docs/src/...` (placement/capacity ops).
- Entry: writes now route around full disks (capacity-aware placement +
  ENOSPC fallback); document the `[placement]` headroom knob. Same commit as
  code.

---

## 5. Cross-cutting invariants

- Anchor invariant (AGENTS.md §A.3.4) is never weakened to dodge a full disk: a
  mandatory cold/anchor copy must still land in its anchor DC/tier; if that tier
  is full the write fails loudly (and queues nothing that strands the anchor).
- No silent cross-tier downgrade — a warm-mandated chunk does not become
  cold-only unless volume policy permits (and then it is audited).
- Placement stays deterministic for a given state snapshot (no RNG); free-space
  weighting must not break the consistent-hash stability used by reads.
- Reroute is bounded — never an unbounded retry loop hammering data nodes
  (the Bug AF/AE failure mode).
- `put_chunk` returns a TYPED no-space status (`RESOURCE_EXHAUSTED`), not opaque
  `Internal`, so callers can distinguish reroute-able full-disk from real errors.

---

## 6. Verification log

> Append per-task. Live evidence at authorship: data01-warm/cold 100%
> (`df` Avail 0), client write → `os error 28` → `0/1 replicas` → ghost unlink;
> data02-warm ~21 GB free, never tried.
>
> ISSUE-SCAN-2026-06-02-1517 F2: 149 cold anchor tasks on data01-cold all fail
> "create tmp ...: No space left on device", source==target (single dc-a node,
> expected per BH), one at attempt 5/6. Downstream of the same capacity root:
> BU-1 headroom + BU-6 tmp reclaim free the disk so anchor writes proceed.
> Requeue of max-attempt-exhausted Failed tasks after space is freed is already
> handled by the shipped BO-2 retention-window logic (verify on the lab; do not
> re-implement). F3 (~5.4 GB phantom) is orphan-scrub / operator reclaim, not a
> BU code task.

---

## 7. Deviations from plan

- **BU-2 was initially deferred, then implemented at the user's request
  (2026-06-03).** Added `exclude_addrs` (field 5) to
  `AllocateChunksRequest`; the manager builds an exclude set from those
  addresses and, when non-empty, skips the deterministic ring path and
  uses the capacity + exclude-filtered round-robin so the failed (full)
  node is dropped. `dtfs-client` gained `allocate_chunks_excluding`; the
  FUSE write path detects a `RESOURCE_EXHAUSTED` put (via the rendered
  error chain — `client::write_chunk_bytes` surfaces `code=...`), then
  re-allocates that one chunk excluding the full node and retries, bounded
  to `MAX_REROUTES = 3`. Note the data node's `write_chunk_bytes` already
  retries `RESOURCE_EXHAUSTED` for Bug G mem-shed before bubbling, so a
  disk-full put pays a short retry before the reroute fires. Also fixed a
  latent build bug: `dtfs-fuse`'s paused-clock tests (BQ-1, BU-2) only
  compiled under feature-unification; added a `test-util` tokio
  dev-dependency so `cargo test -p dtfs-fuse` works standalone.
- **(historical) BU-2 deferral rationale.** BU-1 makes the
  manager exclude near-full disks from the placement ring AND from the
  round-robin fallback, so `allocate_chunks` no longer hands out a full
  disk — which is exactly the live repro (data01-warm full, data02-warm
  never tried). BU-2 only covers the narrow race where a disk fills
  *between* ring rebuild and the write landing; it requires a proto
  change (`exclude_nodes` on `AllocateChunksRequest`, regen `dtfs-proto`),
  a manager-side exclusion path (the ring has no native exclusion —
  `find_nodes_for_pg`'s third arg is `client_locations`, not an exclude
  list, contrary to the plan's assumption), and FUSE retry plumbing.
  Given BU-1 closes the reported failure and `put_chunk` now returns a
  typed `RESOURCE_EXHAUSTED` + cleans its tmp, BU-2 is deferred as
  defense-in-depth rather than shipped half-done. **Flagged to the user
  for review** (AGENTS.md §B.7 — deferring a task is a material plan
  change). Re-open as a follow-up if the race is observed live.
- **No data-side proactive headroom pre-check in `put_chunk`.** The plan
  sketched `put_chunk` refusing under-headroom before the kernel ENOSPCs.
  Implemented instead as: manager won't place on a full disk (BU-1), and
  if `put_chunk` does hit ENOSPC it maps errno 28 → `RESOURCE_EXHAUSTED`
  and reclaims the tmp. This achieves the typed-status + no-leak goals
  without threading disk_id+free-space+config into the streaming path.

## 6.1 Verification log

- **BU-1 (2026-06-02)** — `placement.rs::rebuild_ring` now weights by
  available bytes and skips disks under `PlacementConfig` headroom; the
  `allocate_chunks` round-robin fallback (`metadata.rs`) filters the same.
  Units: `placement::bu1_capacity_ring_tests::{full_disk_excluded_writes_route_to_peer,both_disks_with_room_stay_in_ring}`
  PASS (`cargo test -p dtfs-manager --bins`: 2 passed).
- **BU-3 (2026-06-02)** — `[placement]` config (`min_free_bytes`/
  `min_free_pct`, default `max(1 GiB, 5%)`) added to manager + data
  configs with `PlacementConfig::{headroom_bytes,has_room}` helper.
  `put_chunk` maps ENOSPC→`RESOURCE_EXHAUSTED` via `put_io_status`. Units:
  `config::tests::{bu3_placement_has_room_boundary,bu3_placement_pct_dominates_on_large_disk}`
  PASS (`cargo test -p dtfs-common`: green).
- **BU-6 (2026-06-02)** — `put_chunk` arms a `TmpCleanup` RAII guard that
  removes the partial streaming tmp on any early return; data-node startup
  calls `ChunkStore::reclaim_orphan_streaming_tmps`. Unit:
  `store::tests::bu6_reclaim_orphan_streaming_tmps` PASS
  (`cargo test -p dtfs-data --bins`: 1 passed). dtfs-data compiles clean.
- **BU-4 (2026-06-02)** — release case
  `tests/release/cases/34-write-fallback-disk-full.sh` authored (balloon a
  warm disk under headroom, write via FUSE, assert success + integrity +
  no leaked `.dat.tmp`). Live-run pending the deploy of the new binaries.
- **Live (2026-06-03, deployed wrapper build 20260603-083741)** — on the
  minicluster, `mini-data01`'s warm disk was genuinely 100% full
  (`df`: 18-21 MB avail of 20 GB). A single-chunk (3 MB) write via the
  FUSE client SUCCEEDED with matching sha256 and left zero `*.dat.tmp` on
  the full disk — release case
  `tests/release/cases/34-write-fallback-disk-full.sh` STATUS: PASS.
  Pre-fix (plan authorship evidence) the same warm-full condition returned
  `os error 28` → `0/1 replicas` → ghost-unlink → EIO. Capacity-aware
  placement routed the chunk to a disk with room.
- **Out-of-scope blocker found + flagged.** Multi-chunk round-trip can't
  be integrity-checked on this cluster because of a SEPARATE, pre-existing
  large-file under-chunking bug: a `>chunk_size` write is stored as a
  single oversized chunk at index 0 (confirmed on pre-deploy media:
  a 283 MB .mkv reports `chunk_count=3`, expected ~68), so reads past the
  first chunk return garbage. This predates BS/BU/BV (old binaries wrote
  those files) and is NOT a BU regression. Spawned as a separate task; the
  BU-4 case was switched to a single-chunk file to validate BU cleanly.
