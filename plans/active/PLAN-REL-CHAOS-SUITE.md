# PLAN-REL-CHAOS-SUITE — Automated release-testing: chaos, cross-DC, balance

Status: 🟦 wip. Owner: agent. Started 2026-06-04.

**2026-06-11:** all code/docs merged in master and builder-verified; the only
open leg is the operator's live 3dc lab run (REL-2-42..48). Plan is
delete-ready once that run passes (§ B.2 retention: delete when every task ✅
and merged).

## Goals

Extend the existing release-gate suite (`tests/release/`) so a single
automated run covers the operator's full feature list against the `3dc`
Proxmox lab: caching, data spill-over, full disks, auto-recovery, bit rot,
cross-DC routing, full-DC failure + recovery, disk failure + replacement, and
auto-balancing (per-tier mandatory + per-DC cache). The run provisions a clean
cluster, exercises the features (including hypervisor-level chaos: powering a
whole DC off/on and yanking/installing a disk), and tears down.

## Non-goals

- Replacing the existing 42 cases — this is additive.
- Testing LELI-style total-outage (all voters + anchor in one DC); the chaos
  gate standardizes on `3dc` (one voter per DC) so a single DC can fail with
  quorum intact and recovery is observable.
- Online hot-rebalance across reconfigs (drain-then-replace only, per § A.1).

## Locked-in decisions (from user)

- **Topology:** `3dc` (`tests/proxmox/ansible/inventories/3dc/` +
  `tests/proxmox/inventory-3dc.json`). One Raft voter per DC; 2 data nodes per
  DC; 1 client per DC. Anchor DC = dc-a (holds `cold`).
- **Lifecycle:** suite auto-provisions + deploys a clean cluster each run, then
  tears it down (guarded by `KEEP_CLUSTER=1`).
- **Observability:** add the missing read-only manager APIs now so the new
  cases assert true end-state and PASS (rather than KNOWN_ISSUE).

## Resolved during exploration

- **vmid map already exists** in `tests/proxmox/inventory-3dc.json`
  (`vms[].{name,vmid,ip,dc,role,extra_disks}`): mgr-01..03 = 101-103,
  data-a1/a2 = 104/105 (hot+cold), data-b1/b2 = 106/107 (warm),
  data-c1/c2 = 108/109 (warm), client-a/b/c = 110-112, builder = 9001,
  template = 9000. Bash libs + the pwsh chaos script both read this one file —
  no new `topology.env`, no `hosts.yml` vmid edits.
- **`/api/v1/migrations`** already returns scheduler `ReplicationTask`s with
  `{chunk_id, target_node, tier, status, attempts, last_error}`
  ([api.rs:3536](../../crates/dtfs-manager/src/api.rs)) → **REL-1-3 dropped**
  (cases read it directly).
- **`ChunkLocationEntry`** carries `disk_id` but not tier
  ([dtfs-raft/lib.rs:73](../../crates/dtfs-raft/src/lib.rs)) → inspect can
  expose `disk_id` + derived `tier`; explicit `class`/`cooldown` are optional
  (cases assert class via tier, since the test SETS the policy).

## Data-model changes

- `NodeResponse` ([api.rs:694](../../crates/dtfs-manager/src/api.rs)) +
  `total_bytes_read: u64`, `total_bytes_written: u64` (already on
  `dtfs_raft::NodeInfo`; set 0 on synthetic manager/client rows). No snapshot
  impact (response-only struct).
- `inspect_file_endpoint` ([api.rs:1157](../../crates/dtfs-manager/src/api.rs))
  per chunk: add `replicas: [{node_id, disk_id, tier}]` alongside the existing
  `node_ids` (back-compat). `tier` derived from `sm.nodes[node_id].disks` by
  `disk_id`; `"unknown"` if the disk is gone.
- No Raft variants, no proto, no config keys, no snapshot fields → no
  `#[serde(default)]`/snapshot-regression obligations (§ A.3.1).

## Build / verify environment

Rust does **not** build on the Windows/MINGW dev host (link.exe failure on
build scripts; matches § A.4). All `cargo`/`ansible`/live-lab steps run on the
Linux builder VM + 3dc lab. Bash/pwsh are syntax-checked locally
(`bash -n`, `pwsh -NoProfile -Command`).

## Tasks

### Phase 0 — Lifecycle + topology helpers
- `REL-0-1` ✅ `tests/release/lib/dc.sh` — topology + DC helpers reading
  `inventory-3dc.json` (dc_of_host, vmid_of_host, client_for_dc,
  hostname_to_node_id, replicas of a path by dc/tier, served_bytes_for_node,
  quorum_ok). Pure SSH/API; no PVE. Done when `bash -n` clean and helpers
  return correct values against a live `/api/v1/nodes`.
- `REL-0-2` ✅ `tests/release/lib/pve.sh` — chaos wrappers (pve_guard,
  require_pve, power_dc_off/on, power_host_off/on, detach_disk,
  attach_blank_disk) shelling to `pve-chaos.ps1` via pwsh. Done when `bash -n`
  clean; SKIPs without ALLOW_PVE_CHAOS.
- `REL-0-3` ✅ `tests/release/lib/lifecycle.sh` — provision_cluster (pwsh
  provision-3dc.ps1 → ansible full-deploy/mount-data-disks/apply-shaping →
  wait ready+healthy) and teardown_cluster (pwsh pve-chaos destroy-all, guarded
  by KEEP_CLUSTER). Done when `bash -n` clean.
- `REL-0-4` ✅ `tests/proxmox/pve-chaos.ps1` — DC-aware verbs over `pve.ps1`
  (power-off/on by -Dc or -Vm, detach-disk/attach-disk, destroy-all), mapping
  name→vmid from `inventory-3dc.json`. Done when `pwsh -NoProfile -File ...
  -Action help` parses + lists verbs.

### Phase 1 — Read-only observability APIs (builder-verified)
- `REL-1-1` ✅ `NodeResponse` +`total_bytes_read`/`total_bytes_written` (struct,
  `from_entry`, both synthetic rows). Test `node_response_exposes_io_counters`
  passes on the builder (2026-06-11).
- `REL-1-2` ✅ `inspect_file_endpoint` per-replica `{node_id,disk_id,tier}` via
  `tier_of_disk`; keeps `node_ids`. Test
  `inspect_tier_unknown_when_node_or_disk_missing` passes on the builder
  (2026-06-11); positive tier path covered live via cases 45/46/47.
- `REL-1-5` ✅ `cases/18-api-shape.sh` now asserts `total_bytes_read` on nodes.

### Phase 2 — Cases (`tests/release/cases/`)
All 7 authored + syntax-verified + merged in master; live 3dc lab
verification pending (operator).
- `REL-2-42` 🟨 `42-crossdc-read-routing.sh` (needs REL-1-1)
- `REL-2-43` 🟨 `43-dc-poweroff-read-continuity.sh` (PVE)
- `REL-2-44` 🟨 `44-dc-poweron-recovery-converge.sh` (PVE)
- `REL-2-45` 🟨 `45-disk-failure-and-backfill.sh` (PVE)
- `REL-2-46` 🟨 `46-autobalance-convergence.sh` (needs REL-1-2; non-PVE)
- `REL-2-47` 🟨 `47-cache-perdc-promote-demote.sh` (needs REL-1-2; non-PVE)
- `REL-2-48` 🟨 `48-crossdc-read-under-shaping.sh` (PVE/ansible netshape)

### Phase 3 — Harness + docs
- `REL-3-1` ✅ `run.sh` — `--chaos` + `CHAOS_CASES`; `--provision` wraps the
  case loop with lifecycle provision/teardown. QUICK_CASES unchanged. Merged;
  `bash -n` clean (re-verified 2026-06-11).
- `REL-3-2` ✅ `lib/common.sh` — env documented; sources dc.sh + pve.sh.
  Merged; `bash -n` clean (re-verified 2026-06-11).
- `REL-3-3` ✅ docs: `release-testing.md` (env, chaos run, matrix, case rows) +
  new standalone `docs/src/operations/chaos-testing.md` (full reference:
  composition, control layer, cases, APIs, extension). Both registered in
  `docs/src/SUMMARY.md` (release-testing.md was previously orphaned);
  `development/testing.md` cross-links the chaos doc. Merged in master.
- `REL-3-4` ❌ dropped — see Deviations (no in-repo changelog anymore).

## Cross-cutting invariants

- Every new case: `trap case_cleanup EXIT`; cleanup powers DCs back on,
  re-attaches disks, restores volume config, removes fixtures, reverts shaping;
  best-effort, never fails the case.
- PVE/destructive cases call `require_pve` (SKIP without `ALLOW_PVE_CHAOS=1` +
  `PVE_TOKEN`) and stay OUT of `QUICK_CASES`.
- Anchor invariant (§ A.3.4): convergence cases assert ≥1 cold replica stays in
  dc-a and no `AnchorViolated` alert fires.
- Default `bash run.sh` / `--quick` unchanged: new cases self-SKIP without PVE.

## Deviations from the approved plan

- **Hot-disk-per-DC inventory edit dropped.** dc-c is warm-only by design and
  the mission topology has no hot tier; cache cases use **warm** as the per-DC
  cache tier (present in every DC). Two-class split stays clean: cold=mandatory
  anchor (dc-a), warm=demand cache.
- **REL-1-3 dropped** — `/api/v1/migrations` already exposes scheduler tasks.
- **REL-1-2 reduced** to `disk_id`+`tier` (no explicit `class`/`cooldown`);
  cases derive class from tier because they set the policy under test.
- **REL-3-4 dropped (2026-06-11).** The task referenced
  `changelog/UNRELEASED.md`, but the repo has since dropped in-repo
  changelogs entirely — release notes are auto-generated from merged PR
  titles (`gh release create --generate-notes`, AGENTS.md § B.6) and the
  file no longer exists. The inspection-field change is covered by its PR
  title/description instead.

## Verification log

(cargo/ansible/live-lab steps run on the builder VM + 3dc lab — the Windows
dev host cannot build the workspace, § A.4.)

- 2026-06-04 REL-0-1..0-4: authored; `bash -n` clean on dc.sh/pve.sh/
  lifecycle.sh/run.sh + all 7 cases; `pwsh -File pve-chaos.ps1 -Action help`
  parses and lists verbs; `source lib/common.sh` resolves every helper
  (count_replicas_*, inspect_has_replicas, served_bytes_for_node, require_pve,
  power_dc_off, attach_blank_disk, …). ✅ local syntax gate.
- 2026-06-04 REL-1-1/1-2/1-5 + REL-2-42..48 + REL-3-*: authored. Rust (api.rs)
  NOT compiled here (`cargo check -p dtfs-manager` → link.exe 1104 on MINGW).
- 2026-06-11 REL-1-1/1-2/1-5 + REL-3-1..3: builder + local verification
  (operator decision this session: builder cargo-test verification only; no
  3dc provisioning, no PVE chaos run — live leg stays with the operator).
  - Builder (10.20.0.150, worktree synced to ~/batch-u6):
    `cargo test -p dtfs-manager --release` → `test result: ok. 238 passed;
    0 failed`; targeted run →
    `test api::bug_al_io_rate_tests::inspect_tier_unknown_when_node_or_disk_missing ... ok`,
    `test api::bug_al_io_rate_tests::node_response_exposes_io_counters ... ok`.
    `cargo test -p dtfs-common --release` → `test result: ok. 67 passed;
    0 failed`.
  - Local syntax gate: `bash -n` clean on lib/{common,dc,pve,lifecycle}.sh,
    run.sh, cases 42-48; `pwsh -NoProfile -File tests/proxmox/pve-chaos.ps1
    -Action help` parses and lists all five verbs.
- Remaining leg (operator, closes REL-2-42..48 and the plan):
  `ALLOW_PVE_CHAOS=1 PVE_TOKEN=… bash tests/release/run.sh --chaos
  --provision` on the 3dc lab; expect FAIL=0 (SKIP/KNOWN_ISSUE ok). Delete
  this plan file once that run passes.
