# Chaos / cross-DC / balance testing

> **Dormant.** This page describes the Proxmox lab and its harnesses
> (`tests/release/`, `tests/proxmox/`). Since 2026-09 there is no build VM
> and no cluster; verification is unit/crate tests in Docker (see
> `CLAUDE.md` § A.4). Kept for the day a lab exists again.

The chaos suite is the part of the release gate that exercises behaviour you
cannot reach with in-process failure injection: powering a whole datacenter
off and on, yanking a disk out of a node and installing a fresh one, reading
across datacenters under bandwidth shaping, and driving a volume to a declared
replica-balance target and watching it converge. It runs against a real
Proxmox (PVE) lab — the 3dc topology — and is invoked as a subset of the
release suite (`tests/release/run.sh --chaos`).

It is additive to the SSH-only [release suite](./release-testing.md): the
ordinary `run.sh` / `run.sh --quick` runs are unchanged and need none of the
hypervisor access described here.

## Why a separate lab gate

The 42 standard release cases reach an already-running cluster over SSH and use
`systemctl stop` to fail a process. That covers process crashes and leader
rotation, but it cannot:

- cut power to an entire DC (and confirm Raft quorum survives on the survivors),
- remove a physical disk and re-add a blank one (and confirm re-replication
  then backfill),
- shape inter-DC bandwidth (and confirm reads still complete and Raft stays
  elected because its ports are exempt),
- prove a read was served by the *nearest* replica rather than a remote DC.

These need control of the hypervisor and a topology where a single DC failure
is survivable. That is what this suite adds.

## Required cluster composition

The chaos cases standardize on the **3dc** topology. Its defining property is
**one Raft voter per datacenter**, so losing any single DC leaves a 2/3
majority — quorum holds and recovery is observable. The single source of truth
for the topology (hostnames, Proxmox vmids, datacenters, per-node tier disks)
is `tests/proxmox/inventory-3dc.json`; both the bash helpers and the PVE chaos
script read it.

| Role | dc-a (anchor) | dc-b | dc-c | Why |
|---|---|---|---|---|
| Manager (Raft voter) | mgr-01 | mgr-02 | mgr-03 | 1 voter/DC → any DC can fail with quorum intact |
| Data nodes | data-a1, data-a2 | data-b1, data-b2 | data-c1, data-c2 | 2/DC → an intra-DC node/disk failure still leaves a same-DC replica |
| Client (FUSE) | client-a | client-b | client-c | 1/DC → a case can drive a read *from each DC* |
| Tier disks | hot + cold | warm | warm | dc-a holds the cold **anchor**; warm is the per-DC cache tier |

Key consequences:

- **Anchor DC = dc-a.** dc-a is the only DC with a `cold` disk, so the anchor
  invariant under test is "every chunk keeps at least one cold replica in dc-a"
  (§ A.3.4 in `CLAUDE.md`).
- **Cache tier = warm.** dc-b and dc-c are warm-only by design (dc-c is the
  "no cache, fast burst" path in the reference deployment). The per-DC cache
  cases therefore use `warm` as the cache tier — it exists in every DC — which
  keeps the two replica classes cleanly separated: `cold` = mandatory/anchor,
  `warm` = demand-driven cache.
- **Quorum math.** Killing dc-c (the case-43/44 target) drops mgr-03, leaving
  mgr-01 + mgr-02 = 2/3 voters → a leader stays elected. Killing dc-a would
  also remove the anchor, so dc-a is never the power-off target.

### Testable vs total-outage-by-design

The chaos suite deliberately does **not** target a plexmedia-style topology for
DC failure, because plexmedia concentrates all Raft voters and the anchor in
LELI:

| Scenario | 3dc | plexmedia |
|---|---|---|
| Single DC power-off, quorum holds, reads continue | testable | only TORO/NRB (LELI = total outage) |
| DC power-on recovery converges | testable | only TORO/NRB |
| Intra-DC node/disk failure, same-DC replica survives | testable | not (1 data node/DC) |
| Per-tier mandatory + per-DC cache balance | testable | testable |

## Prerequisites

On the machine that runs the suite (the operator's WSL/Linux host or the
builder VM):

- **`pwsh`** (PowerShell 7) — drives the PVE REST API via
  `tests/proxmox/pve.ps1` and `tests/proxmox/pve-chaos.ps1`. On Debian/WSL:
  `apt install powershell`.
- **`ansible`** — only for `--provision` (deploy playbooks) and case 48
  (netshape). Power/disk chaos itself needs no ansible.
- **`PVE_TOKEN`** — a Proxmox API token (`root@pam!dtfs-test=…`). Never commit
  it; export it for the run only.
- **SSH key** `~/.ssh/dtfs-lab/id_ed25519` (the suite default) reaching the lab
  VMs as `debian@<host>`, with the 3dc hostnames resolvable.

The runner reaches the PVE host directly at `https://10.20.0.121:8006`; the
chaos verbs are REST-only (no ssh to the hypervisor), so they work under Linux
`pwsh`.

## Running

```bash
# Against a cluster that is already deployed (you bring it up):
ALLOW_PVE_CHAOS=1 PVE_TOKEN='root@pam!dtfs-test=…' \
  MGRS=mgr-01,mgr-02,mgr-03 \
  DATA=data-a1,data-a2,data-b1,data-b2,data-c1,data-c2 \
  CLIENT=client-a \
  bash tests/release/run.sh --chaos

# From scratch: provision a clean 3dc cluster, run the cases, tear down:
ALLOW_PVE_CHAOS=1 PVE_TOKEN='root@pam!dtfs-test=…' \
  MGRS=mgr-01,mgr-02,mgr-03 \
  DATA=data-a1,data-a2,data-b1,data-b2,data-c1,data-c2 \
  CLIENT=client-a \
  bash tests/release/run.sh --chaos --provision
```

Expect ~45–60 min for `--chaos`, plus ~15–30 min when `--provision` clones and
deploys the VMs. VM boot/shutdown dominates the power cases.

### Gating env

| Var | Default | Effect |
|---|---|---|
| `ALLOW_PVE_CHAOS` | `0` | `1` enables the hypervisor-destructive cases (43, 44, 45, 48). Without it they **SKIP**, so `--chaos` still runs the non-PVE cases (42, 46, 47) against a live cluster. |
| `PVE_TOKEN` | _(unset)_ | Proxmox API token. Required for any PVE op (and for `--provision`). |
| `INVENTORY_JSON` | `tests/proxmox/inventory-3dc.json` | Topology map (hostname → vmid → dc → disks). |
| `KEEP_CLUSTER` | `0` | After a `--provision` run, `1` leaves the VMs up for debugging instead of destroying them. |

The standard `MGRS` / `DATA` / `CLIENT` / `MOUNT` / `VOL` / `MGR_PORT` /
`SSH_KEY` variables are documented in [release testing](./release-testing.md).

## Control layer

```
case (bash) ── lib/pve.sh ──▶ pwsh tests/proxmox/pve-chaos.ps1 ──▶ PVE REST API
              lib/dc.sh   ──▶ manager REST API + SSH + inventory-3dc.json
              lib/lifecycle.sh ▶ provision-3dc.ps1 + ansible deploy playbooks
```

- **`tests/proxmox/pve-chaos.ps1`** — DC-aware verbs over the tested
  `pve.ps1` helpers, resolving names/DCs from `inventory-3dc.json`. REST-only.

  ```
  pve-chaos.ps1 -Action power-off  -Dc dc-c -Force
  pve-chaos.ps1 -Action power-on   -Dc dc-c
  pve-chaos.ps1 -Action detach-disk -Vm data-a1 -Slot 2
  pve-chaos.ps1 -Action attach-disk -Vm data-a1 -Slot 2 -SizeGb 16
  pve-chaos.ps1 -Action destroy-all
  ```

- **`tests/release/lib/pve.sh`** — bash shims a case calls:
  `require_pve` (SKIP unless `ALLOW_PVE_CHAOS=1` + `PVE_TOKEN`), `power_dc_off`
  / `power_dc_on`, `power_host_off` / `power_host_on`, `detach_disk`,
  `attach_blank_disk` (which then re-runs `mount-data-disks.yml` so the new
  disk is formatted, mounted, and rejoins the ring).

- **`tests/release/lib/dc.sh`** — pure SSH/API + inventory helpers (no
  hypervisor), so the non-PVE cases use them without `ALLOW_PVE_CHAOS`:
  `client_for_dc` / `data_hosts_in_dc`, `hostname_to_node_id`,
  `served_bytes_for_node`, `count_replicas_in_dc` /
  `count_replicas_in_dc_tier` / `count_replicas_in_tier`, `quorum_ok`,
  `inspect_has_replicas`, `nodes_expose_io`.

- **`tests/release/lib/lifecycle.sh`** — `provision_cluster` (pwsh
  `provision-3dc.ps1` → ansible `full-deploy` / `mount-data-disks` /
  `apply-shaping` → wait ready + healthy) and `teardown_cluster` (pwsh
  `pve-chaos.ps1 -Action destroy-all`, honouring `KEEP_CLUSTER`). Wired into
  `run.sh` only when `--provision` is passed.

All three libs are sourced from `lib/common.sh`; the bash chaos cases need only
`source ../lib/common.sh` as usual.

## Observability the cases rely on

The cases assert real end-state through read-only manager APIs (added in the
same change as the suite):

- **`GET /api/v1/nodes`** — per-node `total_bytes_read` / `total_bytes_written`.
  Proves *which* DC served a read (case 42).
- **`GET /api/v1/files/inspect`** — per chunk, a `replicas` array of
  `{node_id, disk_id, tier}` alongside the back-compat `node_ids`. Lets a case
  tell a `cold` (mandatory/anchor) replica from a `warm` (cache) one even on a
  multi-tier node like dc-a (cases 45, 46, 47).
- **`GET /api/v1/migrations`** — scheduler replication/backfill tasks
  (`chunk_id`, `target_node`, `tier`, `status`, `attempts`, `last_error`).
- **`GET /api/v1/audit?limit=N`** — action strings such as `cache.flush_*` and
  the `AnchorViolated` alert; cases assert these appear / do not appear.
- **`GET /api/v1/stats`** — `chunk_placement` distribution (stable / misplaced /
  undergoal / …) for convergence checks.

## The cases

| # | Case | Scenario | Asserts | PVE |
|---|---|---|---|---|
| 42 | crossdc-read-routing | write from dc-a, first read from client-b | dc-b data nodes' `total_bytes_read` jumps ~filesize and out-serves dc-a/dc-c | — |
| 43 | dc-poweroff-read-continuity | `power_dc_off dc-c` | a leader remains (`quorum_ok`) and the file reads byte-exact from dc-b | yes |
| 44 | dc-poweron-recovery-converge | power dc-c down, back up | warm replicas return to target and the dc-a anchor copy survived; no `AnchorViolated` | yes |
| 45 | disk-failure-and-backfill | yank the cold disk holding the anchor, then install a blank one | cold replica re-replicated to the other dc-a node; replacement disk rejoins the ring; file intact | yes |
| 46 | autobalance-convergence | set mandatory `cold=1` (anchor dc-a) + warm cache per-DC `1/1`, read from each DC | converges to 1 cold (dc-a) + 1 warm per reading DC, and stays converged on re-poll | — |
| 47 | cache-perdc-promote-demote | read from dc-b promotes a warm cache replica; then `flush_cache` | warm cache appears in dc-b, drops on flush, cold anchor survives, `cache.flush` audited | — |
| 48 | crossdc-read-under-shaping | pin the only replica to dc-a, apply tc shaping, read from dc-b | cross-DC read completes within the deadline and matches; Raft stays elected (ports exempt) | yes |

Every case registers `trap case_cleanup EXIT` and, best-effort, powers DCs back
on, re-attaches disks, restores the volume policy, reverts shaping, and removes
its fixture — so a mid-case failure does not leave the lab wedged. The PVE cases
call `require_pve` first, so a run without `ALLOW_PVE_CHAOS=1` SKIPs them
cleanly.

### Known behavioural caveat (case 45)

Case 45 has two halves. The detect-and-re-replicate half (a yanked disk's
replica is rebuilt on a healthy peer) is the hard assertion. The second half —
the scheduler actively *backfilling* a freshly-installed blank disk without any
new writes — may not happen on all builds; if it does not, case 45 reports
`KNOWN_ISSUE` for that half rather than failing the gate. The first half passes
regardless.

## Adding a chaos case

1. Add `tests/release/cases/NN-<name>.sh`, `source ../lib/common.sh`.
2. If it touches the hypervisor, call `require_pve` early and use the
   `lib/pve.sh` verbs; otherwise use only `lib/dc.sh` helpers so it can run
   without `ALLOW_PVE_CHAOS`.
3. Register a `case_cleanup` trap that restores any lab state it changed.
4. Append its stem to `CHAOS_CASES` in `tests/release/run.sh`. Keep it **out**
   of `QUICK_CASES`.
5. Last stdout line must be `STATUS: PASS|FAIL|SKIP|KNOWN_ISSUE [<bug>]`.

## See also

- [Release testing](./release-testing.md) — the full release gate and its env.
- [Failure Scenarios](./failure-scenarios.md) — the behaviours these cases
  exercise.
- `plans/active/PLAN-REL-CHAOS-SUITE.md` — design, task status, verification
  log.
