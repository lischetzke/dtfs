# Release testing (Bug BE)

The release-gate suite at `tests/release/` is the single artefact
the operator runs prior to a release tag. Anything not in it is
not gated by release.

## Run

```
bash tests/release/run.sh            # full suite (~60-90 min)
bash tests/release/run.sh --quick    # ~15 min subset
bash tests/release/run.sh --chaos    # chaos/cross-DC/balance subset (3dc lab)
```

Required env (defaults match the lab minicluster):

| Var | Default | Notes |
|---|---|---|
| `MGRS` | `mini-mgr01,mini-mgr02,mini-mgr03` | csv |
| `DATA` | `mini-data01,mini-data02` | csv |
| `CLIENT` | `mini-client01` | host |
| `MOUNT` | `/mnt/dtfs` | FUSE mount on CLIENT |
| `VOL` | `default` | volume id |
| `MGR_PORT` | `9402` | manager API |
| `SSH_KEY` | `~/.ssh/dtfs-lab/id_ed25519` | private key |
| `ALLOW_PVE_CHAOS` | `0` | `1` enables hypervisor-destructive cases (else they SKIP) |
| `PVE_TOKEN` | _(unset)_ | Proxmox API token `root@pam!dtfs-test=…`; never commit |
| `INVENTORY_JSON` | `tests/proxmox/inventory-3dc.json` | topology map (hostname→vmid→dc→disks) |
| `KEEP_CLUSTER` | `0` | after `--provision`, `1` leaves the VMs up for debugging |

## Output

Per run, `target/release-reports/RELEASE-<id>/` (untracked):

- `<case>.log` — per-case stdout
- `summary.json` — counts + per-case status (PASS / FAIL / SKIP /
  KNOWN_ISSUE[bug-id])

Exit code is 0 iff zero FAILs. KNOWN_ISSUE rows do not fail the
suite — they pin to a Bug ID and flip green automatically once
that bug closes.

## Adding a case

1. Add `tests/release/cases/NN-name.sh`. **`NN` must be unique across the whole directory** — take the next free number, do not reuse one. Two cases sharing a number both still run (the harness globs full filenames), but the number stops identifying a scenario, which is how seven collisions accumulated across parallel bug series.
2. Source `tests/release/lib/common.sh` at the top.
3. Register `trap case_cleanup EXIT` and define `case_cleanup`.
4. Last line of stdout MUST be `STATUS: PASS|FAIL|SKIP|KNOWN_ISSUE [<bug>]`.
5. Use `pass / fail / skip / known_issue <bug>` helpers from
   `lib/common.sh`.
6. If the case is cheap (≤ 2 min) and a critical path, append its
   stem to `QUICK_CASES` in `run.sh`.

Required per CLAUDE.md § B.5.1: every test that exercises
cluster-visible behaviour SHIPS a release-suite case in the same
commit as the unit/abuse test. KNOWN_ISSUE is fine when the
scenario depends on un-shipped functionality.

## Current case list

| # | Case | What it verifies | Quick |
|---|---|---|---|
| 00 | prereqs | ssh + mgr API + 6 nodes + FUSE mount | yes |
| 01 | basic-write-read | 4 MiB sha256 roundtrip | yes |
| 02 | replica-placement | mandatory replica count converges | yes |
| 03 | scrub-quick | manual quick scrub trigger | — |
| 04 | scrub-deep | manual deep scrub trigger | — |
| 05 | migration | operator migrate path + sha holds | yes |
| 06 | flush-cache | mandatory preserved + audit emitted | yes |
| 07 | set-schedules | endpoint shape + deprecated-scrub-warn | — |
| 08 | set-anchor | AnchorPolicy applies | — |
| 09 | set-mandatory | RF change | — |
| 10 | leader-rotation | new leader elected ≤30s | — |
| 11 | data-node-restart | reads fall over to surviving replica | — |
| 12 | fuse-restart | mount returns + reads resume | — |
| 13 | integrity-corruption | corrupt → fallback → quarantine flow | yes |
| 14 | orphan-reap | unlink + orphan_scrub reaps after grace | — |
| 15 | soak-15min | condensed mixed workload | yes |
| 16 | version-report | every node reports version + build_date | — |
| 17 | stats-correctness | logical ≤ net ≤ raw; tier sums | yes |
| 18 | api-shape | endpoint schema keys present | yes |
| 19 | config-effective | scope_kind on every row; scrub keys present | — |
| 20 | quarantine-flow | unquarantine endpoint validation | — |
| 83 | sparse-chunk-index-read | a gap in the chunk index space reads as zeros, never as a later chunk's bytes (REV-2-1) | yes |
| 84 | follower-loops-inert | non-leader managers dispatch no background-loop work and delete nothing (REV-1-1/1-2) | — |
| 85 | unaligned-write-integrity | non-chunk-aligned writes lose no bytes (REV-1-6) | yes |
| 86 | webui-escaping-and-config-write | CSP + both escapers present, hostile filename not rendered raw; a config write that would not parse is refused and leaves no temp (REV-4-1/4-3) | — |
| 87 | listchunks-paging-incremental | ListChunks pages incrementally: ascending ids, exclusive cursor, mtimes positionally aligned (REV-3-2) | yes |
| 88 | scrub-survives-bad-chunk | a torn `.meta` and an unreadable bucket are counted, not fatal — the disk pass still completes (REV-4-4) | — |
| 89 | write-slots-no-phantom-cache | put_chunk slots sized from the memory cap alone, with no `[cache]` budget reserved (REV-4-5) | yes |

(cases 21–82 and 90–95 cover later bug-fix and security regressions; see `tests/release/cases/`.)

## Chaos / cross-DC / balance run (3dc lab)

`--chaos` runs the hypervisor-level + cross-DC + auto-balance cases against
the **3dc** lab (one Raft voter per DC, two data nodes per DC, one client per
DC — `tests/proxmox/inventory-3dc.json`). It is a separate ~45–60 min gate
from the SSH-only full run.

```
# from-scratch: provision a clean 3dc cluster, run the chaos cases, tear down
ALLOW_PVE_CHAOS=1 PVE_TOKEN='root@pam!dtfs-test=…' \
  MGRS=mgr-01,mgr-02,mgr-03 \
  DATA=data-a1,data-a2,data-b1,data-b2,data-c1,data-c2 \
  CLIENT=client-a \
  bash tests/release/run.sh --chaos --provision
```

Prerequisites on the runner: `pwsh` (PowerShell 7 — drives the PVE REST API
via `tests/proxmox/pve.ps1`/`pve-chaos.ps1`) and `ansible` (deploy + netshape
playbooks). Without `ALLOW_PVE_CHAOS=1` + `PVE_TOKEN` the PVE-destructive
cases SKIP, so the suite still runs on a pre-provisioned cluster. Power/disk
chaos and teardown go through `tests/proxmox/pve-chaos.ps1` (REST-only, no
ssh, so it works under Linux `pwsh`).

### What's testable on 3dc vs total-outage-by-design

| Scenario | 3dc | plexmedia |
|---|---|---|
| Single DC power-off, quorum holds, reads continue | testable | only TORO/NRB (LELI = total outage) |
| DC power-on recovery converges | testable | only TORO/NRB |
| Intra-DC node/disk failure, same-DC replica survives | testable | not (1 data/DC) |
| Per-tier mandatory + per-DC cache balance | testable | testable |

The 3dc topology spreads one voter per DC, so losing any single DC leaves 2/3
quorum and recovery is observable. plexmedia puts all voters + the anchor in
LELI, so killing LELI is quorum + anchor loss (total outage by design).

### Chaos cases

| # | Case | What it verifies | PVE |
|---|---|---|---|
| 42 | crossdc-read-routing | a dc-b read is served by the dc-b replica (per-node served-bytes) | — |
| 43 | dc-poweroff-read-continuity | power a whole DC off; quorum holds; reads continue | yes |
| 44 | dc-poweron-recovery-converge | power DC back on; under-replication heals; anchor safe | yes |
| 45 | disk-failure-and-backfill | yank a disk → re-replicate; install blank → rejoins ring | yes |
| 46 | autobalance-convergence | declared mandatory+per-DC-cache target converges and stays | — |
| 47 | cache-perdc-promote-demote | per-DC cache promote on read, drop on flush, anchor safe | — |
| 48 | crossdc-read-under-shaping | cross-DC read completes under tc shaping; Raft stable | yes |
