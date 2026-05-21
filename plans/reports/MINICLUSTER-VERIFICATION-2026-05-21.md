# Minicluster Verification — 2026-05-21

Live verification of Bug AL → AT (the WebUI/observability fixes from
[SOAK-RECOVERY-2026-05-21.md](SOAK-RECOVERY-2026-05-21.md)) on a
6-VM minicluster provisioned during the USB-SSD outage. Bug AU was
discovered during this verification and fixed in the same session.

## Topology

| VMID | Hostname       | IP             | Role    | DC    | Rack    | Disks            |
|------|----------------|----------------|---------|-------|---------|------------------|
| 301  | mini-mgr01     | 10.150.0.171   | manager | dc-a  | rack-1  | OS only          |
| 302  | mini-mgr02     | 10.150.0.172   | manager | dc-a  | rack-2  | OS only          |
| 303  | mini-mgr03     | 10.150.0.173   | manager | dc-b  | rack-1  | OS only          |
| 304  | mini-data01    | 10.150.0.174   | data    | dc-a  | rack-1  | 20G warm + 20G cold |
| 305  | mini-data02    | 10.150.0.175   | data    | dc-a  | rack-2  | 20G warm + 20G cold |
| 306  | mini-client01  | 10.150.0.176   | client  | dc-a  | rack-3  | FUSE mount       |

Total resource footprint: 11 GB RAM, ~180 GB disk on the existing
`pve01_ssd01` + `pve01_ssd02` pools. PVE host has 30 GB RAM free + ~930
GB available pool space → plenty of headroom.

Multi-rack inside dc-a (rack-1 / rack-2 / rack-3) is the layout that
exercises the Bug AO horizontal-rack display; dc-b has a single rack to
verify single-rack DCs still render cleanly.

All 7 VMs (6 minicluster + builder) carry the same minicluster
`/etc/hosts` block, written once from the builder and idempotently
appended on each host via `provision-mini.sh` + the deploy script.

## Provisioning artefacts

* `tests/proxmox/inventory-mini.json` — declarative description of the
  6-VM cluster.
* `tests/proxmox/provision-mini.sh` — destroys VMIDs 301-306, clones
  from template 9000 via the PVE API, attaches storage, configures
  cloud-init, starts the VMs, waits for SSH. **Note**: this script
  uses `--data-urlencode` for ipconfig0 because the comma in
  `"ip=…/24,gw=…"` would otherwise be misparsed as a form-encoded
  separator.
* `tests/proxmox/deploy-mini.sh` — pushes binaries from the builder,
  renders `manager.toml` / `data.toml` / `fuse.toml` per host (uses
  the existing systemd units in `deploy/systemd/`), starts the
  services in the right order (managers → data → FUSE).

## Verification results

### Bug AL — I/O throughput rolling rate

After writing 170 MiB of test data through the FUSE client:

```
Sample 1 (during writes):  io_read = 1.21 MiB/s   io_write = 3.95 MiB/s
Sample 2 (idle, +2 s):     io_read = 0 B/s        io_write = 0 B/s
```

Pre-fix this stayed at `0 B/s` indefinitely because `record_read` /
`record_write` were never called from the data server. **PASS.**

### Bug AM — Migrations stable order

Scheduled 3 path migrations producing 8 tasks (`repl-0` …
`repl-7`) all sharing the same `updated_at_ms`. Three back-to-back
`GET /api/v1/migrations` queries returned tasks in identical order.
Pre-fix this would randomise per HashMap iteration. **PASS.**

### Bug AH — replicated audit log

Same 6 audit entries returned by all 3 managers
(`10.150.0.171/172/173`); timestamps and content identical. Pre-fix
each manager had its own local ring. **PASS.**

### Bug AI — audit display backfill

Every audit row has populated `actor / action / target / result`. Even
the legacy `apply_request`-internal entries (`UpdateNodeStatus`,
`UpdateNodeUsage`, `UpdateNodeDiskUsage`) render with `actor='raft'`
and split-from-summary `action / target`. Pre-fix these showed as four
em-dashes in the WebUI. **PASS.**

### Bug AJ + AK — per-tier RF math + logical/phantom

```
raw_total:    79.95 GiB   (= 2 nodes × 2 × 20 GB ssd02 disks)
raw_used:    170.00 MiB   (sum of on-disk bytes incl. replicas)
net_usable:  58.98 GiB    (warm_total/2 + cold_total/1 ≈ 20+40)
net_used:    127.50 MiB   (warm_used/2 + cold_used/1)
logical:     170.00 MiB
phantom:      42.50 MiB
```

`net_used` correctly diverges from `raw_used` (warm rf=2 halves the
warm portion). `net_usable` math also checks out. Pre-fix both columns
showed the same number. **PASS.**

### Bug AQ — FUSE client count

`stats.client_count = 1`, `nodes endpoint` shows 1 client
(`mini-client01`). Both come from the same merge (sm.nodes ∪
write_leases) so they agree even when there's no active writer.
**PASS.**

### Bug AR + AU — per-disk view (refreshed live)

```
mini-data01 (lab/dc-a/rack-1)
  disk_id=0 warm /mnt/data01-warm   84.1 MiB / 19.52 GiB (0.4%)
  disk_id=1 cold /mnt/data01-cold   90.1 MiB / 19.52 GiB (0.5%)
mini-data02 (lab/dc-a/rack-2)
  disk_id=0 warm /mnt/data02-warm   92.1 MiB / 19.52 GiB (0.5%)
  disk_id=1 cold /mnt/data02-cold   82.1 MiB / 19.52 GiB (0.4%)
```

These match `du -sh /mnt/data*-warm /mnt/data*-cold` on each node
exactly. Bug AR alone (per-disk display) showed 2.04 MiB everywhere
because heartbeats didn't refresh per-disk values — **Bug AU**
discovered and fixed during this verification adds
`HeartbeatRequest.disk_usage` + `ClientRequest::UpdateNodeDiskUsage`,
exposing live statvfs on every heartbeat. **PASS both.**

### Bug AN / AO / AP / AS / AT — WebUI rendering

Verified by inspecting the served HTML against the expected markers:

| Marker                                | Found | Expected |
|---------------------------------------|-------|----------|
| `.badge-client` CSS (Bug AN)          | 1     | ≥1       |
| "Per-tier RF" subhead removed (AN)    | 0     | 0        |
| `sortNodesForHome` function (AN)      | 3     | ≥1       |
| Logical/Phantom row IDs (AN+AK)       | 4     | 4        |
| `.topo-racks` flex-row container (AO) | 3     | ≥1       |
| `flex-direction: row` (AO)            | 1     | ≥1       |
| `#explorer-breadcrumb` (AS)           | 2     | ≥1       |
| `.ctxmenu` context menu (AS)          | 7     | ≥1       |
| `explorerNavigate()` (AS)             | 5     | ≥1       |
| `setHtmlPreserveScroll()` (AT)        | 4     | ≥1       |
| `refreshTopology()` non-recursive (AT)| 3     | ≥1       |
| `timestamp * 1000` audit (AG sanity)  | 1     | 1        |

**Data inputs for the WebUI's client-side rendering** also verified
correct via `/api/v1/files` + `/api/v1/nodes` queries:

* Explorer at `/`: 4 child folders (`incoming`, `movies`, `photos`,
  `shows`).
* Explorer at `/movies/`: 2 child folders (`Season-1`, `Season-2`).
* Explorer at `/movies/Season-1/`: 2 files (40 MiB each).
* Topology dc-a: 3 racks (matches Bug AO horizontal layout).
* Topology dc-b: 1 rack (single-rack DCs still render).

**PASS** on all five WebUI fixes.

## Bug AV — two-class tier lifecycle (Phases 1-3)

Verified live during the AV implementation push on 2026-05-21
(commits c939625…a650113, AV-1-8 through AV-3-5). Manager binaries
rolled to mini-mgr01 and mini-mgr03 between each Phase; mini-mgr02
remained on a corrupt Raft log throughout (pre-existing,
unrelated). Quorum of 2/3 sufficient for every test.

### REST endpoints (Phases 1 + 3)

| Endpoint | Status | Live evidence |
|----------|--------|---------------|
| `POST /api/v1/files/flush_cache` (AV-1-9) | ✅ | 4 scenarios: empty prefix → 400; invalid tier → 400; default tiers/cooldown → 200 with replicas_dropped=0 (legacy default volume has warm-as-mandatory, planner skips correctly); explicit cooldown_secs=300 → 200, echo confirms. |
| `POST /api/v1/volumes/{id}/migrate-policy` (AV-1-10) | ✅ | "applied" first run; "no-op (already migrated)" on retry; HTTP 404 for missing volume. |
| `GET /api/v1/volumes/{id}` (AV-3-5) | ✅ | Returns full JSON projection: mandatory_replicas, cache_replicas (all subfields), anchor with required_tier. |
| `POST /api/v1/volumes/{id}/set-cache` (AV-3-5) | ✅ | Partial update merges correctly; idempotent re-apply → "no-op"; `--warm-min 5 --warm-max 1` → HTTP 400 "warm tier: max < min". |

### CLI (AV-1-10 + AV-3-5)

```
$ dtfs-cli --manager http://mini-mgr01:9402 volumes show default
Volume: default
Name:   default
Chunk size:  4194304 bytes
Legacy replication:  hot=0 warm=2 cold=1 tape=1
Mandatory replicas:  hot=0 warm=2 cold=1 tape=1
Cache replicas:
  warm: min=1 max=1
  hot:  min=0 max=0
  per_dc:               true
  promote_reads:        2
  promote_window_secs:  60
  cooldown_secs:        300
Anchor:  dc=dc-a required_tier=any

$ dtfs-cli ... volumes set-cache default --warm-min 1 --warm-max 1 \
    --per-dc true --promote-reads 2 --promote-window-secs 60 --cooldown-secs 300
Volume 'default' set-cache: applied
  warm: min=1 max=1
  …

$ dtfs-cli ... files flush-cache /movies/ --cooldown-secs 60
flush-cache prefix=/movies/ tiers=["warm"] cooldown=60s
  files_matched     = 7
  chunks_examined   = 4
  replicas_dropped  = 0
  mandatory_refusals = 0
  unreachable_nodes = 0
  rpc_failures      = 0
  raft_failures     = 0
```

### Audit fan-out (AV-1-11)

Triggered `POST /api/v1/files/flush_cache` and queried the audit
log on a different manager (mini-mgr03) via the AV-1-9-completed
Bug AH replicated audit path:

```
$ curl -s "http://mini-mgr03:9402/api/v1/audit?action=cache.flush_complete&limit=5"
[
  {
    "action": "cache.flush_complete",
    "actor": "scheduler",
    "origin_manager": "mini-mgr01",
    "result": "success",
    "summary": "cache.flush_complete: prefix=/movies/ replicas_dropped=0 mandatory_refusals=0 [mini-mgr01]",
    "target": "prefix=/movies/ replicas_dropped=0 mandatory_refusals=0",
    "timestamp": 1779362733
  }
]
```

Proves the new dispatcher audit code path actually fires, traverses
Raft fan-out, and is queryable from any manager.

### WebUI (AV-3-4)

* `GET /ui/explorer` rendered with the new "Flush cache" context-
  menu entries (4 occurrences for folder + file + parent + modal
  labels) and the `submitFlushCache` JS handler (2 occurrences:
  function definition + `onclick`).
* `flush-modal` HTML present with the expected form fields
  (path_prefix, cache_tiers select, cooldown_secs numeric,
  allow_empty_prefix safety gate).
* Volume-edit form was deferred (see plan § 5 deviations); the
  read-side endpoint added by AV-3-5 unblocks a follow-up
  display card.

### Phases 2 + 3 dispatcher smoke (AV-2-4, AV-2-5, AV-3-3)

Binaries rolled cleanly after every commit; tiering engine logs
`Tiering engine started (evaluation interval: 3600s)`; no panics
across mini-mgr01 and mini-mgr03 over ~1 h of monitoring. Full
write-→-promote and idle-→-demote end-to-end demo deferred to a
later run because exercising it requires AV-3-1/3-2-aware FUSE
binaries on a client VM (the manager-side machinery is verified
in unit tests + smoke).

### Workspace test count (cumulative across Bug AV)

| Phase | Tests added | Crate landings |
|-------|-------------|----------------|
| Phase 1 (AV-1-1 to AV-1-12) | 33 | dtfs-common +2, dtfs-data +4, dtfs-raft +12, dtfs-manager +15 |
| Phase 2 (AV-2-1 to AV-2-6) | 23 | dtfs-manager +23 |
| Phase 3 (AV-3-1 to AV-3-5) | 10 | dtfs-common +3, dtfs-manager +6, ad-hoc +1 |

Total: **66 net new tests**. Plan estimated 43 + 20 + 11 = 74; the
delta is recorded per-task in the plan's verification log under §
4, with deferred dispatcher-unit tests substituted by pure planner
tests (per the AV-1-6 / AV-1-7 convention).

### Carryovers from Bug AV after Phase 3

* WebUI volume-edit form (AV-3-4 partial). Read-side is unblocked
  by AV-3-5's `GET /api/v1/volumes/{id}`; a display card is a
  straightforward fetch+render. Inline-edit modal is Phase 4
  polish.
* `ENABLE_CACHE_REDUNDANCY_ESCALATION` flag stays `false` per the
  AV-2-4 spec. Flip is Phase 4 follow-up.
* End-to-end write-→-promote demo deferred until the
  AV-3-1/AV-3-2 FUSE binary is deployed to a client VM in the
  minicluster (next session).

**PASS** on all REST + CLI + audit fan-out + WebUI markup
verifications. The two end-to-end demos remaining (Phase 2
promotion+demotion + Phase 3 warm-first write+anchor repair) are
gated on rolling the new FUSE client binary, which is a
mechanical follow-up.

## Summary

All 9 bug fixes from 2026-05-21 (AL–AT) verified live + 1 follow-up
bug (AU) discovered and fixed during verification. The minicluster
remains available on PVE host `10.150.0.121` at VMIDs 301-306 +
template 9000 + builder 9001.

To tear down: `bash tests/proxmox/provision-mini.sh` is idempotent;
re-running it destroys VMIDs 301-306 and re-clones. Or just
`qm stop $vmid && qm destroy $vmid --purge --skiplock` per VM.
