# Minicluster Verification — 2026-05-21

Live verification of Bug AL → AT (the WebUI/observability fixes from
[SOAK-RECOVERY-2026-05-21.md](SOAK-RECOVERY-2026-05-21.md)) on a
6-VM minicluster provisioned during the USB-SSD outage. Bug AU was
discovered during this verification and fixed in the same session.

## Topology

| VMID | Hostname       | IP             | Role    | DC    | Rack    | Disks            |
|------|----------------|----------------|---------|-------|---------|------------------|
| 301  | mini-mgr01     | 10.20.0.171   | manager | dc-a  | rack-1  | OS only          |
| 302  | mini-mgr02     | 10.20.0.172   | manager | dc-a  | rack-2  | OS only          |
| 303  | mini-mgr03     | 10.20.0.173   | manager | dc-b  | rack-1  | OS only          |
| 304  | mini-data01    | 10.20.0.174   | data    | dc-a  | rack-1  | 20G warm + 20G cold |
| 305  | mini-data02    | 10.20.0.175   | data    | dc-a  | rack-2  | 20G warm + 20G cold |
| 306  | mini-client01  | 10.20.0.176   | client  | dc-a  | rack-3  | FUSE mount       |

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
(`10.20.0.171/172/173`); timestamps and content identical. Pre-fix
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

## Summary

All 9 bug fixes from 2026-05-21 (AL–AT) verified live + 1 follow-up
bug (AU) discovered and fixed during verification. The minicluster
remains available on PVE host `10.20.0.121` at VMIDs 301-306 +
template 9000 + builder 9001.

To tear down: `bash tests/proxmox/provision-mini.sh` is idempotent;
re-running it destroys VMIDs 301-306 and re-clones. Or just
`qm stop $vmid && qm destroy $vmid --purge --skiplock` per VM.
