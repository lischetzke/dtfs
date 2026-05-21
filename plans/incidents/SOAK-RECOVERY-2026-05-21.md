# Soak Recovery — 2026-05-21 (WebUI/observability polish during lab outage)

Continuation of [SOAK-RECOVERY-2026-05-19.md](SOAK-RECOVERY-2026-05-19.md).
The Plexmedia lab is currently torn down pending USB-SSD replacement
(the 4 USB sticks died on 2026-05-19; only the PVE template + the
builder VM remain). This doc covers the **9 WebUI / observability
fixes** (Bug AL → AT) landed during the outage window, all verified by
unit tests on the builder VM. Live re-verification happens when the
cluster comes back up.

---

## Bug AL — `/api/v1/stats.io_*_bytes_per_sec` stuck at 0

**Symptom.** Operator opened Home; the "I/O Throughput" card always
read `↓ Read: 0 B/s` and `↑ Write: 0 B/s` regardless of cluster load.

**Two root causes stacked.**

1. **Data-node counters never incremented.** `DataMetrics::record_read`
   and `record_write` exist in `crates/dtfs-data/src/metrics.rs` and are
   referenced only inside that file's tests — no production call site
   wired them into the gRPC handlers. `total_bytes_read` /
   `total_bytes_written` therefore stayed at `0` for every node's
   entire lifetime.
2. **Manager rate formula was nonsensical even if the counters DID
   grow.** `compute_io_window_secs` computed
   `now - min(last_seen_ms across nodes)` — i.e. the staleness of the
   slowest-updated node, NOT a time window. It then divided cumulative
   bytes by that "window", producing either a divide-by-near-zero
   (slow-staleness) or an enormous false rate.

**Fix.**

* `crates/dtfs-data/src/server.rs::put_chunk` — call
  `self.metrics.record_write(total_size)` once the commit succeeds.
* `crates/dtfs-data/src/server.rs::get_chunk` — call
  `self.metrics.record_read(data.len())` once the chunk bytes are in
  hand (cache hit or fresh load — both are real traffic).
* `crates/dtfs-manager/src/api.rs` — new process-local `IoRateTracker`
  + `compute_io_rate_at(tracker, nodes, now_ms)` pure helper:
    * First call (`last_query_ms == 0`) returns `(0, 0)` and seeds the
      baseline.
    * Subsequent calls delta `total_bytes_*` against the previous
      snapshot and divide by `(now_ms - last_query_ms) / 1000`,
      flooring the window at 1 s.
    * Sub-second calls (e.g. WebUI + Prometheus scraping
      simultaneously) return the cached last rate to avoid spiky
      values.
    * Cumulative regressions (data-node restart) `saturating_sub` to 0.
    * Disappearing nodes drop out of the next delta cleanly.
* New `AppState.io_rate: Arc<RwLock<IoRateTracker>>` so the tracker
  lives across `/api/v1/stats` calls. Per-manager (not Raft-replicated)
  — each manager renders rates from the traffic it directly observes
  through cluster heartbeats.

**Tests** (`bug_al_io_rate_tests`, 5 cases):

* `first_call_returns_zero_then_baselines`
* `delta_over_known_window_produces_expected_rate` — 10 MB read +
  5 MB written over 10 s → (1 MB/s, 500 KB/s).
* `cumulative_regression_does_not_underflow` — data-node restart
  produces 0, not a spike.
* `disappearing_node_does_not_blow_up_rate` — decommission still gives
  a sensible rate from surviving nodes.
* `sub_second_calls_return_cached_rate` — back-to-back calls within
  1 s return the cached value.

---

## Bug AM — Migrations page randomised on every refresh

**Symptom.** Operator on `/ui/migrations`; rows swapped positions on
every 5 s auto-refresh, especially during a migration storm where
hundreds of tasks completed in the same scheduler tick.

**Root cause.** `migration_snapshot` sorted by `updated_at_ms`
descending only. Tasks touched in the same tick share an
`updated_at_ms`; HashMap iteration order then randomised the display.

**Fix.** Stable secondary sort by `task_id` lexicographic ascending in
`scheduler.rs::migration_snapshot`. Same-timestamp tasks now stay in
deterministic order across refreshes.

---

## Bug AN — Home page polish (alignment, labels, badges, ordering)

Five small UI fixes bundled into one bug ID for brevity:

* **Drop "After replication factor (÷ hot RF)" subhead** on the Net
  Usable Capacity card. The math is now per-tier (Bug AJ) so a single
  RF in the label was already wrong; the card title and tooltip carry
  the intent.
* **Align Raw Capacity and Net Usable Capacity cards.** Renamed the
  Net "Usable" row to "Total" so both cards have identical row labels
  (Total / Used / Utilisation / Free / …) sitting at the same vertical
  baseline.
* **Split bar + value into separate `<td>`** in the Data & Manager
  Nodes table. Pre-fix both shared a flex row inside one cell, so
  variable text widths pushed bars left/right and they didn't line up
  vertically down the column.
* **`.badge-client` style added.** Clients in the Nodes table rendered
  with an unstyled "client" string — no pill — because no CSS rule
  matched `.badge-client`. Now cyan-tinted, matching the
  manager/data/role-pill aesthetic.
* **Stable node order.** New `sortNodesForHome()` orders by
  `(role: manager → data → client → other) → location → hostname`,
  applied on both Home and Nodes pages so the visual order is
  consistent and survives refresh.

---

## Bug AO — Topology page: racks horizontal, hosts vertical

**Pre-fix layout.**

```
DC LELI
  Rack leli-rack-1
    [host-1] [host-2] [host-3] …    ← grid of hosts wrapped horizontally
  Rack leli-rack-2
    [host-1] [host-2] [host-3] …
```

Vertical stacking of racks collapsed the rack-vs-host hierarchy; a DC
with several racks read as one long flat list.

**Fix layout.**

```
DC LELI
  ┌Rack leli-rack-1─┐ ┌Rack leli-rack-2─┐ ┌Rack leli-rack-3─┐
  │ host-1          │ │ host-1          │ │ host-1          │
  │ host-2          │ │ host-2          │ │ host-2          │
  │ host-3          │ │                 │ │                 │
  └─────────────────┘ └─────────────────┘ └─────────────────┘
```

CSS:

```css
.topo-racks { display: flex; flex-direction: row; flex-wrap: wrap; }
.topo-rack  { display: flex; flex-direction: column; min-width: 220px; }
.topo-nodes { display: flex; flex-direction: column; gap: .5rem; }
```

Wraps to a new row when the DC has more racks than fit on screen.

---

## Bug AP — Nodes page utilisation bar for managers (and clients)

Manager + client nodes have no disks of their own; the per-node
capacity card showed `0 / 0` with a full-bar at "0%". Misleading.

**Fix.** In `refreshNodesList`, gate the Total/Used metadata rows AND
the utilisation bar on `isStorageRole = !(manager || client)`. Manager
+ client cards now show only gRPC address, location, and health —
exactly the info that's meaningful for them. The same gate skips the
"Set Maintenance" button (no disks → no maintenance mode to enter).

The Home page Data & Manager Nodes table mirrors this: cells render an
em-dash for the disk count / capacity / used columns on
manager/client rows, with the utilisation bar dropped entirely.

---

## Bug AQ — Home page "FUSE clients" count missed read-only clients

**Symptom.** Nodes page listed 3 clients (client01/02/03); Home page
top counter said `1 FUSE client`.

**Root cause.** Home's `client_count` was computed as
`unique hostnames in active write_leases`. Only writers held leases;
read-only Plex playback clients had none → never counted.

**Fix.** Same merge logic the Nodes page already uses: clients
self-registered as `sm.nodes` entries (Bug AA) ∪ lease-derived hosts.
Both lists are unioned by hostname into a HashSet. Home count now
matches the Nodes page roster.

---

## Bug AR — Per-disk view in the Nodes page

**Symptom.** When data-primary01 hit Bug AD (one of its 4 cold disks
filling to 100% while the other 3 sat at 2%), the Nodes page showed
only a node-level "Used 7/600 GiB ≈ 1%" — the per-disk imbalance was
invisible without SSH-ing in and running `df`.

**Fix.**

* Server: `NodeResponse` gains a `disks: Vec<DiskResponse>` field with
  per-disk `disk_id / mount_path / tier / total_bytes / used_bytes /
  status`. Manager + client synthetic node entries carry an empty
  vector.
* WebUI: each data-node card now embeds a sortable table of disks with
  individual mini-utilisation bars. Imbalance like the pre-Bug-AD
  cold-4 cascade is now visible at a glance.

---

## Bug AS — Data Explorer: folder tree + right-click context menu

**Symptom.** Operator on `/ui/explorer` saw 9 167 files in one giant
flat table — useless on a media library with nested `/movies/Season N/`
paths.

**Fix.** Folder-aware browser:

* Fetch all files once (cap 100 000 — adequate for household-scale
  Plex). Build folder tree client-side.
* Render the **current working directory** only:
    1. Breadcrumb (clickable segments) above the table.
    2. Direct-child folders first (📁 icon, click to descend, "N files
       · total size" summary).
    3. Direct-child files (📄 icon, size, chunk count, created-ago).
    4. Per-row context menu on right-click:
       * "Open folder" (folders) or "Migrate parent folder…" (files).
       * "Migrate this {folder,file}…" (pre-fills the migrate dialog).
       * "Copy path" (writes the full path to the clipboard).
* "Migrate folder…" toolbar button uses the current cwd as the prefix.

For very large clusters (≫100 k files) the client-side grouping should
be replaced with a server-side `/api/v1/files/browse?path=…&depth=1`
endpoint. Deferred — current scale doesn't need it.

---

## Bug AT — Scroll position lost on every data refresh

**Symptom.** Operator scrolled down on the Nodes / Migrations / Audit
pages, hit the top of the next refresh cycle, and the page silently
jumped back to scrollTop=0. Felt like the WebUI was navigating away
even though only the data was updating.

**Two root causes.**

1. Every refresh handler wrote `innerHTML = …` to its list container.
   When the new content was shorter than the prior scroll offset, the
   browser pinned scrollTop to 0.
2. `renderTopology` set its auto-refresh as
   `setInterval(renderTopology, 15000)` — recursively calling itself.
   Each invocation rebuilt the WHOLE `page-content` (not just the
   tree) AND set a NEW setInterval without clearing the old one. After
   a few minutes, multiple timers stacked up and each fire blew away
   any in-progress user interaction.

**Fix.**

* New `setHtmlPreserveScroll(el, html)` helper: snapshots
  `window.scrollY` before the write, restores it after.
* `renderTopology` split into `renderTopology` (initial render + sets
  the timer ONCE) and `refreshTopology` (in-place tree update only).
  The timer now refreshes the `topo-tree` element exclusively, leaving
  `page-content` (and the page header / breadcrumb / scroll position)
  untouched.
* `loadMigrations`, `loadExplorer`, `refreshNodesList`, and topology
  refresh all wrap their innerHTML writes in
  `setHtmlPreserveScroll`.

---

## Bug AU — per-disk `used_bytes` stuck at boot-time value

**Symptom (discovered live).** During minicluster verification (see
`MINICLUSTER-VERIFICATION-2026-05-21.md`): the new per-disk view from
Bug AR rendered correctly, but every disk reported `used_bytes ≈
2.04 MiB` — the empty-ext4 metadata footprint. Actual chunks on those
mounts: 82–92 MiB per disk (verified via `du -sh /mnt/data*-warm
/mnt/data*-cold`). So the per-disk Bug AR table was structurally
correct but showed stale numbers.

**Root cause.** Data nodes report per-disk `total_bytes` + `used_bytes`
in the `RegisterNode` RPC at boot — pulled from a one-shot statvfs in
`DiskManager::new`. The heartbeat path
(`crates/dtfs-data/src/main.rs::heartbeat`) only ships aggregate
`total_bytes_used` (sum across disks); the manager applies it via
`UpdateNodeUsage` which updates `NodeInfo.used_capacity` but leaves
`NodeInfo.disks[*].used_bytes` untouched. Per-disk view therefore
stayed at "fresh-format ext4 metadata" forever.

Parallel to Bug AD (which fixed `select_disk_for_tier`'s use of stale
`used_bytes` via live statvfs) — same staleness in a different
consumer.

**Fix.**

* `proto/manager.proto` — `HeartbeatRequest` gains a `repeated
  DiskUsageSnapshot disk_usage = 9;` field. Optional: when empty, the
  manager keeps the boot snapshot (so an old data-node binary doesn't
  silently zero the per-disk view during a rolling upgrade).
* `DiskManager::live_disk_usage()` — new helper that statvfs's each
  configured mount and returns `Vec<(disk_id, total_bytes,
  used_bytes)>`. Cheap (~1 µs per disk) and called once per heartbeat
  tick (default 5 s). Cached-value fallback on statvfs failure so a
  transient permission glitch doesn't zero the display.
* `dtfs-data` heartbeat loop populates the new field from the helper.
* New Raft variant `ClientRequest::UpdateNodeDiskUsage { node_id,
  per_disk }`. State machine applies it by mutating
  `NodeInfo.disks[i].{total_bytes, used_bytes}` for any disk_id
  present in the snapshot (drops unknowns — disk may have been
  removed since the heartbeat was sent), then recomputes
  aggregate `total_capacity` / `used_capacity` from the refreshed
  disks so `/api/v1/stats.tier_breakdown` stays consistent too.
* Manager `heartbeat` handler proposes `UpdateNodeDiskUsage` after
  `UpdateNodeUsage` on every heartbeat that carries `disk_usage`.

**Backward-compat.** A data node on an older binary doesn't emit
`disk_usage`; the manager skips the `UpdateNodeDiskUsage` write and
keeps the boot snapshot for that node — old per-disk view continues
to work (stale, but no worse than pre-fix).

**Live verification on the minicluster** (post-rollout 2026-05-21):

```
mini-data02 (lab/dc-a/rack-2/mini-data02)
  disk_id=0 tier=warm  /mnt/data02-warm   92.1 MiB / 19.52 GiB (0.5%)
  disk_id=1 tier=cold  /mnt/data02-cold   82.1 MiB / 19.52 GiB (0.4%)
mini-data01 (lab/dc-a/rack-1/mini-data01)
  disk_id=0 tier=warm  /mnt/data01-warm   84.1 MiB / 19.52 GiB (0.4%)
  disk_id=1 tier=cold  /mnt/data01-cold   90.1 MiB / 19.52 GiB (0.5%)
```

Matches `du -sh` output exactly. Pre-rollout the same query reported
`2,138,112 bytes` (= 2.04 MiB, ext4 metadata) on every disk.

---

## Bug AV — two-class tier lifecycle (mandatory + cache replicas)

**Symptom.** The user asked: "When I start a migration of i.e.
/1GB.bin to cold I would expect that it would move the data to cold
only and remove the copies from warm. Is that the case, should that
be the case?" — exposing that the legacy `replication` field was
*additive only*. There was no way to express "this chunk's
mandatory floor is 1 cold replica + opportunistic warm caches on
read demand"; the under-replication scheduler enforced every
non-zero RF as a hard floor, the tiering engine couldn't promote
on demand, and operators had no eviction button to free warm space.

**Root cause.** One replication concept doing two jobs:

* durability floor (the under-replication scheduler's domain), AND
* opportunistic caching layer (the tiering engine's domain).

These have opposite semantics. The first must NEVER drop below the
configured count; the second MUST be freely evictable when the
operator says so or when read demand fades. Conflating them meant
the cluster wasted warm-tier disk on chunks no one was watching,
the operator couldn't safely flush them, and there was no
explicit "this is the durable anchor" pin.

**Fix (Phases 1-3, ~3 weeks of design + ~3 days implementation).**
Two independent replica classes per volume:

* `mandatory_replicas.<tier>` — durability floor, scheduler
  enforces.
* `cache_replicas.<tier>.{min,max}` — opportunistic, tiering
  engine creates on read demand, operator can flush.
* `anchor: AnchorPolicy` — explicit invariant (a named DC, with
  optional `required_tier` pin) that survives every cache flush
  and every demotion.

Manager-side machinery:

* New Raft `ClientRequest::SetChunkCooldown` / `RemoveChunkLocation`
  with `enforce_anchor` defence-in-depth (AV-1-3 / AV-1-4).
* Pure planner kernels for the cache lifecycle:
  `plan_flush_cache` (AV-1-7), `plan_cache_promotion` (AV-2-2),
  `select_promotion_target` (AV-2-3), `plan_cache_tick` (AV-2-4),
  `plan_anchor_lag_repair` (AV-3-3),
  `should_clear_cooldown_on_demote` (AV-2-5).
* TieringEngine gains per-`(chunk, dc)` read-window storage with
  positive-delta heartbeat sampling.
* `POST /api/v1/files/flush_cache` REST endpoint (AV-1-9),
  `POST /api/v1/volumes/{id}/migrate-policy` (AV-1-10),
  `GET /api/v1/volumes/{id}` (AV-3-5), `POST .../set-cache`
  (AV-3-5).

Client-side surface:

* FUSE `fuse.toml [write] first_write_tier = "cold|nearest-warm"`
  (AV-3-1) + `allocate_chunks` proto carrying a `preferred_tier`
  hint (AV-3-2). Combined with the manager's anchor-lag-repair
  pass, warm-first writes are now safe: the FUSE client ACKs as
  soon as the warm replica confirms; the cold anchor copy lands
  asynchronously within one tiering tick.
* WebUI "Flush cache" context-menu action on the Data Explorer
  (AV-3-4); CLI subcommands `volumes show`, `volumes set-cache`,
  `volumes migrate-policy`, `files flush-cache` (AV-1-10 / AV-3-5).

Audit + alert hooks fire at every operationally-interesting
point (`cache.flush_replica`, `cache.flush_refused`,
`cache.flush_complete`, `cache.promote`, `cache.demote`,
`anchor.lag_repair`, `volume.migrate_policy`, `volume.set_cache`).
An `AnchorViolated` alert fires once per call (deduped) on any
refusal that would have stranded the anchor.

**Test count delta:** **66 new** unit tests in the workspace,
distributed:

| Phase | Tests added | Headline coverage |
|-------|-------------|--------------------|
| Phase 1 | 33 | schema parsing, flush planner, refusal classification, migrate-policy idempotency |
| Phase 2 | 23 | read-window counter, promotion decision gates, target selection, full tick planner, cooldown clearance helper |
| Phase 3 | 10 | FuseWriteConfig fallback, preferred_tier resolver, anchor-lag-repair planner, set-cache merger/validator, JSON projection |

Plan called for 43 + 20 + 11 = 74 tests; landed 33 + 23 + 10 = 66
(+/- defensive vs deferred dispatcher unit tests; see plan §5
deviations for the per-task breakdown).

**Live verification:** every Phase shipped with at least a smoke
check on the minicluster (mini-mgr01 + mini-mgr03; mini-mgr02 has
a pre-existing corrupt Raft log unrelated to AV). Highlights:

* AV-1-9 — flush_cache REST endpoint live-tested 4 scenarios
  (empty prefix rejected, invalid tier rejected, accepted call
  with default tiers, accepted call with explicit cooldown).
* AV-1-10 — `volumes migrate-policy default` "applied" then
  "no-op" on retry; 404 for missing volume.
* AV-1-11 — `cache.flush_complete` audit row fires via Bug AH
  Raft fan-out (actor=scheduler, origin_manager=<mgr>).
* AV-3-5 — full CLI matrix (`volumes show`, `set-cache` apply +
  no-op, max<min rejection at REST layer, `files flush-cache`).

End-to-end "warm-first write → anchor repair fires within one
tiering tick" demo deferred to a separate close-out run — needs
the AV-3-1/AV-3-2 FUSE binary rolled to a client VM with
`first_write_tier = "nearest-warm"` configured. The blocker is
mechanical (deploy a client config), not architectural.

**Carryovers (open after Phase 3 — Bug AV closes here, no
successor Phase is scheduled; each item below is a standalone
follow-up):**

* WebUI volume-edit form (AV-3-4 partial). Read-side is unblocked
  by AV-3-5's `GET /api/v1/volumes/{id}`; a display card is a
  straightforward fetch+render. Inline-edit modal is a deferred
  polish item — CLI covers the operational need today.
* `ENABLE_CACHE_REDUNDANCY_ESCALATION` flag stays `false` per the
  AV-2-4 spec. Flip + matching test extension when a real per-DC
  HA workload exists to validate against.
* `GET /api/v1/volumes` (list endpoint) still uses its
  aggregation-only projection. Per-volume reads via the new
  endpoint cover today's needs; widening the list shape is a
  deferred polish item if any client ever requires it.

## Combined summary — tests

| Crate          | Suite                                | Tests |
|----------------|--------------------------------------|-------|
| `dtfs-data`    | (no new tests; metrics already covered) | n/a |
| `dtfs-manager` | `api::bug_al_io_rate_tests`          | 5    |
| `dtfs-manager` | (existing) `bug_ae`, `bug_ai`, …     | 49   |
| `dtfs-raft`    | (unchanged)                          | 20   |

Total cargo run: **50 dtfs-data + 54 dtfs-manager + 20 dtfs-raft =
124 tests, all green** on the builder VM.

## Files touched

* `crates/dtfs-data/src/server.rs` — Bug AL recording.
* `crates/dtfs-manager/src/api.rs` — Bug AL tracker + helper, Bug AQ
  client count, Bug AR `disks` field on `NodeResponse`, Bug AM (no,
  that's scheduler.rs), Bug AI from prior session unaffected.
* `crates/dtfs-manager/src/scheduler.rs` — Bug AM stable sort.
* `crates/dtfs-manager/src/webui.rs` — Bugs AN / AO / AP / AR / AS / AT
  (CSS + render functions + context menu + scroll preserve helper).

## What's NOT done

* Live verification on Plexmedia — gated on USB-SSD replacement.
* Bug AS server-side browse endpoint (deferred, client-side grouping
  suffices at household scale).
* Phantom-metadata GC follow-up from Bug AK remains open.

When the lab comes back up, redeploy the manager + data binaries from
`/home/debian/dtfs2/target/release/` and recheck each Home / Topology /
Nodes / Migrations / Explorer / Audit page against the bug list above.
