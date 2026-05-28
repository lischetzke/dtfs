# Bug BA — Soak test (2026-05-26)

**Status legend:** ⬜ todo · 🟦 in progress · ✅ done · 🟥 blocked · 🟨 needs review

Soak verification of the cluster after AV / AZ / AY / AX work. Two
runs: 1h (smoke) then 4h (extended). Each run cycles
write/read/promote/demote/flush against the live minicluster and
checks for regressions of issues that have hit production paths
during prior phases.

## 0. Scope

### 0.1 Regression-watch list (issues to specifically look for)

| Watch | Origin | Symptom to detect |
|-------|--------|------------------|
| AV-5-10 | warm-replica auto-delete | warm chunk landed then disappeared within 60s |
| AV-5-9 | replication tier mis-target | warm chunks ending up on cold disk |
| AV-5-7 | heartbeat read-count inflation | per-chunk read count >> number of actual reads |
| AV-4-8 | Bug Z overwriting operator config | default volume mandatory/cache values reverting after mgr restart |
| AV-5-2 / AV-5-11 | Raft log read EOF | mgr fails to start with "when Read Logs: unexpected EOF" |
| AV-4-1d / AV-5-9 | FUSE hardcoded tier | new writes landing on wrong tier despite mandatory |
| AZ-1 | scrub override not propagating | data nodes ignore per-volume scrub_interval_secs |
| AZ-3 | chunk-size resize breaking reads | sha256 mismatch after volume chunk_size_bytes change |
| AZ-5 | invalid config corrupts on-disk | manager.toml gets bad JSON written despite boundary check |
| All | placement integrity | mandatory count != volume policy; anchor invariant broken |
| All | API drift | /api/v1/{volumes,nodes,files,audit,config/effective} returning malformed JSON |

### 0.2 Workload

Mixed:
- W1: 100 fresh 4 MiB writes to /soak/W1/<n>.bin over the soak (random distribution).
- W2: 50 reads per write (Plex-style replay).
- W3: 10 deletes (operator unlinks).
- W4: 5 flush_cache calls (operator cache-reclaim).
- W5: Per 15 minutes: set-schedules toggle scrub_interval_secs between 120 and 0 (clear).
- W6: Per 30 minutes: mgr leader restart (rolling).

### 0.3 Per-check probes

Every 5 minutes:
- `/api/v1/cluster/status` 200.
- `/api/v1/nodes` returns all 6 nodes, all `healthy`.
- `/api/v1/volumes` returns default + soak vol with full AV projection.
- `/api/v1/config/effective` returns >= 40 entries.
- Audit ring buffer: `cache.promote` and `cache.demote` counts non-zero;
  no `noop_already_absent` followed by chunk-on-disk gone.
- `tests/proxmox/abuse/16-bug-az-chunk-size-shrink.sh`-style sha256
  roundtrip on a random soak file.
- For each cache.promote audit row, verify the chunk EXISTS on the
  target node's warm-tier disk (per AV-5-10).
- For each volume, count chunks per tier; sum matches
  `mandatory_replicas.*` + `cache_replicas.*` budget per file.

---

## 1. Task list

### ⬜ BA-1 — 1h soak

* **Files:** `tests/proxmox/abuse/17-bug-ba-soak-1h.sh` (new).
* **Implementation:** driver that runs the workload above for
  3600 s + a checker that polls every 5 min. Writes to
  `plans/reports/SOAK-2026-05-26-1h.md` as it goes.
* **Done when:** report has 12 check rows + bug-list section.

### 🟥 BA-2 — 4h soak (CANCELLED 2026-05-26)

Operator cancelled. 1h soak surfaced enough material (BA-1 orphans,
BA-2 disk-usage apportionment lie, scrub gaps) to drive Bugs BC,
BB, BD. Re-spawn a longer soak after BC + BB land if regression
risk warrants it.

**2026-05-27 on-demand re-run** (Bug BF-14): the soak driver is
parameterised by duration. To run a fresh 4 h soak against the
current binaries:

```
ssh -i ~/.ssh/dtfs-lab/id_ed25519 debian@10.20.0.150 \
    'cd ~/dtfs2 && bash tests/proxmox/abuse/17-bug-ba-soak.sh 14400 \
       plans/reports/SOAK-$(date -u +%Y%m%dT%H%M%SZ)-4h.md'
```

Report lands at `plans/reports/SOAK-<run-id>-4h.md` on the builder.
48 probe rounds expected (5 min cadence × 4 h). Append any new
findings to § 5 of this plan with a fresh `BA-N` id.

### ⬜ BA-3 — Issue catalog

* **Files:** **this file's § 5**.
* **Implementation:** append every regression / new bug surfaced
  during BA-1 or BA-2. Each entry: ID `BA-N`, summary, evidence
  (audit row / log line / find output), severity (P0..P3).
* **Done when:** all entries closed (fixed or explicitly tracked
  as a follow-up plan).

---

## 4. Verification log

> Append per-task. Empty at plan authorship.

---

## 5. Issue catalog (Bug BA)

> Append entries as they surface. Empty at plan authorship.

| ID | Severity | Summary | First seen | Evidence | Status |
|----|----------|---------|------------|----------|--------|
| BA-1 | P1 | Orphan chunks never reclaimed: scrub-driven GC promised in AGENTS.md § A.6 item 8 doesn't actually run. 24+ GB of 128MB-cohort chunks from prior test campaigns linger after FileMetadata unlink. | 2026-05-26 1h soak round 3 | data01+data02: 406 `.dat` files on disk vs 115 chunks in mgr `chunk_locations`; 191 × 128MB chunks have no live FileMetadata referent. `scrub.rs` only verifies on-disk vs local `meta` (bitrot), never cross-checks against manager-side chunk_locations. | folded into Bug BC (orphan_scrub) |
| BA-2 | P2 | `get_stats` per-tier `used_bytes` apportionment fabricates symmetry. Splits `n.used_capacity` by `disk.total_bytes / node_total_bytes` regardless of which tier holds the bytes. Equal-size warm+cold disks → tier breakdown forced to 50/50. RF divisor then compounds the lie in `net_used`. | 2026-05-26 stat audit | mgr `/api/v1/stats`: warm `used_bytes`=13303025664, cold `used_bytes`=13303025664 (identical to byte). `du -sh /mnt/*-warm /mnt/*-cold` on both data nodes: warm 12.6 GB, cold 12.3 GB. `api.rs:2090-2100` apportionment math. | forks to Bug BB |
| BA-3 | P2 | AZ-1 scrub-override propagation race. Round 3 probe: `set-schedules scrub_interval_secs=120` → 90 s wait → data01 journal had NO `AZ-1 scrub override` line. Same probe in rounds 6 + 9 passed. | 2026-05-26 1h soak round 3 | report `SOAK-2026-05-26-1h.md` round 3, contrast with rounds 6 + 9 OK lines. | superseded by Bug BC-13 — per-volume scrub_interval is deprecated; cluster-wide [scrub] block is the new path. The deprecation warn log fires on EVERY heartbeat now (not gated by value change) so future probes don't race. |
| BA-4 | P3 | AV-5-10 false-positive probe race. Round 4 probe: cache.promote audit row reported chunk 9bbe…59b4 on data02; probe slept 30 s then checked disk — not found. All later rounds (5–11) verified chunk persists. | 2026-05-26 1h soak round 4 | report round 4. | not-a-code-bug — probe needs to confirm the chunk wasn't operator-demoted between schedule + check. Lower priority than BC orphan_scrub which already addresses the underlying tracker drift. Probe-only follow-up. |

### 5.1 Fix sketches

**BA-1** — Add `scheduler::reap_orphan_chunks` tick (slow, e.g. every 6h). Each data node periodically reports its full chunk_id set in a heartbeat extension or a dedicated `ListChunks` RPC. Manager diffs against `state_machine.chunk_locations`. Anything on disk that no FileMetadata + no in-flight migration references → mark for delete after grace period (24h default), then issue `DeleteChunk`. Guard with `enforce_anchor` (must not drop a mandatory copy if a stale-metadata race is possible). Audit row `gc.orphan_reaped` per delete. Operator toggle `gc.orphan_reap_enabled` (default off until soak-validated).

**BA-2** — Two fixes:
1. Heartbeat extension: data node reports `used_bytes` PER DISK (not just per node). `proto/data.proto::HeartbeatRequest::disks` already has `total_bytes` per `DiskInfo`; add `used_bytes`. Manager stops apportioning and uses real values.
2. Until heartbeat schema migrates, `api.rs` apportionment can be replaced with `chunk_locations`-derived sums (count chunks per tier × estimated avg chunk size); less precise but no false symmetry.

Prefer (1); (2) is interim if rollout is staged.

### 5.2 Bug BA scope decision

These two issues are reporting / housekeeping concerns. Neither blocks the AV/AZ correctness work. Plan BA-3 below captures them; **separate work plans** carry the design + fix:

- **BA-1 (orphan chunks)** folded into [`PLAN-BUG-BC-SCRUB.md`](./PLAN-BUG-BC-SCRUB.md) as `orphan_scrub_interval` (per user 2026-05-26: data-consistency priority + node quarantine semantics).
- **BA-2 (disk usage fidelity)** → forks into `PLAN-BUG-BB-DISK-USAGE-FIDELITY.md` (per-disk `used_bytes` in heartbeat; remove apportionment lie).

Bug BD covers the WebUI config-pane scope split — see [`PLAN-BUG-BD-CONFIG-PANE.md`](./PLAN-BUG-BD-CONFIG-PANE.md).

---

## 6. Commit-message convention

```
BA-<n>: <one-line summary, <72 chars>>

<body>

Plan: plans/active/PLAN-BUG-BA-SOAK-2026-05-26.md (BA-<n> \xe2\x9c\x85)
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```
