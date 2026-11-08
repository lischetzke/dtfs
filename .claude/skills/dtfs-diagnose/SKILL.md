---
name: dtfs-diagnose
description: Diagnose a live DTFS cluster incident (Plex stall, write reject, node Down, undergoal/missing chunks, OOM, raft flap, quarantine) using the dtfs MCP server tools (cluster_overview, host_logs, host_chunk, ...). Use when the user reports a cluster symptom or asks to analyse cluster health.
---

# DTFS live-cluster diagnosis

Tools come from the `dtfs` MCP server (`tools/dtfs-mcp/dtfs_mcp.py`, setup in
`docs/src/operations/diagnostics-mcp.md`). If a tool returns `setup_required`, tell the
user what is missing (config at `%USERPROFILE%\.dtfs\clusters.toml`) and stop. Reference
runbooks: `docs/src/operations/troubleshooting.md`, `docs/src/operations/failure-scenarios.md`.

## Rules

- Read tools are free to use. Every `op_*` tool changes the cluster (`op_scrub`,
  `op_maintenance`, `op_unquarantine`, `op_flush_cache`, `op_migrate`, `op_decommission`):
  describe the exact action and why, get the user's explicit yes in chat, only then call with
  `confirm=true`. The default call is a dry run (it also previews matching files / node state);
  show it to the user first.
- Op semantics: `op_flush_cache` only drops warm/hot cache replicas under a non-empty prefix
  (server keeps mandatory and anchor replicas, sets a re-promotion cooldown). `op_migrate`
  queues tier/DC moves for a non-empty prefix (server anchor guards apply; watch
  `migrations_status` afterwards). `op_decommission` removes a node from the registry and is
  refused (409) while the node holds any replica: drain first, and never for the node holding a
  chunk's LELI anchor. No op can be pointed at an empty prefix; do not try to work around that.
- Never act against the LELI anchor copy: do not propose deleting, draining, decommissioning,
  quarantining away, or maintenance-moding the only node holding a chunk's anchor replica.
  Every chunk must keep >=1 replica on LELI data-primary01.
- Never delete or rename files on disk; this toolset is read-only on hosts by design.
- Read tools: `file_lookup` (one path), `files_list` (paged; if `truncated` is true there are more
  rows, raise `offset`), `chunk_detail`, `volume_detail`, `node_detail`, `migrations_status`,
  `integrity_faults`, `memory_status` (every manager), `raft_replication`, `config_diff`
  (non-default keys, secrets redacted), `host_disk_ids`, `host_quarantine`.
- New routes (`/api/v1/chunks`, `/api/v1/memory`, raft `replication`) return 404 on older
  manager builds: say so and fall back to `cluster_overview` counts + `host_chunk`.
- Output is capped near 60 KB: narrow with `since`/`until`/`grep`/`lines` instead of repeating.
- Report facts with evidence (tool, host, timestamp). Say clearly what is inference.

## Standard sequence

1. `cluster_overview` -> read `findings` first (critical, then warning). Note leader, term,
   unreachable managers, node health, version skew, chunk placement counts, memory pressure.
2. `rest_get("/api/v1/integrity/faults", {"limit": 50})` and `audit_merged(limit=100)`
   (optionally `action=` prefix such as `node.quarantine`, `migration`) to see what changed
   and when. Pick the incident window from the timestamps.
3. `host_logs` on the implicated hosts, same window, per role (`manager`, `data`, `fuse`,
   `kernel`); use `grep` locally (e.g. `ERROR|WARN|data_loss|checksum|quarantine|oom`).
   Correlate across hosts by timestamp.
4. Chunk drill-down: `rest_get("/api/v1/chunks", {"state": "missing|undergoal|misplaced|overgoal|pending_deletion", "limit": 50})`
   -> `rest_get("/api/v1/chunks/<chunk_id>")` -> `host_chunk(host, chunk_id)` on each
   replica holder (check `.dat`/`.meta`, quarantine, WAL).
5. Host resources: `host_status` (units, restarts, memory, clock skew, OOM), `host_disks`
   (WAL backlog, quarantine, tmp leftovers, free space), `host_smart` (drive health),
   `host_net` (listening ports; probe cross-DC `host:port` when a node looks partitioned).
   `host_config` to confirm effective settings (secrets redacted).

## Symptom -> sequence

- **One file unreadable / Plex says missing**: `file_lookup(path)` -> `problem_chunks`
  (no live replica / no live anchor replica), then `chunk_detail` + `host_chunk` on holders,
  `host_quarantine` on those hosts. `files_list(prefix)` finds the path.

- **Plex stall / slow reads**: overview (node health, memory), `host_logs` fuse + data on the
  client and serving data node (`grep "slow|timeout|failover|data_loss"`), `host_net` cross-DC
  probe, `host_status` for the data node (load, memory). Read path: `docs/src/internals`.
- **Write reject / write errors**: overview `findings` (undergoal, node Down, memory
  Shed), `rest_get("/api/v1/memory")`, `host_logs` manager+fuse (`grep "min_replicas|lease|RESOURCE_EXHAUSTED"`),
  `host_disks` for full disks or WAL backlog. Troubleshooting: "Write Failures".
- **dtfs-data refuses to start (disk position changed)**: `host_logs` data
  (`grep "refusing to start|disk_id"`), `host_disk_ids` (persisted `.dtfs-disk-id` vs 0-based
  position in `[[disks]]`; ok=false is the culprit), `host_config data`. Fix is operator-side:
  restore the original `[[disks]]` order, never remove or reorder entries (replace a drained disk
  in place). Identity is `(node_id, disk_id)`, so a wrong position would misattribute chunks and
  the orphan reaper would delete the wrong copies; do not edit `.dtfs-disk-id` to silence it.
- **Node Down / Suspect**: overview, `host_status` (is the unit active, NRestarts), `host_logs`
  data/manager + `kernel`, `host_net`, `host_status` clock section (HMAC window is 300 s).
  Troubleshooting: "Node Shows as Suspect or Down"; failure-scenarios: data node dies.
- **Undergoal / missing chunks**: chunks list by state, `/chunks/<id>`, `rest_get("/api/v1/migrations")`
  (failed repair tasks and `last_error`), `host_chunk` on holders, `host_disks` (quarantine).
  Repair is owned by the scheduler; do not hand-copy chunks.
- **Quarantined node / integrity faults**: faults + audit, `host_logs` data (`grep "checksum|quarantine|scrub"`),
  `host_smart` on the backing device, `host_disks`. Troubleshooting: "Scrub Reports Corrupt
  Chunks". `host_quarantine` lists `.dtfs/quarantine/*.dat.bad`; after a data-node restart the
  quarantined chunks are re-reported to the manager (log "re-reporting quarantined chunks at
  startup"), so repeated fault rows after a restart are the same chunks, not new corruption:
  compare chunk ids via `integrity_faults`. Unquarantine is an op: only after the root cause is understood and the user agrees.
- **OOM / memory pressure**: `rest_get("/api/v1/memory")`, `host_status` (kernel_oom, MemoryCurrent
  vs MemoryMax), `host_logs` kernel, `host_config` (`[memory]` caps per co-resident role).
- **Raft flap / no leader**: overview (leader_disagreement, term_split, applied_lag,
  replication lag), `raft_replication` (per-follower lag, `millis_since_quorum_ack`),
  `host_logs` manager on each voter (`grep "election|vote|term|leader"`),
  `host_net` between managers, clock section. Troubleshooting: "Raft Leader Election Loops";
  failure-scenarios: manager failures and power-outage cases.
- **Raft auth / unsigned RPCs**: `host_logs` manager (`grep "unsigned"`) shows the rate-limited
  warning when `cluster.require_raft_auth=false` and a peer sends unsigned Raft RPCs (normal
  mid rolling update, a finding afterwards). `config_diff` / `host_config manager` show
  `require_raft_auth`. Stamped-but-invalid RPCs are always rejected, so repeated rejections point
  at a wrong `shared_secret` or clock skew (`host_status` clock section). Enabling
  `require_raft_auth` is an operator config change once every manager signs; it has no effect
  with an empty secret.
- **Config suspicion / process will not start**: `config_diff`, `host_logs` at startup. Interval
  settings (`[tiering]`, `[monitoring]`, raft timings, FUSE cache prune / quorum-loss timeout)
  are validated at startup and a zero or inconsistent value aborts the process naming the key;
  fix the TOML, do not retry-loop.
- **Volume policy edits rejected**: `volume_detail(<id>)` for the current policy. A 409 on a
  volume write means it changed concurrently or already exists: re-read, re-apply, never
  blind-retry. Other 400s are server guards (all-zero mandatory, anchor tier unsatisfied).
- **FUSE mount dead / client cannot connect**: `host_status` (dtfs-fuse@ unit), `host_logs` fuse,
  `host_config` fuse (manager_addrs must include a voter), `host_net` to managers.

## Finishing

Summarise: root cause (or best hypothesis plus what would confirm it), evidence list, blast
radius, recommended next steps. List any ops you would run and wait for the user's decision.
