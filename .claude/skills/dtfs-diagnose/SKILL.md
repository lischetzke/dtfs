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

- Read tools are free to use. `op_scrub`, `op_maintenance`, `op_unquarantine` change the
  cluster: describe the exact action and why, get the user's explicit yes in chat, only then
  call with `confirm=true`. Default call (dry run) is fine to show the user first.
- Never act against the LELI anchor copy: do not propose deleting, draining, decommissioning,
  quarantining away, or maintenance-moding the only node holding a chunk's anchor replica.
  Every chunk must keep >=1 replica on LELI data-primary01.
- Never delete or rename files on disk; this toolset is read-only on hosts by design.
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

- **Plex stall / slow reads**: overview (node health, memory), `host_logs` fuse + data on the
  client and serving data node (`grep "slow|timeout|failover|data_loss"`), `host_net` cross-DC
  probe, `host_status` for the data node (load, memory). Read path: `docs/src/internals`.
- **Write reject / write errors**: overview `findings` (undergoal, node Down, memory
  Shed), `rest_get("/api/v1/memory")`, `host_logs` manager+fuse (`grep "min_replicas|lease|RESOURCE_EXHAUSTED"`),
  `host_disks` for full disks or WAL backlog. Troubleshooting: "Write Failures".
- **Node Down / Suspect**: overview, `host_status` (is the unit active, NRestarts), `host_logs`
  data/manager + `kernel`, `host_net`, `host_status` clock section (HMAC window is 300 s).
  Troubleshooting: "Node Shows as Suspect or Down"; failure-scenarios: data node dies.
- **Undergoal / missing chunks**: chunks list by state, `/chunks/<id>`, `rest_get("/api/v1/migrations")`
  (failed repair tasks and `last_error`), `host_chunk` on holders, `host_disks` (quarantine).
  Repair is owned by the scheduler; do not hand-copy chunks.
- **Quarantined node / integrity faults**: faults + audit, `host_logs` data (`grep "checksum|quarantine|scrub"`),
  `host_smart` on the backing device, `host_disks`. Troubleshooting: "Scrub Reports Corrupt
  Chunks". Unquarantine is an op: only after the root cause is understood and the user agrees.
- **OOM / memory pressure**: `rest_get("/api/v1/memory")`, `host_status` (kernel_oom, MemoryCurrent
  vs MemoryMax), `host_logs` kernel, `host_config` (`[memory]` caps per co-resident role).
- **Raft flap / no leader**: overview (leader_disagreement, term_split, applied_lag,
  replication lag), `host_logs` manager on each voter (`grep "election|vote|term|leader"`),
  `host_net` between managers, clock section. Troubleshooting: "Raft Leader Election Loops";
  failure-scenarios: manager failures and power-outage cases.
- **FUSE mount dead / client cannot connect**: `host_status` (dtfs-fuse@ unit), `host_logs` fuse,
  `host_config` fuse (manager_addrs must include a voter), `host_net` to managers.

## Finishing

Summarise: root cause (or best hypothesis plus what would confirm it), evidence list, blast
radius, recommended next steps. List any ops you would run and wait for the user's decision.
