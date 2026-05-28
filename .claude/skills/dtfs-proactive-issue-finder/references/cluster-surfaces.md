# DTFS cluster surfaces — read-only cheat sheet

Everything here is observational. Run the API/CLI forms first; SSH only for logs
the API does not expose. All `dtfs-cli` invocations take
`--manager <host:9402> --format json` (omitted below for brevity). Default
manager API base is `http://<mgr>:9402`.

## Read-only REST endpoints (GET unless noted)

| Endpoint | Returns | Key fields to read |
|---|---|---|
| `/api/v1/cluster/status` | cluster summary | `cluster_name`, `status`, `data_nodes`, `data_nodes_healthy`, `files`, `total_capacity`, `used_capacity`, `config_version` |
| `/api/v1/health` | manager health | `status` (`healthy` or else) |
| `/api/v1/raft/state` | raft membership | leader id, term, per-peer match/commit (confirm quorum + no lag) |
| `/api/v1/nodes` | all nodes | `node_id`, `hostname`, `location`, `role`, `health`, `disk_count`, `total_capacity`, `used_capacity`; quarantine projection (BC-12a) |
| `/api/v1/nodes/{id}` | one node detail | `health`, `role`, `location.{region,datacenter,rack,host}`, `disks[].{disk_id,tier,mount_path,total_bytes,used_bytes,status}` (SMART/status per disk) |
| `/api/v1/volumes` | volume list | `volumes[].{volume_id,file_count,total_size}` |
| `/api/v1/volumes/{id}` | full volume config | `replication{hot,warm,cold,tape}`, `mandatory_replicas{...}`, `cache_replicas{warm{min,max},hot{min,max},per_dc,promote_reads,promote_window_secs,cooldown_secs}`, `anchor{datacenter,required_tier}` |
| `/api/v1/files` | file list | `count`, `files[].{path,volume_id,chunk_count,total_size}` |
| `/api/v1/files/inspect?path=...` | per-file replica placement | which nodes/tiers hold each chunk (use to confirm anchor coverage) |
| `/api/v1/audit?limit=N&action=PREFIX` | audit log | `timestamp`(secs), `actor`, `action`, `target`, `result`, `summary`, `origin_manager`. NOTE: a good row's `result` is `ok` **or** `success` — only `error`/`failed`/`refused`/`denied` are failures |
| `/api/v1/integrity/faults?limit=N` | integrity feed | same shape as audit; filtered to integrity actions (below) |
| `/api/v1/migrations` | migration queue/tasks | `count`, `tasks[].{task_id,chunk_id,tier,status,attempts,last_error,source_node,target_node,created_at_ms,updated_at_ms}`; read twice minutes apart to detect stuck/looping. Flag `status=failed`, orphan tasks (`last_error` = chunk gone), and `source_node==target_node` |
| `/api/v1/config` | cluster config | `version`, `defaults.{chunk_size_mb,write_safety,replication}`, `tiering.{enabled,...}` |
| `/api/v1/config/effective` | resolved effective config | merged view incl. scoped overrides |
| `/api/v1/stats` | dashboard aggregate | capacity, io rates, quarantined-node count (BC-11). NOTE: rate fields are delta-based between calls; one call may read 0 |

Do NOT call the mutating routes (here for avoidance only): any POST —
`/scrub`, `/files/flush_cache`, `/nodes/{id}/maintenance`,
`/nodes/{id}/unquarantine`, `/volumes` (create), `/volumes/{id}/set-*`,
`/volumes/{id}/migrate-policy`, `/config` (update), `/join/*`.

## Read-only dtfs-cli subcommands

`status` · `nodes [--id <id>]` · `volumes list` · `volumes show <id>` ·
`files list` · `audit [--limit N] [--action PREFIX]` · `config` · `health`

Forbidden (mutating): `scrub`, `maintenance`, `files flush-cache`,
`volumes create|set-cache|set-mandatory|migrate-policy`.

## Audit action vocabulary (filter with `--action <prefix>`)

- `cache.flush_refused` — a flush that would strand a mandatory/anchor copy was
  refused. Pairs with an `AnchorViolated` alert. Investigate: why was a flush
  attempted, is the anchor at risk?
- `cache.flush_replica` / `cache.flush_complete` — normal flush bookkeeping.
- `node.quarantine` / `node.unquarantine` — node health transitions; churn = a
  flapping node.

## Integrity feed actions (`/api/v1/integrity/faults`)

`node.quarantine`, `node.unquarantine`, `scrub.quick.fault`,
`scrub.deep.corrupt`, `read.integrity_fault`, `orphan.in_state_missing`.
Any `*.corrupt` / `*.fault` / `read.integrity_fault` is a data-integrity
finding (high/critical).

## SSH + logs (lab `10.150.0.0/24`, key `~/.ssh/dtfs-lab/id_ed25519`)

Systemd units: `dtfs-manager` (mgr nodes), `dtfs-data` (data nodes).

- `journalctl -u dtfs-manager --no-pager -n 300` — manager log tail.
- `journalctl -u dtfs-data --no-pager -n 300` — data-node log tail.
- `journalctl -u dtfs-data --no-pager --since "1 hour ago"` — windowed.

Grep targets in logs: `panic`, `ERROR`, `WAL`, `recover`, gRPC failures,
memory-pressure / `shed` / `RESOURCE_EXHAUSTED` (Bug G ladder, AGENTS.md § A.5),
anchor / replication warnings.

Read-only only: never `systemctl restart`, never edit `*.toml` on a node, never
write through a mounted FUSE path.

## Expected baseline — Plexmedia topology (AGENTS.md § A.1)

| Hostname | DC | Role | Storage |
|---|---|---|---|
| mgr01-03 | LELI | manager (Raft quorum) | small |
| data-primary01 | LELI | data — **anchor copy of every chunk** | ~50 TB HDD |
| mgr04 | TORO | manager | small |
| data-warm01 | TORO | data — active set / read-ahead | ~1 TB SSD |
| mgr05 | NRB | manager | small |
| data-warm02 | NRB | data — fresh downloads, replicates to LELI | ~1 TB HDD |

Invariants to check live state against:
- **Anchor**: every chunk has ≥1 replica on LELI `data-primary01`. Volume
  `anchor` policy should pin DC=LELI.
- Quorum sits at LELI managers; a missing/lagging manager threatens write
  durability.
- Warm disks (~1 TB) are small — near-full is expected to bite first.
- Bandwidth classes: LELI 40 Mbps, TORO 400 Mbps, NRB 1 Gbps — a migration
  saturating the LELI link can look "stuck" but is bandwidth-bound; distinguish
  bandwidth-bound (progressing slowly) from looping (same chunk re-queued).
