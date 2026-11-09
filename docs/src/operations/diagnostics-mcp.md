# Live Diagnostics (MCP)

`tools/dtfs-mcp/dtfs_mcp.py` is a single-file MCP server (stdio) that lets Claude inspect a
live cluster: manager REST state, journald logs, disk/WAL/quarantine state and per-chunk
placement, without hand-run commands. The `dtfs-diagnose` skill
(`.claude/skills/dtfs-diagnose/SKILL.md`) is the runbook that drives it. It is host-side
tooling and is not part of the Rust build.

## Setup

1. Install [uv](https://docs.astral.sh/uv/) and make sure the OpenSSH client (`ssh`) is on `PATH`.
2. Copy `tools/dtfs-mcp/clusters.example.toml` to `%USERPROFILE%\.dtfs\clusters.toml`
   (or set `DTFS_MCP_CONFIG`). Fill in:
   - `managers`: REST addresses (`host:9402`), reachable from your machine (Tailscale).
   - `shared_secret` or `shared_secret_env` (name of an environment variable). Only needed
     for the gated `op_*` tools; reads are unauthenticated GETs.
   - `ssh_user`, `ssh_key` (the Ansible deploy key, `~/.ssh/dtfs-deploy/id_ed25519`), `ssh_port`.
   - One `[[clusters.<name>.hosts]]` per machine: `name` must equal the node hostname shown
     by `/api/v1/nodes`; `address` is what `ssh` connects to.
3. The repo-root `.mcp.json` registers the server as `dtfs`
   (`uv run --script tools/dtfs-mcp/dtfs_mcp.py`; dependencies come from the inline PEP 723
   header, `mcp>=1.27,<2` and `httpx`). Check with `claude mcp list`.

Without a config the server still starts; every tool returns a `setup_required` message.

## Tools

| Tool | Purpose |
|---|---|
| `cluster_overview(cluster?)` | Every manager's raft state and readiness, plus the leader's status, stats, nodes, memory, integrity faults and migrations; computed `findings` (leader split, term split, applied lag, unreachable manager, unhealthy/quarantined nodes, version skew, missing/undergoal chunks, stale heartbeats, failed migrations, memory pressure, recent faults) |
| `rest_get(path, params?, manager?, cluster?)` | GET under `/api/v1/`, `/metrics`, `/readyz`, `/healthz` only; defaults to the leader |
| `audit_merged(limit?, action?, since_ms?, cluster?)` | `/audit` from every manager, de-duplicated, newest first |
| `metrics(match?, manager?, cluster?)` | Prometheus text parsed to `name{labels}: value` |
| `host_logs(host, unit?, since?, until?, grep?, lines?)` | journald for `manager`, `data`, `fuse`, `all` or `kernel`; `grep` runs locally |
| `host_status(host)` | unit state, restarts, memory, versions, uptime, `df`, clock sync, kernel OOM lines (24 h) |
| `host_disks(host, count_chunks?)` | per `[[disks]]` path from `/etc/dtfs/data.toml`: `df`, WAL, quarantine, `tmp`, disk UUID, backing device |
| `host_smart(host, device?)` | `smartctl` summary for a device listed by `lsblk` |
| `host_config(host, role)` | `/etc/dtfs/<role>.toml` with secret/token/password/key_file values redacted |
| `host_chunk(host, chunk_id)` | `.dat`/`.meta` stat, `.meta` content, quarantine and WAL entries for one chunk |
| `host_net(host, targets?)` | `ss -tlnp` plus optional `host:port` reachability probes from that host |
| `files_list(prefix?, limit?, offset?)` | page `/api/v1/files` (limit 1..1000); returns `total` and `truncated`, page with `offset` |
| `file_lookup(path)` | one file: per-chunk replicas joined with node health, DC, disk status, tier and the volume anchor policy; `problem_chunks` lists chunks with no live replica or no live anchor replica (first 50 chunks shown) |
| `chunk_detail(chunk_id)` | `/api/v1/chunks/{id}`: placement state, owning files, replicas, pending deletion |
| `volume_detail(volume_id?)` | all volumes, or one volume's full policy (mandatory/cache replicas, anchor, write safety, spill) |
| `node_detail(node_id)` | `/api/v1/nodes/{id}`: location, per-disk uuid/tier/capacity/status, health |
| `migrations_status()` | migration/repair queue summarised by status, failed tasks with `last_error` first |
| `integrity_faults(limit?)` | integrity-fault audit rows (checksum, quarantine, scrub), newest first |
| `memory_status()` | `/api/v1/memory` from every manager (pressure, RSS vs hard cap); data/FUSE memory via `host_status` and `metrics` |
| `raft_replication()` | per-manager term, leader, applied lag, voters/learners, per-follower replication lag, `millis_since_quorum_ack` |
| `config_diff()` | manager config keys that differ from defaults (`/config/effective`), secret-like keys masked |
| `host_disk_ids(host)` | persisted `<disk>/.dtfs-disk-id` vs the 0-based `[[disks]]` position; `ok=false` means dtfs-data will refuse to start |
| `host_quarantine(host)` | per data disk: count and listing (capped at 300 lines) of `.dtfs/quarantine` |
| `op_scrub(level?, confirm?)` | gated: `POST /api/v1/scrub` (full scrub of all disks) |
| `op_maintenance(node_id, enabled, confirm?)` | gated: `POST /api/v1/nodes/{id}/maintenance` |
| `op_unquarantine(node_id, operator_id, reason_acknowledged, confirm?)` | gated: `POST /api/v1/nodes/{id}/unquarantine` |
| `op_flush_cache(path_prefix, cache_tiers?, cooldown_secs?, confirm?)` | gated: `POST /api/v1/files/flush_cache`; tiers limited to `warm`/`hot`, prefix must name a subtree; dry run previews matching files |
| `op_migrate(path_prefix, target_tier, target_dc?, max_tasks?, confirm?)` | gated: `POST /api/v1/migrations`; prefix must name a subtree, `max_tasks` 1..10000; dry run previews matching files |
| `op_decommission(node_id, operator_id, reason?, confirm?)` | gated: `POST /api/v1/nodes/{id}/decommission`; dry run shows the node, server refuses while it holds replicas |
| `op_remove_dead(node_id, operator_id, reason?, accept_data_loss?, confirm?)` | gated: `POST /api/v1/nodes/{id}/remove-dead`; without `confirm` it asks the server for its plan (`dry_run`) and changes nothing |
| `op_reap_orphans(grace_secs?, confirm?)` | gated: `POST /api/v1/scrub/orphans`; `grace_secs` >= 3600 (default: configured `scrub.orphan_grace_secs`). Without `confirm` it runs the server's dry run (deletes nothing) and waits up to 120 s for its per-node candidate counts; with `confirm=true` it starts a real reap in the background (result: `rest_get /api/v1/scrub/orphans` on the leader) |

`/api/v1/chunks`, `/api/v1/chunks/{id}`, `/api/v1/memory` and the raft-state `replication`
field exist only on manager builds that include them; older builds answer 404 and the tools
report that instead of failing.

## On-disk layout the host tools assume

`<disk>/chunks/<aa>/<bb>/<chunk_id>.{dat,meta}` where `aa`/`bb` are the first two bytes of the
chunk id in hex; `<disk>/.dtfs/wal/`, `<disk>/.dtfs/quarantine/<id>.dat.bad`, `<disk>/tmp/`,
`<disk>/.dtfs-disk-id` (the disk's 0-based position in `[[disks]]`, written on first start). Units: `dtfs-manager`, `dtfs-data`, `dtfs-fuse@<mount>`.

## Security notes

- SSH access is limited to fixed command templates. Every interpolated value (host, unit,
  time, chunk id, device, `host:port`, disk path, role) is validated against a strict regex and
  then shell-quoted; hosts must be listed in the config. `ssh` runs with `BatchMode=yes`,
  `ConnectTimeout=10` and `StrictHostKeyChecking=accept-new`. Log filtering happens locally.
- The shared secret is only sent as `X-Dtfs-Shared-Secret` on gated ops and is scrubbed from
  any returned text. `host_config` redacts secret-like keys before returning.
- Gated ops are dry runs unless `confirm=true`; `.claude/settings.json` allowlists only the
  read tools, so `op_*` always prompts. A follower's 409 leader hint is followed once.
- No op can target an empty or `/` prefix (the server's `allow_empty_prefix` escape hatch is
  never sent) and flush cannot touch cold. Last-replica and anchor protection stay with the
  server (`apply_remove_chunk_location`, the decommission replica check); the MCP adds no way
  around them. There are no orphan-sweep or anchor-repair endpoints, so there are no tools for
  them (the scheduler owns repair).
- `config_diff` masks secret/token/password/webhook keys on top of the server's own masking;
  `host_config` redacts webhook URLs and credentials.
- `.claude/settings.json` allowlists read tools by name (`mcp__dtfs__<tool>`); the new read tools
  need adding there to avoid prompts. `op_*` must stay off the allowlist.
- Output is capped near 60 KB per call; timeouts are 10 s (REST), 30 s (SSH), 60 s (logs).
  Unreachable managers or hosts become findings, not errors.
- Real SSH and live-cluster behaviour is untested in CI: the tests cover validation, argv and
  command construction, redaction, findings logic and the gated-op flow with fakes.

## Tests

```
uv run --with pytest --with "mcp<2" --with httpx pytest tools/dtfs-mcp -q
```
