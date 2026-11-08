# Live Reload

DTFS has **no live configuration reload**. No daemon watches its TOML file or handles `SIGHUP`. Every option in `manager.toml`, `data.toml` and `fuse.toml` is read once at process start; a change takes effect after the process restarts.

## What applies when

| Change | How it takes effect |
|---|---|
| Any key in a role's TOML file | Restart that daemon (`systemctl restart dtfs-manager`, `dtfs-data`, `dtfs-fuse@<mount>`). |
| Manager `[scrub]` policy (intervals, `max_io_percent`, `verify_on_read`, `quarantine_on_fault`, `auto_unquarantine_secs`) | Restart the managers. Each manager sends the values to every data node in the next heartbeat response; data nodes apply them without a restart. |
| Volume policy (`set-cache`, `set-mandatory`, `set-anchor`, `set-write-safety`, `set-spill`) | Immediately: it is Raft state, picked up by the next write or scheduler pass. See [Volume Configuration](./volume-config.md). |
| `POST /api/v1/config` override | Stored in Raft and returned by `GET /api/v1/config` and `/config/effective`. No daemon applies it at runtime. See [Cluster-wide Configuration](./cluster-config.md). |
| Maintenance mode, unquarantine, migrations, flush-cache | Immediately (REST/CLI operations, not configuration). |

## Rolling restart

For a cluster-wide key, edit the file on every node first, then restart in the order the deploy playbook uses: managers (learners, then followers, then the leader), then data nodes, then clients, waiting for health between nodes. See [CI Pipeline](../operations/ci-pipeline.md).

## Verifying

- Effective values on one manager: `GET /api/v1/config/effective` (secrets masked).
- Daemon version and build date after a restart: `dtfs-<role> --version`.
- Process start problems (failed validation, TLS, disk ids) are in the unit's journal.
