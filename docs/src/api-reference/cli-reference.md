# CLI Reference

`dtfs-cli` is the command-line administration tool for DTFS. It communicates with the manager's REST API to perform cluster operations.

## Global Options

```
dtfs-cli [OPTIONS] <COMMAND>
```

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--manager <HOST:PORT>` | `-m` | `localhost:9402` | Manager REST API address |
| `--format <FORMAT>` | | `table` | Output format: `table` or `json` |

The manager address can also be set via the `DTFS_MANAGER` environment variable:

```bash
export DTFS_MANAGER="manager-01:9402"
```

---

## Commands

### status

Show cluster status overview.

```bash
dtfs-cli status
```

**Example output:**

```
DTFS Cluster Status
===================
Cluster:        production
Status:         ok
Manager:        manager-01
Data Nodes:     6 (6 healthy)
Files:          1423
Total Capacity: 12.0 TB
Used Capacity:  4.0 TB
Config Version: 3
```

**JSON output:**

```bash
dtfs-cli status --format json
```

---

### nodes

List all data nodes or show details for a specific node.

```bash
# List all nodes
dtfs-cli nodes

# Show details for a specific node
dtfs-cli nodes --id <node-id>
```

| Option | Description |
|--------|-------------|
| `--id <ID>` | Show detailed info for a specific node, including disk inventory |

**Example (list):**

```bash
$ dtfs-cli nodes
NODE ID              HOSTNAME             LOCATION                       DISKS  CAPACITY     USED         HEALTH
--------------------------------------------------------------------------------------------------------------
1                    data-01.example.com  us-east/dc1/rack-a/data-01     4      4.0 TB       1.0 TB       Healthy
2                    data-02.example.com  us-east/dc1/rack-b/data-02     4      4.0 TB       1.5 TB       Healthy
3                    data-03.example.com  us-east/dc1/rack-c/data-03     4      4.0 TB       1.5 TB       Healthy

Total: 3 nodes
```

**Example (detail):**

```bash
$ dtfs-cli nodes --id 1
Node Details
============
ID:       1
Hostname: data-01.example.com
Address:  data-01.example.com:9500
Health:   Healthy
Role:     Data
Location: us-east/dc1/rack-a/data-01

Disks (4):
  ID       Tier       Path                     Total        Used         Status
  0        Hot        /mnt/dtfs-disk-a1b2      1.0 TB       250.0 GB     Healthy
  1        Hot        /mnt/dtfs-disk-c3d4      1.0 TB       250.0 GB     Healthy
  2        Warm       /mnt/dtfs-disk-e5f6      1.0 TB       300.0 GB     Healthy
  3        Cold       /mnt/dtfs-disk-g7h8      1.0 TB       200.0 GB     Healthy
```

---

### volumes

List all volumes with file counts and total sizes.

```bash
dtfs-cli volumes
```

**Example output:**

```
VOLUME ID                FILES      TOTAL SIZE
--------------------------------------------------
default                  1200       512.0 GB
archive                  223        100.0 GB
```

#### volumes migrate-policy

Bug AV — copy a volume's legacy `replication` block into the new
`mandatory_replicas` field. Idempotent: re-running it once the
migration has been applied is a no-op.

```bash
dtfs-cli volumes migrate-policy <volume_id>
```

**Example:**

```
$ dtfs-cli volumes migrate-policy default
Volume 'default' migrate-policy: applied (mandatory hot=0 warm=2 cold=1 tape=1)

$ dtfs-cli volumes migrate-policy default
Volume 'default' migrate-policy: no-op (already migrated) (mandatory hot=0 warm=2 cold=1 tape=1)
```

Exits non-zero with an HTTP 404 message if the volume is unknown.
See [REST API → POST /api/v1/volumes/{id}/migrate-policy](./rest-api.md#post-apiv1volumesidmigrate-policy)
for the underlying request.

---

#### volumes show

Bug AV (AV-3-5) — render a volume's full configuration including
the AV `mandatory_replicas` / `cache_replicas` / `anchor` fields.

```bash
dtfs-cli volumes show <volume_id>
```

**Example:**

```
$ dtfs-cli volumes show default
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
  promote_window_secs:  14400
  cooldown_secs:        300
Anchor:  dc=leli required_tier=cold
```

Use `--format json` for scripted consumption (returns the same JSON
shape as `GET /api/v1/volumes/{id}`).

---

#### volumes set-cache

Bug AV (AV-3-5) — set or update a volume's `cache_replicas` policy.
Every flag is optional; omitted flags keep the current value.
Validates `max >= min` per tier at the REST layer.

```bash
dtfs-cli volumes set-cache <volume_id> \
    [--warm-min N] [--warm-max N] [--hot-min N] [--hot-max N] \
    [--per-dc true|false] \
    [--promote-reads N] [--promote-window-secs N] [--cooldown-secs N]
```

**Example — Plexmedia-style warm cache:**

```
$ dtfs-cli volumes set-cache default \
    --warm-min 1 --warm-max 1 --per-dc true \
    --promote-reads 2 --promote-window-secs 14400 \
    --cooldown-secs 300
Volume 'default' set-cache: applied
  warm: min=1 max=1
  hot:  min=0 max=0
  per_dc:               true
  promote_reads:        2
  promote_window_secs:  14400
  cooldown_secs:        300
```

Idempotent: re-running with the same flags reports `no-op`.

**Errors:**

| Status | Reason |
|--------|--------|
| HTTP 400 | `max < min` for a tier (e.g. `--warm-min 2 --warm-max 1`). |
| HTTP 404 | Volume not found. |

---

#### files flush-cache

Bug AV (AV-3-5) — drop cache replicas under a path prefix. Mandatory
(anchor) replicas are always preserved; a chunk whose flush would
strand the anchor is refused and audited (`cache.flush_refused`).

```bash
dtfs-cli files flush-cache <path> \
    [--cache-tiers warm[,hot]] \
    [--cooldown-secs N] \
    [--max-tasks N] \
    [--allow-empty-prefix]
```

**Defaults:** `--cache-tiers warm`, `--cooldown-secs 300`,
`--max-tasks 10000`.

**Example:**

```
$ dtfs-cli files flush-cache /movies/Season-1/ --cooldown-secs 60
flush-cache prefix=/movies/Season-1/ tiers=["warm"] cooldown=60s
  files_matched     = 12
  chunks_examined   = 482
  replicas_dropped  = 482
  mandatory_refusals = 0
  unreachable_nodes = 0
  rpc_failures      = 0
  raft_failures     = 0
```

A non-zero `mandatory_refusals` indicates the planner protected
against data loss for those chunks AND fired an `AnchorViolated`
alert; investigate via the audit log:

```bash
dtfs-cli audit --action cache.flush_refused
```

See [REST API → POST /api/v1/files/flush_cache](./rest-api.md#post-apiv1filesflush_cache)
for the underlying request body and audit semantics.

> **Breaking change:** the legacy `dtfs-cli files --limit N`
> moved to `dtfs-cli files list --limit N` to make room for the
> `files flush-cache` subcommand. Both share the noun-verb
> hierarchy used by `volumes`, `nodes`, etc.

---

### files

File-level operations. Two subcommands:

```bash
dtfs-cli files list [--limit N]      # List files (default 50)
dtfs-cli files flush-cache <path> …  # Bug AV — see above
```

#### files list

```bash
dtfs-cli files list [--limit N]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--limit <N>` | 50 | Maximum number of files to display |

**Example output:**

```
Total files: 1423

PATH                                     VOLUME       CHUNKS   SIZE
---------------------------------------------------------------------------
/data/reports/2024-q1.parquet            default      3        384.0 MB
/data/reports/2024-q2.parquet            default      3        384.0 MB
/data/logs/access-2024-03.csv            default      1        128.0 MB
```

---

### scrub

Trigger a scrub operation. Without target options, triggers a cluster-wide scrub.

```bash
# Cluster-wide deep scrub
dtfs-cli scrub

# Scrub a specific disk
dtfs-cli scrub --disk /mnt/dtfs-disk-a1b2c3d4

# Scrub a specific file
dtfs-cli scrub --file /data/reports/2024-q1.parquet

# Scrub a specific chunk
dtfs-cli scrub --chunk abc123def456

# Quick scrub (metadata only)
dtfs-cli scrub --level quick
```

| Option | Description |
|--------|-------------|
| `--disk <PATH>` | Target a specific disk by mount path |
| `--file <PATH>` | Target a specific file |
| `--chunk <ID>` | Target a specific chunk by ID |
| `--level <LEVEL>` | Scrub level: `quick` (metadata) or `deep` (full checksum). Default: `deep` |

**Example output:**

```
Scrub triggered: disk /mnt/dtfs-disk-a1b2c3d4 (level: deep)
Status: accepted
```

---

### maintenance

Enable or disable maintenance mode on a node. See [Maintenance Mode](../operations/maintenance-mode.md) for the full draining process.

```bash
# Enable maintenance mode
dtfs-cli maintenance <node-id> --enable

# Disable maintenance mode
dtfs-cli maintenance <node-id> --disable
```

| Argument | Description |
|----------|-------------|
| `<node-id>` | Target node ID (required) |

| Option | Description |
|--------|-------------|
| `--enable` | Enter maintenance mode (stops new placements, begins draining) |
| `--disable` | Exit maintenance mode (resumes normal operation) |

**Example output:**

```
Maintenance mode ENABLED for node 2
Current state: maintenance
```

---

### audit

View audit log entries. See [Audit Log](../security/audit-log.md) for details on the log format.

```bash
# Show 20 most recent entries
dtfs-cli audit

# Show more entries
dtfs-cli audit --limit 100

# Filter by action type
dtfs-cli audit --action node.join

# Filter by action prefix
dtfs-cli audit --action config --limit 50
```

| Option | Default | Description |
|--------|---------|-------------|
| `--limit <N>` | 20 | Maximum entries to display |
| `--action <PREFIX>` | (none) | Filter entries by action prefix |

**Example output:**

```
TIME                   ACTOR          ACTION                   TARGET                         RESULT
----------------------------------------------------------------------------------------------------
2024-03-19 14:00:00    api            config.update            version=4                      success
2024-03-19 13:45:12    data-03        node.join.complete       3                              success
2024-03-19 13:45:11    data-03        node.join.request        data-03.example.com            accepted
2024-03-19 12:30:00    api            maintenance.toggle       node=2                         enabled
```

---

### config

View the current cluster configuration.

```bash
dtfs-cli config
```

| Option | Description |
|--------|-------------|
| `--show` | Show current config (default behavior) |

**Example output:**

```
Cluster Configuration
=====================
Config Version: 3

Defaults:
  Chunk Size:       128 MB
  Min Replicas ACK: 2
  Write Timeout:    30s
  Replication:      hot=3, warm=2, cold=1

Tiering:
  Enabled:              true
  Hot Threshold:        5 reads/hour
  Cold Threshold:       14 days
  Migration BW Limit:   200 Mbps
```

---

### health

Check whether the manager is healthy. Exits with code 0 on success, 1 on failure. Useful for scripting and monitoring.

```bash
dtfs-cli health
```

**Example output (healthy):**

```
Manager is healthy
```

**Example output (unhealthy):**

```
Manager status: unavailable
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Command completed successfully |
| 1 | Command failed (unhealthy manager, API error, etc.) |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `DTFS_MANAGER` | Default manager address (overridden by `--manager` flag) |
| `DTFS_TOKEN` | JWT authentication token |
