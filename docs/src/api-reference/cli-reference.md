# CLI Reference

`dtfs-cli` administers a cluster through the manager [REST API](./rest-api.md) and updates local binaries. Source of truth: the clap definitions in `crates/dtfs-cli/src/main.rs`; `dtfs-cli --help` and `dtfs-cli <command> --help` are authoritative for the installed build.

```
dtfs-cli [--manager <host:port>] [--secret <secret>] [--format table|json] <command> ...
```

## Global options

| Option | Env | Default | Meaning |
|---|---|---|---|
| `-m`, `--manager <host:port>` | `DTFS_MANAGER` | `localhost:9402` | Manager REST address. A bare `host:port` becomes `http://host:port`; an explicit `http://` or `https://` scheme is kept. Must appear **before** the subcommand. |
| `--secret <secret>` | `DTFS_SHARED_SECRET` | empty | Cluster `shared_secret`, sent as `X-Dtfs-Shared-Secret` on every mutating request. Required when the cluster has a secret. Global (may appear anywhere). Prefer the env var; a command-line secret is visible in the process list. |
| `--format table|json` | none | `table` | Output format. Global. `json` prints the server response and is the form to script against. |
| `--version` | none | none | Version and build date. |

Mutations must go to the leader (or the call returns `409`/`503`; see [REST API](./rest-api.md#leader-behaviour)). Find it with `dtfs-cli raft members`.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Success. |
| `1` | Any failure: connection error, non-success HTTP status, validation failure, a partially failed operation (see `files flush-cache`), `health` with a non-healthy status, update verification failure, `update` aborted at the prompt (answered `n` or no terminal input; nothing installed), `volumes delete` aborted at a confirmation (nothing deleted). |
| `2` | Usage error (unknown flag, missing argument). |

Error reporting differs by command. Read commands (`status`, `stats`, `nodes`, `files list`, `files inspect`, `audit`, `integrity faults`, `volumes list`, `volumes show`, `config`, `config effective`, `raft members`, `health`) and the mutations `volumes create`, `volumes migrate-policy`, `volumes set-cache`, `volumes set-precache`, `volumes set-mandatory`, `volumes set-write-safety`, `scrub` and `maintenance` report only the HTTP status line on failure. `volumes set-anchor`, `volumes set-spill`, `volumes delete`, `files flush-cache`, `scrub orphans` and `migrations create` print `HTTP <status>: <server error>` (for example `anchor.required_tier = Cold but mandatory_replicas.cold = 0`). `unquarantine`, `decommission`, `config set` and `raft add-learner|promote|remove` print the full JSON error body. To see the body of the first group, repeat the call with `curl`.

## Cluster

| Command | Description |
|---|---|
| `status` | `GET /api/v1/cluster/status`: status, node counts, files, capacity, config version. |
| `health` | `GET /api/v1/health`. Prints `Manager is healthy`; exits `1` otherwise. |
| `stats` | `GET /api/v1/stats`: counts, capacity, tier breakdown, placement states, last scrub. |
| `nodes [--id <node-id>]` | List nodes, or show one node with its disks. |
| `maintenance <node-id> (--enable \| --disable)` | Toggle maintenance mode. Exactly one flag is required. See [Maintenance Mode](../operations/maintenance-mode.md). |
| `unquarantine <node-id> --operator-id <label> --reason-acknowledged <text>` | Clear an integrity quarantine. The reason must be at least 16 characters. Prints the server JSON; exits `1` on failure (for example `409` node not quarantined). |
| `decommission <node-id> --operator-id <label> --reason <text>` | Remove a drained or dead node from the registry. `409` while it still holds replicas. |
| `nodes remove-dead <node-id> [--operator-id <label>] [--reason <text>] [--accept-data-loss] [--dry-run]` | Remove a permanently dead (Down/Failed) node: drop its rows that have another live replica, report `anchor_pending` and data-loss candidates, deregister when no rows remain. `--accept-data-loss` also drops rows with no live replica. Prints the server JSON, then polls the run until it finishes and prints the final JSON; exits `1` on failure. |
| `scrub [--level quick\|deep]` | Trigger a cluster-wide scrub (default `deep`). The `--disk`, `--file` and `--chunk` options exist but the server rejects per-target scrubs with `400`; omit them. |
| `scrub orphans [--grace-secs N] [--dry-run] [--wait]` | `POST /api/v1/scrub/orphans`: run the orphan reaper once now on the leader. `--grace-secs` defaults to the configured `scrub.orphan_grace_secs`, minimum 3600. `--wait` polls until the run ends, prints the result and exits `1` when the run reports an error. `409` while another reap runs. |
| `scrub orphans-status` | `GET /api/v1/scrub/orphans`: last (or running) reap on this manager with per-node counts. |
| `audit [--limit N] [--action <prefix>]` | Audit log, default limit 20. |
| `integrity faults [--limit N] [--action <prefix>]` | Integrity-fault feed, default limit 20. |

### raft

Membership changes go to the leader's address (`--manager <leader>:9402`). A non-leader answers `409` with a leader hint, printed as JSON. The mutating subcommands require the cluster secret in `--secret` (env `DTFS_SHARED_SECRET`); it is sent both as the header and in the request body. They print the server JSON and exit `1` on a non-success status. See [Cluster Management](../operations/cluster-management.md).

| Command | Description |
|---|---|
| `raft members` | Voters, learners and leader as seen by the queried manager. |
| `raft add-learner <hostname> <addr> --secret <s>` | Add a manager as a non-voting learner. `addr` is its peer-routable gRPC `host:port` (never a wildcard); `hostname` is its `[node].hostname`. |
| `raft promote <hostname> --secret <s>` | Promote a caught-up learner to voter. |
| `raft remove <hostname> --secret <s>` | Remove a learner or voter. Refuses to remove the last voter. |

## Volumes

Volume ids are lowercase names with spaces turned into `-`. Field meanings: [Volume Configuration](../configuration/volume-config.md). For all `set-*` commands only the flags you pass change; the response tells whether anything changed (`applied` or `no-op`). `409` means a concurrent update to the same volume won: run `volumes show <id>`, reapply, retry.

| Command | Description |
|---|---|
| `volumes list` | All volumes with file count and size. |
| `volumes show <id>` | Full policy: replication, mandatory, cache, anchor, write safety, spill. |
| `volumes create <name> [--replication-factor N] [--tier-policy T]` | Create a volume. Both options are accepted by the CLI but **ignored by the current server**; the volume takes `[defaults]`. Use `POST /api/v1/volumes` for `chunk_size_mb`, `replication_*` and `anchor_datacenter`. Builds that add `--force` (anchor datacenter unknown to the cluster) list it in `volumes create --help`. |
| `volumes delete <id> [--yes] [--confirm <id>]` | Delete a volume. Reads `volumes show` counts first. Empty volume: asks `Delete volume X? [y/N]` (`--yes` skips it). Volume with files or directory markers: asks `Volume X holds N files (B bytes). Delete the volume AND all its files? [y/N]` (`--yes` skips it), then `Type the volume id to confirm:` which must equal the id exactly (`--confirm <id>` replaces it). A `--confirm` that does not match the id aborts. Without terminal input a missing confirmation aborts. Every abort exits `1` and deletes nothing. Files are deleted like an unlink; the orphan reaper reclaims their bytes after `orphan_grace_secs`. `default` cannot be deleted; a volume with a file open for writing is refused. |
| `volumes migrate-policy <id>` | Copy legacy `replication` into `mandatory_replicas` once. Idempotent. |
| `volumes set-cache <id> [--warm-min N] [--warm-max N] [--hot-min N] [--hot-max N] [--per-dc true\|false] [--promote-reads N] [--promote-window-secs N] [--cooldown-secs N]` | Cache replica policy. `max` must be at least `min`; `0`/`0` disables a tier. Cold and tape cache tiers are REST only. |
| `volumes set-precache <id> [--mode off\|files\|plex] [--next-files N] [--next-max-gb N] [--series-max-gb N] [--max-series N]` | Pre-caching policy (default `off`, `next_files` 2, `next_max_gb` 0 = no cap, `series_max_gb` 20, `max_series` 3). Needs a cache tier and `per_dc`; refused with 409 until every manager runs v0.1.16. |
| `volumes set-mandatory <id> [--hot-replicas N] [--warm-replicas N] [--cold-replicas N] [--tape-replicas N]` | Mandatory replica counts. Rejects an all-zero result, a zero on the anchor's required tier, and tape without a tape disk. |
| `volumes set-write-safety <id> [--min-replicas-before-ack N] [--write-timeout-secs N]` | Write acknowledgement threshold and timeout (each at least 1). |
| `volumes set-anchor <id> [--datacenter <dc>] [--required-tier hot\|warm\|cold\|tape] [--force]` | Anchor datacenter and tier. An empty `--datacenter ""` clears the anchor. A datacenter no registered node reports is refused unless `--force`. `--force` needs a CLI build that includes it (see `volumes set-anchor --help`); the server field is `force`. |
| `volumes set-spill <id> [--allow true\|false] [--forbidden-tiers a,b] [--forbidden-datacenters x,y]` | Per-volume capacity-spill rules. Comma-separated lists; an empty list clears the exclusion. |

## Files

| Command | Description |
|---|---|
| `files list [--limit N]` | List files, default 50. The CLI pages through `GET /api/v1/files` (`limit`/`offset`) until `N` files are collected or the list ends, and prints `showing N of M (raise --limit to see more)` when the list is cut short. With `--format json` the output is `{count, total, truncated, files}`. Replaces the old `files --limit`. |
| `files inspect <path>` | Chunk layout and per-replica placement (node, disk, tier) of one file. |
| `files flush-cache <path-prefix> [--cache-tiers warm,hot] [--cooldown-secs N] [--max-tasks N] [--allow-empty-prefix]` | Drop cache replicas under a prefix; mandatory and anchor replicas are never dropped. Defaults: tiers `warm`, cooldown 300, max tasks 10000. A prefix of `""` or `/` needs `--allow-empty-prefix`. **Exits `1` when `rpc_failures` or `raft_failures` is non-zero** (output is still printed) and prints `truncated` when more remains; re-run to continue. `mandatory_refusals > 0` is reported but is not an error exit; check the audit log (`cache.flush_refused`). |

## Migrations

`migrations create <path-prefix> --target-tier <tier> [--target-dc <dc>] [--allow-empty-prefix] [--max-tasks N]` enqueues moves of every chunk under the prefix. `--target-tier` is one of `hot`, `warm`, `cold`, `tape` and is checked locally before any request. Prints the server's counters (`files_matched`, `tasks_scheduled`, `already_satisfied`, `no_source`, `no_target`). Leader-only. See [Migration](../storage/migration.md).

## Config

| Command | Description |
|---|---|
| `config` (or `config --show`) | Stored override plus effective `[defaults]` and `[tiering]`. |
| `config effective` | Every key with `current`, `default`, `is_default`, scope. Secrets show `***`. The default column is indicative only. |
| `config set --config-json '<json>' [--write-to-disk --acknowledge-cluster-wide]` | Store a sparse override. `--write-to-disk` rewrites `/etc/dtfs/manager.toml` on the manager you contacted and requires `--acknowledge-cluster-wide`. Prints the server JSON; exits `1` on failure. No daemon applies the stored override at runtime; see [Cluster-wide Configuration](../configuration/cluster-config.md). |

## update

```
dtfs-cli update <target> [--check] [--tag <tag>] [--repo-url <url>] [--install-dir <dir>]
                [--config <file>] [--token <token>] [--minisign-key <key>] [--no-restart] [--force] [-y|--yes]
```

Updates binaries on **this host** (run as root). The rolling cluster-wide update is the pipeline playbook, see [CI Pipeline](../operations/ci-pipeline.md).

| Argument | Meaning |
|---|---|
| `<target>` | `manager`, `data`, `fuse` (alias `client`), `cli` or `all`. Binaries and services: `dtfs-manager` / `dtfs-manager.service`, `dtfs-data` / `dtfs-data.service`, `dtfs-fuse` / `dtfs-fuse@*` (all running instances), `dtfs-cli` (no service). |
| `--check` | Print the latest release and installed versions; change nothing. |
| `--tag <tag>` | Install this release. Default: GitHub's latest release (prereleases are skipped unless named). |
| `--repo-url <url>` | Release source. Precedence: flag, `[updater].repo_url`, `DTFS_REPO_URL`, built-in `https://github.com/lischetzke/dtfs`. Must be a github.com repo; anything else is an error. |
| `--install-dir <dir>` | Default `[updater].install_dir`, else `/usr/local/bin`. |
| `--config <file>` | File to read `[updater]` from. Default `/etc/dtfs/<role>.toml` (`manager`, `data`, `fuse`); none for `cli` and `all`. |
| `--token <token>` | Optional GitHub token (rate limits or a private fork). Precedence: flag, `DTFS_REPO_TOKEN`, `[updater].token`. Sent as `Authorization: Bearer` on the first request only. |
| `--minisign-key <key>` | Minisign public key (base64 line or `.pub` text) used to verify `SHA256SUMS`. Precedence: flag, `DTFS_MINISIGN_PUBKEY`, `[updater].minisign_public_key`. Blank values are skipped. |
| `--no-restart` | Install but leave services on the old binary until their next restart. |
| `--force` | Reinstall and restart a binary that is already at the release version. |
| `-y`, `--yes` | Skip the `Proceed? [y/N]` prompt and the no-minisign-key prompt (step 3) (otherwise the command prompts; with no terminal input it prints `aborted; nothing installed` and exits `1`, so always pass `-y` in automation). |

Sequence:

1. Resolve the tarball (`dtfs-linux-amd64.tar.gz`) from the GitHub release (`.../releases/download/<tag>/` or `releases/latest/download/`).
2. Download to a private temporary directory.
3. **Minisign verification** when a public key is configured (`--minisign-key`, `DTFS_MINISIGN_PUBKEY` or `[updater].minisign_public_key`, in that order): also download `SHA256SUMS` and `SHA256SUMS.minisig` from the same location, verify the signature, and require the tarball's SHA-256 to match the signed `SHA256SUMS`. Any failure aborts before extraction. Without a key a warning is printed and only the in-tarball `MANIFEST` is checked. Without a key and without `-y`, the command also asks before running the staged binaries' `--version` (step 5); declining aborts with exit `1` (`aborted before running unverified binaries; nothing installed`).
4. Extract and verify every target binary against the tarball `MANIFEST` SHA-256; one mismatch aborts with nothing installed.
5. Run each staged binary's `--version`. A binary whose version cannot be read aborts the update. With `--tag T` every binary must report `T` (leading `v` optional); without `--tag` a binary older than the installed one is refused as a downgrade (use `--tag` to downgrade). `--force` bypasses neither check. This binds the signed asset set to a version, since `SHA256SUMS` itself names none. A binary whose installed version token is the same (`v0.1.14` = `v0.1.14`, build date ignored) is skipped, unless `--force`. When every target is skipped the command prints `up to date` and exits `0` without installing or restarting anything.
6. Print the plan and ask for confirmation.
7. Install each binary atomically (temporary file in the target directory, then rename), then `systemctl restart` the matching units unless `--no-restart`. A restart counts as successful only when the unit is `active` right away and still `active` 10 s later with an unchanged `NRestarts` (the units use `RestartSec=5`), so a crash-looping daemon exits `1`.

Token handling: the token header is attached only to the first request of each download. HTTP redirects are followed by the CLI itself (at most 5 hops, `https` only after the first hop, TLS 1.2 or newer), and redirect targets never receive the token, so a registry that redirects to object storage does not leak it. The header is passed to `curl` on its stdin (`-H @-`), never on the command line, so other local users cannot read the token from the process list (curl 7.55 or newer). The installer (`scripts/dtfs-install.sh`) does the same, and its secret prompt is a password box that never shows the secret in the process list (blank keeps the existing or generated secret; read it from `/etc/dtfs/<role>.toml`). Exit `1` on any failure.

Examples:

```bash
dtfs-cli update all --check
sudo dtfs-cli update data --tag v1.4.2 -y
sudo DTFS_MINISIGN_PUBKEY="RWQ..." dtfs-cli update manager -y
```

## Common tasks

```bash
export DTFS_MANAGER=mgr-01:9402 DTFS_SHARED_SECRET=...
dtfs-cli status
dtfs-cli files list --limit 200 --format json
dtfs-cli files flush-cache /movies/ --cache-tiers warm --cooldown-secs 600
dtfs-cli volumes set-mandatory media --cold-replicas 1
dtfs-cli maintenance 1234567890 --enable
dtfs-cli --manager mgr-02:9402 raft add-learner mgr04 10.1.0.1:9401 --secret "$DTFS_SHARED_SECRET"
```
