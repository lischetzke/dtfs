# Role Variables and Idempotency

Names starting with `dtfs_` that the repository playbook already reads keep their
meaning (`deploy/ansible/update-cluster.yml`); the rest are suggestions for a
role that also installs and configures. Rename freely, keep the semantics.

## Inventory shape

Groups: `managers`, `data_nodes`, `clients` (names are variables in the
playbook: `dtfs_group_managers`, `dtfs_group_data_nodes`, `dtfs_group_clients`).
A composite host is a member of several groups. Inventory host names must equal
the daemon identity (`node.hostname`, or `location.host` for clients), or set
`dtfs_hostname` per host. Connection: SSH as `root`, no become
(`deploy/ansible/ansible.cfg`).

## Global variables

| Variable | Default | Required | Meaning |
|---|---|---|---|
| `dtfs_version` | none | yes | Exact tag to install, for example `v0.2.0` |
| `dtfs_tarball` / `dtfs_tarball_url` | none | one of | Local path on the control host, or registry URL |
| `dtfs_repo_token` | empty | for a private project | GitLab deploy (`gldt-`) or access token; use `no_log` |
| `dtfs_minisign_pubkey` | empty | recommended | Public key (base64, or an absolute `.pub` path); when set, signature verification of `SHA256SUMS` is mandatory and `minisign` must exist on the control host |
| `dtfs_verify_checksum` | true | no | Require the tarball sha256 in `SHA256SUMS` (also checks `dtfs/MANIFEST`); `update-cluster.yml` fails the play on mismatch |
| `dtfs_sums` / `dtfs_sums_url` / `dtfs_minisig` | beside the tarball | no | Override where `SHA256SUMS` and `SHA256SUMS.minisig` come from |
| `dtfs_refresh_units` | true | no | `update-cluster.yml`: install the tarball's unit file on each updated node (daemon-reload with the restart) |
| `dtfs_enable_raft_auth` | false | no | `update-cluster.yml`: last play sets `[cluster] require_raft_auth = true` on every manager, rolling, after all are on `dtfs_version` |
| `dtfs_manager_config` | `/etc/dtfs/manager.toml` | no | Manager config edited by that play |
| `dtfs_bin_dir` | `/usr/local/bin` | no | Binary directory (per-host override allowed) |
| `dtfs_restart_delay_secs` | 30 | no | Settle pause after each updated node |
| `dtfs_health_timeout_secs` | 300 | no | Per-node and final health budget |
| `dtfs_force_update` | false | no | Reinstall and restart even if already on `dtfs_version` |
| `dtfs_run_healthcheck` | true | no | Run `scripts/dtfs-cluster-healthcheck.sh` at the end |
| `dtfs_api_port` | 9402 | no | Manager REST port (per-host override allowed) |
| `dtfs_uid` / `dtfs_gid` | 980 / 980 | no | Fixed ids for the `dtfs` account |
| `dtfs_cluster_name` | none | yes | `cluster.name` on managers |
| `dtfs_shared_secret` | none (Vault) | yes | Same value for all roles |
| `dtfs_require_raft_auth` | false | no | Flip to true only in a second pass, see [Rolling Update](./rolling-update.md#flag-flips-after-an-update) |
| `dtfs_initial_voters` | none | yes | List of `{hostname, addr: "ip:9401"}` seeded at bootstrap |
| `dtfs_managers` | derived | yes | Every manager `host:9401`, used for data and client `manager_addrs` (must include a voter) |
| `dtfs_manager_api_urls` | derived | yes | Comma list `host:9402` of all managers for the health gate |
| `dtfs_chunk_size_mb` | 4 | no | Must be the same in `defaults.chunk_size_mb` and every client `chunk_size_mb` |
| `dtfs_anchor_datacenter` | none | yes in the reference deployment | `defaults.anchor_datacenter` at bootstrap |
| `dtfs_extra_config` | `{}` | no | Per-role mapping merged into the templates for optional keys (`[memory]`, `[scrub]`, `[tiering]`...) |
| `dtfs_purge` | false | no | Allow removing `/etc/dtfs` and `/var/lib/dtfs` when a role is set absent |
| `dtfs_manage_units` | true | no | Install unit files and drop-ins from the tarball and the variables below |

## Per-host variables

| Variable | Roles | Meaning |
|---|---|---|
| `ansible_host` | all | Address to SSH to |
| `dtfs_hostname` | all | Registered identity; default the inventory name |
| `dtfs_bootstrap` | manager | True on exactly one manager, and only until the first successful gate |
| `dtfs_manager_role` | manager | `voter` or `learner` end state after join |
| `dtfs_location` | all | `{region, datacenter, rack, host}`; clients use `host` as identity |
| `dtfs_disks` | data | Ordered list of `{path, tier}`; append-only (see [Decommission and Replace](./decommission-replace.md)) |
| `dtfs_grpc_listen` | data | Routable `ip:9500` |
| `dtfs_mount_point` | client | Default `/mnt/dtfs`; also drives the unit instance name |
| `dtfs_allow_other` | client | `[mount] allow_other` |
| `dtfs_cache_dir`, `dtfs_cache_max_gb` | client | FUSE local cache |
| `dtfs_memory_mb` | all | Sets `[memory] hard_cap_mb`; derive `MemoryHigh`/`MemoryMax` drop-in values from it |
| `dtfs_composite` | all | True on shared hosts; makes memory drop-ins mandatory |

## Idempotency checklist

A second run with unchanged inputs must report no change and restart nothing.

- [ ] Downloads: skip when the verified tarball with the same sha256 is cached on the control host; the temp unpack directory is removed at the end.
- [ ] Binaries: replaced only when the installed version string differs (or `dtfs_force_update`); `.prev` is written only on that replacement, never when already current.
- [ ] Restart: only after a changed binary, unit file, drop-in or config (handlers), never unconditionally; one node at a time with the health gate.
- [ ] Skip path: node on the target version and unit `active` means no restart, no settle delay.
- [ ] Config files: templated with stable key and list order (disks, voters), validated before replacement, replaced atomically (`validate:` or write to temp then move), `no_log` for the secret; unchanged output means unchanged file (no timestamp comments).
- [ ] `bootstrap`: rendered `true` only on the one bootstrap manager until the first successful cluster gate, then `false`. Never derived from "first host in the group" on every run.
- [ ] Membership: `add-learner` runs only if the hostname is absent from `raft/state.membership`; `promote` only if it is a learner and `applied_log_lag == 0`; both ignore "already a member" as success; neither runs unless this host is not the bootstrap manager.
- [ ] Users/dirs/modes: the declarative modules converge (`user`, `file`), no `chown -R` on data disks.
- [ ] Data disks: never formatted if a filesystem is present; never created, moved or cleaned inside a disk; `[[disks]]` is rejected if it is not an extension of the last rendered list; the mount is verified (`findmnt`) before the unit starts.
- [ ] Units: `daemon-reload` only when a unit or drop-in file changed; `enable` is idempotent; the FUSE instance name comes from `systemd-escape --path`.
- [ ] Maintenance mode: enabled and disabled in a `block/always` pair so a failed play does not leave a node in maintenance forever.
- [ ] Secrets: generated once and stored in Vault, never regenerated by a run.
- [ ] Flag flips (`require_raft_auth`, `audit.replicate_via_raft`, TLS) are separate plays gated on every manager reporting the target version.
- [ ] Check mode (`--check`) must not call mutating REST endpoints or restart anything.
- [ ] Final assertion every run: the cluster gate from [Rolling Update](./rolling-update.md#per-node-procedure) passes.

## Tags / plays suggested

| Play | Touches | Serial |
|---|---|---|
| `prepare` | users, packages, directories, units, drop-ins, configs (fresh install: nothing is running yet; on a live cluster config changes restart through the ordered plays instead) | all hosts in parallel |
| `bootstrap` | start first manager, assert | one |
| `managers` | start/join/promote, update | one at a time |
| `data` | start, join, update | one at a time |
| `clients` | mount, update | one at a time |
| `flags` | second-pass config flips | one at a time, managers only |
| `verify` | cluster gate, optional smoke write | one |
