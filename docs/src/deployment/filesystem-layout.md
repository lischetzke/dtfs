# Filesystem Layout and Units

Sources: `scripts/dtfs-install.sh`, `deploy/systemd/*.service`,
`deploy/ansible/tasks/update-node.yml`, `crates/dtfs-manager/src/main.rs`,
`crates/dtfs-data/src/{store,disk}.rs`.

## Paths, owners, modes

| Path | Owner:group | Mode | Role | Notes |
|---|---|---|---|---|
| `/usr/local/bin/dtfs-manager` | root:root | 0755 | manager | |
| `/usr/local/bin/dtfs-data` | root:root | 0755 | data | |
| `/usr/local/bin/dtfs-fuse` | root:root | 0755 | client | |
| `/usr/local/bin/dtfs-cli` | root:root | 0755 | all | the rolling update replaces it on every node |
| `/usr/local/bin/<binary>.prev` | root:root | 0755 | all | one rollback copy, see [Rolling Update](./rolling-update.md) |
| `/etc/dtfs/` | dtfs:dtfs | 0750 | all | shared by all roles on the host |
| `/etc/dtfs/manager.toml` | dtfs:dtfs | 0640 | manager | holds the shared secret |
| `/etc/dtfs/data.toml` | dtfs:dtfs | 0640 | data | holds the shared secret |
| `/etc/dtfs/fuse.toml` | root:root | 0640 | client | holds the shared secret; the unit runs as root |
| `/etc/dtfs/tls/*.crt, *.key` | dtfs:dtfs | 0640 | all, only if TLS | the client role reads them as root |
| `/var/lib/dtfs/` | dtfs:dtfs | 0750 | manager, data | |
| `/var/lib/dtfs/manager/` | dtfs:dtfs | 0750 | manager | `[node] data_dir`; holds `raft-log/` (RocksDB), `raft-sm/` (snapshot), `audit/` |
| `/var/lib/dtfs/data/` | dtfs:dtfs | 0750 | data | `[node] data_dir` |
| `/var/log/dtfs/` | dtfs:dtfs | 0750 | optional | created by the installer; the units log to journald, so nothing writes here by default |
| `/var/cache/dtfs/` | root:root | 0700 | client, optional | FUSE `[cache] local_cache_dir` |
| `<mount>` of each `[[disks]]` entry | dtfs:dtfs | 0755 | data | one filesystem per entry, see below |
| `<fuse mountpoint>` (default `/mnt/dtfs`) | root:root | 0755 | client | must exist before the unit starts |
| `/etc/systemd/system/dtfs-manager.service` | root:root | 0644 | manager | from the tarball |
| `/etc/systemd/system/dtfs-data.service` | root:root | 0644 | data | from the tarball |
| `/etc/systemd/system/dtfs-fuse@.service` | root:root | 0644 | client | from the tarball |
| `/etc/systemd/system/<unit>.d/*.conf` | root:root | 0644 | per need | drop-ins below |

Data_dir must match the unit. The manager and data units run with
`ProtectSystem=strict` and a single writable path
(`ReadWritePaths=/var/lib/dtfs/manager` respectively `/var/lib/dtfs/data`).
`[node] data_dir` therefore has to be exactly that directory (the installer
renders it so). The compiled-in default and the example files use
`/var/lib/dtfs`; with the stock unit that path is read-only and the Raft log
cannot be opened. Either set `data_dir` as above or extend `ReadWritePaths=` in
a drop-in.

Create `/var/lib/dtfs/manager` or `/var/lib/dtfs/data` before the first start;
systemd fails the unit with status 226/NAMESPACE when a `ReadWritePaths=` path
does not exist.

### What the daemons create themselves

Manager `data_dir`: `raft-log/`, `raft-sm/snapshot.bin`, `audit/audit-YYYYMMDD.jsonl`.

Per data disk (`<mount>`):

```text
<mount>/chunks/<2 hex>/<2 hex>/<chunk id>.dat and .meta   # content, written tmp + fsync + rename
<mount>/tmp/                                              # in-flight streaming puts
<mount>/.dtfs/wal/                                        # write-ahead markers
<mount>/.dtfs/quarantine/<chunk id>.dat.bad               # integrity faults, renamed never deleted
<mount>/.dtfs-disk-id                                     # position in [[disks]], 0-based
<mount>/.dtfs-disk-uuid                                   # stable disk UUID
```

A role must not create, move or clean any of these. Backups of a manager are
`raft-log/` plus `raft-sm/` ([Backup and Recovery](../operations/backup-recovery.md)).

## The three units

All three come from the release tarball (`dtfs/systemd/`). A role should render
or copy them from the same release as the binaries and run
`systemctl daemon-reload` when a unit file changed. The rolling-update playbook
does not copy unit files (it only restarts with `daemon_reload: true`), so a
release that changes a unit needs the role to install it.

| Unit | ExecStart | User | Stop timeout | Hardening |
|---|---|---|---|---|
| `dtfs-manager.service` | `/usr/local/bin/dtfs-manager --config /etc/dtfs/manager.toml` | dtfs | 30 s | `NoNewPrivileges`, `ProtectSystem=strict`, `ProtectHome`, `PrivateTmp`, `ReadWritePaths=/var/lib/dtfs/manager` |
| `dtfs-data.service` | `/usr/local/bin/dtfs-data --config /etc/dtfs/data.toml` | dtfs | 45 s | same, `ReadWritePaths=/var/lib/dtfs/data`; disks must be added by drop-in |
| `dtfs-fuse@.service` | `/usr/local/bin/dtfs-fuse --config /etc/dtfs/fuse.toml --mountpoint /%I` | root | 45 s | `AmbientCapabilities=CAP_SYS_ADMIN`, `MountFlags=shared`, no `PrivateTmp` (it would hide the mount) |

Common: `Restart=on-failure`, `RestartSec=5`, `LimitNOFILE=65535`,
`After=network-online.target`, `WantedBy=multi-user.target`, journald rate limit
(`LogRateLimitIntervalSec=10`, `LogRateLimitBurst=2000`). `RUST_LOG` defaults to
`dtfs=info` plus quiet `tonic`/`h2`/`openraft`.

Instance naming for the client: `dtfs-fuse@<systemd-escape --path MOUNT>.service`.
The stock unit reads the fixed config path `/etc/dtfs/fuse.toml`, so one host
runs one FUSE mount unless the role adds a per-instance drop-in that changes
`--config`.

Enable and start with `systemctl enable --now <unit>`. Stop time matters for
automation: `systemctl stop` sends SIGTERM and waits up to the stop timeout (the
data node drains in-flight writes for up to 30 s, the manager takes a final
Raft snapshot). Use a task timeout above these values.

## Drop-ins

Never edit the unit files from the tarball; put local settings in drop-ins so
that unit updates stay clean.

### Disks for the data unit (required)

`/etc/systemd/system/dtfs-data.service.d/disks.conf`, regenerated from the
`[[disks]]` list on every run, with every path listed:

```ini
[Unit]
RequiresMountsFor=/mnt/disk-a /mnt/disk-b

[Service]
ReadWritePaths=/mnt/disk-a /mnt/disk-b
```

`ReadWritePaths=` is required because of `ProtectSystem=strict`. The installer
writes only that line; `RequiresMountsFor=` is the recommended addition so a
disk that failed to mount stops the start instead of letting the daemon
initialise an empty directory on the root filesystem as a new disk.

### Memory limits on co-hosted boxes

Optional on a dedicated host, required on a host shared with Plex or another
DTFS role. Files:
`/etc/systemd/system/dtfs-manager.service.d/memory.conf`,
`/etc/systemd/system/dtfs-data.service.d/memory.conf`,
`/etc/systemd/system/dtfs-fuse@.service.d/memory.conf`.

| Role | `MemoryHigh` | `MemoryMax` | Matching `[memory] hard_cap_mb` in the TOML | Source of the numbers |
|---|---|---|---|---|
| manager | 400M | 512M | at most 400 | unit comment: "a 512 MB cap is typical" |
| data | 1800M | 2048M | at most 1800 | unit comment: example for a 2 GB data role |
| client | 800M | 1024M | at most 800 | unit comment: 1 GB is a sane Plex client default |

```ini
[Service]
MemoryHigh=1800M
MemoryMax=2048M
```

`hard_cap_mb` is the daemon's own budget (shed caches, pause background work,
optionally reject writes); `MemoryHigh`/`MemoryMax` are the kernel backstop. Keep
`hard_cap_mb` at or below `MemoryHigh` so the daemon reacts first. The sum of all
`MemoryMax` values on the host plus the application and OS headroom must stay
under host RAM. `hard_cap_mb` below 128 is refused at startup (0 means
unbounded).

### Environment

`Environment=RUST_LOG=dtfs=debug,...` in a drop-in overrides the unit default for
a debugging session; remove it afterwards.

## Uninstall boundaries

Removing a node is not the inverse of installing it: config and data stay unless
the operator purges them (`dtfs-install.sh --uninstall --purge --yes` deletes
`/etc/dtfs` and `/var/lib/dtfs`, never data disks). A role's "absent" state
should stop and disable the units, remove binaries and units, and leave
`/etc/dtfs`, `/var/lib/dtfs` and every data disk alone unless an explicit
`dtfs_purge` style variable is set.
