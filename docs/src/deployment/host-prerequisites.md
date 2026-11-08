# Host Prerequisites

Sources: `scripts/dtfs-install.sh`, `deploy/systemd/*.service`,
`deploy/examples/*.toml`, `docs/src/operations/time-sync.md`,
`crates/dtfs-common/src/crypto.rs`.

## Operating system and runtime libraries

- Linux x86_64 with systemd and cgroup v2 (the units use `MemoryHigh`/`MemoryMax`
  drop-ins). The release binaries are built on Debian 12 (glibc 2.36), so the
  host needs glibc 2.36 or newer: Debian 12 or later, Ubuntu 24.04 or later.
  Ubuntu 22.04 (glibc 2.35) is not covered.
- `dtfs-data` links only `libc`, `libm`, `libgcc_s` (measured with `ldd` on a
  bookworm build). No OpenSSL, RocksDB or protobuf packages are needed at
  runtime on a data node. Before the first rollout on a new distro run
  `ldd /usr/local/bin/dtfs-<role>` and fail the play on any `not found`; this
  was not measured for the manager, fuse and cli binaries here.
- All roles: `ca-certificates`, `curl` (only if the host itself downloads),
  a time-sync daemon (below).
- Control host (where Ansible runs): `ansible-core`, `curl`, `jq`, `tar`,
  `sha256sum`, `minisign` (if signatures are required), and for the health
  gate `bash` plus `scripts/dtfs-cluster-healthcheck.sh` from the repository.
- Client role only: package `fuse3` (provides `/usr/bin/fusermount3`; the unit
  hard-codes that path) and the `fuse` kernel module with `/dev/fuse`.

## Users and groups

| Account | Used by | Spec |
|---|---|---|
| `dtfs` | `dtfs-manager`, `dtfs-data` | system user and group, uid 980 and gid 980, home `/var/lib/dtfs`, shell `/usr/sbin/nologin` (as `dtfs-install.sh` creates it) |
| `root` | `dtfs-fuse@.service` | the unit runs as root and holds `CAP_SYS_ADMIN` for the mount |

Pin uid/gid 980 only if it is free on every host; otherwise let the system pick,
because the daemons never encode a numeric id. What matters is that the data
disks, `/var/lib/dtfs/*` and the config files are owned by whatever `dtfs`
resolves to on that host.

## Kernel and FUSE settings (client role)

- `/etc/fuse.conf` must contain `user_allow_other` when `[mount] allow_other =
  true` (the installer appends the line if missing). Without it the mount fails
  for non-root readers such as the Plex user.
- The mount point directory must exist; the unit does not create it. The unit
  clears a stale mount with `ExecStartPre=-/usr/bin/fusermount3 -u /%I`.
- `LimitNOFILE=65535` is set in all three units; no sysctl changes are required.
- The unit name encodes the mount path: `dtfs-fuse@$(systemd-escape --path
  /mnt/dtfs).service` (for `/mnt/dtfs` this is `dtfs-fuse@mnt-dtfs.service`).

## Time synchronisation

Required on every node of every role (chrony or equivalent, see
[Time Synchronisation](../operations/time-sync.md)). Besides audit and tiering
timestamps, every HMAC-stamped RPC (all mutating data-plane and manager RPCs,
Raft RPCs when `require_raft_auth = true`) is rejected when the clocks differ by
more than 300 seconds. A role should assert `chronyc tracking` reports
`Leap status : Normal` and an offset well under that before starting a daemon,
and treat a skew above a few seconds as a failed prerequisite.

## Network ports

Open exactly these; direction is from the connecting side. Nothing needs
inbound access on a client host.

| Port | Proto | Listener | Allowed sources | Purpose |
|---|---|---|---|---|
| 9401 | TCP | manager `[grpc]` | all managers, all data nodes, all clients | Metadata gRPC, registration, heartbeats, and the Raft transport (Raft RPCs ride this port) |
| 9402 | TCP | manager `[api]` | operators, the automation control host, the CI runner, monitoring, other managers | REST API, WebUI, `/healthz`, `/readyz`, `/metrics`, membership endpoints |
| 9500 | TCP | data `[grpc]` | all clients, all managers, all other data nodes | Chunk streaming, replication, deletes |

Notes that prevent wrong firewall rules:

- `[raft] listen_addr` (default `0.0.0.0:9400`) is accepted by the config parser
  but the manager does not bind it. Do not open or monitor 9400.
- Manager-to-manager traffic on 9401 must work in both directions, including
  across datacenters; so must data-node-to-data-node on 9500 (push replication)
  and client-to-data on 9500.
- The addresses a node advertises must be routable by its peers:
  `[[cluster.initial_voters]] addr`, `[cluster] manager_addr(s)` and a data
  node's `[grpc] listen_addr` when the host has several interfaces. Never a
  wildcard in an address other nodes dial.
- Tailscale or another overlay works; use the overlay addresses everywhere.
- The deploy control host additionally needs SSH (22) as `root` to every node
  (`deploy/ansible/ansible.cfg` uses `remote_user = root`, no become).

## Disks (data role)

- One filesystem per `[[disks]]` entry, mounted before `dtfs-data` starts. XFS
  with `noatime,inode64` is the documented choice for production; any POSIX
  filesystem with atomic `rename` and working `fsync` functions.
- Mount through `/etc/fstab` or a `.mount` unit with a stable identifier
  (`UUID=` or `LABEL=`), never `/dev/sdX`, so device renames cannot swap two
  disks between `[[disks]]` positions (see below).
- Mount root owned by `dtfs:dtfs`, mode `0755` or tighter but writable by
  `dtfs`. The daemon creates `chunks/`, `tmp/`, `.dtfs/` (holding `wal/` and
  `quarantine/`) and the two identity files itself.
- Mount options must not make the filesystem read-only; the unit uses
  `ProtectSystem=strict`, so every disk path must also be listed in a
  `ReadWritePaths=` drop-in (see
  [Filesystem Layout](./filesystem-layout.md)).
- Do not put a disk mount point inside `/var/lib/dtfs/data` (that is
  `data_dir`, a different directory with its own role).
- A mount that is missing at start leaves an empty directory on the root
  filesystem: the daemon would then initialise a fresh disk there. Gate the unit
  with `RequiresMountsFor=<disk paths>` in the same drop-in as
  `ReadWritePaths=` so a missing mount fails the start instead.
- Disk identity files at the disk root, written by the daemon on first start:

  | File | Content | Rule |
  |---|---|---|
  | `.dtfs-disk-id` | the 0-based position of the disk in `[[disks]]` | On a later start, a different position makes `dtfs-data` refuse to start. |
  | `.dtfs-disk-uuid` | a UUID | Stable identity of the disk; a garbled file is regenerated. |

  Both survive reboots and must stay with the disk. Never copy them to another
  disk, never edit them. A role that re-images a data node must restore the same
  disk contents or consciously treat the disk as new (see
  [Decommission and Replace](./decommission-replace.md)).
- Capacity headroom: the placement engine keeps `[placement] min_free_bytes`
  (default 1 GiB) and `min_free_pct` (default 5) free per disk.

## Composite hosts and memory

On a host that runs several DTFS roles or an application (for example Plex),
every daemon needs its own `[memory] hard_cap_mb` and a matching systemd
`MemoryHigh`/`MemoryMax` drop-in; the sum must stay below host RAM minus the
application and OS headroom. Values and the drop-in are in
[Filesystem Layout and Units](./filesystem-layout.md#memory-limits-on-co-hosted-boxes).
