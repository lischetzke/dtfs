# Deployment Automation: Overview

This section is a specification. An Ansible role (or any other automation)
that follows it can install a DTFS cluster from release artifacts and keep it
updated, without reading the Rust sources. Every fact here was checked against
the repository files named at the top of each page.

It does not replace the reference material it links to:
[Configuration](../configuration/manager.md) for every key,
[Cluster Management](../operations/cluster-management.md) for day-2 operations,
[CI Pipeline](../operations/ci-pipeline.md) for how releases are produced.

## Reading order

| Page | Answers |
|---|---|
| [Release Artifacts](./artifacts.md) | What do I download, from where, and how do I verify it? |
| [Host Prerequisites](./host-prerequisites.md) | Packages, users, kernel/FUSE, time sync, ports, disks per role. |
| [Filesystem Layout and Units](./filesystem-layout.md) | Which file goes where with which owner/mode; systemd units and drop-ins. |
| [Configuration Templating](./config-templating.md) | Required vs optional keys per role, cluster-wide vs per-node values, secrets, startup validation. |
| [Fresh Install Order](./fresh-install.md) | Bootstrap, add managers, join data nodes, mount clients, with the check after each step. |
| [Rolling Update and Rollback](./rolling-update.md) | Update order, per-node steps, health gates, rollback, yank, flag flips. |
| [Decommission and Replace](./decommission-replace.md) | Replacing a disk or a node without losing the anchor copy. |
| [Role Variables and Idempotency](./role-variables.md) | Variable table a role should expose and the idempotency checklist. |

## Components and roles

A host runs any combination of three roles. The tarball always contains all
four binaries; a role installs only the ones it needs.

| Role | Binaries installed | systemd unit | Config | Listens on |
|---|---|---|---|---|
| manager | `dtfs-manager`, `dtfs-cli` | `dtfs-manager.service` | `/etc/dtfs/manager.toml` | 9401 (gRPC incl. Raft), 9402 (REST, WebUI, `/metrics`) |
| data | `dtfs-data`, `dtfs-cli` | `dtfs-data.service` | `/etc/dtfs/data.toml` | 9500 (chunk gRPC) |
| client (FUSE) | `dtfs-fuse`, `dtfs-cli` | `dtfs-fuse@<escaped-mountpoint>.service` | `/etc/dtfs/fuse.toml` | none |

`dtfs-cli` is installed with every role (the rolling update replaces it on every
node and checks its version stamp). All daemons share one `/etc/dtfs`
directory, so a composite host (for example manager + data + client on one
box) has up to three config files side by side. Each daemon on a composite
host needs its own unique identity string, see
[Configuration Templating](./config-templating.md).

## Reference topology

The deployment these pages are written for (one household Plex library, three
datacenters with asymmetric links) maps to the inventory like this:

| DC | Link | Managers | Data nodes | Clients |
|----|------|----------|------------|---------|
| LELI | up to 40 Mbps | `mgr01`, `mgr02`, `mgr03` (the three Raft voters) | `data-primary01` (about 50 TB HDD, the anchor holder) | `client01` |
| TORO | about 400 Mbps | `mgr04` (learner) | `data-warm01` (SSD) | `client02` |
| NRB | about 1 Gbps | `mgr05` (learner) | `data-warm02` | `client03` |

Keeping the voters in the anchor datacenter means quorum never depends on a WAN
link; the remote managers replicate everything and serve local reads without
voting. A box may host several roles (for example TORO's Plex host runs `mgr04`,
`data-warm01` and `client02`): then each daemon gets its own hostname
(`mgr04`, `data-warm01`, `data-warm01_cln`), its own config file, unit and memory
cap. Every chunk keeps a replica on `data-primary01`.

## What the repository already automates, and what a role must add

| Step | Repository artifact | Gap a role must close |
|---|---|---|
| First install on one host | `scripts/dtfs-install.sh` (interactive or `--noninteractive`) | Single host at a time; renders a minimal config; no checksum/signature check of the download; no cluster-level ordering. Use it as a reference for paths and modes, not as the role. |
| Rolling binary update | `deploy/ansible/update-cluster.yml` + `tasks/update-node.yml` | Verifies the tarball (sha256 against `SHA256SUMS`, optional minisign), replaces binaries, refreshes the shipped unit file, and can flip `require_raft_auth` in an opt-in last play. No config rendering, no maintenance-mode wrapping, no `.prev` restore task. These are specified in the pages above. |
| Release production | `.github/workflows/*.yml`, `scripts/dtfs-release.sh`, `scripts/ci/*` | None for consumers; the role only downloads. |
| Post-deploy gate | `scripts/dtfs-cluster-healthcheck.sh` | Needs `curl` and `jq` on the machine that runs it. |

## Invariants the automation must never violate

1. Never set `bootstrap = true` on more than one manager, and only before the
   very first cluster start. A second bootstrap creates a divergent
   single-voter cluster.
2. Never run `dtfs-cli raft remove` on a manager unless the remaining voters
   keep a majority; never update two managers at the same time.
3. The same `[cluster].shared_secret` on every node of every role. Rotating it
   is a cluster-wide change that is not supported as a rolling operation.
4. Never remove or reorder an entry of a data node's `[[disks]]` list except by
   appending. See [Decommission and Replace](./decommission-replace.md).
5. Never delete or rename anything under a data disk's `chunks/`, `tmp/` or
   `.dtfs/` directories by hand; quarantined chunks live in `.dtfs/quarantine/`
   and are kept on purpose.
6. Every chunk keeps at least one replica on the anchor datacenter (LELI in the
   reference deployment). Do not decommission or wipe the anchor data node as
   part of a routine update.
