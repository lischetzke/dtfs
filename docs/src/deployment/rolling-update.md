# Rolling Update and Rollback

Sources: `deploy/ansible/update-cluster.yml`, `deploy/ansible/tasks/update-node.yml`,
`scripts/dtfs-cluster-healthcheck.sh`, `.github/workflows/release.yml`,
`crates/dtfs-cli/src/updater.rs`, `crates/dtfs-manager/src/{monitor,metadata}.rs`.

This page specifies what a role must do to update a running cluster to a new
release. The first half is what `update-cluster.yml` already does (a role can
include it unchanged or reimplement it); the section
[Additions a role should make](#additions-a-role-should-make) lists what the
playbook leaves out.

## Rules

- One node at a time, cluster-wide: never two managers in parallel (quorum),
  and do not overlap a data-node restart with a manager restart.
- Order: managers (learners, then follower voters, the Raft leader last), then
  data nodes, then clients.
- Every node must pass its health gate, then a settle delay, before the next
  node starts. A failed gate stops the whole run.
- The target is an exact tag (`vX.Y.Z`). Do
  not resolve `latest` inside an unattended run.

## Inputs

| Input | Meaning |
|---|---|
| `dtfs_version` | expected version stamp, for example `v0.2.0` |
| tarball | already downloaded and verified on the control host ([Artifacts](./artifacts.md)); or a URL plus token |
| `dtfs_restart_delay_secs` | settle pause after each updated node, default 30 |
| `dtfs_health_timeout_secs` | per-node and final wait budget, default 300 (poll every 5 s) |
| `dtfs_force_update` | reinstall even when already on `dtfs_version`, default false |
| `dtfs_verify_checksum` | require the tarball sha256 in `SHA256SUMS`, default true |
| `dtfs_sums`, `dtfs_sums_url` | `SHA256SUMS` path or URL; default beside the tarball (or the tarball URL) |
| `dtfs_minisign_pubkey` | minisign public key (base64, or an absolute `.pub` path); when set the signature must verify |
| `dtfs_minisig` | `SHA256SUMS.minisig` path; default `<sums>.minisig` |
| `dtfs_refresh_units` | install the tarball's unit file on each updated node, default true |
| `dtfs_enable_raft_auth` | run the final `require_raft_auth` play, default false |
| `dtfs_manager_config` | manager config path on the nodes, default `/etc/dtfs/manager.toml` |

## Preflight (control host, nothing on the nodes changes)

1. Verify the download before unpacking: the sha256 of the tarball must be a
   line for `dtfs-linux-amd64.tar.gz` in `SHA256SUMS`; when `dtfs_minisign_pubkey`
   is set, `minisign -V` must accept `SHA256SUMS` against `SHA256SUMS.minisig`
   (the `minisign` binary is needed on the control host). Any mismatch fails the
   play before a node is touched. After unpacking, every `dtfs/bin/*` file must
   match `dtfs/MANIFEST`.
2. Unpack the tarball into a fresh temp directory; run `dtfs/bin/dtfs-cli
   --version` and require it to start with `dtfs-cli <dtfs_version> `.
3. `GET http://<mgr>:9402/api/v1/raft/state` on every manager. Every manager must
   answer (otherwise abort: fix it before updating). Exactly one must report
   `is_leader == true` (otherwise a leader election is in progress: abort and
   retry later).
4. Classify: **learners** = managers whose `state` contains `Learner` and that
   are not leader; **leader** = the one with `is_leader`; **followers** = the
   rest. Order = learners, followers, leader.

## Per-node procedure

Roles map to binaries and units:

| Role | Binaries replaced | Unit |
|---|---|---|
| manager | `dtfs-manager`, `dtfs-cli` | `dtfs-manager.service` |
| data | `dtfs-data`, `dtfs-cli` | `dtfs-data.service` |
| client | `dtfs-fuse`, `dtfs-cli` | `dtfs-fuse@<systemd-escape --path MOUNT>.service` |

For each node:

1. **Skip if current.** Run `<bin_dir>/<role binary> --version`. If it matches
   `^<binary> <dtfs_version> ` and `systemctl is-active <unit>` prints `active`
   and `dtfs_force_update` is false, log a skip and continue (no restart, no
   delay). This makes a re-run after a partial failure safe.
2. **Keep the rollback copy.** Only if the node is not already on the target
   version: for each binary, if `<bin>` exists, `cp -p <bin> <bin>.prev`. One
   copy per binary, overwritten by the next successful update. The condition
   matters: a re-run after a failed health check (binary already new) must not
   overwrite `.prev` with the new binary.
3. **Install.** Copy each new binary from the verified tree to
   `<bin_dir>/<binary>`, owner `root`, group `root`, mode `0755`. Use a method
   that replaces the file atomically (Ansible `copy`, or `install` to `<bin>.new`
   then `mv`); never write into the running executable.
   Then copy the role's unit file from `dtfs/systemd/` to
   `/etc/systemd/system/` (`dtfs_refresh_units`, default on; the previous file
   is kept as a timestamped backup). Drop-ins are operator-owned and not touched.
4. **Restart.** `systemctl daemon-reload` (picks up a changed unit file) then `systemctl restart <unit>`. Allow
   the stop timeout (manager 30 s, data and client 45 s).
5. **Health gate** (poll every 5 s up to `dtfs_health_timeout_secs`; fail the run
   on timeout):

   | Role | Condition |
   |---|---|
   | manager | TCP `9402` open on the node, then `GET http://<node>:9402/api/v1/raft/state` returns 200 with `running_state == "ok"` and `current_leader` not null, not the string `None`/`null`/empty |
   | data | on the **leader**: `GET /api/v1/nodes` (a JSON array; also accept `{"nodes": [...]}`) has the row with `hostname == <node identity>` and `health == "Healthy"` and `version == <dtfs_version>` |
   | client | `mountpoint -q <mount>` succeeds |

   The node identity is `[node].hostname` of the daemon (for a client, its
   `[location].host`); the inventory name is used when `dtfs_hostname` is unset,
   so inventory names must equal those values.
6. **Stamp check.** `<bin_dir>/<role binary> --version` must match
   `^<binary> <dtfs_version> `; otherwise fail.
7. **Settle.** Sleep `dtfs_restart_delay_secs` (not after a skipped node).

After the last node run the cluster gate:

```bash
bash scripts/dtfs-cluster-healthcheck.sh --managers <host:9402,...all managers> \
     --expect-version "$VERSION" --min-data-nodes "$N_DATA" --timeout "$TIMEOUT"
```

Exit 0 means: all listed managers `running_state == ok` and agree on one leader;
the leader's `/api/v1/cluster/status` has `status == "ok"`,
`data_nodes_healthy == data_nodes`, `files_missing_chunks == 0` (empty files do
not count, see [Glossary](../glossary.md)); at least
`--min-data-nodes` data nodes; every node that reports a version reports
`--expect-version`. Exit 1 unhealthy after the timeout, 3 bad arguments. The CI
`verify` stage repeats the same command from the runner.

## Additions a role should make

These are not in `update-cluster.yml` and are needed for a role that owns the
whole host. (Checksum, optional minisign verification and the unit-file refresh
are now done by the playbook itself; see the Inputs table.)

1. **Verify before distributing.** minisign and sha256 as in
   [Artifacts](./artifacts.md#how-to-verify-a-download); the playbook does this
   against `SHA256SUMS` as described under Preflight.
2. **Gate on a healthy start.** Before touching the first node run the cluster
   gate without `--expect-version`. Do not update a cluster that is already
   degraded (a node Down, `files_missing_chunks > 0`, no single leader).
3. **Units and config.** The playbook installs the unit file from the tarball
   (`dtfs/systemd/`) before the restart. Re-rendering configs is still the
   role's job, before the restart, so one restart applies binary, unit and
   config together. Keep a copy of the previous config for rollback.
4. **Maintenance mode for long restarts (data nodes).** A plain binary restart
   takes seconds and needs no maintenance. If the node can stay down longer than
   the liveness window (`monitoring.heartbeat_interval_secs` x
   `down_after_missed`, 30 s by default; reboots, OS upgrades, disk work) put it
   in maintenance first, otherwise the monitor marks it `Down` and the repair
   scheduler starts re-replicating its chunks, which on a 40 Mbps link is a
   multi-day backfill storm:

   ```bash
   # resolve the numeric id by hostname, then:
   curl -fsS -X POST "http://$LEADER/api/v1/nodes/$NODE_ID/maintenance" \
        -H 'Content-Type: application/json' -H "X-Dtfs-Shared-Secret: $SECRET" \
        -d '{"enable": true}'
   ```

   What maintenance does (verified in code): the node is excluded from new
   placements, the monitor never marks it `Down`, and a heartbeat does not set it
   back to `Healthy`. What it does **not** do: it does not move or re-replicate
   any chunk, and replicas on it still count as live. Because the state is sticky
   the role must explicitly disable it (`{"enable": false}`) after the restart,
   and only then wait for `health == "Healthy"`; a node left in maintenance never
   passes the health gate above. Managers and clients have no equivalent; the
   monitor only tracks data nodes.
5. **Anchor and consumers.** The data node that holds the anchor copies (LELI
   `data-primary01`) is the only holder of some chunks (cold replication 1 on that
   tier), so reads of those chunks fail while it restarts, until cache replicas
   or the node return. Schedule that node in a quiet window and update it last
   among data nodes. For clients, stop or pause consumers of the mount (Plex)
   before restarting `dtfs-fuse@...`; the restart unmounts and remounts, and open
   files on the old mount return errors.
6. **Manager restarts.** Restarting the leader triggers a re-election (about a
   second with the default timeouts); writes pause for that long. The stop takes
   a final Raft snapshot (up to 5 s) and the unit allows 30 s.
7. **Concurrency control.** Serialize runs (`serial: 1` semantics) and hold a
   cluster-wide lock so two automation runs cannot overlap (the CI uses
   `resource_group: cluster`).

## Flag flips after an update

Some config keys are safe only after **every** manager runs the new binary. Flip
them in a second, separate rolling pass (config change plus restart, in the same
order), never in the same pass as the binary update.

| Key | Default | Flip when | Why |
|---|---|---|---|
| `cluster.require_raft_auth` (manager.toml) | `false` | `false` to `true` once every manager runs a build that signs Raft RPCs (no "accepting UNSIGNED Raft RPC" warning in `journalctl -u dtfs-manager`, or check each manager's version) | `true` rejects unsigned Raft RPCs; with a mixed fleet it splits the cluster. Stamped-but-invalid RPCs are always rejected. Requires a non-empty secret and clocks within 300 s. |
| `cluster.accept_legacy_forward_secret` (manager.toml) | `true` | `true` to `false` on every manager (restart one at a time) once every manager runs a build that HMAC-signs `ForwardClientWrite` | Until flipped, followers still send the raw cluster secret in every forward, so it crosses the manager link in clear text when `[tls]` is off. `false` stops sending it and refuses unsigned forwards; with a pre-HMAC manager still in the fleet its forwards (heartbeats included) stop committing. |
| `cluster.require_delete_binding` (data.toml) | `false` | `false` to `true` on every data node once every manager runs a build that signs the full DeleteChunk binding (no "accepting LEGACY chunk_id-only stamp" warning in `journalctl -u dtfs-data`) | `true` rejects an old manager's chunk_id-only DeleteChunk stamp, so its reaper/flush deletes fail (bytes stay, no data loss). Both update directions work while it is `false`: a new data node accepts an old manager's chunk_id-only stamp, and a new manager whose bound stamp an old data node refuses with `PermissionDenied` retries that delete once with the chunk_id-only stamp (debug log), so flush/demote/orphan-reaper deletes keep freeing bytes while managers are updated first. With `true` that retry is refused again and the delete fails. |
| `audit.replicate_via_raft` (manager.toml) | `false` | to `true` only after every manager understands `AppendAudit` | an old follower cannot deserialize the new Raft entries and gets stuck |
| `[tls] mode` | `auto` | enable TLS (`require` or certificates present) only cluster-wide after every node runs a TLS-capable build | mixed TLS and plaintext nodes cannot talk |

`update-cluster.yml` automates the first row: run it with
`-e dtfs_enable_raft_auth=true` (normally on a second invocation after the
binary update, which is then a no-op skip for already-current nodes). The last
play first requires every manager to report `dtfs_version`, then, followers
first and the leader last, one manager at a time: checks `shared_secret` is set
in `dtfs_manager_config`, sets `require_raft_auth = true` under `[cluster]`
(a timestamped backup of the file is kept), restarts the unit only if the file
changed, and waits for Raft `running_state == ok` and a known leader before
the next manager. Default off.

The release notes of a version list such requirements under an `Update-required:`
trailer; read them before pinning a version.

## Failure handling

| Failure | Behaviour | What to do |
|---|---|---|
| gate times out on a node | the run stops at that node; earlier nodes stay on the new version, later ones on the old | inspect (`journalctl -u <unit>`), then either fix forward and re-run (finished nodes are skipped) or roll back (below) |
| preflight cannot reach a manager or sees no leader | nothing changed | fix or wait; re-run |
| stamp mismatch after install | binary on disk is not the requested tag | stop; check the tarball/tag and the task that copied it |
| release itself is bad | the GitHub release is still `latest` | `gh release delete <tag>` (see [CI Pipeline](../operations/ci-pipeline.md#recovering-from-a-failed-rollout)); the tag stays, the version number is burnt, and the fix ships as the next version |

## Rollback

Roll back only nodes that were updated, in the reverse of the update order
(clients, data nodes, then managers), and only while the cluster has not yet
committed state that needs the new version. A newer leader may have proposed Raft
entry types an older binary cannot apply (for example after enabling
`audit.replicate_via_raft`), so rollback is a stop-gap for a failed update, not an
undo for a version that has been running with new features.

**No manager rollback below v0.1.14 once named removes were proposed.** From
v0.1.14 a leader names the row's `chunk_hash` on `RemoveChunkLocation` and
`RemoveDeadNodeReplica`, but only while every Raft voter and learner is a
registered manager reporting a release `vX.Y.Z` >= v0.1.14 (dev builds such as
`v0.0.0-dev` and suffixed versions such as `v0.1.14-rc1` keep it off). An older
manager ignores the field and removes the row hash-blind where a v0.1.14 node
refuses, so the replicated state diverges. A manager that rejoins on an older
binary closes the gate only at its next registration (a few seconds); entries
already in the log stay named. Roll managers back only before any v0.1.14 leader
ran with all managers on v0.1.14. The same holds for adding or replacing a
manager: never join one below v0.1.14 (for example from an inventory pinned to
an old tag), since it replays the named entries after the leader's snapshot
hash-blind.

Per node:

1. If maintenance mode is warranted (data node, longer than 30 s), enable it.
2. Require `<bin>.prev` to exist for every binary of the role; otherwise stop and
   escalate (a node updated twice has lost the original).
3. Record the previous version from `<bin>.prev --version`.
4. For each binary: `cp -p <bin>.prev <bin>.new && mv <bin>.new <bin>` (atomic).
   Restore the previous unit file and config if the update changed them.
5. `systemctl daemon-reload && systemctl restart <unit>`.
6. Same health gate as above, expecting the previous version string instead of
   `dtfs_version`.
7. Disable maintenance if enabled; settle delay.

The manager role must preserve the data directory: rollback never touches
`raft-log/` or `raft-sm/`.

## Alternative: self-update on the node

`dtfs-cli update <manager|data|client|cli|all> [--tag vX.Y.Z] [--check] [--yes]`
downloads from the configured repo (default: the GitHub releases of
`lischetzke/dtfs`; flags in the [CLI Reference](../api-reference/cli-reference.md)), verifies the minisign
signature when `--minisign-key`, `DTFS_MINISIGN_PUBKEY` or `[updater] minisign_public_key` is set
(a warning otherwise), verifies each binary against `MANIFEST`, installs
atomically and restarts the units that exist on the host (a role with no unit
here, for example a manager binary on a data-only host, is skipped with a
message; FUSE restarts every `dtfs-fuse@*` instance found). It attempts every
restart, then reports all units that failed to restart or did not come back
active and exits non-zero. It does not keep `.prev`, does not install
`dtfs-cli` unless asked (`manager`/`data`/`client` replace only their own
daemon binary), does not update unit files, and has no health gate, so it is for
manual use on a single node, not for fleet automation.
