# Fresh Install Order

Sources: `crates/dtfs-manager/src/{main,membership}.rs`,
`crates/dtfs-manager/src/api/{raft,cluster,nodes}.rs`,
`crates/dtfs-data/src/main.rs`, `scripts/dtfs-install.sh`,
`scripts/dtfs-cluster-healthcheck.sh`,
[Cluster Management](../operations/cluster-management.md).

Run the steps in this order. Every step ends with a gate; do not start the next
step until its gate passes. `M` is any manager REST endpoint `host:9402`;
`LEADER` is the manager that reports `is_leader: true`. Use `jq` for the JSON
assertions. Mutating REST calls and CLI membership commands need the shared
secret (`X-Dtfs-Shared-Secret: <secret>` or `dtfs-cli --secret`/`DTFS_SHARED_SECRET`).

## Step 0: preparation (all hosts)

1. [Artifacts](./artifacts.md) verified on the control host.
2. [Prerequisites](./host-prerequisites.md) met on every host: users, packages,
   time sync, ports, mounted and `dtfs`-owned data disks.
3. Inventory assertions: all daemon hostnames unique (managers, data nodes and
   client `location.host`); every `initial_voters` address routable; secret
   present in Vault.
4. Install on every host: binaries (role-specific, see
   [Overview](./overview.md#components-and-roles)), `/etc/dtfs`, state
   directories, unit files, `disks.conf` and optional memory drop-ins, then
   `systemctl daemon-reload`. Render the config files but do not start any
   daemon yet.

Gate: on each host `dtfs-cli --version` prints the expected tag and
`systemd-analyze verify /etc/systemd/system/dtfs-*.service` is clean.

## Step 1: bootstrap manager

Decide the initial voter set now. At first start the bootstrap manager calls
`raft.initialize` with itself plus every host in its
`[[cluster.initial_voters]]`, so that set becomes the voter set. Recommended
shape for the reference deployment: voters = the three managers of the anchor
datacenter (`mgr01`, `mgr02`, `mgr03`); the managers of other datacenters join
later as learners (Step 2). In the bootstrap manager's file the list contains
exactly those three entries.

1. Render `manager.toml` on `mgr01` with `cluster.bootstrap = true`, all other
   managers with `false`.
2. Start `mgr01`, then immediately `mgr02` and `mgr03` (all `bootstrap =
   false`, same `initial_voters` list). With three voters a leader exists as soon
   as two of them are up. Starting a single-entry list is also valid (a one-voter
   cluster) but then every other manager must be added by Step 2.
3. Within about 20 seconds the bootstrap manager upserts the `default` volume
   from `[defaults]` (insert-if-missing, so it never overwrites later volume
   edits) and takes an early snapshot. `[defaults]` is therefore consumed once,
   here; later changes go through the volume API.

Gate (for each voter):

```bash
curl -fsS http://$M/healthz                      # 200 "ok"
curl -fsS http://$M/api/v1/raft/state | jq -e '
  .running_state == "ok" and .current_leader != null
  and (.voters | length) == 3 and .applied_log_lag == 0'
curl -fsS http://$LEADER/api/v1/volumes | jq -e '.volumes | map(select(.volume_id == "default")) | length == 1'
```

and exactly one manager reports `is_leader == true`, all report the same
`current_leader`. Do not gate on `/readyz` yet: it returns 503 until at least one
data node is healthy.

Do not restart managers in parallel from here on. If the bootstrap manager is
restarted later with `bootstrap = true` still rendered, the second initialise
fails harmlessly ("may already be initialised"), but the safest steady state is
to render `bootstrap = false` after the first successful gate.

## Step 2: add the remaining managers

For each additional manager `mgrNN` (for example the TORO and NRB managers), one
at a time:

1. Install and render with `bootstrap = false`. Its `node.hostname` must equal
   its own `initial_voters` entry in the file; the complete list of all managers
   goes into every manager's file (forwarding and self-registration read it).
2. Start the service and wait for `curl -fsS http://mgrNN:9402/api/v1/health`.
   A manager that was never joined is an inert empty learner: term 0, no leader,
   empty namespace. Do not trust its reads.
3. Join it as a learner on the leader (a follower answers 409 with a `leader`
   hint, so retry there):

   ```bash
   dtfs-cli --manager $LEADER raft add-learner <hostname> <routable-ip>:9401 --secret "$SECRET"
   ```

   The REST form is `POST /api/v1/raft/add-learner` with
   `{"hostname":..,"addr":..,"shared_secret":..}`. The call is refused for a
   wildcard address, an empty hostname, or a hostname that is already a data
   node or client.
4. Wait until caught up (large state arrives as a snapshot; allow minutes over a
   slow WAN):

   ```bash
   curl -fsS http://mgrNN:9402/api/v1/raft/state | jq -e '
     .current_leader != null and .applied_log_lag == 0 and .running_state == "ok"'
   ```

5. Decide the end state. Remote-DC managers are often better left as learners
   (full replication, reads served, no vote, quorum math and election latency
   unchanged). To make it a voter:

   ```bash
   dtfs-cli --manager $LEADER raft promote <hostname> --secret "$SECRET"
   ```

   Keep the voter count odd (3 or 5). The server refuses a membership change
   unless a majority of the current voters are reachable and caught up.

Gate after the last manager: on every manager `raft/state` shows the same
`voters` list and the same `learners` list as planned, `current_leader` agrees,
`running_state == "ok"`, and `.membership | length` equals the number of managers.

## Step 3: data nodes

Order: the node that holds the anchor copies first (LELI `data-primary01` in the
reference deployment), then the others. Placement needs at least
`min_replicas_before_ack` distinct `(node, disk)` targets and an anchor-datacenter
node before the first client write, otherwise writes are rejected.

For each data node:

1. Disks mounted and owned by `dtfs`; `disks.conf` lists them;
   `[cluster] manager_addr` and `manager_addrs` list **all** managers that are
   voters or learners (a rolling manager restart must never strand a node).
2. `systemctl enable --now dtfs-data`. The daemon replays its WAL, binds the gRPC
   port immediately, registers with a manager (shared secret, no separate join
   command), then heartbeats every 5 seconds. It walks the manager list after 3
   failures and re-registers if a manager answers NOT_FOUND.
3. Gate on the leader:

   ```bash
   curl -fsS http://$LEADER/api/v1/nodes | jq -e --arg h "$HOSTNAME" --arg v "$VERSION" '
     map(select(.hostname == $h)) | length == 1
     and .[0].role == "Data" and .[0].health == "Healthy"
     and .[0].version == $v and .[0].disk_count == $DISKS'
   ```

   `disk_count` must equal the number of `[[disks]]` entries. The first start
   writes `.dtfs-disk-id`/`.dtfs-disk-uuid` into every disk; check they exist.

Gate after all data nodes:

```bash
bash scripts/dtfs-cluster-healthcheck.sh --managers <all managers host:9402> \
     --expect-version "$VERSION" --min-data-nodes "$N_DATA" --timeout 300
curl -fsS http://$LEADER/readyz          # now 200 "ready"
```

The healthcheck passes when every listed manager has `running_state == ok` and
the same leader, `cluster/status.status == "ok"`, `data_nodes_healthy ==
data_nodes`, `files_missing_chunks == 0` (empty files do not count, see
[Glossary](../glossary.md)), at least `--min-data-nodes` data nodes
and every reporting node on `--expect-version`. List every voter in
`--managers`; learners are optional.

## Step 4: clients

For each client host:

1. `fuse3` installed, `user_allow_other` in `/etc/fuse.conf` if
   `allow_other = true`, mount directory exists, `fuse.toml` rendered with
   `manager_addrs` containing at least one **voter** (a client that only knows
   learners can read but never write: client mutations are not forwarded).
2. Start the instance unit:
   `systemctl enable --now "dtfs-fuse@$(systemd-escape --path /mnt/dtfs).service"`.
3. Gate on the host and the leader:

   ```bash
   mountpoint -q /mnt/dtfs
   curl -fsS http://$LEADER/api/v1/nodes | jq -e --arg h "$CLIENT_LOCATION_HOST" '
     map(select(.hostname == $h and .role == "Client")) | length == 1'
   ```

4. Write/read smoke test from one client (once, not on every run of the role):

   ```bash
   head -c 8388608 /dev/urandom > /tmp/dtfs-smoke.bin
   cp /tmp/dtfs-smoke.bin /mnt/dtfs/dtfs-smoke.bin
   cmp /tmp/dtfs-smoke.bin /mnt/dtfs/dtfs-smoke.bin
   curl -fsS "http://$LEADER/api/v1/files/inspect?path=/dtfs-smoke.bin" | jq .
   ```

   Assert in the inspect output (`chunks[].replicas[]` with `node_id`, `disk_id`,
   `tier`) that every chunk has at least `min_replicas_before_ack` distinct
   `(node_id, disk_id)` replicas and that one of them is on a node whose
   `location` in `GET /api/v1/nodes` is in the anchor datacenter. Delete the test file afterwards; its chunks are
   reclaimed later by the orphan reaper (after `orphan_grace_secs`, default one
   day), not immediately.

Final gate: rerun `scripts/dtfs-cluster-healthcheck.sh` as in Step 3.

## Failure handling during a fresh install

| Symptom | Meaning | Action |
|---|---|---|
| manager shows `current_leader == null` after all voters are up | quorum not reached: wrong `initial_voters` address or hostname mismatch, firewall on 9401, clock skew above 300 s with `require_raft_auth` | fix config/network, restart that manager; do not bootstrap another node |
| a non-bootstrap manager has term 0, empty namespace | never joined | Step 2.3 |
| data node absent from `/api/v1/nodes` | secret mismatch or unreachable manager; read `journalctl -u dtfs-data` | fix and restart |
| data node refuses to start with "was disk_id N but is now at [[disks]] position M" | disk order changed or disk moved between positions | restore the order; see [Decommission and Replace](./decommission-replace.md) |
| unit exits 226/NAMESPACE | a `ReadWritePaths=` path is missing | create `/var/lib/dtfs/<role>` and mount the disks |
| a client appears but writes fail with a replicas error | fewer distinct replica targets than `min_replicas_before_ack` | add data nodes/disks or lower the volume's write safety |
