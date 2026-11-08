# Decommission and Replace

Sources: `crates/dtfs-data/src/disk.rs`, `crates/dtfs-manager/src/api/nodes.rs`,
`crates/dtfs-manager/src/scheduler/{repair,orphan}.rs`,
`crates/dtfs-manager/src/membership.rs`, `crates/dtfs-manager/src/monitor.rs`,
[Disk Management](../operations/disk-management.md),
[Cluster Management](../operations/cluster-management.md).

All procedures here are drain-then-replace: capacity is added or content is
verified elsewhere first, and only then is something removed. The anchor
invariant applies throughout: every chunk keeps at least one replica on the
anchor datacenter's data node. Never decommission, wipe or replace the only
anchor holder as a side effect of an automation run.

## What exists and what does not

| Capability | State |
|---|---|
| Maintenance mode (`POST /api/v1/nodes/{id}/maintenance`) | Exists. Excludes the node from new placement and from automatic `Down` marking; sticky until disabled. It does **not** move or re-replicate any chunk. |
| Node decommission (`POST /api/v1/nodes/{id}/decommission`, `dtfs-cli decommission`) | Exists. Removes the node from the registry. Refused with HTTP 409 while any file's chunk list still has a replica on that node. |
| Disk "drain" operation | Does not exist. No API sets a disk to draining. Content leaves a disk only through the mechanisms below. |
| Additive migration (`dtfs-cli migrations create <prefix> --target-tier T [--target-dc DC]`) | Exists. Adds replicas; never removes the source. |
| Automatic repair | The manager scheduler is the sole owner of replica repair: it re-creates missing mandatory replicas on healthy targets, and removes only replicas in excess of the goal. |
| Stale-row reconciliation | When a live data node reports (via chunk listing) that chunks the manager believes it holds are missing from its disks, the scheduler drops those rows if another live copy exists elsewhere. If the missing copy was the last live one it does not drop it; it escalates and quarantines the node. |

Consequence: the reliable preconditions for removing content are (a) every
chunk on the disk or node has another live replica on a different
`(node_id, disk_id)`, and (b) the manager shows `files_missing_chunks == 0`
afterwards. Anything that violates (a) is data loss, not a drain.

## Disk identity rules (data node)

- A disk's replica identity is `(node_id, disk_id)`, where `disk_id` is its
  0-based position in `[[disks]]`.
- On first start the daemon writes the position to `<mount>/.dtfs-disk-id` and a
  UUID to `<mount>/.dtfs-disk-uuid`. On every later start a disk whose stored id
  differs from its current position makes `dtfs-data` **refuse to start**:
  `disk <path> was disk_id N but is now at [[disks]] position M`.
- A non-numeric `.dtfs-disk-id` also fails the start.
- Therefore the `[[disks]]` list is **append-only**. Adding a disk: append a new
  entry. Retiring or replacing: keep the entry (same position) and change what is
  mounted behind that path. Never remove an entry from the middle, never reorder,
  never swap two mount paths. A role that renders `[[disks]]` must keep the order
  in persistent inventory data (a list variable), not derive it from a dictionary
  or a sorted set.
- Mount by `UUID=`/`LABEL=` so device renames cannot swap disks (see
  [Host Prerequisites](./host-prerequisites.md#disks-data-role)); and use
  `RequiresMountsFor=` so a missing mount fails the start.
- Do not edit or copy `.dtfs-disk-id` to silence the refusal; fix the order.

## Add a disk

1. Format, mount (by UUID) and `chown dtfs:dtfs` the new filesystem.
2. Append `[[disks]] path = ..., tier = ...` at the end of the list in the
   template; add the path to `disks.conf` (`ReadWritePaths=`, `RequiresMountsFor=`)
   and run `systemctl daemon-reload`.
3. Restart `dtfs-data` (one node at a time, with the
   [update health gate](./rolling-update.md#per-node-procedure)).
4. Assert: the node's `disk_count` in `GET /api/v1/nodes` increased by one, the
   new mount contains `.dtfs-disk-id` equal to its position, `GET
   /api/v1/nodes/{id}` shows the disk healthy. Existing data is not rebalanced;
   new writes prefer emptier disks.

Changing only the tier of an existing entry is safe (position unchanged); the
existing chunks stay and the tiering engine adjusts over time.

## Replace a healthy disk (planned swap, same position)

Use a copy so that nothing has to be evacuated:

1. Optionally enable maintenance for the node if the work may exceed 30 s
   ([Rolling Update, maintenance mode](./rolling-update.md#additions-a-role-should-make)).
2. `systemctl stop dtfs-data` (it drains in-flight writes for up to 30 s).
3. Format the new disk, mount it temporarily elsewhere, and copy the entire
   filesystem including hidden files and all attributes
   (`rsync -aHAXx --numeric-ids old/ new/`, or `xfs_copy`/`dd` for like-for-like
   disks). The copy carries `.dtfs-disk-id` and `.dtfs-disk-uuid`, so the new
   disk keeps its identity and position.
4. Unmount both, mount the new disk at the **same path**, keep the same
   `[[disks]]` entry.
5. Start `dtfs-data`; pass the node health gate; disable maintenance.
6. Verification: trigger a deep scrub (`dtfs-cli scrub --level deep`; scrubs are
   cluster-wide, targeted `--disk` scrubs are rejected by the API) and require
   `files_missing_chunks == 0` in `GET /api/v1/cluster/status`. Chunks are checksummed on every read and by
   scrub, so a bad copy is detected, quarantined (renamed, never deleted) and
   repaired from another replica.

This path is the one the identity rules are designed for; it has not been
exercised in a lab (none exists), so treat the first run as supervised.

## Replace a failed or unreadable disk

1. Do not stop the cluster. The data node reports I/O errors and marks the disk
   `Failed`; the manager excludes it from placement and the repair scheduler
   re-creates its mandatory replicas from other nodes.
2. Wait until nothing is waiting for repair:
   `GET /api/v1/chunks?state=undergoal` and `?state=missing` return no rows and
   `files_missing_chunks == 0`. If a chunk has no replica left on any other
   `(node, disk)`, the content of that disk was the only copy: stop and escalate,
   do not wipe anything.
3. Stop `dtfs-data`, swap and format the disk, mount it at the **same path** at
   the **same `[[disks]]` position**, start `dtfs-data`. The empty disk gets the
   position written as a new `.dtfs-disk-id` plus a new UUID.
4. The manager's rows for the old disk's chunks are stale. The scheduler's
   reconciliation drops them where a live copy exists elsewhere and repair then
   re-populates the new disk toward the policy goal. A node that reports missing
   chunks whose last live copy was on the old disk is quarantined; that is the
   loud signal for case 2 having been skipped. Unquarantine only after the root
   cause is understood (`dtfs-cli unquarantine`, requires a written reason).
5. Assert: node `Healthy`, disk healthy, `files_missing_chunks == 0`.

Replacing a populated disk with an empty one while the old content is still the
only copy is the data-loss case; for the anchor node with replication 1 on its
tier there is no second copy to repair from, so use the copy procedure above or
first create a second replica on another disk or node.

## Retire a disk permanently

The entry cannot be deleted from the middle. Options, in order of preference:
replace it in place with a different filesystem (it keeps its position and takes
new writes), or, only if it is the **last** entry, remove the entry after its
chunks have been moved off by the migration and repair mechanisms and the
manager shows no replica on it. The last-entry removal changes no other disk's
position. A role must refuse to render a `[[disks]]` list that is not an
extension of the previously rendered one (prefix check against saved state).

## Replace a data node

Choose by what is retained:

| Case | Procedure |
|---|---|
| Same hostname, disks kept (OS reinstall, new motherboard) | Stop the unit, move or reuse the disks with their original mount paths, run the role with the **same** `node.hostname`, secret, and `[[disks]]` order, start. The node id derives from the hostname and the disk identities travel with the disks, so nothing is re-replicated. |
| New hostname, empty disks (new hardware) | Install the new node as in [Fresh Install, Step 3](./fresh-install.md#step-3-data-nodes) and wait until it is `Healthy`. Add capacity before removing: let writes and repair fill it, or add replicas with `dtfs-cli migrations create <prefix> --target-tier T --target-dc DC`. Then follow "Remove a data node". |

## Remove a data node

1. Confirm the removal does not break the anchor invariant: no chunk may lose its
   only anchor-datacenter replica. Removing the anchor data node is not a routine
   operation; replace it with a node in the same datacenter first.
2. Enable maintenance on the node (no new placements, no automatic `Down`).
3. Make sure every chunk it holds has replicas elsewhere (replication and
   migration as above). Maintenance does not do this for you.
4. Stop and disable the unit.
5. `dtfs-cli decommission <node-id> --operator-id <who> --reason "<why>"`
   (REST: `POST /api/v1/nodes/{id}/decommission`
   `{"operator_id": "...", "reason": "..."}`). A 409
   `node N still holds chunk replicas` means step 3 is incomplete: do not force
   anything, finish step 3.
6. Remove the host from the inventory, keep its `/etc/dtfs` and disks until the
   cluster has been healthy for a while, then purge.

Known gap: for a node that is permanently dead, the manager's rows that point at
it are not removed by the reconciliation described above (that runs against live
nodes). Whether `decommission` can then succeed without first bringing the node
back is not established by the code and was not exercised; the 409 is the
authoritative answer. Plan on keeping a dead node's identity reachable until
decommission succeeds.

## Replace or remove a manager

Membership is changed only through the leader and never below a majority.

Replace (new hardware, new hostname):

1. Bring up the new manager as in
   [Fresh Install, Step 2](./fresh-install.md#step-2-add-the-remaining-managers):
   `add-learner`, wait for `applied_log_lag == 0`, `promote`.
2. Verify `voters` on every manager lists the new node and the cluster agrees on
   a leader.
3. Make sure the manager to remove is not the leader (restart it so another voter
   wins), then `dtfs-cli --manager $LEADER raft remove <old-hostname> --secret
   "$SECRET"`. The server refuses when a majority would not remain reachable and
   caught up.
4. Stop and disable the old manager; remove its `initial_voters` entry from the
   remaining managers' configs (applied at their next restart, one at a time).

Same hostname, state intact: run the role and restart. Same hostname, state lost
while still in the membership: stop it, move aside `raft-log/` and `raft-sm/`,
start; it reinstalls a snapshot from the leader. If it was already removed, it
must be re-added with `add-learner`; never set `bootstrap = true`.

## Replace a client

Stop and disable `dtfs-fuse@<mount>.service`, install the new host with its own
unique `location.host`, mount it. The old registry row stays in the registry and reports stale health once
the client stops heartbeating; remove the old host from the inventory.
