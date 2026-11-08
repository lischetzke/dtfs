# Maintenance Mode

Maintenance mode tells the manager that a data node is going away on purpose, so
it must not be treated as a failure. It is a flag, not a drain: it does not move
any data.

## What it does

Verified against `api/nodes.rs::set_maintenance`, `monitor.rs`,
`metadata.rs::heartbeat_may_mark_healthy` and `placement.rs`:

- The node's health becomes `Maintenance` (a Raft write, so every manager agrees).
- **Sticky.** The failure monitor never changes a `Maintenance` node to `Suspect`
  or `Down`, and a heartbeat from the node does not set it back to `Healthy`. Only
  an explicit disable does.
- New client writes are not placed on it (the placement ring contains only
  `Healthy` nodes), and the repair scheduler does not pick it as a repair target.
  The tiering engine's target selection does not exclude it, so cache promotions
  can still land there.
- Reads keep working: its replicas still count as live replicas and are served
  while the process runs.

## What it does not do

- It does **not** drain the node. No chunk is moved or re-replicated because of it.
- It does not make its replicas count as missing, so no repair starts. If the node
  is actually stopped for long, the data on it is simply unavailable (reads fail
  over to other replicas where they exist) until it returns.
- There is no `Draining` workflow: the status exists in the type system but no
  operator API or scheduler path sets it. Evacuating a node is covered in
  [Decommission and Replace](../deployment/decommission-replace.md).
- It applies to registered data nodes. Managers and FUSE clients have no
  maintenance state; the monitor only tracks data nodes.

## Why use it

A stopped data node is marked `Suspect` after `suspect_after_missed` and `Down`
after `down_after_missed` missed heartbeats (3 and 6 times
`heartbeat_interval_secs`, so 15 s and 30 s by default). `Down` triggers
re-replication of its chunks to other nodes. For a planned stop that will outlast
that window (reboot, OS upgrade, disk work) this is the wrong reaction,
especially across the 40 Mbps LELI link where it becomes a backfill storm. A
plain binary restart that finishes in seconds does not need maintenance mode.

## Enabling and disabling

```bash
dtfs-cli --secret "$DTFS_SHARED_SECRET" maintenance <node-id> --enable
dtfs-cli --secret "$DTFS_SHARED_SECRET" maintenance <node-id> --disable
```

REST equivalent (leader or any manager; mutating calls need the shared secret):

```bash
curl -X POST http://<manager>:9402/api/v1/nodes/<node-id>/maintenance \
  -H 'Content-Type: application/json' -H "X-Dtfs-Shared-Secret: $SECRET" \
  -d '{"enable": true}'
```

`<node-id>` is the numeric id from `dtfs-cli nodes` / `GET /api/v1/nodes`. An
unknown id returns 404; a refusal by the state machine returns 409. Disabling sets
the status to `Healthy` immediately; the next heartbeat keeps it there. Every
toggle writes a `maintenance.toggle` audit row.

## Procedure for planned work on a data node

1. Check the cluster is healthy (`dtfs-cli status`: no node `Down`, no missing
   chunks) and that the node is not the only holder of data you cannot do without
   (the anchor node holds the only copy of some chunks; reads of those fail while
   it is down).
2. `maintenance --enable`.
3. `systemctl stop dtfs-data` (graceful: drains in-flight writes up to 30 s), do the
   work, `systemctl start dtfs-data`.
4. Wait until the node heartbeats again (`last_seen` advances; its health stays
   `Maintenance`), then `maintenance --disable`.
5. Confirm `Healthy` in `dtfs-cli nodes --id <node-id>`.

Forgetting step 4 leaves the node out of placement indefinitely; automation must
disable it in an always-run step.

## Manager maintenance

There is no mode. Keep a majority of voters reachable: update or stop one manager
at a time, never two, and restart the leader last (it triggers an election of about
a second). See [Rolling Update](../deployment/rolling-update.md).

## Planned downtime checklist

1. `dtfs-cli status` and `dtfs-cli nodes`: all `Healthy`, `files_missing_chunks` zero.
2. One node (or one failure domain) at a time; never take down all replicas of the
   same data. Stagger across racks and datacenters.
3. FUSE clients see higher latency or read failovers while replicas are away; their
   [partition behaviour](../access/partition-behavior.md) decides what happens if
   managers become unreachable.
4. Silence external alerts for the window ([Alerting](./alerting.md)).
5. Review afterwards: `dtfs-cli audit --action maintenance`.
