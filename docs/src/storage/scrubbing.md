# Scrubbing

Data nodes run periodic scrubs to find corruption, bit rot and metadata damage. A faulty chunk is quarantined (renamed, never deleted), reported to the manager, and repaired from a healthy replica.

## Scrub types

### Quick scrub

Reads no chunk data. For each chunk it stats the `.dat`, parses the `.meta`, and compares the file size with the size recorded in the meta. It catches deletion, truncation and a damaged `.meta`. A size disagreement is not taken at face value: the `.dat` is re-hashed against the chunk id (the content address) and, if it matches, the `.meta` is repaired instead of the chunk being quarantined. Default interval: 6 hours.

### Deep scrub

Re-hashes every `.dat` incrementally with BLAKE3 and compares the digest with `ChunkMeta.checksum`. It detects bit rot that leaves size unchanged. A disagreement is judged against the chunk id, not the `.meta`: only a digest that differs from the chunk id is corruption; a digest equal to the id means the `.meta` was wrong and it is rewritten. Default interval: 7 days.

Chunks with a WAL entry (a put or delete in flight) are skipped, and a chunk deleted while the scan runs is not an error. One unreadable bucket directory or one bad file counts against that chunk only; the disk pass continues.

## What happens to a faulty chunk

```text
  .meta missing, unreadable, or unparseable
        |
        v
  re-hash .dat: digest == chunk id ?
     yes -> rewrite .meta (tmp, fsync, rename)      counted "repaired", healthy
            (if the rewrite itself fails: still healthy, retried next scrub)
     no  -> not repairable
  size mismatch / digest mismatch / unreadable .dat / not repairable
     -> rename .dat to .dtfs/quarantine/<id>.dat.bad   (never remove_file)
     -> queue the id for the next heartbeat
```

The three outcomes of a meta repair (repaired, hash ok but write failed, not repairable) are kept distinct on purpose: only a real digest mismatch or unreadable data justifies quarantine. A healthy chunk is never quarantined because its `.meta` was torn.

Quarantine happens inline, per detection, not in a batch at the end of the pass. Just before the rename, the scrubber takes the chunk's lock and re-hashes the `.dat` against the chunk id once more; a chunk that was re-uploaded or healed since it was flagged is left alone (logged as healthy on re-check).

After quarantine:

1. The id is reported in the next heartbeat. If the data node restarts before reporting, it re-queues every id found in `.dtfs/quarantine/` that has no live `.dat` again, so the report is not lost.
2. The manager removes that `(node, disk, chunk)` location through Raft (`RemoveCorruptChunkReplicas`, which never removes the last row of a chunk index, so reads of a chunk with no good copy fail with EIO rather than return zeros) and the repair scheduler copies the chunk from a healthy replica. Clients are not affected while another healthy replica exists.
3. If no healthy replica exists, the chunk cannot be repaired and an alert is raised.

### quarantine_on_fault

With `quarantine_on_fault = true` (default) a node-wide integrity fault in a heartbeat puts the whole node into the `Quarantined` health state until an operator clears it with `POST /api/v1/nodes/{id}/unquarantine` (or `auto_unquarantine_secs` elapses, if set; default 0 means manual only). With `false`, faults are recorded and repaired but the node keeps serving. Neither a scrub mismatch nor a read-path case records such a fault. A scrub mismatch is reported per disk through `ReportChunkStatus`: the manager removes that disk's replica row (`RemoveCorruptChunkReplicas`) and marks the node `degraded`, without quarantining it. A verify-on-read mismatch quarantines the bad copy and reports it for its disk only, so a good copy on another disk of the same node keeps its replica row. A read error skips the copy and is never recorded as a fault. Disabling `verify_on_read` needs an explicit acknowledgement and is the only mode in which a read can return corrupt bytes.

## Configuration

Scrub policy is cluster-wide. It lives in the manager's `[scrub]` block, is stored in Raft, and reaches every data node in heartbeat responses where it replaces the data node's own `[scrub]` block (kept only as a startup fallback):

```toml
[scrub]                       # manager.toml
quick_interval_secs    = 21600     # 6 hours
deep_interval_secs     = 604800    # 7 days
orphan_interval_secs   = 86400     # orphan cross-check, see Data Flow
orphan_grace_secs      = 86400     # minimum 21600 (6 h); lower fails startup
max_io_percent         = 10        # scrub may use about this share of disk time
verify_on_read         = true
quarantine_on_fault    = true
auto_unquarantine_secs = 0
```

The data node's own fallback block uses `quick_interval_hours` (6) and `deep_interval_hours` (168). See [Manager Configuration](../configuration/manager.md#scrub-optional) and [Data Node](../configuration/data-node.md) for the full reference. There is no scheduling window and no bandwidth setting.

### I/O throttling

Throttling is per chunk: after scrubbing a chunk that took `t` seconds, the scrubber sleeps `t * (100 - p) / p` where `p = max_io_percent`, so scrub uses about `p` percent of the disk's time. The cap is re-read for each disk, so a policy change applies to the next disk.

## Manual triggers

`dtfs-cli scrub --level quick|deep` (REST: `POST /api/v1/scrub {"level": "quick"}`) triggers a **cluster-wide** scrub. The manager raft-replicates the request and each data node runs it once (per-trigger id dedupe) on its next heartbeat, about 5 s. Manual scrubs reset that level's interval timer and obey the same `max_io_percent`. Per-node, per-disk or per-chunk targets are not implemented; the API rejects them with `400`. Results appear in `GET /api/v1/stats` (`scrub_results_by_node`, `last_scrub`) and the WebUI Last Scrub card.

## Monitoring

Per-disk scrub results (checked, healthy, corrupt, missing, size mismatch, repaired meta, duration) are logged by the data node and shipped to the manager with the heartbeat. Cluster counters, including quarantined nodes (`dtfs_nodes_quarantined`), are on the manager `/metrics` endpoint; see [Monitoring](../operations/monitoring.md).
