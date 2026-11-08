# Data Distribution

Where do a new file's chunks go, and what keeps them there? This page covers write-time placement (hash ring, placement groups, failure domains, the anchor), capacity rules, and what happens afterwards (repair and tiering, not rebalancing).

## Overview

```text
  AllocateChunks(path, n chunks, volume)
        |
        v
  placement key per chunk:  "<volume>:<path>:chunk:<index>"
        |   xxh3_128 -> 16-byte seed -> placement id
        v
  placement group (PG) = xxh3_64(id) % 256
        |
        v
  walk the hash ring clockwise from hash("pg:<n>")
        |  keep vnodes whose disk tier == wanted tier
        |  one replica per host, spread across racks (relaxed if too few)
        |  force one replica into the ANCHOR datacenter
        v
  ordered node list  [primary, replica-1, ...]  --> returned to the client
```

The placement key only chooses nodes. It is not the chunk id: the stored chunk id is the BLAKE3 hash of the bytes, computed by the client after allocation. Mixing the file path into the key spreads single-chunk files across the ring instead of piling the first chunk of every file onto one node.

## Consistent hash ring

The ring is a sorted map of 32-bit positions to virtual nodes. Each healthy disk contributes `weight * 150` virtual nodes, where position = `xxh3_64("<node>:<disk>:<i>")` truncated to 32 bits.

| Field of a virtual node | Meaning |
|-------------------------|---------|
| `physical_node`, `disk_id` | The disk this vnode stands for |
| `tier` | Disk tier (hot, warm, cold, tape) |
| `location` | Region, datacenter, rack, host of the node |

The ring is rebuilt from the Raft state before every allocation, with three filters:

- Only `Healthy` nodes are included. Suspect, Down, Maintenance, Draining and Quarantined nodes receive no new chunks.
- A disk without room is excluded: free space must stay above `max(min_free_bytes, min_free_pct of the disk)` (defaults 1 GiB and 5 percent, `[placement]`).
- A disk's weight is its **free** bytes divided by 100 GiB, clamped to 0.1 through 10, so emptier disks attract proportionally more new chunks.

## Placement groups

A PG is an indirection between a chunk key and its nodes: 256 PGs by default, `PG = xxh3_64(id) % 256`. The ring walk for a PG starts at `hash("pg:<n>")` and collects nodes clockwise. The PG count is fixed for the life of the cluster. PGs make placement deterministic and cheap to compute; they are not a rebalancing unit (see below).

## Node selection

For each chunk the ring walk applies, in order:

1. **Tier.** Virtual nodes whose disk tier differs from the wanted tier are skipped.
2. **One replica per host.** Always enforced.
3. **Failure domain** (`Rack` by default): prefer a rack not yet used. In the strict pass a repeat rack is skipped while unused racks remain.
4. **Relaxed fill.** If the strict pass cannot fill every slot (too few racks or hosts), a second pass keeps only host and node uniqueness. Placement degrades gracefully instead of failing.
5. **Anchor.** If the volume has an anchor datacenter and none of the selected nodes is in it, the last slot is swapped for a node in the anchor datacenter. The tier constraint is relaxed for that swap: in the reference deployment the warm tier has no node in LELI, but the cold `data-primary01` does, so the anchor copy lands on the cold HDD. Datacenter names compare case-insensitively.

```text
  Example: 3 replicas, anchor = leli, wanted tier = warm

  ring walk (warm only):   toro-ssd -> nrb-hdd -> toro-ssd2 ...
  selected:                [toro-ssd, nrb-hdd]  + 1 free slot
  no node in LELI selected -> swap last slot with a LELI node (any tier):
  result:                  [toro-ssd, leli-primary01]  (cold, anchor)
```

## Which tier a new write lands on

The volume's own effective tier decides (the first non-zero of `mandatory_replicas` hot, warm, cold) and its replica count. A client with `write.first_write_tier = "nearest-warm"` asks for the warm tier instead when any healthy warm disk exists; the hint selects a tier, not a datacenter. The replica count is raised to at least `min_replicas_before_ack` so a write cannot be rejected at commit for being too thin. The placement always uses the file's own registered volume.

If the wanted tier is globally full and `placement.allow_capacity_spill` is true (default), the chunk spills to the fastest, then nearest, tier that has room (audited as `placement.spill`). The anchor copy is never spilled out of its datacenter or tier. A volume's `spill` policy can forbid spilling or restrict target tiers and datacenters. Spilled chunks are flagged misplaced and converge back when capacity returns.

## After placement: repair, tiering, excess

Existing chunks do **not** move when the ring changes. A node joining or leaving only affects where later allocations go. What keeps replicas where policy wants them:

| Mechanism | Owner | Effect |
|-----------|-------|--------|
| Under-replication repair | Scheduler | Counts live mandatory replicas per tier; copies from a live source to a new disk when short |
| Anchor-lag repair | Scheduler | Creates the anchor copy when a write landed without one |
| Excess and spill rebalance | Scheduler | Removes one surplus replica per chunk per tick, never below mandatory plus the cache allowance (one allowance per datacenter when `per_dc`), never the anchor |
| Cache promotion and demotion | Tiering engine | Adds and removes cache replicas by read demand |
| Operator migrate, flush, drain | API and scheduler | Explicit moves, all through the same verified-copy path |

See [Replication](../storage/replication.md), [Tiering](../storage/tiering.md) and [Migration](../storage/migration.md).

## Read-side location

Placement decides where copies are. Which copy a client reads is decided on the client by `select_read_replica`: configured `read_location_priority`, then physical distance, then a warm-over-cold tie-break on equal distance. See [Data Flow](./data-flow.md#read).
