# Data Distribution

DTFS distributes chunk data across data nodes using a consistent hash ring with placement groups (PGs). The system is tier-aware and location-aware, spreading replicas across failure domains while respecting storage tier requirements. This page covers the hash ring, placement groups, xxhash distribution, node selection, tier filtering, and failure domain spreading.

## Overview

```
  File "report.csv" (384 MB, chunk_size = 128 MB)
      |
      |  Split into 3 chunks
      v
  +----------+    +----------+    +----------+
  | Chunk 0  |    | Chunk 1  |    | Chunk 2  |
  +----+-----+    +----+-----+    +----+-----+
       |               |               |
       |  xxh3_128     |  xxh3_128     |  xxh3_128
       v               v               v
  PG = hash % 256  PG = hash % 256  PG = hash % 256
  PG 42            PG 187           PG 91
       |               |               |
       v               v               v
  Hash ring       Hash ring       Hash ring
  walk for PG42   walk for PG187  walk for PG91
       |               |               |
       v               v               v
  [node-a,         [node-c,        [node-b,
   node-d,          node-a,         node-e,
   node-f]          node-e]         node-c]
  (3 replicas)     (3 replicas)    (3 replicas)
```

## Consistent Hash Ring

### Ring Structure

The hash ring is a sorted map (`BTreeMap<u32, VirtualNode>`) where keys are 32-bit positions on a circular number space (0 to 2^32 - 1). Each data node contributes multiple **virtual nodes** to the ring, proportional to its disk capacity.

```
  Ring positions (0 to 2^32-1, shown as clock):

              0
              |
        node-c*   node-a*
           \      /
            \    /
  2^32-1 ----+---- 2^31
            /    \
           /      \
        node-b*   node-d*
              |
           2^32 * 3/4

  * = one of many virtual nodes for that physical node
```

### Virtual Nodes

Each physical node is mapped to ~150 virtual nodes on the ring (scaled by disk capacity weight). This ensures even distribution even with a small number of physical nodes.

```rust
// From hash_ring.rs
pub fn add_node(
    &mut self,
    node_id: &NodeId,
    disk_id: u32,
    tier: DiskTier,
    location: &Location,
    weight: f64,
) {
    let num_vnodes = (weight * 150.0) as u32;
    for i in 0..num_vnodes {
        let key = format!("{}:{}:{}", node_id.0, disk_id, i);
        let position = hash_position(&key);
        self.ring.insert(position, VirtualNode { ... });
    }
}
```

Each `VirtualNode` carries:

| Field | Type | Purpose |
|-------|------|---------|
| `position` | `u32` | Position on the hash ring |
| `physical_node` | `NodeId` | The actual data node this vnode belongs to |
| `disk_id` | `u32` | Which disk on the node |
| `tier` | `DiskTier` | Storage tier (Hot, Warm, Cold) |
| `location` | `Location` | Topology: region/datacenter/rack/host |

### Capacity-Weighted Placement

The number of virtual nodes is proportional to disk capacity:

```
  weight = disk_capacity_bytes / (100 GB)
  weight = clamp(weight, 0.1, 10.0)
  num_vnodes = weight * 150

  Examples:
    100 GB disk  -> weight 1.0  -> 150 vnodes
    500 GB disk  -> weight 5.0  -> 750 vnodes
    1 TB disk    -> weight 10.0 -> 1500 vnodes (capped)
    10 GB disk   -> weight 0.1  -> 15 vnodes (minimum)
```

Larger disks receive more virtual nodes and therefore occupy a larger fraction of the ring. This naturally directs more chunks to nodes with more capacity.

### Ring Position Hashing

Positions on the ring are computed using xxHash (xxh3_64), truncated to 32 bits:

```rust
fn hash_position(key: &str) -> u32 {
    let hash = xxhash_rust::xxh3::xxh3_64(key.as_bytes());
    (hash & 0xFFFFFFFF) as u32
}
```

xxHash was chosen for its speed and excellent distribution properties. The 64-bit hash is truncated to 32 bits for the ring position, which provides more than enough resolution for the ring.

## Placement Groups

### What Are Placement Groups?

A **Placement Group (PG)** is an indirection layer between chunks and data nodes. Instead of mapping each chunk directly to nodes on the hash ring, chunks are first mapped to a PG, and then the PG is mapped to nodes.

```
  Without PGs (direct mapping):
    chunk_id --> hash ring --> nodes
    (billions of mappings to track)

  With PGs:
    chunk_id --> PG (modulo) --> hash ring --> nodes
    (256 PGs to track, each mapping to a node set)
```

Benefits of PGs:

- **Reduced metadata**: The manager tracks node assignments per PG (256 entries) rather than per chunk (millions).
- **Batch rebalancing**: When a node joins or leaves, only the PGs affected by the ring change need rebalancing, and all chunks in those PGs move together.
- **Predictable movement**: The PG abstraction makes it straightforward to compute which data moves where.

### Chunk-to-PG Mapping

A chunk is mapped to its PG using xxh3_64 of the 32-byte chunk ID, modulo the number of PGs:

```rust
pub fn chunk_to_pg(&self, chunk_id: &[u8; 32]) -> u32 {
    let hash = xxhash_rust::xxh3::xxh3_64(chunk_id);
    (hash % self.num_pgs as u64) as u32
}
```

DTFS uses 256 PGs by default. This number can be configured at cluster creation time but should not be changed after data has been written (as it would invalidate all chunk-to-PG mappings).

### PG-to-Node Mapping

Each PG is assigned to a set of nodes by walking the hash ring clockwise from a deterministic starting position:

```rust
let start_position = hash_position(&format!("pg:{}", pg_id));
```

The ring is walked from `start_position` clockwise (ascending positions, wrapping around), collecting nodes that satisfy the placement constraints.

```
  Ring walk for PG 42:

  Start: hash("pg:42") = position 1,073,741,824

  Walk clockwise:
    pos 1,073,900,000  node-c (Hot, dc-west, rack-1, host-3)
                       -> tier matches, first node selected [1/3]

    pos 1,074,100,000  node-c (Hot, dc-west, rack-1, host-3)
                       -> same host, SKIP

    pos 1,074,300,000  node-a (Hot, dc-east, rack-2, host-1)
                       -> different host, different rack, different DC
                       -> selected [2/3]

    pos 1,074,500,000  node-d (Warm, dc-east, rack-3, host-4)
                       -> wrong tier (Warm != Hot), SKIP

    pos 1,074,800,000  node-b (Hot, dc-east, rack-3, host-2)
                       -> different host, different rack
                       -> selected [3/3]

  Result: PG 42 -> [node-c, node-a, node-b]
                     primary   replica  replica
```

## xxHash Distribution

DTFS uses xxHash (the `xxhash-rust` crate, specifically `xxh3_64` and `xxh3_128`) throughout the placement system:

| Context | Hash Function | Input | Output Use |
|---------|--------------|-------|------------|
| Ring positions | `xxh3_64` | `"{node_id}:{disk_id}:{vnode_index}"` | Truncated to u32 for ring position |
| Chunk-to-PG | `xxh3_64` | 32-byte chunk ID | Modulo num_pgs for PG assignment |
| Chunk key derivation | `xxh3_128` | `"{volume_id}:chunk:{index}"` | First 16 bytes used as chunk ID seed |
| PG start position | `xxh3_64` | `"pg:{pg_id}"` | Truncated to u32 for ring walk start |

xxHash properties relevant to DTFS:

- **Speed**: xxh3 processes several GB/s, making placement calculations negligible.
- **Distribution**: Excellent uniformity ensures chunks spread evenly across PGs and PGs spread evenly across nodes.
- **Determinism**: Same input always produces the same output, which is critical for all nodes to compute identical placements independently.

### Chunk Key Derivation

When creating chunks for a new file, the placement engine derives a deterministic 32-byte chunk key from the volume ID and chunk index:

```rust
let key = format!("{}:chunk:{}", volume_id, chunk_index);
let hash = xxhash_rust::xxh3::xxh3_128(key.as_bytes());
let mut chunk_id = [0u8; 32];
chunk_id[..16].copy_from_slice(&hash.to_be_bytes());
```

This ensures that chunk placement is deterministic and reproducible: given the same volume and chunk index, any node will compute the same PG assignment and node selection.

## Location-Aware Node Selection

### Topology Hierarchy

DTFS models the physical infrastructure as a 4-level hierarchy:

```
  Region
    |
    +-- Datacenter
          |
          +-- Rack
                |
                +-- Host

  Example: us-east / dc-virginia / rack-a3 / host-07
```

Every data node registers its `Location` when joining the cluster. Each virtual node on the hash ring inherits the location of its physical node.

### Failure Domain Spreading

The `PlacementRule` specifies which failure domain to spread replicas across:

```rust
pub struct PlacementRule {
    pub tier: DiskTier,
    pub replica_count: u32,
    pub failure_domain: FailureDomain,
    pub prefer_client_locations: bool,
}
```

The `FailureDomain` enum controls how aggressively replicas are separated:

| Failure Domain | Constraint | Effect |
|---------------|------------|--------|
| `Host` | Each replica on a different host | Survives single-host failures |
| `Rack` | Each replica in a different rack | Survives full-rack failures (power, switch) |
| `Datacenter` | Each replica in a different DC | Survives full-DC outages |

The default for DTFS is `Rack`-level failure domain spreading.

### Selection Algorithm

The `find_nodes_for_pg` method walks the ring clockwise and selects nodes using a two-pass approach:

**Pass 1 (strict):** Apply all constraints.

```
  For each virtual node encountered clockwise:

    1. Tier check:
       Does vnode.tier == rule.tier?
       No  -> skip

    2. Host uniqueness:
       Is vnode.host already in selected_hosts?
       Yes -> skip

    3. Failure domain check:
       If failure_domain == Datacenter:
         Is vnode.datacenter already in selected_dcs?
         Yes AND not all DCs covered -> skip
       If failure_domain == Rack:
         Is vnode.rack already in selected_racks?
         Yes AND not all racks covered -> skip

    4. Client location preference:
       If prefer_client_locations AND this is the first replica:
         Is vnode in the same DC as any client?
         No -> skip

    5. Select this node. Record host, rack, DC.
```

**Pass 2 (relaxed):** If pass 1 did not fill all replica slots (e.g., the cluster does not have enough distinct racks), run a second pass with relaxed failure domain constraints. Only host uniqueness is enforced:

```
  For each virtual node encountered clockwise:

    1. Tier check: same as pass 1
    2. Node uniqueness: is this physical node already selected? Skip if so.
    3. Host uniqueness: is this host already selected? Skip if so.
    4. Select this node.
```

This two-pass approach means the system degrades gracefully: it spreads replicas as widely as possible given the available topology, but never fails to place data just because the ideal spreading is not achievable.

### Failure Domain Example

Consider a 6-node cluster across 2 DCs, with `failure_domain = Datacenter` and `replica_count = 3`:

```
  DC-EAST                          DC-WEST
  +----------+  +----------+      +----------+  +----------+
  | node-a   |  | node-b   |      | node-c   |  | node-d   |
  | rack-1   |  | rack-2   |      | rack-1   |  | rack-2   |
  | Hot SSD  |  | Hot SSD  |      | Hot SSD  |  | Hot SSD  |
  +----------+  +----------+      +----------+  +----------+
  +----------+  +----------+
  | node-e   |  | node-f   |
  | rack-3   |  | rack-4   |
  | Hot SSD  |  | Hot SSD  |
  +----------+  +----------+

  Placement for PG 42 (3 replicas, DC failure domain):

  Pass 1 (strict):
    Walk ring from hash("pg:42"):
      node-a (DC-EAST, rack-1) -> selected [1/3], DCs used: {EAST}
      node-b (DC-EAST, rack-2) -> DC-EAST already used, not all DCs covered -> SKIP
      node-c (DC-WEST, rack-1) -> new DC! selected [2/3], DCs used: {EAST, WEST}
      node-e (DC-EAST, rack-3) -> DC-EAST already used, but all DCs now covered -> selected [3/3]

  Result: [node-a, node-c, node-e]
           DC-EAST  DC-WEST  DC-EAST
```

The algorithm ensures at least one replica in each available DC before placing additional replicas in the same DC.

## Tier Filtering

### Per-Tier Ring Segmentation

Virtual nodes on the ring carry their disk's tier. During the ring walk, any virtual node whose tier does not match the placement rule's required tier is silently skipped.

```
  Ring walk with tier=Hot:

  pos 100:  node-a, disk-0, Hot    -> candidate
  pos 200:  node-b, disk-1, Cold   -> SKIP (wrong tier)
  pos 300:  node-c, disk-0, Hot    -> candidate
  pos 400:  node-d, disk-2, Warm   -> SKIP (wrong tier)
  pos 500:  node-e, disk-0, Hot    -> candidate
```

This means the effective ring for any given tier is a subset of the full ring. Nodes with only cold-tier disks are invisible to hot-tier placements, and vice versa.

### Per-Tier Replication Factors

Each volume configures independent replication factors per tier:

```toml
[volume.replication]
hot  = 3    # NVMe/SSD: 3 replicas for low-latency access
warm = 2    # SSD: 2 replicas (cost/performance balance)
cold = 1    # HDD: 1 replica (capacity optimized)
```

When the placement engine runs for a volume, it uses the replication factor matching the chunk's current tier:

```rust
let rule = PlacementRule {
    tier,
    replica_count: replication_factor,  // From TierReplication
    failure_domain: FailureDomain::Rack,
    prefer_client_locations: false,
};
```

### Tier Migration Impact on Placement

When a chunk migrates from hot to warm tier, the placement engine computes a new placement on the warm-tier ring with the warm replication factor. If hot has 3 replicas and warm has 2, one replica is eventually garbage-collected after the migration completes.

```
  Before migration (hot tier, 3 replicas):
    Chunk "xyz" -> [node-a(SSD), node-c(SSD), node-e(SSD)]

  After migration (warm tier, 2 replicas):
    Chunk "xyz" -> [node-b(SSD), node-d(SSD)]
    Old copies on node-a, node-c, node-e scheduled for GC
```

## Client Location Affinity

### How Affinity Works

When `prefer_client_locations` is enabled in the placement rule, the algorithm preferentially places the **first replica** (primary) in a datacenter where a FUSE client is located. This optimizes for read latency by ensuring the primary copy is close to the readers.

```rust
if rule.prefer_client_locations && !client_locations.is_empty() && selected.is_empty() {
    let in_client_dc = client_locations
        .iter()
        .any(|cl| cl.datacenter == vn.location.datacenter);
    if !in_client_dc {
        continue;  // Skip nodes not in a client's DC
    }
}
```

Affinity only affects the first replica. Subsequent replicas are placed normally, spreading across failure domains. If no node in a client's DC matches, the affinity preference is silently dropped and the normal algorithm takes over.

### Usage in the Placement Engine

The `PlacementEngine::place_with_affinity` method is used when the FUSE client's location is known:

```rust
pub fn place_with_affinity(
    &self,
    chunk_key: &[u8; 32],
    tier: DiskTier,
    replication_factor: u32,
    client_locations: &[Location],
) -> Vec<NodeId> {
    let ring = self.ring.read();
    let rule = PlacementRule {
        tier,
        replica_count: replication_factor,
        failure_domain: FailureDomain::Rack,
        prefer_client_locations: !client_locations.is_empty(),
    };
    let pg_id = ring.chunk_to_pg(chunk_key);
    ring.find_nodes_for_pg(pg_id, &rule, client_locations)
}
```

This is called by the migration trigger when placing a new replica closer to a FUSE client that detected a remote read.

## Ring Rebuilds

The hash ring is rebuilt from scratch whenever the node set changes (node join, node leave, disk added, disk removed). The `PlacementEngine` holds the ring behind a `parking_lot::RwLock` and rebuilds it from the Raft state machine:

```rust
pub async fn rebuild_ring(&self, sm: &Arc<TokioRwLock<StateMachineData>>) {
    let data = sm.read().await;
    let mut new_ring = ConsistentHashRing::new(256);

    for (node_id, info) in &data.nodes {
        for disk in &info.disks {
            let weight = (disk.total_bytes as f64) / (100 GB);
            let weight = weight.clamp(0.1, 10.0);
            new_ring.add_node(&nid, disk.disk_id, disk.tier, &info.location, weight);
        }
    }

    *self.ring.write() = new_ring;
}
```

### Rebalancing on Node Changes

When a node joins or leaves, the ring changes and some PGs will map to different node sets. The manager detects which PGs changed and schedules replication tasks:

```
  Before: node-a, node-b, node-c on ring
  PG 42 -> [node-a, node-c, node-b]

  After: node-d joins
  PG 42 -> [node-a, node-d, node-c]   (node-b replaced by node-d)

  Manager action:
    1. Copy chunks in PG 42 from node-b to node-d
    2. Update chunk locations in Raft
    3. Remove chunks from node-b (for PG 42 only)
```

Because virtual nodes are spread evenly, adding a new node only causes approximately `1/N` of the total data to move (where N is the number of nodes after the addition). This is the key property of consistent hashing.

### Minimal Data Movement Example

```
  4 nodes, 256 PGs, 3 replicas per PG:

  Add 1 node (now 5 nodes):
    Expected PGs affected: ~256 * (1/5) = ~51 PGs
    Expected data movement: ~20% of total data

  Remove 1 node (back to 4 nodes):
    All PGs that had the removed node need new replicas.
    Expected PGs affected: ~256 * (1/4) = ~64 PGs
    Data from the removed node is re-replicated to remaining nodes.
```

## Putting It All Together

### Complete Placement Flow

```
  1. File created: "report.csv" in volume "analytics"
     Volume config: chunk_size=128MB, hot_replicas=3, failure_domain=Rack
         |
  2. File split into chunks:
     chunk-0, chunk-1, chunk-2
         |
  3. For each chunk:
         |
     a. Derive chunk key:
        key = xxh3_128("analytics:chunk:0")
        chunk_id[0..16] = key.to_be_bytes()
         |
     b. Map to placement group:
        pg = xxh3_64(chunk_id) % 256
         |
     c. Walk hash ring from hash("pg:{pg}"):
        - Filter: tier == Hot
        - Filter: unique hosts
        - Spread: across racks (best effort)
        - Prefer: client's DC (if affinity enabled)
         |
     d. Result: ordered list of 3 NodeIds
        [primary, replica-1, replica-2]
         |
  4. Write chunk to primary, replicate to others
         |
  5. Update chunk locations in Raft state machine
```

### Placement Decision Table

| Factor | How It Is Enforced |
|--------|--------------------|
| Storage tier | Virtual nodes with wrong tier are skipped during ring walk |
| Replication factor | Ring walk continues until N nodes are collected |
| Host uniqueness | Always enforced: no two replicas on the same host |
| Rack spreading | Enforced in pass 1, relaxed in pass 2 if insufficient racks |
| DC spreading | Enforced in pass 1, relaxed in pass 2 if insufficient DCs |
| Capacity weighting | More virtual nodes for larger disks = more ring coverage |
| Client affinity | First replica preferentially placed in client's DC |
| Determinism | All inputs are deterministic; any node computes the same result |
