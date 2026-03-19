//! Location-aware + tier-aware placement engine — Phase 3 (Clustering).
//!
//! Determines where to place chunks based on disk tier, location constraints,
//! and capacity balance across the cluster. Uses a consistent hash ring from
//! `dtfs_common::hash_ring`.

use std::sync::Arc;

use dtfs_common::hash_ring::{ConsistentHashRing, PlacementRule};
use dtfs_common::topology::Location;
use dtfs_common::types::{DiskTier, FailureDomain, NodeId};
use dtfs_raft::state_machine::StateMachineData;
use parking_lot::RwLock;
use tokio::sync::RwLock as TokioRwLock;

/// A single chunk placement decision.
#[derive(Debug, Clone)]
pub struct ChunkPlacement {
    pub chunk_index: u32,
    /// Ordered list of nodes: first is primary, rest are replicas.
    pub node_ids: Vec<NodeId>,
}

/// Location-aware, tier-aware placement engine.
pub struct PlacementEngine {
    /// Cached hash ring — rebuilt when the node set changes.
    ring: RwLock<ConsistentHashRing>,
}

impl PlacementEngine {
    pub fn new() -> Self {
        Self {
            ring: RwLock::new(ConsistentHashRing::new(256)),
        }
    }

    /// Rebuild the hash ring from the current Raft state machine data.
    pub async fn rebuild_ring(&self, sm: &Arc<TokioRwLock<StateMachineData>>) {
        let data = sm.read().await;
        let mut new_ring = ConsistentHashRing::new(256);

        for (node_id, info) in &data.nodes {
            let nid = NodeId::from_string(node_id.to_string());
            for disk in &info.disks {
                let weight = if disk.total_bytes > 0 {
                    disk.total_bytes as f64 / (1024.0 * 1024.0 * 1024.0 * 100.0)
                } else {
                    1.0
                };
                let weight = weight.max(0.1).min(10.0);
                new_ring.add_node(&nid, disk.disk_id, disk.tier, &info.location, weight);
            }
        }

        *self.ring.write() = new_ring;
    }

    /// Determine chunk placements for a volume.
    ///
    /// Returns one `ChunkPlacement` per chunk, each listing the nodes that
    /// should store replicas of that chunk.
    pub fn place_chunks(
        &self,
        volume_id: &str,
        num_chunks: u32,
        tier: DiskTier,
        replication_factor: u32,
    ) -> Vec<ChunkPlacement> {
        let ring = self.ring.read();

        if ring.is_empty() {
            tracing::warn!("Placement engine: hash ring is empty, no nodes available");
            return Vec::new();
        }

        let rule = PlacementRule {
            tier,
            replica_count: replication_factor,
            failure_domain: FailureDomain::Rack,
            prefer_client_locations: false,
        };

        let mut placements = Vec::with_capacity(num_chunks as usize);

        for i in 0..num_chunks {
            // Derive a deterministic chunk key from volume + index.
            let key = format!("{}:chunk:{}", volume_id, i);
            let hash = xxhash_rust::xxh3::xxh3_128(key.as_bytes());
            let mut chunk_id = [0u8; 32];
            chunk_id[..16].copy_from_slice(&hash.to_be_bytes());

            let pg_id = ring.chunk_to_pg(&chunk_id);
            let nodes = ring.find_nodes_for_pg(pg_id, &rule, &[]);

            placements.push(ChunkPlacement {
                chunk_index: i,
                node_ids: nodes,
            });
        }

        placements
    }

    /// Find nodes for a single placement given explicit client locations.
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
}

impl Default for PlacementEngine {
    fn default() -> Self {
        Self::new()
    }
}
