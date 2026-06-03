//! Location-aware + tier-aware placement engine — Phase 3 (Clustering).
//!
//! Determines where to place chunks based on disk tier, location constraints,
//! and capacity balance across the cluster. Uses a consistent hash ring from
//! `dtfs_common::hash_ring`.

use std::sync::Arc;

use dtfs_common::hash_ring::{ConsistentHashRing, PlacementRule};
use dtfs_common::topology::Location;
use dtfs_common::types::{DiskTier, FailureDomain, HealthStatus, NodeId};
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
    /// Bug BU — free-space headroom used to exclude near-full disks from
    /// the ring so new chunks route to a disk that can hold them.
    placement_cfg: dtfs_common::config::PlacementConfig,
}

impl PlacementEngine {
    pub fn new(placement_cfg: dtfs_common::config::PlacementConfig) -> Self {
        Self {
            ring: RwLock::new(ConsistentHashRing::new(256)),
            placement_cfg,
        }
    }

    /// Rebuild the hash ring from the current Raft state machine data.
    ///
    /// Skips nodes that aren't `Healthy`: assigning chunks to a `Down` or
    /// `Suspect` node guarantees the write will fail at the data plane while
    /// the metadata plane has already created the file entry. (Fix for F-012.)
    pub async fn rebuild_ring(&self, sm: &Arc<TokioRwLock<StateMachineData>>) {
        let data = sm.read().await;
        let mut new_ring = ConsistentHashRing::new(256);

        for (node_id, info) in &data.nodes {
            if info.health != HealthStatus::Healthy {
                tracing::debug!(
                    node_id = %node_id,
                    hostname = %info.hostname,
                    health = ?info.health,
                    "skipping non-healthy node when rebuilding placement ring",
                );
                continue;
            }
            let nid = NodeId::from_string(node_id.to_string());
            for disk in &info.disks {
                // Bug BU — exclude a disk whose free space is below the
                // configured headroom so new chunks never hash to a
                // (near-)full disk and hard-fail at the data plane. The
                // live repro: data01-warm at 100% kept full ring weight,
                // so every chunk hashing to it returned ENOSPC while
                // data02-warm sat idle with 21 GB free.
                if !self.placement_cfg.has_room(disk.total_bytes, disk.used_bytes) {
                    tracing::warn!(
                        node_id = %node_id,
                        hostname = %info.hostname,
                        disk_id = disk.disk_id,
                        tier = ?disk.tier,
                        total_bytes = disk.total_bytes,
                        used_bytes = disk.used_bytes,
                        "Bug BU — disk below free-space headroom; excluding from placement ring",
                    );
                    continue;
                }
                // Bug BU — weight by AVAILABLE bytes (was total), so a
                // disk with more free space attracts proportionally more
                // new placements than a fuller peer of the same size.
                let free = disk.total_bytes.saturating_sub(disk.used_bytes);
                let weight = if disk.total_bytes > 0 {
                    free as f64 / (1024.0 * 1024.0 * 1024.0 * 100.0)
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
            // P0-10 — anchor enforced via a higher-level wrapper that
            // accepts the volume; for the basic placement path we leave
            // None. Callers wanting anchor enforcement should call the
            // explicit `place_chunks_with_anchor` variant added below.
            required_datacenter: None,
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
            required_datacenter: None,
        };
        let pg_id = ring.chunk_to_pg(chunk_key);
        ring.find_nodes_for_pg(pg_id, &rule, client_locations)
    }

    /// P0-10: place chunks for a volume that may have a `required_datacenter`
    /// anchor. Returns one placement per chunk; if the anchor is set but no
    /// candidate exists, the placement may be missing the anchor — the caller
    /// should verify and audit-log the `placement_anchor_unsatisfiable`
    /// condition.
    pub fn place_chunks_with_anchor(
        &self,
        volume_id: &str,
        placement_seed: &str,
        num_chunks: u32,
        tier: DiskTier,
        replication_factor: u32,
        anchor_datacenter: Option<String>,
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
            required_datacenter: anchor_datacenter,
        };

        let mut placements = Vec::with_capacity(num_chunks as usize);
        for i in 0..num_chunks {
            let key = chunk_placement_key(volume_id, placement_seed, i);
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
}

impl Default for PlacementEngine {
    fn default() -> Self {
        Self::new(dtfs_common::config::PlacementConfig::default())
    }
}

/// Bug BO-3 — ring key for a chunk's placement. Previously keyed on
/// `{volume_id}:chunk:{index}` only, so chunk-index-0 of EVERY file hashed
/// to the same key and landed on the same node — single-chunk files (the
/// common case) all collided onto one data node, saturating it while peers
/// sat at 0%. Mixing in a per-file `placement_seed` (the file path) spreads
/// each file's chunks independently across the ring.
pub(crate) fn chunk_placement_key(volume_id: &str, placement_seed: &str, index: u32) -> String {
    format!("{volume_id}:{placement_seed}:chunk:{index}")
}

#[cfg(test)]
mod bu1_capacity_ring_tests {
    use super::*;
    use dtfs_common::types::{DiskInfo, DiskStatus, DiskTier, HealthStatus, NodeRole};

    fn mk_node(hostname: &str, rack: &str, total: u64, used: u64) -> dtfs_raft::NodeInfo {
        dtfs_raft::NodeInfo {
            role: NodeRole::Data,
            hostname: hostname.to_string(),
            grpc_addr: format!("{hostname}:9500"),
            location: dtfs_common::topology::Location::new("lab", "dc-a", rack, hostname),
            disks: vec![DiskInfo {
                disk_id: 0,
                uuid: "u".into(),
                mount_path: "/mnt/x".into(),
                tier: DiskTier::Warm,
                total_bytes: total,
                used_bytes: used,
                status: DiskStatus::Healthy,
            }],
            total_capacity: total,
            used_capacity: used,
            health: HealthStatus::Healthy,
            last_seen_ms: 0,
            total_bytes_read: 0,
            total_bytes_written: 0,
            client_ip: None,
            version: String::new(),
            build_date: String::new(),
            quarantine: None,
        }
    }

    // Bug BU-1 — a 100%-full disk is excluded from the placement ring, so
    // new chunks route to the peer node with free space instead of
    // hard-failing on ENOSPC (the live data01-warm-full repro).
    #[tokio::test]
    async fn full_disk_excluded_writes_route_to_peer() {
        let gib = 1024 * 1024 * 1024u64;
        let mut sm = StateMachineData::default();
        sm.nodes.insert(1, mk_node("data01", "rack-1", 20 * gib, 20 * gib)); // full
        sm.nodes.insert(2, mk_node("data02", "rack-2", 20 * gib, 0)); // empty
        let sm_arc = Arc::new(TokioRwLock::new(sm));

        let engine = PlacementEngine::new(dtfs_common::config::PlacementConfig::default());
        engine.rebuild_ring(&sm_arc).await;

        let placements =
            engine.place_chunks_with_anchor("default", "/season.mkv", 50, DiskTier::Warm, 1, None);
        assert_eq!(placements.len(), 50, "every chunk gets a placement");

        let mut saw_peer = false;
        for p in &placements {
            for nid in &p.node_ids {
                assert_ne!(nid.0, "1", "full data01 must never receive a placement");
                if nid.0 == "2" {
                    saw_peer = true;
                }
            }
        }
        assert!(saw_peer, "empty data02 should receive the placements");
    }

    // Bug BU-1 — when BOTH disks have room the ring keeps both (no spurious
    // exclusion), so this is not a one-node-only artefact.
    #[tokio::test]
    async fn both_disks_with_room_stay_in_ring() {
        let gib = 1024 * 1024 * 1024u64;
        let mut sm = StateMachineData::default();
        sm.nodes.insert(1, mk_node("data01", "rack-1", 20 * gib, 5 * gib));
        sm.nodes.insert(2, mk_node("data02", "rack-2", 20 * gib, 0));
        let sm_arc = Arc::new(TokioRwLock::new(sm));

        let engine = PlacementEngine::new(dtfs_common::config::PlacementConfig::default());
        engine.rebuild_ring(&sm_arc).await;

        let placements =
            engine.place_chunks_with_anchor("default", "/x.mkv", 80, DiskTier::Warm, 1, None);
        let mut seen = std::collections::HashSet::new();
        for p in &placements {
            for nid in &p.node_ids {
                seen.insert(nid.0.clone());
            }
        }
        assert!(seen.contains("1") && seen.contains("2"), "both eligible nodes used, got {seen:?}");
    }
}

#[cfg(test)]
mod bug_bo3_placement_key_tests {
    use super::chunk_placement_key;

    /// BO-3 — two different files' chunk-0 must yield DIFFERENT placement
    /// keys (so single-chunk files spread across nodes), while the same
    /// (volume, path, index) is stable.
    #[test]
    fn distinct_paths_yield_distinct_keys() {
        let a = chunk_placement_key("default", "/a.bin", 0);
        let b = chunk_placement_key("default", "/b.bin", 0);
        assert_ne!(a, b, "chunk-0 of different files must not collide to one key");
        // Deterministic for the same inputs.
        assert_eq!(a, chunk_placement_key("default", "/a.bin", 0));
        // Index still differentiates within a file.
        assert_ne!(
            chunk_placement_key("default", "/a.bin", 0),
            chunk_placement_key("default", "/a.bin", 1),
        );
    }
}
