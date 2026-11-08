//! Location-aware + tier-aware placement engine — Phase 3 (Clustering).
//!
//! Determines where to place chunks based on disk tier, location constraints,
//! and capacity balance across the cluster. Uses a consistent hash ring from
//! `dtfs_common::hash_ring`.

use std::sync::Arc;

use dtfs_common::hash_ring::{ConsistentHashRing, PlacementRule};
use dtfs_common::topology::Location;
use dtfs_common::types::{DiskInfo, DiskTier, FailureDomain, HealthStatus, NodeId};
use dtfs_raft::state_machine::StateMachineData;
use parking_lot::{Mutex, RwLock};
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
    /// Every ring input as of the last build; equal key = identical ring.
    ring_key: Mutex<Option<Vec<RingKeyEntry>>>,
}

/// Per-node ring inputs: (node id, health, location, disks).
type RingKeyEntry = (u64, HealthStatus, Location, Vec<DiskInfo>);

impl PlacementEngine {
    pub fn new(placement_cfg: dtfs_common::config::PlacementConfig) -> Self {
        Self {
            ring: RwLock::new(ConsistentHashRing::new(256)),
            placement_cfg,
            ring_key: Mutex::new(None),
        }
    }

    /// Rebuild the hash ring from the current Raft state machine data.
    ///
    /// Skips nodes that aren't `Healthy`: assigning chunks to a `Down` or
    /// `Suspect` node guarantees the write will fail at the data plane while
    /// the metadata plane has already created the file entry. (Fix for F-012.)
    ///
    /// Returns `false` (ring reused) when every ring input is unchanged since
    /// the last build; `allocate_chunks` calls this per batch.
    pub async fn rebuild_ring(&self, sm: &Arc<TokioRwLock<StateMachineData>>) -> bool {
        let data = sm.read().await;
        let key: Vec<RingKeyEntry> = data
            .nodes
            .iter()
            .map(|(id, n)| (*id, n.health, n.location.clone(), n.disks.clone()))
            .collect();
        // Held across the build so a concurrent caller never sees the new key
        // with the old ring.
        let mut last_key = self.ring_key.lock();
        if last_key.as_ref() == Some(&key) {
            return false;
        }
        *last_key = Some(key);
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
        true
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

/// Bug BX — a capacity-spill candidate: one disk on one healthy node that
/// currently has room (free space above the placement headroom).
#[derive(Debug, Clone)]
pub(crate) struct CandidateDisk {
    pub node_id: u64,
    pub disk_id: u32,
    pub tier: DiskTier,
    pub location: Location,
    pub free_bytes: u64,
}

/// Bug BX — order spill candidates **fastest + nearest** first:
///   1. media speed   — `hot < warm < cold < tape` (`DiskTier::speed_rank`)
///   2. distance      — topological distance from the requester (nearer first)
///   3. free space    — most free first (balance, consistent with BU-1)
///   4. (node_id, disk_id) — deterministic, stable total order tie-break
///
/// Pure + deterministic for a given input (no RNG), so reads/repairs stay
/// stable. The caller filters to disks with room (and excludes the anchor
/// slot) before ranking.
pub(crate) fn rank_candidates(
    candidates: &[CandidateDisk],
    requester: &Location,
) -> Vec<CandidateDisk> {
    let mut out = candidates.to_vec();
    out.sort_by(|a, b| {
        a.tier
            .speed_rank()
            .cmp(&b.tier.speed_rank())
            .then(
                requester
                    .distance(&a.location)
                    .cmp(&requester.distance(&b.location)),
            )
            .then(b.free_bytes.cmp(&a.free_bytes))
            .then(a.node_id.cmp(&b.node_id))
            .then(a.disk_id.cmp(&b.disk_id))
    });
    out
}

#[cfg(test)]
mod bx1_ranking_tests {
    use super::*;
    use dtfs_common::types::DiskTier;

    fn cand(node: u64, disk: u32, tier: DiskTier, dc: &str, free: u64) -> CandidateDisk {
        CandidateDisk {
            node_id: node,
            disk_id: disk,
            tier,
            location: Location::new("lab", dc, "rack-1", &format!("h{node}")),
            free_bytes: free,
        }
    }

    // Bug BX — media speed dominates distance: a faster tier far away beats a
    // slower tier nearby.
    #[test]
    fn speed_beats_distance() {
        let req = Location::new("lab", "dc-a", "rack-1", "writer");
        let cands = vec![
            cand(2, 0, DiskTier::Cold, "dc-a", 100), // near but slow
            cand(1, 0, DiskTier::Hot, "dc-z", 100),  // far but fast
        ];
        let ranked = rank_candidates(&cands, &req);
        assert_eq!(ranked[0].tier, DiskTier::Hot, "fastest tier wins over nearness");
    }

    // Same speed -> nearer wins; same speed+distance -> more free wins.
    #[test]
    fn distance_then_free_break_ties() {
        let req = Location::new("lab", "dc-a", "rack-1", "writer");
        let cands = vec![
            cand(3, 0, DiskTier::Warm, "dc-b", 999), // far, lots free
            cand(2, 0, DiskTier::Warm, "dc-a", 10),  // near, little free
            cand(1, 0, DiskTier::Warm, "dc-a", 50),  // near, more free
        ];
        let ranked = rank_candidates(&cands, &req);
        assert_eq!((ranked[0].node_id, ranked[0].free_bytes), (1, 50));
        assert_eq!(ranked[1].node_id, 2);
        assert_eq!(ranked[2].node_id, 3, "far node last despite most free");
    }

    // Deterministic: same input -> same order; ties broken by disk_id asc.
    #[test]
    fn deterministic_tiebreak() {
        let req = Location::new("lab", "dc-a", "rack-1", "writer");
        let cands = vec![
            cand(2, 1, DiskTier::Warm, "dc-a", 10),
            cand(2, 0, DiskTier::Warm, "dc-a", 10),
        ];
        let a = rank_candidates(&cands, &req);
        let b = rank_candidates(&cands, &req);
        assert_eq!(
            a.iter().map(|c| (c.node_id, c.disk_id)).collect::<Vec<_>>(),
            b.iter().map(|c| (c.node_id, c.disk_id)).collect::<Vec<_>>(),
        );
        assert_eq!((a[0].node_id, a[0].disk_id), (2, 0));
    }
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

    // The ring is rebuilt only when a ring input (node set, health incl.
    // quarantine/maintenance, location, disks incl. free space) changed.
    #[tokio::test]
    async fn ring_rebuilt_only_on_topology_change() {
        let gib = 1024 * 1024 * 1024u64;
        let mut sm = StateMachineData::default();
        sm.nodes.insert(1, mk_node("data01", "rack-1", 20 * gib, 0));
        let sm_arc = Arc::new(TokioRwLock::new(sm));
        let engine = PlacementEngine::new(dtfs_common::config::PlacementConfig::default());

        assert!(engine.rebuild_ring(&sm_arc).await, "first call builds");
        assert!(!engine.rebuild_ring(&sm_arc).await, "unchanged topology reuses the ring");

        sm_arc.write().await.nodes.get_mut(&1).unwrap().health = HealthStatus::Quarantined;
        assert!(engine.rebuild_ring(&sm_arc).await, "health flip rebuilds");
        assert!(engine.place_chunks_with_anchor("default", "/a", 1, DiskTier::Warm, 1, None).is_empty(),
            "quarantined node left the ring");
        sm_arc.write().await.nodes.get_mut(&1).unwrap().health = HealthStatus::Healthy;
        assert!(engine.rebuild_ring(&sm_arc).await, "health flip back rebuilds");
        assert!(!engine.rebuild_ring(&sm_arc).await);

        let mut disk = sm_arc.read().await.nodes[&1].disks[0].clone();
        disk.disk_id = 1;
        sm_arc.write().await.nodes.get_mut(&1).unwrap().disks.push(disk);
        assert!(engine.rebuild_ring(&sm_arc).await, "disk added rebuilds");
        assert!(!engine.rebuild_ring(&sm_arc).await);

        sm_arc.write().await.nodes.get_mut(&1).unwrap().disks[0].used_bytes = gib;
        assert!(engine.rebuild_ring(&sm_arc).await, "free-space change (ring weight) rebuilds");

        sm_arc.write().await.nodes.insert(2, mk_node("data02", "rack-2", 20 * gib, 0));
        assert!(engine.rebuild_ring(&sm_arc).await, "node added rebuilds");
        assert!(!engine.rebuild_ring(&sm_arc).await);
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
