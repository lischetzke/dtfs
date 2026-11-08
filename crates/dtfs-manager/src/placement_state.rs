//! Bug BX — per-chunk placement-state classification.
//!
//! Classifies every logical chunk by its actual replica placement vs the
//! volume goal (mandatory per-tier counts + anchor + cache allowance) into
//! mutually-exclusive buckets. Shared by:
//!   - the under-replication / rebalance scheduler (BX-4 / BX-5), so what gets
//!     repaired matches what's reported, and
//!   - the `/api/v1/stats.chunk_placement` metric (BX-6) that the Bug BT
//!     WebUI chunk-redundancy panel renders.
//!
//! Pure functions over `StateMachineData`; no stored state, no snapshot change.

use std::collections::BTreeMap;

use dtfs_common::types::{AnchorPolicy, DiskTier, TierReplication};
use dtfs_raft::state_machine::StateMachineData;

/// The placement state of a single logical chunk relative to its volume goal.
/// Priority order (a chunk is classified by the FIRST that applies):
/// missing > undergoal > misplaced > overgoal > stable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlacementState {
    /// Zero replicas — realized data loss for this chunk.
    Missing,
    /// Fewer replicas than the mandatory total across tiers.
    Undergoal,
    /// Mandatory count met, but the per-tier distribution doesn't satisfy the
    /// policy (a mandated tier is short / a replica is on the wrong tier) or
    /// the anchor replica isn't in the anchor DC at the required tier.
    /// Count is fine; placement is wrong → relocatable (BX-5).
    Misplaced,
    /// More replicas than mandatory + the cache allowance — excess copies the
    /// tiering engine should demote.
    Overgoal,
    /// Count met AND every mandated tier + the anchor are satisfied.
    Stable,
}

/// Cluster-wide distribution of chunk placement states. `pending_deletion`
/// is sourced separately (orphan/GC queue) and folded in by the caller; the
/// classifier itself only produces the five count-vs-goal states.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlacementCounts {
    pub missing: u64,
    pub undergoal: u64,
    pub misplaced: u64,
    pub stable: u64,
    pub overgoal: u64,
    pub pending_deletion: u64,
}

/// Bug BX — classify one logical chunk from its replicas' `(datacenter, tier)`
/// list against the volume goal. `cache_allowance` is the sum of the volume's
/// per-tier cache `max` (the legitimate extra copies beyond mandatory).
pub fn classify_chunk(
    replicas: &[(String, DiskTier)],
    mandatory: &TierReplication,
    anchor: Option<&AnchorPolicy>,
    cache_allowance: u32,
) -> PlacementState {
    let total = replicas.len() as u32;
    let mandatory_total = mandatory.hot + mandatory.warm + mandatory.cold + mandatory.tape;

    if total == 0 {
        return PlacementState::Missing;
    }
    if total < mandatory_total {
        return PlacementState::Undergoal;
    }

    let goal_met_now = goal_met(replicas.iter().map(|(d, t)| (d.as_str(), *t)), mandatory, anchor);
    if !goal_met_now {
        return PlacementState::Misplaced;
    }
    if total > mandatory_total + cache_allowance {
        return PlacementState::Overgoal;
    }
    PlacementState::Stable
}

/// True when some replica satisfies the anchor policy. Datacenter compares
/// case-insensitively (`AnchorPolicy::is_satisfied_by`), the single anchor
/// predicate for the planner, the rebalance pass and the classifier.
pub(crate) fn anchor_met<'a>(
    anchor: &AnchorPolicy,
    mut replicas: impl Iterator<Item = (&'a str, DiskTier)>,
) -> bool {
    replicas.any(|(dc, t)| anchor.is_satisfied_by(dc, Some(t)))
}

/// True when every mandated tier has enough replicas AND the anchor (if any)
/// is satisfied. One helper for classify / spill removal.
pub(crate) fn goal_met<'a>(
    replicas: impl Iterator<Item = (&'a str, DiskTier)> + Clone,
    mandatory: &TierReplication,
    anchor: Option<&AnchorPolicy>,
) -> bool {
    let on_tier = |t: DiskTier| replicas.clone().filter(|(_, tt)| *tt == t).count() as u32;
    on_tier(DiskTier::Hot) >= mandatory.hot
        && on_tier(DiskTier::Warm) >= mandatory.warm
        && on_tier(DiskTier::Cold) >= mandatory.cold
        && on_tier(DiskTier::Tape) >= mandatory.tape
        && anchor.map_or(true, |a| anchor_met(a, replicas.clone()))
}

/// Bug BX — a volume's legitimate extra (cache) copies. With `per_dc` (the
/// default) each tier's `max` applies per datacenter, so the allowance scales
/// with the number of DCs holding replicas (`dcs_holding`, at least 1).
pub(crate) fn cache_allowance(
    cache: &dtfs_common::types::CacheReplication,
    dcs_holding: u32,
) -> u32 {
    let sum = cache.hot.max + cache.warm.max + cache.cold.max + cache.tape.max;
    if cache.per_dc { sum * dcs_holding.max(1) } else { sum }
}

/// Distinct datacenters (case-insensitive) among `dcs`.
pub(crate) fn distinct_dcs<'a>(dcs: impl Iterator<Item = &'a str>) -> u32 {
    let set: std::collections::HashSet<String> = dcs.map(|d| d.to_ascii_lowercase()).collect();
    set.len() as u32
}

/// Bug BX — classify every logical chunk in the state machine and tally the
/// buckets. Walks each file once, grouping chunk-location rows by
/// `chunk_index` so each logical chunk is counted exactly once. Directories
/// and symlinks (no data chunks) are skipped. `pending_deletion` is left 0
/// here; the caller folds in the orphan/GC count.
pub fn count_placement_states(sm: &StateMachineData) -> PlacementCounts {
    let mut counts = PlacementCounts::default();
    for_each_chunk_placement(sm, |v| {
        match v.state {
            PlacementState::Missing => counts.missing += 1,
            PlacementState::Undergoal => counts.undergoal += 1,
            PlacementState::Misplaced => counts.misplaced += 1,
            PlacementState::Overgoal => counts.overgoal += 1,
            PlacementState::Stable => counts.stable += 1,
        }
        if v.dangling {
            counts.pending_deletion += 1;
        }
    });
    counts
}

/// One logical chunk (file path + chunk_index) as seen by the classifier.
/// Borrowed from the state machine; valid only inside the visitor call.
pub struct ChunkPlacementView<'a> {
    pub path: &'a str,
    pub volume_id: &'a str,
    pub chunk_index: u64,
    /// Hash of the first location row for this index (all rows share it).
    pub chunk_id: &'a str,
    /// Every location row for this chunk index, live or not.
    pub rows: Vec<&'a dtfs_raft::ChunkLocationEntry>,
    /// Live rows resolved to `(datacenter, tier)`; what `classify_chunk` saw.
    pub resolved: Vec<(String, DiskTier)>,
    pub state: PlacementState,
    /// True when at least one row is unresolvable (node/disk gone or dead);
    /// counted as `pending_deletion`.
    pub dangling: bool,
}

/// Visit every logical chunk exactly once (per file, per chunk_index, in
/// path then index order) with its classification. Directories and symlinks
/// are skipped. `count_placement_states` is built on this.
pub fn for_each_chunk_placement<'a>(
    sm: &'a StateMachineData,
    mut visit: impl FnMut(ChunkPlacementView<'a>),
) {
    for (path, file_meta) in sm.files.iter() {
        placement_for_file(sm, path.as_str(), file_meta, &mut visit);
    }
}

/// Classify the logical chunks of a single file (skips directories and
/// symlinks). Per-file body of `for_each_chunk_placement`.
pub fn placement_for_file<'a>(
    sm: &'a StateMachineData,
    path: &'a str,
    file_meta: &'a dtfs_raft::state_machine::FileMetadata,
    mut visit: impl FnMut(ChunkPlacementView<'a>),
) {
    {
        if file_meta.is_directory || file_meta.symlink_target.is_some() {
            return;
        }
        let volume = sm.volumes.get(&file_meta.volume_id);
        let mandatory = volume
            .map(|v| v.effective_mandatory_replicas().clone())
            .unwrap_or(TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 });
        let anchor = volume.and_then(|v| v.effective_anchor());

        // (rows, resolved live replicas, dangling) per chunk_index.
        type Group<'g> = (
            Vec<&'g dtfs_raft::ChunkLocationEntry>,
            Vec<(String, DiskTier)>,
            bool,
        );
        let mut by_idx: BTreeMap<u64, Group<'a>> = BTreeMap::new();
        for c in &file_meta.chunks {
            let entry = by_idx.entry(c.chunk_index).or_default();
            entry.0.push(c);
            if let Some((dc, tier)) = resolve_dc_tier(sm, c) {
                entry.1.push((dc, tier));
            } else {
                // Bug BX-12 — unresolvable row (node gone): the replica
                // doesn't count toward placement, and the dangling entry
                // needs cleanup.
                entry.2 = true;
            }
        }

        for (idx, (rows, resolved, dangling)) in by_idx {
            let allowance = volume
                .map(|v| cache_allowance(&v.cache_replicas, distinct_dcs(resolved.iter().map(|(d, _)| d.as_str()))))
                .unwrap_or(0);
            let state = classify_chunk(&resolved, &mandatory, anchor.as_ref(), allowance);
            let chunk_id = rows.first().map(|r| r.chunk_hash.as_str()).unwrap_or("");
            visit(ChunkPlacementView {
                path,
                volume_id: file_meta.volume_id.as_str(),
                chunk_index: idx,
                chunk_id,
                rows,
                resolved,
                state,
                dangling,
            });
        }
    }
}

/// Resolve a chunk-location row to its `(datacenter, tier)` via the node
/// registry + the node's disk list. Returns None if the node or disk is
/// no longer known, **or** if the replica is not serviceable (Bug CH /
/// LIS-41: Down/Failed node or Failed/Removed disk). Pre-LIS-41 this
/// ignored health, so a wiped DC3 cold still counted as Stable /
/// anchor-satisfied and `undergoal`/`anchor_pending` stayed false-green.
fn resolve_dc_tier(
    sm: &StateMachineData,
    c: &dtfs_raft::ChunkLocationEntry,
) -> Option<(String, DiskTier)> {
    let nid = c.node_id.parse::<u64>().ok()?;
    let info = sm.nodes.get(&nid)?;
    // CNF-1 disk_id keying + health gate both live in live_replica_tier.
    let tier = crate::scheduler::live_replica_tier(Some(info), c.disk_id)?;
    Some((info.location.datacenter.clone(), tier))
}

/// Shared mixed fixture for placement + `/api/v1/chunks` tests.
/// Volume `default`: mandatory warm=1 cold=1, anchor `dc3`/cold.
/// Nodes: 1 Healthy dc1/Warm, 2 Down dc3/Cold, 3 Healthy dc3/Cold.
/// Files: /stable.bin (idx0 aa01: n1+n3 Stable), /under.bin (idx0 aa02: n1+n2
/// Undergoal, dangling), /missing.bin (idx0 aa03: n99 only Missing, dangling),
/// /multi.bin (idx0 aa01 Stable, idx1 aa04 n1 only Undergoal), plus a directory
/// and a symlink that must be skipped.
#[cfg(test)]
pub(crate) mod test_fixture {
    use super::*;
    use dtfs_common::types::{
        CacheReplication, DiskInfo, DiskStatus, HealthStatus, NodeRole, SpillPolicy,
        VolumeConfig, VolumeId, WriteSafety,
    };
    use dtfs_raft::state_machine::FileMetadata;

    fn node(health: HealthStatus, dc: &str, tier: DiskTier) -> dtfs_raft::NodeInfo {
        dtfs_raft::NodeInfo {
            hostname: format!("host-{dc}"),
            grpc_addr: String::new(),
            role: NodeRole::Data,
            disks: vec![DiskInfo {
                disk_id: 0,
                uuid: String::new(),
                mount_path: String::new(),
                tier,
                total_bytes: 0,
                used_bytes: 0,
                status: DiskStatus::Healthy,
            }],
            location: dtfs_common::topology::Location::new("", dc, "", ""),
            health,
            total_capacity: 0,
            used_capacity: 0,
            last_seen_ms: 0,
            total_bytes_read: 0,
            total_bytes_written: 0,
            client_ip: None,
            version: String::new(),
            build_date: String::new(),
            quarantine: None,
        }
    }

    fn row(idx: u64, hash: &str, node_id: &str) -> dtfs_raft::ChunkLocationEntry {
        dtfs_raft::ChunkLocationEntry {
            chunk_index: idx,
            chunk_hash: hash.into(),
            node_id: node_id.into(),
            disk_id: 0,
            size_bytes: 1,
        }
    }

    fn file(path: &str, chunks: Vec<dtfs_raft::ChunkLocationEntry>) -> FileMetadata {
        FileMetadata {
            path: path.into(),
            volume_id: "default".into(),
            chunk_size: 4 << 20,
            chunks,
            created_at_ms: 0,
            updated_at_ms: 0,
            symlink_target: None,
            logical_size: None,
            is_directory: false,
            cache_cooldown_until_ms: BTreeMap::new(),
        }
    }

    pub(crate) fn mixed_fixture() -> StateMachineData {
        let mut sm = StateMachineData::default();
        sm.nodes.insert(1, node(HealthStatus::Healthy, "dc1", DiskTier::Warm));
        sm.nodes.insert(2, node(HealthStatus::Down, "dc3", DiskTier::Cold));
        sm.nodes.insert(3, node(HealthStatus::Healthy, "dc3", DiskTier::Cold));
        let mand = TierReplication { hot: 0, warm: 1, cold: 1, tape: 0 };
        sm.volumes.insert(
            "default".into(),
            VolumeConfig {
                id: VolumeId::new("default".into()),
                name: "default".into(),
                chunk_size_bytes: 4 << 20,
                replication: mand.clone(),
                mandatory_replicas: mand,
                cache_replicas: CacheReplication::default(),
                anchor: Some(AnchorPolicy {
                    datacenter: "dc3".into(),
                    required_tier: Some(DiskTier::Cold),
                }),
                spill: SpillPolicy::default(),
                version: 0,
                anchor_datacenter: None,
                write_safety: WriteSafety {
                    min_replicas_before_ack: 1,
                    write_timeout_secs: 30,
                },
            },
        );
        let mut put = |f: FileMetadata| {
            sm.files.insert(f.path.clone(), f);
        };
        put(file("/stable.bin", vec![row(0, "aa01", "1"), row(0, "aa01", "3")]));
        put(file("/under.bin", vec![row(0, "aa02", "1"), row(0, "aa02", "2")]));
        put(file("/missing.bin", vec![row(0, "aa03", "99")]));
        put(file(
            "/multi.bin",
            vec![row(0, "aa01", "1"), row(0, "aa01", "3"), row(1, "aa04", "1")],
        ));
        let mut dir = file("/dir", vec![row(0, "aa05", "1")]);
        dir.is_directory = true;
        put(dir);
        let mut link = file("/link", vec![row(0, "aa06", "1")]);
        link.symlink_target = Some("/stable.bin".into());
        put(link);
        sm
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Old per-file loop kept verbatim as the reference for the visitor.
    fn reference_counts(sm: &StateMachineData) -> PlacementCounts {
        let mut counts = PlacementCounts::default();
        for file_meta in sm.files.values() {
            if file_meta.is_directory || file_meta.symlink_target.is_some() {
                continue;
            }
            let volume = sm.volumes.get(&file_meta.volume_id);
            let mandatory = volume
                .map(|v| v.effective_mandatory_replicas().clone())
                .unwrap_or(TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 });
            let anchor = volume.and_then(|v| v.effective_anchor());
            let mut by_idx: BTreeMap<u64, Vec<(String, DiskTier)>> = BTreeMap::new();
            let mut dangling: std::collections::BTreeSet<u64> = Default::default();
            for c in &file_meta.chunks {
                let entry = by_idx.entry(c.chunk_index).or_default();
                if let Some((dc, tier)) = resolve_dc_tier(sm, c) {
                    entry.push((dc, tier));
                } else {
                    dangling.insert(c.chunk_index);
                }
            }
            for (_idx, replicas) in by_idx {
                let allowance = volume
                    .map(|v| cache_allowance(&v.cache_replicas, distinct_dcs(replicas.iter().map(|(d, _)| d.as_str()))))
                    .unwrap_or(0);
                match classify_chunk(&replicas, &mandatory, anchor.as_ref(), allowance) {
                    PlacementState::Missing => counts.missing += 1,
                    PlacementState::Undergoal => counts.undergoal += 1,
                    PlacementState::Misplaced => counts.misplaced += 1,
                    PlacementState::Overgoal => counts.overgoal += 1,
                    PlacementState::Stable => counts.stable += 1,
                }
            }
            counts.pending_deletion += dangling.len() as u64;
        }
        counts
    }

    #[test]
    fn visitor_counts_equal_reference_on_mixed_fixture() {
        let sm = test_fixture::mixed_fixture();
        let got = count_placement_states(&sm);
        assert_eq!(got, reference_counts(&sm));
        assert_eq!(got.stable, 2);
        assert_eq!(got.undergoal, 2);
        assert_eq!(got.missing, 1);
        assert_eq!(got.pending_deletion, 2);
    }

    #[test]
    fn visitor_exposes_identity_and_rows() {
        let sm = test_fixture::mixed_fixture();
        let mut seen = Vec::new();
        for_each_chunk_placement(&sm, |v| {
            seen.push((v.path.to_string(), v.chunk_index, v.chunk_id.to_string(), v.rows.len(), v.dangling));
        });
        assert_eq!(seen.len(), 5, "dir and symlink skipped: {seen:?}");
        assert!(seen.contains(&("/under.bin".into(), 0, "aa02".into(), 2, true)));
        assert!(seen.contains(&("/multi.bin".into(), 1, "aa04".into(), 1, false)));
    }

    fn mand(hot: u32, warm: u32, cold: u32, tape: u32) -> TierReplication {
        TierReplication { hot, warm, cold, tape }
    }

    #[test]
    fn bx3_missing_when_no_replicas() {
        let m = mand(0, 2, 1, 0);
        assert_eq!(classify_chunk(&[], &m, None, 0), PlacementState::Missing);
    }

    #[test]
    fn bx3_undergoal_below_mandatory_total() {
        let m = mand(0, 2, 1, 0); // total 3
        let reps = vec![("dc-a".to_string(), DiskTier::Warm)];
        assert_eq!(classify_chunk(&reps, &m, None, 0), PlacementState::Undergoal);
    }

    #[test]
    fn bx3_misplaced_count_met_wrong_tier() {
        // Mandatory warm=2,cold=1 (total 3). Three replicas but all warm:
        // count met, cold tier short -> misplaced.
        let m = mand(0, 2, 1, 0);
        let reps = vec![
            ("dc-a".to_string(), DiskTier::Warm),
            ("dc-a".to_string(), DiskTier::Warm),
            ("dc-b".to_string(), DiskTier::Warm),
        ];
        assert_eq!(classify_chunk(&reps, &m, None, 0), PlacementState::Misplaced);
    }

    #[test]
    fn bx3_misplaced_anchor_wrong_dc() {
        // Tiers satisfied but the cold anchor is in the wrong DC.
        let m = mand(0, 0, 1, 0);
        let anchor = AnchorPolicy { datacenter: "dc-a".into(), required_tier: Some(DiskTier::Cold) };
        let reps = vec![("dc-b".to_string(), DiskTier::Cold)];
        assert_eq!(
            classify_chunk(&reps, &m, Some(&anchor), 0),
            PlacementState::Misplaced
        );
    }

    #[test]
    fn bx3_stable_all_satisfied() {
        let m = mand(0, 2, 1, 0);
        let anchor = AnchorPolicy { datacenter: "dc-a".into(), required_tier: Some(DiskTier::Cold) };
        let reps = vec![
            ("dc-b".to_string(), DiskTier::Warm),
            ("dc-b".to_string(), DiskTier::Warm),
            ("dc-a".to_string(), DiskTier::Cold), // anchor
        ];
        assert_eq!(classify_chunk(&reps, &m, Some(&anchor), 0), PlacementState::Stable);
    }

    #[test]
    fn bx3_overgoal_beyond_mandatory_plus_cache() {
        // Mandatory total 1, cache allowance 1 -> >2 replicas is overgoal.
        let m = mand(0, 0, 1, 0);
        let reps = vec![
            ("dc-a".to_string(), DiskTier::Cold),
            ("dc-a".to_string(), DiskTier::Warm),
            ("dc-b".to_string(), DiskTier::Warm),
        ];
        // tiers_ok (cold>=1), anchor None -> ok; total 3 > 1+1 -> overgoal.
        assert_eq!(classify_chunk(&reps, &m, None, 1), PlacementState::Overgoal);
    }

    #[test]
    fn bx3_cache_allowance_keeps_stable() {
        // Same as above but allowance 2 -> 3 <= 1+2 -> stable.
        let m = mand(0, 0, 1, 0);
        let reps = vec![
            ("dc-a".to_string(), DiskTier::Cold),
            ("dc-a".to_string(), DiskTier::Warm),
            ("dc-b".to_string(), DiskTier::Warm),
        ];
        assert_eq!(classify_chunk(&reps, &m, None, 2), PlacementState::Stable);
    }

    // CNF-1 — resolve_dc_tier must key the disk lookup on the stable
    // `DiskInfo.disk_id`, not the Vec position: a reindexed disk list
    // (only disk carries disk_id=1 at Vec index 0) must still resolve.
    #[test]
    fn cnf1_resolve_dc_tier_uses_stable_disk_id() {
        use dtfs_common::types::{DiskInfo, DiskStatus};
        let mut sm = StateMachineData::default();
        sm.nodes.insert(7, dtfs_raft::NodeInfo {
            role: dtfs_common::types::NodeRole::Data,
            hostname: "d".into(),
            grpc_addr: "d:9500".into(),
            location: dtfs_common::topology::Location::new("lab", "dc-a", "r1", "d"),
            disks: vec![DiskInfo {
                disk_id: 1,
                uuid: "u".into(),
                mount_path: "/m".into(),
                tier: DiskTier::Warm,
                total_bytes: 1,
                used_bytes: 0,
                status: DiskStatus::Healthy,
            }],
            total_capacity: 0,
            used_capacity: 0,
            health: dtfs_common::types::HealthStatus::Healthy,
            last_seen_ms: 0,
            total_bytes_read: 0,
            total_bytes_written: 0,
            client_ip: None,
            version: String::new(),
            build_date: String::new(),
            quarantine: None,
        });
        let entry = |disk_id: u32| dtfs_raft::ChunkLocationEntry {
            chunk_index: 0,
            chunk_hash: "h0".into(),
            node_id: "7".into(),
            disk_id,
            size_bytes: 1,
        };
        assert_eq!(
            resolve_dc_tier(&sm, &entry(1)),
            Some(("dc-a".to_string(), DiskTier::Warm)),
            "disk_id=1 lives at Vec index 0 and must resolve"
        );
        assert_eq!(resolve_dc_tier(&sm, &entry(0)), None, "no disk with id 0 exists");
    }

    // ── LIS-41 — Down-node replicas must not satisfy live coverage ──

    fn lis41_disk(tier: DiskTier) -> dtfs_common::types::DiskInfo {
        dtfs_common::types::DiskInfo {
            disk_id: 0,
            uuid: String::new(),
            mount_path: String::new(),
            tier,
            total_bytes: 0,
            used_bytes: 0,
            status: dtfs_common::types::DiskStatus::Healthy,
        }
    }

    fn lis41_node(
        health: dtfs_common::types::HealthStatus,
        dc: &str,
        tier: DiskTier,
    ) -> dtfs_raft::NodeInfo {
        dtfs_raft::NodeInfo {
            hostname: String::new(),
            grpc_addr: String::new(),
            role: dtfs_common::types::NodeRole::Data,
            disks: vec![lis41_disk(tier)],
            location: dtfs_common::topology::Location::new("", dc, "", ""),
            health,
            total_capacity: 0,
            used_capacity: 0,
            last_seen_ms: 0,
            total_bytes_read: 0,
            total_bytes_written: 0,
            client_ip: None,
            version: String::new(),
            build_date: String::new(),
            quarantine: None,
        }
    }

    /// LIS-41 — production post-wipe shape: warm live on DC1 + cold only
    /// on Down dc3. Pre-fix both rows resolved → Stable; post-fix
    /// Down cold drops out → total 1 < mandatory 2 → Undergoal.
    #[test]
    fn lis41_down_cold_does_not_count_as_stable() {
        use dtfs_common::types::{
            AnchorPolicy, CacheReplication, HealthStatus, TierReplication, VolumeConfig,
            VolumeId, WriteSafety,
        };
        use dtfs_raft::state_machine::FileMetadata;

        let mut sm = StateMachineData::default();
        sm.nodes.insert(
            1,
            lis41_node(HealthStatus::Healthy, "dc1", DiskTier::Warm),
        );
        sm.nodes.insert(
            2,
            lis41_node(HealthStatus::Down, "dc3", DiskTier::Cold),
        );
        sm.volumes.insert(
            "default".into(),
            VolumeConfig {
                id: VolumeId::new("default".into()),
                name: "default".into(),
                chunk_size_bytes: 4 << 20,
                replication: TierReplication {
                    hot: 0,
                    warm: 1,
                    cold: 1,
                    tape: 0,
                },
                mandatory_replicas: TierReplication {
                    hot: 0,
                    warm: 1,
                    cold: 1,
                    tape: 0,
                },
                cache_replicas: CacheReplication::default(),
                anchor: Some(AnchorPolicy {
                    datacenter: "dc3".into(),
                    required_tier: Some(DiskTier::Cold),
                }),
                spill: dtfs_common::types::SpillPolicy::default(),
                version: 0,
                anchor_datacenter: None,
                write_safety: WriteSafety {
                    min_replicas_before_ack: 1,
                    write_timeout_secs: 30,
                },
            },
        );
        sm.files.insert(
            "/media.bin".into(),
            FileMetadata {
                path: "/media.bin".into(),
                volume_id: "default".into(),
                chunk_size: 4 << 20,
                chunks: vec![
                    dtfs_raft::ChunkLocationEntry {
                        chunk_index: 0,
                        chunk_hash: "h0".into(),
                        node_id: "1".into(),
                        disk_id: 0,
                        size_bytes: 1,
                    },
                    dtfs_raft::ChunkLocationEntry {
                        chunk_index: 0,
                        chunk_hash: "h0".into(),
                        node_id: "2".into(),
                        disk_id: 0,
                        size_bytes: 1,
                    },
                ],
                created_at_ms: 0,
                updated_at_ms: 0,
                symlink_target: None,
                logical_size: None,
                is_directory: false,
                cache_cooldown_until_ms: std::collections::BTreeMap::new(),
            },
        );

        let c = count_placement_states(&sm);
        assert_eq!(c.stable, 0, "Down cold must not keep the chunk Stable");
        assert_eq!(
            c.undergoal, 1,
            "live warm-only must be Undergoal vs warm=1+cold=1"
        );
        assert_eq!(c.misplaced, 0);
        assert_eq!(c.missing, 0);
    }
}
