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

    let on_tier = |t: DiskTier| replicas.iter().filter(|(_, tt)| *tt == t).count() as u32;
    let tiers_ok = on_tier(DiskTier::Hot) >= mandatory.hot
        && on_tier(DiskTier::Warm) >= mandatory.warm
        && on_tier(DiskTier::Cold) >= mandatory.cold
        && on_tier(DiskTier::Tape) >= mandatory.tape;

    let anchor_ok = match anchor {
        None => true,
        Some(a) => replicas.iter().any(|(dc, t)| {
            dc == &a.datacenter
                && match a.required_tier {
                    Some(rt) => *t == rt,
                    None => true,
                }
        }),
    };

    if !(tiers_ok && anchor_ok) {
        return PlacementState::Misplaced;
    }
    if total > mandatory_total + cache_allowance {
        return PlacementState::Overgoal;
    }
    PlacementState::Stable
}

/// Bug BX — sum of a volume's per-tier cache `max` (legit extra copies).
fn cache_allowance(cache: &dtfs_common::types::CacheReplication) -> u32 {
    cache.hot.max + cache.warm.max + cache.cold.max + cache.tape.max
}

/// Bug BX — classify every logical chunk in the state machine and tally the
/// buckets. Walks each file once, grouping chunk-location rows by
/// `chunk_index` so each logical chunk is counted exactly once. Directories
/// and symlinks (no data chunks) are skipped. `pending_deletion` is left 0
/// here; the caller folds in the orphan/GC count.
pub fn count_placement_states(sm: &StateMachineData) -> PlacementCounts {
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
        let allowance = volume.map(|v| cache_allowance(&v.cache_replicas)).unwrap_or(0);

        let mut by_idx: BTreeMap<u64, Vec<(String, DiskTier)>> = BTreeMap::new();
        for c in &file_meta.chunks {
            let entry = by_idx.entry(c.chunk_index).or_default();
            if let Some((dc, tier)) = resolve_dc_tier(sm, c) {
                entry.push((dc, tier));
            } else {
                // Keep the index present (so a wholly-unresolvable chunk still
                // counts) but with no resolvable replica -> looks like missing.
                let _ = entry;
            }
        }

        for (_idx, replicas) in by_idx {
            match classify_chunk(&replicas, &mandatory, anchor.as_ref(), allowance) {
                PlacementState::Missing => counts.missing += 1,
                PlacementState::Undergoal => counts.undergoal += 1,
                PlacementState::Misplaced => counts.misplaced += 1,
                PlacementState::Overgoal => counts.overgoal += 1,
                PlacementState::Stable => counts.stable += 1,
            }
        }
    }

    counts
}

/// Resolve a chunk-location row to its `(datacenter, tier)` via the node
/// registry + the node's disk list. Returns None if the node or disk is
/// no longer known (the replica doesn't count toward satisfaction).
fn resolve_dc_tier(
    sm: &StateMachineData,
    c: &dtfs_raft::ChunkLocationEntry,
) -> Option<(String, DiskTier)> {
    let nid = c.node_id.parse::<u64>().ok()?;
    let info = sm.nodes.get(&nid)?;
    let tier = info.disks.get(c.disk_id as usize).map(|d| d.tier)?;
    Some((info.location.datacenter.clone(), tier))
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
