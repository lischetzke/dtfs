//! Unreachable-goal detector: a volume that requires copies on a tier with
//! no registered disks (or more copies than distinct nodes carrying that
//! tier) can never leave `undergoal`. Explains placement state; it does not
//! change classification.

use std::collections::BTreeMap;

use dtfs_common::types::{DiskTier, VolumeConfig};

use crate::audit::AuditLog;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreachableGoal {
    pub tier: DiskTier,
    pub required: u32,
    /// Distinct registered nodes with at least one disk of `tier`.
    pub nodes_with_tier: u32,
}

pub fn tier_label(t: DiskTier) -> &'static str {
    match t {
        DiskTier::Hot => "hot",
        DiskTier::Warm => "warm",
        DiskTier::Cold => "cold",
        DiskTier::Tape => "tape",
    }
}

/// Tiers where the volume's effective mandatory replicas exceed what the
/// cluster can host (zero disks, or fewer nodes than copies).
pub fn unreachable_goals(
    vol: &VolumeConfig,
    nodes: &BTreeMap<u64, dtfs_raft::NodeInfo>,
) -> Vec<UnreachableGoal> {
    let want = vol.effective_mandatory_replicas();
    [DiskTier::Hot, DiskTier::Warm, DiskTier::Cold, DiskTier::Tape]
        .into_iter()
        .filter_map(|tier| {
            let required = want.for_tier(tier);
            let have = nodes
                .values()
                .filter(|n| n.disks.iter().any(|d| d.tier == tier))
                .count() as u32;
            (required > 0 && have < required).then_some(UnreachableGoal {
                tier,
                required,
                nodes_with_tier: have,
            })
        })
        .collect()
}

/// One warn + one audit row per unreachable volume+tier. Returns the count.
pub fn report_unreachable_goals(
    audit: &AuditLog,
    vol: &VolumeConfig,
    nodes: &BTreeMap<u64, dtfs_raft::NodeInfo>,
) -> usize {
    let goals = unreachable_goals(vol, nodes);
    for g in &goals {
        let tier = tier_label(g.tier);
        tracing::warn!(
            volume = %vol.id, tier, required = g.required, nodes_with_tier = g.nodes_with_tier,
            "volume goal unreachable: requires more copies on this tier than the cluster has nodes with it"
        );
        audit.log(
            "system",
            "volume.goal_unreachable",
            &format!("{}:{}", vol.id, tier),
            &format!("required={} nodes_with_tier={}", g.required, g.nodes_with_tier),
        );
    }
    goals.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dtfs_common::types::TierReplication;

    fn vol(hot: u32, warm: u32, cold: u32, tape: u32) -> VolumeConfig {
        let mut v = VolumeConfig::default();
        v.mandatory_replicas = TierReplication { hot, warm, cold, tape };
        v
    }

    fn node(tiers: &[DiskTier]) -> dtfs_raft::NodeInfo {
        use dtfs_common::types::{DiskInfo, DiskStatus, HealthStatus, NodeRole};
        dtfs_raft::NodeInfo {
            role: NodeRole::Data,
            hostname: "d".into(),
            grpc_addr: "d:9500".into(),
            location: dtfs_common::topology::Location::new("", "", "", ""),
            disks: tiers
                .iter()
                .enumerate()
                .map(|(i, t)| DiskInfo {
                    disk_id: i as _,
                    uuid: "u".into(),
                    mount_path: "/mnt/d".into(),
                    tier: *t,
                    total_bytes: 1,
                    used_bytes: 0,
                    status: DiskStatus::Healthy,
                })
                .collect(),
            total_capacity: 0,
            used_capacity: 0,
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

    #[test]
    fn tier_without_disks_is_unreachable() {
        let nodes: BTreeMap<u64, _> =
            [(1, node(&[DiskTier::Cold])), (2, node(&[DiskTier::Warm]))].into();
        let g = unreachable_goals(&vol(3, 1, 1, 0), &nodes);
        assert_eq!(g, vec![UnreachableGoal { tier: DiskTier::Hot, required: 3, nodes_with_tier: 0 }]);
    }

    #[test]
    fn more_copies_than_nodes_is_unreachable_boundary() {
        let nodes: BTreeMap<u64, _> =
            [(1, node(&[DiskTier::Warm, DiskTier::Warm])), (2, node(&[DiskTier::Warm]))].into();
        // 2 warm copies, 2 nodes (3 disks): reachable.
        assert!(unreachable_goals(&vol(0, 2, 0, 0), &nodes).is_empty());
        // 3 warm copies, 2 nodes: unreachable.
        assert_eq!(
            unreachable_goals(&vol(0, 3, 0, 0), &nodes),
            vec![UnreachableGoal { tier: DiskTier::Warm, required: 3, nodes_with_tier: 2 }]
        );
    }

    #[test]
    fn zero_required_tiers_never_reported() {
        assert!(unreachable_goals(&vol(0, 0, 1, 0), &BTreeMap::from([(1, node(&[DiskTier::Cold]))])).is_empty());
    }
}
