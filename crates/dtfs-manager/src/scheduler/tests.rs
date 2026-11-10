//! Unit-test modules lifted out of `scheduler.rs` so the source file
//! stays readable; every module below sees the parent's private items
//! through the glob import.
#![allow(unused_imports, deprecated)]

use super::*;

#[cfg(test)]
mod rev_leader_gate_tests {
    use super::{
        force_tick_after_skips, leader_gate_open, orphan_sweep_allowed,
        should_quarantine_for_missing, task_budget_available,
        IN_STATE_MISSING_QUARANTINE_THRESHOLD, MAX_CONSECUTIVE_PRESSURE_SKIPS, MAX_LIVE_TASKS,
        MAX_NEW_TASKS_PER_TICK,
    };

    #[test]
    fn only_the_leader_passes_the_gate() {
        assert!(leader_gate_open(Some(7), 7));
        assert!(!leader_gate_open(Some(9), 7), "follower must not dispatch");
        assert!(!leader_gate_open(None, 7), "no quorum: nobody dispatches");
    }

    #[test]
    fn orphan_sweep_requires_leadership() {
        assert!(!orphan_sweep_allowed(false, 100, 100, 5));
    }

    #[test]
    fn orphan_sweep_requires_a_caught_up_state_machine() {
        // The two-week-old manager: elected, but still replaying.
        assert!(!orphan_sweep_allowed(true, 10, 5_000, 5));
        assert!(orphan_sweep_allowed(true, 5_000, 5_000, 5));
    }

    #[test]
    fn orphan_sweep_refuses_an_empty_namespace_view() {
        // "Every chunk on every disk is an orphan" is never a real answer.
        assert!(!orphan_sweep_allowed(true, 100, 100, 0));
    }

    #[test]
    fn rev_3_4_task_creation_is_bounded_per_tick_and_overall() {
        // Room in both budgets.
        assert!(task_budget_available(0, 0));
        assert!(task_budget_available(MAX_NEW_TASKS_PER_TICK - 1, MAX_LIVE_TASKS - 1));
        // Per-tick budget exhausted: the rest waits for the next tick. Losing
        // the 50 TB anchor node must not queue one task per chunk in one pass
        // and serialise them all into a single Raft entry.
        assert!(!task_budget_available(MAX_NEW_TASKS_PER_TICK, 0));
        // Total live backlog ceiling, independent of this tick.
        assert!(!task_budget_available(0, MAX_LIVE_TASKS));
    }

    #[test]
    fn rev_3_5_memory_pause_is_self_limiting() {
        assert!(!force_tick_after_skips(0));
        assert!(!force_tick_after_skips(MAX_CONSECUTIVE_PRESSURE_SKIPS - 1));
        assert!(
            force_tick_after_skips(MAX_CONSECUTIVE_PRESSURE_SKIPS),
            "a pause that never ends is an outage of the only owner of \
             mandatory-replica repair, not backpressure"
        );
        assert!(force_tick_after_skips(MAX_CONSECUTIVE_PRESSURE_SKIPS + 5));
    }

    /// REV-3-10 — a stale location row (state says the node holds bytes it
    /// does not) is self-healing: drop the row and the backfill re-copies from
    /// a replica that really exists. Quarantining the node for it left an
    /// otherwise healthy node unusable forever AND left the chunk
    /// under-replicated, because the stale row still counted as coverage.
    #[test]
    fn rev_3_10_a_reconciled_row_does_not_blame_the_node() {
        // One stale row, healed: nothing to escalate.
        assert!(!should_quarantine_for_missing(1, 0));
        assert!(!should_quarantine_for_missing(
            IN_STATE_MISSING_QUARANTINE_THRESHOLD - 1,
            0
        ));
        // Genuine data loss (the row was the LAST live copy) always escalates,
        // even if it is only one chunk.
        assert!(should_quarantine_for_missing(1, 1));
        // Node-scale loss escalates on volume alone.
        assert!(should_quarantine_for_missing(
            IN_STATE_MISSING_QUARANTINE_THRESHOLD,
            0
        ));
    }
}

#[cfg(test)]
mod bx5_excess_removal_tests {
    use super::plan_excess_spill_removal;
    use dtfs_common::types::{AnchorPolicy, DiskTier, TierReplication};

    // CO-1-1 — replica carries (node, disk_id, dc, tier). Default disk_id 0
    // for the single-disk-per-node cases; the co-located test sets it.
    fn rep(node: u64, dc: &str, tier: DiskTier) -> (u64, u32, String, DiskTier) {
        (node, 0, dc.to_string(), tier)
    }

    fn rep_disk(node: u64, disk: u32, dc: &str, tier: DiskTier) -> (u64, u32, String, DiskTier) {
        (node, disk, dc.to_string(), tier)
    }

    // Spilled extra copy beyond mandatory+cache is removed, keeping mandated
    // tiers + anchor satisfied.
    #[test]
    fn removes_excess_keeps_goal() {
        let m = TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 }; // total 1
        let anchor = AnchorPolicy { datacenter: "dc-a".into(), required_tier: Some(DiskTier::Cold) };
        // cold anchor (node 1) + a spilled hot copy (node 2). allowance 0 -> over.
        let reps = vec![rep(1, "dc-a", DiskTier::Cold), rep(2, "dc-b", DiskTier::Hot)];
        // The removable one is node 2 (removing node 1 would strand the anchor).
        assert_eq!(plan_excess_spill_removal(&reps, &m, Some(&anchor), 0), Some((2, 0)));
    }

    // CO-1-1 — a co-located (node 1, cold anchor on disk 0) + (node 1, warm
    // spill on disk 1) must drop the WARM disk-1 spill, never the cold disk-0
    // anchor. The plan must return the exact (node, disk) of the spill.
    #[test]
    fn drops_colocated_spill_not_anchor() {
        let m = TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 }; // total 1
        let anchor = AnchorPolicy { datacenter: "dc-a".into(), required_tier: Some(DiskTier::Cold) };
        let reps = vec![
            rep_disk(1, 0, "dc-a", DiskTier::Cold), // anchor
            rep_disk(1, 1, "dc-a", DiskTier::Warm), // spill on the SAME node
        ];
        // total 2 > mandatory 1 + allowance 0 -> over. Removing disk 0 strands
        // the anchor, so the planner must target (node 1, disk 1).
        assert_eq!(
            plan_excess_spill_removal(&reps, &m, Some(&anchor), 0),
            Some((1, 1)),
        );
    }

    // Within mandatory+cache allowance -> nothing removed (don't fight tiering).
    #[test]
    fn keeps_legit_cache_replica() {
        let m = TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 };
        let reps = vec![rep(1, "dc-a", DiskTier::Cold), rep(2, "dc-a", DiskTier::Warm)];
        // total 2 == mandatory 1 + cache allowance 1 -> not over.
        assert_eq!(plan_excess_spill_removal(&reps, &m, None, 1), None);
    }

    // Never drops below a mandated tier: removing the only mandated copy is refused.
    #[test]
    fn refuses_to_underreplicate() {
        let m = TierReplication { hot: 0, warm: 2, cold: 0, tape: 0 }; // total 2
        // 2 warm (exactly mandated) -> total 2, not over (allowance 0) -> None.
        let reps = vec![rep(1, "dc-a", DiskTier::Warm), rep(2, "dc-b", DiskTier::Warm)];
        assert_eq!(plan_excess_spill_removal(&reps, &m, None, 0), None);
    }
}

#[cfg(test)]
mod bx4_under_replication_tests {
    use super::under_replicated_tiers;
    use dtfs_common::types::{DiskTier, TierReplication};

    // Bug BX-4 — a chunk on cold-only under warm=2,cold=1 reports warm short
    // by 2 (pre-fix: the warm shortfall was invisible).
    #[test]
    fn detects_missing_mandated_tier() {
        let mandatory = TierReplication { hot: 0, warm: 2, cold: 1, tape: 0 };
        let cold_only = |t: DiskTier| if t == DiskTier::Cold { 1 } else { 0 };
        assert_eq!(
            under_replicated_tiers(cold_only, &mandatory),
            vec![(DiskTier::Warm, 2)]
        );
        // Fully satisfied -> nothing short.
        let ok = |t: DiskTier| match t {
            DiskTier::Warm => 2,
            DiskTier::Cold => 1,
            _ => 0,
        };
        assert!(under_replicated_tiers(ok, &mandatory).is_empty());
        // Cache-only tiers (mandatory 0) are never reported short.
        let none = |_t: DiskTier| 0u32;
        let cache_vol = TierReplication { hot: 0, warm: 0, cold: 0, tape: 0 };
        assert!(under_replicated_tiers(none, &cache_vol).is_empty());
    }
}

#[cfg(test)]
mod bug_ch_live_replica_tests {
    use super::live_replica_tier;
    use dtfs_common::types::{DiskInfo, DiskStatus, DiskTier, HealthStatus, NodeRole};

    fn disk(tier: DiskTier, status: DiskStatus) -> DiskInfo {
        DiskInfo {
            disk_id: 0,
            uuid: String::new(),
            mount_path: String::new(),
            tier,
            total_bytes: 0,
            used_bytes: 0,
            status,
        }
    }

    fn node(health: HealthStatus, disks: Vec<DiskInfo>) -> dtfs_raft::NodeInfo {
        dtfs_raft::NodeInfo {
            hostname: String::new(),
            grpc_addr: String::new(),
            role: NodeRole::Data,
            disks,
            location: dtfs_common::topology::Location::new("", "", "", ""),
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

    // Bug CH — a replica on a Down node must not count toward mandatory
    // tier coverage (pre-fix: tier_of ignored health, so the only cold
    // replica sitting on a dead node satisfied cold=1 and node loss was
    // never repaired by the leader tick).
    #[test]
    fn down_node_replica_does_not_count() {
        let n = node(HealthStatus::Down, vec![disk(DiskTier::Cold, DiskStatus::Healthy)]);
        assert_eq!(live_replica_tier(Some(&n), 0), None);
    }

    #[test]
    fn failed_node_replica_does_not_count() {
        let n = node(HealthStatus::Failed, vec![disk(DiskTier::Cold, DiskStatus::Healthy)]);
        assert_eq!(live_replica_tier(Some(&n), 0), None);
    }

    // Replica on a Failed/Removed disk of an otherwise healthy node is
    // equally dead (the handle_disk_failed case, now owned by the tick).
    #[test]
    fn failed_disk_replica_does_not_count() {
        let n = node(HealthStatus::Healthy, vec![disk(DiskTier::Warm, DiskStatus::Failed)]);
        assert_eq!(live_replica_tier(Some(&n), 0), None);
        let n = node(HealthStatus::Healthy, vec![disk(DiskTier::Warm, DiskStatus::Removed)]);
        assert_eq!(live_replica_tier(Some(&n), 0), None);
    }

    #[test]
    fn healthy_replica_counts() {
        let n = node(HealthStatus::Healthy, vec![disk(DiskTier::Cold, DiskStatus::Healthy)]);
        assert_eq!(live_replica_tier(Some(&n), 0), Some(DiskTier::Cold));
    }

    // Suspect = heartbeat ≤30s overdue. Still counts — a transient stall
    // must not trigger a full re-replication wave (cross-cutting
    // invariant in PLAN-BUG-CH).
    #[test]
    fn suspect_node_replica_still_counts() {
        let n = node(HealthStatus::Suspect, vec![disk(DiskTier::Warm, DiskStatus::Healthy)]);
        assert_eq!(live_replica_tier(Some(&n), 0), Some(DiskTier::Warm));
    }

    #[test]
    fn unknown_node_or_disk_does_not_count() {
        assert_eq!(live_replica_tier(None, 0), None);
        let n = node(HealthStatus::Healthy, vec![disk(DiskTier::Cold, DiskStatus::Healthy)]);
        assert_eq!(live_replica_tier(Some(&n), 7), None); // no disk with id 7
    }

    // #114 — a Maintenance (maybe powered off) or Quarantined (bytes
    // suspect) node is never Down, so it stays `live` for repair coverage
    // but must not authorise removing another copy.
    #[test]
    fn issue_114_maintenance_or_quarantined_is_no_survivor() {
        use super::survivor_replica_tier;
        for h in [HealthStatus::Maintenance, HealthStatus::Quarantined] {
            let n = node(h, vec![disk(DiskTier::Cold, DiskStatus::Healthy)]);
            assert_eq!(live_replica_tier(Some(&n), 0), Some(DiskTier::Cold), "{h:?} still counts for repair");
            assert_eq!(survivor_replica_tier(Some(&n), 0), None, "{h:?} is no survivor");
        }
        let mut n = node(HealthStatus::Healthy, vec![disk(DiskTier::Cold, DiskStatus::Healthy)]);
        n.quarantine = Some(dtfs_common::types::QuarantineInfo {
            since_ms: 1,
            reason: dtfs_common::types::QuarantineReason::OrphanScrubInStateMissing,
            triggering_chunk_id: None,
            fault_count_total: 1,
            last_fault_at_ms: 1,
            last_fault_summary: String::new(),
        });
        assert_eq!(survivor_replica_tier(Some(&n), 0), None, "quarantine record set");
        assert_eq!(survivor_replica_tier(None, 0), None);
    }

    #[test]
    fn issue_114_healthy_suspect_draining_are_survivors() {
        use super::survivor_replica_tier;
        for h in [HealthStatus::Healthy, HealthStatus::Suspect, HealthStatus::Draining] {
            let n = node(h, vec![disk(DiskTier::Cold, DiskStatus::Healthy)]);
            assert_eq!(survivor_replica_tier(Some(&n), 0), Some(DiskTier::Cold), "{h:?}");
        }
        let n = node(HealthStatus::Down, vec![disk(DiskTier::Cold, DiskStatus::Healthy)]);
        assert_eq!(survivor_replica_tier(Some(&n), 0), None, "Down stays excluded");
    }

    // #114 — the flush snapshot marks a Maintenance node's disks not
    // live, so its cold row cannot justify flushing the warm copy.
    #[test]
    fn issue_114_flush_snapshot_maintenance_disk_not_live() {
        let mut sm = super::StateMachineData::default();
        sm.nodes.insert(10, node(HealthStatus::Maintenance, vec![disk(DiskTier::Cold, DiskStatus::Healthy)]));
        sm.nodes.insert(11, node(HealthStatus::Healthy, vec![disk(DiskTier::Cold, DiskStatus::Healthy)]));
        let snap = super::FlushSnapshot::from_sm(&sm, "/");
        assert_eq!(snap.nodes[&10].disk_tiers, vec![(0, DiskTier::Cold, false)]);
        assert_eq!(snap.nodes[&11].disk_tiers, vec![(0, DiskTier::Cold, true)]);
    }

    // CNF-1 — `disk_id` is a STABLE key, not a Vec position. After a
    // data node re-registers with a reindexed disk list, the only disk
    // may carry disk_id=1 at Vec index 0; a positional lookup then
    // resolves the wrong disk (or none) and coverage accounting lies.
    #[test]
    fn disk_resolved_by_stable_id_not_vec_index() {
        let mut d = disk(DiskTier::Cold, DiskStatus::Healthy);
        d.disk_id = 1;
        let n = node(HealthStatus::Healthy, vec![d]);
        assert_eq!(live_replica_tier(Some(&n), 1), Some(DiskTier::Cold));
        assert_eq!(live_replica_tier(Some(&n), 0), None, "no disk with id 0 exists");
    }

    fn entry(hash: &str, node: &str, disk_id: u32) -> super::ChunkLocationEntry {
        super::ChunkLocationEntry {
            chunk_index: 0,
            chunk_hash: hash.to_string(),
            node_id: node.to_string(),
            disk_id,
            size_bytes: 1024,
        }
    }

    /// CR-3-5/MGR-5 — `pick_live_source` must skip a replica sitting on
    /// a Failed disk of an otherwise-healthy node, and pick the next
    /// live one instead of defaulting to the first row in the list.
    #[test]
    fn pick_live_source_skips_dead_disk() {
        let mut nodes = std::collections::BTreeMap::new();
        // Node 1: healthy node, but its ONLY disk (id 0) is Failed.
        nodes.insert(1u64, node(HealthStatus::Healthy, vec![disk(DiskTier::Cold, DiskStatus::Failed)]));
        // Node 2: healthy node, healthy disk.
        nodes.insert(2u64, node(HealthStatus::Healthy, vec![disk(DiskTier::Cold, DiskStatus::Healthy)]));

        let e1 = entry("h", "1", 0);
        let e2 = entry("h", "2", 0);
        let entries: Vec<&super::ChunkLocationEntry> = vec![&e1, &e2];

        assert_eq!(
            super::pick_live_source(&entries, &nodes),
            Some(2),
            "must skip node 1 (dead disk) and pick node 2 (live)"
        );
    }

    /// CR-3-5/MGR-5 — when every candidate replica is dead, there is no
    /// source to copy from.
    #[test]
    fn pick_live_source_none_when_all_dead() {
        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(1u64, node(HealthStatus::Down, vec![disk(DiskTier::Cold, DiskStatus::Healthy)]));
        let e1 = entry("h", "1", 0);
        let entries: Vec<&super::ChunkLocationEntry> = vec![&e1];
        assert_eq!(super::pick_live_source(&entries, &nodes), None);
    }

    /// CR-3-5/MGR-5 — `pick_backfill_target` must skip a node whose only
    /// disk of the mandated tier has no room, and pick a node that does.
    #[test]
    fn pick_backfill_target_skips_full_disk() {
        let healthy_nodes: Vec<(u64, Vec<(DiskTier, bool)>)> = vec![
            (1, vec![(DiskTier::Cold, false)]), // right tier, no room
            (2, vec![(DiskTier::Cold, true)]),  // right tier, has room
        ];
        let exclude = std::collections::HashSet::new();
        assert_eq!(
            super::pick_backfill_target(&healthy_nodes, DiskTier::Cold, &exclude),
            Some(2)
        );
    }

    /// CR-3-5/MGR-5 — an excluded node (already holds the chunk on the
    /// target tier) is never picked even if it has room.
    #[test]
    fn pick_backfill_target_respects_exclude() {
        let healthy_nodes: Vec<(u64, Vec<(DiskTier, bool)>)> =
            vec![(1, vec![(DiskTier::Cold, true)])];
        let mut exclude = std::collections::HashSet::new();
        exclude.insert(1u64);
        assert_eq!(
            super::pick_backfill_target(&healthy_nodes, DiskTier::Cold, &exclude),
            None
        );
    }
}

#[cfg(test)]
mod bug_ch_target_validation_tests {
    use super::validate_task_target;
    use dtfs_common::types::{DiskInfo, DiskStatus, DiskTier, HealthStatus, NodeRole};

    fn disk(tier: DiskTier, status: DiskStatus) -> DiskInfo {
        DiskInfo {
            disk_id: 0,
            uuid: String::new(),
            mount_path: String::new(),
            tier,
            total_bytes: 0,
            used_bytes: 0,
            status,
        }
    }

    fn node(role: NodeRole, health: HealthStatus, disks: Vec<DiskInfo>) -> dtfs_raft::NodeInfo {
        dtfs_raft::NodeInfo {
            hostname: String::new(),
            grpc_addr: String::new(),
            role,
            disks,
            location: dtfs_common::topology::Location::new("", "", "", ""),
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

    // Bug CH — the 2026-06-06 incident left 36k tasks targeting manager
    // and client node ids (monitor round-robin over every registered
    // node). Dispatch must fail these in one tick, not burn
    // MAX_MIGRATION_ATTEMPTS gRPC calls against a manager's raft port.
    #[test]
    fn manager_target_rejected() {
        let n = node(NodeRole::Manager, HealthStatus::Healthy, vec![]);
        assert!(validate_task_target(Some(&n), DiskTier::Warm).is_err());
    }

    #[test]
    fn client_target_rejected() {
        let n = node(NodeRole::Client, HealthStatus::Healthy, vec![]);
        assert!(validate_task_target(Some(&n), DiskTier::Warm).is_err());
    }

    #[test]
    fn missing_node_rejected() {
        assert!(validate_task_target(None, DiskTier::Warm).is_err());
    }

    // Hot was hardcoded by the monitor; a data node without any hot disk
    // can never satisfy the task's tier.
    #[test]
    fn data_node_without_tier_rejected() {
        let n = node(
            NodeRole::Data,
            HealthStatus::Healthy,
            vec![disk(DiskTier::Warm, DiskStatus::Healthy)],
        );
        assert!(validate_task_target(Some(&n), DiskTier::Hot).is_err());
    }

    #[test]
    fn data_node_with_only_dead_disk_of_tier_rejected() {
        let n = node(
            NodeRole::Data,
            HealthStatus::Healthy,
            vec![disk(DiskTier::Warm, DiskStatus::Failed)],
        );
        assert!(validate_task_target(Some(&n), DiskTier::Warm).is_err());
    }

    #[test]
    fn unhealthy_data_node_rejected() {
        let n = node(
            NodeRole::Data,
            HealthStatus::Down,
            vec![disk(DiskTier::Warm, DiskStatus::Healthy)],
        );
        assert!(validate_task_target(Some(&n), DiskTier::Warm).is_err());
    }

    #[test]
    fn healthy_data_node_with_live_tier_accepted() {
        let n = node(
            NodeRole::Data,
            HealthStatus::Healthy,
            vec![disk(DiskTier::Warm, DiskStatus::Healthy)],
        );
        assert!(validate_task_target(Some(&n), DiskTier::Warm).is_ok());
    }
}

#[cfg(test)]
mod endpoint_tests {
    use super::*;

    /// Bug W (part 2) — a URL-formatted stored address passes through
    /// unchanged. Prior bug double-prefixed `http://` and tonic returned
    /// `transport error` for every migration RPC.
    #[test]
    fn bug_w_endpoint_url_passthrough() {
        let stored = "http://10.20.0.157:9500";
        assert_eq!(normalize_endpoint(stored), "http://10.20.0.157:9500");
    }

    /// Pre-Bug-W stored addresses (bare host:port) still get the http://
    /// prefix so we don't break older clusters during an upgrade.
    #[test]
    fn bug_w_endpoint_bare_host_gets_scheme() {
        let stored = "10.20.0.157:9500";
        assert_eq!(normalize_endpoint(stored), "http://10.20.0.157:9500");
    }

    /// HTTPS is also passed through (future-proofing for TLS rollout).
    #[test]
    fn bug_w_endpoint_https_passthrough() {
        let stored = "https://data-warm01:9500";
        assert_eq!(normalize_endpoint(stored), "https://data-warm01:9500");
    }
}

#[cfg(test)]
mod path_migration_tests {
    use super::*;

    fn chunk(idx: u64, hash: &str, node: u64, disk: u32, size: u64) -> ChunkLocationEntry {
        ChunkLocationEntry {
            chunk_index: idx,
            chunk_hash: hash.to_string(),
            node_id: node.to_string(),
            disk_id: disk,
            size_bytes: size,
        }
    }

    fn node(
        dc: &str,
        healthy: bool,
        tiers: &[DiskTier],
    ) -> NodeSummary {
        NodeSummary {
            grpc_addr: "127.0.0.1:9500".to_string(),
            datacenter: dc.to_string(),
            healthy,
            disks: tiers.iter().enumerate().map(|(i, t)| (i as u32, *t)).collect(),
        }
    }

    fn node_info(dc: &str, disks: &[(DiskTier, dtfs_common::types::DiskStatus)]) -> dtfs_raft::NodeInfo {
        use dtfs_common::types::{DiskInfo, HealthStatus, NodeRole};
        dtfs_raft::NodeInfo {
            role: NodeRole::Data,
            hostname: "d".into(),
            grpc_addr: "d:9500".into(),
            location: dtfs_common::topology::Location::new(dc, "", "", ""),
            disks: disks
                .iter()
                .enumerate()
                .map(|(i, (t, s))| DiskInfo {
                    disk_id: i as _,
                    uuid: "u".into(),
                    mount_path: "/mnt/d".into(),
                    tier: *t,
                    total_bytes: 1,
                    used_bytes: 0,
                    status: *s,
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

    /// Bug #8 — the replica sits on node 10's HOT disk; node 10 also has a
    /// cold disk. That is not a cold copy: plan one (node 10 may take it on
    /// its cold disk, it holds no cold replica yet).
    #[test]
    fn replica_on_other_tier_disk_of_target_tier_node_is_not_satisfied() {
        let files = vec![("/m".to_string(), vec![chunk(0, "h0", 10, 0, 1)])];
        let nodes: HashMap<u64, NodeSummary> =
            HashMap::from([(10, node("dc-a", true, &[DiskTier::Hot, DiskTier::Cold]))]);
        let (report, plan) = plan_path_migration(&files, &nodes, DiskTier::Cold, None, 1000);
        assert_eq!(report.already_satisfied, 0);
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].target_node, 10);

        // Positive: the replica on the cold disk (1) satisfies.
        let files = vec![("/m".to_string(), vec![chunk(0, "h0", 10, 1, 1)])];
        let (report, plan) = plan_path_migration(&files, &nodes, DiskTier::Cold, None, 1000);
        assert_eq!((report.already_satisfied, plan.len()), (1, 0));
    }

    /// Bug #8 — a cold replica on a Failed (or Removed) disk is not a cold
    /// copy, and a node whose only cold disk failed is no target.
    #[test]
    fn failed_cold_disk_neither_satisfies_nor_targets() {
        use dtfs_common::types::DiskStatus as S;
        let n10 = node_summary(&node_info("dc-a", &[(DiskTier::Cold, S::Failed), (DiskTier::Hot, S::Healthy)]));
        let n11 = node_summary(&node_info("dc-a", &[(DiskTier::Cold, S::Removed)]));
        let n12 = node_summary(&node_info("dc-a", &[(DiskTier::Cold, S::Healthy)]));
        let nodes: HashMap<u64, NodeSummary> = HashMap::from([(10, n10), (11, n11), (12, n12)]);
        // Replica on node 10's failed cold disk 0 plus its live hot disk 1.
        let files = vec![(
            "/m".to_string(),
            vec![chunk(0, "h0", 10, 0, 1), chunk(0, "h0", 10, 1, 1)],
        )];
        let (report, plan) = plan_path_migration(&files, &nodes, DiskTier::Cold, None, 1000);
        assert_eq!(report.already_satisfied, 0);
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].target_node, 12);
    }

    /// Bug #8 — DC names compare case-insensitively (CLAUDE.md A.3.4).
    #[test]
    fn target_dc_compares_case_insensitively() {
        let nodes: HashMap<u64, NodeSummary> = HashMap::from([
            (10, node("toro", true, &[DiskTier::Hot])),
            (14, node("leli", true, &[DiskTier::Cold])),
        ]);
        let on_leli = vec![("/m".to_string(), vec![chunk(0, "h0", 14, 0, 1)])];
        let (report, _) = plan_path_migration(&on_leli, &nodes, DiskTier::Cold, Some("LELI"), 1000);
        assert_eq!(report.already_satisfied, 1);

        let on_toro = vec![("/m".to_string(), vec![chunk(0, "h0", 10, 0, 1)])];
        let (_, plan) = plan_path_migration(&on_toro, &nodes, DiskTier::Cold, Some("LELI"), 1000);
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].target_node, 14);
        // Negative: another DC never matches.
        let (report, plan) = plan_path_migration(&on_toro, &nodes, DiskTier::Cold, Some("NRB"), 1000);
        assert_eq!((report.no_target, plan.len()), (1, 0));
    }

    /// 1 file, 1 chunk currently on a hot DC-A node. Plan a move to warm
    /// tier with no DC constraint. Expect exactly one task targeting a warm
    /// node (any DC).
    #[test]
    fn plans_replication_to_target_tier() {
        let files = vec![(
            "/movies/Foo.mkv".to_string(),
            vec![chunk(0, "h0", 10, 0, 128)],
        )];
        let nodes: HashMap<u64, NodeSummary> = HashMap::from([
            (10, node("dc-a", true, &[DiskTier::Hot])),
            (12, node("dc-b", true, &[DiskTier::Warm])),
            (14, node("dc-c", true, &[DiskTier::Warm])),
        ]);

        let (report, plan) =
            plan_path_migration(&files, &nodes, DiskTier::Warm, None, 1000);

        assert_eq!(report.files_matched, 1);
        assert_eq!(report.chunks_total, 1);
        assert_eq!(report.already_satisfied, 0);
        assert_eq!(report.no_source, 0);
        assert_eq!(report.no_target, 0);
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].source_node, 10);
        // 12 < 14 so deterministic ordering picks node 12.
        assert_eq!(plan[0].target_node, 12);
        assert_eq!(plan[0].tier, DiskTier::Warm);
    }

    /// Same setup but require target_dc = "dc-c". Target must be node 14
    /// even though node 12 is also a warm-tier candidate.
    #[test]
    fn target_dc_constraint_is_enforced() {
        let files = vec![(
            "/movies/Foo.mkv".to_string(),
            vec![chunk(0, "h0", 10, 0, 128)],
        )];
        let nodes: HashMap<u64, NodeSummary> = HashMap::from([
            (10, node("dc-a", true, &[DiskTier::Hot])),
            (12, node("dc-b", true, &[DiskTier::Warm])),
            (14, node("dc-c", true, &[DiskTier::Warm])),
        ]);

        let (report, plan) = plan_path_migration(
            &files,
            &nodes,
            DiskTier::Warm,
            Some("dc-c"),
            1000,
        );

        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].target_node, 14);
        assert_eq!(report.tasks_scheduled, 1);
    }

    /// If at least one existing replica already satisfies the constraint,
    /// no task should be scheduled (additive semantics).
    #[test]
    fn already_satisfied_chunks_are_skipped() {
        let files = vec![(
            "/movies/Bar.mkv".to_string(),
            vec![
                chunk(0, "h0", 10, 0, 128), // dc-a, hot
                chunk(0, "h0", 14, 0, 128), // dc-c, warm
            ],
        )];
        let nodes: HashMap<u64, NodeSummary> = HashMap::from([
            (10, node("dc-a", true, &[DiskTier::Hot])),
            (12, node("dc-b", true, &[DiskTier::Warm])),
            (14, node("dc-c", true, &[DiskTier::Warm])),
        ]);

        let (report, plan) = plan_path_migration(
            &files,
            &nodes,
            DiskTier::Warm,
            Some("dc-c"),
            1000,
        );

        assert_eq!(report.already_satisfied, 1);
        assert_eq!(plan.len(), 0);
    }

    /// Only files starting with `path_prefix` are considered. Matching is
    /// `starts_with` against the absolute path stored in the SM.
    #[test]
    fn path_prefix_filters_files() {
        let files = vec![
            (
                "/movies/Season 1/E01.mkv".to_string(),
                vec![chunk(0, "h0", 10, 0, 128)],
            ),
            (
                "/movies/Season 2/E01.mkv".to_string(),
                vec![chunk(0, "h1", 10, 0, 128)],
            ),
            (
                "/audiobooks/Foo.m4b".to_string(),
                vec![chunk(0, "h2", 10, 0, 128)],
            ),
        ];
        let nodes: HashMap<u64, NodeSummary> = HashMap::from([
            (10, node("dc-a", true, &[DiskTier::Hot])),
            (14, node("dc-c", true, &[DiskTier::Warm])),
        ]);

        // Filter to "/movies/Season 1/" — only the first file should match.
        let (report, plan) = plan_path_migration(
            &files,
            &nodes,
            DiskTier::Warm,
            None,
            1000,
        );

        // Sanity: with no prefix all three match.
        assert_eq!(report.files_matched, 3);

        // Now actually invoke the planner with a filtered slice — emulates
        // what the async wrapper does inside the read-lock.
        let filtered: Vec<_> = files
            .iter()
            .filter(|(p, _)| p.starts_with("/movies/Season 1/"))
            .cloned()
            .collect();
        let (r2, p2) =
            plan_path_migration(&filtered, &nodes, DiskTier::Warm, None, 1000);
        assert_eq!(r2.files_matched, 1);
        assert_eq!(p2.len(), 1);
        let _ = plan; // suppress unused
    }

    /// When the only existing replica is on a Down node we cannot copy
    /// from it. The chunk is reported as `no_source`.
    #[test]
    fn no_healthy_source_is_reported() {
        let files = vec![(
            "/movies/Foo.mkv".to_string(),
            vec![chunk(0, "h0", 10, 0, 128)],
        )];
        let nodes: HashMap<u64, NodeSummary> = HashMap::from([
            (10, node("dc-a", false /* down */, &[DiskTier::Hot])),
            (12, node("dc-b", true, &[DiskTier::Warm])),
        ]);

        let (report, plan) =
            plan_path_migration(&files, &nodes, DiskTier::Warm, None, 1000);
        assert_eq!(report.no_source, 1);
        assert_eq!(report.no_target, 0);
        assert_eq!(plan.len(), 0);
        assert_eq!(report.examples.len(), 1);
        assert_eq!(report.examples[0].reason, "no_source");
    }

    /// No node in the requested DC has the requested tier disk → `no_target`.
    #[test]
    fn no_matching_target_dc_is_reported() {
        let files = vec![(
            "/movies/Foo.mkv".to_string(),
            vec![chunk(0, "h0", 10, 0, 128)],
        )];
        let nodes: HashMap<u64, NodeSummary> = HashMap::from([
            (10, node("dc-a", true, &[DiskTier::Hot])),
            (12, node("dc-b", true, &[DiskTier::Warm])),
            // dc-c has no warm-tier node.
            (14, node("dc-c", true, &[DiskTier::Cold])),
        ]);

        let (report, plan) = plan_path_migration(
            &files,
            &nodes,
            DiskTier::Warm,
            Some("dc-c"),
            1000,
        );
        assert_eq!(report.no_target, 1);
        assert_eq!(plan.len(), 0);
        assert_eq!(report.examples[0].reason, "no_target");
    }

    /// Two files, one chunk each, both need a warm-DC-C copy. Targets must
    /// be different nodes so two chunks don't pile onto a single node when
    /// alternatives exist.
    #[test]
    fn spreads_chunks_across_target_nodes_when_possible() {
        let files = vec![
            (
                "/movies/A.mkv".to_string(),
                vec![chunk(0, "h0", 10, 0, 128)],
            ),
            (
                "/movies/B.mkv".to_string(),
                vec![chunk(0, "h1", 10, 0, 128)],
            ),
        ];
        let nodes: HashMap<u64, NodeSummary> = HashMap::from([
            (10, node("dc-a", true, &[DiskTier::Hot])),
            (14, node("dc-c", true, &[DiskTier::Warm])),
            (15, node("dc-c", true, &[DiskTier::Warm])),
        ]);

        let (_report, plan) = plan_path_migration(
            &files,
            &nodes,
            DiskTier::Warm,
            Some("dc-c"),
            1000,
        );
        assert_eq!(plan.len(), 2);
        // Distinct chunk hashes, so already_targeted only kicks in within
        // the same hash. Both chunks should still get warm-DC-C targets.
        for action in &plan {
            assert!(action.target_node == 14 || action.target_node == 15);
        }
    }

    /// max_tasks limits how many tasks we plan. The rest are counted under
    /// truncated_chunks so the operator knows to re-run.
    #[test]
    fn max_tasks_caps_the_plan() {
        let files: Vec<_> = (0..5u64)
            .map(|i| {
                (
                    format!("/movies/{i}.mkv"),
                    vec![chunk(0, &format!("h{i}"), 10, 0, 128)],
                )
            })
            .collect();
        let nodes: HashMap<u64, NodeSummary> = HashMap::from([
            (10, node("dc-a", true, &[DiskTier::Hot])),
            (14, node("dc-c", true, &[DiskTier::Warm])),
            (15, node("dc-c", true, &[DiskTier::Warm])),
        ]);

        let (report, plan) = plan_path_migration(
            &files,
            &nodes,
            DiskTier::Warm,
            Some("dc-c"),
            2, // cap
        );
        assert_eq!(plan.len(), 2);
        assert_eq!(report.chunks_total, 5);
        assert_eq!(report.truncated_chunks, 3);
    }

    /// All replicas already on warm-DC-C → planner produces nothing.
    #[test]
    fn no_op_when_all_chunks_satisfied() {
        let files = vec![(
            "/movies/Foo.mkv".to_string(),
            vec![
                chunk(0, "h0", 14, 0, 128),
                chunk(1, "h1", 15, 0, 128),
            ],
        )];
        let nodes: HashMap<u64, NodeSummary> = HashMap::from([
            (14, node("dc-c", true, &[DiskTier::Warm])),
            (15, node("dc-c", true, &[DiskTier::Warm])),
        ]);

        let (report, plan) = plan_path_migration(
            &files,
            &nodes,
            DiskTier::Warm,
            Some("dc-c"),
            1000,
        );
        assert_eq!(plan.len(), 0);
        assert_eq!(report.already_satisfied, 2);
    }

    /// Multi-chunk file with multiple replicas per chunk: planner should
    /// still consider each chunk_index exactly once.
    #[test]
    fn multi_chunk_file_groups_by_chunk_index() {
        let files = vec![(
            "/movies/Big.mkv".to_string(),
            vec![
                chunk(0, "h0", 10, 0, 128),
                chunk(0, "h0", 11, 0, 128), // replica of the same chunk_index
                chunk(1, "h1", 10, 0, 128),
            ],
        )];
        let nodes: HashMap<u64, NodeSummary> = HashMap::from([
            (10, node("dc-a", true, &[DiskTier::Hot])),
            (11, node("dc-a", true, &[DiskTier::Hot])),
            (14, node("dc-c", true, &[DiskTier::Warm])),
        ]);

        let (report, plan) = plan_path_migration(
            &files,
            &nodes,
            DiskTier::Warm,
            Some("dc-c"),
            1000,
        );
        assert_eq!(report.chunks_total, 2); // chunk_index 0 + 1, not 3 rows
        assert_eq!(plan.len(), 2);
    }
}

#[cfg(test)]
mod bug_av_flush_cache_tests {
    //! Bug AV (AV-1-7) — pure-planner tests for the cache-flush
    //! routine. The async wrapper that dispatches DeleteChunkReplica +
    //! issues Raft writes is covered live (AV-1-7 "Done when"
    //! verification on the minicluster); these tests cover the
    //! decision logic.
    use super::*;

    fn ent(chunk_index: u64, hash: &str, node: u64, disk: u32) -> ChunkLocationEntry {
        ChunkLocationEntry {
            chunk_index,
            chunk_hash: hash.to_string(),
            node_id: node.to_string(),
            disk_id: disk,
            size_bytes: 1024,
        }
    }

    fn node_summary(dc: &str, tiers: &[DiskTier]) -> FlushNodeSummary {
        // disk_id == position here; CNF-1 sparse-id coverage builds its
        // own summary below. All disks live by default (see
        // node_summary_dead for the CR-3-1 liveness tests).
        node_summary_with_liveness(dc, &tiers.iter().map(|t| (*t, true)).collect::<Vec<_>>())
    }

    /// CR-3-1/MGR-2 — build a node summary where each disk's liveness is
    /// specified explicitly, so tests can put a mandatory-tier replica on
    /// a dead placement and assert it is excluded from
    /// `mandatory_survives`.
    fn node_summary_with_liveness(dc: &str, tiers: &[(DiskTier, bool)]) -> FlushNodeSummary {
        FlushNodeSummary {
            datacenter: dc.to_string(),
            disk_tiers: tiers
                .iter()
                .enumerate()
                .map(|(i, (t, live))| (i as u32, *t, *live))
                .collect(),
        }
    }

    fn mandatory_cold_only() -> TierReplication {
        TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 }
    }

    fn mandatory_warm_and_cold() -> TierReplication {
        TierReplication { hot: 0, warm: 1, cold: 1, tape: 0 }
    }

    /// AV-1-7 test 1 — happy path: a file with one cold (mandatory) +
    /// one warm (cache) replica has its warm replica flushed; cold
    /// preserved.
    #[test]
    fn bug_av_plan_flush_drops_warm_keeps_cold() {
        let snapshot = FlushSnapshot {
            files: vec![(
                "/movies/wicked.mkv".to_string(),
                vec![
                    ent(0, "h", 10, 0), // node 10 = cold
                    ent(0, "h", 11, 0), // node 11 = warm
                ],
                mandatory_cold_only(),
                None,
            )],
            nodes: HashMap::from([
                (10u64, node_summary("leli", &[DiskTier::Cold])),
                (11u64, node_summary("toro", &[DiskTier::Warm])),
            ]),
        };
        let (actions, report) =
            plan_flush_cache(&snapshot, &[DiskTier::Warm], 1000);
        assert_eq!(actions.len(), 1, "drops the warm replica");
        assert_eq!(actions[0].node_id, 11);
        assert_eq!(actions[0].chunk_id, "h");
        assert_eq!(actions[0].chunk_index, 0);
        assert_eq!(report.replicas_dropped, 0, "planner doesn't run the deletes — wrapper does");
        assert_eq!(report.mandatory_refusals, 0);
    }

    /// CR-3-1/MGR-2 — a cold (mandatory) replica on a Down node must NOT
    /// count as the surviving anchor: flushing the warm cache replica
    /// would leave zero LIVE replicas, so the planner must refuse.
    #[test]
    fn cr_3_1_flush_refused_when_mandatory_replica_is_on_dead_node() {
        let snapshot = FlushSnapshot {
            files: vec![(
                "/movies/wicked.mkv".to_string(),
                vec![
                    ent(0, "h", 10, 0), // node 10 = cold, but DOWN
                    ent(0, "h", 11, 0), // node 11 = warm (cache)
                ],
                mandatory_cold_only(),
                None,
            )],
            nodes: HashMap::from([
                (
                    10u64,
                    node_summary_with_liveness("leli", &[(DiskTier::Cold, false)]),
                ),
                (11u64, node_summary("toro", &[DiskTier::Warm])),
            ]),
        };
        let (actions, report) =
            plan_flush_cache(&snapshot, &[DiskTier::Warm], 1000);
        assert!(actions.is_empty(), "refused: mandatory copy is dead");
        assert_eq!(report.mandatory_refusals, 1);

        // Same layout, but the cold node is healthy → flush proceeds.
        let mut healthy_snapshot = snapshot.clone();
        healthy_snapshot.nodes.insert(10u64, node_summary("leli", &[DiskTier::Cold]));
        let (actions, report) =
            plan_flush_cache(&healthy_snapshot, &[DiskTier::Warm], 1000);
        assert_eq!(actions.len(), 1, "allowed: mandatory copy is live");
        assert_eq!(report.mandatory_refusals, 0);
    }

    /// CNF-1 — the flush planner resolves a replica's tier by the STABLE
    /// disk_id key. Node 11's only disk carries disk_id=1 (reindexed
    /// list); its warm cache replica must still be recognised and
    /// flushed (pre-fix the positional lookup missed → replica treated
    /// as non-cache and never flushed).
    #[test]
    fn cnf1_flush_resolves_tier_by_stable_disk_id() {
        let snapshot = FlushSnapshot {
            files: vec![(
                "/movies/wicked.mkv".to_string(),
                vec![
                    ent(0, "h", 10, 0), // node 10 = cold
                    ent(0, "h", 11, 1), // node 11 = warm on disk_id=1
                ],
                mandatory_cold_only(),
                None,
            )],
            nodes: HashMap::from([
                (10u64, node_summary("leli", &[DiskTier::Cold])),
                (11u64, FlushNodeSummary {
                    datacenter: "toro".to_string(),
                    disk_tiers: vec![(1, DiskTier::Warm, true)], // sparse: id 1 at index 0
                }),
            ]),
        };
        let (actions, report) =
            plan_flush_cache(&snapshot, &[DiskTier::Warm], 1000);
        assert_eq!(actions.len(), 1, "warm replica on disk_id=1 is flushed");
        assert_eq!(actions[0].node_id, 11);
        // CO-1-1 — the flush action carries the exact (node, disk) so the
        // delete/RemoveChunkLocation target only this disk's copy.
        assert_eq!(actions[0].disk_id, 1);
        assert_eq!(actions[0].tier, DiskTier::Warm);
        assert_eq!(report.mandatory_refusals, 0);
    }

    /// AV-1-7 test 2 — refuse to flush if doing so would leave no
    /// mandatory-tier replica. Volume mandates 1 cold, file only has
    /// warm replicas (state shouldn't normally happen, but defence
    /// in depth) → refuse.
    #[test]
    fn bug_av_plan_flush_refuses_when_anchor_only_on_warm() {
        let snapshot = FlushSnapshot {
            files: vec![(
                "/movies/oops.mkv".to_string(),
                vec![
                    ent(0, "h", 11, 0), // node 11 = warm (only)
                    ent(0, "h", 12, 0), // node 12 = warm (only)
                ],
                mandatory_cold_only(),
                None,
            )],
            nodes: HashMap::from([
                (11u64, node_summary("toro", &[DiskTier::Warm])),
                (12u64, node_summary("nrb",  &[DiskTier::Warm])),
            ]),
        };
        let (actions, report) =
            plan_flush_cache(&snapshot, &[DiskTier::Warm], 1000);
        assert!(actions.is_empty(), "no actions when mandatory not satisfied");
        assert_eq!(report.mandatory_refusals, 1);
    }

    /// AV-1-7 test 3 — `max_tasks` caps the planned actions; truncated
    /// counter reports the remaining unseen.
    #[test]
    fn bug_av_plan_flush_respects_max_tasks() {
        // 5 files, each with 1 chunk, each with cold + warm replicas.
        let files: Vec<_> = (0..5u64)
            .map(|i| {
                (
                    format!("/movies/f{i}.bin"),
                    vec![ent(0, &format!("h{i}"), 10, 0), ent(0, &format!("h{i}"), 11, 0)],
                    mandatory_cold_only(),
                    None,
                )
            })
            .collect();
        let snapshot = FlushSnapshot {
            files,
            nodes: HashMap::from([
                (10u64, node_summary("leli", &[DiskTier::Cold])),
                (11u64, node_summary("toro", &[DiskTier::Warm])),
            ]),
        };
        let (actions, report) =
            plan_flush_cache(&snapshot, &[DiskTier::Warm], 3);
        assert_eq!(actions.len(), 3, "max_tasks honoured");
        assert!(report.truncated > 0, "remaining counted as truncated");
    }

    /// AV-1-7 test 4 — no cache-tier replicas → empty plan, no refusals.
    #[test]
    fn bug_av_plan_flush_empty_when_no_cache_tier_replicas() {
        let snapshot = FlushSnapshot {
            files: vec![(
                "/movies/cold-only.mkv".to_string(),
                vec![ent(0, "h", 10, 0)], // cold only
                mandatory_cold_only(),
                None,
            )],
            nodes: HashMap::from([
                (10u64, node_summary("leli", &[DiskTier::Cold])),
            ]),
        };
        let (actions, report) =
            plan_flush_cache(&snapshot, &[DiskTier::Warm], 1000);
        assert!(actions.is_empty());
        assert_eq!(report.mandatory_refusals, 0);
        assert_eq!(report.files_matched, 1);
        assert_eq!(report.chunks_examined, 1);
    }

    /// AV-1-11 test 1 — every stranded chunk produces a `FlushRefusal`
    /// with the stable `"mandatory_strand"` reason code. The async
    /// dispatcher converts each entry to one `cache.flush_refused`
    /// audit row + one deduped `AnchorViolated` alert (live-verified;
    /// see verification log).
    #[test]
    fn bug_av_planner_records_refusal_per_stranded_chunk() {
        // Two stranded chunks: same file, different indexes.
        let snapshot = FlushSnapshot {
            files: vec![(
                "/movies/oops.mkv".to_string(),
                vec![
                    ent(0, "h0", 11, 0), // warm only
                    ent(0, "h0", 12, 0), // warm only
                    ent(1, "h1", 11, 0), // warm only
                    ent(1, "h1", 12, 0), // warm only
                ],
                mandatory_cold_only(),
                None,
            )],
            nodes: HashMap::from([
                (11u64, node_summary("toro", &[DiskTier::Warm])),
                (12u64, node_summary("nrb",  &[DiskTier::Warm])),
            ]),
        };
        let (actions, report) =
            plan_flush_cache(&snapshot, &[DiskTier::Warm], 1000);
        assert!(actions.is_empty(), "nothing to drop without mandatory cold");
        assert_eq!(report.mandatory_refusals, 2);
        assert_eq!(report.refusals.len(), 2);
        for r in &report.refusals {
            assert_eq!(r.file_path, "/movies/oops.mkv");
            assert_eq!(r.reason, "mandatory_strand");
        }
        let mut idxs: Vec<u64> =
            report.refusals.iter().map(|r| r.chunk_index).collect();
        idxs.sort();
        assert_eq!(idxs, vec![0, 1]);
    }

    /// AV-1-11 test 2 — the happy path leaves `refusals` empty so the
    /// dispatcher doesn't emit a spurious alert when everything was
    /// flushable.
    #[test]
    fn bug_av_planner_no_refusals_on_happy_path() {
        let snapshot = FlushSnapshot {
            files: vec![(
                "/movies/wicked.mkv".to_string(),
                vec![ent(0, "h", 10, 0), ent(0, "h", 11, 0)],
                mandatory_cold_only(),
                None,
            )],
            nodes: HashMap::from([
                (10u64, node_summary("leli", &[DiskTier::Cold])),
                (11u64, node_summary("toro", &[DiskTier::Warm])),
            ]),
        };
        let (actions, report) =
            plan_flush_cache(&snapshot, &[DiskTier::Warm], 1000);
        assert_eq!(actions.len(), 1);
        assert!(
            report.refusals.is_empty(),
            "no refusals → dispatcher must not fire AnchorViolated alert"
        );
    }

    /// AV-1-11 test 3 — the public JSON shape of `FlushCacheReport`
    /// must not include the per-refusal list (it can be large for big
    /// prefixes; operator gets the counter + audit log entries). Guard
    /// against accidental `#[serde(skip)]` removal during refactors.
    #[test]
    fn bug_av_report_serde_skips_refusals_field() {
        let mut report = FlushCacheReport::default();
        report.refusals.push(FlushRefusal {
            file_path: "/x".to_string(),
            chunk_index: 0,
            reason: "mandatory_strand",
        });
        report.mandatory_refusals = 1;
        let json = serde_json::to_string(&report).unwrap();
        // Use the precise `"refusals":` token so we don't false-match
        // the legitimate `"mandatory_refusals":` counter.
        assert!(
            !json.contains("\"refusals\":"),
            "FlushCacheReport JSON wire shape must omit the 'refusals' field (was: {json})"
        );
        assert!(
            json.contains("\"mandatory_refusals\":1"),
            "counter must remain in JSON (was: {json})"
        );
    }

    /// AV-1-7 test 5 — two files in different volumes evaluated
    /// independently. A volume that mandates warm protects its warm
    /// replicas; a volume that doesn't mandate warm permits flushing.
    #[test]
    fn bug_av_plan_flush_multi_volume_isolation() {
        let snapshot = FlushSnapshot {
            files: vec![
                // Volume "default": cold mandatory; warm is cache → flushable.
                (
                    "/vol-default/foo.bin".to_string(),
                    vec![ent(0, "ha", 10, 0), ent(0, "ha", 11, 0)],
                    mandatory_cold_only(),
                    None,
                ),
                // Volume "office": warm + cold both mandatory; warm
                // protected from flush even though caller asked for it.
                (
                    "/vol-office/bar.docx".to_string(),
                    vec![ent(0, "hb", 10, 0), ent(0, "hb", 11, 0)],
                    mandatory_warm_and_cold(),
                    None,
                ),
            ],
            nodes: HashMap::from([
                (10u64, node_summary("leli", &[DiskTier::Cold])),
                (11u64, node_summary("toro", &[DiskTier::Warm])),
            ]),
        };
        let (actions, report) =
            plan_flush_cache(&snapshot, &[DiskTier::Warm], 1000);
        // Only foo.bin's warm replica drops.
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].file_path, "/vol-default/foo.bin");
        assert_eq!(report.replicas_dropped, 0, "wrapper drives this");
        assert_eq!(report.files_matched, 2);
    }

    /// CO-5-7 — a flush on a NON-default volume carries that volume's OWN
    /// anchor on the produced `FlushAction` (which the dispatcher threads
    /// into `RemoveChunkLocation`'s `enforce_anchor`), not the default
    /// volume's anchor. Two files in different volumes, each with a distinct
    /// anchor DC; each action must reflect its own file's anchor.
    #[test]
    fn co_5_7_flush_action_uses_per_file_volume_anchor() {
        let anchor_leli = dtfs_common::types::AnchorPolicy {
            datacenter: "leli".to_string(),
            required_tier: Some(DiskTier::Cold),
        };
        let anchor_toro = dtfs_common::types::AnchorPolicy {
            datacenter: "toro".to_string(),
            required_tier: Some(DiskTier::Cold),
        };
        let snapshot = FlushSnapshot {
            files: vec![
                (
                    "/vol-default/foo.bin".to_string(),
                    vec![ent(0, "ha", 10, 0), ent(0, "ha", 11, 0)],
                    mandatory_cold_only(),
                    Some(anchor_leli.clone()),
                ),
                (
                    "/vol-other/bar.bin".to_string(),
                    vec![ent(0, "hb", 10, 0), ent(0, "hb", 11, 0)],
                    mandatory_cold_only(),
                    Some(anchor_toro.clone()),
                ),
            ],
            nodes: HashMap::from([
                (10u64, node_summary("leli", &[DiskTier::Cold])),
                (11u64, node_summary("toro", &[DiskTier::Warm])),
            ]),
        };
        let (actions, _report) =
            plan_flush_cache(&snapshot, &[DiskTier::Warm], 1000);
        assert_eq!(actions.len(), 2, "one warm drop per file");
        let foo = actions.iter().find(|a| a.file_path == "/vol-default/foo.bin").unwrap();
        let bar = actions.iter().find(|a| a.file_path == "/vol-other/bar.bin").unwrap();
        assert_eq!(
            foo.anchor.as_ref().map(|a| a.datacenter.as_str()),
            Some("leli"),
            "default-volume file keeps its own (leli) anchor"
        );
        assert_eq!(
            bar.anchor.as_ref().map(|a| a.datacenter.as_str()),
            Some("toro"),
            "non-default-volume file enforces its OWN (toro) anchor, not the default's"
        );
    }

    /// CO-5-8 — when `max_tasks` caps the plan, `truncated` is the count of
    /// the remaining would-drop REPLICA entries we declined to enqueue, not
    /// the old `chunks_examined - actions.len()` (which mixed chunk and
    /// action units). Mixed set: 4 chunks, each with one warm cache replica
    /// to drop; cap at 1 -> 1 action, 3 truncated.
    #[test]
    fn co_5_8_truncated_counts_remaining_would_drop_entries() {
        let chunks: Vec<ChunkLocationEntry> = (0..4u64)
            .flat_map(|i| {
                vec![
                    ent(i, &format!("h{i}"), 10, 0), // cold (mandatory, kept)
                    ent(i, &format!("h{i}"), 11, 0), // warm (cache, would-drop)
                ]
            })
            .collect();
        let snapshot = FlushSnapshot {
            files: vec![(
                "/movies/season.mkv".to_string(),
                chunks,
                mandatory_cold_only(),
                None,
            )],
            nodes: HashMap::from([
                (10u64, node_summary("leli", &[DiskTier::Cold])),
                (11u64, node_summary("toro", &[DiskTier::Warm])),
            ]),
        };
        let (actions, report) =
            plan_flush_cache(&snapshot, &[DiskTier::Warm], 1);
        assert_eq!(actions.len(), 1, "cap honoured: only 1 action enqueued");
        assert_eq!(
            report.truncated, 3,
            "the other 3 would-drop warm replicas are counted as truncated"
        );
    }
}

#[cfg(test)]
mod bug_av_anchor_lag_tests {
    //! AV-3-3 — pure tests for `plan_anchor_lag_repair`. The
    //! async dispatcher wiring (`check_anchor_lag`) is live-only
    //! per the AV-1-7 / AV-2-4 convention; these tests cover the
    //! decision logic in isolation.
    use super::*;
    use dtfs_common::types::AnchorPolicy;
    use std::collections::HashSet;

    fn node(id: u64, dc: &str, healthy: bool, tiers: &[DiskTier]) -> AnchorRepairNode {
        AnchorRepairNode {
            node_id: id,
            datacenter: dc.to_string(),
            healthy,
            tiers: tiers.to_vec(),
        }
    }

    fn placement(id: u64, dc: &str, tier: DiskTier) -> AnchorRepairPlacement {
        AnchorRepairPlacement { node_id: id, dc: dc.to_string(), tier }
    }

    /// AV-3-3 test 1 — a warm-first write has a single warm
    /// placement in `toro`. The Plexmedia anchor is
    /// `(leli, cold)`. The planner picks the only healthy LELI
    /// cold node as the repair target.
    #[test]
    fn bug_av_anchor_lag_repair_picks_anchor_dc_node() {
        let anchor = AnchorPolicy {
            datacenter: "leli".to_string(),
            required_tier: Some(DiskTier::Cold),
        };
        let placements = vec![placement(11, "toro", DiskTier::Warm)];
        let nodes = vec![
            node(10, "leli", true, &[DiskTier::Cold]),
            node(11, "toro", true, &[DiskTier::Warm]),
            node(12, "nrb",  true, &[DiskTier::Warm]),
        ];
        let existing: HashSet<(String, u64)> = HashSet::new();
        let action =
            plan_anchor_lag_repair(&anchor, &placements, &nodes, "h0", &existing);
        let a = action.expect("anchor missing → repair scheduled");
        assert_eq!(a.target_node, 10, "LELI cold node");
        assert_eq!(a.target_tier, DiskTier::Cold);
        assert_eq!(a.source_node, 11, "the only existing replica");
    }

    /// `[write] anchor_sync = false` — a deferred write commits warm toro +
    /// warm nrb with no anchor copy. Anchor-lag schedules the LELI cold copy
    /// from a live warm replica, and neither the window (2 warm) nor the final
    /// state (cold 1 + warm 2 = goal) gives the excess-removal pass a victim.
    #[test]
    fn deferred_anchor_write_converges_to_goal_without_removal() {
        use crate::scheduler::plan_excess_spill_removal;
        use dtfs_common::types::TierReplication;
        let anchor = AnchorPolicy {
            datacenter: "leli".to_string(),
            required_tier: Some(DiskTier::Cold),
        };
        let mandatory = TierReplication { hot: 0, warm: 2, cold: 1, tape: 0 };
        let nodes = vec![
            node(10, "leli", true, &[DiskTier::Cold]),
            node(11, "toro", true, &[DiskTier::Warm]),
            node(12, "nrb", true, &[DiskTier::Warm]),
        ];
        let placements = vec![
            placement(12, "nrb", DiskTier::Warm),
            placement(11, "toro", DiskTier::Warm),
        ];
        let a = plan_anchor_lag_repair(&anchor, &placements, &nodes, "h0", &HashSet::new())
            .expect("committed chunk without anchor copy is anchor-pending");
        assert_eq!((a.target_node, a.target_tier), (10, DiskTier::Cold));
        assert!([11, 12].contains(&a.source_node), "source must be a live warm copy");

        let window = vec![
            (12, 0, "nrb".to_string(), DiskTier::Warm),
            (11, 0, "toro".to_string(), DiskTier::Warm),
        ];
        assert_eq!(plan_excess_spill_removal(&window, &mandatory, Some(&anchor), 0), None);
        let mut done = window.clone();
        done.push((10, 0, "leli".to_string(), DiskTier::Cold));
        assert_eq!(plan_excess_spill_removal(&done, &mandatory, Some(&anchor), 0), None);
        let done_p: Vec<_> = done.iter().map(|(n, _, dc, t)| placement(*n, dc, *t)).collect();
        assert!(plan_anchor_lag_repair(&anchor, &done_p, &nodes, "h0", &HashSet::new()).is_none());
    }

    /// An anchor-pending chunk that is OVER goal by count (extra warm copies
    /// outside LELI) still yields no removal: the goal/anchor guard refuses,
    /// not the total-count early return.
    #[test]
    fn over_goal_anchor_pending_chunk_has_no_removal() {
        use crate::scheduler::plan_excess_spill_removal;
        use dtfs_common::types::TierReplication;
        let w = |n: u64, dc: &str| (n, 0u32, dc.to_string(), DiskTier::Warm);
        // warm 2 / cold 1, four warm copies (4 > 3), none in LELI.
        let cold_anchor = AnchorPolicy { datacenter: "leli".into(), required_tier: Some(DiskTier::Cold) };
        let m = TierReplication { hot: 0, warm: 2, cold: 1, tape: 0 };
        let reps = vec![w(11, "toro"), w(12, "nrb"), w(13, "nrb"), w(14, "toro")];
        assert_eq!(plan_excess_spill_removal(&reps, &m, Some(&cold_anchor), 0), None);
        // Anchor alone: warm 2 / cold 0, three warm (3 > 2), anchor any tier.
        // Every tier count survives a removal, only the anchor refuses it.
        let any_anchor = AnchorPolicy { datacenter: "leli".into(), required_tier: None };
        let m = TierReplication { hot: 0, warm: 2, cold: 0, tape: 0 };
        let reps = vec![w(11, "toro"), w(12, "nrb"), w(13, "nrb")];
        assert_eq!(plan_excess_spill_removal(&reps, &m, Some(&any_anchor), 0), None);
        assert!(plan_excess_spill_removal(&reps, &m, None, 0).is_some(), "control: no anchor -> removable");
    }

    /// AV-3-3 test 2 — `required_tier = Cold` filters out a
    /// LELI warm node that *would* be in the anchor DC but on
    /// the wrong tier.
    #[test]
    fn bug_av_anchor_lag_repair_picks_required_tier() {
        let anchor = AnchorPolicy {
            datacenter: "leli".to_string(),
            required_tier: Some(DiskTier::Cold),
        };
        let placements = vec![placement(11, "toro", DiskTier::Warm)];
        let nodes = vec![
            // LELI warm-only — must NOT be picked (wrong tier).
            node(15, "leli", true, &[DiskTier::Warm]),
            // LELI cold — the correct target.
            node(10, "leli", true, &[DiskTier::Cold]),
            // TORO warm — the existing source.
            node(11, "toro", true, &[DiskTier::Warm]),
        ];
        let action =
            plan_anchor_lag_repair(&anchor, &placements, &nodes, "h0", &HashSet::new());
        let a = action.expect("anchor repair scheduled");
        assert_eq!(a.target_node, 10);
        assert_eq!(a.target_tier, DiskTier::Cold);
    }

    /// AV-3-3 test 3 — when the anchor invariant is already
    /// satisfied (existing LELI cold replica), the planner
    /// returns `None`.
    #[test]
    fn bug_av_anchor_lag_repair_no_op_when_satisfied() {
        let anchor = AnchorPolicy {
            datacenter: "leli".to_string(),
            required_tier: Some(DiskTier::Cold),
        };
        let placements = vec![
            placement(10, "leli", DiskTier::Cold), // anchor already present
            placement(11, "toro", DiskTier::Warm),
        ];
        let nodes = vec![
            node(10, "leli", true, &[DiskTier::Cold]),
            node(11, "toro", true, &[DiskTier::Warm]),
        ];
        let action =
            plan_anchor_lag_repair(&anchor, &placements, &nodes, "h0", &HashSet::new());
        assert!(action.is_none(), "anchor satisfied → no repair");
    }

    /// AV-3-3 defensive — `required_tier = None` accepts any
    /// tier in the anchor DC (preferring Cold by default per
    /// the planner's documented choice).
    #[test]
    fn bug_av_anchor_lag_repair_required_tier_none_defaults_to_cold() {
        let anchor = AnchorPolicy {
            datacenter: "leli".to_string(),
            required_tier: None,
        };
        let placements = vec![placement(11, "toro", DiskTier::Warm)];
        let nodes = vec![
            node(10, "leli", true, &[DiskTier::Cold]),
            node(15, "leli", true, &[DiskTier::Warm]),
            node(11, "toro", true, &[DiskTier::Warm]),
        ];
        let action =
            plan_anchor_lag_repair(&anchor, &placements, &nodes, "h0", &HashSet::new());
        let a = action.expect("required_tier=None still produces a repair");
        // Per the planner doc: defaults to Cold when required_tier
        // is None and a cold candidate exists.
        assert_eq!(a.target_tier, DiskTier::Cold);
        assert_eq!(a.target_node, 10);
    }

    /// AV-3-3 defensive — already-scheduled tasks are detected
    /// via the (chunk_id, target_node) fingerprint and don't
    /// produce a duplicate.
    #[test]
    fn bug_av_anchor_lag_repair_skips_already_scheduled() {
        let anchor = AnchorPolicy {
            datacenter: "leli".to_string(),
            required_tier: Some(DiskTier::Cold),
        };
        let placements = vec![placement(11, "toro", DiskTier::Warm)];
        let nodes = vec![
            node(10, "leli", true, &[DiskTier::Cold]),
            node(11, "toro", true, &[DiskTier::Warm]),
        ];
        let mut existing = HashSet::new();
        existing.insert(("h0".to_string(), 10u64));
        let action =
            plan_anchor_lag_repair(&anchor, &placements, &nodes, "h0", &existing);
        assert!(action.is_none(),
            "task already in-flight → no duplicate");
    }

    /// CR-3-5/MGR-6 — proves the exact fix in `check_anchor_lag`: it now
    /// inserts `(chunk_id, target_node)` into `existing_tasks` right
    /// after scheduling a repair, WITHIN the same pass, instead of only
    /// building the set once at the top. Two files sharing a hash
    /// (content-addressed re-download/copy) evaluated in the same pass
    /// must produce exactly ONE task, not two.
    #[test]
    fn cr_3_5_anchor_lag_dedup_within_one_pass() {
        let anchor = AnchorPolicy {
            datacenter: "leli".to_string(),
            required_tier: Some(DiskTier::Cold),
        };
        let placements = vec![placement(11, "toro", DiskTier::Warm)];
        let nodes = vec![
            node(10, "leli", true, &[DiskTier::Cold]),
            node(11, "toro", true, &[DiskTier::Warm]),
        ];
        let mut existing = HashSet::new();

        // First file referencing hash "h0": schedules a repair.
        let action1 = plan_anchor_lag_repair(&anchor, &placements, &nodes, "h0", &existing)
            .expect("first file schedules the repair");
        assert_eq!(action1.target_node, 10);
        // check_anchor_lag now does this immediately (CR-3-5 fix), not
        // only at the top of the pass.
        existing.insert(("h0".to_string(), action1.target_node));

        // Second file, SAME hash, evaluated later in the SAME pass: must
        // be refused now that the set was updated inline.
        let action2 = plan_anchor_lag_repair(&anchor, &placements, &nodes, "h0", &existing);
        assert!(
            action2.is_none(),
            "second file sharing the hash must not double-schedule within one pass"
        );
    }

    /// AV-3-3 defensive — Draining / unhealthy nodes are
    /// filtered out of the candidate set.
    #[test]
    fn bug_av_anchor_lag_repair_skips_unhealthy_target() {
        let anchor = AnchorPolicy {
            datacenter: "leli".to_string(),
            required_tier: Some(DiskTier::Cold),
        };
        let placements = vec![placement(11, "toro", DiskTier::Warm)];
        let nodes = vec![
            // Only LELI cold node is unhealthy → no candidate.
            node(10, "leli", /* healthy */ false, &[DiskTier::Cold]),
            node(11, "toro", true, &[DiskTier::Warm]),
        ];
        let action =
            plan_anchor_lag_repair(&anchor, &placements, &nodes, "h0", &HashSet::new());
        assert!(action.is_none(),
            "no healthy LELI cold node → no repair");
    }

    /// 2026-05-27 lab-found bug: when the only anchor-DC node ALSO
    /// holds the chunk on a non-anchor tier (warm), the previous
    /// `holders` filter excluded that node entirely, so no cold
    /// replica was ever scheduled. The lab minicluster topology
    /// (data01 holds both warm + cold disks in dc-a) exposed this:
    /// after writes the cold tier stayed empty forever.
    #[test]
    fn anchor_lag_repair_allows_self_when_holding_only_non_required_tier() {
        let anchor = AnchorPolicy {
            datacenter: "dc-a".to_string(),
            required_tier: Some(DiskTier::Cold),
        };
        // data01 (node_id=1) holds the chunk on warm; it ALSO has a
        // cold disk and is the only node in dc-a. Anchor cold must
        // land here.
        let placements = vec![placement(1, "dc-a", DiskTier::Warm)];
        let nodes = vec![
            node(1, "dc-a", true, &[DiskTier::Warm, DiskTier::Cold]),
            node(2, "dc-b", true, &[DiskTier::Warm, DiskTier::Cold]),
        ];
        let action = plan_anchor_lag_repair(
            &anchor, &placements, &nodes, "h0", &HashSet::new(),
        )
        .expect("anchor-DC node with cold disk MUST be a valid target \
                 even when it already holds the chunk on a different tier");
        assert_eq!(action.target_node, 1);
        assert_eq!(action.target_tier, DiskTier::Cold);
    }

    /// Inverse: when the candidate-tier copy already exists on the
    /// node, don't double-schedule.
    #[test]
    fn anchor_lag_repair_skips_self_when_holding_required_tier() {
        let anchor = AnchorPolicy {
            datacenter: "dc-a".to_string(),
            required_tier: Some(DiskTier::Cold),
        };
        // data01 already holds a cold replica → anchor satisfied.
        let placements = vec![placement(1, "dc-a", DiskTier::Cold)];
        let nodes = vec![
            node(1, "dc-a", true, &[DiskTier::Warm, DiskTier::Cold]),
        ];
        assert!(
            plan_anchor_lag_repair(
                &anchor, &placements, &nodes, "h0", &HashSet::new(),
            )
            .is_none(),
            "anchor already satisfied → no repair"
        );
    }

    // #118 — anchor node Down: every chunk reports "no target" as a
    // countable Err (the caller logs one summary), never an action.
    #[test]
    fn issue_118_anchor_down_is_counted_not_planned() {
        let anchor = AnchorPolicy { datacenter: "leli".to_string(), required_tier: None };
        let placements = vec![placement(2, "toro", DiskTier::Warm)];
        let nodes = vec![
            node(1, "leli", false, &[DiskTier::Cold]),
            node(2, "toro", true, &[DiskTier::Warm]),
        ];
        let blocked = (0..50)
            .filter(|i| {
                plan_anchor_lag_repair_checked(&anchor, &placements, &nodes, &format!("h{i}"), &HashSet::new())
                    == Err(DiskTier::Cold)
            })
            .count();
        assert_eq!(blocked, 50);
        // Anchor node healthy again: a real action, not an Err.
        let nodes = vec![node(1, "leli", true, &[DiskTier::Cold])];
        let act = plan_anchor_lag_repair_checked(&anchor, &placements, &nodes, "h0", &HashSet::new());
        assert!(matches!(act, Ok(Some(ref a)) if a.target_node == 1), "{act:?}");
        // Satisfied anchor is Ok(None), not counted as blocked.
        let done = vec![placement(1, "leli", DiskTier::Cold)];
        assert_eq!(plan_anchor_lag_repair_checked(&anchor, &done, &nodes, "h0", &HashSet::new()), Ok(None));
    }
}

#[cfg(test)]
mod bug_ae_orphan_tests {
    //! Bug AE — orphan migration tasks and Failed-task retention.
    //!
    //! Symptom (verified on Plexmedia 4h soak v1, 2026-05-19): 125 failed +
    //! 32 stuck-Copying migration tasks all carrying `"Chunk not found"`
    //! errors. The chunks referenced no longer existed in any
    //! `FileMetadata.chunks` list — they were leftovers from a data-node
    //! disk wipe but `migration_state_json` retained the tasks across the
    //! wipe.
    //!
    //! Root cause: the prune step at the end of `advance_tasks` retained
    //! `Failed` tasks forever (only `Done` was time-bounded), AND the
    //! `Copying` branch had no fast-path for "chunk no longer exists" —
    //! it ran the full 6-retry × 30s cycle, hammering data nodes with
    //! ReplicateChunk RPCs that all returned `Chunk not found`.
    //!
    //! Fix verified by:
    //! - `bug_ae_*` tests below for the pure helpers,
    //! - live-cluster observation that the 125 Failed tasks drain within
    //!   30 minutes of the new manager binary rolling out,
    //! - 4h soak v2 expected to maintain `failed_tasks < 5` throughout.

    use super::*;
    use dtfs_raft::state_machine::FileMetadata;
    use dtfs_raft::ChunkLocationEntry;
    use std::collections::BTreeMap;

    /// The compiled-in defaults (raft.{done,failed}_task_retention_secs).
    const DONE_MS: u64 = 5 * 60 * 1000;
    const FAILED_MS: u64 = 30 * 60 * 1000;

    fn file_with_chunks(path: &str, chunk_hashes: &[&str]) -> FileMetadata {
        FileMetadata {
            path: path.to_string(),
            volume_id: "default".to_string(),
            chunk_size: 128 * 1024 * 1024,
            chunks: chunk_hashes
                .iter()
                .enumerate()
                .map(|(i, h)| ChunkLocationEntry {
                    chunk_index: i as u64,
                    chunk_hash: (*h).to_string(),
                    node_id: "10".to_string(),
                    disk_id: 0,
                    size_bytes: 128 * 1024 * 1024,
                })
                .collect(),
            created_at_ms: 0,
            updated_at_ms: 0,
            symlink_target: None,
            logical_size: None,
            is_directory: false,
            cache_cooldown_until_ms: std::collections::BTreeMap::new(),
            ever_committed: None, replica_added_at_ms: 0,
        }
    }

    /// Pending / Copying / Verifying / Completing must NEVER be pruned —
    /// the state machine in `advance_tasks` owns their progression.
    #[test]
    fn non_terminal_tasks_are_always_retained() {
        let now = 10 * 60 * 60 * 1000; // 10h
        for status in [
            MigrationStatus::Pending,
            MigrationStatus::Copying,
            MigrationStatus::Verifying,
            MigrationStatus::Completing,
        ] {
            // Even one full hour old — keep it.
            assert!(
                should_retain_task(status, now - 60 * 60 * 1000, now, DONE_MS, FAILED_MS),
                "non-terminal task ({status:?}) must be retained regardless of age",
            );
        }
    }

    /// `Done` task within the 5-minute window stays; older one is pruned.
    #[test]
    fn done_tasks_pruned_after_five_minutes() {
        let now = 60 * 60 * 1000; // 1h
        let just_completed = now - 30 * 1000; // 30s ago
        let stale = now - 6 * 60 * 1000;       // 6min ago — past cutoff

        assert!(should_retain_task(MigrationStatus::Done, just_completed, now, DONE_MS, FAILED_MS));
        assert!(!should_retain_task(MigrationStatus::Done, stale, now, DONE_MS, FAILED_MS));
    }

    /// `Failed` task within the 30-minute window stays so operators can
    /// inspect; older one is pruned to bound state size.
    #[test]
    fn failed_tasks_pruned_after_thirty_minutes() {
        let now = 60 * 60 * 1000; // 1h
        let recent_failure = now - 5 * 60 * 1000;  // 5min ago — keep
        let stale_failure  = now - 31 * 60 * 1000; // 31min ago — prune

        assert!(should_retain_task(MigrationStatus::Failed, recent_failure, now, DONE_MS, FAILED_MS));
        assert!(!should_retain_task(MigrationStatus::Failed, stale_failure, now, DONE_MS, FAILED_MS));
    }

    /// CNF-2 — the retention windows are caller-supplied, not hardcoded:
    /// a short configured window (Failed 45s / Done 30s, as the release
    /// case sets) prunes terminal tasks on that schedule.
    #[test]
    fn cnf2_retention_windows_are_configurable() {
        let now = 60 * 60 * 1000; // 1h
        let done_ms = 30_000;
        let failed_ms = 45_000;

        assert!(should_retain_task(MigrationStatus::Failed, now - 44_000, now, done_ms, failed_ms));
        assert!(!should_retain_task(MigrationStatus::Failed, now - 46_000, now, done_ms, failed_ms));
        assert!(should_retain_task(MigrationStatus::Done, now - 29_000, now, done_ms, failed_ms));
        assert!(!should_retain_task(MigrationStatus::Done, now - 31_000, now, done_ms, failed_ms));
    }

    /// Sanity: `updated_at_ms > now_ms` (clock drift / startup race) must
    /// NOT cause underflow; the task should be retained.
    #[test]
    fn updated_after_now_does_not_underflow() {
        let now = 1000;
        let future = 5000;
        // saturating_sub returns 0, < retention windows, so retained.
        assert!(should_retain_task(MigrationStatus::Done, future, now, DONE_MS, FAILED_MS));
        assert!(should_retain_task(MigrationStatus::Failed, future, now, DONE_MS, FAILED_MS));
    }

    /// REV-4-6 — chunks are content-addressed, so identical content in two
    /// files is ONE chunk id referenced from both. Registering a completed
    /// replica against only the first match left every other referencing file
    /// short of its mandatory count, so the same copy was scheduled again on
    /// every tick — it succeeded every time and the deficit never closed.
    #[test]
    fn rev_4_6_a_shared_chunk_resolves_to_every_referencing_file() {
        let mut files: BTreeMap<String, FileMetadata> = BTreeMap::new();
        files.insert("/a.mkv".into(), file_with_chunks("/a.mkv", &["hh", "other"]));
        files.insert("/b.mkv".into(), file_with_chunks("/b.mkv", &["hh"]));
        files.insert("/c.mkv".into(), file_with_chunks("/c.mkv", &["unrelated"]));

        assert_eq!(
            files_referencing_chunk(&files, "hh"),
            vec!["/a.mkv".to_string(), "/b.mkv".to_string()],
            "both referencing files must be registered, not just the first"
        );
        assert!(files_referencing_chunk(&files, "nope").is_empty());
    }

    /// Bug AF — the exact error string produced when a source data node
    /// reports "Chunk not found" must be classified as fast-fail. This is
    /// the live shape observed on Plexmedia 4h soak v2 at 07:30 UTC.
    #[test]
    fn bug_af_chunk_not_found_classified_for_fast_fail() {
        let live = "ReplicateChunk error: ReplicateChunk returned error: \
                    Chunk not found: 6bea15d90aa05f7f445d5136c49618c7aa720ca13bf0f902d8518732ca2a4e46";
        assert!(rpc_error_indicates_chunk_missing(live));

        // Bare form (defensive — if the inner wrapping ever changes).
        assert!(rpc_error_indicates_chunk_missing("Chunk not found"));
    }

    /// Generic transport / network / target-resolution failures must NOT
    /// trigger the fast-fail path — those are transient and should be
    /// retried up to MAX_MIGRATION_ATTEMPTS as normal.
    #[test]
    fn bug_af_transient_errors_not_classified_for_fast_fail() {
        assert!(!rpc_error_indicates_chunk_missing("connect to source 10.0.0.1: timeout"));
        assert!(!rpc_error_indicates_chunk_missing("ReplicateChunk RPC: tonic transport error"));
        assert!(!rpc_error_indicates_chunk_missing("target node 999 unresolvable"));
        assert!(!rpc_error_indicates_chunk_missing("no healthy source replica"));
        assert!(!rpc_error_indicates_chunk_missing(""));
    }

    /// Regression-shape test against the live-cluster scenario: a Failed
    /// task that fired 31 min ago AND references a chunk that's no longer
    /// in any file is pruned (because Failed + stale).  A separate
    /// Copying-state task with the same orphan chunk is retained by the
    /// retain block (the advance_tasks branch is responsible for fast-
    /// pathing it to Failed on the next tick).
    #[test]
    fn live_scenario_125_failed_orphans_drain() {
        let now = 60 * 60 * 1000; // 1h
        let stale_failed = now - 31 * 60 * 1000;
        let stuck_copying = now - 31 * 60 * 1000;

        // Failed + stale → pruned.
        assert!(!should_retain_task(MigrationStatus::Failed, stale_failed, now, DONE_MS, FAILED_MS));
        // Copying + stale → retained; advance_tasks owns the fast-path.
        assert!(should_retain_task(MigrationStatus::Copying, stuck_copying, now, DONE_MS, FAILED_MS));
    }
}

#[cfg(test)]
mod bug_aw_migration_sort_tests {
    //! Bug AW — Migrations page randomised row order on each 5s refresh.
    //!
    //! Symptom: operators watching the Migrations panel during a tier
    //! storm saw rows leap around between refreshes, making the page
    //! unreadable.
    //!
    //! Root cause: the snapshot sort keyed on `updated_at_ms`, which is
    //! bumped on every scheduler tick for any task in flight. Two ticks
    //! in different orders → two different sort orders.
    //!
    //! Fix: sort on `created_at_ms` (set once at insertion) DESC,
    //! tiebreak with a natural-numeric `task_id` compare.
    use super::*;
    use serde_json::json;

    fn t(task_id: &str, created: u64, updated: u64) -> serde_json::Value {
        json!({
            "task_id": task_id,
            "created_at_ms": created,
            "updated_at_ms": updated,
        })
    }

    fn ids(v: &[serde_json::Value]) -> Vec<&str> {
        v.iter()
            .map(|x| x.get("task_id").and_then(|s| s.as_str()).unwrap_or(""))
            .collect()
    }

    /// Two scheduler ticks bump `updated_at_ms` in different orders;
    /// the pre-fix sort would flip the rows. With the fix, ordering
    /// is anchored to `created_at_ms` and is stable.
    #[test]
    fn bug_aw_running_tasks_dont_reshuffle() {
        // Tick 1: repl-1 was updated last.
        let mut tick1 = vec![
            t("repl-1", 1_000, 5_000),
            t("repl-2", 2_000, 4_000),
            t("repl-3", 3_000, 3_000),
        ];
        sort_migration_snapshot(&mut tick1);

        // Tick 2: same three tasks, scheduler touched repl-2 last
        // (different updated_at_ms ordering), creation order unchanged.
        let mut tick2 = vec![
            t("repl-1", 1_000, 6_000),
            t("repl-2", 2_000, 9_000),
            t("repl-3", 3_000, 7_000),
        ];
        sort_migration_snapshot(&mut tick2);

        // Bug AW: order is identical between ticks regardless of how
        // updated_at_ms shifted.
        assert_eq!(ids(&tick1), ids(&tick2));
        // And newest-created is on top.
        assert_eq!(ids(&tick1), vec!["repl-3", "repl-2", "repl-1"]);
    }

    /// Natural numeric tiebreak: a burst of >=10 tasks created in the
    /// same millisecond must render as repl-1..repl-10 in numeric order.
    #[test]
    fn bug_aw_natural_numeric_tiebreak() {
        let mut burst: Vec<_> = (1..=12u64)
            .map(|n| t(&format!("repl-{n}"), 5_000, 5_000))
            .collect();
        // Shuffle insertion order to prove sort doesn't depend on it.
        burst.swap(0, 11);
        burst.swap(2, 7);

        sort_migration_snapshot(&mut burst);

        let want: Vec<String> = (1..=12u64).map(|n| format!("repl-{n}")).collect();
        let got: Vec<String> = ids(&burst).into_iter().map(String::from).collect();
        assert_eq!(got, want);
    }

    /// Pre-Bug-AW lex sort would order ["repl-1","repl-10","repl-2"];
    /// the new natural compare must order ["repl-1","repl-2","repl-10"].
    #[test]
    fn bug_aw_natural_compare_helper() {
        use std::cmp::Ordering::*;
        assert_eq!(natural_task_id_cmp("repl-2", "repl-10"), Less);
        assert_eq!(natural_task_id_cmp("repl-10", "repl-2"), Greater);
        assert_eq!(natural_task_id_cmp("repl-7", "repl-7"), Equal);
    }

    /// Non-numeric IDs fall back to lex compare without panicking.
    #[test]
    fn bug_aw_non_numeric_ids_fallback() {
        use std::cmp::Ordering::*;
        assert_eq!(natural_task_id_cmp("alpha", "beta"), Less);
        assert_eq!(natural_task_id_cmp("repl-foo", "repl-bar"), Greater);
    }

    /// Tasks missing created_at_ms (older snapshots) sort to the bottom
    /// instead of randomising the head of the list.
    #[test]
    fn bug_aw_missing_created_at_sinks() {
        let mut v = vec![
            json!({"task_id": "repl-1"}), // missing created_at_ms
            t("repl-2", 1_000, 1_000),
            t("repl-3", 2_000, 2_000),
        ];
        sort_migration_snapshot(&mut v);
        assert_eq!(ids(&v), vec!["repl-3", "repl-2", "repl-1"]);
    }
}

#[cfg(test)]
mod bug_bo_tests {
    use super::*;

    /// BO-1 — a same-node cross-tier anchor copy must NOT exclude the
    /// target from the source search (the target is the only holder of the
    /// chunk, on its other tier). Cross-node copies still exclude the
    /// target.
    #[test]
    fn bo_1_same_node_source_not_excluded() {
        assert_eq!(anchor_copy_source_exclude(1, 1), None,
            "same-node warm->cold copy: target must not be excluded");
        assert_eq!(anchor_copy_source_exclude(1, 2), Some(2),
            "cross-node copy: target excluded as before");
    }

    /// BO-2 — a Failed task must keep its chunk in the re-queue-blocking
    /// set (so check_anchor_lag does not re-schedule it every tick during
    /// the 30-min retention window); only Done tasks free the chunk.
    #[test]
    fn bo_2_failed_blocks_requeue_done_does_not() {
        assert!(task_blocks_requeue(MigrationStatus::Failed),
            "Failed task must block immediate re-queue (was the tight-loop bug)");
        assert!(!task_blocks_requeue(MigrationStatus::Done),
            "Done task frees the chunk for a future schedule");
        // In-flight states also block (unchanged).
        assert!(task_blocks_requeue(MigrationStatus::Pending));
        assert!(task_blocks_requeue(MigrationStatus::Copying));
        assert!(task_blocks_requeue(MigrationStatus::Verifying));
    }

    /// BP-4 — a chunk referenced by ANY file (hash in the in-state set) is
    /// never an orphan, even if its on-disk location looks stale; only a
    /// hash with no file referent is reapable.
    #[test]
    fn bp_4_chunk_is_orphan_only_when_unreferenced() {
        let mut in_state = std::collections::HashSet::new();
        in_state.insert("referenced".to_string());
        assert!(!chunk_is_orphan(&in_state, "referenced"), "referenced chunk is NOT orphan");
        assert!(chunk_is_orphan(&in_state, "dangling"), "unreferenced chunk IS orphan");
        // Empty in-state set: everything is orphan (no files reference anything).
        let empty = std::collections::HashSet::new();
        assert!(chunk_is_orphan(&empty, "anything"));
    }

    /// Bug CB — the reap decision is a pure function of the chunk's
    /// durable mtime, so it survives manager restarts (no first-observed
    /// clock to reset). Covers: referenced never reaps, recent orphan is
    /// protected, aged orphan reaps, unknown mtime defers.
    #[test]
    fn cb_orphan_reap_due_uses_durable_mtime() {
        // Fixed clock so the test never touches Date::now (forbidden in
        // workflow scripts and avoided in pure tests for determinism).
        let now_ms: u64 = 1_700_000_000_000;
        let grace_secs: u64 = 86_400; // 24h, the stock default

        // Referenced chunk (not orphan) is NEVER reaped, even if ancient.
        assert!(
            !orphan_reap_due(false, now_ms - 10 * grace_secs * 1000, now_ms, grace_secs),
            "a referenced chunk must never be reaped"
        );

        // Orphan but freshly written (1 min old) — within grace, protected
        // against the in-flight put/promote race.
        assert!(
            !orphan_reap_due(true, now_ms - 60_000, now_ms, grace_secs),
            "a recently-written orphan must be protected by grace"
        );

        // Orphan whose .dat is older than grace — reap.
        assert!(
            orphan_reap_due(true, now_ms - (grace_secs + 10) * 1000, now_ms, grace_secs),
            "an aged orphan past grace must be reaped"
        );

        // Exactly at the grace boundary — reap (>=).
        assert!(
            orphan_reap_due(true, now_ms - grace_secs * 1000, now_ms, grace_secs),
            "grace boundary is inclusive"
        );

        // Unknown mtime (0) — a pre-Bug-CB data node or a stat failure.
        // Defer rather than reap with no grace, even though the chunk is
        // an orphan and `now - 0` is astronomically larger than grace.
        assert!(
            !orphan_reap_due(true, 0, now_ms, grace_secs),
            "unknown mtime must defer the reap, not fire it"
        );
    }

    /// Bug CB — regression for the actual symptom: the decision carries NO
    /// state across calls, so a manager that "restarts" (re-evaluates from
    /// scratch) reaches the same verdict. The pre-CB in-memory clock reset
    /// to `now` on every restart, so an aged orphan re-started its 24h
    /// countdown forever and disk was never reclaimed.
    #[test]
    fn cb_orphan_reap_survives_simulated_restart() {
        let grace_secs: u64 = 86_400;
        let mtime_ms: u64 = 1_700_000_000_000; // written once, long ago
        // First "manager process": observes the orphan well past grace.
        let now_1 = mtime_ms + (grace_secs + 1) * 1000;
        assert!(orphan_reap_due(true, mtime_ms, now_1, grace_secs));
        // Manager restarts (any in-memory tracker is gone). A later tick
        // on the fresh process still sees the same durable mtime and still
        // reaps — it does NOT restart the grace countdown.
        let now_2 = now_1 + 5_000;
        assert!(
            orphan_reap_due(true, mtime_ms, now_2, grace_secs),
            "reap verdict must not reset across a manager restart"
        );
    }

    /// CO-3-2 — a chunk on a node's disk whose SM has NO location entry
    /// naming that node is "de-located" and a reap candidate, even though
    /// the chunk hash may still be referenced elsewhere (so it is NOT a
    /// hash-orphan). This is the rebalance / excess-spill leftover: the
    /// `RemoveChunkLocation` dropped the SM entry on this node, but the
    /// bytes linger on disk. Verifies the per-node predicate and that it
    /// supersets the hash-orphan signal.
    #[test]
    fn co_3_2_delocated_chunk_is_reap_candidate() {
        // Node 10 is expected (per current SM) to hold only "keep".
        let mut node10: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        node10.insert("keep".to_string());

        // "keep" stays on node 10 -> NOT de-located.
        assert!(
            !chunk_delocated_on_node(Some(&node10), "keep"),
            "a chunk the SM still names this node for is not de-located"
        );

        // "spilled" was de-located by rebalance: the chunk hash is still
        // referenced by another replica elsewhere (so chunk_is_orphan would
        // be false against the global union), but node 10 no longer holds an
        // entry for it -> de-located on this node -> reap candidate.
        assert!(
            chunk_delocated_on_node(Some(&node10), "spilled"),
            "a chunk no SM entry names on this node IS de-located"
        );

        // A node the SM records nothing for: every on-disk chunk is
        // de-located (e.g. a fully drained node still holding leftover bytes).
        assert!(chunk_delocated_on_node(None, "anything"));

        // CO-3-2 + Bug CB — the de-located candidate is still gated by the
        // durable-mtime grace, so a recently-written chunk whose
        // commit_chunk_locations Raft write has not yet applied is protected.
        let now_ms: u64 = 1_700_000_000_000;
        let grace_secs: u64 = 86_400;
        let reap_candidate = chunk_delocated_on_node(Some(&node10), "spilled");
        assert!(
            !orphan_reap_due(reap_candidate, now_ms - 60_000, now_ms, grace_secs),
            "a freshly-written de-located chunk is still protected by grace"
        );
        assert!(
            orphan_reap_due(reap_candidate, now_ms - (grace_secs + 10) * 1000, now_ms, grace_secs),
            "an aged de-located chunk past grace is reaped"
        );
    }

    /// CR2-7-2 / MGR-10 — a node holding a cold ANCHOR replica (disk 0)
    /// and a de-located WARM leftover (disk 1) of the SAME chunk: the
    /// node-WIDE check (pre-CR2-7-2 semantics, still used for a legacy
    /// data node that reports no disk_id) sees the cold entry and
    /// wrongly calls the whole node "not de-located", so the warm
    /// leftover is never reaped. Keying the check by (node, disk) instead
    /// correctly flags the warm disk as de-located while leaving the
    /// cold anchor disk alone.
    #[test]
    fn mgr_10_delocated_check_is_disk_scoped_when_disk_id_known() {
        let node_id: u64 = 42;
        let cold_disk: u32 = 0;
        let warm_disk: u32 = 1;
        let chunk = "co-located-chunk".to_string();

        // Node-wide expected set: only the cold-disk entry survives (the
        // warm replica's RemoveChunkLocation already committed).
        let mut node_wide: std::collections::HashSet<String> = std::collections::HashSet::new();
        node_wide.insert(chunk.clone());

        // Legacy behaviour (no disk_id reported): node-wide check sees the
        // surviving cold entry and misses the warm leftover entirely.
        assert!(
            !chunk_delocated_on_node(Some(&node_wide), &chunk),
            "node-wide check is blind to a disk-scoped leftover \
             co-located with a still-live replica of the same chunk"
        );

        // CR2-7-2: per-(node, disk) expected sets built the same way
        // orphan_scrub_tick builds `expected_by_node_disk`.
        let mut expected_by_node_disk: std::collections::BTreeMap<
            (u64, u32),
            std::collections::HashSet<String>,
        > = std::collections::BTreeMap::new();
        expected_by_node_disk
            .entry((node_id, cold_disk))
            .or_default()
            .insert(chunk.clone());
        // No entry at all for (node_id, warm_disk): the warm replica's
        // location row was removed.

        let cold_expected = expected_by_node_disk.get(&(node_id, cold_disk));
        let warm_expected = expected_by_node_disk.get(&(node_id, warm_disk));

        assert!(
            !chunk_delocated_on_node(cold_expected, &chunk),
            "the cold anchor disk still has a live location entry"
        );
        assert!(
            chunk_delocated_on_node(warm_expected, &chunk),
            "the warm disk has NO location entry for this chunk -> de-located"
        );
    }

    /// CO-3-3 — the in-state-missing quarantine guard re-checks the CURRENT
    /// state machine before quarantining. A chunk that dropped out of the
    /// node's expected set between the tick's opening snapshot and the
    /// re-check (a concurrent demote / unlink / rebalance) is filtered out,
    /// so a healthy node is NOT quarantined for losing a chunk it was
    /// legitimately told to drop. A chunk still expected stays in the set
    /// and DOES justify a quarantine.
    #[test]
    fn co_3_3_recheck_filters_demote_race() {
        // The tick snapshot saw two chunks expected-but-missing on the node.
        let a = "a".to_string();
        let b = "b".to_string();
        let snapshot_missing: Vec<&String> = vec![&a, &b];

        // Re-check: the CURRENT SM still expects "a" (genuine fault) but no
        // longer expects "b" (a demote committed RemoveChunkLocation between
        // the snapshot and now).
        let mut current_expected: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        current_expected.insert("a".to_string());

        let still = still_missing_after_recheck(&snapshot_missing, &current_expected);
        assert_eq!(still.len(), 1, "only the genuinely-missing chunk survives");
        assert_eq!(still[0], "a");

        // Pure demote race: every "missing" chunk was de-located -> empty ->
        // no quarantine.
        let none_expected: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        let still = still_missing_after_recheck(&snapshot_missing, &none_expected);
        assert!(
            still.is_empty(),
            "a pure demote race must clear the missing set (no quarantine)"
        );
    }
}

#[cfg(test)]
mod co_1_3_delocate_order_tests {
    use super::{delocate_replica, DelocateOps, DelocateOutcome};
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingOps {
        /// Ordered log of side effects: "remove" then "delete".
        calls: Mutex<Vec<&'static str>>,
        remove_fails: bool,
        delete_fails: bool,
    }

    impl DelocateOps for RecordingOps {
        async fn remove_location(&self) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push("remove");
            if self.remove_fails {
                Err(anyhow::anyhow!("forced raft remove failure"))
            } else {
                Ok(())
            }
        }
        async fn delete_bytes(&self, _: Option<u64>) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push("delete");
            if self.delete_fails {
                Err(anyhow::anyhow!("forced byte-delete RPC failure"))
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn happy_path_removes_then_deletes_in_order() {
        let ops = RecordingOps::default();
        let outcome = delocate_replica(&ops).await;
        assert_eq!(outcome, DelocateOutcome::Done);
        assert_eq!(
            *ops.calls.lock().unwrap(),
            vec!["remove", "delete"],
            "Raft remove MUST precede the byte delete"
        );
    }

    /// The plan's primary done-when: on a FORCED Raft-write failure, NO
    /// DeleteChunkReplica/byte-delete RPC is issued, so no phantom replica
    /// (bytes-gone-but-still-advertised) can ever result.
    #[tokio::test]
    async fn forced_raft_failure_issues_no_byte_delete() {
        let ops = RecordingOps { remove_fails: true, ..Default::default() };
        let outcome = delocate_replica(&ops).await;
        assert_eq!(outcome, DelocateOutcome::RemoveFailed);
        assert_eq!(
            *ops.calls.lock().unwrap(),
            vec!["remove"],
            "a failed Raft remove must short-circuit BEFORE any byte delete"
        );
        assert!(
            !ops.calls.lock().unwrap().contains(&"delete"),
            "no byte-delete RPC may be issued when the Raft remove failed"
        );
    }

    /// A byte-delete failure AFTER a committed remove de-locates the replica
    /// in the SM (entry gone); the leftover on-disk bytes are reclaimed by
    /// the CO-3-2 orphan reaper. The committed Raft remove is never rolled
    /// back (only `remove` + `delete` were attempted, in that order).
    #[tokio::test]
    async fn post_commit_byte_delete_failure_leaves_reapable_bytes() {
        let ops = RecordingOps { delete_fails: true, ..Default::default() };
        let outcome = delocate_replica(&ops).await;
        assert_eq!(outcome, DelocateOutcome::RemovedBytesLeftover);
        assert_eq!(
            *ops.calls.lock().unwrap(),
            vec!["remove", "delete"],
            "remove commits first; the failed delete is NOT rolled back"
        );
    }
}

#[cfg(test)]
mod advance_round_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    /// Programmable mock for the side effects of the migration state machine.
    #[derive(Default)]
    struct MockOps {
        /// Live count of `replicate` futures in flight, and the peak observed.
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
        replicate_calls: AtomicUsize,
        verify_calls: AtomicUsize,
        register_calls: AtomicUsize,
        /// CR-3-4/MGR-4 — when > 0, `register_replica` returns
        /// `Err` and decrements; once it reaches 0, calls succeed. Lets a
        /// test assert the task stays Completing across N failures and
        /// reaches Done once the underlying registration succeeds.
        register_fail_times: AtomicUsize,
        /// Per-stage overrides for failure-path tests.
        chunk_missing: bool,          // chunk_bytes -> None (Bug AE)
        invalid_target: bool,         // validate_target -> Err (Bug CH)
        no_source: bool,              // resolve_source -> None
        verify_result: VerifyMode,
        replicate_err: Option<String>,
        audited: Mutex<Vec<(String, String)>>, // (action, outcome)
        failed_alerts: AtomicUsize,
        /// WI-04 — when true, `target_addr` returns a distinct address per
        /// `node_id` (`http://tgt{n}:9500`) instead of one shared address,
        /// so `copies_across_targets_use_the_global_cap` can prove the
        /// per-target gate doesn't degrade into a global throttle.
        distinct_targets: bool,
    }

    #[derive(Clone, Copy, Default)]
    enum VerifyMode {
        #[default]
        Ok,
        Mismatch,
        RpcErr,
    }

    impl MigrationOps for MockOps {
        async fn validate_target(&self, _t: u64, _tier: DiskTier) -> Result<(), String> {
            if self.invalid_target {
                Err("target node is not a data node".to_string())
            } else {
                Ok(())
            }
        }
        async fn chunk_bytes(&self, _c: &str) -> Option<u64> {
            if self.chunk_missing {
                None
            } else {
                Some(4 << 20)
            }
        }
        async fn resolve_source(&self, _c: &str, _ex: Option<u64>) -> Option<(u64, String)> {
            if self.no_source {
                None
            } else {
                Some((1, "http://src:9500".to_string()))
            }
        }
        async fn target_addr(&self, n: u64) -> Option<String> {
            if self.distinct_targets {
                Some(format!("http://tgt{n}:9500"))
            } else {
                Some("http://tgt:9500".to_string())
            }
        }
        async fn replicate(
            &self,
            _src: &str,
            _chunk: &str,
            _tgt: &str,
            _tier: &str,
            _bytes: u64,
        ) -> Result<(), String> {
            self.replicate_calls.fetch_add(1, Ordering::SeqCst);
            // Track peak concurrency: enter, yield enough that buffer_unordered
            // has polled the whole window, then leave.
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            for _ in 0..4 {
                tokio::task::yield_now().await;
            }
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            match &self.replicate_err {
                Some(e) => Err(e.clone()),
                None => Ok(()),
            }
        }
        async fn verify(&self, _tgt: &str, _chunk: &str, _disk: Option<u32>) -> Result<bool, String> {
            self.verify_calls.fetch_add(1, Ordering::SeqCst);
            match self.verify_result {
                VerifyMode::Ok => Ok(true),
                VerifyMode::Mismatch => Ok(false),
                VerifyMode::RpcErr => Err("verify rpc down".to_string()),
            }
        }
        async fn register_replica(&self, _c: &str, _t: u64, _tier: DiskTier) -> Result<(), String> {
            self.register_calls.fetch_add(1, Ordering::SeqCst);
            // CR-3-4/MGR-4 — decrement-and-fail while > 0.
            let mut remaining = self.register_fail_times.load(Ordering::SeqCst);
            while remaining > 0 {
                match self.register_fail_times.compare_exchange(
                    remaining,
                    remaining - 1,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => return Err("mock register_replica failure".to_string()),
                    Err(actual) => remaining = actual,
                }
            }
            Ok(())
        }
        fn audit(&self, action: &str, _detail: &str, outcome: &str) {
            self.audited
                .lock()
                .unwrap()
                .push((action.to_string(), outcome.to_string()));
        }
        fn fire_migration_failed(&self, _task: &ReplicationTask, _reason: &str) {
            self.failed_alerts.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn pending_task(n: usize) -> ReplicationTask {
        ReplicationTask {
            task_id: format!("repl-{n}"),
            chunk_id: format!("chunk-{n}"),
            source_node: 1,
            target_node: 2,
            tier: DiskTier::Warm,
            status: MigrationStatus::Pending,
            created_at_ms: 0,
            updated_at_ms: 0,
            attempts: 0,
            last_error: None,
        }
    }

    fn state_with(n: usize) -> MigrationState {
        let mut s = MigrationState::default();
        for i in 0..n {
            let t = pending_task(i);
            s.tasks.insert(t.task_id.clone(), t);
        }
        s.next_task_id = n as u64;
        s
    }

    /// WI-04 — like `pending_task`, but pinned to an explicit `target_node`
    /// so a test can control how many distinct targets a batch spreads
    /// over (with `MockOps::distinct_targets`, `target_addr` derives a
    /// unique address per node id).
    fn pending_task_to(n: usize, target_node: u64) -> ReplicationTask {
        let mut t = pending_task(n);
        t.target_node = target_node;
        t
    }

    /// WI-04 — `n` tasks, each targeting a DISTINCT node id (100..100+n),
    /// for `copies_across_targets_use_the_global_cap`.
    fn state_with_targets(n: usize) -> MigrationState {
        let mut s = MigrationState::default();
        for i in 0..n {
            let t = pending_task_to(i, 100 + i as u64);
            s.tasks.insert(t.task_id.clone(), t);
        }
        s.next_task_id = n as u64;
        s
    }

    // Bug CN — a large terminal (Failed) backlog must be GC'd regardless of the
    // advance round. Pre-CN the prune ran AFTER advance_migration_tasks, so a
    // flood backlog starved its own GC (prod 2026-06-10: 2439 Failed cache-
    // promote + warm-backfill tasks never aged out, bloating migration_state).
    // prune_terminal_tasks now runs FIRST; this asserts it clears the stale
    // backlog while keeping fresh-Failed (for operator inspection) + actives.
    #[test]
    fn prune_terminal_clears_stale_failed_backlog() {
        // The compiled-in default windows (raft.{done,failed}_task_retention_secs).
        const DONE_MS: u64 = 5 * 60 * 1000;
        const FAILED_MS: u64 = 30 * 60 * 1000;
        let now = 60 * 60 * 1000; // 1h
        let mut s = MigrationState::default();
        for i in 0..100 {
            let mut t = pending_task(i);
            t.status = MigrationStatus::Failed;
            t.updated_at_ms = now - 31 * 60 * 1000; // past 30min retention
            s.tasks.insert(t.task_id.clone(), t);
        }
        for i in 100..105 {
            let mut t = pending_task(i);
            t.status = MigrationStatus::Failed;
            t.updated_at_ms = now - 5 * 60 * 1000; // fresh — keep
            s.tasks.insert(t.task_id.clone(), t);
        }
        for i in 105..108 {
            s.tasks.insert(format!("repl-{i}"), pending_task(i)); // Pending — keep
        }
        let pruned = prune_terminal_tasks(&mut s, now, DONE_MS, FAILED_MS);
        assert_eq!(pruned, 100, "stale Failed backlog must be pruned");
        assert_eq!(s.tasks.len(), 8, "5 fresh-Failed + 3 Pending remain");
    }

    // U3 core: every Pending task reaches Done within ONE advance_migration_tasks
    // call (pre-U3 each tick advanced a task by a single stage, so this would
    // leave them all at Copying).
    #[tokio::test]
    async fn all_tasks_reach_done_in_one_call() {
        let ops = MockOps::default();
        let mut state = state_with(10);

        advance_migration_tasks(&ops, &mut state, 1_000).await;

        assert!(
            state.tasks.values().all(|t| t.status == MigrationStatus::Done),
            "all tasks should be Done after one call, got {:?}",
            state.tasks.values().map(|t| t.status).collect::<Vec<_>>()
        );
        assert_eq!(ops.replicate_calls.load(Ordering::SeqCst), 10);
        assert_eq!(ops.verify_calls.load(Ordering::SeqCst), 10);
        assert_eq!(ops.register_calls.load(Ordering::SeqCst), 10);
    }

    /// CR-3-4/MGR-4 — a `register_replica` failure (Raft write error,
    /// unresolvable hash, no live disk of the tier) must keep the task
    /// Completing (bumping `attempts`) instead of unconditionally
    /// advancing to Done; once registration succeeds, the SAME task
    /// reaches Done and no bytes/coverage are silently lost in between.
    #[tokio::test]
    async fn cr_3_4_register_failure_keeps_task_completing_then_succeeds() {
        let ops = MockOps {
            register_fail_times: AtomicUsize::new(1),
            ..Default::default()
        };
        let mut state = MigrationState::default();
        let mut t = pending_task(0);
        t.status = MigrationStatus::Completing;
        let task_id = t.task_id.clone();
        state.tasks.insert(task_id.clone(), t);

        let mut copied = std::collections::HashSet::new();
        let mut verified = std::collections::HashSet::new();

        // Round 1: register_replica fails -> task stays Completing, attempts+=1.
        let progressed = advance_round(&ops, &mut state, 1_000, &mut copied, &mut verified).await;
        assert!(progressed, "a failed registration still counts as progress (attempts bumped)");
        let task = state.tasks.get(&task_id).unwrap();
        assert_eq!(task.status, MigrationStatus::Completing, "must NOT advance to Done on Err");
        assert_eq!(task.attempts, 1);

        // Next tick (fresh once-per-call guards): register_replica succeeds -> Done.
        let (mut copied, mut verified) = (Default::default(), Default::default());
        let progressed = advance_round(&ops, &mut state, 2_000, &mut copied, &mut verified).await;
        assert!(progressed);
        let task = state.tasks.get(&task_id).unwrap();
        assert_eq!(task.status, MigrationStatus::Done, "retry succeeds once the failure clears");

        assert_eq!(ops.register_calls.load(Ordering::SeqCst), 2);
    }

    /// CR-3-4/MGR-4 — a `register_replica` failure that never clears
    /// eventually fails the task at the same cap as a Copying-stage
    /// failure, so a permanently-stuck registration doesn't retry
    /// forever either.
    #[tokio::test]
    async fn cr_3_4_register_failure_fails_task_at_attempt_cap() {
        let ops = MockOps {
            register_fail_times: AtomicUsize::new(usize::MAX),
            ..Default::default()
        };
        let mut state = MigrationState::default();
        let mut t = pending_task(0);
        t.status = MigrationStatus::Completing;
        let task_id = t.task_id.clone();
        state.tasks.insert(task_id.clone(), t);

        for _ in 0..MAX_MIGRATION_ATTEMPTS {
            // One call per tick: the once-per-call guards are fresh each time.
            advance_migration_tasks(&ops, &mut state, 1_000).await;
        }
        let task = state.tasks.get(&task_id).unwrap();
        assert_eq!(task.status, MigrationStatus::Failed);
        assert_eq!(ops.failed_alerts.load(Ordering::SeqCst), 1);
    }

    /// A failing registration burns exactly ONE attempt per advance call
    /// (tick), not every round of the loop.
    #[tokio::test]
    async fn register_failure_burns_one_attempt_per_call() {
        let ops = MockOps {
            register_fail_times: AtomicUsize::new(usize::MAX),
            ..Default::default()
        };
        let mut state = MigrationState::default();
        let mut t = pending_task(0);
        t.status = MigrationStatus::Completing;
        let task_id = t.task_id.clone();
        state.tasks.insert(task_id.clone(), t);

        advance_migration_tasks(&ops, &mut state, 1_000).await;
        let task = state.tasks.get(&task_id).unwrap();
        assert_eq!(task.attempts, 1);
        assert_eq!(task.status, MigrationStatus::Completing);
        assert_eq!(ops.register_calls.load(Ordering::SeqCst), 1);
    }

    // WI-04 — at most MIGRATION_COPIES_PER_TARGET ReplicateChunk RPCs are
    // ever in flight against ONE target address, even with far more Copying
    // tasks than that cap all aimed at the same target (the anchor-repair
    // shape: every copy lands on the single LELI anchor node). The gate
    // queues rather than drops: all 50 tasks still reach Done in one call.
    #[tokio::test]
    async fn copies_to_one_target_are_bounded() {
        let ops = MockOps::default();
        let mut state = state_with(50); // all target_node: 2 -> one tgt_addr

        advance_migration_tasks(&ops, &mut state, 1_000).await;

        let peak = ops.max_in_flight.load(Ordering::SeqCst);
        assert!(
            peak <= MIGRATION_COPIES_PER_TARGET,
            "peak in-flight to one target {peak} exceeded per-target cap {MIGRATION_COPIES_PER_TARGET}"
        );
        assert!(
            state.tasks.values().all(|t| t.status == MigrationStatus::Done),
            "all tasks should still reach Done in one call, got {:?}",
            state.tasks.values().map(|t| t.status).collect::<Vec<_>>()
        );
        assert_eq!(ops.replicate_calls.load(Ordering::SeqCst), 50);
    }

    // WI-04 — the per-target gate must not become a global throttle: spread
    // the same 50 copies over 50 distinct targets and the peak climbs back
    // up past the per-target cap, bounded only by the global
    // MIGRATION_COPY_CONCURRENCY cap.
    // Supersedes the U3 `copy_concurrency_is_bounded` test, which after
    // WI-04's per-target gate could only ever observe a peak of 2 (all of
    // `state_with`'s tasks share `target_node: 2`).
    #[tokio::test]
    async fn copies_across_targets_use_the_global_cap() {
        let ops = MockOps {
            distinct_targets: true,
            ..Default::default()
        };
        let mut state = state_with_targets(50);

        advance_migration_tasks(&ops, &mut state, 1_000).await;

        let peak = ops.max_in_flight.load(Ordering::SeqCst);
        assert!(
            peak > MIGRATION_COPIES_PER_TARGET,
            "peak {peak} did not exceed the per-target cap {MIGRATION_COPIES_PER_TARGET} \
             across distinct targets — the per-target gate degenerated into a global throttle"
        );
        assert!(
            peak <= MIGRATION_COPY_CONCURRENCY,
            "peak in-flight {peak} exceeded the global cap {MIGRATION_COPY_CONCURRENCY}"
        );
    }

    // Bug CH preserved: an invalid target fast-fails to Failed without ever
    // attempting a copy, and audits migration.invalid_target.
    #[tokio::test]
    async fn invalid_target_fast_fails() {
        let ops = MockOps {
            invalid_target: true,
            ..Default::default()
        };
        let mut state = state_with(3);

        advance_migration_tasks(&ops, &mut state, 1_000).await;

        assert!(state.tasks.values().all(|t| t.status == MigrationStatus::Failed));
        assert_eq!(ops.replicate_calls.load(Ordering::SeqCst), 0);
        assert!(ops
            .audited
            .lock()
            .unwrap()
            .iter()
            .any(|(a, _)| a == "migration.invalid_target"));
    }

    // Bug AE preserved: an orphan chunk (no longer in any file) is dropped to
    // Failed before any copy, auditing migration.orphan_dropped.
    #[tokio::test]
    async fn orphan_chunk_dropped() {
        let ops = MockOps {
            chunk_missing: true,
            ..Default::default()
        };
        let mut state = state_with(2);

        advance_migration_tasks(&ops, &mut state, 1_000).await;

        assert!(state.tasks.values().all(|t| t.status == MigrationStatus::Failed));
        assert_eq!(ops.replicate_calls.load(Ordering::SeqCst), 0);
        assert!(ops
            .audited
            .lock()
            .unwrap()
            .iter()
            .any(|(a, _)| a == "migration.orphan_dropped"));
    }

    // P0-4 preserved: a no-source task burns exactly ONE attempt per call
    // (per tick) — the once-per-call guard preserves the pre-U3 retry
    // cadence (6 ticks to exhaust, not collapsed into one). After
    // MAX_MIGRATION_ATTEMPTS calls it lands Failed with the alert fired once.
    #[tokio::test]
    async fn no_source_burns_one_attempt_per_tick_then_fails() {
        let ops = MockOps {
            no_source: true,
            ..Default::default()
        };
        let mut state = state_with(1);

        // One call charges exactly one attempt; the task stays Copying.
        advance_migration_tasks(&ops, &mut state, 1_000).await;
        let t = state.tasks.values().next().unwrap();
        assert_eq!(t.status, MigrationStatus::Copying);
        assert_eq!(t.attempts, 1, "one attempt per tick");

        // Remaining ticks drive it to Failed exactly at the attempt cap.
        for _ in 1..MAX_MIGRATION_ATTEMPTS {
            advance_migration_tasks(&ops, &mut state, 1_000).await;
        }
        let t = state.tasks.values().next().unwrap();
        assert_eq!(t.status, MigrationStatus::Failed);
        assert_eq!(t.attempts, MAX_MIGRATION_ATTEMPTS);
        assert_eq!(ops.failed_alerts.load(Ordering::SeqCst), 1);
    }

    // Bug AF preserved: a source-side "Chunk not found" promotes straight to
    // Failed on the FIRST attempt (no 6-retry storm).
    #[tokio::test]
    async fn chunk_not_found_instant_fails() {
        let ops = MockOps {
            replicate_err: Some("ReplicateChunk error: Chunk not found".to_string()),
            ..Default::default()
        };
        let mut state = state_with(1);

        advance_migration_tasks(&ops, &mut state, 1_000).await;

        let t = state.tasks.values().next().unwrap();
        assert_eq!(t.status, MigrationStatus::Failed);
        assert_eq!(t.attempts, MAX_MIGRATION_ATTEMPTS);
        // Exactly one copy attempt despite the multi-round loop.
        assert_eq!(ops.replicate_calls.load(Ordering::SeqCst), 1);
    }

    // Verify-mismatch flaps back to Copying and does NOT count as progress,
    // so the round loop terminates (the task re-copies on the next tick
    // rather than spinning all MAX_ADVANCE_ROUNDS in one tick).
    #[tokio::test]
    async fn verify_mismatch_flaps_to_copying_then_stops() {
        let ops = MockOps {
            verify_result: VerifyMode::Mismatch,
            ..Default::default()
        };
        let mut state = state_with(1);

        advance_migration_tasks(&ops, &mut state, 1_000).await;

        let t = state.tasks.values().next().unwrap();
        // Pending->Copying->Verifying(mismatch)->Copying, then the loop's
        // no-progress break stops; copy ran once, verify ran once.
        assert_eq!(t.status, MigrationStatus::Copying);
        assert_eq!(ops.replicate_calls.load(Ordering::SeqCst), 1);
        assert_eq!(ops.verify_calls.load(Ordering::SeqCst), 1);
    }

    // A transient verify RPC error leaves the task in Verifying (no status
    // change), so it retries next tick (not this one).
    #[tokio::test]
    async fn verify_rpc_error_stays_verifying() {
        let ops = MockOps {
            verify_result: VerifyMode::RpcErr,
            ..Default::default()
        };
        let mut state = state_with(1);

        advance_migration_tasks(&ops, &mut state, 1_000).await;

        let t = state.tasks.values().next().unwrap();
        assert_eq!(t.status, MigrationStatus::Verifying);
    }

    // CO-3-4 — a persistently corrupt source (copy succeeds, VerifyChunk
    // always mismatches) MUST eventually fail at MAX_MIGRATION_ATTEMPTS, not
    // loop Copying<->Verifying forever. Today a mismatch flaps back to Copying
    // WITHOUT bumping `attempts` (advance_round phase 3, `Ok(false)` arm), so
    // the task never reaches the cap: it re-copies + re-mismatches every tick
    // indefinitely, pinning a scheduler slot + a data node on garbage bytes.
    // Correct behaviour: a verify mismatch is a failed attempt; after
    // MAX_MIGRATION_ATTEMPTS the task lands Failed with the alert fired.
    #[tokio::test]
    async fn co_3_4_persistent_verify_mismatch_eventually_fails() {
        let ops = MockOps {
            verify_result: VerifyMode::Mismatch,
            ..Default::default()
        };
        let mut state = state_with(1);

        // Drive generously more than the attempt cap (one copy+verify per
        // tick). A correct state machine fails within MAX_MIGRATION_ATTEMPTS
        // ticks; the buggy one never does, so the extra ticks just confirm the
        // infinite loop.
        for _ in 0..(MAX_MIGRATION_ATTEMPTS + 4) {
            advance_migration_tasks(&ops, &mut state, 1_000).await;
        }

        let t = state.tasks.values().next().unwrap();
        assert_eq!(
            t.status,
            MigrationStatus::Failed,
            "a persistently corrupt source must fail, not loop (status={:?}, attempts={})",
            t.status,
            t.attempts
        );
        assert_eq!(
            ops.failed_alerts.load(Ordering::SeqCst),
            1,
            "exhausting retries must fire exactly one MigrationFailed alert"
        );
    }
}

#[cfg(test)]
mod co_3_5_migration_state_race_tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::{Mutex, RwLock};

    /// Run two concurrent load-modify-save "writers" against a shared
    /// `MigrationState`, each inserting one new task and bumping
    /// `next_task_id`, all under one shared mutex (the production pattern).
    /// Both tasks must survive and `next_task_id` must advance by 2.
    #[tokio::test]
    async fn co_3_5_concurrent_rmw_under_mutex_keeps_both_tasks() {
        // The shared "Raft" backing store + the serialising lock.
        let store: Arc<RwLock<MigrationState>> =
            Arc::new(RwLock::new(MigrationState::default()));
        let lock: Arc<Mutex<()>> = Arc::new(Mutex::new(()));

        // One writer mirrors schedule_replication's critical section: take
        // the lock, load, modify (new task + next_task_id++), save. The
        // `await` points inside model the real async load/save.
        let writer = |store: Arc<RwLock<MigrationState>>,
                      lock: Arc<Mutex<()>>,
                      chunk: &'static str| async move {
            let _guard = lock.lock().await;
            // load
            let mut state = store.read().await.clone();
            // a yield between load and save widens the race window so an
            // unguarded version would reliably lose an update.
            tokio::task::yield_now().await;
            // modify
            let id = format!("repl-{}", state.next_task_id);
            state.next_task_id += 1;
            state.tasks.insert(
                id.clone(),
                ReplicationTask {
                    task_id: id,
                    chunk_id: chunk.to_string(),
                    source_node: 1,
                    target_node: 2,
                    tier: DiskTier::Warm,
                    status: MigrationStatus::Pending,
                    created_at_ms: 0,
                    updated_at_ms: 0,
                    attempts: 0,
                    last_error: None,
                },
            );
            // save
            *store.write().await = state;
        };

        let a = tokio::spawn(writer(store.clone(), lock.clone(), "chunk-a"));
        let b = tokio::spawn(writer(store.clone(), lock.clone(), "chunk-b"));
        a.await.unwrap();
        b.await.unwrap();

        let final_state = store.read().await;
        assert_eq!(
            final_state.tasks.len(),
            2,
            "both concurrent schedules must survive (no lost update)"
        );
        assert_eq!(
            final_state.next_task_id, 2,
            "next_task_id must advance monotonically by 2, never regress"
        );
        let chunks: std::collections::HashSet<&str> = final_state
            .tasks
            .values()
            .map(|t| t.chunk_id.as_str())
            .collect();
        assert!(chunks.contains("chunk-a") && chunks.contains("chunk-b"));
    }
}

#[cfg(test)]
mod mgrsched_audit_repair_tests {
    use super::*;
    use crate::placement_state::{anchor_met, cache_allowance, distinct_dcs, goal_met};
    use dtfs_common::types::{
        AnchorPolicy, CacheReplication, CacheTierConfig, DiskInfo, DiskStatus, DiskTier,
        HealthStatus, NodeRole, TierReplication,
    };
    use std::collections::HashSet;

    // ---- Task 1: apply-level Error is a failure ----
    #[test]
    fn client_response_ok_detects_refusal_only() {
        let err = dtfs_raft::ClientResponse::Error { message: "would strand anchor".into() };
        assert_eq!(client_response_ok(&err), Err("would strand anchor".to_string()));
        assert_eq!(client_response_ok(&dtfs_raft::ClientResponse::Success), Ok(()));
    }

    // ---- Task 2: anchor DC case-insensitive ----
    fn anchor(dc: &str) -> AnchorPolicy {
        AnchorPolicy { datacenter: dc.into(), required_tier: Some(DiskTier::Cold) }
    }

    #[test]
    fn anchor_met_ignores_case_and_checks_tier() {
        let a = anchor("leli");
        assert!(anchor_met(&a, [("LELI", DiskTier::Cold)].into_iter()));
        assert!(!anchor_met(&a, [("LELI", DiskTier::Warm)].into_iter()));
        assert!(!anchor_met(&a, [("toro", DiskTier::Cold)].into_iter()));
    }

    #[test]
    fn goal_met_requires_tiers_and_anchor() {
        let m = TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 };
        let reps = [("LELI", DiskTier::Cold)];
        assert!(goal_met(reps.iter().copied(), &m, Some(&anchor("leli"))));
        assert!(!goal_met(reps.iter().copied(), &m, Some(&anchor("toro"))));
        assert!(!goal_met([("LELI", DiskTier::Warm)].iter().copied(), &m, None));
    }

    #[test]
    fn spill_removal_and_lag_repair_ignore_anchor_case() {
        let m = TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 };
        let a = anchor("leli");
        let reps = vec![
            (1, 0, "LELI".to_string(), DiskTier::Cold),
            (2, 0, "toro".to_string(), DiskTier::Warm),
        ];
        // stored "LELI" satisfies anchor "leli", so the spill (node 2) goes, never node 1
        assert_eq!(plan_excess_spill_removal(&reps, &m, Some(&a), 0), Some((2, 0)));
        let placements = vec![AnchorRepairPlacement { node_id: 1, dc: "LELI".into(), tier: DiskTier::Cold }];
        let nodes = vec![AnchorRepairNode { node_id: 2, datacenter: "LELI".into(), healthy: true, tiers: vec![DiskTier::Cold] }];
        assert_eq!(plan_anchor_lag_repair(&a, &placements, &nodes, "h", &HashSet::new()), None);
        // target node in "LELI" is eligible for anchor "leli"
        let placements = vec![AnchorRepairPlacement { node_id: 9, dc: "toro".into(), tier: DiskTier::Warm }];
        let act = plan_anchor_lag_repair(&a, &placements, &nodes, "h", &HashSet::new()).unwrap();
        assert_eq!(act.target_node, 2);
    }

    // ---- Task 4: per-DC cache allowance, Plexmedia policy ----
    fn plex_cache() -> CacheReplication {
        let mut c = CacheReplication::default();
        c.warm = CacheTierConfig { min: 1, max: 1 };
        c.per_dc = true;
        c
    }

    #[test]
    fn cache_allowance_scales_with_dcs_when_per_dc() {
        let mut c = plex_cache();
        assert_eq!(cache_allowance(&c, 1), 1);
        assert_eq!(cache_allowance(&c, 3), 3);
        assert_eq!(cache_allowance(&c, 0), 1);
        c.per_dc = false;
        assert_eq!(cache_allowance(&c, 3), 1);
        assert_eq!(distinct_dcs(["LELI", "leli", "toro"].into_iter()), 2);
    }

    #[test]
    fn plexmedia_two_watching_dcs_not_removed() {
        let m = TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 };
        let a = anchor("LELI");
        let reps = vec![
            (1, 0, "leli".to_string(), DiskTier::Cold),
            (2, 0, "toro".to_string(), DiskTier::Warm),
            (3, 0, "nrb".to_string(), DiskTier::Warm),
        ];
        let c = plex_cache();
        let allow = cache_allowance(&c, distinct_dcs(reps.iter().map(|r| r.2.as_str())));
        assert_eq!(plan_excess_spill_removal(&reps, &m, Some(&a), allow), None);
        // pre-fix flat allowance of 1 would remove a warm copy -> loop
        assert!(plan_excess_spill_removal(&reps, &m, Some(&a), 1).is_some());
        // genuinely over: a 4th warm copy in toro exceeds the per-DC allowance
        let mut over = reps.clone();
        over.push((4, 0, "toro".to_string(), DiskTier::Warm));
        over.push((5, 0, "toro".to_string(), DiskTier::Warm));
        assert!(plan_excess_spill_removal(&over, &m, Some(&a), allow).is_some());
    }

    // ---- Task 7: anchor target tiers from live disks with room ----
    fn disk(id: u32, tier: DiskTier, status: DiskStatus, total: u64, used: u64) -> DiskInfo {
        DiskInfo { disk_id: id, uuid: String::new(), mount_path: String::new(), tier, total_bytes: total, used_bytes: used, status }
    }

    fn node_with(disks: Vec<DiskInfo>) -> dtfs_raft::NodeInfo {
        dtfs_raft::NodeInfo {
            hostname: String::new(),
            grpc_addr: String::new(),
            role: NodeRole::Data,
            disks,
            location: dtfs_common::topology::Location::new("", "", "", ""),
            health: HealthStatus::Healthy,
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

    #[test]
    fn anchor_target_tiers_skip_failed_and_full_disks() {
        let cfg = dtfs_common::config::PlacementConfig::default();
        let n = node_with(vec![
            disk(0, DiskTier::Cold, DiskStatus::Failed, 0, 0),
            disk(1, DiskTier::Cold, DiskStatus::Healthy, 10_000_000_000_000, 10_000_000_000_000), // full
            disk(2, DiskTier::Warm, DiskStatus::Healthy, 10_000_000_000_000, 0),
        ]);
        assert_eq!(anchor_target_tiers(&n, &cfg), vec![DiskTier::Warm]);
        let ok = node_with(vec![disk(3, DiskTier::Cold, DiskStatus::Healthy, 10_000_000_000_000, 0)]);
        assert_eq!(anchor_target_tiers(&ok, &cfg), vec![DiskTier::Cold]);
    }

    #[test]
    fn lag_repair_falls_back_to_other_anchor_dc_node() {
        let cfg = dtfs_common::config::PlacementConfig::default();
        let bad = node_with(vec![disk(0, DiskTier::Cold, DiskStatus::Failed, 0, 0)]);
        let good = node_with(vec![disk(0, DiskTier::Cold, DiskStatus::Healthy, 10_000_000_000_000, 0)]);
        let nodes = vec![
            AnchorRepairNode { node_id: 1, datacenter: "leli".into(), healthy: true, tiers: anchor_target_tiers(&bad, &cfg) },
            AnchorRepairNode { node_id: 2, datacenter: "leli".into(), healthy: true, tiers: anchor_target_tiers(&good, &cfg) },
        ];
        let placements = vec![AnchorRepairPlacement { node_id: 9, dc: "toro".into(), tier: DiskTier::Warm }];
        let act = plan_anchor_lag_repair(&anchor("leli"), &placements, &nodes, "h", &HashSet::new()).unwrap();
        assert_eq!(act.target_node, 2, "node 1's only cold disk failed");
    }
}

#[cfg(test)]
mod batch_schedule_tests {
    use super::*;

    fn ids(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("c{i}")).collect()
    }

    /// N promotions mutate ONE state value (caller saves it once) and bump
    /// next_task_id N times.
    #[test]
    fn batch_creates_all_in_one_state() {
        let ids = ids(5);
        let reqs: Vec<_> = ids.iter().map(|c| (c.as_str(), 1u64, 2u64, DiskTier::Warm)).collect();
        let mut st = MigrationState::default();
        let out = plan_replication_batch(&mut st, &reqs, usize::MAX, MAX_LIVE_TASKS, 10);
        assert!(out.iter().all(|o| *o == ScheduleOutcome::New));
        assert_eq!(st.tasks.len(), 5);
        assert_eq!(st.next_task_id, 5);
    }

    /// Already-scheduled pairs do not count toward the cap, so later chunks
    /// are not starved; the cap bounds NEW tasks only.
    #[test]
    fn dedup_does_not_consume_cap() {
        let ids = ids(6);
        let mut st = MigrationState::default();
        let first: Vec<_> = ids[..3].iter().map(|c| (c.as_str(), 1u64, 2u64, DiskTier::Warm)).collect();
        plan_replication_batch(&mut st, &first, usize::MAX, MAX_LIVE_TASKS, 10);
        let all: Vec<_> = ids.iter().map(|c| (c.as_str(), 1u64, 2u64, DiskTier::Warm)).collect();
        let out = plan_replication_batch(&mut st, &all, 2, MAX_LIVE_TASKS, 11);
        use ScheduleOutcome::*;
        assert_eq!(out, vec![AlreadyScheduled, AlreadyScheduled, AlreadyScheduled, New, New, Deferred]);
        assert_eq!(st.tasks.len(), 5);
    }

    /// Live-task budget refusal is kept.
    #[test]
    fn budget_refuses() {
        let mut st = MigrationState::default();
        let big = ids(MAX_LIVE_TASKS);
        let reqs: Vec<_> = big.iter().map(|c| (c.as_str(), 1u64, 2u64, DiskTier::Warm)).collect();
        plan_replication_batch(&mut st, &reqs, usize::MAX, MAX_LIVE_TASKS, 1);
        let out = plan_replication_batch(&mut st, &[("extra", 1, 2, DiskTier::Warm)], usize::MAX, MAX_LIVE_TASKS, 2);
        assert_eq!(out, vec![ScheduleOutcome::Refused]);
    }
    fn task(id: &str, status: MigrationStatus) -> ReplicationTask {
        ReplicationTask {
            task_id: id.to_string(),
            chunk_id: format!("c-{id}"),
            source_node: 1,
            target_node: 2,
            tier: DiskTier::Warm,
            status,
            created_at_ms: 0,
            updated_at_ms: 0,
            attempts: 0,
            last_error: None,
        }
    }

    /// Failed (dedup-only) and Done tasks must not eat the live budget:
    /// a backlog of permanent failures used to starve mandatory repair.
    #[test]
    fn failed_tasks_do_not_count_toward_live_budget() {
        let mut st = MigrationState::default();
        for i in 0..MAX_LIVE_TASKS {
            let t = task(&format!("f{i}"), MigrationStatus::Failed);
            st.tasks.insert(t.task_id.clone(), t);
        }
        let t = task("p", MigrationStatus::Pending);
        st.tasks.insert(t.task_id.clone(), t);
        assert_eq!(live_task_count(&st), 1);
        let out = plan_replication_batch(&mut st, &[("new", 1, 2, DiskTier::Warm)], usize::MAX, MAX_LIVE_TASKS, 5);
        assert_eq!(out, vec![ScheduleOutcome::New]);
        // A Failed task still dedups the same (chunk, target).
        let out = plan_replication_batch(&mut st, &[("c-f0", 1, 2, DiskTier::Warm)], usize::MAX, MAX_LIVE_TASKS, 6);
        assert_eq!(out, vec![ScheduleOutcome::AlreadyScheduled]);
    }

    /// Tiering's lower ceiling refuses while the global ceiling still has
    /// room, exactly at the boundary.
    #[test]
    fn tiering_live_ceiling_is_lower_than_global() {
        assert!(TIERING_MAX_LIVE_TASKS < MAX_LIVE_TASKS);
        let mut st = MigrationState::default();
        for i in 0..TIERING_MAX_LIVE_TASKS {
            let t = task(&format!("p{i}"), MigrationStatus::Pending);
            st.tasks.insert(t.task_id.clone(), t);
        }
        let req = [("new", 1u64, 2u64, DiskTier::Warm)];
        let out = plan_replication_batch(&mut st, &req, usize::MAX, TIERING_MAX_LIVE_TASKS, 1);
        assert_eq!(out, vec![ScheduleOutcome::Refused], "promotions refused at their ceiling");
        let out = plan_replication_batch(&mut st, &req, usize::MAX, MAX_LIVE_TASKS, 2);
        assert_eq!(out, vec![ScheduleOutcome::New], "repair still has headroom");
    }

    #[test]
    fn rotated_from_visits_every_entry_once_from_the_seed() {
        let m: std::collections::BTreeMap<u32, ()> = (0..5).map(|i| (i, ())).collect();
        let keys = |seed| rotated_from(&m, seed).map(|(k, _)| *k).collect::<Vec<_>>();
        assert_eq!(keys(0), vec![0, 1, 2, 3, 4]);
        assert_eq!(keys(2), vec![2, 3, 4, 0, 1]);
        assert_eq!(keys(7), vec![2, 3, 4, 0, 1], "seed wraps modulo len");
        let empty: std::collections::BTreeMap<u32, ()> = Default::default();
        assert_eq!(rotated_from(&empty, 9).count(), 0);
    }

    #[test]
    fn active_pairs_exclude_terminal_tasks() {
        let mut st = MigrationState::default();
        for (id, s) in [
            ("a", MigrationStatus::Pending),
            ("b", MigrationStatus::Verifying),
            ("c", MigrationStatus::Done),
            ("d", MigrationStatus::Failed),
        ] {
            let t = task(id, s);
            st.tasks.insert(t.task_id.clone(), t);
        }
        let pairs = active_pairs_of(&st);
        assert_eq!(pairs.len(), 2);
        assert!(pairs.contains(&("c-a".to_string(), 2)));
        assert!(pairs.contains(&("c-b".to_string(), 2)));
        assert!(!pairs.contains(&("c-c".to_string(), 2)));
    }

    /// Only NEW tasks are reported as scheduled (dedup/refusal are not).
    #[test]
    fn new_task_count_counts_only_new() {
        use ScheduleOutcome::*;
        assert_eq!(new_task_count(&[New, AlreadyScheduled, Refused, Deferred, New]), 2);
        assert_eq!(new_task_count(&[]), 0);
    }
}

#[cfg(test)]
mod anchor_tier_fallback_tests {
    use super::*;
    use dtfs_common::types::AnchorPolicy;
    use std::collections::HashSet;

    fn anchor(tier: Option<DiskTier>) -> AnchorPolicy {
        AnchorPolicy { datacenter: "leli".to_string(), required_tier: tier }
    }
    fn node(id: u64, tiers: &[DiskTier]) -> AnchorRepairNode {
        AnchorRepairNode { node_id: id, datacenter: "leli".into(), healthy: true, tiers: tiers.to_vec() }
    }
    fn elsewhere() -> Vec<AnchorRepairPlacement> {
        vec![AnchorRepairPlacement { node_id: 9, dc: "toro".into(), tier: DiskTier::Warm }]
    }

    /// No required tier and no cold disk with room in the anchor DC: fall
    /// back to a tier that has room instead of stranding the anchor.
    #[test]
    fn none_required_tier_falls_back_when_cold_unavailable() {
        let nodes = vec![node(1, &[DiskTier::Warm])];
        let act = plan_anchor_lag_repair(&anchor(None), &elsewhere(), &nodes, "h", &HashSet::new())
            .expect("warm fallback");
        assert_eq!((act.target_node, act.target_tier), (1, DiskTier::Warm));
    }

    #[test]
    fn none_required_tier_still_prefers_cold() {
        let nodes = vec![node(1, &[DiskTier::Warm]), node(2, &[DiskTier::Cold])];
        let act = plan_anchor_lag_repair(&anchor(None), &elsewhere(), &nodes, "h", &HashSet::new()).unwrap();
        assert_eq!((act.target_node, act.target_tier), (2, DiskTier::Cold));
    }

    /// A required tier is exact: no fallback to another tier.
    #[test]
    fn required_tier_has_no_fallback() {
        let nodes = vec![node(1, &[DiskTier::Warm])];
        let a = anchor(Some(DiskTier::Cold));
        assert_eq!(plan_anchor_lag_repair(&a, &elsewhere(), &nodes, "h", &HashSet::new()), None);
    }

    /// No node with room in the anchor DC at all: no action.
    #[test]
    fn no_target_in_anchor_dc_yields_none() {
        let nodes = vec![node(1, &[])];
        assert_eq!(
            plan_anchor_lag_repair(&anchor(None), &elsewhere(), &nodes, "h", &HashSet::new()),
            None
        );
    }
}

#[cfg(test)]
mod issue_157_default_volume_tests {
    use super::super::default_volume_request;
    use dtfs_raft::state_machine::StateMachineData;
    use dtfs_raft::ClientRequest;

    fn cfg() -> dtfs_common::types::VolumeConfig {
        let mut v = dtfs_common::types::VolumeConfig::default();
        v.id = dtfs_common::types::VolumeId::new("default".to_string());
        v.name = "default".into();
        v
    }

    /// Issue #157 — a leader tick on a state without `default` proposes a
    /// create-only UpsertVolume; once it exists, nothing is proposed.
    #[test]
    fn issue_157_default_volume_created_only_when_missing() {
        let mut sm = StateMachineData::default();
        match default_volume_request(&sm, &cfg(), 42) {
            Some(ClientRequest::UpsertVolume { config, expected_version, now_ms }) => {
                assert_eq!(config.id.0, "default");
                assert_eq!(expected_version, Some(0), "must be create-only");
                assert_eq!(now_ms, 42);
            }
            other => panic!("expected UpsertVolume, got {other:?}"),
        }
        let mut existing = cfg();
        existing.version = 3;
        sm.volumes.insert("default".into(), existing);
        assert!(default_volume_request(&sm, &cfg(), 42).is_none());
    }
}
