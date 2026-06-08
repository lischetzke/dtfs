//! Health monitoring of data nodes — Phase 4 (Resilience).
//!
//! Periodically checks heartbeat and health status of each data-node,
//! detects failures, commits the status transitions to Raft, and fires
//! operator alerts.
//!
//! Bug CH — this module DETECTS only; it never schedules repair. Replica
//! repair (node loss, disk loss, under-replication) is owned exclusively
//! by the leader-gated scheduler tick: `live_replica_tier` stops counting
//! replicas on Down/Failed nodes and Failed/Removed disks, so
//! `check_under_replicated` backfills them through its deduped,
//! tier-aware planner. The previous in-module evacuation path
//! (`handle_node_down` / `handle_disk_failed`) ran on every manager
//! (no leader gate), round-robined targets over ALL registered nodes
//! (managers and FUSE clients included) and hardcoded `DiskTier::Hot`
//! — on 2026-06-06 it flooded the production leader with 36k garbage
//! tasks and took the cluster down (PLAN-BUG-CH).

use std::sync::Arc;
use std::time::Duration;

use dtfs_common::types::NodeRole;
use dtfs_raft::state_machine::StateMachineData;
use dtfs_raft::{ClientRequest, DtfsRaft};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::cluster::NodeStatus;

/// Default liveness thresholds (mirror the historical ClusterManager
/// values). A data node heartbeats every 5 s.
const SUSPECT_MS: u64 = 15_000;
const DOWN_MS: u64 = 30_000;

/// Bug CJ — compute health transitions from the REPLICATED `last_seen_ms`
/// in the state machine, not a per-manager in-memory heartbeat map.
///
/// A data node heartbeats exactly ONE manager (it walks the list only on
/// failure), and that manager commits `last_seen_ms` to raft via
/// `UpdateNodeUsage` (Bug BL forward), so every manager observes the same
/// freshness. The old monitor judged liveness from `ClusterManager`'s
/// local `DashMap`, which is only populated on the receiving manager — a
/// follower therefore held a stale/absent entry and judged the node Down.
/// That was harmless until Bug CH-1-6 made follower status writes actually
/// commit (instead of silently dropping ForwardToLeader): afterwards a
/// follower's bogus Down fought the receiver's `healthy` write every few
/// seconds and the node FLAPPED (live on production 2026-06-07,
/// dtfs-prod-data01).
///
/// Only `role == Data` nodes heartbeat (managers use raft; the FUSE client
/// never calls the heartbeat RPC), so judging any other role by
/// `last_seen_ms` — which never advances for them — would mark it
/// permanently Down. `last_seen_ms == 0` means "registered, no heartbeat
/// yet" and is left alone. Healthy is omitted (recovery is the heartbeat
/// handler's `UpdateNodeStatus(healthy)`), matching the prior contract.
pub(crate) fn liveness_changes(
    nodes: &std::collections::BTreeMap<u64, dtfs_raft::NodeInfo>,
    now_ms: u64,
    suspect_ms: u64,
    down_ms: u64,
) -> Vec<(u64, NodeStatus)> {
    let mut out = Vec::new();
    for (id, info) in nodes {
        if info.role != NodeRole::Data || info.last_seen_ms == 0 {
            continue;
        }
        let elapsed = now_ms.saturating_sub(info.last_seen_ms);
        if elapsed > down_ms {
            out.push((*id, NodeStatus::Down));
        } else if elapsed > suspect_ms {
            out.push((*id, NodeStatus::Suspect));
        }
    }
    out
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Periodic health monitoring loop.
pub struct HealthMonitor {
    check_interval: Duration,
    state_data: Arc<RwLock<StateMachineData>>,
    /// Raft handle used to commit node-status changes to the cluster state machine.
    raft: Arc<DtfsRaft>,
    /// Bug CH — manager peers for leader-forwarding status commits.
    /// Heartbeats land on ONE arbitrary manager, so the monitor that
    /// detects a transition is usually a follower; a bare client_write
    /// would silently drop the status change (case 55 regression).
    voters: Vec<dtfs_common::config::InitialVoter>,
    /// P1-8: optional alert dispatcher. NodeDown / DiskFailed fire here.
    alerts: Option<Arc<crate::alerts::AlertDispatcher>>,
}

impl HealthMonitor {
    pub fn new(
        check_interval: Duration,
        state_data: Arc<RwLock<StateMachineData>>,
        raft: Arc<DtfsRaft>,
        voters: Vec<dtfs_common::config::InitialVoter>,
    ) -> Self {
        Self {
            check_interval,
            state_data,
            raft,
            voters,
            alerts: None,
        }
    }

    /// Inject the alert dispatcher after construction.
    pub fn with_alerts(mut self, alerts: Arc<crate::alerts::AlertDispatcher>) -> Self {
        self.alerts = Some(alerts);
        self
    }

    /// Run the monitoring loop. This is intended to be spawned as a background
    /// tokio task and runs indefinitely.
    pub async fn run(&self) {
        info!(
            "Health monitor started (interval: {:?})",
            self.check_interval
        );

        let mut interval = tokio::time::interval(self.check_interval);
        loop {
            interval.tick().await;

            // Bug CJ — judge liveness from the replicated `last_seen_ms`
            // so every manager agrees (see `liveness_changes`); the old
            // per-manager heartbeat map made followers flap the node.
            let changes = {
                let now_ms = now_millis();
                let sm = self.state_data.read().await;
                liveness_changes(&sm.nodes, now_ms, SUSPECT_MS, DOWN_MS)
            };

            for (node_id, status) in &changes {
                match status {
                    NodeStatus::Suspect => {
                        warn!(
                            "Node {} is SUSPECT -- heartbeat overdue",
                            node_id
                        );
                        // Commit the suspect status to the cluster state machine so
                        // placement can avoid new writes to this node.
                        self.update_node_status_in_raft(*node_id, "suspect").await;
                    }
                    NodeStatus::Down => {
                        warn!("Node {} is DOWN", node_id);
                        // Mark in Raft so placement and tiering stop using
                        // this node — and so the leader's scheduler tick
                        // stops counting its replicas (Bug CH:
                        // live_replica_tier) and backfills the lost
                        // copies. No repair is scheduled from here.
                        self.update_node_status_in_raft(*node_id, "down").await;
                        if let Some(alerts) = &self.alerts {
                            alerts.fire(
                                crate::alerts::AlertKind::NodeDown,
                                "data node missed heartbeats",
                                serde_json::json!({ "node_id": node_id }),
                            );
                        }
                    }
                    NodeStatus::Healthy => {
                        // Node recovered — restore healthy status in Raft.
                        self.update_node_status_in_raft(*node_id, "healthy").await;
                    }
                }
            }

            // Check for SMART-like disk health warnings in heartbeat reports and
            // schedule re-replication for any newly-failed disks.
            self.check_disk_health().await;
        }
    }

    /// Write a `UpdateNodeStatus` entry to the Raft log, committing the health
    /// transition cluster-wide.
    ///
    /// Bug CH — committed via `write_via_leader`: heartbeats land on ONE
    /// arbitrary manager, so the detecting monitor is usually a follower
    /// and a bare `client_write` would silently return ForwardToLeader
    /// (pre-CH the status change then never committed unless the
    /// heartbeat receiver happened to be the leader — caught by release
    /// case 55). Failures stay at debug: with no leader at all there is
    /// nobody to commit to, and the next 10s tick retries.
    async fn update_node_status_in_raft(&self, node_id: u64, status: &str) {
        let req = ClientRequest::UpdateNodeStatus {
            node_id,
            status: status.to_string(),
        };
        match crate::metadata::write_via_leader(&self.raft, &self.voters, req).await {
            Ok(()) => info!("Committed UpdateNodeStatus({}, {}) to Raft", node_id, status),
            Err(e) => debug!(
                "Could not write UpdateNodeStatus to Raft (no leader reachable?): {}",
                e
            ),
        }
    }

    /// Check for SMART-like disk health indicators from heartbeat data.
    ///
    /// Logs warnings for Degraded/Failed disks. Repair of the chunks on a
    /// Failed disk is NOT scheduled from here (Bug CH) — the disk's
    /// replicas stop counting in `live_replica_tier`, so the leader's
    /// `check_under_replicated` tick backfills them.
    async fn check_disk_health(&self) {
        let sm = self.state_data.read().await;
        for (node_id, info) in &sm.nodes {
            for disk in &info.disks {
                match disk.status {
                    dtfs_common::types::DiskStatus::Degraded => {
                        warn!(
                            "SMART warning: Disk {} on node {} ({}) is DEGRADED -- \
                             consider proactive replacement",
                            disk.disk_id, node_id, disk.mount_path
                        );
                    }
                    dtfs_common::types::DiskStatus::Failed => {
                        warn!(
                            "SMART alert: Disk {} on node {} ({}) has FAILED -- \
                             its replicas no longer count; the scheduler \
                             backfills affected chunks",
                            disk.disk_id, node_id, disk.mount_path
                        );
                    }
                    _ => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod bug_cj_liveness_tests {
    use super::{liveness_changes, SUSPECT_MS, DOWN_MS};
    use crate::cluster::NodeStatus;
    use dtfs_common::types::NodeRole;

    fn node(role: NodeRole, last_seen_ms: u64) -> dtfs_raft::NodeInfo {
        dtfs_raft::NodeInfo {
            hostname: String::new(),
            grpc_addr: String::new(),
            role,
            disks: Vec::new(),
            location: dtfs_common::topology::Location::new("", "", "", ""),
            health: dtfs_common::types::HealthStatus::Healthy,
            total_capacity: 0,
            used_capacity: 0,
            last_seen_ms,
            total_bytes_read: 0,
            total_bytes_written: 0,
            client_ip: None,
            version: String::new(),
            build_date: String::new(),
            quarantine: None,
        }
    }

    fn map(items: &[(u64, dtfs_raft::NodeInfo)]) -> std::collections::BTreeMap<u64, dtfs_raft::NodeInfo> {
        items.iter().cloned().collect()
    }

    const NOW: u64 = 1_000_000_000;

    // Bug CJ — a data node whose replicated last_seen is fresh must NOT be
    // marked Down, even though (pre-fix) a follower's local heartbeat map
    // had no entry for it. This is the production flap.
    #[test]
    fn fresh_data_node_not_down() {
        let nodes = map(&[(1, node(NodeRole::Data, NOW - 3_000))]);
        assert!(liveness_changes(&nodes, NOW, SUSPECT_MS, DOWN_MS).is_empty());
    }

    #[test]
    fn stale_data_node_is_down() {
        let nodes = map(&[(1, node(NodeRole::Data, NOW - 40_000))]);
        assert_eq!(
            liveness_changes(&nodes, NOW, SUSPECT_MS, DOWN_MS),
            vec![(1, NodeStatus::Down)]
        );
    }

    #[test]
    fn overdue_data_node_is_suspect() {
        let nodes = map(&[(1, node(NodeRole::Data, NOW - 20_000))]);
        assert_eq!(
            liveness_changes(&nodes, NOW, SUSPECT_MS, DOWN_MS),
            vec![(1, NodeStatus::Suspect)]
        );
    }

    // A manager's last_seen_ms never advances (managers don't send the
    // data heartbeat RPC), so judging it by last_seen would mark it
    // permanently Down — the monitor must skip non-Data roles.
    #[test]
    fn stale_manager_is_never_marked() {
        let nodes = map(&[
            (1, node(NodeRole::Manager, NOW - 10_000_000)),
            (2, node(NodeRole::Client, NOW - 10_000_000)),
        ]);
        assert!(liveness_changes(&nodes, NOW, SUSPECT_MS, DOWN_MS).is_empty());
    }

    // A freshly-registered data node that has not heartbeated yet
    // (last_seen_ms == 0) is left alone, not marked Down.
    #[test]
    fn never_heartbeated_data_node_skipped() {
        let nodes = map(&[(1, node(NodeRole::Data, 0))]);
        assert!(liveness_changes(&nodes, NOW, SUSPECT_MS, DOWN_MS).is_empty());
    }
}
