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

use dtfs_raft::state_machine::StateMachineData;
use dtfs_raft::{ClientRequest, DtfsRaft};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::cluster::{ClusterManager, NodeStatus};

/// Periodic health monitoring loop.
pub struct HealthMonitor {
    cluster: Arc<ClusterManager>,
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
        cluster: Arc<ClusterManager>,
        check_interval: Duration,
        state_data: Arc<RwLock<StateMachineData>>,
        raft: Arc<DtfsRaft>,
        voters: Vec<dtfs_common::config::InitialVoter>,
    ) -> Self {
        Self {
            cluster,
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

            let changes = self.cluster.check_nodes().await;

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
