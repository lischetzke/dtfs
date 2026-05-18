//! Health monitoring of data nodes — Phase 4 (Resilience).
//!
//! Periodically checks heartbeat and health status of each data-node,
//! detects failures, and triggers recovery / re-replication as needed.
//! On detecting a "down" node, enumerates all chunks on that node and
//! schedules re-replication via the `TaskScheduler`.

use std::sync::Arc;
use std::time::Duration;

use dtfs_common::types::{DiskTier, HealthStatus};
use dtfs_raft::state_machine::StateMachineData;
use dtfs_raft::{ClientRequest, DtfsRaft};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::cluster::{ClusterManager, NodeStatus};
use crate::scheduler::TaskScheduler;

/// Periodic health monitoring loop.
pub struct HealthMonitor {
    cluster: Arc<ClusterManager>,
    check_interval: Duration,
    scheduler: Arc<TaskScheduler>,
    state_data: Arc<RwLock<StateMachineData>>,
    /// Raft handle used to commit node-status changes to the cluster state machine.
    raft: Arc<DtfsRaft>,
    /// P1-8: optional alert dispatcher. NodeDown / DiskFailed fire here.
    alerts: Option<Arc<crate::alerts::AlertDispatcher>>,
}

impl HealthMonitor {
    pub fn new(
        cluster: Arc<ClusterManager>,
        check_interval: Duration,
        scheduler: Arc<TaskScheduler>,
        state_data: Arc<RwLock<StateMachineData>>,
        raft: Arc<DtfsRaft>,
    ) -> Self {
        Self {
            cluster,
            check_interval,
            scheduler,
            state_data,
            raft,
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
                        warn!(
                            "Node {} is DOWN -- triggering re-replication check",
                            node_id
                        );
                        // Mark in Raft so placement and tiering stop using this node.
                        self.update_node_status_in_raft(*node_id, "down").await;
                        if let Some(alerts) = &self.alerts {
                            alerts.fire(
                                crate::alerts::AlertKind::NodeDown,
                                "data node missed heartbeats",
                                serde_json::json!({ "node_id": node_id }),
                            );
                        }
                        self.handle_node_down(*node_id).await;
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

    /// Handle a node that has been detected as down.
    ///
    /// Collects ALL chunk replication tasks while holding the state-machine
    /// lock, then releases the lock before scheduling them. This ensures every
    /// chunk on the failed node is handled in a single monitor tick instead of
    /// one chunk per tick (fix for issue #1).
    async fn handle_node_down(&self, node_id: u64) {
        // Collect (chunk_hash, source_node, target_node) triples while holding
        // the read lock. We never await with the lock held.
        let tasks: Vec<(String, u64, u64)> = {
            let sm = self.state_data.read().await;
            let node_id_str = node_id.to_string();

            let healthy_nodes: Vec<u64> = sm
                .nodes
                .iter()
                .filter(|(id, info)| {
                    **id != node_id && info.health == HealthStatus::Healthy
                })
                .map(|(id, _)| *id)
                .collect();

            if healthy_nodes.is_empty() {
                warn!(
                    "No healthy nodes available for re-replication of node {} chunks",
                    node_id
                );
                return;
            }

            let mut out = Vec::new();
            let mut round_robin: usize = 0;

            for (_path, file_meta) in &sm.files {
                for chunk_loc in &file_meta.chunks {
                    if chunk_loc.node_id != node_id_str {
                        continue;
                    }

                    // Find another node that already has this chunk (as source).
                    let source = file_meta.chunks.iter().find(|c| {
                        c.chunk_index == chunk_loc.chunk_index
                            && c.node_id != node_id_str
                    });

                    let source_node: u64 = match source {
                        Some(s) => s.node_id.parse().unwrap_or(0),
                        None => {
                            warn!(
                                "Chunk {} has no other replica to copy from (data loss risk!)",
                                chunk_loc.chunk_hash
                            );
                            continue;
                        }
                    };

                    if source_node == 0 {
                        continue;
                    }

                    // Pick a target from healthy nodes, round-robin.
                    let target = healthy_nodes[round_robin % healthy_nodes.len()];
                    round_robin += 1;

                    out.push((chunk_loc.chunk_hash.clone(), source_node, target));
                }
            }
            out
        }; // read lock released here

        let count = tasks.len();

        // Schedule all collected tasks without holding the state lock.
        for (chunk_id, source_node, target_node) in tasks {
            self.scheduler
                .schedule_replication(&chunk_id, source_node, target_node, DiskTier::Hot)
                .await;
        }

        if count > 0 {
            info!(
                "Scheduled {} re-replication tasks for chunks on down node {}",
                count, node_id
            );
        } else {
            info!(
                "No chunks found on down node {} that need re-replication",
                node_id
            );
        }
    }

    /// Write a `UpdateNodeStatus` entry to the Raft log, committing the health
    /// transition cluster-wide.  Errors are only at debug level because this
    /// node may not be the Raft leader; the leader's own monitor will issue the
    /// same write.
    async fn update_node_status_in_raft(&self, node_id: u64, status: &str) {
        let req = ClientRequest::UpdateNodeStatus {
            node_id,
            status: status.to_string(),
        };
        match self.raft.client_write(req).await {
            Ok(_) => info!("Committed UpdateNodeStatus({}, {}) to Raft", node_id, status),
            Err(e) => debug!(
                "Could not write UpdateNodeStatus to Raft (may not be leader): {}",
                e
            ),
        }
    }

    /// Check for SMART-like disk health indicators from heartbeat data.
    ///
    /// Logs warnings for degraded disks and schedules re-replication for any
    /// disk whose status has transitioned to `Failed`.
    async fn check_disk_health(&self) {
        // Collect failed-disk info without holding the read-lock across awaits.
        let failed_disks: Vec<(u64, u32, String)> = {
            let sm = self.state_data.read().await;
            let mut out = Vec::new();
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
                                 scheduling re-replication for chunks on this disk",
                                disk.disk_id, node_id, disk.mount_path
                            );
                            out.push((*node_id, disk.disk_id, disk.mount_path.clone()));
                        }
                        _ => {}
                    }
                }
            }
            out
        };

        // For each failed disk, find chunks on it and schedule re-replication.
        for (failed_node_id, failed_disk_id, _mount_path) in failed_disks {
            self.handle_disk_failed(failed_node_id, failed_disk_id).await;
        }
    }

    /// Schedule re-replication for all chunks that were on a disk that has
    /// been reported as failed.
    ///
    /// Like `handle_node_down`, collects all tasks while holding the lock,
    /// then releases it before scheduling (fix for issue #1).
    async fn handle_disk_failed(&self, node_id: u64, disk_id: u32) {
        let tasks: Vec<(String, u64, u64)> = {
            let sm = self.state_data.read().await;
            let node_id_str = node_id.to_string();

            let healthy_nodes: Vec<u64> = sm
                .nodes
                .iter()
                .filter(|(id, info)| **id != node_id && info.health == HealthStatus::Healthy)
                .map(|(id, _)| *id)
                .collect();

            if healthy_nodes.is_empty() {
                warn!(
                    "No healthy nodes available for re-replication after disk {} on node {} failed",
                    disk_id, node_id
                );
                return;
            }

            let mut out = Vec::new();
            let mut round_robin: usize = 0;

            for (_path, file_meta) in &sm.files {
                for chunk_loc in &file_meta.chunks {
                    // Only process chunks hosted on this specific failed disk.
                    if chunk_loc.node_id != node_id_str || chunk_loc.disk_id != disk_id {
                        continue;
                    }

                    // Find an alternate healthy source replica for this chunk.
                    let alt_source = file_meta.chunks.iter().find(|c| {
                        c.chunk_index == chunk_loc.chunk_index
                            && c.node_id != node_id_str
                    });

                    let source_node: u64 = match alt_source {
                        Some(s) => match s.node_id.parse() {
                            Ok(id) => id,
                            Err(_) => continue,
                        },
                        None => {
                            warn!(
                                "Chunk {} (disk {}, node {}) has no surviving replica!",
                                chunk_loc.chunk_hash, disk_id, node_id
                            );
                            continue;
                        }
                    };

                    let target = healthy_nodes[round_robin % healthy_nodes.len()];
                    round_robin += 1;

                    out.push((chunk_loc.chunk_hash.clone(), source_node, target));
                }
            }
            out
        }; // read lock released here

        let count = tasks.len();

        for (chunk_id, source_node, target_node) in tasks {
            self.scheduler
                .schedule_replication(&chunk_id, source_node, target_node, DiskTier::Hot)
                .await;
            info!(
                "Scheduled re-replication for chunk {} (disk {} on node {} failed)",
                chunk_id, disk_id, node_id
            );
        }

        if count == 0 {
            debug!(
                "No chunks found on failed disk {} of node {} requiring re-replication",
                disk_id, node_id
            );
        }
    }
}
