//! Cluster membership management — Phase 3 (Clustering).
//!
//! Tracks which data-nodes are active members of the cluster, handles
//! membership changes, and maintains the authoritative node list via Raft.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use dtfs_raft::NodeInfo;
use tracing::info;

/// Per-node health status as seen by the manager.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Healthy,
    Suspect,
    Down,
}

/// Manages cluster membership and heartbeat tracking.
pub struct ClusterManager {
    /// node_id -> last heartbeat received time
    node_heartbeats: DashMap<u64, Instant>,
    /// Duration after which a node is considered suspect.
    suspect_threshold: Duration,
    /// Duration after which a node is considered down.
    down_threshold: Duration,
}

impl ClusterManager {
    pub fn new() -> Self {
        Self {
            node_heartbeats: DashMap::new(),
            suspect_threshold: Duration::from_secs(15),
            down_threshold: Duration::from_secs(30),
        }
    }

    /// Configure custom thresholds.
    pub fn with_thresholds(mut self, suspect: Duration, down: Duration) -> Self {
        self.suspect_threshold = suspect;
        self.down_threshold = down;
        self
    }

    /// Process an incoming heartbeat from a data node.
    /// Updates the last-seen timestamp used for health monitoring.
    pub fn process_heartbeat(&self, node_id: u64) {
        self.node_heartbeats.insert(node_id, Instant::now());
    }

    /// Get the health status of a specific node.
    pub fn get_node_status(&self, node_id: u64) -> NodeStatus {
        match self.node_heartbeats.get(&node_id) {
            Some(entry) => {
                let elapsed = entry.value().elapsed();
                if elapsed > self.down_threshold {
                    NodeStatus::Down
                } else if elapsed > self.suspect_threshold {
                    NodeStatus::Suspect
                } else {
                    NodeStatus::Healthy
                }
            }
            // A node that has never sent a heartbeat is considered down.
            None => NodeStatus::Down,
        }
    }

    /// Periodic check of all tracked nodes.
    ///
    /// Returns `(node_id, new_status)` pairs for every node whose heartbeat
    /// is overdue. Healthy nodes (recent heartbeat) are omitted — their
    /// recovery is handled directly in the heartbeat RPC handler via
    /// `UpdateNodeStatus { status: "healthy" }`.
    ///
    /// **No Raft writes are performed here.** All status transitions are
    /// committed to Raft by `HealthMonitor::run()`, which is the single
    /// owner of that responsibility (fix for issue #4 — duplicate Raft
    /// writes that flooded the log on every monitor tick).
    pub async fn check_nodes(&self) -> Vec<(u64, NodeStatus)> {
        let mut status_changes = Vec::new();

        for entry in self.node_heartbeats.iter() {
            let node_id = *entry.key();
            let elapsed = entry.value().elapsed();

            if elapsed > self.down_threshold {
                status_changes.push((node_id, NodeStatus::Down));
            } else if elapsed > self.suspect_threshold {
                status_changes.push((node_id, NodeStatus::Suspect));
            }
            // Healthy nodes are intentionally omitted.
        }

        status_changes
    }

    /// Return a list of all registered nodes from Raft state.
    /// This reads directly from the local state machine (potentially stale on
    /// followers, which is acceptable for reads).
    pub async fn get_all_nodes(
        &self,
        sm: &Arc<tokio::sync::RwLock<dtfs_raft::state_machine::StateMachineData>>,
    ) -> Vec<(u64, NodeInfo)> {
        let data = sm.read().await;
        data.nodes.iter().map(|(k, v)| (*k, v.clone())).collect()
    }

    /// Check whether a given node_id is being tracked.
    pub fn is_tracked(&self, node_id: u64) -> bool {
        self.node_heartbeats.contains_key(&node_id)
    }
}

impl Default for ClusterManager {
    fn default() -> Self {
        Self::new()
    }
}

// ClusterManager no longer holds a DtfsRaft reference. The one place that
// used to write Raft status here (check_nodes) was causing duplicate writes
// since HealthMonitor::update_node_status_in_raft does the same thing.
// Removing the dependency makes the ownership clear.
