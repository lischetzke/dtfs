//! Cluster membership management — Phase 3 (Clustering).
//!
//! Tracks which data-nodes are active members of the cluster, handles
//! membership changes, and maintains the authoritative node list via Raft.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use dtfs_raft::{ClientRequest, DtfsRaft, NodeInfo};
use tracing::{info, warn};

/// Per-node health status as seen by the manager.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Healthy,
    Suspect,
    Down,
}

/// Manages cluster membership and heartbeat tracking.
pub struct ClusterManager {
    raft: Arc<DtfsRaft>,
    /// node_id -> last heartbeat received time
    node_heartbeats: DashMap<u64, Instant>,
    /// Duration after which a node is considered suspect.
    suspect_threshold: Duration,
    /// Duration after which a node is considered down.
    down_threshold: Duration,
}

impl ClusterManager {
    pub fn new(raft: Arc<DtfsRaft>) -> Self {
        Self {
            raft,
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
    /// Returns a config version the node should be at (for config push).
    pub fn process_heartbeat(&self, node_id: u64) -> u64 {
        self.node_heartbeats.insert(node_id, Instant::now());
        // Config version is tracked in Raft state; for now return 0.
        0
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
            None => NodeStatus::Down,
        }
    }

    /// Periodic check of all tracked nodes.
    /// Returns lists of (node_id, new_status) for nodes whose status changed
    /// from Healthy to Suspect or Down so the caller can react.
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
        }

        // Try to update Raft state for status changes.
        for (node_id, status) in &status_changes {
            let status_str = match status {
                NodeStatus::Healthy => "healthy",
                NodeStatus::Suspect => "suspect",
                NodeStatus::Down => "down",
            };
            let req = ClientRequest::UpdateNodeStatus {
                node_id: *node_id,
                status: status_str.to_string(),
            };
            if let Err(e) = self.raft.client_write(req).await {
                warn!(
                    "Failed to update Raft status for node {}: {} (may not be leader)",
                    node_id, e
                );
            } else {
                info!("Node {} marked as {:?}", node_id, status);
            }
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
