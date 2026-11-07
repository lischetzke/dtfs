//! Health monitoring of data nodes — Phase 4 (Resilience).
//!
//! Periodically checks heartbeat and health status of each data-node,
//! detects failures, commits the status transitions to Raft, and fires
//! operator alerts.
//!
//! Bug CM — this module is also LEADER-GATED: only the current raft leader
//! evaluates liveness + commits `UpdateNodeStatus` (see `monitor_status_changes`
//! / `run`), mirroring the scheduler's Bug AC gate. A non-leader committing off
//! its own bursty-stale replicated view flapped nodes on prod (CJ's residual).
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
/// values). A data node heartbeats every 5 s. MGR-MON-1 — the live values
/// come from `[monitoring]` (`heartbeat_interval_secs *
/// suspect_after_missed` / `* down_after_missed`, see
/// `liveness_thresholds_ms`); these constants are the test defaults.
const SUSPECT_MS: u64 = 15_000;
const DOWN_MS: u64 = 30_000;

/// MGR-MON-1 — derive the monitor's `(suspect_ms, down_ms)` from the
/// operator's `[monitoring]` block. Pre-fix the block was parsed and shown
/// by `/api/v1/config` but never read: the monitor hardcoded 15 s / 30 s,
/// so tuning it had no effect. `down` is clamped to be strictly above
/// `suspect` so a misordered pair cannot skip the Suspect state.
pub(crate) fn liveness_thresholds_ms(cfg: &dtfs_common::config::MonitoringConfig) -> (u64, u64) {
    let hb_ms = cfg.heartbeat_interval_secs.max(1).saturating_mul(1000);
    let suspect = hb_ms.saturating_mul(u64::from(cfg.suspect_after_missed.max(1)));
    let down = hb_ms
        .saturating_mul(u64::from(cfg.down_after_missed.max(1)))
        .max(suspect.saturating_add(hb_ms));
    (suspect, down)
}

/// MGR-MON-2 — `last_seen_ms` is stamped by whichever manager received the
/// heartbeat, but judged against THIS (leader) manager's clock. A receiver
/// clock ahead of the leader's by more than this hides staleness; one
/// behind it fakes staleness and can mark a live node Suspect/Down. Log
/// the ahead case (the only one observable from here) so an operator sees
/// the skew instead of an unexplained flap.
const CLOCK_SKEW_WARN_MS: u64 = 5_000;

pub(crate) fn clock_skew_suspects(
    nodes: &std::collections::BTreeMap<u64, dtfs_raft::NodeInfo>,
    now_ms: u64,
) -> Vec<(u64, u64)> {
    nodes
        .iter()
        .filter(|(_, i)| i.role == NodeRole::Data && i.last_seen_ms > 0)
        .filter_map(|(id, i)| {
            let ahead = i.last_seen_ms.saturating_sub(now_ms);
            (ahead > CLOCK_SKEW_WARN_MS).then_some((*id, ahead))
        })
        .collect()
}

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
        let target = if elapsed > down_ms {
            Some(NodeStatus::Down)
        } else if elapsed > suspect_ms {
            Some(NodeStatus::Suspect)
        } else {
            None
        };
        // CO-4-8 — only emit a transition when it actually changes the
        // committed status. Without this an already-Down (or already-Suspect)
        // node re-emits the same `(id, status)` every 10s tick, re-committing
        // an identical `UpdateNodeStatus` to raft forever (leader write + audit
        // churn per dead node).
        if let Some(status) = target {
            if !health_matches_status(info.health, status) {
                out.push((*id, status));
            }
        }
    }
    out
}

/// True when the node's current replicated `health` already equals the
/// liveness `status` this monitor would commit, so re-emitting it is a no-op
/// (CO-4-8). `NodeStatus` only models the liveness subset; richer health
/// states (Quarantined, Maintenance, ...) never equal a liveness target.
fn health_matches_status(
    health: dtfs_common::types::HealthStatus,
    status: NodeStatus,
) -> bool {
    use dtfs_common::types::HealthStatus;
    matches!(
        (status, health),
        (NodeStatus::Down, HealthStatus::Down)
            | (NodeStatus::Suspect, HealthStatus::Suspect)
            | (NodeStatus::Healthy, HealthStatus::Healthy)
    )
}

/// Bug CM — the monitor must only evaluate + commit liveness on the raft
/// LEADER. Returns no changes on a follower/learner.
///
/// Bug CJ (#29) moved liveness off the per-manager heartbeat map onto the
/// replicated `last_seen_ms`, on the assumption that "every manager observes
/// the same freshness". That holds only while a manager's applied log is
/// caught up. A learner on a flaky cross-DC link (prod `dtfs-dc1-mgr01` /
/// `dtfs-dc2-mgr01`) is bursty-stale between catch-up rounds — the leader's
/// `AppendEntries` to it times out — so its 10 s monitor tick reads an OLD
/// `last_seen`, judges the data node Down, and commits `UpdateNodeStatus`
/// via `write_via_leader`. That fights the heartbeat handler's `healthy`
/// write and the node FLAPS (prod v0.1.5, 2026-06-09; CJ's residual case).
///
/// The scheduler already avoids this with its Bug AC leader gate. Mirror it:
/// only the leader, whose log is authoritative and who is always a fast-link
/// voter (learners never lead), judges data-node liveness. Recovery stays the
/// heartbeat handler's `UpdateNodeStatus(healthy)` (any manager, forwarded),
/// so a node still un-marks promptly after a real Down.
pub(crate) fn monitor_status_changes(
    nodes: &std::collections::BTreeMap<u64, dtfs_raft::NodeInfo>,
    now_ms: u64,
    suspect_ms: u64,
    down_ms: u64,
    is_leader: bool,
) -> Vec<(u64, NodeStatus)> {
    if !is_leader {
        return Vec::new();
    }
    liveness_changes(nodes, now_ms, suspect_ms, down_ms)
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
    /// MGR-MON-1 — liveness thresholds derived from `[monitoring]`.
    suspect_ms: u64,
    down_ms: u64,
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
    /// CF-6 — resolved `[tls]` context for leader-forwarded commits.
    tls: dtfs_common::tls::TlsContext,
    /// CO-2-2 — cluster shared secret stamped onto leader-forwarded
    /// status commits so the leader authenticates them.
    shared_secret: String,
    /// CR2-8-1 (CMN-3) — configured cap for the leader-forwarded
    /// `ForwardClientWrite` client, sized the same as the listener it
    /// calls into rather than tonic's 4 MB default.
    max_message_size_mb: u32,
}

impl HealthMonitor {
    pub fn new(
        check_interval: Duration,
        monitoring: &dtfs_common::config::MonitoringConfig,
        state_data: Arc<RwLock<StateMachineData>>,
        raft: Arc<DtfsRaft>,
        voters: Vec<dtfs_common::config::InitialVoter>,
        tls: dtfs_common::tls::TlsContext,
        shared_secret: String,
        max_message_size_mb: u32,
    ) -> Self {
        let (suspect_ms, down_ms) = liveness_thresholds_ms(monitoring);
        Self {
            check_interval,
            suspect_ms,
            down_ms,
            state_data,
            raft,
            voters,
            alerts: None,
            tls,
            shared_secret,
            max_message_size_mb,
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
            interval = ?self.check_interval,
            suspect_ms = self.suspect_ms,
            down_ms = self.down_ms,
            "Health monitor started"
        );

        let mut interval = tokio::time::interval(self.check_interval);
        loop {
            interval.tick().await;

            // Bug CM — leader gate (mirrors the scheduler's Bug AC gate). Only
            // the current leader judges + commits liveness; a follower/learner
            // reading its own bursty-stale replicated view would otherwise
            // flap the node (CJ's residual case). openraft's metrics watch is a
            // Receiver — borrow_watched gives a borrow without consuming it.
            let is_leader = {
                use openraft::async_runtime::watch::WatchReceiver;
                let m = self.raft.metrics().borrow_watched().clone();
                m.current_leader.map(|leader| leader == m.id).unwrap_or(false)
            };

            // Bug CJ — judge liveness from the replicated `last_seen_ms` (not a
            // per-manager heartbeat map). Bug CM — only on the leader.
            let (changes, membership_nodes) = {
                let now_ms = now_millis();
                let sm = self.state_data.read().await;
                if is_leader {
                    for (node_id, ahead_ms) in clock_skew_suspects(&sm.nodes, now_ms) {
                        warn!(
                            node_id,
                            ahead_ms,
                            "MGR-MON-2: data node last_seen_ms is ahead of this leader's clock; \
                             the receiving manager's clock is skewed (check NTP) - liveness \
                             judgements for this node are unreliable"
                        );
                    }
                }
                let changes = monitor_status_changes(
                    &sm.nodes,
                    now_ms,
                    self.suspect_ms,
                    self.down_ms,
                    is_leader,
                );
                // MGR-MON-3 — snapshot the raft membership only when there is
                // something to commit, so the leader-forward below can resolve
                // a CF-2-promoted leader absent from `initial_voters`.
                let membership_nodes = if changes.is_empty() {
                    std::collections::BTreeMap::new()
                } else {
                    sm.nodes.clone()
                };
                (changes, membership_nodes)
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
                        self.update_node_status_in_raft(*node_id, "suspect", &membership_nodes).await;
                    }
                    NodeStatus::Down => {
                        warn!(node_id = %node_id, "Node is DOWN");
                        // Mark in Raft so placement and tiering stop using
                        // this node — and so the leader's scheduler tick
                        // stops counting its replicas (Bug CH:
                        // live_replica_tier) and backfills the lost
                        // copies. No repair is scheduled from here.
                        self.update_node_status_in_raft(*node_id, "down", &membership_nodes).await;
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
                        self.update_node_status_in_raft(*node_id, "healthy", &membership_nodes).await;
                    }
                }
            }

            // Check for SMART-like disk health warnings in heartbeat reports.
            // Bug CM — leader-only too: avoids duplicate alerts + follower log
            // noise (repair is the leader's scheduler, Bug CH).
            if is_leader {
                self.check_disk_health().await;
            }
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
    async fn update_node_status_in_raft(
        &self,
        node_id: u64,
        status: &str,
        membership_nodes: &std::collections::BTreeMap<u64, dtfs_raft::NodeInfo>,
    ) {
        let req = ClientRequest::UpdateNodeStatus {
            node_id,
            status: status.to_string(),
        };
        match crate::metadata::write_via_leader(
            &self.raft,
            &self.voters,
            membership_nodes,
            &self.tls,
            &self.shared_secret,
            self.max_message_size_mb,
            req,
        )
        .await
        {
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
mod mgr_mon_threshold_tests {
    use super::{clock_skew_suspects, liveness_thresholds_ms};
    use dtfs_common::config::MonitoringConfig;
    use dtfs_common::types::NodeRole;

    #[test]
    fn defaults_reproduce_the_historical_15s_30s() {
        let cfg = MonitoringConfig {
            heartbeat_interval_secs: 5,
            suspect_after_missed: 3,
            down_after_missed: 6,
        };
        assert_eq!(liveness_thresholds_ms(&cfg), (15_000, 30_000));
    }

    #[test]
    fn operator_values_are_honoured() {
        let cfg = MonitoringConfig {
            heartbeat_interval_secs: 10,
            suspect_after_missed: 2,
            down_after_missed: 9,
        };
        assert_eq!(liveness_thresholds_ms(&cfg), (20_000, 90_000));
    }

    #[test]
    fn down_is_always_strictly_above_suspect() {
        let cfg = MonitoringConfig {
            heartbeat_interval_secs: 5,
            suspect_after_missed: 6,
            down_after_missed: 3,
        };
        let (s, d) = liveness_thresholds_ms(&cfg);
        assert!(d > s, "suspect={s} down={d}");
    }

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

    #[test]
    fn skew_flags_only_data_nodes_ahead_of_the_leader_clock() {
        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(1, node(NodeRole::Data, 100_000 + 6_000));
        nodes.insert(2, node(NodeRole::Data, 100_000 + 1_000));
        nodes.insert(3, node(NodeRole::Manager, 100_000 + 60_000));
        let out = clock_skew_suspects(&nodes, 100_000);
        assert_eq!(out, vec![(1, 6_000)]);
    }
}

#[cfg(test)]
mod bug_cj_liveness_tests {
    use super::{liveness_changes, monitor_status_changes, SUSPECT_MS, DOWN_MS};
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

    // Bug CM — the same stale data node yields a Down transition on the
    // leader but NOTHING on a follower/learner. A non-leader must never
    // commit a status change: its replicated view can be bursty-stale and
    // its Down write fights the heartbeat's healthy write, flapping the node.
    #[test]
    fn follower_emits_no_status_changes_even_when_stale() {
        let nodes = map(&[(1, node(NodeRole::Data, NOW - 60_000))]);
        // Leader: detects Down.
        assert_eq!(
            monitor_status_changes(&nodes, NOW, SUSPECT_MS, DOWN_MS, true),
            vec![(1, NodeStatus::Down)]
        );
        // Follower/learner: silent (no flap).
        assert!(monitor_status_changes(&nodes, NOW, SUSPECT_MS, DOWN_MS, false).is_empty());
    }

    // A non-leader stays silent for a SUSPECT-range node too.
    #[test]
    fn follower_silent_for_suspect_range() {
        let nodes = map(&[(1, node(NodeRole::Data, NOW - 20_000))]);
        assert!(monitor_status_changes(&nodes, NOW, SUSPECT_MS, DOWN_MS, false).is_empty());
        assert_eq!(
            monitor_status_changes(&nodes, NOW, SUSPECT_MS, DOWN_MS, true),
            vec![(1, NodeStatus::Suspect)]
        );
    }
}

#[cfg(test)]
mod audit_co_tests {
    use super::{liveness_changes, SUSPECT_MS, DOWN_MS};
    use crate::cluster::NodeStatus;
    use dtfs_common::types::{HealthStatus, NodeRole};

    fn node(role: NodeRole, last_seen_ms: u64, health: HealthStatus) -> dtfs_raft::NodeInfo {
        dtfs_raft::NodeInfo {
            hostname: String::new(),
            grpc_addr: String::new(),
            role,
            disks: Vec::new(),
            location: dtfs_common::topology::Location::new("", "", "", ""),
            health,
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

    // CO-4-8 — a node that is ALREADY Down with a stale last_seen must NOT
    // re-emit a Down transition every tick. Today `liveness_changes` ignores
    // `info.health` entirely, so a long-down node yields `(id, Down)` on every
    // 10s monitor tick, re-committing the same `UpdateNodeStatus("down")` to
    // raft forever (audit-log churn, needless leader writes). Correct
    // behaviour: only emit a transition when the node is not already in the
    // target status.
    #[test]
    fn co_4_8_already_down_node_emits_no_transition() {
        let nodes = map(&[(1, node(NodeRole::Data, NOW - 60_000, HealthStatus::Down))]);
        assert!(
            liveness_changes(&nodes, NOW, SUSPECT_MS, DOWN_MS).is_empty(),
            "an already-Down node must not re-emit a Down transition"
        );
    }

    // CO-4-8 companion — a genuinely Healthy node with a stale last_seen IS a
    // real transition and must still surface as Down (guards against an
    // over-broad fix that suppresses all stale nodes).
    #[test]
    fn co_4_8_healthy_stale_node_still_transitions_down() {
        let nodes = map(&[(1, node(NodeRole::Data, NOW - 60_000, HealthStatus::Healthy))]);
        assert_eq!(
            liveness_changes(&nodes, NOW, SUSPECT_MS, DOWN_MS),
            vec![(1, NodeStatus::Down)],
            "a Healthy node gone stale must transition to Down"
        );
    }
}
