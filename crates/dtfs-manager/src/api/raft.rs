//! REST handlers: raft state + membership change endpoints. Split out of api.rs on 2026-09-13; every
//! item sees api.rs's imports and private helpers via the glob.

use super::*;

/// Bug AC — Raft state introspection endpoint.
///
/// Returns openraft's view of the cluster's consensus state so that
/// operators can answer "who's the leader?" and "is this manager
/// caught up?" without parsing log lines. Mirrors what the openraft
/// Watch<RaftMetrics> publishes internally.
///
/// Output (example for a healthy follower):
/// ```json
/// {
///   "node_id": "12709699821947187762",
///   "hostname": "mgr01",
///   "running_state": "ok",
///   "state": "Follower",
///   "is_leader": false,
///   "current_term": 18,
///   "current_leader": "15786580940693171021",
///   "vote_term": 18,
///   "vote_committed": true,
///   "last_log_index": 4521,
///   "last_applied_index": 4521,
///   "applied_log_lag": 0,
///   "snapshot_index": 4000,
///   "voters": ["12709...", "15786...", "5766..."],
///   "millis_since_quorum_ack": null
/// }
/// ```
///
/// Critical fields for diagnostics:
/// - `current_leader = null` means quorum is currently lost.
/// - `applied_log_lag > 0` on a follower means it's behind the leader.
/// - `is_leader = false && current_leader != null` is the healthy
///   follower state.
pub(super) async fn raft_state(State(state): State<Arc<AppState>>) -> Json<Value> {
    use openraft::async_runtime::watch::WatchReceiver;
    let metrics = state.raft.metrics().borrow_watched().clone();
    let running_state = match &metrics.running_state {
        Ok(()) => "ok".to_string(),
        Err(e) => format!("fatal: {e}"),
    };
    let last_log_index = metrics.last_log_index.unwrap_or(0);
    let last_applied_index = metrics
        .last_applied
        .as_ref()
        .map(|l| l.index)
        .unwrap_or(0);
    let applied_log_lag = last_log_index.saturating_sub(last_applied_index);
    let membership = metrics.membership_config.membership();
    let voter_set: BTreeSet<u64> = membership.voter_ids().collect();
    let voters: Vec<String> = voter_set.iter().map(|id| id.to_string()).collect();
    // CF-2 — learners + full membership so an operator can see an
    // added-but-not-promoted manager (and its replication addr) without
    // grepping the leader's log.
    let learners: Vec<String> = membership
        .learner_ids()
        .map(|id| id.to_string())
        .collect();
    let members: Vec<Value> = membership
        .nodes()
        .map(|(id, n)| {
            json!({
                "node_id": id.to_string(),
                "hostname": n.hostname,
                "addr": n.addr,
                "is_voter": voter_set.contains(id),
            })
        })
        .collect();
    let self_node_id = metrics.id;
    let is_leader = metrics
        .current_leader
        .map(|leader| leader == self_node_id)
        .unwrap_or(false);
    Json(json!({
        "node_id": self_node_id.to_string(),
        "hostname": state.config.node.hostname,
        "running_state": running_state,
        "state": format!("{:?}", metrics.state),
        "is_leader": is_leader,
        "current_term": metrics.current_term,
        "current_leader": metrics
            .current_leader
            .map(|l| l.to_string()),
        "vote_term": format!("{:?}", metrics.vote.leader_id()),
        "vote_committed": metrics.vote.is_committed(),
        "last_log_index": last_log_index,
        "last_applied_index": last_applied_index,
        "applied_log_lag": applied_log_lag,
        "snapshot_index": metrics
            .snapshot
            .as_ref()
            .map(|l| l.index)
            .unwrap_or(0),
        "voters": voters,
        // CF-2 — see `members`/`learners` above.
        "learners": learners,
        "membership": members,
        // Bug BF-12 — openraft 0.10 deprecated `millis_since_quorum_ack`
        // (computed-on-emit; clock-skew bait) in favour of an absolute
        // `last_quorum_acked: SerdeInstant`. Derive the legacy millis
        // shape client-side via `.elapsed()` so the JSON contract the
        // WebUI + scripts consume is unchanged.
        "millis_since_quorum_ack": metrics.last_quorum_acked
            .as_ref()
            .map(|t| t.elapsed().as_millis() as u64),
        // Additive: per-peer replication progress, leader only (null else).
        "replication": replication_json(
            metrics.replication.as_ref().map(|r| {
                r.iter()
                    .map(|(id, lid)| (*id, lid.as_ref().map(|l| l.index)))
                    .collect::<std::collections::BTreeMap<u64, Option<u64>>>()
            }),
            last_log_index,
        ),
    }))
}

/// Leader replication progress as `[{node_id, matched_index, lag}]`, or
/// `null` when this node is not the leader. `matched_index` is null when a
/// peer has acknowledged nothing yet (lag then counts from 0).
pub(super) fn replication_json(
    matched: Option<std::collections::BTreeMap<u64, Option<u64>>>,
    last_log_index: u64,
) -> Value {
    match matched {
        None => Value::Null,
        Some(m) => Value::Array(
            m.into_iter()
                .map(|(id, idx)| {
                    json!({
                        "node_id": id.to_string(),
                        "matched_index": idx,
                        "lag": last_log_index.saturating_sub(idx.unwrap_or(0)),
                    })
                })
                .collect(),
        ),
    }
}

// ---------------------------------------------------------------------------
// CF-2 — online raft membership change (add-learner / promote / remove)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub(super) struct MembershipChangeRequest {
    /// Manager hostname (`[node].hostname` in its manager.toml) — the
    /// raft node id is `hash(hostname)`.
    pub(super) hostname: String,
    /// Raft/gRPC addr `host:port`. Required for add-learner (peers dial
    /// it for replication); ignored for promote/remove.
    #[serde(default)]
    pub(super) addr: Option<String>,
    /// Must match `[cluster].shared_secret`.
    pub(super) shared_secret: String,
}

/// Gate 1: membership changes require the cluster shared secret.
/// LIS-20 — uses the same fail-closed helper as RegisterNode.
pub(super) fn check_membership_secret(
    state: &AppState,
    provided: &str,
) -> Result<(), (StatusCode, Json<Value>)> {
    if !state
        .config
        .cluster
        .accepts_shared_secret(Some(provided))
    {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({"error": "shared_secret mismatch"})),
        ));
    }
    Ok(())
}

/// Gate 2: membership changes are leader-only (openraft would reject
/// them anyway; rejecting here lets us return a useful leader hint
/// instead of a generic ForwardToLeader error).
pub(super) fn check_is_leader(state: &AppState) -> Result<(), (StatusCode, Json<Value>)> {
    use openraft::async_runtime::watch::WatchReceiver;
    let metrics = state.raft.metrics().borrow_watched().clone();
    if metrics.current_leader == Some(metrics.id) {
        return Ok(());
    }
    let leader = metrics.current_leader.and_then(|l| {
        metrics
            .membership_config
            .membership()
            .get_node(&l)
            .map(|n| {
                json!({
                    "leader_id": l.to_string(),
                    "leader_hostname": n.hostname,
                    "leader_addr": n.addr,
                })
            })
    });
    Err((
        StatusCode::CONFLICT,
        Json(json!({
            "error": "not the leader — send membership changes to the leader's API",
            "leader": leader,
        })),
    ))
}

/// 409 unless the target's registered release meets every apply gate the
/// cluster has opened (an older manager diverges on log replay).
async fn check_target_apply_gates(
    state: &AppState,
    hostname: &str,
) -> Result<(), (StatusCode, Json<Value>)> {
    let sm = state.sm_data.read().await;
    let registered = sm
        .nodes
        .get(&dtfs_common::node_id_from_hostname(hostname))
        .map(|n| (n.role, n.version.as_str()));
    crate::membership::ensure_target_meets_apply_gates(
        hostname,
        registered,
        dtfs_raft::state_machine::cluster_on_v0_1_16(&sm),
        dtfs_raft::state_machine::cluster_on_v0_1_15(&sm),
        dtfs_raft::state_machine::cluster_understands_named_remove(&sm),
    )
    .map_err(|e| (StatusCode::CONFLICT, Json(json!({"error": e}))))
}

/// POST /api/v1/raft/add-learner — add a manager to the cluster as a
/// non-voting learner. The leader sets up replication immediately
/// (snapshot + log); progress is visible in the target's
/// `/api/v1/raft/state` (`applied_log_lag`, `current_leader`).
/// Idempotent: re-adding an existing learner re-registers its addr.
pub(super) async fn raft_add_learner(
    State(state): State<Arc<AppState>>,
    Json(body): Json<MembershipChangeRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    check_membership_secret(&state, &body.shared_secret)?;
    check_is_leader(&state)?;
    let addr = body.addr.as_deref().unwrap_or("").trim();
    if addr.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "addr (host:port) required for add-learner"})),
        ));
    }
    let hostname = body.hostname.trim().to_string();
    let registered_role = {
        let sm = state.sm_data.read().await;
        sm.nodes
            .get(&dtfs_common::node_id_from_hostname(&hostname))
            .map(|n| n.role)
    };
    let nid = crate::membership::validate_membership_target(
        &hostname,
        Some(addr),
        registered_role,
    )
    .map_err(|e| (StatusCode::BAD_REQUEST, Json(json!({"error": e}))))?;
    check_target_apply_gates(&state, &hostname).await?;

    let node = dtfs_raft::DtfsNode {
        addr: addr.to_string(),
        hostname: hostname.clone(),
    };
    let target = format!("{hostname}({nid})@{addr}");
    match state.raft.add_learner(nid, node, false).await {
        Ok(resp) => {
            state.audit_log.log("api", "raft.add_learner", &target, "ok");
            info!("CF-2 add-learner {} committed at log index {}", target, resp.log_id.index);
            Ok(Json(json!({
                "status": "ok",
                "node_id": nid.to_string(),
                "hostname": hostname,
                "addr": addr,
                "log_index": resp.log_id.index,
            })))
        }
        Err(e) => {
            state.audit_log.log("api", "raft.add_learner", &target, "error");
            warn!(target = %target, error = %e, "CF-2 add-learner failed");
            Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": format!("add_learner failed: {e}")})),
            ))
        }
    }
}

/// A peer counts as caught up when its matched index is within this many
/// entries of the leader's last log index.
const MEMBERSHIP_CATCHUP_LAG: u64 = 1000;

/// Leader plus every replication-map peer whose matched index is within
/// `MEMBERSHIP_CATCHUP_LAG` of `last_log_index`.
fn caught_up_set(
    leader_id: u64,
    matched: &std::collections::BTreeMap<u64, Option<u64>>,
    last_log_index: u64,
) -> BTreeSet<u64> {
    let mut set: BTreeSet<u64> = matched
        .iter()
        .filter(|(_, m)| m.is_some_and(|m| last_log_index.saturating_sub(m) <= MEMBERSHIP_CATCHUP_LAG))
        .map(|(id, _)| *id)
        .collect();
    set.insert(leader_id);
    set
}

/// Refuse a voter-set change that would leave the old or new voter set
/// without a reachable, caught-up majority. `target_of` maps the current
/// voters to the post-change voters.
fn guard_voter_change(
    state: &AppState,
    target_of: impl FnOnce(&BTreeSet<u64>) -> BTreeSet<u64>,
) -> Result<(), (StatusCode, Json<Value>)> {
    use openraft::async_runtime::watch::WatchReceiver;
    let metrics = state.raft.metrics().borrow_watched().clone();
    let current: BTreeSet<u64> = metrics.membership_config.membership().voter_ids().collect();
    let target = target_of(&current);
    let matched: std::collections::BTreeMap<u64, Option<u64>> = metrics
        .replication
        .as_ref()
        .map(|r| r.iter().map(|(id, l)| (*id, l.as_ref().map(|l| l.index))).collect())
        .unwrap_or_default();
    let caught = caught_up_set(metrics.id, &matched, metrics.last_log_index.unwrap_or(0));
    crate::membership::ensure_voter_change_has_quorum(&current, &target, &caught)
        .map_err(|e| (StatusCode::CONFLICT, Json(json!({"error": e}))))
}

/// POST /api/v1/raft/promote — upgrade an up-to-date learner to voter.
pub(super) async fn raft_promote(
    State(state): State<Arc<AppState>>,
    Json(body): Json<MembershipChangeRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    use openraft::async_runtime::watch::WatchReceiver;
    check_membership_secret(&state, &body.shared_secret)?;
    check_is_leader(&state)?;
    let hostname = body.hostname.trim().to_string();
    let registered_role = {
        let sm = state.sm_data.read().await;
        sm.nodes
            .get(&dtfs_common::node_id_from_hostname(&hostname))
            .map(|n| n.role)
    };
    let nid = crate::membership::validate_membership_target(&hostname, None, registered_role)
        .map_err(|e| (StatusCode::BAD_REQUEST, Json(json!({"error": e}))))?;
    check_target_apply_gates(&state, &hostname).await?;

    {
        let metrics = state.raft.metrics().borrow_watched().clone();
        let membership = metrics.membership_config.membership().clone();
        if membership.get_node(&nid).is_none() {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({"error": format!(
                    "{hostname} ({nid}) is not in the membership — add-learner first"
                )})),
            ));
        }
        if membership.voter_ids().any(|v| v == nid) {
            return Err((
                StatusCode::CONFLICT,
                Json(json!({"error": format!("{hostname} ({nid}) is already a voter")})),
            ));
        }
    }

    let target = format!("{hostname}({nid})");
    guard_voter_change(&state, |cur| {
        let mut t = cur.clone();
        t.insert(nid);
        t
    })?;
    let change = openraft::ChangeMembers::AddVoterIds(BTreeSet::from([nid]));
    match state.raft.change_membership(change, false).await {
        Ok(resp) => {
            state.audit_log.log("api", "raft.promote", &target, "ok");
            info!("CF-2 promote {} committed at log index {}", target, resp.log_id.index);
            Ok(Json(json!({
                "status": "ok",
                "node_id": nid.to_string(),
                "hostname": hostname,
                "log_index": resp.log_id.index,
            })))
        }
        Err(e) => {
            state.audit_log.log("api", "raft.promote", &target, "error");
            warn!(target = %target, error = %e, "CF-2 promote failed");
            Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": format!("change_membership failed: {e}")})),
            ))
        }
    }
}

/// POST /api/v1/raft/remove — remove a learner or voter from the
/// membership entirely (a removed voter is NOT retained as learner).
/// Refuses to remove the last voter.
pub(super) async fn raft_remove(
    State(state): State<Arc<AppState>>,
    Json(body): Json<MembershipChangeRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    use openraft::async_runtime::watch::WatchReceiver;
    check_membership_secret(&state, &body.shared_secret)?;
    check_is_leader(&state)?;
    let hostname = body.hostname.trim().to_string();
    let nid = crate::membership::validate_membership_target(&hostname, None, None)
        .map_err(|e| (StatusCode::BAD_REQUEST, Json(json!({"error": e}))))?;

    let is_voter = {
        let metrics = state.raft.metrics().borrow_watched().clone();
        let membership = metrics.membership_config.membership().clone();
        if membership.get_node(&nid).is_none() {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({"error": format!("{hostname} ({nid}) is not in the membership")})),
            ));
        }
        let voters: Vec<u64> = membership.voter_ids().collect();
        if voters.contains(&nid) && voters.len() == 1 {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "refusing to remove the last voter"})),
            ));
        }
        voters.contains(&nid)
    };

    if is_voter {
        guard_voter_change(&state, |cur| {
            let mut t = cur.clone();
            t.remove(&nid);
            t
        })?;
    }
    let change = if is_voter {
        openraft::ChangeMembers::RemoveVoters(BTreeSet::from([nid]))
    } else {
        openraft::ChangeMembers::RemoveNodes(BTreeSet::from([nid]))
    };
    let target = format!("{hostname}({nid})");
    match state.raft.change_membership(change, false).await {
        Ok(resp) => {
            // RemoveVoters + retain=false removes the voter from the node
            // set too (verified against openraft 0.10.0-alpha.17 docs) —
            // no second RemoveNodes step needed.
            state.audit_log.log("api", "raft.remove", &target, "ok");
            info!("CF-2 remove {} committed at log index {}", target, resp.log_id.index);
            Ok(Json(json!({
                "status": "ok",
                "node_id": nid.to_string(),
                "hostname": hostname,
                "was_voter": is_voter,
                "log_index": resp.log_id.index,
            })))
        }
        Err(e) => {
            state.audit_log.log("api", "raft.remove", &target, "error");
            warn!(target = %target, error = %e, "CF-2 remove failed");
            Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": format!("change_membership failed: {e}")})),
            ))
        }
    }
}

#[cfg(test)]
mod membership_guard_tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn caught_up_set_includes_leader_and_lag_boundary() {
        let m = BTreeMap::from([
            (2, Some(9000)),  // lag 1000: in
            (3, Some(8999)),  // lag 1001: out
            (4, None),        // never matched: out
            (5, Some(10000)), // lag 0: in
        ]);
        assert_eq!(caught_up_set(1, &m, 10000), BTreeSet::from([1, 2, 5]));
        // Matched ahead of last_log_index (stale read) does not underflow.
        assert!(caught_up_set(1, &BTreeMap::from([(2, Some(50))]), 10).contains(&2));
    }

    #[test]
    fn removing_voter_with_lagging_peer_is_refused() {
        let cur = BTreeSet::from([1, 2, 3]);
        let caught = caught_up_set(1, &BTreeMap::from([(2, None), (3, None)]), 100);
        let err = crate::membership::ensure_voter_change_has_quorum(
            &cur,
            &BTreeSet::from([1, 2]),
            &caught,
        )
        .unwrap_err();
        assert!(err.contains("1 of 3 current voters"), "{err}");
    }
}
