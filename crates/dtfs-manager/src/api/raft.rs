//! REST handlers: raft state + membership change endpoints. Split out of api.rs on 2026-09-13; every
//! item sees api.rs's imports and private helpers via the glob.
#![allow(unused_imports)]

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
    }))
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
            warn!("CF-2 add-learner {} failed: {}", target, e);
            Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": format!("add_learner failed: {e}")})),
            ))
        }
    }
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
            warn!("CF-2 promote {} failed: {}", target, e);
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
            warn!("CF-2 remove {} failed: {}", target, e);
            Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": format!("change_membership failed: {e}")})),
            ))
        }
    }
}
