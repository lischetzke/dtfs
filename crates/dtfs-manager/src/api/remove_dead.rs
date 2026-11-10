//! REST: `POST /api/v1/nodes/{id}/remove-dead` -- clear the location rows of a
//! node that will never come back, then deregister it. The pure planner sorts
//! every row of the node into removable / anchor-pending / data-loss; the
//! handler executes the plan through guarded Raft commands (leader only).
#![allow(unused_imports)]

use super::*;
use dtfs_common::types::AnchorPolicy;
use dtfs_raft::state_machine::{
    cluster_understands_named_remove, remove_chunk_location_check, replica_row_is_live,
};

/// One `(file, chunk_index, disk)` row held by the dead node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeadRow {
    pub path: String,
    pub chunk_index: u64,
    pub disk_id: u32,
    /// The row's hash, so a file renamed over `path` after the plan is
    /// spared; `None` while some manager predates named removes (RAFT-1).
    pub chunk_hash: Option<String>,
    /// The file's volume anchor, threaded into `RemoveChunkLocation`.
    pub anchor: Option<AnchorPolicy>,
}

impl DeadRow {
    fn label(&self) -> String {
        format!("{}#{}", self.path, self.chunk_index)
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct RemoveDeadPlan {
    /// Another live replica exists and the guarded removal is allowed.
    pub removable: Vec<DeadRow>,
    /// Another live replica exists but removal would strand the anchor;
    /// left in place until anchor-lag repair lands an anchor-DC copy.
    pub anchor_pending: Vec<DeadRow>,
    /// No other live replica: removed only with `accept_data_loss`.
    pub data_loss: Vec<DeadRow>,
}

impl RemoveDeadPlan {
    pub(crate) fn total(&self) -> usize {
        self.removable.len() + self.anchor_pending.len() + self.data_loss.len()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PlanRefusal {
    NotFound,
    NotDead(HealthStatus),
    /// Down/Failed, but last heard from less than the Down threshold ago.
    TooSoon { age_ms: u64, need_ms: u64 },
}

/// Pure planner. `down_ms` is the monitor's Down threshold
/// (`liveness_thresholds_ms().1`); a node with `last_seen_ms == 0` skips the
/// age check (never heartbeated).
pub(crate) fn plan_remove_dead(
    sm: &StateMachineData,
    nid: u64,
    now_ms: u64,
    down_ms: u64,
) -> Result<RemoveDeadPlan, PlanRefusal> {
    let node = sm.nodes.get(&nid).ok_or(PlanRefusal::NotFound)?;
    if !matches!(node.health, HealthStatus::Down | HealthStatus::Failed) {
        return Err(PlanRefusal::NotDead(node.health));
    }
    let age_ms = now_ms.saturating_sub(node.last_seen_ms);
    if node.last_seen_ms != 0 && age_ms < down_ms {
        return Err(PlanRefusal::TooSoon { age_ms, need_ms: down_ms });
    }
    let nid_str = nid.to_string();
    let mut plan = RemoveDeadPlan::default();
    let named = cluster_understands_named_remove(sm);
    for (path, f) in &sm.files {
        let anchor = sm.volumes.get(&f.volume_id).and_then(|v| v.effective_anchor());
        for c in f.chunks.iter().filter(|c| c.node_id == nid_str) {
            let row = DeadRow {
                path: path.clone(),
                chunk_index: c.chunk_index,
                disk_id: c.disk_id,
                chunk_hash: named.then(|| c.chunk_hash.clone()),
                anchor: anchor.clone(),
            };
            let other_live = f.chunks.iter().any(|o| {
                o.chunk_index == c.chunk_index
                    && !(o.node_id == nid_str && o.disk_id == c.disk_id)
                    && replica_row_is_live(sm, o)
            });
            if !other_live {
                plan.data_loss.push(row);
            } else if remove_chunk_location_check(sm, path, c.chunk_index, &nid_str, c.disk_id, row.chunk_hash.as_deref(), anchor.as_ref())
                .is_ok()
            {
                plan.removable.push(row);
            } else {
                plan.anchor_pending.push(row);
            }
        }
    }
    Ok(plan)
}

fn sample(rows: &[DeadRow]) -> Vec<String> {
    rows.iter().take(5).map(DeadRow::label).collect()
}

fn plan_json(nid: u64, plan: &RemoveDeadPlan) -> Value {
    json!({
        "node_id": nid.to_string(),
        "removable": plan.removable.len(),
        "anchor_pending": plan.anchor_pending.len(),
        "data_loss_candidates": plan.data_loss.len(),
        "sample": {
            "removable": sample(&plan.removable),
            "anchor_pending": sample(&plan.anchor_pending),
            "data_loss": sample(&plan.data_loss),
        },
    })
}

#[derive(Deserialize)]
pub(super) struct RemoveDeadRequest {
    pub(super) operator_id: String,
    #[serde(default)]
    pub(super) reason: String,
    /// Also drop rows that have no other live replica (the chunk is lost).
    #[serde(default)]
    pub(super) accept_data_loss: bool,
    /// Return the plan without proposing anything.
    #[serde(default)]
    pub(super) dry_run: bool,
}

type ApiErr = (StatusCode, Json<Value>);

async fn propose(state: &AppState, req: ClientRequest) -> Result<(), String> {
    let resp = state.raft.client_write(req).await.map_err(|e| format!("raft write failed: {e}"))?;
    crate::scheduler::client_response_ok(&resp.data)
}

pub(super) async fn remove_dead_node_endpoint(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(node_id): axum::extract::Path<String>,
    Json(body): Json<RemoveDeadRequest>,
) -> Result<Json<Value>, ApiErr> {
    let nid: u64 = node_id
        .parse()
        .map_err(|_| (StatusCode::BAD_REQUEST, Json(json!({"error": "node_id must be u64"}))))?;
    let op = body.operator_id.trim().to_string();
    if op.is_empty() {
        return Err((StatusCode::BAD_REQUEST, Json(json!({"error": "operator_id required"}))));
    }
    check_is_leader(&state)?;

    let (_, down_ms) = crate::monitor::liveness_thresholds_ms(&state.config.monitoring);
    let plan = {
        let sm = state.sm_data.read().await;
        plan_remove_dead(&sm, nid, crate::metadata::stamp_now_ms(), down_ms)
    }
    .map_err(|r| match r {
        PlanRefusal::NotFound => {
            (StatusCode::NOT_FOUND, Json(json!({"error": format!("node {nid} not found")})))
        }
        PlanRefusal::NotDead(h) => (
            StatusCode::CONFLICT,
            Json(json!({"error": format!("node {nid} is {h:?}; only a Down/Failed node can be removed as dead")})),
        ),
        PlanRefusal::TooSoon { age_ms, need_ms } => (
            StatusCode::CONFLICT,
            Json(json!({"error": format!(
                "node {nid} has been silent for {}s; wait until it has been Down for {}s",
                age_ms / 1000, need_ms / 1000)})),
        ),
    })?;

    let mut out = plan_json(nid, &plan);
    out["dry_run"] = json!(body.dry_run);
    out["accept_data_loss"] = json!(body.accept_data_loss);
    if body.dry_run {
        return Ok(Json(out));
    }

    let nid_str = nid.to_string();
    let (mut removed, mut loss_removed, mut failed) = (0usize, 0usize, 0usize);
    for r in &plan.removable {
        let req = ClientRequest::RemoveChunkLocation {
            path: r.path.clone(),
            chunk_index: r.chunk_index,
            node_id: nid_str.clone(),
            disk_id: r.disk_id,
            enforce_anchor: r.anchor.clone(),
            now_ms: crate::metadata::stamp_now_ms(),
            chunk_hash: r.chunk_hash.clone(),
        };
        match propose(&state, req).await {
            Ok(()) => removed += 1,
            Err(e) => {
                warn!(row = %r.label(), error = %e, "remove-dead: guarded removal refused");
                failed += 1;
            }
        }
    }
    if body.accept_data_loss {
        for r in &plan.data_loss {
            let req = ClientRequest::RemoveDeadNodeReplica {
                path: r.path.clone(),
                chunk_index: r.chunk_index,
                node_id: nid_str.clone(),
                disk_id: r.disk_id,
                now_ms: crate::metadata::stamp_now_ms(),
                chunk_hash: r.chunk_hash.clone(),
            };
            match propose(&state, req).await {
                Ok(()) => loss_removed += 1,
                Err(e) => {
                    warn!(row = %r.label(), error = %e, "remove-dead: data-loss removal refused");
                    failed += 1;
                }
            }
        }
        // A file that lost a chunk index was moved to /.dtfs-lost/, so its other
        // dead rows (planned under the old path) are still there: sweep them.
        let rest = {
            let sm = state.sm_data.read().await;
            plan_remove_dead(&sm, nid, crate::metadata::stamp_now_ms(), down_ms).map(|p| p.data_loss)
        };
        for r in rest.unwrap_or_default().iter().filter(|r| r.path.starts_with("/.dtfs-lost/")) {
            let req = ClientRequest::RemoveDeadNodeReplica {
                path: r.path.clone(),
                chunk_index: r.chunk_index,
                node_id: nid_str.clone(),
                disk_id: r.disk_id,
                now_ms: crate::metadata::stamp_now_ms(),
                chunk_hash: r.chunk_hash.clone(),
            };
            match propose(&state, req).await {
                Ok(()) => loss_removed += 1,
                Err(e) => {
                    warn!(row = %r.label(), error = %e, "remove-dead: data-loss removal refused");
                    failed += 1;
                }
            }
        }
    }

    let remaining = {
        let sm = state.sm_data.read().await;
        sm.files.values().flat_map(|f| f.chunks.iter()).filter(|c| c.node_id == nid_str).count()
    };
    let mut deregistered = false;
    if remaining == 0 {
        match propose(&state, ClientRequest::DeregisterNode { node_id: nid, now_ms: crate::metadata::stamp_now_ms() })
            .await
        {
            Ok(()) => deregistered = true,
            Err(e) => out["deregister_error"] = json!(e),
        }
    }
    out["removed"] = json!(removed);
    out["data_loss_removed"] = json!(loss_removed);
    out["failed"] = json!(failed);
    out["remaining_rows"] = json!(remaining);
    out["deregistered"] = json!(deregistered);

    state.audit_log.log(
        "api",
        "node.remove_dead",
        &format!(
            "node={nid} by={op} removed={removed} data_loss_removed={loss_removed} remaining={remaining} deregistered={deregistered}"
        ),
        &body.reason,
    );
    info!(node = nid, by = %op, removed, loss_removed, remaining, deregistered, "node remove-dead executed");
    Ok(Json(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dtfs_common::types::{DiskInfo, DiskTier, VolumeConfig};

    fn node(dc: &str, health: HealthStatus, last_seen_ms: u64) -> RaftNodeInfo {
        RaftNodeInfo {
            hostname: "n".into(),
            grpc_addr: String::new(),
            role: NodeRole::Data,
            disks: vec![DiskInfo {
                disk_id: 0,
                uuid: String::new(),
                mount_path: String::new(),
                tier: DiskTier::Cold,
                total_bytes: 0,
                used_bytes: 0,
                status: DiskStatus::Healthy,
            }],
            location: Location::new("lab", dc, "r", "n"),
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

    fn row(idx: u64, nid: u64) -> dtfs_raft::ChunkLocationEntry {
        dtfs_raft::ChunkLocationEntry {
            chunk_index: idx,
            chunk_hash: format!("h{idx}"),
            node_id: nid.to_string(),
            disk_id: 0,
            size_bytes: 1,
        }
    }

    /// Node 1 (leli, DEAD), 2 (toro, live), 3 (leli, live).
    /// idx0: dead + toro  -> anchor pending (leli anchor, only non-anchor survivor)
    /// idx1: dead + leli3 -> removable
    /// idx2: dead only    -> data loss
    fn sm() -> StateMachineData {
        let mut sm = StateMachineData::default();
        sm.nodes.insert(1, node("leli", HealthStatus::Down, 1_000));
        sm.nodes.insert(2, node("toro", HealthStatus::Healthy, 0));
        sm.nodes.insert(3, node("leli", HealthStatus::Healthy, 0));
        let mut vol = VolumeConfig::default();
        vol.id = dtfs_common::types::VolumeId::new("default".to_string());
        vol.anchor = Some(AnchorPolicy { datacenter: "leli".into(), required_tier: None });
        sm.volumes.insert("default".into(), vol);
        sm.files.insert(
            "/a".into(),
            dtfs_raft::state_machine::FileMetadata {
                path: "/a".into(),
                volume_id: "default".into(),
                chunk_size: 1,
                chunks: vec![row(0, 1), row(0, 2), row(1, 1), row(1, 3), row(2, 1)],
                created_at_ms: 1,
                updated_at_ms: 1,
                symlink_target: None,
                logical_size: None,
                is_directory: false,
                cache_cooldown_until_ms: Default::default(),
                ever_committed: None,
            },
        );
        sm
    }

    #[test]
    fn plan_classifies_every_row() {
        let p = plan_remove_dead(&sm(), 1, 1_000_000, 30_000).unwrap();
        let idx = |v: &[DeadRow]| v.iter().map(|r| r.chunk_index).collect::<Vec<_>>();
        assert_eq!(idx(&p.removable), vec![1]);
        assert_eq!(idx(&p.anchor_pending), vec![0]);
        assert_eq!(idx(&p.data_loss), vec![2]);
        assert_eq!(p.total(), 3);
    }

    /// Single-voter membership on manager 10 reporting `version`.
    fn with_manager(mut s: StateMachineData, version: &str) -> StateMachineData {
        let mut m = node("leli", HealthStatus::Healthy, 0);
        m.role = NodeRole::Manager;
        m.version = version.into();
        s.nodes.insert(10, m);
        let nodes: std::collections::BTreeMap<u64, dtfs_raft::DtfsNode> =
            [(10, dtfs_raft::DtfsNode { addr: String::new(), hostname: String::new() })].into();
        let mem = openraft::Membership::new(vec![[10u64].into_iter().collect()], nodes).unwrap();
        s.last_membership = openraft::StoredMembership::new(None, mem);
        s
    }

    /// RAFT-1 — the proposed removal names the hash only when every
    /// manager understands it.
    #[test]
    fn plan_names_chunk_hash_only_when_cluster_understands_it() {
        let hashes = |p: &RemoveDeadPlan| {
            p.removable.iter().chain(&p.data_loss).map(|r| r.chunk_hash.clone()).collect::<Vec<_>>()
        };
        let p = plan_remove_dead(&with_manager(sm(), "v0.1.14"), 1, 1_000_000, 30_000).unwrap();
        assert_eq!(hashes(&p), vec![Some("h1".to_string()), Some("h2".to_string())]);
        // Gate closed: a v0.1.13 manager, a dev build, or no membership.
        for s in [with_manager(sm(), "v0.1.13"), with_manager(sm(), "v0.0.0-dev"), sm()] {
            let p = plan_remove_dead(&s, 1, 1_000_000, 30_000).unwrap();
            assert_eq!(hashes(&p), vec![None, None]);
        }
    }

    #[test]
    fn plan_anchor_pending_clears_once_another_anchor_copy_exists() {
        let mut s = sm();
        // Anchor-lag repair landed a copy of chunk 0 on live leli node 3.
        s.files.get_mut("/a").unwrap().chunks.push(row(0, 3));
        let p = plan_remove_dead(&s, 1, 1_000_000, 30_000).unwrap();
        assert!(p.anchor_pending.is_empty());
        assert_eq!(p.removable.len(), 2);
    }

    #[test]
    fn plan_refuses_live_unknown_and_recently_dead_nodes() {
        let s = sm();
        assert_eq!(plan_remove_dead(&s, 9, 1_000_000, 30_000), Err(PlanRefusal::NotFound));
        assert_eq!(plan_remove_dead(&s, 2, 1_000_000, 30_000), Err(PlanRefusal::NotDead(HealthStatus::Healthy)));
        let mut suspect = sm();
        suspect.nodes.get_mut(&1).unwrap().health = HealthStatus::Suspect;
        assert_eq!(plan_remove_dead(&suspect, 1, 1_000_000, 30_000), Err(PlanRefusal::NotDead(HealthStatus::Suspect)));
        // Boundary: last seen exactly `down_ms` ago is allowed, 1 ms less is not.
        assert!(plan_remove_dead(&s, 1, 1_000 + 30_000, 30_000).is_ok());
        assert_eq!(
            plan_remove_dead(&s, 1, 1_000 + 29_999, 30_000),
            Err(PlanRefusal::TooSoon { age_ms: 29_999, need_ms: 30_000 })
        );
    }

    #[test]
    fn plan_is_empty_for_a_dead_node_without_rows() {
        let mut s = sm();
        s.files.get_mut("/a").unwrap().chunks.retain(|c| c.node_id != "1");
        assert_eq!(plan_remove_dead(&s, 1, 1_000_000, 30_000).unwrap().total(), 0);
    }

    fn req(dry_run: bool) -> Json<RemoveDeadRequest> {
        Json(RemoveDeadRequest { operator_id: "op".into(), reason: String::new(), accept_data_loss: false, dry_run })
    }

    #[tokio::test]
    async fn handler_validates_input_and_requires_leader() {
        let h = super::super::test_support::harness().await;
        let path = |s: &str| axum::extract::Path(s.to_string());
        let e = remove_dead_node_endpoint(State(h.state.clone()), path("x"), req(true)).await.unwrap_err();
        assert_eq!(e.0, StatusCode::BAD_REQUEST);
        let blank = Json(RemoveDeadRequest { operator_id: " ".into(), reason: String::new(), accept_data_loss: false, dry_run: true });
        let e = remove_dead_node_endpoint(State(h.state.clone()), path("1"), blank).await.unwrap_err();
        assert_eq!(e.0, StatusCode::BAD_REQUEST);
        // The harness raft is never leader: dry-run is leader-only too.
        let e = remove_dead_node_endpoint(State(h.state.clone()), path("1"), req(true)).await.unwrap_err();
        assert_eq!(e.0, StatusCode::CONFLICT);
        assert!(e.1.0["error"].as_str().unwrap().contains("not the leader"));
    }

    #[test]
    fn plan_json_reports_counts_and_samples() {
        let p = plan_remove_dead(&sm(), 1, 1_000_000, 30_000).unwrap();
        let v = plan_json(1, &p);
        assert_eq!(v["removable"], 1);
        assert_eq!(v["anchor_pending"], 1);
        assert_eq!(v["data_loss_candidates"], 1);
        assert_eq!(v["sample"]["data_loss"][0], "/a#2");
    }
}
