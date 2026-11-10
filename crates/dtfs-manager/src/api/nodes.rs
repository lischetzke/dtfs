//! REST handlers: node listing, maintenance, unquarantine, decommission. Split out of api.rs on 2026-09-13; every
//! item sees api.rs's imports and private helpers via the glob.

use super::*;

// ---------------------------------------------------------------------------
// Node endpoints
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub(super) struct DiskResponse {
    pub(super) disk_id: u32,
    pub(super) uuid: String,
    pub(super) mount_path: String,
    pub(super) tier: String,
    pub(super) total_bytes: u64,
    pub(super) used_bytes: u64,
    pub(super) status: String,
}

#[derive(Serialize)]
pub(super) struct NodeResponse {
    pub(super) node_id: String,
    pub(super) hostname: String,
    pub(super) grpc_addr: String,
    pub(super) location: String,
    pub(super) disk_count: usize,
    pub(super) total_capacity: u64,
    pub(super) used_capacity: u64,
    /// REL-1-1 — cumulative bytes this node has served / stored. Surfaced so a
    /// release test can prove a read was served by the nearest-DC replica.
    pub(super) total_bytes_read: u64,
    pub(super) total_bytes_written: u64,
    pub(super) health: String,
    pub(super) role: String,
    /// Unix milliseconds when this node was last observed alive.
    /// For data nodes: latest heartbeat. For managers: synthesised
    /// from the cluster config + the API server's "now" (since this
    /// API only responds while it's alive). For FUSE clients:
    /// approximate via the most-recent write_lease for any of their
    /// owner_id hostnames.
    pub(super) last_seen_ms: u64,
    /// Bug AX — peer IP for client (FUSE) nodes, captured at
    /// RegisterNode time. `None` for data/manager nodes (their IP is
    /// already in `grpc_addr`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) client_ip: Option<String>,
    /// Bug AX — true for the manager whose `node_id` matches openraft's
    /// `current_leader` at request time. Always false for data/client
    /// roles. The leader can shift between calls (Raft elections), so
    /// the WebUI should treat this as a snapshot, not a durable label.
    pub(super) is_leader: bool,
    /// Bug AY — binary version this node reported. Empty for nodes that
    /// pre-date Bug AY or for synthetic/lease-derived entries the
    /// manager couldn't ask directly.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub(super) version: String,
    /// Bug AY — build date of the binary this node is running, format
    /// `YYYYMMDD-HHmmss` (UTC, e.g. `"20260522-141034"`). Empty when
    /// unknown.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub(super) build_date: String,
    /// Bug AR — per-disk detail so the Nodes page can render a row per
    /// disk (tier, mount_path, individual utilisation). The list was
    /// previously hidden behind `disk_count`, making it impossible for
    /// the WebUI to show which disk in a multi-disk data node was
    /// filling up. Empty for manager/client roles.
    pub(super) disks: Vec<DiskResponse>,
    /// Bug BC-12a — `Some(_)` whenever the node is Quarantined.
    /// Carries the reason / triggering chunk / fault history the
    /// WebUI nodes page renders alongside the QUARANTINED badge.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) quarantine: Option<QuarantineResponse>,
    /// CF-4c — for Manager-role rows: `Some(true)` if this node_id is a
    /// raft voter or learner, `Some(false)` if it is configured/registered
    /// but not in membership (the production dc1 symptom — flagged
    /// "UNJOINED" in the WebUI instead of plain "Healthy"). `None` for
    /// data/client rows, which don't participate in raft membership.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) raft_member: Option<bool>,
}

/// Bug BC-12a — quarantine projection on /api/v1/nodes rows.
#[derive(Serialize)]
pub(super) struct QuarantineResponse {
    pub(super) since_ms: u64,
    pub(super) reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) triggering_chunk_id: Option<String>,
    pub(super) fault_count_total: u32,
    pub(super) last_fault_at_ms: u64,
    pub(super) last_fault_summary: String,
}

impl QuarantineResponse {
    pub(super) fn from_info(q: &dtfs_common::types::QuarantineInfo) -> Self {
        Self {
            since_ms: q.since_ms,
            reason: format!("{:?}", q.reason),
            triggering_chunk_id: q.triggering_chunk_id.clone(),
            fault_count_total: q.fault_count_total,
            last_fault_at_ms: q.last_fault_at_ms,
            last_fault_summary: q.last_fault_summary.clone(),
        }
    }
}

impl NodeResponse {
    pub(super) fn from_entry(id: u64, info: &dtfs_raft::NodeInfo) -> Self {
        Self {
            node_id: id.to_string(),
            hostname: info.hostname.clone(),
            grpc_addr: info.grpc_addr.clone(),
            location: format!(
                "{}/{}/{}/{}",
                info.location.region,
                info.location.datacenter,
                info.location.rack,
                info.location.host,
            ),
            disk_count: info.disks.len(),
            total_capacity: info.total_capacity,
            used_capacity: info.used_capacity,
            total_bytes_read: info.total_bytes_read,
            total_bytes_written: info.total_bytes_written,
            health: format!("{:?}", info.health),
            role: format!("{:?}", info.role),
            last_seen_ms: info.last_seen_ms,
            client_ip: info.client_ip.clone(),
            is_leader: false,
            version: info.version.clone(),
            build_date: info.build_date.clone(),
            disks: info
                .disks
                .iter()
                .map(|d| DiskResponse {
                    disk_id: d.disk_id,
                    uuid: d.uuid.clone(),
                    mount_path: d.mount_path.clone(),
                    tier: format!("{:?}", d.tier).to_ascii_lowercase(),
                    total_bytes: d.total_bytes,
                    used_bytes: d.used_bytes,
                    status: format!("{:?}", d.status).to_ascii_lowercase(),
                })
                .collect(),
            quarantine: info.quarantine.as_ref().map(QuarantineResponse::from_info),
            // CF-4c — set by list_nodes for Manager rows; None otherwise.
            raft_member: None,
        }
    }
}

pub(super) async fn list_nodes(State(state): State<Arc<AppState>>) -> Json<Vec<NodeResponse>> {
    let sm = state.sm_data.read().await;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    // Bug AX — query Raft's current leader once so we can mark whichever
    // manager NodeResponse matches. None when quorum is lost.
    // CF-4c — same metrics snapshot yields the membership sets so a
    // Manager row that is neither voter nor learner is flagged "unjoined"
    // (the production dc1 symptom) rather than rendered plain "Healthy".
    use openraft::async_runtime::watch::WatchReceiver;
    let metrics = state.raft.metrics().borrow_watched().clone();
    let leader_id: Option<u64> = metrics.current_leader;
    let membership = metrics.membership_config.membership();
    let voter_ids: BTreeSet<u64> = membership.voter_ids().collect();
    let learner_ids: BTreeSet<u64> = membership.learner_ids().collect();

    // 1. Data + manager nodes registered in Raft (have heartbeat /
    //    join records).
    let liveness_window = state.config.raft.node_liveness_secs;
    let mut nodes: Vec<NodeResponse> = sm
        .nodes
        .iter()
        .map(|(id, info)| {
            let mut r = NodeResponse::from_entry(*id, info);
            // Bug BP-3 — project stale-liveness downgrade onto the reported
            // health so a frozen-heartbeat node is not shown Healthy.
            let eff = effective_health(info.health, info.last_seen_ms, now_ms, liveness_window);
            r.health = format!("{eff:?}");
            if Some(*id) == leader_id {
                r.is_leader = true;
            }
            // CF-4c — only Manager rows carry the membership flag.
            if matches!(info.role, NodeRole::Manager) {
                r.raft_member = Some(is_raft_member(*id, &voter_ids, &learner_ids));
            }
            r
        })
        .collect();

    // 2. WebUI bug — managers don't self-register through the
    //    data-node heartbeat path so they were missing from this list
    //    and from Home → "Data & Manager Nodes" and from Topology.
    //    Synthesise entries from the cluster's initial-voters list;
    //    if a manager's hostname is also in `sm.nodes` (e.g. because
    //    a manager also runs as a data node on toro-plex), skip the
    //    synthetic entry.
    let known_hostnames: std::collections::HashSet<&str> = sm
        .nodes
        .values()
        .map(|n| n.hostname.as_str())
        .collect();
    for v in &state.config.cluster.initial_voters {
        if known_hostnames.contains(v.hostname.as_str()) {
            continue;
        }
        let id = dtfs_common::node_id_from_hostname(&v.hostname);
        // Bug X — render real location when the operator configured it;
        // fall back to the `?` placeholder otherwise so legacy
        // (location-less) initial_voters keep working.
        let render_seg = |s: &str| if s.is_empty() { "?".to_string() } else { s.to_string() };
        let location = format!(
            "{}/{}/{}/{}",
            render_seg(&v.region),
            render_seg(&v.datacenter),
            render_seg(&v.rack),
            v.hostname,
        );
        // Bug AY — managers don't go through RegisterNode, so the only
        // version we can confidently fill is the local manager's own
        // (the one serving this API call). Other managers' version
        // stays empty until a future per-manager status RPC is wired.
        let is_local = v.hostname == state.config.node.hostname;
        let synth_version = if is_local {
            dtfs_common::version::VERSION.to_string()
        } else {
            String::new()
        };
        let synth_build_date = if is_local {
            dtfs_common::version::BUILD_DATE.to_string()
        } else {
            String::new()
        };
        nodes.push(NodeResponse {
            node_id: id.to_string(),
            hostname: v.hostname.clone(),
            grpc_addr: format!("http://{}", v.addr),
            location,
            disk_count: 0,
            total_capacity: 0,
            used_capacity: 0,
            total_bytes_read: 0,
            total_bytes_written: 0,
            health: "Healthy".to_string(),
            role: "Manager".to_string(),
            // The API server replying is itself a manager — peers are
            // assumed alive within the cluster heartbeat window. A
            // future fix wires per-manager last-seen via Raft membership.
            last_seen_ms: now_ms,
            client_ip: None,
            is_leader: Some(id) == leader_id,
            version: synth_version,
            build_date: synth_build_date,
            disks: Vec::new(),
            quarantine: None,
            // CF-4c — a configured initial_voter that never actually joined
            // raft (the production dc1 case) is neither voter nor learner;
            // surface that so the WebUI flags it "UNJOINED".
            raft_member: Some(is_raft_member(id, &voter_ids, &learner_ids)),
        });
    }

    // 3. FUSE clients — Bug AA now self-register via RegisterNode with
    //    role="client", so they appear in step 1 (sm.nodes) with their
    //    full Location. Fall back to lease-derived synthesis for any
    //    lease whose owner host isn't already registered (defends
    //    against older FUSE binaries that don't self-register yet).
    //
    //    owner_id format (Bug X compatible):
    //      legacy: "<host>:<pid>:<fh>"
    //      Bug X:  "<host>:<pid>:<fh>:<dc>"
    //    Existing prefix-match logic (Bug J) ignores the trailing fields,
    //    so this is backward-compatible with old leases.
    let mut client_seen: std::collections::HashMap<String, (u64, String)> = Default::default();
    for lease in sm.write_leases.values() {
        let parts: Vec<&str> = lease.owner_id.split(':').collect();
        let host = match parts.first() {
            Some(h) if !h.is_empty() => h.to_string(),
            _ => continue,
        };
        // 4th field (index 3) is Bug X's optional DC.
        let dc = parts.get(3).map(|s| s.to_string()).unwrap_or_default();
        let e = client_seen.entry(host).or_insert((0, dc.clone()));
        if lease.expires_at_ms > e.0 {
            e.0 = lease.expires_at_ms;
        }
        if e.1.is_empty() && !dc.is_empty() {
            e.1 = dc;
        }
    }
    for (host, (last_seen, dc)) in client_seen {
        if known_hostnames.contains(host.as_str()) {
            continue;
        }
        let id = dtfs_common::node_id_from_hostname(&host);
        let dc_seg = if dc.is_empty() { "?".to_string() } else { dc };
        nodes.push(NodeResponse {
            node_id: id.to_string(),
            hostname: host.clone(),
            grpc_addr: String::new(),
            location: format!("?/{}/?/{}", dc_seg, host),
            disk_count: 0,
            total_capacity: 0,
            used_capacity: 0,
            total_bytes_read: 0,
            total_bytes_written: 0,
            health: "Healthy".to_string(),
            role: "Client".to_string(),
            last_seen_ms: last_seen,
            // Lease-derived clients pre-date Bug AX self-registration,
            // so we have no peer IP for them.
            client_ip: None,
            is_leader: false,
            version: String::new(),
            build_date: String::new(),
            disks: Vec::new(),
            quarantine: None,
            // CF-4c — client rows don't participate in raft membership.
            raft_member: None,
        });
    }

    Json(nodes)
}

pub(super) async fn get_node(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(node_id): axum::extract::Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let nid: u64 = node_id.parse().map_err(|_| err_body(StatusCode::BAD_REQUEST, "node_id must be u64"))?;
    let sm = state.sm_data.read().await;

    let info = sm
        .nodes
        .get(&nid)
        .ok_or_else(|| err_body(StatusCode::NOT_FOUND, format!("node {nid} not found")))?;

    let disks: Vec<Value> = info
        .disks
        .iter()
        .map(|d| {
            json!({
                "disk_id": d.disk_id,
                "uuid": d.uuid,
                "mount_path": d.mount_path,
                "tier": format!("{:?}", d.tier),
                "total_bytes": d.total_bytes,
                "used_bytes": d.used_bytes,
                "status": format!("{:?}", d.status),
            })
        })
        .collect();

    Ok(Json(json!({
        "node_id": nid.to_string(),
        "hostname": info.hostname,
        "grpc_addr": info.grpc_addr,
        "location": {
            "region": info.location.region,
            "datacenter": info.location.datacenter,
            "rack": info.location.rack,
            "host": info.location.host,
        },
        "disks": disks,
        "total_capacity": info.total_capacity,
        "used_capacity": info.used_capacity,
        "health": format!("{:?}", liveness_projector(&state)(info)),
        "role": format!("{:?}", info.role),
    })))
}

pub(super) fn err_body(code: StatusCode, msg: impl std::fmt::Display) -> (StatusCode, Json<Value>) {
    (code, Json(json!({"error": msg.to_string()})))
}

#[derive(Deserialize)]
pub(super) struct MaintenanceRequest {
    pub(super) enable: bool,
}

pub(super) async fn set_maintenance(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(node_id): axum::extract::Path<String>,
    Json(body): Json<MaintenanceRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let nid: u64 = node_id.parse().map_err(|_| err_body(StatusCode::BAD_REQUEST, "node_id must be u64"))?;
    // The apply silently ignores an unknown node; refuse it here.
    {
        let sm = state.sm_data.read().await;
        let Some(node) = sm.nodes.get(&nid) else {
            return Err(err_body(StatusCode::NOT_FOUND, format!("node {nid} not found")));
        };
        // Audit #7 — the apply refuses quarantined nodes, and a disable on a
        // non-Maintenance node would mark e.g. a Down node Healthy.
        let refusal = if node.quarantine.is_some() {
            Some(format!("node {nid} is quarantined; unquarantine it first"))
        } else if !body.enable && node.health != dtfs_common::types::HealthStatus::Maintenance {
            Some(format!("node {nid} is not in maintenance (health {:?})", node.health))
        } else {
            None
        };
        if let Some(msg) = refusal {
            warn!(node = nid, enable = body.enable, error = %msg, "maintenance toggle refused");
            return Err(err_body(StatusCode::CONFLICT, msg));
        }
    }

    let status_str = if body.enable {
        "maintenance"
    } else {
        "healthy"
    };

    let req = ClientRequest::UpdateNodeStatus { now_ms: crate::metadata::stamp_now_ms(), keep_sticky: false,
        node_id: nid,
        status: status_str.to_string(),
    };

    let resp = state
        .raft
        .client_write(req)
        .await
        .map_err(|e| err_body(StatusCode::SERVICE_UNAVAILABLE, format!("raft write failed: {e}")))?;
    if let Err(message) = crate::scheduler::client_response_ok(&resp.data) {
        warn!(node = nid, error = %message, "maintenance toggle refused by apply");
        return Err(err_body(StatusCode::CONFLICT, message));
    }

    state.audit_log.log(
        "api",
        "maintenance.toggle",
        &format!("node={nid}"),
        if body.enable { "enabled" } else { "disabled" },
    );

    info!(
        "Maintenance mode {} for node {}",
        if body.enable { "enabled" } else { "disabled" },
        nid
    );

    Ok(Json(json!({
        "node_id": nid.to_string(),
        "maintenance": body.enable,
    })))
}

/// Bug BC-12 — body for `POST /api/v1/nodes/{id}/unquarantine`.
///
/// `operator_id` identifies WHO unquarantined (free-form; in the absence
/// of auth this is a label the operator picks, e.g. their initials or
/// a ticket number). Empty rejected so the audit row always has a name.
///
/// `reason_acknowledged` is the operator's written justification for
/// clearing the integrity-fault state. Minimum 16 chars to force a
/// real sentence rather than "ok". The text lands in the audit log
/// + the WebUI integrity feed so a future operator can reconstruct
/// why the quarantine was lifted.
#[derive(Deserialize)]
pub(super) struct UnquarantineRequest {
    pub(super) operator_id: String,
    pub(super) reason_acknowledged: String,
}

pub(super) async fn unquarantine_node_endpoint(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(node_id): axum::extract::Path<String>,
    Json(body): Json<UnquarantineRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let nid: u64 = node_id.parse().map_err(|_| (
        StatusCode::BAD_REQUEST,
        Json(json!({"error": "node_id must be u64"})),
    ))?;

    let op = body.operator_id.trim();
    if op.is_empty() {
        return Err((StatusCode::BAD_REQUEST,
            Json(json!({"error": "operator_id required"}))));
    }
    if body.reason_acknowledged.trim().len() < 16 {
        return Err((StatusCode::BAD_REQUEST, Json(json!({
            "error": "reason_acknowledged must be at least 16 characters \
                      (force a real justification, not 'ok'). Recommended: \
                      describe what you investigated + why the chunk \
                      is now trusted."
        }))));
    }

    // Confirm the node is actually Quarantined before raft-writing —
    // unquarantine on a Healthy node is a no-op but operators
    // probably typed the wrong node id.
    {
        let sm = state.sm_data.read().await;
        match sm.nodes.get(&nid) {
            None => return Err((StatusCode::NOT_FOUND, Json(json!({
                "error": format!("node {nid} not found")
            })))),
            Some(n) if n.quarantine.is_none() => {
                return Err((StatusCode::CONFLICT, Json(json!({
                    "error": format!("node {nid} is not Quarantined (current health={:?})", n.health)
                }))));
            }
            _ => {}
        }
    }

    let req = ClientRequest::UnquarantineNode { now_ms: crate::metadata::stamp_now_ms(),
        node_id: nid,
        operator_id: op.to_string(),
        reason_acknowledged: body.reason_acknowledged.clone(),
    };

    let resp = state
        .raft
        .client_write(req)
        .await
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": format!("raft write failed: {e}")}))))?;
    if let Err(message) = crate::scheduler::client_response_ok(&resp.data) {
        warn!(node = nid, error = %message, "unquarantine refused by apply");
        return Err(err_body(StatusCode::CONFLICT, message));
    }

    state.audit_log.log(
        "api",
        "node.unquarantine",
        &format!("node={nid} by={op}"),
        &body.reason_acknowledged,
    );

    info!("Bug BC-12: node {nid} unquarantined by {op}");
    Ok(Json(json!({
        "node_id": nid.to_string(),
        "ok": true,
    })))
}

/// Body for `POST /api/v1/nodes/{id}/decommission`. `operator_id` is a
/// free-form actor label (initials / ticket) so the audit row has a name;
/// `reason` is the written justification for removing the node.
#[derive(Deserialize)]
pub(super) struct DecommissionRequest {
    pub(super) operator_id: String,
    #[serde(default)]
    pub(super) reason: String,
}

/// Remove a drained/dead node from the cluster registry. Integrity guard
/// (A.6 #5): the node MUST hold no chunk replicas, else removing it would
/// dangle its location entries and silently lower the replication factor —
/// drain (re-replicate the chunks off) first. Leader-gated via the Raft
/// `DeregisterNode` command, whose apply re-checks the same invariant
/// authoritatively (so a chunk landing between this pre-check and the apply
/// is still refused).
pub(super) async fn decommission_node_endpoint(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(node_id): axum::extract::Path<String>,
    Json(body): Json<DecommissionRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let nid: u64 = node_id.parse().map_err(|_| (
        StatusCode::BAD_REQUEST,
        Json(json!({"error": "node_id must be u64"})),
    ))?;
    let op = body.operator_id.trim();
    if op.is_empty() {
        return Err((StatusCode::BAD_REQUEST,
            Json(json!({"error": "operator_id required"}))));
    }

    // Pre-check: node exists + holds no chunk replicas (the apply re-checks).
    {
        let sm = state.sm_data.read().await;
        if !sm.nodes.contains_key(&nid) {
            return Err((StatusCode::NOT_FOUND,
                Json(json!({"error": format!("node {nid} not found")}))));
        }
        let nid_str = nid.to_string();
        let holds = sm.files.values()
            .any(|f| f.chunks.iter().any(|c| c.node_id == nid_str));
        if holds {
            return Err((StatusCode::CONFLICT, Json(json!({
                "error": format!("node {nid} still holds chunk replicas; \
                                  drain/re-replicate before decommission")
            }))));
        }
    }

    let resp = state
        .raft
        .client_write(ClientRequest::DeregisterNode { now_ms: crate::metadata::stamp_now_ms(), node_id: nid })
        .await
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": format!("raft write failed: {e}")}))))?;

    // The apply is the authoritative writer; surface a committed refusal.
    if let Err(message) = crate::scheduler::client_response_ok(&resp.data) {
        return Err((StatusCode::CONFLICT, Json(json!({"error": message}))));
    }

    state.audit_log.log("api", "node.decommission",
        &format!("node={nid} by={op}"), &body.reason);
    info!("node {nid} decommissioned by {op}");
    Ok(Json(json!({"node_id": nid.to_string(), "ok": true})))
}
