//! Handler-level tests over the `test_support` harness.
#![allow(unused_imports, deprecated)]

use super::test_support::harness;
use super::*;

#[tokio::test]
async fn harness_builds() {
    let h = harness().await;
    assert!(h.state.sm_data.read().await.nodes.is_empty());
}

// ---- Task 1: secret redaction ------------------------------------------------

fn webhook_cfg() -> Value {
    json!({"alerts": {"webhooks": [
        {"url": "https://hooks.example/T0K3N", "secret": "s3cr3t", "kinds": ["a"]},
    ]}, "cluster": {"shared_secret": "top", "name": "c"}})
}

#[test]
fn redact_masks_webhook_secret_and_url_inside_arrays() {
    let mut v = webhook_cfg();
    redact_secret_leaves(&mut v);
    let w = &v["alerts"]["webhooks"][0];
    assert_eq!(w["secret"], "***");
    assert_eq!(w["url"], "***");
    assert_eq!(w["kinds"][0], "a");
    assert_eq!(v["cluster"]["shared_secret"], "***");
    assert_eq!(v["cluster"]["name"], "c");
}

#[test]
fn secret_key_names() {
    for k in ["a.token", "x.password", "secret", "cluster.shared_secret"] {
        assert!(config_key_is_secret(k), "{k}");
    }
    for k in ["cluster.name", "tokens_per_sec_limit", "alerts.webhooks"] {
        assert!(!config_key_is_secret(k), "{k}");
    }
}

#[test]
fn flatten_masks_array_leaf_secrets() {
    let mut rows = Vec::new();
    flatten_config(&webhook_cfg(), &Value::Null, "manager", "", &mut rows);
    let row = rows.iter().find(|r| r["name"] == "manager.alerts.webhooks").unwrap();
    let s = row["current"].to_string();
    assert!(!s.contains("s3cr3t") && !s.contains("T0K3N"), "{s}");
    assert_eq!(row["is_default"], false);
}

#[test]
fn sentinel_round_trip_restores_array_secrets() {
    let old = webhook_cfg();
    let mut posted = json!({"alerts": {"webhooks": [{"url": "***", "secret": "***", "dedup_secs": 5}]}});
    restore_array_sentinels(&mut posted, &old);
    let w = &posted["alerts"]["webhooks"][0];
    assert_eq!(w["url"], "https://hooks.example/T0K3N");
    assert_eq!(w["secret"], "s3cr3t");
    assert_eq!(w["dedup_secs"], 5);
    // A genuinely new value is kept.
    let mut posted = json!({"alerts": {"webhooks": [{"url": "https://new", "secret": "n"}]}});
    restore_array_sentinels(&mut posted, &old);
    assert_eq!(posted["alerts"]["webhooks"][0]["secret"], "n");
}

// ---- Task 3: file mode + node-scoped keys ------------------------------------

#[cfg(unix)]
#[test]
fn manager_toml_written_0600_even_over_stale_tmp() {
    use std::os::unix::fs::PermissionsExt;
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("manager.toml");
    let stale = p.with_extension("toml.az5.tmp");
    std::fs::write(&stale, "old").unwrap();
    std::fs::set_permissions(&stale, std::fs::Permissions::from_mode(0o644)).unwrap();
    write_manager_toml_atomic(&p, "a = 1\n").unwrap();
    assert_eq!(std::fs::read_to_string(&p).unwrap(), "a = 1\n");
    assert_eq!(std::fs::metadata(&p).unwrap().permissions().mode() & 0o777, 0o600);
}

#[test]
fn non_cluster_keys_flags_node_scoped_only() {
    let v = json!({"node": {"hostname": "x"}, "api": {"listen_addr": "y"}, "tiering": {"evaluation_interval_secs": 5}});
    let mut bad = non_cluster_keys(&v);
    bad.sort();
    assert_eq!(bad, vec!["api.listen_addr", "node.hostname"]);
    assert!(non_cluster_keys(&json!({"tiering": {"a": 1}, "alerts": {"webhooks": []}})).is_empty());
}

#[tokio::test]
async fn update_config_rejects_node_scoped_key_with_400() {
    let h = harness().await;
    let req = ConfigUpdateRequest {
        config_json: r#"{"node":{"hostname":"evil"}}"#.into(),
        write_to_disk: false,
        acknowledge_cluster_wide: false,
    };
    let err = update_config(State(h.state.clone()), Json(req)).await.unwrap_err();
    assert_eq!(err.0, StatusCode::BAD_REQUEST);
    assert!(err.1.0["error"].as_str().unwrap().contains("node.hostname"));
}

#[test]
fn merge_onto_stored_override_keeps_earlier_keys() {
    let m = merge_onto_stored_override(
        r#"{"tiering":{"a":1,"b":2},"defaults":{"x":1}}"#,
        r#"{"tiering":{"b":3}}"#,
    );
    let v: Value = serde_json::from_str(&m).unwrap();
    assert_eq!(v["tiering"]["a"], 1);
    assert_eq!(v["tiering"]["b"], 3);
    assert_eq!(v["defaults"]["x"], 1);
    // explicit reset and empty store
    assert_eq!(merge_onto_stored_override(r#"{"a":1}"#, ""), "");
    assert_eq!(merge_onto_stored_override("", r#"{"a":1}"#), r#"{"a":1}"#);
}

// ---- Tasks 2, 4, 7: files endpoints ------------------------------------------

fn file(path: &str) -> dtfs_raft::state_machine::FileMetadata {
    let row = |node: &str| dtfs_raft::ChunkLocationEntry {
        chunk_index: 0,
        chunk_hash: "h".into(),
        node_id: node.into(),
        disk_id: 0,
        size_bytes: 10,
    };
    dtfs_raft::state_machine::FileMetadata {
        path: path.into(),
        volume_id: "default".into(),
        chunk_size: 10,
        chunks: vec![row("a"), row("b")],
        created_at_ms: 0,
        updated_at_ms: 0,
        symlink_target: None,
        logical_size: None,
        is_directory: false,
        cache_cooldown_until_ms: Default::default(),
        ever_committed: None, replica_added_at_ms: 0,
    }
}

fn mig(prefix: &str, allow: bool) -> StartMigrationRequest {
    StartMigrationRequest {
        path_prefix: prefix.into(),
        target_tier: "warm".into(),
        target_dc: None,
        allow_empty_prefix: allow,
        max_tasks: None,
    }
}

#[test]
fn prefix_is_empty_cases() {
    for p in ["", "/", "  ", " / "] {
        assert!(prefix_is_empty(p), "{p:?}");
    }
    for p in ["/a", "/a/", "a"] {
        assert!(!prefix_is_empty(p), "{p:?}");
    }
}

#[tokio::test]
async fn migration_root_prefix_needs_allow_flag() {
    let h = harness().await;
    let err = start_path_migration(State(h.state.clone()), Json(mig("/", false))).await.unwrap_err();
    assert_eq!(err.0, StatusCode::BAD_REQUEST);
    assert!(err.1.0["error"].as_str().unwrap().contains("allow_empty_prefix"));
}

#[tokio::test]
async fn migration_on_non_leader_is_refused_not_accepted() {
    let h = harness().await;
    // Valid body, gate satisfied: must stop at the leader check (409), not 200.
    let err = start_path_migration(State(h.state.clone()), Json(mig("/a/", false))).await.unwrap_err();
    assert_eq!(err.0, StatusCode::CONFLICT);
    let err = start_path_migration(State(h.state.clone()), Json(mig("/", true))).await.unwrap_err();
    assert_eq!(err.0, StatusCode::CONFLICT);
}

#[tokio::test]
async fn flush_root_prefix_needs_allow_flag_after_leader_gate() {
    // Non-leader harness: the leader gate fires first.
    let h = harness().await;
    let req = FlushCacheRequest { path_prefix: "/".into(), cache_tiers: None, cooldown_secs: None, max_tasks: None, allow_empty_prefix: false };
    assert_eq!(flush_cache_endpoint(State(h.state.clone()), Json(req)).await.unwrap_err().0, StatusCode::CONFLICT);
}

#[tokio::test]
async fn list_files_paging_prefix_and_truncation() {
    let h = harness().await;
    {
        let mut sm = h.state.sm_data.write().await;
        for i in 0..5 {
            sm.files.insert(format!("/a/{i}"), file(&format!("/a/{i}")));
        }
        sm.files.insert("/b/0".into(), file("/b/0"));
    }
    let q = |limit, offset, prefix: Option<&str>| Query(ListFilesQuery { limit, offset, prefix: prefix.map(String::from) });
    let r = list_files(State(h.state.clone()), q(Some(2), None, None)).await.0;
    assert_eq!(r["files"].as_array().unwrap().len(), 2);
    assert_eq!(r["total"], 6);
    assert_eq!(r["count"], 6);
    assert_eq!(r["truncated"], true);
    // Replica rows must not multiply size/chunk_count.
    assert_eq!(r["files"][0]["total_size"], 10);
    assert_eq!(r["files"][0]["chunk_count"], 1);
    // Last page: not truncated.
    let r = list_files(State(h.state.clone()), q(Some(2), Some(4), None)).await.0;
    assert_eq!(r["files"].as_array().unwrap().len(), 2);
    assert_eq!(r["truncated"], false);
    // Prefix filter.
    let r = list_files(State(h.state.clone()), q(None, None, Some("/b/"))).await.0;
    assert_eq!(r["total"], 1);
    assert_eq!(r["truncated"], false);
    // limit=0 clamps to 1, huge limit clamps, offset past end is empty.
    let r = list_files(State(h.state.clone()), q(Some(0), None, None)).await.0;
    assert_eq!(r["files"].as_array().unwrap().len(), 1);
    let r = list_files(State(h.state.clone()), q(Some(usize::MAX), Some(99), None)).await.0;
    assert_eq!(r["files"].as_array().unwrap().len(), 0);
    assert_eq!(r["truncated"], false);
}

// ---- Tasks 5, 8, 10: node endpoints ------------------------------------------

fn data_node(last_seen_ms: u64) -> dtfs_raft::NodeInfo {
    dtfs_raft::NodeInfo {
        hostname: "d1".into(),
        grpc_addr: String::new(),
        role: dtfs_common::types::NodeRole::Data,
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

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
}

async fn seed_node(h: &super::test_support::Harness, last_seen_ms: u64) {
    h.state.sm_data.write().await.nodes.insert(7, data_node(last_seen_ms));
}

fn path7() -> axum::extract::Path<String> {
    axum::extract::Path("7".to_string())
}

#[tokio::test]
async fn get_node_projects_stale_liveness() {
    let h = harness().await;
    seed_node(&h, now_ms() - 3_600_000).await;
    let v = get_node(State(h.state.clone()), path7()).await.unwrap().0;
    assert_eq!(v["health"], "Down");
    seed_node(&h, now_ms()).await;
    let v = get_node(State(h.state.clone()), path7()).await.unwrap().0;
    assert_eq!(v["health"], "Healthy");
}

#[tokio::test]
async fn stats_and_readyz_use_effective_health() {
    let h = harness().await;
    seed_node(&h, now_ms() - 3_600_000).await;
    let s = get_stats(State(h.state.clone())).await.0;
    assert_eq!(s["healthy_nodes"], 0, "{s}");
    assert_ne!(s["health"], "healthy");
    let (_, msg) = readyz_probe(State(h.state.clone())).await;
    assert!(msg.contains("any_healthy=false"), "{msg}");
    let (_, _, metrics) = prometheus_metrics(State(h.state.clone())).await;
    assert!(metrics.contains("dtfs_nodes_down 1\n") && metrics.contains("dtfs_nodes_healthy 0\n"), "{metrics}");

    seed_node(&h, now_ms()).await;
    let s = get_stats(State(h.state.clone())).await.0;
    assert_eq!(s["healthy_nodes"], 1, "{s}");
    // Stats expose the unreachable-goal signal (no volumes in this harness).
    assert_eq!(s["goal_unreachable_volumes"], 0, "{s}");
    assert!(s["goal_unreachable"].as_array().unwrap().is_empty(), "{s}");
    let (_, msg) = readyz_probe(State(h.state.clone())).await;
    assert!(msg.contains("any_healthy=true"), "{msg}");
}

#[tokio::test]
async fn cluster_status_ok_with_empty_file_degraded_on_lost_data() {
    let h = harness().await;
    seed_node(&h, now_ms()).await;
    {
        let mut sm = h.state.sm_data.write().await;
        let mut empty = file("/empty");
        empty.chunks.clear();
        empty.ever_committed = Some(false);
        sm.files.insert("/empty".into(), empty);
    }
    let v = cluster_status(State(h.state.clone())).await.0;
    assert_eq!((v["status"].as_str(), v["files_missing_chunks"].as_u64()), (Some("ok"), Some(0)), "{v}");

    {
        let mut sm = h.state.sm_data.write().await;
        let mut lost = file("/lost");
        lost.chunks.clear();
        lost.ever_committed = Some(true);
        sm.files.insert("/lost".into(), lost);
    }
    let v = cluster_status(State(h.state.clone())).await.0;
    assert_eq!((v["status"].as_str(), v["files_missing_chunks"].as_u64()), (Some("degraded"), Some(1)), "{v}");
}

#[tokio::test]
async fn node_errors_carry_json_bodies_and_unknown_node_is_404() {
    let h = harness().await;
    let e = get_node(State(h.state.clone()), path7()).await.unwrap_err();
    assert_eq!(e.0, StatusCode::NOT_FOUND);
    assert!(e.1.0["error"].as_str().unwrap().contains("not found"));
    let e = get_node(State(h.state.clone()), axum::extract::Path("x".into())).await.unwrap_err();
    assert_eq!(e.0, StatusCode::BAD_REQUEST);
    assert!(e.1.0["error"].is_string());
    let e = set_maintenance(State(h.state.clone()), path7(), Json(MaintenanceRequest { enable: true }))
        .await
        .unwrap_err();
    assert_eq!(e.0, StatusCode::NOT_FOUND);
    assert!(e.1.0["error"].is_string());
    // Known node but no leader: the failed write is a 503 with a body, never 200.
    seed_node(&h, now_ms()).await;
    let e = set_maintenance(State(h.state.clone()), path7(), Json(MaintenanceRequest { enable: true }))
        .await
        .unwrap_err();
    assert_eq!(e.0, StatusCode::SERVICE_UNAVAILABLE);
    assert!(e.1.0["error"].is_string());
}

/// Audit #7 — the toggle must refuse (409) instead of reporting success
/// when the apply would ignore it: any quarantined node, or a disable on
/// a node that is not in Maintenance (it would mark a Down node Healthy).
#[tokio::test]
async fn maintenance_toggle_refuses_quarantined_and_non_maintenance_disable() {
    use dtfs_common::types::HealthStatus;
    let h = harness().await;
    seed_node(&h, now_ms()).await;
    let e = set_maintenance(State(h.state.clone()), path7(), Json(MaintenanceRequest { enable: false }))
        .await
        .unwrap_err();
    assert_eq!(e.0, StatusCode::CONFLICT);
    assert!(e.1.0["error"].as_str().unwrap().contains("not in maintenance"), "{:?}", e.1.0);
    h.state.sm_data.write().await.nodes.get_mut(&7).unwrap().health = HealthStatus::Down;
    let e = set_maintenance(State(h.state.clone()), path7(), Json(MaintenanceRequest { enable: false }))
        .await
        .unwrap_err();
    assert_eq!(e.0, StatusCode::CONFLICT);
    // Boundary: a node in Maintenance passes the guard (then 503: no leader).
    h.state.sm_data.write().await.nodes.get_mut(&7).unwrap().health = HealthStatus::Maintenance;
    let e = set_maintenance(State(h.state.clone()), path7(), Json(MaintenanceRequest { enable: false }))
        .await
        .unwrap_err();
    assert_eq!(e.0, StatusCode::SERVICE_UNAVAILABLE);
    {
        let mut sm = h.state.sm_data.write().await;
        let n = sm.nodes.get_mut(&7).unwrap();
        n.health = HealthStatus::Quarantined;
        n.quarantine = Some(dtfs_common::types::QuarantineInfo {
            since_ms: 1,
            reason: dtfs_common::types::QuarantineReason::DeepScrubHashMismatch,
            triggering_chunk_id: None,
            fault_count_total: 1,
            last_fault_at_ms: 1,
            last_fault_summary: String::new(),
        });
    }
    for enable in [true, false] {
        let e = set_maintenance(State(h.state.clone()), path7(), Json(MaintenanceRequest { enable }))
            .await
            .unwrap_err();
        assert_eq!(e.0, StatusCode::CONFLICT, "enable={enable}");
        assert!(e.1.0["error"].as_str().unwrap().contains("quarantined"), "{:?}", e.1.0);
    }
}

// ---- Task 6: anchor datacenter handling --------------------------------------

fn vol(id: &str) -> dtfs_common::types::VolumeConfig {
    use dtfs_common::types::*;
    VolumeConfig {
        id: VolumeId::new(id.to_string()),
        name: id.into(),
        chunk_size_bytes: 1 << 20,
        replication: TierReplication { hot: 1, warm: 1, cold: 1, tape: 1 },
        write_safety: WriteSafety { min_replicas_before_ack: 1, write_timeout_secs: 30 },
        anchor_datacenter: None,
        mandatory_replicas: TierReplication { hot: 0, warm: 1, cold: 1, tape: 0 },
        cache_replicas: CacheReplication::default(),
        anchor: None,
        spill: SpillPolicy::default(),
        version: 0,
        precache: PrecachePolicy::default(),
    }
}

fn create(name: &str, dc: Option<&str>, force: bool) -> CreateVolumeRequest {
    CreateVolumeRequest {
        name: name.into(),
        chunk_size_mb: None,
        replication_hot: None,
        replication_warm: None,
        replication_cold: None,
        anchor_datacenter: dc.map(String::from),
        force,
    }
}

async fn seed_dc(h: &super::test_support::Harness, dc: &str) {
    let mut n = data_node(now_ms());
    n.location = dtfs_common::topology::Location::new("r", dc, "k", "h");
    h.state.sm_data.write().await.nodes.insert(9, n);
}

#[test]
fn anchor_dc_compares_case_insensitively_and_trims() {
    use dtfs_common::types::{AnchorPolicy, DiskTier};
    let a = AnchorPolicy { datacenter: "LELI".into(), required_tier: None };
    assert!(anchor_satisfied_by(&[("leli".into(), DiskTier::Warm)], &a));
    assert!(!anchor_satisfied_by(&[("toro".into(), DiskTier::Warm)], &a));
    assert_eq!(normalize_dc(Some("  leli ")).as_deref(), Some("leli"));
    assert_eq!(normalize_dc(Some("  ")), None);
    assert_eq!(normalize_dc(Some("")), None);
    assert_eq!(normalize_dc(None), None);
}

#[tokio::test]
async fn create_volume_unknown_anchor_dc_is_400_unless_forced() {
    let h = harness().await;
    seed_dc(&h, "LELI").await;
    let e = create_volume(State(h.state.clone()), Json(create("v1", Some("nowhere"), false))).await.unwrap_err();
    assert_eq!(e.0, StatusCode::BAD_REQUEST);
    assert!(e.1.0["error"].as_str().unwrap().contains("nowhere"));
    // Past the DC gate the (leaderless) Raft write is what fails: 503, not 400.
    for (dc, force) in [(Some(" leli "), false), (Some("nowhere"), true), (Some(""), false), (None, false)] {
        let e = create_volume(State(h.state.clone()), Json(create("v2", dc, force))).await.unwrap_err();
        assert_eq!(e.0, StatusCode::SERVICE_UNAVAILABLE, "{dc:?} force={force}");
        assert!(e.1.0["error"].is_string());
    }
}

#[tokio::test]
async fn create_volume_chunk_size_out_of_range_is_400() {
    let h = harness().await;
    for bad in [65u32, 128, 4096] {
        let mut r = create("cs", None, false);
        r.chunk_size_mb = Some(bad);
        let e = create_volume(State(h.state.clone()), Json(r)).await.unwrap_err();
        assert_eq!(e.0, StatusCode::BAD_REQUEST, "{bad}");
        assert!(e.1.0["error"].as_str().unwrap().contains("chunk_size_mb"));
    }
    // In range (and 0 = default) passes the gate; the leaderless Raft write then fails: 503.
    for ok in [0u32, 1, 64] {
        let mut r = create("cs", None, false);
        r.chunk_size_mb = Some(ok);
        let e = create_volume(State(h.state.clone()), Json(r)).await.unwrap_err();
        assert_eq!(e.0, StatusCode::SERVICE_UNAVAILABLE, "{ok}");
    }
}

#[tokio::test]
async fn create_volume_duplicate_is_409_with_body() {
    let h = harness().await;
    h.state.sm_data.write().await.volumes.insert("dup".into(), vol("dup"));
    let e = create_volume(State(h.state.clone()), Json(create("dup", None, false))).await.unwrap_err();
    assert_eq!(e.0, StatusCode::CONFLICT);
    assert!(e.1.0["error"].is_string());
}

#[tokio::test]
async fn set_anchor_unknown_dc_is_400_unless_forced() {
    let h = harness().await;
    seed_dc(&h, "LELI").await;
    h.state.sm_data.write().await.volumes.insert("v".into(), vol("v"));
    let body = |dc: &str, force| SetAnchorRequest { datacenter: Some(dc.into()), required_tier: None, force };
    let p = || axum::extract::Path("v".to_string());
    let e = set_volume_anchor_endpoint(State(h.state.clone()), p(), Json(body("nowhere", false))).await.unwrap_err();
    assert_eq!(e.0, StatusCode::BAD_REQUEST);
    for (dc, force) in [(" leli ", false), ("nowhere", true)] {
        let e = set_volume_anchor_endpoint(State(h.state.clone()), p(), Json(body(dc, force))).await.unwrap_err();
        assert_eq!(e.0, StatusCode::SERVICE_UNAVAILABLE, "{dc:?} force={force}");
    }
    // Clearing the anchor never needs a known DC (no-op here, so no write).
    let r = set_volume_anchor_endpoint(State(h.state.clone()), p(), Json(body("", false))).await.unwrap();
    assert_eq!(r.0["changed"], false);
}

#[test]
fn apply_set_anchor_trims_datacenter() {
    let next = apply_set_anchor(&vol("v"), &SetAnchorRequest { datacenter: Some(" leli ".into()), required_tier: None, force: false }).unwrap();
    assert_eq!(next.anchor.unwrap().datacenter, "leli");
    assert_eq!(next.anchor_datacenter.as_deref(), Some("leli"));
}

// ---- Task 9: integrity feed scans the whole ring -----------------------------

#[tokio::test]
async fn integrity_faults_found_beyond_the_newest_window() {
    let h = harness().await;
    h.state.audit_log.log("scheduler", "orphan.in_state_missing", "node=1", "quarantined");
    for i in 0..100 {
        h.state.audit_log.log("api", "config.update", &format!("v{i}"), "success");
    }
    let q = Query(AuditQuery { limit: Some(5), ..Default::default() });
    let rows = get_integrity_faults(State(h.state.clone()), q).await.0;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["action"], "orphan.in_state_missing");
}

fn sm_audit(action: &str, summary: &str, ts_ms: u64) -> dtfs_raft::state_machine::AuditEntry {
    dtfs_raft::state_machine::AuditEntry {
        log_index: 1,
        summary: summary.into(),
        timestamp_ms: ts_ms,
        actor: if action.is_empty() { String::new() } else { "system".into() },
        action: action.into(),
        target: String::new(),
        result: String::new(),
        origin_manager: String::new(),
    }
}

/// Issue #116 — with the default `replicate_via_raft = false`, the faults
/// that only land in the replicated audit log (heartbeat
/// `read.integrity_fault`, the `node.quarantine` apply row) are returned,
/// merged newest-first with the local ring.
#[tokio::test]
async fn issue_116_integrity_faults_include_replicated_rows_by_default() {
    let h = harness().await;
    assert!(!h.state.config.audit.replicate_via_raft);
    {
        let mut sm = h.state.sm_data.write().await;
        sm.audit_log.push_back(sm_audit("read.integrity_fault", "", 1_000));
        sm.audit_log.push_back(sm_audit("", "node.quarantine: 7 verify_on_read", 2_000));
        sm.audit_log.push_back(sm_audit("config.update", "", 3_000));
    }
    h.state.audit_log.log("scheduler", "orphan.in_state_missing", "node=1", "quarantined");
    let rows = get_integrity_faults(State(h.state.clone()), Query(AuditQuery::default())).await.0;
    let actions: Vec<&str> = rows.iter().map(|r| r["action"].as_str().unwrap()).collect();
    assert_eq!(
        actions,
        ["orphan.in_state_missing", "node.quarantine", "read.integrity_fault"],
        "{rows:?}"
    );
    // Negative: limit applies to the merged list.
    let q = Query(AuditQuery { limit: Some(1), ..Default::default() });
    assert_eq!(get_integrity_faults(State(h.state.clone()), q).await.0.len(), 1);
}

// ---- Manual orphan reap: POST/GET /api/v1/scrub/orphans ----------------------

fn reap(grace_secs: Option<u64>, dry_run: bool) -> Json<OrphanReapRequest> {
    Json(OrphanReapRequest { grace_secs, dry_run })
}

#[test]
fn manual_orphan_grace_defaults_to_config_and_enforces_the_floor() {
    assert_eq!(manual_orphan_grace(None, 86_400), Ok(86_400));
    assert_eq!(manual_orphan_grace(Some(3600), 86_400), Ok(3600));
    let e = manual_orphan_grace(Some(3599), 86_400).unwrap_err();
    assert!(e.contains("below the manual floor of 3600"), "{e}");
}

#[tokio::test]
async fn orphan_reap_grace_below_floor_is_400() {
    let h = harness().await;
    let e = trigger_orphan_reap(State(h.state.clone()), reap(Some(3599), true)).await.unwrap_err();
    assert_eq!(e.0, StatusCode::BAD_REQUEST);
    assert!(e.1.0["error"].as_str().unwrap().contains("below the manual floor"));
}

#[tokio::test]
async fn orphan_reap_on_a_follower_is_409_not_leader() {
    let h = harness().await; // never leader
    let e = trigger_orphan_reap(State(h.state.clone()), reap(None, true)).await.unwrap_err();
    assert_eq!(e.0, StatusCode::CONFLICT);
    assert!(e.1.0["error"].as_str().unwrap().contains("not the leader"), "{:?}", e.1.0);
    // Refused before running: nothing recorded, nothing audited as accepted.
    assert!(h.state.scheduler.last_orphan_run().is_none());
    assert!(!h.state.scheduler.orphan_run_active());
}

#[tokio::test]
async fn orphan_reap_is_single_flight_between_timer_and_manual() {
    let h = harness().await;
    // A running reap (timer or manual) holds the token ...
    let running = h.state.scheduler.try_begin_orphan_run().expect("first token");
    // ... so the timer's next tick skips ...
    assert!(h.state.scheduler.try_begin_orphan_run().is_none());
    // ... and a manual request is 409.
    let e = trigger_orphan_reap(State(h.state.clone()), reap(Some(3600), false)).await.unwrap_err();
    assert_eq!(e.0, StatusCode::CONFLICT);
    assert_eq!(e.1.0["error"], "an orphan reap is already running");
    let status = get_orphan_reap(State(h.state.clone())).await.0;
    assert_eq!(status["running"], true);
    drop(running);
    assert!(h.state.scheduler.try_begin_orphan_run().is_some());
}

#[tokio::test]
async fn orphan_reap_status_reports_config_and_floor() {
    let h = harness().await;
    let v = get_orphan_reap(State(h.state.clone())).await.0;
    assert_eq!(v["running"], false);
    assert_eq!(v["manual_grace_floor_secs"], 3600);
    assert_eq!(v["configured_grace_secs"], h.state.config.scrub.orphan_grace_secs);
    assert!(v["last_run"].is_null());
}

#[tokio::test]
async fn orphan_reap_without_the_shared_secret_is_401() {
    use tower::ServiceExt;
    let h = harness().await;
    let mut config = h.state.config.clone();
    config.cluster.shared_secret = "s3cret".into();
    let state = Arc::new(AppState {
        config,
        raft: h.state.raft.clone(),
        sm_data: h.state.sm_data.clone(),
        audit_log: h.state.audit_log.clone(),
        scheduler: h.state.scheduler.clone(),
        io_rate: h.state.io_rate.clone(),
        memory: None,
        row_aggregates: Default::default(),
        remove_dead_job: Default::default(),
        tiering: None,
    });
    let req = axum::http::Request::builder()
        .method("POST")
        .uri("/api/v1/scrub/orphans")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(r#"{"dry_run":true}"#))
        .unwrap();
    let resp = build_router(state).oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert!(h.state.scheduler.last_orphan_run().is_none());
}

/// Audit #10 — a requested write_to_disk that failed must not report a
/// plain 200/"success": 500, `raft_stored: true`, audit "partial".
#[test]
fn config_update_outcome_reports_failed_disk_write() {
    use super::config::config_update_outcome;
    let (s, b, a) = config_update_outcome(4, Some(Err("EACCES".into())));
    assert_eq!((s, a), (StatusCode::INTERNAL_SERVER_ERROR, "partial"), "{b}");
    assert_eq!((b["raft_stored"].as_bool(), b["write_to_disk"].as_bool()), (Some(true), Some(false)), "{b}");
    assert!(b["error"].as_str().unwrap().contains("EACCES"), "{b}");
    for (disk, wrote) in [(None, false), (Some(Ok(())), true)] {
        let (s, b, a) = config_update_outcome(4, disk);
        assert_eq!((s, a), (StatusCode::OK, "success"), "{b}");
        assert_eq!(b["write_to_disk"].as_bool(), Some(wrote), "{b}");
    }
}

/// #127 — denied calls coalesce per route template, not per raw path, so
/// random path parameters cannot flood the audit ring.
#[tokio::test]
async fn auth_denied_rows_coalesce_per_route_not_per_raw_path() {
    use tower::ServiceExt;
    let h = harness().await;
    let mut config = h.state.config.clone();
    config.cluster.shared_secret = "s3cret".into();
    let state = Arc::new(AppState {
        config,
        raft: h.state.raft.clone(),
        sm_data: h.state.sm_data.clone(),
        audit_log: h.state.audit_log.clone(),
        scheduler: h.state.scheduler.clone(),
        io_rate: h.state.io_rate.clone(),
        memory: None,
        row_aggregates: Default::default(),
        remove_dead_job: Default::default(),
        tiering: None,
    });
    let router = build_router(state);
    for i in 0..100 {
        let req = axum::http::Request::builder()
            .method("POST")
            .uri(format!("/api/v1/nodes/n{i}/maintenance"))
            .header("content-type", "application/json")
            .body(axum::body::Body::from(r#"{"enabled":true}"#))
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
    let rows: Vec<_> = h
        .state
        .audit_log
        .query(&crate::audit::AuditFilter { action: Some("auth.denied".into()), ..Default::default() })
        .into_iter()
        .filter(|e| e.target.contains("/maintenance"))
        .collect();
    assert_eq!(rows.len(), 1, "first row: {:?}", rows.first());
}

// ---- DELETE /api/v1/volumes/{id} ------------------------------------------------

fn vfile(path: &str, volume: &str, dir: bool) -> dtfs_raft::state_machine::FileMetadata {
    let mut f = file(path);
    f.volume_id = volume.into();
    f.is_directory = dir;
    if dir {
        f.chunks.clear();
    }
    f
}

/// Volume `media` with /m (dir), /m/a, /m/b; volume `keep` with /k.
async fn seed_media(h: &super::test_support::Harness) {
    let mut sm = h.state.sm_data.write().await;
    sm.volumes.insert("media".into(), vol("media"));
    sm.volumes.insert("keep".into(), vol("keep"));
    for f in [vfile("/m", "media", true), vfile("/m/a", "media", false), vfile("/m/b", "media", false), vfile("/k", "keep", false)] {
        sm.files.insert(f.path.clone(), f);
    }
}

async fn del_vol(
    h: &super::test_support::Harness,
    id: &str,
    delete_files: bool,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    delete_volume_endpoint(
        State(h.state.clone()),
        axum::extract::Path(id.to_string()),
        Query(DeleteVolumeQuery { delete_files }),
    )
    .await
}

#[tokio::test]
async fn delete_volume_refuses_default_unknown_and_follower() {
    let h = harness().await;
    h.state.sm_data.write().await.volumes.insert("default".into(), vol("default"));
    h.state.sm_data.write().await.volumes.insert("empty".into(), vol("empty"));
    let e = del_vol(&h, "default", true).await.unwrap_err();
    assert_eq!(e.0, StatusCode::BAD_REQUEST);
    let e = del_vol(&h, "nope", false).await.unwrap_err();
    assert_eq!(e.0, StatusCode::NOT_FOUND);
    let e = del_vol(&h, "empty", false).await.unwrap_err();
    assert_eq!(e.0, StatusCode::CONFLICT);
    assert!(e.1.0["error"].as_str().unwrap().contains("not the leader"), "{:?}", e.1.0);
    assert!(h.state.sm_data.read().await.volumes.contains_key("empty"));
}

#[tokio::test]
async fn delete_volume_with_files_needs_delete_files() {
    let h = super::test_support::leader_harness().await;
    seed_media(&h).await;
    let e = del_vol(&h, "media", false).await.unwrap_err();
    assert_eq!(e.0, StatusCode::CONFLICT);
    assert_eq!((e.1.0["files"].clone(), e.1.0["directories"].clone(), e.1.0["bytes"].clone()), (json!(2), json!(1), json!(20)));
    // GET reports the same counts (the CLI reads them to pick its confirmations).
    let g = get_volume_endpoint(State(h.state.clone()), axum::extract::Path("media".into())).await.unwrap();
    assert_eq!((g.0["files"].clone(), g.0["directories"].clone(), g.0["bytes"].clone()), (json!(2), json!(1), json!(20)));
    let sm = h.state.sm_data.read().await;
    assert!(sm.volumes.contains_key("media"));
    assert_eq!(sm.files.len(), 4);
}

#[tokio::test]
async fn delete_volume_empty_is_removed_and_audited() {
    let h = super::test_support::leader_harness().await;
    seed_media(&h).await;
    h.state.sm_data.write().await.volumes.insert("empty".into(), vol("empty"));
    let ok = del_vol(&h, "empty", false).await.unwrap();
    assert_eq!(ok.0["files_deleted"], json!(0));
    let sm = h.state.sm_data.read().await;
    assert!(!sm.volumes.contains_key("empty"));
    assert!(sm.volumes.contains_key("media"));
    let rows = h.state.audit_log.query(&crate::audit::AuditFilter { action: Some("volume.delete".into()), ..Default::default() });
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].result, "success files_deleted=0");
}

#[tokio::test]
async fn delete_volume_with_delete_files_removes_its_files_only() {
    let h = super::test_support::leader_harness().await;
    seed_media(&h).await;
    let ok = del_vol(&h, "media", true).await.unwrap();
    assert_eq!(ok.0["files_deleted"], json!(3));
    let sm = h.state.sm_data.read().await;
    assert!(!sm.volumes.contains_key("media"));
    assert_eq!(sm.files.keys().cloned().collect::<Vec<_>>(), vec!["/k".to_string()]);
    assert!(sm.volumes.contains_key("keep"));
}

#[tokio::test]
async fn delete_volume_refuses_while_a_file_is_leased() {
    let h = super::test_support::leader_harness().await;
    seed_media(&h).await;
    let lease = |exp| dtfs_raft::state_machine::WriteLease { owner_id: "o".into(), lease_id: "l".into(), expires_at_ms: exp };
    // An expired lease does not block; a live one does.
    h.state.sm_data.write().await.write_leases.insert("/m/b".into(), lease(1));
    h.state.sm_data.write().await.write_leases.insert("/m/a".into(), lease(now_ms() + 600_000));
    let e = del_vol(&h, "media", true).await.unwrap_err();
    assert_eq!(e.0, StatusCode::CONFLICT);
    assert_eq!(e.1.0["leased_files"], json!(1));
    let sm = h.state.sm_data.read().await;
    assert!(sm.volumes.contains_key("media"));
    assert_eq!(sm.files.len(), 4);
}

#[tokio::test]
async fn delete_volume_stops_when_a_directory_holds_another_volumes_file() {
    let h = super::test_support::leader_harness().await;
    seed_media(&h).await;
    let f = vfile("/m/other", "keep", false);
    h.state.sm_data.write().await.files.insert(f.path.clone(), f);
    let e = del_vol(&h, "media", true).await.unwrap_err();
    assert_eq!(e.0, StatusCode::CONFLICT);
    assert_eq!(e.1.0["files_deleted"], json!(2));
    let sm = h.state.sm_data.read().await;
    assert!(sm.volumes.contains_key("media"), "volume must stay while /m references it");
    assert!(sm.files.contains_key("/m") && sm.files.contains_key("/m/other"));
}

/// The pre-DeleteVolume re-check: an entry that appeared during the delete
/// loop keeps the volume (409), an empty volume passes.
#[test]
fn delete_volume_recheck_refuses_a_file_that_appeared() {
    let mut sm = StateMachineData::default();
    assert!(delete_volume_check("media", &volume_usage(&sm, "media", 0), false).is_ok());
    let f = vfile("/late", "media", false);
    sm.files.insert(f.path.clone(), f);
    let u = volume_usage(&sm, "media", 0);
    assert_eq!(u.paths, vec!["/late".to_string()]);
    let e = delete_volume_check("media", &u, false).unwrap_err();
    assert_eq!(e.0, StatusCode::CONFLICT);
    assert_eq!(e.1.0["files"], json!(1));
    assert!(delete_volume_check("media", &u, true).is_ok());
}

// ---- pre-caching policy endpoint --------------------------------------------

fn plex_req() -> SetPrecacheRequest {
    SetPrecacheRequest { mode: Some(dtfs_common::types::PrecacheMode::Plex), ..Default::default() }
}

/// Volume with warm cache 1/1 per-DC and warm mandatory 0 (precache-capable).
fn precache_vol(id: &str) -> dtfs_common::types::VolumeConfig {
    let mut v = vol(id);
    v.mandatory_replicas = dtfs_common::types::TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 };
    v.cache_replicas.warm = dtfs_common::types::CacheTierConfig { min: 1, max: 1 };
    v.cache_replicas.per_dc = true;
    v
}

fn open_v0_1_16_gate(sm: &mut dtfs_raft::state_machine::StateMachineData) {
    let mut m = data_node(0);
    m.role = dtfs_common::types::NodeRole::Manager;
    m.version = "v0.1.16".into();
    sm.nodes.insert(10, m);
    let nodes: std::collections::BTreeMap<u64, dtfs_raft::DtfsNode> =
        [(10, dtfs_raft::DtfsNode { addr: String::new(), hostname: String::new() })].into();
    let mem = openraft::Membership::new(vec![[10u64].into_iter().collect()], nodes).unwrap();
    sm.last_membership = openraft::StoredMembership::new(None, mem);
}

#[test]
fn apply_set_precache_merges_and_refuses() {
    use dtfs_common::types::PrecacheMode;
    let next = apply_set_precache(&precache_vol("v"), &SetPrecacheRequest {
        series_max_gb: Some(5), max_series: Some(2), ..plex_req()
    }).unwrap();
    assert_eq!(next.precache.mode, PrecacheMode::Plex);
    assert_eq!((next.precache.series_max_gb, next.precache.max_series, next.precache.next_files), (5, 2, 2));
    // No cache tier on the volume.
    assert!(apply_set_precache(&vol("v"), &plex_req()).unwrap_err().contains("cache tier"));
    // per_dc off.
    let mut v = precache_vol("v");
    v.cache_replicas.per_dc = false;
    assert!(apply_set_precache(&v, &plex_req()).unwrap_err().contains("per_dc"));
    // Plex with series_max_gb 0.
    let e = apply_set_precache(&precache_vol("v"), &SetPrecacheRequest { series_max_gb: Some(0), ..plex_req() }).unwrap_err();
    assert!(e.contains("series_max_gb"), "{e}");
    // max_gb_per_dc merges; above 1 PiB is refused.
    let next = apply_set_precache(&precache_vol("v"), &SetPrecacheRequest { max_gb_per_dc: Some(500), ..plex_req() }).unwrap();
    assert_eq!(next.precache.max_gb_per_dc, 500);
    let e = apply_set_precache(&precache_vol("v"), &SetPrecacheRequest { max_gb_per_dc: Some((1 << 20) + 1), ..plex_req() }).unwrap_err();
    assert_eq!(e, "max_gb_per_dc must be at most 1048576 (1 PiB)");
}

#[tokio::test]
async fn set_precache_404_400_409_and_noop() {
    let h = harness().await;
    let p = || axum::extract::Path("v".to_string());
    let e = set_volume_precache_endpoint(State(h.state.clone()), p(), Json(plex_req())).await.unwrap_err();
    assert_eq!(e.0, StatusCode::NOT_FOUND);

    h.state.sm_data.write().await.volumes.insert("v".into(), vol("v"));
    let e = set_volume_precache_endpoint(State(h.state.clone()), p(), Json(plex_req())).await.unwrap_err();
    assert_eq!(e.0, StatusCode::BAD_REQUEST, "no cache tier");

    h.state.sm_data.write().await.volumes.insert("v".into(), precache_vol("v"));
    // Dev/unstamped cluster: gate closed, a real change is refused.
    let e = set_volume_precache_endpoint(State(h.state.clone()), p(), Json(plex_req())).await.unwrap_err();
    assert_eq!(e.0, StatusCode::CONFLICT);
    assert!(e.1.0["error"].as_str().unwrap().contains("v0.1.16"), "{:?}", e.1.0);
    assert_eq!(e.1.0["min_version"], "v0.1.16");

    // A no-change body needs no gate and no write.
    let r = set_volume_precache_endpoint(State(h.state.clone()), p(), Json(SetPrecacheRequest::default())).await.unwrap();
    assert_eq!(r.0["changed"], false);
    assert_eq!(r.0["config"]["precache"]["mode"], "off");

    // Gate open: past the gate; the leaderless Raft write is what fails (503).
    open_v0_1_16_gate(&mut *h.state.sm_data.write().await);
    let e = set_volume_precache_endpoint(State(h.state.clone()), p(), Json(plex_req())).await.unwrap_err();
    assert_eq!(e.0, StatusCode::SERVICE_UNAVAILABLE);
}

fn assert_v0_1_16_conflict(e: &(StatusCode, Json<serde_json::Value>)) {
    assert_eq!(e.0, StatusCode::CONFLICT, "{:?}", e.1.0);
    assert_eq!(e.1.0["min_version"], "v0.1.16");
}

/// Mixed cluster: set-cache with `retention_secs` and set-precache with
/// `cursor_ttl_secs` are 409; on v0.1.16 both pass the gate (503 = the
/// leaderless Raft write). 0 needs no gate.
#[tokio::test]
async fn retention_and_cursor_ttl_need_v0_1_16() {
    let h = harness().await;
    let p = || axum::extract::Path("v".to_string());
    h.state.sm_data.write().await.volumes.insert("v".into(), precache_vol("v"));
    let ret = |s| SetCacheRequest { retention_secs: Some(s), ..Default::default() };
    let ttl = |s| SetPrecacheRequest { cursor_ttl_secs: Some(s), ..Default::default() };

    let e = set_volume_cache_endpoint(State(h.state.clone()), p(), Json(ret(1_209_600))).await.unwrap_err();
    assert_v0_1_16_conflict(&e);
    assert!(e.1.0["error"].as_str().unwrap().contains("retention_secs"), "{:?}", e.1.0);
    let e = set_volume_precache_endpoint(State(h.state.clone()), p(), Json(ttl(3600))).await.unwrap_err();
    assert_v0_1_16_conflict(&e);
    let cap = SetPrecacheRequest { max_gb_per_dc: Some(500), ..Default::default() };
    let e = set_volume_precache_endpoint(State(h.state.clone()), p(), Json(cap)).await.unwrap_err();
    assert_v0_1_16_conflict(&e);
    // retention 0 on a mixed cluster is not gated (noop here: already 0).
    let r = set_volume_cache_endpoint(State(h.state.clone()), p(), Json(ret(0))).await.unwrap();
    assert_eq!(r.0["changed"], false);

    open_v0_1_16_gate(&mut *h.state.sm_data.write().await);
    let e = set_volume_cache_endpoint(State(h.state.clone()), p(), Json(ret(1_209_600))).await.unwrap_err();
    assert_eq!(e.0, StatusCode::SERVICE_UNAVAILABLE);
    let e = set_volume_precache_endpoint(State(h.state.clone()), p(), Json(ttl(3600))).await.unwrap_err();
    assert_eq!(e.0, StatusCode::SERVICE_UNAVAILABLE);
}

/// A volume already carrying the fields is refused on a mixed cluster by
/// every other full-config upsert too (an older applier would drop them).
#[tokio::test]
async fn upserts_carrying_retention_or_cursor_ttl_need_v0_1_16() {
    let h = harness().await;
    let p = || axum::extract::Path("v".to_string());
    let ws = || Json(SetWriteSafetyRequest { min_replicas_before_ack: None, write_timeout_secs: Some(60) });
    for (retention, ttl, cap) in [(1u64, 0u64, 0u64), (0, 1, 0), (0, 0, 1)] {
        let mut v = precache_vol("v");
        v.cache_replicas.retention_secs = retention;
        v.precache.cursor_ttl_secs = ttl;
        v.precache.max_gb_per_dc = cap;
        h.state.sm_data.write().await.volumes.insert("v".into(), v);
        let e = set_volume_write_safety_endpoint(State(h.state.clone()), p(), ws()).await.unwrap_err();
        assert_v0_1_16_conflict(&e);
        let e = set_volume_cache_endpoint(State(h.state.clone()), p(), Json(SetCacheRequest { promote_reads: Some(9), ..Default::default() }))
            .await
            .unwrap_err();
        assert_v0_1_16_conflict(&e);
    }
    // Neither field set: the same upsert passes the gate (503 = leaderless write).
    h.state.sm_data.write().await.volumes.insert("v".into(), precache_vol("v"));
    let e = set_volume_write_safety_endpoint(State(h.state.clone()), p(), ws()).await.unwrap_err();
    assert_eq!(e.0, StatusCode::SERVICE_UNAVAILABLE);
}

/// set-cache `fill`: on_read is 409 on a mixed cluster, passes the gate on
/// v0.1.16 (503 = leaderless write); on_promote (the default) is a noop;
/// the volume JSON carries the snake_case value.
#[tokio::test]
async fn fill_on_read_needs_v0_1_16() {
    use dtfs_common::types::CacheFill;
    let h = harness().await;
    let p = || axum::extract::Path("v".to_string());
    h.state.sm_data.write().await.volumes.insert("v".into(), precache_vol("v"));
    let fill = |f| Json(SetCacheRequest { fill: Some(f), ..Default::default() });

    let e = set_volume_cache_endpoint(State(h.state.clone()), p(), fill(CacheFill::OnRead)).await.unwrap_err();
    assert_v0_1_16_conflict(&e);
    assert!(e.1.0["error"].as_str().unwrap().contains("fill"), "{:?}", e.1.0);
    let r = set_volume_cache_endpoint(State(h.state.clone()), p(), fill(CacheFill::OnPromote)).await.unwrap();
    assert_eq!(r.0["changed"], false);
    // A volume already on_read gates every other full-config upsert.
    let mut v = precache_vol("v");
    v.cache_replicas.fill = CacheFill::OnRead;
    h.state.sm_data.write().await.volumes.insert("v".into(), v.clone());
    let ws = Json(SetWriteSafetyRequest { min_replicas_before_ack: None, write_timeout_secs: Some(60) });
    assert_v0_1_16_conflict(&set_volume_write_safety_endpoint(State(h.state.clone()), p(), ws).await.unwrap_err());
    assert_eq!(volume_config_to_json(&v)["cache_replicas"]["fill"], "on_read");

    h.state.sm_data.write().await.volumes.insert("v".into(), precache_vol("v"));
    open_v0_1_16_gate(&mut *h.state.sm_data.write().await);
    let e = set_volume_cache_endpoint(State(h.state.clone()), p(), fill(CacheFill::OnRead)).await.unwrap_err();
    assert_eq!(e.0, StatusCode::SERVICE_UNAVAILABLE);
}

#[test]
fn apply_set_precache_pins_replace_and_validate() {
    use dtfs_common::types::PrecachePin;
    let pin = |p: &str| PrecachePin { prefix: p.into(), datacenter: "toro".into() };
    let req = |pins: Vec<PrecachePin>| SetPrecacheRequest { pins: Some(pins), ..Default::default() };
    let one = apply_set_precache(&precache_vol("v"), &req(vec![pin("/tv/a")])).unwrap();
    assert_eq!(one.precache.pins, vec![pin("/tv/a")]);
    // Replaces, does not append; None keeps; empty clears.
    let two = apply_set_precache(&one, &req(vec![pin("/tv/b")])).unwrap();
    assert_eq!(two.precache.pins, vec![pin("/tv/b")]);
    assert_eq!(apply_set_precache(&two, &SetPrecacheRequest::default()).unwrap().precache.pins, two.precache.pins);
    assert!(apply_set_precache(&two, &req(vec![])).unwrap().precache.pins.is_empty());
    for bad in ["", "/", "tv/x"] {
        assert!(apply_set_precache(&precache_vol("v"), &req(vec![pin(bad)])).is_err(), "{bad}");
    }
    assert!(apply_set_precache(&precache_vol("v"), &req((0..33).map(|i| pin(&format!("/p{i}"))).collect())).is_err());
}

#[tokio::test]
async fn set_precache_pin_unknown_dc_is_400() {
    use dtfs_common::types::PrecachePin;
    let h = harness().await;
    {
        let mut sm = h.state.sm_data.write().await;
        sm.volumes.insert("v".into(), precache_vol("v"));
        open_v0_1_16_gate(&mut sm);
        sm.nodes.get_mut(&10).unwrap().location = dtfs_common::topology::Location::new("r", "toro", "rk", "h");
    }
    let body = |dc: &str| SetPrecacheRequest {
        pins: Some(vec![PrecachePin { prefix: "/tv/a".into(), datacenter: dc.into() }]),
        ..Default::default()
    };
    let p = || axum::extract::Path("v".to_string());
    let e = set_volume_precache_endpoint(State(h.state.clone()), p(), Json(body("nowhere"))).await.unwrap_err();
    assert_eq!(e.0, StatusCode::BAD_REQUEST);
    assert!(e.1.0["error"].as_str().unwrap().contains("nowhere"), "{:?}", e.1.0);
    // Known DC (case-insensitive) passes the check; the leaderless Raft write then fails (503).
    let e = set_volume_precache_endpoint(State(h.state.clone()), p(), Json(body("TORO"))).await.unwrap_err();
    assert_eq!(e.0, StatusCode::SERVICE_UNAVAILABLE);
}

#[test]
fn volume_json_carries_precache_block() {
    let mut v = precache_vol("v");
    v.precache.mode = dtfs_common::types::PrecacheMode::Plex;
    let j = volume_config_to_json(&v);
    assert_eq!(j["precache"]["mode"], "plex");
    assert_eq!(j["precache"]["series_max_gb"], 20);
}

fn with_engine(h: &super::test_support::Harness, status: Option<crate::precache::PrecacheStatus>) -> Arc<AppState> {
    let engine = Arc::new(crate::tiering::TieringEngine::new(
        h.state.config.tiering.clone(),
        h.state.scheduler.clone(),
        h.state.audit_log.clone(),
        h.state.sm_data.clone(),
    ));
    if let Some(s) = status {
        engine.set_precache_status_for_test(s);
    }
    Arc::new(AppState {
        config: h.state.config.clone(),
        raft: h.state.raft.clone(),
        sm_data: h.state.sm_data.clone(),
        audit_log: h.state.audit_log.clone(),
        scheduler: h.state.scheduler.clone(),
        io_rate: h.state.io_rate.clone(),
        memory: None,
        row_aggregates: Default::default(),
        remove_dead_job: Default::default(),
        tiering: Some(engine),
    })
}

fn seeded_precache_status() -> crate::precache::PrecacheStatus {
    crate::precache::PrecacheStatus {
        is_leader: true,
        inflight_tasks: 7,
        live_tasks: 31,
        yielded_live_tasks: true,
        dcs: vec![crate::precache::DcStatus {
            dc: "toro".into(),
            tier: dtfs_common::types::DiskTier::Warm,
            room_bytes: 100,
            desired_bytes: 42,
            cached_bytes: 10,
            capacity_limited: true,
            no_target: false,
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn precache_status_as_leader_returns_arrays() {
    let h = super::test_support::leader_harness().await;
    let st = with_engine(&h, Some(seeded_precache_status()));
    let j = get_precache_status(State(st)).await.unwrap().0;
    assert_eq!(j["is_leader"], json!(true));
    assert_eq!(j["dcs"][0]["dc"], "toro");
    assert!(j["series"].is_array() && j["cursors"].is_array(), "{j}");
}

#[tokio::test]
async fn precache_status_as_follower_is_409_not_the_leader() {
    let h = super::test_support::harness().await;
    let e = get_precache_status(State(with_engine(&h, None))).await.unwrap_err();
    assert_eq!(e.0, StatusCode::CONFLICT);
    assert!(e.1.0["error"].as_str().unwrap().contains("not the leader"), "{:?}", e.1.0);
}

#[tokio::test]
async fn metrics_include_precache_gauges_for_seeded_status() {
    let h = super::test_support::harness().await;
    let (_, _, body) = super::cluster::prometheus_metrics(State(with_engine(&h, Some(seeded_precache_status())))).await;
    for want in [
        "dtfs_precache_is_leader 1",
        "dtfs_precache_inflight_tasks 7",
        "dtfs_precache_live_tasks 31",
        "dtfs_precache_desired_bytes{dc=\"toro\"} 42",
        "dtfs_precache_cached_bytes{dc=\"toro\"} 10",
        "dtfs_precache_series_active{dc=\"toro\"} 0",
        "dtfs_precache_capacity_limited{dc=\"toro\"} 1",
    ] {
        assert!(body.contains(want), "missing {want} in {body}");
    }
    // No engine: no precache lines at all.
    let (_, _, plain) = super::cluster::prometheus_metrics(State(h.state.clone())).await;
    assert!(!plain.contains("dtfs_precache"));
}

// ---- cache observability ---------------------------------------------------------

/// A warm disk on an unreachable node plus a cold-only node.
async fn seed_cache_nodes(h: &super::test_support::Harness) {
    use dtfs_common::types::{DiskInfo, DiskStatus, DiskTier};
    let disk = |tier| DiskInfo {
        disk_id: 7,
        uuid: String::new(),
        mount_path: String::new(),
        tier,
        total_bytes: 1000,
        used_bytes: 250,
        status: DiskStatus::Healthy,
    };
    let mut warm = data_node(now_ms());
    warm.hostname = "data-warm01".into();
    warm.grpc_addr = "http://127.0.0.1:1".into();
    warm.location = dtfs_common::topology::Location::new("lab", "toro", "r1", "data-warm01");
    warm.disks = vec![disk(DiskTier::Warm)];
    let mut cold = data_node(now_ms());
    cold.disks = vec![disk(DiskTier::Cold)];
    let mut sm = h.state.sm_data.write().await;
    sm.nodes.insert(4, warm);
    sm.nodes.insert(1, cold);
}

#[tokio::test]
async fn cache_status_as_follower_is_409_not_the_leader() {
    let h = super::test_support::harness().await;
    let e = get_cache_status(State(h.state.clone())).await.unwrap_err();
    assert_eq!(e.0, StatusCode::CONFLICT);
    assert!(e.1.0["error"].as_str().unwrap().contains("not the leader"), "{:?}", e.1.0);
}

/// Hot/warm disks only; an unreachable data node gives null plus `oldest_error`.
#[tokio::test]
async fn cache_status_shape_and_unreachable_node_gives_null_plus_error() {
    let h = super::test_support::leader_harness().await;
    seed_cache_nodes(&h).await;
    let j = get_cache_status(State(h.state.clone())).await.unwrap().0;
    let t = &h.state.config.tiering;
    assert_eq!(j["watermarks"], json!({"high_pct": t.cache_high_watermark_pct, "low_pct": t.cache_low_watermark_pct}));
    let disks = j["disks"].as_array().unwrap();
    assert_eq!(disks.len(), 1, "cold disk is not a cache disk: {j}");
    let d = &disks[0];
    assert_eq!((d["node_id"].as_u64(), d["disk_id"].as_u64(), d["tier"].as_str()), (Some(4), Some(7), Some("warm")));
    assert_eq!((d["hostname"].as_str(), d["dc"].as_str()), (Some("data-warm01"), Some("toro")));
    assert_eq!((d["total_bytes"].as_u64(), d["used_bytes"].as_u64(), d["used_pct"].as_f64()), (Some(1000), Some(250), Some(25.0)));
    assert!(d["oldest_last_read_ms"].is_null() && d["oldest_last_read_age_secs"].is_null(), "{d}");
    assert!(!d["oldest_error"].as_str().unwrap().is_empty(), "{d}");
    assert!(j["counters"]["removed"]["evict_capacity"].is_u64(), "{j}");
    assert!(j["counters"]["removed"]["retention"].is_u64() && j["counters"]["removed"]["idle_demote"].is_u64());
    assert!(j["link_budget"].is_array());
}

/// /metrics renders the cached disk view the status call stored, plus the counters.
#[tokio::test]
async fn metrics_include_cache_series() {
    let h = super::test_support::leader_harness().await;
    seed_cache_nodes(&h).await;
    let _ = get_cache_status(State(h.state.clone())).await.unwrap();
    let (_, _, body) = super::cluster::prometheus_metrics(State(h.state.clone())).await;
    for want in [
        "dtfs_cache_disk_used_bytes{node=\"data-warm01\",disk=\"7\",dc=\"toro\",tier=\"warm\"} 250",
        "dtfs_cache_disk_total_bytes{node=\"data-warm01\",disk=\"7\",dc=\"toro\",tier=\"warm\"} 1000",
        "dtfs_cache_removed_total{reason=\"idle_demote\"}",
        "dtfs_cache_fill_total",
        "dtfs_link_admitted_bytes_total",
        "dtfs_link_tokens_bytes",
    ] {
        assert!(body.contains(want), "missing {want}");
    }
}

/// A non-leader (e.g. a former leader) exports no disk gauges, though the
/// process-wide view still holds the last leader-time snapshot.
#[tokio::test]
async fn metrics_on_non_leader_export_no_disk_series() {
    let l = super::test_support::leader_harness().await;
    seed_cache_nodes(&l).await;
    let _ = get_cache_status(State(l.state.clone())).await.unwrap();
    assert!(!crate::cache_stats::STATS.view().1.is_empty(), "view populated");
    let f = super::test_support::harness().await;
    let (_, _, body) = super::cluster::prometheus_metrics(State(f.state.clone())).await;
    assert!(!body.contains("dtfs_cache_disk_used_bytes{"), "non-leader exported disk series");
    assert!(!body.contains("dtfs_cache_oldest_last_read_age_seconds{"));
    assert!(body.contains("dtfs_cache_removed_total{reason=\"idle_demote\"}"), "counters still exported");
}
