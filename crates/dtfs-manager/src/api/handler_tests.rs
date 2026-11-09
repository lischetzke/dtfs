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

#[tokio::test]
async fn join_request_rejections_are_audit_coalesced() {
    let h = harness().await;
    let j = crate::join::JoinHandler::new(
        "real-secret".into(),
        "c".into(),
        h.state.raft.clone(),
        h.state.audit_log.clone(),
    );
    let before = h.state.audit_log.len();
    for _ in 0..5 {
        let r = j.handle_join_request("n", &[0u8; 32], "coalesce-test-host");
        assert!(!r.accepted);
    }
    assert_eq!(h.state.audit_log.len() - before, 1, "5 identical rejections -> 1 audit row");
    j.handle_join_request("n", &[0u8; 32], "other-test-host");
    assert_eq!(h.state.audit_log.len() - before, 2, "distinct key still audited");
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
        ever_committed: None,
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
    h.state.audit_log.log("api", "scrub.deep.corrupt", "chunk-1", "fail");
    for i in 0..100 {
        h.state.audit_log.log("api", "config.update", &format!("v{i}"), "success");
    }
    let q = Query(AuditQuery { limit: Some(5), ..Default::default() });
    let rows = get_integrity_faults(State(h.state.clone()), q).await.0;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["action"], "scrub.deep.corrupt");
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
        join: h.state.join.clone(),
        scheduler: h.state.scheduler.clone(),
        io_rate: h.state.io_rate.clone(),
        memory: None,
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
