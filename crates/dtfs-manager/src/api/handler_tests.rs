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
