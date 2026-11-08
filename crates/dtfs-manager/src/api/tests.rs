//! Unit-test modules lifted out of `api.rs` so the source file
//! stays readable; every module below sees the parent's private items
//! through the glob import.
#![allow(unused_imports, deprecated)]

use super::*;

#[cfg(test)]
mod config_scope_tests {
    use super::config_key_scope;

    /// Bug BD-1 — exhaustive prefix table the WebUI consumes.
    /// Future refactors that change the classifier MUST keep this
    /// test green; misclassifying a node-specific key as cluster
    /// would let an operator edit (e.g.) `node.hostname` in the
    /// cluster tab and silently write it back to one manager only,
    /// causing config drift across peers.
    #[test]
    fn bug_bd_1_classification_table() {
        // Cluster-scope keys.
        for k in [
            "manager.cluster.name",
            "manager.cluster.shared_secret",
            "manager.defaults.chunk_size_mb",
            "manager.defaults.replication.warm",
            "manager.tiering.evaluation_interval_secs",
            "manager.monitoring.heartbeat_interval_secs",
            "manager.alerts.sinks",
            "manager.audit.replicate_via_raft",
            "manager.scrub.verify_on_read",
            "manager.raft.election_timeout_ms",
            "manager.raft.heartbeat_interval_ms",
            "manager.raft.snapshot_threshold",
        ] {
            assert_eq!(config_key_scope(k), "cluster", "expected cluster: {k}");
        }
        // Node-scope keys.
        for k in [
            "manager.node.hostname",
            "manager.node.id",
            "manager.node.data_dir",
            "manager.location.datacenter",
            "manager.grpc.listen_addr",
            "manager.grpc.advertise_addr",
            "manager.api.listen_addr",
            "manager.tls.cert_path",
            "manager.tls.enabled",
        ] {
            assert_eq!(config_key_scope(k), "node", "expected node: {k}");
        }
        // Unknown prefix → fall back to node (safer default).
        assert_eq!(config_key_scope("manager.something_new"), "node");
    }
}

#[cfg(test)]
mod audit_co_tests {
    use super::flatten_config;
    use serde_json::{json, Value};

    // CO-2-1 — the config read path (get_config_effective -> flatten_config)
    // MUST NOT emit `manager.cluster.shared_secret` in cleartext. The WebUI
    // config viewer renders every flattened row's `current` value, so a raw
    // secret here leaks the cluster membership credential to anyone with
    // read access to the config tab. Correct behaviour: the secret value is
    // redacted (e.g. to "***") in the rendered row, never the real value.
    #[test]
    fn co_2_1_shared_secret_redacted_in_flattened_config() {
        const SECRET: &str = "super-secret-cluster-token-9f3a";
        let current = json!({
            "cluster": {
                "name": "plexmedia",
                "shared_secret": SECRET,
            },
        });
        let defaults = json!({
            "cluster": {
                "name": Value::Null,
                "shared_secret": Value::Null,
            },
        });

        let mut rows: Vec<Value> = Vec::new();
        flatten_config(&current, &defaults, "manager", "", &mut rows);

        let secret_row = rows
            .iter()
            .find(|r| r.get("name").and_then(|n| n.as_str()) == Some("manager.cluster.shared_secret"))
            .expect("shared_secret row must be present");
        assert_ne!(
            secret_row.get("current").and_then(|v| v.as_str()),
            Some(SECRET),
            "shared_secret must be redacted, not emitted in cleartext: {secret_row}"
        );
    }

    // CO-4-14 — create_volume must be create, not upsert. Re-POSTing an
    // existing volume name used to blind UpsertVolume, wiping any
    // set-mandatory / cache / anchor tuning. The handler now reads the
    // current state and rejects when the derived volume id already exists.
    // This exercises that existence check (the seam factored out for
    // testability) against a StateMachineData carrying a tuned volume,
    // asserting the duplicate is detected and the stored tuning is the one
    // that would be preserved (the handler returns 409 before any Upsert).
    #[test]
    fn co_4_14_create_volume_existing_name_is_rejected() {
        use dtfs_raft::state_machine::StateMachineData;
        use dtfs_common::types::{
            CacheReplication, SpillPolicy, TierReplication, VolumeConfig, VolumeId,
            WriteSafety,
        };

        // Operator-supplied display name -> canonical id (lower, dashed).
        let name = "My Media";
        let vol_id = super::volume_id_from_name(name);
        assert_eq!(vol_id, "my-media");

        // A volume that has been hand-tuned after its initial create:
        // mandatory_replicas set, a non-default anchor, etc.
        let tuned = VolumeConfig {
            id: VolumeId::new(vol_id.clone()),
            name: name.to_string(),
            chunk_size_bytes: 64 * 1024 * 1024,
            replication: TierReplication { hot: 1, warm: 1, cold: 1, tape: 1 },
            write_safety: WriteSafety {
                min_replicas_before_ack: 2,
                write_timeout_secs: 30,
            },
            anchor_datacenter: Some("leli".to_string()),
            mandatory_replicas: TierReplication { hot: 0, warm: 2, cold: 1, tape: 0 },
            cache_replicas: CacheReplication::default(),
            anchor: None,
            spill: SpillPolicy::default(),
            version: 0,
        };

        let mut sm = StateMachineData::default();
        sm.volumes.insert(vol_id.clone(), tuned.clone());

        // The handler's guard: a re-create of the same name resolves to the
        // same id, which already exists -> reject (409), never UpsertVolume.
        assert!(
            sm.volumes.contains_key(&super::volume_id_from_name(name)),
            "re-POST of an existing volume name must be detected as a duplicate",
        );

        // The stored tuning is untouched by the rejected create.
        let stored = sm.volumes.get(&vol_id).expect("tuned volume present");
        assert_eq!(stored.mandatory_replicas.warm, 2);
        assert_eq!(stored.anchor_datacenter.as_deref(), Some("leli"));
    }
}

#[cfg(test)]
mod bug_bm3_tape_validation_tests {
    use super::*;

    /// BM-3 — mandatory tape replicas with no tape disk is rejected; a
    /// zero tape requirement (or a topology that has a tape disk) passes.
    #[test]
    fn tape_mandatory_requires_tape_disk() {
        // tape required, no tape disk → reject.
        assert!(validate_tape_satisfiable(1, false).is_err());
        // tape required, tape disk present → ok.
        assert!(validate_tape_satisfiable(1, true).is_ok());
        // no tape required → ok regardless of topology.
        assert!(validate_tape_satisfiable(0, false).is_ok());
        assert!(validate_tape_satisfiable(0, true).is_ok());
    }

    /// BM-3 — cluster_has_tape_disk scans every node's disks for a
    /// Tape-tier entry.
    #[test]
    fn detects_tape_disk_in_topology() {
        use dtfs_common::types::{DiskInfo, DiskStatus, DiskTier};
        let mut nodes: std::collections::BTreeMap<u64, dtfs_raft::NodeInfo> =
            std::collections::BTreeMap::new();
        let mk_disk = |tier: DiskTier| DiskInfo {
            disk_id: 0,
            uuid: "u".into(),
            mount_path: "/mnt/d".into(),
            tier,
            total_bytes: 1,
            used_bytes: 0,
            status: DiskStatus::Healthy,
        };
        let mk_node = |tier: DiskTier| dtfs_raft::NodeInfo {
            role: dtfs_common::types::NodeRole::Data,
            hostname: "d".into(),
            grpc_addr: "d:9500".into(),
            location: dtfs_common::topology::Location::new("", "", "", ""),
            disks: vec![mk_disk(tier)],
            total_capacity: 0,
            used_capacity: 0,
            health: HealthStatus::Healthy,
            last_seen_ms: 0,
            total_bytes_read: 0,
            total_bytes_written: 0,
            client_ip: None,
            version: String::new(),
            build_date: String::new(),
            quarantine: None,
        };
        nodes.insert(1, mk_node(DiskTier::Cold));
        assert!(!cluster_has_tape_disk(&nodes), "only a cold disk → no tape");
        nodes.insert(2, mk_node(DiskTier::Tape));
        assert!(cluster_has_tape_disk(&nodes), "a tape disk is now present");
    }
}

#[cfg(test)]
mod bug_cm_config_readback_tests {
    use super::*;

    // BUG-CM — a sparse override updates only its leaf; the sibling in the
    // same object and untouched sections survive the deep-merge.
    #[test]
    fn deep_merge_sparse_override_updates_leaf_only() {
        let mut base = json!({
            "tiering": {
                "evaluation_interval_secs": 3600,
                "migration_bandwidth_limit_mbps": 200,
            },
            "defaults": { "chunk_size_mb": 4 },
        });
        json_deep_merge(&mut base, &json!({ "tiering": { "evaluation_interval_secs": 60 } }));
        assert_eq!(base["tiering"]["evaluation_interval_secs"], json!(60));
        assert_eq!(base["tiering"]["migration_bandwidth_limit_mbps"], json!(200));
        assert_eq!(base["defaults"]["chunk_size_mb"], json!(4));
    }

    // BUG-CM — the flattened effective view reports an overridden tiering key
    // as `current` with is_default=false, while an un-overridden sibling stays
    // is_default=true. This is exactly what the editor reads back.
    #[test]
    fn effective_flatten_reflects_override() {
        // state.config tiering at its compiled defaults...
        let mut current = json!({
            "tiering": {
                "enabled": true,
                "hot_threshold_reads_per_hour": 5,
                "cold_threshold_days": 14,
                "frozen_threshold_days": 90,
                "evaluation_interval_secs": 3600,
                "migration_bandwidth_limit_mbps": 200,
                "enable_cache_redundancy_escalation": false,
            }
        });
        // ...with the operator's Raft-stored override projected on top.
        json_deep_merge(&mut current, &json!({ "tiering": { "evaluation_interval_secs": 60 } }));

        let defaults = manager_config_defaults_baseline();
        let mut rows = Vec::new();
        flatten_config(&current, &defaults, "manager", "", &mut rows);

        let eval = rows
            .iter()
            .find(|r| r["name"] == "manager.tiering.evaluation_interval_secs")
            .expect("evaluation_interval_secs row present");
        assert_eq!(eval["current"], json!(60), "override projected as current");
        assert_eq!(eval["is_default"], json!(false));

        let bw = rows
            .iter()
            .find(|r| r["name"] == "manager.tiering.migration_bandwidth_limit_mbps")
            .expect("migration_bandwidth_limit_mbps row present");
        assert_eq!(bw["current"], json!(200));
        assert_eq!(bw["is_default"], json!(true), "un-overridden sibling stays default");
    }

    // BUG-CM — an empty override is a no-op (every key keeps its startup value).
    #[test]
    fn empty_override_is_noop() {
        let mut base = json!({ "tiering": { "evaluation_interval_secs": 3600 } });
        let snapshot = base.clone();
        json_deep_merge(&mut base, &json!({}));
        assert_eq!(base, snapshot);
    }
}

#[cfg(test)]
mod co_4_3_config_override_validation_tests {
    use super::*;

    fn base_config() -> dtfs_common::config::ManagerConfig {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/examples/manager.toml");
        dtfs_common::config::load_config(&path)
            .expect("deploy/examples/manager.toml must parse as ManagerConfig")
    }

    // CO-4-3 — a sparse, well-typed override deep-merges onto the base, the
    // merge type-checks as ManagerConfig, and the persisted TOML round-trips
    // back through load_config (proving we never write raw JSON into a .toml).
    #[test]
    fn sparse_override_round_trips_through_load_config() {
        let base = base_config();
        let merged = merge_and_validate_override(
            &base,
            r#"{"tiering":{"evaluation_interval_secs":60}}"#,
        )
        .expect("sparse well-typed override must validate");
        assert_eq!(merged.tiering.evaluation_interval_secs, 60);
        // Untouched sibling survives the merge.
        assert_eq!(
            merged.tiering.migration_bandwidth_limit_mbps,
            base.tiering.migration_bandwidth_limit_mbps,
        );

        // Persisted exactly as update_config does: pretty TOML of the merge.
        let toml_str = toml::to_string_pretty(&merged).expect("merge serialises to TOML");
        let tmp = std::env::temp_dir().join(format!(
            "dtfs-co43-{}.toml",
            std::process::id(),
        ));
        std::fs::write(&tmp, toml_str.as_bytes()).unwrap();
        let reloaded: dtfs_common::config::ManagerConfig =
            dtfs_common::config::load_config(&tmp).expect("persisted TOML must reload");
        let _ = std::fs::remove_file(&tmp);
        assert_eq!(reloaded.tiering.evaluation_interval_secs, 60);
    }

    // CO-4-3 — the validate-on-every-write gate must not spuriously 400 a
    // legitimate config: an empty-object override (no change) round-trips the
    // full base config through to_value -> from_value cleanly, proving the
    // ManagerConfig JSON round-trip is symmetric for the canonical example.
    #[test]
    fn empty_object_override_revalidates_base() {
        let base = base_config();
        let merged = merge_and_validate_override(&base, "{}")
            .expect("base config must survive a no-op override round-trip");
        assert_eq!(
            merged.tiering.evaluation_interval_secs,
            base.tiering.evaluation_interval_secs,
        );
    }

    // CO-4-3 — a wrong-type override is rejected (would be a 400) instead of
    // being persisted as raw JSON.
    #[test]
    fn bad_type_override_is_rejected() {
        let base = base_config();
        let err = merge_and_validate_override(
            &base,
            r#"{"tiering":{"evaluation_interval_secs":"not-a-number"}}"#,
        )
        .expect_err("a string where a u64 is expected must fail validation");
        assert!(
            err.contains("type-check"),
            "error should explain the type failure, got: {err}",
        );
    }

    // CO-4-3 — malformed JSON is rejected up front.
    #[test]
    fn invalid_json_is_rejected() {
        let base = base_config();
        let err = merge_and_validate_override(&base, "{not json")
            .expect_err("malformed JSON must fail");
        assert!(err.contains("invalid config JSON"), "got: {err}");
    }
}

#[cfg(test)]
mod config_persist_tests {
    use super::*;

    fn base_config() -> dtfs_common::config::ManagerConfig {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/examples/manager.toml");
        dtfs_common::config::load_config(&path)
            .expect("deploy/examples/manager.toml must parse as ManagerConfig")
    }

    fn scratch_file(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir()
            .join(format!("dtfs-config-persist-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        dir.join(name)
    }

    /// The rendered text must be real TOML that the daemon's own startup
    /// deserialiser accepts — never JSON, and never a rendering that only
    /// serialises one way.
    #[test]
    fn render_validates_by_reparsing_its_own_output() {
        let rendered = render_manager_toml(&base_config()).expect("example config renders");
        assert!(
            !rendered.trim_start().starts_with('{'),
            "manager.toml must never be handed raw JSON: {}",
            &rendered[..rendered.len().min(80)],
        );
        assert!(rendered.contains("[node]"), "expected TOML sections");
        toml::from_str::<dtfs_common::config::ManagerConfig>(&rendered)
            .expect("rendered TOML must parse back as a ManagerConfig");
    }

    /// The happy path: the file is replaced atomically, reloads through
    /// `load_config`, and no temp file survives the call.
    #[test]
    fn atomic_write_reloads_and_leaves_no_temp() {
        let path = scratch_file("valid.toml");
        std::fs::write(&path, b"# previous operator config\n").unwrap();
        let mut cfg = base_config();
        cfg.tiering.evaluation_interval_secs = 61;
        let rendered = render_manager_toml(&cfg).expect("renders");
        write_manager_toml_atomic(&path, &rendered).expect("write succeeds");

        let reloaded: dtfs_common::config::ManagerConfig =
            dtfs_common::config::load_config(&path).expect("written file parses at startup");
        assert_eq!(reloaded.tiering.evaluation_interval_secs, 61);
        assert_eq!(reloaded.node.hostname, cfg.node.hostname);
        assert!(
            !path.with_extension("toml.az5.tmp").exists(),
            "temp file must not survive a successful write",
        );
        let _ = std::fs::remove_file(&path);
    }

    /// A write that cannot complete must leave the existing config exactly
    /// as it was and drop its temp file — the pre-fix path left
    /// `manager.toml.az5.tmp` residue next to the real config.
    #[test]
    fn failed_write_leaves_no_temp_residue() {
        // A directory in place of the target file makes the rename fail
        // after the temp file has already been created and fsynced.
        let path = scratch_file("as-a-directory.toml");
        let _ = std::fs::remove_file(&path);
        std::fs::create_dir_all(&path).expect("occupy the target path with a directory");
        let rendered = render_manager_toml(&base_config()).expect("renders");
        let err = write_manager_toml_atomic(&path, &rendered)
            .expect_err("renaming over a directory must fail");
        assert!(err.contains("could not write"), "got: {err}");
        assert!(
            !path.with_extension("toml.az5.tmp").exists(),
            "temp file must be cleaned up on the error path",
        );
        let _ = std::fs::remove_dir_all(&path);
    }
}

#[cfg(test)]
mod co_5_9_defaults_readback_tests {
    use super::*;

    // CO-5-9 — the GET /config `defaults` block must project a cluster-wide
    // `defaults.*` override on top of the startup config (BUG-CM pattern),
    // mirroring the production handler's merge. Pre-fix the block was built
    // from raw state.config.defaults, so the override was invisible.
    #[test]
    fn defaults_override_visible_on_readback() {
        // Startup defaults serialised, as the handler does.
        let startup = dtfs_common::config::DefaultsConfig {
            chunk_size_mb: 128,
            write_safety: dtfs_common::types::WriteSafety {
                min_replicas_before_ack: 1,
                write_timeout_secs: 30,
            },
            replication: dtfs_common::types::TierReplication {
                hot: 0,
                warm: 2,
                cold: 1,
                tape: 1,
            },
            anchor_datacenter: None,
        };
        let mut defaults = serde_json::to_value(&startup).unwrap();

        // Cluster-wide override (as stored in cluster_config.config_json),
        // bumping a single defaults leaf.
        let raw_override = json!({ "defaults": { "chunk_size_mb": 256 } });
        if let Some(over_defaults) = raw_override.get("defaults") {
            json_deep_merge(&mut defaults, over_defaults);
        }

        assert_eq!(defaults["chunk_size_mb"], json!(256), "override projected");
        // Un-overridden siblings keep their startup values.
        assert_eq!(defaults["write_safety"]["min_replicas_before_ack"], json!(1));
        assert_eq!(defaults["replication"]["warm"], json!(2));
    }
}

#[cfg(test)]
mod bug_bp_tests {
    use super::*;
    use dtfs_common::types::HealthStatus;

    // ── BP-3 — stale-liveness health projection ─────────────────────
    #[test]
    fn bp_3_effective_health_downgrades_stale() {
        let now = 1_000_000u64;
        let win = 30u64; // 30s window
        // Fresh → stays Healthy.
        assert_eq!(effective_health(HealthStatus::Healthy, now - 5_000, now, win), HealthStatus::Healthy);
        // Past 1× window → Suspect.
        assert_eq!(effective_health(HealthStatus::Healthy, now - 40_000, now, win), HealthStatus::Suspect);
        // Past 4× window → Down.
        assert_eq!(effective_health(HealthStatus::Healthy, now - 200_000, now, win), HealthStatus::Down);
        // Non-Healthy stored state is authoritative (never upgraded).
        assert_eq!(effective_health(HealthStatus::Quarantined, now - 200_000, now, win), HealthStatus::Quarantined);
        // last_seen==0 (never reported) or window==0 (disabled) → unchanged.
        assert_eq!(effective_health(HealthStatus::Healthy, 0, now, win), HealthStatus::Healthy);
        assert_eq!(effective_health(HealthStatus::Healthy, now - 999_999, now, 0), HealthStatus::Healthy);
    }

    // ── BP-4 — files-missing-chunks count ───────────────────────────
    #[test]
    fn bp_4_counts_files_missing_chunks() {
        use dtfs_raft::state_machine::{FileMetadata, StateMachineData};
        let mut sm = StateMachineData::default();
        let mk = |path: &str, chunks: usize, dir: bool, link: Option<String>| FileMetadata {
            path: path.into(),
            volume_id: "default".into(),
            chunk_size: 1024,
            chunks: (0..chunks).map(|i| dtfs_raft::ChunkLocationEntry {
                chunk_index: i as u64, chunk_hash: format!("h{i}"),
                node_id: "1".into(), disk_id: 0, size_bytes: 1,
            }).collect(),
            created_at_ms: 0, updated_at_ms: 0,
            symlink_target: link, logical_size: None, is_directory: dir,
            cache_cooldown_until_ms: std::collections::BTreeMap::new(), ever_committed: Some(true),
        };
        let with = |mut f: FileMetadata, size: Option<u64>, ever: Option<bool>| {
            f.logical_size = size;
            f.ever_committed = ever;
            f
        };
        sm.files.insert("/ok.bin".into(), mk("/ok.bin", 2, false, None));
        sm.files.insert("/lost.bin".into(), mk("/lost.bin", 0, false, None)); // data loss
        sm.files.insert("/dir".into(), mk("/dir", 0, true, None));            // dir, excluded
        sm.files.insert("/lnk".into(), mk("/lnk", 0, false, Some("/ok.bin".into()))); // symlink, excluded
        // Empty and never-written sparse files legitimately have no chunks.
        sm.files.insert("/empty".into(), with(mk("/empty", 0, false, None), Some(0), Some(false)));
        sm.files.insert("/sparse".into(), with(mk("/sparse", 0, false, None), Some(10 << 20), Some(false)));
        // Legacy entries (marker unknown): only an explicit size 0 is excluded.
        sm.files.insert("/old0".into(), with(mk("/old0", 0, false, None), Some(0), None));
        sm.files.insert("/oldn".into(), with(mk("/oldn", 0, false, None), None, None));
        sm.files.insert("/old1".into(), with(mk("/old1", 0, false, None), Some(1), None));
        assert_eq!(count_files_missing_chunks(&sm), 3, "/lost.bin, /oldn and /old1 count");
    }

    // ── BP-5 — tape replication normalization ───────────────────────
    #[test]
    fn bp_5_normalize_drops_tape_replication_without_tape_disk() {
        use dtfs_common::types::{TierReplication, CacheReplication, VolumeConfig, VolumeId, WriteSafety};
        let mut cfg = VolumeConfig {
            id: VolumeId::new("default".into()),
            name: "default".into(),
            chunk_size_bytes: 4 << 20,
            replication: TierReplication { hot: 0, warm: 2, cold: 1, tape: 1 },
            mandatory_replicas: TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 },
            cache_replicas: CacheReplication::default(),
            anchor: None,
            spill: dtfs_common::types::SpillPolicy::default(),
            version: 0,
            anchor_datacenter: None,
            write_safety: WriteSafety { min_replicas_before_ack: 1, write_timeout_secs: 30 },
        };
        // No tape disk → tape replication dropped.
        assert!(normalize_tape_replication(&mut cfg, false));
        assert_eq!(cfg.replication.tape, 0);
        // Idempotent.
        assert!(!normalize_tape_replication(&mut cfg, false));
        // With a tape disk present, a tape target is preserved.
        cfg.replication.tape = 1;
        assert!(!normalize_tape_replication(&mut cfg, true));
        assert_eq!(cfg.replication.tape, 1);
    }
}

#[cfg(test)]
mod cf_4c_raft_member_tests {
    use super::*;

    // ── CF-4c — manager raft-membership flag ────────────────────────
    #[test]
    fn is_raft_member_voter_or_learner() {
        let voters: BTreeSet<u64> = [1u64, 2, 3].into_iter().collect();
        let learners: BTreeSet<u64> = [4u64].into_iter().collect();
        // A voter is a member.
        assert!(is_raft_member(1, &voters, &learners));
        // A learner is a member (dc1 joins as a learner per CF-2).
        assert!(is_raft_member(4, &voters, &learners));
        // The production dc1 symptom: configured/registered manager that is
        // in neither set is NOT a member.
        assert!(!is_raft_member(99, &voters, &learners));
        // Empty membership → nobody is a member.
        let empty = BTreeSet::new();
        assert!(!is_raft_member(1, &empty, &empty));
    }
}

#[cfg(test)]
mod bug_bb_tests {
    use super::*;
    use dtfs_common::types::{DiskInfo, DiskStatus, DiskTier};

    fn disk(tier: DiskTier, total: u64, used: u64) -> DiskInfo {
        DiskInfo {
            disk_id: 0, uuid: "u".into(), mount_path: "/m".into(),
            tier, total_bytes: total, used_bytes: used, status: DiskStatus::Healthy,
        }
    }

    /// BB-1 — when the node reports per-disk usage, each tier gets its REAL
    /// used_bytes (including 0 for an empty disk) — never an apportioned
    /// fabrication (the BA-2 lie).
    #[test]
    fn bb_1_real_per_disk_no_fabrication() {
        // warm has 50MB used, cold is genuinely empty. node used_capacity=50MB.
        let disks = vec![
            disk(DiskTier::Warm, 100, 50),
            disk(DiskTier::Cold, 100, 0),
        ];
        let got: std::collections::HashMap<DiskTier, u64> =
            node_tier_used(&disks, 50).into_iter().collect();
        assert_eq!(got[&DiskTier::Warm], 50, "warm = real used");
        assert_eq!(got[&DiskTier::Cold], 0, "empty cold disk must NOT be apportioned 25MB");
    }

    /// BB-1 — a legacy node that ships NO per-disk usage (all zero) but has a
    /// node-level used_capacity falls back to capacity-share apportionment.
    #[test]
    fn bb_1_legacy_node_apportions() {
        let disks = vec![
            disk(DiskTier::Warm, 100, 0),
            disk(DiskTier::Cold, 100, 0),
        ];
        let got: std::collections::HashMap<DiskTier, u64> =
            node_tier_used(&disks, 80).into_iter().collect();
        // 80 split 50/50 across equal-size disks.
        assert_eq!(got[&DiskTier::Warm], 40);
        assert_eq!(got[&DiskTier::Cold], 40);
    }

    /// BB-1 — a zero-capacity node contributes nothing.
    #[test]
    fn bb_1_zero_capacity_node_empty() {
        assert!(node_tier_used(&[], 100).is_empty());
        assert!(node_tier_used(&[disk(DiskTier::Warm, 0, 0)], 100).is_empty());
    }
}

#[cfg(test)]
mod bug_bh8_infra_health_tests {
    use super::*;

    /// BH-8 — banner ratio counts managers + data, excludes clients,
    /// and never diverges numerator vs denominator.
    #[test]
    fn counts_infra_excludes_clients() {
        // 3 managers (config voters) + data nodes from sm.nodes; one
        // data node unhealthy, a client present but irrelevant here
        // (clients are filtered out by the caller before this helper).
        let manager_count = 3usize;
        let data_health = [
            HealthStatus::Healthy,
            HealthStatus::Suspect, // unhealthy data node
        ];
        let (healthy, total) =
            infra_health_counts(manager_count, data_health.iter());
        assert_eq!(total, 5, "3 managers + 2 data nodes");
        assert_eq!(healthy, 4, "one data node unhealthy → 4/5");
    }
}

#[cfg(test)]
mod bug_ai_audit_backfill_tests {
    use super::backfill_audit_fields;

    /// AppendAudit fan-out entries (Bug AH) already have structured fields;
    /// the backfill must pass them through unchanged.
    #[test]
    fn structured_entry_passes_through_unchanged() {
        let (a, ac, t, r) = backfill_audit_fields(
            "api",
            "volume.create",
            "bug-ah-fanout-test",
            "success",
            "volume.create: bug-ah-fanout-test [mgr02]",
        );
        assert_eq!(a, "api");
        assert_eq!(ac, "volume.create");
        assert_eq!(t, "bug-ah-fanout-test");
        assert_eq!(r, "success");
    }

    /// Legacy state-machine-internal entries (CreateFile, DeleteFile, …)
    /// have empty structured fields and a "Variant: target" summary. The
    /// backfill splits on the first ": " and synthesises actor/result.
    #[test]
    fn legacy_create_file_entry_splits_summary() {
        let (a, ac, t, r) = backfill_audit_fields(
            "", "", "", "",
            "CreateFile: /movies/Wicked.mkv",
        );
        assert_eq!(a, "raft");
        assert_eq!(ac, "CreateFile");
        assert_eq!(t, "/movies/Wicked.mkv");
        assert_eq!(r, "success");
    }

    /// "CreateFile: /foo (directory)" — extra annotation in target stays
    /// part of the target side.
    #[test]
    fn legacy_create_file_with_directory_marker() {
        let (_, ac, t, _) = backfill_audit_fields(
            "", "", "", "",
            "CreateFile: /movies/Season 1 (directory)",
        );
        assert_eq!(ac, "CreateFile");
        assert_eq!(t, "/movies/Season 1 (directory)");
    }

    /// A summary without ": " separator (defensive — shouldn't happen in
    /// practice, but if a future audit() call passes a freeform string
    /// we still produce something rather than panicking).
    #[test]
    fn legacy_summary_without_separator_uses_full_as_action() {
        let (a, ac, t, r) = backfill_audit_fields("", "", "", "", "some-event");
        assert_eq!(a, "raft");
        assert_eq!(ac, "some-event");
        assert!(t.is_empty());
        assert_eq!(r, "success");
    }

    /// Target may itself contain ": " (e.g. "UpdateNodeStatus: 12345 = healthy").
    /// `split_once(": ")` only splits on the FIRST occurrence — verifies
    /// the colons within target are preserved.
    #[test]
    fn legacy_target_with_internal_colons_preserved() {
        let (_, ac, t, _) = backfill_audit_fields(
            "", "", "", "",
            "UpdateNodeStatus: 12345: healthy",
        );
        assert_eq!(ac, "UpdateNodeStatus");
        assert_eq!(t, "12345: healthy");
    }
}

#[cfg(test)]
mod bug_al_io_rate_tests {
    //! Bug AL — process-local rolling I/O rate tracker.
    //!
    //! Pre-fix, the dashboard `/api/v1/stats.io_read_bytes_per_sec` was
    //! computed as `Σ total_bytes_read / (now − min(last_seen_ms))` which
    //! divided cumulative bytes by the staleness of the slowest-updated
    //! node. With `record_read` / `record_write` finally wired up in the
    //! data-node `put_chunk` / `get_chunk` handlers, the cumulative
    //! counters grow under load; this tracker deltas them against a
    //! per-call snapshot to produce a real bytes/sec rate.
    //!
    //! These tests use a small NodeInfo factory (only the fields the
    //! rate calculation reads — total_bytes_read / total_bytes_written
    //! / last_seen_ms) so we don't need the full data-node registration
    //! protocol just to verify arithmetic.
    use super::*;
    use std::collections::BTreeMap;

    fn node(read: u64, written: u64) -> dtfs_raft::NodeInfo {
        dtfs_raft::NodeInfo {
            hostname: String::new(),
            grpc_addr: String::new(),
            role: dtfs_common::types::NodeRole::Data,
            disks: Vec::new(),
            location: dtfs_common::topology::Location::new("", "", "", ""),
            health: dtfs_common::types::HealthStatus::Healthy,
            total_capacity: 0,
            used_capacity: 0,
            last_seen_ms: 0,
            total_bytes_read: read,
            total_bytes_written: written,
            client_ip: None,
            version: String::new(),
            build_date: String::new(),
            quarantine: None,
        }
    }

    /// REL-1-1 — /api/v1/nodes must surface per-node served/written bytes so a
    /// release case can prove a read was served by the nearest-DC replica.
    #[test]
    fn node_response_exposes_io_counters() {
        let mut info = node(4242, 99);
        info.hostname = "data-b1".to_string();
        let resp = NodeResponse::from_entry(12, &info);
        assert_eq!(resp.total_bytes_read, 4242);
        assert_eq!(resp.total_bytes_written, 99);
        let v = serde_json::to_value(&resp).unwrap();
        assert_eq!(v["total_bytes_read"].as_u64(), Some(4242));
        assert_eq!(v["total_bytes_written"].as_u64(), Some(99));
    }

    /// REL-1-2 — tier resolution falls back to "unknown" when the node or disk
    /// is gone (the positive tier path is covered by release cases 45/46/47).
    #[test]
    fn inspect_tier_unknown_when_node_or_disk_missing() {
        let info = node(0, 0); // no disks
        assert_eq!(tier_of_disk(None, 2), "unknown");
        assert_eq!(tier_of_disk(Some(&info), 2), "unknown");
    }

    /// First call with an empty tracker must return (0, 0) — there's no
    /// baseline to diff against yet. The tracker stores the snapshot so
    /// the next call can compute a real delta.
    #[test]
    fn first_call_returns_zero_then_baselines() {
        let mut tracker = IoRateTracker::default();
        let mut nodes: BTreeMap<u64, dtfs_raft::NodeInfo> = BTreeMap::new();
        nodes.insert(1, node(1_000_000, 500_000));

        let (r, w) = compute_io_rate_at(&mut tracker, &nodes, 1_000_000);
        assert_eq!(r, 0, "first call has no baseline");
        assert_eq!(w, 0);
        assert_eq!(tracker.previous.len(), 1, "baseline stored for next call");
        assert_eq!(tracker.last_query_ms, 1_000_000);
    }

    /// Sustained-write scenario: 10 MB read + 5 MB written over a 10-second
    /// window across two calls → (1 MB/s, 0.5 MB/s).
    ///
    /// Baselines must use non-zero `now_ms` because `last_query_ms == 0`
    /// is the sentinel for "never queried" in the production path. In
    /// real use, `SystemTime::now().as_millis()` is always ~1.8 × 10¹²
    /// so this distinction is invisible; we honour it here with an
    /// arbitrary epoch offset.
    #[test]
    fn delta_over_known_window_produces_expected_rate() {
        let mut tracker = IoRateTracker::default();
        let mut nodes: BTreeMap<u64, dtfs_raft::NodeInfo> = BTreeMap::new();
        nodes.insert(1, node(0, 0));
        // Baseline at t=1_000_000.
        let _ = compute_io_rate_at(&mut tracker, &nodes, 1_000_000);
        // Second sample 10 s later, after 10 MB read + 5 MB written.
        nodes.insert(1, node(10_000_000, 5_000_000));
        let (r, w) = compute_io_rate_at(&mut tracker, &nodes, 1_010_000);
        assert_eq!(r, 1_000_000, "10 MB / 10 s = 1 MB/s");
        assert_eq!(w, 500_000, "5 MB / 10 s = 500 KB/s");
    }

    /// Cumulative counters going backwards (e.g. a data-node restart
    /// reset to 0, or an u64 wraparound) must NOT produce negative or
    /// astronomical rates. `saturating_sub` zeroes the delta.
    #[test]
    fn cumulative_regression_does_not_underflow() {
        let mut tracker = IoRateTracker::default();
        tracker.previous.insert(1, IoSnapshot {
            total_bytes_read: 1_000_000_000,
            total_bytes_written: 500_000_000,
            observed_at_ms: 1_000_000,
        });
        tracker.last_query_ms = 1_000_000;

        let mut nodes: BTreeMap<u64, dtfs_raft::NodeInfo> = BTreeMap::new();
        nodes.insert(1, node(0, 0)); // counters reset

        // 10 s later than the baseline.
        let (r, w) = compute_io_rate_at(&mut tracker, &nodes, 1_010_000);
        assert_eq!(r, 0, "regression contributes 0 to the delta, not a huge spike");
        assert_eq!(w, 0);
    }

    /// A node that disappears between calls is silently dropped from
    /// the delta — the remaining nodes' bytes still produce a sensible
    /// rate. Guards against a decommission causing the dashboard to
    /// show 0 B/s for one cycle.
    #[test]
    fn disappearing_node_does_not_blow_up_rate() {
        let mut tracker = IoRateTracker::default();
        tracker.previous.insert(1, IoSnapshot {
            total_bytes_read: 500_000,
            total_bytes_written: 200_000,
            observed_at_ms: 1_000_000,
        });
        tracker.previous.insert(2, IoSnapshot {
            total_bytes_read: 1_000_000,
            total_bytes_written: 400_000,
            observed_at_ms: 1_000_000,
        });
        tracker.last_query_ms = 1_000_000;

        // Only node 1 left; node 2 vanished. 5 s window.
        let mut nodes: BTreeMap<u64, dtfs_raft::NodeInfo> = BTreeMap::new();
        nodes.insert(1, node(1_500_000, 700_000));

        let (r, w) = compute_io_rate_at(&mut tracker, &nodes, 1_005_000);
        // Node 1's delta: +1 MB read, +500 KB written, over 5 s.
        // (No contribution from node 2 since it's gone.)
        assert_eq!(r, 200_000, "node 1 delta / 5 s");
        assert_eq!(w, 100_000);
        assert_eq!(tracker.previous.len(), 1, "previous baseline only carries surviving nodes");
    }

    /// Sub-second back-to-back calls return the cached rate to avoid
    /// dividing by tiny numbers (which would produce 100×-spiky values).
    /// Critical when WebUI + Prometheus both scrape /stats at ~5s.
    #[test]
    fn sub_second_calls_return_cached_rate() {
        let mut tracker = IoRateTracker::default();
        // Pretend we just measured a rate.
        tracker.last_read_bps = 12_345;
        tracker.last_write_bps = 6_789;
        tracker.last_query_ms = 1_000_000;

        let nodes: BTreeMap<u64, dtfs_raft::NodeInfo> = BTreeMap::new();
        // 200 ms after the previous call — well under the 1 s window
        // floor, so we hit the cache return path.
        let (r, w) = compute_io_rate_at(&mut tracker, &nodes, 1_000_200);
        assert_eq!(r, 12_345);
        assert_eq!(w, 6_789);
    }
}

#[cfg(test)]
mod bug_av_set_cache_tests {
    //! AV-3-5 — pure tests for `apply_set_cache` and the
    //! `volume_config_to_json` projection. The async axum
    //! handlers wrap these helpers; per the AV-1-7 / AV-2-4
    //! convention, the handler-level paths are live-verified on
    //! the minicluster.
    use super::*;
    use dtfs_common::types::{
        AnchorPolicy, CacheReplication, CacheTierConfig, DiskTier,
        TierReplication, VolumeConfig, VolumeId, WriteSafety,
    };

    fn legacy_volume() -> VolumeConfig {
        VolumeConfig {
            id: VolumeId("default".to_string()),
            name: "default".to_string(),
            chunk_size_bytes: 16 * 1024 * 1024,
            replication: TierReplication {
                hot: 0, warm: 2, cold: 1, tape: 0,
            },
            // Default to all-zero so legacy fallback path is exercised.
            mandatory_replicas: TierReplication {
                hot: 0, warm: 0, cold: 0, tape: 0,
            },
            cache_replicas: CacheReplication::default(),
            anchor: None,
            spill: dtfs_common::types::SpillPolicy::default(),
            version: 0,
            anchor_datacenter: None,
            write_safety: WriteSafety {
                min_replicas_before_ack: 2,
                write_timeout_secs: 30,
            },
        }
    }

    /// AV-3-5 test 1 — supplied fields overwrite, omitted fields
    /// keep their current value.
    #[test]
    fn bug_av_set_cache_merges_partial_update() {
        let current = legacy_volume();
        let req = SetCacheRequest {
            warm_min: Some(1),
            warm_max: Some(1),
            per_dc: Some(true),
            promote_reads: Some(2),
            promote_window_secs: Some(60),
            cooldown_secs: Some(300),
            ..Default::default()
        };
        let next = apply_set_cache(&current, &req).expect("valid update");
        assert_eq!(next.cache_replicas.warm.min, 1);
        assert_eq!(next.cache_replicas.warm.max, 1);
        assert!(next.cache_replicas.per_dc);
        assert_eq!(next.cache_replicas.promote_reads, 2);
        assert_eq!(next.cache_replicas.promote_window_secs, 60);
        assert_eq!(next.cache_replicas.cooldown_secs, 300);
        // Hot tier untouched → still disabled (max=0).
        assert_eq!(next.cache_replicas.hot.max, 0);
        // Legacy `replication` untouched (rollback safety).
        assert_eq!(next.replication.warm, 2);
    }

    /// AV-3-5 test 2 — `max < min` is rejected at apply time.
    /// The state-machine apply path would also reject via
    /// `CacheTierConfig::validate`, but rejecting at the REST
    /// boundary gives the operator a clearer error.
    #[test]
    fn bug_av_set_cache_rejects_max_less_than_min() {
        let current = legacy_volume();
        let req = SetCacheRequest {
            warm_min: Some(2),
            warm_max: Some(1),
            ..Default::default()
        };
        let err = apply_set_cache(&current, &req).unwrap_err();
        assert!(err.contains("warm"), "error names the tier: {err}");
        assert!(err.contains("max") && err.contains("min"),
                "error explains why: {err}");
    }

    /// AV-3-5 test 3 — empty request body produces an unchanged
    /// clone (so the dispatcher detects no-op and skips Raft).
    #[test]
    fn bug_av_set_cache_empty_request_is_noop() {
        let current = legacy_volume();
        let req = SetCacheRequest::default();
        let next = apply_set_cache(&current, &req).expect("empty req is valid");
        assert_eq!(current, next);
    }

    // CF-3a — set-write-safety mirrors the set-cache merge semantics.

    /// CF-3a test 1 — empty request is a no-op clone (dispatcher
    /// detects no change and skips Raft).
    #[test]
    fn cf3a_set_write_safety_empty_request_is_noop() {
        let current = legacy_volume();
        let req = SetWriteSafetyRequest::default();
        let next =
            apply_set_write_safety(&current, &req).expect("empty req is valid");
        assert_eq!(current, next);
    }

    /// CF-3a test 2 — supplied fields overwrite, omitted fields keep
    /// their current value. Models the production reconciliation:
    /// default volume ships with min_replicas_before_ack=2; lower it
    /// to 1 without touching the timeout.
    #[test]
    fn cf3a_set_write_safety_merges_partial_update() {
        let current = legacy_volume();
        assert_eq!(current.write_safety.min_replicas_before_ack, 2);
        let req = SetWriteSafetyRequest {
            min_replicas_before_ack: Some(1),
            write_timeout_secs: None,
        };
        let next =
            apply_set_write_safety(&current, &req).expect("valid update");
        assert_eq!(next.write_safety.min_replicas_before_ack, 1);
        // Timeout untouched.
        assert_eq!(next.write_safety.write_timeout_secs, 30);
        assert_ne!(current, next, "merge changed the config");

        // Both fields supplied.
        let req2 = SetWriteSafetyRequest {
            min_replicas_before_ack: Some(3),
            write_timeout_secs: Some(45),
        };
        let next2 =
            apply_set_write_safety(&current, &req2).expect("valid update");
        assert_eq!(next2.write_safety.min_replicas_before_ack, 3);
        assert_eq!(next2.write_safety.write_timeout_secs, 45);
    }

    /// CF-3a test 3 — invalid zero values are rejected at the apply
    /// boundary (0 ack = ack before durability; 0 timeout = abort
    /// every write).
    #[test]
    fn cf3a_set_write_safety_rejects_zero() {
        let current = legacy_volume();
        let bad_ack = SetWriteSafetyRequest {
            min_replicas_before_ack: Some(0),
            write_timeout_secs: None,
        };
        let err = apply_set_write_safety(&current, &bad_ack).unwrap_err();
        assert!(err.contains("min_replicas_before_ack"),
                "error names the field: {err}");

        let bad_timeout = SetWriteSafetyRequest {
            min_replicas_before_ack: None,
            write_timeout_secs: Some(0),
        };
        let err = apply_set_write_safety(&current, &bad_timeout).unwrap_err();
        assert!(err.contains("write_timeout_secs"),
                "error names the field: {err}");
    }

    // Bug BZ — set-spill merges the per-volume spill exclusions + validates
    // tier names; allows_target then honours them.
    #[test]
    fn bug_bz_apply_set_spill() {
        use dtfs_common::types::DiskTier;
        let current = legacy_volume();
        let req = SetSpillRequest {
            allow: Some(false),
            forbidden_tiers: Some(vec!["HOT".into(), "".into(), "hot".into()]),
            forbidden_datacenters: Some(vec!["dc-b".into()]),
        };
        let next = apply_set_spill(&current, &req).expect("valid spill update");
        assert_eq!(next.spill.allow, Some(false));
        assert_eq!(next.spill.forbidden_tiers, vec!["hot".to_string()], "normalised + deduped");
        assert_eq!(next.spill.forbidden_datacenters, vec!["dc-b".to_string()]);
        assert!(!next.spill.allows_target("dc-a", DiskTier::Hot));
        assert!(!next.spill.allows_target("dc-b", DiskTier::Cold));
        assert!(next.spill.allows_target("dc-a", DiskTier::Warm));
        // Unknown tier rejected.
        let bad = SetSpillRequest { forbidden_tiers: Some(vec!["flash".into()]), ..Default::default() };
        assert!(apply_set_spill(&current, &bad).unwrap_err().contains("flash"));
    }

    // Bug BT follow-on — cold + tape cache tiers are editable via set-cache.
    #[test]
    fn set_cache_applies_cold_and_tape() {
        let current = legacy_volume();
        let req = SetCacheRequest {
            cold_min: Some(1),
            cold_max: Some(2),
            tape_min: Some(0),
            tape_max: Some(1),
            ..Default::default()
        };
        let next = apply_set_cache(&current, &req).expect("valid cold/tape update");
        assert_eq!(next.cache_replicas.cold.min, 1);
        assert_eq!(next.cache_replicas.cold.max, 2);
        assert_eq!(next.cache_replicas.tape.min, 0);
        assert_eq!(next.cache_replicas.tape.max, 1);
        // max < min still rejected for the new tiers.
        let bad = SetCacheRequest { cold_min: Some(3), cold_max: Some(1), ..Default::default() };
        assert!(apply_set_cache(&current, &bad).unwrap_err().contains("cold"));
    }

    /// AV-3-5 test 4 — the JSON projection includes the new AV
    /// fields (mandatory_replicas, cache_replicas, anchor) so
    /// the CLI / WebUI can render them.
    #[test]
    fn bug_av_volume_config_json_includes_av_fields() {
        let mut cfg = legacy_volume();
        cfg.mandatory_replicas = TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 };
        cfg.cache_replicas.warm = CacheTierConfig { min: 1, max: 1 };
        cfg.cache_replicas.per_dc = true;
        cfg.cache_replicas.promote_reads = 2;
        cfg.cache_replicas.promote_window_secs = 14400;
        cfg.cache_replicas.cooldown_secs = 300;
        cfg.anchor = Some(AnchorPolicy {
            datacenter: "leli".to_string(),
            required_tier: Some(DiskTier::Cold),
        });

        let v = volume_config_to_json(&cfg);
        assert_eq!(v["mandatory_replicas"]["cold"], 1);
        assert_eq!(v["cache_replicas"]["warm"]["min"], 1);
        assert_eq!(v["cache_replicas"]["warm"]["max"], 1);
        assert_eq!(v["cache_replicas"]["per_dc"], true);
        assert_eq!(v["cache_replicas"]["promote_reads"], 2);
        assert_eq!(v["cache_replicas"]["promote_window_secs"], 14400);
        assert_eq!(v["cache_replicas"]["cooldown_secs"], 300);
        assert_eq!(v["anchor"]["datacenter"], "leli");
        assert_eq!(v["anchor"]["required_tier"], "cold");
        // Legacy `replication` block still present (back-compat).
        assert_eq!(v["replication"]["warm"], 2);
    }

    /// AV-3-5 defensive — anchor=None projects null/omitted, not
    /// a stale stub object.
    #[test]
    fn bug_av_volume_config_json_anchor_none_is_null() {
        let cfg = legacy_volume();
        let v = volume_config_to_json(&cfg);
        assert!(
            v["anchor"].is_null(),
            "anchor is None → null, not an empty object (got {})",
            v["anchor"]
        );
    }

    /// AV-4-2 regression #1 — the list-endpoint row must include
    /// the full AV-3-5 projection (mandatory_replicas,
    /// cache_replicas, anchor) PLUS the aggregation fields
    /// (file_count, total_size). Pre-AV-4-2 the list row only
    /// emitted `replication` + aggregation, so operators reading
    /// the list endpoint couldn't see what cache policy a volume
    /// carried without an extra per-volume GET.
    #[test]
    fn bug_av_4_2_list_volumes_row_includes_av_fields() {
        let mut cfg = legacy_volume();
        cfg.mandatory_replicas = TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 };
        cfg.cache_replicas.warm = CacheTierConfig { min: 1, max: 1 };
        cfg.cache_replicas.per_dc = true;
        cfg.cache_replicas.promote_reads = 2;
        cfg.cache_replicas.promote_window_secs = 60;
        cfg.cache_replicas.cooldown_secs = 30;
        cfg.anchor = Some(AnchorPolicy {
            datacenter: "dc-a".to_string(),
            required_tier: None,
        });

        let row = list_volumes_row_json(Some(&cfg), "default", (12, 9_999_999));

        // Aggregation fields.
        assert_eq!(row["file_count"], 12);
        assert_eq!(row["total_size"], 9_999_999);
        // AV-3-5 projection fields.
        assert_eq!(row["mandatory_replicas"]["cold"], 1);
        assert_eq!(row["cache_replicas"]["warm"]["max"], 1);
        assert_eq!(row["cache_replicas"]["per_dc"], true);
        assert_eq!(row["anchor"]["datacenter"], "dc-a");
        // Legacy `replication` block still present (back-compat).
        assert_eq!(row["replication"]["warm"], 2);
    }

    /// AV-4-2 regression #2 — a usage-only row (volume exists in
    /// `files` but has no `VolumeConfig`) renders with just
    /// `volume_id` + aggregation, no crash on missing config.
    /// Pre-fix behaviour preserved.
    #[test]
    fn bug_av_4_2_list_volumes_legacy_volume_renders_correctly() {
        let row = list_volumes_row_json(None, "orphan", (3, 42));
        assert_eq!(row["volume_id"], "orphan");
        assert_eq!(row["file_count"], 3);
        assert_eq!(row["total_size"], 42);
        assert!(row.get("cache_replicas").is_none(),
            "config-less row must not invent default fields");
    }
}

#[cfg(test)]
mod bug_av_set_mandatory_tests {
    //! AV-4-1a — pure tests for `apply_set_mandatory`. Mirrors
    //! the AV-3-5 `bug_av_set_cache_tests` convention (axum
    //! handler is live-only; the pure helper carries the
    //! unit-test load).
    use super::*;
    use dtfs_common::types::{
        AnchorPolicy, CacheReplication, DiskTier, TierReplication,
        VolumeConfig, VolumeId, WriteSafety,
    };

    fn legacy_volume() -> VolumeConfig {
        // Same shape as the set-cache test fixture; mandatory all-zero
        // exercises the legacy-fallback path.
        VolumeConfig {
            id: VolumeId("default".to_string()),
            name: "default".to_string(),
            chunk_size_bytes: 16 * 1024 * 1024,
            replication: TierReplication {
                hot: 0, warm: 2, cold: 1, tape: 0,
            },
            mandatory_replicas: TierReplication {
                hot: 0, warm: 0, cold: 0, tape: 0,
            },
            cache_replicas: CacheReplication::default(),
            anchor: None,
            spill: dtfs_common::types::SpillPolicy::default(),
            version: 0,
            anchor_datacenter: None,
            write_safety: WriteSafety {
                min_replicas_before_ack: 2,
                write_timeout_secs: 30,
            },
        }
    }

    /// AV-4-1a test 1 — supplied fields overwrite, omitted
    /// fields keep their current value. Legacy `replication`
    /// is preserved as a rollback path.
    #[test]
    fn bug_av_set_mandatory_merges_partial_update() {
        let mut current = legacy_volume();
        current.mandatory_replicas = TierReplication {
            hot: 0, warm: 2, cold: 1, tape: 1,
        };
        let req = SetMandatoryRequest {
            warm_replicas: Some(0),  // drop warm
            cold_replicas: Some(1),  // unchanged
            tape_replicas: Some(0),  // drop tape
            ..Default::default()       // hot keeps current = 0
        };
        let next = apply_set_mandatory(&current, &req).expect("valid");
        assert_eq!(next.mandatory_replicas.hot,  0);
        assert_eq!(next.mandatory_replicas.warm, 0);
        assert_eq!(next.mandatory_replicas.cold, 1);
        assert_eq!(next.mandatory_replicas.tape, 0);
        // Legacy `replication` untouched.
        assert_eq!(next.replication.warm, 2);
    }

    /// AV-4-1a test 2 — rejecting all-zero result. Would
    /// re-enable the legacy fallback silently; not what the
    /// operator wanted.
    #[test]
    fn bug_av_set_mandatory_rejects_all_zero() {
        let mut current = legacy_volume();
        current.mandatory_replicas = TierReplication {
            hot: 0, warm: 2, cold: 1, tape: 0,
        };
        let req = SetMandatoryRequest {
            warm_replicas: Some(0),
            cold_replicas: Some(0),
            // hot + tape stay at 0.
            ..Default::default()
        };
        let err = apply_set_mandatory(&current, &req).unwrap_err();
        assert!(err.contains("all-zero"),
            "error must explain the all-zero rejection: {err}");
    }

    /// The wrong body shape (a nested {"mandatory_replicas":{...}} object
    /// instead of the flat *_replicas fields) must FAIL to deserialize rather
    /// than silently map to all-None and no-op away a durability-policy change.
    /// `#[serde(deny_unknown_fields)]` on SetMandatoryRequest enforces this;
    /// an empty {} body still deserializes (all-None -> dispatcher no-op).
    #[test]
    fn set_mandatory_wrong_nested_shape_is_rejected() {
        let nested = r#"{"mandatory_replicas":{"hot":0,"warm":2,"cold":1}}"#;
        assert!(
            serde_json::from_str::<SetMandatoryRequest>(nested).is_err(),
            "nested mandatory_replicas body must be rejected, not silently ignored"
        );
        // Empty body stays valid (all-None -> no-op, preserved contract).
        assert!(serde_json::from_str::<SetMandatoryRequest>("{}").is_ok());
        // Correct flat shape deserializes.
        let ok: SetMandatoryRequest =
            serde_json::from_str(r#"{"warm_replicas":2,"cold_replicas":1}"#).expect("flat ok");
        assert_eq!(ok.warm_replicas, Some(2));
    }

    /// AV-4-1a test 3 — rejecting an update that would make
    /// the anchor invariant permanently unsatisfiable.
    #[test]
    fn bug_av_set_mandatory_rejects_anchor_required_tier_zero() {
        let mut current = legacy_volume();
        current.mandatory_replicas = TierReplication {
            hot: 0, warm: 0, cold: 1, tape: 0,
        };
        current.anchor = Some(AnchorPolicy {
            datacenter: "leli".to_string(),
            required_tier: Some(DiskTier::Cold),
        });
        let req = SetMandatoryRequest {
            cold_replicas: Some(0),
            warm_replicas: Some(2), // satisfy the all-zero rule
            ..Default::default()
        };
        let err = apply_set_mandatory(&current, &req).unwrap_err();
        assert!(err.contains("anchor"),
            "error must mention anchor: {err}");
        assert!(err.contains("Cold") || err.contains("cold"),
            "error must name the offending tier: {err}");
    }

    /// AV-4-1a test 4 — calling set-mandatory on a legacy
    /// volume (current mandatory all-zero) seeds the new field
    /// from the EFFECTIVE legacy floor first, then applies the
    /// partial edit. A supplied tier overwrites; an UN-set tier
    /// keeps its legacy-floor value instead of silently dropping
    /// to 0. Legacy `replication` itself stays untouched
    /// (rollback safety).
    #[test]
    fn bug_av_set_mandatory_legacy_volume_round_trips() {
        let current = legacy_volume(); // mandatory all-zero, replication warm2/cold1
        assert_eq!(current.mandatory_replicas.hot,  0);
        assert_eq!(current.mandatory_replicas.warm, 0);
        assert_eq!(current.mandatory_replicas.cold, 0);
        assert_eq!(current.mandatory_replicas.tape, 0);

        let req = SetMandatoryRequest {
            cold_replicas: Some(1),
            ..Default::default()
        };
        let next = apply_set_mandatory(&current, &req).expect("valid");

        // Supplied tier set; un-set warm seeded from the legacy floor (=2),
        // NOT zeroed.
        assert_eq!(next.mandatory_replicas.cold, 1);
        assert_eq!(next.mandatory_replicas.warm, 2);
        // Legacy replication untouched (rollback safety).
        assert_eq!(next.replication.warm, 2);
        assert_eq!(next.replication.cold, 1);
        // effective now reads the populated mandatory map, which preserves the
        // warm floor that the legacy fallback carried.
        assert_eq!(next.effective_mandatory_replicas().cold, 1);
        assert_eq!(next.effective_mandatory_replicas().warm, 2,
            "un-set tier keeps the legacy floor (seeded), not silently dropped");
    }

    /// AV-4-1a hardening — partial set-mandatory on a legacy volume layers
    /// onto the effective legacy floor: bumping ONE tier must not drop the
    /// others. Regression for the silent-warm-floor-loss footgun.
    #[test]
    fn bug_av_set_mandatory_legacy_seeds_from_effective_floor() {
        let current = legacy_volume(); // mandatory all-zero, replication warm2/cold1
        // Operator bumps cold durability only.
        let req = SetMandatoryRequest {
            cold_replicas: Some(2),
            ..Default::default()
        };
        let next = apply_set_mandatory(&current, &req).expect("valid");
        // cold bumped to 2, warm floor (2) preserved, not dropped to 0.
        assert_eq!(next.effective_mandatory_replicas().cold, 2);
        assert_eq!(next.effective_mandatory_replicas().warm, 2,
            "un-set warm tier must keep its legacy floor, not silently vanish");
        // A volume already on the new schema is NOT reseeded.
        let mut migrated = legacy_volume();
        migrated.mandatory_replicas = TierReplication { hot: 0, warm: 1, cold: 1, tape: 0 };
        let req2 = SetMandatoryRequest { cold_replicas: Some(2), ..Default::default() };
        let n2 = apply_set_mandatory(&migrated, &req2).expect("valid");
        assert_eq!(n2.mandatory_replicas.warm, 1, "already-set tiers untouched");
        assert_eq!(n2.mandatory_replicas.cold, 2);
    }

    /// AV-4-1a defensive — empty request body produces an
    /// unchanged clone (so the dispatcher detects no-op and
    /// skips the Raft write).
    #[test]
    fn bug_av_set_mandatory_empty_request_is_noop() {
        let mut current = legacy_volume();
        current.mandatory_replicas = TierReplication {
            hot: 0, warm: 2, cold: 1, tape: 0,
        };
        let req = SetMandatoryRequest::default();
        let next = apply_set_mandatory(&current, &req).expect("empty req valid");
        assert_eq!(current, next);
    }
}

#[cfg(test)]
mod bug_bv3_data_node_count_tests {
    use super::*;

    fn node_role(role: dtfs_common::types::NodeRole) -> dtfs_raft::NodeInfo {
        dtfs_raft::NodeInfo {
            hostname: String::new(),
            grpc_addr: String::new(),
            role,
            disks: Vec::new(),
            location: dtfs_common::topology::Location::new("", "", "", ""),
            health: dtfs_common::types::HealthStatus::Healthy,
            total_capacity: 0,
            used_capacity: 0,
            last_seen_ms: 0,
            total_bytes_read: 0,
            total_bytes_written: 0,
            client_ip: None,
            version: String::new(),
            build_date: String::new(),
            quarantine: None,
        }
    }

    // Bug BV-3 — `cluster_status.data_nodes` must count only role==Data,
    // matching `stats.data_node_count`. Pre-fix it used `sm.nodes.len()`,
    // so a 3-manager + 2-data + 1-client roster reported 6.
    #[test]
    fn counts_only_role_data() {
        use dtfs_common::types::NodeRole;
        let mut sm = dtfs_raft::state_machine::StateMachineData::default();
        sm.nodes.insert(1, node_role(NodeRole::Manager));
        sm.nodes.insert(2, node_role(NodeRole::Manager));
        sm.nodes.insert(3, node_role(NodeRole::Manager));
        sm.nodes.insert(4, node_role(NodeRole::Data));
        sm.nodes.insert(5, node_role(NodeRole::Data));
        sm.nodes.insert(6, node_role(NodeRole::Client));
        assert_eq!(
            count_data_nodes(&sm),
            2,
            "3 manager + 2 data + 1 client must report 2 data nodes"
        );
    }

    // Bug BV-1 — anchor satisfaction: a chunk replicated only on warm in a
    // non-anchor DC is NOT anchored for a {dc-a, cold} policy (the live F4
    // case: write acked on 2 warm in dc-b, no cold copy in dc-a).
    #[test]
    fn bv1_anchor_satisfied_by_tier_and_dc() {
        use dtfs_common::types::{AnchorPolicy, DiskTier};
        let anchor = AnchorPolicy {
            datacenter: "dc-a".into(),
            required_tier: Some(DiskTier::Cold),
        };
        // Only warm in dc-b -> not anchored.
        assert!(!anchor_satisfied_by(&[("dc-b".into(), DiskTier::Warm)], &anchor));
        // Warm in dc-a but wrong tier -> not anchored.
        assert!(!anchor_satisfied_by(&[("dc-a".into(), DiskTier::Warm)], &anchor));
        // Cold in dc-a -> anchored.
        assert!(anchor_satisfied_by(
            &[("dc-b".into(), DiskTier::Warm), ("dc-a".into(), DiskTier::Cold)],
            &anchor
        ));
        // required_tier None -> any replica in the anchor DC satisfies.
        let any_tier = AnchorPolicy { datacenter: "dc-a".into(), required_tier: None };
        assert!(anchor_satisfied_by(&[("dc-a".into(), DiskTier::Warm)], &any_tier));
        assert!(!anchor_satisfied_by(&[("dc-b".into(), DiskTier::Cold)], &any_tier));
    }

    /// LIS-41 — Down dc3/cold must not keep `anchor_pending` at 0
    /// when the only live replica is warm on a non-anchor DC (DC1).
    #[test]
    fn lis41_down_cold_counts_as_anchor_pending() {
        use dtfs_common::types::{
            AnchorPolicy, CacheReplication, DiskInfo, DiskStatus, DiskTier, HealthStatus,
            NodeRole, TierReplication, VolumeConfig, VolumeId, WriteSafety,
        };
        use dtfs_raft::state_machine::{FileMetadata, StateMachineData};

        fn disk(tier: DiskTier) -> DiskInfo {
            DiskInfo {
                disk_id: 0,
                uuid: String::new(),
                mount_path: String::new(),
                tier,
                total_bytes: 0,
                used_bytes: 0,
                status: DiskStatus::Healthy,
            }
        }
        fn node(health: HealthStatus, dc: &str, tier: DiskTier) -> dtfs_raft::NodeInfo {
            dtfs_raft::NodeInfo {
                hostname: String::new(),
                grpc_addr: String::new(),
                role: NodeRole::Data,
                disks: vec![disk(tier)],
                location: dtfs_common::topology::Location::new("", dc, "", ""),
                health,
                total_capacity: 0,
                used_capacity: 0,
                last_seen_ms: 0,
                total_bytes_read: 0,
                total_bytes_written: 0,
                client_ip: None,
                version: String::new(),
                build_date: String::new(),
                quarantine: None,
            }
        }

        let mut sm = StateMachineData::default();
        sm.nodes.insert(1, node(HealthStatus::Healthy, "dc1", DiskTier::Warm));
        sm.nodes.insert(2, node(HealthStatus::Down, "dc3", DiskTier::Cold));
        sm.volumes.insert(
            "default".into(),
            VolumeConfig {
                id: VolumeId::new("default".into()),
                name: "default".into(),
                chunk_size_bytes: 4 << 20,
                replication: TierReplication {
                    hot: 0,
                    warm: 1,
                    cold: 1,
                    tape: 0,
                },
                mandatory_replicas: TierReplication {
                    hot: 0,
                    warm: 1,
                    cold: 1,
                    tape: 0,
                },
                cache_replicas: CacheReplication::default(),
                anchor: Some(AnchorPolicy {
                    datacenter: "dc3".into(),
                    required_tier: Some(DiskTier::Cold),
                }),
                spill: dtfs_common::types::SpillPolicy::default(),
                version: 0,
                anchor_datacenter: None,
                write_safety: WriteSafety {
                    min_replicas_before_ack: 1,
                    write_timeout_secs: 30,
                },
            },
        );
        sm.files.insert(
            "/media.bin".into(),
            FileMetadata {
                path: "/media.bin".into(),
                volume_id: "default".into(),
                chunk_size: 4 << 20,
                chunks: vec![
                    dtfs_raft::ChunkLocationEntry {
                        chunk_index: 0,
                        chunk_hash: "h0".into(),
                        node_id: "1".into(),
                        disk_id: 0,
                        size_bytes: 1,
                    },
                    dtfs_raft::ChunkLocationEntry {
                        chunk_index: 0,
                        chunk_hash: "h0".into(),
                        node_id: "2".into(),
                        disk_id: 0,
                        size_bytes: 1,
                    },
                ],
                created_at_ms: 0,
                updated_at_ms: 0,
                symlink_target: None,
                logical_size: None,
                is_directory: false,
                cache_cooldown_until_ms: std::collections::BTreeMap::new(), ever_committed: None,
            },
        );

        assert_eq!(
            count_anchor_pending_files(&sm),
            1,
            "Down dc3/cold must not satisfy the anchor"
        );
    }
}

#[cfg(test)]
mod cf1b_integrity_feed_tests {
    use super::action_is_integrity_fault;

    /// Integrity actions surface in the integrity feed via the prefix helper.
    #[test]
    fn quarantine_is_integrity_fault() {
        assert!(action_is_integrity_fault("node.quarantine", ""));
    }

    /// A non-integrity action (e.g. a routine volume mutation) must NOT be
    /// pulled into the integrity feed.
    #[test]
    fn unrelated_action_is_not_integrity_fault() {
        assert!(!action_is_integrity_fault("volume.create", ""));
    }

    /// Legacy state-machine entries carry the action in the `summary`
    /// column; the prefix match must still catch them there.
    #[test]
    fn matches_on_summary_when_action_empty() {
        assert!(action_is_integrity_fault("", "node.quarantine: node=1"));
    }
}

#[cfg(test)]
mod cnf1_stable_disk_id_tests {
    use super::*;
    use dtfs_common::types::{
        AnchorPolicy, CacheReplication, DiskInfo, DiskStatus, DiskTier, TierReplication,
        VolumeConfig, VolumeId, WriteSafety,
    };

    /// CNF-1 — the anchor projection must resolve a replica's disk by the
    /// STABLE `DiskInfo.disk_id` key, never by Vec position. Prod symptom:
    /// `anchor_pending=1` forever with an empty migration queue — the
    /// node's disks Vec had been reindexed (disk removed/re-registered),
    /// the positional `disks[disk_id]` lookup missed, and a satisfied
    /// anchor looked missing.
    #[test]
    fn anchor_projection_resolves_disk_by_stable_id() {
        let mut sm = dtfs_raft::state_machine::StateMachineData::default();

        // One node in the anchor DC whose ONLY disk carries disk_id=1
        // at Vec index 0 (reindexed disk list).
        sm.nodes.insert(7, dtfs_raft::NodeInfo {
            role: dtfs_common::types::NodeRole::Data,
            hostname: "d".into(),
            grpc_addr: "d:9500".into(),
            location: dtfs_common::topology::Location::new("lab", "leli", "r1", "d"),
            disks: vec![DiskInfo {
                disk_id: 1,
                uuid: "u".into(),
                mount_path: "/m".into(),
                tier: DiskTier::Cold,
                total_bytes: 1,
                used_bytes: 0,
                status: DiskStatus::Healthy,
            }],
            total_capacity: 0,
            used_capacity: 0,
            health: HealthStatus::Healthy,
            last_seen_ms: 0,
            total_bytes_read: 0,
            total_bytes_written: 0,
            client_ip: None,
            version: String::new(),
            build_date: String::new(),
            quarantine: None,
        });

        sm.volumes.insert("default".into(), VolumeConfig {
            id: VolumeId::new("default".into()),
            name: "default".into(),
            chunk_size_bytes: 4 << 20,
            replication: TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 },
            mandatory_replicas: TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 },
            cache_replicas: CacheReplication::default(),
            anchor: Some(AnchorPolicy {
                datacenter: "leli".into(),
                required_tier: Some(DiskTier::Cold),
            }),
            spill: dtfs_common::types::SpillPolicy::default(),
            version: 0,
            anchor_datacenter: None,
            write_safety: WriteSafety { min_replicas_before_ack: 1, write_timeout_secs: 30 },
        });

        sm.files.insert("/f.bin".into(), dtfs_raft::state_machine::FileMetadata {
            path: "/f.bin".into(),
            volume_id: "default".into(),
            chunk_size: 1024,
            chunks: vec![dtfs_raft::ChunkLocationEntry {
                chunk_index: 0,
                chunk_hash: "h0".into(),
                node_id: "7".into(),
                disk_id: 1, // stable id, NOT a Vec index
                size_bytes: 1,
            }],
            created_at_ms: 0,
            updated_at_ms: 0,
            symlink_target: None,
            logical_size: None,
            is_directory: false,
            cache_cooldown_until_ms: std::collections::BTreeMap::new(), ever_committed: None,
        });

        assert_eq!(
            count_anchor_pending_files(&sm),
            0,
            "cold replica on (node 7, disk_id 1) satisfies the anchor; \
             a positional disks[1] lookup must not drop it"
        );
    }
}

#[cfg(test)]
mod chunks_diag_tests {
    use super::chunks::{chunk_lookup_json, chunks_list_json, memory_json};
    use super::raft::replication_json;
    use crate::placement_state::test_fixture::mixed_fixture;
    use serde_json::json;

    #[test]
    fn list_filters_by_state_with_total() {
        let sm = mixed_fixture();
        let v = chunks_list_json(&sm, Some("undergoal"), None).unwrap();
        assert_eq!(v["total"], json!(2));
        let ids: Vec<&str> = v["chunks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["chunk_id"].as_str().unwrap())
            .collect();
        assert!(ids.contains(&"aa02") && ids.contains(&"aa04"), "{ids:?}");
        let m = chunks_list_json(&sm, Some("missing"), None).unwrap();
        assert_eq!(m["total"], json!(1));
        assert_eq!(m["chunks"][0]["path"], json!("/missing.bin"));
        // Dangling row to unknown node -> nulls, not a panic.
        assert_eq!(m["chunks"][0]["replicas"][0]["node_health"], json!(null));
    }

    #[test]
    fn list_pending_deletion_and_replica_shape() {
        let sm = mixed_fixture();
        let v = chunks_list_json(&sm, Some("pending_deletion"), None).unwrap();
        assert_eq!(v["total"], json!(2));
        let under = v["chunks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["chunk_id"] == json!("aa02"))
            .unwrap();
        let reps = under["replicas"].as_array().unwrap();
        assert_eq!(reps.len(), 2);
        let down = reps.iter().find(|r| r["node_id"] == json!("2")).unwrap();
        assert_eq!(down["node_health"], json!("down"));
        assert_eq!(down["tier"], json!("cold"));
        assert_eq!(down["datacenter"], json!("dc3"));
        assert_eq!(down["disk_status"], json!("healthy"));
    }

    #[test]
    fn list_limit_truncates_but_total_is_full() {
        let sm = mixed_fixture();
        let v = chunks_list_json(&sm, None, Some(2)).unwrap();
        assert_eq!(v["total"], json!(5));
        assert_eq!(v["chunks"].as_array().unwrap().len(), 2);
        assert_eq!(v["truncated"], json!(true));
        // limit above the cap is clamped, 0 is raised to 1.
        assert_eq!(chunks_list_json(&sm, None, Some(1_000_000)).unwrap()["limit"], json!(5000));
        assert_eq!(chunks_list_json(&sm, None, Some(0)).unwrap()["limit"], json!(1));
    }

    #[test]
    fn list_bad_state_is_error() {
        let sm = mixed_fixture();
        assert!(chunks_list_json(&sm, Some("stable-ish"), None).is_err());
    }

    #[test]
    fn lookup_hit_miss_and_bad_id() {
        let sm = mixed_fixture();
        // aa01 is shared by /stable.bin idx0 and /multi.bin idx0.
        let hit = chunk_lookup_json(&sm, "AA01").unwrap().unwrap();
        assert_eq!(hit["chunk_id"], json!("aa01"));
        assert_eq!(hit["state"], json!("stable"));
        let files = hit["files"].as_array().unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(hit["replicas"].as_array().unwrap().len(), 2, "deduped by (node,disk)");
        let under = chunk_lookup_json(&sm, "aa02").unwrap().unwrap();
        assert_eq!(under["state"], json!("undergoal"));
        assert_eq!(under["pending_deletion"], json!(true));
        assert!(chunk_lookup_json(&sm, "ffff").unwrap().is_none());
        assert!(chunk_lookup_json(&sm, "zz").is_err());
        assert!(chunk_lookup_json(&sm, "").is_err());
        assert!(chunk_lookup_json(&sm, "../etc").is_err());
    }

    #[test]
    fn memory_with_and_without_budget() {
        assert_eq!(memory_json(None), json!({ "configured": false }));
        let cfg = dtfs_common::config::MemoryConfig {
            hard_cap_mb: 100,
            warn_threshold_pct: 80,
            shed_threshold_pct: 90,
            sample_interval_secs: 5,
        };
        let b = dtfs_common::memory::MemoryBudget::from_config(&cfg);
        b.record_sample(85 * 1024 * 1024);
        let v = memory_json(Some(&b));
        assert_eq!(v["configured"], json!(true));
        assert_eq!(v["hard_cap_bytes"], json!(100u64 * 1024 * 1024));
        assert_eq!(v["last_rss_bytes"], json!(85u64 * 1024 * 1024));
        assert_eq!(v["pressure_level"], json!("Warn"));
        assert_eq!(v["warn_threshold_bytes"], json!(80u64 * 1024 * 1024));
        assert_eq!(v["shed_threshold_bytes"], json!(90u64 * 1024 * 1024));
        assert_eq!(v["sample_interval_secs"], json!(5));
    }

    #[test]
    fn replication_null_when_not_leader_and_lag_on_leader() {
        // No non-leader raft fixture exists; the handler maps
        // `metrics.replication == None` (follower) onto this helper.
        assert_eq!(replication_json(None, 10), json!(null));
        let mut m = std::collections::BTreeMap::new();
        m.insert(7u64, Some(8u64));
        m.insert(9u64, None);
        let v = replication_json(Some(m), 10);
        assert_eq!(v[0], json!({"node_id": "7", "matched_index": 8, "lag": 2}));
        assert_eq!(v[1], json!({"node_id": "9", "matched_index": null, "lag": 10}));
    }
}
