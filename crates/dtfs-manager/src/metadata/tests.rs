//! Unit-test modules lifted out of `metadata.rs` so the source file
//! stays readable; every module below sees the parent's private items
//! through the glob import.
#![allow(unused_imports, deprecated)]

use super::*;

#[cfg(test)]
mod mgr_11_forward_allow_list_tests {
    use super::forwardable;
    use dtfs_raft::ClientRequest;

    #[test]
    fn update_config_is_not_forwardable() {
        assert!(!forwardable(&ClientRequest::UpdateConfig {
            config_json: "{}".into(),
            version: 0,
            expected_version: u64::MAX,
        }));
    }

    #[test]
    fn update_node_usage_is_forwardable() {
        assert!(forwardable(&ClientRequest::UpdateNodeUsage {
            node_id: 1,
            used_capacity: 0,
            chunk_count: 0,
            total_bytes_read: 0,
            total_bytes_written: 0,
            observed_at_ms: 0,
        }));
    }

    #[test]
    fn update_node_status_is_forwardable() {
        assert!(forwardable(&ClientRequest::UpdateNodeStatus {
            node_id: 1,
            status: "Healthy".into(),
        }));
    }
}

#[cfg(test)]
mod mgr_12_forward_result_tests {
    use super::forward_result;
    use dtfs_proto::dtfs::manager::ForwardWriteResponse;

    #[test]
    fn success_response_is_ok() {
        let resp = ForwardWriteResponse {
            success: true,
            response_json: Vec::new(),
            error: String::new(),
        };
        assert!(forward_result(&resp).is_ok());
    }

    #[test]
    fn rejected_response_is_err() {
        let resp = ForwardWriteResponse {
            success: false,
            response_json: Vec::new(),
            error: "forwarded write variant not allowed (MGR-11)".into(),
        };
        let err = forward_result(&resp).unwrap_err();
        assert_eq!(err, "forwarded write variant not allowed (MGR-11)");
    }
}

#[cfg(test)]
mod co_2_2_forward_auth_tests {
    use super::forward_secret_ok;

    #[test]
    fn forward_secret_back_compat_empty_configured_accepts_anything() {
        // Plaintext cluster: leader has no secret configured -> accept all,
        // regardless of what (if anything) the forwarder presents.
        assert!(forward_secret_ok("", ""));
        assert!(forward_secret_ok("", "anything"));
    }

    #[test]
    fn forward_secret_requires_match_when_configured() {
        assert!(forward_secret_ok("s3cr3t", "s3cr3t"));
        assert!(!forward_secret_ok("s3cr3t", "wrong"));
        // Configured-but-missing (empty presented) is rejected.
        assert!(!forward_secret_ok("s3cr3t", ""));
    }
}

#[cfg(test)]
mod lis56_mgr_rpc_auth_tests {
    use super::require_mgr_rpc_auth;
    use dtfs_common::crypto::{
        stamp_mgr_rpc_auth, DATA_RPC_AUTH_MAC, DATA_RPC_AUTH_TS,
    };
    use tonic::metadata::MetadataMap;

    fn meta_with(ts: &str, mac: &str) -> MetadataMap {
        let mut m = MetadataMap::new();
        m.insert(DATA_RPC_AUTH_TS, ts.parse().unwrap());
        m.insert(DATA_RPC_AUTH_MAC, mac.parse().unwrap());
        m
    }

    #[test]
    fn empty_secret_allows_without_headers() {
        assert!(require_mgr_rpc_auth("", "DeleteFile", "/x", &MetadataMap::new()).is_ok());
    }

    #[test]
    fn missing_headers_denied_when_secret_set() {
        let err = require_mgr_rpc_auth("s3cret", "DeleteFile", "/x", &MetadataMap::new())
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(err.message().contains("missing x-dtfs-rpc-ts"));
    }

    #[test]
    fn valid_stamp_accepted() {
        let secret = "s3cret";
        let (ts, mac) = stamp_mgr_rpc_auth(secret, "DeleteFile", "/media/a").unwrap();
        let meta = meta_with(&ts, &mac);
        assert!(require_mgr_rpc_auth(secret, "DeleteFile", "/media/a", &meta).is_ok());
    }

    #[test]
    fn wrong_binding_denied() {
        let secret = "s3cret";
        let (ts, mac) = stamp_mgr_rpc_auth(secret, "DeleteFile", "/a").unwrap();
        let meta = meta_with(&ts, &mac);
        let err = require_mgr_rpc_auth(secret, "DeleteFile", "/b", &meta).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    // CR-5-1 (PROTO-1) — Heartbeat and ReportChunkStatus were the only two
    // mutating ManagerService RPCs NOT gated by LIS-56: any node id could
    // send them (poison node status / drop replica entries) without the
    // cluster secret.
    #[test]
    fn heartbeat_empty_secret_allows_without_headers() {
        assert!(require_mgr_rpc_auth("", "Heartbeat", "42", &MetadataMap::new()).is_ok());
    }

    #[test]
    fn heartbeat_missing_headers_denied_when_secret_set() {
        let err = require_mgr_rpc_auth("s3cret", "Heartbeat", "42", &MetadataMap::new())
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn heartbeat_valid_stamp_accepted() {
        let secret = "s3cret";
        let (ts, mac) = stamp_mgr_rpc_auth(secret, "Heartbeat", "42").unwrap();
        let meta = meta_with(&ts, &mac);
        assert!(require_mgr_rpc_auth(secret, "Heartbeat", "42", &meta).is_ok());
    }

    #[test]
    fn report_chunk_status_empty_secret_allows_without_headers() {
        assert!(
            require_mgr_rpc_auth("", "ReportChunkStatus", "42", &MetadataMap::new()).is_ok()
        );
    }

    #[test]
    fn report_chunk_status_missing_headers_denied_when_secret_set() {
        let err = require_mgr_rpc_auth("s3cret", "ReportChunkStatus", "42", &MetadataMap::new())
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn report_chunk_status_valid_stamp_accepted() {
        let secret = "s3cret";
        let (ts, mac) = stamp_mgr_rpc_auth(secret, "ReportChunkStatus", "42").unwrap();
        let meta = meta_with(&ts, &mac);
        assert!(require_mgr_rpc_auth(secret, "ReportChunkStatus", "42", &meta).is_ok());
    }

    #[test]
    fn heartbeat_mac_not_valid_for_report_chunk_status() {
        // Method name is part of the MAC input, so a Heartbeat-stamped
        // header must not authenticate a ReportChunkStatus call.
        let secret = "s3cret";
        let (ts, mac) = stamp_mgr_rpc_auth(secret, "Heartbeat", "42").unwrap();
        let meta = meta_with(&ts, &mac);
        let err =
            require_mgr_rpc_auth(secret, "ReportChunkStatus", "42", &meta).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }
}

#[cfg(test)]
mod bug_ch_forward_classify_tests {
    use super::is_forward_to_leader_err;

    // Both error shapes seen in the wild must classify as forwardable;
    // anything else must surface as a real error.
    #[test]
    fn classifies_forward_errors() {
        assert!(is_forward_to_leader_err(
            "has to forward request to: Some(123), Some(DtfsNode { .. })"
        ));
        assert!(is_forward_to_leader_err("ForwardToLeader(..)"));
        assert!(!is_forward_to_leader_err("storage error: disk full"));
    }
}

#[cfg(test)]
#[allow(unused_imports)]
mod bug_z_tier_tests {
    use super::*;
    use dtfs_common::types::{DiskTier, TierReplication};

    /// Bug Z — Plexmedia config (hot=0, warm=2, cold=1) picks Warm.
    #[test]
    fn bug_z_plexmedia_picks_warm() {
        let rep = TierReplication {
            hot: 0,
            warm: 2,
            cold: 1,
            tape: 0,
        };
        assert_eq!(pick_effective_tier(&rep), (DiskTier::Warm, 2));
    }

    /// Bug Z — legacy hot-only config still picks Hot.
    #[test]
    fn bug_z_legacy_hot_picks_hot() {
        let rep = TierReplication {
            hot: 3,
            warm: 0,
            cold: 0,
            tape: 0,
        };
        assert_eq!(pick_effective_tier(&rep), (DiskTier::Hot, 3));
    }

    /// Bug Z — cold-only deep-archive config picks Cold.
    #[test]
    fn bug_z_cold_only_picks_cold() {
        let rep = TierReplication {
            hot: 0,
            warm: 0,
            cold: 2,
            tape: 0,
        };
        assert_eq!(pick_effective_tier(&rep), (DiskTier::Cold, 2));
    }
}

#[cfg(test)]
mod bug_av_preferred_tier_tests {
    //! AV-3-2 — pure tests for `resolve_preferred_tier`. Verifies
    //! the hint resolution gates: honour-when-available, fall-
    //! back-when-not-available, empty-hint uses pick_effective_tier.
    use super::*;
    use dtfs_common::types::{DiskTier, TierReplication};

    fn plex_rep() -> TierReplication {
        // Plexmedia-shape: cold mandatory, warm/hot 0.
        TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 }
    }

    /// AV-3-2 test 1 — hint="warm" + a healthy warm node exists →
    /// honour the hint, return `(Warm, 1)`.
    #[test]
    fn bug_av_allocate_chunks_honours_preferred_tier() {
        let avail = vec![DiskTier::Cold, DiskTier::Warm];
        let (tier, rf) = resolve_preferred_tier("warm", &plex_rep(), &avail);
        assert_eq!(tier, DiskTier::Warm);
        assert_eq!(rf, 1, "first-chunk on cache tier places exactly 1 replica");
    }

    /// AV-3-2 test 2 — hint="warm" but no healthy warm node →
    /// fall back to `pick_effective_tier` (Cold for Plexmedia).
    #[test]
    fn bug_av_allocate_chunks_falls_back_when_tier_full() {
        // "tier full" in the operator sense = no node has that tier
        // available (the placement-engine handles per-disk fullness;
        // resolver only sees tier availability).
        let avail = vec![DiskTier::Cold];
        let (tier, rf) = resolve_preferred_tier("warm", &plex_rep(), &avail);
        assert_eq!(tier, DiskTier::Cold);
        assert_eq!(rf, 1, "Plexmedia cold rf=1");
    }

    /// AV-3-2 test 3 — empty hint → legacy behaviour
    /// (pick_effective_tier).
    #[test]
    fn bug_av_allocate_chunks_empty_hint_uses_default() {
        let avail = vec![DiskTier::Cold, DiskTier::Warm, DiskTier::Hot];
        let (tier, rf) = resolve_preferred_tier("", &plex_rep(), &avail);
        assert_eq!(tier, DiskTier::Cold,
            "Plexmedia (hot=0, warm=0, cold=1) picks Cold by default");
        assert_eq!(rf, 1);
    }

    /// AV-3-2 defensive — unknown hint string falls back to
    /// volume default (forward-compat: a future client adds
    /// a new tier name).
    #[test]
    fn bug_av_allocate_chunks_unknown_hint_falls_back() {
        let avail = vec![DiskTier::Cold, DiskTier::Warm];
        let (tier, rf) = resolve_preferred_tier("nearest-warm", &plex_rep(), &avail);
        // "nearest-warm" is what `fuse.toml` carries but the proto
        // protocol uses the bare tier names — sending the raw config
        // string here is treated as unknown.
        assert_eq!(tier, DiskTier::Cold, "unknown hint → pick_effective_tier");
        assert_eq!(rf, 1);
    }

    /// AV-3-2 defensive — parse_preferred_tier_hint covers every
    /// recognised tier + empty + unknown.
    #[test]
    fn bug_av_parse_preferred_tier_hint_coverage() {
        assert_eq!(parse_preferred_tier_hint(""), None);
        assert_eq!(parse_preferred_tier_hint("hot"), Some(DiskTier::Hot));
        assert_eq!(parse_preferred_tier_hint("warm"), Some(DiskTier::Warm));
        assert_eq!(parse_preferred_tier_hint("cold"), Some(DiskTier::Cold));
        assert_eq!(parse_preferred_tier_hint("tape"), Some(DiskTier::Tape));
        assert_eq!(parse_preferred_tier_hint("banana"), None);
        assert_eq!(parse_preferred_tier_hint("Hot"), None,
            "case-sensitive: protocol values are lowercase");
    }

    // ─────────────────────────────────────────────────────────────
    // Bug AV (AV-4-1c) — placement uses mandatory, not legacy
    // ─────────────────────────────────────────────────────────────
    //
    // The bug that prompted these tests: a volume with
    // `mandatory_replicas.cold=1` (operator just ran
    // `volumes set-mandatory --cold-replicas 1 --warm-replicas 0
    // --tape-replicas 0`) still had the pre-AV-migration legacy
    // `replication = {warm:2, cold:1, tape:1}` field set. The
    // `allocate_chunks` handler passed legacy `replication` to
    // `resolve_preferred_tier`, which (per `pick_effective_tier`)
    // returned (Warm, 2). New writes landed on warm, saturating
    // the `cache_replicas.warm.max=1` cap on first write and
    // permanently blocking the cache-promotion path (the planner
    // saw 2 warm replicas already > target of 1, so
    // `saturating_sub` returned 0 → no promotion ever fired).
    //
    // The fix wires `allocate_chunks` to consult
    // `effective_mandatory_replicas()` via the
    // `resolve_volume_placement` helper. These tests pin that
    // contract so a future refactor can't silently regress it.

    fn vol_with_split_mandatory_and_legacy() -> dtfs_common::types::VolumeConfig {
        use dtfs_common::types::*;
        VolumeConfig {
            id: VolumeId::new("default".to_string()),
            name: "default".to_string(),
            chunk_size_bytes: 4 * 1024 * 1024,
            // Legacy: warm=2, cold=1 — the pre-AV state.
            replication: TierReplication { hot: 0, warm: 2, cold: 1, tape: 1 },
            // Mandatory: cold-only — the post-set-mandatory state.
            mandatory_replicas: TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 },
            cache_replicas: CacheReplication {
                hot: CacheTierConfig::default(),
                warm: CacheTierConfig { min: 1, max: 1 },
                cold: CacheTierConfig::default(),
                tape: CacheTierConfig::default(),
                per_dc: true,
                promote_reads: 2,
                promote_window_secs: 60,
                cooldown_secs: 30,
            },
            anchor: None,
            schedules: dtfs_common::types::VolumeSchedules::default(),
            spill: dtfs_common::types::SpillPolicy::default(),
            anchor_datacenter: Some("dc-a".to_string()),
            write_safety: WriteSafety {
                min_replicas_before_ack: 1,
                write_timeout_secs: 30,
            },
        }
    }

    /// AV-4-1c regression #1: a volume with mandatory=cold-only AND
    /// legacy=warm-heavy MUST place new writes on COLD when the
    /// client hint is empty (default first-write-tier). Pre-fix
    /// the handler passed legacy `replication` to
    /// `resolve_preferred_tier` and writes landed on warm,
    /// poisoning the cache-promotion lifecycle.
    #[test]
    fn bug_av_4_1c_placement_uses_mandatory_not_legacy_replication() {
        let v = vol_with_split_mandatory_and_legacy();
        let available = vec![DiskTier::Hot, DiskTier::Warm, DiskTier::Cold];
        let (tier, rf) = resolve_volume_placement(&v, "", &available);
        assert_eq!(
            tier, DiskTier::Cold,
            "must place on Cold (the mandatory tier), not Warm (the legacy tier); \
             writes-land-on-cache-tier silently broke cache promotion"
        );
        assert_eq!(rf, 1, "rf must come from mandatory.cold=1, not legacy.warm=2");
    }

    /// AV-4-1c regression #2: when the client explicitly requests
    /// `preferred_tier="warm"` (FUSE `first_write_tier =
    /// "nearest-warm"`), the placement engine honours the hint
    /// even though mandatory says cold-only. Mandatory is the
    /// fall-back floor, not a hard barrier — the hint takes
    /// priority when the operator has explicitly opted in.
    #[test]
    fn bug_av_4_1c_explicit_warm_hint_still_wins_over_mandatory() {
        let v = vol_with_split_mandatory_and_legacy();
        let available = vec![DiskTier::Warm, DiskTier::Cold];
        let (tier, rf) = resolve_volume_placement(&v, "warm", &available);
        assert_eq!(tier, DiskTier::Warm, "explicit hint honoured");
        assert_eq!(rf, 1, "hint placements always rf=1");
    }

    /// AV-4-1d regression: every `ChunkPlacement` returned by
    /// `allocate_chunks` must carry the manager-resolved tier as a
    /// lowercase wire-format string. Pre-fix the field didn't exist,
    /// so the FUSE writer hardcoded `"hot"` and the data-node's
    /// `select_disk_for_tier(Hot)` fell back to "any healthy disk by
    /// free capacity", silently landing chunks on the wrong tier.
    /// Pinning the DiskTier → wire-string contract here means a
    /// future enum-rename or Display-impl change can't silently
    /// re-introduce the bug.
    #[test]
    fn bug_av_4_1d_allocate_chunks_response_carries_tier() {
        use dtfs_common::types::DiskTier;
        assert_eq!(chunk_placement_tier_string(DiskTier::Cold), "cold");
        assert_eq!(chunk_placement_tier_string(DiskTier::Warm), "warm");
        assert_eq!(chunk_placement_tier_string(DiskTier::Hot), "hot");
        assert_eq!(chunk_placement_tier_string(DiskTier::Tape), "tape");
    }

    /// AV-4-1c regression #3: a legacy volume that has NEVER run
    /// `migrate-policy` (mandatory_replicas all-zero) falls back
    /// to legacy `replication` — preserves the pre-AV behaviour.
    /// Guards against the fix accidentally breaking
    /// pre-Bug-AV deployments.
    #[test]
    fn bug_av_4_1c_legacy_volume_keeps_legacy_replication_behaviour() {
        use dtfs_common::types::*;
        let mut v = vol_with_split_mandatory_and_legacy();
        // Strip mandatory (legacy state).
        v.mandatory_replicas = TierReplication { hot: 0, warm: 0, cold: 0, tape: 0 };
        let available = vec![DiskTier::Warm, DiskTier::Cold];
        let (tier, rf) = resolve_volume_placement(&v, "", &available);
        // With mandatory all-zero, effective_mandatory_replicas
        // falls back to legacy.replication = {warm:2, cold:1, ...}.
        // pick_effective_tier prefers higher tiers → returns Warm.
        assert_eq!(tier, DiskTier::Warm, "legacy fallback uses replication.warm=2");
        assert_eq!(rf, 2);
    }
}

#[cfg(test)]
mod rev_4_10_hint_ack_floor_tests {
    //! REV-4-10 — an honoured `preferred_tier` hint hard-returned rf=1,
    //! but `commit_chunk_locations` enforces the volume's
    //! `write_safety.min_replicas_before_ack` (default 2) via
    //! `ack_floor_satisfied`. With `first_write_tier = "nearest-warm"`
    //! (what the docs recommend for the fast-uplink download DC) EVERY
    //! write was streamed in FULL and only then rejected at commit, and
    //! the FUSE client's fresh-file cleanup unlinked the file. The hint
    //! must choose the TIER, not the replica count.
    use super::*;
    use dtfs_common::types::*;

    fn vol_with_ack_floor(floor: u32) -> VolumeConfig {
        VolumeConfig {
            id: VolumeId::new("default".to_string()),
            name: "default".to_string(),
            chunk_size_bytes: 4 * 1024 * 1024,
            replication: TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 },
            mandatory_replicas: TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 },
            cache_replicas: CacheReplication {
                hot: CacheTierConfig::default(),
                warm: CacheTierConfig { min: 1, max: 2 },
                cold: CacheTierConfig::default(),
                tape: CacheTierConfig::default(),
                per_dc: true,
                promote_reads: 2,
                promote_window_secs: 60,
                cooldown_secs: 30,
            },
            anchor: None,
            schedules: VolumeSchedules::default(),
            spill: SpillPolicy::default(),
            anchor_datacenter: Some("dc-a".to_string()),
            write_safety: WriteSafety {
                min_replicas_before_ack: floor,
                write_timeout_secs: 30,
            },
        }
    }

    /// The hint is still honoured (tier = Warm) but rf is clamped UP to
    /// the volume's ack floor, so the commit gate can be satisfied.
    #[test]
    fn rev_4_10_hint_rf_clamped_to_ack_floor() {
        let v = vol_with_ack_floor(2);
        let available = vec![DiskTier::Warm, DiskTier::Cold];
        let (tier, rf) = resolve_volume_placement(&v, "warm", &available);
        assert_eq!(tier, DiskTier::Warm, "the hint still selects the tier");
        assert!(
            rf >= 2,
            "rf must reach write_safety.min_replicas_before_ack=2, got {rf}"
        );
    }

    /// One-directional: a volume whose ack floor is 1 keeps the pre-fix
    /// rf=1 hint behaviour. The clamp only ever RAISES rf to the floor.
    #[test]
    fn rev_4_10_ack_floor_one_leaves_hint_unchanged() {
        let v = vol_with_ack_floor(1);
        let available = vec![DiskTier::Warm, DiskTier::Cold];
        assert_eq!(
            resolve_volume_placement(&v, "warm", &available),
            (DiskTier::Warm, 1)
        );
    }

    /// The clamped rf must actually satisfy the gate that rejected the
    /// write — pin the two helpers against each other.
    #[test]
    fn rev_4_10_clamped_rf_satisfies_the_commit_gate() {
        let v = vol_with_ack_floor(2);
        let (_, rf) = resolve_volume_placement(&v, "warm", &[DiskTier::Warm, DiskTier::Cold]);
        let commits: Vec<ChunkLocationEntry> = (0..rf)
            .map(|i| ChunkLocationEntry {
                chunk_index: 0,
                chunk_hash: "c0".to_string(),
                node_id: format!("node-{i}"),
                disk_id: 0,
                size_bytes: 1,
            })
            .collect();
        assert!(ack_floor_satisfied(&commits, v.write_safety.min_replicas_before_ack));
    }
}

#[cfg(test)]
mod rev_3_8_integrity_fault_batch_tests {
    //! REV-3-8 — one heartbeat's integrity faults collapse into ONE
    //! audit row + ONE QuarantineNode + ONE batched
    //! RemoveCorruptChunkReplicas, capped, instead of three unbatched
    //! Raft writes PER FAULT.
    use super::*;

    fn fault(chunk: &str) -> IntegrityFault {
        IntegrityFault {
            chunk_id: chunk.to_string(),
            observed_at_ms: 7,
            detector: "verify_on_read".to_string(),
            expected_len: 4,
            actual_len: 4,
            expected_hash: "aaaa".to_string(),
            actual_hash: "bbbb".to_string(),
        }
    }

    /// N faults across M distinct chunk ids collapse to ONE batch of M
    /// ids — the handler then issues 3 Raft writes total, not 3N.
    #[test]
    fn rev_3_8_dedupes_faults_to_distinct_chunk_ids() {
        let faults: Vec<IntegrityFault> =
            (0..9).map(|i| fault(&format!("chunk-{}", i % 3))).collect();
        let batch = batch_integrity_faults(&faults, MAX_INTEGRITY_FAULTS_PER_HEARTBEAT);
        assert_eq!(batch.chunk_ids, vec!["chunk-0", "chunk-1", "chunk-2"]);
        assert_eq!(batch.dropped, 0);
        assert_eq!(batch.scanned, 9);
        assert_eq!(
            batch.representative.map(|f| f.chunk_id.as_str()),
            Some("chunk-0")
        );
    }

    /// A node whose whole `.meta` directory is damaged fills the
    /// data-side collector to its 1024 cap; the manager must examine
    /// only MAX_INTEGRITY_FAULTS_PER_HEARTBEAT of them.
    #[test]
    fn rev_3_8_honours_per_heartbeat_cap() {
        let faults: Vec<IntegrityFault> =
            (0..1024).map(|i| fault(&format!("chunk-{i}"))).collect();
        let batch = batch_integrity_faults(&faults, MAX_INTEGRITY_FAULTS_PER_HEARTBEAT);
        assert_eq!(batch.chunk_ids.len(), MAX_INTEGRITY_FAULTS_PER_HEARTBEAT);
        assert_eq!(batch.scanned, MAX_INTEGRITY_FAULTS_PER_HEARTBEAT);
        assert_eq!(batch.dropped, 1024 - MAX_INTEGRITY_FAULTS_PER_HEARTBEAT);
    }

    /// A fault with no chunk id still quarantines the node but must not
    /// put an empty hash into the batched removal.
    #[test]
    fn rev_3_8_skips_empty_chunk_ids() {
        let faults = vec![fault(""), fault("chunk-a")];
        let batch = batch_integrity_faults(&faults, MAX_INTEGRITY_FAULTS_PER_HEARTBEAT);
        assert_eq!(batch.chunk_ids, vec!["chunk-a"]);
        assert!(batch.representative.is_some());
    }

    /// An empty report produces no batch at all — the handler skips the
    /// whole block, exactly as before.
    #[test]
    fn rev_3_8_empty_report_is_a_noop() {
        let batch = batch_integrity_faults(&[], MAX_INTEGRITY_FAULTS_PER_HEARTBEAT);
        assert!(batch.chunk_ids.is_empty());
        assert!(batch.representative.is_none());
        assert_eq!(batch.dropped, 0);
    }
}

#[cfg(test)]
mod rev_4_2_allocate_bound_tests {
    //! REV-4-2 — `AllocateChunksRequest.num_chunks` is peer-controlled
    //! and used to size an allocation + drive loops under the
    //! state-machine read lock.
    use super::*;

    /// An oversized count is refused at the boundary — nothing is
    /// allocated and the state-machine lock is never taken.
    #[test]
    fn rev_4_2_rejects_oversized_num_chunks() {
        assert!(
            validate_num_chunks(MAX_CHUNKS_PER_ALLOCATION + 1, MAX_CHUNKS_PER_ALLOCATION).is_err()
        );
        assert!(validate_num_chunks(u32::MAX, MAX_CHUNKS_PER_ALLOCATION).is_err());
    }

    /// Real writes still go through: a 30 GB season file at the 4 MiB
    /// default chunk size is 7680 chunks, well inside the cap, and so is
    /// a ~100 GB UHD remux (25600 chunks).
    #[test]
    fn rev_4_2_accepts_normal_num_chunks() {
        for n in [0u32, 1, 7_680, 25_600, MAX_CHUNKS_PER_ALLOCATION] {
            assert!(
                validate_num_chunks(n, MAX_CHUNKS_PER_ALLOCATION).is_ok(),
                "num_chunks {n} must be accepted"
            );
        }
    }

    /// Pins the derivation: the cap is 256 GiB of 4 MiB chunks, which is
    /// the `DefaultsConfig::chunk_size_mb` default.
    #[test]
    fn rev_4_2_cap_covers_largest_workload_file() {
        // `DefaultsConfig::chunk_size_mb` defaults to 4 (pinned by
        // dtfs-common's own `defaults_chunk_size_is_four` test).
        assert_eq!(
            MAX_CHUNKS_PER_ALLOCATION as u64 * 4 * 1024 * 1024,
            256 * 1024 * 1024 * 1024
        );
    }
}

#[cfg(test)]
mod bug_cf_3b_ack_floor_tests {
    //! CF-3b — pure tests for `ack_floor_satisfied`, the write-safety
    //! ack-floor gate the manager applies in `commit_chunk_locations`.
    use super::*;

    fn entry(chunk_index: u64, node_id: &str, disk_id: u32) -> ChunkLocationEntry {
        ChunkLocationEntry {
            chunk_index,
            chunk_hash: format!("hash-{chunk_index}-{node_id}-{disk_id}"),
            node_id: node_id.to_string(),
            disk_id,
            size_bytes: 1,
        }
    }

    /// floor=2, one chunk with two distinct nodes → satisfied.
    #[test]
    fn two_distinct_nodes_meets_floor_2() {
        let commits = vec![entry(0, "1", 0), entry(0, "2", 0)];
        assert!(ack_floor_satisfied(&commits, 2));
    }

    /// floor=2, one chunk with a single replica → NOT satisfied.
    #[test]
    fn single_replica_fails_floor_2() {
        let commits = vec![entry(0, "1", 0)];
        assert!(!ack_floor_satisfied(&commits, 2));
    }

    /// Bug BH identity rule: two entries on the SAME node but DIFFERENT
    /// disk_id (a mandatory + cache replica on different tiers of one
    /// box) count as two distinct replicas → floor=2 satisfied.
    #[test]
    fn same_node_two_disks_counts_as_two() {
        let commits = vec![entry(0, "1", 0), entry(0, "1", 1)];
        assert!(
            ack_floor_satisfied(&commits, 2),
            "(node,disk) identity: two disks on one node are two replicas"
        );
    }

    /// Same node, same disk committed twice (degenerate duplicate) is
    /// ONE distinct replica → floor=2 NOT satisfied.
    #[test]
    fn same_node_same_disk_dedups_to_one() {
        let commits = vec![entry(0, "1", 0), entry(0, "1", 0)];
        assert!(!ack_floor_satisfied(&commits, 2));
    }

    /// Multi-chunk batch: the gate is the MINIMUM across chunks. One
    /// chunk meets floor=2, another only has 1 replica → batch fails.
    #[test]
    fn weakest_chunk_governs_the_batch() {
        let commits = vec![
            entry(0, "1", 0),
            entry(0, "2", 0),
            entry(1, "1", 0), // chunk 1 has only one replica
        ];
        assert!(
            !ack_floor_satisfied(&commits, 2),
            "the under-replicated chunk in the batch must fail the whole commit"
        );
    }

    /// floor<=1 is always satisfied (single durable copy accepted, or
    /// the gate is disabled), even for an empty batch.
    #[test]
    fn floor_one_or_zero_always_satisfied() {
        assert!(ack_floor_satisfied(&[], 1));
        assert!(ack_floor_satisfied(&[], 0));
        assert!(ack_floor_satisfied(&[entry(0, "1", 0)], 1));
        assert!(ack_floor_satisfied(&[entry(0, "1", 0)], 0));
    }
}

#[cfg(test)]
mod bug_ci_fanout_liveness_tests {
    use super::fanout_delete_allowed;
    use dtfs_raft::state_machine::FileMetadata;
    use dtfs_raft::ChunkLocationEntry;

    fn file_with_chunks(path: &str, chunk_hashes: &[&str]) -> FileMetadata {
        FileMetadata {
            path: path.to_string(),
            volume_id: "default".to_string(),
            chunk_size: 4 * 1024 * 1024,
            chunks: chunk_hashes
                .iter()
                .enumerate()
                .map(|(i, h)| ChunkLocationEntry {
                    chunk_index: i as u64,
                    chunk_hash: (*h).to_string(),
                    node_id: "10".to_string(),
                    disk_id: 0,
                    size_bytes: 4 * 1024 * 1024,
                })
                .collect(),
            created_at_ms: 0,
            updated_at_ms: 0,
            symlink_target: None,
            logical_size: None,
            is_directory: false,
            cache_cooldown_until_ms: std::collections::BTreeMap::new(),
        }
    }

    // Bug CI — chunks are content-addressed, so an identical re-upload (or a
    // second file sharing identical content) re-creates the SAME chunk ids
    // the unlinked file used. The async delete fan-out captured its id list
    // at unlink time; on 2026-06-06 production it swept that list WHILE the
    // re-upload recommitted the same ids, deleting 2840 of 4135 freshly
    // written chunks from every node (state kept the entries — silent loss).
    // The fan-out must re-check liveness against CURRENT state per chunk at
    // dispatch time and skip any id a live file still references.
    #[test]
    fn live_referenced_chunk_must_not_be_deleted() {
        let mut files = std::collections::BTreeMap::new();
        files.insert(
            "/media/movie-v2.mkv".to_string(),
            file_with_chunks("/media/movie-v2.mkv", &["shared_x", "shared_y"]),
        );
        assert!(!fanout_delete_allowed("shared_x", &files));
        assert!(!fanout_delete_allowed("shared_y", &files));
    }

    #[test]
    fn unreferenced_chunk_is_deleted() {
        let mut files = std::collections::BTreeMap::new();
        files.insert(
            "/media/movie-v2.mkv".to_string(),
            file_with_chunks("/media/movie-v2.mkv", &["shared_x"]),
        );
        assert!(fanout_delete_allowed("gone_for_real", &files));
        let empty = std::collections::BTreeMap::new();
        assert!(fanout_delete_allowed("anything", &empty));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    /// Bug N regression — the registry stores a fully-formed URL
    /// (`http://ip:port`, Bug W). The fan-out planner must NOT add a
    /// second scheme; pre-fix it produced `http://http://ip:port` and
    /// every delete fan-out failed with a tonic transport error.
    #[test]
    fn bug_n_fanout_registry_url_not_double_prefixed() {
        let mut addrs = std::collections::HashMap::new();
        addrs.insert("7".to_string(), "http://100.64.0.20:9500".to_string());
        let plan = plan_delete_fanout(
            vec![("chunk-a".to_string(), "7".to_string())],
            &addrs,
        );
        assert_eq!(plan.len(), 1);
        assert!(
            plan.contains_key("http://100.64.0.20:9500"),
            "endpoint must be the registry URL verbatim, got keys: {:?}",
            plan.keys().collect::<Vec<_>>()
        );
    }

    /// Bug N — pre-Bug-W registries stored bare `host:port`; those still
    /// get exactly one scheme via normalize_endpoint.
    #[test]
    fn bug_n_fanout_bare_addr_gets_single_scheme() {
        let mut addrs = std::collections::HashMap::new();
        addrs.insert("7".to_string(), "10.0.0.4:9500".to_string());
        let plan = plan_delete_fanout(
            vec![("chunk-a".to_string(), "7".to_string())],
            &addrs,
        );
        assert!(plan.contains_key("http://10.0.0.4:9500"));
    }

    /// Bug N — chunks group per endpoint; targets with no registry entry
    /// are skipped (node gone — reconciler's job).
    #[test]
    fn bug_n_fanout_groups_and_skips_unknown_nodes() {
        let mut addrs = std::collections::HashMap::new();
        addrs.insert("7".to_string(), "http://10.0.0.4:9500".to_string());
        let plan = plan_delete_fanout(
            vec![
                ("chunk-a".to_string(), "7".to_string()),
                ("chunk-b".to_string(), "7".to_string()),
                ("chunk-c".to_string(), "999".to_string()), // unknown node
            ],
            &addrs,
        );
        assert_eq!(plan.len(), 1);
        assert_eq!(plan["http://10.0.0.4:9500"].len(), 2);
    }

    /// CF-1b — the fan-out failure audit summary identifies the data node
    /// endpoint and the count of chunks left orphaned, so an operator
    /// reading `/api/v1/audit` or the integrity feed can see which node and
    /// how much disk is at stake without grepping journald.
    #[test]
    fn cf1b_fanout_failed_summary_carries_node_and_count() {
        let summary = fanout_failed_summary("http://10.0.0.4:9500", 3);
        assert_eq!(summary, "node_addr=http://10.0.0.4:9500 orphan_chunks=3");
    }

    /// Bug W regression — peer IP wins over reported hostname.
    /// This is the actual prod bug from Plexmedia: data-warm01 resolves
    /// to fe80::… IPv6 link-local, but the peer IP is 10.20.0.157.
    #[test]
    fn bug_w_peer_ipv4_overrides_hostname() {
        let peer = Some(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(10, 20, 0, 157)),
            33421,
        ));
        let addr = build_data_grpc_addr(peer, "data-warm01", 9500);
        // Peer IP, registration port (NOT 33421).
        assert_eq!(addr, "http://10.20.0.157:9500");
    }

    /// Bug W regression — IPv6 peer address is wrapped properly. Without
    /// `[...]` wrapping, tonic's URL parser breaks on the colons.
    #[test]
    fn bug_w_peer_ipv6_address_wraps_brackets() {
        // Tonic's SocketAddr Display does wrap IPv6 with [...] already, so
        // SocketAddr::ip() Display omits brackets. We need to verify the
        // resulting URL is parseable.
        let peer = Some(SocketAddr::new(
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
            33421,
        ));
        let addr = build_data_grpc_addr(peer, "data-warm01", 9500);
        // IpAddr::V6 Display gives "2001:db8::1" without brackets; we
        // must add brackets ourselves so the resulting URL parses.
        // This test will fail if we don't — driving the fix.
        let _: tonic::transport::Endpoint = addr
            .parse()
            .expect("addr must parse as a tonic endpoint");
    }

    /// Bug W regression — fall back to hostname when peer is absent
    /// (UDS transport, in-process test, etc.).
    #[test]
    fn bug_w_no_peer_falls_back_to_hostname() {
        let addr = build_data_grpc_addr(None, "data-warm01", 9500);
        assert_eq!(addr, "http://data-warm01:9500");
    }

    // ── Bug BL — heartbeat non-leader forwarding helpers ────────────

    fn mk_voter(hostname: &str, addr: &str) -> dtfs_common::config::InitialVoter {
        dtfs_common::config::InitialVoter {
            hostname: hostname.to_string(),
            addr: addr.to_string(),
            region: String::new(),
            datacenter: String::new(),
            rack: String::new(),
        }
    }

    /// BL-3 — a node that reported an integrity fault this cycle must
    /// never be told it is clear, even if Raft state still shows it as
    /// not-yet-quarantined (the QuarantineNode write commits async on the
    /// leader). Without this the data node re-enables put_chunk on a
    /// corrupt node within one heartbeat.
    #[test]
    fn bl3_self_quarantine_cleared_guard() {
        // Healthy node, no faults → cleared.
        assert!(compute_self_quarantine_cleared(false, 0));
        // Reported a fault this cycle → NOT cleared, even though not yet
        // quarantined in local state.
        assert!(!compute_self_quarantine_cleared(false, 1));
        // Already quarantined → not cleared.
        assert!(!compute_self_quarantine_cleared(true, 0));
        // Quarantined AND reporting faults → not cleared.
        assert!(!compute_self_quarantine_cleared(true, 2));
    }

    /// BL-1 — the leader's gRPC addr resolves from the voter set by
    /// matching node_id_from_hostname against the openraft leader id when
    /// no membership entry exists.
    #[test]
    fn bl1_resolve_leader_addr_matches_by_hostname_id() {
        let voters = vec![
            mk_voter("mini-mgr01", "10.0.0.1:9401"),
            mk_voter("mini-mgr02", "10.0.0.2:9401"),
            mk_voter("mini-mgr03", "10.0.0.3:9401"),
        ];
        let empty = std::collections::BTreeMap::new();
        let leader_id = dtfs_common::node_id_from_hostname("mini-mgr02");
        assert_eq!(
            resolve_leader_addr(&voters, &empty, leader_id),
            Some("10.0.0.2:9401".to_string())
        );
        // Unknown leader id (not in voter set, not in membership) → None.
        assert_eq!(resolve_leader_addr(&voters, &empty, 0), None);
    }

    fn mk_membership_node(grpc_addr: &str) -> RaftNodeInfo {
        RaftNodeInfo {
            role: NodeRole::Manager,
            hostname: String::new(),
            grpc_addr: grpc_addr.to_string(),
            location: Location::new("", "", "", ""),
            disks: Vec::new(),
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
        }
    }

    /// CO-3-9 — a CF-2-promoted leader is present ONLY in the live Raft
    /// membership (not in the bootstrap `initial_voters`); it must still
    /// resolve, from its membership `grpc_addr`.
    #[test]
    fn co_3_9_leader_only_in_membership_resolves() {
        let voters = vec![mk_voter("mini-mgr01", "10.0.0.1:9401")];
        let leader_id = 42; // a promoted node, no matching voter hostname
        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(leader_id, mk_membership_node("http://10.0.0.9:9401"));
        assert_eq!(
            resolve_leader_addr(&voters, &nodes, leader_id),
            Some("http://10.0.0.9:9401".to_string()),
            "a promoted leader absent from initial_voters resolves via membership"
        );
    }

    /// CO-3-9 — a leader present only in `initial_voters` (not yet in the
    /// membership map, e.g. early boot) still resolves via the fallback.
    #[test]
    fn co_3_9_leader_only_in_voters_resolves() {
        let voters = vec![mk_voter("mini-mgr02", "10.0.0.2:9401")];
        let leader_id = dtfs_common::node_id_from_hostname("mini-mgr02");
        let empty = std::collections::BTreeMap::new();
        assert_eq!(
            resolve_leader_addr(&voters, &empty, leader_id),
            Some("10.0.0.2:9401".to_string())
        );
    }

    /// CO-3-9 — membership is preferred over voters when both have the id.
    #[test]
    fn co_3_9_membership_preferred_over_voters() {
        let voters = vec![mk_voter("mini-mgr02", "10.0.0.2:9401")];
        let leader_id = dtfs_common::node_id_from_hostname("mini-mgr02");
        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(leader_id, mk_membership_node("http://10.0.0.99:9401"));
        assert_eq!(
            resolve_leader_addr(&voters, &nodes, leader_id),
            Some("http://10.0.0.99:9401".to_string())
        );
    }

    /// CO-3-8 — only the transient liveness states may be re-stamped to
    /// Healthy by a heartbeat; Maintenance/Draining/Quarantined are sticky.
    #[test]
    fn co_3_8_heartbeat_may_mark_healthy_truth_table() {
        assert!(heartbeat_may_mark_healthy(HealthStatus::Healthy));
        assert!(heartbeat_may_mark_healthy(HealthStatus::Suspect));
        assert!(heartbeat_may_mark_healthy(HealthStatus::Down));
        // Disk-derived states stay heartbeat-clearable (recovery path).
        assert!(heartbeat_may_mark_healthy(HealthStatus::Degraded));
        assert!(heartbeat_may_mark_healthy(HealthStatus::Failed));
        // Sticky operator/drain/quarantine states are NOT.
        assert!(!heartbeat_may_mark_healthy(HealthStatus::Maintenance));
        assert!(!heartbeat_may_mark_healthy(HealthStatus::Draining));
        assert!(!heartbeat_may_mark_healthy(HealthStatus::Quarantined));
    }

    /// CR-3-6/MGR-7 — the heartbeat integrity-fault batch is accepted
    /// only when BOTH forwarded writes succeed; either failure means the
    /// node is neither quarantined nor de-located, so the caller must
    /// report `accepted = false` (CO-3-7 parity: the data node
    /// re-enqueues drained faults on that).
    #[test]
    fn cr_3_6_integrity_fault_batch_accepted_truth_table() {
        assert!(integrity_fault_batch_accepted(true, true));
        assert!(!integrity_fault_batch_accepted(false, true), "QuarantineNode failed");
        assert!(!integrity_fault_batch_accepted(true, false), "RemoveCorruptChunkReplicas failed");
        assert!(!integrity_fault_batch_accepted(false, false), "both failed");
    }

    /// CO-4-13 — a file registered under a floor=2 volume must enforce
    /// floor 2 even when the commit request claims a different (or
    /// unknown) volume that resolves to the default floor.
    #[test]
    fn co_4_13_resolve_ack_floor_uses_stricter_file_volume() {
        use dtfs_raft::state_machine::FileMetadata;

        let mut sm = StateMachineData::default();
        // Register a "strict" volume with floor=2 and put the file on it.
        sm.volumes.insert("strict".to_string(), vol_with_floor("strict", 2));
        sm.files.insert(
            "/movies/a.mkv".to_string(),
            FileMetadata {
                path: "/movies/a.mkv".to_string(),
                volume_id: "strict".to_string(),
                chunk_size: 4 * 1024 * 1024,
                chunks: Vec::new(),
                created_at_ms: 0,
                updated_at_ms: 0,
                symlink_target: None,
                logical_size: None,
                is_directory: false,
                cache_cooldown_until_ms: std::collections::BTreeMap::new(),
            },
        );

        // Request claims the unknown/default volume (floor would be 1) but
        // the file's stored volume floor (2) must win.
        assert_eq!(
            resolve_ack_floor(&sm, "/movies/a.mkv", "default", 1),
            2,
            "file's stored-volume floor (2) must override the request default (1)"
        );

        // Request volume is stricter than the file's volume → request wins.
        sm.volumes.insert("stricter".to_string(), vol_with_floor("stricter", 3));
        assert_eq!(
            resolve_ack_floor(&sm, "/movies/a.mkv", "stricter", 1),
            3,
            "the stricter of the two volumes governs"
        );

        // Unknown file (first commit) → only the request volume governs.
        assert_eq!(resolve_ack_floor(&sm, "/movies/new.mkv", "strict", 1), 2);
        assert_eq!(resolve_ack_floor(&sm, "/movies/new.mkv", "default", 1), 1);
    }

    /// CR-6-4 (FUSE-6) — a file already registered under volume
    /// "strict" governs placement even when the AllocateChunks request
    /// claims "default" (a client that hasn't learned or has forgotten
    /// the real volume_id); only an unknown/new file uses the request
    /// value.
    #[test]
    fn cr_6_4_resolve_placement_volume_id_prefers_existing_file() {
        assert_eq!(
            resolve_placement_volume_id(Some("strict"), "default"),
            "strict"
        );
        assert_eq!(
            resolve_placement_volume_id(None, "strict"),
            "strict",
            "new file (no existing entry) uses the request volume"
        );
        assert_eq!(
            resolve_placement_volume_id(Some("default"), "default"),
            "default"
        );
    }

    /// CR-6-2 (CMN-2) — a volume floor (2) that exceeds the effective
    /// mandatory RF of the tier the chunks landed on (cold, RF 1 via
    /// `vol_with_floor`) is clamped down to that RF, not left to reject
    /// every commit forever.
    #[test]
    fn cr_6_2_ack_floor_clamped_to_placed_tier_rf() {
        let mut sm = StateMachineData::default();
        sm.volumes.insert("strict".to_string(), vol_with_floor("strict", 2));
        sm.nodes.insert(1, mk_data_node(DiskTier::Cold));
        let chunks = vec![dtfs_raft::ChunkLocationEntry {
            chunk_index: 0,
            chunk_hash: "abc".to_string(),
            node_id: "1".to_string(),
            disk_id: 0,
            size_bytes: 1024,
        }];
        assert_eq!(
            clamp_ack_floor_to_rf(2, &sm, &chunks, "strict", "/movies/a.mkv", None),
            1,
            "cold RF is 1, so the floor of 2 must clamp down to 1"
        );

        // A floor already at or below the RF is left untouched.
        assert_eq!(
            clamp_ack_floor_to_rf(1, &sm, &chunks, "strict", "/movies/a.mkv", None),
            1
        );

        // A tier that resolves to a higher RF than the floor doesn't
        // raise the floor.
        sm.nodes.insert(2, mk_data_node(DiskTier::Warm));
        let mut vol_warm = vol_with_floor("warm-strict", 2);
        vol_warm.mandatory_replicas = dtfs_common::types::TierReplication {
            hot: 0,
            warm: 3,
            cold: 1,
            tape: 0,
        };
        sm.volumes.insert("warm-strict".to_string(), vol_warm);
        let warm_chunks = vec![dtfs_raft::ChunkLocationEntry {
            chunk_index: 0,
            chunk_hash: "abc".to_string(),
            node_id: "2".to_string(),
            disk_id: 0,
            size_bytes: 1024,
        }];
        assert_eq!(
            clamp_ack_floor_to_rf(2, &sm, &warm_chunks, "warm-strict", "/movies/b.mkv", None),
            2,
            "warm RF (3) exceeds the floor (2); the floor is never raised"
        );

        // Unknown node → tier unresolved → clamp is a no-op.
        let unknown_chunks = vec![dtfs_raft::ChunkLocationEntry {
            chunk_index: 0,
            chunk_hash: "abc".to_string(),
            node_id: "999".to_string(),
            disk_id: 0,
            size_bytes: 1024,
        }];
        assert_eq!(
            clamp_ack_floor_to_rf(2, &sm, &unknown_chunks, "strict", "/movies/c.mkv", None),
            2
        );
    }

    /// R1 review fix — a chunk placed on MULTIPLE tiers (warm RF 3 + cold
    /// RF 1) must clamp to the HIGHEST placed tier's RF (3), never the
    /// lowest (1). Taking the min silently downgraded the floor to 1 and
    /// let a write ack at 2 replicas even though warm's own policy
    /// demands 3 — exactly the RF downgrade the ack floor exists to
    /// prevent (A.3.3 #5).
    #[test]
    fn cr_6_2_ack_floor_clamp_uses_highest_placed_tier_rf_not_lowest() {
        let mut sm = StateMachineData::default();
        sm.nodes.insert(1, mk_data_node(DiskTier::Warm));
        sm.nodes.insert(2, mk_data_node(DiskTier::Cold));
        let mut vol = vol_with_floor("mixed", 3);
        vol.mandatory_replicas = dtfs_common::types::TierReplication {
            hot: 0,
            warm: 3,
            cold: 1,
            tape: 0,
        };
        sm.volumes.insert("mixed".to_string(), vol);
        let mixed_chunks = vec![
            dtfs_raft::ChunkLocationEntry {
                chunk_index: 0,
                chunk_hash: "abc".to_string(),
                node_id: "1".to_string(),
                disk_id: 0,
                size_bytes: 1024,
            },
            dtfs_raft::ChunkLocationEntry {
                chunk_index: 0,
                chunk_hash: "abc".to_string(),
                node_id: "2".to_string(),
                disk_id: 0,
                size_bytes: 1024,
            },
        ];
        assert_eq!(
            clamp_ack_floor_to_rf(3, &sm, &mixed_chunks, "mixed", "/movies/d.mkv", None),
            3,
            "warm's RF (3) must govern, not cold's RF (1) — the floor stays unchanged"
        );
    }

    /// CR-3-3/MGR-1 helper — a Data node with one disk of the given tier.
    fn mk_data_node(tier: DiskTier) -> RaftNodeInfo {
        RaftNodeInfo {
            role: NodeRole::Data,
            hostname: "data-1".to_string(),
            grpc_addr: String::new(),
            location: Location::new("lab", "leli", "rack-1", "data-1"),
            disks: vec![dtfs_common::types::DiskInfo {
                disk_id: 0,
                uuid: String::new(),
                mount_path: String::new(),
                tier,
                total_bytes: 0,
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
        }
    }

    fn file_with_chunk(
        path: &str,
        chunk_hash: &str,
        node_id: u64,
        disk_id: u32,
    ) -> dtfs_raft::state_machine::FileMetadata {
        dtfs_raft::state_machine::FileMetadata {
            path: path.to_string(),
            volume_id: "default".to_string(),
            chunk_size: 4 * 1024 * 1024,
            chunks: vec![dtfs_raft::ChunkLocationEntry {
                chunk_index: 0,
                chunk_hash: chunk_hash.to_string(),
                node_id: node_id.to_string(),
                disk_id,
                size_bytes: 1024,
            }],
            created_at_ms: 0,
            updated_at_ms: 0,
            symlink_target: None,
            logical_size: None,
            is_directory: false,
            cache_cooldown_until_ms: std::collections::BTreeMap::new(),
        }
    }

    /// CR-3-3/MGR-1 — an unknown chunk (no file references it) resolves
    /// to `None`, so the intercept skips the report instead of feeding
    /// the tiering engine a synthetic `""` file_path / `Hot` tier.
    #[test]
    fn cr_3_3_unknown_chunk_resolves_to_none() {
        let sm = StateMachineData::default();
        assert!(resolve_chunk_read_context(&sm, "no-such-hash", 1).is_none());
    }

    /// CR-3-3/MGR-1 — a known chunk resolves to its real file_path and
    /// the replica's real tier on the reporting node.
    #[test]
    fn cr_3_3_known_chunk_resolves_real_path_and_tier() {
        let mut sm = StateMachineData::default();
        sm.nodes.insert(1, mk_data_node(DiskTier::Warm));
        sm.files.insert(
            "/movies/x.mkv".to_string(),
            file_with_chunk("/movies/x.mkv", "h0", 1, 0),
        );
        let (path, tier) = resolve_chunk_read_context(&sm, "h0", 1).expect("known chunk");
        assert_eq!(path, "/movies/x.mkv");
        assert_eq!(tier, DiskTier::Warm);
    }

    /// R2/MGR-1 review fix — a replica on a Down node cannot be resolved
    /// to a serviceable tier via `live_replica_tier`. It must resolve to
    /// `None` (skip the report), NEVER fall back to a fake `Hot` tier —
    /// that reintroduces the exact fabricated-tier bug this task fixed
    /// (`evaluate` would plan a bogus Hot->Warm "demote" for a replica
    /// that isn't actually being served).
    #[test]
    fn cr_3_3_dead_replica_resolves_to_none() {
        let mut sm = StateMachineData::default();
        let mut node = mk_data_node(DiskTier::Cold);
        node.health = HealthStatus::Down;
        sm.nodes.insert(1, node);
        sm.files.insert(
            "/movies/x.mkv".to_string(),
            file_with_chunk("/movies/x.mkv", "h0", 1, 0),
        );
        assert!(
            resolve_chunk_read_context(&sm, "h0", 1).is_none(),
            "a dead replica must never fabricate a Hot tier"
        );
    }

    /// R1/MGR-3 review fix — `build_chunk_read_context_index` resolves
    /// every wanted chunk with ONE pass over `sm.files`: 3 files, 2
    /// wanted hashes -> both resolve to their correct path + live tier;
    /// an unrelated hash present in the tree but NOT wanted is absent
    /// from the index.
    #[test]
    fn cr_3_3_batch_index_resolves_only_wanted_hashes() {
        let mut sm = StateMachineData::default();
        sm.nodes.insert(1, mk_data_node(DiskTier::Warm));
        sm.nodes.insert(2, mk_data_node(DiskTier::Cold));
        sm.files.insert("/a.mkv".to_string(), file_with_chunk("/a.mkv", "ha", 1, 0));
        sm.files.insert("/b.mkv".to_string(), file_with_chunk("/b.mkv", "hb", 2, 0));
        sm.files.insert("/c.mkv".to_string(), file_with_chunk("/c.mkv", "hc-unwanted", 1, 0));

        let wanted: std::collections::HashSet<String> =
            ["ha".to_string(), "hb".to_string()].into_iter().collect();
        let index = build_chunk_read_context_index(&sm.files, &sm.nodes, &wanted, 1);

        // "ha" was reported by node 1 (matches) -> resolves live Warm.
        let (path_a, tier_a) = index.get("ha").expect("ha present");
        assert_eq!(path_a, "/a.mkv");
        assert_eq!(*tier_a, Some(DiskTier::Warm));

        // "hb" only has a row for node 2; this batch is reported by node 1,
        // so no row in "/b.mkv" matches the reporting node -> tier None,
        // but the path is still resolved (first referencing file).
        let (path_b, tier_b) = index.get("hb").expect("hb present");
        assert_eq!(path_b, "/b.mkv");
        assert_eq!(*tier_b, None);

        // Unrelated/unwanted hash never enters the index at all.
        assert!(index.get("hc-unwanted").is_none());
        assert_eq!(index.len(), 2, "only the wanted hashes are indexed");
    }

    /// R1/MGR-3 — a wanted hash with no referencing file at all is
    /// simply absent from the index (mirrors
    /// `resolve_chunk_read_context`'s None for an unknown chunk).
    #[test]
    fn cr_3_3_batch_index_omits_unresolvable_hash() {
        let sm = StateMachineData::default();
        let wanted: std::collections::HashSet<String> = ["ghost".to_string()].into_iter().collect();
        let index = build_chunk_read_context_index(&sm.files, &sm.nodes, &wanted, 1);
        assert!(index.get("ghost").is_none());
    }

    fn vol_with_floor(id: &str, floor: u32) -> dtfs_common::types::VolumeConfig {
        use dtfs_common::types::*;
        VolumeConfig {
            id: VolumeId::new(id.to_string()),
            name: id.to_string(),
            chunk_size_bytes: 4 * 1024 * 1024,
            replication: TierReplication { hot: 0, warm: 2, cold: 1, tape: 1 },
            mandatory_replicas: TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 },
            cache_replicas: CacheReplication {
                hot: CacheTierConfig::default(),
                warm: CacheTierConfig::default(),
                cold: CacheTierConfig::default(),
                tape: CacheTierConfig::default(),
                per_dc: true,
                promote_reads: 2,
                promote_window_secs: 60,
                cooldown_secs: 30,
            },
            anchor: None,
            schedules: VolumeSchedules::default(),
            spill: SpillPolicy::default(),
            anchor_datacenter: None,
            write_safety: WriteSafety {
                min_replicas_before_ack: floor,
                write_timeout_secs: 30,
            },
        }
    }
}
