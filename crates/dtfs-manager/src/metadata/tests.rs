//! Unit-test modules lifted out of `metadata.rs` so the source file
//! stays readable; every module below sees the parent's private items
//! through the glob import.
#![allow(unused_imports, deprecated)]

use super::*;

/// CF-3b gate as the service applies it (default state: every replica
/// counts), for the pure batch tests below.
fn ack_floor_satisfied(commits: &[ChunkLocationEntry], floor: u32) -> bool {
    min_maintained_replicas(&StateMachineData::default(), "v", commits) as u64 >= floor as u64
}

#[cfg(test)]
mod chunk_status_disk_grouping_tests {
    use super::group_corrupt_by_disk;
    use dtfs_proto::dtfs::manager::ChunkStatus;

    fn cs(id: &str, status: &str, disk_id: Option<u32>) -> ChunkStatus {
        ChunkStatus { chunk_id: id.into(), size: 0, status: status.into(), disk_id }
    }

    #[test]
    fn groups_by_disk_and_legacy_for_absent() {
        let g = group_corrupt_by_disk(&[
            cs("a", "corrupt", Some(2)),
            cs("b", "MISSING", Some(2)),
            cs("c", "corrupt", Some(0)),
            cs("d", "corrupt", None),
            cs("e", "healthy", Some(2)),
        ]);
        assert_eq!(g[&2], vec!["a", "b"]);
        assert_eq!(g[&0], vec!["c"]);
        assert_eq!(g[&dtfs_raft::legacy_disk_id()], vec!["d"]);
        assert_eq!(g.len(), 3);
        assert!(group_corrupt_by_disk(&[cs("e", "healthy", None)]).is_empty());
    }
}

#[cfg(test)]
mod mgr_11_forward_allow_list_tests {
    use super::forwardable;
    use dtfs_raft::ClientRequest;

    #[test]
    fn update_config_is_not_forwardable() {
        assert!(!forwardable(&ClientRequest::UpdateConfig { now_ms: 0,
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
        assert!(forwardable(&ClientRequest::UpdateNodeStatus { now_ms: 0,
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
    fn applied_but_refused_response_is_err() {
        let json = serde_json::to_vec(&dtfs_raft::ClientResponse::Error {
            message: "would strand anchor".into(),
        })
        .unwrap();
        let resp = ForwardWriteResponse { success: true, response_json: json, error: String::new() };
        assert_eq!(forward_result(&resp), Err("would strand anchor".to_string()));
        let ok = serde_json::to_vec(&dtfs_raft::ClientResponse::Success).unwrap();
        let resp = ForwardWriteResponse { success: true, response_json: ok, error: String::new() };
        assert!(forward_result(&resp).is_ok());
    }

    #[test]
    fn committed_refusal_is_err_via_pipe() {
        use super::super::CommittedOk;
        let refused: Result<dtfs_raft::ClientResponse, tonic::Status> =
            Ok(dtfs_raft::ClientResponse::Error { message: "nope".into() });
        assert_eq!(refused.pipe_committed_ok(), Err("nope".to_string()));
        let fine: Result<dtfs_raft::ClientResponse, tonic::Status> = Ok(dtfs_raft::ClientResponse::Success);
        assert!(fine.pipe_committed_ok().is_ok());
        let st: Result<dtfs_raft::ClientResponse, tonic::Status> = Err(tonic::Status::unavailable("x"));
        assert!(st.pipe_committed_ok().is_err());
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

/// The forward is authenticated by the `dtfs-mgr-rpc` HMAC bound to the
/// body, never by the raw secret crossing the (possibly plaintext) WAN.
#[cfg(test)]
mod forward_hmac_auth_tests {
    use super::{check_forward_auth, forward_write_request};
    use dtfs_common::crypto::{DATA_RPC_AUTH_MAC, DATA_RPC_AUTH_TS};
    use dtfs_proto::dtfs::manager::ForwardWriteRequest;
    use tonic::metadata::MetadataMap;

    const SECRET: &str = "s3cr3t-cluster";
    const BODY: &[u8] = br#"{"UpdateNodeUsage":{"node_id":7}}"#;

    // Rolling update: while the flag is on, the follower still fills the
    // legacy field so a not-yet-updated leader accepts the forward.
    #[test]
    fn follower_request_carries_hmac_and_secret_while_legacy_on() {
        let req = forward_write_request(SECRET, true, BODY.to_vec());
        assert_eq!(req.get_ref().shared_secret, SECRET);
        assert!(req.metadata().get(DATA_RPC_AUTH_TS).is_some());
        assert!(req.metadata().get(DATA_RPC_AUTH_MAC).is_some());
    }

    #[test]
    fn follower_request_carries_hmac_not_secret_with_legacy_off() {
        let req = forward_write_request(SECRET, false, BODY.to_vec());
        assert_eq!(req.get_ref().shared_secret, "");
        assert!(req.metadata().get(DATA_RPC_AUTH_TS).is_some());
        assert!(req.metadata().get(DATA_RPC_AUTH_MAC).is_some());
    }

    // A new leader verifies the stamp and ignores the legacy field.
    #[test]
    fn stamped_forward_ignores_legacy_field() {
        let req = forward_write_request(SECRET, false, BODY.to_vec());
        let body = ForwardWriteRequest { client_request_json: BODY.to_vec(), shared_secret: "wrong".into() };
        check_forward_auth(SECRET, false, &body, req.metadata()).unwrap();
    }

    #[test]
    fn hmac_forward_accepted_with_legacy_disabled() {
        let req = forward_write_request(SECRET, false, BODY.to_vec());
        check_forward_auth(SECRET, false, req.get_ref(), req.metadata()).unwrap();
    }

    #[test]
    fn hmac_forward_with_tampered_body_refused() {
        let req = forward_write_request(SECRET, false, BODY.to_vec());
        let tampered = ForwardWriteRequest {
            client_request_json: br#"{"UpdateNodeUsage":{"node_id":8}}"#.to_vec(),
            shared_secret: String::new(),
        };
        let err = check_forward_auth(SECRET, true, &tampered, req.metadata()).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn hmac_forward_signed_with_wrong_secret_refused() {
        let req = forward_write_request("other-secret", false, BODY.to_vec());
        let err = check_forward_auth(SECRET, true, req.get_ref(), req.metadata()).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    fn legacy(secret: &str) -> ForwardWriteRequest {
        ForwardWriteRequest { client_request_json: BODY.to_vec(), shared_secret: secret.into() }
    }

    #[test]
    fn legacy_secret_accepted_while_flag_on() {
        check_forward_auth(SECRET, true, &legacy(SECRET), &MetadataMap::new()).unwrap();
    }

    #[test]
    fn legacy_wrong_secret_refused_even_with_flag_on() {
        let err =
            check_forward_auth(SECRET, true, &legacy("wrong"), &MetadataMap::new()).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn legacy_secret_refused_with_flag_off() {
        let err =
            check_forward_auth(SECRET, false, &legacy(SECRET), &MetadataMap::new()).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(err.message().contains("accept_legacy_forward_secret"), "{}", err.message());
    }

    #[test]
    fn empty_configured_secret_skips_auth() {
        let req = forward_write_request("", true, BODY.to_vec());
        assert!(req.metadata().get(DATA_RPC_AUTH_MAC).is_none());
        check_forward_auth("", false, req.get_ref(), req.metadata()).unwrap();
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
            spill: dtfs_common::types::SpillPolicy::default(),
            version: 0,
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
            spill: SpillPolicy::default(),
            version: 0,
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
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

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

    /// The Raft membership address wins over a (possibly loopback) registry
    /// entry; empty membership falls back to registry/voters.
    #[test]
    fn leader_addr_prefers_raft_membership_over_registry() {
        let voters = vec![mk_voter("mgr01", "10.0.0.1:9401")];
        let leader_id = dtfs_common::node_id_from_hostname("mgr01");
        let empty = std::collections::BTreeMap::new();
        assert_eq!(
            resolve_leader_addr_with(Some("100.64.0.9:9401"), &voters, &empty, leader_id),
            Some("100.64.0.9:9401".to_string())
        );
        assert_eq!(
            resolve_leader_addr_with(Some(""), &voters, &empty, leader_id),
            Some("10.0.0.1:9401".to_string())
        );
        assert_eq!(resolve_leader_addr_with(None, &voters, &empty, 0), None);
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

    /// An already-Healthy node needs no `healthy` write (no-op apply every
    /// 5 s); every recoverable non-Healthy state still gets one; sticky
    /// states never do.
    #[test]
    fn heartbeat_skips_healthy_write_when_already_healthy() {
        assert!(!heartbeat_should_write_healthy(HealthStatus::Healthy));
        for s in [
            HealthStatus::Suspect,
            HealthStatus::Down,
            HealthStatus::Degraded,
            HealthStatus::Failed,
        ] {
            assert!(heartbeat_should_write_healthy(s), "{s:?}");
        }
        for s in [HealthStatus::Maintenance, HealthStatus::Draining, HealthStatus::Quarantined] {
            assert!(!heartbeat_should_write_healthy(s), "{s:?}");
        }
    }

    /// Lock leases are clamped to [1 s, 5 min]; a lock is live strictly
    /// before its expiry.
    #[test]
    fn posix_lock_lease_clamp_and_liveness() {
        assert_eq!(clamp_posix_lock_lease_ms(0), 1_000);
        assert_eq!(clamp_posix_lock_lease_ms(1_000), 1_000);
        assert_eq!(clamp_posix_lock_lease_ms(300_000), 300_000);
        assert_eq!(clamp_posix_lock_lease_ms(300_001), 300_000);
        assert_eq!(clamp_posix_lock_lease_ms(u64::MAX), 300_000);
        assert!(posix_lock_is_live(101, 100));
        assert!(!posix_lock_is_live(100, 100));
        assert!(!posix_lock_is_live(0, 100));
    }

    /// RenameFile rejections carry typed gRPC codes (FUSE: ENOENT/ENOTEMPTY).
    #[test]
    fn rename_error_status_codes() {
        use tonic::Code;
        let code = |m: &str| rename_error_status(m).map(|s| s.code());
        assert_eq!(code("RenameFile: source '/a' not found"), Some(Code::NotFound));
        assert_eq!(
            code("RenameFile: destination '/d' is a non-empty directory"),
            Some(Code::FailedPrecondition)
        );
        assert_eq!(
            code("RenameFile: cannot rename '/a' into its own subtree '/a/b'"),
            Some(Code::InvalidArgument)
        );
        assert_eq!(code("something else"), None);
        assert_eq!(
            rename_error_status("RenameFile: source '/a' not found").unwrap().message(),
            "RenameFile: source '/a' not found"
        );
    }

    /// New files take the volume's chunk size; unset/zero/unknown volume
    /// falls back to `[defaults] chunk_size_mb`.
    #[test]
    fn create_file_chunk_size_prefers_volume() {
        assert_eq!(resolve_create_chunk_size(Some(16 << 20), 4), 16 << 20);
        assert_eq!(resolve_create_chunk_size(Some(0), 4), 4 << 20);
        assert_eq!(resolve_create_chunk_size(None, 4), 4 << 20);
    }

    /// `[scrub] quarantine_on_fault` gates the heartbeat QuarantineNode write.
    #[test]
    fn heartbeat_quarantine_write_follows_scrub_setting() {
        assert!(heartbeat_quarantine_write_enabled(true));
        assert!(!heartbeat_quarantine_write_enabled(false));
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
                cache_cooldown_until_ms: std::collections::BTreeMap::new(), ever_committed: None,
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

    // ---- Ack floor filled across maintained tiers ----

    fn lab_node(host: &str, dc: &str, tier: DiskTier, health: HealthStatus) -> RaftNodeInfo {
        let mut n = mk_data_node(tier);
        n.hostname = host.to_string();
        n.grpc_addr = format!("{host}:9500");
        n.location = Location::new("lab", dc, "rack-1", host);
        n.health = health;
        n
    }

    /// data01 = only cold (anchor DC), data02/03 = warm; cold 1 / warm 2, floor 2.
    fn live_topology() -> StateMachineData {
        let mut sm = StateMachineData::default();
        let mut vol = vol_with_floor("default", 2);
        vol.mandatory_replicas =
            dtfs_common::types::TierReplication { hot: 0, warm: 2, cold: 1, tape: 0 };
        vol.anchor_datacenter = Some("leli".to_string());
        sm.volumes.insert("default".to_string(), vol);
        sm.nodes.insert(1, lab_node("data01", "leli", DiskTier::Cold, HealthStatus::Healthy));
        sm.nodes.insert(2, lab_node("data02", "toro", DiskTier::Warm, HealthStatus::Healthy));
        sm.nodes.insert(3, lab_node("data03", "nrb", DiskTier::Warm, HealthStatus::Healthy));
        sm
    }

    fn fill(sm: &StateMachineData, existing: &[&str], floor: u32) -> Vec<String> {
        let vol = &sm.volumes["default"];
        fill_floor_targets(
            &sm.nodes,
            &vol.effective_mandatory_replicas(),
            &vol.spill,
            &dtfs_common::config::PlacementConfig::default(),
            &Location::new("", "", "", ""),
            DiskTier::Cold,
            &existing.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            floor,
            &std::collections::HashSet::new(),
        )
    }

    fn ent(node: &str) -> dtfs_raft::ChunkLocationEntry {
        dtfs_raft::ChunkLocationEntry {
            chunk_index: 0,
            chunk_hash: "abc".to_string(),
            node_id: node.to_string(),
            disk_id: 0,
            size_bytes: 1024,
        }
    }

    /// The ring places the single cold node only; the fill adds a warm node
    /// so each chunk has 2 targets on 2 distinct nodes, anchor cold first.
    #[tokio::test]
    async fn floor_fill_adds_warm_target_next_to_anchor_cold() {
        let sm = live_topology();
        let arc = Arc::new(tokio::sync::RwLock::new(sm.clone()));
        let engine = PlacementEngine::default();
        engine.rebuild_ring(&arc).await;
        let placed = engine.place_chunks_with_anchor(
            "default", "/a", 4, DiskTier::Cold, 2, Some("leli".to_string()),
        );
        assert_eq!(placed.len(), 4);
        for p in placed {
            assert_eq!(p.node_ids.len(), 1, "ring alone finds one cold node (the bug)");
            assert_eq!(p.node_ids[0].0, "1");
            let extra = fill(&sm, &["data01:9500"], 2);
            assert_eq!(extra.len(), 1);
            assert!(extra[0] == "data02:9500" || extra[0] == "data03:9500");
        }
    }

    /// Floor 3 on that topology: both warm nodes, distinct.
    #[test]
    fn floor_fill_distinct_nodes_up_to_floor() {
        let sm = live_topology();
        let mut got = fill(&sm, &["data01:9500"], 3);
        got.sort();
        assert_eq!(got, vec!["data02:9500".to_string(), "data03:9500".to_string()]);
        // Already satisfied: nothing added.
        assert!(fill(&sm, &["data01:9500", "data02:9500"], 2).is_empty());
    }

    /// Quarantined / maintenance / excluded warm nodes are not fill targets.
    #[test]
    fn floor_fill_skips_unhealthy_and_excluded_nodes() {
        let mut sm = live_topology();
        sm.nodes.get_mut(&2).unwrap().health = HealthStatus::Quarantined;
        sm.nodes.get_mut(&3).unwrap().health = HealthStatus::Maintenance;
        assert!(fill(&sm, &["data01:9500"], 2).is_empty());
        // Only node 3 healthy but client-excluded: still none.
        sm.nodes.get_mut(&3).unwrap().health = HealthStatus::Healthy;
        let vol = &sm.volumes["default"];
        let ex: std::collections::HashSet<String> = ["data03:9500".to_string()].into();
        let got = fill_floor_targets(
            &sm.nodes, &vol.effective_mandatory_replicas(), &vol.spill,
            &dtfs_common::config::PlacementConfig::default(),
            &Location::new("", "", "", ""), DiskTier::Cold,
            &["data01:9500".to_string()], 2, &ex,
        );
        assert!(got.is_empty());
    }

    /// Cache-only (RF 0) disks and BZ-forbidden tiers are never fill targets.
    #[test]
    fn floor_fill_skips_cache_only_and_forbidden_tiers() {
        let mut sm = live_topology();
        sm.nodes.insert(4, lab_node("data04", "toro", DiskTier::Hot, HealthStatus::Healthy));
        // hot RF 0 => data04 ignored; data02/03 still picked.
        let got = fill(&sm, &["data01:9500"], 2);
        assert_eq!(got.len(), 1);
        assert_ne!(got[0], "data04:9500");
        // Forbid warm spill: nothing left.
        sm.volumes.get_mut("default").unwrap().spill.forbidden_tiers = vec!["warm".into()];
        assert!(fill(&sm, &["data01:9500"], 2).is_empty());
    }

    /// Cold-only commit on the live topology is rejected (min 1 < floor 2);
    /// cold + warm passes.
    #[test]
    fn commit_gate_cold_only_rejected_cold_plus_warm_accepted() {
        let sm = live_topology();
        let floor = clamp_ack_floor_to_capacity(2, &sm, "default", None, "/a", None);
        assert_eq!(floor, 2, "3 maintained nodes can hold 2 copies");
        let cold_only = min_maintained_replicas(&sm, "default", &[ent("1")]);
        assert_eq!(cold_only, 1);
        assert!((cold_only as u32) < floor, "single cold copy must be rejected");
        let both = min_maintained_replicas(&sm, "default", &[ent("1"), ent("2")]);
        assert_eq!(both, 2);
        assert!(both as u32 >= floor);
    }

    // ---- [write] anchor_sync = false (defer_anchor) ----

    /// Same composition as `allocate_chunks`: a deferred anchor uses
    /// `deferred_anchor_targets` (when it finds any); otherwise ring placement
    /// on the volume's first maintained tier (warm rf 2) with
    /// `allocation_anchor_dc`, then the ack-floor fill (floor 2) ranked from
    /// the requester. Per-chunk targets.
    async fn alloc_targets(sm: &StateMachineData, defer: bool, requester_dc: &str) -> Vec<Vec<String>> {
        let vol = &sm.volumes["default"];
        let anchor_dc = volume_anchor_dc(vol);
        let anchor = allocation_anchor_dc(anchor_dc.clone(), defer, requester_dc);
        if anchor_dc.is_some() && anchor.is_none() {
            let t = deferred_anchor_targets(
                &sm.nodes,
                &vol.effective_mandatory_replicas(),
                &spill_for_fill(&vol.spill, anchor_dc.as_deref()),
                &dtfs_common::config::PlacementConfig::default(),
                &Location::new("", requester_dc, "", ""),
                DiskTier::Warm,
                anchor_dc.as_deref().unwrap(),
                2,
                &std::collections::HashSet::new(),
            );
            if !t.is_empty() {
                return vec![t; 8];
            }
        }
        let arc = Arc::new(tokio::sync::RwLock::new(sm.clone()));
        let engine = PlacementEngine::default();
        engine.rebuild_ring(&arc).await;
        let placed = engine.place_chunks_with_anchor("default", "/a", 8, DiskTier::Warm, 2, anchor);
        assert_eq!(placed.len(), 8);
        placed
            .into_iter()
            .map(|p| {
                let mut addrs: Vec<String> = p
                    .node_ids
                    .iter()
                    .map(|n| sm.nodes[&n.0.parse::<u64>().unwrap()].grpc_addr.clone())
                    .collect();
                let extra = fill_floor_targets(
                    &sm.nodes,
                    &vol.effective_mandatory_replicas(),
                    &vol.spill,
                    &dtfs_common::config::PlacementConfig::default(),
                    &Location::new("", requester_dc, "", ""),
                    DiskTier::Warm,
                    &addrs,
                    2,
                    &std::collections::HashSet::new(),
                );
                addrs.extend(extra);
                addrs
            })
            .collect()
    }

    /// Deferral from NRB: both warm nodes meet the floor, no LELI target, and
    /// the unchanged commit gate accepts the two warm copies.
    #[tokio::test]
    async fn defer_anchor_from_nrb_places_no_anchor_target() {
        let sm = live_topology();
        for t in alloc_targets(&sm, true, "nrb").await {
            assert!(!t.contains(&"data01:9500".to_string()), "anchor target placed: {t:?}");
            let mut s = t.clone();
            s.sort();
            assert_eq!(s, vec!["data02:9500".to_string(), "data03:9500".to_string()]);
        }
        let floor = clamp_ack_floor_to_capacity(2, &sm, "default", None, "/a", None);
        assert!(min_maintained_replicas(&sm, "default", &[ent("2"), ent("3")]) as u32 >= floor);
    }

    /// Deferral never lowers the floor: with toro warm Down only one near
    /// node is left, so the anchor node fills the second slot.
    #[tokio::test]
    async fn defer_anchor_includes_anchor_when_near_nodes_miss_floor() {
        let mut sm = live_topology();
        sm.nodes.get_mut(&2).unwrap().health = HealthStatus::Down;
        for t in alloc_targets(&sm, true, "nrb").await {
            let mut s = t.clone();
            s.sort();
            assert_eq!(s, vec!["data01:9500".to_string(), "data03:9500".to_string()]);
        }
    }

    /// No deferral (field absent) = today's placement: anchor forced.
    #[tokio::test]
    async fn no_defer_anchor_forces_anchor_target() {
        let sm = live_topology();
        for t in alloc_targets(&sm, false, "nrb").await {
            assert!(t.contains(&"data01:9500".to_string()), "anchor missing: {t:?}");
            assert_eq!(t.len(), 2);
        }
    }

    /// A requester inside the anchor DC is unaffected (case-insensitive).
    #[tokio::test]
    async fn defer_anchor_from_anchor_dc_still_forces_anchor() {
        let sm = live_topology();
        for t in alloc_targets(&sm, true, "LELI").await {
            assert!(t.contains(&"data01:9500".to_string()), "anchor missing: {t:?}");
        }
    }

    #[test]
    fn allocation_anchor_dc_cases() {
        let a = || Some("leli".to_string());
        assert_eq!(allocation_anchor_dc(a(), false, "nrb"), a());
        assert_eq!(allocation_anchor_dc(a(), true, "nrb"), None);
        assert_eq!(allocation_anchor_dc(a(), true, "Leli"), a());
        assert_eq!(allocation_anchor_dc(None, true, "nrb"), None);
    }

    fn deferred(sm: &StateMachineData, placed_tier: DiskTier, requester_dc: &str, floor: u32) -> Vec<String> {
        let vol = &sm.volumes["default"];
        deferred_anchor_targets(
            &sm.nodes,
            &vol.effective_mandatory_replicas(),
            &spill_for_fill(&vol.spill, Some("leli")),
            &dtfs_common::config::PlacementConfig::default(),
            &Location::new("", requester_dc, "", ""),
            placed_tier,
            "leli",
            floor,
            &std::collections::HashSet::new(),
        )
    }

    /// LELI holds a cold-only and a warm node, TORO/NRB one warm each. A
    /// deferred write from NRB lands on nrb + toro whatever the placed tier
    /// (the ring would hash onto the LELI warm node or the cold one).
    #[test]
    fn deferred_targets_skip_anchor_dc_for_any_placed_tier() {
        let mut sm = live_topology();
        sm.nodes.insert(4, lab_node("data04", "leli", DiskTier::Warm, HealthStatus::Healthy));
        for tier in [DiskTier::Warm, DiskTier::Cold] {
            let mut got = deferred(&sm, tier, "nrb", 2);
            got.sort();
            assert_eq!(
                got,
                vec!["data02:9500".to_string(), "data03:9500".to_string()],
                "placed tier {tier:?}"
            );
        }
    }

    /// Only one healthy node outside LELI: the floor is met by adding an
    /// anchor-DC node, never lowered. No maintained node at all: empty
    /// (caller falls back to the normal path).
    #[test]
    fn deferred_targets_fall_back_to_anchor_dc_for_floor() {
        let mut sm = live_topology();
        sm.nodes.get_mut(&2).unwrap().health = HealthStatus::Down;
        let got = deferred(&sm, DiskTier::Warm, "nrb", 2);
        assert_eq!(got, vec!["data03:9500".to_string(), "data01:9500".to_string()]);
        for n in sm.nodes.values_mut() {
            n.health = HealthStatus::Down;
        }
        assert!(deferred(&sm, DiskTier::Warm, "nrb", 2).is_empty());
    }

    /// With the requester's full location (region included, as the FUSE
    /// client now sends it) the floor fill and deferred placement pick the
    /// same-DC node; a region-less requester is CrossRegion to every node and
    /// falls back to the node-id tie-break (toro).
    #[test]
    fn requester_region_makes_ranking_prefer_same_dc() {
        let sm = live_topology();
        let vol = &sm.volumes["default"];
        let pick = |req: Location| {
            fill_floor_targets(
                &sm.nodes,
                &vol.effective_mandatory_replicas(),
                &vol.spill,
                &dtfs_common::config::PlacementConfig::default(),
                &req,
                DiskTier::Cold,
                &["data01:9500".to_string()],
                2,
                &std::collections::HashSet::new(),
            )
        };
        assert_eq!(pick(Location::new("lab", "nrb", "rack-1", "c")), vec!["data03:9500".to_string()]);
        assert_eq!(pick(Location::new("", "nrb", "", "")), vec!["data02:9500".to_string()]);
        let first = |req: Location| {
            deferred_anchor_targets(
                &sm.nodes,
                &vol.effective_mandatory_replicas(),
                &spill_for_fill(&vol.spill, Some("leli")),
                &dtfs_common::config::PlacementConfig::default(),
                &req,
                DiskTier::Warm,
                "leli",
                2,
                &std::collections::HashSet::new(),
            )[0]
            .clone()
        };
        assert_eq!(first(Location::new("lab", "nrb", "rack-1", "c")), "data03:9500");
        assert_eq!(first(Location::new("lab", "toro", "rack-1", "c")), "data02:9500");
    }

    /// A volume configured only via the newer `anchor = {..}` policy (no
    /// legacy `anchor_datacenter`) still has an anchor at allocation.
    #[test]
    fn volume_anchor_dc_reads_anchor_policy() {
        let mut v = vol_with_floor("default", 2);
        v.anchor_datacenter = None;
        v.anchor = Some(dtfs_common::types::AnchorPolicy {
            datacenter: "leli".into(),
            required_tier: Some(DiskTier::Cold),
        });
        assert_eq!(volume_anchor_dc(&v), Some("leli".to_string()));
        v.anchor = None;
        assert_eq!(volume_anchor_dc(&v), None);
        v.anchor_datacenter = Some("toro".into());
        assert_eq!(volume_anchor_dc(&v), Some("toro".to_string()));
    }

    /// One data node / one maintained disk: floor clamps to 1 and commits
    /// (CR-6-2 reason the clamp exists).
    #[test]
    fn commit_gate_single_node_clamps_floor_to_one() {
        let mut sm = StateMachineData::default();
        sm.volumes.insert("default".to_string(), vol_with_floor("default", 2));
        sm.nodes.insert(1, lab_node("data01", "leli", DiskTier::Cold, HealthStatus::Healthy));
        assert_eq!(clamp_ack_floor_to_capacity(2, &sm, "default", None, "/a", None), 1);
        assert_eq!(min_maintained_replicas(&sm, "default", &[ent("1")]), 1);
        // A floor already within capacity is untouched.
        assert_eq!(clamp_ack_floor_to_capacity(1, &sm, "default", None, "/a", None), 1);
    }

    /// A replica on a cache-only tier (RF 0) does not count toward the floor.
    #[test]
    fn cache_only_replica_does_not_count_toward_floor() {
        let mut sm = live_topology();
        sm.nodes.insert(4, lab_node("data04", "toro", DiskTier::Hot, HealthStatus::Healthy));
        // cold + hot(cache): only the cold one counts.
        assert_eq!(min_maintained_replicas(&sm, "default", &[ent("1"), ent("4")]), 1);
        // hot only: zero maintained replicas.
        assert_eq!(min_maintained_replicas(&sm, "default", &[ent("4")]), 0);
        // Hot nodes add no capacity.
        assert_eq!(
            maintained_node_capacity(&sm, &sm.volumes["default"].effective_mandatory_replicas(), &Default::default(), Some(DiskTier::Cold)),
            3
        );
    }

    /// Other nodes quarantined / in maintenance: capacity (and so the
    /// clamp) drops to what is available, here 1 (only data01), so the
    /// cold-only commit is accepted rather than failing forever; with one
    /// warm node back it needs 2 again.
    #[test]
    fn floor_clamps_to_available_when_peers_unavailable() {
        let mut sm = live_topology();
        sm.nodes.get_mut(&2).unwrap().health = HealthStatus::Quarantined;
        sm.nodes.get_mut(&3).unwrap().health = HealthStatus::Maintenance;
        assert_eq!(clamp_ack_floor_to_capacity(2, &sm, "default", None, "/a", None), 1);
        sm.nodes.get_mut(&3).unwrap().health = HealthStatus::Healthy;
        assert_eq!(clamp_ack_floor_to_capacity(2, &sm, "default", None, "/a", None), 2);
    }

    /// Property-style: over many topologies, the allocator fill plus one
    /// anchor target reaches min(floor, capacity) distinct nodes, never
    /// duplicates, and keeps the anchor target.
    #[test]
    fn floor_fill_reaches_min_of_floor_and_capacity() {
        let mut seed = 0x9e3779b97f4a7c15u64;
        let mut rnd = move |m: u64| {
            seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17;
            seed % m
        };
        for _ in 0..300 {
            let mut sm = StateMachineData::default();
            let floor = 1 + rnd(4) as u32;
            let mut vol = vol_with_floor("default", floor);
            vol.mandatory_replicas = dtfs_common::types::TierReplication {
                hot: rnd(2) as u32, warm: rnd(3) as u32, cold: 1, tape: 0,
            };
            sm.volumes.insert("default".to_string(), vol);
            sm.nodes.insert(1, lab_node("data01", "leli", DiskTier::Cold, HealthStatus::Healthy));
            for id in 2..(2 + rnd(5)) {
                let tier = [DiskTier::Hot, DiskTier::Warm, DiskTier::Cold][rnd(3) as usize];
                let health = [HealthStatus::Healthy, HealthStatus::Healthy, HealthStatus::Quarantined, HealthStatus::Maintenance][rnd(4) as usize];
                sm.nodes.insert(id, lab_node(&format!("data{id:02}"), "toro", tier, health));
            }
            let mand = sm.volumes["default"].effective_mandatory_replicas();
            let cap = maintained_node_capacity(&sm, &mand, &Default::default(), Some(DiskTier::Cold));
            let existing = vec!["data01:9500".to_string()];
            let vol = &sm.volumes["default"];
            let extra = fill_floor_targets(
                &sm.nodes, &mand, &vol.spill,
                &dtfs_common::config::PlacementConfig::default(),
                &Location::new("", "", "", ""), DiskTier::Cold, &existing, floor,
                &std::collections::HashSet::new(),
            );
            let mut all = existing.clone();
            all.extend(extra);
            let uniq: std::collections::HashSet<_> = all.iter().collect();
            assert_eq!(uniq.len(), all.len(), "no duplicate targets");
            assert_eq!(all[0], "data01:9500", "anchor target kept first");
            assert_eq!(all.len() as u32, floor.min(cap), "floor {floor} cap {cap}");
        }
    }

    fn add_disk(sm: &mut StateMachineData, node: u64, id: u32, tier: DiskTier, total: u64, used: u64) {
        sm.nodes.get_mut(&node).unwrap().disks.push(dtfs_common::types::DiskInfo {
            disk_id: id,
            uuid: String::new(),
            mount_path: String::new(),
            tier,
            total_bytes: total,
            used_bytes: used,
            status: DiskStatus::Healthy,
        });
    }

    /// MGR-1: with no Healthy maintained node the clamp must not reach 0
    /// (that acked a write on cache-only disks with zero durable copies).
    #[test]
    fn clamp_never_drops_to_zero_and_cache_only_commit_is_rejected() {
        let mut sm = live_topology();
        for id in [1, 2, 3] {
            sm.nodes.get_mut(&id).unwrap().health = HealthStatus::Suspect;
        }
        sm.nodes.insert(4, lab_node("data04", "toro", DiskTier::Hot, HealthStatus::Healthy));
        let floor = clamp_ack_floor_to_capacity(2, &sm, "default", None, "/a", None);
        assert_eq!(floor, 2, "no capacity: keep the configured floor, never 0");
        let have = min_maintained_replicas(&sm, "default", &[ent("4")]);
        assert_eq!(have, 0);
        assert!((have as u32) < floor, "cache-only commit must be rejected");
    }

    /// Capacity must use the fill's per-node rule: a warm node with a hot
    /// RF-0 cache disk beside its warm disk is never a fill target for a
    /// cold placement, so it must not count as capacity either (else the
    /// floor stays 2 and every commit is rejected forever).
    #[test]
    fn capacity_matches_fill_on_mixed_cache_node() {
        let mut sm = StateMachineData::default();
        let mut vol = vol_with_floor("mixed-vol", 2);
        vol.mandatory_replicas =
            dtfs_common::types::TierReplication { hot: 0, warm: 2, cold: 1, tape: 0 };
        sm.volumes.insert("mixed-vol".to_string(), vol);
        sm.nodes.insert(1, lab_node("data01", "leli", DiskTier::Cold, HealthStatus::Healthy));
        sm.nodes.insert(2, lab_node("data02", "toro", DiskTier::Warm, HealthStatus::Healthy));
        add_disk(&mut sm, 2, 1, DiskTier::Hot, 1 << 40, 0);
        let v = &sm.volumes["mixed-vol"];
        let mand = v.effective_mandatory_replicas();
        let fill_targets = fill_floor_targets(
            &sm.nodes, &mand, &v.spill,
            &dtfs_common::config::PlacementConfig::default(),
            &Location::new("", "", "", ""), DiskTier::Cold,
            &["data01:9500".to_string()], 2, &std::collections::HashSet::new(),
        );
        assert!(fill_targets.is_empty(), "fill cannot target the mixed node");
        assert_eq!(maintained_node_capacity(&sm, &mand, &v.spill, Some(DiskTier::Cold)), 1);
        assert_eq!(clamp_ack_floor_to_capacity(2, &sm, "mixed-vol", Some(DiskTier::Cold), "/a", None), 1);
        assert!(WARNED_FLOOR_CLAMPED.get().unwrap().lock().unwrap().contains("mixed-vol"));
        assert!(min_maintained_replicas(&sm, "mixed-vol", &[ent("1")]) >= 1);
        // Normal single-tier topology still needs 2.
        let sm2 = live_topology();
        let v2 = &sm2.volumes["default"];
        assert_eq!(
            maintained_node_capacity(&sm2, &v2.effective_mandatory_replicas(), &v2.spill, Some(DiskTier::Cold)),
            3
        );
        assert_eq!(clamp_ack_floor_to_capacity(2, &sm2, "default", Some(DiskTier::Cold), "/a", None), 2);
    }

    /// The commit side derives the placement tier from the first resolvable
    /// replica's disk.
    #[test]
    fn placed_tier_is_the_first_replicas_disk_tier() {
        let sm = live_topology();
        assert_eq!(placed_tier_of(&sm, &[ent("1"), ent("2")]), Some(DiskTier::Cold));
        assert_eq!(placed_tier_of(&sm, &[ent("2")]), Some(DiskTier::Warm));
        assert_eq!(placed_tier_of(&sm, &[ent("99")]), None);
        assert_eq!(placed_tier_of(&sm, &[]), None);
    }

    /// F1: spill rules govern extra placement, not the anchor copy. A volume
    /// forbidding its own anchor DC for spill must still count the anchor
    /// node as capacity, else one warm node down clamps the floor to 1 and
    /// a warm-only commit (no anchor copy) could be acked.
    #[test]
    fn forbidden_anchor_dc_still_counts_anchor_node_as_capacity() {
        let mut sm = live_topology();
        sm.volumes.get_mut("default").unwrap().spill.forbidden_datacenters = vec!["LELI".into()];
        sm.nodes.get_mut(&3).unwrap().health = HealthStatus::Down;
        let floor = clamp_ack_floor_to_capacity(2, &sm, "default", Some(DiskTier::Cold), "/a", None);
        assert_eq!(floor, 2, "anchor (data01) + remaining warm (data02) = 2");
        // A forbidden NON-anchor DC is still excluded from capacity.
        sm.volumes.get_mut("default").unwrap().spill.forbidden_datacenters = vec!["toro".into()];
        assert_eq!(
            clamp_ack_floor_to_capacity(2, &sm, "default", Some(DiskTier::Cold), "/a", None),
            1,
            "data02 (toro) forbidden, data03 down: only the anchor remains"
        );
    }

    /// MGR-4: the warn-once latch re-arms after the volume stops clamping.
    #[test]
    fn clamp_warning_latch_rearms_after_recovery() {
        let latched = |v: &str| {
            WARNED_FLOOR_CLAMPED
                .get()
                .map(|s| s.lock().unwrap().contains(v))
                .unwrap_or(false)
        };
        let v = "mgr4-vol";
        let mut sm = StateMachineData::default();
        sm.volumes.insert(v.to_string(), vol_with_floor(v, 2));
        sm.nodes.insert(1, lab_node("data01", "leli", DiskTier::Cold, HealthStatus::Healthy));
        assert_eq!(clamp_ack_floor_to_capacity(2, &sm, v, None, "/a", None), 1);
        assert!(latched(v));
        sm.nodes.insert(2, lab_node("data02", "leli", DiskTier::Cold, HealthStatus::Healthy));
        assert_eq!(clamp_ack_floor_to_capacity(2, &sm, v, None, "/a", None), 2);
        assert!(!latched(v), "recovered volume clears the latch");
        sm.nodes.remove(&2);
        assert_eq!(clamp_ack_floor_to_capacity(2, &sm, v, None, "/a", None), 1);
        assert!(latched(v), "a later clamp episode warns again");
    }

    /// MGR-3: a fill node is accepted only if the disk the data node will
    /// pick is maintained with room. data05 has a big hot (RF 0) disk plus
    /// a small warm one and no cold disk, so `select_disk_for_tier(cold)`
    /// may pick the hot disk: not a target.
    #[test]
    fn floor_fill_rejects_node_whose_chosen_disk_may_be_cache_only() {
        let mut sm = live_topology();
        sm.nodes.get_mut(&2).unwrap().health = HealthStatus::Maintenance;
        sm.nodes.get_mut(&3).unwrap().health = HealthStatus::Maintenance;
        sm.nodes.insert(5, lab_node("data05", "toro", DiskTier::Warm, HealthStatus::Healthy));
        add_disk(&mut sm, 5, 1, DiskTier::Hot, 1 << 40, 0);
        assert!(fill(&sm, &["data01:9500"], 2).is_empty());
    }

    /// MGR-3: an exact placed-tier disk wins the data node's pick, so a
    /// hot disk beside it is irrelevant; but a full disk among the
    /// possible picks disqualifies the node.
    #[test]
    fn floor_fill_follows_the_data_node_disk_choice_rule() {
        let mut sm = live_topology();
        sm.nodes.get_mut(&2).unwrap().health = HealthStatus::Maintenance;
        sm.nodes.get_mut(&3).unwrap().health = HealthStatus::Maintenance;
        // Cold exact match + hot cache disk: pick is the cold disk.
        sm.nodes.insert(6, lab_node("data06", "toro", DiskTier::Cold, HealthStatus::Healthy));
        add_disk(&mut sm, 6, 1, DiskTier::Hot, 1 << 40, 0);
        assert_eq!(fill(&sm, &["data01:9500"], 2), vec!["data06:9500".to_string()]);
        // Two warm disks (no cold), one full: either may be picked.
        sm.nodes.get_mut(&6).unwrap().health = HealthStatus::Maintenance;
        sm.nodes.insert(7, lab_node("data07", "toro", DiskTier::Warm, HealthStatus::Healthy));
        add_disk(&mut sm, 7, 1, DiskTier::Warm, 1 << 30, 1 << 30);
        assert!(fill(&sm, &["data01:9500"], 2).is_empty());
    }

    /// MGR-2: capacity-spill candidates never include cache-only (RF 0)
    /// tiers, so a non-durable spill fails at allocate time.
    #[test]
    fn spill_candidates_exclude_cache_only_tiers() {
        let mut sm = live_topology();
        sm.nodes.insert(4, lab_node("data04", "toro", DiskTier::Hot, HealthStatus::Healthy));
        let vol = &sm.volumes["default"];
        let got = spill_candidates(
            &sm.nodes,
            DiskTier::Cold,
            &std::collections::HashSet::new(),
            &dtfs_common::config::PlacementConfig::default(),
            &vol.spill,
            &vol.effective_mandatory_replicas(),
        );
        let mut nodes: Vec<u64> = got.iter().map(|c| c.node_id).collect();
        nodes.sort();
        assert_eq!(nodes, vec![2, 3], "only the warm (RF 2) nodes; hot RF 0 dropped");
        assert!(got.iter().all(|c| c.tier == DiskTier::Warm));
    }

    /// MGR-2: after a spill to a maintained tier with a single node, the
    /// fill (placed tier = spill tier) adds the second warm node for floor 2.
    #[test]
    fn spill_to_warm_gets_fill_target_for_floor_two() {
        let sm = live_topology();
        let vol = &sm.volumes["default"];
        let got = fill_floor_targets(
            &sm.nodes, &vol.effective_mandatory_replicas(), &vol.spill,
            &dtfs_common::config::PlacementConfig::default(),
            &Location::new("", "", "", ""), DiskTier::Warm,
            &["data02:9500".to_string()], 2, &std::collections::HashSet::new(),
        );
        assert_eq!(got.len(), 1);
        assert_ne!(got[0], "data02:9500");
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
            cache_cooldown_until_ms: std::collections::BTreeMap::new(), ever_committed: None,
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
            spill: SpillPolicy::default(),
            version: 0,
            anchor_datacenter: None,
            write_safety: WriteSafety {
                min_replicas_before_ack: floor,
                write_timeout_secs: 30,
            },
        }
    }
}

mod mgr_hb_liveness_write_status {
    use super::super::liveness_write_status;
    use dtfs_raft::ClientResponse;
    use tonic::{Code, Status};

    #[test]
    fn success_is_ok() {
        assert!(liveness_write_status(Ok(ClientResponse::Success)).is_ok());
    }

    #[test]
    fn unknown_node_apply_error_is_not_found() {
        let r = liveness_write_status(Ok(ClientResponse::Error {
            message: "node not found: 42".into(),
        }));
        assert_eq!(r.unwrap_err().code(), Code::NotFound);
    }

    #[test]
    fn other_apply_error_is_internal() {
        let r = liveness_write_status(Ok(ClientResponse::Error {
            message: "boom".into(),
        }));
        assert_eq!(r.unwrap_err().code(), Code::Internal);
    }

    #[test]
    fn quarantine_refusal_is_ok() {
        let r = liveness_write_status(Ok(ClientResponse::Error {
            message: "UpdateNodeStatus: node 7 is quarantined".into(),
        }));
        assert!(r.is_ok());
    }

    #[test]
    fn forward_failure_passes_through_unavailable() {
        let r = liveness_write_status(Err(Status::unavailable("no leader")));
        assert_eq!(r.unwrap_err().code(), Code::Unavailable);
    }
}

mod mgr_loc_replica_is_live {
    use super::super::replica_is_live;
    use dtfs_common::types::{DiskStatus, DiskTier, HealthStatus, NodeRole};
    use dtfs_raft::state_machine::StateMachineData;
    use dtfs_common::types::DiskInfo;
    use dtfs_raft::{ChunkLocationEntry, NodeInfo};

    fn sm_with(health: HealthStatus, disk_status: DiskStatus) -> StateMachineData {
        let mut sm = StateMachineData::default();
        sm.nodes.insert(
            7,
            NodeInfo {
                hostname: "d".into(),
                grpc_addr: "http://d:9500".into(),
                role: NodeRole::Data,
                disks: vec![DiskInfo {
                    disk_id: 0,
                    uuid: String::new(),
                    mount_path: "/d0".into(),
                    tier: DiskTier::Cold,
                    total_bytes: 1,
                    used_bytes: 0,
                    status: disk_status,
                }],
                location: dtfs_common::topology::Location::new("", "", "", ""),
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
            },
        );
        sm
    }

    fn row(disk_id: u32) -> ChunkLocationEntry {
        ChunkLocationEntry {
            chunk_index: 0,
            chunk_hash: "h".into(),
            node_id: "7".into(),
            disk_id,
            size_bytes: 1,
        }
    }

    #[test]
    fn healthy_node_healthy_disk_is_live() {
        assert!(replica_is_live(&sm_with(HealthStatus::Healthy, DiskStatus::Healthy), &row(0)));
    }

    #[test]
    fn down_node_is_dead() {
        assert!(!replica_is_live(&sm_with(HealthStatus::Down, DiskStatus::Healthy), &row(0)));
    }

    #[test]
    fn failed_disk_is_dead_even_on_healthy_node() {
        assert!(!replica_is_live(&sm_with(HealthStatus::Healthy, DiskStatus::Failed), &row(0)));
    }

    #[test]
    fn suspect_node_still_counts_as_live() {
        assert!(replica_is_live(&sm_with(HealthStatus::Suspect, DiskStatus::Healthy), &row(0)));
    }

    #[test]
    fn unknown_node_is_dead() {
        let sm = StateMachineData::default();
        assert!(!replica_is_live(&sm, &row(0)));
    }

    #[test]
    fn non_numeric_node_id_is_not_live() {
        let sm = sm_with(HealthStatus::Healthy, DiskStatus::Healthy);
        let mut r = row(0);
        r.node_id = "http://gone:9500".into();
        assert!(!replica_is_live(&sm, &r));
    }

    #[test]
    fn created_layout_prefers_stored_entry() {
        use super::super::created_layout;
        assert_eq!(
            created_layout(Some(("old".into(), 8)), "new".into(), 4),
            ("old".to_string(), 8)
        );
        assert_eq!(created_layout(None, "new".into(), 4), ("new".to_string(), 4));
    }

    #[test]
    fn unresolved_commit_nodes_lists_only_addresses() {
        let mut a = row(0);
        a.node_id = "7".into();
        let mut b = row(0);
        b.node_id = "10.0.0.9:9500".into();
        let got = super::super::unresolved_commit_nodes(&[a.clone(), b.clone(), b]);
        assert_eq!(got, vec!["10.0.0.9:9500".to_string()]);
        assert!(super::super::unresolved_commit_nodes(&[a]).is_empty());
    }

    #[test]
    fn legacy_disk_sentinel_judges_node_only() {
        let sm = sm_with(HealthStatus::Healthy, DiskStatus::Failed);
        assert!(replica_is_live(&sm, &row(dtfs_raft::legacy_disk_id())));
    }
}
