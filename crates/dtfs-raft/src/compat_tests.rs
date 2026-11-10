//! Wire / on-disk compat guards (CLAUDE.md § A.3.1).
//!
//! Fixtures under `testdata/` are APPEND-ONLY: never regenerate an existing
//! `*_v1.json`. When the format grows, add `*_v2.json` alongside (via the
//! `#[ignore]` generators below) and keep the v1 tests, so old snapshots and
//! log entries stay loadable. A failing v1 test means a compat break.

use std::collections::{BTreeMap, BTreeSet};
use std::io;

use dtfs_common::topology::Location;
use dtfs_common::types::{
    AnchorPolicy, DiskInfo, DiskStatus, DiskTier, HealthStatus, NodeRole, QuarantineInfo,
    QuarantineReason, VolumeConfig,
};

use crate::snapshot::{deserialize_snapshot, serialize_snapshot, StateMachineSnapshot};
use crate::state_machine::{
    AuditEntry, ClusterConfig, DistributedLock, FileMetadata, ScrubRecord, WriteLease,
};
use crate::{ChunkLocationEntry, ClientRequest, NodeInfo, PendingScrub, StoredMembership};

const SNAPSHOT_V1: &str = include_str!("../testdata/snapshot_v1.json");
const REQUESTS_V1: &str = include_str!("../testdata/client_requests_v1.json");
/// Hand-written: entries whose wire shape grew after v1 (append-only).
const REQUESTS_V2: &str = include_str!("../testdata/client_requests_v2.json");

fn chunk(i: u64) -> ChunkLocationEntry {
    ChunkLocationEntry {
        chunk_index: i,
        chunk_hash: format!("{:064x}", i + 1),
        node_id: "7".into(),
        disk_id: 1,
        size_bytes: 4096,
    }
}

fn node_info() -> NodeInfo {
    NodeInfo {
        role: NodeRole::Data,
        hostname: "data-primary01".into(),
        grpc_addr: "10.0.0.7:9200".into(),
        location: Location::new("eu", "LELI", "r1", "data-primary01"),
        disks: vec![DiskInfo {
            disk_id: 1,
            uuid: "uuid-1".into(),
            mount_path: "/srv/d1".into(),
            tier: DiskTier::Cold,
            total_bytes: 1000,
            used_bytes: 10,
            status: DiskStatus::Healthy,
        }],
        total_capacity: 1000,
        used_capacity: 10,
        health: HealthStatus::Quarantined,
        last_seen_ms: 111,
        total_bytes_read: 5,
        total_bytes_written: 6,
        client_ip: Some("10.0.0.9".into()),
        version: "v1.2.3".into(),
        build_date: "20260101-000000".into(),
        quarantine: Some(quarantine()),
    }
}

fn quarantine() -> QuarantineInfo {
    QuarantineInfo {
        since_ms: 1,
        reason: QuarantineReason::DeepScrubHashMismatch,
        triggering_chunk_id: Some("abc".into()),
        fault_count_total: 2,
        last_fault_at_ms: 3,
        last_fault_summary: "bad hash".into(),
    }
}

fn scrub(level: &str) -> ScrubRecord {
    ScrubRecord {
        node_id: 7,
        disk_path: "/srv/d1".into(),
        level: level.into(),
        completed_at_ms: 99,
        chunks_checked: 10,
        chunks_healthy: 8,
        chunks_corrupt: 1,
        chunks_missing: 1,
        duration_secs: 1.5,
    }
}

fn full_snapshot() -> StateMachineSnapshot {
    let mut cooldown = BTreeMap::new();
    cooldown.insert(0u64, 5000u64);
    let file = FileMetadata {
        path: "/media/a.mkv".into(),
        volume_id: "default".into(),
        chunk_size: 134217728,
        chunks: vec![chunk(0), chunk(1)],
        created_at_ms: 10,
        updated_at_ms: 20,
        symlink_target: Some("/media/b.mkv".into()),
        logical_size: Some(8192),
        is_directory: false,
        cache_cooldown_until_ms: cooldown,
        ever_committed: None,
    };
    let mut snap = StateMachineSnapshot {
        last_applied_log: Some((3, 42)),
        last_membership: Some(serde_json::to_value(StoredMembership::default()).unwrap()),
        files: BTreeMap::new(),
        nodes: BTreeMap::new(),
        cluster_config: ClusterConfig { config_json: "{\"k\":1}".into(), version: 4 },
        audit_log: vec![AuditEntry {
            log_index: 40,
            summary: "volume.create: v1".into(),
            timestamp_ms: 77,
            actor: "admin".into(),
            action: "volume.create".into(),
            target: "v1".into(),
            result: "success".into(),
            origin_manager: "mgr01".into(),
        }],
        scrub_results: BTreeMap::new(),
        scrub_results_by_level: BTreeMap::new(),
        client_count: 2,
        volumes: BTreeMap::new(),
        migration_state_json: "{\"tasks\":[]}".into(),
        posix_locks: BTreeMap::new(),
        write_leases: BTreeMap::new(),
        pending_scrub: Some(PendingScrub { id: "s1".into(), level: "deep".into(), requested_at_ms: 5 }),
    };
    snap.files.insert(file.path.clone(), file);
    snap.nodes.insert(7, node_info());
    snap.scrub_results.insert(7, scrub("quick"));
    snap.scrub_results_by_level
        .insert(7, BTreeMap::from([("quick".to_string(), scrub("quick")), ("deep".to_string(), scrub("deep"))]));
    let mut vol = VolumeConfig::default();
    vol.anchor = Some(AnchorPolicy { datacenter: "LELI".into(), required_tier: Some(DiskTier::Cold) });
    snap.volumes.insert("default".into(), vol);
    snap.posix_locks.insert(
        "/media/a.mkv".into(),
        vec![DistributedLock { owner_id: "o1".into(), start: 0, end: 10, write: true, expires_at_ms: 9 }],
    );
    snap.write_leases.insert(
        "/media/a.mkv".into(),
        WriteLease { owner_id: "o1".into(), lease_id: "l1".into(), expires_at_ms: 8 },
    );
    snap
}

/// One instance per `ClientRequest` variant.
fn all_requests() -> Vec<ClientRequest> {
    use ClientRequest::*;
    let s = |x: &str| x.to_string();
    vec![
        CreateFile { path: s("/a"), volume_id: s("default"), chunk_size: 1024, now_ms: 0 },
        DeleteFile { path: s("/a"), now_ms: 5 },
        UpdateChunkLocations { path: s("/a"), chunks: vec![chunk(0)], lease_id: s("l1"), now_ms: 5 },
        RemoveCorruptChunkReplicas { node_id: s("7"), chunk_hashes: vec![s("ab")], disk_id: 1, now_ms: 5 },
        RegisterNode { node_id: 7, info: node_info(), now_ms: 5 },
        DeregisterNode { node_id: 7, now_ms: 5 },
        UpdateNodeStatus { node_id: 7, status: s("down"), now_ms: 5 },
        QuarantineNode { node_id: 7, info: quarantine(), now_ms: 5 },
        UnquarantineNode { node_id: 7, operator_id: s("op"), reason_acknowledged: s("fixed"), now_ms: 5 },
        UpdateNodeUsage {
            node_id: 7, used_capacity: 1, chunk_count: 2, total_bytes_read: 3, total_bytes_written: 4,
            observed_at_ms: 5,
        },
        UpdateConfig { config_json: s("{}"), version: 2, expected_version: 1, now_ms: 5 },
        UpsertVolume { config: VolumeConfig::default(), expected_version: None, now_ms: 5 },
        DeleteVolume { volume_id: s("default"), now_ms: 5 },
        ReportScrubResult {
            node_id: 7, disk_path: s("/d"), level: s("quick"), completed_at_ms: 1, chunks_checked: 2,
            chunks_healthy: 3, chunks_corrupt: 4, chunks_missing: 5, duration_secs: 0.5, now_ms: 5,
        },
        UpdateMigrationState { json: s("{}") },
        RenameFile { from: s("/a"), to: s("/b"), now_ms: 5 },
        SetFileSize { path: s("/a"), new_size: 10, now_ms: 5 },
        CreateSymlink { path: s("/l"), target: s("/a"), now_ms: 5 },
        CreateLink { source: s("/a"), dest: s("/c"), now_ms: 5 },
        AcquireLock { path: s("/a"), owner_id: s("o"), start: 0, end: 9, write: true, lease_ms: 1000, now_ms: 5 },
        ReleaseLock { path: s("/a"), owner_id: s("o"), start: 0, end: 9 },
        AcquireWriteLease { path: s("/a"), owner_id: s("o"), lease_ms: 1000, now_ms: 5, proposed_lease_id: s("l1") },
        RenewWriteLease { path: s("/a"), lease_id: s("l1"), extension_ms: 1000, now_ms: 5 },
        ReleaseWriteLease { path: s("/a"), lease_id: s("l1"), now_ms: 5 },
        ReleaseLeasesByHost { hostname_prefix: s("h"), except_pid: 1, now_ms: 5 },
        SweepExpiredLeases { now_ms: 5, sweep_posix_locks: false },
        ReportChunkReads { node_id: 7, entries: vec![(s("c1"), [(s("LELI"), 3u32)].into_iter().collect())] },
        TriggerScrub { id: s("s1"), level: s("deep"), requested_at_ms: 5 },
        UpdateNodeDiskUsage { node_id: 7, per_disk: vec![(1, 100, 10)] },
        AppendAudit {
            actor: s("a"), action: s("x.y"), target: s("t"), result: s("success"),
            origin_manager: s("mgr01"), timestamp_ms: 5,
        },
        RemoveChunkLocation {
            path: s("/a"), chunk_index: 0, node_id: s("7"), disk_id: 1,
            enforce_anchor: Some(AnchorPolicy { datacenter: s("LELI"), required_tier: None }), now_ms: 5,
            chunk_hash: None,
        },
        SetChunkCooldown { path: s("/a"), chunk_indexes: vec![0, 1], expires_at_ms: 9 },
    ]
}

/// Exhaustive (no wildcard): adding a `ClientRequest` variant breaks the build
/// here, forcing an update of this fn, `all_requests`, and a `_v2` fixture.
fn variant_name(r: &ClientRequest) -> &'static str {
    use ClientRequest::*;
    match r {
        CreateFile { .. } => "CreateFile",
        DeleteFile { .. } => "DeleteFile",
        UpdateChunkLocations { .. } => "UpdateChunkLocations",
        RemoveCorruptChunkReplicas { .. } => "RemoveCorruptChunkReplicas",
        RegisterNode { .. } => "RegisterNode",
        DeregisterNode { .. } => "DeregisterNode",
        UpdateNodeStatus { .. } => "UpdateNodeStatus",
        QuarantineNode { .. } => "QuarantineNode",
        UnquarantineNode { .. } => "UnquarantineNode",
        UpdateNodeUsage { .. } => "UpdateNodeUsage",
        UpdateConfig { .. } => "UpdateConfig",
        UpsertVolume { .. } => "UpsertVolume",
        DeleteVolume { .. } => "DeleteVolume",
        ReportScrubResult { .. } => "ReportScrubResult",
        UpdateMigrationState { .. } => "UpdateMigrationState",
        RenameFile { .. } => "RenameFile",
        SetFileSize { .. } => "SetFileSize",
        CreateSymlink { .. } => "CreateSymlink",
        CreateLink { .. } => "CreateLink",
        AcquireLock { .. } => "AcquireLock",
        ReleaseLock { .. } => "ReleaseLock",
        AcquireWriteLease { .. } => "AcquireWriteLease",
        RenewWriteLease { .. } => "RenewWriteLease",
        ReleaseWriteLease { .. } => "ReleaseWriteLease",
        ReleaseLeasesByHost { .. } => "ReleaseLeasesByHost",
        SweepExpiredLeases { .. } => "SweepExpiredLeases",
        ReportChunkReads { .. } => "ReportChunkReads",
        TriggerScrub { .. } => "TriggerScrub",
        UpdateNodeDiskUsage { .. } => "UpdateNodeDiskUsage",
        AppendAudit { .. } => "AppendAudit",
        RemoveChunkLocation { .. } => "RemoveChunkLocation",
        SetChunkCooldown { .. } => "SetChunkCooldown",
        RemoveDeadNodeReplica { .. } => "RemoveDeadNodeReplica",
    }
}

/// Added after the v1 fixtures were cut: not in `all_requests`/v1, so its
/// own wire shape is pinned here (an entry without `now_ms` defaults to 0).
#[test]
fn remove_dead_node_replica_wire_shape() {
    let r: ClientRequest = serde_json::from_str(
        r#"{"RemoveDeadNodeReplica":{"path":"/a","chunk_index":2,"node_id":"7","disk_id":1}}"#,
    )
    .unwrap();
    match r {
        ClientRequest::RemoveDeadNodeReplica { path, chunk_index, node_id, disk_id, now_ms } => {
            assert_eq!((path.as_str(), chunk_index, node_id.as_str(), disk_id, now_ms), ("/a", 2, "7", 1, 0));
        }
        other => panic!("wrong variant: {}", variant_name(&other)),
    }
}

fn testdata(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata").join(name)
}

/// Writes the v1 fixtures. Run once; NEVER re-run over committed v1 files.
#[test]
#[ignore]
fn generate_v1_fixtures() {
    std::fs::create_dir_all(testdata("")).unwrap();
    let snap = serialize_snapshot(&full_snapshot()).unwrap().into_inner();
    let v: serde_json::Value = serde_json::from_slice(&snap).unwrap();
    std::fs::write(testdata("snapshot_v1.json"), serde_json::to_string_pretty(&v).unwrap() + "\n").unwrap();
    let reqs = serde_json::to_string_pretty(&all_requests()).unwrap() + "\n";
    std::fs::write(testdata("client_requests_v1.json"), reqs).unwrap();
}

#[test]
fn snapshot_v1_fixture_loads() {
    let snap = deserialize_snapshot(io::Cursor::new(SNAPSHOT_V1.as_bytes().to_vec())).expect("v1 snapshot must load");
    assert_eq!(snap.last_applied_log, Some((3, 42)));
    assert_eq!(snap.files["/media/a.mkv"].chunks.len(), 2);
    assert_eq!(snap.files["/media/a.mkv"].cache_cooldown_until_ms[&0], 5000);
    assert_eq!(snap.nodes[&7].disks[0].uuid, "uuid-1");
    assert_eq!(snap.nodes[&7].quarantine.as_ref().unwrap().fault_count_total, 2);
    assert_eq!(snap.cluster_config.version, 4);
    assert_eq!(snap.audit_log[0].origin_manager, "mgr01");
    assert_eq!(snap.scrub_results_by_level[&7].len(), 2);
    assert_eq!(snap.volumes["default"].anchor.as_ref().unwrap().datacenter, "LELI");
    assert_eq!(snap.posix_locks["/media/a.mkv"][0].end, 10);
    assert_eq!(snap.write_leases["/media/a.mkv"].lease_id, "l1");
    assert_eq!(snap.pending_scrub.unwrap().id, "s1");
    assert_eq!(snap.migration_state_json, "{\"tasks\":[]}");
    assert_eq!(snap.client_count, 2);
}

/// The v1 fixtures predate `FileMetadata.ever_committed` and
/// `CreateFile.now_ms`: they load as "unknown" (None) and legacy `0`.
#[test]
fn v1_fixtures_default_ever_committed_and_create_now_ms() {
    let snap = deserialize_snapshot(io::Cursor::new(SNAPSHOT_V1.as_bytes().to_vec())).unwrap();
    assert!(snap.files.values().all(|f| f.ever_committed.is_none()));
    let reqs: Vec<ClientRequest> = serde_json::from_str(REQUESTS_V1).unwrap();
    let create = reqs.iter().find_map(|r| match r {
        ClientRequest::CreateFile { now_ms, .. } => Some(*now_ms),
        _ => None,
    });
    assert_eq!(create, Some(0));
}

/// Every top-level snapshot field except the original required six must be
/// `#[serde(default)]`: a pre-growth snapshot carries only those six.
/// (`StateMachineData` itself is never serialised, and its non-defaulted
/// fields make `{}` fail by design; `StateMachineSnapshot` is the persisted form.)
#[test]
fn snapshot_minimal_legacy_json_loads_with_defaults() {
    let json = r#"{"last_applied_log":null,"last_membership":null,"files":{},"nodes":{},
        "cluster_config":{"config_json":"","version":0},"audit_log":[]}"#;
    let snap = deserialize_snapshot(io::Cursor::new(json.as_bytes().to_vec())).unwrap();
    assert!(snap.scrub_results.is_empty() && snap.scrub_results_by_level.is_empty());
    assert!(snap.volumes.is_empty() && snap.posix_locks.is_empty() && snap.write_leases.is_empty());
    assert!(snap.pending_scrub.is_none());
    assert_eq!((snap.client_count, snap.migration_state_json.as_str()), (0, ""));
}

#[test]
fn snapshot_missing_required_field_is_invalid_data() {
    // `files` is not defaulted.
    let json = r#"{"last_applied_log":null,"last_membership":null,"nodes":{},
        "cluster_config":{"config_json":"","version":0},"audit_log":[]}"#;
    let err = deserialize_snapshot(io::Cursor::new(json.as_bytes().to_vec())).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn snapshot_wrong_type_is_invalid_data() {
    let json = r#"{"last_applied_log":null,"last_membership":null,"files":{},"nodes":{},
        "cluster_config":{"config_json":"","version":0},"audit_log":[],"client_count":"two"}"#;
    let err = deserialize_snapshot(io::Cursor::new(json.as_bytes().to_vec())).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn client_requests_v1_fixture_covers_every_variant() {
    let reqs: Vec<ClientRequest> = serde_json::from_str(REQUESTS_V1).expect("every v1 entry must deserialize");
    let fixture: BTreeSet<&str> = reqs.iter().map(variant_name).collect();
    let live: BTreeSet<&str> = all_requests().iter().map(variant_name).collect();
    assert_eq!(fixture.len(), reqs.len(), "duplicate variant in fixture");
    assert_eq!(fixture, live, "ClientRequest variant set drifted from client_requests_v1.json; add client_requests_v2.json");
}

/// The v1 fixture predates `VolumeConfig.version` and
/// `UpsertVolume.expected_version`: both must default (0 / unconditional).
#[test]
fn upsert_volume_v1_entry_defaults_cas_fields() {
    let reqs: Vec<ClientRequest> = serde_json::from_str(REQUESTS_V1).unwrap();
    let up = reqs.iter().find_map(|r| match r {
        ClientRequest::UpsertVolume { config, expected_version, .. } => Some((config, expected_version)),
        _ => None,
    });
    let (config, expected) = up.expect("fixture has UpsertVolume");
    assert_eq!((config.version, *expected), (0, None));
    assert_eq!(snap_volume_version(), 0);
}

/// The v1 fixture predates the audit `now_ms` on these variants: it must
/// default to 0 (apply then falls back to `log_id.index`).
#[test]
fn audited_variants_v1_entries_default_now_ms_to_zero() {
    let reqs: Vec<ClientRequest> = serde_json::from_str(REQUESTS_V1).unwrap();
    let mut seen = 0;
    for r in &reqs {
        use ClientRequest::*;
        match r {
            DeleteFile { now_ms, .. } | RegisterNode { now_ms, .. } | DeregisterNode { now_ms, .. }
            | UpdateNodeStatus { now_ms, .. } | QuarantineNode { now_ms, .. }
            | UnquarantineNode { now_ms, .. } | UpdateConfig { now_ms, .. } | UpsertVolume { now_ms, .. }
            | DeleteVolume { now_ms, .. } | ReportScrubResult { now_ms, .. }
            | ReleaseWriteLease { now_ms, .. } => {
                assert_eq!(*now_ms, 0, "{}", variant_name(r));
                seen += 1;
            }
            _ => {}
        }
    }
    assert_eq!(seen, 11);
}

fn snap_volume_version() -> u64 {
    let snap = deserialize_snapshot(io::Cursor::new(SNAPSHOT_V1.as_bytes().to_vec())).unwrap();
    snap.volumes["default"].version
}

/// Apply one `SweepExpiredLeases` to a state holding one expired POSIX lock
/// and return whether the lock survived.
fn sweep_keeps_expired_lock(sweep: &ClientRequest) -> bool {
    use crate::state_machine::{apply_request, StateMachineData};
    use openraft::vote::RaftLeaderId;
    let lid = |i| openraft::LogId::new(openraft::impls::leader_id_adv::LeaderId::<u64, u64>::new(0, 0), i);
    let mut sm = StateMachineData::default();
    apply_request(&mut sm, &lid(1), &ClientRequest::AcquireLock {
        path: "/a".into(), owner_id: "crashed".into(), start: 0, end: 9, write: true, lease_ms: 1_000, now_ms: 1_000,
    });
    assert!(sm.posix_locks.contains_key("/a"));
    apply_request(&mut sm, &lid(2), sweep);
    sm.posix_locks.contains_key("/a")
}

/// RAFT-1 — a v1 `SweepExpiredLeases` (no `sweep_posix_locks`) replays as
/// old binaries applied it: write leases only, POSIX locks untouched.
#[test]
fn sweep_expired_leases_v1_entry_does_not_prune_locks() {
    let reqs: Vec<ClientRequest> = serde_json::from_str(REQUESTS_V1).unwrap();
    let sweep = reqs.iter().find(|r| matches!(r, ClientRequest::SweepExpiredLeases { .. })).unwrap();
    assert!(matches!(sweep, ClientRequest::SweepExpiredLeases { now_ms: 5, sweep_posix_locks: false }));
    let replay = ClientRequest::SweepExpiredLeases { now_ms: 5_000, sweep_posix_locks: false };
    assert!(sweep_keeps_expired_lock(&replay), "legacy sweep must not prune POSIX locks");
}

/// RAFT-1 — the new proposer's entry (`sweep_posix_locks: true`) prunes.
#[test]
fn sweep_expired_leases_v2_entry_prunes_locks() {
    let reqs: Vec<ClientRequest> = serde_json::from_str(REQUESTS_V2).unwrap();
    let ClientRequest::SweepExpiredLeases { now_ms, sweep_posix_locks } = &reqs[0] else {
        panic!("v2 fixture starts with SweepExpiredLeases");
    };
    assert_eq!((*now_ms, *sweep_posix_locks), (5_000, true));
    assert!(!sweep_keeps_expired_lock(&reqs[0]));
}

/// Audit #7 — a v1 `RemoveChunkLocation` (no `chunk_hash`) decodes to
/// `None`, the hash-blind match old proposers relied on.
#[test]
fn remove_chunk_location_v1_entry_defaults_chunk_hash_none() {
    let reqs: Vec<ClientRequest> = serde_json::from_str(REQUESTS_V1).unwrap();
    let hash = reqs.iter().find_map(|r| match r {
        ClientRequest::RemoveChunkLocation { chunk_hash, .. } => Some(chunk_hash.clone()),
        _ => None,
    });
    assert_eq!(hash, Some(None));
}

/// Audit #7 — the v2 entry carries the planned hash; applied to a row with
/// another hash at the same (path, index, node, disk) it removes nothing.
#[test]
fn remove_chunk_location_v2_entry_carries_chunk_hash() {
    use crate::state_machine::{apply_request, StateMachineData};
    use openraft::vote::RaftLeaderId;
    let reqs: Vec<ClientRequest> = serde_json::from_str(REQUESTS_V2).unwrap();
    let remove = reqs
        .iter()
        .find(|r| matches!(r, ClientRequest::RemoveChunkLocation { .. }))
        .expect("v2 fixture has RemoveChunkLocation");
    let ClientRequest::RemoveChunkLocation { chunk_hash, .. } = remove else { unreachable!() };
    assert_eq!(chunk_hash.as_deref(), Some(chunk(0).chunk_hash.as_str()));

    let mut sm = StateMachineData::default();
    let mut f = full_snapshot().files["/media/a.mkv"].clone();
    f.path = "/a".into();
    f.chunks = vec![ChunkLocationEntry { chunk_hash: "other".into(), ..chunk(0) }];
    sm.files.insert("/a".into(), f);
    let lid = openraft::LogId::new(openraft::impls::leader_id_adv::LeaderId::<u64, u64>::new(0, 0), 1);
    apply_request(&mut sm, &lid, remove);
    assert_eq!(sm.files["/a"].chunks.len(), 1, "a different hash must not be removed");
}

#[test]
fn client_request_unknown_variant_fails() {
    let err = serde_json::from_str::<ClientRequest>(r#"{"NoSuchVariant":{"path":"/a"}}"#).unwrap_err();
    assert!(err.is_data(), "expected a data error, got {err}");
    assert!(err.to_string().contains("unknown variant `NoSuchVariant`"), "{err}");
}
