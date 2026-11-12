//! Fill on read (`cache_replicas.fill = on_read`): the fill target handed to
//! a FUSE reader in GetFileLocations, and the CommitCacheFill validation.

use dtfs_common::config::PlacementConfig;
use dtfs_common::types::{CacheFill, DiskTier, HealthStatus, VolumeConfig};
use dtfs_proto::dtfs::manager::CommitCacheFillRequest;
use dtfs_raft::state_machine::StateMachineData;
use dtfs_raft::ChunkLocationEntry;
use tonic::Status;

const TIERS: [DiskTier; 4] = [DiskTier::Hot, DiskTier::Warm, DiskTier::Cold, DiskTier::Tape];

/// Same rule as `tiering::is_cache_tier`: cache max > 0 and not mandatory.
fn is_volume_cache_tier(vol: &VolumeConfig, t: DiskTier) -> bool {
    vol.cache_replicas.for_tier(t).max > 0 && vol.effective_mandatory_replicas().for_tier(t) == 0
}

/// Node-level survivor (`survivor_replica_tier` without the disk).
fn node_is_survivor(n: &dtfs_raft::NodeInfo) -> bool {
    n.quarantine.is_none()
        && !matches!(
            n.health,
            HealthStatus::Maintenance | HealthStatus::Quarantined | HealthStatus::Down | HealthStatus::Failed
        )
}

/// `(node_id, grpc_addr, tier)` a reader in `client_dc` fills to, or `None`:
/// volume not on_read, empty `client_dc`, no cache tier, or no eligible node
/// in the DC. Eligibility is the promotion target selection
/// (`node_selection_view` + `select_promotion_target`: not draining, a tier
/// disk with room, below the low watermark; most free, then lowest id) plus
/// survivor. Hottest cache tier first. Pure, no I/O.
pub(crate) fn cache_fill_target(
    sm: &StateMachineData,
    volume_id: &str,
    client_dc: &str,
    pcfg: &PlacementConfig,
    low_pct: u8,
) -> Option<(u64, String, DiskTier)> {
    if client_dc.is_empty() {
        return None;
    }
    let vol = sm.volumes.get(volume_id)?;
    if vol.cache_replicas.fill != CacheFill::OnRead {
        return None;
    }
    TIERS.into_iter().filter(|t| is_volume_cache_tier(vol, *t)).find_map(|t| {
        let views: Vec<_> = sm
            .nodes
            .iter()
            .filter(|(_, n)| node_is_survivor(n) && !n.grpc_addr.is_empty())
            .map(|(id, n)| crate::tiering::node_selection_view(*id, n, t, pcfg, low_pct))
            .collect();
        let id = crate::tiering::select_promotion_target(&views, client_dc)?;
        Some((id, sm.nodes[&id].grpc_addr.clone(), t))
    })
}

/// Validate a CommitCacheFill against `sm`. `Ok(Some(row))` = register this
/// row, `Ok(None)` = the row exists already (idempotent).
pub(crate) fn check_cache_fill(
    sm: &StateMachineData,
    req: &CommitCacheFillRequest,
) -> Result<Option<ChunkLocationEntry>, Status> {
    let file = sm
        .files
        .get(&req.path)
        .ok_or_else(|| Status::not_found(format!("cache fill: no file {}", req.path)))?;
    let vol = sm
        .volumes
        .get(&file.volume_id)
        .filter(|v| v.cache_replicas.fill == CacheFill::OnRead)
        .ok_or_else(|| Status::failed_precondition(format!("cache fill: volume {} is not fill=on_read", file.volume_id)))?;
    let row = file
        .chunks
        .iter()
        .find(|c| c.chunk_index == req.chunk_index && c.chunk_hash == req.chunk_id)
        .ok_or_else(|| {
            Status::failed_precondition(format!(
                "cache fill: chunk {} is not at index {} of {}",
                req.chunk_id, req.chunk_index, req.path
            ))
        })?;
    let node = sm
        .nodes
        .get(&req.node_id)
        .ok_or_else(|| Status::failed_precondition(format!("cache fill: unknown node {}", req.node_id)))?;
    if !node.location.datacenter.eq_ignore_ascii_case(&req.client_dc) {
        return Err(Status::failed_precondition(format!(
            "cache fill: node {} is in DC {:?}, reader in {:?}",
            req.node_id, node.location.datacenter, req.client_dc
        )));
    }
    let disk = node
        .disk(req.disk_id)
        .ok_or_else(|| Status::invalid_argument(format!("cache fill: node {} has no disk {}", req.node_id, req.disk_id)))?;
    if crate::scheduler::survivor_replica_tier(Some(node), req.disk_id).is_none() {
        return Err(Status::failed_precondition(format!(
            "cache fill: node {} disk {} is not a survivor",
            req.node_id, req.disk_id
        )));
    }
    let tier = disk.tier;
    if !is_volume_cache_tier(vol, tier) {
        return Err(Status::invalid_argument(format!(
            "cache fill: tier {tier} is not a cache tier of volume {}",
            file.volume_id
        )));
    }
    let node_key = req.node_id.to_string();
    let same_chunk = || {
        file.chunks
            .iter()
            .filter(|c| c.chunk_index == req.chunk_index && c.chunk_hash == req.chunk_id)
    };
    if same_chunk().any(|c| c.node_id == node_key && c.disk_id == req.disk_id) {
        return Ok(None);
    }
    // Live rows on the tier, in the reader's DC when per_dc (the planner's count).
    let per_dc = vol.cache_replicas.per_dc;
    let held = same_chunk()
        .filter(|c| {
            let n = c.node_id.parse::<u64>().ok().and_then(|id| sm.nodes.get(&id));
            crate::scheduler::live_replica_tier(n, c.disk_id) == Some(tier)
                && (!per_dc || n.is_some_and(|n| n.location.datacenter.eq_ignore_ascii_case(&req.client_dc)))
        })
        .count() as u32;
    let max = vol.cache_replicas.for_tier(tier).max;
    if held >= max {
        return Err(Status::failed_precondition(format!(
            "cache fill: chunk {} already has {held} {tier} cache copies (max {max})",
            req.chunk_id
        )));
    }
    Ok(Some(ChunkLocationEntry {
        chunk_index: req.chunk_index,
        chunk_hash: req.chunk_id.clone(),
        node_id: node_key,
        disk_id: req.disk_id,
        size_bytes: row.size_bytes,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dtfs_common::topology::Location;
    use dtfs_common::types::{CacheTierConfig, DiskInfo, DiskStatus, NodeRole, TierReplication};
    use dtfs_proto::dtfs::manager::manager_service_server::ManagerService;
    use dtfs_raft::state_machine::FileMetadata;
    use std::sync::Arc;
    use tonic::{Code, Request};

    /// `total_gb` / `used_gb` in GiB (below the default headroom a disk has no room).
    fn node(dc: &str, addr: &str, tier: DiskTier, total_gb: u64, used_gb: u64) -> dtfs_raft::NodeInfo {
        let (total, used) = (total_gb << 30, used_gb << 30);
        dtfs_raft::NodeInfo {
            role: NodeRole::Data,
            hostname: addr.to_string(),
            grpc_addr: addr.to_string(),
            location: Location::new("lab", dc, "r1", addr),
            disks: vec![DiskInfo {
                disk_id: 7,
                uuid: String::new(),
                mount_path: String::new(),
                tier,
                total_bytes: total,
                used_bytes: used,
                status: DiskStatus::Healthy,
            }],
            total_capacity: total,
            used_capacity: used,
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

    fn row(node_id: u64, disk_id: u32) -> ChunkLocationEntry {
        ChunkLocationEntry {
            chunk_index: 3,
            chunk_hash: "c3".to_string(),
            node_id: node_id.to_string(),
            disk_id,
            size_bytes: 1024,
        }
    }

    /// leli/cold anchor (1, holds the chunk), hun/warm 2 (more free) and 3,
    /// toro/warm 4. Volume: cold mandatory 1, warm cache max 1 per DC, on_read.
    fn sm() -> StateMachineData {
        let mut sm = StateMachineData::default();
        let mut v = VolumeConfig::default();
        v.mandatory_replicas = TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 };
        v.cache_replicas.warm = CacheTierConfig { min: 0, max: 1 };
        v.cache_replicas.fill = CacheFill::OnRead;
        v.cache_replicas.per_dc = true;
        sm.volumes.insert("default".into(), v);
        sm.nodes.insert(1, node("leli", "leli1:9500", DiskTier::Cold, 0, 0));
        sm.nodes.insert(2, node("hun", "hun2:9500", DiskTier::Warm, 1000, 100));
        sm.nodes.insert(3, node("hun", "hun3:9500", DiskTier::Warm, 1000, 300));
        sm.nodes.insert(4, node("toro", "toro4:9500", DiskTier::Warm, 1000, 0));
        sm.files.insert(
            "/m.mkv".into(),
            FileMetadata {
                path: "/m.mkv".into(),
                volume_id: "default".into(),
                chunk_size: 4 * 1024 * 1024,
                chunks: vec![row(1, 7)],
                created_at_ms: 0,
                updated_at_ms: 0,
                symlink_target: None,
                logical_size: None,
                is_directory: false,
                cache_cooldown_until_ms: Default::default(),
                ever_committed: None,
                replica_added_at_ms: 0,
            },
        );
        sm
    }

    fn target(sm: &StateMachineData, dc: &str, low: u8) -> Option<(u64, String, DiskTier)> {
        cache_fill_target(sm, "default", dc, &PlacementConfig::default(), low)
    }

    #[test]
    fn fill_target_picks_most_free_node_in_client_dc() {
        assert_eq!(target(&sm(), "HUN", 0), Some((2, "hun2:9500".into(), DiskTier::Warm)));
        assert_eq!(target(&sm(), "toro", 0), Some((4, "toro4:9500".into(), DiskTier::Warm)));
        // A non-survivor is skipped.
        let mut s = sm();
        s.nodes.get_mut(&2).unwrap().health = HealthStatus::Maintenance;
        assert_eq!(target(&s, "hun", 0).map(|t| t.0), Some(3));
    }

    #[test]
    fn no_fill_target_without_node_in_dc_or_room_or_cache_tier_or_on_read_or_dc() {
        assert_eq!(target(&sm(), "nrb", 0), None, "no node in the DC");
        assert_eq!(target(&sm(), "hun", 5), None, "both hun disks above the low watermark");
        assert_eq!(target(&sm(), "hun", 25).map(|t| t.0), Some(2), "below low: eligible");
        let mut s = sm();
        s.volumes.get_mut("default").unwrap().mandatory_replicas.warm = 1;
        assert_eq!(target(&s, "hun", 0), None, "warm is mandatory, not a cache tier");
        let mut s = sm();
        s.volumes.get_mut("default").unwrap().cache_replicas.fill = CacheFill::OnPromote;
        assert_eq!(target(&s, "hun", 0), None, "volume on_promote");
        assert_eq!(target(&sm(), "", 0), None, "empty client_dc");
    }

    fn req(node_id: u64, disk_id: u32) -> CommitCacheFillRequest {
        CommitCacheFillRequest {
            path: "/m.mkv".into(),
            chunk_index: 3,
            chunk_id: "c3".into(),
            node_id,
            disk_id,
            client_dc: "Hun".into(),
        }
    }

    fn code(sm: &StateMachineData, r: &CommitCacheFillRequest) -> Code {
        check_cache_fill(sm, r).unwrap_err().code()
    }

    #[test]
    fn check_accepts_cache_copy_in_reader_dc_and_is_idempotent() {
        let mut s = sm();
        assert_eq!(check_cache_fill(&s, &req(2, 7)).unwrap(), Some(row(2, 7)));
        s.files.get_mut("/m.mkv").unwrap().chunks.push(row(2, 7));
        assert_eq!(check_cache_fill(&s, &req(2, 7)).unwrap(), None, "same (node, disk): idempotent");
    }

    #[test]
    fn check_refusals_carry_exact_status() {
        let s = sm();
        let mut r = req(2, 7);
        r.path = "/nope".into();
        assert_eq!(code(&s, &r), Code::NotFound);

        let mut on_promote = sm();
        on_promote.volumes.get_mut("default").unwrap().cache_replicas.fill = CacheFill::OnPromote;
        assert_eq!(code(&on_promote, &req(2, 7)), Code::FailedPrecondition, "volume not on_read");

        let mut r = req(2, 7);
        r.chunk_id = "other".into();
        assert_eq!(code(&s, &r), Code::FailedPrecondition, "hash not at index");
        let mut r = req(2, 7);
        r.chunk_index = 4;
        assert_eq!(code(&s, &r), Code::FailedPrecondition, "no row at index");

        assert_eq!(code(&s, &req(9, 7)), Code::FailedPrecondition, "unknown node");
        let mut q = sm();
        q.nodes.get_mut(&2).unwrap().health = HealthStatus::Quarantined;
        assert_eq!(code(&q, &req(2, 7)), Code::FailedPrecondition, "not a survivor");
        assert_eq!(code(&s, &req(4, 7)), Code::FailedPrecondition, "node in another DC");

        assert_eq!(code(&s, &req(2, 8)), Code::InvalidArgument, "disk not on node");
        let mut cold = sm();
        cold.nodes.insert(5, node("hun", "hun5:9500", DiskTier::Cold, 0, 0));
        assert_eq!(code(&cold, &req(5, 7)), Code::InvalidArgument, "cold is not a cache tier");

        // Cap: hun already holds max (1) live warm copy on node 3.
        let mut full = sm();
        full.files.get_mut("/m.mkv").unwrap().chunks.push(row(3, 7));
        assert_eq!(code(&full, &req(2, 7)), Code::FailedPrecondition, "DC at cache max");
        // A dead copy does not count; a copy in another DC does not either (per_dc).
        full.nodes.get_mut(&3).unwrap().health = HealthStatus::Down;
        full.files.get_mut("/m.mkv").unwrap().chunks.push(row(4, 7));
        assert!(check_cache_fill(&full, &req(2, 7)).unwrap().is_some());
        // per_dc = false: the toro copy counts toward the global max.
        full.volumes.get_mut("default").unwrap().cache_replicas.per_dc = false;
        assert_eq!(code(&full, &req(2, 7)), Code::FailedPrecondition, "global cache max");
    }

    async fn service(secret: &str) -> (crate::api::test_support::Harness, super::super::MetadataService) {
        let h = crate::api::test_support::leader_harness().await;
        {
            let (mut cur, s) = (h.state.sm_data.write().await, sm());
            (cur.volumes, cur.nodes, cur.files) = (s.volumes, s.nodes, s.files);
        }
        let mut config = h.state.config.clone();
        config.cluster.shared_secret = secret.to_string();
        let svc = super::super::MetadataService::new(
            config,
            h.state.raft.clone(),
            h.state.sm_data.clone(),
            Arc::new(crate::placement::PlacementEngine::default()),
            dtfs_common::tls::TlsContext::plaintext(),
        );
        (h, svc)
    }

    /// The handler registers the row through Raft; a repeat is a no-op.
    #[tokio::test]
    async fn commit_cache_fill_registers_row_and_repeat_is_idempotent() {
        let (h, svc) = service("").await;
        svc.commit_cache_fill(Request::new(req(2, 7))).await.unwrap();
        svc.commit_cache_fill(Request::new(req(2, 7))).await.unwrap();
        {
            let sm = h.state.sm_data.read().await;
            let rows = &sm.files["/m.mkv"].chunks;
            assert_eq!(rows.iter().filter(|c| **c == row(2, 7)).count(), 1, "{rows:?}");
            assert_eq!(rows.len(), 2);
        }
        let e = svc.commit_cache_fill(Request::new(req(3, 7))).await.unwrap_err();
        assert_eq!(e.code(), Code::FailedPrecondition, "hun at max after the fill");
    }

    /// With a shared secret, an unstamped or wrongly bound request is refused.
    #[tokio::test]
    async fn commit_cache_fill_requires_mgr_rpc_auth() {
        use dtfs_common::crypto::{stamp_mgr_rpc_auth, DATA_RPC_AUTH_MAC, DATA_RPC_AUTH_TS};
        let (h, svc) = service("s3cret").await;
        let e = svc.commit_cache_fill(Request::new(req(2, 7))).await.unwrap_err();
        assert_eq!(e.code(), Code::PermissionDenied);
        let stamped = |r: CommitCacheFillRequest, binding: &str| {
            let (ts, mac) = stamp_mgr_rpc_auth("s3cret", "CommitCacheFill", binding).unwrap();
            let mut q = Request::new(r);
            q.metadata_mut().insert(DATA_RPC_AUTH_TS, ts.parse().unwrap());
            q.metadata_mut().insert(DATA_RPC_AUTH_MAC, mac.parse().unwrap());
            q
        };
        // Stamp bound to another disk: refused.
        let wrong = dtfs_common::crypto::cache_fill_binding("/m.mkv", 3, "c3", 2, 8);
        let e = svc.commit_cache_fill(stamped(req(2, 7), &wrong)).await.unwrap_err();
        assert_eq!(e.code(), Code::PermissionDenied);
        assert_eq!(h.state.sm_data.read().await.files["/m.mkv"].chunks.len(), 1);
        let ok = dtfs_common::crypto::cache_fill_binding("/m.mkv", 3, "c3", 2, 7);
        svc.commit_cache_fill(stamped(req(2, 7), &ok)).await.unwrap();
        assert_eq!(h.state.sm_data.read().await.files["/m.mkv"].chunks.len(), 2);
    }
}
