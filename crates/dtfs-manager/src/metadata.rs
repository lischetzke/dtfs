use std::collections::BTreeMap;
use std::sync::Arc;

use dtfs_common::config::ManagerConfig;
use dtfs_common::topology::Location;
use dtfs_common::types::{DiskStatus, DiskTier, HealthStatus, NodeRole};
use dtfs_proto::dtfs::manager::manager_service_client::ManagerServiceClient;
use dtfs_proto::dtfs::manager::manager_service_server::ManagerService;
use dtfs_proto::dtfs::manager::*;
use dtfs_raft::state_machine::StateMachineData;
use dtfs_raft::{
    ChunkLocationEntry, ClientRequest, ClientResponse, DtfsRaft, NodeInfo as RaftNodeInfo,
};
use tonic::{Request, Response, Status};
use tracing::info;

use crate::cluster::ClusterManager;
use crate::placement::PlacementEngine;

/// Bug BF-9 / BF-10 — warn-once gates for the three deprecated
/// `VolumeSchedules` fields. Each AtomicBool flips on first observation
/// so the warn lands once per manager process lifetime instead of every
/// heartbeat.
static WARNED_SCRUB_INTERVAL: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
static WARNED_FSCK_INTERVAL: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
static WARNED_CUSTOM_SCHEDULE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// The helper itself MUST read the deprecated fields to detect
// them. Suppress the deprecation warning at the read site only.
#[allow(deprecated)]
fn warn_once_on_dead_schedule_fields(
    volumes: &std::collections::BTreeMap<String, dtfs_common::types::VolumeConfig>,
) {
    use std::sync::atomic::Ordering;
    for v in volumes.values() {
        if v.schedules.scrub_interval_secs.is_some()
            && !WARNED_SCRUB_INTERVAL.swap(true, Ordering::Relaxed)
        {
            tracing::warn!(
                volume_id = %v.id,
                value = ?v.schedules.scrub_interval_secs,
                "Bug BF-9: VolumeSchedules.scrub_interval_secs is removed from \
                 the active path. Cluster-wide [scrub] block governs cadence. \
                 The field is preserved for snapshot compatibility but has no \
                 effect. Remove it from your volume config."
            );
        }
        if v.schedules.fsck_interval_secs.is_some()
            && !WARNED_FSCK_INTERVAL.swap(true, Ordering::Relaxed)
        {
            tracing::warn!(
                volume_id = %v.id,
                value = ?v.schedules.fsck_interval_secs,
                "Bug BF-10: VolumeSchedules.fsck_interval_secs is reserved \
                 but never consumed. No fsck scheduler exists. Scheduled for \
                 removal in next minor release."
            );
        }
        if !v.schedules.custom.is_empty()
            && !WARNED_CUSTOM_SCHEDULE.swap(true, Ordering::Relaxed)
        {
            tracing::warn!(
                volume_id = %v.id,
                count = v.schedules.custom.len(),
                "Bug BF-10: VolumeSchedules.custom map is reserved but no \
                 scheduler consumes named-job entries. Scheduled for removal."
            );
        }
    }
}

/// gRPC metadata service backed by Raft-replicated state.
///
/// Write operations (CreateFile, DeleteFile) are replicated through Raft.
/// Read operations are served from the local state machine copy (which may be
/// slightly stale on followers).
pub struct MetadataService {
    pub config: ManagerConfig,
    pub raft: Arc<DtfsRaft>,
    pub sm_data: Arc<tokio::sync::RwLock<StateMachineData>>,
    pub placement: Arc<PlacementEngine>,
    /// Used to record heartbeat timestamps so `HealthMonitor` can detect
    /// missed heartbeats (fix for issue #2).
    pub cluster: Arc<ClusterManager>,
    /// P0-9: tiering engine; receives per-chunk-per-DC reads from
    /// heartbeats so the auto-move-replica-toward-active-DC decisions
    /// are driven by real access patterns.
    pub tiering: Option<Arc<crate::tiering::TieringEngine>>,
    /// Bug BL — cached manager-to-manager clients keyed by leader gRPC
    /// addr. A non-leader heartbeat handler forwards the heartbeat's Raft
    /// writes to the current leader via `ForwardClientWrite`; caching the
    /// channel avoids a TCP+HTTP/2 handshake on every 5 s heartbeat tick.
    leader_clients: Arc<
        tokio::sync::Mutex<
            std::collections::HashMap<
                String,
                ManagerServiceClient<tonic::transport::Channel>,
            >,
        >,
    >,
}

impl MetadataService {
    pub fn new(
        config: ManagerConfig,
        raft: Arc<DtfsRaft>,
        sm_data: Arc<tokio::sync::RwLock<StateMachineData>>,
        placement: Arc<PlacementEngine>,
        cluster: Arc<ClusterManager>,
    ) -> Self {
        Self {
            config,
            raft,
            sm_data,
            placement,
            cluster,
            tiering: None,
            leader_clients: Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
        }
    }

    /// Inject the tiering engine after construction (avoids circular Arc
    /// initialisation; the engine itself takes references that originate
    /// from the same Arcs used by the metadata service).
    pub fn with_tiering(mut self, tiering: Arc<crate::tiering::TieringEngine>) -> Self {
        self.tiering = Some(tiering);
        self
    }

    /// Helper: propose a write through Raft and return the response.
    async fn raft_write(&self, req: ClientRequest) -> Result<ClientResponse, Status> {
        // Bug Y diagnostic — log the actual openraft error before
        // wrapping it as a generic Status::internal.
        //
        // Bug AC — when the local view reports `current_leader=None`
        // (election in progress, just restarted, transient partition),
        // a naive `client_write` returns `ForwardToLeader(None, None)`
        // immediately. The caller (FUSE, CLI) gets EIO even though the
        // cluster as a whole is fine and would converge in a few hundred
        // ms. Wait up to 3 s for a leader to emerge before failing.
        use openraft::async_runtime::watch::WatchReceiver;
        let mut metrics_rx = self.raft.metrics();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            let has_leader = metrics_rx.borrow_watched().current_leader.is_some();
            if has_leader {
                break;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                tracing::warn!("Raft write: no leader after 3s wait, will attempt anyway");
                break;
            }
            // Wait for metrics change or remaining timeout, whichever first.
            tokio::select! {
                _ = metrics_rx.changed() => continue,
                _ = tokio::time::sleep(remaining) => break,
            }
        }

        match self.raft.client_write(req).await {
            Ok(resp) => Ok(resp.data),
            Err(e) => {
                // Bug AC — distinguish "should retry on another manager"
                // from "real error". ForwardToLeader (or "we don't know
                // the leader") must return Status::Unavailable so the
                // FUSE client's `call_manager` retry-on-transport-error
                // walks to another manager. Without this distinction
                // every failover during a leader kill returned
                // Status::Internal which the client did NOT retry,
                // causing user-visible EIO during ~10s of every leader
                // election (~30% of stress-test write attempts).
                let msg = format!("{}", e);
                if msg.contains("forward request to")
                    || msg.contains("ForwardToLeader")
                {
                    tracing::warn!(error = %e, "Raft write deferred — not leader, client should retry");
                    return Err(Status::unavailable(format!(
                        "manager is not the leader: {msg}"
                    )));
                }
                tracing::warn!(error = %e, "Raft write failed");
                Err(Status::internal(format!("Raft write failed: {}", e)))
            }
        }
    }

    /// Bug BL — propose a heartbeat-originated write, forwarding to the
    /// current leader when this manager is a follower.
    ///
    /// On a follower, `client_write` returns `ForwardToLeader`; the four
    /// heartbeat writes (UpdateNodeUsage, UpdateNodeDiskUsage,
    /// QuarantineNode, RemoveCorruptChunkReplicas) were previously
    /// `let _`-dropped, so node liveness, quarantine state, and integrity
    /// faults never committed when a data node heartbeat landed on a
    /// non-leader. This wrapper proxies the request to the leader via the
    /// `ForwardClientWrite` RPC instead of dropping it.
    async fn raft_write_or_forward(&self, req: ClientRequest) -> Result<ClientResponse, Status> {
        match self.raft.client_write(req.clone()).await {
            Ok(resp) => Ok(resp.data),
            Err(e) => {
                let msg = format!("{e}");
                if msg.contains("forward request to") || msg.contains("ForwardToLeader") {
                    tracing::debug!(error = %e, "heartbeat raft write: forwarding to leader");
                    self.proxy_to_leader(req).await
                } else {
                    Err(Status::internal(format!("Raft write failed: {e}")))
                }
            }
        }
    }

    /// Bug BL — forward a `ClientRequest` to the current Raft leader and
    /// return the leader's `ClientResponse`.
    async fn proxy_to_leader(&self, req: ClientRequest) -> Result<ClientResponse, Status> {
        use openraft::async_runtime::watch::WatchReceiver;
        let leader_id = self
            .raft
            .metrics()
            .borrow_watched()
            .current_leader
            .ok_or_else(|| Status::unavailable("no leader to forward heartbeat write to"))?;

        // Resolve the leader's gRPC addr from the configured voter set
        // (node_id_from_hostname matches the openraft node id).
        let leader_addr = resolve_leader_addr(&self.config.cluster.initial_voters, leader_id)
            .ok_or_else(|| {
                Status::unavailable(format!(
                    "leader {leader_id} not found in cluster.initial_voters"
                ))
            })?;

        let mut client = self.leader_client(&leader_addr).await?;
        let body = serde_json::to_vec(&req)
            .map_err(|e| Status::internal(format!("serialize ClientRequest: {e}")))?;
        let resp = client
            .forward_client_write(ForwardWriteRequest { client_request_json: body })
            .await
            .map_err(|e| {
                // Drop the cached client on transport error so the next
                // forward reconnects (leader may have restarted).
                Status::unavailable(format!("forward to leader {leader_addr} failed: {e}"))
            })?
            .into_inner();

        if !resp.success {
            return Err(Status::unavailable(format!(
                "leader rejected forwarded write: {}",
                resp.error
            )));
        }
        serde_json::from_slice(&resp.response_json)
            .map_err(|e| Status::internal(format!("deserialize ClientResponse: {e}")))
    }

    /// Bug BL — return a cached (or freshly-connected) manager client to
    /// the given leader addr.
    async fn leader_client(
        &self,
        addr: &str,
    ) -> Result<ManagerServiceClient<tonic::transport::Channel>, Status> {
        {
            let cache = self.leader_clients.lock().await;
            if let Some(c) = cache.get(addr) {
                return Ok(c.clone());
            }
        }
        let endpoint = format!("http://{addr}");
        let client = ManagerServiceClient::connect(endpoint)
            .await
            .map_err(|e| Status::unavailable(format!("connect leader {addr}: {e}")))?;
        self.leader_clients
            .lock()
            .await
            .insert(addr.to_string(), client.clone());
        Ok(client)
    }
}

/// Bug BL-3 — a data node may be told it is clear (so it re-enables
/// `put_chunk`) only when it is not currently quarantined in Raft state
/// AND it did not report any integrity fault in this heartbeat. The
/// second clause closes the same-cycle window: a node that just reported
/// corruption must not be re-enabled before the QuarantineNode write
/// (forwarded to the leader) has committed and replicated back.
fn compute_self_quarantine_cleared(is_quarantined: bool, reported_fault_count: usize) -> bool {
    !is_quarantined && reported_fault_count == 0
}

/// Bug BL — resolve the gRPC address of the current Raft leader from the
/// configured voter set. The openraft node id is derived from the
/// hostname via `node_id_from_hostname`, so a voter matches the leader
/// when their derived id equals `leader_id`.
fn resolve_leader_addr(
    voters: &[dtfs_common::config::InitialVoter],
    leader_id: u64,
) -> Option<String> {
    voters
        .iter()
        .find(|v| dtfs_common::node_id_from_hostname(&v.hostname) == leader_id)
        .map(|v| v.addr.clone())
}

fn tier_from_proto(s: &str) -> DiskTier {
    match s.to_ascii_lowercase().as_str() {
        "warm" => DiskTier::Warm,
        "cold" => DiskTier::Cold,
        "tape" => DiskTier::Tape,
        _ => DiskTier::Hot,
    }
}

fn disk_status_from_proto(s: &str) -> DiskStatus {
    match s.to_ascii_lowercase().as_str() {
        "degraded" => DiskStatus::Degraded,
        "draining" => DiskStatus::Draining,
        "failed" => DiskStatus::Failed,
        "removed" => DiskStatus::Removed,
        _ => DiskStatus::Healthy,
    }
}

fn location_from_proto(loc: Option<dtfs_proto::dtfs::common::Location>) -> Location {
    let Some(l) = loc else {
        return Location::new("", "", "", "");
    };
    Location {
        region: l.region,
        datacenter: l.datacenter,
        rack: l.rack,
        host: l.host,
    }
}

fn disks_from_register_req(
    disks: &[dtfs_proto::dtfs::common::DiskInfo],
) -> Vec<dtfs_common::types::DiskInfo> {
    disks
        .iter()
        .map(|d| dtfs_common::types::DiskInfo {
            disk_id: d.disk_id,
            uuid: d.uuid.clone(),
            mount_path: d.mount_path.clone(),
            tier: tier_from_proto(&d.tier),
            total_bytes: d.total_bytes,
            used_bytes: d.used_bytes,
            status: disk_status_from_proto(&d.status),
        })
        .collect()
}

/// Resolve a stored chunk `node_id` field to a client-connectable gRPC base URL.
fn resolve_node_contact(sm: &StateMachineData, stored: &str) -> String {
    if stored.starts_with("http://") || stored.starts_with("https://") {
        return stored.to_string();
    }
    if let Ok(id) = stored.parse::<u64>() {
        if let Some(n) = sm.nodes.get(&id) {
            return n.grpc_addr.clone();
        }
    }
    stored.to_string()
}

/// Reverse-lookup the numeric node ID for a given gRPC address.
///
/// Storing numeric IDs in `ChunkLocationEntry.node_id` keeps the field type
/// consistent — the scheduler always parses it as `u64` (fix for issue #7).
fn node_id_from_grpc_addr(sm: &StateMachineData, addr: &str) -> String {
    sm.nodes
        .iter()
        .find(|(_, n)| n.grpc_addr == addr)
        .map(|(id, _)| id.to_string())
        .unwrap_or_else(|| addr.to_string())
}

#[tonic::async_trait]
impl ManagerService for MetadataService {
    async fn create_file(
        &self,
        request: Request<CreateFileRequest>,
    ) -> Result<Response<CreateFileResponse>, Status> {
        let req = request.into_inner();
        info!("CreateFile: {}", req.path);

        let volume_id = req
            .volume_id
            .clone()
            .unwrap_or_else(|| "default".to_string());

        let chunk_size = self.config.defaults.chunk_size_mb as u64 * 1024 * 1024;

        let resp = self
            .raft_write(ClientRequest::CreateFile {
                path: req.path.clone(),
                volume_id: volume_id.clone(),
                chunk_size,
            })
            .await?;

        match resp {
            ClientResponse::FileCreated { volume_id, .. } => {
                Ok(Response::new(CreateFileResponse {
                    success: true,
                    chunk_size,
                    volume_id,
                }))
            }
            ClientResponse::Error { message } => Err(Status::internal(message)),
            _ => Ok(Response::new(CreateFileResponse {
                success: true,
                chunk_size,
                volume_id,
            })),
        }
    }

    async fn get_file_locations(
        &self,
        request: Request<GetFileLocationsRequest>,
    ) -> Result<Response<GetFileLocationsResponse>, Status> {
        let req = request.into_inner();

        let data = self.sm_data.read().await;
        // Use the canonical lookup helper so empty `mkdir` entries and
        // synthetic directories (paths implied by existing children) both
        // resolve. Without this, `getattr("/foo")` returned ENOENT even
        // when files existed under `/foo/`, breaking rsync mkstemp inside
        // freshly-written directories.
        let file = data
            .lookup_path(&req.path)
            .ok_or_else(|| Status::not_found("File not found"))?;

        // Group replicas by chunk_index (not chunk_hash) so that two distinct
        // chunks that happen to share a hash are never merged into one entry,
        // which would cause the client to see fewer chunks than the file has
        // (fix for issue #12).
        let mut by_chunk: BTreeMap<u64, ChunkLocationInfo> = BTreeMap::new();
        for c in &file.chunks {
            let addr = resolve_node_contact(&data, &c.node_id);
            let e = by_chunk
                .entry(c.chunk_index)
                .or_insert_with(|| ChunkLocationInfo {
                    chunk_id: c.chunk_hash.clone(),
                    size: c.size_bytes,
                    node_addresses: Vec::new(),
                });
            e.size = e.size.max(c.size_bytes);
            if !e.node_addresses.contains(&addr) {
                e.node_addresses.push(addr);
            }
        }

        let chunk_locations: Vec<ChunkLocationInfo> = by_chunk.into_values().collect();
        // F-037 / F-033: if the file has an explicit logical size (set via
        // truncate or fallocate), honour it; otherwise derive from the
        // chunk sizes.
        let total_size: u64 = file
            .logical_size
            .unwrap_or_else(|| chunk_locations.iter().map(|c| c.size).sum());

        Ok(Response::new(GetFileLocationsResponse {
            path: req.path,
            total_size,
            chunks: chunk_locations,
            // Expose the logical chunk size so clients can correctly compute
            // chunk indices from byte offsets (fix for issue #13).
            chunk_size: file.chunk_size,
            // Propagate timestamps so the FUSE client can return real
            // mtime/ctime via getattr (fix for F-014).
            created_at_ms: file.created_at_ms as i64,
            modified_at_ms: file.updated_at_ms as i64,
            is_directory: file.is_directory,
        }))
    }

    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        // Bug W — capture the peer address BEFORE consuming the request,
        // so we can build a routable grpc_addr from the actual source IP
        // instead of relying on the data node's hostname (which may
        // resolve to IPv6 link-local on some networks, breaking all
        // cross-node migration / replication).
        let peer_addr = request.remote_addr();
        let req = request.into_inner();
        info!(
            "RegisterNode: hostname={} peer={:?}",
            req.hostname, peer_addr
        );

        let secret_required = !self.config.cluster.shared_secret.is_empty();
        if secret_required {
            let ok = req
                .shared_secret
                .as_deref()
                .map(|s| s == self.config.cluster.shared_secret)
                .unwrap_or(false);
            if !ok {
                return Ok(Response::new(RegisterNodeResponse {
                    accepted: false,
                    node_id: String::new(),
                    config_version: 0,
                }));
            }
        }

        let node_id = dtfs_common::node_id_from_hostname(&req.hostname);
        // Bug AA — clients have no gRPC listener; storing a sentinel
        // 0-port URL would invite the scheduler to dial it on the
        // failover path. Use empty string for clients; data + manager
        // entries use the proper peer-derived URL (Bug W).
        let is_client = req.role.eq_ignore_ascii_case("client");
        let grpc_addr = if is_client {
            String::new()
        } else {
            build_data_grpc_addr(peer_addr, &req.hostname, req.grpc_port)
        };
        // Bug AX — clients have no grpc_addr; record their source IP so
        // operators can see where the FUSE mount is connecting from.
        let client_ip = if is_client {
            peer_addr.map(|sa| sa.ip().to_string())
        } else {
            None
        };
        let disks = disks_from_register_req(&req.disks);
        let total_capacity: u64 = disks.iter().map(|d| d.total_bytes).sum();
        let used_capacity: u64 = disks.iter().map(|d| d.used_bytes).sum();

        let info = RaftNodeInfo {
            // Bug AA — accept "client" role for FUSE self-registration.
            // Clients appear in /api/v1/nodes with their location but
            // are excluded from chunk placement (PlacementEngine filters
            // by NodeRole::Data).
            role: if req.role.eq_ignore_ascii_case("manager") {
                NodeRole::Manager
            } else if is_client {
                NodeRole::Client
            } else {
                NodeRole::Data
            },
            hostname: req.hostname.clone(),
            grpc_addr,
            location: location_from_proto(req.location),
            disks,
            total_capacity,
            used_capacity,
            health: HealthStatus::Healthy,
            last_seen_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            total_bytes_read: 0,
            total_bytes_written: 0,
            client_ip,
            // Bug AY — propagate the reporting node's version + build date
            // into Raft state so every manager surfaces the same value
            // in /api/v1/nodes and the WebUI Nodes page.
            version: req.version.clone(),
            build_date: req.build_date.clone(),
            quarantine: None,
        };

        let resp = self
            .raft_write(ClientRequest::RegisterNode { node_id, info })
            .await?;

        if let ClientResponse::Error { message } = resp {
            return Err(Status::internal(message));
        }

        let config_version = self.sm_data.read().await.cluster_config.version;

        Ok(Response::new(RegisterNodeResponse {
            accepted: true,
            node_id: node_id.to_string(),
            config_version,
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        tracing::debug!("Heartbeat from node: {}", req.node_id);

        let node_id: u64 = req
            .node_id
            .parse()
            .map_err(|_| Status::invalid_argument("node_id must be a u64 from registration"))?;

        // Record the heartbeat timestamp so HealthMonitor can detect missed
        // heartbeats. Without this call the ClusterManager's heartbeat map was
        // never populated and every node appeared as Down immediately (fix #2).
        self.cluster.process_heartbeat(node_id);

        // Bug BC-3 — heartbeat carries the full cluster scrub +
        // integrity policy from `ManagerConfig.scrub`. The data-side
        // scheduler hot-reloads within DECISION_TICK_SECS.
        //
        // Back-compat: the AZ-1 fields `quick_interval_hours` +
        // `deep_interval_hours` are kept on the wire so a data node
        // still on AZ-1 code reads the same values it always has.
        // The new fields (orphan + verify_on_read + quarantine +
        // auto-unquarantine) are populated from the same
        // ClusterScrubConfig source of truth.
        //
        // Per-volume `VolumeSchedules.scrub_interval_secs` is now
        // Bug BF-9 — per-volume `scrub_interval_secs` removed from the
        // active path. Cluster-wide `manager.scrub.quick_interval_secs`
        // is now the only source. The serde field on VolumeSchedules
        // stays for back-compat reads of older snapshots but no longer
        // influences the heartbeat override.
        //
        // Bug BF-10 — `fsck_interval_secs` + `custom` schedule fields
        // are reserved-but-unused. We warn-once per process on first
        // observation so operators see the deprecation in journalctl
        // without the noise of a per-heartbeat warn.
        let scrub_cfg = &self.config.scrub;
        let (cfg_version, scrub_override) = {
            let sm = self.sm_data.read().await;
            let cfg_version = sm.cluster_config.version;
            warn_once_on_dead_schedule_fields(&sm.volumes);
            let effective_quick_secs = scrub_cfg.quick_interval_secs.max(60);
            let quick_h = (effective_quick_secs + 3599) / 3600;
            let deep_h = (scrub_cfg.deep_interval_secs.max(60) + 3599) / 3600;
            (
                cfg_version,
                Some(dtfs_proto::dtfs::manager::ScrubConfigOverride {
                    quick_interval_hours:   quick_h.max(1),
                    deep_interval_hours:    deep_h.max(1),
                    orphan_interval_secs:   scrub_cfg.orphan_interval_secs,
                    orphan_grace_secs:      scrub_cfg.orphan_grace_secs,
                    max_io_percent:         scrub_cfg.max_io_percent,
                    verify_on_read:         scrub_cfg.verify_on_read,
                    quarantine_on_fault:    scrub_cfg.quarantine_on_fault,
                    auto_unquarantine_secs: scrub_cfg.auto_unquarantine_secs,
                }),
            )
        };

        let _ = self
            .raft_write_or_forward(ClientRequest::UpdateNodeUsage {
                node_id,
                used_capacity: req.total_bytes_used,
                chunk_count: req.total_chunks,
                total_bytes_read: req.total_bytes_read,
                total_bytes_written: req.total_bytes_written,
                observed_at_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0),
            })
            .await;

        // Bug AU — refresh per-disk usage from the heartbeat so the
        // Nodes-page disk table (Bug AR) tracks reality across the
        // life of the data node. Drops cleanly when the data binary
        // hasn't been upgraded yet (req.disk_usage empty → manager
        // keeps the boot snapshot).
        if !req.disk_usage.is_empty() {
            let per_disk: Vec<(u32, u64, u64)> = req
                .disk_usage
                .iter()
                .map(|d| (d.disk_id, d.total_bytes, d.used_bytes))
                .collect();
            let _ = self
                .raft_write_or_forward(ClientRequest::UpdateNodeDiskUsage { node_id, per_disk })
                .await;
        }

        // Bug BF-6 — only mark Healthy if the node isn't currently
        // Quarantined. Without this guard the very next heartbeat after
        // an integrity-fault auto-quarantine would clobber the
        // Quarantined status back to Healthy.
        // Bug BF (data-side self-quarantine clear) — also surfaces the
        // current Quarantined state so the heartbeat response can tell
        // the data node it is free again.
        let is_quarantined = {
            let sm = self.sm_data.read().await;
            sm.nodes
                .get(&node_id)
                .map(|n| n.quarantine.is_some())
                .unwrap_or(false)
        };
        // Bug BL-3 — never tell a data node it is clear in the same cycle
        // it reported an integrity fault. Without this guard, a non-leader
        // whose QuarantineNode write was dropped (now forwarded, but
        // committed asynchronously on the leader) would read the local,
        // not-yet-replicated `quarantine = None` and re-enable put_chunk
        // on a node that just reported a corrupt chunk. Combined with the
        // forwarding above, subsequent cycles read the replicated
        // quarantine state correctly.
        let self_quarantine_cleared =
            compute_self_quarantine_cleared(is_quarantined, req.integrity_faults.len());
        if !is_quarantined {
            let _ = self
                .raft_write_or_forward(ClientRequest::UpdateNodeStatus {
                    node_id,
                    status: "healthy".to_string(),
                })
                .await;
        }

        // Bug BF-6 — process every integrity_fault the data node
        // reported since its last heartbeat. For each fault:
        //   1. raft-write QuarantineNode (state_machine apply path
        //      accumulates fault_count_total across faults).
        //   2. raft-write RemoveCorruptChunkReplicas so the under-
        //      replication scheduler re-replicates the chunk from a
        //      healthy node immediately.
        // Older data nodes don't emit field 10; for them the vec is
        // empty + this loop is a no-op (back-compat with pre-BF data).
        if !req.integrity_faults.is_empty() {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            // Bug BO-4 (BC-12e remainder) — the data node's verify-on-read
            // mismatch surfaces only as a node.quarantine row today. Emit a
            // distinct `read.integrity_fault` audit per fault so the
            // /api/v1/integrity/faults feed shows the raw read event (chunk
            // + hashes) separately from the node-quarantine action.
            let hostname = {
                let sm = self.sm_data.read().await;
                sm.nodes.get(&node_id).map(|n| n.hostname.clone()).unwrap_or_default()
            };
            let origin_manager = self.config.node.hostname.clone();
            for fault in &req.integrity_faults {
                let _ = self
                    .raft_write_or_forward(ClientRequest::AppendAudit {
                        actor: "system".to_string(),
                        action: "read.integrity_fault".to_string(),
                        target: format!("{node_id} ({hostname})"),
                        result: format!(
                            "detector={} chunk={} expected={} actual={}",
                            fault.detector,
                            fault.chunk_id,
                            if fault.expected_hash.is_empty() { "?" } else { &fault.expected_hash },
                            if fault.actual_hash.is_empty() { "?" } else { &fault.actual_hash },
                        ),
                        origin_manager: origin_manager.clone(),
                        timestamp_ms: now_ms,
                    })
                    .await;
                let reason = match fault.detector.as_str() {
                    "verify_on_read" => dtfs_common::types::QuarantineReason::VerifyOnReadMismatch,
                    "verify_on_read_meta_absent" => dtfs_common::types::QuarantineReason::VerifyOnReadMismatch,
                    _ => dtfs_common::types::QuarantineReason::VerifyOnReadMismatch,
                };
                let info = dtfs_common::types::QuarantineInfo {
                    since_ms: now_ms,
                    reason,
                    triggering_chunk_id: Some(fault.chunk_id.clone()),
                    fault_count_total: 0,  // apply path accumulates
                    last_fault_at_ms: fault.observed_at_ms,
                    last_fault_summary: format!(
                        "{}: expected={} actual={}",
                        fault.detector,
                        if fault.expected_hash.is_empty() { "?" } else { &fault.expected_hash },
                        if fault.actual_hash.is_empty() { "?" } else { &fault.actual_hash },
                    ),
                };
                let _ = self
                    .raft_write_or_forward(ClientRequest::QuarantineNode { node_id, info })
                    .await;
                // Drop the (node, chunk) replica entry so the
                // under-replication scheduler immediately picks a
                // healthy source + re-replicates. RemoveCorruptChunkReplicas
                // is the existing apply path used by Bug O scrub.
                let _ = self
                    .raft_write_or_forward(ClientRequest::RemoveCorruptChunkReplicas {
                        node_id: node_id.to_string(),
                        chunk_hashes: vec![fault.chunk_id.clone()],
                    })
                    .await;
            }
            tracing::warn!(
                node_id,
                count = req.integrity_faults.len(),
                "Bug BF-6: auto-quarantined node after integrity faults reported via heartbeat"
            );
        }

        // P0-9: fan out per-chunk read counters into the tiering engine.
        // The TieringEngine aggregates by chunk_id and uses `reads_by_dc`
        // to score migration target nodes (find_target_node).
        if !req.chunk_reads.is_empty() {
            if let Some(tiering) = &self.tiering {
                // Map proto → engine call. The tiering engine wants the
                // *current* tier and *current* node for the chunk; we leave
                // those zeroed since the engine looks them up from the
                // state machine when evaluating.
                for entry in &req.chunk_reads {
                    let reads_1h = entry.reads_by_dc_1h.values().sum::<u32>();
                    let reads_24h = entry.reads_by_dc_24h.values().sum::<u32>();
                    tiering
                        .report_access(
                            &entry.chunk_id,
                            "", // file_path unknown at heartbeat time; engine fills via SM lookup
                            reads_1h,
                            reads_24h,
                            entry.reads_by_dc_1h.clone(),
                            dtfs_common::types::DiskTier::Hot, // placeholder; engine recomputes
                            node_id,
                        )
                        .await;
                }
                tracing::debug!(
                    "tiering: ingested {} chunk-read entries from heartbeat",
                    req.chunk_reads.len()
                );
            }
        }

        // AZ-1 — emit a minimal ConfigUpdate carrying the scrub
        // override. config_json stays empty (data-side parses it as
        // DataNodeConfig only when non-empty; the legacy live-reload
        // path is unaffected). Sent every heartbeat — the data-side
        // RwLock write is a noop when the value is unchanged.
        let config_update = scrub_override.map(|s| {
            dtfs_proto::dtfs::manager::ConfigUpdate {
                version: cfg_version,
                config_json: String::new(),
                scrub_override: Some(s),
            }
        });

        Ok(Response::new(HeartbeatResponse {
            accepted: true,
            config_version: cfg_version,
            config_update,
            self_quarantine_cleared,
        }))
    }

    async fn report_chunk_status(
        &self,
        request: Request<ChunkStatusReport>,
    ) -> Result<Response<ChunkStatusAck>, Status> {
        let req = request.into_inner();
        tracing::debug!("ChunkStatusReport: {} chunks", req.chunks.len());

        let node_id_u64: u64 = req
            .node_id
            .parse()
            .map_err(|_| Status::invalid_argument("node_id must be a u64 from registration"))?;

        // Bug O — partition the reported chunks into corrupt/missing
        // (which need replica entries dropped + re-replication queued)
        // vs healthy (which keep their entries).
        let mut corrupt_ids: Vec<String> = Vec::new();
        for ch in &req.chunks {
            if ch.status.eq_ignore_ascii_case("corrupt")
                || ch.status.eq_ignore_ascii_case("missing")
            {
                corrupt_ids.push(ch.chunk_id.clone());
            }
        }

        let status = if !corrupt_ids.is_empty() {
            "degraded"
        } else {
            "healthy"
        };
        let _ = self
            .raft_write(ClientRequest::UpdateNodeStatus {
                node_id: node_id_u64,
                status: status.to_string(),
            })
            .await;

        // Bug O — drop the (this-node, chunk_hash) replica entries so
        // the scheduler's under-replication pass queues a copy from a
        // healthy peer. node_id is the stringified form the data-node
        // uses in `ChunkLocationEntry.node_id`.
        if !corrupt_ids.is_empty() {
            tracing::warn!(
                node_id = %req.node_id,
                count = corrupt_ids.len(),
                "Bug O dropping corrupt-replica entries + queueing re-replication"
            );
            let _ = self
                .raft_write(ClientRequest::RemoveCorruptChunkReplicas {
                    node_id: req.node_id.clone(),
                    chunk_hashes: corrupt_ids,
                })
                .await;
        }

        Ok(Response::new(ChunkStatusAck { accepted: true }))
    }

    /// Bug BL — leader-side handler for writes forwarded by a non-leader
    /// manager (see `proxy_to_leader`). Deserializes the `ClientRequest`,
    /// applies it through Raft, and returns the serialized `ClientResponse`.
    /// If this manager is itself no longer the leader (leadership changed
    /// mid-flight), `client_write` returns `ForwardToLeader` and we reply
    /// `success = false` with the error so the caller drops the write
    /// (best-effort; the next heartbeat re-attempts).
    async fn forward_client_write(
        &self,
        request: Request<ForwardWriteRequest>,
    ) -> Result<Response<ForwardWriteResponse>, Status> {
        let body = request.into_inner();
        let req: ClientRequest = serde_json::from_slice(&body.client_request_json)
            .map_err(|e| Status::invalid_argument(format!("bad ClientRequest json: {e}")))?;
        match self.raft.client_write(req).await {
            Ok(resp) => {
                let response_json = serde_json::to_vec(&resp.data)
                    .map_err(|e| Status::internal(format!("serialize ClientResponse: {e}")))?;
                Ok(Response::new(ForwardWriteResponse {
                    success: true,
                    response_json,
                    error: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(ForwardWriteResponse {
                success: false,
                response_json: Vec::new(),
                error: format!("{e}"),
            })),
        }
    }

    async fn delete_file(
        &self,
        request: Request<DeleteFileRequest>,
    ) -> Result<Response<DeleteFileResponse>, Status> {
        let req = request.into_inner();
        info!("DeleteFile: {}", req.path);

        // Bug N — collect the chunk locations BEFORE the Raft delete
        // commits so we know which data nodes need to reclaim disk
        // space. After the commit `sm.files` will no longer have the
        // entry. We dedup (chunk_hash, node_id) to avoid sending the
        // same DeleteChunk twice for replicated chunks.
        let chunk_targets: Vec<(String, String)> = {
            let data = self.sm_data.read().await;
            if let Some(meta) = data.files.get(&req.path) {
                let mut seen: std::collections::HashSet<(String, String)> =
                    Default::default();
                let mut out = Vec::new();
                for c in &meta.chunks {
                    let key = (c.chunk_hash.clone(), c.node_id.clone());
                    if seen.insert(key.clone()) {
                        out.push(key);
                    }
                }
                out
            } else {
                Vec::new()
            }
        };

        // Resolve node_id → grpc_addr so we can talk to each data node
        // without re-locking the state machine per chunk.
        let node_addrs: std::collections::HashMap<String, String> = {
            let data = self.sm_data.read().await;
            data.nodes
                .iter()
                .map(|(id, info)| (id.to_string(), info.grpc_addr.clone()))
                .collect()
        };

        // Commit the Raft-side delete first. The chunk fan-out is
        // best-effort: a failure to reach a data node leaves an orphan
        // that the scheduler's reconciler picks up later. We
        // INTENTIONALLY commit the metadata removal first because
        // a half-deleted file (metadata gone, chunks remain) is the
        // correct failure mode — a dangling-pointer file (metadata
        // remains, chunks gone) would be worse.
        let resp = self
            .raft_write(ClientRequest::DeleteFile {
                path: req.path.clone(),
            })
            .await?;

        // Now best-effort fan-out chunk deletes. Group by node so we
        // open one connection per target.
        let mut by_node: std::collections::HashMap<String, Vec<String>> =
            Default::default();
        for (chunk_hash, node_id) in chunk_targets {
            if let Some(addr) = node_addrs.get(&node_id) {
                by_node.entry(addr.clone()).or_default().push(chunk_hash);
            }
        }

        let path_for_log = req.path.clone();
        tokio::spawn(async move {
            for (addr, chunks) in by_node {
                let endpoint = format!("http://{}", addr);
                let mut client = match dtfs_proto::dtfs::data::data_service_client::DataServiceClient::connect(endpoint.clone()).await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!(
                            path = %path_for_log,
                            node_addr = %addr,
                            error = %e,
                            "Bug N delete fan-out: connect failed, leaving orphan chunks for reconciler"
                        );
                        continue;
                    }
                };
                for chunk_id in chunks {
                    let req = dtfs_proto::dtfs::data::DeleteChunkRequest {
                        chunk_id: chunk_id.clone(),
                    };
                    match client.delete_chunk(req).await {
                        Ok(_) => {
                            tracing::debug!(
                                path = %path_for_log,
                                node_addr = %addr,
                                chunk_id = %chunk_id,
                                "Bug N delete fan-out: chunk reclaimed"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                path = %path_for_log,
                                node_addr = %addr,
                                chunk_id = %chunk_id,
                                error = %e,
                                "Bug N delete fan-out: RPC failed, orphan remains for reconciler"
                            );
                        }
                    }
                }
            }
        });

        match resp {
            ClientResponse::Success => Ok(Response::new(DeleteFileResponse { success: true })),
            ClientResponse::Error { message } => Err(Status::not_found(message)),
            _ => Ok(Response::new(DeleteFileResponse { success: true })),
        }
    }

    async fn list_directory(
        &self,
        request: Request<ListDirectoryRequest>,
    ) -> Result<Response<ListDirectoryResponse>, Status> {
        let req = request.into_inner();

        let data = self.sm_data.read().await;
        // Delegate the actual aggregation to the canonical helper. It
        // handles three cases atomically (Bug E):
        //   - explicit mkdir entry + child files at the same name → dir
        //   - synthetic directory implied by child paths → dir
        //   - regular file → not a dir, size from chunks/logical_size
        // Iteration order of `data.files` no longer affects the type bit.
        let listed = data.list_directory_entries(&req.path);

        let entries: Vec<DirectoryEntry> = listed
            .into_iter()
            .map(|e| DirectoryEntry {
                name: e.name,
                size: e.size_bytes,
                is_directory: e.is_directory,
                created_at: e.created_at_ms as i64,
                modified_at: e.updated_at_ms as i64,
            })
            .collect();

        Ok(Response::new(ListDirectoryResponse { entries }))
    }

    async fn allocate_chunks(
        &self,
        request: Request<AllocateChunksRequest>,
    ) -> Result<Response<AllocateChunksResponse>, Status> {
        let req = request.into_inner();
        info!(
            "AllocateChunks for file: {}, count: {}",
            req.path, req.num_chunks
        );

        self.placement.rebuild_ring(&self.sm_data).await;

        let data = self.sm_data.read().await;

        if data.nodes.is_empty() {
            return Err(Status::unavailable("No data nodes available"));
        }

        // Bug Z — look up the volume's anchor_datacenter and effective
        // tier+replication. Falls back to manager-config defaults when
        // the volume isn't registered yet (e.g. very early after bootstrap
        // before the UpsertVolume Raft entry has applied). The effective
        // tier is the first one with rf > 0 in (hot, warm, cold) order;
        // this lets Plexmedia (hot=0, warm=2, cold=1) work without
        // hard-coding Hot. The anchor enforcement happens in
        // `place_chunks_with_anchor` via the hash-ring rule.
        //
        // Bug AV (AV-3-2) — if the client provided a `preferred_tier`
        // hint AND a healthy node with that tier exists in the
        // cluster, honour the hint and place a single first-chunk
        // replica on that tier. The anchor migration to cold (if
        // needed) happens asynchronously via AV-3-3's
        // `check_anchor_lag` pass.
        let volume_cfg = data.volumes.get(&req.volume_id).cloned();
        let available_tiers: Vec<DiskTier> = {
            // DiskTier doesn't impl Ord (and adding it ripples through
            // the workspace), so dedup via HashSet then collect.
            let set: std::collections::HashSet<DiskTier> = data
                .nodes
                .values()
                .filter(|n| n.health == dtfs_common::types::HealthStatus::Healthy)
                .flat_map(|n| n.disks.iter().map(|d| d.tier))
                .collect();
            set.into_iter().collect()
        };
        let (effective_tier, effective_rf, anchor_dc) = match &volume_cfg {
            Some(v) => {
                // Bug AV (AV-4-1c) — `resolve_volume_placement` pins
                // the contract that placement consults
                // `effective_mandatory_replicas`, not the legacy
                // `replication` field. See the helper's doc
                // comment + regression test.
                let (tier, rf) = resolve_volume_placement(
                    v,
                    &req.preferred_tier,
                    &available_tiers,
                );
                (tier, rf, v.anchor_datacenter.clone())
            }
            None => {
                let (tier, rf) = resolve_preferred_tier(
                    &req.preferred_tier,
                    &self.config.defaults.replication,
                    &available_tiers,
                );
                (
                    tier,
                    rf.max(self.config.defaults.replication.hot.max(1)),
                    self.config.defaults.anchor_datacenter.clone(),
                )
            }
        };
        let replication = effective_rf.max(1).min(data.nodes.len() as u32);

        // Bug BU-2 — addresses the client asked to exclude (a node whose
        // put_chunk just returned RESOURCE_EXHAUSTED). Normalise off any
        // scheme prefix so `http://h:9500` and `h:9500` compare equal.
        let norm_addr = |a: &str| {
            a.trim_start_matches("http://")
                .trim_start_matches("https://")
                .to_string()
        };
        let exclude_set: std::collections::HashSet<String> =
            req.exclude_addrs.iter().map(|a| norm_addr(a)).collect();

        // The deterministic hash-ring path has no native node exclusion
        // (`find_nodes_for_pg` takes client_locations, not an exclude list),
        // so when the client is rerouting we skip it and fall straight to
        // the capacity + exclude-filtered round-robin below.
        let placed = if exclude_set.is_empty() {
            self.placement.place_chunks_with_anchor(
                &req.volume_id,
                &req.path, // Bug BO-3 — per-file seed so chunk-0 doesn't collide cross-file
                req.num_chunks,
                effective_tier,
                replication,
                anchor_dc.clone(),
            )
        } else {
            Vec::new()
        };

        let mut placements = Vec::new();

        if placed.len() == req.num_chunks as usize && !placed.is_empty() {
            for p in placed {
                let mut addrs = Vec::new();
                for nid in &p.node_ids {
                    let Ok(id) = nid.0.parse::<u64>() else {
                        continue;
                    };
                    if let Some(n) = data.nodes.get(&id) {
                        addrs.push(n.grpc_addr.clone());
                    }
                }
                if addrs.is_empty() {
                    continue;
                }
                let primary = addrs[0].clone();
                let replica_addresses = addrs[1..].to_vec();
                placements.push(ChunkPlacement {
                    chunk_index: p.chunk_index,
                    primary_address: primary,
                    replica_addresses,
                    tier: chunk_placement_tier_string(effective_tier),
                });
            }
        }

        if placements.len() != req.num_chunks as usize {
            // Fallback: round-robin across all HEALTHY nodes. Includes replica
            // addresses so that the replication factor is still respected and
            // skips nodes that aren't Healthy so writes don't target Down hosts
            // (fix for F-010 + F-012).
            placements.clear();
            // Bug BU — the fallback must also avoid full disks: a node is
            // eligible only if it has at least one disk on the target tier
            // with free space above the headroom. Otherwise this path
            // happily round-robins a chunk onto a 100%-full node and
            // put_chunk hard-fails with ENOSPC.
            let pcfg = &self.config.placement;
            let node_list: Vec<_> = data
                .nodes
                .values()
                .filter(|n| n.health == dtfs_common::types::HealthStatus::Healthy)
                // Bug BU-2 — drop client-excluded nodes (the full one it
                // just failed on) so the retry reroutes elsewhere.
                .filter(|n| !exclude_set.contains(&norm_addr(&n.grpc_addr)))
                .filter(|n| {
                    n.disks.iter().any(|d| {
                        d.tier == effective_tier && pcfg.has_room(d.total_bytes, d.used_bytes)
                    })
                })
                .collect();
            if node_list.is_empty() {
                return Err(Status::unavailable(format!(
                    "No healthy data node has free space on tier {effective_tier:?} (all candidate disks below placement headroom or excluded)"
                )));
            }
            let rf = (replication as usize).min(node_list.len());

            for i in 0..req.num_chunks {
                let primary_idx = i as usize % node_list.len();
                let primary = node_list[primary_idx];

                let mut replica_addresses = Vec::new();
                for r in 1..rf {
                    let replica_idx = (primary_idx + r) % node_list.len();
                    // Only add if it's a distinct node from the primary.
                    if replica_idx != primary_idx {
                        replica_addresses.push(node_list[replica_idx].grpc_addr.clone());
                    }
                }

                placements.push(ChunkPlacement {
                    chunk_index: i,
                    primary_address: primary.grpc_addr.clone(),
                    replica_addresses,
                    tier: chunk_placement_tier_string(effective_tier),
                });
            }
        }

        Ok(Response::new(AllocateChunksResponse { placements }))
    }

    async fn commit_chunk_locations(
        &self,
        request: Request<CommitChunkLocationsRequest>,
    ) -> Result<Response<CommitChunkLocationsResponse>, Status> {
        let req = request.into_inner();
        let _ = &req.volume_id; // reserved for volume-scoped policies

        // Reverse-look up numeric node IDs from gRPC addresses before storing.
        // `ChunkLocationEntry.node_id` must be a numeric ID string so that the
        // scheduler's `chunk_loc.node_id.parse::<u64>()` succeeds (fix #7).
        let chunks: Vec<ChunkLocationEntry> = {
            let data = self.sm_data.read().await;
            req.chunks
                .into_iter()
                .map(|c| ChunkLocationEntry {
                    chunk_index: c.chunk_index as u64,
                    chunk_hash: c.chunk_id,
                    node_id: node_id_from_grpc_addr(&data, &c.primary_address),
                    disk_id: c.disk_id,
                    size_bytes: c.size_bytes,
                })
                .collect()
        }; // sm_data lock released here

        let resp = self
            .raft_write(ClientRequest::UpdateChunkLocations {
                path: req.path.clone(),
                chunks,
                lease_id: req.lease_id.clone(),
            })
            .await?;

        match resp {
            ClientResponse::Success => Ok(Response::new(CommitChunkLocationsResponse {
                success: true,
                lease_conflict: false,
                error: String::new(),
            })),
            // Bug P — lease_id didn't match the currently-held lease.
            // FUSE client must surface this so the writer aborts.
            ClientResponse::WriteLeaseConflict { owner_id, .. } => {
                Ok(Response::new(CommitChunkLocationsResponse {
                    success: false,
                    lease_conflict: true,
                    error: format!(
                        "write lease for {} no longer held by this writer (current owner: {})",
                        req.path, owner_id
                    ),
                }))
            }
            ClientResponse::Error { message } => Err(Status::failed_precondition(message)),
            _ => Ok(Response::new(CommitChunkLocationsResponse {
                success: true,
                lease_conflict: false,
                error: String::new(),
            })),
        }
    }

    // ---- Extended POSIX surface (F-034 .. F-038) ----

    async fn rename_file(
        &self,
        request: Request<RenameFileRequest>,
    ) -> Result<Response<RenameFileResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("RenameFile: {} -> {}", req.from, req.to);
        let resp = self
            .raft_write(ClientRequest::RenameFile {
                from: req.from.clone(),
                to: req.to.clone(),
            })
            .await?;
        match resp {
            ClientResponse::Success => Ok(Response::new(RenameFileResponse {
                success: true,
                error: String::new(),
            })),
            ClientResponse::Error { message } => Ok(Response::new(RenameFileResponse {
                success: false,
                error: message,
            })),
            _ => Ok(Response::new(RenameFileResponse {
                success: false,
                error: "unexpected response".into(),
            })),
        }
    }

    async fn set_file_size(
        &self,
        request: Request<SetFileSizeRequest>,
    ) -> Result<Response<SetFileSizeResponse>, Status> {
        let req = request.into_inner();
        tracing::debug!("SetFileSize: {} -> {}", req.path, req.new_size);
        let resp = self
            .raft_write(ClientRequest::SetFileSize {
                path: req.path,
                new_size: req.new_size,
            })
            .await?;
        match resp {
            ClientResponse::Success => Ok(Response::new(SetFileSizeResponse {
                success: true,
                error: String::new(),
            })),
            ClientResponse::Error { message } => Ok(Response::new(SetFileSizeResponse {
                success: false,
                error: message,
            })),
            _ => Ok(Response::new(SetFileSizeResponse {
                success: false,
                error: "unexpected response".into(),
            })),
        }
    }

    async fn create_symlink(
        &self,
        request: Request<CreateSymlinkRequest>,
    ) -> Result<Response<CreateSymlinkResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("Symlink: {} -> {}", req.path, req.target);
        let resp = self
            .raft_write(ClientRequest::CreateSymlink {
                path: req.path,
                target: req.target,
            })
            .await?;
        match resp {
            ClientResponse::Success => Ok(Response::new(CreateSymlinkResponse {
                success: true,
                error: String::new(),
            })),
            ClientResponse::Error { message } => Ok(Response::new(CreateSymlinkResponse {
                success: false,
                error: message,
            })),
            _ => Ok(Response::new(CreateSymlinkResponse {
                success: false,
                error: "unexpected response".into(),
            })),
        }
    }

    async fn read_symlink(
        &self,
        request: Request<ReadSymlinkRequest>,
    ) -> Result<Response<ReadSymlinkResponse>, Status> {
        let req = request.into_inner();
        let data = self.sm_data.read().await;
        if let Some(meta) = data.files.get(&req.path) {
            if let Some(t) = &meta.symlink_target {
                return Ok(Response::new(ReadSymlinkResponse {
                    target: t.clone(),
                    found: true,
                }));
            }
        }
        Ok(Response::new(ReadSymlinkResponse {
            target: String::new(),
            found: false,
        }))
    }

    async fn create_link(
        &self,
        request: Request<CreateLinkRequest>,
    ) -> Result<Response<CreateLinkResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("Link: {} -> {}", req.source, req.dest);
        let resp = self
            .raft_write(ClientRequest::CreateLink {
                source: req.source,
                dest: req.dest,
            })
            .await?;
        match resp {
            ClientResponse::Success => Ok(Response::new(CreateLinkResponse {
                success: true,
                error: String::new(),
            })),
            ClientResponse::Error { message } => Ok(Response::new(CreateLinkResponse {
                success: false,
                error: message,
            })),
            _ => Ok(Response::new(CreateLinkResponse {
                success: false,
                error: "unexpected response".into(),
            })),
        }
    }

    async fn cluster_capacity(
        &self,
        _request: Request<ClusterCapacityRequest>,
    ) -> Result<Response<ClusterCapacityResponse>, Status> {
        let data = self.sm_data.read().await;
        let total: u64 = data.nodes.values().map(|n| n.total_capacity).sum();
        let used: u64 = data.nodes.values().map(|n| n.used_capacity).sum();
        let files = data
            .files
            .values()
            .filter(|f| f.symlink_target.is_none())
            .count() as u64;
        Ok(Response::new(ClusterCapacityResponse {
            total_bytes: total,
            used_bytes: used,
            file_count: files,
        }))
    }

    // ---- Distributed POSIX byte-range locks (F-039) ----

    async fn acquire_lock(
        &self,
        request: Request<AcquireLockRequest>,
    ) -> Result<Response<AcquireLockResponse>, Status> {
        let req = request.into_inner();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let resp = self
            .raft_write(ClientRequest::AcquireLock {
                path: req.path,
                owner_id: req.owner_id,
                start: req.start,
                end: req.end,
                write: req.write,
                lease_ms: req.lease_ms.max(1000),
                now_ms,
            })
            .await?;
        match resp {
            ClientResponse::LockGranted => Ok(Response::new(AcquireLockResponse {
                granted: true,
                ..Default::default()
            })),
            ClientResponse::LockConflict {
                owner_id,
                start,
                end,
                write,
            } => Ok(Response::new(AcquireLockResponse {
                granted: false,
                conflict_owner: owner_id,
                conflict_start: start,
                conflict_end: end,
                conflict_write: write,
            })),
            ClientResponse::Error { message } => Err(Status::internal(message)),
            _ => Err(Status::internal("unexpected AcquireLock response")),
        }
    }

    async fn release_lock(
        &self,
        request: Request<ReleaseLockRequest>,
    ) -> Result<Response<ReleaseLockResponse>, Status> {
        let req = request.into_inner();
        let _ = self
            .raft_write(ClientRequest::ReleaseLock {
                path: req.path,
                owner_id: req.owner_id,
                start: req.start,
                end: req.end,
            })
            .await?;
        Ok(Response::new(ReleaseLockResponse { ok: true }))
    }

    async fn probe_lock(
        &self,
        request: Request<ProbeLockRequest>,
    ) -> Result<Response<ProbeLockResponse>, Status> {
        let req = request.into_inner();
        let data = self.sm_data.read().await;
        if let Some(locks) = data.posix_locks.get(&req.path) {
            for l in locks {
                if l.owner_id == req.owner_id {
                    continue;
                }
                if req.start >= l.end || req.end <= l.start {
                    continue;
                }
                if l.write || req.write {
                    return Ok(Response::new(ProbeLockResponse {
                        would_grant: false,
                        conflict_owner: l.owner_id.clone(),
                        conflict_start: l.start,
                        conflict_end: l.end,
                        conflict_write: l.write,
                    }));
                }
            }
        }
        Ok(Response::new(ProbeLockResponse {
            would_grant: true,
            ..Default::default()
        }))
    }

    // ---- P0-1: single-writer leases ----

    async fn acquire_write_lease(
        &self,
        request: Request<AcquireWriteLeaseRequest>,
    ) -> Result<Response<AcquireWriteLeaseResponse>, Status> {
        let req = request.into_inner();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        // The proposed_lease_id is computed once on the leader so every Raft
        // replica applies the same UUID deterministically.
        let proposed_lease_id = uuid::Uuid::new_v4().to_string();
        let resp = self
            .raft_write(ClientRequest::AcquireWriteLease {
                path: req.path,
                owner_id: req.owner_id,
                lease_ms: req.lease_ms.max(1_000).min(300_000),
                now_ms,
                proposed_lease_id,
            })
            .await?;
        match resp {
            ClientResponse::WriteLeaseGranted {
                lease_id,
                expires_at_ms,
            } => Ok(Response::new(AcquireWriteLeaseResponse {
                granted: true,
                lease_id,
                expires_at_ms,
                conflict_owner: String::new(),
                conflict_expires_at_ms: 0,
            })),
            ClientResponse::WriteLeaseConflict {
                owner_id,
                expires_at_ms,
            } => Ok(Response::new(AcquireWriteLeaseResponse {
                granted: false,
                lease_id: String::new(),
                expires_at_ms: 0,
                conflict_owner: owner_id,
                conflict_expires_at_ms: expires_at_ms,
            })),
            ClientResponse::Error { message } => Err(Status::internal(message)),
            _ => Err(Status::internal("unexpected AcquireWriteLease response")),
        }
    }

    async fn renew_write_lease(
        &self,
        request: Request<RenewWriteLeaseRequest>,
    ) -> Result<Response<RenewWriteLeaseResponse>, Status> {
        let req = request.into_inner();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let resp = self
            .raft_write(ClientRequest::RenewWriteLease {
                path: req.path,
                lease_id: req.lease_id,
                extension_ms: req.extension_ms.max(1_000).min(300_000),
                now_ms,
            })
            .await?;
        match resp {
            ClientResponse::WriteLeaseGranted {
                expires_at_ms, ..
            } => Ok(Response::new(RenewWriteLeaseResponse {
                granted: true,
                new_expires_at_ms: expires_at_ms,
            })),
            ClientResponse::WriteLeaseConflict { .. } => {
                Ok(Response::new(RenewWriteLeaseResponse {
                    granted: false,
                    new_expires_at_ms: 0,
                }))
            }
            ClientResponse::Error { message } => Err(Status::internal(message)),
            _ => Err(Status::internal("unexpected RenewWriteLease response")),
        }
    }

    async fn release_write_lease(
        &self,
        request: Request<ReleaseWriteLeaseRequest>,
    ) -> Result<Response<ReleaseWriteLeaseResponse>, Status> {
        let req = request.into_inner();
        let _ = self
            .raft_write(ClientRequest::ReleaseWriteLease {
                path: req.path,
                lease_id: req.lease_id,
            })
            .await?;
        Ok(Response::new(ReleaseWriteLeaseResponse { ok: true }))
    }

    async fn release_leases_by_host(
        &self,
        request: Request<ReleaseLeasesByHostRequest>,
    ) -> Result<Response<ReleaseLeasesByHostResponse>, Status> {
        let req = request.into_inner();
        let resp = self
            .raft_write(ClientRequest::ReleaseLeasesByHost {
                hostname_prefix: req.hostname,
                except_pid: req.except_pid,
            })
            .await?;
        let count = match resp {
            ClientResponse::WriteLeasesReleased { count } => count,
            _ => 0,
        };
        Ok(Response::new(ReleaseLeasesByHostResponse {
            released_count: count,
        }))
    }
}

/// Bug Z — pick the first tier with rf > 0 from a `TierReplication`,
/// returning the tier and its replication factor. Plexmedia ships
/// `hot=0, warm=2, cold=1` so the right tier for new allocations is
/// `Warm` with rf=2 (anchor handling adds the cold copy via the hash-ring
/// rule). Tier preference order is hot → warm → cold so existing
/// deployments with rf hot > 0 keep their old behavior.
pub(crate) fn pick_effective_tier(
    rep: &dtfs_common::types::TierReplication,
) -> (dtfs_common::types::DiskTier, u32) {
    use dtfs_common::types::DiskTier;
    if rep.hot > 0 {
        (DiskTier::Hot, rep.hot)
    } else if rep.warm > 0 {
        (DiskTier::Warm, rep.warm)
    } else if rep.cold > 0 {
        (DiskTier::Cold, rep.cold)
    } else {
        // Fully zero — caller will fall back through the round-robin path.
        (DiskTier::Hot, 1)
    }
}

/// Bug AV (AV-3-2) — parse the client-supplied `preferred_tier`
/// hint string to a `DiskTier`. Returns `None` for the empty
/// string (legacy clients) or any unrecognised value (forward-
/// compat: a future client adds a new tier name).
pub(crate) fn parse_preferred_tier_hint(
    hint: &str,
) -> Option<dtfs_common::types::DiskTier> {
    use dtfs_common::types::DiskTier;
    match hint {
        "" => None,
        "hot" => Some(DiskTier::Hot),
        "warm" => Some(DiskTier::Warm),
        "cold" => Some(DiskTier::Cold),
        "tape" => Some(DiskTier::Tape),
        _ => None,
    }
}

/// Bug AV (AV-4-1c) — top-level resolver used by the
/// `allocate_chunks` handler. Wraps `resolve_preferred_tier`
/// while pinning the contract that placement decisions MUST
/// consult `mandatory_replicas` (via
/// `effective_mandatory_replicas`), NOT the legacy
/// `replication` field. Pre-fix the handler passed legacy
/// `replication` directly, which silently routed writes onto
/// the cache tier (e.g. `mandatory.cold=1` + `legacy.warm=2` →
/// writes landed on warm), saturating the cache-replica cap on
/// first write and blocking every demand-driven promotion
/// forever.
///
/// Regression-tested by
/// `bug_av_4_1c_placement_uses_mandatory_not_legacy_replication`.
pub(crate) fn resolve_volume_placement(
    volume: &dtfs_common::types::VolumeConfig,
    hint: &str,
    available_tiers: &[dtfs_common::types::DiskTier],
) -> (dtfs_common::types::DiskTier, u32) {
    resolve_preferred_tier(
        hint,
        volume.effective_mandatory_replicas(),
        available_tiers,
    )
}

/// Bug AV (AV-4-1d) — wire-format string we put into
/// `ChunkPlacement.tier` so the FUSE writer can pass the
/// manager-resolved tier to the data-node's `put_chunk` RPC. Pins the
/// DiskTier → wire-string contract; if either side ever drifts, the
/// regression test catches it.
pub(crate) fn chunk_placement_tier_string(tier: dtfs_common::types::DiskTier) -> String {
    tier.to_string()
}

/// Bug AV (AV-3-2) — pure-function resolution of the
/// `preferred_tier` hint into a concrete `(tier, replication)`
/// pair. The placement engine uses this output instead of
/// `pick_effective_tier` for the first-chunk allocation.
///
/// Resolution rules:
/// 1. Empty or unknown hint → `pick_effective_tier(legacy_replication)`
///    (existing behaviour).
/// 2. Hint matches a tier that's actually available on a healthy
///    node in the cluster (per `available_tiers`) → honour the
///    hint and return `(tier, 1)`. RF is 1 because the
///    first-chunk placement on a non-mandatory tier is a single
///    cache replica; the volume's anchor-lag-repair (AV-3-3) +
///    under-replication scheduler add any further replicas
///    asynchronously.
/// 3. Hint refers to a tier with no healthy nodes → fall back
///    to `pick_effective_tier` so the write doesn't fail.
pub(crate) fn resolve_preferred_tier(
    hint: &str,
    legacy_replication: &dtfs_common::types::TierReplication,
    available_tiers: &[dtfs_common::types::DiskTier],
) -> (dtfs_common::types::DiskTier, u32) {
    if let Some(tier) = parse_preferred_tier_hint(hint) {
        if available_tiers.contains(&tier) {
            return (tier, 1);
        }
    }
    pick_effective_tier(legacy_replication)
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

/// Bug W — build the data node's reachable gRPC address.
///
/// The original code was:
/// ```ignore
/// let grpc_addr = format!("http://{}:{}", req.hostname, req.grpc_port);
/// ```
/// which assumed the data node's hostname resolves to a routable IPv4
/// address from every other node in the cluster. On the Plexmedia lab
/// the systemd-resolved stub returns IPv6 link-local (fe80::) addresses
/// for unqualified hostnames; tonic's HTTP/2 connector can't dial those
/// without a scope and the scheduler logs:
///
///   `connect to source http://data-warm01:9500: transport error`
///
/// All cross-node replication, migration, and read-fallback hits this
/// path, so it manifests as: writes succeed locally but never replicate;
/// data piles up on the first-chosen node; the cluster looks broken
/// even though every daemon is "healthy".
///
/// Fix: prefer the tonic peer address (the actual IP that opened the
/// RegisterNode gRPC connection). Fall back to the reported hostname
/// only when remote_addr isn't available (unit-test path, or a transport
/// that doesn't surface a peer — UDS etc.).
pub(crate) fn build_data_grpc_addr(
    peer: Option<std::net::SocketAddr>,
    reported_hostname: &str,
    reported_port: u32,
) -> String {
    if let Some(peer) = peer {
        // Use only the IP from the peer; the *port* comes from the
        // registration payload because the client's source port is
        // ephemeral, not the gRPC listen port. IPv6 addresses must be
        // bracketed for the URL to parse (`http://[2001:db8::1]:9500`),
        // matching RFC 3986 §3.2.2.
        let ip = peer.ip();
        if ip.is_ipv6() {
            return format!("http://[{}]:{}", ip, reported_port);
        }
        return format!("http://{}:{}", ip, reported_port);
    }
    format!("http://{}:{}", reported_hostname, reported_port)
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

    /// BL-1 — the leader's gRPC addr resolves from the voter set by
    /// matching node_id_from_hostname against the openraft leader id.
    #[test]
    fn bl1_resolve_leader_addr_matches_by_hostname_id() {
        let voters = vec![
            mk_voter("mini-mgr01", "10.0.0.1:9401"),
            mk_voter("mini-mgr02", "10.0.0.2:9401"),
            mk_voter("mini-mgr03", "10.0.0.3:9401"),
        ];
        let leader_id = dtfs_common::node_id_from_hostname("mini-mgr02");
        assert_eq!(
            resolve_leader_addr(&voters, leader_id),
            Some("10.0.0.2:9401".to_string())
        );
        // Unknown leader id (not in voter set) → None.
        assert_eq!(resolve_leader_addr(&voters, 0), None);
    }
}
