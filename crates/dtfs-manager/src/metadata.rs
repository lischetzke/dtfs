use std::collections::BTreeMap;
use std::sync::Arc;

use dtfs_common::config::ManagerConfig;
use dtfs_common::topology::Location;
use dtfs_common::types::{DiskStatus, DiskTier, HealthStatus, NodeRole};
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

        let cfg_version = self.sm_data.read().await.cluster_config.version;

        let _ = self
            .raft_write(ClientRequest::UpdateNodeUsage {
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

        let _ = self
            .raft_write(ClientRequest::UpdateNodeStatus {
                node_id,
                status: "healthy".to_string(),
            })
            .await;

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

        Ok(Response::new(HeartbeatResponse {
            accepted: true,
            config_version: cfg_version,
            config_update: None,
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
        let volume_cfg = data.volumes.get(&req.volume_id).cloned();
        let (effective_tier, effective_rf, anchor_dc) = match &volume_cfg {
            Some(v) => {
                let (tier, rf) = pick_effective_tier(&v.replication);
                (tier, rf, v.anchor_datacenter.clone())
            }
            None => (
                DiskTier::Hot,
                self.config.defaults.replication.hot.max(1),
                self.config.defaults.anchor_datacenter.clone(),
            ),
        };
        let replication = effective_rf.max(1).min(data.nodes.len() as u32);

        let placed = self.placement.place_chunks_with_anchor(
            &req.volume_id,
            req.num_chunks,
            effective_tier,
            replication,
            anchor_dc.clone(),
        );

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
                });
            }
        }

        if placements.len() != req.num_chunks as usize {
            // Fallback: round-robin across all HEALTHY nodes. Includes replica
            // addresses so that the replication factor is still respected and
            // skips nodes that aren't Healthy so writes don't target Down hosts
            // (fix for F-010 + F-012).
            placements.clear();
            let node_list: Vec<_> = data
                .nodes
                .values()
                .filter(|n| n.health == dtfs_common::types::HealthStatus::Healthy)
                .collect();
            if node_list.is_empty() {
                return Err(Status::unavailable("No healthy data nodes available"));
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
}
