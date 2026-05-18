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
        self.raft
            .client_write(req)
            .await
            .map(|resp| resp.data)
            .map_err(|e| Status::internal(format!("Raft write failed: {}", e)))
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
        let req = request.into_inner();
        info!("RegisterNode: {}", req.hostname);

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
        let grpc_addr = format!("http://{}:{}", req.hostname, req.grpc_port);
        let disks = disks_from_register_req(&req.disks);
        let total_capacity: u64 = disks.iter().map(|d| d.total_bytes).sum();
        let used_capacity: u64 = disks.iter().map(|d| d.used_bytes).sum();

        let info = RaftNodeInfo {
            role: if req.role.eq_ignore_ascii_case("manager") {
                NodeRole::Manager
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

        let node_id: u64 = req
            .node_id
            .parse()
            .map_err(|_| Status::invalid_argument("node_id must be a u64 from registration"))?;

        let mut unhealthy = false;
        for ch in &req.chunks {
            if ch.status.eq_ignore_ascii_case("corrupt")
                || ch.status.eq_ignore_ascii_case("missing")
            {
                unhealthy = true;
                break;
            }
        }

        let status = if unhealthy { "degraded" } else { "healthy" };
        let _ = self
            .raft_write(ClientRequest::UpdateNodeStatus {
                node_id,
                status: status.to_string(),
            })
            .await;

        Ok(Response::new(ChunkStatusAck { accepted: true }))
    }

    async fn delete_file(
        &self,
        request: Request<DeleteFileRequest>,
    ) -> Result<Response<DeleteFileResponse>, Status> {
        let req = request.into_inner();
        info!("DeleteFile: {}", req.path);

        let resp = self
            .raft_write(ClientRequest::DeleteFile {
                path: req.path.clone(),
            })
            .await?;

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

        let replication = self
            .config
            .defaults
            .replication
            .hot
            .max(1)
            .min(data.nodes.len() as u32);

        let placed = self.placement.place_chunks(
            &req.volume_id,
            req.num_chunks,
            DiskTier::Hot,
            replication,
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
                path: req.path,
                chunks,
            })
            .await?;

        match resp {
            ClientResponse::Success => Ok(Response::new(CommitChunkLocationsResponse {
                success: true,
            })),
            ClientResponse::Error { message } => Err(Status::failed_precondition(message)),
            _ => Ok(Response::new(CommitChunkLocationsResponse { success: true })),
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
