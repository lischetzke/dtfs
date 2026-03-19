use std::sync::Arc;

use dtfs_common::config::ManagerConfig;
use dtfs_proto::dtfs::manager::manager_service_server::ManagerService;
use dtfs_proto::dtfs::manager::*;
use dtfs_raft::state_machine::StateMachineData;
use dtfs_raft::{ClientRequest, ClientResponse, DtfsRaft};
use tonic::{Request, Response, Status};
use tracing::info;

/// gRPC metadata service backed by Raft-replicated state.
///
/// Write operations (CreateFile, DeleteFile) are replicated through Raft.
/// Read operations are served from the local state machine copy (which may be
/// slightly stale on followers).
pub struct MetadataService {
    pub config: ManagerConfig,
    pub raft: Arc<DtfsRaft>,
    pub sm_data: Arc<tokio::sync::RwLock<StateMachineData>>,
}

impl MetadataService {
    pub fn new(
        config: ManagerConfig,
        raft: Arc<DtfsRaft>,
        sm_data: Arc<tokio::sync::RwLock<StateMachineData>>,
    ) -> Self {
        Self {
            config,
            raft,
            sm_data,
        }
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

        // Read chunk_size from config defaults.
        let chunk_size = self.config.defaults.chunk_size_mb as u64 * 1024 * 1024;

        // Replicate through Raft.
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

        // Read from local state machine (no Raft write needed).
        let data = self.sm_data.read().await;
        let file = data
            .files
            .get(&req.path)
            .ok_or_else(|| Status::not_found("File not found"))?;

        let chunk_locations: Vec<ChunkLocationInfo> = file
            .chunks
            .iter()
            .map(|c| ChunkLocationInfo {
                chunk_id: c.chunk_hash.clone(),
                size: c.size_bytes,
                node_addresses: vec![c.node_id.clone()],
            })
            .collect();

        let total_size: u64 = file.chunks.iter().map(|c| c.size_bytes).sum();

        Ok(Response::new(GetFileLocationsResponse {
            path: req.path,
            total_size,
            chunks: chunk_locations,
        }))
    }

    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        let req = request.into_inner();
        info!("RegisterNode: {}", req.hostname);

        let node_id_str = uuid::Uuid::new_v4().to_string();

        Ok(Response::new(RegisterNodeResponse {
            accepted: true,
            node_id: node_id_str,
            config_version: 0,
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        tracing::debug!("Heartbeat from node: {}", req.node_id);

        Ok(Response::new(HeartbeatResponse {
            accepted: true,
            config_version: 0,
            config_update: None,
        }))
    }

    async fn report_chunk_status(
        &self,
        request: Request<ChunkStatusReport>,
    ) -> Result<Response<ChunkStatusAck>, Status> {
        let req = request.into_inner();
        tracing::debug!("ChunkStatusReport: {} chunks", req.chunks.len());

        Ok(Response::new(ChunkStatusAck { accepted: true }))
    }

    async fn delete_file(
        &self,
        request: Request<DeleteFileRequest>,
    ) -> Result<Response<DeleteFileResponse>, Status> {
        let req = request.into_inner();
        info!("DeleteFile: {}", req.path);

        // Replicate through Raft.
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

        // Read from local state machine.
        let data = self.sm_data.read().await;

        let prefix = if req.path.ends_with('/') {
            req.path.clone()
        } else {
            format!("{}/", req.path)
        };

        let entries: Vec<DirectoryEntry> = data
            .files
            .range(prefix.clone()..)
            .take_while(|(k, _)| k.starts_with(&prefix))
            .map(|(path, meta)| DirectoryEntry {
                name: path
                    .strip_prefix(&prefix)
                    .unwrap_or(path)
                    .to_string(),
                size: meta.chunks.iter().map(|c| c.size_bytes).sum(),
                is_directory: false,
                created_at: meta.created_at_ms as i64,
                modified_at: meta.updated_at_ms as i64,
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

        // Read nodes from state machine.
        let data = self.sm_data.read().await;

        if data.nodes.is_empty() {
            return Err(Status::unavailable("No data nodes available"));
        }

        let node_list: Vec<_> = data.nodes.values().collect();

        // Simple round-robin allocation (will be replaced by PlacementEngine
        // integration in a future iteration).
        let mut placements = Vec::new();
        for i in 0..req.num_chunks {
            let node = &node_list[i as usize % node_list.len()];
            placements.push(ChunkPlacement {
                chunk_index: i,
                primary_address: node.grpc_addr.clone(),
                replica_addresses: vec![],
            });
        }

        Ok(Response::new(AllocateChunksResponse { placements }))
    }
}
