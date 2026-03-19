use std::sync::Arc;

use dtfs_common::config::DataNodeConfig;
use dtfs_common::types::{ChunkId, DiskTier, VolumeId};
use dtfs_proto::dtfs::data::data_service_server::{DataService, DataServiceServer};
use dtfs_proto::dtfs::data::*;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::metrics::DataMetrics;
use crate::replication::ReplicationManager;
use crate::store::ChunkStore;

pub struct DataServiceImpl {
    config: DataNodeConfig,
    store: Arc<ChunkStore>,
    replication: Arc<ReplicationManager>,
    metrics: Arc<DataMetrics>,
}

pub async fn run_grpc_server(
    config: DataNodeConfig,
    store: Arc<ChunkStore>,
    metrics: Arc<DataMetrics>,
) -> anyhow::Result<()> {
    let addr = config.grpc.listen_addr.parse()?;
    let replication = Arc::new(ReplicationManager::new(store.clone(), config.clone()));
    let service = DataServiceImpl {
        config: config.clone(),
        store,
        replication,
        metrics,
    };

    tracing::info!("Data node gRPC server listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(DataServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}

#[tonic::async_trait]
impl DataService for DataServiceImpl {
    type GetChunkStream = ReceiverStream<Result<GetChunkResponse, Status>>;

    async fn put_chunk(
        &self,
        request: Request<Streaming<PutChunkRequest>>,
    ) -> Result<Response<PutChunkResponse>, Status> {
        let mut stream = request.into_inner();
        let mut data = Vec::new();
        let mut volume_id = String::new();
        let mut tier = DiskTier::Hot;

        while let Some(msg) = stream
            .message()
            .await
            .map_err(|e| Status::internal(e.to_string()))?
        {
            if !msg.volume_id.is_empty() {
                volume_id = msg.volume_id;
            }
            if !msg.tier.is_empty() {
                tier = match msg.tier.as_str() {
                    "hot" => DiskTier::Hot,
                    "warm" => DiskTier::Warm,
                    "cold" => DiskTier::Cold,
                    _ => DiskTier::Hot,
                };
            }
            data.extend_from_slice(&msg.data);
        }

        // Compute chunk ID from data hash
        let hash = dtfs_common::checksum::compute(&data);
        let chunk_id = ChunkId::from_bytes(hash);

        let vol_id = if volume_id.is_empty() {
            VolumeId::new("default".to_string())
        } else {
            VolumeId::new(volume_id)
        };

        let meta = self
            .store
            .put_chunk(&chunk_id, &data, &vol_id, tier)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(PutChunkResponse {
            chunk_id: chunk_id.as_hex(),
            size: meta.size,
            checksum: hex::encode(&meta.checksum),
            success: true,
        }))
    }

    async fn get_chunk(
        &self,
        request: Request<GetChunkRequest>,
    ) -> Result<Response<Self::GetChunkStream>, Status> {
        let req = request.into_inner();

        // Parse chunk ID from hex
        let chunk_id_bytes = hex::decode(&req.chunk_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid chunk ID: {}", e)))?;
        if chunk_id_bytes.len() != 32 {
            return Err(Status::invalid_argument("Chunk ID must be 32 bytes"));
        }
        let mut id_array = [0u8; 32];
        id_array.copy_from_slice(&chunk_id_bytes);
        let chunk_id = ChunkId::from_bytes(id_array);

        let data = self
            .store
            .get_chunk(&chunk_id)
            .await
            .map_err(|e| Status::not_found(e.to_string()))?;

        let (tx, rx) = mpsc::channel(4);

        // Stream the data in 1MB chunks
        tokio::spawn(async move {
            let chunk_size = 1024 * 1024; // 1MB streaming chunks
            let total = data.len();
            let mut offset = 0;

            while offset < total {
                let end = std::cmp::min(offset + chunk_size, total);
                let chunk_data = data.slice(offset..end);

                let resp = GetChunkResponse {
                    data: chunk_data.to_vec(),
                    offset: offset as u64,
                    total_size: total as u64,
                };

                if tx.send(Ok(resp)).await.is_err() {
                    break;
                }
                offset = end;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn delete_chunk(
        &self,
        request: Request<DeleteChunkRequest>,
    ) -> Result<Response<DeleteChunkResponse>, Status> {
        let req = request.into_inner();
        let chunk_id_bytes = hex::decode(&req.chunk_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid chunk ID: {}", e)))?;
        if chunk_id_bytes.len() != 32 {
            return Err(Status::invalid_argument("Chunk ID must be 32 bytes"));
        }
        let mut id_array = [0u8; 32];
        id_array.copy_from_slice(&chunk_id_bytes);
        let chunk_id = ChunkId::from_bytes(id_array);

        self.store
            .delete_chunk(&chunk_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(DeleteChunkResponse { success: true }))
    }

    async fn verify_chunk(
        &self,
        request: Request<VerifyChunkRequest>,
    ) -> Result<Response<VerifyChunkResponse>, Status> {
        let req = request.into_inner();
        let chunk_id_bytes = hex::decode(&req.chunk_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid chunk ID: {}", e)))?;
        if chunk_id_bytes.len() != 32 {
            return Err(Status::invalid_argument("Chunk ID must be 32 bytes"));
        }
        let mut id_array = [0u8; 32];
        id_array.copy_from_slice(&chunk_id_bytes);
        let chunk_id = ChunkId::from_bytes(id_array);

        // Try to read the chunk — this verifies checksum
        match self.store.get_chunk(&chunk_id).await {
            Ok(_) => Ok(Response::new(VerifyChunkResponse {
                valid: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(VerifyChunkResponse {
                valid: false,
                error: e.to_string(),
            })),
        }
    }

    async fn get_disk_status(
        &self,
        _request: Request<DiskStatusRequest>,
    ) -> Result<Response<DiskStatusResponse>, Status> {
        Ok(Response::new(DiskStatusResponse {
            hostname: self.config.node.hostname.clone(),
            disks: vec![], // TODO: populate from disk manager
        }))
    }

    async fn replicate_chunk(
        &self,
        request: Request<ReplicateChunkRequest>,
    ) -> Result<Response<ReplicateChunkResponse>, Status> {
        let req = request.into_inner();

        let chunk_id_bytes = hex::decode(&req.chunk_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid chunk ID: {}", e)))?;
        if chunk_id_bytes.len() != 32 {
            return Err(Status::invalid_argument("Chunk ID must be 32 bytes"));
        }
        let mut id_array = [0u8; 32];
        id_array.copy_from_slice(&chunk_id_bytes);
        let chunk_id = ChunkId::from_bytes(id_array);

        match self
            .replication
            .replicate_to(&chunk_id, &req.target_address)
            .await
        {
            Ok(()) => Ok(Response::new(ReplicateChunkResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(ReplicateChunkResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn report_stats(
        &self,
        _request: Request<StatsRequest>,
    ) -> Result<Response<StatsResponse>, Status> {
        let snap = self.metrics.snapshot();
        Ok(Response::new(StatsResponse {
            total_chunks: snap.total_reads + snap.total_writes,
            total_bytes: snap.total_bytes_read + snap.total_bytes_written,
            cache_hits: snap.cache_hits,
            cache_misses: snap.cache_misses,
        }))
    }
}

mod hex {
    pub fn decode(s: &str) -> Result<Vec<u8>, String> {
        if s.len() % 2 != 0 {
            return Err("Hex string must have even length".to_string());
        }
        (0..s.len())
            .step_by(2)
            .map(|i| {
                u8::from_str_radix(&s[i..i + 2], 16)
                    .map_err(|e| format!("Invalid hex char: {}", e))
            })
            .collect()
    }

    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}
