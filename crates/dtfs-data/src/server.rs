use std::sync::Arc;

use bytes::Bytes;
use dtfs_common::config::DataNodeConfig;
use dtfs_common::types::{ChunkId, DiskStatus, DiskTier, VolumeId};
use dtfs_proto::dtfs::data::data_service_server::{DataService, DataServiceServer};
use dtfs_proto::dtfs::data::*;
use tokio::sync::{mpsc, Semaphore};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::cache::ArcCache;
use crate::memory::MemoryBudget;
use crate::metrics::DataMetrics;
use crate::replication::ReplicationManager;
use crate::store::ChunkStore;

pub struct DataServiceImpl {
    config: DataNodeConfig,
    store: Arc<ChunkStore>,
    replication: Arc<ReplicationManager>,
    metrics: Arc<DataMetrics>,
    cache: Arc<ArcCache>,
    /// Bug D — caps in-flight `put_chunk` operations so peak memory is
    /// bounded. Each in-flight write buffers up to one chunk
    /// (~128 MB by default), so an unbounded gRPC stream count is fatal
    /// on small VMs (4–8 GB RAM). Default 8 via
    /// `config.grpc.max_concurrent_chunk_writes`. Bug G now sizes this
    /// from the hard memory cap when one is configured.
    write_slots: Arc<Semaphore>,
    /// Bug G — runtime memory-pressure observer. When `is_shedding()`
    /// flips true, new `put_chunk` calls return `RESOURCE_EXHAUSTED`
    /// instead of accumulating more chunks into RAM.
    memory: Arc<MemoryBudget>,
}

pub async fn run_grpc_server(
    config: DataNodeConfig,
    store: Arc<ChunkStore>,
    metrics: Arc<DataMetrics>,
    cache: Arc<ArcCache>,
    memory: Arc<MemoryBudget>,
) -> anyhow::Result<()> {
    let addr = config.grpc.listen_addr.parse()?;
    let replication = Arc::new(ReplicationManager::new(store.clone(), config.clone()));

    // Bug D / Bug G — derive the put_chunk concurrency limit. If the
    // operator set a hard memory cap, recommend a slot count that fits
    // alongside the ARC cache + tonic + raft overhead. Otherwise fall
    // back to whatever they configured in `grpc.max_concurrent_chunk_writes`
    // (default 8). The data node doesn't have per-volume chunk size
    // visibility here, so we estimate at 128 MB (the cluster default).
    let chunk_size_bytes: u64 = 128 * 1024 * 1024;
    // Overhead estimate: ARC cache + 256 MB for tonic/raft/kernel pages.
    let static_overhead_bytes =
        (config.cache.max_memory_mb as u64) * 1024 * 1024 + 256 * 1024 * 1024;
    let fallback_slots = config.grpc.max_concurrent_chunk_writes.max(1);
    let max_writes =
        memory.recommend_chunk_write_slots(chunk_size_bytes, static_overhead_bytes, fallback_slots);
    if memory.hard_cap_bytes() > 0 {
        tracing::info!(
            cap_mb = memory.hard_cap_bytes() / 1024 / 1024,
            chunk_mb = chunk_size_bytes / 1024 / 1024,
            overhead_mb = static_overhead_bytes / 1024 / 1024,
            slots = max_writes,
            "Bug G memory budget engaged: sized put_chunk slots from hard cap"
        );
    } else {
        tracing::info!(
            "Bug D put_chunk concurrency limit: {} in-flight writes (no hard memory cap configured)",
            max_writes
        );
    }
    let service = DataServiceImpl {
        config: config.clone(),
        store,
        replication,
        metrics,
        cache,
        write_slots: Arc::new(Semaphore::new(max_writes)),
        memory,
    };

    tracing::info!("Data node gRPC server listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(
            DataServiceServer::new(service)
                // Allow the 8 MB streaming pieces in GetChunk (default cap
                // is 4 MB).
                .max_decoding_message_size(32 * 1024 * 1024)
                .max_encoding_message_size(32 * 1024 * 1024),
        )
        .serve(addr)
        .await?;

    Ok(())
}

fn disk_status_proto(s: DiskStatus) -> String {
    match s {
        DiskStatus::Healthy => "healthy",
        DiskStatus::Degraded => "degraded",
        DiskStatus::Draining => "draining",
        DiskStatus::Failed => "failed",
        DiskStatus::Removed => "removed",
    }
    .to_string()
}

#[tonic::async_trait]
impl DataService for DataServiceImpl {
    type GetChunkStream = ReceiverStream<Result<GetChunkResponse, Status>>;

    async fn put_chunk(
        &self,
        request: Request<Streaming<PutChunkRequest>>,
    ) -> Result<Response<PutChunkResponse>, Status> {
        // Bug G — if a hard memory cap is configured and the background
        // sampler has flagged the process as past the shed threshold,
        // refuse the write outright. The FUSE client retries on
        // ResourceExhausted with backoff, which gives the kernel +
        // RocksDB time to flush buffers and drop the RSS below the
        // hysteresis floor.
        if self.memory.is_shedding() {
            tracing::warn!(
                rss_mb = self.memory.last_rss_bytes() / 1024 / 1024,
                cap_mb = self.memory.hard_cap_bytes() / 1024 / 1024,
                "put_chunk rejected: memory budget shedding"
            );
            return Err(Status::resource_exhausted(
                "data node over memory shed threshold (Bug G) — retry shortly",
            ));
        }
        // Bug D — acquire one of the bounded concurrency slots before we
        // start buffering this chunk's bytes. Without this gate every
        // concurrent gRPC stream holds ~128 MB in memory and a few dozen
        // writers under sustained streaming load OOM-kill the data node.
        // Waiting here gives the kernel TCP backpressure to the writer
        // instead — slower, but never crashes.
        let _slot = self
            .write_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| Status::internal(format!("put_chunk slot acquire: {e}")))?;
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

        let id_hex = chunk_id.as_hex();
        self.cache.put(id_hex.clone(), Bytes::from(data));

        Ok(Response::new(PutChunkResponse {
            chunk_id: id_hex,
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

        let chunk_id_bytes = chunk_hex::decode(&req.chunk_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid chunk ID: {}", e)))?;
        if chunk_id_bytes.len() != 32 {
            return Err(Status::invalid_argument("Chunk ID must be 32 bytes"));
        }
        let mut id_array = [0u8; 32];
        id_array.copy_from_slice(&chunk_id_bytes);
        let chunk_id = ChunkId::from_bytes(id_array);
        let key = req.chunk_id.clone();

        // P0-9: record this read against the requester's DC for tiering's
        // auto-move-replica-toward-active-DC decisions.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.metrics
            .record_chunk_read(&req.chunk_id, &req.requester_dc, now_ms);

        let data = if let Some(cached) = self.cache.get(&key) {
            self.metrics.record_cache_hit();
            cached
        } else {
            self.metrics.record_cache_miss();
            let loaded = self
                .store
                .get_chunk(&chunk_id)
                .await
                .map_err(|e| Status::not_found(e.to_string()))?;
            self.cache.put(key, loaded.clone());
            loaded
        };

        // Stream pieces of 8 MB each through a 64-slot channel — gives
        // tonic enough room to keep the HTTP/2 send window full on a
        // multi-gigabit LAN. The client side bumps its max_decoding_message_size
        // to match (default is only 4 MB).
        // 2 MB pieces + channel(32) capped reads at ~5 MB/s in the F-011
        // verification, even with healthy disk and network underneath.
        const PIECE_SIZE: usize = 8 * 1024 * 1024;
        let (tx, rx) = mpsc::channel(64);

        tokio::spawn(async move {
            let total = data.len();
            let mut offset = 0;

            while offset < total {
                let end = std::cmp::min(offset + PIECE_SIZE, total);
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
        let chunk_id_bytes = chunk_hex::decode(&req.chunk_id)
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
        let chunk_id_bytes = chunk_hex::decode(&req.chunk_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid chunk ID: {}", e)))?;
        if chunk_id_bytes.len() != 32 {
            return Err(Status::invalid_argument("Chunk ID must be 32 bytes"));
        }
        let mut id_array = [0u8; 32];
        id_array.copy_from_slice(&chunk_id_bytes);
        let chunk_id = ChunkId::from_bytes(id_array);

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
        let disks: Vec<dtfs_proto::dtfs::common::DiskInfo> = self
            .store
            .disk_manager()
            .all_disks()
            .iter()
            .map(|d| dtfs_proto::dtfs::common::DiskInfo {
                disk_id: d.disk_id,
                uuid: String::new(),
                mount_path: d.mount_path.display().to_string(),
                tier: d.tier.to_string(),
                total_bytes: d.total_bytes,
                used_bytes: d.used_bytes,
                status: disk_status_proto(d.status),
            })
            .collect();

        Ok(Response::new(DiskStatusResponse {
            hostname: self.config.node.hostname.clone(),
            disks,
        }))
    }

    async fn replicate_chunk(
        &self,
        request: Request<ReplicateChunkRequest>,
    ) -> Result<Response<ReplicateChunkResponse>, Status> {
        let req = request.into_inner();

        let chunk_id_bytes = chunk_hex::decode(&req.chunk_id)
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
        let store_stats = self.store.stats();
        Ok(Response::new(StatsResponse {
            total_chunks: store_stats.total_chunks,
            total_bytes: store_stats.total_bytes_stored,
            cache_hits: snap.cache_hits,
            cache_misses: snap.cache_misses,
        }))
    }
}

mod chunk_hex {
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
