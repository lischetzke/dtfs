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

/// Handle that main can use to drive Bug U graceful shutdown after the
/// gRPC server has finished accepting new connections.
pub struct ServerContext {
    /// Bug D / Bug G semaphore that bounds in-flight put_chunk operations.
    /// On SIGTERM we wait for every permit to come back before exiting.
    pub write_slots: Arc<Semaphore>,
    /// Initial permit count — Semaphore doesn't expose this.
    pub max_writes: usize,
}

pub async fn run_grpc_server(
    config: DataNodeConfig,
    store: Arc<ChunkStore>,
    metrics: Arc<DataMetrics>,
    cache: Arc<ArcCache>,
    memory: Arc<MemoryBudget>,
    shutdown: impl std::future::Future<Output = ()>,
    ctx_tx: Option<tokio::sync::oneshot::Sender<ServerContext>>,
) -> anyhow::Result<()> {
    let addr = config.grpc.listen_addr.parse()?;
    let replication = Arc::new(ReplicationManager::new(store.clone(), config.clone()));

    // Bug D / Bug G — derive the put_chunk concurrency limit. If the
    // operator set a hard memory cap, recommend a slot count that fits
    // alongside the ARC cache + tonic + raft overhead. Otherwise fall
    // back to whatever they configured in `grpc.max_concurrent_chunk_writes`
    // (default 8). The data node doesn't have per-volume chunk size
    // visibility here, so we use a conservative worst-case estimate of
    // 16 MB: 4× the workspace default chunk size (4 MB) so larger
    // volumes still get a sensible (smaller) slot count rather than
    // over-allocating slots that would breach the memory budget.
    // Streaming put_chunk (Bug I) means the actual in-memory cost per
    // slot is ~1 MB, so this estimate is mostly defensive.
    let chunk_size_bytes: u64 = 16 * 1024 * 1024;
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
    let write_slots = Arc::new(Semaphore::new(max_writes));
    // Bug U — hand the slot semaphore back to main so it can wait for the
    // in-flight writes to drain when SIGTERM arrives.
    if let Some(tx) = ctx_tx {
        let _ = tx.send(ServerContext {
            write_slots: write_slots.clone(),
            max_writes,
        });
    }

    let service = DataServiceImpl {
        config: config.clone(),
        store,
        replication,
        metrics,
        cache,
        write_slots,
        memory,
    };

    tracing::info!("Data node gRPC server listening on {}", addr);

    // Bug U — `serve_with_shutdown` lets tonic stop accepting new connections
    // when the signal fires, but in-flight requests keep running until they
    // complete (or until we abandon them in main).
    tonic::transport::Server::builder()
        .add_service(
            DataServiceServer::new(service)
                // Allow the 8 MB streaming pieces in GetChunk (default cap
                // is 4 MB).
                .max_decoding_message_size(32 * 1024 * 1024)
                .max_encoding_message_size(32 * 1024 * 1024),
        )
        .serve_with_shutdown(addr, shutdown)
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
        // ResourceExhausted with backoff. With Bug I in place the
        // streaming write only buffers ~1 MB per concurrent call, so
        // shedding should almost never fire — it's a hard fail-safe
        // for pathological cases (kernel-side bloat, RocksDB compaction
        // pause, etc.).
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
        // Bug D — bound the number of concurrent in-flight writes. With
        // streaming-to-disk (Bug I) each slot only buffers one
        // STREAM_PIECE_SIZE worth of bytes in memory (~1 MB), so 8 slots
        // = ~8 MB peak instead of the legacy ~1 GB.
        let _slot = self
            .write_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| Status::internal(format!("put_chunk slot acquire: {e}")))?;

        // Bug I — stream the incoming gRPC pieces straight to a tmp
        // file on the target disk, hashing incrementally. We do NOT
        // accumulate the chunk in a Vec.
        let mut stream = request.into_inner();

        // Pull the first piece to learn the requested volume_id + tier.
        let first = match stream
            .message()
            .await
            .map_err(|e| Status::internal(e.to_string()))?
        {
            Some(m) => m,
            None => {
                return Err(Status::invalid_argument(
                    "put_chunk: empty request stream",
                ));
            }
        };

        let tier = match first.tier.as_str() {
            "hot" => DiskTier::Hot,
            "warm" => DiskTier::Warm,
            "cold" => DiskTier::Cold,
            _ => DiskTier::Hot,
        };
        let vol_id = if first.volume_id.is_empty() {
            VolumeId::new("default".to_string())
        } else {
            VolumeId::new(first.volume_id.clone())
        };

        // Allocate the tmp file on the disk that will host the chunk.
        let (tmp_path, _disk_mount) = self
            .store
            .allocate_streaming_tmp(tier)
            .map_err(|e| Status::internal(format!("allocate tmp: {e}")))?;

        let mut hasher = dtfs_common::checksum::IncrementalHasher::new();
        let mut total_size: u64 = 0;
        {
            let mut f = tokio::fs::File::create(&tmp_path).await.map_err(|e| {
                Status::internal(format!("create tmp {}: {e}", tmp_path.display()))
            })?;
            // First piece (already consumed from the stream).
            if !first.data.is_empty() {
                hasher.update(&first.data);
                total_size += first.data.len() as u64;
                tokio::io::AsyncWriteExt::write_all(&mut f, &first.data)
                    .await
                    .map_err(|e| Status::internal(format!("write tmp: {e}")))?;
            }
            // Remaining pieces.
            while let Some(msg) = stream
                .message()
                .await
                .map_err(|e| Status::internal(e.to_string()))?
            {
                if msg.data.is_empty() {
                    continue;
                }
                hasher.update(&msg.data);
                total_size += msg.data.len() as u64;
                tokio::io::AsyncWriteExt::write_all(&mut f, &msg.data)
                    .await
                    .map_err(|e| Status::internal(format!("write tmp: {e}")))?;
            }
            tokio::io::AsyncWriteExt::flush(&mut f)
                .await
                .map_err(|e| Status::internal(format!("flush tmp: {e}")))?;
            // fsync is done implicitly by commit_chunk_from_tmp before
            // the rename to the final path; we can save the duplicate
            // sync_all on the tmp.
        }
        let hash = hasher.finalize();
        let chunk_id = ChunkId::from_bytes(hash);

        let (meta, disk_id) = self
            .store
            .commit_chunk_from_tmp(&chunk_id, &tmp_path, &vol_id, tier, total_size, hash)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Bug AL — record the write so the manager-side I/O Throughput
        // card can compute bytes/sec. Pre-fix `record_write` was
        // declared in metrics.rs but never called from server.rs, so
        // every data node's `total_bytes_written` stayed at 0 forever
        // and the dashboard "↑ Write" showed 0 B/s under any load.
        self.metrics.record_write(total_size);

        // Bug I — do NOT insert the just-written chunk into the ARC
        // cache. Caching every write defeats the streaming-to-disk
        // memory win (it puts the whole chunk back in RAM). Cold-read
        // performance is unchanged; the first reader pulls from disk
        // and populates the cache then.
        let id_hex = chunk_id.as_hex();
        let _ = &self.cache; // intentionally unused on the write path

        Ok(Response::new(PutChunkResponse {
            chunk_id: id_hex,
            size: meta.size,
            checksum: hex::encode(&meta.checksum),
            success: true,
            disk_id,
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

        // Bug AL — record the read for the manager-side I/O Throughput
        // rolling-rate. Cache hits are still real read traffic to the
        // requesting FUSE client, so count them too.
        self.metrics.record_read(data.len() as u64);

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

        // Bug AV — idempotent delete. The cache-flush scheduler routine
        // (AV-1-7) calls this directly; missing chunks (e.g. a retry
        // after a previously-successful delete) return success with
        // was_present=false rather than tearing down the whole flush
        // batch with an error.
        let (was_present, freed_bytes) = self
            .store
            .delete_chunk_idempotent(&chunk_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Bug AL — keep the read-counter machinery consistent; a
        // deleted chunk is no longer eligible for serving so its
        // per-DC read counter (if any) is dropped.
        self.metrics.forget_chunk(&req.chunk_id);

        Ok(Response::new(DeleteChunkResponse {
            success: true,
            was_present,
            freed_bytes,
        }))
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

    /// Bug AV (AV-4-1c) — record a client-side cache hit. The
    /// FUSE-side readahead + local-disk caches absorb repeat
    /// reads so the data-node never sees them via `get_chunk` —
    /// which means the per-DC read counter never reaches the
    /// promote_reads threshold and the tiering engine never
    /// fires a promotion. The FUSE client calls this on every
    /// readahead/local-cache hit so the counter reflects the
    /// real client-facing demand.
    ///
    /// Idempotent (counter increment is associative). No
    /// disk/cache I/O on the data-node side — just a counter
    /// bump.
    async fn record_cache_hit(
        &self,
        request: Request<RecordCacheHitRequest>,
    ) -> Result<Response<RecordCacheHitResponse>, Status> {
        let req = request.into_inner();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.metrics
            .record_chunk_read(&req.chunk_id, &req.requester_dc, now_ms);
        // Bug AL — also count the read for the manager's
        // dashboard rate calculation. The bytes-served estimate
        // is unknown at this layer, so we report 0 bytes; the
        // count side still tracks correctly because record_read
        // bumps a separate counter.
        // (We deliberately do NOT call record_read(bytes) here
        // — bandwidth came from FUSE-local cache, not from this
        // data node. Recording the read for tiering is the
        // semantic we care about.)
        Ok(Response::new(RecordCacheHitResponse {}))
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
