use std::sync::Arc;

use dtfs_common::config::DataNodeConfig;
use dtfs_common::error::DtfsError;
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
use dtfs_common::checksum;
use dtfs_common::config::ScrubConfig;
use dtfs_common::crypto::{
    stamp_data_rpc_auth, verify_data_rpc_auth, DATA_RPC_AUTH_MAC, DATA_RPC_AUTH_TS,
};
use tokio::sync::RwLock;

/// Bug BU-6 — RAII guard that removes a streaming `.tmp` file unless
/// disarmed. `put_chunk` arms it right after allocating the tmp and
/// disarms it only after `commit_chunk_from_tmp` renames the tmp to its
/// final path; any early return (ENOSPC mid-stream, client abort, error)
/// therefore reclaims the partial tmp instead of leaking it.
struct TmpCleanup {
    path: Option<std::path::PathBuf>,
}

impl TmpCleanup {
    fn new(path: std::path::PathBuf) -> Self {
        Self { path: Some(path) }
    }
    fn disarm(&mut self) {
        self.path = None;
    }
}

impl Drop for TmpCleanup {
    fn drop(&mut self) {
        if let Some(p) = self.path.take() {
            if std::fs::remove_file(&p).is_ok() {
                tracing::debug!(tmp = %p.display(), "Bug BU-6 reclaimed orphaned streaming tmp on error");
            }
        }
    }
}

/// Bug BU/BU-3 — map a put_chunk I/O error to a typed gRPC status. A
/// no-space error (ENOSPC, errno 28) becomes `RESOURCE_EXHAUSTED` so the
/// client can distinguish "disk full, reroute" from a real internal
/// fault; everything else stays `Internal`.
fn put_io_status(ctx: &str, e: std::io::Error) -> Status {
    if e.raw_os_error() == Some(28) {
        Status::resource_exhausted(format!("{ctx}: disk full (ENOSPC)"))
    } else {
        Status::internal(format!("{ctx}: {e}"))
    }
}

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
    /// Bug BC-8 — shared cluster scrub + integrity policy.
    /// `verify_on_read` here gates the hash-on-read code path in
    /// `get_chunk`. Hot-reloaded by the heartbeat handler in main.
    scrub_config: Arc<RwLock<ScrubConfig>>,
    /// Bug BF-2 — integrity-fault collector. Recorded into on every
    /// `verify_on_read` mismatch (BF-3); drained by the heartbeat
    /// loop (BF-5); also read at put_chunk-entry for self-reject
    /// when the node is locally Quarantined (BF-4).
    integrity_faults: Arc<crate::integrity::IntegrityFaultCollector>,
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
    // CF-6 — resolved [tls] context; when enabled the listener requires a
    // mutual-TLS handshake and outbound replication speaks mTLS too.
    tls: dtfs_common::tls::TlsContext,
    store: Arc<ChunkStore>,
    metrics: Arc<DataMetrics>,
    cache: Arc<ArcCache>,
    memory: Arc<MemoryBudget>,
    scrub_config: Arc<RwLock<ScrubConfig>>,
    integrity_faults: Arc<crate::integrity::IntegrityFaultCollector>,
    shutdown: impl std::future::Future<Output = ()>,
    ctx_tx: Option<tokio::sync::oneshot::Sender<ServerContext>>,
) -> anyhow::Result<()> {
    let addr = config.grpc.listen_addr.parse()?;
    let replication = Arc::new(ReplicationManager::new(
        store.clone(),
        config.clone(),
        tls.clone(),
    ));

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
        scrub_config,
        integrity_faults,
    };

    tracing::info!(
        "Data node gRPC server listening on {} ({})",
        addr,
        if tls.is_enabled() { "mTLS" } else { "plaintext" }
    );

    // CF-6 — when the [tls] context resolved certificates, every incoming
    // connection must complete a mutual-TLS handshake against the cluster CA.
    let mut builder = tonic::transport::Server::builder();
    if let Some(server_tls) = tls.server_tls() {
        builder = builder
            .tls_config(server_tls)
            .map_err(|e| anyhow::anyhow!("data gRPC TLS config: {e}"))?;
    }

    // Bug U — `serve_with_shutdown` lets tonic stop accepting new connections
    // when the signal fires, but in-flight requests keep running until they
    // complete (or until we abandon them in main).
    builder
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

/// LIS-19 / Sec R2 — fail-closed auth for mutating data-plane RPCs when
/// `cluster.shared_secret` is configured. Empty secret = legacy plaintext
/// cluster (same skip as join / ForwardClientWrite).
fn require_data_rpc_auth(
    secret: &str,
    method: &str,
    binding: &str,
    metadata: &tonic::metadata::MetadataMap,
) -> Result<(), Status> {
    if secret.is_empty() {
        return Ok(());
    }
    let ts = metadata
        .get(DATA_RPC_AUTH_TS)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Status::permission_denied("missing x-dtfs-rpc-ts"))?;
    let mac = metadata
        .get(DATA_RPC_AUTH_MAC)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Status::permission_denied("missing x-dtfs-rpc-mac"))?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if !verify_data_rpc_auth(secret, method, ts, mac, binding, now) {
        return Err(Status::permission_denied(
            "invalid data-plane RPC auth (LIS-19)",
        ));
    }
    Ok(())
}

/// Insert `x-dtfs-rpc-*` metadata onto a tonic request when a shared secret
/// is configured. Used by callers (tests, replication) that build Requests.
pub fn attach_data_rpc_auth<T>(
    request: &mut Request<T>,
    secret: &str,
    method: &str,
    binding: &str,
) {
    if let Some((ts, mac)) = stamp_data_rpc_auth(secret, method, binding) {
        if let (Ok(ts_v), Ok(mac_v)) = (ts.parse(), mac.parse()) {
            request.metadata_mut().insert(DATA_RPC_AUTH_TS, ts_v);
            request.metadata_mut().insert(DATA_RPC_AUTH_MAC, mac_v);
        }
    }
}

/// GetChunk streaming piece size. 8 MB gives tonic enough room to keep
/// the HTTP/2 send window full on a multi-gigabit LAN (the client side
/// bumps `max_decoding_message_size` to match — default is only 4 MB).
/// 2 MB pieces + a 32-slot channel capped reads at ~5 MB/s in the F-011
/// verification even with healthy disk + network underneath.
const GET_PIECE_SIZE: usize = 8 * 1024 * 1024;

/// CO-4-1 — depth of the GetChunk piece channel feeding the gRPC response
/// stream. A detached feeder spawn races ahead of a slow consumer, so the
/// channel is the buffering bound: at depth 64 a stalled reader could pin
/// ~64 * 8 MB = 512 MB in heap, violating the Bug-I streaming budget. Keep
/// it tiny (2 pieces in flight) so peak buffering stays ~16 MB.
const GET_CHANNEL_DEPTH: usize = 2;

/// U2 — BLAKE3-hash an on-disk chunk file incrementally, holding at most
/// one ~8 MB piece in RAM at a time (never the whole chunk). Used by the
/// verify_on_read cache-MISS path so a corrupt on-disk chunk is detected
/// before any bytes leave the process, with Bug-I memory parity.
async fn hash_file_incrementally(
    path: &std::path::Path,
    total: u64,
) -> std::io::Result<[u8; 32]> {
    use tokio::io::AsyncReadExt;
    let mut f = tokio::fs::File::open(path).await?;
    let mut hasher = dtfs_common::checksum::IncrementalHasher::new();
    let mut read: u64 = 0;
    let mut buf = vec![0u8; GET_PIECE_SIZE];
    while read < total {
        let want = (total - read).min(GET_PIECE_SIZE as u64) as usize;
        f.read_exact(&mut buf[..want]).await?;
        hasher.update(&buf[..want]);
        read += want as u64;
    }
    Ok(hasher.finalize())
}

/// U2 — stream an on-disk chunk file to the gRPC response channel in
/// `GET_PIECE_SIZE` pieces, reading one piece at a time via `read_exact`.
/// Mirrors `replication::replicate_to`'s feeder: only the channel depth
/// worth of pieces is ever in memory, never the whole chunk.
fn stream_file(
    dat_path: std::path::PathBuf,
    total_size: u64,
) -> ReceiverStream<Result<GetChunkResponse, Status>> {
    use tokio::io::AsyncReadExt;
    let (tx, rx) = mpsc::channel(GET_CHANNEL_DEPTH);
    tokio::spawn(async move {
        let mut f = match tokio::fs::File::open(&dat_path).await {
            Ok(f) => f,
            Err(e) => {
                let _ = tx
                    .send(Err(Status::internal(format!(
                        "open chunk {}: {e}",
                        dat_path.display()
                    ))))
                    .await;
                return;
            }
        };
        let mut offset: u64 = 0;
        let mut buf = vec![0u8; GET_PIECE_SIZE];
        while offset < total_size {
            let want = (total_size - offset).min(GET_PIECE_SIZE as u64) as usize;
            if let Err(e) = f.read_exact(&mut buf[..want]).await {
                let _ = tx
                    .send(Err(Status::internal(format!("read chunk: {e}"))))
                    .await;
                return;
            }
            let resp = GetChunkResponse {
                data: buf[..want].to_vec(),
                offset,
                total_size,
            };
            if tx.send(Ok(resp)).await.is_err() {
                return;
            }
            offset += want as u64;
        }
    });
    ReceiverStream::new(rx)
}

impl DataServiceImpl {
    /// U2 — stream an already-resident in-memory chunk (cache HIT) in
    /// `GET_PIECE_SIZE` pieces. The buffer already exists in heap, so
    /// slicing it costs nothing extra.
    fn stream_bytes(
        &self,
        data: bytes::Bytes,
    ) -> ReceiverStream<Result<GetChunkResponse, Status>> {
        let (tx, rx) = mpsc::channel(GET_CHANNEL_DEPTH);
        tokio::spawn(async move {
            let total = data.len();
            let mut offset = 0;
            while offset < total {
                let end = std::cmp::min(offset + GET_PIECE_SIZE, total);
                let resp = GetChunkResponse {
                    data: data.slice(offset..end).to_vec(),
                    offset: offset as u64,
                    total_size: total as u64,
                };
                if tx.send(Ok(resp)).await.is_err() {
                    break;
                }
                offset = end;
            }
        });
        ReceiverStream::new(rx)
    }

    /// Bug BC-8 — verify a cache-HIT chunk against its stored meta before
    /// the bytes leave the process. Records the integrity fault (BF-3) and
    /// returns DATA_LOSS on mismatch / missing meta so the FUSE client
    /// falls over to a healthy replica (P0-2).
    async fn verify_cached_or_fault(
        &self,
        chunk_id: &ChunkId,
        chunk_hex: &str,
        data: &[u8],
    ) -> Result<(), Status> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let expected = match self.store.get_chunk_meta(chunk_id).await {
            Ok(m) => m.checksum,
            Err(_) => {
                self.integrity_faults.record(crate::integrity::IntegrityFault {
                    chunk_id: chunk_hex.to_string(),
                    observed_at_ms: now_ms,
                    detector: "verify_on_read_meta_absent".into(),
                    expected_len: 0,
                    actual_len: data.len() as u64,
                    expected_hash: String::new(),
                    actual_hash: String::new(),
                });
                return Err(Status::data_loss(format!(
                    "verify_on_read: meta absent for chunk {chunk_hex}"
                )));
            }
        };
        if !checksum::verify(data, &expected) {
            let actual_hex = hex::encode(checksum::compute(data));
            tracing::error!(
                chunk_id = %chunk_hex,
                actual = %actual_hex,
                "Bug BC-8: read.integrity_fault — BLAKE3 mismatch \
                 against stored meta (cached). Refusing to serve."
            );
            self.integrity_faults.record(crate::integrity::IntegrityFault {
                chunk_id: chunk_hex.to_string(),
                observed_at_ms: now_ms,
                detector: "verify_on_read".into(),
                expected_len: 0,
                actual_len: data.len() as u64,
                expected_hash: hex::encode(expected),
                actual_hash: actual_hex,
            });
            // Note: ArcCache has no per-key invalidate. The cached entry
            // ages out via LRU; meanwhile every subsequent read hits this
            // same check and refuses, so a corrupt byte never escapes.
            return Err(Status::data_loss(format!(
                "verify_on_read mismatch on chunk {chunk_hex}"
            )));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl DataService for DataServiceImpl {
    type GetChunkStream = ReceiverStream<Result<GetChunkResponse, Status>>;

    async fn put_chunk(
        &self,
        request: Request<Streaming<PutChunkRequest>>,
    ) -> Result<Response<PutChunkResponse>, Status> {
        // LIS-19 — authenticate before allocating a write slot / tmp.
        require_data_rpc_auth(
            &self.config.cluster.shared_secret,
            "PutChunk",
            "",
            request.metadata(),
        )?;
        // Bug BF-4 — self-reject ALL writes while this node has at least
        // one un-acked integrity fault outstanding. The mgr's
        // auto-quarantine path (BF-6) clears the flag via ConfigUpdate
        // once it has issued QuarantineNode + the operator has
        // unquarantined. Refusing here is defence-in-depth: routing
        // already excludes Quarantined nodes (BC-11), but a stale
        // mgr-side cache could still send a put_chunk our way during
        // the propagation window.
        if self.integrity_faults.is_self_quarantined() {
            return Err(Status::failed_precondition(
                "Bug BF-4: data node self-quarantined: verify_on_read \
                 fault pending mgr ack. Operator must unquarantine via \
                 /ui/integrity before this node accepts writes.",
            ));
        }
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

        // Bug BU-6 — guarantee the streaming tmp is removed on ANY early
        // return below (write/flush ENOSPC, stream error). A failed put
        // that leaks its partial .tmp keeps the disk full, making the
        // ENOSPC condition self-sustaining (live: 84 leaked tmps = 18.5 GB
        // on a disk holding only 1.25 GB of committed chunks). Disarmed
        // after a successful commit (which renames the tmp away).
        let mut tmp_guard = TmpCleanup::new(tmp_path.clone());

        let mut hasher = dtfs_common::checksum::IncrementalHasher::new();
        let mut total_size: u64 = 0;
        {
            let mut f = tokio::fs::File::create(&tmp_path)
                .await
                .map_err(|e| put_io_status(&format!("create tmp {}", tmp_path.display()), e))?;
            // First piece (already consumed from the stream).
            if !first.data.is_empty() {
                hasher.update(&first.data);
                total_size += first.data.len() as u64;
                tokio::io::AsyncWriteExt::write_all(&mut f, &first.data)
                    .await
                    .map_err(|e| put_io_status("write tmp", e))?;
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
                    .map_err(|e| put_io_status("write tmp", e))?;
            }
            tokio::io::AsyncWriteExt::flush(&mut f)
                .await
                .map_err(|e| put_io_status("flush tmp", e))?;
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
            // CO-5-3 — a commit-time I/O failure (e.g. ENOSPC on the
            // final rename/fsync) must surface through put_io_status so
            // a disk-full error maps to RESOURCE_EXHAUSTED and the client
            // backs off, not a generic Internal. The tmp_guard Drop still
            // reclaims the tmp on this error path (disarm runs only on the
            // success branch below).
            .map_err(|e| match e {
                DtfsError::Io(io) => put_io_status("commit chunk", io),
                other => Status::internal(other.to_string()),
            })?;
        // Commit renamed the tmp to its final path; nothing to clean up.
        tmp_guard.disarm();

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

    /// Bug BC-6 — enumerate every chunk_id this data node holds.
    /// Walks each configured disk's `chunks/aa/bb/<id>.dat` tree,
    /// collects stems whose `.meta` companion also exists, applies
    /// the page_token cursor + limit, and returns the slice.
    /// Used by the manager's orphan_scrub tick (BC-7).
    async fn list_chunks(
        &self,
        request: Request<ListChunksRequest>,
    ) -> Result<Response<ListChunksResponse>, Status> {
        let req = request.into_inner();
        let limit = if req.limit == 0 { 1000 } else { req.limit as usize };

        // Cheap implementation: enumerate everything, sort, then slice.
        // A future revision can cache the listing between paged calls
        // (orphan_scrub typically iterates all pages back-to-back).
        // Bug CB — carry each chunk's `.dat` mtime alongside its id so the
        // manager's orphan reaper has a durable grace clock. Sort by id so
        // the opaque page_token cursor stays stable; the mtime rides along.
        let mut all: Vec<(String, u64)> = self.store.list_all_chunks_with_mtime().await
            .map_err(|e| Status::internal(format!("list chunks failed: {e}")))?;
        all.sort_by(|a, b| a.0.cmp(&b.0));

        let start_idx = if req.page_token.is_empty() {
            0
        } else {
            all.binary_search_by(|c| c.0.as_str().cmp(req.page_token.as_str()))
                .map(|i| i + 1)
                .unwrap_or_else(|i| i)
        };
        let end_idx = (start_idx + limit).min(all.len());
        let next = if end_idx < all.len() {
            all[end_idx - 1].0.clone()
        } else {
            String::new()
        };

        let page = &all[start_idx..end_idx];
        Ok(Response::new(ListChunksResponse {
            chunk_ids: page.iter().map(|c| c.0.clone()).collect(),
            chunk_mtimes_ms: page.iter().map(|c| c.1).collect(),
            next_page_token: next,
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

        let verify_on_read = self.scrub_config.read().await.verify_on_read;

        // Cache HIT — the chunk is already resident in the ARC cache, so
        // serving it from the in-memory `Bytes` costs no extra heap (the
        // buffer already exists). Verify against meta (BC-8) before any
        // bytes leave the process, then stream the in-memory buffer in
        // 8 MB pieces.
        if let Some(cached) = self.cache.get(&key) {
            self.metrics.record_cache_hit();

            if verify_on_read {
                self.verify_cached_or_fault(&chunk_id, &req.chunk_id, &cached)
                    .await?;
            }

            // Bug AL — count this as read traffic (cache hits are still
            // real reads to the requesting FUSE client).
            self.metrics.record_read(cached.len() as u64);
            return Ok(Response::new(self.stream_bytes(cached)));
        }

        // Cache MISS — U2 (Bug-I parity for the read side). Resolve the
        // on-disk `.dat` path + size WITHOUT reading the file into heap,
        // then stream it in pieces. We never hold the whole chunk in RAM
        // and (like put_chunk, Bug I) do NOT populate the ARC cache here:
        // caching every cold read would re-buffer the full chunk and
        // defeat the streaming-to-disk memory win. A hot set re-promoted
        // by tiering still lands in cache via the HIT path above on
        // subsequent reads.
        self.metrics.record_cache_miss();
        let (dat_path, total_size) = self
            .store
            .chunk_file_path(&chunk_id)
            .ok_or_else(|| Status::not_found(format!("chunk not found: {}", req.chunk_id)))?;

        // Bug BC-8 — verify_on_read. When the cluster integrity knob is
        // enabled (default true), BLAKE3-hash the on-disk chunk against
        // the stored ChunkMeta.checksum BEFORE any bytes leave this
        // process. We hash the file incrementally (one piece in RAM at a
        // time, never the whole chunk), and on a mismatch return DATA_LOSS
        // so the FUSE client retries the next replica (P0-2 fallback). On
        // a match we then stream the file a second pass from disk; reading
        // the file twice keeps memory bounded and is acceptable per U2.
        //
        // Disabling this flag is gated by an explicit-acknowledgement
        // boundary at the manager API (BC-8 ack guard). Once disabled,
        // this code path skips the hash and streams the raw bytes —
        // operator-acknowledged risk, AGENTS.md flags the false branch as
        // the only mode where reads can deliver corrupt bytes.
        if verify_on_read {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let expected = match self.store.get_chunk_meta(&chunk_id).await {
                Ok(m) => m.checksum,
                Err(_) => {
                    // No meta on this node — can't verify. Bug BF-3:
                    // record an integrity fault (detector =
                    // "verify_on_read_meta_absent") so the heartbeat
                    // surfaces this + the mgr quarantines us. Then
                    // return DATA_LOSS so the FUSE client falls over
                    // to a healthy replica.
                    self.integrity_faults.record(
                        crate::integrity::IntegrityFault {
                            chunk_id:      req.chunk_id.clone(),
                            observed_at_ms: now_ms,
                            detector:      "verify_on_read_meta_absent".into(),
                            expected_len:  0,
                            actual_len:    total_size,
                            expected_hash: String::new(),
                            actual_hash:   String::new(),
                        }
                    );
                    return Err(Status::data_loss(format!(
                        "verify_on_read: meta absent for chunk {}", req.chunk_id
                    )));
                }
            };
            let actual = hash_file_incrementally(&dat_path, total_size)
                .await
                .map_err(|e| Status::internal(format!("verify_on_read hash: {e}")))?;
            if actual != expected {
                let actual_hex = hex::encode(actual);
                tracing::error!(
                    chunk_id = %req.chunk_id,
                    actual = %actual_hex,
                    "Bug BC-8: read.integrity_fault — BLAKE3 mismatch \
                     against stored meta. Refusing to serve."
                );
                // Bug BF-3 — record the fault so heartbeat ships it +
                // mgr auto-quarantines this node (BF-6). The exact
                // expected + actual hashes go to the operator's
                // integrity feed (BC-12e).
                self.integrity_faults.record(
                    crate::integrity::IntegrityFault {
                        chunk_id:      req.chunk_id.clone(),
                        observed_at_ms: now_ms,
                        detector:      "verify_on_read".into(),
                        expected_len:  0,
                        actual_len:    total_size,
                        expected_hash: hex::encode(expected),
                        actual_hash:   actual_hex,
                    }
                );
                return Err(Status::data_loss(format!(
                    "verify_on_read mismatch on chunk {}", req.chunk_id
                )));
            }
        }

        // Bug AL — record the read for the manager-side I/O Throughput
        // rolling-rate.
        self.metrics.record_read(total_size);

        Ok(Response::new(stream_file(dat_path, total_size)))
    }

    async fn delete_chunk(
        &self,
        request: Request<DeleteChunkRequest>,
    ) -> Result<Response<DeleteChunkResponse>, Status> {
        // LIS-19 — bind MAC to chunk_id so a stolen stamp cannot delete
        // an arbitrary other chunk within the skew window.
        let md = request.metadata().clone();
        let req = request.into_inner();
        require_data_rpc_auth(
            &self.config.cluster.shared_secret,
            "DeleteChunk",
            &req.chunk_id,
            &md,
        )?;
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
        //
        // CO-1-1 — `disk_id` is `optional` on the wire: a present value
        // unlinks ONLY that disk's copy (protects a co-located anchor on
        // another disk); ABSENT (old manager, or a path with no disk
        // context) falls back to the legacy first-found-across-all-disks.
        let (was_present, freed_bytes) = self
            .store
            .delete_chunk_idempotent_on_disk(&chunk_id, req.disk_id)
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

        // CO-1-5 (§ A.6 #6) — VerifyChunk must BLAKE3 the on-disk bytes
        // against the stored ChunkMeta.checksum, not merely confirm the
        // .dat is readable. Migration marks a copy Done on this, so a
        // false `valid:true` lets bit-rot propagate and tier-demote drop
        // the only good source. Missing meta = unverifiable = invalid.
        let meta = match self.store.get_chunk_meta(&chunk_id).await {
            Ok(meta) => meta,
            Err(e) => {
                return Ok(Response::new(VerifyChunkResponse {
                    valid: false,
                    error: e.to_string(),
                }));
            }
        };
        let dat_path = match self.store.chunk_file_path(&chunk_id) {
            Some((path, _size)) => path,
            None => {
                return Ok(Response::new(VerifyChunkResponse {
                    valid: false,
                    error: format!("chunk {} .dat missing", req.chunk_id),
                }));
            }
        };
        // Stream-hash the on-disk file (one ~8 MB piece in RAM, never the
        // whole chunk — Bug-I parity).
        match hash_file_incrementally(&dat_path, meta.size).await {
            Ok(digest) if digest == meta.checksum => Ok(Response::new(VerifyChunkResponse {
                valid: true,
                error: String::new(),
            })),
            Ok(_) => Ok(Response::new(VerifyChunkResponse {
                valid: false,
                error: format!("checksum mismatch for chunk {}", req.chunk_id),
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
            .replicate_to(&chunk_id, &req.target_address, &req.target_tier)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::ArcCache;
    use crate::disk::DiskManager;
    use crate::integrity::IntegrityFaultCollector;
    use crate::metrics::DataMetrics;
    use crate::store::ChunkStore;
    use dtfs_common::config::ScrubConfig;
    use tokio_stream::StreamExt;

    // Minimal data-node config whose single hot disk is `disk_path`.
    fn test_config(disk_path: &std::path::Path) -> DataNodeConfig {
        let toml_str = format!(
            r#"
[node]
id = "auto"
hostname = "data-u2.example.com"
data_dir = "/var/lib/dtfs/data"

[location]
region = "eu"
datacenter = "frankfurt"
rack = "rack-a1"
host = "data-u2"

[cluster]
manager_addr = "mgr-01:9401"
shared_secret = "test-secret"

[grpc]
listen_addr = "0.0.0.0:9500"

[tls]
mode = "auto"

[[disks]]
path = "{}"
tier = "hot"

[cache]
max_memory_mb = 16
policy = "arc"

[scrub]
quick_interval_hours = 6
deep_interval_hours = 168
max_io_percent = 10
"#,
            disk_path.display().to_string().replace('\\', "/")
        );
        toml::from_str(&toml_str).unwrap()
    }

    fn scrub_cfg(verify_on_read: bool) -> ScrubConfig {
        ScrubConfig {
            quick_interval_hours: 6,
            deep_interval_hours: 168,
            quick_cron: None,
            deep_cron: None,
            max_io_percent: 10,
            verify_on_read,
            quarantine_on_fault: true,
        }
    }

    async fn make_service(
        disk_path: &std::path::Path,
        verify_on_read: bool,
    ) -> DataServiceImpl {
        std::fs::create_dir_all(disk_path.join(".dtfs")).unwrap();
        std::fs::create_dir_all(disk_path.join(".dtfs").join("wal")).unwrap();
        std::fs::create_dir_all(disk_path.join("chunks")).unwrap();
        std::fs::create_dir_all(disk_path.join("tmp")).unwrap();

        let disk_configs = vec![dtfs_common::config::DiskConfig {
            path: disk_path.to_path_buf(),
            tier: DiskTier::Hot,
        }];
        let dm = DiskManager::new(&disk_configs).unwrap();
        let store = Arc::new(ChunkStore::new(dm));
        let config = test_config(disk_path);
        let replication = Arc::new(ReplicationManager::new(
            store.clone(),
            config.clone(),
            dtfs_common::tls::TlsContext::plaintext(),
        ));
        DataServiceImpl {
            config,
            store,
            replication,
            metrics: Arc::new(DataMetrics::new()),
            cache: Arc::new(ArcCache::new(64)),
            write_slots: Arc::new(Semaphore::new(8)),
            memory: Arc::new(MemoryBudget::from_config(
                &dtfs_common::config::MemoryConfig::default(),
            )),
            scrub_config: Arc::new(RwLock::new(scrub_cfg(verify_on_read))),
            integrity_faults: Arc::new(IntegrityFaultCollector::new(64)),
        }
    }

    async fn collect_stream(
        svc: &DataServiceImpl,
        chunk_hex: &str,
    ) -> Result<Vec<u8>, Status> {
        let resp = svc
            .get_chunk(Request::new(GetChunkRequest {
                chunk_id: chunk_hex.to_string(),
                requester_dc: String::new(),
                requester_host: String::new(),
            }))
            .await?;
        let mut stream = resp.into_inner();
        let mut out: Vec<u8> = Vec::new();
        let mut expected_offset: u64 = 0;
        let mut total_seen: Option<u64> = None;
        while let Some(item) = stream.next().await {
            let piece = item?;
            assert_eq!(piece.offset, expected_offset, "pieces must be contiguous");
            if let Some(t) = total_seen {
                assert_eq!(piece.total_size, t, "total_size must be stable");
            } else {
                total_seen = Some(piece.total_size);
            }
            expected_offset += piece.data.len() as u64;
            out.extend_from_slice(&piece.data);
        }
        if let Some(t) = total_seen {
            assert_eq!(out.len() as u64, t, "delivered bytes match total_size");
        }
        Ok(out)
    }

    // U2 — a cache-MISS read of a chunk LARGER than one streaming piece
    // must round-trip byte-for-byte through the disk-streaming get path,
    // with correct offset/total_size on every piece.
    #[tokio::test]
    async fn getchunk_streams_multi_piece_chunk_from_disk() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path(), true).await;

        // > one GET_PIECE_SIZE so the stream spans >1 piece.
        let mut data = vec![0u8; GET_PIECE_SIZE + 1234];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        let hash = checksum::compute(&data);
        let chunk_id = ChunkId::from_bytes(hash);
        let chunk_hex = chunk_id.as_hex();
        svc.store
            .put_chunk(&chunk_id, &data, &VolumeId::new("default".to_string()), DiskTier::Hot)
            .await
            .unwrap();

        let got = collect_stream(&svc, &chunk_hex).await.unwrap();
        assert_eq!(got.len(), data.len());
        assert_eq!(got, data, "streamed bytes must match the stored chunk");
    }

    // Bug BC-8 / U2 — verify_on_read mismatch on the streaming MISS path
    // must still yield DATA_LOSS and deliver NO bytes as success.
    #[tokio::test]
    async fn getchunk_verify_on_read_mismatch_yields_data_loss() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path(), true).await;

        let mut data = vec![7u8; GET_PIECE_SIZE + 99];
        data[0] = 1;
        let hash = checksum::compute(&data);
        let chunk_id = ChunkId::from_bytes(hash);
        let chunk_hex = chunk_id.as_hex();
        svc.store
            .put_chunk(&chunk_id, &data, &VolumeId::new("default".to_string()), DiskTier::Hot)
            .await
            .unwrap();

        // Corrupt the on-disk .dat so its BLAKE3 no longer matches the
        // stored ChunkMeta.checksum.
        let (dat_path, _size) = svc.store.chunk_file_path(&chunk_id).unwrap();
        let mut corrupt = std::fs::read(&dat_path).unwrap();
        corrupt[0] ^= 0xFF;
        std::fs::write(&dat_path, &corrupt).unwrap();

        let err = collect_stream(&svc, &chunk_hex)
            .await
            .expect_err("corrupt chunk must not stream as success");
        assert_eq!(
            err.code(),
            tonic::Code::DataLoss,
            "verify_on_read mismatch must map to DATA_LOSS"
        );
    }

    // U2 — with verify_on_read disabled the streaming MISS path still
    // round-trips correct bytes (the only mode where it would serve raw
    // bytes; here the bytes are intact so the read succeeds).
    #[tokio::test]
    async fn getchunk_verify_off_streams_bytes() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path(), false).await;

        let data = vec![3u8; GET_PIECE_SIZE * 2 + 5];
        let hash = checksum::compute(&data);
        let chunk_id = ChunkId::from_bytes(hash);
        let chunk_hex = chunk_id.as_hex();
        svc.store
            .put_chunk(&chunk_id, &data, &VolumeId::new("default".to_string()), DiskTier::Hot)
            .await
            .unwrap();

        let got = collect_stream(&svc, &chunk_hex).await.unwrap();
        assert_eq!(got, data);
    }

    // LIS-19 — unauthenticated DeleteChunk must be rejected when a
    // shared_secret is configured (test_config uses "test-secret").
    #[tokio::test]
    async fn delete_chunk_rejects_missing_auth() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path(), true).await;
        let data = b"lis19-delete-auth";
        let hash = checksum::compute(data);
        let chunk_id = ChunkId::from_bytes(hash);
        let chunk_hex = chunk_id.as_hex();
        svc.store
            .put_chunk(
                &chunk_id,
                data,
                &VolumeId::new("default".to_string()),
                DiskTier::Hot,
            )
            .await
            .unwrap();

        let err = svc
            .delete_chunk(Request::new(DeleteChunkRequest {
                chunk_id: chunk_hex.clone(),
                disk_id: None,
                tier: String::new(),
            }))
            .await
            .expect_err("unauthenticated DeleteChunk must fail");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        // Bytes must still be on disk.
        assert!(svc.store.chunk_file_path(&chunk_id).is_some());
    }

    // LIS-19 — PutChunk uses the same require_data_rpc_auth gate before any
    // stream I/O. tonic::Streaming is not constructible without a server, so
    // this exercises the exact call put_chunk makes with empty metadata.
    #[tokio::test]
    async fn put_chunk_rejects_missing_auth() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path(), true).await;
        let md = tonic::metadata::MetadataMap::new();
        let err = require_data_rpc_auth(
            &svc.config.cluster.shared_secret,
            "PutChunk",
            "",
            &md,
        )
        .expect_err("unauthenticated PutChunk must fail");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(err.message().contains("missing x-dtfs-rpc"));
    }

    // LIS-19 — valid stamp allows DeleteChunk.
    #[tokio::test]
    async fn delete_chunk_accepts_valid_auth() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path(), true).await;
        let data = b"lis19-delete-ok";
        let hash = checksum::compute(data);
        let chunk_id = ChunkId::from_bytes(hash);
        let chunk_hex = chunk_id.as_hex();
        svc.store
            .put_chunk(
                &chunk_id,
                data,
                &VolumeId::new("default".to_string()),
                DiskTier::Hot,
            )
            .await
            .unwrap();

        let mut req = Request::new(DeleteChunkRequest {
            chunk_id: chunk_hex.clone(),
            disk_id: None,
            tier: String::new(),
        });
        attach_data_rpc_auth(
            &mut req,
            &svc.config.cluster.shared_secret,
            "DeleteChunk",
            &chunk_hex,
        );
        let resp = svc.delete_chunk(req).await.expect("authed delete");
        assert!(resp.into_inner().success);
        assert!(svc.store.chunk_file_path(&chunk_id).is_none());
    }
}

// ─────────────────────────────────────────────────────────────────────
// Audit CO findings — RED tests that encode CORRECT behaviour and so
// FAIL against the current (unfixed) code. Do not "fix" by relaxing the
// asserts; fix the production code.
// ─────────────────────────────────────────────────────────────────────
#[cfg(test)]
mod audit_co_tests {
    use super::*;
    use crate::cache::ArcCache;
    use crate::disk::DiskManager;
    use crate::integrity::IntegrityFaultCollector;
    use crate::metrics::DataMetrics;
    use crate::store::ChunkStore;
    use dtfs_common::config::ScrubConfig;

    // Local copy of the tests-module service builder (that one is private
    // to its sibling module). Single hot disk, verify_on_read on.
    async fn make_service(disk_path: &std::path::Path) -> DataServiceImpl {
        std::fs::create_dir_all(disk_path.join(".dtfs").join("wal")).unwrap();
        std::fs::create_dir_all(disk_path.join("chunks")).unwrap();
        std::fs::create_dir_all(disk_path.join("tmp")).unwrap();

        let toml_str = format!(
            r#"
[node]
id = "auto"
hostname = "data-co.example.com"
data_dir = "/var/lib/dtfs/data"

[location]
region = "eu"
datacenter = "frankfurt"
rack = "rack-a1"
host = "data-co"

[cluster]
manager_addr = "mgr-01:9401"
shared_secret = "test-secret"

[grpc]
listen_addr = "0.0.0.0:9500"

[tls]
mode = "auto"

[[disks]]
path = "{}"
tier = "hot"

[cache]
max_memory_mb = 16
policy = "arc"

[scrub]
quick_interval_hours = 6
deep_interval_hours = 168
max_io_percent = 10
"#,
            disk_path.display().to_string().replace('\\', "/")
        );
        let config: DataNodeConfig = toml::from_str(&toml_str).unwrap();

        let disk_configs = vec![dtfs_common::config::DiskConfig {
            path: disk_path.to_path_buf(),
            tier: DiskTier::Hot,
        }];
        let dm = DiskManager::new(&disk_configs).unwrap();
        let store = Arc::new(ChunkStore::new(dm));
        let replication = Arc::new(ReplicationManager::new(
            store.clone(),
            config.clone(),
            dtfs_common::tls::TlsContext::plaintext(),
        ));
        let scrub_config = ScrubConfig {
            quick_interval_hours: 6,
            deep_interval_hours: 168,
            quick_cron: None,
            deep_cron: None,
            max_io_percent: 10,
            verify_on_read: true,
            quarantine_on_fault: true,
        };
        DataServiceImpl {
            config,
            store,
            replication,
            metrics: Arc::new(DataMetrics::new()),
            cache: Arc::new(ArcCache::new(64)),
            write_slots: Arc::new(Semaphore::new(8)),
            memory: Arc::new(MemoryBudget::from_config(
                &dtfs_common::config::MemoryConfig::default(),
            )),
            scrub_config: Arc::new(RwLock::new(scrub_config)),
            integrity_faults: Arc::new(IntegrityFaultCollector::new(64)),
        }
    }

    // CO-1-5(a) — VerifyChunk must BLAKE3 the on-disk bytes against the
    // stored ChunkMeta.checksum. With one byte flipped in the .dat the
    // handler must report invalid; today it only checks get_chunk() == Ok
    // (no hashing) so it wrongly reports valid:true.
    #[tokio::test]
    async fn co_1_5a_verify_chunk_detects_flipped_byte() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path()).await;

        let data = vec![5u8; 4096];
        let chunk_id = ChunkId::from_bytes(checksum::compute(&data));
        let chunk_hex = chunk_id.as_hex();
        svc.store
            .put_chunk(&chunk_id, &data, &VolumeId::new("default".to_string()), DiskTier::Hot)
            .await
            .unwrap();

        // Flip one byte on disk so BLAKE3 no longer matches the meta.
        let (dat_path, _size) = svc.store.chunk_file_path(&chunk_id).unwrap();
        let mut corrupt = std::fs::read(&dat_path).unwrap();
        corrupt[0] ^= 0xFF;
        std::fs::write(&dat_path, &corrupt).unwrap();

        let resp = svc
            .verify_chunk(Request::new(VerifyChunkRequest {
                chunk_id: chunk_hex,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(
            !resp.valid,
            "VerifyChunk must report invalid when on-disk bytes don't match the stored checksum"
        );
    }

    // CO-1-5(b) — VerifyChunk must report invalid when the .meta is gone
    // (no checksum to verify against / chunk no longer trustworthy).
    // Today get_chunk() still returns the bytes (meta isn't consulted) so
    // the handler wrongly reports valid:true.
    #[tokio::test]
    async fn co_1_5b_verify_chunk_detects_missing_meta() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path()).await;

        let data = vec![9u8; 4096];
        let chunk_id = ChunkId::from_bytes(checksum::compute(&data));
        let chunk_hex = chunk_id.as_hex();
        svc.store
            .put_chunk(&chunk_id, &data, &VolumeId::new("default".to_string()), DiskTier::Hot)
            .await
            .unwrap();

        // Remove only the .meta; the .dat is intact.
        let (dat_path, _size) = svc.store.chunk_file_path(&chunk_id).unwrap();
        let meta_path = dat_path.with_extension("meta");
        std::fs::remove_file(&meta_path).unwrap();

        let resp = svc
            .verify_chunk(Request::new(VerifyChunkRequest {
                chunk_id: chunk_hex,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(
            !resp.valid,
            "VerifyChunk must report invalid when the chunk's .meta is missing"
        );
    }

    // CO-4-1 — the GetChunk feeder channel must not let a slow consumer
    // pin an unbounded pile of 8 MB pieces in heap. At the old depth of 64
    // that was 64 * 8 MB = 512 MB; cap the worst-case buffering well under
    // a sane ceiling.
    #[test]
    fn co_4_1_get_channel_buffering_is_bounded() {
        const CEILING: usize = 32 * 1024 * 1024;
        assert!(
            GET_CHANNEL_DEPTH * GET_PIECE_SIZE <= CEILING,
            "GetChunk channel can buffer {} bytes, over the {} ceiling",
            GET_CHANNEL_DEPTH * GET_PIECE_SIZE,
            CEILING
        );
    }

    // CO-5-3 — a commit-time ENOSPC (raw_os_error 28) must map to
    // RESOURCE_EXHAUSTED via put_io_status so the client backs off and
    // reroutes, not a generic Internal. The put path routes the
    // commit_chunk_from_tmp DtfsError::Io through this helper.
    #[test]
    fn co_5_3_commit_enospc_maps_to_resource_exhausted() {
        let enospc = std::io::Error::from_raw_os_error(28);
        let status = put_io_status("commit chunk", enospc);
        assert_eq!(
            status.code(),
            tonic::Code::ResourceExhausted,
            "ENOSPC on commit must surface as RESOURCE_EXHAUSTED"
        );

        let other = std::io::Error::from_raw_os_error(5); // EIO
        let status = put_io_status("commit chunk", other);
        assert_eq!(
            status.code(),
            tonic::Code::Internal,
            "non-ENOSPC commit I/O errors stay Internal"
        );
    }
}
