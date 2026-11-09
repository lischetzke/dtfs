use std::sync::Arc;

use dtfs_common::config::DataNodeConfig;
use dtfs_common::error::DtfsError;
use dtfs_common::types::{ChunkId, DiskStatus, DiskTier, VolumeId};
use dtfs_proto::dtfs::data::data_service_server::{DataService, DataServiceServer};
use dtfs_proto::dtfs::data::*;
use tokio::sync::{mpsc, Semaphore};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::memory::MemoryBudget;
use crate::metrics::DataMetrics;
use crate::replication::ReplicationManager;
use crate::store::ChunkStore;
// REV-4-5 — the last production user of this was the cache-HIT verify
// path, removed with the phantom ARC cache; the test modules still use it.
#[cfg(test)]
use dtfs_common::checksum;
use dtfs_common::config::ScrubConfig;
use dtfs_common::crypto::{
    data_rpc_delete_binding, stamp_data_rpc_auth, verify_data_rpc_auth, DATA_RPC_AUTH_MAC,
    DATA_RPC_AUTH_TS,
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

/// Bug D / Bug G — worst-case in-memory cost charged per in-flight
/// put_chunk. The data node has no per-volume chunk-size visibility here,
/// so this is a conservative 16 MB: 4x the workspace default chunk size
/// (4 MB) so larger volumes still get a sensible (smaller) slot count
/// rather than over-allocating slots that would breach the budget.
/// Streaming put_chunk (Bug I) means the real cost per slot is ~1 MB, so
/// the estimate is mostly defensive.
const PUT_CHUNK_SLOT_ESTIMATE_BYTES: u64 = 16 * 1024 * 1024;

/// REV-4-5 — non-put_chunk heap the node must leave headroom for: tonic,
/// raft and kernel pages. This deliberately does NOT include
/// `cache.max_memory_mb`: the data node has no server-side chunk cache
/// (see the module docs on `crate::cache`), so reserving its configured
/// budget was pure phantom accounting that strangled write concurrency —
/// a 768 MB cache line against a 1 GB cap left put_chunk with 1 slot.
const STATIC_OVERHEAD_BYTES: u64 = 256 * 1024 * 1024;

/// Bug D / Bug G — how many put_chunk streams may run concurrently. With
/// a hard memory cap set, size it from the cap; otherwise fall back to
/// `grpc.max_concurrent_chunk_writes` (default 8).
fn plan_write_slots(config: &DataNodeConfig, memory: &MemoryBudget) -> usize {
    memory.recommend_chunk_write_slots(
        PUT_CHUNK_SLOT_ESTIMATE_BYTES,
        STATIC_OVERHEAD_BYTES,
        config.grpc.max_concurrent_chunk_writes.max(1),
    )
}

pub async fn run_grpc_server(
    config: DataNodeConfig,
    // CF-6 — resolved [tls] context; when enabled the listener requires a
    // mutual-TLS handshake and outbound replication speaks mTLS too.
    tls: dtfs_common::tls::TlsContext,
    store: Arc<ChunkStore>,
    metrics: Arc<DataMetrics>,
    memory: Arc<MemoryBudget>,
    scrub_config: Arc<RwLock<ScrubConfig>>,
    integrity_faults: Arc<crate::integrity::IntegrityFaultCollector>,
    shutdown: impl std::future::Future<Output = ()>,
    ctx_tx: Option<tokio::sync::oneshot::Sender<ServerContext>>,
) -> anyhow::Result<()> {
    let addr = anyhow::Context::with_context(config.grpc.listen_addr.parse(), || {
        format!("grpc.listen_addr {:?} must be ip:port", config.grpc.listen_addr)
    })?;
    let replication = Arc::new(ReplicationManager::new(
        store.clone(),
        config.clone(),
        tls.clone(),
    ));

    // Bug D / Bug G — derive the put_chunk concurrency limit.
    let max_writes = plan_write_slots(&config, &memory);
    if memory.hard_cap_bytes() > 0 {
        tracing::info!(
            cap_mb = memory.hard_cap_bytes() / 1024 / 1024,
            chunk_mb = PUT_CHUNK_SLOT_ESTIMATE_BYTES / 1024 / 1024,
            overhead_mb = STATIC_OVERHEAD_BYTES / 1024 / 1024,
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
    // CMN-3 — the cap was previously a hardcoded 32 MB literal, making
    // `[grpc].max_message_size_mb` a documented no-op; now the configured
    // value (default 256 MB, docs/src/configuration/data-node.md) actually
    // governs the wire limit for the 8 MB GetChunk streaming pieces
    // (default tonic cap is 4 MB).
    let max_message_bytes = dtfs_common::config::max_message_bytes(config.grpc.max_message_size_mb);
    builder
        .add_service(
            DataServiceServer::new(service)
                .max_decoding_message_size(max_message_bytes)
                .max_encoding_message_size(max_message_bytes),
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

/// Warn about legacy chunk_id-only DeleteChunk stamps at most once per 60 s.
fn warn_legacy_delete_binding() {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    static LAST: AtomicU64 = AtomicU64::new(0);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let last = LAST.load(Relaxed);
    if now.saturating_sub(last) >= 60 && LAST.compare_exchange(last, now, Relaxed, Relaxed).is_ok() {
        tracing::warn!(
            "DeleteChunk: accepting LEGACY chunk_id-only stamp (cluster.require_delete_binding=false); \
             set it true once every manager signs the full binding"
        );
    }
}

/// LIS-19 / LIS-55 / LIS-83 — fail-closed auth for data-plane RPCs when
/// `cluster.shared_secret` is configured. Empty secret = legacy plaintext
/// cluster (same skip as join / ForwardClientWrite). Covers PutChunk,
/// DeleteChunk, ReplicateChunk, GetChunk, ListChunks, and VerifyChunk.
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

use crate::store::hash_chunk_file_incrementally as hash_file_incrementally;

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
    /// verify_on_read found no readable `.meta`. If the `.dat` is gone too
    /// a delete raced the read (delete removes `.dat` first): plain
    /// NOT_FOUND, no fault. Otherwise Bug BF-3: record an integrity fault
    /// and return DATA_LOSS so the client fails over.
    fn meta_absent_status(
        &self,
        chunk_id: &ChunkId,
        chunk_hex: &str,
        total_size: u64,
        now_ms: u64,
    ) -> Status {
        if self.store.chunk_file_path(chunk_id).is_none() {
            return Status::not_found(format!("chunk deleted during read: {chunk_hex}"));
        }
        self.integrity_faults.record(crate::integrity::IntegrityFault {
            chunk_id: chunk_hex.to_string(),
            observed_at_ms: now_ms,
            detector: "verify_on_read_meta_absent".into(),
            expected_len: 0,
            actual_len: total_size,
            expected_hash: String::new(),
            actual_hash: String::new(),
        });
        Status::data_loss(format!("verify_on_read: meta absent for chunk {chunk_hex}"))
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
        let computed_hex = chunk_id.as_hex();

        // CR-4-5 (DATA-5, server half) — the FUSE/client caller declares
        // the chunk_id it expects (BLAKE3 of the bytes it's about to
        // stream) in the FIRST piece; reject a mismatch between that
        // declaration and what we actually hashed off the wire BEFORE
        // committing anything. An empty declared id is the documented
        // compat case (older callers that don't declare one) and is
        // accepted unconditionally. `tmp_guard` (not yet disarmed) cleans
        // up the tmp file on this early return.
        if !first.chunk_id.is_empty() && first.chunk_id != computed_hex {
            return Err(Status::data_loss(
                "declared chunk_id does not match received bytes",
            ));
        }

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

        Ok(Response::new(PutChunkResponse {
            chunk_id: computed_hex,
            size: meta.size,
            checksum: hex::encode(&meta.checksum),
            success: true,
            disk_id,
        }))
    }

    /// Bug BC-6 — enumerate the chunk_ids this data node holds, one page
    /// at a time. Walks each configured disk's `chunks/aa/bb/<id>.dat`
    /// tree in id order starting from `page_token` (exclusive), collects
    /// stems whose `.meta` companion also exists, and stops at `limit`
    /// (REV-3-2: only the buckets the page touches are read).
    /// Used by the manager's orphan_scrub tick (BC-7).
    async fn list_chunks(
        &self,
        request: Request<ListChunksRequest>,
    ) -> Result<Response<ListChunksResponse>, Status> {
        // LIS-83 — inventory disclose; empty binding (no single chunk id).
        let md = request.metadata().clone();
        require_data_rpc_auth(
            &self.config.cluster.shared_secret,
            "ListChunks",
            "",
            &md,
        )?;
        let req = request.into_inner();
        let limit = if req.limit == 0 { 1000 } else { req.limit as usize };

        // REV-3-2 — incremental walk. The pre-fix implementation
        // enumerated + sorted the ENTIRE chunk tree on every page, so the
        // manager's orphan scrub (which pages through the whole node) cost
        // O(N^2) syscalls and spiked an N-sized Vec per page. The on-disk
        // layout `chunks/<aa>/<bb>/<id>.dat` is already id-sorted, so the
        // page token maps straight to a position in the walk and a page
        // reads only the buckets it actually needs.
        //
        // Bug CB — each chunk's `.dat` mtime rides along positionally with
        // its id so the manager's orphan reaper has a durable grace clock.
        //
        // Ask for one extra entry so we can tell "page is full and more
        // remain" (→ emit a token) from "page ends the listing" (→ empty
        // token), preserving the exact pre-fix page-token semantics.
        let mut page: Vec<(String, u64, u32)> = self
            .store
            .list_chunks_page(&req.page_token, limit.saturating_add(1))
            .await
            .map_err(|e| Status::internal(format!("list chunks failed: {e}")))?;
        let has_more = page.len() > limit;
        if has_more {
            page.truncate(limit);
        }
        let next = if has_more {
            page.last().map(|c| c.0.clone()).unwrap_or_default()
        } else {
            String::new()
        };

        Ok(Response::new(ListChunksResponse {
            chunk_ids: page.iter().map(|c| c.0.clone()).collect(),
            chunk_mtimes_ms: page.iter().map(|c| c.1).collect(),
            // CR2-7-2 / MGR-10 — index-aligned with chunk_ids, same
            // positional convention as chunk_mtimes_ms.
            chunk_disk_ids: page.iter().map(|c| c.2).collect(),
            next_page_token: next,
        }))
    }

    async fn get_chunk(
        &self,
        request: Request<GetChunkRequest>,
    ) -> Result<Response<Self::GetChunkStream>, Status> {
        // LIS-83 — bind MAC to chunk_id so a stolen stamp cannot exfil
        // an arbitrary other chunk within the skew window.
        let md = request.metadata().clone();
        let req = request.into_inner();
        require_data_rpc_auth(
            &self.config.cluster.shared_secret,
            "GetChunk",
            &req.chunk_id,
            &md,
        )?;

        // CR-4-7 (DATA-10) — ChunkId::from_hex byte-slices by hex-digit
        // PAIR, not by raw string index, so a non-ASCII chunk_id (e.g. one
        // containing a multi-byte UTF-8 char) is rejected as invalid_argument
        // instead of panicking on a str-slice-not-a-char-boundary.
        let chunk_id = ChunkId::from_hex(&req.chunk_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid chunk ID: {}", e)))?;

        // P0-9: record this read against the requester's DC for tiering's
        // auto-move-replica-toward-active-DC decisions.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.metrics
            .record_chunk_read(&req.chunk_id, &req.requester_dc, now_ms);

        let verify_on_read = self.scrub_config.read().await.verify_on_read;

        // REV-4-5 — there is no server-side chunk cache on the data node;
        // every read streams from disk. U2 (Bug-I parity for the read
        // side): resolve the on-disk `.dat` path + size WITHOUT reading
        // the file into heap, then stream it in pieces, so we never hold
        // a whole chunk in RAM. The client-side caches (FUSE readahead +
        // local disk cache) are what absorb repeat reads; they report
        // back via the RecordCacheHit RPC.
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
        // operator-acknowledged risk, CLAUDE.md flags the false branch as
        // the only mode where reads can deliver corrupt bytes.
        if verify_on_read {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let expected = match self.store.get_chunk_meta(&chunk_id).await {
                // Compare against the content address (chunk id), not the
                // sidecar: a damaged-but-parseable .meta must not make good
                // bytes look corrupt and self-quarantine the node.
                Ok(_) => chunk_id.0,
                Err(_) => {
                    return Err(self.meta_absent_status(&chunk_id, &req.chunk_id, total_size, now_ms));
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
        // LIS-19 — bind MAC to chunk_id|disk_id|if_mtime_le_ms so a stolen
        // stamp cannot delete another chunk, or the same chunk on another
        // disk / without the mtime guard, within the skew window.
        let md = request.metadata().clone();
        let req = request.into_inner();
        let secret = &self.config.cluster.shared_secret;
        let binding = data_rpc_delete_binding(&req.chunk_id, req.disk_id, req.if_mtime_le_ms);
        if let Err(e) = require_data_rpc_auth(secret, "DeleteChunk", &binding, &md) {
            // Rolling update: an older manager signs chunk_id only.
            let legacy_ok = !self.config.cluster.require_delete_binding
                && require_data_rpc_auth(secret, "DeleteChunk", &req.chunk_id, &md).is_ok();
            if !legacy_ok {
                tracing::warn!(chunk_id = %req.chunk_id, disk_id = ?req.disk_id, "DeleteChunk rejected: invalid auth stamp");
                return Err(e);
            }
            warn_legacy_delete_binding();
        }
        // CR-4-7 (DATA-10) — ChunkId::from_hex byte-slices by hex-digit
        // PAIR, not by raw string index, so a non-ASCII chunk_id (e.g. one
        // containing a multi-byte UTF-8 char) is rejected as invalid_argument
        // instead of panicking on a str-slice-not-a-char-boundary.
        let chunk_id = ChunkId::from_hex(&req.chunk_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid chunk ID: {}", e)))?;

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
            .delete_chunk_idempotent_guarded(&chunk_id, req.disk_id, req.if_mtime_le_ms)
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
        // LIS-83 — bind MAC to chunk_id (presence / integrity oracle).
        let md = request.metadata().clone();
        let req = request.into_inner();
        require_data_rpc_auth(
            &self.config.cluster.shared_secret,
            "VerifyChunk",
            &req.chunk_id,
            &md,
        )?;
        // CR-4-7 (DATA-10) — ChunkId::from_hex byte-slices by hex-digit
        // PAIR, not by raw string index, so a non-ASCII chunk_id (e.g. one
        // containing a multi-byte UTF-8 char) is rejected as invalid_argument
        // instead of panicking on a str-slice-not-a-char-boundary.
        let chunk_id = ChunkId::from_hex(&req.chunk_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid chunk ID: {}", e)))?;

        // CO-1-5 (§ A.6 #6) — VerifyChunk must BLAKE3 the on-disk bytes
        // against the chunk id, not merely confirm the
        // .dat is readable. Migration marks a copy Done on this, so a
        // false `valid:true` lets bit-rot propagate and tier-demote drop
        // the only good source. Missing meta = unverifiable = invalid.
        let meta = match self.store.get_chunk_meta_on_disk(&chunk_id, req.disk_id).await {
            Ok(meta) => meta,
            Err(e) => {
                return Ok(Response::new(VerifyChunkResponse {
                    valid: false,
                    error: e.to_string(),
                }));
            }
        };
        let dat_path = match self.store.chunk_file_path_on_disk(&chunk_id, req.disk_id) {
            Some((path, stat_size)) => {
                // CR-4-6 (DATA-9) — a size mismatch is corruption/truncation
                // that a checksum over the WRONG number of bytes can't
                // reliably catch (e.g. a truncated file can still hash to
                // a codepath that reads short and "succeeds"). Catch it
                // before spending an I/O pass hashing the file.
                if stat_size != meta.size {
                    return Ok(Response::new(VerifyChunkResponse {
                        valid: false,
                        error: "size mismatch".to_string(),
                    }));
                }
                path
            }
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
            // Only an id (content address) mismatch is corruption; a stale
            // `.meta` checksum is scrub's to repair (§ A.3.3 (6)).
            Ok(digest) if digest == chunk_id.0 => Ok(Response::new(VerifyChunkResponse {
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
        request: Request<DiskStatusRequest>,
    ) -> Result<Response<DiskStatusResponse>, Status> {
        require_data_rpc_auth(&self.config.cluster.shared_secret, "GetDiskStatus", "", request.metadata())?;
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
        // LIS-55 — unauthenticated ReplicateChunk is exfil/SSRF: the
        // handler streams chunk bytes via PutChunk to a caller-chosen
        // target_address. Bind MAC to chunk_id|target_address.
        let md = request.metadata().clone();
        let req = request.into_inner();
        let binding = dtfs_common::crypto::data_rpc_replicate_binding(
            &req.chunk_id,
            &req.target_address,
        );
        require_data_rpc_auth(
            &self.config.cluster.shared_secret,
            "ReplicateChunk",
            &binding,
            &md,
        )?;

        // CR-4-7 (DATA-10) — ChunkId::from_hex byte-slices by hex-digit
        // PAIR, not by raw string index, so a non-ASCII chunk_id (e.g. one
        // containing a multi-byte UTF-8 char) is rejected as invalid_argument
        // instead of panicking on a str-slice-not-a-char-boundary.
        let chunk_id = ChunkId::from_hex(&req.chunk_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid chunk ID: {}", e)))?;

        match self
            .replication
            .replicate_to(&chunk_id, &req.target_address, &req.target_tier)
            .await
        {
            Ok(target_disk_id) => Ok(Response::new(ReplicateChunkResponse {
                success: true,
                error: String::new(),
                disk_id: Some(target_disk_id),
            })),
            Err(e) => Ok(Response::new(ReplicateChunkResponse {
                success: false,
                error: e.to_string(),
                disk_id: None,
            })),
        }
    }

    async fn report_stats(
        &self,
        request: Request<StatsRequest>,
    ) -> Result<Response<StatsResponse>, Status> {
        require_data_rpc_auth(&self.config.cluster.shared_secret, "ReportStats", "", request.metadata())?;
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
        // DATA-11 — this was the last mutating data-plane RPC not gated
        // by LIS-19/55/83: any caller could inflate a chunk's read
        // counter (and so its tiering promotion score) without the
        // cluster secret. Bind MAC to chunk_id, same as GetChunk.
        let md = request.metadata().clone();
        let req = request.into_inner();
        require_data_rpc_auth(
            &self.config.cluster.shared_secret,
            "RecordCacheHit",
            &req.chunk_id,
            &md,
        )?;
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

#[cfg(test)]
mod tests {
    use super::*;
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
            quick_interval_secs: 0,
            deep_interval_secs: 0,
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
            write_slots: Arc::new(Semaphore::new(8)),
            memory: Arc::new(MemoryBudget::from_config(
                &dtfs_common::config::MemoryConfig::default(),
            )),
            scrub_config: Arc::new(RwLock::new(scrub_cfg(verify_on_read))),
            integrity_faults: Arc::new(IntegrityFaultCollector::new(64)),
        }
    }

    // ── REV-4-5 — no phantom ARC-cache budget ──────────────────────

    /// REV-4-5 regression.
    ///
    /// Nothing in the workspace ever called `ArcCache::put`, so the
    /// data-node ARC cache was permanently empty: the CF-5 ladder's
    /// "shrink ARC cache" rung was a no-op AND `cache.max_memory_mb`
    /// (512 MB in the shipped examples) was still reserved out of
    /// `memory.hard_cap_mb` when sizing put_chunk concurrency. On a
    /// 1 GB-capped node that phantom reservation collapsed the write
    /// path to a single in-flight chunk.
    ///
    /// Invariant: **put_chunk slot sizing has no cache reservation**
    /// (`[cache]` is no longer parsed at all).
    #[test]
    fn rev_4_5_write_slots_ignore_phantom_arc_cache_budget() {
        let mut small = test_config(std::path::Path::new("/tmp/rev-4-5"));
        small.memory.hard_cap_mb = 1024;
        let budget = MemoryBudget::from_config(&small.memory);
        let with_large = plan_write_slots(&small, &budget);
        assert!(
            with_large > 1,
            "a 1 GB cap must still allow parallel streaming writes; got \
             {with_large} slot(s) — the phantom cache reservation is back"
        );
    }

    /// REV-4-5 — the data node no longer carries a server-side chunk
    /// cache, so `get_chunk` always streams from disk (Bug I / U2 memory
    /// parity) and repeat reads of the same chunk stay byte-identical.
    #[tokio::test]
    async fn rev_4_5_repeat_reads_stream_from_disk() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path(), true).await;

        let data = vec![42u8; 4096];
        let chunk_id = ChunkId::from_bytes(dtfs_common::checksum::compute(&data));
        let chunk_hex = chunk_id.as_hex();
        svc.store
            .put_chunk(
                &chunk_id,
                &data,
                &VolumeId::new("default".to_string()),
                DiskTier::Hot,
            )
            .await
            .unwrap();

        for _ in 0..3 {
            assert_eq!(collect_stream(&svc, &chunk_hex).await.unwrap(), data);
        }
        let snap = svc.metrics.snapshot();
        assert_eq!(
            snap.cache_hits, 0,
            "the data node has no server-side chunk cache; hits must stay 0"
        );
        assert_eq!(snap.cache_misses, 3, "every read is served from disk");
    }

    async fn collect_stream(
        svc: &DataServiceImpl,
        chunk_hex: &str,
    ) -> Result<Vec<u8>, Status> {
        let mut req = Request::new(GetChunkRequest {
            chunk_id: chunk_hex.to_string(),
            requester_dc: String::new(),
            requester_host: String::new(),
        });
        attach_data_rpc_auth(
            &mut req,
            &svc.config.cluster.shared_secret,
            "GetChunk",
            chunk_hex,
        );
        let resp = svc.get_chunk(req).await?;
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

    // A damaged-but-parseable .meta (wrong checksum) must not make good
    // bytes look corrupt: verify_on_read compares against the chunk id.
    #[tokio::test]
    async fn getchunk_verify_on_read_ignores_damaged_meta_checksum() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path(), true).await;
        let data = vec![9u8; 4096];
        let chunk_id = ChunkId::from_bytes(checksum::compute(&data));
        svc.store
            .put_chunk(&chunk_id, &data, &VolumeId::new("default".to_string()), DiskTier::Hot)
            .await
            .unwrap();
        let (dat_path, _) = svc.store.chunk_file_path(&chunk_id).unwrap();
        let meta_path = dat_path.with_extension("meta");
        let mut m: dtfs_common::types::ChunkMeta =
            serde_json::from_str(&std::fs::read_to_string(&meta_path).unwrap()).unwrap();
        m.checksum = [0xCD; 32];
        std::fs::write(&meta_path, serde_json::to_string(&m).unwrap()).unwrap();
        let got = collect_stream(&svc, &chunk_id.as_hex()).await.expect("good bytes must stream");
        assert_eq!(got, data);
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
                if_mtime_le_ms: None,
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
            if_mtime_le_ms: None,
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

    async fn put_test_chunk(svc: &DataServiceImpl, data: &[u8]) -> (ChunkId, String) {
        let chunk_id = ChunkId::from_bytes(checksum::compute(data));
        svc.store
            .put_chunk(&chunk_id, data, &VolumeId::new("default".to_string()), DiskTier::Hot)
            .await
            .unwrap();
        let hex = chunk_id.as_hex();
        (chunk_id, hex)
    }

    fn delete_req(
        svc: &DataServiceImpl,
        chunk_hex: &str,
        disk_id: Option<u32>,
        if_mtime_le_ms: Option<u64>,
        binding: &str,
    ) -> Request<DeleteChunkRequest> {
        let mut req = Request::new(DeleteChunkRequest {
            chunk_id: chunk_hex.to_string(),
            disk_id,
            tier: String::new(),
            if_mtime_le_ms,
        });
        attach_data_rpc_auth(&mut req, &svc.config.cluster.shared_secret, "DeleteChunk", binding);
        req
    }

    // Audit #3 — a stamp from a new manager covers disk_id and
    // if_mtime_le_ms: stripping the disk id or loosening the mtime guard
    // is PermissionDenied even in the rollout window (flag off).
    #[tokio::test]
    async fn delete_chunk_rejects_tampered_disk_or_mtime() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path(), true).await;
        let (chunk_id, hex) = put_test_chunk(&svc, b"audit3-tamper").await;
        let signed = data_rpc_delete_binding(&hex, Some(1), Some(1_000));

        for (disk, mtime) in [(None, Some(1_000)), (Some(1), Some(u64::MAX)), (Some(1), None)] {
            let err = svc
                .delete_chunk(delete_req(&svc, &hex, disk, mtime, &signed))
                .await
                .expect_err("tampered DeleteChunk must fail");
            assert_eq!(err.code(), tonic::Code::PermissionDenied, "disk={disk:?} mtime={mtime:?}");
        }
        assert!(svc.store.chunk_file_path(&chunk_id).is_some(), "bytes must survive");
    }

    // Audit #3 — with require_delete_binding a legacy chunk_id-only stamp
    // (replayable with disk_id stripped) is PermissionDenied; the full
    // binding is accepted.
    #[tokio::test]
    async fn delete_chunk_legacy_stamp_rejected_when_binding_required() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut svc = make_service(tmp.path(), true).await;
        svc.config.cluster.require_delete_binding = true;
        let (chunk_id, hex) = put_test_chunk(&svc, b"audit3-legacy").await;

        let err = svc
            .delete_chunk(delete_req(&svc, &hex, None, None, &hex))
            .await
            .expect_err("legacy stamp must fail when the binding is required");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(svc.store.chunk_file_path(&chunk_id).is_some(), "bytes must survive");

        let binding = data_rpc_delete_binding(&hex, None, None);
        let resp = svc
            .delete_chunk(delete_req(&svc, &hex, None, None, &binding))
            .await
            .expect("full binding accepted");
        assert!(resp.into_inner().was_present);
        assert!(svc.store.chunk_file_path(&chunk_id).is_none());
    }

    // LIS-55 — unauthenticated ReplicateChunk must be PermissionDenied
    // before any outbound stream (exfil/SSRF).
    #[tokio::test]
    async fn replicate_chunk_rejects_missing_auth() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path(), true).await;
        let data = b"lis55-replicate-auth";
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
            .replicate_chunk(Request::new(ReplicateChunkRequest {
                chunk_id: chunk_hex.clone(),
                target_address: "http://127.0.0.1:19999".into(),
                target_tier: "warm".into(),
            }))
            .await
            .expect_err("unauthenticated ReplicateChunk must fail");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(svc.store.chunk_file_path(&chunk_id).is_some());
    }

    // LIS-55 — stamp for a different target_address must not authorize.
    #[tokio::test]
    async fn replicate_chunk_rejects_wrong_target_binding() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path(), true).await;
        let data = b"lis55-replicate-bind";
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

        let stamped_target = "http://peer-a:9500";
        let actual_target = "http://evil:9500";
        let binding =
            dtfs_common::crypto::data_rpc_replicate_binding(&chunk_hex, stamped_target);
        let mut req = Request::new(ReplicateChunkRequest {
            chunk_id: chunk_hex.clone(),
            target_address: actual_target.into(),
            target_tier: "warm".into(),
        });
        attach_data_rpc_auth(
            &mut req,
            &svc.config.cluster.shared_secret,
            "ReplicateChunk",
            &binding,
        );
        let err = svc
            .replicate_chunk(req)
            .await
            .expect_err("wrong target binding must fail");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(svc.store.chunk_file_path(&chunk_id).is_some());
    }

    // LIS-83 — unauthenticated GetChunk must be PermissionDenied (exfil).
    #[tokio::test]
    async fn get_chunk_rejects_missing_auth() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path(), true).await;
        let data = b"lis83-get-auth";
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
            .get_chunk(Request::new(GetChunkRequest {
                chunk_id: chunk_hex.clone(),
                requester_dc: String::new(),
                requester_host: String::new(),
            }))
            .await
            .expect_err("unauthenticated GetChunk must fail");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    // LIS-83 — unauthenticated ListChunks must be PermissionDenied (inventory).
    #[tokio::test]
    async fn list_chunks_rejects_missing_auth() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path(), true).await;
        let err = svc
            .list_chunks(Request::new(ListChunksRequest {
                page_token: String::new(),
                limit: 10,
            }))
            .await
            .expect_err("unauthenticated ListChunks must fail");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    // LIS-83 — unauthenticated VerifyChunk must be PermissionDenied (oracle).
    #[tokio::test]
    async fn verify_chunk_rejects_missing_auth() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path(), true).await;
        let err = svc
            .verify_chunk(Request::new(VerifyChunkRequest {
                chunk_id: "00".repeat(32),
                disk_id: None,
            }))
            .await
            .expect_err("unauthenticated VerifyChunk must fail");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    // Audit #13 — GetDiskStatus / ReportStats leak mount paths, hostname
    // and counts: unauthenticated calls are PermissionDenied, stamped pass.
    #[tokio::test]
    async fn status_rpcs_require_auth() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path(), true).await;
        let secret = svc.config.cluster.shared_secret.clone();

        let err = svc.get_disk_status(Request::new(DiskStatusRequest {})).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        let err = svc.report_stats(Request::new(StatsRequest {})).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);

        let mut req = Request::new(DiskStatusRequest {});
        attach_data_rpc_auth(&mut req, &secret, "GetDiskStatus", "");
        assert_eq!(svc.get_disk_status(req).await.unwrap().into_inner().disks.len(), 1);
        let mut req = Request::new(StatsRequest {});
        attach_data_rpc_auth(&mut req, &secret, "ReportStats", "");
        svc.report_stats(req).await.expect("stamped ReportStats");
        // A stamp for another method does not open the gate.
        let mut req = Request::new(StatsRequest {});
        attach_data_rpc_auth(&mut req, &secret, "GetDiskStatus", "");
        let err = svc.report_stats(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    // DATA-11 — unauthenticated RecordCacheHit must be PermissionDenied
    // (unauthenticated callers must not be able to inflate a chunk's
    // read counter / tiering promotion score).
    #[tokio::test]
    async fn record_cache_hit_rejects_missing_auth() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path(), true).await;
        let err = svc
            .record_cache_hit(Request::new(RecordCacheHitRequest {
                chunk_id: "00".repeat(32),
                requester_dc: String::new(),
                requester_host: String::new(),
            }))
            .await
            .expect_err("unauthenticated RecordCacheHit must fail");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    // LIS-83 — valid stamp allows GetChunk past the auth gate.
    #[tokio::test]
    async fn get_chunk_accepts_valid_auth() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path(), true).await;
        let data = b"lis83-get-ok";
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

        let got = collect_stream(&svc, &chunk_hex).await.expect("authed get");
        assert_eq!(got, data);
    }

    /// replicate_to must report the disk the TARGET stored the copy on, not
    /// a guess: the target has hot disk 0 and warm disk 1, tier "warm" lands on 1.
    #[tokio::test]
    async fn replicate_to_returns_target_disk_id() {
        use tokio_stream::wrappers::TcpListenerStream;

        let src_dir = tempfile::TempDir::new().unwrap();
        let src = make_service(src_dir.path(), true).await;
        let data = b"replicate-reports-target-disk";
        let chunk_id = ChunkId::from_bytes(checksum::compute(data));
        src.store
            .put_chunk(&chunk_id, data, &VolumeId::new("default".to_string()), DiskTier::Hot)
            .await
            .unwrap();

        let d0 = tempfile::TempDir::new().unwrap();
        let d1 = tempfile::TempDir::new().unwrap();
        let dm = DiskManager::new(&[
            dtfs_common::config::DiskConfig { path: d0.path().to_path_buf(), tier: DiskTier::Hot },
            dtfs_common::config::DiskConfig { path: d1.path().to_path_buf(), tier: DiskTier::Warm },
        ])
        .unwrap();
        let store = Arc::new(ChunkStore::new(dm));
        let config = test_config(d0.path());
        let target = DataServiceImpl {
            config: config.clone(),
            store: store.clone(),
            replication: Arc::new(ReplicationManager::new(
                store.clone(),
                config,
                dtfs_common::tls::TlsContext::plaintext(),
            )),
            metrics: Arc::new(DataMetrics::new()),
            write_slots: Arc::new(Semaphore::new(8)),
            memory: Arc::new(MemoryBudget::from_config(
                &dtfs_common::config::MemoryConfig::default(),
            )),
            scrub_config: Arc::new(RwLock::new(scrub_cfg(true))),
            integrity_faults: Arc::new(IntegrityFaultCollector::new(64)),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(DataServiceServer::new(target))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );

        let disk_id = src
            .replication
            .replicate_to(&chunk_id, &format!("http://{addr}"), "warm")
            .await
            .expect("replicate");
        assert_eq!(disk_id, 1, "copy landed on the warm disk");
        let (p, _) = store.chunk_file_path(&chunk_id).expect("stored on target");
        assert!(p.starts_with(d1.path()));
    }

    #[tokio::test]
    async fn meta_absent_with_dat_present_is_data_loss_and_fault() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path(), true).await;
        let data = b"meta-absent-dat-present";
        let chunk_id = ChunkId::from_bytes(checksum::compute(data));
        svc.store
            .put_chunk(&chunk_id, data, &VolumeId::new("default".to_string()), DiskTier::Hot)
            .await
            .unwrap();
        let st = svc.meta_absent_status(&chunk_id, &chunk_id.as_hex(), data.len() as u64, 1);
        assert_eq!(st.code(), tonic::Code::DataLoss);
        assert_eq!(svc.integrity_faults.pending_len(), 1);
    }

    #[tokio::test]
    async fn meta_absent_after_delete_race_is_not_found_without_fault() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path(), true).await;
        let chunk_id = ChunkId::from_bytes(checksum::compute(b"gone"));
        let st = svc.meta_absent_status(&chunk_id, &chunk_id.as_hex(), 4, 1);
        assert_eq!(st.code(), tonic::Code::NotFound);
        assert_eq!(svc.integrity_faults.pending_len(), 0);
        assert!(!svc.integrity_faults.is_self_quarantined());
    }

    // ── CR-4-5 (DATA-5, server half) — declared chunk_id vs bytes ───

    /// Start the real `DataServiceImpl` behind a plaintext in-process gRPC
    /// server and drive a real streaming `PutChunk` RPC (a `Streaming<T>`
    /// can't be constructed by hand outside a real transport). Returns
    /// the RPC result and the store used, so callers can assert on-disk
    /// state afterwards.
    async fn run_put_chunk(
        disk_path: &std::path::Path,
        declared_chunk_id: &str,
        data: &[u8],
    ) -> (Result<PutChunkResponse, Status>, Arc<ChunkStore>) {
        use dtfs_proto::dtfs::data::data_service_client::DataServiceClient;
        use tokio_stream::wrappers::TcpListenerStream;

        let svc = make_service(disk_path, true).await;
        let store = svc.store.clone();
        let secret = svc.config.cluster.shared_secret.clone();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(DataServiceServer::new(svc))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );

        let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = DataServiceClient::new(channel);

        let req = PutChunkRequest {
            chunk_id: declared_chunk_id.to_string(),
            volume_id: String::new(),
            tier: "hot".to_string(),
            data: data.to_vec(),
        };
        let mut request = Request::new(tokio_stream::iter(vec![req]));
        if let Some((ts, mac)) = stamp_data_rpc_auth(&secret, "PutChunk", "") {
            request
                .metadata_mut()
                .insert(DATA_RPC_AUTH_TS, ts.parse().unwrap());
            request
                .metadata_mut()
                .insert(DATA_RPC_AUTH_MAC, mac.parse().unwrap());
        }

        let result = client
            .put_chunk(request)
            .await
            .map(|r| r.into_inner())
            .map_err(|e| e);
        (result, store)
    }

    /// A declared `chunk_id` that doesn't match the BLAKE3 of the bytes
    /// actually streamed must be rejected with `data_loss` BEFORE
    /// anything is committed — no `.dat` for either id ends up on disk.
    #[tokio::test]
    async fn cr_4_5_put_chunk_declared_id_mismatch_rejected() {
        let tmp = tempfile::TempDir::new().unwrap();
        let data = b"cr-4-5-real-bytes";
        let wrong_id = "ab".repeat(32);

        let (result, store) = run_put_chunk(tmp.path(), &wrong_id, data).await;
        let err = result.expect_err("declared id mismatch must fail");
        assert_eq!(err.code(), tonic::Code::DataLoss);

        let real_id = ChunkId::from_bytes(checksum::compute(data));
        assert!(!store.has_chunk(&real_id).await, "no chunk committed under the real hash");
        assert!(
            store.get_chunk_meta(&ChunkId::from_hex(&wrong_id).unwrap()).await.is_err(),
            "no chunk committed under the (wrong) declared id either"
        );
    }

    /// An empty declared `chunk_id` is the documented back-compat case
    /// (older callers that never declared one) and must still be
    /// accepted, with the response carrying the server-computed id.
    #[tokio::test]
    async fn cr_4_5_put_chunk_empty_declared_id_accepted() {
        let tmp = tempfile::TempDir::new().unwrap();
        let data = b"cr-4-5-empty-declared-id";

        let (result, store) = run_put_chunk(tmp.path(), "", data).await;
        let resp = result.expect("empty declared id must be accepted (compat)");
        let real_id = ChunkId::from_bytes(checksum::compute(data));
        assert_eq!(resp.chunk_id, real_id.as_hex());
        assert!(store.has_chunk(&real_id).await);
    }

    /// A declared `chunk_id` that DOES match the streamed bytes is
    /// accepted, same as before this check existed.
    #[tokio::test]
    async fn cr_4_5_put_chunk_matching_declared_id_accepted() {
        let tmp = tempfile::TempDir::new().unwrap();
        let data = b"cr-4-5-matching-declared-id";
        let real_id = ChunkId::from_bytes(checksum::compute(data));

        let (result, store) = run_put_chunk(tmp.path(), &real_id.as_hex(), data).await;
        let resp = result.expect("matching declared id must be accepted");
        assert_eq!(resp.chunk_id, real_id.as_hex());
        assert!(store.has_chunk(&real_id).await);
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
            quick_interval_secs: 0,
            deep_interval_secs: 0,
            max_io_percent: 10,
            verify_on_read: true,
            quarantine_on_fault: true,
        };
        DataServiceImpl {
            config,
            store,
            replication,
            metrics: Arc::new(DataMetrics::new()),
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

        let mut req = Request::new(VerifyChunkRequest {
            chunk_id: chunk_hex.clone(),
            disk_id: None,
        });
        attach_data_rpc_auth(
            &mut req,
            &svc.config.cluster.shared_secret,
            "VerifyChunk",
            &chunk_hex,
        );
        let resp = svc
            .verify_chunk(req)
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

        let mut req = Request::new(VerifyChunkRequest {
            chunk_id: chunk_hex.clone(),
            disk_id: None,
        });
        attach_data_rpc_auth(
            &mut req,
            &svc.config.cluster.shared_secret,
            "VerifyChunk",
            &chunk_hex,
        );
        let resp = svc
            .verify_chunk(req)
            .await
            .unwrap()
            .into_inner();

        assert!(
            !resp.valid,
            "VerifyChunk must report invalid when the chunk's .meta is missing"
        );
    }

    // CR-4-6 (DATA-9) — a size mismatch between the on-disk .dat and
    // ChunkMeta.size must be caught up front as "size mismatch", before
    // spending an I/O pass hashing a file that can't possibly match.
    #[tokio::test]
    async fn cr_4_6_verify_chunk_detects_size_mismatch() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path()).await;

        let data = vec![7u8; 4096];
        let chunk_id = ChunkId::from_bytes(checksum::compute(&data));
        let chunk_hex = chunk_id.as_hex();
        svc.store
            .put_chunk(&chunk_id, &data, &VolumeId::new("default".to_string()), DiskTier::Hot)
            .await
            .unwrap();

        // Append trailing garbage: the .meta still claims the original size.
        let (dat_path, _size) = svc.store.chunk_file_path(&chunk_id).unwrap();
        let mut with_garbage = data.clone();
        with_garbage.extend_from_slice(b"trailing-garbage");
        std::fs::write(&dat_path, &with_garbage).unwrap();

        let mut req = Request::new(VerifyChunkRequest {
            chunk_id: chunk_hex.clone(),
            disk_id: None,
        });
        attach_data_rpc_auth(
            &mut req,
            &svc.config.cluster.shared_secret,
            "VerifyChunk",
            &chunk_hex,
        );
        let resp = svc
            .verify_chunk(req)
            .await
            .unwrap()
            .into_inner();

        assert!(!resp.valid, "size mismatch must report invalid");
        assert_eq!(resp.error, "size mismatch");
    }

    async fn verify(svc: &DataServiceImpl, chunk_hex: &str) -> VerifyChunkResponse {
        let mut req = Request::new(VerifyChunkRequest { chunk_id: chunk_hex.to_string(), disk_id: None });
        attach_data_rpc_auth(&mut req, &svc.config.cluster.shared_secret, "VerifyChunk", chunk_hex);
        svc.verify_chunk(req).await.unwrap().into_inner()
    }

    /// Rewrite the `.meta` checksum (size unchanged) to `checksum`.
    fn set_meta_checksum(dat_path: &std::path::Path, checksum: [u8; 32]) {
        let meta_path = dat_path.with_extension("meta");
        let mut meta: dtfs_common::types::ChunkMeta =
            serde_json::from_slice(&std::fs::read(&meta_path).unwrap()).unwrap();
        meta.checksum = checksum;
        std::fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();
    }

    // Audit #13 — VerifyChunk compares against the chunk id (content
    // address), not `.meta`: a stale `.meta` checksum over a `.dat` that
    // hashes to its id is valid (scrub repairs the sidecar).
    #[tokio::test]
    async fn verify_chunk_valid_when_dat_matches_id_despite_stale_meta() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path()).await;
        let data = vec![3u8; 4096];
        let chunk_id = ChunkId::from_bytes(checksum::compute(&data));
        svc.store
            .put_chunk(&chunk_id, &data, &VolumeId::new("default".to_string()), DiskTier::Hot)
            .await
            .unwrap();
        let (dat_path, _) = svc.store.chunk_file_path(&chunk_id).unwrap();
        set_meta_checksum(&dat_path, [0xEE; 32]);

        let resp = verify(&svc, &chunk_id.as_hex()).await;
        assert!(resp.valid, "dat hashing to its id must verify: {}", resp.error);
    }

    // Audit #13 — corrupt bytes with a `.meta` rewritten to match them are
    // still corruption: only the id is authoritative.
    #[tokio::test]
    async fn verify_chunk_invalid_when_meta_matches_corrupt_dat() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path()).await;
        let data = vec![4u8; 4096];
        let chunk_id = ChunkId::from_bytes(checksum::compute(&data));
        svc.store
            .put_chunk(&chunk_id, &data, &VolumeId::new("default".to_string()), DiskTier::Hot)
            .await
            .unwrap();
        let (dat_path, _) = svc.store.chunk_file_path(&chunk_id).unwrap();
        let mut corrupt = data.clone();
        corrupt[0] ^= 0xFF;
        std::fs::write(&dat_path, &corrupt).unwrap();
        set_meta_checksum(&dat_path, checksum::compute(&corrupt));

        let resp = verify(&svc, &chunk_id.as_hex()).await;
        assert!(!resp.valid, "dat not hashing to its id must be invalid");
        assert_eq!(resp.error, format!("checksum mismatch for chunk {}", chunk_id.as_hex()));
    }

    // CR-4-7 (DATA-10) — a non-ASCII chunk_id must be rejected as
    // invalid_argument, never panic. The old hand-rolled `chunk_hex::decode`
    // byte-sliced a `&str` by raw index, which panics on a str-slice that
    // doesn't land on a UTF-8 char boundary; `ChunkId::from_hex` rejects
    // non-ASCII input up front instead of slicing it.
    #[tokio::test]
    async fn cr_4_7_non_ascii_chunk_id_rejected_not_panicked() {
        let tmp = tempfile::TempDir::new().unwrap();
        let svc = make_service(tmp.path()).await;

        let bad_id = "a\u{e9}a".to_string();
        let mut req = Request::new(VerifyChunkRequest { chunk_id: bad_id.clone(), disk_id: None });
        attach_data_rpc_auth(&mut req, &svc.config.cluster.shared_secret, "VerifyChunk", &bad_id);

        let result = svc.verify_chunk(req).await;
        let err = result.expect_err("non-ASCII chunk_id must be rejected, not accepted");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
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
