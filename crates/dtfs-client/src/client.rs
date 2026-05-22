// High-level DTFS client API — Phase 2 (FUSE Client).
// Provides open, read, write, list operations with connection pooling.

use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use dashmap::DashMap;
use tonic::transport::Channel;
use tracing::{debug, instrument};

use dtfs_proto::dtfs::data::data_service_client::DataServiceClient;
use dtfs_proto::dtfs::data::{GetChunkRequest, PutChunkRequest};
use dtfs_proto::dtfs::manager::manager_service_client::ManagerServiceClient;
use dtfs_proto::dtfs::manager::{
    AcquireLockRequest, AcquireLockResponse, AcquireWriteLeaseRequest, AcquireWriteLeaseResponse,
    AllocateChunksRequest, AllocateChunksResponse, ChunkLocationCommit, ClusterCapacityRequest,
    CommitChunkLocationsRequest, CreateFileRequest, CreateFileResponse, CreateLinkRequest,
    CreateSymlinkRequest, DeleteFileRequest, DeleteFileResponse, GetFileLocationsRequest,
    GetFileLocationsResponse, ListDirectoryRequest, ListDirectoryResponse, ProbeLockRequest,
    ProbeLockResponse, ReadSymlinkRequest, ReleaseLeasesByHostRequest,
    ReleaseLeasesByHostResponse, ReleaseLockRequest, ReleaseWriteLeaseRequest, RenameFileRequest,
    RenewWriteLeaseRequest, RenewWriteLeaseResponse, SetFileSizeRequest,
};

/// Size of each streaming piece for chunk I/O (1 MB).
const STREAM_PIECE_SIZE: usize = 1024 * 1024;

/// Bug G — retry policy for `RESOURCE_EXHAUSTED` from put_chunk. The
/// data node returns this status while its memory budget is in the
/// "shedding" state. Client backs off exponentially and retries — this
/// is the cooperative half of the hard-cap design.
const PUT_CHUNK_MAX_RETRIES: u32 = 6;
const PUT_CHUNK_BASE_BACKOFF_MS: u64 = 250;
const PUT_CHUNK_MAX_BACKOFF_MS: u64 = 8_000;

/// Decide how long to back off before retry on a put_chunk failure.
/// Returns `None` when the caller should give up (non-retryable status
/// or attempts exhausted).
///
/// Pure function so we can unit-test the policy without running gRPC.
pub(crate) fn put_chunk_retry_backoff(
    code: tonic::Code,
    attempt_zero_based: u32,
) -> Option<std::time::Duration> {
    if attempt_zero_based >= PUT_CHUNK_MAX_RETRIES {
        return None;
    }
    // Only retry on transient/cooperative-overload codes.
    match code {
        tonic::Code::ResourceExhausted | tonic::Code::Unavailable => {
            let ms = PUT_CHUNK_BASE_BACKOFF_MS
                .saturating_mul(1 << attempt_zero_based)
                .min(PUT_CHUNK_MAX_BACKOFF_MS);
            Some(std::time::Duration::from_millis(ms))
        }
        _ => None,
    }
}

/// Inner shared state for the client, wrapped in Arc for cheap cloning.
struct DtfsClientInner {
    /// Cached manager gRPC channels, keyed by address.
    manager_channels: DashMap<String, Channel>,
    /// Cached data node gRPC channels, keyed by address.
    data_channels: DashMap<String, Channel>,
    /// Ordered list of manager addresses; first reachable becomes active.
    /// On Unavailable RPC errors the active index advances so subsequent
    /// calls hit the next manager (P0-3 runtime failover).
    manager_addrs: parking_lot::RwLock<Vec<String>>,
    /// Index into `manager_addrs` of the currently-preferred manager.
    active_idx: parking_lot::RwLock<usize>,
    /// P0-9: client's DC label, attached to every GetChunk request so the
    /// data node can bucket per-chunk reads by DC for the tiering engine's
    /// auto-move-replica-toward-active-DC decision.
    requester_dc: parking_lot::RwLock<String>,
    /// Optional host name for the same purpose (mostly diagnostic).
    requester_host: parking_lot::RwLock<String>,
}

/// High-level client for interacting with the DTFS cluster.
///
/// Talks to manager nodes for metadata operations and directly to data nodes
/// for chunk I/O. Maintains a pool of gRPC connections via `DashMap` for
/// efficient reuse.
///
/// This type is `Clone + Send + Sync` — cloning is cheap (Arc-based).
#[derive(Clone)]
pub struct DtfsClient {
    inner: Arc<DtfsClientInner>,
}

impl DtfsClient {
    /// Create a new `DtfsClient` without connecting to any manager.
    /// Call [`connect`](Self::connect) or [`connect_many`](Self::connect_many)
    /// to establish a connection.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DtfsClientInner {
                manager_channels: DashMap::new(),
                data_channels: DashMap::new(),
                manager_addrs: parking_lot::RwLock::new(Vec::new()),
                active_idx: parking_lot::RwLock::new(0),
                requester_dc: parking_lot::RwLock::new(String::new()),
                requester_host: parking_lot::RwLock::new(String::new()),
            }),
        }
    }

    /// P0-9: set the client's DC/host labels for `read_chunk` telemetry.
    /// Should be called once after construction (typically from the FUSE
    /// startup path using the topology location from fuse.toml).
    pub fn set_requester_location(&self, dc: &str, host: &str) {
        *self.inner.requester_dc.write() = dc.to_string();
        *self.inner.requester_host.write() = host.to_string();
    }

    /// Connect to a single manager node. Backward-compat shim for callers
    /// that have one address; prefer [`connect_many`](Self::connect_many)
    /// for HA clusters.
    pub async fn connect(&self, manager_addr: &str) -> Result<()> {
        self.connect_many(&[manager_addr.to_string()]).await
    }

    /// Connect to a cluster of manager nodes. The list is stored in order;
    /// the first reachable becomes the active target. Subsequent RPC failures
    /// with transport-level errors (Unavailable/DeadlineExceeded) advance to
    /// the next address, providing runtime failover (P0-3).
    #[instrument(skip(self))]
    pub async fn connect_many(&self, addrs: &[String]) -> Result<()> {
        if addrs.is_empty() {
            anyhow::bail!("connect_many called with empty address list");
        }
        {
            let mut slot = self.inner.manager_addrs.write();
            *slot = addrs.to_vec();
            *self.inner.active_idx.write() = 0;
        }

        // Probe each address until one connects; record the working idx.
        let n = addrs.len();
        let mut last_err: Option<anyhow::Error> = None;
        for i in 0..n {
            match self
                .get_or_create_channel(&self.inner.manager_channels, &addrs[i])
                .await
            {
                Ok(_) => {
                    *self.inner.active_idx.write() = i;
                    debug!(
                        addr = %addrs[i],
                        idx = i,
                        total = n,
                        "connected to manager"
                    );
                    return Ok(());
                }
                Err(e) => {
                    debug!(addr = %addrs[i], error = %e, "manager unreachable, trying next");
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no manager addresses reachable")))
    }

    /// Advance the active manager index by 1 (mod len). Called when an RPC
    /// returns a transport-level error suggesting the current manager is
    /// unreachable. Idempotent if only one address is configured.
    pub(crate) fn advance_active_manager(&self) {
        let len = self.inner.manager_addrs.read().len();
        if len <= 1 {
            return;
        }
        let mut idx = self.inner.active_idx.write();
        let old = *idx;
        *idx = (*idx + 1) % len;
        let addrs = self.inner.manager_addrs.read();
        tracing::warn!(
            old_addr = %addrs[old],
            new_addr = %addrs[*idx],
            "advancing active manager after RPC failure"
        );
    }

    /// Create a new file in the DTFS namespace.
    #[instrument(skip(self))]
    pub async fn create_file(
        &self,
        path: &str,
        volume_id: Option<&str>,
    ) -> Result<CreateFileResponse> {
        let request = CreateFileRequest {
            path: path.to_string(),
            volume_id: volume_id.map(|v| v.to_string()),
        };
        let response = self
            .call_manager("create_file", |mut c| {
                let r = request.clone();
                async move { c.create_file(r).await }
            })
            .await?;
        Ok(response.into_inner())
    }

    /// Get chunk locations for a file.
    #[instrument(skip(self))]
    pub async fn get_file_locations(&self, path: &str) -> Result<GetFileLocationsResponse> {
        let request = GetFileLocationsRequest {
            path: path.to_string(),
        };
        let response = self
            .call_manager("get_file_locations", |mut c| {
                let r = request.clone();
                async move { c.get_file_locations(r).await }
            })
            .await?;
        Ok(response.into_inner())
    }

    /// Delete a file from the DTFS namespace.
    #[instrument(skip(self))]
    pub async fn delete_file(&self, path: &str) -> Result<DeleteFileResponse> {
        let request = DeleteFileRequest {
            path: path.to_string(),
        };
        let response = self
            .call_manager("delete_file", |mut c| {
                let r = request.clone();
                async move { c.delete_file(r).await }
            })
            .await?;
        Ok(response.into_inner())
    }

    /// List directory entries at the given path.
    #[instrument(skip(self))]
    pub async fn list_directory(&self, path: &str) -> Result<ListDirectoryResponse> {
        let request = ListDirectoryRequest {
            path: path.to_string(),
        };
        let response = self
            .call_manager("list_directory", |mut c| {
                let r = request.clone();
                async move { c.list_directory(r).await }
            })
            .await?;
        Ok(response.into_inner())
    }

    /// Allocate chunk placements for writing to a file.
    ///
    /// Bug AV (AV-3-2) — when `preferred_tier` is non-empty, the
    /// manager honours the tier hint for first-chunk placement if
    /// a healthy node with that tier exists in the cluster.
    /// Recognised values: `"cold"`, `"warm"`, `"hot"`, `"tape"`.
    /// Empty string = use the volume default (legacy behaviour).
    #[instrument(skip(self))]
    pub async fn allocate_chunks(
        &self,
        path: &str,
        num_chunks: u32,
        volume_id: &str,
    ) -> Result<AllocateChunksResponse> {
        self.allocate_chunks_with_hint(path, num_chunks, volume_id, "")
            .await
    }

    /// Bug AV (AV-3-2) — like [`allocate_chunks`] but with an
    /// explicit `preferred_tier` hint. Callers that don't care
    /// about tier preference should use [`allocate_chunks`].
    #[instrument(skip(self))]
    pub async fn allocate_chunks_with_hint(
        &self,
        path: &str,
        num_chunks: u32,
        volume_id: &str,
        preferred_tier: &str,
    ) -> Result<AllocateChunksResponse> {
        let request = AllocateChunksRequest {
            path: path.to_string(),
            num_chunks,
            volume_id: volume_id.to_string(),
            preferred_tier: preferred_tier.to_string(),
        };
        let response = self
            .call_manager("allocate_chunks", |mut c| {
                let r = request.clone();
                async move { c.allocate_chunks(r).await }
            })
            .await?;
        Ok(response.into_inner())
    }

    /// Persist chunk locations to the manager metadata (Raft) after successful writes.
    ///
    /// Bug P — `lease_id` is the writer's current write-lease token.
    /// When non-empty, the manager rejects the commit if it doesn't
    /// match the held lease — protecting against interleaved bytes
    /// when a stale writer's flush races a fresh writer's. Empty
    /// `lease_id` preserves legacy (no-verification) behaviour.
    #[instrument(skip(self, commits))]
    pub async fn commit_chunk_locations(
        &self,
        path: &str,
        volume_id: &str,
        commits: Vec<ChunkLocationCommit>,
        lease_id: &str,
    ) -> Result<bool> {
        let request = CommitChunkLocationsRequest {
            path: path.to_string(),
            volume_id: volume_id.to_string(),
            chunks: commits,
            lease_id: lease_id.to_string(),
        };
        let response = self
            .call_manager("commit_chunk_locations", |mut c| {
                let r = request.clone();
                async move { c.commit_chunk_locations(r).await }
            })
            .await?;
        let inner = response.into_inner();
        if inner.lease_conflict {
            anyhow::bail!(
                "commit_chunk_locations rejected: write lease no longer held ({})",
                inner.error
            );
        }
        Ok(inner.success)
    }

    /// Bug AV (AV-4-1c) — notify a data node that a chunk was
    /// served to a local FUSE-side cache hit. Used so the
    /// data-node's per-DC read counter reflects ALL client
    /// reads (not only the ones that traverse the network).
    /// Without this, the FUSE readahead absorbs repeat reads
    /// and the tiering engine never sees enough demand to fire
    /// a promotion.
    ///
    /// Fire-and-forget at the call site (use `tokio::spawn`).
    /// Errors are logged but never propagated — a missed
    /// metrics ping is acceptable; an aborted read is not.
    #[instrument(skip(self))]
    pub async fn record_cache_hit(
        &self,
        node_addr: &str,
        chunk_id: &str,
    ) -> Result<()> {
        use dtfs_proto::dtfs::data::RecordCacheHitRequest;
        let mut client = self.data_client(node_addr).await?;
        let request = RecordCacheHitRequest {
            chunk_id: chunk_id.to_string(),
            requester_dc: self.inner.requester_dc.read().clone(),
            requester_host: self.inner.requester_host.read().clone(),
        };
        client
            .record_cache_hit(request)
            .await
            .context("record_cache_hit RPC failed")?;
        Ok(())
    }

    /// Read a chunk from a data node.
    ///
    /// Uses server streaming: the data node sends the chunk back in 1 MB
    /// pieces which are reassembled into a single `Bytes` buffer.
    #[instrument(skip(self))]
    pub async fn read_chunk(&self, node_addr: &str, chunk_id: &str) -> Result<Bytes> {
        let mut client = self.data_client(node_addr).await?;
        let request = GetChunkRequest {
            chunk_id: chunk_id.to_string(),
            requester_dc: self.inner.requester_dc.read().clone(),
            requester_host: self.inner.requester_host.read().clone(),
        };
        let response = client
            .get_chunk(request)
            .await
            .context("get_chunk RPC failed")?;

        let mut stream = response.into_inner();
        // Pre-allocate based on the total_size hint in the first piece to
        // avoid repeated Vec re-allocations for large chunks (fix for F-011).
        let mut data: Vec<u8> = Vec::new();
        let mut total_hint = 0u64;

        while let Some(piece) = stream
            .message()
            .await
            .context("error reading chunk stream")?
        {
            if total_hint == 0 && piece.total_size > 0 {
                total_hint = piece.total_size;
                data.reserve(piece.total_size as usize);
            }
            data.extend_from_slice(&piece.data);
        }

        debug!(
            chunk_id,
            size = data.len(),
            node = node_addr,
            "read chunk complete"
        );
        Ok(Bytes::from(data))
    }

    /// Write a chunk to a data node.
    ///
    /// Uses client streaming: the data is split into 1 MB pieces and sent
    /// as a stream of `PutChunkRequest` messages. The first message carries
    /// the metadata (chunk_id, volume_id, tier); subsequent messages carry
    /// only the data payload.
    #[instrument(skip(self, data))]
    pub async fn write_chunk(
        &self,
        node_addr: &str,
        chunk_id: &str,
        volume_id: &str,
        tier: &str,
        data: &[u8],
    ) -> Result<dtfs_proto::dtfs::data::PutChunkResponse> {
        // Internal helper that takes Bytes (cheap to clone, refcounted) so
        // callers writing in parallel don't multiply 128 MB by N replicas.
        self.write_chunk_bytes(
            node_addr,
            chunk_id,
            volume_id,
            tier,
            bytes::Bytes::copy_from_slice(data),
        )
        .await
    }

    /// Stream a chunk to a data node WITHOUT eagerly building the full
    /// message list in memory. Each piece is yielded one at a time over a
    /// bounded mpsc channel, so the peak memory while writing a 128 MB chunk
    /// stays at roughly `STREAM_PIECE_SIZE × channel_capacity` (≈ 8 MB), not
    /// 256 MB. This is what allows F-010 (parallel writes to N replicas) to
    /// not OOM small clients on large-file workloads.
    pub async fn write_chunk_bytes(
        &self,
        node_addr: &str,
        chunk_id: &str,
        volume_id: &str,
        tier: &str,
        data: bytes::Bytes,
    ) -> Result<dtfs_proto::dtfs::data::PutChunkResponse> {
        // Bug G — retry on RESOURCE_EXHAUSTED with exponential backoff
        // so the FUSE client cooperates with data-node memory shedding
        // instead of bubbling errors up to userspace immediately.
        let mut attempt: u32 = 0;
        loop {
            match self
                .write_chunk_bytes_once(
                    node_addr,
                    chunk_id,
                    volume_id,
                    tier,
                    data.clone(),
                )
                .await
            {
                Ok(resp) => return Ok(resp),
                Err(status) => {
                    if let Some(delay) = put_chunk_retry_backoff(status.code(), attempt) {
                        debug!(
                            chunk_id = %chunk_id,
                            node = %node_addr,
                            attempt,
                            backoff_ms = delay.as_millis() as u64,
                            code = ?status.code(),
                            "put_chunk transient error, retrying after backoff"
                        );
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(anyhow::anyhow!(
                        "put_chunk RPC failed (code={:?}, attempts={}): {}",
                        status.code(),
                        attempt + 1,
                        status.message()
                    ));
                }
            }
        }
    }

    /// One pass of the put_chunk streaming RPC. Returns the gRPC
    /// `Status` on failure so the retry wrapper can inspect the code.
    async fn write_chunk_bytes_once(
        &self,
        node_addr: &str,
        chunk_id: &str,
        volume_id: &str,
        tier: &str,
        data: bytes::Bytes,
    ) -> std::result::Result<dtfs_proto::dtfs::data::PutChunkResponse, tonic::Status> {
        let mut client = self
            .data_client(node_addr)
            .await
            .map_err(|e| tonic::Status::unavailable(format!("connect: {e}")))?;

        let chunk_id_owned = chunk_id.to_string();
        let volume_id_owned = volume_id.to_string();
        let tier_owned = tier.to_string();

        let (tx, rx) = tokio::sync::mpsc::channel::<PutChunkRequest>(8);
        let id_for_task = chunk_id_owned.clone();
        let vol_for_task = volume_id_owned.clone();
        let tier_for_task = tier_owned.clone();
        tokio::spawn(async move {
            let total = data.len();
            if total == 0 {
                let _ = tx
                    .send(PutChunkRequest {
                        chunk_id: id_for_task,
                        volume_id: vol_for_task,
                        tier: tier_for_task,
                        data: Vec::new(),
                    })
                    .await;
                return;
            }
            let mut offset = 0;
            let mut first = true;
            while offset < total {
                let end = std::cmp::min(offset + STREAM_PIECE_SIZE, total);
                let piece = data.slice(offset..end);
                let msg = if first {
                    PutChunkRequest {
                        chunk_id: id_for_task.clone(),
                        volume_id: vol_for_task.clone(),
                        tier: tier_for_task.clone(),
                        data: piece.to_vec(),
                    }
                } else {
                    PutChunkRequest {
                        chunk_id: String::new(),
                        volume_id: String::new(),
                        tier: String::new(),
                        data: piece.to_vec(),
                    }
                };
                if tx.send(msg).await.is_err() {
                    break;
                }
                first = false;
                offset = end;
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let response = client.put_chunk(stream).await?;
        let put_resp = response.into_inner();
        debug!(
            chunk_id = chunk_id_owned,
            size = put_resp.size,
            node = node_addr,
            "write chunk complete"
        );
        Ok(put_resp)
    }

    // --- Internal helpers ---

    /// Get a `ManagerServiceClient` connected to the active manager.
    ///
    /// If the active manager's channel cannot be created (process dead, DNS
    /// failure, port closed), advances `active_idx` and tries the next
    /// configured manager. Repeats until a channel succeeds or all addresses
    /// have been tried.
    async fn manager_client(&self) -> Result<ManagerServiceClient<Channel>> {
        let addrs = self.inner.manager_addrs.read().clone();
        if addrs.is_empty() {
            anyhow::bail!("not connected to a manager — call connect()/connect_many() first");
        }
        let n = addrs.len();
        let start = *self.inner.active_idx.read();
        let mut last_err: Option<anyhow::Error> = None;
        for offset in 0..n {
            let idx = (start + offset) % n;
            let addr = &addrs[idx];
            match self
                .get_or_create_channel(&self.inner.manager_channels, addr)
                .await
            {
                Ok(channel) => {
                    if idx != start {
                        *self.inner.active_idx.write() = idx;
                        tracing::info!(
                            old_addr = %addrs[start],
                            new_addr = %addr,
                            "manager failover (channel-level)"
                        );
                    }
                    return Ok(ManagerServiceClient::new(channel));
                }
                Err(e) => {
                    // Evict the broken channel so a subsequent attempt rebuilds it.
                    self.inner.manager_channels.remove(addr);
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no managers reachable")))
    }

    /// Run a manager RPC closure with automatic failover on transport errors.
    ///
    /// Retries up to `manager_addrs.len()` times. On each retry, the closure
    /// is given a freshly-built `ManagerServiceClient` connected to the
    /// (possibly newly-active) manager. Stops retrying on application-level
    /// errors (FailedPrecondition, NotFound, AlreadyExists, etc.) — those
    /// surface immediately.
    pub(crate) async fn call_manager<F, Fut, T>(&self, op_name: &'static str, mut f: F) -> Result<T>
    where
        F: FnMut(ManagerServiceClient<Channel>) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, tonic::Status>>,
    {
        let attempts = self.inner.manager_addrs.read().len().max(1);
        let mut last_err: Option<tonic::Status> = None;
        for attempt in 0..attempts {
            let client = self.manager_client().await?;
            match f(client).await {
                Ok(v) => return Ok(v),
                Err(status) => {
                    let code = status.code();
                    if matches!(
                        code,
                        tonic::Code::Unavailable
                            | tonic::Code::DeadlineExceeded
                            | tonic::Code::Unknown
                    ) {
                        tracing::warn!(
                            rpc = op_name,
                            code = ?code,
                            attempt = attempt + 1,
                            "manager RPC transport error — failing over"
                        );
                        // Evict the current channel so manager_client rebuilds.
                        if let Some(addr) = self.inner.manager_addrs.read().get(*self.inner.active_idx.read()) {
                            self.inner.manager_channels.remove(addr);
                        }
                        self.advance_active_manager();
                        last_err = Some(status);
                        continue;
                    } else {
                        // Application-level error — return immediately.
                        return Err(anyhow::Error::new(status)
                            .context(format!("{op_name} RPC failed")));
                    }
                }
            }
        }
        Err(last_err
            .map(|s| anyhow::Error::new(s).context(format!("{op_name} RPC failed (all managers exhausted)")))
            .unwrap_or_else(|| anyhow::anyhow!("{op_name}: no managers configured")))
    }

    /// Get a `DataServiceClient` connected to the given data node address.
    async fn data_client(&self, addr: &str) -> Result<DataServiceClient<Channel>> {
        let channel = self
            .get_or_create_channel(&self.inner.data_channels, addr)
            .await?;
        // Tonic's default max_decoding_message_size is 4 MB; the data node
        // streams chunk payloads in 8 MB pieces, so bump the cap (32 MB is
        // safely above the piece size and matches the encode cap on the
        // server side).
        Ok(DataServiceClient::new(channel)
            .max_decoding_message_size(32 * 1024 * 1024)
            .max_encoding_message_size(32 * 1024 * 1024))
    }

    /// Get an existing channel from the pool or create a new one.
    async fn get_or_create_channel(
        &self,
        pool: &DashMap<String, Channel>,
        addr: &str,
    ) -> Result<Channel> {
        // Fast path: channel already exists.
        if let Some(entry) = pool.get(addr) {
            return Ok(entry.value().clone());
        }

        // Slow path: create a new channel.
        let endpoint = tonic::transport::Endpoint::from_shared(addr.to_string())
            .context("invalid endpoint address")?
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(60));

        let channel = endpoint
            .connect()
            .await
            .with_context(|| format!("failed to connect to {addr}"))?;

        pool.insert(addr.to_string(), channel.clone());
        debug!(addr, "created new gRPC channel");
        Ok(channel)
    }

    // ---- Extended POSIX wrappers (F-034 .. F-038) ----

    pub async fn rename_file(&self, from: &str, to: &str) -> Result<()> {
        let request = RenameFileRequest {
            from: from.to_string(),
            to: to.to_string(),
        };
        let resp = self
            .call_manager("rename_file", |mut c| {
                let r = request.clone();
                async move { c.rename_file(r).await }
            })
            .await?
            .into_inner();
        if resp.success {
            Ok(())
        } else {
            anyhow::bail!("RenameFile failed: {}", resp.error)
        }
    }

    pub async fn set_file_size(&self, path: &str, new_size: u64) -> Result<()> {
        let request = SetFileSizeRequest {
            path: path.to_string(),
            new_size,
        };
        let resp = self
            .call_manager("set_file_size", |mut c| {
                let r = request.clone();
                async move { c.set_file_size(r).await }
            })
            .await?
            .into_inner();
        if resp.success {
            Ok(())
        } else {
            anyhow::bail!("SetFileSize failed: {}", resp.error)
        }
    }

    pub async fn symlink(&self, path: &str, target: &str) -> Result<()> {
        let request = CreateSymlinkRequest {
            path: path.to_string(),
            target: target.to_string(),
        };
        let resp = self
            .call_manager("create_symlink", |mut c| {
                let r = request.clone();
                async move { c.create_symlink(r).await }
            })
            .await?
            .into_inner();
        if resp.success {
            Ok(())
        } else {
            anyhow::bail!("Symlink failed: {}", resp.error)
        }
    }

    pub async fn readlink(&self, path: &str) -> Result<String> {
        let request = ReadSymlinkRequest {
            path: path.to_string(),
        };
        let resp = self
            .call_manager("read_symlink", |mut c| {
                let r = request.clone();
                async move { c.read_symlink(r).await }
            })
            .await?
            .into_inner();
        if resp.found {
            Ok(resp.target)
        } else {
            anyhow::bail!("not a symlink: {path}")
        }
    }

    pub async fn link_file(&self, source: &str, dest: &str) -> Result<()> {
        let request = CreateLinkRequest {
            source: source.to_string(),
            dest: dest.to_string(),
        };
        let resp = self
            .call_manager("create_link", |mut c| {
                let r = request.clone();
                async move { c.create_link(r).await }
            })
            .await?
            .into_inner();
        if resp.success {
            Ok(())
        } else {
            anyhow::bail!("Link failed: {}", resp.error)
        }
    }

    pub async fn cluster_capacity(&self) -> Result<(u64, u64, u64)> {
        let request = ClusterCapacityRequest {};
        let resp = self
            .call_manager("cluster_capacity", |mut c| {
                let r = request.clone();
                async move { c.cluster_capacity(r).await }
            })
            .await?
            .into_inner();
        Ok((resp.total_bytes, resp.used_bytes, resp.file_count))
    }

    // ---- Distributed POSIX byte-range locks (F-039) ----

    pub async fn acquire_lock(
        &self,
        path: &str,
        owner_id: &str,
        start: u64,
        end: u64,
        write: bool,
        blocking: bool,
        lease_ms: u64,
    ) -> Result<AcquireLockResponse> {
        let request = AcquireLockRequest {
            path: path.to_string(),
            owner_id: owner_id.to_string(),
            start,
            end,
            write,
            blocking,
            lease_ms,
        };
        let resp = self
            .call_manager("acquire_lock", |mut c| {
                let r = request.clone();
                async move { c.acquire_lock(r).await }
            })
            .await?
            .into_inner();
        Ok(resp)
    }

    pub async fn release_lock(
        &self,
        path: &str,
        owner_id: &str,
        start: u64,
        end: u64,
    ) -> Result<()> {
        let request = ReleaseLockRequest {
            path: path.to_string(),
            owner_id: owner_id.to_string(),
            start,
            end,
        };
        let _ = self
            .call_manager("release_lock", |mut c| {
                let r = request.clone();
                async move { c.release_lock(r).await }
            })
            .await?;
        Ok(())
    }

    pub async fn probe_lock(
        &self,
        path: &str,
        owner_id: &str,
        start: u64,
        end: u64,
        write: bool,
    ) -> Result<ProbeLockResponse> {
        let request = ProbeLockRequest {
            path: path.to_string(),
            owner_id: owner_id.to_string(),
            start,
            end,
            write,
        };
        let resp = self
            .call_manager("probe_lock", |mut c| {
                let r = request.clone();
                async move { c.probe_lock(r).await }
            })
            .await?
            .into_inner();
        Ok(resp)
    }

    // ---- P0-1: single-writer leases ----

    pub async fn acquire_write_lease(
        &self,
        path: &str,
        owner_id: &str,
        lease_ms: u64,
    ) -> Result<AcquireWriteLeaseResponse> {
        let request = AcquireWriteLeaseRequest {
            path: path.to_string(),
            owner_id: owner_id.to_string(),
            lease_ms,
        };
        let resp = self
            .call_manager("acquire_write_lease", |mut c| {
                let r = request.clone();
                async move { c.acquire_write_lease(r).await }
            })
            .await?
            .into_inner();
        Ok(resp)
    }

    pub async fn renew_write_lease(
        &self,
        path: &str,
        lease_id: &str,
        extension_ms: u64,
    ) -> Result<RenewWriteLeaseResponse> {
        let request = RenewWriteLeaseRequest {
            path: path.to_string(),
            lease_id: lease_id.to_string(),
            extension_ms,
        };
        let resp = self
            .call_manager("renew_write_lease", |mut c| {
                let r = request.clone();
                async move { c.renew_write_lease(r).await }
            })
            .await?
            .into_inner();
        Ok(resp)
    }

    pub async fn release_write_lease(
        &self,
        path: &str,
        lease_id: &str,
    ) -> Result<()> {
        let request = ReleaseWriteLeaseRequest {
            path: path.to_string(),
            lease_id: lease_id.to_string(),
        };
        let _ = self
            .call_manager("release_write_lease", |mut c| {
                let r = request.clone();
                async move { c.release_write_lease(r).await }
            })
            .await?;
        Ok(())
    }

    pub async fn release_leases_by_host(
        &self,
        hostname: &str,
        except_pid: u32,
    ) -> Result<ReleaseLeasesByHostResponse> {
        let request = ReleaseLeasesByHostRequest {
            hostname: hostname.to_string(),
            except_pid,
        };
        let resp = self
            .call_manager("release_leases_by_host", |mut c| {
                let r = request.clone();
                async move { c.release_leases_by_host(r).await }
            })
            .await?
            .into_inner();
        Ok(resp)
    }

    /// Bug AA — register this FUSE client with the manager so its full
    /// `Location` (region/dc/rack/host) appears in `/api/v1/nodes` even
    /// when the client never acquires a write lease (read-only Plex
    /// viewers, dormant download clients, etc.).
    ///
    /// Re-uses the existing `RegisterNode` RPC with `role="client"`.
    /// `shared_secret` matches the cluster secret so the manager
    /// accepts the registration; data nodes use the same gate.
    pub async fn register_self_as_client(
        &self,
        hostname: &str,
        location: dtfs_proto::dtfs::common::Location,
        shared_secret: Option<String>,
    ) -> Result<()> {
        let request = dtfs_proto::dtfs::manager::RegisterNodeRequest {
            hostname: hostname.to_string(),
            role: "client".to_string(),
            grpc_port: 0,
            location: Some(location),
            disks: Vec::new(),
            shared_secret,
            // Bug AY — report what this client binary is running so the
            // manager Nodes page can flag stale FUSE mounts.
            version: dtfs_common::version::VERSION.to_string(),
            build_date: dtfs_common::version::BUILD_DATE.to_string(),
        };
        let _ = self
            .call_manager("register_node", |mut c| {
                let r = request.clone();
                async move { c.register_node(r).await }
            })
            .await?;
        Ok(())
    }
}

impl Default for DtfsClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod put_chunk_retry_tests {
    use super::*;
    use std::time::Duration;

    /// Bug G — RESOURCE_EXHAUSTED is the data node saying "memory budget
    /// shedding, retry later". Client MUST back off and retry, with
    /// exponential growth and a per-attempt cap.
    #[test]
    fn bug_g_resource_exhausted_first_attempts_back_off_exponentially() {
        // attempt 0 → 250 ms
        assert_eq!(
            put_chunk_retry_backoff(tonic::Code::ResourceExhausted, 0),
            Some(Duration::from_millis(250))
        );
        // attempt 1 → 500 ms
        assert_eq!(
            put_chunk_retry_backoff(tonic::Code::ResourceExhausted, 1),
            Some(Duration::from_millis(500))
        );
        // attempt 2 → 1000 ms
        assert_eq!(
            put_chunk_retry_backoff(tonic::Code::ResourceExhausted, 2),
            Some(Duration::from_millis(1000))
        );
        // attempt 5 → 8000 ms (capped at PUT_CHUNK_MAX_BACKOFF_MS)
        assert_eq!(
            put_chunk_retry_backoff(tonic::Code::ResourceExhausted, 5),
            Some(Duration::from_millis(8000))
        );
    }

    /// After PUT_CHUNK_MAX_RETRIES attempts, give up.
    #[test]
    fn bug_g_resource_exhausted_eventually_gives_up() {
        assert_eq!(
            put_chunk_retry_backoff(tonic::Code::ResourceExhausted, PUT_CHUNK_MAX_RETRIES),
            None,
            "after max retries the wrapper must surface the error to the caller"
        );
    }

    /// Unavailable (channel closed, server restarting) is also retryable.
    #[test]
    fn bug_g_unavailable_is_retryable() {
        assert!(put_chunk_retry_backoff(tonic::Code::Unavailable, 0).is_some());
    }

    /// Anything else (InvalidArgument, InternalError, …) must NOT retry —
    /// retrying a bad request just amplifies the failure.
    #[test]
    fn bug_g_non_retryable_codes_return_none() {
        for code in [
            tonic::Code::InvalidArgument,
            tonic::Code::NotFound,
            tonic::Code::PermissionDenied,
            tonic::Code::Internal,
            tonic::Code::DataLoss,
        ] {
            assert!(
                put_chunk_retry_backoff(code, 0).is_none(),
                "code {:?} must not be auto-retried",
                code
            );
        }
    }
}
