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
use crate::scheduler::live_replica_tier;

/// CR-6-3 (PROTO-2) — warn once per manager process when a sub-hour
/// quick/deep scrub cadence is configured, since an older data node
/// can't honour it (rounds up to the nearest hour via the legacy
/// `*_interval_hours` fields).
static WARNED_SUBHOUR_SCRUB_CADENCE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// RAFT-4 — proposer wall-clock stamped into the `now_ms` field of the
/// seven ClientRequest variants apply now applies deterministically
/// (see `dtfs-raft/src/state_machine.rs`'s `audit_at`/`eff_now_ms`).
fn stamp_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
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
    /// Bug BX-10 — audit log for `placement.spill` rows on the allocate
    /// path. Optional so construction order stays simple; injected via
    /// `with_audit` from `main`.
    audit: Option<Arc<crate::audit::AuditLog>>,
    /// CF-6 — resolved `[tls]` context; manager-to-manager forwards and
    /// the delete fan-out to data nodes speak mTLS when enabled.
    tls: dtfs_common::tls::TlsContext,
}

impl MetadataService {
    pub fn new(
        config: ManagerConfig,
        raft: Arc<DtfsRaft>,
        sm_data: Arc<tokio::sync::RwLock<StateMachineData>>,
        placement: Arc<PlacementEngine>,
        cluster: Arc<ClusterManager>,
        tls: dtfs_common::tls::TlsContext,
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
            audit: None,
            tls,
        }
    }

    /// Bug BX-10 — inject the audit log so capacity spills record a
    /// `placement.spill` row.
    pub fn with_audit(mut self, audit: Arc<crate::audit::AuditLog>) -> Self {
        self.audit = Some(audit);
        self
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
                if is_forward_to_leader_err(&msg) {
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

        // CO-3-9 — resolve the leader's gRPC addr from the live Raft
        // membership first (covers CF-2-promoted leaders absent from
        // initial_voters), falling back to the configured voter set.
        let leader_addr = {
            let sm = self.sm_data.read().await;
            resolve_leader_addr(&self.config.cluster.initial_voters, &sm.nodes, leader_id)
        }
        .ok_or_else(|| {
            Status::unavailable(format!(
                "leader {leader_id} not found in raft membership or cluster.initial_voters"
            ))
        })?;

        let mut client = self.leader_client(&leader_addr).await?;
        let body = serde_json::to_vec(&req)
            .map_err(|e| Status::internal(format!("serialize ClientRequest: {e}")))?;
        let resp = client
            .forward_client_write(ForwardWriteRequest {
                client_request_json: body,
                // CO-2-2 — authenticate this forward to the leader.
                shared_secret: self.config.cluster.shared_secret.clone(),
            })
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
        // CR2-8-1 (CMN-3) — size this manager-to-manager forward client
        // from the configured cap rather than tonic's 4 MB default.
        let max_bytes =
            dtfs_common::config::max_message_bytes(self.config.grpc.max_message_size_mb);
        let client = self
            .tls
            .connect(addr)
            .await
            .map(|ch| {
                ManagerServiceClient::new(ch)
                    .max_decoding_message_size(max_bytes)
                    .max_encoding_message_size(max_bytes)
            })
            .map_err(|e| Status::unavailable(format!("connect leader {addr}: {e}")))?;
        self.leader_clients
            .lock()
            .await
            .insert(addr.to_string(), client.clone());
        Ok(client)
    }
}

/// Partition corrupt/missing entries of a `ChunkStatusReport` by disk. Reports
/// without a disk id (older data nodes) land under `legacy_disk_id()`, which
/// the apply arm treats as a node-wide sweep.
fn group_corrupt_by_disk(
    chunks: &[dtfs_proto::dtfs::manager::ChunkStatus],
) -> std::collections::BTreeMap<u32, Vec<String>> {
    let mut out: std::collections::BTreeMap<u32, Vec<String>> = Default::default();
    for ch in chunks {
        if ch.status.eq_ignore_ascii_case("corrupt") || ch.status.eq_ignore_ascii_case("missing") {
            out.entry(ch.disk_id.unwrap_or_else(dtfs_raft::legacy_disk_id))
                .or_default()
                .push(ch.chunk_id.clone());
        }
    }
    out
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

/// CR-3-6/MGR-7 — both forwarded Raft writes for a heartbeat's
/// integrity-fault batch (`QuarantineNode` and, when the batch carries
/// chunk ids, `RemoveCorruptChunkReplicas`) must succeed for the batch
/// to count as accepted. Either failure means the node is not
/// quarantined and/or the corrupt replica is not de-located, so the
/// caller must surface `HeartbeatResponse.accepted = false` — the data
/// node already re-enqueues its drained faults on that (Bug BF-5),
/// mirroring the CO-3-7 contract `report_chunk_status` already has.
fn integrity_fault_batch_accepted(quarantine_ok: bool, remove_corrupt_ok: bool) -> bool {
    quarantine_ok && remove_corrupt_ok
}

/// CO-3-8 — may a heartbeat re-stamp this node's health back to Healthy?
///
/// The heartbeat handler raft-writes `UpdateNodeStatus(healthy)` every
/// cycle, but the apply path only guards `Quarantined`. Without this gate,
/// an operator-set `Maintenance` (or scheduler-set `Draining`) status
/// self-clears within one heartbeat (≤5s), pulling the node back into
/// serving/placement before the drain completes.
///
/// `Maintenance`/`Draining`/`Quarantined` are STICKY — they clear only via
/// their own explicit paths (operator `set_maintenance(false)`, drain
/// completion, `UnquarantineNode`), never via a liveness heartbeat. Every
/// other state — the transient liveness states (Healthy/Suspect/Down) AND
/// the disk-derived states (Degraded/Failed) — stays heartbeat-clearable,
/// preserving the pre-CO-3-8 recovery path where a node that comes back
/// healthy re-stamps itself on its next heartbeat. The explicit false-list
/// (rather than an allow-list) keeps that recovery intact; a NEW sticky
/// state must be added to the false arm here.
fn heartbeat_may_mark_healthy(current: HealthStatus) -> bool {
    match current {
        HealthStatus::Maintenance | HealthStatus::Draining | HealthStatus::Quarantined => false,
        HealthStatus::Healthy
        | HealthStatus::Suspect
        | HealthStatus::Down
        | HealthStatus::Degraded
        | HealthStatus::Failed => true,
    }
}

/// Lease bounds for a distributed POSIX lock: at least 1 s, at most 5 min,
/// so a client cannot pin a byte range for an unbounded time.
fn clamp_posix_lock_lease_ms(lease_ms: u64) -> u64 {
    lease_ms.clamp(1_000, 300_000)
}

/// A lock counts for conflicts only until `expires_at_ms` (same predicate as
/// the state machine's expiry sweep: live iff `expires_at_ms > now`).
fn posix_lock_is_live(expires_at_ms: u64, now_ms: u64) -> bool {
    expires_at_ms > now_ms
}

/// Map the state machine's `RenameFile` rejection text to a typed gRPC status
/// so FUSE can answer ENOENT / ENOTEMPTY / EINVAL instead of EIO. `None` =
/// unrecognised message (kept as the legacy `success:false` response).
fn rename_error_status(message: &str) -> Option<Status> {
    if message.contains("not found") {
        Some(Status::not_found(message))
    } else if message.contains("non-empty directory") {
        Some(Status::failed_precondition(message))
    } else if message.contains("its own subtree") {
        Some(Status::invalid_argument(message))
    } else {
        None
    }
}

/// Chunk size for a new file: the volume's own `chunk_size_bytes` when set
/// (> 0), else the `[defaults] chunk_size_mb` fallback.
fn resolve_create_chunk_size(volume_chunk_size_bytes: Option<u64>, default_mb: u32) -> u64 {
    match volume_chunk_size_bytes {
        Some(b) if b > 0 => b,
        _ => default_mb as u64 * 1024 * 1024,
    }
}

/// Skip the heartbeat `UpdateNodeStatus(healthy)` Raft write (one every 5 s
/// per node) when the node is already Healthy: the apply is a no-op.
fn heartbeat_should_write_healthy(current: HealthStatus) -> bool {
    current != HealthStatus::Healthy && heartbeat_may_mark_healthy(current)
}

/// `[scrub] quarantine_on_fault` gate for the heartbeat integrity-fault
/// batch's `QuarantineNode` write. `RemoveCorruptChunkReplicas` is
/// unconditional: the corrupt replica must be de-located either way.
fn heartbeat_quarantine_write_enabled(quarantine_on_fault: bool) -> bool {
    quarantine_on_fault
}

/// REV-3-8 — hard cap on how many integrity faults ONE heartbeat may
/// carry into the manager's fault-processing path.
///
/// The data node's fault collector holds up to 1024 entries and ships
/// them all on a single 5s heartbeat. Pre-fix the handler issued three
/// serially-awaited Raft writes PER FAULT (audit + QuarantineNode +
/// a single-id `RemoveCorruptChunkReplicas`, the last of which sweeps
/// the whole namespace), i.e. ~3072 round-trips each taking the
/// state-machine write lock while the collector refilled — self-
/// sustaining, and it stalled every other metadata operation.
///
/// 128 distinct chunks is far above what any healthy node reports in one
/// heartbeat window, and it bounds the single batched removal's Raft
/// payload. Nothing is lost past the cap: the node is quarantined off
/// every serving path on the FIRST fault, and the remaining faults
/// arrive on a later heartbeat (the data-side collector keeps them) or
/// are found by scrub.
pub(crate) const MAX_INTEGRITY_FAULTS_PER_HEARTBEAT: usize = 128;

/// REV-3-8 — the deduped, capped result of one heartbeat's integrity
/// faults. Shaped so the handler performs exactly ONE summary audit row,
/// ONE `QuarantineNode` and ONE batched
/// `RemoveCorruptChunkReplicas { chunk_hashes }` per heartbeat — the same
/// batched shape `report_chunk_status` already uses for the Bug O scrub
/// path, so no `dtfs-raft` change is needed.
pub(crate) struct IntegrityFaultBatch<'a> {
    /// Distinct chunk ids, first-seen order, at most `cap` entries.
    pub chunk_ids: Vec<String>,
    /// First fault in the report — drives the QuarantineInfo + audit row.
    pub representative: Option<&'a IntegrityFault>,
    /// Faults examined before the cap stopped the scan.
    pub scanned: usize,
    /// Faults left unexamined because of the cap.
    pub dropped: usize,
}

/// REV-3-8 — dedupe `faults` by chunk_id and stop once `cap` distinct
/// ids have been collected. Pure, so it is unit-tested directly.
pub(crate) fn batch_integrity_faults(
    faults: &[IntegrityFault],
    cap: usize,
) -> IntegrityFaultBatch<'_> {
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut chunk_ids: Vec<String> = Vec::new();
    let mut scanned = 0usize;
    for f in faults {
        if chunk_ids.len() >= cap {
            break;
        }
        scanned += 1;
        if f.chunk_id.is_empty() {
            continue;
        }
        if seen.insert(f.chunk_id.as_str()) {
            chunk_ids.push(f.chunk_id.clone());
        }
    }
    IntegrityFaultBatch {
        chunk_ids,
        representative: faults.first(),
        scanned,
        dropped: faults.len().saturating_sub(scanned),
    }
}

/// REV-4-2 — upper bound on `AllocateChunksRequest.num_chunks`.
///
/// `num_chunks` is peer-controlled: pre-fix the handler fed it straight
/// into `place_chunks_with_anchor` (one placement allocated per chunk)
/// and into two `0..num_chunks`-shaped loops, all under the
/// state-machine READ lock — so one malformed request with
/// `num_chunks = u32::MAX` allocated unbounded memory and blocked every
/// writer for the duration.
///
/// The bound is derived from the workload, not picked for looks. DTFS
/// serves one household Plex library; the largest single file it can
/// contain is a full 4K UHD Blu-ray remux, which tops out near 100 GB.
/// The default chunk size is 4 MiB (`DefaultsConfig::chunk_size_mb` = 4),
/// so 65536 chunks x 4 MiB = 256 GiB in ONE allocate call — ~2.5x the
/// largest file the workload implies, while capping the handler's work
/// at a few MB of placements. A file larger than that is still writable:
/// the client splits it across several allocate_chunks calls.
pub(crate) const MAX_CHUNKS_PER_ALLOCATION: u32 = 65_536;

/// REV-4-2 — validate the untrusted chunk count at the API boundary,
/// BEFORE any allocation or lock acquisition. Pure, so it is unit-tested
/// directly.
pub(crate) fn validate_num_chunks(num_chunks: u32, max: u32) -> Result<(), String> {
    if num_chunks > max {
        return Err(format!(
            "num_chunks {num_chunks} exceeds the per-request maximum {max} \
             (256 GiB at the 4 MiB default chunk size); split the file across \
             several allocate_chunks calls"
        ));
    }
    Ok(())
}

/// Bug BL / CO-3-9 — resolve the gRPC address of the current Raft leader.
///
/// Prefers the live Raft membership (`StateMachineData::nodes`, keyed by
/// the openraft node id): a CF-2-promoted leader was added online and is
/// NOT in the bootstrap-only `cluster.initial_voters`, so resolving from
/// voters alone returns `None` and heartbeat-originated writes on that
/// follower silently drop. The configured voter set is the fallback (an
/// early-boot leader may not yet be registered in the SM, and its node id
/// is derived from the hostname via `node_id_from_hostname`).
fn resolve_leader_addr(
    voters: &[dtfs_common::config::InitialVoter],
    membership_nodes: &std::collections::BTreeMap<u64, RaftNodeInfo>,
    leader_id: u64,
) -> Option<String> {
    membership_nodes
        .get(&leader_id)
        .map(|n| n.grpc_addr.clone())
        .or_else(|| {
            voters
                .iter()
                .find(|v| dtfs_common::node_id_from_hostname(&v.hostname) == leader_id)
                .map(|v| v.addr.clone())
        })
}

/// Bug CH — classify a raft `client_write` error as openraft's
/// ForwardToLeader rejection (a follower received the write). Shared by
/// the Bug BL heartbeat path and the health monitor's status commits.
pub(crate) fn is_forward_to_leader_err(msg: &str) -> bool {
    msg.contains("forward request to") || msg.contains("ForwardToLeader")
}

/// MGR-HB-1 — map the outcome of a heartbeat liveness write
/// (`UpdateNodeUsage` / `UpdateNodeStatus(healthy)`) onto the gRPC status
/// the data node sees. Transport/forward failures pass through unchanged
/// (they are already `unavailable`); an apply-level "node not found"
/// becomes `NOT_FOUND` so the data node re-registers instead of
/// heartbeating an id the state machine no longer knows; any other apply
/// error is `internal`. Pure so it is unit-testable.
pub(crate) fn liveness_write_status(
    res: Result<ClientResponse, Status>,
) -> Result<(), Status> {
    match res {
        Ok(ClientResponse::Error { message }) if message.contains("node not found") => {
            Err(Status::not_found(message))
        }
        Ok(ClientResponse::Error { message }) => Err(Status::internal(message)),
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}

/// CO-2-2 — does a forwarded write carry an acceptable shared secret?
///
/// `configured` is the leader's `config.cluster.shared_secret`. When it is
/// empty the cluster runs without a shared secret (back-compat for existing
/// plaintext clusters) and ANY forward is accepted. Otherwise the presented
/// secret must match in CONSTANT TIME — never `==`, which short-circuits on
/// the first mismatching byte and leaks the matching-prefix length via
/// timing. Reuses the audited `dtfs_common::crypto::constant_time_eq` (also
/// used for the join-handshake HMAC) rather than a second hand-rolled copy.
/// MGR-11 — a follower's `ForwardClientWrite` may only carry the
/// `ClientRequest` variants a legitimate background path (heartbeat
/// forward, health monitor, tiering read-report) actually proposes today.
/// Built from `grep -n "raft_write_or_forward\|proxy_to_leader\|write_via_leader"`
/// over metadata.rs / monitor.rs: `UpdateNodeUsage`, `UpdateNodeDiskUsage`,
/// `UpdateNodeStatus` (heartbeat + health monitor), `AppendAudit`,
/// `QuarantineNode`, `RemoveCorruptChunkReplicas`, `ReportScrubResult`,
/// `ReportChunkReads`. Pure so it is unit-testable without a live service.
fn forwardable(req: &ClientRequest) -> bool {
    matches!(
        req,
        ClientRequest::UpdateNodeUsage { .. }
            | ClientRequest::UpdateNodeDiskUsage { .. }
            | ClientRequest::UpdateNodeStatus { .. }
            | ClientRequest::AppendAudit { .. }
            | ClientRequest::QuarantineNode { .. }
            | ClientRequest::RemoveCorruptChunkReplicas { .. }
            | ClientRequest::ReportScrubResult { .. }
            | ClientRequest::ReportChunkReads { .. }
    )
}

/// Short variant name for logging/auditing a denied forward without
/// dumping the (potentially large) request body.
fn client_request_variant_name(req: &ClientRequest) -> &'static str {
    match req {
        ClientRequest::UpdateNodeUsage { .. } => "UpdateNodeUsage",
        ClientRequest::UpdateNodeDiskUsage { .. } => "UpdateNodeDiskUsage",
        ClientRequest::UpdateNodeStatus { .. } => "UpdateNodeStatus",
        ClientRequest::AppendAudit { .. } => "AppendAudit",
        ClientRequest::QuarantineNode { .. } => "QuarantineNode",
        ClientRequest::RemoveCorruptChunkReplicas { .. } => "RemoveCorruptChunkReplicas",
        ClientRequest::ReportScrubResult { .. } => "ReportScrubResult",
        ClientRequest::ReportChunkReads { .. } => "ReportChunkReads",
        _ => "other",
    }
}

fn forward_secret_ok(configured: &str, presented: &str) -> bool {
    if configured.is_empty() {
        return true;
    }
    dtfs_common::crypto::constant_time_eq(configured.as_bytes(), presented.as_bytes())
}

/// LIS-56 — fail-closed auth for mutating ManagerService client RPCs when
/// `cluster.shared_secret` is configured. Empty secret = legacy plaintext
/// skip (same contract as LIS-19 / ForwardClientWrite). Uses the
/// `dtfs-mgr-rpc` MAC plane so data-plane stamps cannot be replayed here.
fn require_mgr_rpc_auth(
    secret: &str,
    method: &str,
    binding: &str,
    metadata: &tonic::metadata::MetadataMap,
) -> Result<(), Status> {
    use dtfs_common::crypto::{
        verify_mgr_rpc_auth, DATA_RPC_AUTH_MAC, DATA_RPC_AUTH_TS,
    };
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
    if !verify_mgr_rpc_auth(secret, method, ts, mac, binding, now) {
        tracing::warn!(method, "ManagerService mutation rejected: invalid RPC auth (LIS-56)");
        return Err(Status::permission_denied(
            "invalid manager metadata RPC auth (LIS-56)",
        ));
    }
    Ok(())
}

/// Bug CH — commit a `ClientRequest` from ANY manager: local
/// `client_write` when this node is the leader, `ForwardClientWrite`
/// proxy to the leader otherwise.
///
/// Exists for the health monitor: data nodes heartbeat ONE arbitrary
/// manager, so down-detection happens wherever the heartbeats land —
/// pre-CH its `UpdateNodeStatus(down)` was a bare `client_write` that
/// silently errored on a follower, and a node loss observed by a
/// non-leader never committed (release case 55 caught this: the victim
/// never showed Down when the heartbeat receiver wasn't the leader).
/// Uncached connection per call — callers are low-rate (status
/// transitions, 10s monitor tick at worst). The gRPC heartbeat path
/// keeps its cached variant (`raft_write_or_forward`).
pub(crate) async fn write_via_leader(
    raft: &DtfsRaft,
    voters: &[dtfs_common::config::InitialVoter],
    // MGR-MON-3 — live raft membership (the monitor's SM snapshot) so a
    // CF-2-promoted leader absent from `initial_voters` still resolves.
    membership_nodes: &std::collections::BTreeMap<u64, RaftNodeInfo>,
    tls: &dtfs_common::tls::TlsContext,
    // CO-2-2 — cluster shared secret stamped onto the forward so the
    // leader authenticates it (see `forward_secret_ok`).
    shared_secret: &str,
    // CR2-8-1 (CMN-3) — size this forward client the same as the
    // listener it calls into, rather than tonic's 4 MB default.
    max_message_size_mb: u32,
    req: ClientRequest,
) -> Result<(), String> {
    use openraft::async_runtime::watch::WatchReceiver;
    match raft.client_write(req.clone()).await {
        Ok(_) => Ok(()),
        Err(e) => {
            let msg = format!("{e}");
            if !is_forward_to_leader_err(&msg) {
                return Err(msg);
            }
            let leader_id = raft
                .metrics()
                .borrow_watched()
                .current_leader
                .ok_or_else(|| "no leader to forward to".to_string())?;
            // CO-3-9 / MGR-MON-3 — membership first (covers a leader
            // promoted after bootstrap), `initial_voters` as the fallback.
            let leader_addr = resolve_leader_addr(voters, membership_nodes, leader_id)
                .ok_or_else(|| {
                    format!("leader {leader_id} not in raft membership or cluster.initial_voters")
                })?;
            let max_bytes = dtfs_common::config::max_message_bytes(max_message_size_mb);
            let mut client = tls
                .connect(&leader_addr)
                .await
                .map(|ch| {
                    ManagerServiceClient::new(ch)
                        .max_decoding_message_size(max_bytes)
                        .max_encoding_message_size(max_bytes)
                })
                .map_err(|e| format!("connect leader {leader_addr}: {e}"))?;
            let body =
                serde_json::to_vec(&req).map_err(|e| format!("serialize ClientRequest: {e}"))?;
            let resp = client
                .forward_client_write(ForwardWriteRequest {
                    client_request_json: body,
                    shared_secret: shared_secret.to_string(),
                })
                .await
                .map_err(|e| format!("forward to leader {leader_addr}: {e}"))?;
            forward_result(&resp.into_inner())
        }
    }
}

/// MGR-12 — a `ForwardClientWrite` that transports fine but is REJECTED by
/// the leader (bad shared secret, MGR-11 variant not allow-listed, or the
/// underlying Raft apply itself failed) comes back as `Ok(Response)` at the
/// tonic layer with `ForwardWriteResponse.success == false`. Pre-fix,
/// `write_via_leader` only mapped the transport `Result`, so a rejected
/// forward was silently treated as success — the caller (health monitor,
/// heartbeat forward) believed the write landed when it never did. Pure so
/// it is unit-testable against the response struct directly.
fn forward_result(resp: &ForwardWriteResponse) -> Result<(), String> {
    if resp.success {
        Ok(())
    } else {
        Err(resp.error.clone())
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

/// U4: resolve a chunk replica's storage tier (lowercase string) from the
/// disk it lives on. The chunk entry stores `node_id` (numeric in current
/// builds) + `disk_id`; the disk's `tier` comes from the node registry. Used
/// to populate `ChunkLocationInfo.node_tiers` so the read client can prefer
/// warm over the cold anchor HDD on a distance tie. Returns `""` when the
/// node/disk is not (yet) known, which the client treats as "no tie-break".
/// REV-2-1 — an empty `ChunkLocationInfo` for one chunk index.
///
/// The `chunk_index` label is what lets the client resolve a chunk by index
/// instead of by array position: the manager emits only the BTreeMap values, so
/// the array is dense even when the file's index space is not, and a positional
/// read served a different chunk's bytes at a valid offset.
fn new_chunk_location(chunk_hash: &str, size: u64, chunk_index: u64) -> ChunkLocationInfo {
    ChunkLocationInfo {
        chunk_id: chunk_hash.to_string(),
        size,
        node_addresses: Vec::new(),
        node_tiers: Vec::new(),
        node_locations: Vec::new(),
        chunk_index,
    }
}

/// MGR-LOC-1 — is this chunk row servable right now? Same test as the
/// scheduler's `live_replica_tier`: node registered and not Down/Failed,
/// disk present and not Failed/Removed. A legacy row with the `u32::MAX`
/// disk sentinel is judged on the node alone.
pub(crate) fn replica_is_live(sm: &StateMachineData, c: &ChunkLocationEntry) -> bool {
    let Ok(id) = c.node_id.parse::<u64>() else {
        return true; // pre-numeric row: nothing to judge against
    };
    let Some(n) = sm.nodes.get(&id) else {
        return false;
    };
    if matches!(
        n.health,
        dtfs_common::types::HealthStatus::Down | dtfs_common::types::HealthStatus::Failed
    ) {
        return false;
    }
    if c.disk_id == dtfs_raft::legacy_disk_id() {
        return true;
    }
    match n.disks.iter().find(|d| d.disk_id == c.disk_id) {
        Some(d) => !matches!(
            d.status,
            dtfs_common::types::DiskStatus::Failed | dtfs_common::types::DiskStatus::Removed
        ),
        None => false,
    }
}

fn replica_tier_string(sm: &StateMachineData, node_id: &str, disk_id: u32) -> String {
    let Ok(id) = node_id.parse::<u64>() else {
        return String::new();
    };
    sm.nodes
        .get(&id)
        .and_then(|n| n.disks.iter().find(|d| d.disk_id == disk_id))
        .map(|d| d.tier.to_string())
        .unwrap_or_default()
}

/// CO-3-10: resolve a chunk replica's topology `Location` (proto form) from
/// the node it lives on, so `GetFileLocations` can carry it positionally
/// alongside `node_addresses`. The FUSE client loads these into
/// `known_locations` to rank read replicas by distance + operator intent.
/// Returns `None` when the node is not (yet) known, which the client treats
/// as an unknown location (arbitrary/pure-distance order).
fn replica_location_proto(
    sm: &StateMachineData,
    node_id: &str,
) -> Option<dtfs_proto::dtfs::common::Location> {
    let id = node_id.parse::<u64>().ok()?;
    let n = sm.nodes.get(&id)?;
    Some(dtfs_proto::dtfs::common::Location {
        region: n.location.region.clone(),
        datacenter: n.location.datacenter.clone(),
        rack: n.location.rack.clone(),
        host: n.location.host.clone(),
    })
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

/// CR-3-3/MGR-1 — resolve the real `file_path` + the replica's real,
/// LIVE tier on `node_id` for a single `ReportChunkReads` entry, so the
/// tiering intercept stops feeding the engine a synthetic `""` /
/// `DiskTier::Hot` profile. Returns `None` when the chunk references no
/// known file (already deleted/GC'd, or never existed) OR when the
/// reporting node's replica cannot be resolved as LIVE via
/// `scheduler::live_replica_tier` (unknown disk, or the node/disk is
/// Down/Failed/Removed) — R2/MGR-1 review: falling back to a fake
/// `DiskTier::Hot` here reintroduces exactly the fabricated-tier bug
/// this task set out to fix (`evaluate` would plan a bogus Hot->Warm
/// "demote" for an unresolvable replica). The caller skips the report
/// entirely on `None` rather than fabricate data.
///
/// This single-chunk helper stays for direct unit testing; the real
/// `ReportChunkReads` intercept uses the batch
/// `build_chunk_read_context_index` below (R1/MGR-3) so a heartbeat with
/// N entries costs one pass over `sm.files`, not N.
fn resolve_chunk_read_context(
    sm: &StateMachineData,
    chunk_id: &str,
    node_id: u64,
) -> Option<(String, DiskTier)> {
    let file_path = crate::scheduler::files_referencing_chunk(&sm.files, chunk_id)
        .into_iter()
        .next()?;
    let node_id_str = node_id.to_string();
    let disk_id = sm.files.get(&file_path).and_then(|fm| {
        fm.chunks
            .iter()
            .find(|c| c.chunk_hash == chunk_id && c.node_id == node_id_str)
            .map(|c| c.disk_id)
    });
    let tier = disk_id.and_then(|d| crate::scheduler::live_replica_tier(sm.nodes.get(&node_id), d))?;
    Some((file_path, tier))
}

/// R1/MGR-3 — build the `(file_path, live_tier)` lookup for exactly the
/// chunk ids in `wanted` (one `ReportChunkReads` batch) with a SINGLE
/// pass over `sm.files`, instead of one `files_referencing_chunk`
/// O(files x chunks) scan per reported chunk. Pre-fix, the intercept
/// held the SM read lock across every `tiering.report_access(...).await`
/// in the per-entry loop (each of which takes tiering's own
/// `access_stats` write lock) AND re-scanned the whole file tree per
/// entry — O(N x total_chunks) work under the SM read lock for an
/// N-entry heartbeat, blocking the (fair) Raft apply writer for the
/// duration (the MGR-3 anti-pattern).
///
/// For each wanted `chunk_hash`, records the FIRST referencing file's
/// path (by `sm.files`' BTreeMap/lexicographic order, matching
/// `files_referencing_chunk`'s "first match" semantics) and, within
/// THAT SAME file's rows, the live tier of the row whose `node_id`
/// matches the reporting node (`None` when no such row exists or its
/// replica cannot be resolved as live — R2: never a fabricated tier).
/// A `None` tier entry is still returned (with the resolved path) so
/// the caller can log the skip; it must NOT be fed to `report_access`.
pub(crate) fn build_chunk_read_context_index(
    files: &std::collections::BTreeMap<String, dtfs_raft::state_machine::FileMetadata>,
    nodes: &std::collections::BTreeMap<u64, dtfs_raft::NodeInfo>,
    wanted: &std::collections::HashSet<String>,
    node_id: u64,
) -> std::collections::HashMap<String, (String, Option<DiskTier>)> {
    use std::collections::hash_map::Entry;
    let node_id_str = node_id.to_string();
    let mut out: std::collections::HashMap<String, (String, Option<DiskTier>)> = std::collections::HashMap::new();
    for (path, fm) in files.iter() {
        for c in &fm.chunks {
            if !wanted.contains(&c.chunk_hash) {
                continue;
            }
            match out.entry(c.chunk_hash.clone()) {
                Entry::Occupied(mut o) => {
                    // Path already fixed by the first referencing file.
                    // Only fill in the tier if it's still unresolved AND
                    // this row belongs to that SAME file AND matches the
                    // reporting node — a later file's rows never
                    // override the first file's path (matches
                    // files_referencing_chunk's "first match" pick).
                    if o.get().1.is_none() && o.get().0 == *path && c.node_id == node_id_str {
                        o.get_mut().1 = crate::scheduler::live_replica_tier(nodes.get(&node_id), c.disk_id);
                    }
                }
                Entry::Vacant(v) => {
                    let tier = if c.node_id == node_id_str {
                        crate::scheduler::live_replica_tier(nodes.get(&node_id), c.disk_id)
                    } else {
                        None
                    };
                    v.insert((path.clone(), tier));
                }
            }
        }
    }
    out
}

/// CO-4-13 — resolve the CF-3b ack floor for a `commit_chunk_locations`
/// call as the STRICTER of two volume floors:
///   * the floor of the volume the file is actually registered under
///     (`sm.files[path].volume_id`), and
///   * the floor of the volume named in the request (`req_volume_id`).
///
/// Resolving from the request volume alone (the pre-fix behaviour) let a
/// wrong/unknown `req.volume_id` fall back to the manager default and
/// silently bypass a stricter per-file floor — a CF-3b write-safety
/// downgrade. An unregistered volume id contributes the `default_floor`,
/// so an unknown id can only raise the floor, never lower it below the
/// file's own.
fn resolve_ack_floor(
    sm: &StateMachineData,
    path: &str,
    req_volume_id: &str,
    default_floor: u32,
) -> u32 {
    let floor_of = |vol_id: &str| -> u32 {
        sm.volumes
            .get(vol_id)
            .map(|v| v.write_safety.min_replicas_before_ack)
            .unwrap_or(default_floor)
    };
    let request_floor = floor_of(req_volume_id);
    // The file may not exist yet on first commit; then only the request
    // volume governs.
    match sm.files.get(path) {
        Some(file) => request_floor.max(floor_of(&file.volume_id)),
        None => request_floor,
    }
}

/// CR-6-4 (FUSE-6) — the volume that governs placement for
/// `AllocateChunks`: the file's OWN registered volume when it already
/// exists (a client can send a stale/wrong `req.volume_id`, e.g. it
/// never learned the real one, or defaulted to "default"), otherwise
/// the request's volume (first write, file doesn't exist yet).
fn resolve_placement_volume_id(existing_file_volume_id: Option<&str>, req_volume_id: &str) -> String {
    existing_file_volume_id
        .map(|v| v.to_string())
        .unwrap_or_else(|| req_volume_id.to_string())
}

/// CR-6-2 (CMN-2) — warn-once-per-volume gate for the ack-floor clamp
/// below, so a volume whose floor exceeds its RF doesn't spam a warning
/// on every commit.
static WARNED_FLOOR_CLAMPED: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashSet<String>>,
> = std::sync::OnceLock::new();

/// CR-6-2 (CMN-2) — the volume's `write_safety.min_replicas_before_ack`
/// floor (e.g. the compiled-in default of 2) can exceed the effective
/// mandatory RF of the tier the chunks actually landed on (e.g. cold
/// RF 1), which made `commit_chunk_locations` reject every write to
/// that tier forever (CF-3b never re-checks, so the write buffer just
/// keeps retrying and failing). Clamp the floor to the MAXIMUM
/// `effective_mandatory_replicas().for_tier()` across the distinct
/// tiers the committed chunks resolved to — R1 review fix: taking the
/// minimum let a multi-tier placement (e.g. warm RF 3 + cold RF 1) with
/// one warm + one cold replica clamp the floor down to 1 and ack at 2
/// replicas, silently downgrading warm's own RF 3 requirement (exactly
/// the RF downgrade A.3.3 #5 forbids). The floor may only be lowered to
/// the HIGHEST RF any placed tier actually demands, never below it. A
/// chunk whose node/disk can't be resolved to a live tier (unknown
/// node, e.g. an old/rejoining data node) is skipped — it doesn't
/// weaken the clamp, since it isn't evidence of a real placement tier.
fn clamp_ack_floor_to_rf(
    floor: u32,
    sm: &StateMachineData,
    chunks: &[ChunkLocationEntry],
    volume_id: &str,
    path: &str,
    audit: Option<&crate::audit::AuditLog>,
) -> u32 {
    let volume = match sm.volumes.get(volume_id) {
        Some(v) => v,
        None => return floor,
    };
    let mandatory = volume.effective_mandatory_replicas();
    // R1 review fix — MAX, not min, across the distinct tiers the chunks
    // actually landed on. A multi-tier volume (e.g. warm RF 3 + cold RF 1)
    // with a chunk placed 1 warm + 1 cold must still enforce warm's RF 3;
    // taking the min silently downgraded the floor to 1 and acked the
    // write at 2 replicas, exactly the RF downgrade the floor exists to
    // prevent (A.3.3 #5). The floor may only be lowered to the HIGHEST RF
    // any placed tier actually requires — never below what any placed
    // tier itself demands.
    let max_rf = chunks
        .iter()
        .filter_map(|c| {
            let node_id: u64 = c.node_id.parse().ok()?;
            let tier = live_replica_tier(sm.nodes.get(&node_id), c.disk_id)?;
            // RF 0 = cache-only tier: not evidence of a durable placement,
            // and clamping to 0 would ack a single evictable copy.
            Some(mandatory.for_tier(tier)).filter(|rf| *rf > 0)
        })
        .max();
    let Some(rf) = max_rf else {
        return floor;
    };
    if rf >= floor {
        return floor;
    }
    let first_time = {
        let set =
            WARNED_FLOOR_CLAMPED.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()));
        set.lock().unwrap().insert(volume_id.to_string())
    };
    if first_time {
        tracing::warn!(
            volume_id,
            configured_floor = floor,
            effective_rf = rf,
            "CR-6-2 (CMN-2): write_safety.min_replicas_before_ack exceeds the \
             effective mandatory RF of a placed tier; clamping the ack floor \
             to the RF so writes to that tier can commit"
        );
    }
    if let Some(audit) = audit {
        audit.log(
            "write_safety",
            "write_safety.floor_clamped",
            &format!(
                "path={} volume={} configured_floor={} effective_rf={}",
                path, volume_id, floor, rf
            ),
            "clamped",
        );
    }
    rf
}

#[tonic::async_trait]
impl ManagerService for MetadataService {
    async fn create_file(
        &self,
        request: Request<CreateFileRequest>,
    ) -> Result<Response<CreateFileResponse>, Status> {
        let binding = request.get_ref().path.clone();
        require_mgr_rpc_auth(
            &self.config.cluster.shared_secret,
            "CreateFile",
            &binding,
            request.metadata(),
        )?;
        let req = request.into_inner();
        info!("CreateFile: {}", req.path);

        let volume_id = req
            .volume_id
            .clone()
            .unwrap_or_else(|| "default".to_string());

        let chunk_size = {
            let sm = self.sm_data.read().await;
            resolve_create_chunk_size(
                sm.volumes.get(&volume_id).map(|v| v.chunk_size_bytes),
                self.config.defaults.chunk_size_mb,
            )
        };

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
        // MGR-LOC-1 — hand the client only replicas it can actually read:
        // a row whose node is Down/Failed or whose disk is Failed/Removed
        // (`live_replica_tier`, the scheduler's own definition) is dropped.
        // Pre-fix every row was returned and the FUSE client walked the
        // dead ones with a full `chunk_read_timeout_secs` (60 s) each
        // before reaching a live copy. If NO row of a chunk is live the
        // dead rows are kept (a stale health mark must not turn a readable
        // chunk into ENOENT), so the change only ever prunes, never empties.
        let mut by_chunk: BTreeMap<u64, ChunkLocationInfo> = BTreeMap::new();
        let mut live_by_index: std::collections::HashMap<u64, bool> =
            std::collections::HashMap::new();
        for c in &file.chunks {
            let live = replica_is_live(&data, c);
            live_by_index
                .entry(c.chunk_index)
                .and_modify(|l| *l |= live)
                .or_insert(live);
        }
        for c in &file.chunks {
            if live_by_index.get(&c.chunk_index).copied().unwrap_or(false)
                && !replica_is_live(&data, c)
            {
                continue;
            }
            let addr = resolve_node_contact(&data, &c.node_id);
            // U4: derive this replica's storage tier from the disk it lives
            // on (node.disks[disk_id].tier), positionally aligned with
            // node_addresses so the client can use it as a read tie-break.
            let tier = replica_tier_string(&data, &c.node_id, c.disk_id);
            // CO-3-10: the replica's registered Location, positionally
            // aligned with node_addresses so the FUSE client can load it
            // into known_locations for distance/priority read ranking. An
            // unknown node yields a default (all-empty) Location, which the
            // client skips (unknown -> arbitrary/pure-distance order).
            let location = replica_location_proto(&data, &c.node_id).unwrap_or_default();
            let e = by_chunk.entry(c.chunk_index).or_insert_with(|| {
                // REV-2-1 — built by a free fn rather than an inline struct
                // literal: the literal form ICEs rustc 1.94 while type-checking
                // this function (same compiler as the dead-code ICE workaround
                // noted in CLAUDE.md A.4).
                new_chunk_location(&c.chunk_hash, c.size_bytes, c.chunk_index)
            });
            e.size = e.size.max(c.size_bytes);
            if !e.node_addresses.contains(&addr) {
                e.node_addresses.push(addr);
                e.node_tiers.push(tier);
                e.node_locations.push(location);
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
            // CR-6-4 (FUSE-6) — the file's ACTUAL volume, so the client
            // stops hardcoding "default" on every refresh.
            volume_id: file.volume_id.clone(),
            is_symlink: file.symlink_target.is_some(),
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

        // LIS-20 / Sec R3 — never silently admit when shared_secret is empty.
        // Empty secret is only accepted when allow_insecure_empty_secret was
        // set at config load (lab escape hatch); otherwise validate_shared_secret
        // already refused startup.
        if !self
            .config
            .cluster
            .accepts_shared_secret(req.shared_secret.as_deref())
        {
            return Ok(Response::new(RegisterNodeResponse {
                accepted: false,
                node_id: String::new(),
                config_version: 0,
            }));
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
        let binding = request.get_ref().node_id.clone();
        require_mgr_rpc_auth(
            &self.config.cluster.shared_secret,
            "Heartbeat",
            &binding,
            request.metadata(),
        )?;
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
        let scrub_cfg = &self.config.scrub;
        let (cfg_version, scrub_override, pending_scrub) = {
            let sm = self.sm_data.read().await;
            let cfg_version = sm.cluster_config.version;
            let effective_quick_secs = scrub_cfg.quick_interval_secs.max(60);
            let effective_deep_secs = scrub_cfg.deep_interval_secs.max(60);
            let quick_h = (effective_quick_secs + 3599) / 3600;
            let deep_h = (effective_deep_secs + 3599) / 3600;
            // CR-6-3 (PROTO-2) — warn once at startup that a sub-hour
            // quick/deep cadence needs a data node on this build or newer
            // to actually honour the sub-hour value; older data nodes
            // silently round up to the nearest hour via the legacy
            // *_hours fields (still filled below, unchanged).
            if (quick_h * 3600 != effective_quick_secs || deep_h * 3600 != effective_deep_secs)
                && !WARNED_SUBHOUR_SCRUB_CADENCE.swap(true, std::sync::atomic::Ordering::Relaxed)
            {
                tracing::info!(
                    quick_interval_secs = effective_quick_secs,
                    deep_interval_secs = effective_deep_secs,
                    "CR-6-3 (PROTO-2): sub-hour scrub cadence configured; \
                     data nodes on this build or newer honour the exact \
                     secs value, older data nodes round up to the nearest hour"
                );
            }
            (
                cfg_version,
                Some(dtfs_proto::dtfs::manager::ScrubConfigOverride {
                    quick_interval_hours:   quick_h.max(1),
                    deep_interval_hours:    deep_h.max(1),
                    quick_interval_secs:    effective_quick_secs,
                    deep_interval_secs:     effective_deep_secs,
                    orphan_interval_secs:   scrub_cfg.orphan_interval_secs,
                    orphan_grace_secs:      scrub_cfg.orphan_grace_secs,
                    max_io_percent:         scrub_cfg.max_io_percent,
                    verify_on_read:         scrub_cfg.verify_on_read,
                    quarantine_on_fault:    scrub_cfg.quarantine_on_fault,
                    auto_unquarantine_secs: scrub_cfg.auto_unquarantine_secs,
                }),
                // CF-8 — relay the latest manual scrub trigger on every
                // heartbeat (works from any manager — replicated state).
                // The data node dedupes by id, so repetition is free.
                sm.pending_scrub.clone(),
            )
        };

        // MGR-HB-1 — the liveness write is the whole point of a heartbeat:
        // `last_seen_ms` only advances through this commit, and the leader's
        // monitor judges Down from it. Pre-fix this was `let _ =`, so a
        // data node heartbeating a follower/learner that could not reach the
        // leader (cross-DC learner on a flaky link) saw `Ok` forever while
        // the leader marked it Down after 30 s; the node never counted a
        // failure and never walked its manager list. Surface the failure as
        // a gRPC error so the data node's failure counter trips and it
        // reconnects elsewhere; a "node not found" apply error becomes
        // NOT_FOUND so the data node re-registers.
        liveness_write_status(
            self.raft_write_or_forward(ClientRequest::UpdateNodeUsage {
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
            .await,
        )?;

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
        // CO-3-8 — surface the node's current health (not just the
        // quarantine flag) so the heartbeat "healthy" write below can be
        // suppressed for the sticky operator/drain states as well. An
        // unregistered node defaults to Healthy (heartbeat may proceed).
        let (is_quarantined, current_health) = {
            let sm = self.sm_data.read().await;
            match sm.nodes.get(&node_id) {
                Some(n) => (n.quarantine.is_some(), n.health),
                None => (false, HealthStatus::Healthy),
            }
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
        // CO-3-8 — only re-stamp Healthy for the transient liveness states.
        // Maintenance/Draining (and Quarantined) are sticky: a heartbeat
        // must not clear an operator drain within one cycle.
        if heartbeat_should_write_healthy(current_health) {
            // MGR-HB-1 — same contract as the UpdateNodeUsage write above:
            // a dropped `healthy` commit leaves a recovered node Down.
            liveness_write_status(
                self.raft_write_or_forward(ClientRequest::UpdateNodeStatus {
                    node_id,
                    status: "healthy".to_string(),
                })
                .await,
            )?;
        }

        // Bug BF-6 / REV-3-8 — process the integrity faults the data node
        // reported since its last heartbeat as ONE batch:
        //   1. one `read.integrity_fault` audit summary row,
        //   2. one QuarantineNode (the apply path accumulates
        //      fault_count_total),
        //   3. one BATCHED RemoveCorruptChunkReplicas so the under-
        //      replication scheduler re-replicates every reported chunk
        //      from a healthy node immediately.
        // Pre-REV-3-8 this was three serially-awaited Raft writes PER
        // FAULT (the last of them a full-namespace sweep). A node with a
        // damaged `.meta` directory fills its collector (cap 1024) and
        // ships them all on one 5s heartbeat, so the manager took the
        // state-machine write lock ~3072 times while the collector
        // refilled — self-sustaining, and it stalled every other
        // metadata operation for the duration.
        // Older data nodes don't emit field 10; for them the vec is
        // empty + this block is a no-op (back-compat with pre-BF data).
        // CR-3-6/MGR-7 — surfaced to the caller as `HeartbeatResponse.
        // accepted`; the data node re-enqueues its drained faults / any
        // dropped ChunkStatusReport-equivalent state when this comes back
        // false (CO-3-7 parity).
        let mut accepted = true;
        if !req.integrity_faults.is_empty() {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let hostname = {
                let sm = self.sm_data.read().await;
                sm.nodes.get(&node_id).map(|n| n.hostname.clone()).unwrap_or_default()
            };
            let origin_manager = self.config.node.hostname.clone();
            // REV-3-8 — dedupe by chunk_id and cap the scan, then emit
            // exactly three Raft writes for the WHOLE heartbeat.
            let batch = batch_integrity_faults(
                &req.integrity_faults,
                MAX_INTEGRITY_FAULTS_PER_HEARTBEAT,
            );
            let rep = batch.representative;
            let rep_detector = rep.map(|f| f.detector.as_str()).unwrap_or("unknown");
            let rep_chunk = rep.map(|f| f.chunk_id.as_str()).unwrap_or("");
            let rep_expected = rep
                .map(|f| f.expected_hash.as_str())
                .filter(|h| !h.is_empty())
                .unwrap_or("?");
            let rep_actual = rep
                .map(|f| f.actual_hash.as_str())
                .filter(|h| !h.is_empty())
                .unwrap_or("?");
            let rep_observed = rep.map(|f| f.observed_at_ms).unwrap_or(now_ms);
            let reported = req.integrity_faults.len();
            let distinct = batch.chunk_ids.len();
            // Bug BO-4 (BC-12e remainder) — the data node's verify-on-read
            // mismatch surfaces only as a node.quarantine row today, so the
            // /api/v1/integrity/faults feed needs its own raw read event.
            // REV-3-8 makes it ONE summary row per heartbeat; the action
            // name is unchanged because `api.rs::action_is_integrity_fault`
            // matches on it.
            let _ = self
                .raft_write_or_forward(ClientRequest::AppendAudit {
                    actor: "system".to_string(),
                    action: "read.integrity_fault".to_string(),
                    target: format!("{node_id} ({hostname})"),
                    result: format!(
                        "detector={rep_detector} faults={reported} chunks={distinct} \
                         dropped={} chunk={rep_chunk} expected={rep_expected} \
                         actual={rep_actual}",
                        batch.dropped,
                    ),
                    origin_manager,
                    timestamp_ms: now_ms,
                })
                .await;
            let reason = match rep_detector {
                "verify_on_read" => dtfs_common::types::QuarantineReason::VerifyOnReadMismatch,
                "verify_on_read_meta_absent" => dtfs_common::types::QuarantineReason::VerifyOnReadMismatch,
                _ => dtfs_common::types::QuarantineReason::VerifyOnReadMismatch,
            };
            let info = dtfs_common::types::QuarantineInfo {
                since_ms: now_ms,
                reason,
                triggering_chunk_id: rep.map(|f| f.chunk_id.to_string()),
                fault_count_total: 0,  // apply path accumulates
                last_fault_at_ms: rep_observed,
                last_fault_summary: format!(
                    "{rep_detector}: expected={rep_expected} actual={rep_actual} \
                     (faults={reported} chunks={distinct})"
                ),
            };
            // CR-3-6/MGR-7 — capture failures from BOTH forwarded writes
            // and surface them as `accepted: false` via
            // `integrity_fault_batch_accepted` (mirrors the CO-3-7 fix
            // for `report_chunk_status`, which this heartbeat path
            // previously lacked). During an election (or any transient
            // forward failure) a `let _ =` here left the node neither
            // quarantined nor de-located, and the corrupt replica stayed
            // advertised healthy until the next scrub; the data node
            // already re-enqueues drained faults when the ack is
            // `accepted == false` (Bug BF-5, CO-3-7 parity).
            let quarantine_ok = if !heartbeat_quarantine_write_enabled(scrub_cfg.quarantine_on_fault) {
                tracing::warn!(
                    node_id,
                    "[scrub] quarantine_on_fault=false: integrity fault reported, node NOT quarantined"
                );
                true
            } else {
                match self
                    .raft_write_or_forward(ClientRequest::QuarantineNode { node_id, info })
                    .await
                {
                    Ok(_) => true,
                    Err(e) => {
                        tracing::warn!(error = %e, node_id, "CR-3-6: failed to commit QuarantineNode from heartbeat integrity fault");
                        false
                    }
                }
            };
            // Drop the (node, chunk) replica entries so the
            // under-replication scheduler immediately picks a healthy
            // source + re-replicates. RemoveCorruptChunkReplicas takes a
            // batch (the same shape `report_chunk_status` uses for the
            // Bug O scrub path), so ONE write covers every reported chunk.
            let remove_corrupt_ok = if batch.chunk_ids.is_empty() {
                true
            } else {
                match self
                    .raft_write_or_forward(ClientRequest::RemoveCorruptChunkReplicas { now_ms: stamp_now_ms(),
                        node_id: node_id.to_string(),
                        chunk_hashes: batch.chunk_ids,
                        // CO-1-1 — the heartbeat integrity-fault report
                        // (IntegrityFault) carries no disk_id, so keep the
                        // node-wide legacy sweep. Wiring per-disk corruption
                        // would need a proto change and is out of scope here.
                        disk_id: dtfs_raft::legacy_disk_id(),
                    })
                    .await
                {
                    Ok(_) => true,
                    Err(e) => {
                        tracing::warn!(error = %e, node_id, "CR-3-6: failed to commit RemoveCorruptChunkReplicas from heartbeat integrity fault");
                        false
                    }
                }
            };
            if !integrity_fault_batch_accepted(quarantine_ok, remove_corrupt_ok) {
                accepted = false;
            }
            tracing::warn!(
                node_id,
                count = reported,
                distinct_chunks = distinct,
                dropped = batch.dropped,
                "Bug BF-6/REV-3-8: auto-quarantined node after integrity faults reported via heartbeat"
            );
        }

        // Bug BY — record completed scrub results reported by the data node
        // so the WebUI "Last Scrub" card + stats.scrub_results_by_node
        // populate. Forwarded to the leader when this manager is a follower
        // (Bug BL), like the integrity-fault writes above.
        if !req.scrub_results.is_empty() {
            for s in &req.scrub_results {
                let _ = self
                    .raft_write_or_forward(ClientRequest::ReportScrubResult {
                        node_id,
                        disk_path: s.disk_path.clone(),
                        level: s.level.clone(),
                        completed_at_ms: s.completed_at_ms,
                        chunks_checked: s.chunks_checked,
                        chunks_healthy: s.chunks_healthy,
                        chunks_corrupt: s.chunks_corrupt,
                        chunks_missing: s.chunks_missing,
                        duration_secs: s.duration_secs,
                    })
                    .await;
            }
            tracing::debug!(
                node_id,
                count = req.scrub_results.len(),
                "Bug BY recorded scrub results reported via heartbeat"
            );
        }

        // P0-9: fan out per-chunk read counters into the tiering engine.
        // The TieringEngine aggregates by chunk_id and uses `reads_by_dc`
        // to score migration target nodes (find_target_node).
        if !req.chunk_reads.is_empty() {
            // Route per-chunk read counters to the LEADER's tiering engine. The
            // read-window and the promote/demote tick are in-memory on the
            // leader, but this heartbeat may have landed on a follower; calling
            // `report_access` on the local (follower) engine would hide the
            // reads from the leader's tick and read-hot chunks would never
            // promote. `proxy_to_leader` sends `ReportChunkReads` to the leader
            // via `ForwardClientWrite`, where the manager service intercepts it
            // before Raft and applies it to the tiering engine (the leader
            // self-forwards). Best-effort: a dropped report (e.g. mid-election)
            // just costs one read sample; the next heartbeat re-reports.
            let entries: Vec<(String, std::collections::HashMap<String, u32>)> = req
                .chunk_reads
                .iter()
                .map(|e| (e.chunk_id.clone(), e.reads_by_dc_1h.clone()))
                .collect();
            let _ = self
                .proxy_to_leader(ClientRequest::ReportChunkReads { node_id, entries })
                .await;
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

        // CF-8 — manual scrub trigger relay.
        let scrub_request = pending_scrub.map(|p| {
            dtfs_proto::dtfs::manager::ScrubRequestNow {
                id: p.id,
                level: p.level,
                requested_at_ms: p.requested_at_ms,
            }
        });

        Ok(Response::new(HeartbeatResponse {
            accepted,
            config_version: cfg_version,
            config_update,
            self_quarantine_cleared,
            scrub_request,
        }))
    }

    async fn report_chunk_status(
        &self,
        request: Request<ChunkStatusReport>,
    ) -> Result<Response<ChunkStatusAck>, Status> {
        let binding = request.get_ref().node_id.clone();
        require_mgr_rpc_auth(
            &self.config.cluster.shared_secret,
            "ReportChunkStatus",
            &binding,
            request.metadata(),
        )?;
        let req = request.into_inner();
        tracing::debug!("ChunkStatusReport: {} chunks", req.chunks.len());

        let node_id_u64: u64 = req
            .node_id
            .parse()
            .map_err(|_| Status::invalid_argument("node_id must be a u64 from registration"))?;

        // Bug O — partition the reported chunks into corrupt/missing
        // (which need replica entries dropped + re-replication queued)
        // vs healthy (which keep their entries).
        let corrupt_by_disk = group_corrupt_by_disk(&req.chunks);
        let corrupt_ids: Vec<&String> = corrupt_by_disk.values().flatten().collect();

        let status = if !corrupt_ids.is_empty() {
            "degraded"
        } else {
            "healthy"
        };
        // CO-3-7 — route through `raft_write_or_forward` (not the
        // ForwardToLeader-dropping `raft_write`). A corrupt/missing report
        // can land on ANY manager; a data node heartbeats one arbitrary
        // peer, so the receiver is usually a FOLLOWER. With a bare
        // `let _ = raft_write(...)` the follower silently dropped both the
        // status downgrade AND the replica de-location, so the replica was
        // never dropped and re-replication never queued — the corrupt copy
        // stayed advertised as healthy. We now forward to the leader and
        // surface failure as `accepted: false` so the data node retries.
        let mut accepted = true;
        if let Err(e) = self
            .raft_write_or_forward(ClientRequest::UpdateNodeStatus {
                node_id: node_id_u64,
                status: status.to_string(),
            })
            .await
        {
            tracing::warn!(error = %e, "CO-3-7 failed to commit node-status from chunk report");
            accepted = false;
        }

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
            // One write per disk: a report with a disk id drops only that
            // (node, disk) copy; legacy reports (no disk id) stay node-wide.
            for (disk_id, chunk_hashes) in corrupt_by_disk {
                if let Err(e) = self
                    .raft_write_or_forward(ClientRequest::RemoveCorruptChunkReplicas { now_ms: stamp_now_ms(),
                        node_id: req.node_id.clone(),
                        chunk_hashes,
                        disk_id,
                    })
                    .await
                {
                    tracing::warn!(error = %e, disk_id, "CO-3-7 failed to commit corrupt-replica removal");
                    accepted = false;
                }
            }
        }

        Ok(Response::new(ChunkStatusAck { accepted }))
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
        // CO-2-2 — authenticate the forwarding manager BEFORE applying any
        // write. Without this a follower (which accepts forwards from any
        // peer, including unauthenticated client writes like DeleteFile)
        // could be driven by anyone who can reach its gRPC port. The check
        // is skipped only when this leader has no configured shared secret
        // (back-compat for plaintext clusters); the compare is constant-time.
        if !forward_secret_ok(&self.config.cluster.shared_secret, &body.shared_secret) {
            tracing::warn!("ForwardClientWrite rejected: shared-secret mismatch");
            return Err(Status::permission_denied(
                "forwarded write shared-secret mismatch",
            ));
        }
        let req: ClientRequest = serde_json::from_slice(&body.client_request_json)
            .map_err(|e| Status::invalid_argument(format!("bad ClientRequest json: {e}")))?;
        // MGR-11 — CO-2-2 only authenticates WHO is forwarding, not WHAT is
        // forwarded: any ClientRequest variant round-tripped through this
        // RPC previously reached `self.raft.client_write` unchecked, so an
        // authenticated-but-compromised/buggy follower could propose any
        // mutation (e.g. UpdateConfig) as if it were a legitimate heartbeat/
        // monitor forward. Allow-list exactly the variants a follower
        // legitimately proposes via `raft_write_or_forward` / `proxy_to_leader`
        // / `write_via_leader` today; anything else is denied + audited.
        if !forwardable(&req) {
            tracing::warn!(
                variant = client_request_variant_name(&req),
                "ForwardClientWrite rejected: variant not forwardable (MGR-11)"
            );
            if let Some(audit) = &self.audit {
                audit.log(
                    "raft",
                    "auth.denied",
                    &format!("variant={}", client_request_variant_name(&req)),
                    "forward_client_write: variant not in forward allow-list",
                );
            }
            return Err(Status::permission_denied(
                "forwarded write variant not allowed (MGR-11)",
            ));
        }
        // Per-chunk read reports are NOT Raft state — they feed this (leader's)
        // in-memory tiering read-window. Intercept BEFORE Raft and apply
        // directly so reads forwarded from a follower reach the engine that
        // ticks. Without this, the leader's read-window only ever sees reads
        // from data nodes that happen to heartbeat to the leader, and read-hot
        // chunks served via other managers never promote.
        if let ClientRequest::ReportChunkReads { node_id, entries } = &req {
            if let Some(tiering) = &self.tiering {
                // CR-3-3/MGR-1 — resolve the REAL file_path + the
                // replica's REAL tier on `node_id` before feeding the
                // tiering engine. Pre-fix this always passed `""` /
                // `DiskTier::Hot`, which made `evaluate`'s CO-4-6
                // AV-volume skip a no-op (`path_to_volume.get("")` is
                // always `None`) and fed the legacy temperature path
                // fabricated data for every reported chunk.
                //
                // R1/MGR-3 review fix — build the whole batch's lookup
                // under ONE short SM read lock (one pass over
                // `sm.files`), then DROP the lock before awaiting
                // `report_access` (which takes tiering's own
                // `access_stats` write lock per call). Pre-fix this held
                // the SM read lock across every `report_access(...).await`
                // in the loop AND re-scanned the whole file tree per
                // entry via `resolve_chunk_read_context` — O(N x
                // total_chunks) work under the SM lock for an N-entry
                // heartbeat, blocking the Raft apply writer (tokio
                // RwLock is fair) for the duration.
                let wanted: std::collections::HashSet<String> =
                    entries.iter().map(|(id, _)| id.clone()).collect();
                let index = {
                    let sm = self.sm_data.read().await;
                    build_chunk_read_context_index(&sm.files, &sm.nodes, &wanted, *node_id)
                };
                for (chunk_id, reads_by_dc) in entries {
                    let total: u32 = reads_by_dc.values().sum();
                    match index.get(chunk_id) {
                        // Unknown chunk (already deleted/GC'd) — skip
                        // rather than feed the engine a synthetic
                        // profile for content that no longer exists.
                        None => {
                            tracing::debug!(chunk_id, node_id, "CR-3-3: unknown chunk in ReportChunkReads, skipping");
                            continue;
                        }
                        // R2/MGR-1 — the replica's tier could not be
                        // resolved as LIVE (unknown disk, or the node/
                        // disk is Down/Failed/Removed). NEVER fall back
                        // to a fake tier here — that is exactly the
                        // fabricated-Hot-tier bug this task fixed.
                        Some((_, None)) => {
                            tracing::debug!(chunk_id, node_id, "CR-3-3/R2: replica tier not live, skipping report_access");
                            continue;
                        }
                        Some((file_path, Some(tier))) => {
                            tiering
                                .report_access(
                                    chunk_id,
                                    file_path,
                                    total,
                                    total,
                                    reads_by_dc.clone(),
                                    *tier,
                                    *node_id,
                                )
                                .await;
                        }
                    }
                }
            }
            let response_json = serde_json::to_vec(&ClientResponse::Success)
                .map_err(|e| Status::internal(format!("serialize ClientResponse: {e}")))?;
            return Ok(Response::new(ForwardWriteResponse {
                success: true,
                response_json,
                error: String::new(),
            }));
        }
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
        let binding = request.get_ref().path.clone();
        require_mgr_rpc_auth(
            &self.config.cluster.shared_secret,
            "DeleteFile",
            &binding,
            request.metadata(),
        )?;
        let req = request.into_inner();
        info!("DeleteFile: {}", req.path);

        // Bug CI — metadata removal only. There is deliberately NO direct
        // DeleteChunk fan-out: a content-identical re-upload can land its
        // bytes before CommitChunkLocations, so any "no file references this
        // chunk" re-check races it and deletes a just-committed file's
        // bytes. The mtime-graced orphan reaper reclaims the space.
        let resp = self
            .raft_write(ClientRequest::DeleteFile {
                path: req.path.clone(),
            })
            .await?;

        // CR-2-4 (RAFT-7) — DeleteFile refuses a non-empty directory.
        if let ClientResponse::Error { message } = &resp {
            return Err(Status::failed_precondition(message.clone()));
        }

        Ok(Response::new(DeleteFileResponse { success: true }))
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

        let dir_prefix = if req.path.ends_with('/') {
            req.path.clone()
        } else {
            format!("{}/", req.path)
        };
        let entries: Vec<DirectoryEntry> = listed
            .into_iter()
            .map(|e| {
                let is_symlink = data
                    .files
                    .get(&format!("{}{}", dir_prefix, e.name))
                    .is_some_and(|f| f.symlink_target.is_some());
                DirectoryEntry {
                    name: e.name,
                    size: e.size_bytes,
                    is_directory: e.is_directory,
                    created_at: e.created_at_ms as i64,
                    modified_at: e.updated_at_ms as i64,
                    is_symlink,
                }
            })
            .collect();

        Ok(Response::new(ListDirectoryResponse { entries }))
    }

    async fn allocate_chunks(
        &self,
        request: Request<AllocateChunksRequest>,
    ) -> Result<Response<AllocateChunksResponse>, Status> {
        let binding = request.get_ref().path.clone();
        require_mgr_rpc_auth(
            &self.config.cluster.shared_secret,
            "AllocateChunks",
            &binding,
            request.metadata(),
        )?;
        let req = request.into_inner();
        info!(
            "AllocateChunks for file: {}, count: {}",
            req.path, req.num_chunks
        );

        // REV-4-2 — `num_chunks` is peer-controlled. Bound it BEFORE the
        // ring rebuild, before the placement allocation and before the
        // state-machine read lock, so a malformed/malicious count can
        // neither allocate unbounded memory nor hold the lock across a
        // huge loop while every writer waits behind it.
        validate_num_chunks(req.num_chunks, MAX_CHUNKS_PER_ALLOCATION)
            .map_err(Status::invalid_argument)?;

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
        // CR-6-4 (FUSE-6) — when the file already exists, its OWN
        // registered volume governs placement, never the request's
        // `volume_id`. Before this, a stale/wrong client-cached
        // volume_id (e.g. a client that never learned the real one, or
        // hardcoded "default") silently placed every later write of a
        // non-default-volume file with the DEFAULT volume's RF/tier/
        // anchor. Only a brand-new file (no `files` entry yet) uses the
        // request value.
        let existing_file_volume_id = data.files.get(&req.path).map(|f| f.volume_id.as_str());
        if let Some(fv) = existing_file_volume_id {
            if fv != req.volume_id {
                tracing::debug!(
                    path = %req.path,
                    request_volume_id = %req.volume_id,
                    file_volume_id = %fv,
                    "CR-6-4: AllocateChunks request volume_id differs from \
                     the file's registered volume; using the file's volume"
                );
            }
        }
        let effective_volume_id =
            resolve_placement_volume_id(existing_file_volume_id, &req.volume_id);
        let volume_cfg = data.volumes.get(&effective_volume_id).cloned();
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
                    // REV-4-10 — same clamp on the not-yet-registered
                    // volume path: `commit_chunk_locations` falls back to
                    // `defaults.write_safety.min_replicas_before_ack` for
                    // the ack floor, so the allocation must reach it too
                    // or the write is rejected after streaming in full.
                    rf.max(self.config.defaults.replication.hot.max(1))
                        .max(self.config.defaults.write_safety.min_replicas_before_ack),
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
                &effective_volume_id,
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
                // Bug BX-2 — capacity spill. The mandated tier is globally
                // full. If spill is enabled, place on the fastest+nearest
                // OTHER tier with room (ranked by BX-1) instead of failing the
                // write; the chunk lands "misplaced" and the BX-5 rebalancer
                // relocates it once the mandated tier frees. The anchor is
                // never spilled out of its DC/tier — the async anchor-lag
                // scheduler still places the anchor copy (a missing anchor
                // stays visible via BV-1 anchor_pending / BX-3 misplaced).
                // Bug BZ — per-volume spill policy. `allow` overrides the
                // cluster default (so a durability-first volume can fail-loud
                // even when the cluster permits spill); `forbidden_tiers` /
                // `forbidden_datacenters` constrain WHERE non-anchor spill may
                // land (e.g. "never spill the cold archive onto hot SSD" or
                // "never spill into dc-b").
                let spill_policy = volume_cfg
                    .as_ref()
                    .map(|v| v.spill.clone())
                    .unwrap_or_default();
                let spill_allowed = spill_policy
                    .allow
                    .unwrap_or(pcfg.allow_capacity_spill);
                if !spill_allowed {
                    return Err(Status::unavailable(format!(
                        "No healthy data node has free space on tier {effective_tier:?} (all candidate disks below placement headroom or excluded); capacity spill disabled for this volume"
                    )));
                }
                let mut candidates: Vec<crate::placement::CandidateDisk> = Vec::new();
                for (id, info) in &data.nodes {
                    if info.health != dtfs_common::types::HealthStatus::Healthy {
                        continue;
                    }
                    if exclude_set.contains(&norm_addr(&info.grpc_addr)) {
                        continue;
                    }
                    for d in &info.disks {
                        if d.tier == effective_tier {
                            continue; // the mandated tier is the full one
                        }
                        if !pcfg.has_room(d.total_bytes, d.used_bytes) {
                            continue;
                        }
                        // Bug BZ — drop spill targets this volume forbids.
                        if !spill_policy.allows_target(&info.location.datacenter, d.tier) {
                            continue;
                        }
                        candidates.push(crate::placement::CandidateDisk {
                            node_id: *id,
                            disk_id: d.disk_id,
                            tier: d.tier,
                            location: info.location.clone(),
                            free_bytes: d.total_bytes.saturating_sub(d.used_bytes),
                        });
                    }
                }
                // Bug BX-9 — the writer's location (sent by the FUSE client)
                // makes nearness effective: same-DC candidates rank ahead of
                // far ones at equal speed. Absent location => neutral distance
                // => speed -> free-space ordering (legacy behaviour).
                let requester = req
                    .requester_location
                    .as_ref()
                    .map(|l| {
                        dtfs_common::topology::Location::new(
                            &l.region,
                            &l.datacenter,
                            &l.rack,
                            &l.host,
                        )
                    })
                    .unwrap_or_else(|| dtfs_common::topology::Location::new("", "", "", ""));
                let ranked = crate::placement::rank_candidates(&candidates, &requester);
                let Some(best) = ranked.first() else {
                    return Err(Status::unavailable(
                        "No data node has free space on ANY tier (cluster full); cannot spill",
                    ));
                };
                let spill_tier = best.tier;
                let mut spill_nodes: Vec<&dtfs_raft::NodeInfo> = Vec::new();
                let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
                for c in ranked.iter().filter(|c| c.tier == spill_tier) {
                    if seen.insert(c.node_id) {
                        if let Some(n) = data.nodes.get(&c.node_id) {
                            spill_nodes.push(n);
                        }
                    }
                }
                tracing::warn!(
                    path = %req.path,
                    intended_tier = ?effective_tier,
                    spill_tier = ?spill_tier,
                    nodes = spill_nodes.len(),
                    "Bug BX-2 capacity spill: mandated tier full, placing on next-best tier (chunk will classify misplaced + be rebalanced)"
                );
                // Bug BX-10 — record a placement.spill audit row so the spill
                // is visible in /api/v1/audit, not just the manager log.
                if let Some(audit) = &self.audit {
                    audit.log(
                        "placement",
                        "placement.spill",
                        &format!(
                            "path={} intended_tier={:?} spill_tier={:?} reason=tier_full",
                            req.path, effective_tier, spill_tier
                        ),
                        "spilled",
                    );
                }
                let rf = (replication as usize).min(spill_nodes.len());
                for i in 0..req.num_chunks {
                    let primary_idx = i as usize % spill_nodes.len();
                    let primary = spill_nodes[primary_idx];
                    let mut replica_addresses = Vec::new();
                    for r in 1..rf {
                        let replica_idx = (primary_idx + r) % spill_nodes.len();
                        if replica_idx != primary_idx {
                            replica_addresses.push(spill_nodes[replica_idx].grpc_addr.clone());
                        }
                    }
                    placements.push(ChunkPlacement {
                        chunk_index: i,
                        primary_address: primary.grpc_addr.clone(),
                        replica_addresses,
                        tier: chunk_placement_tier_string(spill_tier),
                    });
                }
                return Ok(Response::new(AllocateChunksResponse { placements }));
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
        let binding = request.get_ref().path.clone();
        require_mgr_rpc_auth(
            &self.config.cluster.shared_secret,
            "CommitChunkLocations",
            &binding,
            request.metadata(),
        )?;
        let req = request.into_inner();

        // Reverse-look up numeric node IDs from gRPC addresses before storing.
        // `ChunkLocationEntry.node_id` must be a numeric ID string so that the
        // scheduler's `chunk_loc.node_id.parse::<u64>()` succeeds (fix #7).
        //
        // CF-3b — resolve the volume's `write_safety.min_replicas_before_ack`
        // in the same lock scope, so the ack-floor gate below decides on a
        // consistent snapshot.
        //
        // CO-4-13 — resolve the floor as the STRICTER of the request volume
        // and the volume the file is actually registered under, so a
        // wrong/unknown `req.volume_id` can't fall back to the default and
        // bypass a stricter per-file floor (a silent RF downgrade).
        let (chunks, ack_floor): (Vec<ChunkLocationEntry>, u32) = {
            let data = self.sm_data.read().await;
            let ack_floor = resolve_ack_floor(
                &data,
                &req.path,
                &req.volume_id,
                self.config.defaults.write_safety.min_replicas_before_ack,
            );
            let chunks: Vec<ChunkLocationEntry> = req
                .chunks
                .into_iter()
                .map(|c| ChunkLocationEntry {
                    chunk_index: c.chunk_index as u64,
                    chunk_hash: c.chunk_id,
                    node_id: node_id_from_grpc_addr(&data, &c.primary_address),
                    disk_id: c.disk_id,
                    size_bytes: c.size_bytes,
                })
                .collect();
            // CR-6-2 (CMN-2) — clamp the floor to the effective RF of the
            // tier the chunks actually landed on (the file's own volume
            // when it exists, matching the STRICTER-volume resolution
            // above; the request volume otherwise).
            let effective_volume_id = data
                .files
                .get(&req.path)
                .map(|f| f.volume_id.clone())
                .unwrap_or_else(|| req.volume_id.clone());
            let ack_floor = clamp_ack_floor_to_rf(
                ack_floor,
                &data,
                &chunks,
                &effective_volume_id,
                &req.path,
                self.audit.as_deref(),
            );
            (chunks, ack_floor)
        }; // sm_data lock released here

        // CF-3b — write-safety ack floor. The FUSE writer only commits
        // replicas that already landed (no more are coming), so we reject
        // here rather than wait: if any chunk in this batch has fewer than
        // `ack_floor` distinct (node_id, disk_id) replicas, refuse the
        // commit WITHOUT raft-writing the locations. The orphan reaper
        // (Bug CB) reclaims the abandoned chunks on the data nodes. The
        // gate lives at the manager boundary (defence-in-depth), NOT inside
        // the raft state machine, so there is no snapshot-compat impact.
        if !ack_floor_satisfied(&chunks, ack_floor) {
            let min_committed = chunks
                .iter()
                .fold(
                    std::collections::HashMap::<u64, std::collections::HashSet<(String, u32)>>::new(),
                    |mut acc, c| {
                        acc.entry(c.chunk_index)
                            .or_default()
                            .insert((c.node_id.clone(), c.disk_id));
                        acc
                    },
                )
                .values()
                .map(|s| s.len())
                .min()
                .unwrap_or(0);
            tracing::warn!(
                path = %req.path,
                volume = %req.volume_id,
                ack_floor,
                min_committed,
                "Bug CF-3b: rejecting commit below write_safety.min_replicas_before_ack"
            );
            return Ok(Response::new(CommitChunkLocationsResponse {
                success: false,
                lease_conflict: false,
                error: format!(
                    "write rejected: a chunk landed {min_committed} replica(s) but volume \
                     {} requires write_safety.min_replicas_before_ack={ack_floor}",
                    req.volume_id
                ),
            }));
        }

        let resp = self
            .raft_write(ClientRequest::UpdateChunkLocations { now_ms: stamp_now_ms(),
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
        let binding = format!("{}\n{}", request.get_ref().from, request.get_ref().to);
        require_mgr_rpc_auth(
            &self.config.cluster.shared_secret,
            "RenameFile",
            &binding,
            request.metadata(),
        )?;
        let req = request.into_inner();
        tracing::info!("RenameFile: {} -> {}", req.from, req.to);
        let resp = self
            .raft_write(ClientRequest::RenameFile { now_ms: stamp_now_ms(),
                from: req.from.clone(),
                to: req.to.clone(),
            })
            .await?;
        match resp {
            ClientResponse::Success => Ok(Response::new(RenameFileResponse {
                success: true,
                error: String::new(),
            })),
            ClientResponse::Error { message } => match rename_error_status(&message) {
                Some(status) => Err(status),
                None => Ok(Response::new(RenameFileResponse {
                    success: false,
                    error: message,
                })),
            },
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
        let binding = request.get_ref().path.clone();
        require_mgr_rpc_auth(
            &self.config.cluster.shared_secret,
            "SetFileSize",
            &binding,
            request.metadata(),
        )?;
        let req = request.into_inner();
        tracing::debug!("SetFileSize: {} -> {}", req.path, req.new_size);
        let resp = self
            .raft_write(ClientRequest::SetFileSize { now_ms: stamp_now_ms(),
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
        let binding = request.get_ref().path.clone();
        require_mgr_rpc_auth(
            &self.config.cluster.shared_secret,
            "CreateSymlink",
            &binding,
            request.metadata(),
        )?;
        let req = request.into_inner();
        tracing::info!("Symlink: {} -> {}", req.path, req.target);
        let resp = self
            .raft_write(ClientRequest::CreateSymlink { now_ms: stamp_now_ms(),
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
        let binding = format!("{}\n{}", request.get_ref().source, request.get_ref().dest);
        require_mgr_rpc_auth(
            &self.config.cluster.shared_secret,
            "CreateLink",
            &binding,
            request.metadata(),
        )?;
        let req = request.into_inner();
        tracing::info!("Link: {} -> {}", req.source, req.dest);
        let resp = self
            .raft_write(ClientRequest::CreateLink { now_ms: stamp_now_ms(),
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
        let binding = request.get_ref().path.clone();
        require_mgr_rpc_auth(
            &self.config.cluster.shared_secret,
            "AcquireLock",
            &binding,
            request.metadata(),
        )?;
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
                lease_ms: clamp_posix_lock_lease_ms(req.lease_ms),
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
        let binding = request.get_ref().path.clone();
        require_mgr_rpc_auth(
            &self.config.cluster.shared_secret,
            "ReleaseLock",
            &binding,
            request.metadata(),
        )?;
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
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let data = self.sm_data.read().await;
        if let Some(locks) = data.posix_locks.get(&req.path) {
            for l in locks {
                if l.owner_id == req.owner_id || !posix_lock_is_live(l.expires_at_ms, now_ms) {
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
        let binding = request.get_ref().path.clone();
        require_mgr_rpc_auth(
            &self.config.cluster.shared_secret,
            "AcquireWriteLease",
            &binding,
            request.metadata(),
        )?;
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
        let binding = request.get_ref().path.clone();
        require_mgr_rpc_auth(
            &self.config.cluster.shared_secret,
            "RenewWriteLease",
            &binding,
            request.metadata(),
        )?;
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
        let binding = request.get_ref().path.clone();
        require_mgr_rpc_auth(
            &self.config.cluster.shared_secret,
            "ReleaseWriteLease",
            &binding,
            request.metadata(),
        )?;
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
        let binding = request.get_ref().hostname.clone();
        require_mgr_rpc_auth(
            &self.config.cluster.shared_secret,
            "ReleaseLeasesByHost",
            &binding,
            request.metadata(),
        )?;
        let req = request.into_inner();
        // Bug CD — supply now_ms so the apply reaps only expired host leases
        // (a live, renewing concurrent client is never expired, never dropped).
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let resp = self
            .raft_write(ClientRequest::ReleaseLeasesByHost {
                hostname_prefix: req.hostname,
                except_pid: req.except_pid,
                now_ms,
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
    let (tier, rf) = resolve_preferred_tier(
        hint,
        volume.effective_mandatory_replicas(),
        available_tiers,
    );
    // REV-4-10 — the hint chooses the TIER, not the replica count. An
    // honoured hint hard-returns rf=1, but `commit_chunk_locations`
    // enforces `write_safety.min_replicas_before_ack` (default 2) via
    // `ack_floor_satisfied`, so with `first_write_tier = "nearest-warm"`
    // every write was streamed in FULL and only then rejected at commit
    // (the FUSE client's fresh-file cleanup then unlinked it). Clamp UP
    // to the floor; never down, so a mandatory-driven rf is untouched
    // and a volume whose floor is 1 keeps the pre-fix behaviour.
    (tier, rf.max(volume.write_safety.min_replicas_before_ack))
}

/// Bug AV (AV-4-1d) — wire-format string we put into
/// `ChunkPlacement.tier` so the FUSE writer can pass the
/// manager-resolved tier to the data-node's `put_chunk` RPC. Pins the
/// DiskTier → wire-string contract; if either side ever drifts, the
/// regression test catches it.
pub(crate) fn chunk_placement_tier_string(tier: dtfs_common::types::DiskTier) -> String {
    tier.to_string()
}

/// Bug CF-3b — write-safety ack floor. Returns `true` only when EVERY
/// chunk in the commit batch carries at least `floor` distinct
/// `(node_id, disk_id)` replicas. The FUSE writer commits one
/// `ChunkLocationEntry` per replica that actually landed, so the per-
/// chunk distinct-target count is the durable replica count for that
/// chunk at commit time. Replica identity is `(node_id, disk_id)`,
/// never `node_id` alone (CLAUDE.md A.3.4, Bug BH): two copies on one
/// node but different disks/tiers count as two.
///
/// `floor <= 1` is always satisfied (a single durable copy is enough,
/// or the gate is disabled). An empty batch is vacuously satisfied.
/// Pure, so it is unit-tested directly.
pub(crate) fn ack_floor_satisfied(commits: &[ChunkLocationEntry], floor: u32) -> bool {
    if floor <= 1 {
        return true;
    }
    let mut per_chunk: std::collections::HashMap<u64, std::collections::HashSet<(String, u32)>> =
        std::collections::HashMap::new();
    for c in commits {
        per_chunk
            .entry(c.chunk_index)
            .or_default()
            .insert((c.node_id.clone(), c.disk_id));
    }
    per_chunk
        .values()
        .all(|targets| targets.len() as u32 >= floor)
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
mod tests;
