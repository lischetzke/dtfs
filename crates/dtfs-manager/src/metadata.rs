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

/// Bug BF-9 / BF-10 — warn-once gates for the three deprecated
/// `VolumeSchedules` fields. Each AtomicBool flips on first observation
/// so the warn lands once per manager process lifetime instead of every
/// heartbeat.
static WARNED_SCRUB_INTERVAL: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
static WARNED_FSCK_INTERVAL: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
static WARNED_CUSTOM_SCHEDULE: std::sync::atomic::AtomicBool =
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

// The helper itself MUST read the deprecated fields to detect
// them. Suppress the deprecation warning at the read site only.
#[allow(deprecated)]
fn warn_once_on_dead_schedule_fields(
    volumes: &std::collections::BTreeMap<String, dtfs_common::types::VolumeConfig>,
) {
    use std::sync::atomic::Ordering;
    for v in volumes.values() {
        if v.schedules.scrub_interval_secs.is_some()
            && !WARNED_SCRUB_INTERVAL.swap(true, Ordering::Relaxed)
        {
            tracing::warn!(
                volume_id = %v.id,
                value = ?v.schedules.scrub_interval_secs,
                "Bug BF-9: VolumeSchedules.scrub_interval_secs is removed from \
                 the active path. Cluster-wide [scrub] block governs cadence. \
                 The field is preserved for snapshot compatibility but has no \
                 effect. Remove it from your volume config."
            );
        }
        if v.schedules.fsck_interval_secs.is_some()
            && !WARNED_FSCK_INTERVAL.swap(true, Ordering::Relaxed)
        {
            tracing::warn!(
                volume_id = %v.id,
                value = ?v.schedules.fsck_interval_secs,
                "Bug BF-10: VolumeSchedules.fsck_interval_secs is reserved \
                 but never consumed. No fsck scheduler exists. Scheduled for \
                 removal in next minor release."
            );
        }
        if !v.schedules.custom.is_empty()
            && !WARNED_CUSTOM_SCHEDULE.swap(true, Ordering::Relaxed)
        {
            tracing::warn!(
                volume_id = %v.id,
                count = v.schedules.custom.len(),
                "Bug BF-10: VolumeSchedules.custom map is reserved but no \
                 scheduler consumes named-job entries. Scheduled for removal."
            );
        }
    }
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
        let client = self
            .tls
            .connect(addr)
            .await
            .map(ManagerServiceClient::new)
            .map_err(|e| Status::unavailable(format!("connect leader {addr}: {e}")))?;
        self.leader_clients
            .lock()
            .await
            .insert(addr.to_string(), client.clone());
        Ok(client)
    }
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

/// CO-2-2 — does a forwarded write carry an acceptable shared secret?
///
/// `configured` is the leader's `config.cluster.shared_secret`. When it is
/// empty the cluster runs without a shared secret (back-compat for existing
/// plaintext clusters) and ANY forward is accepted. Otherwise the presented
/// secret must match in CONSTANT TIME — never `==`, which short-circuits on
/// the first mismatching byte and leaks the matching-prefix length via
/// timing. Reuses the audited `dtfs_common::crypto::constant_time_eq` (also
/// used for the join-handshake HMAC) rather than a second hand-rolled copy.
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
    tls: &dtfs_common::tls::TlsContext,
    // CO-2-2 — cluster shared secret stamped onto the forward so the
    // leader authenticates it (see `forward_secret_ok`).
    shared_secret: &str,
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
            // CO-3-9 — this caller (the health monitor) does not hold the
            // state-machine membership map, so it resolves from the voter
            // set only. The membership-preferring path is the heartbeat
            // forward (`proxy_to_leader`), which passes the live SM nodes.
            let leader_addr =
                resolve_leader_addr(voters, &std::collections::BTreeMap::new(), leader_id)
                    .ok_or_else(|| format!("leader {leader_id} not in cluster.initial_voters"))?;
            let mut client = tls
                .connect(&leader_addr)
                .await
                .map(ManagerServiceClient::new)
                .map_err(|e| format!("connect leader {leader_addr}: {e}"))?;
            let body =
                serde_json::to_vec(&req).map_err(|e| format!("serialize ClientRequest: {e}"))?;
            client
                .forward_client_write(ForwardWriteRequest {
                    client_request_json: body,
                    shared_secret: shared_secret.to_string(),
                })
                .await
                .map_err(|e| format!("forward to leader {leader_addr}: {e}"))?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod co_2_2_forward_auth_tests {
    use super::forward_secret_ok;

    #[test]
    fn forward_secret_back_compat_empty_configured_accepts_anything() {
        // Plaintext cluster: leader has no secret configured -> accept all,
        // regardless of what (if anything) the forwarder presents.
        assert!(forward_secret_ok("", ""));
        assert!(forward_secret_ok("", "anything"));
    }

    #[test]
    fn forward_secret_requires_match_when_configured() {
        assert!(forward_secret_ok("s3cr3t", "s3cr3t"));
        assert!(!forward_secret_ok("s3cr3t", "wrong"));
        // Configured-but-missing (empty presented) is rejected.
        assert!(!forward_secret_ok("s3cr3t", ""));
    }
}

#[cfg(test)]
mod lis56_mgr_rpc_auth_tests {
    use super::require_mgr_rpc_auth;
    use dtfs_common::crypto::{
        stamp_mgr_rpc_auth, DATA_RPC_AUTH_MAC, DATA_RPC_AUTH_TS,
    };
    use tonic::metadata::MetadataMap;

    fn meta_with(ts: &str, mac: &str) -> MetadataMap {
        let mut m = MetadataMap::new();
        m.insert(DATA_RPC_AUTH_TS, ts.parse().unwrap());
        m.insert(DATA_RPC_AUTH_MAC, mac.parse().unwrap());
        m
    }

    #[test]
    fn empty_secret_allows_without_headers() {
        assert!(require_mgr_rpc_auth("", "DeleteFile", "/x", &MetadataMap::new()).is_ok());
    }

    #[test]
    fn missing_headers_denied_when_secret_set() {
        let err = require_mgr_rpc_auth("s3cret", "DeleteFile", "/x", &MetadataMap::new())
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(err.message().contains("missing x-dtfs-rpc-ts"));
    }

    #[test]
    fn valid_stamp_accepted() {
        let secret = "s3cret";
        let (ts, mac) = stamp_mgr_rpc_auth(secret, "DeleteFile", "/media/a").unwrap();
        let meta = meta_with(&ts, &mac);
        assert!(require_mgr_rpc_auth(secret, "DeleteFile", "/media/a", &meta).is_ok());
    }

    #[test]
    fn wrong_binding_denied() {
        let secret = "s3cret";
        let (ts, mac) = stamp_mgr_rpc_auth(secret, "DeleteFile", "/a").unwrap();
        let meta = meta_with(&ts, &mac);
        let err = require_mgr_rpc_auth(secret, "DeleteFile", "/b", &meta).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }
}

#[cfg(test)]
mod bug_ch_forward_classify_tests {
    use super::is_forward_to_leader_err;

    // Both error shapes seen in the wild must classify as forwardable;
    // anything else must surface as a real error.
    #[test]
    fn classifies_forward_errors() {
        assert!(is_forward_to_leader_err(
            "has to forward request to: Some(123), Some(DtfsNode { .. })"
        ));
        assert!(is_forward_to_leader_err("ForwardToLeader(..)"));
        assert!(!is_forward_to_leader_err("storage error: disk full"));
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

/// CR-3-3/MGR-1 — resolve the real `file_path` + the replica's real tier
/// on `node_id` for a `ReportChunkReads` entry, so the tiering intercept
/// stops feeding the engine a synthetic `""` / `DiskTier::Hot` profile.
/// Returns `None` for a chunk that references no known file (already
/// deleted/GC'd, or never existed) — the caller skips the report rather
/// than fabricate data. Tier resolution goes through
/// `scheduler::live_replica_tier` so a Down/Failed node or Failed/
/// Removed disk resolves to `None` (falls back to `Hot`, same as the
/// unknown-tier case) rather than a serviceable-looking tier.
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
    let tier = disk_id
        .and_then(|d| crate::scheduler::live_replica_tier(sm.nodes.get(&node_id), d))
        .unwrap_or(DiskTier::Hot);
    Some((file_path, tier))
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
                // noted in AGENTS.md A.4).
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
        //
        // Per-volume `VolumeSchedules.scrub_interval_secs` is now
        // Bug BF-9 — per-volume `scrub_interval_secs` removed from the
        // active path. Cluster-wide `manager.scrub.quick_interval_secs`
        // is now the only source. The serde field on VolumeSchedules
        // stays for back-compat reads of older snapshots but no longer
        // influences the heartbeat override.
        //
        // Bug BF-10 — `fsck_interval_secs` + `custom` schedule fields
        // are reserved-but-unused. We warn-once per process on first
        // observation so operators see the deprecation in journalctl
        // without the noise of a per-heartbeat warn.
        let scrub_cfg = &self.config.scrub;
        let (cfg_version, scrub_override, pending_scrub) = {
            let sm = self.sm_data.read().await;
            let cfg_version = sm.cluster_config.version;
            warn_once_on_dead_schedule_fields(&sm.volumes);
            let effective_quick_secs = scrub_cfg.quick_interval_secs.max(60);
            let quick_h = (effective_quick_secs + 3599) / 3600;
            let deep_h = (scrub_cfg.deep_interval_secs.max(60) + 3599) / 3600;
            (
                cfg_version,
                Some(dtfs_proto::dtfs::manager::ScrubConfigOverride {
                    quick_interval_hours:   quick_h.max(1),
                    deep_interval_hours:    deep_h.max(1),
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

        let _ = self
            .raft_write_or_forward(ClientRequest::UpdateNodeUsage {
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
        if heartbeat_may_mark_healthy(current_health) {
            let _ = self
                .raft_write_or_forward(ClientRequest::UpdateNodeStatus {
                    node_id,
                    status: "healthy".to_string(),
                })
                .await;
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
            let _ = self
                .raft_write_or_forward(ClientRequest::QuarantineNode { node_id, info })
                .await;
            // Drop the (node, chunk) replica entries so the
            // under-replication scheduler immediately picks a healthy
            // source + re-replicates. RemoveCorruptChunkReplicas takes a
            // batch (the same shape `report_chunk_status` uses for the
            // Bug O scrub path), so ONE write covers every reported chunk.
            if !batch.chunk_ids.is_empty() {
                let _ = self
                    .raft_write_or_forward(ClientRequest::RemoveCorruptChunkReplicas { now_ms: stamp_now_ms(),
                        node_id: node_id.to_string(),
                        chunk_hashes: batch.chunk_ids,
                        // CO-1-1 — the heartbeat integrity-fault report
                        // (IntegrityFault) carries no disk_id, so keep the
                        // node-wide legacy sweep. Wiring per-disk corruption
                        // would need a proto change and is out of scope here.
                        disk_id: dtfs_raft::legacy_disk_id(),
                    })
                    .await;
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
            accepted: true,
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
            if let Err(e) = self
                .raft_write_or_forward(ClientRequest::RemoveCorruptChunkReplicas { now_ms: stamp_now_ms(),
                    node_id: req.node_id.clone(),
                    chunk_hashes: corrupt_ids,
                    // CO-1-1 — the scrub ChunkStatusReport carries no
                    // disk_id, so keep the node-wide legacy sweep (Bug O
                    // behaviour). Per-disk would need a proto change.
                    disk_id: dtfs_raft::legacy_disk_id(),
                })
                .await
            {
                tracing::warn!(error = %e, "CO-3-7 failed to commit corrupt-replica removal");
                accepted = false;
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
                let sm = self.sm_data.read().await;
                for (chunk_id, reads_by_dc) in entries {
                    let total: u32 = reads_by_dc.values().sum();
                    // Unknown chunk (already deleted/GC'd) — skip rather
                    // than feed the engine a synthetic profile for
                    // content that no longer exists.
                    let Some((file_path, tier)) =
                        resolve_chunk_read_context(&sm, chunk_id, *node_id)
                    else {
                        continue;
                    };
                    tiering
                        .report_access(
                            chunk_id,
                            &file_path,
                            total,
                            total,
                            reads_by_dc.clone(),
                            tier,
                            *node_id,
                        )
                        .await;
                }
                drop(sm);
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

        // CR-2-4 (RAFT-7) — DeleteFile now refuses a non-empty directory
        // (ClientResponse::Error). The chunk fan-out below must NOT run in
        // that case: the file was never removed, so its chunks are still
        // live and deleting them would corrupt a tree the apply just
        // refused to touch. Bail out before computing/spawning the fan-out.
        if let ClientResponse::Error { message } = &resp {
            return Err(Status::failed_precondition(message.clone()));
        }

        // Now best-effort fan-out chunk deletes. Group by node so we
        // open one connection per target. `plan_delete_fanout` returns
        // tonic-ready endpoints (Bug N fix: the registry grpc_addr is
        // already a full URL, re-prefixing doubled the scheme and made
        // every fan-out fail with a transport error).
        let by_node = plan_delete_fanout(chunk_targets, &node_addrs);

        let path_for_log = req.path.clone();
        // CF-1b — thread the audit log into the spawned fan-out task so a
        // failure emits a `delete.fanout_failed` row (Option<Arc<_>> is
        // cheap to clone). Pre-CF-1b these failures were journald-only,
        // which masked the ~238 GB production orphan leak (Bug N).
        let audit = self.audit.clone();
        // Bug CI — the fan-out runs async, so a content-identical re-upload
        // can recommit the SAME chunk ids before our DeleteChunk RPCs fire.
        // Carry a state handle and re-check liveness per chunk at dispatch.
        let sm_for_liveness = self.sm_data.clone();
        // CF-6 — carry the TLS context into the spawned fan-out.
        let tls = self.tls.clone();
        // LIS-19 — capture secret before spawn (self is not 'static).
        let delete_secret = self.config.cluster.shared_secret.clone();
        tokio::spawn(async move {
            let mut skipped_live: usize = 0;
            for (addr, chunks) in by_node {
                let mut client = match tls.connect(&addr).await
                    .map(dtfs_proto::dtfs::data::data_service_client::DataServiceClient::new)
                {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!(
                            path = %path_for_log,
                            node_addr = %addr,
                            error = %e,
                            "Bug N delete fan-out: connect failed, leaving orphan chunks for reconciler"
                        );
                        if let Some(audit) = &audit {
                            audit.log(
                                "metadata",
                                "delete.fanout_failed",
                                &fanout_failed_summary(&addr, chunks.len()),
                                "connect",
                            );
                        }
                        continue;
                    }
                };
                for chunk_id in chunks {
                    // Bug CI — skip ids a live file references RIGHT NOW.
                    // The .dat we would have deleted is that file's replica.
                    let still_live = {
                        let sm = sm_for_liveness.read().await;
                        !fanout_delete_allowed(&chunk_id, &sm.files)
                    };
                    if still_live {
                        skipped_live += 1;
                        tracing::debug!(
                            path = %path_for_log,
                            chunk_id = %chunk_id,
                            "Bug CI delete fan-out: id re-referenced by a live file; skipping delete"
                        );
                        continue;
                    }
                    // CO-1-1 — the unlink fan-out reclaims the chunk
                    // entirely (the file is gone), so it stays node-wide:
                    // disk_id absent -> legacy first-found.
                    let mut req = tonic::Request::new(dtfs_proto::dtfs::data::DeleteChunkRequest {
                        chunk_id: chunk_id.clone(),
                        disk_id: None,
                        tier: String::new(),
                    });
                    {
                        use dtfs_common::crypto::{
                            stamp_data_rpc_auth, DATA_RPC_AUTH_MAC, DATA_RPC_AUTH_TS,
                        };
                        if let Some((ts, mac)) =
                            stamp_data_rpc_auth(&delete_secret, "DeleteChunk", &chunk_id)
                        {
                            if let (Ok(ts_v), Ok(mac_v)) = (ts.parse(), mac.parse()) {
                                req.metadata_mut().insert(DATA_RPC_AUTH_TS, ts_v);
                                req.metadata_mut().insert(DATA_RPC_AUTH_MAC, mac_v);
                            }
                        }
                    }
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
                            if let Some(audit) = &audit {
                                audit.log(
                                    "metadata",
                                    "delete.fanout_failed",
                                    &fanout_failed_summary(&addr, 1),
                                    "rpc",
                                );
                            }
                        }
                    }
                }
            }
            // Bug CI — surface how many ids were spared because a live
            // file re-referenced them (one row per unlink, not per chunk).
            if skipped_live > 0 {
                tracing::info!(
                    path = %path_for_log,
                    skipped_live,
                    "Bug CI delete fan-out: skipped ids re-referenced by live files"
                );
                if let Some(audit) = &audit {
                    audit.log(
                        "metadata",
                        "delete.fanout_skipped_live",
                        &format!("path={path_for_log} skipped={skipped_live}"),
                        "ok",
                    );
                }
            }
        });

        // Only Success reaches here — Error already returned above.
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
        let volume_cfg = data.volumes.get(&req.volume_id).cloned();
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
                &req.volume_id,
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
            let chunks = req
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
/// never `node_id` alone (AGENTS.md A.3.4, Bug BH): two copies on one
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

#[cfg(test)]
mod bug_av_preferred_tier_tests {
    //! AV-3-2 — pure tests for `resolve_preferred_tier`. Verifies
    //! the hint resolution gates: honour-when-available, fall-
    //! back-when-not-available, empty-hint uses pick_effective_tier.
    use super::*;
    use dtfs_common::types::{DiskTier, TierReplication};

    fn plex_rep() -> TierReplication {
        // Plexmedia-shape: cold mandatory, warm/hot 0.
        TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 }
    }

    /// AV-3-2 test 1 — hint="warm" + a healthy warm node exists →
    /// honour the hint, return `(Warm, 1)`.
    #[test]
    fn bug_av_allocate_chunks_honours_preferred_tier() {
        let avail = vec![DiskTier::Cold, DiskTier::Warm];
        let (tier, rf) = resolve_preferred_tier("warm", &plex_rep(), &avail);
        assert_eq!(tier, DiskTier::Warm);
        assert_eq!(rf, 1, "first-chunk on cache tier places exactly 1 replica");
    }

    /// AV-3-2 test 2 — hint="warm" but no healthy warm node →
    /// fall back to `pick_effective_tier` (Cold for Plexmedia).
    #[test]
    fn bug_av_allocate_chunks_falls_back_when_tier_full() {
        // "tier full" in the operator sense = no node has that tier
        // available (the placement-engine handles per-disk fullness;
        // resolver only sees tier availability).
        let avail = vec![DiskTier::Cold];
        let (tier, rf) = resolve_preferred_tier("warm", &plex_rep(), &avail);
        assert_eq!(tier, DiskTier::Cold);
        assert_eq!(rf, 1, "Plexmedia cold rf=1");
    }

    /// AV-3-2 test 3 — empty hint → legacy behaviour
    /// (pick_effective_tier).
    #[test]
    fn bug_av_allocate_chunks_empty_hint_uses_default() {
        let avail = vec![DiskTier::Cold, DiskTier::Warm, DiskTier::Hot];
        let (tier, rf) = resolve_preferred_tier("", &plex_rep(), &avail);
        assert_eq!(tier, DiskTier::Cold,
            "Plexmedia (hot=0, warm=0, cold=1) picks Cold by default");
        assert_eq!(rf, 1);
    }

    /// AV-3-2 defensive — unknown hint string falls back to
    /// volume default (forward-compat: a future client adds
    /// a new tier name).
    #[test]
    fn bug_av_allocate_chunks_unknown_hint_falls_back() {
        let avail = vec![DiskTier::Cold, DiskTier::Warm];
        let (tier, rf) = resolve_preferred_tier("nearest-warm", &plex_rep(), &avail);
        // "nearest-warm" is what `fuse.toml` carries but the proto
        // protocol uses the bare tier names — sending the raw config
        // string here is treated as unknown.
        assert_eq!(tier, DiskTier::Cold, "unknown hint → pick_effective_tier");
        assert_eq!(rf, 1);
    }

    /// AV-3-2 defensive — parse_preferred_tier_hint covers every
    /// recognised tier + empty + unknown.
    #[test]
    fn bug_av_parse_preferred_tier_hint_coverage() {
        assert_eq!(parse_preferred_tier_hint(""), None);
        assert_eq!(parse_preferred_tier_hint("hot"), Some(DiskTier::Hot));
        assert_eq!(parse_preferred_tier_hint("warm"), Some(DiskTier::Warm));
        assert_eq!(parse_preferred_tier_hint("cold"), Some(DiskTier::Cold));
        assert_eq!(parse_preferred_tier_hint("tape"), Some(DiskTier::Tape));
        assert_eq!(parse_preferred_tier_hint("banana"), None);
        assert_eq!(parse_preferred_tier_hint("Hot"), None,
            "case-sensitive: protocol values are lowercase");
    }

    // ─────────────────────────────────────────────────────────────
    // Bug AV (AV-4-1c) — placement uses mandatory, not legacy
    // ─────────────────────────────────────────────────────────────
    //
    // The bug that prompted these tests: a volume with
    // `mandatory_replicas.cold=1` (operator just ran
    // `volumes set-mandatory --cold-replicas 1 --warm-replicas 0
    // --tape-replicas 0`) still had the pre-AV-migration legacy
    // `replication = {warm:2, cold:1, tape:1}` field set. The
    // `allocate_chunks` handler passed legacy `replication` to
    // `resolve_preferred_tier`, which (per `pick_effective_tier`)
    // returned (Warm, 2). New writes landed on warm, saturating
    // the `cache_replicas.warm.max=1` cap on first write and
    // permanently blocking the cache-promotion path (the planner
    // saw 2 warm replicas already > target of 1, so
    // `saturating_sub` returned 0 → no promotion ever fired).
    //
    // The fix wires `allocate_chunks` to consult
    // `effective_mandatory_replicas()` via the
    // `resolve_volume_placement` helper. These tests pin that
    // contract so a future refactor can't silently regress it.

    fn vol_with_split_mandatory_and_legacy() -> dtfs_common::types::VolumeConfig {
        use dtfs_common::types::*;
        VolumeConfig {
            id: VolumeId::new("default".to_string()),
            name: "default".to_string(),
            chunk_size_bytes: 4 * 1024 * 1024,
            // Legacy: warm=2, cold=1 — the pre-AV state.
            replication: TierReplication { hot: 0, warm: 2, cold: 1, tape: 1 },
            // Mandatory: cold-only — the post-set-mandatory state.
            mandatory_replicas: TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 },
            cache_replicas: CacheReplication {
                hot: CacheTierConfig::default(),
                warm: CacheTierConfig { min: 1, max: 1 },
                cold: CacheTierConfig::default(),
                tape: CacheTierConfig::default(),
                per_dc: true,
                promote_reads: 2,
                promote_window_secs: 60,
                cooldown_secs: 30,
            },
            anchor: None,
            schedules: dtfs_common::types::VolumeSchedules::default(),
            spill: dtfs_common::types::SpillPolicy::default(),
            anchor_datacenter: Some("dc-a".to_string()),
            write_safety: WriteSafety {
                min_replicas_before_ack: 1,
                write_timeout_secs: 30,
            },
        }
    }

    /// AV-4-1c regression #1: a volume with mandatory=cold-only AND
    /// legacy=warm-heavy MUST place new writes on COLD when the
    /// client hint is empty (default first-write-tier). Pre-fix
    /// the handler passed legacy `replication` to
    /// `resolve_preferred_tier` and writes landed on warm,
    /// poisoning the cache-promotion lifecycle.
    #[test]
    fn bug_av_4_1c_placement_uses_mandatory_not_legacy_replication() {
        let v = vol_with_split_mandatory_and_legacy();
        let available = vec![DiskTier::Hot, DiskTier::Warm, DiskTier::Cold];
        let (tier, rf) = resolve_volume_placement(&v, "", &available);
        assert_eq!(
            tier, DiskTier::Cold,
            "must place on Cold (the mandatory tier), not Warm (the legacy tier); \
             writes-land-on-cache-tier silently broke cache promotion"
        );
        assert_eq!(rf, 1, "rf must come from mandatory.cold=1, not legacy.warm=2");
    }

    /// AV-4-1c regression #2: when the client explicitly requests
    /// `preferred_tier="warm"` (FUSE `first_write_tier =
    /// "nearest-warm"`), the placement engine honours the hint
    /// even though mandatory says cold-only. Mandatory is the
    /// fall-back floor, not a hard barrier — the hint takes
    /// priority when the operator has explicitly opted in.
    #[test]
    fn bug_av_4_1c_explicit_warm_hint_still_wins_over_mandatory() {
        let v = vol_with_split_mandatory_and_legacy();
        let available = vec![DiskTier::Warm, DiskTier::Cold];
        let (tier, rf) = resolve_volume_placement(&v, "warm", &available);
        assert_eq!(tier, DiskTier::Warm, "explicit hint honoured");
        assert_eq!(rf, 1, "hint placements always rf=1");
    }

    /// AV-4-1d regression: every `ChunkPlacement` returned by
    /// `allocate_chunks` must carry the manager-resolved tier as a
    /// lowercase wire-format string. Pre-fix the field didn't exist,
    /// so the FUSE writer hardcoded `"hot"` and the data-node's
    /// `select_disk_for_tier(Hot)` fell back to "any healthy disk by
    /// free capacity", silently landing chunks on the wrong tier.
    /// Pinning the DiskTier → wire-string contract here means a
    /// future enum-rename or Display-impl change can't silently
    /// re-introduce the bug.
    #[test]
    fn bug_av_4_1d_allocate_chunks_response_carries_tier() {
        use dtfs_common::types::DiskTier;
        assert_eq!(chunk_placement_tier_string(DiskTier::Cold), "cold");
        assert_eq!(chunk_placement_tier_string(DiskTier::Warm), "warm");
        assert_eq!(chunk_placement_tier_string(DiskTier::Hot), "hot");
        assert_eq!(chunk_placement_tier_string(DiskTier::Tape), "tape");
    }

    /// AV-4-1c regression #3: a legacy volume that has NEVER run
    /// `migrate-policy` (mandatory_replicas all-zero) falls back
    /// to legacy `replication` — preserves the pre-AV behaviour.
    /// Guards against the fix accidentally breaking
    /// pre-Bug-AV deployments.
    #[test]
    fn bug_av_4_1c_legacy_volume_keeps_legacy_replication_behaviour() {
        use dtfs_common::types::*;
        let mut v = vol_with_split_mandatory_and_legacy();
        // Strip mandatory (legacy state).
        v.mandatory_replicas = TierReplication { hot: 0, warm: 0, cold: 0, tape: 0 };
        let available = vec![DiskTier::Warm, DiskTier::Cold];
        let (tier, rf) = resolve_volume_placement(&v, "", &available);
        // With mandatory all-zero, effective_mandatory_replicas
        // falls back to legacy.replication = {warm:2, cold:1, ...}.
        // pick_effective_tier prefers higher tiers → returns Warm.
        assert_eq!(tier, DiskTier::Warm, "legacy fallback uses replication.warm=2");
        assert_eq!(rf, 2);
    }
}

#[cfg(test)]
mod rev_4_10_hint_ack_floor_tests {
    //! REV-4-10 — an honoured `preferred_tier` hint hard-returned rf=1,
    //! but `commit_chunk_locations` enforces the volume's
    //! `write_safety.min_replicas_before_ack` (default 2) via
    //! `ack_floor_satisfied`. With `first_write_tier = "nearest-warm"`
    //! (what the docs recommend for the fast-uplink download DC) EVERY
    //! write was streamed in FULL and only then rejected at commit, and
    //! the FUSE client's fresh-file cleanup unlinked the file. The hint
    //! must choose the TIER, not the replica count.
    use super::*;
    use dtfs_common::types::*;

    fn vol_with_ack_floor(floor: u32) -> VolumeConfig {
        VolumeConfig {
            id: VolumeId::new("default".to_string()),
            name: "default".to_string(),
            chunk_size_bytes: 4 * 1024 * 1024,
            replication: TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 },
            mandatory_replicas: TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 },
            cache_replicas: CacheReplication {
                hot: CacheTierConfig::default(),
                warm: CacheTierConfig { min: 1, max: 2 },
                cold: CacheTierConfig::default(),
                tape: CacheTierConfig::default(),
                per_dc: true,
                promote_reads: 2,
                promote_window_secs: 60,
                cooldown_secs: 30,
            },
            anchor: None,
            schedules: VolumeSchedules::default(),
            spill: SpillPolicy::default(),
            anchor_datacenter: Some("dc-a".to_string()),
            write_safety: WriteSafety {
                min_replicas_before_ack: floor,
                write_timeout_secs: 30,
            },
        }
    }

    /// The hint is still honoured (tier = Warm) but rf is clamped UP to
    /// the volume's ack floor, so the commit gate can be satisfied.
    #[test]
    fn rev_4_10_hint_rf_clamped_to_ack_floor() {
        let v = vol_with_ack_floor(2);
        let available = vec![DiskTier::Warm, DiskTier::Cold];
        let (tier, rf) = resolve_volume_placement(&v, "warm", &available);
        assert_eq!(tier, DiskTier::Warm, "the hint still selects the tier");
        assert!(
            rf >= 2,
            "rf must reach write_safety.min_replicas_before_ack=2, got {rf}"
        );
    }

    /// One-directional: a volume whose ack floor is 1 keeps the pre-fix
    /// rf=1 hint behaviour. The clamp only ever RAISES rf to the floor.
    #[test]
    fn rev_4_10_ack_floor_one_leaves_hint_unchanged() {
        let v = vol_with_ack_floor(1);
        let available = vec![DiskTier::Warm, DiskTier::Cold];
        assert_eq!(
            resolve_volume_placement(&v, "warm", &available),
            (DiskTier::Warm, 1)
        );
    }

    /// The clamped rf must actually satisfy the gate that rejected the
    /// write — pin the two helpers against each other.
    #[test]
    fn rev_4_10_clamped_rf_satisfies_the_commit_gate() {
        let v = vol_with_ack_floor(2);
        let (_, rf) = resolve_volume_placement(&v, "warm", &[DiskTier::Warm, DiskTier::Cold]);
        let commits: Vec<ChunkLocationEntry> = (0..rf)
            .map(|i| ChunkLocationEntry {
                chunk_index: 0,
                chunk_hash: "c0".to_string(),
                node_id: format!("node-{i}"),
                disk_id: 0,
                size_bytes: 1,
            })
            .collect();
        assert!(ack_floor_satisfied(&commits, v.write_safety.min_replicas_before_ack));
    }
}

#[cfg(test)]
mod rev_3_8_integrity_fault_batch_tests {
    //! REV-3-8 — one heartbeat's integrity faults collapse into ONE
    //! audit row + ONE QuarantineNode + ONE batched
    //! RemoveCorruptChunkReplicas, capped, instead of three unbatched
    //! Raft writes PER FAULT.
    use super::*;

    fn fault(chunk: &str) -> IntegrityFault {
        IntegrityFault {
            chunk_id: chunk.to_string(),
            observed_at_ms: 7,
            detector: "verify_on_read".to_string(),
            expected_len: 4,
            actual_len: 4,
            expected_hash: "aaaa".to_string(),
            actual_hash: "bbbb".to_string(),
        }
    }

    /// N faults across M distinct chunk ids collapse to ONE batch of M
    /// ids — the handler then issues 3 Raft writes total, not 3N.
    #[test]
    fn rev_3_8_dedupes_faults_to_distinct_chunk_ids() {
        let faults: Vec<IntegrityFault> =
            (0..9).map(|i| fault(&format!("chunk-{}", i % 3))).collect();
        let batch = batch_integrity_faults(&faults, MAX_INTEGRITY_FAULTS_PER_HEARTBEAT);
        assert_eq!(batch.chunk_ids, vec!["chunk-0", "chunk-1", "chunk-2"]);
        assert_eq!(batch.dropped, 0);
        assert_eq!(batch.scanned, 9);
        assert_eq!(
            batch.representative.map(|f| f.chunk_id.as_str()),
            Some("chunk-0")
        );
    }

    /// A node whose whole `.meta` directory is damaged fills the
    /// data-side collector to its 1024 cap; the manager must examine
    /// only MAX_INTEGRITY_FAULTS_PER_HEARTBEAT of them.
    #[test]
    fn rev_3_8_honours_per_heartbeat_cap() {
        let faults: Vec<IntegrityFault> =
            (0..1024).map(|i| fault(&format!("chunk-{i}"))).collect();
        let batch = batch_integrity_faults(&faults, MAX_INTEGRITY_FAULTS_PER_HEARTBEAT);
        assert_eq!(batch.chunk_ids.len(), MAX_INTEGRITY_FAULTS_PER_HEARTBEAT);
        assert_eq!(batch.scanned, MAX_INTEGRITY_FAULTS_PER_HEARTBEAT);
        assert_eq!(batch.dropped, 1024 - MAX_INTEGRITY_FAULTS_PER_HEARTBEAT);
    }

    /// A fault with no chunk id still quarantines the node but must not
    /// put an empty hash into the batched removal.
    #[test]
    fn rev_3_8_skips_empty_chunk_ids() {
        let faults = vec![fault(""), fault("chunk-a")];
        let batch = batch_integrity_faults(&faults, MAX_INTEGRITY_FAULTS_PER_HEARTBEAT);
        assert_eq!(batch.chunk_ids, vec!["chunk-a"]);
        assert!(batch.representative.is_some());
    }

    /// An empty report produces no batch at all — the handler skips the
    /// whole block, exactly as before.
    #[test]
    fn rev_3_8_empty_report_is_a_noop() {
        let batch = batch_integrity_faults(&[], MAX_INTEGRITY_FAULTS_PER_HEARTBEAT);
        assert!(batch.chunk_ids.is_empty());
        assert!(batch.representative.is_none());
        assert_eq!(batch.dropped, 0);
    }
}

#[cfg(test)]
mod rev_4_2_allocate_bound_tests {
    //! REV-4-2 — `AllocateChunksRequest.num_chunks` is peer-controlled
    //! and used to size an allocation + drive loops under the
    //! state-machine read lock.
    use super::*;

    /// An oversized count is refused at the boundary — nothing is
    /// allocated and the state-machine lock is never taken.
    #[test]
    fn rev_4_2_rejects_oversized_num_chunks() {
        assert!(
            validate_num_chunks(MAX_CHUNKS_PER_ALLOCATION + 1, MAX_CHUNKS_PER_ALLOCATION).is_err()
        );
        assert!(validate_num_chunks(u32::MAX, MAX_CHUNKS_PER_ALLOCATION).is_err());
    }

    /// Real writes still go through: a 30 GB season file at the 4 MiB
    /// default chunk size is 7680 chunks, well inside the cap, and so is
    /// a ~100 GB UHD remux (25600 chunks).
    #[test]
    fn rev_4_2_accepts_normal_num_chunks() {
        for n in [0u32, 1, 7_680, 25_600, MAX_CHUNKS_PER_ALLOCATION] {
            assert!(
                validate_num_chunks(n, MAX_CHUNKS_PER_ALLOCATION).is_ok(),
                "num_chunks {n} must be accepted"
            );
        }
    }

    /// Pins the derivation: the cap is 256 GiB of 4 MiB chunks, which is
    /// the `DefaultsConfig::chunk_size_mb` default.
    #[test]
    fn rev_4_2_cap_covers_largest_workload_file() {
        // `DefaultsConfig::chunk_size_mb` defaults to 4 (pinned by
        // dtfs-common's own `defaults_chunk_size_is_four` test).
        assert_eq!(
            MAX_CHUNKS_PER_ALLOCATION as u64 * 4 * 1024 * 1024,
            256 * 1024 * 1024 * 1024
        );
    }
}

#[cfg(test)]
mod bug_cf_3b_ack_floor_tests {
    //! CF-3b — pure tests for `ack_floor_satisfied`, the write-safety
    //! ack-floor gate the manager applies in `commit_chunk_locations`.
    use super::*;

    fn entry(chunk_index: u64, node_id: &str, disk_id: u32) -> ChunkLocationEntry {
        ChunkLocationEntry {
            chunk_index,
            chunk_hash: format!("hash-{chunk_index}-{node_id}-{disk_id}"),
            node_id: node_id.to_string(),
            disk_id,
            size_bytes: 1,
        }
    }

    /// floor=2, one chunk with two distinct nodes → satisfied.
    #[test]
    fn two_distinct_nodes_meets_floor_2() {
        let commits = vec![entry(0, "1", 0), entry(0, "2", 0)];
        assert!(ack_floor_satisfied(&commits, 2));
    }

    /// floor=2, one chunk with a single replica → NOT satisfied.
    #[test]
    fn single_replica_fails_floor_2() {
        let commits = vec![entry(0, "1", 0)];
        assert!(!ack_floor_satisfied(&commits, 2));
    }

    /// Bug BH identity rule: two entries on the SAME node but DIFFERENT
    /// disk_id (a mandatory + cache replica on different tiers of one
    /// box) count as two distinct replicas → floor=2 satisfied.
    #[test]
    fn same_node_two_disks_counts_as_two() {
        let commits = vec![entry(0, "1", 0), entry(0, "1", 1)];
        assert!(
            ack_floor_satisfied(&commits, 2),
            "(node,disk) identity: two disks on one node are two replicas"
        );
    }

    /// Same node, same disk committed twice (degenerate duplicate) is
    /// ONE distinct replica → floor=2 NOT satisfied.
    #[test]
    fn same_node_same_disk_dedups_to_one() {
        let commits = vec![entry(0, "1", 0), entry(0, "1", 0)];
        assert!(!ack_floor_satisfied(&commits, 2));
    }

    /// Multi-chunk batch: the gate is the MINIMUM across chunks. One
    /// chunk meets floor=2, another only has 1 replica → batch fails.
    #[test]
    fn weakest_chunk_governs_the_batch() {
        let commits = vec![
            entry(0, "1", 0),
            entry(0, "2", 0),
            entry(1, "1", 0), // chunk 1 has only one replica
        ];
        assert!(
            !ack_floor_satisfied(&commits, 2),
            "the under-replicated chunk in the batch must fail the whole commit"
        );
    }

    /// floor<=1 is always satisfied (single durable copy accepted, or
    /// the gate is disabled), even for an empty batch.
    #[test]
    fn floor_one_or_zero_always_satisfied() {
        assert!(ack_floor_satisfied(&[], 1));
        assert!(ack_floor_satisfied(&[], 0));
        assert!(ack_floor_satisfied(&[entry(0, "1", 0)], 1));
        assert!(ack_floor_satisfied(&[entry(0, "1", 0)], 0));
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
/// Bug N (endpoint fix) — group per-chunk delete targets by data node and
/// return a map of tonic-ready endpoint URL → chunk hashes. The registry
/// `grpc_addr` is a fully-formed URL (`http://ip:port`, Bug W), so it must
/// go through `scheduler::normalize_endpoint` rather than getting another
/// `http://` prefix — the old inline `format!("http://{}", addr)` produced
/// `http://http://…`, every DeleteChunk fan-out failed with a transport
/// error, and every deleted file leaked its chunks to the orphan reconciler
/// (observed live on production 2026-06-06, 119/123 deletes failing).
pub(crate) fn plan_delete_fanout(
    chunk_targets: Vec<(String, String)>,
    node_addrs: &std::collections::HashMap<String, String>,
) -> std::collections::HashMap<String, Vec<String>> {
    let mut by_endpoint: std::collections::HashMap<String, Vec<String>> =
        Default::default();
    for (chunk_hash, node_id) in chunk_targets {
        if let Some(addr) = node_addrs.get(&node_id) {
            by_endpoint
                .entry(crate::scheduler::normalize_endpoint(addr))
                .or_default()
                .push(chunk_hash);
        }
    }
    by_endpoint
}

/// Bug CI — may this chunk id actually be deleted from data nodes?
///
/// Chunks are content-addressed: an identical re-upload (WinSCP retry of
/// the same movie) or a second file with identical content re-creates the
/// SAME chunk ids the unlinked file used. The unlink fan-out captures its
/// id list at unlink time and deletes asynchronously, so by the time a
/// DeleteChunk RPC fires, the id may belong to a LIVE file again. Deleting
/// it then is silent data loss: the new file's location entries stay, the
/// bytes vanish (§ A.6(4) — 2026-06-06 production lost 2840/4135 chunks of
/// a re-uploaded library this way). The fan-out re-checks each id against
/// CURRENT state immediately before its RPC and skips live ones; the
/// skipped .dat simply remains (it IS the live file's replica).
pub(crate) fn fanout_delete_allowed(
    chunk_hash: &str,
    files: &std::collections::BTreeMap<String, dtfs_raft::state_machine::FileMetadata>,
) -> bool {
    !crate::scheduler::chunk_in_any_file(chunk_hash, files)
}

#[cfg(test)]
mod bug_ci_fanout_liveness_tests {
    use super::fanout_delete_allowed;
    use dtfs_raft::state_machine::FileMetadata;
    use dtfs_raft::ChunkLocationEntry;

    fn file_with_chunks(path: &str, chunk_hashes: &[&str]) -> FileMetadata {
        FileMetadata {
            path: path.to_string(),
            volume_id: "default".to_string(),
            chunk_size: 4 * 1024 * 1024,
            chunks: chunk_hashes
                .iter()
                .enumerate()
                .map(|(i, h)| ChunkLocationEntry {
                    chunk_index: i as u64,
                    chunk_hash: (*h).to_string(),
                    node_id: "10".to_string(),
                    disk_id: 0,
                    size_bytes: 4 * 1024 * 1024,
                })
                .collect(),
            created_at_ms: 0,
            updated_at_ms: 0,
            symlink_target: None,
            logical_size: None,
            is_directory: false,
            cache_cooldown_until_ms: std::collections::BTreeMap::new(),
        }
    }

    // Bug CI — chunks are content-addressed, so an identical re-upload (or a
    // second file sharing identical content) re-creates the SAME chunk ids
    // the unlinked file used. The async delete fan-out captured its id list
    // at unlink time; on 2026-06-06 production it swept that list WHILE the
    // re-upload recommitted the same ids, deleting 2840 of 4135 freshly
    // written chunks from every node (state kept the entries — silent loss).
    // The fan-out must re-check liveness against CURRENT state per chunk at
    // dispatch time and skip any id a live file still references.
    #[test]
    fn live_referenced_chunk_must_not_be_deleted() {
        let mut files = std::collections::BTreeMap::new();
        files.insert(
            "/media/movie-v2.mkv".to_string(),
            file_with_chunks("/media/movie-v2.mkv", &["shared_x", "shared_y"]),
        );
        assert!(!fanout_delete_allowed("shared_x", &files));
        assert!(!fanout_delete_allowed("shared_y", &files));
    }

    #[test]
    fn unreferenced_chunk_is_deleted() {
        let mut files = std::collections::BTreeMap::new();
        files.insert(
            "/media/movie-v2.mkv".to_string(),
            file_with_chunks("/media/movie-v2.mkv", &["shared_x"]),
        );
        assert!(fanout_delete_allowed("gone_for_real", &files));
        let empty = std::collections::BTreeMap::new();
        assert!(fanout_delete_allowed("anything", &empty));
    }
}

/// Bug N / CF-1b — build the `delete.fanout_failed` audit summary. The
/// summary identifies the data node endpoint plus how many chunks were
/// left orphaned by the failure; the audit `result` carries the failure
/// class (`connect` for a dial failure, `rpc` for a DeleteChunk RPC
/// error). The orphan reconciler still reclaims the chunks later, but the
/// audit row makes the failure observable in `/api/v1/audit` and the WebUI
/// integrity feed instead of being journald-only (the gap that masked the
/// ~238 GB production orphan leak).
pub(crate) fn fanout_failed_summary(node_addr: &str, chunk_count: usize) -> String {
    format!("node_addr={node_addr} orphan_chunks={chunk_count}")
}

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

    /// Bug N regression — the registry stores a fully-formed URL
    /// (`http://ip:port`, Bug W). The fan-out planner must NOT add a
    /// second scheme; pre-fix it produced `http://http://ip:port` and
    /// every delete fan-out failed with a tonic transport error.
    #[test]
    fn bug_n_fanout_registry_url_not_double_prefixed() {
        let mut addrs = std::collections::HashMap::new();
        addrs.insert("7".to_string(), "http://100.86.56.97:9500".to_string());
        let plan = plan_delete_fanout(
            vec![("chunk-a".to_string(), "7".to_string())],
            &addrs,
        );
        assert_eq!(plan.len(), 1);
        assert!(
            plan.contains_key("http://100.86.56.97:9500"),
            "endpoint must be the registry URL verbatim, got keys: {:?}",
            plan.keys().collect::<Vec<_>>()
        );
    }

    /// Bug N — pre-Bug-W registries stored bare `host:port`; those still
    /// get exactly one scheme via normalize_endpoint.
    #[test]
    fn bug_n_fanout_bare_addr_gets_single_scheme() {
        let mut addrs = std::collections::HashMap::new();
        addrs.insert("7".to_string(), "10.0.0.4:9500".to_string());
        let plan = plan_delete_fanout(
            vec![("chunk-a".to_string(), "7".to_string())],
            &addrs,
        );
        assert!(plan.contains_key("http://10.0.0.4:9500"));
    }

    /// Bug N — chunks group per endpoint; targets with no registry entry
    /// are skipped (node gone — reconciler's job).
    #[test]
    fn bug_n_fanout_groups_and_skips_unknown_nodes() {
        let mut addrs = std::collections::HashMap::new();
        addrs.insert("7".to_string(), "http://10.0.0.4:9500".to_string());
        let plan = plan_delete_fanout(
            vec![
                ("chunk-a".to_string(), "7".to_string()),
                ("chunk-b".to_string(), "7".to_string()),
                ("chunk-c".to_string(), "999".to_string()), // unknown node
            ],
            &addrs,
        );
        assert_eq!(plan.len(), 1);
        assert_eq!(plan["http://10.0.0.4:9500"].len(), 2);
    }

    /// CF-1b — the fan-out failure audit summary identifies the data node
    /// endpoint and the count of chunks left orphaned, so an operator
    /// reading `/api/v1/audit` or the integrity feed can see which node and
    /// how much disk is at stake without grepping journald.
    #[test]
    fn cf1b_fanout_failed_summary_carries_node_and_count() {
        let summary = fanout_failed_summary("http://10.0.0.4:9500", 3);
        assert_eq!(summary, "node_addr=http://10.0.0.4:9500 orphan_chunks=3");
    }

    /// Bug W regression — peer IP wins over reported hostname.
    /// This is the actual prod bug from Plexmedia: data-warm01 resolves
    /// to fe80::… IPv6 link-local, but the peer IP is 10.150.0.157.
    #[test]
    fn bug_w_peer_ipv4_overrides_hostname() {
        let peer = Some(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(10, 150, 0, 157)),
            33421,
        ));
        let addr = build_data_grpc_addr(peer, "data-warm01", 9500);
        // Peer IP, registration port (NOT 33421).
        assert_eq!(addr, "http://10.150.0.157:9500");
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

    // ── Bug BL — heartbeat non-leader forwarding helpers ────────────

    fn mk_voter(hostname: &str, addr: &str) -> dtfs_common::config::InitialVoter {
        dtfs_common::config::InitialVoter {
            hostname: hostname.to_string(),
            addr: addr.to_string(),
            region: String::new(),
            datacenter: String::new(),
            rack: String::new(),
        }
    }

    /// BL-3 — a node that reported an integrity fault this cycle must
    /// never be told it is clear, even if Raft state still shows it as
    /// not-yet-quarantined (the QuarantineNode write commits async on the
    /// leader). Without this the data node re-enables put_chunk on a
    /// corrupt node within one heartbeat.
    #[test]
    fn bl3_self_quarantine_cleared_guard() {
        // Healthy node, no faults → cleared.
        assert!(compute_self_quarantine_cleared(false, 0));
        // Reported a fault this cycle → NOT cleared, even though not yet
        // quarantined in local state.
        assert!(!compute_self_quarantine_cleared(false, 1));
        // Already quarantined → not cleared.
        assert!(!compute_self_quarantine_cleared(true, 0));
        // Quarantined AND reporting faults → not cleared.
        assert!(!compute_self_quarantine_cleared(true, 2));
    }

    /// BL-1 — the leader's gRPC addr resolves from the voter set by
    /// matching node_id_from_hostname against the openraft leader id when
    /// no membership entry exists.
    #[test]
    fn bl1_resolve_leader_addr_matches_by_hostname_id() {
        let voters = vec![
            mk_voter("mini-mgr01", "10.0.0.1:9401"),
            mk_voter("mini-mgr02", "10.0.0.2:9401"),
            mk_voter("mini-mgr03", "10.0.0.3:9401"),
        ];
        let empty = std::collections::BTreeMap::new();
        let leader_id = dtfs_common::node_id_from_hostname("mini-mgr02");
        assert_eq!(
            resolve_leader_addr(&voters, &empty, leader_id),
            Some("10.0.0.2:9401".to_string())
        );
        // Unknown leader id (not in voter set, not in membership) → None.
        assert_eq!(resolve_leader_addr(&voters, &empty, 0), None);
    }

    fn mk_membership_node(grpc_addr: &str) -> RaftNodeInfo {
        RaftNodeInfo {
            role: NodeRole::Manager,
            hostname: String::new(),
            grpc_addr: grpc_addr.to_string(),
            location: Location::new("", "", "", ""),
            disks: Vec::new(),
            total_capacity: 0,
            used_capacity: 0,
            health: HealthStatus::Healthy,
            last_seen_ms: 0,
            total_bytes_read: 0,
            total_bytes_written: 0,
            client_ip: None,
            version: String::new(),
            build_date: String::new(),
            quarantine: None,
        }
    }

    /// CO-3-9 — a CF-2-promoted leader is present ONLY in the live Raft
    /// membership (not in the bootstrap `initial_voters`); it must still
    /// resolve, from its membership `grpc_addr`.
    #[test]
    fn co_3_9_leader_only_in_membership_resolves() {
        let voters = vec![mk_voter("mini-mgr01", "10.0.0.1:9401")];
        let leader_id = 42; // a promoted node, no matching voter hostname
        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(leader_id, mk_membership_node("http://10.0.0.9:9401"));
        assert_eq!(
            resolve_leader_addr(&voters, &nodes, leader_id),
            Some("http://10.0.0.9:9401".to_string()),
            "a promoted leader absent from initial_voters resolves via membership"
        );
    }

    /// CO-3-9 — a leader present only in `initial_voters` (not yet in the
    /// membership map, e.g. early boot) still resolves via the fallback.
    #[test]
    fn co_3_9_leader_only_in_voters_resolves() {
        let voters = vec![mk_voter("mini-mgr02", "10.0.0.2:9401")];
        let leader_id = dtfs_common::node_id_from_hostname("mini-mgr02");
        let empty = std::collections::BTreeMap::new();
        assert_eq!(
            resolve_leader_addr(&voters, &empty, leader_id),
            Some("10.0.0.2:9401".to_string())
        );
    }

    /// CO-3-9 — membership is preferred over voters when both have the id.
    #[test]
    fn co_3_9_membership_preferred_over_voters() {
        let voters = vec![mk_voter("mini-mgr02", "10.0.0.2:9401")];
        let leader_id = dtfs_common::node_id_from_hostname("mini-mgr02");
        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(leader_id, mk_membership_node("http://10.0.0.99:9401"));
        assert_eq!(
            resolve_leader_addr(&voters, &nodes, leader_id),
            Some("http://10.0.0.99:9401".to_string())
        );
    }

    /// CO-3-8 — only the transient liveness states may be re-stamped to
    /// Healthy by a heartbeat; Maintenance/Draining/Quarantined are sticky.
    #[test]
    fn co_3_8_heartbeat_may_mark_healthy_truth_table() {
        assert!(heartbeat_may_mark_healthy(HealthStatus::Healthy));
        assert!(heartbeat_may_mark_healthy(HealthStatus::Suspect));
        assert!(heartbeat_may_mark_healthy(HealthStatus::Down));
        // Disk-derived states stay heartbeat-clearable (recovery path).
        assert!(heartbeat_may_mark_healthy(HealthStatus::Degraded));
        assert!(heartbeat_may_mark_healthy(HealthStatus::Failed));
        // Sticky operator/drain/quarantine states are NOT.
        assert!(!heartbeat_may_mark_healthy(HealthStatus::Maintenance));
        assert!(!heartbeat_may_mark_healthy(HealthStatus::Draining));
        assert!(!heartbeat_may_mark_healthy(HealthStatus::Quarantined));
    }

    /// CO-4-13 — a file registered under a floor=2 volume must enforce
    /// floor 2 even when the commit request claims a different (or
    /// unknown) volume that resolves to the default floor.
    #[test]
    fn co_4_13_resolve_ack_floor_uses_stricter_file_volume() {
        use dtfs_raft::state_machine::FileMetadata;

        let mut sm = StateMachineData::default();
        // Register a "strict" volume with floor=2 and put the file on it.
        sm.volumes.insert("strict".to_string(), vol_with_floor("strict", 2));
        sm.files.insert(
            "/movies/a.mkv".to_string(),
            FileMetadata {
                path: "/movies/a.mkv".to_string(),
                volume_id: "strict".to_string(),
                chunk_size: 4 * 1024 * 1024,
                chunks: Vec::new(),
                created_at_ms: 0,
                updated_at_ms: 0,
                symlink_target: None,
                logical_size: None,
                is_directory: false,
                cache_cooldown_until_ms: std::collections::BTreeMap::new(),
            },
        );

        // Request claims the unknown/default volume (floor would be 1) but
        // the file's stored volume floor (2) must win.
        assert_eq!(
            resolve_ack_floor(&sm, "/movies/a.mkv", "default", 1),
            2,
            "file's stored-volume floor (2) must override the request default (1)"
        );

        // Request volume is stricter than the file's volume → request wins.
        sm.volumes.insert("stricter".to_string(), vol_with_floor("stricter", 3));
        assert_eq!(
            resolve_ack_floor(&sm, "/movies/a.mkv", "stricter", 1),
            3,
            "the stricter of the two volumes governs"
        );

        // Unknown file (first commit) → only the request volume governs.
        assert_eq!(resolve_ack_floor(&sm, "/movies/new.mkv", "strict", 1), 2);
        assert_eq!(resolve_ack_floor(&sm, "/movies/new.mkv", "default", 1), 1);
    }

    /// CR-3-3/MGR-1 helper — a Data node with one disk of the given tier.
    fn mk_data_node(tier: DiskTier) -> RaftNodeInfo {
        RaftNodeInfo {
            role: NodeRole::Data,
            hostname: "data-1".to_string(),
            grpc_addr: String::new(),
            location: Location::new("lab", "leli", "rack-1", "data-1"),
            disks: vec![dtfs_common::types::DiskInfo {
                disk_id: 0,
                uuid: String::new(),
                mount_path: String::new(),
                tier,
                total_bytes: 0,
                used_bytes: 0,
                status: DiskStatus::Healthy,
            }],
            total_capacity: 0,
            used_capacity: 0,
            health: HealthStatus::Healthy,
            last_seen_ms: 0,
            total_bytes_read: 0,
            total_bytes_written: 0,
            client_ip: None,
            version: String::new(),
            build_date: String::new(),
            quarantine: None,
        }
    }

    fn file_with_chunk(
        path: &str,
        chunk_hash: &str,
        node_id: u64,
        disk_id: u32,
    ) -> dtfs_raft::state_machine::FileMetadata {
        dtfs_raft::state_machine::FileMetadata {
            path: path.to_string(),
            volume_id: "default".to_string(),
            chunk_size: 4 * 1024 * 1024,
            chunks: vec![dtfs_raft::ChunkLocationEntry {
                chunk_index: 0,
                chunk_hash: chunk_hash.to_string(),
                node_id: node_id.to_string(),
                disk_id,
                size_bytes: 1024,
            }],
            created_at_ms: 0,
            updated_at_ms: 0,
            symlink_target: None,
            logical_size: None,
            is_directory: false,
            cache_cooldown_until_ms: std::collections::BTreeMap::new(),
        }
    }

    /// CR-3-3/MGR-1 — an unknown chunk (no file references it) resolves
    /// to `None`, so the intercept skips the report instead of feeding
    /// the tiering engine a synthetic `""` file_path / `Hot` tier.
    #[test]
    fn cr_3_3_unknown_chunk_resolves_to_none() {
        let sm = StateMachineData::default();
        assert!(resolve_chunk_read_context(&sm, "no-such-hash", 1).is_none());
    }

    /// CR-3-3/MGR-1 — a known chunk resolves to its real file_path and
    /// the replica's real tier on the reporting node.
    #[test]
    fn cr_3_3_known_chunk_resolves_real_path_and_tier() {
        let mut sm = StateMachineData::default();
        sm.nodes.insert(1, mk_data_node(DiskTier::Warm));
        sm.files.insert(
            "/movies/x.mkv".to_string(),
            file_with_chunk("/movies/x.mkv", "h0", 1, 0),
        );
        let (path, tier) = resolve_chunk_read_context(&sm, "h0", 1).expect("known chunk");
        assert_eq!(path, "/movies/x.mkv");
        assert_eq!(tier, DiskTier::Warm);
    }

    /// CR-3-3/MGR-1 — a replica on a Down node cannot be resolved to a
    /// serviceable tier via `live_replica_tier`, so it falls back to
    /// `Hot` (same fallback as an unresolvable tier) rather than
    /// reporting a tier that isn't actually being served.
    #[test]
    fn cr_3_3_dead_replica_falls_back_to_hot_tier() {
        let mut sm = StateMachineData::default();
        let mut node = mk_data_node(DiskTier::Cold);
        node.health = HealthStatus::Down;
        sm.nodes.insert(1, node);
        sm.files.insert(
            "/movies/x.mkv".to_string(),
            file_with_chunk("/movies/x.mkv", "h0", 1, 0),
        );
        let (path, tier) = resolve_chunk_read_context(&sm, "h0", 1).expect("known chunk");
        assert_eq!(path, "/movies/x.mkv");
        assert_eq!(tier, DiskTier::Hot);
    }

    fn vol_with_floor(id: &str, floor: u32) -> dtfs_common::types::VolumeConfig {
        use dtfs_common::types::*;
        VolumeConfig {
            id: VolumeId::new(id.to_string()),
            name: id.to_string(),
            chunk_size_bytes: 4 * 1024 * 1024,
            replication: TierReplication { hot: 0, warm: 2, cold: 1, tape: 1 },
            mandatory_replicas: TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 },
            cache_replicas: CacheReplication {
                hot: CacheTierConfig::default(),
                warm: CacheTierConfig::default(),
                cold: CacheTierConfig::default(),
                tape: CacheTierConfig::default(),
                per_dc: true,
                promote_reads: 2,
                promote_window_secs: 60,
                cooldown_secs: 30,
            },
            anchor: None,
            schedules: VolumeSchedules::default(),
            spill: SpillPolicy::default(),
            anchor_datacenter: None,
            write_safety: WriteSafety {
                min_replicas_before_ack: floor,
                write_timeout_secs: 30,
            },
        }
    }
}
