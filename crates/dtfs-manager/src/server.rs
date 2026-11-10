use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use dtfs_common::config::ManagerConfig;
use dtfs_proto::dtfs::manager::manager_service_server::ManagerServiceServer;
use dtfs_raft::network::base64_decode;
use dtfs_raft::DtfsRaft;
use http_body_util::BodyExt;
use openraft::raft::{AppendEntriesRequest, VoteRequest};
use serde::{Deserialize, Serialize};
use tonic::transport::Server;
use tonic::Status;
use tracing::{debug, warn};

use crate::metadata::MetadataService;

// ---------------------------------------------------------------------------
// Raft RPC envelope types (must match dtfs-raft::network)
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
enum RaftRpcKind {
    AppendEntries,
    Vote,
    Snapshot,
}

#[derive(Debug, Serialize, Deserialize)]
struct RaftMessage {
    kind: RaftRpcKind,
    payload: String,
}

// ---------------------------------------------------------------------------
// RaftTransport handler
// ---------------------------------------------------------------------------

/// Handles incoming Raft RPCs.
#[derive(Clone)]
pub struct RaftTransportHandler {
    raft: Arc<DtfsRaft>,
}

impl RaftTransportHandler {
    pub fn new(raft: Arc<DtfsRaft>) -> Self {
        Self { raft }
    }

    async fn handle_rpc(&self, body: Bytes) -> Result<Bytes, Status> {
        let msg: RaftMessage = serde_json::from_slice(&body)
            .map_err(|e| Status::invalid_argument(format!("bad raft msg: {}", e)))?;
        // Snapshot-sized: keep one copy at a time (issue #113 raised the cap).
        drop(body);

        let response_payload = match msg.kind {
            RaftRpcKind::AppendEntries => self.handle_append_entries(&msg.payload).await,
            RaftRpcKind::Vote => self.handle_vote(&msg.payload).await,
            RaftRpcKind::Snapshot => self.handle_snapshot(msg.payload).await,
        };

        let resp_msg = RaftMessage {
            kind: msg.kind,
            payload: response_payload,
        };

        let resp_bytes = serde_json::to_vec(&resp_msg)
            .map_err(|e| Status::internal(format!("serialize response: {}", e)))?;

        Ok(Bytes::from(resp_bytes))
    }

    async fn handle_append_entries(&self, payload: &str) -> String {
        match serde_json::from_str::<AppendEntriesRequest<dtfs_raft::TypeConfig>>(payload) {
            Ok(req) => {
                debug!("RaftTransport: AppendEntries");
                match self.raft.append_entries(req).await {
                    Ok(resp) => serde_json::to_string(&resp).unwrap_or_default(),
                    Err(e) => {
                        warn!(error = %e, "AppendEntries error");
                        String::new()
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to parse AppendEntries");
                String::new()
            }
        }
    }

    async fn handle_vote(&self, payload: &str) -> String {
        match serde_json::from_str::<VoteRequest<dtfs_raft::TypeConfig>>(payload) {
            Ok(req) => {
                debug!("RaftTransport: Vote");
                match self.raft.vote(req).await {
                    Ok(resp) => serde_json::to_string(&resp).unwrap_or_default(),
                    Err(e) => {
                        warn!(error = %e, "Vote error");
                        String::new()
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to parse Vote");
                String::new()
            }
        }
    }

    async fn handle_snapshot(&self, payload: String) -> String {
        #[derive(Deserialize)]
        struct SnapshotPayload {
            vote: openraft::type_config::alias::VoteOf<dtfs_raft::TypeConfig>,
            meta: dtfs_raft::SnapshotMeta,
            data_base64: String,
        }

        let parsed = serde_json::from_str::<SnapshotPayload>(&payload);
        drop(payload);
        match parsed {
            Ok(snap) => {
                debug!("RaftTransport: InstallSnapshot");
                let data = base64_decode(&snap.data_base64);
                // CNF-4: free the base64 copy before installing the snapshot.
                drop(snap.data_base64);
                let snapshot = openraft::storage::Snapshot {
                    meta: snap.meta,
                    snapshot: std::io::Cursor::new(data),
                };
                match self.raft.install_full_snapshot(snap.vote, snapshot).await {
                    Ok(resp) => serde_json::to_string(&resp).unwrap_or_default(),
                    Err(e) => {
                        warn!(error = %e, "InstallSnapshot error");
                        String::new()
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to parse Snapshot");
                String::new()
            }
        }
    }
}

// base64 decode lives in dtfs_raft::network (shared with the sender's
// encoder so the pair is round-trip tested together — CNF-4).

// ---------------------------------------------------------------------------
// Raft transport auth + framing helpers
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
enum RaftAuthDecision {
    Accept,
    /// No stamp, permissive mode (rolling update).
    AcceptUnsigned,
    Reject(&'static str),
}

/// Decide whether a Raft RPC is allowed. Empty secret skips auth. A stamp that
/// is present but incomplete/invalid is ALWAYS rejected; a missing stamp is
/// rejected only when `require` is set.
fn raft_auth_decision(
    secret: &str,
    require: bool,
    ts: Option<&str>,
    mac: Option<&str>,
    binding: &str,
    now_unix: u64,
) -> RaftAuthDecision {
    if secret.is_empty() {
        return RaftAuthDecision::Accept;
    }
    match (ts, mac) {
        // Empty binding = no x-dtfs-raft-from header: the MAC would cover no
        // sender/target/body, so a captured legacy stamp could authorize any body.
        (Some(_), Some(_)) if binding.is_empty() => {
            if require {
                RaftAuthDecision::Reject("missing sender header")
            } else {
                RaftAuthDecision::AcceptUnsigned
            }
        }
        (Some(ts), Some(mac)) => {
            if dtfs_common::crypto::verify_mgr_rpc_auth(
                secret,
                dtfs_raft::network::RAFT_RPC_AUTH_METHOD,
                ts,
                mac,
                binding,
                now_unix,
            ) {
                RaftAuthDecision::Accept
            } else {
                RaftAuthDecision::Reject("invalid stamp")
            }
        }
        (None, None) if require => RaftAuthDecision::Reject("missing stamp"),
        (None, None) => RaftAuthDecision::AcceptUnsigned,
        _ => RaftAuthDecision::Reject("incomplete stamp"),
    }
}

/// Warn about unsigned Raft RPCs at most once per 60 s (they arrive every heartbeat).
fn warn_unsigned_raft_rpc(now_unix: u64) {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    static LAST: AtomicU64 = AtomicU64::new(0);
    let last = LAST.load(Relaxed);
    if now_unix.saturating_sub(last) >= 60
        && LAST.compare_exchange(last, now_unix, Relaxed, Relaxed).is_ok()
    {
        warn!(
            "RaftTransport: accepting UNSIGNED Raft RPC (cluster.require_raft_auth=false); \
             set it true once every manager signs Raft RPCs"
        );
    }
}

fn grpc_error_response(code: i32, msg: &str) -> tonic::codegen::http::Response<tonic::body::Body> {
    tonic::codegen::http::Response::builder()
        .status(200)
        .header("content-type", "application/grpc")
        .header("grpc-status", code.to_string())
        .header("grpc-message", msg)
        .body(tonic::body::Body::empty())
        .unwrap()
}

/// Peel the gRPC 5-byte frame and the prost `bytes` field (tag 0x0A + varint
/// length) off a request body. Lenient on truncation (clamps to the body) but
/// never panics: an unterminated/over-long varint is an error.
fn decode_raft_payload(body_bytes: Bytes) -> Result<Bytes, &'static str> {
    if body_bytes.len() < 5 {
        return Ok(body_bytes);
    }
    let outer_len =
        u32::from_be_bytes([body_bytes[1], body_bytes[2], body_bytes[3], body_bytes[4]]) as usize;
    let outer_end = 5usize.saturating_add(outer_len).min(body_bytes.len());
    let inner = body_bytes.slice(5..outer_end);
    if inner.is_empty() || inner[0] != 0x0A {
        return Ok(inner);
    }
    let mut value: u64 = 0;
    let mut consumed = 0usize;
    let mut done = false;
    for (i, b) in inner[1..].iter().take(10).enumerate() {
        consumed = i + 1;
        value |= ((b & 0x7F) as u64) << (7 * i as u32);
        if b & 0x80 == 0 {
            done = true;
            break;
        }
    }
    if !done {
        return Err("bad varint length");
    }
    let start = 1 + consumed;
    let end = start
        .saturating_add(usize::try_from(value).unwrap_or(usize::MAX))
        .min(inner.len());
    Ok(inner.slice(start..end))
}

// ---------------------------------------------------------------------------
// RaftTransport as a tonic-compatible service
// ---------------------------------------------------------------------------

/// A tonic-compatible service for the Raft transport.
#[derive(Clone)]
struct RaftTransportSvc {
    handler: RaftTransportHandler,
    secret: String,
    require_auth: bool,
    /// Cap for AppendEntries / Vote (and legacy-path snapshots): the gRPC cap.
    max_body_bytes: usize,
    /// This manager's node id: the MAC "target" binding.
    self_id: u64,
    /// One snapshot-sized body buffered at a time; a second is refused.
    snapshot_slot: Arc<tokio::sync::Semaphore>,
}

impl RaftTransportSvc {
    fn new(handler: RaftTransportHandler, config: &ManagerConfig) -> Self {
        Self {
            handler,
            secret: config.cluster.shared_secret.clone(),
            require_auth: config.cluster.require_raft_auth,
            self_id: dtfs_common::node_id_from_hostname(&config.node.hostname),
            max_body_bytes: dtfs_common::config::max_message_bytes(config.grpc.max_message_size_mb),
            snapshot_slot: Arc::new(tokio::sync::Semaphore::new(1)),
        }
    }
}

/// Issue #113 — a snapshot is the whole state in ONE body, so any cap below its
/// size stalls the follower forever. Bound only by the gRPC frame (u32 length)
/// the sender can encode. shortcut: snapshots above ~4 GiB on the wire (~20M
/// chunk rows) still fail, chunked install when needed.
const RAFT_MAX_BODY_BYTES: usize = 5 + u32::MAX as usize;
/// gRPC frame header (5) + prost tag (1) + max varint (10).
const RAFT_FRAME_OVERHEAD: usize = 16;

/// Everything decidable from headers alone, run BEFORE any body is buffered.
/// Returns the body cap to read with, or the gRPC status to answer.
/// `hdr` looks up a request header.
fn raft_pre_body_check<'a>(
    secret: &str,
    require: bool,
    snapshot_path: bool,
    self_id: u64,
    grpc_cap: usize,
    now: u64,
    hdr: impl Fn(&str) -> Option<&'a str>,
) -> Result<usize, (i32, &'static str)> {
    use dtfs_raft::network as n;
    let (ts, mac) = (
        hdr(dtfs_common::crypto::DATA_RPC_AUTH_TS),
        hdr(dtfs_common::crypto::DATA_RPC_AUTH_MAC),
    );
    if !secret.is_empty() {
        if ts.is_none() || mac.is_none() {
            if let RaftAuthDecision::Reject(why) = raft_auth_decision(secret, require, ts, mac, "", now) {
                return Err((7, why));
            }
        }
        // A sender-bound stamp outside the skew window fails later anyway.
        if let (Some(ts), Some(_)) = (ts, hdr(n::RAFT_RPC_FROM_HEADER)) {
            let fresh = ts
                .parse::<u64>()
                .is_ok_and(|t| t.abs_diff(now) <= dtfs_common::crypto::DATA_RPC_MAX_SKEW_SECS);
            if !fresh {
                return Err((7, "stale stamp"));
            }
        }
    }
    if !snapshot_path {
        return Ok(grpc_cap);
    }
    if secret.is_empty() {
        return Ok(RAFT_MAX_BODY_BYTES);
    }
    // Only new senders use the snapshot path, and they always pre-stamp the
    // declared length, so no unauthenticated peer can force a large buffer.
    let from = hdr(n::RAFT_RPC_FROM_HEADER).and_then(|f| f.parse::<u64>().ok());
    let len = hdr(n::RAFT_SNAPSHOT_LEN_HEADER).and_then(|l| l.parse::<usize>().ok());
    let (Some(from), Some(len), Some(pts), Some(pmac)) = (
        from,
        len,
        hdr(n::RAFT_SNAPSHOT_PRE_TS_HEADER),
        hdr(n::RAFT_SNAPSHOT_PRE_MAC_HEADER),
    ) else {
        return Err((7, "missing snapshot pre-stamp"));
    };
    let binding = n::raft_snapshot_pre_binding(from, self_id, len);
    if !dtfs_common::crypto::verify_mgr_rpc_auth(secret, n::RAFT_SNAPSHOT_PRE_METHOD, pts, pmac, &binding, now) {
        return Err((7, "invalid snapshot pre-stamp"));
    }
    Ok(len.saturating_add(RAFT_FRAME_OVERHEAD).min(RAFT_MAX_BODY_BYTES).max(grpc_cap))
}

impl tonic::server::NamedService for RaftTransportSvc {
    const NAME: &'static str = "dtfs.manager.RaftTransport";
}

impl tower::Service<tonic::codegen::http::Request<tonic::body::Body>> for RaftTransportSvc {
    type Response = tonic::codegen::http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: tonic::codegen::http::Request<tonic::body::Body>) -> Self::Future {
        let handler = self.handler.clone();

        let secret = self.secret.clone();
        let require_auth = self.require_auth;
        let max_body = self.max_body_bytes;
        let self_id = self.self_id;
        let snapshot_slot = self.snapshot_slot.clone();

        Box::pin(async move {
            let (parts, body) = req.into_parts();

            let hdr = |k: &str| parts.headers.get(k).and_then(|v| v.to_str().ok());
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);

            // Issue #113 — header-only checks before buffering any body; only
            // a pre-stamped snapshot-path RPC gets the snapshot-sized cap.
            let snapshot_path = parts.uri.path() == dtfs_raft::network::RAFT_SNAPSHOT_PATH;
            let max_body =
                match raft_pre_body_check(&secret, require_auth, snapshot_path, self_id, max_body, now, hdr) {
                    Ok(cap) => cap,
                    Err((code, why)) => {
                        warn!(reason = why, "RaftTransport: rejected unauthenticated Raft RPC");
                        return Ok(grpc_error_response(code, "raft rpc authentication failed"));
                    }
                };
            // Refuse (not queue) a second concurrent snapshot: only the leader
            // sends them, openraft retries, and a queue would hold sockets open.
            let _slot = if snapshot_path {
                match snapshot_slot.try_acquire_owned() {
                    Ok(p) => Some(p),
                    Err(_) => {
                        warn!("RaftTransport: snapshot install already in progress; refusing another");
                        return Ok(grpc_error_response(14, "snapshot install busy"));
                    }
                }
            } else {
                None
            };

            // Bounded collect: tonic's decode cap does not apply to this
            // hand-rolled service, so an unbounded body would buffer in RAM.
            let collected = http_body_util::Limited::new(body, max_body).collect().await;
            let body_bytes = match collected {
                Ok(c) => c.to_bytes(),
                Err(e) if e.downcast_ref::<http_body_util::LengthLimitError>().is_some() => {
                    warn!(limit = max_body, "RaftTransport: request body over limit");
                    return Ok(grpc_error_response(8, "raft rpc body too large"));
                }
                Err(_) => return Ok(grpc_error_response(13, "body read error")),
            };

            let payload_bytes = match decode_raft_payload(body_bytes) {
                Ok(p) => p,
                Err(msg) => {
                    warn!(error = msg, "RaftTransport: malformed request framing");
                    return Ok(grpc_error_response(3, msg));
                }
            };

            // The MAC covers the payload, so auth runs after the (bounded) read.
            let binding = match hdr(dtfs_raft::network::RAFT_RPC_FROM_HEADER)
                .and_then(|f| f.parse::<u64>().ok())
            {
                Some(from) => {
                    dtfs_raft::network::raft_rpc_binding(from, self_id, &payload_bytes)
                }
                None => String::new(),
            };
            match raft_auth_decision(
                &secret,
                require_auth,
                hdr(dtfs_common::crypto::DATA_RPC_AUTH_TS),
                hdr(dtfs_common::crypto::DATA_RPC_AUTH_MAC),
                &binding,
                now,
            ) {
                RaftAuthDecision::Accept => {}
                RaftAuthDecision::AcceptUnsigned => warn_unsigned_raft_rpc(now),
                RaftAuthDecision::Reject(why) => {
                    warn!(reason = why, "RaftTransport: rejected unauthenticated Raft RPC");
                    return Ok(grpc_error_response(7, "raft rpc authentication failed"));
                }
            }

            tracing::debug!(
                len = payload_bytes.len(),
                head = ?payload_bytes.iter().take(40).collect::<Vec<_>>(),
                "RaftTransport: decoded request payload",
            );

            match handler.handle_rpc(payload_bytes).await {
                Ok(resp_bytes) => {
                    // Frame the response symmetrically: prost-encode the
                    // `bytes` field (tag 0x0A + varint length + payload),
                    // then gRPC-frame that with the 5-byte length prefix.
                    let mut prost = bytes::BytesMut::with_capacity(1 + 10 + resp_bytes.len());
                    prost.extend_from_slice(&[0x0A]); // field 1, wire type 2
                    let mut val = resp_bytes.len() as u64;
                    loop {
                        let mut b = (val & 0x7F) as u8;
                        val >>= 7;
                        if val != 0 {
                            b |= 0x80;
                            prost.extend_from_slice(&[b]);
                        } else {
                            prost.extend_from_slice(&[b]);
                            break;
                        }
                    }
                    prost.extend_from_slice(&resp_bytes);
                    let prost = prost.freeze();

                    let mut framed = bytes::BytesMut::with_capacity(5 + prost.len());
                    framed.extend_from_slice(&[0u8]); // not compressed
                    framed.extend_from_slice(&(prost.len() as u32).to_be_bytes());
                    framed.extend_from_slice(&prost);
                    let resp = tonic::codegen::http::Response::builder()
                        .status(200)
                        .header("content-type", "application/grpc")
                        .header("grpc-status", "0")
                        .body(tonic::body::Body::new(
                            http_body_util::Full::new(framed.freeze()),
                        ))
                        .unwrap();
                    Ok(resp)
                }
                Err(status) => {
                    let code = status.code() as i32;
                    let resp = tonic::codegen::http::Response::builder()
                        .status(200)
                        .header("content-type", "application/grpc")
                        .header("grpc-status", code.to_string())
                        .header("grpc-message", status.message())
                        .body(tonic::body::Body::empty())
                        .unwrap();
                    Ok(resp)
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

pub async fn run_grpc_server(
    config: ManagerConfig,
    tls: dtfs_common::tls::TlsContext,
    metadata_service: MetadataService,
    raft: Arc<DtfsRaft>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let addr: std::net::SocketAddr = config.grpc.listen_addr.parse().map_err(|e| {
        anyhow::anyhow!("invalid grpc.listen_addr '{}': {e}", config.grpc.listen_addr)
    })?;

    let raft_svc = RaftTransportSvc::new(RaftTransportHandler::new(raft), &config);

    // Bug CK — bind the listener explicitly BEFORE logging "listening", so a
    // failure (e.g. the Tailscale interface not up yet at boot →
    // EADDRNOTAVAIL) surfaces as a clear, typed error that propagates to
    // `main` and forces a non-zero exit (systemd then retries). Pre-CK
    // tonic bound internally, the failure was a generic "transport error",
    // and the "listening" line was logged even when the bind then failed.
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| crate::bind_error("gRPC", addr, e))?;

    tracing::info!(
        "Manager gRPC server listening on {} ({})",
        addr,
        if tls.is_enabled() { "mTLS" } else { "plaintext" }
    );

    // CF-6 — when the [tls] context resolved certificates, every incoming
    // connection (ManagerService AND the Raft transport) must complete a
    // mutual-TLS handshake against the cluster CA.
    let mut builder = Server::builder();
    if let Some(server_tls) = tls.server_tls() {
        builder = builder
            .tls_config(server_tls)
            .map_err(|e| anyhow::anyhow!("manager gRPC TLS config: {e}"))?;
    }

    // CR2-8-1 (CMN-3) — the listener docs/src/configuration/manager.md
    // documents as governed by [grpc].max_message_size_mb had never
    // actually applied it (tonic's 4 MB default silently stayed in
    // force). Size it from the same shared helper the data-node listener
    // and every gRPC client stub in this crate use.
    let max_message_bytes =
        dtfs_common::config::max_message_bytes(config.grpc.max_message_size_mb);

    // Bug U — stop accepting new connections when the shutdown signal fires.
    // In-flight requests continue until they complete; main is responsible
    // for any post-server cleanup (Raft snapshot, audit flush).
    builder
        .add_service(
            ManagerServiceServer::new(metadata_service)
                .max_decoding_message_size(max_message_bytes)
                .max_encoding_message_size(max_message_bytes),
        )
        .add_service(raft_svc)
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
            shutdown,
        )
        .await?;

    Ok(())
}

#[cfg(test)]
mod cr2_8_1_manager_listener_tests {
    /// CR2-8-1 (CMN-3) — the manager listener wiring is a straight-line
    /// call into the shared `dtfs_common::config::max_message_bytes`
    /// helper (already unit-tested generically in dtfs-common); this
    /// pins the specific value `run_grpc_server` derives from a
    /// `GrpcConfig` shaped like the manager's, so a future edit that
    /// swaps in a different field or drops the `* 1024 * 1024` factor
    /// fails here rather than only at a live listener.
    #[test]
    fn max_message_bytes_matches_manager_grpc_config() {
        let cfg = dtfs_common::config::GrpcConfig {
            listen_addr: "0.0.0.0:9401".to_string(),
            max_message_size_mb: 256,
            max_concurrent_chunk_writes: 8,
        };
        assert_eq!(
            dtfs_common::config::max_message_bytes(cfg.max_message_size_mb),
            256 * 1024 * 1024
        );
    }
}

#[cfg(test)]
mod raft_transport_hardening_tests {
    use super::*;

    const SECRET: &str = "s3cret";

    fn binding() -> String {
        dtfs_raft::network::raft_rpc_binding(1, 2, b"body")
    }

    fn stamp() -> (String, String) {
        dtfs_common::crypto::stamp_mgr_rpc_auth(
            SECRET,
            dtfs_raft::network::RAFT_RPC_AUTH_METHOD,
            &binding(),
        )
        .unwrap()
    }

    #[test]
    fn mac_bound_to_body_and_target() {
        let (ts, mac) = stamp();
        let d = |b: &str| raft_auth_decision(SECRET, true, Some(&ts), Some(&mac), b, now());
        assert_eq!(d(&binding()), RaftAuthDecision::Accept);
        // tampered body
        let tampered = dtfs_raft::network::raft_rpc_binding(1, 2, b"bodY");
        assert_eq!(d(&tampered), RaftAuthDecision::Reject("invalid stamp"));
        // replayed against a different target
        let wrong_to = dtfs_raft::network::raft_rpc_binding(1, 3, b"body");
        assert_eq!(d(&wrong_to), RaftAuthDecision::Reject("invalid stamp"));
        // spoofed sender
        let wrong_from = dtfs_raft::network::raft_rpc_binding(9, 2, b"body");
        assert_eq!(d(&wrong_from), RaftAuthDecision::Reject("invalid stamp"));
    }

    fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    #[test]
    fn valid_stamp_accepted_even_when_required() {
        let (ts, mac) = stamp();
        assert_eq!(
            raft_auth_decision(SECRET, true, Some(&ts), Some(&mac), &binding(), now()),
            RaftAuthDecision::Accept
        );
    }

    #[test]
    fn missing_stamp_permissive_vs_required() {
        assert_eq!(
            raft_auth_decision(SECRET, false, None, None, &binding(), now()),
            RaftAuthDecision::AcceptUnsigned
        );
        assert_eq!(
            raft_auth_decision(SECRET, true, None, None, &binding(), now()),
            RaftAuthDecision::Reject("missing stamp")
        );
    }

    #[test]
    fn bad_mac_rejected_even_when_permissive() {
        let (ts, mac) = stamp();
        let mut bad = mac.clone();
        bad.replace_range(0..1, if mac.starts_with('0') { "1" } else { "0" });
        assert_eq!(
            raft_auth_decision(SECRET, false, Some(&ts), Some(&bad), &binding(), now()),
            RaftAuthDecision::Reject("invalid stamp")
        );
        // Wrong secret on the verifier side is the same failure.
        assert_eq!(
            raft_auth_decision("other", false, Some(&ts), Some(&mac), &binding(), now()),
            RaftAuthDecision::Reject("invalid stamp")
        );
    }

    #[test]
    fn half_stamp_rejected_even_when_permissive() {
        let (ts, _) = stamp();
        assert_eq!(
            raft_auth_decision(SECRET, false, Some(&ts), None, &binding(), now()),
            RaftAuthDecision::Reject("incomplete stamp")
        );
    }

    #[test]
    fn skew_boundary() {
        let (ts, mac) = stamp();
        let t: u64 = ts.parse().unwrap();
        let max = dtfs_common::crypto::DATA_RPC_MAX_SKEW_SECS;
        assert_eq!(
            raft_auth_decision(SECRET, false, Some(&ts), Some(&mac), &binding(), t + max),
            RaftAuthDecision::Accept
        );
        assert_eq!(
            raft_auth_decision(SECRET, false, Some(&ts), Some(&mac), &binding(), t + max + 1),
            RaftAuthDecision::Reject("invalid stamp")
        );
    }

    #[test]
    fn stamp_without_from_header_rejected_when_required_else_unsigned() {
        // A legacy stamp (empty binding) must not authorize an arbitrary body.
        let (ts, mac) = dtfs_common::crypto::stamp_mgr_rpc_auth(
            SECRET,
            dtfs_raft::network::RAFT_RPC_AUTH_METHOD,
            "",
        )
        .unwrap();
        assert_eq!(
            raft_auth_decision(SECRET, true, Some(&ts), Some(&mac), "", now()),
            RaftAuthDecision::Reject("missing sender header")
        );
        assert_eq!(
            raft_auth_decision(SECRET, false, Some(&ts), Some(&mac), "", now()),
            RaftAuthDecision::AcceptUnsigned
        );
    }

    #[test]
    fn empty_secret_skips_auth() {
        assert_eq!(raft_auth_decision("", true, None, None, &binding(), now()), RaftAuthDecision::Accept);
    }

    fn frame(prost: &[u8]) -> Bytes {
        let mut v = vec![0u8];
        v.extend_from_slice(&(prost.len() as u32).to_be_bytes());
        v.extend_from_slice(prost);
        Bytes::from(v)
    }

    #[test]
    fn decode_roundtrip_and_clamp() {
        let body = frame(&[0x0A, 3, b'a', b'b', b'c']);
        assert_eq!(decode_raft_payload(body).unwrap(), Bytes::from_static(b"abc"));
        // Declared length past the end clamps instead of panicking.
        let body = frame(&[0x0A, 100, b'a']);
        assert_eq!(decode_raft_payload(body).unwrap(), Bytes::from_static(b"a"));
    }

    #[test]
    fn decode_malformed_varint_is_error_not_panic() {
        // 11 continuation bytes: never terminates within 10 bytes.
        let mut p = vec![0x0A];
        p.extend_from_slice(&[0xFF; 11]);
        assert_eq!(decode_raft_payload(frame(&p)), Err("bad varint length"));
        // Varint cut off by end of body.
        assert_eq!(decode_raft_payload(frame(&[0x0A, 0x80])), Err("bad varint length"));
        // Huge declared length with a max-width varint: clamped, no overflow.
        let mut p = vec![0x0A];
        p.extend_from_slice(&[0xFF; 9]);
        p.push(0x01);
        p.extend_from_slice(b"xy");
        assert_eq!(decode_raft_payload(frame(&p)).unwrap(), Bytes::from_static(b"xy"));
        // Frame length of u32::MAX does not overflow.
        let body = Bytes::from_static(&[0, 0xFF, 0xFF, 0xFF, 0xFF, 0x0A, 1, b'z']);
        assert_eq!(decode_raft_payload(body).unwrap(), Bytes::from_static(b"z"));
    }

    #[tokio::test]
    async fn oversize_body_rejected_with_resource_exhausted() {
        let big = Bytes::from(vec![0u8; 64]);
        let limited = http_body_util::Limited::new(http_body_util::Full::new(big), 16);
        let err = limited.collect().await.unwrap_err();
        assert!(err.downcast_ref::<http_body_util::LengthLimitError>().is_some());
        let resp = grpc_error_response(8, "raft rpc body too large");
        assert_eq!(resp.headers().get("grpc-status").unwrap(), "8");
    }

    async fn svc_with(
        mb: u32,
        secret: &str,
        require: bool,
    ) -> (RaftTransportSvc, crate::api::test_support::Harness) {
        let h = crate::api::test_support::harness().await;
        let mut cfg = h.state.config.clone();
        cfg.grpc.max_message_size_mb = mb;
        cfg.cluster.shared_secret = secret.to_string();
        cfg.cluster.require_raft_auth = require;
        (RaftTransportSvc::new(RaftTransportHandler::new(h.state.raft.clone()), &cfg), h)
    }

    fn grpc_status(resp: &tonic::codegen::http::Response<tonic::body::Body>) -> String {
        resp.headers().get("grpc-status").unwrap().to_str().unwrap().to_string()
    }

    const SNAP: &str = dtfs_raft::network::RAFT_SNAPSHOT_PATH;
    const RPC: &str = "/dtfs.manager.RaftTransport/RaftRpc";

    /// 2 MiB RaftMessage (unparseable inner payload), returned as (json, framed body).
    fn big_msg(kind: RaftRpcKind) -> (Vec<u8>, Bytes) {
        let msg = RaftMessage { kind, payload: "x".repeat(2 * 1024 * 1024) };
        let json = serde_json::to_vec(&msg).unwrap();
        let mut prost = vec![0x0A];
        let mut v = json.len() as u64;
        while v >= 0x80 {
            prost.push((v as u8) | 0x80);
            v >>= 7;
        }
        prost.push(v as u8);
        prost.extend_from_slice(&json);
        (json, frame(&prost))
    }

    /// Reading this body errors (inner limit), so a read would answer 8/13.
    fn unreadable() -> tonic::body::Body {
        tonic::body::Body::new(http_body_util::Limited::new(
            http_body_util::Full::new(Bytes::from_static(b"must not be read")),
            1,
        ))
    }

    async fn call(
        svc: &mut RaftTransportSvc,
        path: &str,
        headers: &[(&str, String)],
        body: tonic::body::Body,
    ) -> String {
        use tower::Service;
        let mut b = tonic::codegen::http::Request::builder().uri(path);
        for (k, v) in headers {
            b = b.header(*k, v);
        }
        grpc_status(&svc.call(b.body(body).unwrap()).await.unwrap())
    }

    /// Body stamp (+ snapshot pre-stamp) as a fully signed new sender sends it.
    fn signed(svc: &RaftTransportSvc, json: &[u8], ts_override: Option<u64>) -> Vec<(&'static str, String)> {
        use dtfs_raft::network as n;
        let from = 42u64;
        let (mut ts, mut mac) = dtfs_common::crypto::stamp_mgr_rpc_auth(
            SECRET,
            n::RAFT_RPC_AUTH_METHOD,
            &n::raft_rpc_binding(from, svc.self_id, json),
        )
        .unwrap();
        if let Some(t) = ts_override {
            ts = t.to_string();
            mac = "00".repeat(32);
        }
        let pre = n::raft_snapshot_pre_binding(from, svc.self_id, json.len());
        let (pts, pmac) =
            dtfs_common::crypto::stamp_mgr_rpc_auth(SECRET, n::RAFT_SNAPSHOT_PRE_METHOD, &pre).unwrap();
        vec![
            (n::RAFT_RPC_FROM_HEADER, from.to_string()),
            (dtfs_common::crypto::DATA_RPC_AUTH_TS, ts),
            (dtfs_common::crypto::DATA_RPC_AUTH_MAC, mac),
            (n::RAFT_SNAPSHOT_LEN_HEADER, json.len().to_string()),
            (n::RAFT_SNAPSHOT_PRE_TS_HEADER, pts),
            (n::RAFT_SNAPSHOT_PRE_MAC_HEADER, pmac),
        ]
    }

    /// Issue #113 — a snapshot body above `grpc.max_message_size_mb` on the
    /// snapshot path reaches the handler instead of status 8 forever.
    #[tokio::test]
    async fn issue_113_snapshot_body_above_grpc_cap_is_accepted() {
        let (mut svc, _h) = svc_with(1, "", false).await;
        let (_, body) = big_msg(RaftRpcKind::Snapshot);
        // Unparseable snapshot payload -> handler answers empty with status 0.
        assert_eq!(call(&mut svc, SNAP, &[], tonic::body::Body::new(http_body_util::Full::new(body))).await, "0");
    }

    /// Issue #113 — with a secret, a fully signed snapshot above the gRPC cap
    /// is accepted.
    #[tokio::test]
    async fn issue_113_signed_snapshot_above_grpc_cap_is_accepted() {
        let (mut svc, _h) = svc_with(1, SECRET, true).await;
        let (json, body) = big_msg(RaftRpcKind::Snapshot);
        let h = signed(&svc, &json, None);
        assert_eq!(call(&mut svc, SNAP, &h, tonic::body::Body::new(http_body_util::Full::new(body))).await, "0");
    }

    /// Issue #113 negative — AppendEntries keeps the gRPC cap.
    #[tokio::test]
    async fn issue_113_append_entries_above_grpc_cap_rejected() {
        let (mut svc, _h) = svc_with(1, "", false).await;
        let (_, body) = big_msg(RaftRpcKind::AppendEntries);
        assert_eq!(call(&mut svc, RPC, &[], tonic::body::Body::new(http_body_util::Full::new(body))).await, "8");
    }

    /// Issue #113 negative — with auth required, an unstamped RPC is refused
    /// before its body is buffered.
    #[tokio::test]
    async fn issue_113_unstamped_rpc_rejected_before_body_read() {
        let (mut svc, _h) = svc_with(1, SECRET, true).await;
        assert_eq!(call(&mut svc, RPC, &[], unreadable()).await, "7");
    }

    /// Issue #113 negative — a stale-ts AppendEntries is refused before its
    /// body is buffered, even in permissive mode; the skew boundary passes.
    #[tokio::test]
    async fn issue_113_stale_stamp_rejected_before_body_read() {
        let (mut svc, _h) = svc_with(1, SECRET, false).await;
        let max = dtfs_common::crypto::DATA_RPC_MAX_SKEW_SECS;
        let h = signed(&svc, b"", Some(now() - max - 5));
        assert_eq!(call(&mut svc, RPC, &h, unreadable()).await, "7");
        // Within the window: the body is read (and fails to read -> not 7).
        let h = signed(&svc, b"", Some(now() - max + 5));
        assert_ne!(call(&mut svc, RPC, &h, unreadable()).await, "7");
    }

    /// Issue #113 negative — the snapshot path with a secret needs a valid
    /// pre-stamp before any body is read.
    #[tokio::test]
    async fn issue_113_snapshot_path_needs_pre_stamp() {
        let (mut svc, _h) = svc_with(1, SECRET, false).await;
        assert_eq!(call(&mut svc, SNAP, &[], unreadable()).await, "7");
        let mut h = signed(&svc, b"abc", None);
        h[3].1 = "999999999".to_string(); // declared length no longer matches the pre-MAC
        assert_eq!(call(&mut svc, SNAP, &h, unreadable()).await, "7");
    }

    /// Issue #113 negative — a second concurrent snapshot body is refused.
    #[tokio::test]
    async fn issue_113_second_concurrent_snapshot_refused() {
        let (mut svc, _h) = svc_with(1, "", false).await;
        let _held = svc.snapshot_slot.clone().try_acquire_owned().unwrap();
        assert_eq!(call(&mut svc, SNAP, &[], unreadable()).await, "14");
        drop(_held);
        assert_ne!(call(&mut svc, SNAP, &[], unreadable()).await, "14");
    }
}
