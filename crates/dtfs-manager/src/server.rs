use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use dtfs_common::config::ManagerConfig;
use dtfs_proto::dtfs::manager::manager_service_server::ManagerServiceServer;
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

        let response_payload = match msg.kind {
            RaftRpcKind::AppendEntries => self.handle_append_entries(&msg.payload).await,
            RaftRpcKind::Vote => self.handle_vote(&msg.payload).await,
            RaftRpcKind::Snapshot => self.handle_snapshot(&msg.payload).await,
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
                        warn!("AppendEntries error: {}", e);
                        String::new()
                    }
                }
            }
            Err(e) => {
                warn!("Failed to parse AppendEntries: {}", e);
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
                        warn!("Vote error: {}", e);
                        String::new()
                    }
                }
            }
            Err(e) => {
                warn!("Failed to parse Vote: {}", e);
                String::new()
            }
        }
    }

    async fn handle_snapshot(&self, payload: &str) -> String {
        #[derive(Deserialize)]
        struct SnapshotPayload {
            vote: openraft::type_config::alias::VoteOf<dtfs_raft::TypeConfig>,
            meta: dtfs_raft::SnapshotMeta,
            data_base64: String,
        }

        match serde_json::from_str::<SnapshotPayload>(payload) {
            Ok(snap) => {
                debug!("RaftTransport: InstallSnapshot");
                let data = base64_decode(&snap.data_base64);
                let snapshot = openraft::storage::Snapshot {
                    meta: snap.meta,
                    snapshot: std::io::Cursor::new(data),
                };
                match self.raft.install_full_snapshot(snap.vote, snapshot).await {
                    Ok(resp) => serde_json::to_string(&resp).unwrap_or_default(),
                    Err(e) => {
                        warn!("InstallSnapshot error: {}", e);
                        String::new()
                    }
                }
            }
            Err(e) => {
                warn!("Failed to parse Snapshot: {}", e);
                String::new()
            }
        }
    }
}

/// Minimal base64 decoder.
fn base64_decode(input: &str) -> Vec<u8> {
    const TABLE: [u8; 256] = {
        let mut t = [255u8; 256];
        let chars = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut i = 0;
        while i < 64 {
            t[chars[i] as usize] = i as u8;
            i += 1;
        }
        t
    };

    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    let mut buf: u32 = 0;
    let mut bits: u32 = 0;

    for &b in bytes {
        if b == b'=' {
            break;
        }
        let val = TABLE[b as usize];
        if val == 255 {
            continue;
        }
        buf = (buf << 6) | val as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
            buf &= (1 << bits) - 1;
        }
    }
    out
}

// ---------------------------------------------------------------------------
// RaftTransport as a tonic-compatible service
// ---------------------------------------------------------------------------

/// A tonic-compatible service for the Raft transport.
#[derive(Clone)]
struct RaftTransportSvc {
    handler: RaftTransportHandler,
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

        Box::pin(async move {
            // Collect body into bytes using http_body_util::BodyExt.
            let (_parts, body) = req.into_parts();
            let collected = body.collect().await;
            let body_bytes = match collected {
                Ok(c) => c.to_bytes(),
                Err(_) => {
                    let resp = tonic::codegen::http::Response::builder()
                        .status(200)
                        .header("content-type", "application/grpc")
                        .header("grpc-status", "13")
                        .body(tonic::body::Body::empty())
                        .unwrap();
                    return Ok(resp);
                }
            };

            // The client uses tonic::client::Grpc::unary() with
            // ProstCodec<Bytes, Bytes>; prost wraps each top-level `bytes`
            // field as a length-prefixed entry (`tag<<3|wire=2`, then varint
            // length, then payload). Then gRPC additionally wraps the whole
            // protobuf message in its own length-prefix frame:
            //   [1 byte compressed flag][4 byte big-endian length][prost message]
            // So to recover the JSON payload we have to peel BOTH layers.
            // (Without this the JSON parser sees framing bytes and fails with
            // "bad raft msg: expected value at line 1 column 1" — breaking
            // multi-voter Raft elections, F-009 v2.)
            let payload_bytes = if body_bytes.len() >= 5 {
                let outer_len = u32::from_be_bytes([
                    body_bytes[1],
                    body_bytes[2],
                    body_bytes[3],
                    body_bytes[4],
                ]) as usize;
                let outer_end = (5 + outer_len).min(body_bytes.len());
                let inner = body_bytes.slice(5..outer_end);
                // The first byte of the prost-encoded payload is the field
                // tag (0x0A for field 1, wire type 2 = LEN). Skip it, then
                // read a varint for the length, then take the rest.
                if !inner.is_empty() && inner[0] == 0x0A {
                    let (varlen, varbytes_consumed) = {
                        let mut value: u64 = 0;
                        let mut shift = 0;
                        let mut consumed = 0;
                        for (i, b) in inner[1..].iter().enumerate() {
                            consumed = i + 1;
                            value |= ((b & 0x7F) as u64) << shift;
                            if b & 0x80 == 0 {
                                break;
                            }
                            shift += 7;
                            if shift > 63 {
                                break;
                            }
                        }
                        (value as usize, consumed)
                    };
                    let start = 1 + varbytes_consumed;
                    let end = (start + varlen).min(inner.len());
                    inner.slice(start..end)
                } else {
                    inner
                }
            } else {
                body_bytes
            };

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
    metadata_service: MetadataService,
    raft: Arc<DtfsRaft>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let addr = config.grpc.listen_addr.parse()?;

    if !config.tls.mode.eq_ignore_ascii_case("disabled")
        && !config.tls.mode.eq_ignore_ascii_case("off")
    {
        tracing::warn!(
            "TLS mode '{}' is set but manager gRPC still listens in plaintext; see ISSUES_TO_FIX.md §10",
            config.tls.mode
        );
    }

    let raft_svc = RaftTransportSvc {
        handler: RaftTransportHandler::new(raft),
    };

    tracing::info!("Manager gRPC server listening on {}", addr);

    // Bug U — stop accepting new connections when the shutdown signal fires.
    // In-flight requests continue until they complete; main is responsible
    // for any post-server cleanup (Raft snapshot, audit flush).
    Server::builder()
        .add_service(ManagerServiceServer::new(metadata_service))
        .add_service(raft_svc)
        .serve_with_shutdown(addr, shutdown)
        .await?;

    Ok(())
}
