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

            match handler.handle_rpc(body_bytes).await {
                Ok(resp_bytes) => {
                    let resp = tonic::codegen::http::Response::builder()
                        .status(200)
                        .header("content-type", "application/grpc")
                        .header("grpc-status", "0")
                        .body(tonic::body::Body::new(
                            http_body_util::Full::new(resp_bytes),
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
) -> anyhow::Result<()> {
    let addr = config.grpc.listen_addr.parse()?;

    let raft_svc = RaftTransportSvc {
        handler: RaftTransportHandler::new(raft),
    };

    tracing::info!("Manager gRPC server listening on {}", addr);

    Server::builder()
        .add_service(ManagerServiceServer::new(metadata_service))
        .add_service(raft_svc)
        .serve(addr)
        .await?;

    Ok(())
}
