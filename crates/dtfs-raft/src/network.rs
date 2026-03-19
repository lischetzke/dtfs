//! gRPC Raft transport.
//!
//! Implements `RaftNetworkFactory` and `RaftNetworkV2` using tonic gRPC.
//!
//! The Raft RPC messages (AppendEntries, Vote, InstallSnapshot) are
//! serialized as JSON and sent over a simple generic gRPC service.
//!
//! For the initial implementation we encode all Raft messages as JSON byte
//! payloads and use a single generic `RaftRpc` unary gRPC method.

use std::future::Future;
use std::io;

use openraft::errors::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RaftNetworkFactory, RaftNetworkV2, RPCOption};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{SnapshotOf, VoteOf};
use serde::{Deserialize, Serialize};
use tonic::transport::Channel;

use crate::{DtfsNode, TypeConfig};

// ---------------------------------------------------------------------------
// RPC wrapper types (serialised to JSON over gRPC)
// ---------------------------------------------------------------------------

/// Identifies which Raft RPC variant the message carries.
#[derive(Debug, Serialize, Deserialize)]
enum RaftRpcKind {
    AppendEntries,
    Vote,
    Snapshot,
}

/// Envelope sent/received on the wire.
#[derive(Debug, Serialize, Deserialize)]
struct RaftMessage {
    kind: RaftRpcKind,
    payload: String,
}

// ---------------------------------------------------------------------------
// Helper to construct RPCError::Unreachable
// ---------------------------------------------------------------------------

fn unreachable_err(msg: String) -> RPCError<TypeConfig> {
    RPCError::Unreachable(Unreachable::from_string(msg))
}

// ---------------------------------------------------------------------------
// NetworkFactory
// ---------------------------------------------------------------------------

/// Factory that creates `DtfsNetwork` instances for each target node.
#[derive(Debug, Clone)]
pub struct NetworkFactory;

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = DtfsNetwork;

    async fn new_client(&mut self, _target: u64, node: &DtfsNode) -> Self::Network {
        DtfsNetwork {
            addr: node.addr.clone(),
            channel: None,
        }
    }
}

// ---------------------------------------------------------------------------
// DtfsNetwork — a single connection to one peer
// ---------------------------------------------------------------------------

/// A network connection to a single Raft peer.
///
/// The tonic `Channel` is lazily created on first use and cached.
#[derive(Debug)]
pub struct DtfsNetwork {
    addr: String,
    channel: Option<Channel>,
}

impl DtfsNetwork {
    async fn ensure_channel(&mut self) -> Result<Channel, RPCError<TypeConfig>> {
        if let Some(ref ch) = self.channel {
            return Ok(ch.clone());
        }

        let endpoint = format!("http://{}", self.addr);
        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|e| unreachable_err(format!("invalid endpoint {endpoint}: {e}")))?
            .connect()
            .await
            .map_err(|e| unreachable_err(format!("connect to {endpoint} failed: {e}")))?;

        self.channel = Some(channel.clone());
        Ok(channel)
    }

    /// Send a generic JSON-encoded Raft RPC and get the response bytes back.
    async fn send_rpc(
        &mut self,
        kind: RaftRpcKind,
        payload_json: String,
    ) -> Result<String, RPCError<TypeConfig>> {
        let channel = self.ensure_channel().await?;

        let msg = RaftMessage {
            kind,
            payload: payload_json,
        };
        let body = serde_json::to_vec(&msg)
            .map_err(|e| unreachable_err(format!("serialize request: {e}")))?;

        let request = tonic::Request::new(bytes::Bytes::from(body));

        // Use a raw gRPC call to `dtfs.manager.RaftTransport/RaftRpc`.
        let mut client = tonic::client::Grpc::new(channel);
        client.ready().await.map_err(|e| {
            self.channel = None;
            unreachable_err(format!("grpc not ready: {e}"))
        })?;

        let path = "/dtfs.manager.RaftTransport/RaftRpc"
            .parse()
            .expect("valid path");
        let codec =
            tonic::codec::ProstCodec::<bytes::Bytes, bytes::Bytes>::default();

        let response = client.unary(request, path, codec).await.map_err(|e| {
            self.channel = None;
            unreachable_err(format!("rpc failed: {e}"))
        })?;

        let resp_bytes = response.into_inner();
        let resp_msg: RaftMessage = serde_json::from_slice(&resp_bytes)
            .map_err(|e| unreachable_err(format!("deserialize response: {e}")))?;

        Ok(resp_msg.payload)
    }
}

// ---------------------------------------------------------------------------
// RaftNetworkV2 implementation
// ---------------------------------------------------------------------------

impl RaftNetworkV2<TypeConfig> for DtfsNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        let payload = serde_json::to_string(&rpc)
            .map_err(|e| unreachable_err(format!("serialize append_entries: {e}")))?;

        let resp_json = self.send_rpc(RaftRpcKind::AppendEntries, payload).await?;

        serde_json::from_str(&resp_json)
            .map_err(|e| unreachable_err(format!("deserialize append_entries response: {e}")))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let payload = serde_json::to_string(&rpc)
            .map_err(|e| unreachable_err(format!("serialize vote: {e}")))?;

        let resp_json = self.send_rpc(RaftRpcKind::Vote, payload).await?;

        serde_json::from_str(&resp_json)
            .map_err(|e| unreachable_err(format!("deserialize vote response: {e}")))
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: SnapshotOf<TypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let snap_bytes = crate::snapshot::bytes_from_cursor(snapshot.snapshot).map_err(|e| {
            StreamingError::StorageError(openraft::StorageError::from_io_error(
                openraft::ErrorSubject::Snapshot(Some(snapshot.meta.signature())),
                openraft::ErrorVerb::Read,
                e,
            ))
        })?;

        #[derive(Serialize)]
        struct SnapshotPayload {
            vote: VoteOf<TypeConfig>,
            meta: crate::SnapshotMeta,
            data_base64: String,
        }

        let payload_struct = SnapshotPayload {
            vote,
            meta: snapshot.meta,
            data_base64: base64_encode(&snap_bytes),
        };

        let payload = serde_json::to_string(&payload_struct).map_err(|e| {
            StreamingError::StorageError(openraft::StorageError::from_io_error(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Read,
                io::Error::new(io::ErrorKind::InvalidData, e),
            ))
        })?;

        // Use tokio::select to honour cancellation.
        let rpc_fut = self.send_rpc(RaftRpcKind::Snapshot, payload);
        tokio::pin!(rpc_fut);

        let cancel_fut = cancel;
        tokio::pin!(cancel_fut);

        let resp_json = tokio::select! {
            result = &mut rpc_fut => {
                result.map_err(|e| {
                    StreamingError::StorageError(openraft::StorageError::from_io_error(
                        openraft::ErrorSubject::Snapshot(None),
                        openraft::ErrorVerb::Write,
                        io::Error::new(io::ErrorKind::Other, e.to_string()),
                    ))
                })?
            }
            closed = &mut cancel_fut => {
                return Err(StreamingError::Closed(closed));
            }
        };

        let resp: SnapshotResponse<TypeConfig> =
            serde_json::from_str(&resp_json).map_err(|e| {
                StreamingError::StorageError(openraft::StorageError::from_io_error(
                    openraft::ErrorSubject::Snapshot(None),
                    openraft::ErrorVerb::Read,
                    io::Error::new(io::ErrorKind::InvalidData, e),
                ))
            })?;

        Ok(resp)
    }
}

// ---------------------------------------------------------------------------
// Minimal base64 helper
// ---------------------------------------------------------------------------

fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        out.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}
