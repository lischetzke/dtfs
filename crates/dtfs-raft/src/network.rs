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
// CNF-4 — snapshot-send concurrency bound
// ---------------------------------------------------------------------------

/// Max snapshot sends in flight across all peers. Each in-flight send holds
/// roughly 2.7x the snapshot size in heap (base64 string + JSON envelope);
/// with several lagging learners the unbounded fan-out could stack N of
/// those on an 8 GB manager.
const SNAPSHOT_SEND_CONCURRENCY: usize = 2;

static SNAPSHOT_SEND_PERMITS: tokio::sync::Semaphore =
    tokio::sync::Semaphore::const_new(SNAPSHOT_SEND_CONCURRENCY);

/// Acquire a process-wide snapshot-send permit (CNF-4). Split out so tests
/// can exercise the bound directly.
async fn acquire_snapshot_send_permit() -> tokio::sync::SemaphorePermit<'static> {
    SNAPSHOT_SEND_PERMITS
        .acquire()
        .await
        .expect("snapshot-send semaphore is never closed")
}

// ---------------------------------------------------------------------------
// NetworkFactory
// ---------------------------------------------------------------------------

/// Factory that creates `DtfsNetwork` instances for each target node.
#[derive(Debug, Clone)]
pub struct NetworkFactory {
    /// CF-6 — the manager's resolved `[tls]` context; peer channels speak
    /// mTLS when it is enabled, plaintext otherwise.
    pub tls: dtfs_common::tls::TlsContext,
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = DtfsNetwork;

    async fn new_client(&mut self, _target: u64, node: &DtfsNode) -> Self::Network {
        DtfsNetwork {
            addr: node.addr.clone(),
            channel: None,
            tls: self.tls.clone(),
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
    /// CF-6 — TLS context applied when (re)opening the channel.
    tls: dtfs_common::tls::TlsContext,
}

impl DtfsNetwork {
    /// Bug AC — connection and request timeouts. With the previous
    /// "no timeout" config, a tonic Channel could hang indefinitely
    /// when the peer was DOWN but TCP SYN timeouts were long. Under
    /// election storms this monopolized the openraft tick budget
    /// and prevented vote responses from being processed.
    const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
    const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
    /// HTTP/2 keep-alive — pokes the peer every 30s with a ping. If
    /// the peer doesn't respond, the connection is dropped and the
    /// next RPC reopens it. Without keep-alive, a TCP half-open
    /// connection (peer crashed, NAT timeout) lingers and every
    /// AppendEntries hangs to its full timeout.
    const KEEPALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
    const KEEPALIVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

    async fn ensure_channel(&mut self) -> Result<Channel, RPCError<TypeConfig>> {
        if let Some(ref ch) = self.channel {
            return Ok(ch.clone());
        }

        // CF-6 — scheme + client TLS come from the resolved [tls] context.
        let endpoint = self.tls.endpoint_url(&self.addr);
        let channel = self
            .tls
            .endpoint(&self.addr)
            .map_err(|e| unreachable_err(format!("invalid endpoint {endpoint}: {e}")))?
            .connect_timeout(Self::CONNECT_TIMEOUT)
            .timeout(Self::RPC_TIMEOUT)
            .http2_keep_alive_interval(Self::KEEPALIVE_INTERVAL)
            .keep_alive_timeout(Self::KEEPALIVE_TIMEOUT)
            .keep_alive_while_idle(true)
            .tcp_keepalive(Some(Self::KEEPALIVE_INTERVAL))
            .tcp_nodelay(true)
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
        // CNF-4: for snapshot sends `msg.payload` is snapshot-sized; free it
        // before the (potentially slow) network call so only `body` is held.
        drop(msg);

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
        let cancel_fut = cancel;
        tokio::pin!(cancel_fut);

        // CNF-4 — bound cross-peer snapshot-send concurrency. openraft runs
        // one replication task per peer (sends to the SAME peer are already
        // serial); this process-wide semaphore caps how many peers' sends
        // hold a snapshot-sized payload in heap at once. The RAII permit is
        // held across the select! below, so cancellation releases it.
        let _permit = tokio::select! {
            permit = acquire_snapshot_send_permit() => permit,
            closed = &mut cancel_fut => {
                return Err(StreamingError::Closed(closed));
            }
        };

        // The snapshot cursor already owns the bytes — take them without the
        // extra full copy `bytes_from_cursor` would make (CNF-4).
        let snap_bytes = snapshot.snapshot.into_inner();

        #[derive(Serialize)]
        struct SnapshotPayload {
            vote: VoteOf<TypeConfig>,
            meta: crate::SnapshotMeta,
            data_base64: String,
        }

        let data_base64 = base64_encode(&snap_bytes);
        // Free the raw bytes before JSON-serializing: peak heap per send is
        // then base64 + JSON instead of raw + base64 + JSON (CNF-4).
        drop(snap_bytes);

        let payload_struct = SnapshotPayload {
            vote,
            meta: snapshot.meta,
            data_base64,
        };

        let payload = serde_json::to_string(&payload_struct).map_err(|e| {
            StreamingError::StorageError(openraft::StorageError::from_io_error(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Read,
                io::Error::new(io::ErrorKind::InvalidData, e),
            ))
        })?;
        // The base64 string lives on inside `payload`; drop the struct copy.
        drop(payload_struct);

        // Use tokio::select to honour cancellation.
        let rpc_fut = self.send_rpc(RaftRpcKind::Snapshot, payload);
        tokio::pin!(rpc_fut);

        let resp_json = tokio::select! {
            result = &mut rpc_fut => {
                // Bug CL — a snapshot RPC that fails because the FOLLOWER is
                // unreachable / timed out is a NETWORK error, not local
                // storage. Mapping it to StreamingError::StorageError made
                // openraft treat it as a fatal storage failure and SHUT DOWN
                // the leader's RaftCore (prod 2026-06-09: "Write Snapshot(None):
                // Unreachable node Timeout" -> Shutdown -> cluster-wide quorum
                // loss). Route the RPCError through the transient
                // Unreachable/Timeout/Network mapping so openraft just retries
                // replication to that peer instead of dying.
                result.map_err(StreamingError::from)?
            }
            closed = &mut cancel_fut => {
                return Err(StreamingError::Closed(closed));
            }
        };

        let resp: SnapshotResponse<TypeConfig> =
            serde_json::from_str(&resp_json).map_err(|e| {
                // Bug CL — an empty / garbage snapshot response means the peer
                // returned nothing because it is down or unreachable, NOT local
                // snapshot corruption. (Prod 2026-06-09: "Read Snapshot(None):
                // EOF while parsing a value at line 1 column 0" -> fatal
                // Shutdown.) Treat as a transient Unreachable so openraft
                // retries, rather than a fatal StorageError.
                StreamingError::from(unreachable_err(format!(
                    "deserialize snapshot response: {e}"
                )))
            })?;

        Ok(resp)
    }
}

// ---------------------------------------------------------------------------
// Minimal base64 helpers — shared with the snapshot receiver
// (dtfs-manager server.rs `handle_snapshot`). The wire format is part of the
// raft transport contract; keep encode/decode exact inverses.
// ---------------------------------------------------------------------------

pub fn base64_encode(data: &[u8]) -> String {
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

/// Minimal base64 decoder — exact inverse of [`base64_encode`]. Moved here
/// from dtfs-manager so sender and receiver share one implementation
/// (CNF-4); behaviour unchanged.
pub fn base64_decode(input: &str) -> Vec<u8> {
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

#[cfg(test)]
mod cnf4_snapshot_send_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    // CNF-4 — the sender's base64_encode and the receiver's base64_decode
    // (shared from this module; dtfs-manager's handle_snapshot uses it) must
    // invert each other for every length mod 3 (padding cases) and arbitrary
    // binary data. The wire format must stay byte-identical across versions.
    #[test]
    fn base64_round_trip_with_receiver_decoder() {
        let cases: Vec<Vec<u8>> = vec![
            vec![],
            vec![0],
            vec![0, 255],
            vec![1, 2, 3],
            vec![251, 252, 253, 254],
            (0..=255u8).collect(),
            (0..4096u32).map(|i| (i * 31 % 256) as u8).collect(),
        ];
        for data in cases {
            let encoded = base64_encode(&data);
            assert_eq!(
                base64_decode(&encoded),
                data,
                "round-trip failed for len {}",
                data.len()
            );
        }
    }

    // CNF-4 — at most SNAPSHOT_SEND_CONCURRENCY snapshot sends may hold a
    // permit at once; extra senders queue instead of stacking snapshot-sized
    // buffers (~2.7x snapshot size each) in heap.
    #[tokio::test(flavor = "multi_thread")]
    async fn snapshot_send_permits_bound_concurrency() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let in_flight = in_flight.clone();
            let max_seen = max_seen.clone();
            handles.push(tokio::spawn(async move {
                let _permit = acquire_snapshot_send_permit().await;
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let max = max_seen.load(Ordering::SeqCst);
        assert!(
            max <= SNAPSHOT_SEND_CONCURRENCY,
            "max in-flight snapshot sends {max} exceeded cap {SNAPSHOT_SEND_CONCURRENCY}"
        );
    }
}

#[cfg(test)]
mod cl_snapshot_error_tests {
    use super::*;

    // Bug CL — a snapshot-stream RPC failure (peer unreachable / timed out /
    // empty response from a dead peer) MUST classify as a transient
    // StreamingError so openraft retries replication. It must NEVER become a
    // StorageError, which openraft treats as a fatal local-storage failure and
    // responds to by shutting down the leader's RaftCore (prod 2026-06-09
    // cluster-wide quorum loss). full_snapshot now routes RPC errors through
    // `StreamingError::from(RPCError)`; this guards that mapping.
    #[test]
    fn snapshot_rpc_error_is_transient_not_fatal_storage() {
        let e: StreamingError<TypeConfig> =
            StreamingError::from(unreachable_err("peer down".into()));
        assert!(
            matches!(e, StreamingError::Unreachable(_)),
            "snapshot RPC failure must be transient Unreachable, not fatal StorageError"
        );
    }
}
