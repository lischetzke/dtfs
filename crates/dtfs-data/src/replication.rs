// Chunk replication engine.
// Push replication from this node to a replica target. The pull path
// (`receive_replication`) was dead code and is gone — the manager drives
// all replication through `ReplicateChunk` on the source node.

use std::sync::Arc;
use std::time::Duration;

use dtfs_common::config::DataNodeConfig;
use dtfs_common::types::ChunkId;
use dtfs_proto::dtfs::data::data_service_client::DataServiceClient;
use dtfs_proto::dtfs::data::{PutChunkRequest, PutChunkResponse};
use tokio::io::AsyncReadExt;

use crate::store::ChunkStore;

/// Bug CI follow-up — hard ceiling for one outbound replication (connect +
/// stream + remote commit). Pre-fix there was NO deadline anywhere on the
/// path: a memory-throttled target stalled `put_chunk` indefinitely, the
/// in-flight RPC pinned its buffers, and the manager's sequential dispatch
/// tick wedged behind it (2026-06-06). 120s is ~9x the worst legit case
/// (a 64 MiB chunk over the 40 Mbps LELI uplink ≈ 13s).
const REPLICATE_TIMEOUT: Duration = Duration::from_secs(120);

/// Streaming piece size — matches the put_chunk intake's expectation of
/// ~1 MiB pieces (Bug I).
const STREAM_PIECE_SIZE: usize = 1024 * 1024;

/// CO-5-4 — validate a replication target's `PutChunkResponse` against the
/// chunk we asked it to store. `resp.chunk_id` is the BLAKE3 hash the target
/// independently computed over the bytes it received; if it differs from the
/// source chunk's hash the target stored corrupt/wrong bytes, so the
/// replication MUST fail even though the old code only checked `success`
/// (which a target sets true after its own self-consistent commit). Integrity
/// is the prime directive: a replica must be byte-identical or it is not a
/// replica.
///
/// An EMPTY `chunk_id` means the target didn't echo its computed hash (proto3
/// string default) — i.e. a pre-CO peer with no hash to compare. There we fall
/// back to the old `success`-only check rather than fail a healthy replica
/// across a version boundary; every current data node populates it.
fn validate_put_response(expected_hex: &str, resp: &PutChunkResponse) -> anyhow::Result<()> {
    if !resp.success {
        anyhow::bail!("remote rejected the chunk (success=false)");
    }
    if !resp.chunk_id.is_empty() && resp.chunk_id != expected_hex {
        anyhow::bail!(
            "replicated chunk hash mismatch: target computed {}, expected {}",
            resp.chunk_id,
            expected_hex,
        );
    }
    Ok(())
}

/// Manages chunk replication between data nodes.
pub struct ReplicationManager {
    store: Arc<ChunkStore>,
    config: DataNodeConfig,
    /// CF-6 — resolved [tls] context; node-to-node replication speaks
    /// mTLS when enabled.
    tls: dtfs_common::tls::TlsContext,
}

impl ReplicationManager {
    /// Create a new replication manager.
    pub fn new(
        store: Arc<ChunkStore>,
        config: DataNodeConfig,
        tls: dtfs_common::tls::TlsContext,
    ) -> Self {
        Self { store, config, tls }
    }

    /// Replicate a chunk from this node to a remote target data node.
    ///
    /// Bug CI follow-up — streams the chunk straight from its on-disk
    /// `.dat` file in ~1 MiB pieces (Bug I parity: the pre-fix version
    /// buffered the WHOLE chunk in memory and copied every piece again
    /// via `to_vec`, which pinned the node at its memory cap during the
    /// 2026-06-06 storm). The entire operation is bounded by
    /// `REPLICATE_TIMEOUT`.
    pub async fn replicate_to(
        &self,
        chunk_id: &ChunkId,
        target_addr: &str,
        target_tier: &str,
    ) -> anyhow::Result<()> {
        tracing::info!("Replicating chunk {} to {}", chunk_id, target_addr);

        // Resolve the on-disk path; absent chunk keeps the exact
        // "Chunk not found" wording the manager's Bug AF fast-fail matches.
        let (dat_path, total) = self
            .store
            .chunk_file_path(chunk_id)
            .ok_or_else(|| anyhow::anyhow!("Chunk not found: {}", chunk_id.as_hex()))?;

        // Bug W (part 2) — `target_addr` comes from `NodeInfo.grpc_addr` via
        // the manager-relayed `ReplicateChunkRequest.target_address`. The
        // manager stores grpc_addr already URL-formatted ("http://ip:port");
        // `TlsContext::endpoint_url` accepts both forms and rewrites the
        // scheme to match this node's TLS state (CF-6).
        let endpoint = self.tls.endpoint_url(target_addr);

        let chunk_hex = chunk_id.as_hex();
        let tier = target_tier.to_string();

        let work = async {
            let mut client = self.tls.connect(&endpoint).await.map(DataServiceClient::new)?;

            let (tx, rx) = tokio::sync::mpsc::channel::<PutChunkRequest>(4);

            // Feeder: read the .dat sequentially in ~1 MiB pieces. Only the
            // channel depth (4) worth of pieces is ever in memory.
            let feeder = {
                let chunk_hex = chunk_hex.clone();
                let dat_path = dat_path.clone();
                async move {
                    let mut f = tokio::fs::File::open(&dat_path).await?;
                    let mut sent: u64 = 0;
                    let mut first = true;
                    loop {
                        let remaining = total.saturating_sub(sent);
                        if remaining == 0 {
                            break;
                        }
                        let want = remaining.min(STREAM_PIECE_SIZE as u64) as usize;
                        let mut piece = vec![0u8; want];
                        f.read_exact(&mut piece).await?;
                        sent += want as u64;

                        let req = PutChunkRequest {
                            chunk_id: if first { chunk_hex.clone() } else { String::new() },
                            volume_id: String::new(),
                            // AV-5-9 — preserve the manager-resolved tier
                            // (e.g. "warm" for a cache promotion). Empty
                            // stays empty for legacy callers.
                            tier: if first { tier.clone() } else { String::new() },
                            data: piece,
                        };
                        first = false;
                        if tx.send(req).await.is_err() {
                            anyhow::bail!("replication stream closed prematurely");
                        }
                    }
                    Ok::<(), anyhow::Error>(())
                }
            };

            let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
            let put = client.put_chunk(stream);
            // Drive feeder + RPC together so a feeder I/O error surfaces
            // instead of the RPC hanging on a silently-dropped sender.
            let (fed, response) = tokio::join!(feeder, put);
            // RPC error first — when the remote rejects the stream the
            // feeder also fails (receiver dropped), but the RPC status is
            // the informative one.
            let resp = response?.into_inner();
            fed?;

            // CO-5-4 — verify the target stored byte-identical content
            // (matching BLAKE3 hash), not merely that it self-reported
            // success.
            validate_put_response(&chunk_hex, &resp).map_err(|e| {
                anyhow::anyhow!(
                    "Replication of chunk {} to {} failed: {}",
                    chunk_hex,
                    target_addr,
                    e,
                )
            })?;
            tracing::info!(
                "Successfully replicated chunk {} ({} bytes) to {}",
                chunk_hex,
                resp.size,
                target_addr,
            );
            Ok(())
        };

        match tokio::time::timeout(REPLICATE_TIMEOUT, work).await {
            Ok(res) => res,
            Err(_) => anyhow::bail!(
                "replication of chunk {} to {} timed out after {:?}",
                chunk_hex,
                target_addr,
                REPLICATE_TIMEOUT,
            ),
        }
    }

    /// Get a reference to the underlying config.
    pub fn config(&self) -> &DataNodeConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resp(chunk_id: &str, success: bool) -> PutChunkResponse {
        PutChunkResponse {
            chunk_id: chunk_id.to_string(),
            size: 4096,
            checksum: chunk_id.to_string(),
            success,
            disk_id: 0,
        }
    }

    // CO-5-4 — a matching hash + success is the only Ok case.
    #[test]
    fn co_5_4_matching_chunk_id_ok() {
        let hex = "a".repeat(64);
        assert!(validate_put_response(&hex, &resp(&hex, true)).is_ok());
    }

    // CO-5-4 — a target that stored different bytes returns a different
    // hash; that MUST fail even though success=true.
    #[test]
    fn co_5_4_wrong_chunk_id_errors() {
        let expected = "a".repeat(64);
        let wrong = "b".repeat(64);
        assert!(validate_put_response(&expected, &resp(&wrong, true)).is_err());
    }

    // CO-5-4 — success=false fails regardless of the hash.
    #[test]
    fn co_5_4_unsuccessful_errors() {
        let hex = "a".repeat(64);
        assert!(validate_put_response(&hex, &resp(&hex, false)).is_err());
    }

    // CO-5-4 — an empty chunk_id (pre-CO target that doesn't echo its hash)
    // falls back to the success-only check instead of failing a healthy
    // replica across a version boundary.
    #[test]
    fn co_5_4_empty_chunk_id_falls_back_to_success() {
        let expected = "a".repeat(64);
        assert!(validate_put_response(&expected, &resp("", true)).is_ok());
        assert!(validate_put_response(&expected, &resp("", false)).is_err());
    }
}
