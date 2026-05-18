// Chunk replication engine.
// Handles push replication from primary to replica nodes and pull replication
// for receiving chunks from source nodes.

use std::sync::Arc;

use dtfs_common::config::DataNodeConfig;
use dtfs_common::types::ChunkId;
use dtfs_proto::dtfs::data::data_service_client::DataServiceClient;
use dtfs_proto::dtfs::data::{GetChunkRequest, PutChunkRequest};

use crate::store::ChunkStore;

/// Manages chunk replication between data nodes.
pub struct ReplicationManager {
    store: Arc<ChunkStore>,
    config: DataNodeConfig,
}

impl ReplicationManager {
    /// Create a new replication manager.
    pub fn new(store: Arc<ChunkStore>, config: DataNodeConfig) -> Self {
        Self { store, config }
    }

    /// Replicate a chunk from this node to a remote target data node.
    ///
    /// Reads the chunk from the local store, connects to `target_addr` via
    /// gRPC, and streams it using `PutChunk`.
    pub async fn replicate_to(
        &self,
        chunk_id: &ChunkId,
        target_addr: &str,
    ) -> anyhow::Result<()> {
        tracing::info!(
            "Replicating chunk {} to {}",
            chunk_id,
            target_addr,
        );

        // Read chunk from local store.
        let data = self.store.get_chunk(chunk_id).await?;

        // Bug W (part 2) — `target_addr` comes from `NodeInfo.grpc_addr` via
        // the manager-relayed `ReplicateChunkRequest.target_address`. The
        // manager stores grpc_addr already URL-formatted ("http://ip:port"),
        // so prefixing "http://" again would produce `http://http://...`.
        let endpoint = if target_addr.starts_with("http://") || target_addr.starts_with("https://") {
            target_addr.to_string()
        } else {
            format!("http://{}", target_addr)
        };
        let mut client = DataServiceClient::connect(endpoint).await?;

        // Stream the chunk data in 1MB pieces via PutChunk.
        let chunk_hex = chunk_id.as_hex();
        let chunk_size = 1024 * 1024; // 1MB streaming chunks
        let total = data.len();
        let mut offset = 0;
        let mut first = true;

        let (tx, rx) = tokio::sync::mpsc::channel(4);

        let volume_id = String::new();
        let tier = String::new();

        tokio::spawn({
            let chunk_hex = chunk_hex.clone();
            async move {
                while offset < total {
                    let end = std::cmp::min(offset + chunk_size, total);
                    let piece = data.slice(offset..end);

                    let req = PutChunkRequest {
                        chunk_id: if first { chunk_hex.clone() } else { String::new() },
                        volume_id: if first { volume_id.clone() } else { String::new() },
                        tier: if first { tier.clone() } else { String::new() },
                        data: piece.to_vec(),
                    };

                    first = false;

                    if tx.send(req).await.is_err() {
                        tracing::error!("Replication stream closed prematurely");
                        break;
                    }
                    offset = end;
                }
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let response = client.put_chunk(stream).await?;
        let resp = response.into_inner();

        if resp.success {
            tracing::info!(
                "Successfully replicated chunk {} ({} bytes) to {}",
                chunk_hex,
                resp.size,
                target_addr,
            );
        } else {
            anyhow::bail!(
                "Replication of chunk {} to {} failed at remote side",
                chunk_hex,
                target_addr,
            );
        }

        Ok(())
    }

    /// Fetch a chunk from a remote source data node and store it locally.
    ///
    /// Connects to `source_addr` via gRPC `GetChunk`, reassembles the
    /// streamed data, and writes it to the local chunk store.
    pub async fn receive_replication(
        &self,
        chunk_id: &ChunkId,
        source_addr: &str,
    ) -> anyhow::Result<()> {
        tracing::info!(
            "Receiving chunk {} from {}",
            chunk_id,
            source_addr,
        );

        // Bug W (part 2) — same fix as `replicate_to`: source_addr is
        // pre-formatted with the scheme.
        let endpoint = if source_addr.starts_with("http://") || source_addr.starts_with("https://") {
            source_addr.to_string()
        } else {
            format!("http://{}", source_addr)
        };
        let mut client = DataServiceClient::connect(endpoint).await?;

        let request = GetChunkRequest {
            chunk_id: chunk_id.as_hex(),
            // P0-9: replication is an internal data-node-to-data-node copy,
            // not a user read. Tag with a sentinel DC so the tiering engine
            // can filter these out of its access-pattern bucketing if it
            // chooses; for now, the empty string falls into the "unknown"
            // bucket and is correctly ignored by tier scoring (it doesn't
            // match any real DC name in `reads_by_dc`).
            requester_dc: String::new(),
            requester_host: "dtfs-data-replication".to_string(),
        };

        let response = client.get_chunk(request).await?;
        let mut stream = response.into_inner();

        // Reassemble streamed data.
        let mut data = Vec::new();
        while let Some(msg) = stream.message().await? {
            data.extend_from_slice(&msg.data);
        }

        // Store locally using default volume and hot tier.
        let vol_id = dtfs_common::types::VolumeId::new("default".to_string());
        let _meta = self
            .store
            .put_chunk(chunk_id, &data, &vol_id, dtfs_common::types::DiskTier::Hot)
            .await?;

        tracing::info!(
            "Successfully received chunk {} ({} bytes) from {}",
            chunk_id,
            data.len(),
            source_addr,
        );

        Ok(())
    }

    /// Get a reference to the underlying config.
    pub fn config(&self) -> &DataNodeConfig {
        &self.config
    }
}
