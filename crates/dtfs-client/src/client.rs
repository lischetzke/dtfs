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
    AllocateChunksRequest, AllocateChunksResponse, CreateFileRequest, CreateFileResponse,
    DeleteFileRequest, DeleteFileResponse, GetFileLocationsRequest, GetFileLocationsResponse,
    ListDirectoryRequest, ListDirectoryResponse,
};

/// Size of each streaming piece for chunk I/O (1 MB).
const STREAM_PIECE_SIZE: usize = 1024 * 1024;

/// Inner shared state for the client, wrapped in Arc for cheap cloning.
struct DtfsClientInner {
    /// Cached manager gRPC channels, keyed by address.
    manager_channels: DashMap<String, Channel>,
    /// Cached data node gRPC channels, keyed by address.
    data_channels: DashMap<String, Channel>,
    /// The primary manager address this client is connected to.
    manager_addr: parking_lot::RwLock<Option<String>>,
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
    /// Call [`connect`](Self::connect) to establish a connection.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DtfsClientInner {
                manager_channels: DashMap::new(),
                data_channels: DashMap::new(),
                manager_addr: parking_lot::RwLock::new(None),
            }),
        }
    }

    /// Connect to a manager node at the given address.
    ///
    /// The address should be a URI like `http://host:port`. This establishes
    /// a gRPC channel and caches it for future use.
    #[instrument(skip(self))]
    pub async fn connect(&self, manager_addr: &str) -> Result<()> {
        let channel = self
            .get_or_create_channel(&self.inner.manager_channels, manager_addr)
            .await?;

        // Verify connectivity by creating a client (lightweight, no RPC yet).
        let _client = ManagerServiceClient::new(channel);

        *self.inner.manager_addr.write() = Some(manager_addr.to_string());
        debug!(addr = manager_addr, "connected to manager");
        Ok(())
    }

    /// Create a new file in the DTFS namespace.
    #[instrument(skip(self))]
    pub async fn create_file(
        &self,
        path: &str,
        volume_id: Option<&str>,
    ) -> Result<CreateFileResponse> {
        let mut client = self.manager_client().await?;
        let request = CreateFileRequest {
            path: path.to_string(),
            volume_id: volume_id.map(|v| v.to_string()),
        };
        let response = client
            .create_file(request)
            .await
            .context("create_file RPC failed")?;
        Ok(response.into_inner())
    }

    /// Get chunk locations for a file.
    #[instrument(skip(self))]
    pub async fn get_file_locations(&self, path: &str) -> Result<GetFileLocationsResponse> {
        let mut client = self.manager_client().await?;
        let request = GetFileLocationsRequest {
            path: path.to_string(),
        };
        let response = client
            .get_file_locations(request)
            .await
            .context("get_file_locations RPC failed")?;
        Ok(response.into_inner())
    }

    /// Delete a file from the DTFS namespace.
    #[instrument(skip(self))]
    pub async fn delete_file(&self, path: &str) -> Result<DeleteFileResponse> {
        let mut client = self.manager_client().await?;
        let request = DeleteFileRequest {
            path: path.to_string(),
        };
        let response = client
            .delete_file(request)
            .await
            .context("delete_file RPC failed")?;
        Ok(response.into_inner())
    }

    /// List directory entries at the given path.
    #[instrument(skip(self))]
    pub async fn list_directory(&self, path: &str) -> Result<ListDirectoryResponse> {
        let mut client = self.manager_client().await?;
        let request = ListDirectoryRequest {
            path: path.to_string(),
        };
        let response = client
            .list_directory(request)
            .await
            .context("list_directory RPC failed")?;
        Ok(response.into_inner())
    }

    /// Allocate chunk placements for writing to a file.
    #[instrument(skip(self))]
    pub async fn allocate_chunks(
        &self,
        path: &str,
        num_chunks: u32,
        volume_id: &str,
    ) -> Result<AllocateChunksResponse> {
        let mut client = self.manager_client().await?;
        let request = AllocateChunksRequest {
            path: path.to_string(),
            num_chunks,
            volume_id: volume_id.to_string(),
        };
        let response = client
            .allocate_chunks(request)
            .await
            .context("allocate_chunks RPC failed")?;
        Ok(response.into_inner())
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
        };
        let response = client
            .get_chunk(request)
            .await
            .context("get_chunk RPC failed")?;

        let mut stream = response.into_inner();
        let mut data = Vec::new();

        while let Some(piece) = stream
            .message()
            .await
            .context("error reading chunk stream")?
        {
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
        let mut client = self.data_client(node_addr).await?;

        let chunk_id_owned = chunk_id.to_string();
        let volume_id_owned = volume_id.to_string();
        let tier_owned = tier.to_string();
        let data_owned = data.to_vec();

        // Build the stream of PutChunkRequest messages.
        let mut messages = Vec::new();

        if data_owned.is_empty() {
            // Even for empty data, send one message with metadata.
            messages.push(PutChunkRequest {
                chunk_id: chunk_id_owned.clone(),
                volume_id: volume_id_owned.clone(),
                tier: tier_owned.clone(),
                data: Vec::new(),
            });
        } else {
            for (i, piece) in data_owned.chunks(STREAM_PIECE_SIZE).enumerate() {
                if i == 0 {
                    // First message carries metadata + data.
                    messages.push(PutChunkRequest {
                        chunk_id: chunk_id_owned.clone(),
                        volume_id: volume_id_owned.clone(),
                        tier: tier_owned.clone(),
                        data: piece.to_vec(),
                    });
                } else {
                    // Subsequent messages carry only data.
                    messages.push(PutChunkRequest {
                        chunk_id: String::new(),
                        volume_id: String::new(),
                        tier: String::new(),
                        data: piece.to_vec(),
                    });
                }
            }
        }

        let stream = futures::stream::iter(messages);
        let response = client
            .put_chunk(stream)
            .await
            .context("put_chunk RPC failed")?;

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

    /// Get a `ManagerServiceClient` connected to the primary manager.
    async fn manager_client(&self) -> Result<ManagerServiceClient<Channel>> {
        let addr = self
            .inner
            .manager_addr
            .read()
            .clone()
            .context("not connected to a manager — call connect() first")?;
        let channel = self
            .get_or_create_channel(&self.inner.manager_channels, &addr)
            .await?;
        Ok(ManagerServiceClient::new(channel))
    }

    /// Get a `DataServiceClient` connected to the given data node address.
    async fn data_client(&self, addr: &str) -> Result<DataServiceClient<Channel>> {
        let channel = self
            .get_or_create_channel(&self.inner.data_channels, addr)
            .await?;
        Ok(DataServiceClient::new(channel))
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
}

impl Default for DtfsClient {
    fn default() -> Self {
        Self::new()
    }
}
