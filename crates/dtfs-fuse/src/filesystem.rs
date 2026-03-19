// Core FUSE filesystem abstraction — Phase 2 (FUSE Client).
//
// Implements a `DtfsFilesystem` struct with async methods that mirror
// POSIX filesystem operations. This is not yet a fuse3 trait impl (fuse3
// is Linux-only) but provides the complete abstraction layer that will
// plug into fuse3 on Linux.
//
// All cluster communication goes through `DtfsClient`. Metadata is cached
// with configurable TTL. Read-ahead and write buffering are handled by
// the companion `ReadAheadEngine` and `WriteBuffer`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use dashmap::DashMap;
use tracing::{debug, info, instrument, warn};

use dtfs_client::client::DtfsClient;
use dtfs_common::config::FuseConfig;
use dtfs_common::topology::Location;

use crate::migration::MigrationTrigger;
use crate::readahead::{ReadAheadEngine, ReadAheadMode};
use crate::writebuf::WriteBuffer;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Cached file location metadata returned by the manager.
#[derive(Debug, Clone)]
pub struct FileLocations {
    /// Full file path in the DTFS namespace.
    pub path: String,
    /// Total file size in bytes.
    pub total_size: u64,
    /// Per-chunk info: (chunk_id, size, node_addresses).
    pub chunks: Vec<ChunkInfo>,
    /// Volume ID this file belongs to.
    pub volume_id: String,
}

/// Information about a single chunk within a file.
#[derive(Debug, Clone)]
pub struct ChunkInfo {
    pub chunk_id: String,
    pub size: u64,
    pub node_addresses: Vec<String>,
}

/// File attributes (stat-like information).
#[derive(Debug, Clone)]
pub struct FileAttr {
    pub size: u64,
    pub is_directory: bool,
    pub created_at: SystemTime,
    pub modified_at: SystemTime,
}

/// A directory entry returned by readdir.
#[derive(Debug, Clone)]
pub struct DirEntry {
    pub name: String,
    pub size: u64,
    pub is_directory: bool,
    pub created_at: SystemTime,
    pub modified_at: SystemTime,
}

/// O_RDONLY / O_WRONLY / O_RDWR flags (simplified).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenFlags {
    ReadOnly,
    WriteOnly,
    ReadWrite,
}

/// An open file handle with its associated state.
struct OpenFile {
    path: String,
    flags: OpenFlags,
    /// Cached locations (refreshed on open).
    locations: Option<FileLocations>,
    /// Chunk size in bytes for this file's volume.
    chunk_size: u64,
}

// ---------------------------------------------------------------------------
// Default metadata cache TTL
// ---------------------------------------------------------------------------

/// Default metadata cache TTL: 5 seconds.
const DEFAULT_METADATA_TTL: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// DtfsFilesystem
// ---------------------------------------------------------------------------

/// Core filesystem abstraction for the DTFS FUSE client.
///
/// Provides async methods corresponding to POSIX filesystem operations.
/// Internally communicates with the DTFS cluster via `DtfsClient` and
/// caches metadata to reduce round-trips.
pub struct DtfsFilesystem {
    /// gRPC client for cluster communication.
    client: DtfsClient,
    /// This client's location in the topology.
    client_location: Location,
    /// FUSE configuration.
    config: FuseConfig,
    /// Metadata cache: path -> (FileLocations, cached_at).
    metadata_cache: DashMap<String, (FileLocations, Instant)>,
    /// Metadata TTL.
    metadata_ttl: Duration,
    /// Open file handles: fh -> OpenFile.
    open_files: DashMap<u64, OpenFile>,
    /// Next file handle counter.
    next_fh: AtomicU64,
    /// Read-ahead engine for prefetching.
    readahead: ReadAheadEngine,
    /// Write buffer for coalescing writes.
    write_buffer: WriteBuffer,
    /// Migration trigger for requesting closer replicas.
    migration: MigrationTrigger,
    /// Known node locations for nearest-node selection.
    /// Populated as we discover node addresses from metadata responses.
    known_locations: DashMap<String, Location>,
}

impl DtfsFilesystem {
    /// Create a new `DtfsFilesystem`.
    pub fn new(
        client: DtfsClient,
        client_location: Location,
        config: FuseConfig,
    ) -> Self {
        let readahead_mode = ReadAheadMode::from_config(
            &config.readahead.mode,
            config.readahead.prefetch_chunks,
            config.readahead.prefetch_next_files,
        );

        // Default chunk size from the manager defaults. We use 128 MB as
        // the default; the actual chunk size per file comes from the
        // volume config when the file is opened.
        let default_chunk_size = 128 * 1024 * 1024;

        let readahead = ReadAheadEngine::new(
            readahead_mode,
            client.clone(),
            256, // max prefetch cache entries
        );

        let write_buffer = WriteBuffer::new(default_chunk_size);

        let migration = MigrationTrigger::new(
            client.clone(),
            client_location.clone(),
        );

        Self {
            client,
            client_location,
            config,
            metadata_cache: DashMap::new(),
            metadata_ttl: DEFAULT_METADATA_TTL,
            open_files: DashMap::new(),
            next_fh: AtomicU64::new(1),
            readahead,
            write_buffer,
            migration,
            known_locations: DashMap::new(),
        }
    }

    /// Override the metadata cache TTL.
    pub fn set_metadata_ttl(&mut self, ttl: Duration) {
        self.metadata_ttl = ttl;
    }

    // -----------------------------------------------------------------------
    // Filesystem operations
    // -----------------------------------------------------------------------

    /// Stat a file — look up its attributes.
    ///
    /// First checks the metadata cache, then falls back to the manager.
    #[instrument(skip(self), fields(path = %path))]
    pub async fn lookup(&self, path: &str) -> Result<FileAttr> {
        // Try the metadata cache first.
        if let Some(cached) = self.get_cached_metadata(path) {
            return Ok(file_attr_from_locations(&cached));
        }

        // Fetch from the manager.
        let locations = self.fetch_file_locations(path).await?;
        let attr = file_attr_from_locations(&locations);
        self.cache_metadata(path, locations);
        Ok(attr)
    }

    /// List directory entries.
    #[instrument(skip(self), fields(path = %path))]
    pub async fn readdir(&self, path: &str) -> Result<Vec<DirEntry>> {
        let response = self
            .client
            .list_directory(path)
            .await
            .with_context(|| format!("readdir failed for {path}"))?;

        let proto_entries = response.entries;
        let count = proto_entries.len();

        let entries = proto_entries
            .into_iter()
            .map(|e| {
                DirEntry {
                    name: e.name,
                    size: e.size,
                    is_directory: e.is_directory,
                    created_at: system_time_from_epoch_secs(e.created_at),
                    modified_at: system_time_from_epoch_secs(e.modified_at),
                }
            })
            .collect();

        debug!(path, count, "readdir complete");
        Ok(entries)
    }

    /// Open a file and return a file handle.
    #[instrument(skip(self), fields(path = %path, ?flags))]
    pub async fn open(&self, path: &str, flags: OpenFlags) -> Result<u64> {
        // Fetch (or refresh) file locations from the manager.
        let locations = self.fetch_file_locations(path).await?;
        let chunk_size = self.resolve_chunk_size(&locations);

        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);

        self.open_files.insert(
            fh,
            OpenFile {
                path: path.to_string(),
                flags,
                locations: Some(locations.clone()),
                chunk_size,
            },
        );

        // Cache metadata.
        self.cache_metadata(path, locations);

        debug!(path, fh, "file opened");
        Ok(fh)
    }

    /// Read data from an open file at the given offset.
    ///
    /// Translates the byte offset+size into chunk reads, fetching data from
    /// the nearest available data node. Uses the read-ahead cache when
    /// available.
    #[instrument(skip(self), fields(fh, offset, size))]
    pub async fn read(&self, fh: u64, offset: u64, size: u32) -> Result<Bytes> {
        let file = self
            .open_files
            .get(&fh)
            .context("invalid file handle")?;

        let locations = file
            .locations
            .as_ref()
            .context("file has no location data")?;
        let chunk_size = file.chunk_size;
        let file_path = file.path.clone();
        let volume_id = locations.volume_id.clone();

        // Clamp read to file size.
        let file_size = locations.total_size;
        if offset >= file_size {
            return Ok(Bytes::new());
        }
        let actual_size = size.min((file_size - offset) as u32);

        // Determine which chunks we need to read.
        let start_chunk = (offset / chunk_size) as usize;
        let end_offset = offset + actual_size as u64;
        let end_chunk = ((end_offset + chunk_size - 1) / chunk_size) as usize;

        let mut result = Vec::with_capacity(actual_size as usize);

        // Build chunk locations list for readahead.
        let chunk_locs: Vec<(String, Vec<String>)> = locations
            .chunks
            .iter()
            .map(|c| (c.chunk_id.clone(), c.node_addresses.clone()))
            .collect();

        for chunk_idx in start_chunk..end_chunk.min(locations.chunks.len()) {
            let chunk = &locations.chunks[chunk_idx];
            if chunk.node_addresses.is_empty() {
                bail!("no data nodes available for chunk {}", chunk.chunk_id);
            }

            // Select nearest node.
            let node_addr = self.select_nearest_node(&chunk.node_addresses);

            // Try read-ahead cache first.
            let chunk_data =
                if let Some(cached) = self.readahead.get_cached(&node_addr, &chunk.chunk_id) {
                    debug!(chunk_id = %chunk.chunk_id, "readahead cache hit");
                    cached
                } else {
                    // Fetch from data node.
                    let data = self
                        .client
                        .read_chunk(&node_addr, &chunk.chunk_id)
                        .await
                        .with_context(|| {
                            format!(
                                "failed to read chunk {} from {}",
                                chunk.chunk_id, node_addr
                            )
                        })?;

                    // Trigger readahead for upcoming chunks.
                    self.readahead.on_chunk_read(
                        &file_path,
                        chunk_idx as u32,
                        locations.chunks.len() as u32,
                        &chunk_locs,
                    );

                    // Trigger migration check if the read was remote.
                    // We use a simple heuristic: if the node address contains
                    // a known location, check distance.
                    if let Some(node_loc) = self.known_locations.get(&node_addr) {
                        let node_loc = node_loc.clone();
                        let chunk_id = chunk.chunk_id.clone();
                        let file_path = file_path.clone();
                        let volume_id = volume_id.clone();
                        // Fire-and-forget async migration check.
                        let migration = &self.migration;
                        migration
                            .on_remote_read(&chunk_id, &node_loc, &file_path, &volume_id)
                            .await;
                    }

                    data
                };

            // Extract the relevant byte range from this chunk.
            let chunk_start_byte = chunk_idx as u64 * chunk_size;
            let slice_start = if offset > chunk_start_byte {
                (offset - chunk_start_byte) as usize
            } else {
                0
            };
            let slice_end = if end_offset < chunk_start_byte + chunk_size {
                (end_offset - chunk_start_byte) as usize
            } else {
                chunk_data.len()
            }
            .min(chunk_data.len());

            if slice_start < slice_end {
                result.extend_from_slice(&chunk_data[slice_start..slice_end]);
            }
        }

        Ok(Bytes::from(result))
    }

    /// Buffer a write for an open file.
    ///
    /// Data is accumulated in the write buffer. Complete chunks are flushed
    /// to data nodes automatically. Returns the number of bytes written.
    #[instrument(skip(self, data), fields(fh, offset, len = data.len()))]
    pub async fn write(&self, fh: u64, offset: u64, data: &[u8]) -> Result<u32> {
        let file = self
            .open_files
            .get(&fh)
            .context("invalid file handle")?;

        if file.flags == OpenFlags::ReadOnly {
            bail!("file opened read-only, cannot write");
        }

        let path = file.path.clone();
        drop(file); // Release the lock before writing.

        let written = self.write_buffer.write(&path, offset, data);

        // Check if any complete chunks are ready to flush.
        let segments = self.write_buffer.flush(&path);
        if !segments.is_empty() {
            self.flush_segments_to_cluster(&path, &segments).await?;
        }

        debug!(path = %path, written, "write buffered");
        Ok(written as u32)
    }

    /// Flush all buffered writes for an open file to the cluster.
    #[instrument(skip(self), fields(fh))]
    pub async fn flush(&self, fh: u64) -> Result<()> {
        let file = self
            .open_files
            .get(&fh)
            .context("invalid file handle")?;

        let path = file.path.clone();
        drop(file);

        let segments = self.write_buffer.flush_all(&path);
        if !segments.is_empty() {
            self.flush_segments_to_cluster(&path, &segments).await?;
        }

        // Invalidate metadata cache since the file may have changed.
        self.metadata_cache.remove(&path);

        debug!(path = %path, "flush complete");
        Ok(())
    }

    /// Release (close) an open file handle.
    ///
    /// Flushes any remaining buffered writes, then removes the handle.
    #[instrument(skip(self), fields(fh))]
    pub async fn release(&self, fh: u64) -> Result<()> {
        // Flush before closing.
        if self.open_files.contains_key(&fh) {
            self.flush(fh).await?;
        }

        if let Some((_, file)) = self.open_files.remove(&fh) {
            // Clean up write buffer for this path.
            self.write_buffer.remove(&file.path);
            debug!(path = %file.path, fh, "file released");
        }

        Ok(())
    }

    /// Create a new file in the DTFS namespace and return a file handle.
    #[instrument(skip(self), fields(path = %path))]
    pub async fn create(&self, path: &str, _mode: u32) -> Result<u64> {
        let response = self
            .client
            .create_file(path, None)
            .await
            .with_context(|| format!("create_file failed for {path}"))?;

        if !response.success {
            bail!("manager rejected file creation for {path}");
        }

        info!(
            path,
            chunk_size = response.chunk_size,
            volume_id = %response.volume_id,
            "file created"
        );

        // Open the newly created file for read-write.
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.open_files.insert(
            fh,
            OpenFile {
                path: path.to_string(),
                flags: OpenFlags::ReadWrite,
                locations: Some(FileLocations {
                    path: path.to_string(),
                    total_size: 0,
                    chunks: Vec::new(),
                    volume_id: response.volume_id,
                }),
                chunk_size: response.chunk_size,
            },
        );

        Ok(fh)
    }

    /// Delete a file from the DTFS namespace.
    #[instrument(skip(self), fields(path = %path))]
    pub async fn unlink(&self, path: &str) -> Result<()> {
        let response = self
            .client
            .delete_file(path)
            .await
            .with_context(|| format!("delete_file failed for {path}"))?;

        if !response.success {
            bail!("manager rejected file deletion for {path}");
        }

        // Invalidate caches.
        self.metadata_cache.remove(path);
        self.write_buffer.remove(path);

        info!(path, "file deleted");
        Ok(())
    }

    /// Create a directory in the DTFS namespace.
    ///
    /// Directories in DTFS are implicit (path-prefix based), but we still
    /// notify the manager to create the directory entry for listing purposes.
    #[instrument(skip(self), fields(path = %path))]
    pub async fn mkdir(&self, path: &str) -> Result<()> {
        // Ensure the path ends with "/" to signal directory semantics.
        let dir_path = if path.ends_with('/') {
            path.to_string()
        } else {
            format!("{path}/")
        };

        // Use create_file with a directory marker. The manager interprets
        // paths ending in "/" as directory entries.
        let response = self
            .client
            .create_file(&dir_path, None)
            .await
            .with_context(|| format!("mkdir failed for {path}"))?;

        if !response.success {
            bail!("manager rejected directory creation for {path}");
        }

        info!(path, "directory created");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Fetch file locations from the manager (no caching).
    async fn fetch_file_locations(&self, path: &str) -> Result<FileLocations> {
        let response = self
            .client
            .get_file_locations(path)
            .await
            .with_context(|| format!("get_file_locations failed for {path}"))?;

        let chunks = response
            .chunks
            .into_iter()
            .map(|c| ChunkInfo {
                chunk_id: c.chunk_id,
                size: c.size,
                node_addresses: c.node_addresses,
            })
            .collect();

        Ok(FileLocations {
            path: response.path,
            total_size: response.total_size,
            chunks,
            // The manager response doesn't include volume_id directly in
            // GetFileLocationsResponse. We default to "default" and it
            // gets overridden when the file is created or opened with
            // a known volume.
            volume_id: "default".to_string(),
        })
    }

    /// Get cached metadata if it exists and hasn't expired.
    fn get_cached_metadata(&self, path: &str) -> Option<FileLocations> {
        let entry = self.metadata_cache.get(path)?;
        let (locations, cached_at) = entry.value();
        if cached_at.elapsed() < self.metadata_ttl {
            Some(locations.clone())
        } else {
            // Expired — remove from cache.
            drop(entry);
            self.metadata_cache.remove(path);
            None
        }
    }

    /// Store metadata in the cache.
    fn cache_metadata(&self, path: &str, locations: FileLocations) {
        self.metadata_cache
            .insert(path.to_string(), (locations, Instant::now()));
    }

    /// Resolve chunk size for a file. Falls back to 128 MB default.
    fn resolve_chunk_size(&self, _locations: &FileLocations) -> u64 {
        // In a full implementation this would come from the volume config
        // cached during file open. For now, use the default.
        128 * 1024 * 1024
    }

    /// Select the nearest data node from a list of addresses.
    fn select_nearest_node(&self, addresses: &[String]) -> String {
        if addresses.len() == 1 {
            return addresses[0].clone();
        }

        // Build a HashMap of known locations for the candidate nodes.
        let known: HashMap<String, Location> = addresses
            .iter()
            .filter_map(|addr| {
                self.known_locations
                    .get(addr)
                    .map(|loc| (addr.clone(), loc.clone()))
            })
            .collect();

        dtfs_client::location::select_nearest_node(
            Some(&self.client_location),
            addresses,
            &known,
        )
        .unwrap_or_else(|| addresses[0].clone())
    }

    /// Flush chunk segments to the cluster.
    ///
    /// Allocates placements from the manager and writes each segment to
    /// the assigned data node.
    async fn flush_segments_to_cluster(
        &self,
        path: &str,
        segments: &[crate::writebuf::ChunkSegment],
    ) -> Result<()> {
        if segments.is_empty() {
            return Ok(());
        }

        // Determine the volume_id. Check if we have it cached from an
        // open file handle, otherwise default.
        let volume_id = self
            .find_volume_id_for_path(path)
            .unwrap_or_else(|| "default".to_string());

        // Request placements from the manager.
        let alloc_response = self
            .client
            .allocate_chunks(path, segments.len() as u32, &volume_id)
            .await
            .with_context(|| format!("allocate_chunks failed for {path}"))?;

        // Write each segment to its assigned primary node.
        for (i, segment) in segments.iter().enumerate() {
            let placement = alloc_response
                .placements
                .get(i)
                .with_context(|| {
                    format!(
                        "manager returned fewer placements than requested ({} < {})",
                        alloc_response.placements.len(),
                        segments.len()
                    )
                })?;

            let chunk_id = format!("{path}:chunk-{}", segment.chunk_index);

            self.client
                .write_chunk(
                    &placement.primary_address,
                    &chunk_id,
                    &volume_id,
                    "hot", // New writes go to hot tier.
                    &segment.data,
                )
                .await
                .with_context(|| {
                    format!(
                        "write_chunk failed for {} on {}",
                        chunk_id, placement.primary_address
                    )
                })?;

            debug!(
                path,
                chunk_index = segment.chunk_index,
                node = %placement.primary_address,
                size = segment.data.len(),
                "chunk written to cluster"
            );
        }

        Ok(())
    }

    /// Look up the volume_id for a path from any open file handle.
    fn find_volume_id_for_path(&self, path: &str) -> Option<String> {
        for entry in self.open_files.iter() {
            let file = entry.value();
            if file.path == path {
                if let Some(loc) = &file.locations {
                    return Some(loc.volume_id.clone());
                }
            }
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert epoch seconds (i64) to `SystemTime`.
fn system_time_from_epoch_secs(secs: i64) -> SystemTime {
    if secs >= 0 {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs as u64)
    } else {
        SystemTime::UNIX_EPOCH
    }
}

/// Build a `FileAttr` from `FileLocations`.
fn file_attr_from_locations(locations: &FileLocations) -> FileAttr {
    FileAttr {
        size: locations.total_size,
        is_directory: false,
        created_at: SystemTime::UNIX_EPOCH,
        modified_at: SystemTime::UNIX_EPOCH,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_time_from_epoch_positive() {
        let t = system_time_from_epoch_secs(1_000_000);
        let elapsed = t.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        assert_eq!(elapsed.as_secs(), 1_000_000);
    }

    #[test]
    fn test_system_time_from_epoch_zero() {
        let t = system_time_from_epoch_secs(0);
        assert_eq!(t, SystemTime::UNIX_EPOCH);
    }

    #[test]
    fn test_system_time_from_epoch_negative() {
        let t = system_time_from_epoch_secs(-1);
        assert_eq!(t, SystemTime::UNIX_EPOCH);
    }

    #[test]
    fn test_file_attr_from_locations() {
        let loc = FileLocations {
            path: "/test/file".to_string(),
            total_size: 42,
            chunks: Vec::new(),
            volume_id: "default".to_string(),
        };
        let attr = file_attr_from_locations(&loc);
        assert_eq!(attr.size, 42);
        assert!(!attr.is_directory);
    }
}
