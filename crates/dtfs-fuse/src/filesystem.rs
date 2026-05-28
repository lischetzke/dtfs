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
use dtfs_proto::dtfs::manager::ChunkLocationCommit;
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
    /// Logical chunk size for this file in bytes.
    ///
    /// Used to map byte offsets to chunk indices. Populated from the manager's
    /// `GetFileLocationsResponse.chunk_size` field (fix for issue #13).
    pub chunk_size: u64,
    /// File creation time, epoch milliseconds. Comes from the manager's
    /// `FileMetadata.created_at_ms` (fix for F-014).
    pub created_at_ms: i64,
    /// File last-modified time, epoch milliseconds (fix for F-014).
    pub modified_at_ms: i64,
    /// True iff this path is a directory (explicit mkdir or synthetic).
    /// Set by the manager's `GetFileLocationsResponse.is_directory`. Fixes
    /// the SOAK-RECOVERY-2026-05-18 bugs A & B where `getattr` on a
    /// freshly-mkdir'd folder returned ENOENT.
    pub is_directory: bool,
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

impl OpenFlags {
    /// Whether this open mode permits writes (and therefore needs a
    /// single-writer lease — P0-1).
    pub fn is_writable(self) -> bool {
        matches!(self, Self::WriteOnly | Self::ReadWrite)
    }
}

/// Sentinel error returned when an open-for-write fails because another
/// process already holds the write lease on the same path. The fuse adapter
/// downcasts this to libc::EBUSY so applications see `Resource busy` rather
/// than a generic I/O error.
#[derive(Debug)]
pub struct LeaseBusy {
    pub path: String,
    pub conflict_owner: String,
    pub expires_at_ms: u64,
}

impl std::fmt::Display for LeaseBusy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "write lease busy for {} (owner={}, expires_at_ms={})",
            self.path, self.conflict_owner, self.expires_at_ms
        )
    }
}

impl std::error::Error for LeaseBusy {}

/// Lease parameters (P0-1).
///
/// 15s was too tight in practice: a multi-GB streaming write through FUSE
/// only renews the lease on `flush`, so the lease expires mid-flush on
/// files larger than ~1 GB at ~100 MB/s. We bump to 120s so a single
/// chunk write (128 MB) plus any FUSE buffering latency can never burn the
/// whole budget. Renewal at half-life (60s) gives ample headroom even with
/// network jitter and slow data nodes. P2-x: replace this with a per-fh
/// background renewer task so the lease is held as long as the handle is
/// open, regardless of write tempo.
pub(crate) const WRITE_LEASE_MS: u64 = 120_000;
pub(crate) const WRITE_LEASE_RENEW_AFTER_MS: u64 = 60_000;

/// An open file handle with its associated state.
struct OpenFile {
    path: String,
    flags: OpenFlags,
    /// `O_APPEND` semantics — every write goes to the *current* end of file,
    /// regardless of the offset the kernel hands us. (F-021)
    append: bool,
    /// Cached locations (refreshed on open).
    locations: Option<FileLocations>,
    /// Chunk size in bytes for this file's volume.
    chunk_size: u64,
    /// P0-1: write lease token for this file handle. None for read-only opens.
    lease_id: Option<String>,
    /// P0-1: absolute Unix-ms expiry of the lease (renew at half-life).
    lease_expires_at_ms: Option<u64>,
    /// P0-1: owner_id used to acquire the lease (for renew/release).
    lease_owner_id: Option<String>,
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
    /// P0-1: prefix used for write-lease `owner_id` values. Format:
    /// `<hostname>:<pid>:`. The per-open `:<fh>` suffix is appended at
    /// open-time to make each owner_id unique within the process.
    lease_owner_prefix: String,
    /// P0-11: disk-backed chunk cache. Sits between the in-memory readahead
    /// and the network. None when `local_cache_dir` is not configured.
    pub(crate) local_cache: Option<std::sync::Arc<crate::local_cache::LocalChunkCache>>,
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

        // P0-1: lease owner = "<topology host>:<pid>:". Using the topology
        // host (set via fuse.toml) keeps the identity stable across PID
        // changes and avoids a new system-dep on the `hostname` crate.
        let lease_owner_prefix = format!(
            "{}:{}:",
            client_location.host, std::process::id()
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
            lease_owner_prefix,
            local_cache: None,
        }
    }

    /// P0-11: install the disk-backed local chunk cache. Called from fuse
    /// main.rs after building the filesystem when `local_cache_dir` is set.
    pub fn set_local_cache(
        &mut self,
        cache: std::sync::Arc<crate::local_cache::LocalChunkCache>,
    ) {
        // Also wire the disk cache into the readahead engine so its
        // prefetched chunks (e.g. next-episode warm-up in folder mode)
        // mirror into persistent storage. Without this, the prefetch's
        // benefit dies the moment the in-memory ring evicts the entry.
        self.readahead.set_local_cache(cache.clone());
        self.local_cache = Some(cache);
    }

    /// P0-1: hostname (without PID/fh) used for the orphan-lease cleanup
    /// call at FUSE startup. Returns just the hostname segment.
    pub fn lease_hostname(&self) -> String {
        // Strip the trailing ":pid:" — release_leases_by_host takes the
        // hostname only (it appends ":" itself when matching prefixes).
        self.lease_owner_prefix
            .split(':')
            .next()
            .unwrap_or("unknown-host")
            .to_string()
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
        // The manager has no metadata entry for "/" — files are stored at
        // "/foo", "/bar", etc. Return a synthetic directory attribute for the
        // root so getattr(ino=1) succeeds. Without this, every `ls`/`stat` on
        // the mount returns ENOENT even though writes through paths like
        // "/mnt/dtfs/foo" still succeed. (Fix for the ls/stat regression
        // surfaced after the F-001..F-016 rebuild.)
        if path == "/" {
            // F-023: return SystemTime::now() as mtime so the kernel
            // treats the root directory as freshly modified after every
            // lookup. Combined with a sub-second ENTRY_TTL this prevents
            // the kernel from serving stale getdents() out of its dcache
            // immediately after a bulk file-creation loop.
            let now = SystemTime::now();
            return Ok(FileAttr {
                size: 4096,
                is_directory: true,
                created_at: now,
                modified_at: now,
            });
        }

        // Try the metadata cache first.
        if let Some(cached) = self.get_cached_metadata(path) {
            let mut attr = file_attr_from_locations(&cached);
            // F-029: if writes are still buffered locally, the cluster-side
            // size is stale — fold in the buffered logical size so `stat`
            // reports what the user can actually `read()`.
            if let Some(buffered_eof) = self.write_buffer.logical_size(path) {
                if buffered_eof > attr.size {
                    attr.size = buffered_eof;
                }
            }
            return Ok(attr);
        }

        // Fetch from the manager.
        let locations = self.fetch_file_locations(path).await?;
        let mut attr = file_attr_from_locations(&locations);
        if let Some(buffered_eof) = self.write_buffer.logical_size(path) {
            if buffered_eof > attr.size {
                attr.size = buffered_eof;
            }
        }
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

        // NB: the manager's ListDirectory handler populates `created_at` /
        // `modified_at` with FileMetadata's millisecond values (despite the
        // proto field names not having `_ms`). Convert as milliseconds so ls
        // doesn't show year 58344. (Aligned with F-014's _ms convention.)
        let entries = proto_entries
            .into_iter()
            .map(|e| {
                DirEntry {
                    name: e.name,
                    size: e.size,
                    is_directory: e.is_directory,
                    created_at: system_time_from_epoch_ms(e.created_at),
                    modified_at: system_time_from_epoch_ms(e.modified_at),
                }
            })
            .collect();

        debug!(path, count, "readdir complete");
        Ok(entries)
    }

    /// Open a file and return a file handle.
    #[instrument(skip(self), fields(path = %path, ?flags))]
    pub async fn open(&self, path: &str, flags: OpenFlags) -> Result<u64> {
        self.open_with_append(path, flags, false).await
    }

    /// Same as [`open`] but lets the caller signal `O_APPEND`. Used by the
    /// fuse3 adapter when the kernel passes the open-flag bitmask, so every
    /// subsequent write lands at the current end-of-file (F-021).
    pub async fn open_with_append(
        &self,
        path: &str,
        flags: OpenFlags,
        append: bool,
    ) -> Result<u64> {
        // Fetch (or refresh) file locations from the manager.
        let locations = self.fetch_file_locations(path).await?;
        let chunk_size = self.resolve_chunk_size(&locations);

        // F-021: when opened with O_APPEND, pre-seed the write buffer with
        // the file's existing tail chunk so the subsequent write(s) extend
        // it instead of replacing the chunk wholesale. Without this, the
        // next flush emits a chunk-0 segment containing ONLY the appended
        // bytes, which silently truncates the file at the append offset.
        //
        // For media-server workloads we expect append to be small and rare;
        // the memory cost (= size of the last chunk, capped at chunk_size)
        // is acceptable.
        if append && !locations.chunks.is_empty() {
            let last_idx = locations.chunks.len() - 1;
            let last_chunk = &locations.chunks[last_idx];
            if !last_chunk.node_addresses.is_empty() {
                let node = self.select_nearest_node(&last_chunk.node_addresses);
                match self.client.read_chunk(&node, &last_chunk.chunk_id).await {
                    Ok(existing) => {
                        let offset_in_file = (last_idx as u64) * chunk_size;
                        self.write_buffer.write(path, offset_in_file, &existing);
                        debug!(
                            path,
                            offset_in_file,
                            len = existing.len(),
                            "O_APPEND: pre-seeded tail chunk into write buffer"
                        );
                    }
                    Err(e) => {
                        warn!(
                            path,
                            error = %e,
                            "O_APPEND: failed to pre-seed tail chunk; append may truncate"
                        );
                    }
                }
            }
        }

        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);

        // P0-1: for writable opens, acquire the single-writer lease BEFORE
        // inserting the OpenFile entry, so a busy condition leaves no
        // dangling fh. A returned LeaseBusy error surfaces as EBUSY to the
        // caller via the fuse adapter's downcast.
        let (lease_id, lease_expires_at_ms, lease_owner_id) = if flags.is_writable() {
            // Bug X — append the client's DC as a 4th field. Existing
            // parsers (starts_with("host:"), split(':').next()) ignore
            // trailing fields, so this is backward-compatible. The
            // manager API uses the DC to render the right location for
            // this client in the topology view.
            let owner_id = if self.client_location.datacenter.is_empty() {
                format!("{}{}", self.lease_owner_prefix, fh)
            } else {
                format!(
                    "{}{}:{}",
                    self.lease_owner_prefix, fh, self.client_location.datacenter
                )
            };
            let resp = self
                .client
                .acquire_write_lease(path, &owner_id, WRITE_LEASE_MS)
                .await
                .with_context(|| format!("acquire_write_lease({path})"))?;
            if !resp.granted {
                return Err(anyhow::Error::new(LeaseBusy {
                    path: path.to_string(),
                    conflict_owner: resp.conflict_owner,
                    expires_at_ms: resp.conflict_expires_at_ms,
                }));
            }
            (
                Some(resp.lease_id),
                Some(resp.expires_at_ms),
                Some(owner_id),
            )
        } else {
            (None, None, None)
        };

        self.open_files.insert(
            fh,
            OpenFile {
                path: path.to_string(),
                flags,
                append,
                locations: Some(locations.clone()),
                chunk_size,
                lease_id,
                lease_expires_at_ms,
                lease_owner_id,
            },
        );

        // Cache metadata.
        self.cache_metadata(path, locations);

        debug!(path, fh, append, "file opened");
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

        // F-029: effective file size = max(persisted size, buffered logical
        // size). Without this, every read of bytes that have only been
        // *buffered* (not yet flushed to a chunk) returns Bytes::new() and
        // SQLite / any DB engine sees an empty database after `CREATE TABLE`.
        let buffered_eof = self.write_buffer.logical_size(&file_path).unwrap_or(0);
        let file_size = locations.total_size.max(buffered_eof);
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

            // P0-2: build candidate list sorted by distance (nearest first),
            // try each on failure until one succeeds. Single-replica clusters
            // and single-address chunks degenerate to one attempt.
            let candidates = self.sort_candidates(&chunk.node_addresses);

            // Try read-ahead in-memory cache first: any candidate's cache
            // key works because chunks are content-addressed by chunk_id.
            let mut chunk_data: Option<bytes::Bytes> = None;
            let mut cache_hit_notify_addr: Option<String> = None;
            for cand in &candidates {
                if let Some(cached) = self.readahead.get_cached(cand, &chunk.chunk_id) {
                    debug!(chunk_id = %chunk.chunk_id, node = %cand, "readahead cache hit");
                    chunk_data = Some(cached);
                    // Bug AV (AV-4-1c) — notify the data-node so its
                    // per-DC counter reflects this read even though we
                    // didn't traverse the network. Without this the
                    // tiering engine never sees the demand that drives
                    // cache promotion.
                    cache_hit_notify_addr = Some(cand.clone());
                    break;
                }
            }

            // P0-11: fall through to the disk-backed local cache. This
            // survives FUSE restarts and is governed by `max_age_days` for
            // per-DC retention policies (the warm DC keeps only files
            // accessed in the last 90 days).
            if chunk_data.is_none() {
                if let Some(lc) = &self.local_cache {
                    // `size` is the kernel's requested read length for this
                    // call. Pass it as bytes_hint so the cache can
                    // distinguish viewer reads (refresh atime) from scanner
                    // reads (preserve last-viewer atime for LRU).
                    if let Some(cached) =
                        lc.get(&chunk.chunk_id, size as usize).await
                    {
                        debug!(chunk_id = %chunk.chunk_id, "local-disk cache hit");
                        chunk_data = Some(cached);
                        // AV-4-1c — pick any candidate (the disk cache
                        // is content-addressed; the bytes are
                        // identical regardless of which node we
                        // notify). First candidate is the nearest
                        // healthy replica per `sort_candidates`.
                        if cache_hit_notify_addr.is_none() {
                            cache_hit_notify_addr =
                                candidates.first().cloned();
                        }
                    }
                }
            }

            // Bug AV (AV-4-1c) — fire-and-forget notification so the
            // data-node's per-DC counter sees this read. The actual
            // I/O completed via the cache; the RPC is purely metrics.
            // Errors are logged but never propagated (a missed
            // metrics ping is acceptable; an aborted user read is
            // not).
            if let Some(notify_addr) = cache_hit_notify_addr.clone() {
                let client = self.client.clone();
                let chunk_id_clone = chunk.chunk_id.clone();
                tokio::spawn(async move {
                    if let Err(e) = client
                        .record_cache_hit(&notify_addr, &chunk_id_clone)
                        .await
                    {
                        debug!(
                            chunk_id = %chunk_id_clone,
                            node = %notify_addr,
                            error = %e,
                            "record_cache_hit notify failed (non-fatal)"
                        );
                    }
                });
            }

            let chunk_data = if let Some(d) = chunk_data {
                d
            } else {
                // Cache miss: fetch from data node, falling back through
                // candidates on transport / not-found errors.
                let mut last_err: Option<anyhow::Error> = None;
                let mut fetched: Option<(String, bytes::Bytes)> = None;
                for cand in &candidates {
                    match self.client.read_chunk(cand, &chunk.chunk_id).await {
                        Ok(d) => {
                            fetched = Some((cand.clone(), d));
                            break;
                        }
                        Err(e) => {
                            // Bug BC-9 — distinguish a verify_on_read
                            // integrity fault from a transport error so
                            // operators see corruption fail-over clearly.
                            if e.downcast_ref::<dtfs_client::client::IntegrityFaultError>()
                                .is_some()
                            {
                                tracing::warn!(
                                    chunk_id = %chunk.chunk_id,
                                    node = %cand,
                                    error = %e,
                                    "Bug BC-9: integrity fault on replica, failing over to next"
                                );
                            } else {
                                tracing::warn!(
                                    chunk_id = %chunk.chunk_id,
                                    node = %cand,
                                    error = %e,
                                    "replica read failed, trying next"
                                );
                            }
                            last_err = Some(e);
                        }
                    }
                }
                let (node_addr, data) = fetched.ok_or_else(|| {
                    last_err.unwrap_or_else(|| {
                        anyhow::anyhow!(
                            "no replicas available for chunk {} (candidates exhausted)",
                            chunk.chunk_id
                        )
                    })
                })?;

                // CRITICAL: store the just-fetched chunk in the cache so
                // subsequent FUSE reads (which arrive in 128 KB pieces
                // for a 128 MB chunk = ~1024 reads per chunk) hit the
                // cache instead of re-issuing the full fetch. Without
                // this the readahead cache only ever contained chunks
                // fetched by `on_chunk_read` prefetch, and cold reads
                // ran at ~1.2 MB/s because every kernel page read
                // triggered a fresh 128 MB chunk fetch.
                self.readahead
                    .insert_cached(&node_addr, &chunk.chunk_id, data.clone());

                // P0-11: also write to the disk cache so the chunk
                // survives readahead-cache eviction + FUSE restarts.
                if let Some(lc) = &self.local_cache {
                    if let Err(e) = lc.put(&chunk.chunk_id, &data).await {
                        tracing::warn!(
                            chunk_id = %chunk.chunk_id,
                            error = %e,
                            "local disk cache put failed"
                        );
                    }
                }

                // Trigger readahead for upcoming chunks.
                self.readahead.on_chunk_read(
                    &file_path,
                    chunk_idx as u32,
                    locations.chunks.len() as u32,
                    &chunk_locs,
                );

                // Trigger migration check using the actually-served node so
                // tiering sees the real cross-DC traffic, not the nearest
                // candidate we wished we'd used.
                if let Some(node_loc) = self.known_locations.get(&node_addr) {
                    let node_loc = node_loc.clone();
                    let chunk_id = chunk.chunk_id.clone();
                    let file_path = file_path.clone();
                    let volume_id = volume_id.clone();
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

        // F-029: overlay any buffered (not-yet-flushed) writes for this
        // path on top of the cluster-fetched bytes. The buffer may also
        // extend beyond the cluster-known size, so first pad `result` up to
        // the read's actual_size so the overlay has somewhere to land.
        if (result.len() as u32) < actual_size {
            result.resize(actual_size as usize, 0);
        }
        let overlaid = self.write_buffer.overlay(&file_path, offset, &mut result);
        if overlaid > 0 {
            debug!(
                path = %file_path,
                offset,
                size = actual_size,
                overlaid,
                "overlaid buffered writes on read"
            );
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
        let append = file.append;
        // P0-1 follow-up: a streaming write of a multi-GB file can hold the
        // file open for longer than WRITE_LEASE_MS without ever calling
        // flush — and the original flush-only renewal then sees the lease
        // already swept by manager GC and aborts the close with
        // "write lease lost". Renew proactively from here too: cheap because
        // the renewal only fires once per half-life window, and the manager
        // dedups same-token renewals.
        let lease_id_for_renew = file.lease_id.clone();
        let lease_expires_for_renew = file.lease_expires_at_ms;
        // For O_APPEND we ignore the kernel-supplied offset and target the
        // *current* end of file. Use the cached file size; the buffer may
        // already have unflushed data, in which case the buffer reports a
        // larger logical end than `locations.total_size`. (F-021)
        let cached_eof = file
            .locations
            .as_ref()
            .map(|l| l.total_size)
            .unwrap_or(0);
        let chunk_size = file.chunk_size;
        let locations_snapshot = file.locations.clone();
        drop(file); // Release the lock before writing.

        let effective_offset = if append {
            let buffered = self.write_buffer.logical_size(&path).unwrap_or(0);
            cached_eof.max(buffered)
        } else {
            offset
        };

        // Read-modify-write for partial-chunk writes (F-021).
        //
        // The kernel masks O_APPEND in the open-flag word it passes to FUSE
        // and instead issues write() at offset=current_eof. So even if the
        // application opened with O_APPEND, we only see a write at offset N.
        // For our chunked storage, if the existing chunk at that offset has
        // earlier bytes that we don't have in the local write buffer, the
        // subsequent flush would emit a partial chunk segment and the data
        // node would *replace* the whole chunk — silently dropping the
        // earlier content (bash `echo line1 > f; echo line2 >> f`).
        //
        // To prevent that, seed the buffer with the existing chunk content
        // when we're about to write at a position that isn't already covered
        // by buffered data.
        if chunk_size > 0 && effective_offset > 0 {
            let chunk_idx = effective_offset / chunk_size;
            let chunk_start = chunk_idx * chunk_size;
            let need_seed = match self.write_buffer.logical_size(&path) {
                Some(end_in_buffer) => end_in_buffer <= chunk_start,
                None => true,
            };
            if need_seed {
                if let Some(loc) = &locations_snapshot {
                    if let Some(chunk) = loc.chunks.get(chunk_idx as usize) {
                        if !chunk.node_addresses.is_empty() {
                            let node = self.select_nearest_node(&chunk.node_addresses);
                            match self.client.read_chunk(&node, &chunk.chunk_id).await {
                                Ok(existing) if !existing.is_empty() => {
                                    self.write_buffer
                                        .write(&path, chunk_start, &existing);
                                    debug!(
                                        path = %path,
                                        chunk_idx,
                                        len = existing.len(),
                                        "RMW: seeded existing chunk before partial write"
                                    );
                                }
                                Ok(_) => {}
                                Err(e) => warn!(
                                    path = %path,
                                    chunk_idx,
                                    error = %e,
                                    "RMW: failed to read existing chunk — partial write may truncate"
                                ),
                            }
                        }
                    }
                }
            }
        }

        // Sparse-file semantics (F-022): if the write lands past the current
        // logical end, zero-fill the gap so `stat` reports the right size and
        // subsequent reads of the hole return zeros rather than ENOENT.
        let buffered_size = self.write_buffer.logical_size(&path).unwrap_or(0);
        let logical_size = cached_eof.max(buffered_size);
        if effective_offset > logical_size {
            let hole = (effective_offset - logical_size) as usize;
            // Cap the zero-fill at 64 MiB per call to keep memory bounded;
            // larger holes are handled by chunking the fill internally.
            const FILL_CHUNK: usize = 64 * 1024 * 1024;
            let mut remaining = hole;
            let mut at = logical_size;
            while remaining > 0 {
                let n = remaining.min(FILL_CHUNK);
                let zeros = vec![0u8; n];
                self.write_buffer.write(&path, at, &zeros);
                at += n as u64;
                remaining -= n;
            }
            debug!(path = %path, hole, "zero-filled sparse gap");
        }

        let written = self.write_buffer.write(&path, effective_offset, data);

        // Check if any complete chunks are ready to flush.
        let segments = self.write_buffer.flush(&path);
        if !segments.is_empty() {
            self.flush_segments_to_cluster(&path, &segments).await?;
        }

        // Renew the write lease mid-stream so a long-running streaming
        // write (multi-GB file, rsync over slow uplink) doesn't lose its
        // lease before flush has a chance to renew it. Renews at most once
        // per half-life window — manager dedups same-token renewals.
        if let (Some(lease_id), Some(expires_at_ms)) =
            (lease_id_for_renew, lease_expires_for_renew)
        {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let remaining = expires_at_ms.saturating_sub(now_ms);
            if remaining < (WRITE_LEASE_MS - WRITE_LEASE_RENEW_AFTER_MS) {
                match self
                    .client
                    .renew_write_lease(&path, &lease_id, WRITE_LEASE_MS)
                    .await
                {
                    Ok(resp) if resp.granted => {
                        if let Some(mut entry) = self.open_files.get_mut(&fh) {
                            entry.value_mut().lease_expires_at_ms =
                                Some(resp.new_expires_at_ms);
                        }
                    }
                    Ok(_) => {
                        // Lease lost — abort writes so we don't corrupt
                        // another writer's data. The buffered bytes from
                        // this call have already been accepted, but the
                        // caller will see the error and stop pushing more.
                        bail!(
                            "write lease lost for {path} during streaming write"
                        );
                    }
                    Err(e) => {
                        // Manager unreachable. Continue best-effort; the
                        // existing lease may still be valid until GC.
                        tracing::debug!(
                            path = %path,
                            error = %e,
                            "lease renew during write failed (non-fatal until expiry)"
                        );
                    }
                }
            }
        }

        debug!(
            path = %path,
            written,
            offset = effective_offset,
            append,
            "write buffered"
        );
        Ok(written as u32)
    }

    /// Flush all buffered writes for an open file to the cluster.
    ///
    /// On failure (e.g. a chunk write fails because a target node is down),
    /// a brand-new file — one that has never had any chunks committed — is
    /// unlinked from the manager so it doesn't survive as a 0-byte ghost.
    /// Returning the error to the caller is necessary but not sufficient: the
    /// Linux kernel may suppress close()-time errors, so leaving an orphan
    /// metadata entry creates the appearance of silent data loss. (Fix for
    /// F-013.)
    #[instrument(skip(self), fields(fh))]
    pub async fn flush(&self, fh: u64) -> Result<()> {
        let file = self
            .open_files
            .get(&fh)
            .context("invalid file handle")?;

        let path = file.path.clone();
        let was_fresh = file
            .locations
            .as_ref()
            .map(|l| l.total_size == 0 && l.chunks.is_empty())
            .unwrap_or(true);
        // P0-1: capture lease state for renewal-before-flush.
        let lease_id = file.lease_id.clone();
        let lease_expires_at_ms = file.lease_expires_at_ms;
        drop(file);

        // P0-1: if a lease is held and is past the renewal threshold,
        // renew BEFORE issuing the cluster writes. Otherwise a large flush
        // could exceed the lease lifetime and another writer would acquire
        // the path mid-flush.
        if let (Some(lease_id), Some(expires_at_ms)) = (lease_id.as_ref(), lease_expires_at_ms) {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let elapsed = expires_at_ms.saturating_sub(now_ms);
            // If less than half the lease remains, refresh proactively.
            if elapsed < (WRITE_LEASE_MS - WRITE_LEASE_RENEW_AFTER_MS) {
                match self
                    .client
                    .renew_write_lease(&path, lease_id, WRITE_LEASE_MS)
                    .await
                {
                    Ok(resp) if resp.granted => {
                        if let Some(mut entry) = self.open_files.get_mut(&fh) {
                            entry.value_mut().lease_expires_at_ms = Some(resp.new_expires_at_ms);
                        }
                    }
                    Ok(_) => {
                        // Lease lost (someone else took it). Abort the flush
                        // so we don't write past another writer's data.
                        return Err(anyhow::anyhow!(
                            "write lease lost for {path} during flush — aborting"
                        ));
                    }
                    Err(e) => {
                        // Manager unreachable. Continue best-effort: the
                        // existing lease may still be valid until GC.
                        tracing::warn!(
                            path = %path,
                            error = %e,
                            "lease renew failed; proceeding with existing lease until expiry"
                        );
                    }
                }
            }
        }

        let segments = self.write_buffer.flush_all(&path);
        let had_segments = !segments.is_empty();
        if had_segments {
            if let Err(e) = self.flush_segments_to_cluster(&path, &segments).await {
                if was_fresh {
                    warn!(
                        path = %path,
                        error = %e,
                        "flush of fresh file failed; unlinking ghost entry to surface error",
                    );
                    if let Err(unlink_err) = self.client.delete_file(&path).await {
                        warn!(
                            path = %path,
                            error = %unlink_err,
                            "failed to unlink ghost file after flush failure",
                        );
                    }
                    self.metadata_cache.remove(&path);
                }
                return Err(e);
            }
        }

        // Invalidate metadata cache since the file may have changed.
        self.metadata_cache.remove(&path);

        // F-029: after a successful flush, refresh the OpenFile's locations
        // so subsequent reads on the same fd see the freshly committed
        // chunks. Without this, the in-memory `OpenFile.locations` keeps the
        // pre-flush state (often total_size=0 + empty chunks for a new file)
        // and read() returns Bytes::new() because it thinks the file is
        // empty — which is what makes SQLite see no rows after an fsync.
        if had_segments {
            if let Ok(fresh) = self.fetch_file_locations(&path).await {
                for mut entry in self.open_files.iter_mut() {
                    if entry.value().path == path {
                        entry.value_mut().locations = Some(fresh.clone());
                    }
                }
                self.cache_metadata(&path, fresh);
            }
        }

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
            // P0-1: release the write lease (if held). Best-effort: a
            // network error here is logged, not propagated, since the lease
            // GC will reap it on expiry anyway.
            if let Some(lease_id) = &file.lease_id {
                if let Err(e) = self
                    .client
                    .release_write_lease(&file.path, lease_id)
                    .await
                {
                    tracing::warn!(
                        path = %file.path,
                        error = %e,
                        "release_write_lease failed; lease will expire naturally"
                    );
                }
            }
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

        // Open the newly created file for read-write. P0-1: acquire the
        // single-writer lease so a second create() against the same path
        // (race between two clients) returns EBUSY.
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        // Bug X — owner_id carries DC as a 4th field for the topology view.
        let owner_id = if self.client_location.datacenter.is_empty() {
            format!("{}{}", self.lease_owner_prefix, fh)
        } else {
            format!(
                "{}{}:{}",
                self.lease_owner_prefix, fh, self.client_location.datacenter
            )
        };
        let lease_resp = self
            .client
            .acquire_write_lease(path, &owner_id, WRITE_LEASE_MS)
            .await
            .with_context(|| format!("acquire_write_lease({path}) on create"))?;
        if !lease_resp.granted {
            return Err(anyhow::Error::new(LeaseBusy {
                path: path.to_string(),
                conflict_owner: lease_resp.conflict_owner,
                expires_at_ms: lease_resp.conflict_expires_at_ms,
            }));
        }

        self.open_files.insert(
            fh,
            OpenFile {
                path: path.to_string(),
                flags: OpenFlags::ReadWrite,
                append: false,
                locations: Some(FileLocations {
                    path: path.to_string(),
                    total_size: 0,
                    chunks: Vec::new(),
                    volume_id: response.volume_id,
                    chunk_size: response.chunk_size,
                    // Newly-created file: timestamps will be filled in on the
                    // next getattr/lookup once metadata round-trips back from
                    // the manager. 0 means "use UNIX_EPOCH for now".
                    created_at_ms: 0,
                    modified_at_ms: 0,
                    is_directory: false,
                }),
                chunk_size: response.chunk_size,
                lease_id: Some(lease_resp.lease_id),
                lease_expires_at_ms: Some(lease_resp.expires_at_ms),
                lease_owner_id: Some(owner_id),
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

    /// Return cluster-wide (total_bytes, used_bytes, file_count) for
    /// `statvfs` (F-038). Falls back to an error if the manager is down.
    pub async fn cluster_capacity(&self) -> Result<(u64, u64, u64)> {
        self.client.cluster_capacity().await
    }

    /// Update the manager-side logical size (F-033 fallocate / F-037
    /// truncate-grow helper). Drops the on-disk write buffer if the new
    /// size shrinks below buffered EOF.
    pub async fn set_size(&self, fh: u64, new_size: u64) -> Result<()> {
        let path = self
            .open_files
            .get(&fh)
            .map(|f| f.path.clone())
            .context("invalid fh")?;
        self.client
            .set_file_size(&path, new_size)
            .await
            .with_context(|| format!("set_size failed for {path}"))?;
        self.metadata_cache.remove(&path);
        Ok(())
    }

    /// F-037 — truncate. `new_size = 0` is the SQLite journal-recycle case.
    /// Shrink drops trailing chunks; grow extends sparsely.
    #[instrument(skip(self), fields(path = %path, new_size))]
    pub async fn truncate(
        &self,
        path: &str,
        fh: Option<u64>,
        new_size: u64,
    ) -> Result<()> {
        self.client
            .set_file_size(path, new_size)
            .await
            .with_context(|| format!("truncate failed for {path}"))?;
        // Clear any buffered writes past the new EOF.
        if let Some(buffered_eof) = self.write_buffer.logical_size(path) {
            if buffered_eof > new_size {
                self.write_buffer.remove(path);
            }
        }
        // Refresh cached locations.
        self.metadata_cache.remove(path);
        if let Some(fh) = fh {
            if let Ok(fresh) = self.fetch_file_locations(path).await {
                if let Some(mut entry) = self.open_files.get_mut(&fh) {
                    entry.value_mut().locations = Some(fresh.clone());
                }
                self.cache_metadata(path, fresh);
            }
        }
        Ok(())
    }

    /// F-034 — rename a file (atomic) in the manager namespace.
    #[instrument(skip(self), fields(from = %from, to = %to))]
    pub async fn rename(&self, from: &str, to: &str) -> Result<()> {
        self.client.rename_file(from, to).await
            .with_context(|| format!("rename {from} -> {to}"))?;
        self.metadata_cache.remove(from);
        self.metadata_cache.remove(to);
        self.write_buffer.remove(from);
        // Update any open files pointing at the old path.
        for mut entry in self.open_files.iter_mut() {
            if entry.value().path == from {
                entry.value_mut().path = to.to_string();
            }
        }
        Ok(())
    }

    /// F-035 — create a symbolic link.
    pub async fn symlink(&self, path: &str, target: &str) -> Result<()> {
        self.client.symlink(path, target).await
            .with_context(|| format!("symlink {path} -> {target}"))?;
        Ok(())
    }

    /// F-035 — read a symbolic link's target.
    pub async fn readlink(&self, path: &str) -> Result<String> {
        self.client.readlink(path).await
            .with_context(|| format!("readlink {path}"))
    }

    /// F-036 — create a hard link from src to dst.
    pub async fn link(&self, src: &str, dst: &str) -> Result<FileAttr> {
        self.client.link_file(src, dst).await
            .with_context(|| format!("link {src} -> {dst}"))?;
        let locs = self.fetch_file_locations(dst).await?;
        let attr = file_attr_from_locations(&locs);
        self.cache_metadata(dst, locs);
        Ok(attr)
    }

    /// F-039 — try to acquire a byte-range lock through the cluster manager.
    /// Returns `Ok(true)` on grant, `Ok(false)` on conflict, `Err(_)` on RPC failure.
    pub async fn try_acquire_lock(
        &self,
        path: &str,
        owner_id: &str,
        start: u64,
        end: u64,
        write: bool,
        lease_ms: u64,
    ) -> Result<bool> {
        let resp = self.client
            .acquire_lock(path, owner_id, start, end, write, false, lease_ms)
            .await?;
        Ok(resp.granted)
    }

    /// F-039 — release a previously-granted byte-range lock.
    pub async fn release_cluster_lock(
        &self,
        path: &str,
        owner_id: &str,
        start: u64,
        end: u64,
    ) -> Result<()> {
        self.client.release_lock(path, owner_id, start, end).await
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

        // Use the chunk_size the manager returned; fall back to 128 MiB only
        // when the field is absent (e.g. talking to an older manager build).
        let chunk_size = if response.chunk_size > 0 {
            response.chunk_size
        } else {
            128 * 1024 * 1024
        };

        Ok(FileLocations {
            path: response.path,
            total_size: response.total_size,
            chunks,
            // The manager response doesn't include volume_id directly in
            // GetFileLocationsResponse. We default to "default" and it
            // gets overridden when the file is created or opened with
            // a known volume.
            volume_id: "default".to_string(),
            chunk_size,
            created_at_ms: response.created_at_ms,
            modified_at_ms: response.modified_at_ms,
            is_directory: response.is_directory,
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

    /// Resolve the logical chunk size for a file.
    ///
    /// Uses the value returned by the manager in `GetFileLocationsResponse`
    /// (now stored in `FileLocations.chunk_size`). Falls back to 128 MiB only
    /// when the field was absent (fix for issue #13 — previously always
    /// returned 128 MiB, making byte-offset → chunk-index mapping wrong for
    /// any volume configured with a different chunk size).
    fn resolve_chunk_size(&self, locations: &FileLocations) -> u64 {
        if locations.chunk_size > 0 {
            locations.chunk_size
        } else {
            128 * 1024 * 1024
        }
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

    /// Sort candidate addresses by distance, nearest first.
    /// Used for replica fallback on read failure (P0-2).
    fn sort_candidates(&self, addresses: &[String]) -> Vec<String> {
        if addresses.len() <= 1 {
            return addresses.to_vec();
        }

        let known: HashMap<String, Location> = addresses
            .iter()
            .filter_map(|addr| {
                self.known_locations
                    .get(addr)
                    .map(|loc| (addr.clone(), loc.clone()))
            })
            .collect();

        dtfs_client::location::sort_nodes_by_distance(
            Some(&self.client_location),
            addresses,
            &known,
        )
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

        // Bug AV (AV-3-2) — translate the FUSE write config to a
        // proto-friendly tier hint. `Cold` keeps the legacy
        // behaviour (empty string = "no preference, use volume
        // default"); `NearestWarm` sends `"warm"` so the manager
        // honours the hint if a healthy warm node exists in the
        // client's DC.
        let preferred_tier: &str = match self.config.write.effective_first_write_tier() {
            dtfs_common::config::FirstWriteTier::Cold => "",
            dtfs_common::config::FirstWriteTier::NearestWarm => "warm",
        };

        // Request placements from the manager.
        let alloc_response = self
            .client
            .allocate_chunks_with_hint(
                path,
                segments.len() as u32,
                &volume_id,
                preferred_tier,
            )
            .await
            .with_context(|| format!("allocate_chunks failed for {path}"))?;

        let mut commits: Vec<ChunkLocationCommit> = Vec::with_capacity(segments.len());

        // For each segment: write the chunk to the primary AND every replica
        // target the manager assigned. Wait for all writes in parallel; require
        // at least quorum (ceil(N/2)) to succeed before acking.  Each
        // successful target produces a `ChunkLocationCommit` so the manager
        // ends up with N location entries — that's what actually makes
        // replication-factor=N real (fix for F-010).
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

            let mut targets: Vec<String> = Vec::with_capacity(1 + placement.replica_addresses.len());
            targets.push(placement.primary_address.clone());
            for r in &placement.replica_addresses {
                if !targets.contains(r) {
                    targets.push(r.clone());
                }
            }
            let rf = targets.len();
            let min_ok = ((rf + 1) / 2).max(1);

            // Issue writes in parallel — Bytes is refcounted so cloning is
            // cheap (zero-copy). Use the Bytes overload so the data buffer is
            // shared across all N replica writers instead of being copied N
            // times.
            // Bug AV (AV-4-1d) — pass the manager-resolved tier
            // through to the data-node. Empty string preserves the
            // legacy "any healthy disk" fallback for managers that
            // pre-date AV-4-1d. See `fuse_tier_for_placement`.
            let tier = fuse_tier_for_placement(&placement.tier).to_string();
            let writes = targets.iter().map(|addr| {
                let client = self.client.clone();
                let addr = addr.clone();
                let volume_id = volume_id.clone();
                let data = segment.data.clone();
                let tier = tier.clone();
                async move {
                    let res = client
                        .write_chunk_bytes(&addr, "", &volume_id, &tier, data)
                        .await;
                    (addr, res)
                }
            });
            let results = futures_util::future::join_all(writes).await;

            let mut successes: Vec<(String, _)> = Vec::with_capacity(rf);
            let mut last_err: Option<anyhow::Error> = None;
            for (addr, res) in results {
                match res {
                    Ok(put_resp) => successes.push((addr, put_resp)),
                    Err(e) => {
                        warn!(
                            chunk_index = segment.chunk_index,
                            node = %addr,
                            error = %e,
                            "write_chunk failed on replica",
                        );
                        last_err = Some(anyhow::anyhow!(e));
                    }
                }
            }

            if successes.len() < min_ok {
                return Err(last_err.unwrap_or_else(|| {
                    anyhow::anyhow!(
                        "write_chunk failed for chunk_index {}: 0/{} replicas succeeded",
                        segment.chunk_index,
                        rf
                    )
                })
                .context(format!(
                    "write_chunk failed for chunk_index {}: only {}/{} replicas succeeded (need {})",
                    segment.chunk_index,
                    successes.len(),
                    rf,
                    min_ok
                )));
            }

            for (addr, put_resp) in successes {
                commits.push(ChunkLocationCommit {
                    chunk_index: segment.chunk_index,
                    chunk_id: put_resp.chunk_id.clone(),
                    primary_address: addr,
                    size_bytes: put_resp.size,
                    // AV-4-8 — propagate the data-node's actual
                    // chosen disk_id so the manager's tiering
                    // engine resolves tier correctly. Pre-fix
                    // hardcoded 0 caused cold writes to be
                    // counted as warm and the cache planner
                    // wrongly thought warm cap was already met.
                    disk_id: put_resp.disk_id,
                });
            }

            debug!(
                path,
                chunk_index = segment.chunk_index,
                replicas = rf,
                size = segment.data.len(),
                "chunk written to cluster",
            );
        }

        // Bug P — find the lease_id for this path so the manager can
        // verify it before applying the commit. If no open file handle
        // has a lease (system flow, e.g. read-only path), an empty
        // string preserves the legacy no-verification behaviour.
        let lease_id = self
            .find_lease_id_for_path(path)
            .unwrap_or_default();

        self.client
            .commit_chunk_locations(path, &volume_id, commits, &lease_id)
            .await
            .with_context(|| format!("commit_chunk_locations failed for {path}"))?;

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

    /// Bug P — find the current write-lease id for an open file handle
    /// on `path`. Returns the first matching writer's lease_id; empty
    /// `None` if no writer holds a lease.
    fn find_lease_id_for_path(&self, path: &str) -> Option<String> {
        for entry in self.open_files.iter() {
            let file = entry.value();
            if file.path == path {
                if let Some(lid) = &file.lease_id {
                    return Some(lid.clone());
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

/// Build a `FileAttr` from `FileLocations`. Timestamps come from the manager's
/// `FileMetadata` and are exposed to the FUSE kernel via getattr (fix for F-014).
/// `is_directory` is propagated from the manager so empty-mkdir directories
/// and synthetic ones (implied by child paths) both report as directories —
/// SOAK-RECOVERY-2026-05-18 bugs A & B.
fn file_attr_from_locations(locations: &FileLocations) -> FileAttr {
    FileAttr {
        size: if locations.is_directory { 4096 } else { locations.total_size },
        is_directory: locations.is_directory,
        created_at: system_time_from_epoch_ms(locations.created_at_ms),
        modified_at: system_time_from_epoch_ms(locations.modified_at_ms),
    }
}

/// Bug AV (AV-4-1d) — returns the tier string FUSE will pass to the
/// data-node `put_chunk` RPC for the given manager-resolved placement
/// tier. Pre-AV-4-1d FUSE hardcoded `"hot"` here regardless of what
/// the manager resolved, which made the data-node's
/// `select_disk_for_tier(Hot)` fall back to "any healthy disk by free
/// capacity" (`disk.rs:106`) and silently land the chunk on the wrong
/// tier. Empty string is preserved verbatim — pre-AV-4-1d managers
/// don't populate `placement.tier`, and the data-node's legacy "any
/// healthy disk" fallback matches the old behaviour for that case.
fn fuse_tier_for_placement(placement_tier: &str) -> &str {
    placement_tier
}

/// Convert epoch milliseconds (i64) to `SystemTime`. Returns `UNIX_EPOCH` when
/// the input is zero or negative — those are sentinels for "not known yet".
fn system_time_from_epoch_ms(ms: i64) -> SystemTime {
    if ms > 0 {
        SystemTime::UNIX_EPOCH + Duration::from_millis(ms as u64)
    } else {
        SystemTime::UNIX_EPOCH
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bug AV (AV-4-1d) regression #1: the writer used to hardcode
    /// `"hot"` regardless of what the manager resolved. Pinning the
    /// contract so a future refactor can't silently regress.
    #[test]
    fn bug_av_4_1d_fuse_uses_placement_tier_not_hardcoded() {
        assert_eq!(
            fuse_tier_for_placement("cold"),
            "cold",
            "FUSE must propagate the manager-resolved tier, not hardcode 'hot'"
        );
        assert_eq!(fuse_tier_for_placement("warm"), "warm");
        assert_eq!(fuse_tier_for_placement("tape"), "tape");
        assert_eq!(fuse_tier_for_placement("hot"), "hot");
    }

    /// Bug AV (AV-4-1d) regression #2: old managers don't populate
    /// `placement.tier`. FUSE passes empty through so the data-node
    /// uses its legacy "any healthy disk" fallback (matches pre-fix
    /// behaviour for older clusters).
    #[test]
    fn bug_av_4_1d_fuse_empty_tier_preserves_legacy_fallback() {
        assert_eq!(fuse_tier_for_placement(""), "");
    }

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
            chunk_size: 128 * 1024 * 1024,
            created_at_ms: 0,
            modified_at_ms: 0,
            is_directory: false,
        };
        let attr = file_attr_from_locations(&loc);
        assert_eq!(attr.size, 42);
        assert!(!attr.is_directory);
    }

    /// SOAK-RECOVERY-2026-05-18 bugs A+B (FUSE side): when the manager
    /// reports `is_directory=true` (either an explicit mkdir entry or a
    /// synthetic directory implied by a child file path), `file_attr_from_
    /// locations` must propagate that flag through to the FUSE FileAttr.
    /// Before the fix, `is_directory` was hardcoded `false`, so every
    /// `getattr` returned "regular file" and `ls`/`stat` lost the
    /// directory-ness.
    #[test]
    fn bug_a_b_directory_is_directory_propagates_through_attrs() {
        let loc = FileLocations {
            path: "/corpus".to_string(),
            total_size: 0,
            chunks: Vec::new(),
            volume_id: "default".to_string(),
            chunk_size: 0,
            created_at_ms: 1,
            modified_at_ms: 1,
            is_directory: true,
        };
        let attr = file_attr_from_locations(&loc);
        assert!(attr.is_directory, "directory bit must round-trip into FileAttr");
        // A directory's logical size is moot — we report a placeholder
        // (4096 — one filesystem block) so `stat` doesn't say 0 bytes.
        assert_eq!(attr.size, 4096);
    }
}
