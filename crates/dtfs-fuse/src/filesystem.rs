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

impl FileLocations {
    /// REV-2-1 — true when the manager labelled its chunk entries, so the
    /// index carried by each entry can be trusted.
    ///
    /// A manager that predates `ChunkLocationInfo.chunk_index` sends 0 for
    /// every entry, which is indistinguishable from a real index 0. Treat the
    /// labels as authoritative only when at least one is non-zero, or when
    /// there is at most one chunk (where labelled and positional lookup agree
    /// anyway).
    fn indices_are_authoritative(&self) -> bool {
        self.chunks.len() <= 1 || self.chunks.iter().any(|c| c.chunk_index.unwrap_or(0) != 0)
    }

    /// REV-2-1 — the chunk covering file offset `index * chunk_size`.
    ///
    /// The manager builds its response from a `BTreeMap<chunk_index, _>` and
    /// emits only the VALUES, so the array is DENSE while a file's index space
    /// may be sparse (a truncate followed by a tail write, a partial
    /// multi-chunk commit, a location row removed for a whole index). Indexing
    /// that array by position therefore returned a DIFFERENT chunk's bytes at
    /// a valid offset — silently, with a valid checksum. Resolve by index
    /// instead, and report a genuine hole as absent rather than serving a
    /// neighbour.
    pub fn chunk_at(&self, index: u64) -> Option<&ChunkInfo> {
        if self.indices_are_authoritative() {
            self.chunks
                .iter()
                .find(|c| c.chunk_index.unwrap_or(0) == index)
        } else {
            self.chunks.get(index as usize)
        }
    }
}

/// Information about a single chunk within a file.
#[derive(Debug, Clone)]
pub struct ChunkInfo {
    pub chunk_id: String,
    pub size: u64,
    pub node_addresses: Vec<String>,
    /// U4 read-locality: per-replica storage tier (lowercase string),
    /// positionally aligned 1:1 with `node_addresses`. Comes from the
    /// manager's `ChunkLocationInfo.node_tiers`. Used as a distance tie-break
    /// in read replica selection (prefer warm over the cold anchor HDD). An
    /// older manager leaves this empty (or shorter); missing entries are
    /// treated as unknown tier, falling back to pure distance.
    pub node_tiers: Vec<String>,
    /// CO-3-10 read-locality: per-replica topology Location, positionally
    /// aligned 1:1 with `node_addresses`. Comes from the manager's
    /// `ChunkLocationInfo.node_locations`. Loaded into `known_locations` so
    /// `select_read_replica` can rank replicas by physical distance +
    /// `read_location_priority`. An older manager leaves this empty (or
    /// shorter); a missing/all-empty entry is treated as an unknown location
    /// (no insert -> arbitrary/pure-distance order).
    pub node_locations: Vec<Location>,
    /// REV-2-1: this entry's chunk index within the file, straight from the
    /// manager's `ChunkLocationInfo.chunk_index`. `None` means the entry was
    /// synthesised locally; an older manager sends 0 for every entry, which
    /// `FileLocations::indices_are_authoritative` detects so the caller falls
    /// back to positional lookup.
    pub chunk_index: Option<u64>,
}

impl ChunkInfo {
    /// Pair each replica address with its tier string for read selection.
    /// Tolerates a shorter/absent `node_tiers` (older manager) by treating
    /// the missing positions as unknown (`""`).
    fn replicas_with_tiers(&self) -> Vec<(String, String)> {
        self.node_addresses
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                let tier = self.node_tiers.get(i).cloned().unwrap_or_default();
                (addr.clone(), tier)
            })
            .collect()
    }
}

/// CO-3-10: a Location is "known" only if at least one field is non-empty.
/// An all-empty Location is the manager's placeholder for an unknown replica
/// node (or an older manager that sent nothing), so it must NOT be learned
/// into `known_locations` — leaving it absent keeps the pre-CO-3-10
/// arbitrary/pure-distance fallback for that replica.
fn location_is_known(loc: &Location) -> bool {
    !(loc.region.is_empty()
        && loc.datacenter.is_empty()
        && loc.rack.is_empty()
        && loc.host.is_empty())
}

/// CO-3-10: learn each replica's Location (positionally aligned with
/// `addrs`) into `known_locations`, keyed by node address. Skips any
/// all-empty Location (unknown node / older manager) so the read path keeps
/// its arbitrary/pure-distance fallback for that replica. `addrs` and `locs`
/// may differ in length (older manager sends fewer/zero locations); `zip`
/// stops at the shorter, leaving the surplus replicas unknown.
fn learn_locations(known: &DashMap<String, Location>, addrs: &[String], locs: &[Location]) {
    for (addr, loc) in addrs.iter().zip(locs.iter()) {
        if location_is_known(loc) {
            known.insert(addr.clone(), loc.clone());
        }
    }
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
    /// CR-1-4 (FUSE-3): true once `flush_segments_to_cluster` has
    /// successfully committed at least one chunk for this path, ever. Set
    /// on EVERY `OpenFile` sharing the path (mirrors the F-029 locations
    /// refresh), so a stale per-fh snapshot can never wrongly report
    /// "nothing committed yet" for a file another fh already flushed.
    /// `release()` uses `!committed_any` (never the transient per-flush
    /// `locations` snapshot) to decide whether a flush failure may
    /// ghost-unlink the file — a file with any committed chunk must never
    /// be unlinked, and its drained-but-unsent segments must always be
    /// restored instead.
    committed_any: bool,
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
    /// Migration trigger for requesting closer replicas. WI-09 — held as an
    /// `Arc` so `read()` can spawn the advisory RPC (`spawn_on_remote_read`)
    /// instead of awaiting it inline while holding a `known_locations`
    /// DashMap guard (CO-3-12 anti-pattern).
    migration: std::sync::Arc<MigrationTrigger>,
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

        // Bug BW — the write buffer MUST split files at the same chunk size
        // the volume uses (recorded in file metadata, default 4 MB), because
        // reads derive chunk offsets from that metadata. The client used to
        // hardcode 128 MB here, so a >4 MB file was stored as one oversized
        // chunk while metadata said 4 MB and reads past the first 4 MB
        // returned garbage. Driven from config now (must match the volume's
        // chunk_size_bytes).
        let chunk_size_bytes = (config.client.chunk_size_mb.max(1) as usize) * 1024 * 1024;

        let readahead = ReadAheadEngine::new(
            readahead_mode,
            client.clone(),
            // Bug CE — byte budget, not entry count. 256 entries x whole
            // chunks OOM-killed the 1 GB client; bound by config bytes now.
            (config.readahead.cache_mb.max(1) as usize) * 1024 * 1024,
            // WI-05 — cap on concurrent prefetch GetChunk streams so the
            // near chunk isn't starved by an uncapped fan-out on a slow
            // link (e.g. the 40 Mbps LELI path).
            config.readahead.prefetch_max_inflight.max(1) as usize,
        );

        // WI-01 — hold back complete chunks until `flush_batch_chunks` of
        // them exist, amortizing the allocate+commit round trip over a
        // batch instead of firing once per chunk (default 4).
        let write_buffer = WriteBuffer::new_with_batch(
            chunk_size_bytes,
            config.write.flush_batch_chunks as usize,
        );

        let migration = std::sync::Arc::new(MigrationTrigger::new(
            client.clone(),
            client_location.clone(),
        ));

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

    /// CF-5 — cheap clonable handle to the readahead cache, captured before
    /// the filesystem is moved into the fuse adapter so the memory-pressure
    /// ladder task can shrink/clear the prefetch cache under RSS pressure.
    pub fn readahead_controller(&self) -> crate::readahead::ReadAheadCacheController {
        self.readahead.cache_controller()
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

        // F-021 / CR-1-2 (FUSE-7): when opened with O_APPEND, pre-seed the
        // write buffer with the file's existing tail chunk so the
        // subsequent write(s) extend it instead of replacing the chunk
        // wholesale. Without this, the next flush emits a chunk segment
        // containing ONLY the appended bytes, which silently truncates the
        // file at the append offset. Runs AFTER the write lease is
        // acquired, so a losing racer never seeds a buffer it will not be
        // allowed to write through; resolves the tail chunk by its real
        // index (`chunk_at(max index)`), never by array position, since the
        // manager's array is dense while the file's index space may be
        // sparse (REV-2-1).
        if append {
            let max_idx = if locations.indices_are_authoritative() {
                locations.chunks.iter().filter_map(|c| c.chunk_index).max()
            } else if locations.chunks.is_empty() {
                None
            } else {
                Some(locations.chunks.len() as u64 - 1)
            };
            if let Some(last_idx) = max_idx {
                if let Some(last_chunk) = locations.chunk_at(last_idx) {
                    if !last_chunk.node_addresses.is_empty() {
                        let node = self.select_nearest_node(&last_chunk.node_addresses);
                        match self.client.read_chunk(&node, &last_chunk.chunk_id).await {
                            Ok(existing) => {
                                let offset_in_file = last_idx * chunk_size;
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
            }
        }

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
                committed_any: false,
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
        // CO-3-12: clone everything the chunk loop needs out of the DashMap
        // entry, then DROP the guard before the per-chunk network loop. A
        // stalled chunk fetch must not hold the open_files entry guard, or it
        // serializes / deadlocks concurrent opens of the same file. Mirrors
        // the write()/flush() idiom.
        let (locations, chunk_size, file_path, volume_id) = {
            let file = self
                .open_files
                .get(&fh)
                .context("invalid file handle")?;
            let locations = file
                .locations
                .clone()
                .context("file has no location data")?;
            let chunk_size = file.chunk_size;
            let file_path = file.path.clone();
            let volume_id = locations.volume_id.clone();
            (locations, chunk_size, file_path, volume_id)
        }; // guard dropped here, before the await loop below

        // F-029: effective file size = max(persisted size, buffered logical
        // size). Without this, every read of bytes that have only been
        // *buffered* (not yet flushed to a chunk) returns Bytes::new() and
        // SQLite / any DB engine sees an empty database after `CREATE TABLE`.
        let buffered_eof = self.write_buffer.logical_size(&file_path).unwrap_or(0);
        let file_size = locations.total_size.max(buffered_eof);
        if offset >= file_size {
            return Ok(Bytes::new());
        }
        let actual_size = effective_read_len(file_size, offset, size);

        // Determine which chunks we need to read.
        let start_chunk = (offset / chunk_size) as usize;
        let end_offset = offset + actual_size as u64;
        let end_chunk = ((end_offset + chunk_size - 1) / chunk_size) as usize;

        // REV-2-1 — the buffer is pre-zeroed and each chunk is copied to its
        // OWN offset within the read, not appended. A sparse file may have a
        // hole in the middle of the requested range; appending would slide
        // every later chunk's bytes down into the hole.
        let mut result = vec![0u8; actual_size as usize];

        // REV-2-3 — readahead needs each chunk's REAL index (the manager's
        // array is dense, the index space may be sparse — REV-2-1) plus a
        // candidate list in the same order the foreground read uses. Both are
        // built lazily at the cache-miss point below: `chunk_locs` is only
        // consumed there, and `read_candidates` sorts per chunk, so building
        // it for every chunk of the file on every 128 KB read would be pure
        // waste on the (common) cache-hit path.
        let indices_trusted = locations.indices_are_authoritative();
        let total_chunks = if indices_trusted {
            locations
                .chunks
                .iter()
                .filter_map(|c| c.chunk_index)
                .max()
                .map(|m| m + 1)
                .unwrap_or(0) as u32
        } else {
            locations.chunks.len() as u32
        };

        for chunk_idx in start_chunk..end_chunk {
            // REV-2-1 — resolve by chunk index, never by array position. The
            // manager's array is dense while the file's index space may be
            // sparse, so positional lookup served a different chunk's bytes.
            let Some(chunk) = locations.chunk_at(chunk_idx as u64) else {
                // A genuine hole. Sparse regions read as zeros: `result` is
                // resized to `actual_size` below, so skipping leaves this
                // chunk's span zero-filled rather than shifting a later
                // chunk's bytes into it.
                debug!(
                    path = %file_path,
                    chunk_idx,
                    "REV-2-1: no chunk entry for this index; serving zeros for the hole"
                );
                continue;
            };
            if chunk.node_addresses.is_empty() {
                bail!("no data nodes available for chunk {}", chunk.chunk_id);
            }

            // P0-2 + U4: build the candidate list with the U4-preferred
            // replica first (read_location_priority -> distance -> warm tie-
            // break), then distance-ordered fallback. Try each on failure
            // until one succeeds. Single-replica clusters and single-address
            // chunks degenerate to one attempt.
            let candidates = self.read_candidates(chunk);

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

            // WI-05 (Step 4) + B2 — a chunk already ON THE WIRE as a
            // prefetch is JOINED, not duplicated. Only the FIRST candidate
            // is tried: prefetch targets `read_candidates`' head (REV-2-3),
            // the same replica this read would pick, so no other key can
            // hold a pending entry for this chunk — and trying only one
            // candidate keeps the worst case at exactly one
            // `readahead::PREFETCH_JOIN_TIMEOUT` per chunk. `join_pending`
            // returns `None` immediately for a queued-but-not-started
            // prefetch, and after 2s for a stalled one, so the
            // unconditional fall-through below always runs the ordinary
            // candidate loop (P0-2 replica fallback stays reachable) well
            // inside case 69's `ELAPSED <= TMO*3` budget.
            if chunk_data.is_none() {
                if let Some(cand) = candidates.first() {
                    if let Some(joined) = self
                        .readahead
                        .join_pending(cand, &chunk.chunk_id, crate::readahead::PREFETCH_JOIN_TIMEOUT)
                        .await
                    {
                        debug!(
                            chunk_id = %chunk.chunk_id,
                            node = %cand,
                            "readahead prefetch join hit"
                        );
                        chunk_data = Some(joined);
                        if cache_hit_notify_addr.is_none() {
                            cache_hit_notify_addr = Some(cand.clone());
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
                // FUSE-HOL — bound each per-replica fetch with a timeout so a
                // hung/dead node (accepts the connection but never responds, so
                // no transport error fires) cannot stall the read — and the
                // application's FUSE op — indefinitely. On timeout we fail over
                // to the next candidate exactly like a transport error. `0`
                // disables the bound (legacy unbounded await).
                let fetch_timeout_secs = self.config.client.chunk_read_timeout_secs;
                let mut last_err: Option<anyhow::Error> = None;
                let mut fetched: Option<(String, bytes::Bytes)> = None;
                for cand in &candidates {
                    let fetch = self.client.read_chunk(cand, &chunk.chunk_id);
                    let result = if fetch_timeout_secs == 0 {
                        fetch.await
                    } else {
                        match tokio::time::timeout(
                            std::time::Duration::from_secs(fetch_timeout_secs),
                            fetch,
                        )
                        .await
                        {
                            Ok(r) => r,
                            Err(_) => {
                                tracing::warn!(
                                    chunk_id = %chunk.chunk_id,
                                    node = %cand,
                                    timeout_secs = fetch_timeout_secs,
                                    "FUSE-HOL: chunk fetch timed out, failing over to next replica"
                                );
                                last_err = Some(anyhow::anyhow!(
                                    "chunk fetch from {cand} timed out after {fetch_timeout_secs}s"
                                ));
                                continue;
                            }
                        }
                    };
                    match result {
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
                debug!(
                    chunk_id = %chunk.chunk_id,
                    node = %node_addr,
                    size = data.len(),
                    "chunk fetch complete (foreground miss)"
                );

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

                // Trigger readahead for upcoming chunks. REV-2-3 — label each
                // entry with its real index and pre-order its replicas with
                // `read_candidates`, so the prefetch hits the same replica the
                // foreground read would pick and caches under the key that
                // read looks up.
                let chunk_locs: Vec<(u64, String, Vec<String>)> = locations
                    .chunks
                    .iter()
                    .enumerate()
                    .map(|(pos, c)| {
                        let idx = if indices_trusted {
                            c.chunk_index.unwrap_or(pos as u64)
                        } else {
                            pos as u64
                        };
                        (idx, c.chunk_id.clone(), self.read_candidates(c))
                    })
                    .collect();
                self.readahead.on_chunk_read(
                    &file_path,
                    chunk_idx as u32,
                    total_chunks,
                    &chunk_locs,
                );

                // Trigger migration check using the actually-served node so
                // tiering sees the real cross-DC traffic, not the nearest
                // candidate we wished we'd used.
                //
                // WI-09 (CO-3-12) — `.map(|r| r.clone())` drops the
                // DashMap `Ref` guard at the end of this statement, before
                // `spawn_on_remote_read` runs, so no `known_locations`
                // shard guard is held across the spawn (previously the
                // if-let bound the guard itself for the whole body and an
                // inline `.await` on the migration RPC could hold it for
                // up to the ~15s manager-walk deadline, blocking any
                // concurrent `learn_locations` insert into that shard).
                // The RPC is fire-and-forget: its result is advisory only.
                let node_loc = self.known_locations.get(&node_addr).map(|r| r.clone());
                if let Some(node_loc) = node_loc {
                    self.migration.clone().spawn_on_remote_read(
                        &chunk.chunk_id,
                        node_loc,
                        &file_path,
                        &volume_id,
                    );
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
                // Destination is this chunk's position within the read, so a
                // skipped hole stays zero-filled instead of shifting bytes.
                let dest_start =
                    (chunk_start_byte + slice_start as u64).saturating_sub(offset) as usize;
                let dest_end = (dest_start + (slice_end - slice_start)).min(result.len());
                if dest_start < dest_end {
                    let take = dest_end - dest_start;
                    result[dest_start..dest_end]
                        .copy_from_slice(&chunk_data[slice_start..slice_start + take]);
                }
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
        // for EVERY chunk this write touches (including index 0 — CR-1-2
        // drops the old `effective_offset > 0` gate, which left chunk 0
        // truncated on a header-patch write such as `dd conv=notrunc` or
        // `mkvpropedit`) that isn't already fully covered by buffered data.
        // A seed read failure is fatal (`?` -> EIO) rather than a warning:
        // continuing would let `write()` merge the caller's bytes into a
        // chunk buffer that still doesn't cover the whole chunk, which the
        // next flush would then commit as the chunk's complete (truncated)
        // content — silent data loss, not just a truncated append.
        if chunk_size > 0 && !data.is_empty() {
            for chunk_idx in
                chunks_needing_seed(&self.write_buffer, &path, chunk_size, effective_offset, data.len())
            {
                let Some(loc) = &locations_snapshot else { continue };
                // REV-2-1 — resolve by index, not by array position:
                // seeding the RMW buffer from a NEIGHBOURING chunk would
                // write that chunk's bytes back under this chunk's index.
                let Some(chunk) = loc.chunk_at(chunk_idx) else { continue };
                if chunk.node_addresses.is_empty() {
                    continue;
                }
                let chunk_start = chunk_idx * chunk_size;
                let node = self.select_nearest_node(&chunk.node_addresses);
                let existing = self
                    .client
                    .read_chunk(&node, &chunk.chunk_id)
                    .await
                    .with_context(|| {
                        format!("RMW seed read failed for {path} chunk {chunk_idx}")
                    })?;
                if !existing.is_empty() {
                    self.write_buffer.write(&path, chunk_start, &existing);
                    debug!(
                        path = %path,
                        chunk_idx,
                        len = existing.len(),
                        "RMW: seeded existing chunk before partial write"
                    );
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
            if let Err(e) = self.flush_segments_to_cluster(&path, &segments).await {
                // CO-1-8 (a): `flush` already drained these complete chunks out
                // of the buffer. Restore them on a cluster-write error so the
                // bytes survive for the next flush instead of being dropped
                // while the write call reports failure upstream.
                self.write_buffer.restore_segments(&path, &segments);
                return Err(e);
            }
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

        // CR-1-3 (FUSE-2): the kernel issues FUSE flush on every close(2),
        // including a read-only open. A reader closing a file a writer is
        // still streaming into must never touch the writer's path-keyed
        // write buffer — that forced a mid-stream partial commit of the
        // writer's tail and could drop bytes the writer buffered in the
        // meantime. Bail out before doing anything for a non-writable fh.
        if !file.flags.is_writable() {
            return Ok(());
        }

        let path = file.path.clone();
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
                // CR-1-4 (FUSE-3): ALWAYS restore the drained segments on a
                // cluster-write failure, regardless of whether this file
                // has ever committed anything before. `flush_all` already
                // drained these bytes out of the buffer; without restoring
                // them here, a file that already had chunks committed
                // would silently lose its newest (uncommitted) tail on a
                // transient failure (e.g. an election taking longer than
                // the manager-walk deadline) — the ghost-unlink decision
                // belongs to `release()`, not here, and must never run
                // instead of this restore.
                self.write_buffer.restore_segments(&path, &segments);
                return Err(e);
            }
            // CR-1-4: mark every OpenFile sharing this path as having
            // committed at least once, ever — this is what makes
            // `release()`'s ghost-unlink decision safe. A per-fh snapshot
            // of `locations` was stale between write-driven batch flushes
            // (only refreshed by `flush()` itself), so a file that HAD
            // committed several chunks via mid-stream batch flushes could
            // still read as "fresh" (was_fresh) at a later `flush()` if a
            // fresh OpenFile's `locations` snapshot predated those
            // commits.
            for mut entry in self.open_files.iter_mut() {
                if entry.value().path == path {
                    entry.value_mut().committed_any = true;
                }
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
        // CO-1-8 (c): flush before closing, but DO NOT early-return on a flush
        // error. The kernel closes the fd regardless, so we must always tear
        // down the handle, its write lease, and its write buffer; otherwise the
        // fh leaks and the stale buffer overlays (incorrectly) future reads of
        // the same path. Capture the error and surface it AFTER cleanup.
        let flush_result = if self.open_files.contains_key(&fh) {
            self.flush(fh).await
        } else {
            Ok(())
        };

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
            // CR-1-3 (FUSE-2): only a writable fh may drop the path-keyed
            // write buffer, and only when no OTHER writable fh on the same
            // path remains open. Without this, a reader's release() (e.g.
            // a Plex scanner closing a file a downloader is still
            // streaming into) drained the writer's buffer — bytes the
            // writer had buffered, or that CO-1-8's `restore_segments` had
            // just put back, vanished with no error surfaced to the
            // writer. `file` (this fh) has already been removed from
            // `open_files` above, so the scan below only sees OTHER fhs.
            if file.flags.is_writable() {
                let remaining: Vec<(String, OpenFlags)> = self
                    .open_files
                    .iter()
                    .map(|e| (e.value().path.clone(), e.value().flags))
                    .collect();
                if !other_writer_open(&remaining, &file.path) {
                    // On a flush failure the un-flushed bytes are dropped
                    // here (the data is lost on the cluster too) but the
                    // error is propagated below so the caller sees the
                    // failure rather than a silent success.
                    self.write_buffer.remove(&file.path);
                }
            }

            // CR-1-4 (FUSE-3): the ghost-unlink now lives here, not in
            // flush(), and fires ONLY when this path has NEVER committed a
            // single chunk (`!committed_any` — tracked across every
            // OpenFile of the path, so it can't go stale between
            // write()-driven batch flushes). A file with any committed
            // chunk is never unlinked on a flush failure; `flush()` always
            // restores the drained segments instead so the next flush can
            // retry them.
            if should_ghost_unlink(flush_result.is_err(), file.committed_any) {
                warn!(
                    path = %file.path,
                    "flush of never-committed file failed at release; unlinking ghost entry",
                );
                if let Err(unlink_err) = self.client.delete_file(&file.path).await {
                    warn!(
                        path = %file.path,
                        error = %unlink_err,
                        "failed to unlink ghost file after flush failure",
                    );
                }
                self.metadata_cache.remove(&file.path);
            }

            debug!(path = %file.path, fh, "file released");
        }

        flush_result
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
                committed_any: false,
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
        // CO-1-8 (b): clear buffered writes AT OR ABOVE the new EOF but keep
        // buffered bytes below it. Removing the whole buffer (the old
        // behaviour) lost un-flushed writes that the truncation never deleted.
        self.write_buffer.truncate(path, new_size);
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

        let chunks: Vec<ChunkInfo> = response
            .chunks
            .into_iter()
            .map(|c| {
                // CO-3-10: convert the manager's per-replica proto Locations
                // (positionally aligned with node_addresses) into topology
                // Locations and learn them into known_locations so read
                // selection can rank by distance + read_location_priority.
                // An older manager sends no node_locations (or fewer than
                // node_addresses); a missing/all-empty entry is skipped, so
                // that replica's location stays unknown (pre-CO-3-10 order).
                let node_locations: Vec<Location> = c
                    .node_locations
                    .into_iter()
                    .map(|l| Location::new(&l.region, &l.datacenter, &l.rack, &l.host))
                    .collect();
                learn_locations(&self.known_locations, &c.node_addresses, &node_locations);
                ChunkInfo {
                    chunk_id: c.chunk_id,
                    size: c.size,
                    node_addresses: c.node_addresses,
                    node_tiers: c.node_tiers,
                    node_locations,
                    // REV-2-1 — carry the manager's label. An older manager
                    // sends 0 for every entry; `indices_are_authoritative`
                    // spots that and keeps the positional fallback.
                    chunk_index: Some(c.chunk_index),
                }
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

    /// U4: ordered read candidates for one chunk. The first element is the
    /// U4-preferred replica (configured `read_location_priority` -> physical
    /// distance -> warm-over-cold tier tie-break); the remaining elements are
    /// distance-ordered fallback (P0-2), so a read that fails the preferred
    /// replica still walks the rest nearest-first.
    fn read_candidates(&self, chunk: &ChunkInfo) -> Vec<String> {
        let addresses = &chunk.node_addresses;
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

        let replicas = chunk.replicas_with_tiers();
        let preferred = dtfs_client::location::select_read_replica(
            Some(&self.client_location),
            &replicas,
            &known,
            &self.config.read_location_priority,
        );

        // Distance-ordered fallback for the remaining replicas.
        let mut ordered = dtfs_client::location::sort_nodes_by_distance(
            Some(&self.client_location),
            addresses,
            &known,
        );
        if let Some(winner) = preferred {
            // Hoist the U4 winner to the front, keeping the rest's order.
            ordered.retain(|a| a != &winner);
            ordered.insert(0, winner);
        }
        ordered
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
            let mut placement = alloc_response
                .placements
                .get(i)
                .with_context(|| {
                    format!(
                        "manager returned fewer placements than requested ({} < {})",
                        alloc_response.placements.len(),
                        segments.len()
                    )
                })?
                .clone();

            // Bug BU-2 — bounded reroute. If the assigned placement can't
            // accept the write because its target-tier disk is full
            // (put_chunk returns RESOURCE_EXHAUSTED), re-allocate this chunk
            // with the full node excluded and retry on a node with room,
            // instead of failing the write and unlinking the file. BU-1
            // already keeps full disks out of the ring, so this only fires
            // on the fill-mid-write race.
            const MAX_REROUTES: usize = 3;
            let mut excluded: Vec<String> = Vec::new();
            let mut reroute = 0usize;

            let successes = loop {
                let mut targets: Vec<String> =
                    Vec::with_capacity(1 + placement.replica_addresses.len());
                targets.push(placement.primary_address.clone());
                for r in &placement.replica_addresses {
                    if !targets.contains(r) {
                        targets.push(r.clone());
                    }
                }
                let rf = targets.len();
                let min_ok = ((rf + 1) / 2).max(1);
                // Bug AV (AV-4-1d) — pass the manager-resolved tier through to
                // the data-node so it lands the chunk on the right tier.
                let tier = fuse_tier_for_placement(&placement.tier).to_string();

                // Issue writes in parallel — Bytes is refcounted so cloning is
                // cheap (zero-copy), so the buffer is shared across replicas.
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
                let mut exhausted_addrs: Vec<String> = Vec::new();
                for (addr, res) in results {
                    match res {
                        Ok(put_resp) => successes.push((addr, put_resp)),
                        Err(e) => {
                            if is_resource_exhausted(&e) {
                                exhausted_addrs.push(addr.clone());
                            }
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

                if successes.len() >= min_ok {
                    break successes;
                }

                // Shortfall — reroute only if a target was full AND we have
                // retry budget; otherwise surface the original error.
                if reroute >= MAX_REROUTES || exhausted_addrs.is_empty() {
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

                reroute += 1;
                for a in &exhausted_addrs {
                    if !excluded.contains(a) {
                        excluded.push(a.clone());
                    }
                }
                warn!(
                    chunk_index = segment.chunk_index,
                    reroute,
                    excluded = ?excluded,
                    "Bug BU-2 rerouting chunk around full disk(s)"
                );
                let realloc = self
                    .client
                    .allocate_chunks_excluding(path, 1, &volume_id, preferred_tier, &excluded)
                    .await
                    .with_context(|| format!("BU-2 reroute allocate failed for {path}"))?;
                placement = realloc.placements.into_iter().next().with_context(|| {
                    format!(
                        "BU-2 reroute for {path} chunk {}: no eligible placement (tier exhausted)",
                        segment.chunk_index
                    )
                })?;
            };

            for (addr, put_resp) in successes {
                commits.push(ChunkLocationCommit {
                    chunk_index: segment.chunk_index,
                    chunk_id: put_resp.chunk_id.clone(),
                    primary_address: addr,
                    size_bytes: put_resp.size,
                    // AV-4-8 — propagate the data-node's actual chosen disk_id
                    // so the manager's tiering engine resolves tier correctly.
                    disk_id: put_resp.disk_id,
                });
            }

            debug!(
                path,
                chunk_index = segment.chunk_index,
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

/// CO-1-6 — clamp a read length to the bytes actually available without
/// the `(file_size - offset) as u32` mod-2^32 truncation that broke reads
/// of files >= 4 GiB. `(file_size - offset)` can exceed `u32::MAX` for a
/// large file read near its start; casting that straight to `u32` wraps
/// (e.g. exactly 4 GiB -> 0), so the kernel saw a premature EOF and
/// large-remux playback truncated ~4 GiB early. Clamp the available byte
/// count to `u32::MAX` BEFORE the cast, then `min` with the requested size.
/// Callers must guarantee `offset < file_size` (read() returns empty
/// otherwise), so `file_size - offset` never underflows here.
fn effective_read_len(file_size: u64, offset: u64, size: u32) -> u32 {
    size.min((file_size - offset).min(u32::MAX as u64) as u32)
}

/// CR-1-2 (FUSE-7) — chunk indices a write of `len` bytes at `offset`
/// touches that are NOT already fully covered by buffered data, and
/// therefore need an RMW seed read before the write proceeds. Pure w.r.t.
/// the network: the caller still needs the manager's existing chunk
/// metadata to know whether there is anything to seed FROM (a brand-new
/// chunk the cluster has never seen has nothing to seed).
///
/// Deliberately includes chunk index 0 — the pre-CR-1-2 code gated this on
/// `effective_offset > 0`, which left chunk 0 unseeded on a header-patch
/// write (`dd conv=notrunc`, `mkvpropedit`) starting at offset 0.
fn chunks_needing_seed(
    write_buffer: &WriteBuffer,
    path: &str,
    chunk_size: u64,
    offset: u64,
    len: usize,
) -> Vec<u64> {
    if chunk_size == 0 || len == 0 {
        return Vec::new();
    }
    let start_chunk = offset / chunk_size;
    let end_chunk = (offset + len as u64 - 1) / chunk_size;
    (start_chunk..=end_chunk)
        .filter(|&idx| !write_buffer.chunk_is_fully_buffered(path, idx as u32))
        .collect()
}

/// CR-1-4 (FUSE-3) — should `release()` ghost-unlink the path after a
/// failed flush? Only when the flush actually failed AND the path has
/// NEVER committed a single chunk. A file with any committed chunk must
/// never be unlinked; `flush()` always restores its drained segments on
/// failure instead, regardless of this decision.
fn should_ghost_unlink(flush_failed: bool, committed_any: bool) -> bool {
    flush_failed && !committed_any
}

/// CR-1-3 (FUSE-2) — true iff `remaining` (the OTHER open file handles,
/// i.e. with the fh being released already excluded) contains a writable
/// handle on `path`. Extracted so `release()`'s "who owns the write
/// buffer" decision is testable without a live `DtfsFilesystem`/
/// `DtfsClient`.
fn other_writer_open(remaining: &[(String, OpenFlags)], path: &str) -> bool {
    remaining
        .iter()
        .any(|(p, flags)| p == path && flags.is_writable())
}

/// Bug BU-2 — did this write error originate from a data-node out-of-space
/// (ENOSPC)? `client::write_chunk_bytes` surfaces the gRPC status code in
/// its error message (`code=ResourceExhausted`) after exhausting the Bug G
/// memory-shed retries, so a disk-full put is reroute-able while a genuine
/// internal fault is not. Matching on the rendered chain keeps this
/// independent of the error's concrete type.
fn is_resource_exhausted(e: &anyhow::Error) -> bool {
    format!("{e:#}").contains("ResourceExhausted")
}

#[cfg(test)]
mod cr_1_2_seed_tests {
    use super::{chunks_needing_seed, WriteBuffer};

    // (a) CR-1-2 drops the old `effective_offset > 0` gate: a write at
    // offset 0 of an as-yet-unbuffered chunk must still be flagged for
    // seeding, not skipped just because it starts at the beginning.
    #[test]
    fn offset_zero_is_flagged_for_seeding() {
        let wb = WriteBuffer::new(4096);
        let touched = chunks_needing_seed(&wb, "/f", 4096, 0, 100);
        assert_eq!(touched, vec![0], "chunk 0 must be seeded, not skipped");
    }

    // (b) a write straddling two chunks must flag BOTH for seeding when
    // neither is already fully buffered.
    #[test]
    fn straddling_write_flags_both_chunks() {
        let wb = WriteBuffer::new(4096);
        let touched = chunks_needing_seed(&wb, "/f", 4096, 4096 - 50, 100);
        assert_eq!(touched, vec![0, 1], "both straddled chunks need seeding");
    }

    // A chunk that is already fully buffered (e.g. a previous seed + write
    // already completed it) must NOT be flagged again.
    #[test]
    fn fully_buffered_chunk_is_not_flagged() {
        let wb = WriteBuffer::new(4096);
        wb.write("/f", 0, &vec![0u8; 4096]); // fully covers chunk 0
        let touched = chunks_needing_seed(&wb, "/f", 4096, 0, 100);
        assert!(touched.is_empty(), "already-complete chunk needs no seed");
    }

    // A write spanning three chunks where only the middle one is already
    // resident-and-complete must flag exactly the other two.
    #[test]
    fn only_incomplete_chunks_are_flagged() {
        let wb = WriteBuffer::new(4096);
        wb.write("/f", 4096, &vec![0u8; 4096]); // chunk 1 fully buffered
        let touched = chunks_needing_seed(&wb, "/f", 4096, 0, 4096 * 3);
        assert_eq!(touched, vec![0, 2]);
    }
}

#[cfg(test)]
mod cr_1_4_ghost_unlink_tests {
    use super::should_ghost_unlink;

    // A file that already committed at least one chunk must NEVER be
    // ghost-unlinked, no matter how the flush fails — this is the FUSE-3
    // regression: a 20 MB file with 4 chunks already committed via
    // write()-driven batch flushes was unlinked (tail dropped) because a
    // stale per-fh `was_fresh` snapshot said "fresh".
    #[test]
    fn never_unlinks_a_file_with_any_committed_chunk() {
        assert!(!should_ghost_unlink(true, true), "committed_any must veto the unlink");
        assert!(!should_ghost_unlink(false, true));
    }

    // A brand-new file (create()'d, never committed) whose only flush
    // attempt fails is the one case the ghost-unlink exists for.
    #[test]
    fn unlinks_only_a_never_committed_file_on_flush_failure() {
        assert!(should_ghost_unlink(true, false));
        assert!(!should_ghost_unlink(false, false), "a successful flush is never unlinked");
    }
}

#[cfg(test)]
mod cr_1_3_flush_release_gating_tests {
    use super::{other_writer_open, OpenFlags};

    // open RO + RW on the same path: releasing the RO fh must NOT drop the
    // write buffer while the RW fh (a writer still mid-stream) is open.
    #[test]
    fn ro_release_must_not_win_when_a_writer_is_still_open() {
        let remaining_after_ro_removed =
            vec![("/f".to_string(), OpenFlags::ReadWrite)];
        assert!(
            other_writer_open(&remaining_after_ro_removed, "/f"),
            "the RW fh is still open, so the RO release must not drop the buffer"
        );
    }

    // Releasing the RW fh (the last writer) with no other writable fh left
    // open must clear the way to drop the write buffer.
    #[test]
    fn rw_release_with_no_other_writer_frees_the_buffer() {
        let remaining_after_rw_removed: Vec<(String, OpenFlags)> = vec![];
        assert!(!other_writer_open(&remaining_after_rw_removed, "/f"));

        // A remaining READ-ONLY fh on the same path must not count as a
        // writer either.
        let remaining_with_only_a_reader =
            vec![("/f".to_string(), OpenFlags::ReadOnly)];
        assert!(!other_writer_open(&remaining_with_only_a_reader, "/f"));
    }

    // A writable fh on a DIFFERENT path must never block this path's
    // buffer from being dropped.
    #[test]
    fn other_path_writer_is_irrelevant() {
        let remaining = vec![("/other".to_string(), OpenFlags::ReadWrite)];
        assert!(!other_writer_open(&remaining, "/f"));
    }
}

#[cfg(test)]
mod bu2_reroute_tests {
    use super::is_resource_exhausted;

    #[test]
    fn classifies_resource_exhausted_vs_other() {
        let exhausted = anyhow::anyhow!(
            "put_chunk RPC failed (code=ResourceExhausted, attempts=3): disk full (ENOSPC)"
        );
        assert!(is_resource_exhausted(&exhausted));

        let internal = anyhow::anyhow!(
            "put_chunk RPC failed (code=Internal, attempts=1): write tmp: broken pipe"
        );
        assert!(!is_resource_exhausted(&internal));

        // Context-wrapped chain still matches.
        let wrapped = exhausted_chain();
        assert!(is_resource_exhausted(&wrapped));
    }

    fn exhausted_chain() -> anyhow::Error {
        let base = anyhow::anyhow!("put_chunk RPC failed (code=ResourceExhausted): full");
        base.context("write_chunk failed on replica data01:9500")
    }
}

#[cfg(test)]
mod rev_2_1_chunk_index_tests {
    use super::{ChunkInfo, FileLocations};

    /// Built through a free function on purpose: an inline struct literal in
    /// each test tripped a rustc 1.94 typeck ICE.
    fn locs(entries: &[(u64, &str)]) -> FileLocations {
        let chunks = entries
            .iter()
            .map(|(idx, id)| ChunkInfo {
                chunk_id: (*id).to_string(),
                size: 4 * 1024 * 1024,
                node_addresses: vec!["http://n1:9500".to_string()],
                node_tiers: vec!["cold".to_string()],
                node_locations: Vec::new(),
                chunk_index: Some(*idx),
            })
            .collect();
        FileLocations {
            path: "/movies/x.mkv".to_string(),
            total_size: 0,
            chunks,
            volume_id: "default".to_string(),
            chunk_size: 4 * 1024 * 1024,
            created_at_ms: 0,
            modified_at_ms: 0,
            is_directory: false,
        }
    }

    /// REV-2-1: a truncate followed by a tail write leaves a single entry at a
    /// high index. Positional lookup handed that entry back for offset 0.
    #[test]
    fn rev_2_1_sparse_file_never_serves_a_neighbouring_chunk() {
        let l = locs(&[(25, "h25")]);
        assert_eq!(l.chunk_at(25).unwrap().chunk_id, "h25");
        assert!(
            l.chunk_at(0).is_none(),
            "offset 0 has no chunk; positional indexing used to hand back h25"
        );
    }

    /// REV-2-1: a gap in the index space must not shift later chunks.
    #[test]
    fn rev_2_1_a_gap_does_not_shift_later_chunks() {
        let l = locs(&[(0, "h0"), (2, "h2"), (3, "h3")]);
        assert_eq!(l.chunk_at(0).unwrap().chunk_id, "h0");
        assert!(l.chunk_at(1).is_none(), "index 1 is a hole");
        assert_eq!(
            l.chunk_at(2).unwrap().chunk_id,
            "h2",
            "positional lookup would have returned h2 for index 1 and h3 for 2"
        );
        assert_eq!(l.chunk_at(3).unwrap().chunk_id, "h3");
    }

    /// The common dense case must behave exactly as before.
    #[test]
    fn rev_2_1_dense_files_are_unaffected() {
        let l = locs(&[(0, "h0"), (1, "h1"), (2, "h2")]);
        for (i, id) in [(0u64, "h0"), (1, "h1"), (2, "h2")] {
            assert_eq!(l.chunk_at(i).unwrap().chunk_id, id);
        }
    }

    /// Back-compat: an older manager sends 0 for EVERY entry, which is
    /// indistinguishable from a real index 0. Those labels must not be
    /// trusted, or a multi-chunk file would resolve every index to entry 0.
    #[test]
    fn rev_2_1_an_older_manager_falls_back_to_positional() {
        let mut l = locs(&[(0, "h0"), (0, "h1"), (0, "h2")]);
        for c in &mut l.chunks {
            c.chunk_index = Some(0);
        }
        assert_eq!(l.chunk_at(1).unwrap().chunk_id, "h1");
        assert_eq!(l.chunk_at(2).unwrap().chunk_id, "h2");
        assert!(l.chunk_at(3).is_none(), "past the end is still absent");
    }
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

    /// CO-1-6: the old `(file_size - offset) as u32` truncated mod 2^32, so a
    /// read at the start of a >= 4 GiB file clamped to 0 (premature EOF). The
    /// helper must clamp the available byte count to u32::MAX before the cast.
    #[test]
    fn co_1_6_effective_read_len_is_u32_safe() {
        const GIB: u64 = 1024 * 1024 * 1024;
        // Exactly 4 GiB available, 128 KiB requested. Naive `(4*GIB) as u32`
        // == 0, so the read returned nothing; the helper must return 131072.
        assert_eq!(
            effective_read_len(4 * GIB, 0, 131072),
            131072,
            "4 GiB available must not wrap to 0"
        );
        // 5 GiB file, read 128 KiB starting 64 bytes before the 4 GiB mark:
        // available = 5 GiB - (4 GiB - 64) = 1 GiB + 64, still > u32::MAX, so
        // the request size wins.
        assert_eq!(
            effective_read_len(5 * GIB, 4 * GIB - 64, 131072),
            131072,
            "available far exceeds the request; request size governs"
        );
        // Tail of a small file: only 10 bytes remain, request 4096 -> 10.
        assert_eq!(
            effective_read_len(100, 90, 4096),
            10,
            "near EOF the available remainder clamps the request"
        );
        // Available exactly u32::MAX is representable and must not wrap.
        assert_eq!(
            effective_read_len(u32::MAX as u64, 0, u32::MAX),
            u32::MAX,
            "u32::MAX bytes available is exact, not a wrap"
        );
    }

    // CO-3-10 helpers: mirror exactly how `read_candidates` builds the
    // per-chunk `known_locations` HashMap from the populated DashMap, then
    // drive `select_read_replica` through it. This is the populate-then-select
    // path the bug killed (the map was never populated).
    fn known_for(map: &DashMap<String, Location>, addrs: &[String]) -> HashMap<String, Location> {
        addrs
            .iter()
            .filter_map(|a| map.get(a).map(|l| (a.clone(), l.clone())))
            .collect()
    }

    /// CO-3-10: with the map populated from the manager's per-replica
    /// locations, a NON-first replica that matches `read_location_priority`
    /// is selected (operator intent beats list order). Before the fix the
    /// map was empty, so every replica looked CrossRegion and the first one
    /// won regardless of priority.
    #[test]
    fn co_3_10_populated_locations_select_priority_replica() {
        let known: DashMap<String, Location> = DashMap::new();
        let addrs = vec!["node-a:9500".to_string(), "node-b:9500".to_string()];
        // Manager-sent locations, positionally aligned: node-a is far
        // (us/east), node-b is in the operator-preferred datacenter.
        let locs = vec![
            Location::new("us", "east", "rack-1", "host-a"),
            Location::new("eu", "fra", "rack-2", "host-b"),
        ];
        learn_locations(&known, &addrs, &locs);
        assert_eq!(known.len(), 2, "both replica locations must be learned");

        // Client far from both; only the priority list distinguishes them.
        let client = Location::new("us", "west", "rack-9", "host-z");
        let replicas: Vec<(String, String)> =
            addrs.iter().map(|a| (a.clone(), String::new())).collect();
        let priority = vec!["fra".to_string()];
        let picked = dtfs_client::location::select_read_replica(
            Some(&client),
            &replicas,
            &known_for(&known, &addrs),
            &priority,
        );
        assert_eq!(
            picked.as_deref(),
            Some("node-b:9500"),
            "the non-first replica matching read_location_priority must win"
        );
    }

    /// CO-3-10: with the map populated, a strictly-nearer NON-first replica
    /// wins on pure distance (no priority configured).
    #[test]
    fn co_3_10_populated_locations_select_nearer_replica() {
        let known: DashMap<String, Location> = DashMap::new();
        let addrs = vec!["far:9500".to_string(), "near:9500".to_string()];
        let locs = vec![
            Location::new("us", "east", "rack-1", "host-a"),
            Location::new("eu", "fra", "rack-2", "host-b"),
        ];
        learn_locations(&known, &addrs, &locs);
        let client = Location::new("eu", "fra", "rack-2", "host-c"); // same rack as "near"
        let replicas: Vec<(String, String)> =
            addrs.iter().map(|a| (a.clone(), String::new())).collect();
        let picked = dtfs_client::location::select_read_replica(
            Some(&client),
            &replicas,
            &known_for(&known, &addrs),
            &[],
        );
        assert_eq!(
            picked.as_deref(),
            Some("near:9500"),
            "the strictly-nearer non-first replica must win on distance"
        );
    }

    /// CO-3-10: an older manager sends no node_locations (or all-empty
    /// placeholders). `learn_locations` must insert nothing, and selection
    /// must fall back to the first replica without panicking.
    #[test]
    fn co_3_10_older_manager_empty_locations_falls_back() {
        let known: DashMap<String, Location> = DashMap::new();
        let addrs = vec!["node-a:9500".to_string(), "node-b:9500".to_string()];

        // Case 1: no locations at all (shorter vec).
        learn_locations(&known, &addrs, &[]);
        assert_eq!(known.len(), 0, "no locations -> nothing learned");

        // Case 2: all-empty placeholders (manager has the addr but no
        // registered Location for the node).
        let empties = vec![Location::new("", "", "", ""), Location::new("", "", "", "")];
        learn_locations(&known, &addrs, &empties);
        assert_eq!(known.len(), 0, "all-empty locations must not be learned");

        let client = Location::new("eu", "fra", "rack-1", "host-z");
        let replicas: Vec<(String, String)> =
            addrs.iter().map(|a| (a.clone(), String::new())).collect();
        let picked = dtfs_client::location::select_read_replica(
            Some(&client),
            &replicas,
            &known_for(&known, &addrs),
            &["fra".to_string()],
        );
        assert_eq!(
            picked.as_deref(),
            Some("node-a:9500"),
            "empty known_locations must fall back to the first replica"
        );
    }
}
