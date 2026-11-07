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

use crate::readahead::{ReadAheadEngine, ReadAheadMode};
use crate::writebuf::{rekey_path, WriteBuffer};

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

/// CR-6-5 (FUSE-5) — default bound on `metadata_cache` entries.
pub const METADATA_CACHE_MAX_ENTRIES: usize = 4096;

/// CR-6-6 (FUSE-8) — bound on in-flight `record_cache_hit` notify tasks.
pub const CACHE_HIT_NOTIFY_MAX_INFLIGHT: usize = 8;

/// CR-6-6 (FUSE-8 / FUSE-10) — true iff `current` is a genuinely new
/// chunk index for this fh (never seen, or different from the last one),
/// i.e. a transition. Pure decision boundary shared by the cache-hit
/// notify dedup (fire at most once per transition) and the
/// `readahead.on_chunk_read` call (fire on every transition, hit or
/// miss) — both care about the exact same event, just gate different
/// actions on it.
fn should_notify(last: Option<u32>, current: u32) -> bool {
    last != Some(current)
}

/// CR2-8-3 (FUSE-11) — decide whether a read looks like a viewer
/// (playback) rather than a scanner (stat + header read) for the disk
/// cache's atime hint.
///
/// The kernel hands FUSE reads in ~128 KB pieces (no `max_read`/
/// `max_pages` configured), well under
/// `local_cache::ATIME_UPDATE_MIN_BYTES` (1 MiB) — passing that per-call
/// length straight to the cache meant every playback read was
/// misclassified as a scanner, so the LRU degraded to put-order and
/// watched chunks got evicted like scanned ones. Instead we look at
/// cumulative consumption on THIS fh (`bytes_on_fh`, updated after each
/// read — so it crosses the threshold after a handful of 128 KB pieces,
/// same as one contiguous >=1 MiB read would) OR whether readahead is
/// actively prefetching for this mount (a scanner's stat+header access
/// never drives sequential prefetch, so an active readahead engine is
/// itself a viewer signal).
pub(crate) fn is_viewer_read(bytes_on_fh: u64, readahead_active: bool) -> bool {
    bytes_on_fh >= crate::local_cache::ATIME_UPDATE_MIN_BYTES as u64 || readahead_active
}

/// A local disk-cache hit goes into the readahead memory cache too, so the
/// ~1000 following 128 KB kernel reads of the same chunk hit memory instead
/// of re-reading and re-hashing the whole chunk from disk each time.
fn promote_disk_hit(
    readahead: &crate::readahead::ReadAheadEngine,
    candidates: &[String],
    chunk_id: &str,
    data: &bytes::Bytes,
) {
    if let Some(cand) = candidates.first() {
        readahead.insert_cached(cand, chunk_id, data.clone());
    }
}

/// CR-6-6 (FUSE-9) — outcome of trying to claim ownership of a
/// foreground chunk fetch. `Owner` must perform the real fetch and
/// resolve it via `complete_foreground_fetch`; `Waiter` should await the
/// receiver instead of fetching.
enum ForegroundFetchClaim {
    Owner(tokio::sync::watch::Sender<Option<bytes::Bytes>>),
    Waiter(tokio::sync::watch::Receiver<Option<bytes::Bytes>>),
}

/// CR-6-6 (FUSE-9) — single-flight claim: the first caller for a given
/// `chunk_id` becomes the `Owner` (and is responsible for calling
/// `complete_foreground_fetch` exactly once when done); every concurrent
/// caller for the SAME `chunk_id` before that becomes a `Waiter` holding
/// a receiver that resolves to the Owner's result (or `None` if the
/// Owner's fetch failed).
fn claim_foreground_fetch(
    inflight: &DashMap<String, tokio::sync::watch::Receiver<Option<bytes::Bytes>>>,
    chunk_id: &str,
) -> ForegroundFetchClaim {
    use dashmap::mapref::entry::Entry;
    match inflight.entry(chunk_id.to_string()) {
        Entry::Occupied(e) => ForegroundFetchClaim::Waiter(e.get().clone()),
        Entry::Vacant(e) => {
            let (tx, rx) = tokio::sync::watch::channel(None);
            e.insert(rx);
            ForegroundFetchClaim::Owner(tx)
        }
    }
}

/// CR-6-6 (FUSE-9) — the Owner calls this exactly once when its fetch is
/// done (success or failure) to publish the result to any Waiters and
/// remove the in-flight entry so the next miss of this chunk_id starts a
/// fresh fetch instead of joining a stale one.
fn complete_foreground_fetch(
    inflight: &DashMap<String, tokio::sync::watch::Receiver<Option<bytes::Bytes>>>,
    chunk_id: &str,
    tx: tokio::sync::watch::Sender<Option<bytes::Bytes>>,
    result: Option<bytes::Bytes>,
) {
    let _ = tx.send(result);
    inflight.remove(chunk_id);
}

/// CR-6-5 (FUSE-5) — evict the oldest-inserted entries from
/// `metadata_cache` until it is at or under `max_entries`. `order` is the
/// insertion-order queue (may contain a path more than once, from
/// repeated re-caches); a popped key that is already absent, or that no
/// longer names the entry it was pushed for, is simply a no-op removal
/// (lazy stale-key skip) — cheap and correct without extra per-entry
/// bookkeeping. Also drains far-stale queue entries once `order` itself
/// grows well past the bound, so a long-lived process re-caching the same
/// hot paths forever doesn't leak the queue itself.
fn evict_metadata_cache(
    cache: &DashMap<String, (FileLocations, Instant)>,
    order: &mut std::collections::VecDeque<String>,
    max_entries: usize,
) {
    while cache.len() > max_entries {
        match order.pop_front() {
            Some(k) => {
                cache.remove(&k);
            }
            None => break,
        }
    }
    let order_cap = max_entries.saturating_mul(4);
    while order.len() > order_cap {
        match order.pop_front() {
            Some(k) => {
                cache.remove(&k);
            }
            None => break,
        }
    }
}

/// CR-6-5 (FUSE-5) — remove every entry whose TTL has elapsed. Pure over
/// the cache handle so it's directly testable without a full
/// `DtfsFilesystem`.
fn sweep_expired_metadata_cache_entries(
    cache: &DashMap<String, (FileLocations, Instant)>,
    ttl: Duration,
) {
    cache.retain(|_, (_, cached_at)| cached_at.elapsed() < ttl);
}

/// CR-6-4 (FUSE-6) — resolve the volume_id to store after a
/// `GetFileLocationsResponse` refresh. A non-empty response value (a
/// manager on this build or newer) always wins. An empty response value
/// (an older manager, field absent) means "keep whatever this client
/// already believes" rather than falling back to the default volume,
/// which would silently apply the wrong RF/tier/anchor to every later
/// write of a non-default-volume file. Only a path with no prior cached
/// value (never seen before) falls back to `"default"`.
fn resolve_refreshed_volume_id(response_volume_id: &str, cached_volume_id: Option<&str>) -> String {
    if !response_volume_id.is_empty() {
        return response_volume_id.to_string();
    }
    cached_volume_id
        .map(|v| v.to_string())
        .unwrap_or_else(|| "default".to_string())
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
    /// P0-1: LOCAL monotonic deadline of the lease (grant/renew instant +
    /// lease duration). The leader's absolute `expires_at_ms` is never
    /// compared with this host's wall clock: a client clock skewed by more
    /// than the lease duration would never (or always) renew.
    lease_deadline: Option<Instant>,
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
    /// restored instead. Starts `true` for a file opened (it already
    /// exists); only a file this process `create()`d starts `false`.
    /// Set on every handle of the path after ANY successful commit,
    /// including write()-driven batch/cap flushes.
    committed_any: bool,
    /// CR-6-6 (FUSE-8/FUSE-10) — the last chunk index `read()` touched on
    /// THIS fh. Gates both: (a) `record_cache_hit` fires only on the
    /// first read of a new chunk index, not once per 128 KB kernel read
    /// (pre-fix: ~32 RPCs per 4 MB chunk); (b) `readahead.on_chunk_read`
    /// fires on every genuine chunk-index transition (hit or miss), not
    /// only on a miss, so the prefetch window advances continuously
    /// instead of stalling every `prefetch_chunks+1` chunks.
    last_read_chunk: Option<u32>,
    /// CR2-8-3 (FUSE-11) — cumulative bytes served by `read()` on THIS fh.
    /// Feeds `is_viewer_read` so the disk-cache atime hint reflects actual
    /// consumption on this handle rather than one kernel call's (~128 KB)
    /// length.
    bytes_read_on_fh: u64,
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
    /// CR-6-5 (FUSE-5) — insertion-order queue backing bounded eviction of
    /// `metadata_cache`. A path can appear more than once (re-cached); a
    /// popped entry whose key is no longer the live one is a harmless
    /// no-op remove (lazy stale-key skip) — see `evict_metadata_cache`.
    metadata_cache_order: std::sync::Mutex<std::collections::VecDeque<String>>,
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
    /// CR-6-6 (FUSE-8) — bounds the number of in-flight `record_cache_hit`
    /// notify RPCs (fire-and-forget metrics pings). Without a bound, a
    /// slow data node could accumulate unbounded outstanding notify
    /// tasks; a permit-less notify is simply skipped (a missed metrics
    /// ping is acceptable, per the existing comment on this call site).
    cache_hit_notify_permits: std::sync::Arc<tokio::sync::Semaphore>,
    /// CR-6-6 (FUSE-9) — single-flights foreground chunk-miss fetches by
    /// chunk_id (content-addressed, so any candidate node's fetch is
    /// interchangeable). The first reader to miss becomes the Owner and
    /// does the real `GetChunk`; concurrent readers of the SAME chunk
    /// (e.g. two kernel 128 KB reads that both land on the same cold
    /// chunk before the first one completes) become Waiters and receive
    /// the Owner's result instead of each issuing their own fetch.
    foreground_inflight: DashMap<String, tokio::sync::watch::Receiver<Option<bytes::Bytes>>>,
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
            // FUSE-2 — same per-fetch bound the foreground read uses.
            config.client.chunk_read_timeout_secs,
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
            metadata_cache_order: std::sync::Mutex::new(std::collections::VecDeque::new()),
            metadata_ttl: DEFAULT_METADATA_TTL,
            open_files: DashMap::new(),
            next_fh: AtomicU64::new(1),
            readahead,
            write_buffer,
            cache_hit_notify_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
                CACHE_HIT_NOTIFY_MAX_INFLIGHT,
            )),
            foreground_inflight: DashMap::new(),
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

    /// Open a file and return a file handle; the caller signals `O_APPEND`. Used by the
    /// fuse3 adapter when the kernel passes the open-flag bitmask, so every
    /// subsequent write lands at the current end-of-file (F-021).
    #[instrument(skip(self), fields(path = %path, ?flags))]
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
        if flags.is_writable() {
            if let Err(msg) =
                check_chunk_size_matches(locations.chunk_size, self.write_buffer.chunk_size())
            {
                tracing::error!(path, error = %msg, "refusing writable open");
                bail!(msg);
            }
        }
        let (lease_id, lease_deadline, lease_owner_id) = if flags.is_writable() {
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
            let sent = Instant::now();
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
                Some(lease_deadline_from(sent)),
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
                match self
                    .read_seed_bytes(path, &locations, last_idx, chunk_size, locations.total_size)
                    .await
                {
                    Ok(Some(existing)) => {
                        self.write_buffer.seed_chunk(path, last_idx as u32, &existing);
                        debug!(
                            path,
                            last_idx,
                            len = existing.len(),
                            "O_APPEND: pre-seeded tail chunk into write buffer"
                        );
                    }
                    Ok(None) => {}
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

        self.open_files.insert(
            fh,
            OpenFile {
                path: path.to_string(),
                flags,
                append,
                locations: Some(locations.clone()),
                chunk_size,
                lease_id,
                lease_deadline,
                lease_owner_id,
                committed_any: committed_any_on_open(false),
                last_read_chunk: None,
                bytes_read_on_fh: 0,
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
        let (locations, chunk_size, file_path) = {
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
            (locations, chunk_size, file_path)
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

        // CR2-8-3 (FUSE-11) — accumulate this fh's cumulative consumption
        // and combine it with whether readahead is actively prefetching to
        // decide the disk-cache viewer/scanner hint for every chunk this
        // call touches. No `.await` between the DashMap lookup and the
        // update (CO-3-12).
        let is_viewer = {
            match self.open_files.get_mut(&fh) {
                Some(mut of) => {
                    of.bytes_read_on_fh =
                        of.bytes_read_on_fh.saturating_add(actual_size as u64);
                    is_viewer_read(of.bytes_read_on_fh, self.readahead.is_active())
                }
                // fh closed concurrently mid-read — be conservative and
                // treat it as a scanner (never touch atime).
                None => false,
            }
        };

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

            // CR-6-6 (FUSE-8/FUSE-10): decide (and record) whether this is
            // a genuinely new chunk index for THIS fh, before touching any
            // cache. Gates the cache-hit notify dedup below and the
            // prefetch-window advance after chunk_data is resolved.
            let is_new_chunk_for_fh = match self.open_files.get_mut(&fh) {
                Some(mut of) => {
                    let is_new = should_notify(of.last_read_chunk, chunk_idx as u32);
                    of.last_read_chunk = Some(chunk_idx as u32);
                    is_new
                }
                // fh closed concurrently mid-read — be conservative.
                None => true,
            };

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
                    // CR2-8-3 (FUSE-11) — `is_viewer` reflects cumulative
                    // consumption on this fh (plus active readahead), not
                    // this single kernel call's ~128 KB length, so the
                    // cache can actually distinguish viewer reads (refresh
                    // atime) from scanner reads (preserve last-viewer
                    // atime for LRU).
                    if let Some(cached) =
                        lc.get_with_hint(&chunk.chunk_id, is_viewer).await
                    {
                        debug!(chunk_id = %chunk.chunk_id, "local-disk cache hit");
                        promote_disk_hit(&self.readahead, &candidates, &chunk.chunk_id, &cached);
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
            //
            // CR-6-6 (FUSE-8): only on a genuine chunk-index transition
            // for this fh — a 4 MB chunk read in 128 KB kernel pieces
            // used to fire ~32 of these RPCs per chunk. Also bounded by
            // a semaphore: a permit-less notify is simply skipped (a
            // missed metrics ping is acceptable, unlike a stalled read).
            if is_new_chunk_for_fh {
                if let Some(notify_addr) = cache_hit_notify_addr.clone() {
                    match self.cache_hit_notify_permits.clone().try_acquire_owned() {
                        Ok(permit) => {
                            let client = self.client.clone();
                            let chunk_id_clone = chunk.chunk_id.clone();
                            tokio::spawn(async move {
                                let _permit = permit;
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
                        Err(_) => {
                            debug!(
                                chunk_id = %chunk.chunk_id,
                                "CR-6-6: cache-hit notify skipped, no permit available"
                            );
                        }
                    }
                }
            }

            let chunk_data = if let Some(d) = chunk_data {
                d
            } else {
                // Cache miss: fetch from data node, falling back through
                // candidates on transport / not-found errors.
                //
                // CR-6-6 (FUSE-9): single-flight this chunk_id so
                // concurrent kernel reads racing into the same cold chunk
                // (content-addressed, so any candidate is interchangeable)
                // fetch it exactly once, not N times over the (possibly
                // 40 Mbps) link.
                match claim_foreground_fetch(&self.foreground_inflight, &chunk.chunk_id) {
                    ForegroundFetchClaim::Waiter(mut rx) => {
                        let joined = if rx.changed().await.is_ok() {
                            rx.borrow().clone()
                        } else {
                            None
                        };
                        match joined {
                            Some(d) => {
                                debug!(
                                    chunk_id = %chunk.chunk_id,
                                    "foreground fetch single-flight join hit"
                                );
                                d
                            }
                            None => {
                                // Owner's fetch failed (or its entry was
                                // dropped before publishing a result) —
                                // fetch it ourselves rather than failing
                                // this read outright.
                                let (_, d) = self
                                    .fetch_chunk_from_candidates(&candidates, &chunk.chunk_id)
                                    .await?;
                                d
                            }
                        }
                    }
                    ForegroundFetchClaim::Owner(tx) => {
                        let fetch_result = self
                            .fetch_chunk_from_candidates(&candidates, &chunk.chunk_id)
                            .await;
                        let (node_addr, data) = match fetch_result {
                            Ok(v) => v,
                            Err(e) => {
                                complete_foreground_fetch(
                                    &self.foreground_inflight,
                                    &chunk.chunk_id,
                                    tx,
                                    None,
                                );
                                return Err(e);
                            }
                        };
                        complete_foreground_fetch(
                            &self.foreground_inflight,
                            &chunk.chunk_id,
                            tx,
                            Some(data.clone()),
                        );
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

                        // FUSE-5 — the former "migration trigger" fired one
                        // full `AllocateChunks` RPC per cross-DC miss and
                        // discarded the reply; the manager rebuilt its
                        // placement ring per call and nothing was ever
                        // migrated. Demand reaches the tiering engine through
                        // the data node's per-DC read counters and
                        // `RecordCacheHit` instead. Removed.

                        data
                    }
                }
            };

            // CR-6-6 (FUSE-10): advance the prefetch window on every
            // genuine chunk-index transition for this fh, whether this
            // chunk was a hit or a miss — previously this only fired on a
            // miss, so a synchronous foreground fetch (~0.8s at 40 Mbps)
            // sat between two runs of already-prefetched chunks instead
            // of a continuously sliding window, stalling playback every
            // `prefetch_chunks+1` chunks. `pending`/cache dedup inside
            // `on_chunk_read` is unchanged.
            if is_new_chunk_for_fh {
                // REV-2-3 — label each entry with its real index and
                // pre-order its replicas with `read_candidates`, so the
                // prefetch hits the same replica the foreground read
                // would pick and caches under the key that read looks up.
                // Only the prefetch window is built: on_chunk_read looks at
                // no chunk beyond chunk_idx + prefetch_chunks.
                let window = self.readahead.prefetch_window();
                let chunk_locs: Vec<(u64, String, Vec<String>)> = locations
                    .chunks
                    .iter()
                    .enumerate()
                    .filter_map(|(pos, c)| {
                        let idx = if indices_trusted {
                            c.chunk_index.unwrap_or(pos as u64)
                        } else {
                            pos as u64
                        };
                        crate::readahead::in_prefetch_window(idx, chunk_idx as u32, window)
                            .then(|| (idx, c.chunk_id.clone(), self.read_candidates(c)))
                    })
                    .collect();
                self.readahead.on_chunk_read(
                    &file_path,
                    chunk_idx as u32,
                    total_chunks,
                    &chunk_locs,
                );
            }

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
        let lease_deadline_for_renew = file.lease_deadline;
        let lease_owner_for_renew = file.lease_owner_id.clone();
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

        // A write buffer split at a different chunk size than the file's own
        // would silently corrupt it (reads use the file's chunk size).
        let file_chunk_size = locations_snapshot.as_ref().map(|l| l.chunk_size).unwrap_or(0);
        if let Err(msg) = check_chunk_size_matches(file_chunk_size, self.write_buffer.chunk_size())
        {
            tracing::error!(path = %path, error = %msg, "refusing write");
            bail!(msg);
        }

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
                // R2 (FUSE-7(c)) — skipping the seed is only safe when the
                // chunk genuinely isn't on the cluster yet; otherwise the
                // still-partial ChunkBuf is caught at commit time by
                // `segments_committable` (EIO, bytes kept for retry).
                let Some(loc) = &locations_snapshot else {
                    warn!(
                        path = %path,
                        chunk_idx,
                        "RMW: no cached locations snapshot; seed skipped, relying on the commit-time partial-segment guard"
                    );
                    continue;
                };
                // Seed bytes are clamped to the file's logical EOF, so stale
                // bytes left in a chunk by a shrinking truncate never come back.
                if let Some(existing) = self
                    .read_seed_bytes(&path, loc, chunk_idx, chunk_size, loc.total_size)
                    .await?
                {
                    self.write_buffer.seed_chunk(&path, chunk_idx as u32, &existing);
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
                // R1 (FUSE-1 memory half) — a torrent client writing its
                // first piece at, say, offset 100 MB used to materialise
                // the WHOLE hole into the write buffer (64 MiB at a time,
                // uncapped in total) before the caller's actual bytes ever
                // landed. Check the cap BEFORE adding this slice and, if
                // it would be met or exceeded, force-drain whatever is
                // already flushable (every all-zero chunk this hole fill
                // has already completed) to the cluster right now instead
                // of accumulating further.
                if dirty_cap_exceeded(self.write_buffer.dirty_bytes(&path), n) {
                    let segs = self.write_buffer.flush_complete(&path);
                    if !segs.is_empty() {
                        if let Err(e) = self.flush_segments_to_cluster(&path, &segs).await {
                            self.write_buffer.restore_segments(&path, &segs);
                            return Err(e);
                        }
                        self.mark_committed_and_refresh(&path).await;
                    }
                }
                let zeros = vec![0u8; n];
                self.write_buffer.write(&path, at, &zeros);
                at += n as u64;
                remaining -= n;
            }
            debug!(path = %path, hole, "zero-filled sparse gap");
        }

        // R1 — one more check before merging the caller's own bytes: a hole
        // fill that landed exactly at the cap boundary, or a write with no
        // hole at all into an already-near-cap buffer, must not be allowed
        // to push it further over.
        if dirty_cap_exceeded(self.write_buffer.dirty_bytes(&path), data.len()) {
            let segs = self.write_buffer.flush_complete(&path);
            if !segs.is_empty() {
                if let Err(e) = self.flush_segments_to_cluster(&path, &segs).await {
                    self.write_buffer.restore_segments(&path, &segs);
                    return Err(e);
                }
                self.mark_committed_and_refresh(&path).await;
            }
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
            self.mark_committed_and_refresh(&path).await;
        }

        // Renew the write lease mid-stream so a long-running streaming
        // write (multi-GB file, rsync over slow uplink) doesn't lose its
        // lease before flush has a chance to renew it. Renews at most once
        // per half-life window — manager dedups same-token renewals.
        if let (Some(lease_id), Some(deadline)) =
            (lease_id_for_renew, lease_deadline_for_renew)
        {
            let sent = Instant::now();
            if lease_renew_due(deadline, sent) {
                match self
                    .client
                    .renew_write_lease(&path, &lease_id, WRITE_LEASE_MS)
                    .await
                {
                    Ok(resp) if resp.granted => {
                        if let Some(mut entry) = self.open_files.get_mut(&fh) {
                            entry.value_mut().lease_deadline = Some(lease_deadline_from(sent));
                        }
                    }
                    Ok(_) => {
                        // FUSE-1 — the lease was swept (handle idle past
                        // WRITE_LEASE_MS, or a manager-side GC race). Try
                        // once to take it back under our own owner id
                        // before declaring it lost; only a lease genuinely
                        // held by ANOTHER writer aborts. Pre-fix this arm
                        // was terminal: every later write/flush on the fh
                        // failed and release() ghost-unlinked a never-
                        // committed file.
                        self.reacquire_write_lease(fh, &path, lease_owner_for_renew.as_deref())
                            .await
                            .with_context(|| {
                                format!("write lease lost for {path} during streaming write")
                            })?;
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
        let lease_deadline = file.lease_deadline;
        let lease_owner_id = file.lease_owner_id.clone();
        drop(file);

        // P0-1: if a lease is held and is past the renewal threshold,
        // renew BEFORE issuing the cluster writes. Otherwise a large flush
        // could exceed the lease lifetime and another writer would acquire
        // the path mid-flush.
        if let (Some(lease_id), Some(deadline)) = (lease_id.as_ref(), lease_deadline) {
            let sent = Instant::now();
            // If less than half the lease remains, refresh proactively.
            if lease_renew_due(deadline, sent) {
                match self
                    .client
                    .renew_write_lease(&path, lease_id, WRITE_LEASE_MS)
                    .await
                {
                    Ok(resp) if resp.granted => {
                        if let Some(mut entry) = self.open_files.get_mut(&fh) {
                            entry.value_mut().lease_deadline = Some(lease_deadline_from(sent));
                        }
                    }
                    Ok(_) => {
                        // FUSE-1 — see write(): re-acquire under our owner
                        // id first; abort only if another writer holds it.
                        self.reacquire_write_lease(fh, &path, lease_owner_id.as_deref())
                            .await
                            .with_context(|| {
                                format!("write lease lost for {path} during flush - aborting")
                            })?;
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
        }

        // Invalidate metadata cache since the file may have changed.
        self.metadata_cache.remove(&path);

        // CR-1-4 + F-029: mark every handle of the path committed and refresh
        // their locations so reads on the same fd see the new chunks.
        if had_segments {
            self.mark_committed_and_refresh(&path).await;
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

            // CR-1-4 (FUSE-3): the ghost-unlink lives here, not in flush(),
            // and fires ONLY for a file this process create()d that has not
            // committed a single chunk (`!committed_any`): opened files start
            // `true`, and every successful commit (flush() or write()-driven
            // batch/cap flush) sets it on all handles of the path. `flush()`
            // always restores drained segments on failure so a retry works.
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
        let sent = Instant::now();
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
                lease_deadline: Some(lease_deadline_from(sent)),
                lease_owner_id: Some(owner_id),
                committed_any: committed_any_on_open(true),
                last_read_chunk: None,
                bytes_read_on_fh: 0,
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
        _fh: Option<u64>,
        new_size: u64,
    ) -> Result<()> {
        // A shrink to a non-chunk-aligned size leaves the old tail bytes in
        // the cluster's tail chunk. Read that chunk (clamped to the new EOF)
        // BEFORE mutating the manager, so a read failure leaves the file
        // untouched, and re-buffer it below so the normal commit path
        // rewrites it zero-trimmed. Only with a writable handle: the buffer
        // is flushed by a writable fh's flush/release, and without one the
        // seeded bytes would be stranded (shortcut: a bare path truncate
        // with no writable handle keeps the stale tail until the next
        // writer's EOF-clamped seed).
        let mut tail_seed: Option<(u64, Bytes)> = None;
        if self.has_writable_handle(path) {
            let before = self
                .fetch_file_locations(path)
                .await
                .with_context(|| format!("truncate: locations for {path}"))?;
            let chunk_size = self.resolve_chunk_size(&before);
            if let Some((idx, _keep)) = tail_rewrite_target(before.total_size, new_size, chunk_size)
            {
                if self.write_buffer.chunk_needs_seed(path, idx as u32) {
                    if let Some(b) = self
                        .read_seed_bytes(path, &before, idx, chunk_size, new_size)
                        .await?
                    {
                        tail_seed = Some((idx, b));
                    }
                }
            }
        }
        self.client
            .set_file_size(path, new_size)
            .await
            .with_context(|| format!("truncate failed for {path}"))?;
        // CO-1-8 (b): clear buffered writes AT OR ABOVE the new EOF but keep
        // buffered bytes below it. Removing the whole buffer (the old
        // behaviour) lost un-flushed writes that the truncation never deleted.
        self.write_buffer.truncate(path, new_size);
        if let Some((idx, bytes)) = tail_seed {
            self.write_buffer.seed_chunk(path, idx as u32, &bytes);
        }
        // Refresh cached locations on every handle of the path (seed reads
        // clamp to `locations.total_size`, so a stale size would resurrect
        // truncated bytes).
        self.metadata_cache.remove(path);
        if let Ok(fresh) = self.fetch_file_locations(path).await {
            for mut entry in self.open_files.iter_mut() {
                if entry.value().path == path {
                    entry.value_mut().locations = Some(fresh.clone());
                }
            }
            self.cache_metadata(path, fresh);
        }
        Ok(())
    }

    /// F-034 — rename a file (atomic) in the manager namespace.
    #[instrument(skip(self), fields(from = %from, to = %to))]
    pub async fn rename(&self, from: &str, to: &str) -> Result<()> {
        self.client.rename_file(from, to).await
            .with_context(|| format!("rename {from} -> {to}"))?;
        // Invalidate the cache for the exact paths and (directory rename)
        // everything below either prefix.
        self.metadata_cache
            .retain(|k, _| rekey_path(k, from, "").is_none() && rekey_path(k, to, "").is_none());
        // Move unflushed bytes with the file (a.part -> a keeps its data).
        self.write_buffer.rename(from, to);
        // Re-key open files at or below the old path.
        for mut entry in self.open_files.iter_mut() {
            if let Some(np) = rekey_path(&entry.value().path, from, to) {
                entry.value_mut().path = np;
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

        // CR-6-4 (FUSE-6) — the manager now returns the file's actual
        // volume_id (proto field 8). An older manager sends "" (field
        // absent); in that case, KEEP whatever volume_id this path
        // already believes rather than silently falling back to
        // "default", which would apply the wrong volume's RF/tier/
        // anchor on every subsequent write. The refresh call sites in
        // `flush()`/`truncate()` invalidate `metadata_cache` BEFORE
        // calling this, so the fallback checks any still-open
        // `OpenFile` for this path first (its `locations` isn't
        // cleared by that invalidation) and only then the metadata
        // cache; a path never seen before falls back to "default".
        let existing_volume_id = self
            .open_files
            .iter()
            .find_map(|e| {
                if e.value().path == path {
                    e.value().locations.as_ref().map(|l| l.volume_id.clone())
                } else {
                    None
                }
            })
            .or_else(|| self.get_cached_metadata(path).map(|loc| loc.volume_id));
        let volume_id =
            resolve_refreshed_volume_id(&response.volume_id, existing_volume_id.as_deref());

        Ok(FileLocations {
            path: response.path,
            total_size: response.total_size,
            chunks,
            volume_id,
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

    /// Store metadata in the cache. CR-6-5 (FUSE-5): bounds the cache at
    /// `METADATA_CACHE_MAX_ENTRIES` by evicting the oldest-inserted paths
    /// first (FIFO, not strict LRU — simplest correct structure for a
    /// getattr-heavy scan workload, per CLAUDE.md § B.1's "smallest fix").
    /// Before this, a Plex library scan pinned one full chunk-location list
    /// per path ever stat'ed, unboundedly.
    fn cache_metadata(&self, path: &str, locations: FileLocations) {
        self.metadata_cache
            .insert(path.to_string(), (locations, Instant::now()));
        let mut order = self.metadata_cache_order.lock().unwrap();
        order.push_back(path.to_string());
        evict_metadata_cache(&self.metadata_cache, &mut order, METADATA_CACHE_MAX_ENTRIES);
    }

    /// CR-6-5 (FUSE-5) — return this client's configured metadata TTL, so
    /// the sweep task in `main.rs` can run on the same cadence entries
    /// naturally expire.
    pub fn metadata_ttl(&self) -> Duration {
        self.metadata_ttl
    }

    /// CR-6-5 (FUSE-5) — remove every `metadata_cache` entry whose TTL has
    /// elapsed. `get_cached_metadata` already does this lazily per-path on
    /// read, but a path that stops being read (e.g. Plex finished scanning
    /// it) would otherwise pin its full chunk-location list forever;
    /// called periodically from `main.rs` on the `metadata_ttl` cadence.
    pub fn sweep_expired_metadata_cache(&self) {
        sweep_expired_metadata_cache_entries(&self.metadata_cache, self.metadata_ttl);
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

    /// CR-6-6 (FUSE-9) — extracted foreground fetch loop: try each
    /// candidate node in order, bounded per-replica by
    /// `client.chunk_read_timeout_secs` (0 = unbounded, FUSE-HOL),
    /// returning the serving node's address + bytes on the first success.
    /// Shared by the single-flight Owner and by a Waiter whose Owner's
    /// fetch failed (fallback: fetch it ourselves rather than fail the
    /// read outright).
    /// FUSE-1 — take the write lease on `path` back under this handle's
    /// own `owner_id` after a renewal reported it gone. The manager grants
    /// a fresh lease when nobody else holds one (or re-issues ours when
    /// the same owner still does), and we store the new token/expiry on
    /// the fh so subsequent renewals validate. `Err` only when another
    /// writer holds the path (`LeaseBusy`) or the manager is unreachable.
    async fn reacquire_write_lease(
        &self,
        fh: u64,
        path: &str,
        owner_id: Option<&str>,
    ) -> Result<()> {
        let owner_id = owner_id
            .ok_or_else(|| anyhow::anyhow!("no lease owner recorded for fh {fh}"))?;
        let sent = Instant::now();
        let resp = self
            .client
            .acquire_write_lease(path, owner_id, WRITE_LEASE_MS)
            .await
            .with_context(|| format!("re-acquire write lease for {path}"))?;
        if !resp.granted {
            return Err(anyhow::Error::new(LeaseBusy {
                path: path.to_string(),
                conflict_owner: resp.conflict_owner,
                expires_at_ms: resp.conflict_expires_at_ms,
            }));
        }
        if let Some(mut entry) = self.open_files.get_mut(&fh) {
            let f = entry.value_mut();
            f.lease_id = Some(resp.lease_id);
            f.lease_deadline = Some(lease_deadline_from(sent));
        }
        tracing::warn!(
            path = %path,
            owner = %owner_id,
            "FUSE-1: write lease had lapsed; re-acquired under the same owner"
        );
        Ok(())
    }

    async fn fetch_chunk_from_candidates(
        &self,
        candidates: &[String],
        chunk_id: &str,
    ) -> Result<(String, bytes::Bytes)> {
        let fetch_timeout_secs = self.config.client.chunk_read_timeout_secs;
        let mut last_err: Option<anyhow::Error> = None;
        for cand in candidates {
            let fetch = self.client.read_chunk(cand, chunk_id);
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
                            chunk_id = %chunk_id,
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
                Ok(d) => return Ok((cand.clone(), d)),
                Err(e) => {
                    // Bug BC-9 — distinguish a verify_on_read integrity
                    // fault from a transport error so operators see
                    // corruption fail-over clearly.
                    if e.downcast_ref::<dtfs_client::client::IntegrityFaultError>()
                        .is_some()
                    {
                        tracing::warn!(
                            chunk_id = %chunk_id,
                            node = %cand,
                            error = %e,
                            "Bug BC-9: integrity fault on replica, failing over to next"
                        );
                    } else {
                        tracing::warn!(
                            chunk_id = %chunk_id,
                            node = %cand,
                            error = %e,
                            "replica read failed, trying next"
                        );
                    }
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| {
            anyhow::anyhow!(
                "no replicas available for chunk {} (candidates exhausted)",
                chunk_id
            )
        }))
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

        // R2 (FUSE-7(c)) — refuse up front to commit a segment that starts
        // mid-chunk (`offset_in_chunk > 0`, or equivalently `needs_seed`):
        // the data node stores whatever bytes it's handed as the chunk's
        // WHOLE content starting at byte 0, so a segment that only covers
        // a sub-range would land shifted by `offset_in_chunk` — this is
        // reachable whenever `write()`'s RMW seed was skipped (no cached
        // `locations_snapshot`, no `chunk_at(idx)` entry, or an empty
        // `node_addresses` list), not just on a seed I/O error. Every
        // caller of this method already `restore_segments`s on `Err`, so
        // refusing here trades silent corruption for EIO with the bytes
        // intact for a retry.
        if let Err(msg) = segments_committable(segments, |i| self.chunk_exists_on_cluster(path, i)) {
            warn!(path, error = %msg, "refusing to commit a partial (needs_seed) segment");
            return Err(anyhow::anyhow!(msg));
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

            // CR-1-5 (DATA-5 FUSE half) — hash the segment once and send the
            // hex digest as the declared `chunk_id` on the wire, so the
            // data node's PutChunk handler (Group 4's server-side half) can
            // reject a bit-flip between this buffer and its own
            // incremental hasher instead of silently committing whatever
            // bytes arrived under a self-consistent id. Computed once per
            // segment — identical across every reroute attempt below.
            let expected_chunk_id = blake3_hex(segment.data.as_ref());

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
                    let expected_chunk_id = expected_chunk_id.clone();
                    async move {
                        let res = client
                            .write_chunk_bytes(&addr, &expected_chunk_id, &volume_id, &tier, data)
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
                        // CR-1-5 — a data node that returns a DIFFERENT
                        // chunk_id than what we declared and hashed
                        // ourselves means the bytes it stored don't match
                        // what we sent (or it silently ignored our
                        // declared id). Never commit a location under
                        // that mismatch: treat it exactly like a failed
                        // replica so this node is excluded from `successes`
                        // and the quorum check below can still succeed on
                        // the other replicas, or fail the whole segment.
                        Ok(put_resp) if put_resp.chunk_id != expected_chunk_id => {
                            warn!(
                                chunk_index = segment.chunk_index,
                                node = %addr,
                                expected = %expected_chunk_id,
                                got = %put_resp.chunk_id,
                                "write_chunk returned a mismatched chunk_id; not committing this replica"
                            );
                            last_err = Some(anyhow::anyhow!(
                                "chunk_id mismatch for chunk_index {} on {}: expected {}, got {}",
                                segment.chunk_index,
                                addr,
                                expected_chunk_id,
                                put_resp.chunk_id
                            ));
                        }
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

    /// True iff some open handle on `path` is writable.
    fn has_writable_handle(&self, path: &str) -> bool {
        self.open_files
            .iter()
            .any(|e| e.value().path == path && e.value().flags.is_writable())
    }

    /// True iff any open handle's locations snapshot has chunk `idx` of `path`.
    fn chunk_exists_on_cluster(&self, path: &str, idx: u32) -> bool {
        self.open_files.iter().any(|e| {
            e.value().path == path
                && e.value()
                    .locations
                    .as_ref()
                    .is_some_and(|l| l.chunk_at(idx as u64).is_some())
        })
    }

    /// After a successful commit of `path`'s segments (from flush() or a
    /// write()-driven batch/cap flush): mark every handle of the path
    /// committed (so release() never ghost-unlinks it) and refresh their
    /// locations (so a later header rewrite seeds from the committed
    /// chunks instead of committing a partial segment as the whole chunk).
    async fn mark_committed_and_refresh(&self, path: &str) {
        for mut entry in self.open_files.iter_mut() {
            if entry.value().path == path {
                entry.value_mut().committed_any = true;
            }
        }
        self.metadata_cache.remove(path);
        match self.fetch_file_locations(path).await {
            Ok(fresh) => {
                for mut entry in self.open_files.iter_mut() {
                    if entry.value().path == path {
                        entry.value_mut().locations = Some(fresh.clone());
                    }
                }
                self.cache_metadata(path, fresh);
            }
            Err(e) => warn!(path, error = %e, "post-commit locations refresh failed"),
        }
    }

    /// Read chunk `chunk_idx` of `locations` for an RMW seed, walking the
    /// same ordered, timeout-bounded candidate list as foreground reads, and
    /// clamp it to `eof` (bytes at or beyond the logical EOF are stale).
    /// `Ok(None)`: nothing to seed from (not on the cluster / no replica).
    async fn read_seed_bytes(
        &self,
        path: &str,
        locations: &FileLocations,
        chunk_idx: u64,
        chunk_size: u64,
        eof: u64,
    ) -> Result<Option<Bytes>> {
        let Some(chunk) = locations.chunk_at(chunk_idx) else {
            warn!(path, chunk_idx, "RMW: no chunk_at() entry for this index; seed skipped, relying on the commit-time partial-segment guard");
            return Ok(None);
        };
        if chunk.node_addresses.is_empty() {
            warn!(path, chunk_idx, "RMW: chunk has no live replica address; seed skipped, relying on the commit-time partial-segment guard");
            return Ok(None);
        }
        let candidates = self.read_candidates(chunk);
        let (_, existing) = self
            .fetch_chunk_from_candidates(&candidates, &chunk.chunk_id)
            .await
            .with_context(|| format!("RMW seed read failed for {path} chunk {chunk_idx}"))?;
        let keep = seed_len_within_eof(existing.len(), chunk_idx * chunk_size, eof);
        Ok(Some(existing.slice(0..keep)))
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

/// R1 (FUSE-1 memory half) — true when a buffer already holding `dirty`
/// bytes would meet or exceed `writebuf::DIRTY_BYTES_CAP` after `incoming`
/// more bytes are added. Extracted as a pure predicate so the cap check in
/// `write()`'s hole-fill loop (and the one guarding the caller's own
/// bytes) is directly testable without a live `WriteBuffer`/network call.
fn dirty_cap_exceeded(dirty: usize, incoming: usize) -> bool {
    dirty.saturating_add(incoming) >= crate::writebuf::DIRTY_BYTES_CAP
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
        .filter(|&idx| write_buffer.chunk_needs_seed(path, idx as u32))
        .collect()
}

/// R2 (FUSE-7(c)) — refuse to commit any segment that starts mid-chunk.
/// The data node stores `PutChunkRequest` bytes as the chunk's WHOLE
/// content starting at offset 0, so a segment whose `offset_in_chunk > 0`
/// would be stored shifted. Likewise a `needs_seed` segment (partial and
/// never seeded) for a chunk that already exists on the cluster
/// (`exists_on_cluster`) would replace it wholesale with the partial bytes.
/// Returns the first offending chunk's error message.
fn segments_committable(
    segments: &[crate::writebuf::ChunkSegment],
    exists_on_cluster: impl Fn(u32) -> bool,
) -> Result<(), String> {
    for seg in segments {
        if seg.offset_in_chunk > 0 {
            return Err(format!(
                "segment for chunk {} starts at offset {} — cannot commit a partial chunk without seeding",
                seg.chunk_index, seg.offset_in_chunk
            ));
        }
        if seg.needs_seed && exists_on_cluster(seg.chunk_index) {
            return Err(format!(
                "segment for chunk {} is partial and unseeded but the chunk exists on the cluster — cannot commit without seeding",
                seg.chunk_index
            ));
        }
    }
    Ok(())
}

/// Bytes of a cluster chunk (`existing_len` long, starting at file offset
/// `chunk_start`) that lie below the file's logical `eof`; the rest is stale.
fn seed_len_within_eof(existing_len: usize, chunk_start: u64, eof: u64) -> usize {
    eof.saturating_sub(chunk_start).min(existing_len as u64) as usize
}

/// Shrinking `old_size` -> `new_size` to a non-chunk-aligned size leaves the
/// old tail bytes in chunk `new_size / chunk_size`; returns that chunk index
/// and how many leading bytes survive, or `None` when no rewrite is needed.
fn tail_rewrite_target(old_size: u64, new_size: u64, chunk_size: u64) -> Option<(u64, u64)> {
    if chunk_size == 0 || new_size >= old_size || new_size % chunk_size == 0 {
        None
    } else {
        Some((new_size / chunk_size, new_size % chunk_size))
    }
}

/// A write buffer sized for a different chunk size than the file's own
/// would corrupt it. `file_chunk_size == 0` means the manager did not say.
fn check_chunk_size_matches(file_chunk_size: u64, buffer_chunk_size: usize) -> Result<(), String> {
    if file_chunk_size == 0 || file_chunk_size == buffer_chunk_size as u64 {
        Ok(())
    } else {
        Err(format!(
            "file chunk size {file_chunk_size} differs from the write buffer's {buffer_chunk_size}; set [client].chunk_size_mb to the volume's chunk size"
        ))
    }
}

/// Initial `OpenFile.committed_any`: a file opened (not created here) already
/// exists, so a failed flush must never ghost-unlink it.
fn committed_any_on_open(created_here: bool) -> bool {
    !created_here
}

/// Local deadline of a lease requested at `sent` for `WRITE_LEASE_MS`.
fn lease_deadline_from(sent: Instant) -> Instant {
    sent + Duration::from_millis(WRITE_LEASE_MS)
}

/// True when less than half the lease remains before the local `deadline`.
fn lease_renew_due(deadline: Instant, now: Instant) -> bool {
    deadline.saturating_duration_since(now)
        < Duration::from_millis(WRITE_LEASE_MS - WRITE_LEASE_RENEW_AFTER_MS)
}

/// CR-1-5 (DATA-5 FUSE half) — hex BLAKE3 digest of `data`, in the same
/// form the data node's own incremental hasher produces as
/// `ChunkMeta.checksum` / the committed `ChunkId`. Extracted so the exact
/// hashing code `flush_segments_to_cluster` sends on the wire as the
/// declared `chunk_id` is directly unit-testable.
fn blake3_hex(data: &[u8]) -> String {
    dtfs_common::types::ChunkId::from_bytes(dtfs_common::checksum::compute(data)).as_hex()
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
mod cr_6_6_should_notify_tests {
    use super::should_notify;

    #[test]
    fn first_read_of_any_chunk_notifies() {
        assert!(should_notify(None, 0));
        assert!(should_notify(None, 7));
    }

    #[test]
    fn sequential_hits_on_the_same_chunk_notify_once() {
        // 32 sequential 128 KB reads of one 4 MB chunk: only the FIRST
        // transition into chunk 0 should notify.
        let mut last: Option<u32> = None;
        let mut notify_count = 0;
        for _ in 0..32 {
            if should_notify(last, 0) {
                notify_count += 1;
            }
            last = Some(0);
        }
        assert_eq!(notify_count, 1);
    }

    #[test]
    fn a_new_chunk_index_notifies_again() {
        assert!(!should_notify(Some(3), 3));
        assert!(should_notify(Some(3), 4));
    }
}

#[cfg(test)]
mod cr2_8_3_is_viewer_read_tests {
    use super::is_viewer_read;
    use crate::local_cache::ATIME_UPDATE_MIN_BYTES;

    #[test]
    fn disk_hit_is_promoted_into_readahead_memory_cache() {
        use crate::readahead::{ReadAheadEngine, ReadAheadMode};
        let engine = ReadAheadEngine::new(
            ReadAheadMode::Disabled,
            dtfs_client::client::DtfsClient::new(),
            60,
            1 << 20,
            2,
        );
        let data = bytes::Bytes::from_static(b"chunk bytes");
        super::promote_disk_hit(&engine, &["n:1".to_string(), "n:2".to_string()], "cid", &data);
        // Looked up via any candidate key; the first candidate holds it.
        assert_eq!(engine.get_cached("n:1", "cid"), Some(data.clone()));
        // Negative: no candidates -> nothing stored, no panic.
        super::promote_disk_hit(&engine, &[], "other", &data);
        assert_eq!(engine.cache_len(), 1);
    }

    #[test]
    fn below_threshold_and_no_readahead_is_scanner() {
        assert!(!is_viewer_read(0, false));
        assert!(!is_viewer_read((ATIME_UPDATE_MIN_BYTES - 1) as u64, false));
    }

    #[test]
    fn cumulative_bytes_crossing_the_threshold_is_viewer() {
        // A handful of 128 KB kernel reads accumulating past 1 MiB on one
        // fh must be classified as a viewer even though no single call
        // ever reached the threshold (the FUSE-11 bug: the old code
        // passed only the per-call length).
        assert!(is_viewer_read(ATIME_UPDATE_MIN_BYTES as u64, false));
        assert!(is_viewer_read(ATIME_UPDATE_MIN_BYTES as u64 + 1, false));
    }

    #[test]
    fn active_readahead_is_viewer_regardless_of_bytes_so_far() {
        assert!(is_viewer_read(0, true));
        assert!(is_viewer_read(128 * 1024, true));
    }
}

#[cfg(test)]
mod cr_6_6_foreground_single_flight_tests {
    use super::{claim_foreground_fetch, complete_foreground_fetch, ForegroundFetchClaim};
    use dashmap::DashMap;

    /// Two concurrent misses on the SAME chunk_id: the first claim must be
    /// the Owner, the second (before the Owner completes) must be a
    /// Waiter — i.e. only one caller is ever responsible for the real
    /// fetch, which is the property that keeps a single `GetChunk` in
    /// flight for the chunk regardless of how many kernel reads race
    /// into it.
    #[test]
    fn second_concurrent_claim_is_a_waiter_not_a_second_owner() {
        let inflight: DashMap<String, tokio::sync::watch::Receiver<Option<bytes::Bytes>>> =
            DashMap::new();

        let first = claim_foreground_fetch(&inflight, "chunk-a");
        assert!(matches!(first, ForegroundFetchClaim::Owner(_)));

        let second = claim_foreground_fetch(&inflight, "chunk-a");
        assert!(matches!(second, ForegroundFetchClaim::Waiter(_)));

        // A different chunk_id is unaffected — its own Owner.
        let other = claim_foreground_fetch(&inflight, "chunk-b");
        assert!(matches!(other, ForegroundFetchClaim::Owner(_)));
    }

    /// The Waiter's receiver resolves to the Owner's published bytes, and
    /// `complete_foreground_fetch` removes the entry so the NEXT miss of
    /// this chunk_id starts a fresh single-flight round rather than
    /// joining a stale, already-resolved one.
    #[tokio::test]
    async fn waiter_receives_owners_result_and_entry_is_cleared() {
        let inflight: DashMap<String, tokio::sync::watch::Receiver<Option<bytes::Bytes>>> =
            DashMap::new();

        let owner_tx = match claim_foreground_fetch(&inflight, "chunk-a") {
            ForegroundFetchClaim::Owner(tx) => tx,
            ForegroundFetchClaim::Waiter(_) => panic!("first claim must be Owner"),
        };
        let mut waiter_rx = match claim_foreground_fetch(&inflight, "chunk-a") {
            ForegroundFetchClaim::Waiter(rx) => rx,
            ForegroundFetchClaim::Owner(_) => panic!("second claim must be Waiter"),
        };

        let payload = bytes::Bytes::from_static(b"chunk bytes");
        complete_foreground_fetch(&inflight, "chunk-a", owner_tx, Some(payload.clone()));

        waiter_rx.changed().await.expect("owner published a result");
        assert_eq!(waiter_rx.borrow().as_ref(), Some(&payload));

        // Entry cleared: the next claim for this chunk_id is a fresh Owner.
        assert!(!inflight.contains_key("chunk-a"));
        let next = claim_foreground_fetch(&inflight, "chunk-a");
        assert!(matches!(next, ForegroundFetchClaim::Owner(_)));
    }

    /// The Owner's fetch failing (published as `None`) must let the
    /// Waiter distinguish "no bytes" from "still fetching" so it falls
    /// back to its own fetch instead of returning zero-length data.
    #[tokio::test]
    async fn waiter_sees_none_when_owners_fetch_failed() {
        let inflight: DashMap<String, tokio::sync::watch::Receiver<Option<bytes::Bytes>>> =
            DashMap::new();

        let owner_tx = match claim_foreground_fetch(&inflight, "chunk-a") {
            ForegroundFetchClaim::Owner(tx) => tx,
            ForegroundFetchClaim::Waiter(_) => panic!("first claim must be Owner"),
        };
        let mut waiter_rx = match claim_foreground_fetch(&inflight, "chunk-a") {
            ForegroundFetchClaim::Waiter(rx) => rx,
            ForegroundFetchClaim::Owner(_) => panic!("second claim must be Waiter"),
        };

        complete_foreground_fetch(&inflight, "chunk-a", owner_tx, None);

        waiter_rx.changed().await.expect("owner published a (failure) result");
        assert_eq!(waiter_rx.borrow().as_ref(), None);
    }
}

#[cfg(test)]
mod cr_6_5_metadata_cache_bound_tests {
    use super::{evict_metadata_cache, sweep_expired_metadata_cache_entries, FileLocations};
    use dashmap::DashMap;
    use std::collections::VecDeque;
    use std::time::{Duration, Instant};

    /// Built through a free function on purpose (same rustc 1.94 struct-
    /// literal ICE workaround as `rev_2_1_chunk_index_tests::locs`).
    fn empty_locations(path: &str) -> FileLocations {
        FileLocations {
            path: path.to_string(),
            total_size: 0,
            chunks: Vec::new(),
            volume_id: "default".to_string(),
            chunk_size: 4 * 1024 * 1024,
            created_at_ms: 0,
            modified_at_ms: 0,
            is_directory: false,
        }
    }

    #[test]
    fn insert_5000_stays_bounded() {
        let cache: DashMap<String, (FileLocations, Instant)> = DashMap::new();
        let mut order: VecDeque<String> = VecDeque::new();
        for i in 0..5000 {
            let path = format!("/movies/{i}.mkv");
            cache.insert(path.clone(), (empty_locations(&path), Instant::now()));
            order.push_back(path);
            evict_metadata_cache(&cache, &mut order, 4096);
        }
        assert!(cache.len() <= 4096, "cache grew past the bound: {}", cache.len());
        // The most recently inserted path must still be present (FIFO
        // evicts the oldest, never the newest).
        assert!(cache.contains_key("/movies/4999.mkv"));
        // An early path must have been evicted.
        assert!(!cache.contains_key("/movies/0.mkv"));
    }

    #[test]
    fn expired_entry_gone_after_sweep() {
        let cache: DashMap<String, (FileLocations, Instant)> = DashMap::new();
        cache.insert(
            "/stale.mkv".to_string(),
            (empty_locations("/stale.mkv"), Instant::now() - Duration::from_secs(120)),
        );
        cache.insert(
            "/fresh.mkv".to_string(),
            (empty_locations("/fresh.mkv"), Instant::now()),
        );
        sweep_expired_metadata_cache_entries(&cache, Duration::from_secs(60));
        assert!(!cache.contains_key("/stale.mkv"), "expired entry must be swept");
        assert!(cache.contains_key("/fresh.mkv"), "fresh entry must survive the sweep");
    }
}

#[cfg(test)]
mod cr_6_4_resolve_refreshed_volume_id_tests {
    use super::resolve_refreshed_volume_id;

    #[test]
    fn nonempty_response_wins() {
        assert_eq!(resolve_refreshed_volume_id("strict", Some("default")), "strict");
        assert_eq!(resolve_refreshed_volume_id("strict", None), "strict");
    }

    #[test]
    fn empty_response_keeps_cached_value() {
        // an older manager sends "" — never silently downgrade to "default"
        // when this client already believes the file is on "strict".
        assert_eq!(resolve_refreshed_volume_id("", Some("strict")), "strict");
    }

    #[test]
    fn empty_response_and_no_cache_falls_back_to_default() {
        assert_eq!(resolve_refreshed_volume_id("", None), "default");
    }
}

#[cfg(test)]
mod r1_dirty_cap_tests {
    use super::dirty_cap_exceeded;
    use crate::writebuf::DIRTY_BYTES_CAP;

    #[test]
    fn under_cap_is_not_exceeded() {
        assert!(!dirty_cap_exceeded(0, 100));
        assert!(!dirty_cap_exceeded(DIRTY_BYTES_CAP - 1024, 100));
    }

    #[test]
    fn meeting_or_crossing_cap_is_exceeded() {
        assert!(dirty_cap_exceeded(DIRTY_BYTES_CAP, 1));
        assert!(dirty_cap_exceeded(DIRTY_BYTES_CAP - 10, 10), "meeting the cap exactly counts");
        assert!(dirty_cap_exceeded(DIRTY_BYTES_CAP, 0), "already at the cap with nothing incoming");
    }

    #[test]
    fn saturates_instead_of_overflowing() {
        assert!(dirty_cap_exceeded(usize::MAX, usize::MAX));
    }
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
mod r2_segments_committable_tests {
    use super::segments_committable;
    use crate::writebuf::ChunkSegment;

    fn seg(chunk_index: u32, offset_in_chunk: usize, needs_seed: bool) -> ChunkSegment {
        ChunkSegment {
            chunk_index,
            data: bytes::Bytes::from_static(b"x"),
            offset_in_chunk,
            needs_seed,
        }
    }

    #[test]
    fn all_aligned_segments_are_committable() {
        let segs = vec![seg(0, 0, false), seg(1, 0, false), seg(2, 0, false)];
        assert!(segments_committable(&segs, |_| false).is_ok());
    }

    #[test]
    fn a_single_mid_chunk_segment_is_refused() {
        let segs = vec![seg(0, 0, false), seg(1, 4096, true), seg(2, 0, false)];
        let err = segments_committable(&segs, |_| false).unwrap_err();
        assert!(err.contains("chunk 1"), "error must name the offending chunk: {err}");
        assert!(err.contains("4096"));
    }

    #[test]
    fn empty_segments_are_committable() {
        assert!(segments_committable(&[], |_| true).is_ok());
    }
}

#[cfg(test)]
mod cr_1_5_chunk_id_tests {
    use super::blake3_hex;

    #[test]
    fn is_deterministic_and_64_hex_chars() {
        let a = blake3_hex(b"hello world");
        let b = blake3_hex(b"hello world");
        assert_eq!(a, b, "same bytes must hash to the same id every time");
        assert_eq!(a.len(), 64, "BLAKE3 hex digest is 64 chars");
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn differs_for_different_content() {
        assert_ne!(blake3_hex(b"hello world"), blake3_hex(b"hello world!"));
    }

    // The core CR-1-5 contract: a data node that returns a chunk_id other
    // than the one this segment actually hashes to must be rejected — the
    // comparison `write_chunk_bytes` result feeds is exactly `blake3_hex(data)
    // != put_resp.chunk_id`.
    #[test]
    fn mismatch_against_declared_id_is_detectable() {
        let data = b"segment bytes";
        let declared = blake3_hex(data);
        let node_returned = blake3_hex(b"different bytes entirely");
        assert_ne!(
            declared, node_returned,
            "a tampered/mismatched response must not equal what we declared and hashed"
        );
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

#[cfg(test)]
mod audit_write_path_tests {
    use super::*;
    use crate::writebuf::ChunkSegment;

    fn seg(chunk_index: u32, needs_seed: bool) -> ChunkSegment {
        ChunkSegment {
            chunk_index,
            data: Bytes::from_static(b"x"),
            offset_in_chunk: 0,
            needs_seed,
        }
    }

    // Ghost-unlink: an opened (pre-existing) file starts committed, so a
    // failed final flush never deletes it.
    #[test]
    fn opened_existing_file_is_never_ghost_unlinked() {
        assert!(!should_ghost_unlink(true, committed_any_on_open(false)));
    }

    // A batch-committed file (mark_committed_and_refresh set committed_any)
    // survives a failed final flush; a fresh create with a failed first
    // flush is the one case that is unlinked.
    #[test]
    fn fresh_create_failed_first_flush_unlinks_but_committed_does_not() {
        assert!(should_ghost_unlink(true, committed_any_on_open(true)));
        let after_batch_commit = true;
        assert!(!should_ghost_unlink(true, after_batch_commit));
    }

    #[test]
    fn unseeded_partial_segment_for_existing_chunk_is_refused() {
        let err = segments_committable(&[seg(0, true)], |i| i == 0).unwrap_err();
        assert!(err.contains("chunk 0") && err.contains("exists on the cluster"), "{err}");
    }

    #[test]
    fn unseeded_partial_segment_for_new_chunk_is_committable() {
        assert!(segments_committable(&[seg(3, true)], |i| i == 0).is_ok());
    }

    #[test]
    fn seeded_or_complete_segment_for_existing_chunk_is_committable() {
        assert!(segments_committable(&[seg(0, false)], |_| true).is_ok());
    }

    #[test]
    fn seed_is_clamped_to_logical_eof() {
        // chunk 1 of 100-byte chunks, cluster holds 100 bytes, EOF at 130.
        assert_eq!(seed_len_within_eof(100, 100, 130), 30);
        assert_eq!(seed_len_within_eof(100, 100, 100), 0, "EOF at chunk start: nothing live");
        assert_eq!(seed_len_within_eof(100, 100, 50), 0, "EOF before chunk: nothing live");
        assert_eq!(seed_len_within_eof(40, 100, 500), 40, "never longer than the chunk");
    }

    #[test]
    fn tail_rewrite_only_for_unaligned_shrink() {
        assert_eq!(tail_rewrite_target(1000, 250, 100), Some((2, 50)));
        assert_eq!(tail_rewrite_target(1000, 300, 100), None, "aligned");
        assert_eq!(tail_rewrite_target(1000, 0, 100), None, "to zero");
        assert_eq!(tail_rewrite_target(100, 250, 100), None, "grow");
        assert_eq!(tail_rewrite_target(250, 250, 100), None, "same size");
        assert_eq!(tail_rewrite_target(1000, 250, 0), None, "unknown chunk size");
    }

    #[test]
    fn chunk_size_mismatch_is_refused() {
        assert!(check_chunk_size_matches(4 << 20, 4 << 20).is_ok());
        assert!(check_chunk_size_matches(0, 4 << 20).is_ok(), "unknown size accepted");
        let err = check_chunk_size_matches(128 << 20, 4 << 20).unwrap_err();
        assert!(err.contains("134217728") && err.contains("4194304"), "{err}");
    }

    // The deadline is a local Instant: renewal depends only on elapsed
    // local time, never on the leader's wall clock.
    #[test]
    fn lease_renews_in_the_last_half_only() {
        let t0 = Instant::now();
        let deadline = lease_deadline_from(t0);
        assert!(!lease_renew_due(deadline, t0));
        assert!(!lease_renew_due(deadline, t0 + Duration::from_millis(WRITE_LEASE_RENEW_AFTER_MS)));
        assert!(lease_renew_due(
            deadline,
            t0 + Duration::from_millis(WRITE_LEASE_RENEW_AFTER_MS + 1)
        ));
        assert!(lease_renew_due(deadline, t0 + Duration::from_millis(WRITE_LEASE_MS * 2)));
    }

    #[test]
    fn a_seeded_partial_chunk_is_not_flagged_again() {
        let wb = WriteBuffer::new(4096);
        wb.seed_chunk("/f", 0, &[1u8; 100]);
        assert!(chunks_needing_seed(&wb, "/f", 4096, 0, 10).is_empty());
    }
}
