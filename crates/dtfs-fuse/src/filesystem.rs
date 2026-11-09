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
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use dashmap::DashMap;
use tracing::{debug, error, info, instrument, warn};

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
    /// Path is a symlink (manager `is_symlink`; false from old managers).
    pub is_symlink: bool,
}

impl FileLocations {
    /// REV-2-1 — true when the manager labelled its chunk entries, so the
    /// index carried by each entry can be trusted.
    ///
    /// A manager that predates `ChunkLocationInfo.chunk_index` sends 0 for
    /// every entry, which is indistinguishable from a real index 0. Treat the
    /// labels as authoritative only when at least one is non-zero, or when
    /// there is at most one chunk (where labelled and positional lookup agree
    /// anyway). O(1): `chunks` is sorted by label (`sort_chunks_by_index`), so
    /// "some label non-zero" is "the last label non-zero".
    fn indices_are_authoritative(&self) -> bool {
        self.chunks.len() <= 1 || self.chunks.last().is_some_and(|c| chunk_label(c) != 0)
    }

    /// Number of chunk slots: highest index + 1 (labelled), else the entry
    /// count (positional). O(1).
    fn chunk_count(&self) -> u64 {
        if self.indices_are_authoritative() {
            self.chunks.last().map_or(0, |c| chunk_label(c) + 1)
        } else {
            self.chunks.len() as u64
        }
    }

    /// Entries with index in `from..=to`, ascending, with their index.
    /// O(log n + entries returned).
    fn chunks_in(&self, from: u64, to: u64) -> impl Iterator<Item = (u64, &ChunkInfo)> {
        let labelled = self.indices_are_authoritative();
        let start = if labelled {
            self.chunks.partition_point(|c| chunk_label(c) < from)
        } else {
            (from as usize).min(self.chunks.len())
        };
        self.chunks[start..]
            .iter()
            .enumerate()
            .map(move |(i, c)| (if labelled { chunk_label(c) } else { (start + i) as u64 }, c))
            .take_while(move |(idx, _)| *idx <= to)
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
    /// neighbour. Binary search over the index-sorted `chunks`: O(log n).
    pub fn chunk_at(&self, index: u64) -> Option<&ChunkInfo> {
        if self.indices_are_authoritative() {
            let pos = self.chunks.partition_point(|c| chunk_label(c) < index);
            self.chunks.get(pos).filter(|c| chunk_label(c) == index)
        } else {
            self.chunks.get(index as usize)
        }
    }
}

/// A chunk entry's index label (`None`, synthesised locally, counts as 0).
fn chunk_label(c: &ChunkInfo) -> u64 {
    c.chunk_index.unwrap_or(0)
}

/// Invariant behind the O(log n) lookups: `FileLocations.chunks` is sorted
/// ascending by label. Stable, so an older manager's all-zero labels keep
/// their positional order; already-sorted input (the manager's BTreeMap
/// order) costs one linear pass. The post-commit merge keeps it sorted.
fn sort_chunks_by_index(chunks: &mut [ChunkInfo]) {
    chunks.sort_by_key(chunk_label);
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

/// Bound on background disk-cache puts after foreground misses; each holds
/// one chunk's `Bytes` (refcounted, usually shared with the readahead cache).
pub const LOCAL_CACHE_PUT_MAX_INFLIGHT: usize = 2;

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
    cache: &DashMap<String, (Arc<FileLocations>, Instant)>,
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
    cache: &DashMap<String, (Arc<FileLocations>, Instant)>,
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
    pub is_symlink: bool,
    pub created_at: SystemTime,
    pub modified_at: SystemTime,
}

/// A directory entry returned by readdir.
#[derive(Debug, Clone)]
pub struct DirEntry {
    pub name: String,
    pub size: u64,
    pub is_directory: bool,
    pub is_symlink: bool,
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

/// Per-path `(commits_in_flight, generation, fetches_in_flight)`. There is no
/// per-path commit serialization, so a post-commit merge is only order-safe
/// when no other commit on the path overlapped this one
/// (`CommitToken::exclusive`), and a full fetch may only be installed when
/// no commit overlapped the fetch (`FetchToken::may_install`). The entry
/// lives while any commit or fetch is in flight, so generations compare.
/// The `Notify` wakes `wait_quiet` callers when a path's commits reach 0.
#[derive(Default)]
struct CommitTracker(DashMap<String, (u32, u64, u32)>, tokio::sync::Notify);

/// Upper bound for waiting out in-flight commits before a locations fetch:
/// a commit cannot legitimately outlive the write lease it runs under.
const QUIET_WAIT: Duration = Duration::from_millis(WRITE_LEASE_MS);

/// What `refetch_locations` does with a fetch result.
#[derive(Debug, PartialEq)]
enum FetchStep {
    /// No commit overlapped the fetch: install it and use it.
    Install,
    /// Overlapped: wait for the path to go quiet, then fetch again.
    WaitQuiet,
    /// Overlapped and out of time: fail (EIO), never use a maybe-stale list.
    TimedOut,
}

fn next_fetch_step(may_install: bool, now: Instant, deadline: Instant) -> FetchStep {
    if may_install {
        FetchStep::Install
    } else if now >= deadline {
        FetchStep::TimedOut
    } else {
        FetchStep::WaitQuiet
    }
}

/// One full locations fetch's tracker slot (dropped = fetch over).
struct FetchToken<'a> {
    tracker: &'a CommitTracker,
    path: String,
    generation: u64,
    quiet_at_start: bool,
}

impl FetchToken<'_> {
    /// True iff no commit on the path was in flight at the fetch's start or
    /// end and none started in between: the result is not older than any
    /// commit applied meanwhile, so it may be installed on the handles.
    fn may_install(&self) -> bool {
        self.quiet_at_start
            && self
                .tracker
                .0
                .get(&self.path)
                .is_some_and(|e| (e.0, e.1) == (0, self.generation))
    }
}

impl Drop for FetchToken<'_> {
    fn drop(&mut self) {
        self.tracker.0.remove_if_mut(&self.path, |_, e| {
            e.2 -= 1;
            e.0 == 0 && e.2 == 0
        });
    }
}

/// One commit's tracker slot; dropping it (success or any error path) ends
/// the commit and removes the path's entry at zero.
struct CommitToken<'a> {
    tracker: &'a CommitTracker,
    path: String,
    generation: u64,
    overlapped: bool,
}

impl CommitTracker {
    fn begin(&self, path: &str) -> CommitToken<'_> {
        let (in_flight, generation, _) = {
            let mut e = self.0.entry(path.to_string()).or_insert((0, 0, 0));
            e.0 += 1;
            e.1 += 1;
            *e
        };
        CommitToken { tracker: self, path: path.to_string(), generation, overlapped: in_flight > 1 }
    }

    fn begin_fetch(&self, path: &str) -> FetchToken<'_> {
        let (in_flight, generation, _) = {
            let mut e = self.0.entry(path.to_string()).or_insert((0, 0, 0));
            e.2 += 1;
            *e
        };
        FetchToken { tracker: self, path: path.to_string(), generation, quiet_at_start: in_flight == 0 }
    }

    /// Wait (woken by the last commit's drop, no polling) until no commit on
    /// `path` is in flight. Err after `timeout`; it maps to EIO.
    async fn wait_quiet(&self, path: &str, timeout: Duration) -> Result<()> {
        let wait = async {
            loop {
                // Register before checking so a drop in between is not missed.
                let notified = self.1.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.0.get(path).map_or(true, |e| e.0 == 0) {
                    return;
                }
                notified.await;
            }
        };
        tokio::time::timeout(timeout, wait).await.map_err(|_| {
            warn!(path, ?timeout, "a commit stayed in flight; locations not refreshed");
            anyhow::anyhow!("{path}: a commit stayed in flight for {timeout:?}; locations not refreshed")
        })
    }
}

/// Snapshot to store on a new handle / in the cache from an unguarded-path
/// fetch: only when no commit overlapped it (`FetchToken::may_install`),
/// else `None` (stale; the handle re-fetches through the guarded path).
fn gated_snapshot(snapshot: Arc<FileLocations>, may_install: bool) -> Option<Arc<FileLocations>> {
    may_install.then_some(snapshot)
}

/// May a writer seed (RMW / O_APPEND tail) from this snapshot? Only one
/// fetched after the write lease was granted (the previous writer, on any
/// host, has finished committing) that no local commit overlapped.
fn seed_snapshot_usable(fetched_after_lease: bool, may_install: bool) -> bool {
    fetched_after_lease && may_install
}

/// read(): a missing chunk entry below the snapshot's size may be a chunk
/// the snapshot missed (not a hole). Re-fetch once before serving zeros,
/// unless this snapshot already came from such a re-fetch (`confirmed`).
fn gap_needs_refetch(missing: bool, idx: u64, total_size: u64, chunk_size: u64, confirmed: bool) -> bool {
    missing && !confirmed && chunk_size > 0 && idx.saturating_mul(chunk_size) < total_size
}

impl CommitToken<'_> {
    /// True iff this was the only commit on the path for its whole window.
    fn exclusive(&self) -> bool {
        !self.overlapped
            && self
                .tracker
                .0
                .get(&self.path)
                .is_some_and(|e| (e.0, e.1) == (1, self.generation))
    }
}

impl Drop for CommitToken<'_> {
    fn drop(&mut self) {
        // Generations only compare overlapping windows, so resetting at zero is safe.
        let mut quiet = false;
        self.tracker.0.remove_if_mut(&self.path, |_, e| {
            e.0 -= 1;
            quiet = e.0 == 0;
            e.0 == 0 && e.2 == 0
        });
        if quiet {
            self.tracker.1.notify_waiters();
        }
    }
}

/// The chunks a commit registered plus its tracker slot (`None` chunks = a
/// lease-less commit, which the manager unions: re-fetch instead).
struct Committed<'a> {
    chunks: Option<Vec<ChunkInfo>>,
    token: Option<CommitToken<'a>>,
}

/// Post-commit: may the committed chunks be merged locally? Only for an
/// exclusive commit whose fh is still open on the same path (a detached
/// handle or a file re-created under the path must re-fetch).
fn merge_allowed(exclusive: bool, fh_path: Option<&str>, path: &str) -> bool {
    exclusive && fh_path == Some(path)
}

/// An open file handle with its associated state.
struct OpenFile {
    path: String,
    flags: OpenFlags,
    /// `O_APPEND` semantics — every write goes to the *current* end of file,
    /// regardless of the offset the kernel hands us. (F-021)
    append: bool,
    /// Cached locations (refreshed on open). `Arc` so read()/write() share
    /// the snapshot instead of cloning the chunk list per kernel call.
    locations: Option<Arc<FileLocations>>,
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
    /// The snapshot read() last re-fetched to check a gap below EOF: its
    /// missing entries are confirmed holes (until the snapshot changes).
    holes_confirmed: Option<Weak<FileLocations>>,
    /// `write.async_commit`: a failed background commit of this path,
    /// reported once by the next fsync/flush/release on this fh (EIO).
    write_error: Option<String>,
    /// `write.async_commit`: a flush (close) of this fh has run. Such an fh
    /// no longer counts as another writer for `defer_close_commit`. Sticky:
    /// a dup'd fd closing early only makes other closes commit more.
    close_seen: bool,
}

// ---------------------------------------------------------------------------
// Default metadata cache TTL
// ---------------------------------------------------------------------------

/// Default metadata cache TTL: 5 seconds.
const DEFAULT_METADATA_TTL: Duration = Duration::from_secs(5);

/// `write.async_commit`: max paths the age timer commits per tick.
const AGED_COMMITS_PER_TICK: usize = 8;

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
    metadata_cache: DashMap<String, (Arc<FileLocations>, Instant)>,
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
    /// Bounds background disk-cache puts after a foreground miss
    /// (`LOCAL_CACHE_PUT_MAX_INFLIGHT`); a put without a permit is dropped.
    cache_put_permits: std::sync::Arc<tokio::sync::Semaphore>,
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
    /// In-flight commits per path; gates the post-commit incremental merge.
    commit_tracker: CommitTracker,
    /// `write.async_commit` only: serialises the background age commit with
    /// write(), a writer's truncate and release on the same path
    /// (`path_guard`); unused (empty) when async_commit is off.
    path_locks: DashMap<String, Arc<tokio::sync::Mutex<()>>>,
    /// `write.async_commit` only: path -> (sorted chunk indices, bytes) of
    /// the ONE batch write() uploads in the background (`spawn_bg_upload`);
    /// present until restore/merge are done.
    bg_uploads: DashMap<String, (Vec<u32>, usize)>,
    /// Woken whenever a background upload ends (`wait_bg_uploads`).
    bg_done: tokio::sync::Notify,
    /// Test hook run by write_inner right after it read the handle: models a
    /// commit retiring during the awaits that follow.
    #[cfg(test)]
    write_race_hook: std::sync::Mutex<Option<Box<dyn FnOnce(&DtfsFilesystem) + Send>>>,
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
            cache_put_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
                LOCAL_CACHE_PUT_MAX_INFLIGHT,
            )),
            foreground_inflight: DashMap::new(),
            known_locations: DashMap::new(),
            commit_tracker: CommitTracker::default(),
            path_locks: DashMap::new(),
            bg_uploads: DashMap::new(),
            bg_done: tokio::sync::Notify::new(),
            #[cfg(test)]
            write_race_hook: std::sync::Mutex::new(None),
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
                is_symlink: false,
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
            if let Some(buffered_eof) = self.write_buffer.visible_size(path) {
                if buffered_eof > attr.size {
                    attr.size = buffered_eof;
                }
            }
            return Ok(attr);
        }

        // Fetch from the manager.
        let fetch = self.commit_tracker.begin_fetch(path);
        let locations = self.fetch_file_locations(path).await?;
        let mut attr = file_attr_from_locations(&locations);
        if let Some(buffered_eof) = self.write_buffer.visible_size(path) {
            if buffered_eof > attr.size {
                attr.size = buffered_eof;
            }
        }
        if let Some(l) = gated_snapshot(Arc::new(locations), fetch.may_install()) {
            self.cache_metadata(path, l);
        }
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
                    is_symlink: e.is_symlink,
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
        let mut fetch = self.commit_tracker.begin_fetch(path);
        // knfsd opens a file per NFS COMMIT: a writable reopen of a path with
        // an open leased writer takes its lease and snapshot, no manager RPC.
        let reused = if flags.is_writable() { self.shared_writer(path) } else { None };
        let mut locations = match &reused {
            Some((_, _, _, l)) => l.clone(),
            None => Arc::new(self.fetch_file_locations(path).await?),
        };
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
        let (lease_id, lease_deadline, lease_owner_id) = if let Some((o, id, dl, _)) = &reused {
            debug!(path, owner = %o, "writable reopen shares the open writer's lease and snapshot");
            (Some(id.clone()), *dl, Some(o.clone()))
        } else if flags.is_writable() {
            // Bug X — append the client's DC as a 4th field. Existing
            // parsers (starts_with("host:"), split(':').next()) ignore
            // trailing fields, so this is backward-compatible. The
            // manager API uses the DC to render the right location for
            // this client in the topology view.
            let owner_id = if let Some(o) = shared_lease_owner(&self.open_lease_owners(), path) {
                o
            } else if self.client_location.datacenter.is_empty() {
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

        // A writer seeds from this snapshot (O_APPEND tail below, write()'s
        // RMW): the pre-lease fetch can predate the previous writer's last
        // commit, so re-fetch after the grant through the guarded path. On
        // failure skip the open-time seed; the handle then stays stale (or
        // the fetch below is still current) and write() re-fetches guarded.
        // A reused snapshot is the lease holder's own, kept current by merges.
        let mut seed_ok = reused.is_none() || fetch.may_install();
        if flags.is_writable() && reused.is_none() && !seed_snapshot_usable(false, fetch.may_install()) {
            match self.quiet_fetch(path).await {
                Ok((l, f)) => {
                    locations = l;
                    fetch = f;
                }
                Err(e) => {
                    warn!(path, error = %e, "post-lease locations fetch failed; open-time seed skipped");
                    seed_ok = false;
                }
            }
        }

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
        // Sole writer only: with another writable handle open (a reused
        // snapshot or not) the tail chunk may be buffered or in a background
        // batch (no path_guard / bg wait here; a stale partial seed would
        // truncate it at commit). write()'s RMW seeds it under the guard.
        let sole_writer = reused.is_none() && !self.has_writable_handle(path);
        if append && flags.is_writable() && seed_ok && sole_writer {
            let max_idx = locations
                .chunk_count()
                .checked_sub(1)
                .filter(|&i| self.write_buffer.chunk_needs_seed(path, i as u32));
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
                holes_confirmed: None,
                write_error: None,
                close_seen: false,
            },
        );

        // Checked after the insert: a commit starting later sees this handle
        // in its merge loop; one that overlapped the fetch changed the
        // generation, so the snapshot may miss its chunk (lazy refetch).
        // A reused snapshot was not fetched: never cached, stale if a commit
        // was in flight at the copy (it may merge into the lender before
        // this insert) or started/ended since.
        if reused.is_some() {
            if !fetch.may_install() {
                self.invalidate_fh_locations(fh);
            }
        } else {
            match gated_snapshot(locations, fetch.may_install()) {
                Some(l) => self.cache_metadata(path, l),
                None => self.invalidate_fh_locations(fh),
            }
        }

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
        let (cached, chunk_size, file_path) = {
            let file = self
                .open_files
                .get(&fh)
                .context("invalid file handle")?;
            (file.locations.clone(), file.chunk_size, file.path.clone())
        }; // guard dropped here, before the await loop below
        // `None` = a post-commit refresh failed and marked the snapshot stale.
        let locations = match cached {
            Some(l) => l,
            None => self.refetch_locations(&file_path).await?,
        };

        // F-029: effective file size = max(persisted size, buffered logical
        // size). Without this, every read of bytes that have only been
        // *buffered* (not yet flushed to a chunk) returns Bytes::new() and
        // SQLite / any DB engine sees an empty database after `CREATE TABLE`.
        // Includes drained segments whose commit is still in flight.
        let buffered_eof = self.write_buffer.visible_size(&file_path).unwrap_or(0);
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
        // Safety net: an entry missing below the snapshot's size may be a
        // chunk the snapshot missed rather than a hole. One guarded re-fetch
        // per snapshot before serving zeros; its own gaps are confirmed holes.
        let confirmed = self.open_files.get(&fh).is_some_and(|f| {
            f.holes_confirmed
                .as_ref()
                .is_some_and(|w| w.ptr_eq(&Arc::downgrade(&locations)))
        });
        let gap = (start_chunk..end_chunk).any(|i| {
            gap_needs_refetch(
                locations.chunk_at(i as u64).is_none(),
                i as u64,
                locations.total_size,
                chunk_size,
                confirmed,
            )
        });
        let locations = if gap {
            debug!(path = %file_path, "chunk entry missing below EOF; re-fetching before serving zeros");
            let fresh = self.refetch_locations(&file_path).await?;
            if let Some(mut f) = self.open_files.get_mut(&fh) {
                f.holes_confirmed = Some(Arc::downgrade(&fresh));
            }
            fresh
        } else {
            locations
        };

        let total_chunks = locations.chunk_count() as u32;

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
                        // healthy replica per `read_candidates`.
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
                        // Off the foreground path, bounded; a dropped or torn
                        // put is just a later miss (entries verify on read).
                        if let Some(lc) = self.local_cache.clone() {
                            let (id, bytes) = (chunk.chunk_id.clone(), data.clone());
                            let spawned = spawn_bounded(&self.cache_put_permits, async move {
                                if let Err(e) = lc.put(&id, &bytes).await {
                                    tracing::warn!(
                                        chunk_id = %id,
                                        error = %e,
                                        "local disk cache put failed"
                                    );
                                }
                            });
                            if !spawned {
                                debug!(chunk_id = %chunk.chunk_id, "local disk cache put skipped, bound full");
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
                // Exactly `in_prefetch_window`: chunk_idx+1 ..= chunk_idx+window.
                let first = chunk_idx as u64 + 1;
                let chunk_locs: Vec<(u64, String, Vec<String>)> = locations
                    .chunks_in(first, chunk_idx as u64 + window as u64)
                    .map(|(idx, c)| (idx, c.chunk_id.clone(), self.read_candidates(c)))
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
    pub async fn write(self: &Arc<Self>, fh: u64, offset: u64, data: &[u8]) -> Result<u32> {
        let (path, guard) = self.fh_path_guard(fh).await;
        // A chunk of the background batch must not be re-seeded from the
        // pre-commit snapshot or later clobbered by its restore: wait for it.
        if let Some(p) = path.as_deref().filter(|p| self.write_hits_bg_upload(p, offset, data.len())) {
            self.wait_bg_uploads(p).await;
        }
        let res = self.write_inner(fh, offset, data).await;
        if let Some(p) = path {
            self.drop_path_guard(&p, guard);
        }
        res
    }

    async fn write_inner(self: &Arc<Self>, fh: u64, offset: u64, data: &[u8]) -> Result<u32> {
        let file = self
            .open_files
            .get(&fh)
            .context("invalid file handle")?;

        if file.flags == OpenFlags::ReadOnly {
            bail!("file opened read-only, cannot write");
        }

        let path = file.path.clone();
        let append = file.append;
        // For O_APPEND we ignore the kernel-supplied offset and target the
        // *current* end of file. Use the cached file size; the buffer may
        // already have unflushed data, in which case the buffer reports a
        // larger logical end than `locations.total_size`. (F-021)
        let chunk_size = file.chunk_size;
        drop(file); // Release the lock before writing.
        #[cfg(test)]
        if let Some(hook) = self.write_race_hook.lock().unwrap().take() {
            hook(self);
        }
        // P0-1 follow-up: renew BEFORE any commit (top of write, before each
        // hole-fill drain and the final drain), not after: a long hole fill
        // or slow commit must not outlive the lease it runs under.
        self.renew_if_due(fh, &path).await?;
        // EOF (append offset, tail seed) and the seed snapshot, read only now
        // (a commit may have retired during the awaits above).
        let (eof_now, snapshot) = self.eof_after(fh, &path, self.write_eof(&path)).await?;
        let locations_snapshot = Some(snapshot);

        // A write buffer split at a different chunk size than the file's own
        // would silently corrupt it (reads use the file's chunk size).
        let file_chunk_size = locations_snapshot.as_ref().map(|l| l.chunk_size).unwrap_or(0);
        if let Err(msg) = check_chunk_size_matches(file_chunk_size, self.write_buffer.chunk_size())
        {
            tracing::error!(path = %path, error = %msg, "refusing write");
            bail!(msg);
        }

        let effective_offset = if append { eof_now } else { offset };

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
        // Chunks this write seeded that held no bytes before: on a failed cap
        // drain they hold nothing the caller was promised, so they are
        // dropped (else retried failing writes grow the buffer unbounded).
        // The hole tail seed is kept: hole zeros above it must keep masking
        // the stale bytes a shrink left in that chunk, and it is seeded once.
        let mut fresh_seeds: Vec<u32> = Vec::new();
        let mut tail_seed = None;
        if chunk_size > 0 && !data.is_empty() {
            let mut seed_list =
                chunks_needing_seed(&self.write_buffer, &path, chunk_size, effective_offset, data.len());
            // A write past EOF hole-fills the existing partial tail chunk
            // with zeros; it must be seeded first or the zero-padded tail
            // replaces its real bytes at commit.
            if let Some(t) = hole_tail_chunk(eof_now, effective_offset, chunk_size) {
                if !seed_list.contains(&t) && self.write_buffer.chunk_needs_seed(&path, t as u32) {
                    seed_list.insert(0, t);
                    tail_seed = Some(t);
                }
            }
            for chunk_idx in seed_list {
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
                // A missing index (zeros) also counts the buffered end: the
                // skipped chunks of a sparse write lie above the cluster EOF.
                let seed_eof = if loc.chunk_at(chunk_idx).is_none() { eof_now } else { loc.total_size };
                if let Some(existing) = self
                    .read_seed_bytes(&path, loc, chunk_idx, chunk_size, seed_eof)
                    .await?
                {
                    if tail_seed != Some(chunk_idx) && !self.write_buffer.has_chunk(&path, chunk_idx as u32) {
                        fresh_seeds.push(chunk_idx as u32);
                    }
                    if loc.chunk_at(chunk_idx).is_none() {
                        // Zeros of a missing index were never written: gap.
                        self.write_buffer.seed_hole(&path, chunk_idx as u32, &existing);
                    } else {
                        self.write_buffer.seed_chunk(&path, chunk_idx as u32, &existing);
                    }
                    debug!(
                        path = %path,
                        chunk_idx,
                        len = existing.len(),
                        "RMW: seeded existing chunk before partial write"
                    );
                }
            }
        }

        // Sparse-file semantics (F-022): a write past the logical end
        // zero-fills only the edge chunks of the hole (`hole_fill_ranges`):
        // the old partial tail and the write's own chunk below the offset.
        // Chunk indices entirely inside the hole stay unbuffered and are
        // never uploaded: a missing index below EOF reads as zeros.
        // `write_eof` also counts a batch uploading in the background.
        // Re-read after the seed awaits (same order as above).
        let (logical_size, _) = self.eof_after(fh, &path, self.write_eof(&path)).await?;
        let fill = crate::writebuf::hole_fill_ranges(logical_size, effective_offset, chunk_size);
        for (i, r) in fill.iter().enumerate() {
            if i == 1 && r.start > fill[0].end {
                // Whole chunks skipped: pin the manager's size before any
                // chunk above the hole commits. A file without one (never
                // truncated) reports the sum of its chunk sizes, which a hole
                // makes too small; once set, commits grow it to their extent.
                // At least every committed, in-flight and buffered end: a
                // SetFileSize drops the chunks above it.
                let (end, _) = self.eof_after(fh, &path, self.write_buffer.visible_size(&path)).await?;
                let pin = logical_size.max(end);
                self.set_size(fh, pin).await?;
                debug!(path = %path, size = pin, "pinned size before a sparse write");
            }
            // Cap the zero-fill at 64 MiB per step to keep memory bounded.
            const FILL_CHUNK: usize = 64 * 1024 * 1024;
            let mut remaining = (r.end - r.start) as usize;
            let mut at = r.start;
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
                self.bg_backpressure(&path, n).await;
                if dirty_cap_exceeded(self.write_buffer.dirty_bytes(&path), n) {
                    self.renew_if_due(fh, &path).await?;
                    let (segs, _inflight) = self.cap_drain(&path, at, n);
                    if !segs.is_empty() {
                        let committed = match self.flush_segments_to_cluster(&path, &segs).await {
                            Ok(c) => c,
                            Err(e) => {
                                self.write_buffer.restore_segments(&path, &segs);
                                self.write_buffer.remove_chunks(&path, &fresh_seeds);
                                return Err(e);
                            }
                        };
                        self.mark_committed_and_refresh(&path, fh, committed).await;
                    }
                }
                self.write_buffer.write_hole(&path, at, n);
                at += n as u64;
                remaining -= n;
            }
        }

        // R1 — one more check before merging the caller's own bytes: a hole
        // fill that landed exactly at the cap boundary, or a write with no
        // hole at all into an already-near-cap buffer, must not be allowed
        // to push it further over.
        self.bg_backpressure(&path, data.len()).await;
        if dirty_cap_exceeded(self.write_buffer.dirty_bytes(&path), data.len()) {
            self.renew_if_due(fh, &path).await?;
            let (segs, _inflight) = self.cap_drain(&path, effective_offset, data.len());
            if !segs.is_empty() {
                let committed = match self.flush_segments_to_cluster(&path, &segs).await {
                    Ok(c) => c,
                    Err(e) => {
                        self.write_buffer.restore_segments(&path, &segs);
                        self.write_buffer.remove_chunks(&path, &fresh_seeds);
                        return Err(e);
                    }
                };
                self.mark_committed_and_refresh(&path, fh, committed).await;
            }
        }

        let written = self.write_buffer.write(&path, effective_offset, data);

        // async_commit: the batch uploads in the background (write() returns
        // now; knfsd's single thread keeps receiving). While one is in
        // flight, complete chunks stay buffered (bounded by the cap above).
        if self.config.write.async_commit {
            if !self.bg_uploads.contains_key(&path) {
                // Overlay registered with the drain: the batch is visible
                // (reads, the next write's EOF) the moment write() returns.
                if let (segments, Some(g)) = self.write_buffer.flush_inflight(&path) {
                    self.spawn_bg_upload(fh, path.clone(), segments, g.into_id());
                }
            }
            return Ok(written as u32);
        }

        // Check if any complete chunks are ready to flush.
        let (segments, _inflight) = self.write_buffer.flush_inflight(&path);
        if !segments.is_empty() {
            if let Err(e) = self.renew_if_due(fh, &path).await {
                self.write_buffer.restore_segments(&path, &segments);
                return Err(e);
            }
            let committed = match self.flush_segments_to_cluster(&path, &segments).await {
                Ok(c) => c,
                Err(e) => {
                    // CO-1-8 (a): `flush` already drained these complete chunks out
                    // of the buffer. Restore them on a cluster-write error so the
                    // bytes survive for the next flush instead of being dropped
                    // while the write call reports failure upstream.
                    self.write_buffer.restore_segments(&path, &segments);
                    return Err(e);
                }
            };
            self.mark_committed_and_refresh(&path, fh, committed).await;
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

    /// Renew the fh's write lease once less than half remains (at most once
    /// per half-life window; the manager dedups same-token renewals). A swept
    /// lease is re-acquired once under the fh's own owner id (FUSE-1); only a
    /// lease held by another writer (or a detached handle) errors. A manager
    /// that is unreachable is non-fatal until the lease actually expires.
    async fn renew_if_due(&self, fh: u64, path: &str) -> Result<()> {
        let (lease_id, deadline, owner) = match self.open_files.get(&fh) {
            Some(f) => (f.lease_id.clone(), f.lease_deadline, f.lease_owner_id.clone()),
            None => return Ok(()),
        };
        let (Some(lease_id), Some(deadline)) = (lease_id, deadline) else {
            return Ok(());
        };
        let sent = Instant::now();
        if !lease_renew_due(deadline, sent) {
            return Ok(());
        }
        match self.client.renew_write_lease(path, &lease_id, WRITE_LEASE_MS).await {
            Ok(resp) if resp.granted => {
                if let Some(mut entry) = self.open_files.get_mut(&fh) {
                    entry.value_mut().lease_deadline = Some(lease_deadline_from(sent));
                }
            }
            Ok(_) => {
                self.reacquire_write_lease(fh, path, owner.as_deref())
                    .await
                    .with_context(|| format!("write lease lost for {path}"))?;
            }
            Err(e) => {
                tracing::debug!(path = %path, error = %e, "lease renew failed (non-fatal until expiry)");
            }
        }
        Ok(())
    }

    /// FUSE flush (every close(2)): always commit everything (`commit_all`),
    /// then report a pending background commit error once. close(2) returns
    /// this result; RELEASE is async and its reply is ignored, so committing
    /// only there would let close() return with the bytes in memory.
    /// With `write.async_commit`, a close while another writer of the path
    /// stays open commits nothing (`defer_close_commit`).
    pub async fn flush(&self, fh: u64) -> Result<()> {
        let (path, guard) = self.fh_path_guard(fh).await;
        let commit = if self.defer_close_commit(fh) {
            Ok(())
        } else {
            self.commit_all(fh).await
        };
        let res = commit.and_then(|()| {
            match self.open_files.get_mut(&fh).and_then(|mut f| f.write_error.take()) {
                Some(msg) => Err(anyhow::anyhow!(msg)),
                None => Ok(()),
            }
        });
        if let Some(p) = path {
            self.drop_path_guard(&p, guard);
        }
        res
    }

    /// FUSE fsync. Default: commit everything (`commit_all`). With
    /// `write.async_commit`: commit nothing extra (partial chunks wait for the
    /// age timer, the dirty cap or close) and report a pending background
    /// commit error once.
    pub async fn fsync(&self, fh: u64) -> Result<()> {
        if !self.config.write.async_commit {
            return self.commit_all(fh).await;
        }
        // No `wait_bg_uploads`: knfsd fsyncs per NFS COMMIT and waiting
        // serialised its one thread on every batch; close/timer still wait.
        let err = self
            .open_files
            .get_mut(&fh)
            .context("invalid file handle")?
            .write_error
            .take();
        match err {
            Some(msg) => Err(anyhow::anyhow!(msg)),
            None => Ok(()),
        }
    }

    /// `DIRTY_BYTES_CAP` drain before buffering `len` bytes at `at`: complete
    /// chunks; with async_commit also partial chunks outside `at..at+len`
    /// (those were just seeded for this write and must stay seeded).
    /// Returns the drained segments with their in-flight overlay.
    fn cap_drain(
        &self,
        path: &str,
        at: u64,
        len: usize,
    ) -> (Vec<crate::writebuf::ChunkSegment>, Option<crate::writebuf::InflightGuard<'_>>) {
        let cs = self.write_buffer.chunk_size().max(1) as u64;
        let keep = (at / cs) as u32..=((at + len.max(1) as u64 - 1) / cs) as u32;
        self.write_buffer.cap_drain_inflight(
            path,
            len,
            crate::writebuf::DIRTY_BYTES_CAP,
            self.config.write.async_commit,
            keep,
        )
    }

    /// `write.async_commit` only: should `fh`'s flush/release skip its
    /// commit because another writable, non-detached, not-closing fh of the
    /// same path is open? knfsd opens and closes a separate file per NFS
    /// COMMIT; committing there re-uploaded every partial chunk. Callers hold
    /// `fh`'s `path_guard` and `fh` gets `close_seen` under it, so of two
    /// racing closes the later one sees no other writer and commits.
    fn defer_close_commit(&self, fh: u64) -> bool {
        if !self.config.write.async_commit {
            return false;
        }
        let Some(mut f) = self.open_files.get_mut(&fh) else { return false };
        if !f.flags.is_writable() || is_detached_path(&f.path) {
            return false;
        }
        f.close_seen = true;
        let path = f.path.clone();
        drop(f);
        let others: Vec<(String, OpenFlags)> = self
            .open_files
            .iter()
            .filter(|e| *e.key() != fh && !e.value().close_seen)
            .map(|e| (e.value().path.clone(), e.value().flags))
            .collect();
        other_writer_open(&others, &path)
    }

    /// `write.async_commit`: hand write()'s batch to a background upload of
    /// `path` (at most one per path; callers hold `path_guard` and checked
    /// `bg_uploads`). One batch, not more: two overlapping commits of a path
    /// make the post-commit merge fall back to a full re-fetch (O(file) per
    /// batch); the batch itself keeps `upload_concurrency` puts busy.
    /// `inflight`: the batch's overlay id (`flush_inflight`), adopted by the task.
    fn spawn_bg_upload(self: &Arc<Self>, fh: u64, path: String, segs: Vec<crate::writebuf::ChunkSegment>, inflight: u64) {
        let mut chunks: Vec<u32> = segs.iter().map(|s| s.chunk_index).collect();
        chunks.sort_unstable();
        let bytes = segs.iter().map(|s| s.data.len()).sum();
        self.bg_uploads.insert(path.clone(), (chunks, bytes));
        let this = Arc::clone(self);
        tokio::spawn(async move { this.bg_upload(fh, path, segs, inflight).await });
    }

    /// The background half of write()'s batch flush. Holds no `path_guard`
    /// (writes would block on it again): nobody else touches these chunk
    /// indices meanwhile (overlapping writes, cap drains, commit_all,
    /// truncate, rename and unlink wait in `wait_bg_uploads`), so the
    /// restore cannot clobber newer bytes. The overlay stays until the
    /// restore/merge is done; `bg_uploads` is cleared after that.
    async fn bg_upload(&self, fh: u64, path: String, segs: Vec<crate::writebuf::ChunkSegment>, inflight: u64) {
        let _slot = BgSlot { fs: self, path: path.clone() };
        let _inflight = self.write_buffer.adopt_inflight(inflight);
        let res = match self.renew_if_due(fh, &path).await {
            Ok(()) => self.flush_segments_to_cluster(&path, &segs).await,
            Err(e) => Err(e),
        };
        match res {
            Ok(committed) => {
                self.mark_committed_and_refresh(&path, fh, committed).await;
                self.record_aged_commit(&path, Ok(()));
            }
            Err(e) => {
                // unlink may have detached the fh meanwhile: restore under
                // its current path, never into a replacement at `path`.
                let cur = self.open_files.get(&fh).map(|f| f.path.clone()).unwrap_or(path);
                self.write_buffer.restore_segments(&cur, &segs);
                self.record_aged_commit(&cur, Err(e));
            }
        }
    }

    /// Wait until `path` has no background upload in flight (no-op when
    /// async_commit is off: nothing is ever spawned).
    async fn wait_bg_uploads(&self, path: &str) {
        loop {
            let done = self.bg_done.notified();
            tokio::pin!(done);
            done.as_mut().enable();
            if !self.bg_uploads.contains_key(path) {
                return;
            }
            done.await;
        }
    }

    /// True iff `path`'s background batch holds a chunk `offset..+len` touches.
    fn write_hits_bg_upload(&self, path: &str, offset: u64, len: usize) -> bool {
        let cs = self.write_buffer.chunk_size() as u64;
        self.bg_uploads
            .get(path)
            .is_some_and(|e| range_hits_upload(&e.0, offset, len, cs))
    }

    /// async_commit backpressure: if buffered plus in-flight bytes of `path`
    /// would meet `DIRTY_BYTES_CAP` with `incoming` more, wait for the
    /// background upload first (the cap drain then sees the restored bytes).
    async fn bg_backpressure(&self, path: &str, incoming: usize) {
        let inflight = self.bg_uploads.get(path).map_or(0, |e| e.1);
        if inflight > 0 && dirty_cap_exceeded(self.write_buffer.dirty_bytes(path) + inflight, incoming) {
            self.wait_bg_uploads(path).await;
        }
    }

    /// Write-side EOF of the buffer: async_commit also counts the batch
    /// uploading in the background (else the next append would zero-fill
    /// over it); off, `logical_size` as before.
    fn write_eof(&self, path: &str) -> Option<u64> {
        if self.config.write.async_commit {
            self.write_buffer.visible_size(path)
        } else {
            self.write_buffer.logical_size(path)
        }
    }

    /// Write-side EOF of `fh`'s file: `buffered` (buffer [+ in-flight],
    /// evaluated by the caller FIRST), then the handle's CURRENT snapshot
    /// (refetched when stale), max. A commit merges into the handles before
    /// it drops its overlay, so this order always counts its bytes (FUSE-1).
    /// Returns that snapshot too: seeds must use it.
    async fn eof_after(&self, fh: u64, path: &str, buffered: Option<u64>) -> Result<(u64, Arc<FileLocations>)> {
        let cur = self.open_files.get(&fh).and_then(|f| f.locations.clone());
        // `None` = stale (a post-commit refresh failed): seeding from it would
        // zero-seed over committed chunks.
        let loc = match cur {
            Some(l) => l,
            None => self.refetch_locations(path).await?,
        };
        Ok((buffered.unwrap_or(0).max(loc.total_size), loc))
    }

    /// `write.async_commit` serialisation of the timer commit with write(),
    /// a writer's truncate and release on `path`; `None` when off.
    async fn path_guard(&self, path: &str) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        if !self.config.write.async_commit {
            return None;
        }
        let lock = self.path_locks.entry(path.to_string()).or_default().clone();
        Some(lock.lock_owned().await)
    }

    /// `path_guard` of `fh`'s current path, plus that path (for
    /// `drop_path_guard`); `(None, None)` for an unknown fh. A rename can
    /// re-key the handle while we wait: re-check and retry on the new path.
    async fn fh_path_guard(
        &self,
        fh: u64,
    ) -> (Option<String>, Option<tokio::sync::OwnedMutexGuard<()>>) {
        loop {
            let Some(p) = self.open_files.get(&fh).map(|f| f.path.clone()) else {
                return (None, None);
            };
            let g = self.path_guard(&p).await;
            let moved = self.open_files.get(&fh).is_some_and(|f| f.path != p);
            if !moved || g.is_none() {
                return (Some(p), g);
            }
            self.drop_path_guard(&p, g);
        }
    }

    /// Unlock and forget `path`'s lock once nobody holds or awaits it, so
    /// `path_locks` only holds paths in use.
    fn drop_path_guard(&self, path: &str, guard: Option<tokio::sync::OwnedMutexGuard<()>>) {
        if guard.is_some() {
            drop(guard);
            self.path_locks.remove_if(path, |_, m| Arc::strong_count(m) == 1);
        }
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
    async fn commit_all(&self, fh: u64) -> Result<()> {
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
        // async_commit: the last writer's close / the timer commit only after
        // write()'s background batch is committed or restored (its error is
        // then pending on the fh). Callers hold `path_guard`: no new one starts.
        self.wait_bg_uploads(&path).await;

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

        // Visible to readers until restored or merged (dropped at return).
        let (segments, _inflight) = self.write_buffer.flush_all_inflight(&path);
        let had_segments = !segments.is_empty();
        let mut committed = None;
        if had_segments {
            match self.flush_segments_to_cluster(&path, &segments).await {
                Ok(c) => committed = Some(c),
                Err(e) => {
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
                // Re-read the path: a concurrent rename may have re-keyed the
                // handle (and its buffer) while the commit was in flight.
                let cur = self
                    .open_files
                    .get(&fh)
                    .map(|f| f.path.clone())
                    .unwrap_or_else(|| path.clone());
                self.write_buffer.restore_segments(&cur, &segments);
                return Err(e);
                }
            }
        }

        // Invalidate metadata cache since the file may have changed.
        self.metadata_cache.remove(&path);

        // CR-1-4 + F-029: mark every handle of the path committed and refresh
        // their locations so reads on the same fd see the new chunks.
        if let Some(committed) = committed {
            self.mark_committed_and_refresh(&path, fh, committed).await;
        }
        // Everything buffered (incl. a failed batch's restored bytes) is
        // committed now: a pending background error is stale.
        if self.config.write.async_commit {
            self.record_aged_commit(&path, Ok(()));
        }

        debug!(path = %path, "flush complete");
        Ok(())
    }

    /// `write.async_commit` timer tick: commit (flush_all, through
    /// `commit_all`) up to `AGED_COMMITS_PER_TICK` paths whose oldest buffered
    /// byte is `async_commit_secs` old at `now`. Skips detached paths and
    /// paths without a writable handle. A failure keeps the bytes (restored,
    /// age kept, retried next tick) and arms each writer's sticky error.
    pub async fn commit_aged(&self, now: Instant) {
        let max_age = Duration::from_secs(self.config.write.async_commit_secs);
        for path in self.write_buffer.aged_paths(now, max_age, AGED_COMMITS_PER_TICK) {
            if is_detached_path(&path) {
                continue;
            }
            let Some(fh) = self.writable_fh(&path) else { continue };
            let guard = self.path_guard(&path).await;
            // Re-check under the lock: release or rename may have won.
            let still_open = self.open_files.get(&fh).map(|f| f.path == path) == Some(true);
            if still_open {
                let result = self.commit_all(fh).await;
                self.record_aged_commit(&path, result);
            }
            self.drop_path_guard(&path, guard);
        }
    }

    /// Timer commit result for `path`: a failure is logged and arms every
    /// writer's sticky error (bytes restored, retried next tick); a success
    /// clears it (the bytes it reported are committed now).
    fn record_aged_commit(&self, path: &str, result: Result<()>) {
        let Err(e) = result else {
            for mut f in self.open_files.iter_mut() {
                if f.path == path {
                    f.write_error = None;
                }
            }
            return;
        };
        let (idx, bytes) = self.write_buffer.pending_summary(path);
        error!(
            path = %path,
            chunk_indices = ?idx,
            bytes,
            error = %format!("{e:#}"),
            "async commit failed; bytes kept for retry, next fsync/close returns EIO"
        );
        let msg = format!("async commit of {path} failed: {e:#}");
        for mut f in self.open_files.iter_mut() {
            if f.path == path && f.flags.is_writable() {
                f.write_error = Some(msg.clone());
            }
        }
    }

    /// Any writable, open handle of `path` (lease info for a commit).
    fn writable_fh(&self, path: &str) -> Option<u64> {
        self.open_files
            .iter()
            .find(|e| e.value().path == path && e.value().flags.is_writable())
            .map(|e| *e.key())
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
        let (guard_path, guard) = self.fh_path_guard(fh).await;
        // A skipped commit (async_commit, another writer open) is Ok: no
        // buffer drop (that writer remains) and no ghost unlink.
        let commit_result = if self.open_files.contains_key(&fh) && !self.defer_close_commit(fh) {
            self.commit_all(fh).await
        } else {
            Ok(())
        };
        // Release commits everything (unless deferred above); a pending async_commit error
        // (never set when off) is reported if the commit succeeds, but only
        // the commit itself decides the buffer drop and the ghost unlink.
        let commit_failed = commit_result.is_err();
        let flush_result = commit_result.and_then(|()| {
            match self.open_files.get_mut(&fh).and_then(|mut f| f.write_error.take()) {
                Some(msg) => Err(anyhow::anyhow!(msg)),
                None => Ok(()),
            }
        });

        if let Some((_, file)) = self.open_files.remove(&fh) {
            // P0-1: release the write lease (if held). Best-effort: a
            // network error here is logged, not propagated, since the lease
            // GC will reap it on expiry anyway.
            let shared = file.lease_owner_id.as_deref().is_some_and(|owner| {
                let remaining: Vec<Option<String>> = self
                    .open_files
                    .iter()
                    .map(|e| e.value().lease_owner_id.clone())
                    .collect();
                lease_owner_still_open(&remaining, owner)
            });
            if let Some(lease_id) = file.lease_id.as_ref().filter(|_| !shared) {
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
                    if commit_failed {
                        let (idx, bytes) = self.write_buffer.pending_summary(&file.path);
                        if bytes > 0 {
                            error!(
                                path = %file.path,
                                chunk_indices = ?idx,
                                bytes_lost = bytes,
                                "release: commit refused, dropping unflushed write buffer (data loss)"
                            );
                        }
                    }
                    self.write_buffer.remove(&file.path);
                }
            }

            // CR-1-4 (FUSE-3): the ghost-unlink lives here, not in flush(),
            // and fires ONLY for a file this process create()d that has not
            // committed a single chunk (`!committed_any`): opened files start
            // `true`, and every successful commit (flush() or write()-driven
            // batch/cap flush) sets it on all handles of the path. `flush()`
            // always restores drained segments on failure so a retry works.
            if should_ghost_unlink(commit_failed, file.committed_any) {
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
        if let Some(p) = guard_path {
            self.drop_path_guard(&p, guard);
        }

        flush_result
    }

    /// Create a new file in the DTFS namespace and return a file handle.
    #[instrument(skip(self), fields(path = %path))]
    pub async fn create(&self, path: &str, _mode: u32) -> Result<u64> {
        let response = self.create_entry(path).await?;

        // Open the newly created file for read-write. P0-1: acquire the
        // single-writer lease so a second create() against the same path
        // (race between two clients) returns EBUSY.
        self.open_created(path, response).await
    }

    /// Namespace half of `create`: the manager entry only, no lease and no
    /// fh. Shared with FUSE_MKNOD (NFS re-export), whose follow-up OPEN takes
    /// the lease like any writable open.
    pub async fn create_entry(
        &self,
        path: &str,
    ) -> Result<dtfs_proto::dtfs::manager::CreateFileResponse> {
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
        Ok(response)
    }

    async fn open_created(
        &self,
        path: &str,
        response: dtfs_proto::dtfs::manager::CreateFileResponse,
    ) -> Result<u64> {
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        // Bug X — owner_id carries DC as a 4th field for the topology view.
        let owner_id = if let Some(o) = shared_lease_owner(&self.open_lease_owners(), path) {
            o
        } else if self.client_location.datacenter.is_empty() {
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

        // The manager keeps a populated file another client just created and
        // filled: seed from its real snapshot so a partial write RMWs it.
        let fetch = self.commit_tracker.begin_fetch(path);
        let fetched = self.fetch_file_locations(path).await.ok().filter(|l| !l.chunks.is_empty());
        let committed_any = fetched
            .as_ref()
            .map(|l| committed_any_from_created_snapshot(l.chunks.len()))
            .unwrap_or_else(|| committed_any_on_open(true));
        let snapshot = fetched.unwrap_or_else(|| FileLocations {
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
            is_symlink: false,
        });
        self.open_files.insert(
            fh,
            OpenFile {
                path: path.to_string(),
                flags: OpenFlags::ReadWrite,
                append: false,
                locations: Some(Arc::new(snapshot)),
                chunk_size: response.chunk_size,
                lease_id: Some(lease_resp.lease_id),
                lease_deadline: Some(lease_deadline_from(sent)),
                lease_owner_id: Some(owner_id),
                committed_any,
                last_read_chunk: None,
                bytes_read_on_fh: 0,
                holes_confirmed: None,
                write_error: None,
                close_seen: false,
            },
        );
        // Checked after the insert: a later commit sees this handle in its
        // merge loop; an earlier one changed the generation.
        if !fetch.may_install() {
            self.invalidate_fh_locations(fh);
        }

        Ok(fh)
    }

    /// Delete a file from the DTFS namespace.
    #[instrument(skip(self), fields(path = %path))]
    pub async fn unlink(&self, path: &str) -> Result<()> {
        // A background batch commits by path: let it land in the old file
        // before the name can be deleted and re-created.
        self.wait_bg_uploads(path).await;
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
        self.detach_handles(path);

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

    /// Logical size of the file behind `fh`: manager size or buffered end,
    /// whichever is larger (fallocate must only ever grow from this).
    pub async fn logical_size(&self, fh: u64) -> Result<u64> {
        let path = self
            .open_files
            .get(&fh)
            .map(|f| f.path.clone())
            .context("invalid fh")?;
        // Fresh manager fetch (never the metadata cache); a failure is an
        // error, not a guess, since a too-small size would shrink the file.
        self.metadata_cache.remove(&path);
        self.size_floor(&path, async { Ok::<_, anyhow::Error>(self.fetch_file_locations(&path).await?.total_size) })
            .await
    }

    /// max(manager size from `manager`, buffered end, handle sizes) of `path`.
    /// Buffer + in-flight are read BEFORE the fetch and the handles: a batch
    /// that retires meanwhile is in the fetch (committed) or the handles
    /// (merged before the overlay drops).
    async fn size_floor(&self, path: &str, manager: impl std::future::Future<Output = Result<u64>>) -> Result<u64> {
        let buffered = self.write_buffer.visible_size(path);
        let manager = manager.await?;
        let handle_sizes: Vec<u64> = self
            .open_files
            .iter()
            .filter(|e| e.value().path == path)
            .filter_map(|e| e.value().locations.as_ref().map(|l| l.total_size))
            .collect();
        Ok(authoritative_size(manager, buffered, &handle_sizes))
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
        {
            // Counts as a commit (as in truncate), so a fetch overlapping the
            // size change is never installed with the old size.
            let _commit = self.commit_tracker.begin(&path);
            self.client
                .set_file_size(&path, new_size)
                .await
                .with_context(|| format!("set_size {new_size} failed for {path}"))?;
        }
        self.metadata_cache.remove(&path);
        // Handle snapshots hold the old size (O_APPEND EOF, hole-fill base);
        // None makes the next write()/read() refetch.
        self.invalidate_handle_locations(&path);
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
        // A bare path truncate (no writable handle) of an unaligned shrink
        // opens a temporary internal writer so the tail chunk is rewritten
        // through the normal lease + commit path, then released.
        let writer = self.has_writable_handle(path);
        let mut temp_fh = None;
        if !writer {
            // Decides the tail rewrite from the size: guarded like the seed.
            let (before, _fetch) = self
                .quiet_fetch(path)
                .await
                .with_context(|| format!("truncate: locations for {path}"))?;
            let cs = self.resolve_chunk_size(&before);
            if truncate_needs_temp_writer(false, before.total_size, new_size, cs) {
                temp_fh = Some(
                    self.open_with_append(path, OpenFlags::ReadWrite, false)
                        .await
                        .with_context(|| format!("truncate: temp writer for {path}"))?,
                );
            }
        }
        let guard = self.path_guard(path).await;
        self.wait_bg_uploads(path).await;
        let res = self.truncate_inner(path, new_size, writer || temp_fh.is_some()).await;
        self.drop_path_guard(path, guard);
        if let Some(fh) = temp_fh {
            let rel = self.release(fh).await;
            res?;
            return rel;
        }
        res
    }

    async fn truncate_inner(&self, path: &str, new_size: u64, writer: bool) -> Result<()> {
        // A shrink to a non-chunk-aligned size leaves the old tail bytes in
        // the cluster's tail chunk. Read that chunk (clamped to the new EOF)
        // BEFORE mutating the manager, so a read failure leaves the file
        // untouched, and re-buffer it below so the normal commit path
        // rewrites it zero-trimmed. Only with a writable handle: the buffer
        // is flushed by a writable fh's flush/release, and without one the
        // seeded bytes would be stranded (`truncate` supplies a temporary
        // writer for the bare-path case).
        let mut tail_seed: Option<(u64, Bytes, bool)> = None;
        if writer {
            // Seeds the tail chunk: guarded fetch (waits out in-flight
            // commits), never a snapshot older than one of them.
            let (before, _fetch) = self
                .quiet_fetch(path)
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
                        tail_seed = Some((idx, b, before.chunk_at(idx).is_none()));
                    }
                }
            }
        }
        {
            // Counts as a commit, so a fetch overlapping the size change is
            // never installed (it would resurrect truncated bytes).
            let _commit = self.commit_tracker.begin(path);
            self.client
                .set_file_size(path, new_size)
                .await
                .with_context(|| format!("truncate failed for {path}"))?;
        }
        // CO-1-8 (b): clear buffered writes AT OR ABOVE the new EOF but keep
        // buffered bytes below it. Removing the whole buffer (the old
        // behaviour) lost un-flushed writes that the truncation never deleted.
        self.write_buffer.truncate(path, new_size);
        match tail_seed {
            Some((idx, bytes, true)) => self.write_buffer.seed_hole(path, idx as u32, &bytes),
            Some((idx, bytes, false)) => self.write_buffer.seed_chunk(path, idx as u32, &bytes),
            None => {}
        }
        // Refresh cached locations on every handle of the path (seed reads
        // clamp to `locations.total_size`, so a stale size would resurrect
        // truncated bytes). Same overlap guard as any re-fetch.
        match self.refetch_locations(path).await {
            Ok(_) => {}
            Err(e) => {
                // A stale larger snapshot would resurrect truncated bytes;
                // None makes write()/read() refetch first.
                error!(path, error = %e, "post-truncate locations refresh failed; marking handle locations stale");
                self.invalidate_handle_locations(path);
            }
        }
        Ok(())
    }

    /// F-034 — rename a file (atomic) in the manager namespace.
    #[instrument(skip(self), fields(from = %from, to = %to))]
    pub async fn rename(&self, from: &str, to: &str) -> Result<()> {
        // async_commit: the timer's commit is not serialised with rename by
        // the kernel. Hold the guard of `from` and of every open writer at or
        // below `from` or `to` (sorted: one global order, no deadlock) so no
        // commit runs across the rename and the buffer/handle re-key.
        let mut guards = Vec::new();
        if self.config.write.async_commit {
            let open: Vec<(String, OpenFlags)> = self
                .open_files
                .iter()
                .map(|e| (e.value().path.clone(), e.value().flags))
                .collect();
            for p in rename_lock_paths(&open, from, to) {
                let g = self.path_guard(&p).await;
                guards.push((p, g));
            }
            // No background upload may commit or restore across the re-key.
            for (p, _) in &guards {
                self.wait_bg_uploads(p).await;
            }
        }
        let res = self.rename_inner(from, to).await;
        for (p, g) in guards {
            self.drop_path_guard(&p, g);
        }
        res
    }

    async fn rename_inner(&self, from: &str, to: &str) -> Result<()> {
        self.client.rename_file(from, to).await
            .with_context(|| format!("rename {from} -> {to}"))?;
        // Invalidate the cache for the exact paths and (directory rename)
        // everything below either prefix.
        self.metadata_cache
            .retain(|k, _| rekey_path(k, from, "").is_none() && rekey_path(k, to, "").is_none());
        // Writers open on the replaced destination must never commit into
        // the replacement: detach them (their unflushed bytes are dropped).
        self.write_buffer.remove(to);
        self.detach_handles(to);
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

    /// Move every handle at or under `target` to a detached path (see
    /// `detached_handle_path`); their commits then fail instead of landing
    /// in whatever now lives at `target`.
    fn detach_handles(&self, target: &str) {
        let tag = self.next_fh.fetch_add(1, Ordering::Relaxed);
        for mut entry in self.open_files.iter_mut() {
            if let Some(np) = detached_handle_path(&entry.value().path, target, tag) {
                warn!(path = %entry.value().path, "open handle detached: its path was replaced or deleted");
                entry.value_mut().path = np;
            }
        }
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
        let fetch = self.commit_tracker.begin_fetch(dst);
        let locs = self.fetch_file_locations(dst).await?;
        let attr = file_attr_from_locations(&locs);
        if let Some(l) = gated_snapshot(Arc::new(locs), fetch.may_install()) {
            self.cache_metadata(dst, l);
        }
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
        let mut chunks = chunks;
        sort_chunks_by_index(&mut chunks);

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
            .or_else(|| self.get_cached_metadata(path).map(|loc| loc.volume_id.clone()));
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
            is_symlink: response.is_symlink,
        })
    }

    /// Get cached metadata if it exists and hasn't expired.
    fn get_cached_metadata(&self, path: &str) -> Option<Arc<FileLocations>> {
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
    fn cache_metadata(&self, path: &str, locations: Arc<FileLocations>) {
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
        if is_detached_path(path) {
            bail!("handle {fh} was detached (its path was replaced or deleted); not re-acquiring a lease");
        }
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
        self.adopt_reacquired_lease(owner_id, &resp.lease_id, lease_deadline_from(sent));
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
    /// the assigned data node. On success returns the committed chunks as
    /// `ChunkInfo`s for `mark_committed_and_refresh` (`None` when the commit
    /// was lease-less) plus the commit's tracker slot, held until the merge.
    async fn flush_segments_to_cluster(
        &self,
        path: &str,
        segments: &[crate::writebuf::ChunkSegment],
    ) -> Result<Committed<'_>> {
        if segments.is_empty() {
            return Ok(Committed { chunks: Some(Vec::new()), token: None });
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

        if alloc_response.placements.len() < segments.len() {
            bail!(
                "manager returned fewer placements than requested ({} < {})",
                alloc_response.placements.len(),
                segments.len()
            );
        }
        let placements = &alloc_response.placements;
        let volume_id = volume_id.as_str();

        // For each segment: write the chunk to the primary AND every replica
        // target the manager assigned. Wait for all writes in parallel; require
        // at least quorum (ceil(N/2)) to succeed before acking.  Each
        // successful target produces a `ChunkLocationCommit` so the manager
        // ends up with N location entries — that's what actually makes
        // replication-factor=N real (fix for F-010). The segments of the
        // batch upload concurrently; any failed segment fails the batch
        // before the single commit below.
        let concurrency = self.config.write.effective_upload_concurrency();
        let uploaded = upload_segments(segments.len(), concurrency, |i| async move {
            let segment = &segments[i];
            let mut placement = placements[i].clone();

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

            let (successes, tier) = loop {
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
                    break (successes, tier);
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

            let info = committed_chunk_info(
                segment.chunk_index,
                &expected_chunk_id,
                successes[0].1.size,
                successes.iter().map(|(a, _)| a.clone()).collect(),
                &tier,
            );
            let mut seg_commits = Vec::with_capacity(successes.len());
            for (addr, put_resp) in successes {
                seg_commits.push(ChunkLocationCommit {
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
            Ok((info, seg_commits))
        })
        .await?;

        let mut commits: Vec<ChunkLocationCommit> = Vec::with_capacity(segments.len());
        let mut committed: Vec<ChunkInfo> = Vec::with_capacity(segments.len());
        for (info, seg_commits) in uploaded {
            committed.push(info);
            commits.extend(seg_commits);
        }

        // Bug P — find the lease_id for this path so the manager can
        // verify it before applying the commit. If no open file handle
        // has a lease (system flow, e.g. read-only path), an empty
        // string preserves the legacy no-verification behaviour.
        let lease_id = self
            .find_lease_id_for_path(path)
            .unwrap_or_default();

        // Dropped on the `?` below, so a failed commit ends its slot too.
        let token = self.commit_tracker.begin(path);
        self.client
            .commit_chunk_locations(path, &volume_id, commits, &lease_id)
            .await
            .with_context(|| format!("commit_chunk_locations failed for {path}"))?;

        // A lease-less commit is union-merged by the manager, so the local
        // replace-merge would not match it: make the caller re-fetch.
        Ok(Committed { chunks: (!lease_id.is_empty()).then_some(committed), token: Some(token) })
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
    /// committed (so release() never ghost-unlinks it) and bring their
    /// locations up to date (so a later header rewrite seeds from the
    /// committed chunks instead of committing a partial segment as the whole
    /// chunk). `committed` is merged into each snapshot in place; the whole
    /// file is re-fetched only when some handle cannot take the merge
    /// (`merge_into_snapshot`), since a per-batch full re-fetch is quadratic
    /// in file size.
    async fn mark_committed_and_refresh(&self, path: &str, fh: u64, committed: Committed<'_>) {
        // `_token` stays alive through any re-fetch below, so a commit that
        // starts meanwhile sees the overlap and re-fetches after this one.
        let Committed { chunks, token: _token } = committed;
        let exclusive = _token.as_ref().is_some_and(|t| t.exclusive());
        let fh_path = self.open_files.get(&fh).map(|f| f.path.clone());
        let committed = chunks
            .as_deref()
            .filter(|_| merge_allowed(exclusive, fh_path.as_deref(), path));
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        // Take the cache's copy out first, held only weakly when a handle
        // shares it, so a sole-writer handle merges in place without a copy.
        let cached = self.metadata_cache.remove(path).map(|(_, (arc, at))| {
            if Arc::strong_count(&arc) > 1 {
                (Err(Arc::downgrade(&arc)), at)
            } else {
                (Ok(arc), at)
            }
        });
        let mut done = Vec::new();
        let mut refetch = false;
        for mut entry in self.open_files.iter_mut() {
            if entry.value().path == path {
                let f = entry.value_mut();
                f.committed_any = true;
                refetch |= !merge_shared(&mut f.locations, committed, now_ms, &mut done);
            }
        }
        if !refetch {
            let restored = match cached {
                Some((Ok(arc), at)) => {
                    let mut slot = Some(arc);
                    merge_shared(&mut slot, committed, now_ms, &mut done).then(|| (slot, at))
                }
                Some((Err(weak), at)) => {
                    merged_of(&done, &weak).map(|arc| (Some(arc), at))
                }
                None => None,
            };
            if let Some((Some(arc), at)) = restored {
                self.metadata_cache.insert(path.to_string(), (arc, at));
            }
            return;
        }
        // No eager re-fetch: with overlapping commits a fetch result can land
        // after a newer one and hide a committed chunk. Mark the handles
        // stale (cache entry already dropped); read()/write() re-fetch
        // lazily through `refetch_locations`, which installs only a result
        // no commit overlapped.
        debug!(path, "post-commit merge not applicable; marking handle locations stale");
        self.invalidate_handle_locations(path);
    }

    /// Mark one handle stale (`locations = None`).
    fn invalidate_fh_locations(&self, fh: u64) {
        if let Some(mut f) = self.open_files.get_mut(&fh) {
            f.locations = None;
        }
    }

    /// Mark every handle of `path` stale (`locations = None`); the next
    /// write()/read() refetches before using a snapshot.
    fn invalidate_handle_locations(&self, path: &str) {
        for mut entry in self.open_files.iter_mut() {
            if entry.value().path == path {
                entry.value_mut().locations = None;
            }
        }
    }

    /// Re-fetch `path`'s locations and install them on every handle of it.
    /// Only a fetch no commit on the path overlapped is used (an older result
    /// could hide a committed chunk: a write would seed zeros over it, a read
    /// would return a hole); otherwise wait for the path to go quiet and
    /// fetch again, failing (EIO) after `QUIET_WAIT`.
    async fn refetch_locations(&self, path: &str) -> Result<Arc<FileLocations>> {
        self.metadata_cache.remove(path);
        let (fresh, _fetch) = self.quiet_fetch(path).await?;
        // No await since `may_install` held: install right away.
        for mut entry in self.open_files.iter_mut() {
            if entry.value().path == path {
                entry.value_mut().locations = Some(fresh.clone());
            }
        }
        Ok(fresh)
    }

    /// Fetch `path`'s locations until a fetch no commit on the path overlapped
    /// succeeds (waiting for quiet in between), failing (EIO) after
    /// `QUIET_WAIT`. Returns the snapshot and its still-open fetch slot, so a
    /// caller that stores it later can re-check `may_install` at that point.
    async fn quiet_fetch(&self, path: &str) -> Result<(Arc<FileLocations>, FetchToken<'_>)> {
        let deadline = Instant::now() + QUIET_WAIT;
        loop {
            let fetch = self.commit_tracker.begin_fetch(path);
            let fresh = Arc::new(
                self.fetch_file_locations(path)
                    .await
                    .with_context(|| format!("stale locations for {path} and re-fetch failed"))?,
            );
            match next_fetch_step(fetch.may_install(), Instant::now(), deadline) {
                FetchStep::Install => return Ok((fresh, fetch)),
                FetchStep::WaitQuiet => {
                    drop(fetch);
                    debug!(path, "locations fetch overlapped a commit; waiting for it");
                    self.commit_tracker
                        .wait_quiet(path, deadline.saturating_duration_since(Instant::now()))
                        .await?;
                }
                FetchStep::TimedOut => {
                    warn!(path, "locations fetch kept overlapping commits; failing the call");
                    bail!("{path}: locations kept changing under in-flight commits");
                }
            }
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
            // A hole below EOF reads as zeros: seed zeros so a partial write
            // into it commits as one zero-padded chunk (guard would refuse).
            let n = seed_len_within_eof(chunk_size as usize, chunk_idx * chunk_size, eof);
            debug!(path, chunk_idx, n, "RMW: no cluster chunk at index; zero-seeding hole");
            return Ok(Some(Bytes::from(vec![0u8; n])));
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
#[cfg(test)]
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
        is_symlink: locations.is_symlink,
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

/// True iff the write `offset..offset+len` touches a chunk index of the
/// background batch (`batch` sorted).
fn range_hits_upload(batch: &[u32], offset: u64, len: usize, chunk_size: u64) -> bool {
    if len == 0 || chunk_size == 0 {
        return false;
    }
    let first = offset / chunk_size;
    let last = (offset + len as u64 - 1) / chunk_size;
    let i = batch.partition_point(|&c| (c as u64) < first);
    batch.get(i).is_some_and(|&c| c as u64 <= last)
}

/// Clears a path's `bg_uploads` entry and wakes waiters when the background
/// upload ends, also if its task panics or is dropped at runtime shutdown.
struct BgSlot<'a> {
    fs: &'a DtfsFilesystem,
    path: String,
}

impl Drop for BgSlot<'_> {
    fn drop(&mut self) {
        self.fs.bg_uploads.remove(&self.path);
        self.fs.bg_done.notify_waiters();
    }
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

/// The `ChunkInfo` a successful commit registered for one segment: the
/// replicas that acked the put, all at the placement's tier. Locations stay
/// empty (as from an old manager); `known_locations` already ranks them.
fn committed_chunk_info(
    chunk_index: u32,
    chunk_id: &str,
    size: u64,
    node_addresses: Vec<String>,
    tier: &str,
) -> ChunkInfo {
    let node_tiers = vec![tier.to_string(); node_addresses.len()];
    ChunkInfo {
        chunk_id: chunk_id.to_string(),
        size,
        node_addresses,
        node_tiers,
        node_locations: Vec::new(),
        chunk_index: Some(chunk_index as u64),
    }
}

/// Post-commit: merge `committed` into a handle's `snapshot` the way the
/// manager's leased merge applies it (each committed index's entry replaced,
/// size grown to the committed extent). Returns false (snapshot untouched)
/// when the handle needs the whole-file re-fetch instead: no snapshot, a
/// lease-less commit (`None`), positional old-manager indices, or an unknown
/// chunk size.
fn merge_into_snapshot(
    snapshot: Option<&mut FileLocations>,
    committed: Option<&[ChunkInfo]>,
    now_ms: i64,
) -> bool {
    let (Some(loc), Some(committed)) = (snapshot, committed) else {
        return false;
    };
    if !can_merge(loc, Some(committed)) {
        return false;
    }
    for c in committed {
        let idx = c.chunk_index.unwrap_or(0);
        // Manager snapshots are sorted by index; merges keep them sorted.
        let pos = loc.chunks.partition_point(|e| e.chunk_index.unwrap_or(0) < idx);
        if loc.chunks.get(pos).is_some_and(|e| e.chunk_index.unwrap_or(0) == idx) {
            loc.chunks[pos] = c.clone();
        } else {
            loc.chunks.insert(pos, c.clone());
        }
        loc.total_size = loc.total_size.max(idx * loc.chunk_size + c.size);
    }
    loc.modified_at_ms = loc.modified_at_ms.max(now_ms);
    true
}

/// Whether `merge_into_snapshot` would apply `committed` to `loc`.
fn can_merge(loc: &FileLocations, committed: Option<&[ChunkInfo]>) -> bool {
    committed.is_some_and(|c| c.iter().all(|c| c.chunk_index.is_some()))
        && loc.chunk_size != 0
        && loc.indices_are_authoritative()
}

/// Copy-on-write `merge_into_snapshot` for one shared holder. A holder whose
/// snapshot was already merged this round (`done`) takes that result, so
/// holders that shared one `Arc` still share one; a sole owner is merged in
/// place without copying the chunk list. Other holders (an in-flight read)
/// keep the old snapshot. False (slot untouched) = needs the full re-fetch.
fn merge_shared(
    slot: &mut Option<Arc<FileLocations>>,
    committed: Option<&[ChunkInfo]>,
    now_ms: i64,
    done: &mut Vec<(Weak<FileLocations>, Arc<FileLocations>)>,
) -> bool {
    let Some(cur) = slot.as_mut() else {
        return false;
    };
    if let Some(new) = merged_of(done, &Arc::downgrade(cur)) {
        *cur = new;
        return true;
    }
    // Check before make_mut so a refused merge never copies.
    if !can_merge(cur, committed) {
        return false;
    }
    // The Weak keeps the old allocation's address from being reused while
    // `done` still compares against it.
    let old = Arc::downgrade(cur);
    merge_into_snapshot(Some(Arc::make_mut(cur)), committed, now_ms);
    done.push((old, cur.clone()));
    true
}

/// The merged snapshot recorded in `done` for the pre-merge `old`.
fn merged_of(
    done: &[(Weak<FileLocations>, Arc<FileLocations>)],
    old: &Weak<FileLocations>,
) -> Option<Arc<FileLocations>> {
    done.iter().find(|(o, _)| o.ptr_eq(old)).map(|(_, new)| new.clone())
}

/// Largest size any source knows; fallocate grows from this so a stale
/// manager/cache view can never make it shrink the file.
fn authoritative_size(manager: u64, buffered_end: Option<u64>, handle_sizes: &[u64]) -> u64 {
    handle_sizes
        .iter()
        .copied()
        .chain(buffered_end)
        .fold(manager, u64::max)
}

/// Chunk index holding the existing partial tail when a write at
/// `write_offset` lands past `eof` (the hole fill rewrites that chunk with
/// zeros after `eof`, so it needs a seed first); `None` otherwise.
fn hole_tail_chunk(eof: u64, write_offset: u64, chunk_size: u64) -> Option<u64> {
    if chunk_size == 0 || write_offset <= eof || eof % chunk_size == 0 {
        None
    } else {
        Some(eof / chunk_size)
    }
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

/// A path truncate with no writable handle needs a temporary internal writer
/// exactly when it is an unaligned shrink (stale tail bytes must be rewritten).
fn truncate_needs_temp_writer(has_writer: bool, old_size: u64, new_size: u64, chunk_size: u64) -> bool {
    !has_writer && tail_rewrite_target(old_size, new_size, chunk_size).is_some()
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

/// create() landed on a file another client already filled (the manager kept
/// the populated entry): committed iff the fetched snapshot has chunks.
fn committed_any_from_created_snapshot(chunk_count: usize) -> bool {
    chunk_count > 0
}

/// Key a handle is moved to when a rename/unlink replaces its path. The NUL
/// byte can never be a manager path, so later commits fail (ENOENT/EIO)
/// instead of writing into the replacement. `tag` makes each detach unique.
fn detached_handle_path(path: &str, target: &str, tag: u64) -> Option<String> {
    rekey_path(path, target, &format!("{target}\0detached-{tag}"))
}

fn is_detached_path(path: &str) -> bool {
    path.contains('\0')
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

/// knfsd reopens a file this process already holds open for write: the new
/// writable fh joins the existing lease (same owner = manager re-acquire)
/// instead of conflicting with ourselves (EBUSY). `others` = `(path,
/// lease_owner_id)` of the other open handles; detached paths never match.
fn shared_lease_owner(others: &[(String, Option<String>)], path: &str) -> Option<String> {
    others
        .iter()
        .find(|(p, o)| p == path && o.is_some())
        .and_then(|(_, o)| o.clone())
}

/// `release()` keeps the lease while another remaining handle shares its
/// owner (`shared_lease_owner`); the last one releases it.
fn lease_owner_still_open(remaining: &[Option<String>], owner: &str) -> bool {
    remaining.iter().any(|o| o.as_deref() == Some(owner))
}

impl DtfsFilesystem {
    /// Every handle sharing `owner_id` (knfsd reopen) takes the re-acquired
    /// lease id, so whichever closes last releases the live lease, not a
    /// stale one that lingers until expiry.
    fn adopt_reacquired_lease(&self, owner_id: &str, lease_id: &str, deadline: Instant) {
        for mut entry in self.open_files.iter_mut() {
            let f = entry.value_mut();
            if f.lease_owner_id.as_deref() == Some(owner_id) {
                f.lease_id = Some(lease_id.to_string());
                f.lease_deadline = Some(deadline);
            }
        }
    }

    /// `(owner, lease_id, deadline, snapshot)` of an open writable handle of
    /// `path` holding a lease and a current snapshot (detached handles have
    /// another path). `None` = no such handle, or its snapshot is stale.
    fn shared_writer(&self, path: &str) -> Option<(String, String, Option<Instant>, Arc<FileLocations>)> {
        self.open_files.iter().find_map(|e| {
            let f = e.value();
            if f.path != path || !f.flags.is_writable() {
                return None;
            }
            Some((f.lease_owner_id.clone()?, f.lease_id.clone()?, f.lease_deadline, f.locations.clone()?))
        })
    }

    fn open_lease_owners(&self) -> Vec<(String, Option<String>)> {
        self.open_files
            .iter()
            .map(|e| (e.value().path.clone(), e.value().lease_owner_id.clone()))
            .collect()
    }
}

/// `write.async_commit` rename: the paths whose `path_guard` rename holds,
/// sorted and unique: `from`, plus every writable handle at or below `from`
/// (re-keyed) or `to` (detached). Only those can have a timer commit.
fn rename_lock_paths(open: &[(String, OpenFlags)], from: &str, to: &str) -> Vec<String> {
    let mut paths: std::collections::BTreeSet<String> = open
        .iter()
        .filter(|(p, f)| {
            f.is_writable() && (rekey_path(p, from, to).is_some() || rekey_path(p, to, to).is_some())
        })
        .map(|(p, _)| p.clone())
        .collect();
    paths.insert(from.to_string());
    paths.into_iter().collect()
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

/// Runs `upload(i)` for every segment index of a flush batch and returns the
/// results in index order, or the lowest-index error. At most `concurrency`
/// uploads run at once (`1` = sequential); after the first error in index
/// order no further segment starts and the other in-flight puts are dropped.
async fn upload_segments<T, F, Fut>(n: usize, concurrency: usize, upload: F) -> Result<Vec<T>>
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    use futures_util::{StreamExt, TryStreamExt};
    futures_util::stream::iter((0..n).map(upload))
        .buffered(concurrency.max(1))
        .try_collect()
        .await
}

/// Spawns `fut` only if `permits` has a free permit (held until `fut` ends);
/// returns false and drops `fut` when the bound is full, never waiting.
fn spawn_bounded<F>(permits: &Arc<tokio::sync::Semaphore>, fut: F) -> bool
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    match permits.clone().try_acquire_owned() {
        Ok(permit) => {
            tokio::spawn(async move {
                let _permit = permit;
                fut.await;
            });
            true
        }
        Err(_) => false,
    }
}

#[cfg(test)]
mod perf_io_concurrency_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[tokio::test]
    async fn upload_segments_keeps_at_most_n_in_flight_in_order() {
        for n in [1usize, 2, 4] {
            let inflight = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));
            let res = upload_segments(6, n, |i| {
                let (inflight, peak) = (inflight.clone(), peak.clone());
                async move {
                    let now = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    // Later segments finish first: order must still hold.
                    tokio::time::sleep(Duration::from_millis(30 - 4 * i as u64)).await;
                    inflight.fetch_sub(1, Ordering::SeqCst);
                    Ok(i * 10)
                }
            })
            .await
            .unwrap();
            assert_eq!(peak.load(Ordering::SeqCst), n, "concurrency {n}");
            assert_eq!(res, vec![0, 10, 20, 30, 40, 50]);
        }
    }

    #[tokio::test]
    async fn upload_segments_n1_is_sequential_and_stops_at_first_error() {
        let started = Arc::new(AtomicUsize::new(0));
        let res: Result<Vec<usize>> = upload_segments(4, 1, |i| {
            let started = started.clone();
            async move {
                started.fetch_add(1, Ordering::SeqCst);
                if i == 1 { bail!("segment {i} failed") } else { Ok(i) }
            }
        })
        .await;
        assert_eq!(res.unwrap_err().to_string(), "segment 1 failed");
        assert_eq!(started.load(Ordering::SeqCst), 2, "no segment starts after the failure");
    }

    #[tokio::test]
    async fn upload_segments_returns_lowest_index_error() {
        let started = Arc::new(AtomicUsize::new(0));
        let res: Result<Vec<usize>> = upload_segments(6, 2, |i| {
            let started = started.clone();
            async move {
                started.fetch_add(1, Ordering::SeqCst);
                match i {
                    // Segment 1 fails after segment 2 already failed.
                    1 => {
                        tokio::time::sleep(Duration::from_millis(30)).await;
                        bail!("segment 1 failed")
                    }
                    2 => bail!("segment 2 failed"),
                    _ => Ok(i),
                }
            }
        })
        .await;
        assert_eq!(res.unwrap_err().to_string(), "segment 1 failed");
        assert_eq!(started.load(Ordering::SeqCst), 3, "only segments 0..=2 started");
    }

    #[tokio::test]
    async fn spawn_bounded_drops_when_full_and_never_blocks() {
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        assert!(spawn_bounded(&permits, async move {
            let _ = rx.await;
        }));
        // Bound full: dropped immediately, not queued.
        let ran = Arc::new(AtomicUsize::new(0));
        let r = ran.clone();
        assert!(!spawn_bounded(&permits, async move {
            r.fetch_add(1, Ordering::SeqCst);
        }));
        tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while permits.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("permit released when the task ends");
        assert!(spawn_bounded(&permits, async {}));
        tokio::task::yield_now().await;
        assert_eq!(ran.load(Ordering::SeqCst), 0, "dropped put never ran");
    }
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
    use std::sync::Arc;
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
            is_symlink: false,
        }
    }

    #[test]
    fn symlink_bit_propagates_into_attrs() {
        let mut loc = empty_locations("/l");
        assert!(!super::file_attr_from_locations(&loc).is_symlink);
        loc.is_symlink = true;
        assert!(super::file_attr_from_locations(&loc).is_symlink);
    }

    #[test]
    fn insert_5000_stays_bounded() {
        let cache: DashMap<String, (Arc<FileLocations>, Instant)> = DashMap::new();
        let mut order: VecDeque<String> = VecDeque::new();
        for i in 0..5000 {
            let path = format!("/movies/{i}.mkv");
            cache.insert(path.clone(), (Arc::new(empty_locations(&path)), Instant::now()));
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
        let cache: DashMap<String, (Arc<FileLocations>, Instant)> = DashMap::new();
        cache.insert(
            "/stale.mkv".to_string(),
            (Arc::new(empty_locations("/stale.mkv")), Instant::now() - Duration::from_secs(120)),
        );
        cache.insert(
            "/fresh.mkv".to_string(),
            (Arc::new(empty_locations("/fresh.mkv")), Instant::now()),
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
            since: None,
            gaps: Vec::new(),
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

    /// knfsd reopen: a second writable open on the same path joins the
    /// first handle's lease owner; other paths, readers and detached
    /// handles never lend theirs.
    #[test]
    fn writable_reopen_shares_the_same_path_lease_owner() {
        use super::shared_lease_owner;
        let o = |p: &str, owner: Option<&str>| (p.to_string(), owner.map(str::to_string));
        let others = vec![o("/f", None), o("/g", Some("h:1")), o("/f", Some("h:2"))];
        assert_eq!(shared_lease_owner(&others, "/f").as_deref(), Some("h:2"));
        assert_eq!(shared_lease_owner(&[o("/g", Some("h:1"))], "/f"), None);
        assert_eq!(shared_lease_owner(&[o("/f", None)], "/f"), None);
        assert_eq!(shared_lease_owner(&[o("/f\0detached-1", Some("h:1"))], "/f"), None);
        assert_eq!(shared_lease_owner(&[], "/f"), None);
    }

    /// release() keeps a shared lease while another handle still uses its
    /// owner; the last handle (or a different owner) releases.
    #[test]
    fn release_keeps_lease_while_owner_still_open() {
        use super::lease_owner_still_open;
        assert!(lease_owner_still_open(&[None, Some("h:2".into())], "h:2"));
        assert!(!lease_owner_still_open(&[], "h:2"));
        assert!(!lease_owner_still_open(&[None, Some("h:3".into())], "h:2"));
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
            is_symlink: false,
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

    /// Unsorted labelled input (an older manager, or any non-BTreeMap order)
    /// is sorted at construction so the binary-search lookups are correct.
    #[test]
    fn unsorted_manager_input_is_sorted_for_lookup() {
        let mut l = locs(&[(3, "h3"), (0, "h0"), (7, "h7"), (1, "h1")]);
        super::sort_chunks_by_index(&mut l.chunks);
        for (i, id) in [(0u64, "h0"), (1, "h1"), (3, "h3"), (7, "h7")] {
            assert_eq!(l.chunk_at(i).map(|c| c.chunk_id.as_str()), Some(id), "index {i}");
        }
        assert!(l.chunk_at(2).is_none() && l.chunk_at(8).is_none(), "holes stay holes");
        assert_eq!(l.chunk_count(), 8);
    }

    /// All-zero labels (positional, older manager) keep their order.
    #[test]
    fn positional_labels_keep_order_after_sort() {
        let mut l = locs(&[(0, "a"), (0, "b"), (0, "c")]);
        super::sort_chunks_by_index(&mut l.chunks);
        assert_eq!(l.chunk_at(1).unwrap().chunk_id, "b");
        assert_eq!(l.chunk_count(), 3);
        let w: Vec<(u64, &str)> = l.chunks_in(1, 5).map(|(i, c)| (i, c.chunk_id.as_str())).collect();
        assert_eq!(w, [(1, "b"), (2, "c")]);
    }

    /// `chunks_in` returns exactly the labelled entries in range, skipping
    /// holes; `chunk_count` is max index + 1 even with holes.
    #[test]
    fn chunks_in_respects_holes_and_bounds() {
        let l = locs(&[(0, "h0"), (2, "h2"), (3, "h3"), (9, "h9")]);
        let w: Vec<u64> = l.chunks_in(1, 3).map(|(i, _)| i).collect();
        assert_eq!(w, [2, 3]);
        assert_eq!(l.chunks_in(4, 8).count(), 0);
        assert_eq!(l.chunks_in(10, 20).count(), 0);
        assert_eq!(l.chunk_count(), 10);
        assert_eq!(locs(&[]).chunk_count(), 0);
    }

    /// The post-commit merge keeps `chunks` sorted (lookup invariant).
    #[test]
    fn merge_keeps_chunks_sorted() {
        let mut l = locs(&[(0, "h0"), (5, "h5")]);
        let c = [committed(9, "n9", 1), committed(2, "n2", 1), committed(5, "n5", 1)];
        assert!(super::merge_into_snapshot(Some(&mut l), Some(&c), 1));
        let idx: Vec<u64> = l.chunks.iter().map(super::chunk_label).collect();
        assert_eq!(idx, [0, 2, 5, 9]);
        assert_eq!(l.chunk_at(2).unwrap().chunk_id, "n2");
        assert_eq!(l.chunk_at(5).unwrap().chunk_id, "n5");
    }

    const CS: u64 = 4 * 1024 * 1024;

    fn committed(idx: u32, id: &str, size: u64) -> ChunkInfo {
        super::committed_chunk_info(idx, id, size, vec!["http://w1:9500".to_string()], "warm")
    }

    /// Post-commit merge replaces the committed index, inserts new ones in
    /// index order, grows size to the committed end and leaves others alone.
    #[test]
    fn merge_replaces_and_inserts_committed_chunks_and_grows_size() {
        let mut l = locs(&[(0, "h0"), (1, "h1"), (3, "h3")]);
        l.total_size = 4 * CS;
        let c = [committed(1, "n1", CS), committed(2, "n2", CS), committed(4, "n4", 100)];
        assert!(super::merge_into_snapshot(Some(&mut l), Some(&c), 7), "merge must apply");
        let ids: Vec<&str> = l.chunks.iter().map(|c| c.chunk_id.as_str()).collect();
        assert_eq!(ids, ["h0", "n1", "n2", "h3", "n4"]);
        assert_eq!(l.chunk_at(1).unwrap().node_addresses, ["http://w1:9500"]);
        assert_eq!(l.chunk_at(1).unwrap().node_tiers, ["warm"]);
        assert_eq!(l.chunk_at(0).unwrap().node_addresses, ["http://n1:9500"], "untouched");
        assert_eq!(l.total_size, 4 * CS + 100);
        assert_eq!(l.modified_at_ms, 7);
        // A rewrite below EOF never shrinks the size.
        assert!(super::merge_into_snapshot(Some(&mut l), Some(&[committed(0, "x", 1)]), 8));
        assert_eq!(l.total_size, 4 * CS + 100);
    }

    /// A handle with no snapshot, or a lease-less commit (`None`), takes the
    /// whole-file re-fetch; a snapshot plus committed chunks does not.
    #[test]
    fn full_refetch_only_without_snapshot_or_for_leaseless_commit() {
        let mut l = locs(&[(0, "h0")]);
        assert!(super::merge_into_snapshot(Some(&mut l), Some(&[committed(1, "n1", CS)]), 1));
        assert!(!super::merge_into_snapshot(None, Some(&[committed(1, "n1", CS)]), 1));
        let before = format!("{l:?}");
        assert!(!super::merge_into_snapshot(Some(&mut l), None, 1));
        assert_eq!(format!("{l:?}"), before, "refused merge leaves the snapshot untouched");
    }

    /// Old-manager positional labels (all 0) or an unknown chunk size cannot
    /// be merged by index: refuse without touching the snapshot.
    #[test]
    fn merge_refuses_positional_or_sizeless_snapshots_untouched() {
        let mut old = locs(&[(0, "h0"), (0, "h1")]);
        let before = format!("{old:?}");
        assert!(!super::merge_into_snapshot(Some(&mut old), Some(&[committed(2, "n2", CS)]), 1));
        assert_eq!(format!("{old:?}"), before);
        let mut sizeless = locs(&[]);
        sizeless.chunk_size = 0;
        assert!(!super::merge_into_snapshot(Some(&mut sizeless), Some(&[committed(0, "n0", 1)]), 1));
        assert!(sizeless.chunks.is_empty());
    }

    /// After a batch commit of chunk 0 into a freshly created file, a header
    /// rewrite of chunk 0 must seed from the cluster: the chunk is found with
    /// a replica, and an unseeded partial segment for it is refused.
    #[test]
    fn header_rewrite_after_merge_seeds_from_committed_chunk() {
        let mut l = locs(&[]);
        assert!(super::merge_into_snapshot(Some(&mut l), Some(&[committed(0, "n0", CS)]), 1));
        assert!(!l.chunk_at(0).unwrap().node_addresses.is_empty(), "seed source exists");
        let wb = crate::writebuf::WriteBuffer::new(CS as usize);
        wb.write("/f", 0, b"hdr");
        assert_eq!(super::chunks_needing_seed(&wb, "/f", CS, 0, 3), [0]);
        let segs = wb.flush_all("/f");
        assert!(super::segments_committable(&segs, |i| l.chunk_at(i as u64).is_some()).is_err());
    }

    /// Copy-on-write: merging one holder of a shared snapshot never changes
    /// what another holder (an in-flight read) sees; a second holder of the
    /// same snapshot takes the same merged Arc; a sole owner merges in place.
    #[test]
    fn shared_snapshot_merge_is_copy_on_write() {
        use std::sync::Arc;
        let original = Arc::new(locs(&[(0, "h0")]));
        let reader = original.clone();
        let mut h1 = Some(original.clone());
        let mut h2 = Some(original);
        let mut done = Vec::new();
        let c = [committed(1, "n1", CS)];
        assert!(super::merge_shared(&mut h1, Some(&c), 1, &mut done));
        assert!(super::merge_shared(&mut h2, Some(&c), 1, &mut done));
        assert_eq!(reader.chunks.len(), 1, "another holder's snapshot must not change");
        assert!(reader.chunk_at(1).is_none());
        let (h1, h2) = (h1.unwrap(), h2.unwrap());
        assert_eq!(h1.chunk_at(1).unwrap().chunk_id, "n1");
        assert!(Arc::ptr_eq(&h1, &h2), "holders that shared one snapshot share the merge");
        // Refused merge (lease-less commit) leaves the slot untouched.
        let mut h3 = Some(reader.clone());
        assert!(!super::merge_shared(&mut h3, None, 1, &mut Vec::new()));
        assert!(Arc::ptr_eq(h3.as_ref().unwrap(), &reader));
        // Sole owner: no chunk-list copy (same Vec buffer after the merge).
        drop((h1, h2, h3));
        let mut sole = Some(Arc::new(locs(&[(0, "h0"), (1, "h1")])));
        let buf = sole.as_ref().unwrap().chunks.as_ptr();
        assert!(super::merge_shared(&mut sole, Some(&[committed(1, "n1", CS)]), 1, &mut Vec::new()));
        assert_eq!(sole.as_ref().unwrap().chunks.as_ptr(), buf, "sole owner merges in place");
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
            is_symlink: false,
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
            is_symlink: false,
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
            since: None,
            gaps: Vec::new(),
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
    fn temp_writer_only_for_bare_unaligned_shrink() {
        let cs = 100;
        assert!(truncate_needs_temp_writer(false, 250, 150, cs));
        assert!(!truncate_needs_temp_writer(true, 250, 150, cs), "writer already open");
        assert!(!truncate_needs_temp_writer(false, 250, 200, cs), "aligned shrink");
        assert!(!truncate_needs_temp_writer(false, 250, 300, cs), "grow");
        assert!(!truncate_needs_temp_writer(false, 250, 250, cs), "no-op");
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

#[cfg(test)]
mod w3_write_hole_tests {
    use super::*;

    /// Stale view (manager/cache says 0) must not win over the buffered end
    /// or a handle's known size, else fallocate(0, 1M) would shrink to 1M.
    #[test]
    fn authoritative_size_takes_max_of_all_sources() {
        let g = 1u64 << 30;
        assert_eq!(authoritative_size(0, Some(g), &[]), g);
        assert_eq!(authoritative_size(0, None, &[g, 5]), g);
        assert_eq!(authoritative_size(10, None, &[]), 10);
        assert_eq!(authoritative_size(0, None, &[]), 0);
        assert_eq!(crate::fuse_adapter::fallocate_target_size(0, 1 << 20, 0, g).unwrap(), None);
    }

    #[test]
    fn hole_tail_chunk_only_for_unaligned_eof_and_write_past_it() {
        assert_eq!(hole_tail_chunk(10, 100, 8), Some(1));
        assert_eq!(hole_tail_chunk(10, 10, 8), None, "write at EOF is not past it");
        assert_eq!(hole_tail_chunk(16, 100, 8), None, "aligned EOF has no partial tail");
        assert_eq!(hole_tail_chunk(0, 100, 8), None);
        assert_eq!(hole_tail_chunk(10, 100, 0), None);
    }

    /// Byte outcome of a partial write into a hole chunk below EOF
    /// (`truncate -s 1G; dd seek=5 conv=notrunc`): zero base from
    /// `seed_len_within_eof`, then the write -> one committable zero-padded chunk.
    #[test]
    fn write_into_hole_below_eof_commits_zero_padded_chunk() {
        let cs = 8u64;
        let eof = 100u64;
        let wb = WriteBuffer::new(cs as usize);
        wb.write("/f", 5 * cs + 2, b"xy");
        let n = seed_len_within_eof(cs as usize, 5 * cs, eof);
        assert_eq!(n, 8, "hole chunk below EOF seeds a full chunk");
        wb.seed_chunk("/f", 5, &vec![0u8; n]);
        let segs = wb.flush_all("/f");
        assert!(segments_committable(&segs, |_| false).is_ok());
        assert_eq!(segs[0].data.as_ref(), &[0, 0, b'x', b'y', 0, 0, 0, 0]);
        // Last chunk is bounded by EOF.
        assert_eq!(seed_len_within_eof(cs as usize, 12 * cs, eof), 4);
    }

    /// Without a zero seed the same write is refused by the commit guard.
    #[test]
    fn unseeded_hole_write_is_refused_by_guard() {
        let wb = WriteBuffer::new(8);
        wb.write("/f", 5 * 8 + 2, b"xy");
        let segs = wb.flush_all("/f");
        assert!(segments_committable(&segs, |_| false).is_err(), "offset_in_chunk > 0");
    }

    // Finding 1: a writer at a path that a rename/unlink replaces is detached
    // to a key the manager can never resolve.
    #[test]
    fn detached_handle_path_exact_child_and_sibling() {
        let d = detached_handle_path("/a", "/a", 7).unwrap();
        assert!(is_detached_path(&d));
        assert_ne!(d, "/a");
        let c = detached_handle_path("/a/x", "/a", 7).unwrap();
        assert!(is_detached_path(&c) && c.ends_with("/x"));
        // Negative: sibling with a shared prefix and unrelated paths stay put.
        assert_eq!(detached_handle_path("/ab", "/a", 7), None);
        assert_eq!(detached_handle_path("/b", "/a", 7), None);
        assert!(!is_detached_path("/a"));
    }

    // Detached writer bytes never merge into the replacement's buffer.
    #[test]
    fn detached_writer_bytes_do_not_reach_replacement() {
        let wb = WriteBuffer::new(16);
        wb.write("/a.part", 0, b"new");
        let det = detached_handle_path("/a", "/a", 1).unwrap();
        wb.write(&det, 0, b"old-writer");
        wb.rename("/a.part", "/a");
        let segs = wb.flush_all("/a");
        assert_eq!(segs[0].data.as_ref(), b"new");
        assert_eq!(wb.flush_all(&det)[0].data.as_ref(), b"old-writer");
    }

    // Finding 6: a create() that landed on a populated file is committed.
    #[test]
    fn created_file_committed_any_follows_chunks() {
        assert!(!committed_any_from_created_snapshot(0));
        assert!(committed_any_from_created_snapshot(1));
    }
}

#[cfg(test)]
mod commit_tracker_tests {
    use super::{merge_allowed, CommitTracker};

    /// Back-to-back commits on one path are each exclusive (merge allowed).
    #[test]
    fn sequential_commits_are_exclusive() {
        let t = CommitTracker::default();
        let a = t.begin("/f");
        assert!(a.exclusive());
        drop(a);
        let b = t.begin("/f");
        assert!(b.exclusive());
        // Another path never interferes.
        let other = t.begin("/g");
        assert!(b.exclusive() && other.exclusive());
    }

    /// Overlapping commits X, Y on one path: neither may merge (manager
    /// apply order is unknown), even after the other one finished.
    #[test]
    fn overlapping_commits_force_full_refetch() {
        let t = CommitTracker::default();
        let x = t.begin("/f");
        let y = t.begin("/f");
        assert!(!x.exclusive(), "a later commit started inside X's window");
        assert!(!y.exclusive(), "Y started while X was in flight");
        drop(x);
        assert!(!y.exclusive(), "Y overlapped X even though X has finished");
        drop(y);
        assert!(t.begin("/f").exclusive(), "a fresh commit after both is exclusive");
    }

    /// The slot is released on an error return (drop guard) and the path's
    /// entry is removed at zero.
    #[test]
    fn tracker_entry_cleaned_up_on_error_path() {
        let t = CommitTracker::default();
        let failing = || -> Result<(), &'static str> {
            let _token = t.begin("/f");
            Err("commit_chunk_locations failed")?;
            Ok(())
        };
        assert_eq!(failing(), Err("commit_chunk_locations failed"));
        assert!(t.0.is_empty(), "entry must be removed when no commit is in flight");
        let a = t.begin("/f");
        let b = t.begin("/f");
        drop(a);
        assert_eq!(t.0.get("/f").map(|e| e.0), Some(1));
        drop(b);
        assert!(t.0.is_empty());
    }

    /// Simulated reorder: a fetch taken before a concurrent commit (started
    /// and even finished during the fetch) must not be installed; neither may
    /// one started while a commit was in flight. A quiet fetch may.
    #[test]
    fn fetch_overlapping_a_commit_is_not_installed() {
        let t = CommitTracker::default();
        let f = t.begin_fetch("/f");
        drop(t.begin("/f"));
        assert!(!f.may_install(), "a commit applied during the fetch makes it stale");
        drop(f);
        let c = t.begin("/f");
        let f = t.begin_fetch("/f");
        drop(c);
        assert!(!f.may_install(), "fetch started while a commit was in flight");
        drop(f);
        assert!(t.0.is_empty(), "entry removed once no commit or fetch is in flight");
        let f = t.begin_fetch("/f");
        let _other_path = t.begin("/g");
        assert!(f.may_install(), "quiet fetch (other paths do not count) is installed");
        let c = t.begin("/f");
        assert!(!f.may_install(), "a commit in flight at the fetch's end blocks install");
        assert!(c.exclusive(), "a fetch does not make a commit non-exclusive");
    }

    /// An overlapped fetch waits for the path to go quiet (it is never used),
    /// a quiet one installs, and only an overlapped one past the deadline
    /// fails. Fake clock: plain `Instant`s.
    #[test]
    fn overlapped_fetch_waits_instead_of_being_used() {
        use super::{next_fetch_step, FetchStep};
        let t0 = std::time::Instant::now();
        let later = t0 + std::time::Duration::from_secs(1);
        assert_eq!(next_fetch_step(false, t0, later), FetchStep::WaitQuiet);
        assert_eq!(next_fetch_step(true, t0, later), FetchStep::Install);
        assert_eq!(next_fetch_step(true, later, later), FetchStep::Install);
        assert_eq!(next_fetch_step(false, later, later), FetchStep::TimedOut, "deadline reached");
    }

    /// A writer's re-fetch waits for the in-flight commit to end (woken, not
    /// polled), and the fetch it then takes is installable.
    #[tokio::test]
    async fn writer_refetch_waits_for_in_flight_commit_then_installs() {
        use std::time::Duration;
        let t = CommitTracker::default();
        let commit = t.begin("/f");
        let start = std::time::Instant::now();
        let (_, (waited, took)) = tokio::join!(
            async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                drop(commit);
            },
            async { (t.wait_quiet("/f", Duration::from_secs(10)).await, start.elapsed()) },
        );
        assert!(waited.is_ok());
        assert!(took >= Duration::from_millis(50), "must wait for the commit to end");
        assert!(t.begin_fetch("/f").may_install(), "fetch after the commit is installable");
    }

    /// A commit that never ends: the wait times out with an error that the
    /// adapter maps to EIO (never a guessed snapshot).
    #[tokio::test]
    async fn quiet_wait_timeout_is_eio() {
        use std::time::Duration;
        let t = CommitTracker::default();
        let _commit = t.begin("/f");
        let err = t.wait_quiet("/f", Duration::from_millis(30)).await.unwrap_err();
        assert_eq!(
            crate::fuse_adapter::cluster_err_to_errno(&err),
            fuse3::Errno::from(libc::EIO)
        );
        // No commit in flight: returns at once.
        assert!(t.wait_quiet("/g", Duration::from_millis(1)).await.is_ok());
    }

    /// truncate holds a commit slot around SetFileSize: a refresh fetch that
    /// overlapped it is not installed.
    #[test]
    fn truncate_size_change_blocks_overlapping_fetch_install() {
        let t = CommitTracker::default();
        let fetch = t.begin_fetch("/f");
        drop(t.begin("/f")); // truncate_inner's slot around set_file_size
        assert!(!fetch.may_install());
    }

    /// open() vs a local commit: open's fetch ran before a commit applied
    /// (commit started and ended during the fetch, before the handle was
    /// inserted). The snapshot must not be stored on the handle or cached.
    #[test]
    fn open_snapshot_fetched_before_commit_is_not_installed() {
        use super::{gated_snapshot, FileLocations};
        use std::sync::Arc;
        let t = CommitTracker::default();
        let snap = |p: &str| Arc::new(FileLocations {
            path: p.to_string(),
            total_size: 0,
            chunks: Vec::new(),
            volume_id: "default".to_string(),
            chunk_size: 4,
            created_at_ms: 0,
            modified_at_ms: 0,
            is_directory: false,
            is_symlink: false,
        });
        let fetch = t.begin_fetch("/f");
        drop(t.begin("/f")); // writer's commit lands, merge loop runs
        assert!(gated_snapshot(snap("/f"), fetch.may_install()).is_none());
        drop(fetch);
        let fetch = t.begin_fetch("/f");
        assert!(gated_snapshot(snap("/f"), fetch.may_install()).is_some(), "quiet open installs");
    }

    /// A writer seeds (O_APPEND tail, RMW, truncate tail) only from a
    /// snapshot fetched after its lease was granted and not overlapped by a
    /// local commit. A pre-lease fetch is never enough, even when quiet: the
    /// previous writer may be on another host (not tracked locally).
    #[test]
    fn seed_needs_post_lease_quiet_snapshot() {
        use super::seed_snapshot_usable;
        assert!(!seed_snapshot_usable(false, true), "pre-lease fetch, even if quiet");
        assert!(!seed_snapshot_usable(true, false), "post-lease but overlapped a commit");
        assert!(!seed_snapshot_usable(false, false));
        assert!(seed_snapshot_usable(true, true));
    }

    /// read(): a missing entry below EOF triggers one re-fetch; a missing
    /// entry at/after EOF, a present entry, or a snapshot that already came
    /// from such a re-fetch serve zeros directly.
    #[test]
    fn read_gap_below_size_refetches_before_zeros() {
        use super::gap_needs_refetch;
        assert!(gap_needs_refetch(true, 2, 12, 4, false), "chunk 2 starts below EOF 12");
        assert!(!gap_needs_refetch(true, 3, 12, 4, false), "chunk 3 starts at EOF");
        assert!(!gap_needs_refetch(false, 2, 12, 4, false), "entry present");
        assert!(!gap_needs_refetch(true, 2, 12, 4, true), "hole already confirmed");
        assert!(!gap_needs_refetch(true, 0, 12, 0, false), "unknown chunk size");
    }

    /// A detached fh (unlink/rename moved it) or a closed fh must not merge
    /// into whatever now lives under the path; same path + exclusive may.
    #[test]
    fn detached_or_closed_fh_never_merges() {
        assert!(merge_allowed(true, Some("/f"), "/f"));
        assert!(!merge_allowed(true, Some("/f\0detached-3"), "/f"));
        assert!(!merge_allowed(true, None, "/f"));
        assert!(!merge_allowed(false, Some("/f"), "/f"));
    }
}

/// `write.async_commit` on a filesystem whose client has no manager: every
/// upload attempt fails fast ("no managers configured"), so an `Err` proves
/// an upload was attempted and `Ok` with the bytes still buffered proves none was.
#[cfg(test)]
mod async_commit_fs_tests {
    use super::*;

    const AGE: Duration = Duration::from_secs(30);

    fn fs(async_on: bool) -> DtfsFilesystem {
        let mut cfg: FuseConfig =
            toml::from_str(include_str!("../../../deploy/examples/fuse.toml")).unwrap();
        cfg.write.async_commit = async_on;
        DtfsFilesystem::new(DtfsClient::new(), cfg.location.clone(), cfg)
    }

    fn open(fs: &DtfsFilesystem, path: &str, flags: OpenFlags) -> u64 {
        let fh = fs.next_fh.fetch_add(1, Ordering::Relaxed);
        fs.open_files.insert(
            fh,
            OpenFile {
                path: path.to_string(),
                flags,
                append: false,
                locations: None,
                chunk_size: fs.write_buffer.chunk_size() as u64,
                lease_id: None,
                lease_deadline: None,
                lease_owner_id: None,
                committed_any: true,
                last_read_chunk: None,
                bytes_read_on_fh: 0,
                holes_confirmed: None,
                write_error: None,
                close_seen: false,
            },
        );
        fh
    }

    fn set_lease(fs: &DtfsFilesystem, fh: u64, owner: &str, id: &str) {
        let mut f = fs.open_files.get_mut(&fh).unwrap();
        f.lease_owner_id = Some(owner.to_string());
        f.lease_id = Some(id.to_string());
    }

    fn lease_id(fs: &DtfsFilesystem, fh: u64) -> Option<String> {
        fs.open_files.get(&fh).unwrap().lease_id.clone()
    }

    /// Lease lapse mid-write with a shared owner (knfsd reopen): the handle
    /// that re-acquires gets a new lease id, and so must every other handle
    /// of that owner, else the one closing last releases a stale id and the
    /// lease lingers until expiry. Other owners and readers keep theirs.
    #[test]
    fn reacquired_lease_reaches_every_handle_of_the_owner() {
        let fs = fs(false);
        let a = open(&fs, "/f", OpenFlags::ReadWrite);
        let b = open(&fs, "/f", OpenFlags::ReadWrite);
        let other = open(&fs, "/g", OpenFlags::ReadWrite);
        let reader = open(&fs, "/f", OpenFlags::ReadOnly);
        set_lease(&fs, a, "h:1", "old");
        set_lease(&fs, b, "h:1", "old");
        set_lease(&fs, other, "h:2", "g-lease");

        let deadline = Instant::now() + Duration::from_secs(60);
        fs.adopt_reacquired_lease("h:1", "new", deadline);

        assert_eq!(lease_id(&fs, a).as_deref(), Some("new"));
        assert_eq!(lease_id(&fs, b).as_deref(), Some("new"), "sibling of the re-acquirer");
        assert_eq!(fs.open_files.get(&b).unwrap().lease_deadline, Some(deadline));
        assert_eq!(lease_id(&fs, other).as_deref(), Some("g-lease"), "other owner untouched");
        assert_eq!(lease_id(&fs, reader), None, "reader holds no lease");
    }

    /// Chunkless snapshot of `size` bytes: every seed is a zero seed, no I/O
    /// (free function: rustc 1.94 struct-literal ICE workaround).
    fn holes(path: &str, size: u64, chunk_size: u64) -> FileLocations {
        FileLocations {
            path: path.to_string(),
            total_size: size,
            chunks: Vec::new(),
            volume_id: "default".to_string(),
            chunk_size,
            created_at_ms: 0,
            modified_at_ms: 0,
            is_directory: false,
            is_symlink: false,
        }
    }

    /// A write whose cap drain fails returns EIO and must not leave the seeds
    /// it just added: retried writes would otherwise grow the buffer past
    /// `DIRTY_BYTES_CAP` without bound while uploads fail. Final drain: the
    /// in-range seed is dropped. Hole-fill drain: the tail seed (100 bytes)
    /// stays, once, so the buffer does not grow across retries.
    #[tokio::test]
    async fn failed_cap_drain_drops_the_writes_own_seeds() {
        let cap = crate::writebuf::DIRTY_BYTES_CAP;
        for async_on in [false, true] {
            let fs = Arc::new(fs(async_on));
            let cs = fs.write_buffer.chunk_size() as u64;
            let full = (cap as u64 / cs) as u32;
            for (eof, offset) in [(64 * cs, 40 * cs + 10), (full as u64 * cs + 100, (full as u64 + 2) * cs)] {
                let fh = open(&fs, "/f", OpenFlags::ReadWrite);
                fs.open_files.get_mut(&fh).unwrap().locations = Some(Arc::new(holes("/f", eof, cs)));
                for i in 0..full {
                    fs.write_buffer.write("/f", i as u64 * cs, &vec![1; cs as usize]);
                }
                assert_eq!(fs.write_buffer.dirty_bytes("/f"), cap);
                let tail = (eof % cs) as usize * usize::from(offset > eof);
                for k in 0..3u64 {
                    let err = fs.write(fh, offset + k * cs, &[2; 10]).await.unwrap_err();
                    assert!(format!("{err:#}").contains("allocate_chunks failed for /f"), "{err:#}");
                    assert_eq!(
                        fs.write_buffer.dirty_bytes("/f"),
                        cap + tail,
                        "async={async_on} eof={eof} try {k}: seeds left behind"
                    );
                }
                fs.write_buffer.remove("/f");
                fs.open_files.remove(&fh);
            }
        }
    }

    #[tokio::test]
    async fn async_fsync_on_partial_tail_uploads_nothing() {
        let fs = fs(true);
        let fh = open(&fs, "/f", OpenFlags::ReadWrite);
        fs.write_buffer.write("/f", 0, &[7; 100]);
        fs.fsync(fh).await.expect("async fsync must not upload the partial tail");
        assert_eq!(fs.write_buffer.dirty_bytes("/f"), 100);
    }

    /// close(2) sends FUSE_FLUSH and returns its result; RELEASE is async and
    /// its reply ignored, so flush must commit even in async mode.
    #[tokio::test]
    async fn async_flush_on_close_commits_the_partial_tail() {
        let fs = fs(true);
        let fh = open(&fs, "/f", OpenFlags::ReadWrite);
        fs.write_buffer.write("/f", 0, &[7; 100]);
        let err = fs.flush(fh).await.unwrap_err();
        assert!(format!("{err:#}").contains("allocate_chunks failed for /f"), "{err:#}");
        assert_eq!(fs.write_buffer.dirty_bytes("/f"), 100, "restored after the failed upload");
    }

    /// knfsd opens, fsyncs and closes a separate file for every NFS COMMIT
    /// while the NFSv4 open-state file stays open: that close must not
    /// upload the partial chunks (prod: x6.9 chunk bytes per byte received).
    #[tokio::test]
    async fn async_close_of_a_second_writer_uploads_nothing() {
        let fs = fs(true);
        let fh1 = open(&fs, "/f", OpenFlags::ReadWrite);
        let fh2 = open(&fs, "/f", OpenFlags::ReadWrite);
        fs.write_buffer.write("/f", 0, &[7; 100]);
        fs.flush(fh2).await.expect("another writer is open: flush uploads nothing");
        fs.release(fh2).await.expect("another writer is open: release uploads nothing");
        assert_eq!(fs.write_buffer.dirty_bytes("/f"), 100, "buffer kept for the last writer");
        let err = fs.flush(fh1).await.unwrap_err();
        assert!(format!("{err:#}").contains("allocate_chunks failed for /f"), "{err:#}");
    }

    /// Two closes racing: the handle whose flush already ran (skipped) no
    /// longer counts as "another writer", so the second flush commits.
    #[tokio::test]
    async fn async_two_flushes_cannot_both_skip() {
        let fs = fs(true);
        let fh1 = open(&fs, "/f", OpenFlags::ReadWrite);
        let fh2 = open(&fs, "/f", OpenFlags::ReadWrite);
        fs.write_buffer.write("/f", 0, &[7; 100]);
        fs.flush(fh2).await.expect("fh1 still open and not closing");
        assert!(fs.flush(fh1).await.is_err(), "fh2 is closing: fh1 is the last writer");
        assert!(fs.release(fh2).await.is_err(), "fh1 closing too: release commits");
    }

    /// The pending background error is still reported by a skipped flush.
    #[tokio::test]
    async fn async_skipped_flush_reports_the_pending_error() {
        let fs = fs(true);
        let _fh1 = open(&fs, "/f", OpenFlags::ReadWrite);
        let fh2 = open(&fs, "/f", OpenFlags::ReadWrite);
        fs.open_files.get_mut(&fh2).unwrap().write_error = Some("bg failed".into());
        let err = fs.flush(fh2).await.unwrap_err();
        assert_eq!(format!("{err:#}"), "bg failed");
    }

    /// FUSE-4: a transient background failure armed the sticky error; the
    /// last writer's close then commits everything (here: nothing left) and
    /// must return Ok, not the stale EIO. A reader's close commits nothing
    /// and must not clear the writer's error.
    #[tokio::test]
    async fn async_last_writer_close_after_transient_bg_failure_is_ok() {
        let fs = fs(true);
        let fh = open(&fs, "/f", OpenFlags::ReadWrite);
        let r = open(&fs, "/f", OpenFlags::ReadOnly);
        fs.record_aged_commit("/f", Err(anyhow::anyhow!("put failed")));
        fs.flush(r).await.expect("reader close");
        assert!(fs.open_files.get(&fh).unwrap().write_error.is_some(), "reader close cleared it");
        fs.flush(fh).await.expect("commit_all succeeded: the stale error must not fail close");
        fs.record_aged_commit("/f", Err(anyhow::anyhow!("put failed")));
        fs.release(fh).await.expect("release: same");
    }

    #[tokio::test]
    async fn sync_close_of_a_second_writer_still_commits() {
        let fs = fs(false);
        let _fh1 = open(&fs, "/f", OpenFlags::ReadWrite);
        let fh2 = open(&fs, "/f", OpenFlags::ReadWrite);
        fs.write_buffer.write("/f", 0, &[7; 100]);
        assert!(fs.flush(fh2).await.is_err(), "async off: every close commits");
    }

    #[tokio::test]
    async fn async_reader_or_detached_writer_is_not_another_writer() {
        let fs = fs(true);
        let _r = open(&fs, "/f", OpenFlags::ReadOnly);
        let det = detached_handle_path("/f", "/f", 1).unwrap();
        let _d = open(&fs, &det, OpenFlags::ReadWrite);
        let fh = open(&fs, "/f", OpenFlags::ReadWrite);
        fs.write_buffer.write("/f", 0, &[7; 100]);
        let err = fs.flush(fh).await.unwrap_err();
        assert!(format!("{err:#}").contains("allocate_chunks failed for /f"), "{err:#}");
    }

    #[tokio::test]
    async fn sync_fsync_still_uploads_partial_tail() {
        let fs = fs(false);
        let fh = open(&fs, "/f", OpenFlags::ReadWrite);
        fs.write_buffer.write("/f", 0, &[7; 100]);
        let err = fs.fsync(fh).await.unwrap_err();
        assert!(format!("{err:#}").contains("allocate_chunks failed for /f"), "{err:#}");
        assert_eq!(fs.write_buffer.dirty_bytes("/f"), 100, "restored after the failed upload");
    }

    #[tokio::test]
    async fn timer_commits_after_the_age_and_failure_is_reported_once() {
        let fs = fs(true);
        let fh = open(&fs, "/f", OpenFlags::ReadWrite);
        fs.write_buffer.write("/f", 0, &[7; 100]);
        fs.commit_aged(Instant::now() + AGE - Duration::from_secs(1)).await;
        fs.fsync(fh).await.expect("younger than the age: no commit attempted");
        fs.commit_aged(Instant::now() + AGE).await;
        assert_eq!(fs.write_buffer.dirty_bytes("/f"), 100, "failed commit keeps the bytes");
        let err = fs.fsync(fh).await.unwrap_err();
        assert!(format!("{err:#}").contains("async commit of /f failed"), "{err:#}");
        fs.fsync(fh).await.expect("the sticky error is reported once");
        // Retried on the next tick (age kept), reported again once.
        fs.commit_aged(Instant::now() + AGE).await;
        assert!(fs.fsync(fh).await.is_err());
    }

    #[tokio::test]
    async fn timer_never_commits_detached_or_reader_only_paths() {
        let fs = fs(true);
        let det = detached_handle_path("/d", "/d", 1).unwrap();
        let dfh = open(&fs, &det, OpenFlags::ReadWrite);
        fs.write_buffer.write(&det, 0, b"old-writer");
        let rfh = open(&fs, "/r", OpenFlags::ReadOnly);
        fs.write_buffer.write("/r", 0, b"orphan");
        let fh = open(&fs, "/f", OpenFlags::ReadWrite);
        fs.write_buffer.write("/f", 0, b"live");
        fs.commit_aged(Instant::now() + AGE).await;
        assert!(fs.fsync(fh).await.is_err(), "the live path was attempted");
        assert!(fs.open_files.get(&dfh).unwrap().write_error.is_none(), "detached: not attempted");
        assert!(fs.open_files.get(&rfh).unwrap().write_error.is_none());
        assert_eq!(fs.write_buffer.dirty_bytes(&det), 10);
        assert_eq!(fs.write_buffer.dirty_bytes("/r"), 6);
    }

    /// A held `path_guard` stands for the timer's in-flight commit: rename
    /// must wait for it on the source, a destination with an open writer, and
    /// an open writer below a renamed directory.
    #[tokio::test]
    async fn async_rename_waits_for_commits_on_source_destination_and_children() {
        for (writer, held, from, to) in [
            ("/a", "/a", "/a", "/b"),
            ("/b", "/b", "/a", "/b"),
            ("/d/x", "/d/x", "/d", "/e"),
        ] {
            let fs = fs(true);
            open(&fs, writer, OpenFlags::ReadWrite);
            let g = fs.path_guard(held).await;
            let r = tokio::time::timeout(Duration::from_millis(200), fs.rename(from, to)).await;
            assert!(r.is_err(), "rename {from} -> {to} must wait for the commit on {held}");
            fs.drop_path_guard(held, g);
            let err = fs.rename(from, to).await.unwrap_err();
            assert!(format!("{err:#}").contains(&format!("rename {from} -> {to}")), "{err:#}");
        }
    }

    /// write/flush/release read the fh's path, then wait for its guard; a
    /// directory rename can re-key the handle meanwhile. They must end up
    /// holding the guard of the path the handle has now (the one
    /// `commit_aged` locks), not the old one.
    #[tokio::test]
    async fn fh_path_guard_follows_a_rename_while_waiting() {
        let fs = Arc::new(fs(true));
        let fh = open(&fs, "/d/f", OpenFlags::ReadWrite);
        let held = fs.path_guard("/d/f").await; // rename holds it while re-keying
        let f2 = fs.clone();
        let waiter = tokio::spawn(async move { f2.fh_path_guard(fh).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        fs.open_files.get_mut(&fh).unwrap().path = "/e/f".to_string();
        fs.drop_path_guard("/d/f", held);
        let (p, g) = waiter.await.unwrap();
        assert_eq!(p.as_deref(), Some("/e/f"), "guard taken on the pre-rename path");
        let timer = tokio::time::timeout(Duration::from_millis(100), fs.path_guard("/e/f")).await;
        assert!(timer.is_err(), "commit_aged on /e/f must wait for the writer");
        fs.drop_path_guard("/e/f", g);
        assert_eq!(fs.fh_path_guard(999).await.0, None, "unknown fh");
    }

    /// Client connected to a manager that accepts and never answers: a
    /// commit through it stays in flight until aborted.
    async fn black_hole_client() -> (DtfsClient, tokio::task::JoinHandle<()>) {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let hole = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((s, _)) = l.accept().await {
                held.push(s);
            }
        });
        let client = DtfsClient::new();
        client.connect(&addr).await.expect("black-hole manager accepts");
        (client, hole)
    }

    /// Writer fs over `client` with a chunkless snapshot of `eof` bytes on
    /// `/f` (zero seeds, no I/O); `fill` complete chunks of 1s buffered.
    fn writer_fs(client: &DtfsClient, async_on: bool, eof_chunks: u64, eof_extra: u64, fill: u32) -> (Arc<DtfsFilesystem>, u64, u64) {
        let mut cfg: FuseConfig =
            toml::from_str(include_str!("../../../deploy/examples/fuse.toml")).unwrap();
        cfg.write.async_commit = async_on;
        let fs = Arc::new(DtfsFilesystem::new(client.clone(), cfg.location.clone(), cfg));
        let cs = fs.write_buffer.chunk_size() as u64;
        let fh = open(&fs, "/f", OpenFlags::ReadWrite);
        let eof = eof_chunks * cs + eof_extra;
        fs.open_files.get_mut(&fh).unwrap().locations = Some(Arc::new(holes("/f", eof, cs)));
        for i in 0..fill {
            fs.write_buffer.write("/f", i as u64 * cs, &vec![1; cs as usize]);
        }
        (fs, fh, cs)
    }

    /// Start `fs.write(fh, offset, data)` and wait until its commit is
    /// stuck on the black-hole manager.
    async fn stuck_write(fs: &Arc<DtfsFilesystem>, fh: u64, offset: u64, data: Vec<u8>) -> tokio::task::JoinHandle<Result<u32>> {
        let f2 = fs.clone();
        let w = tokio::spawn(async move { f2.write(fh, offset, &data).await });
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!w.is_finished(), "the commit must still be in flight");
        w
    }

    fn overlay_len(fs: &DtfsFilesystem, at: u64, len: usize) -> usize {
        fs.write_buffer.overlay("/f", at, &mut vec![0u8; len])
    }

    /// The final cap drain of a write into a zero-seeded chunk below EOF
    /// drains the other complete chunks but not the write's own (`keep`):
    /// drained, it would take the seed and leave the write's bytes in a
    /// fresh unseeded chunk. The drained chunks stay visible in flight.
    #[tokio::test]
    async fn cap_drain_keeps_the_pending_writes_seeded_chunk_and_stays_visible() {
        let (client, hole) = black_hole_client().await;
        let full = (crate::writebuf::DIRTY_BYTES_CAP / (4 << 20)) as u32;
        for async_on in [false, true] {
            let (fs, fh, cs) = writer_fs(&client, async_on, 64, 0, full);
            assert_eq!(full as u64 * cs, crate::writebuf::DIRTY_BYTES_CAP as u64);
            let w = stuck_write(&fs, fh, 40 * cs + 10, vec![2; 10]).await;
            assert!(fs.write_buffer.has_chunk("/f", 40), "async={async_on}: the write's seeded chunk was drained");
            assert_eq!(fs.write_buffer.dirty_bytes("/f"), cs as usize, "only chunk 40 left");
            assert_eq!(overlay_len(&fs, 0, cs as usize), cs as usize, "async={async_on}: drained chunk 0 invisible");
            w.abort();
            let _ = w.await;
            assert_eq!(overlay_len(&fs, 0, cs as usize), 0, "overlay ends with the commit");
        }
        hole.abort();
    }

    /// The hole-fill cap drain keeps the drained chunks visible in flight.
    #[tokio::test]
    async fn hole_fill_cap_drain_stays_visible_in_flight() {
        let (client, hole) = black_hole_client().await;
        let full = (crate::writebuf::DIRTY_BYTES_CAP / (4 << 20)) as u32;
        for async_on in [false, true] {
            let (fs, fh, cs) = writer_fs(&client, async_on, full as u64, 100, full);
            let w = stuck_write(&fs, fh, (full as u64 + 2) * cs, vec![2; 10]).await;
            assert!(!fs.write_buffer.has_chunk("/f", 0), "chunk 0 drained by the hole-fill cap drain");
            assert_eq!(overlay_len(&fs, 0, cs as usize), cs as usize, "async={async_on}: drained chunk 0 invisible");
            w.abort();
            let _ = w.await;
        }
        hole.abort();
    }

    /// A write 200 MiB past the end buffers only the old tail chunk's zero
    /// padding, then pins the manager size (stuck on the black hole) before
    /// any upload: the chunks inside the hole are never materialised (prod:
    /// 213 MB hole fills drained gappy zero chunks on every write, x1.6).
    #[tokio::test]
    async fn sparse_write_pads_only_the_tail_chunk_and_pins_the_size() {
        let (client, hole) = black_hole_client().await;
        for async_on in [false, true] {
            let (fs, fh, cs) = writer_fs(&client, async_on, 0, 0, 0);
            fs.write_buffer.write("/f", 0, &[1; 100]);
            let w = stuck_write(&fs, fh, 200 << 20, vec![2; 10]).await;
            assert_eq!(fs.write_buffer.dirty_bytes("/f"), cs as usize, "async={async_on}: hole materialised");
            assert_eq!(overlay_len(&fs, 0, 8 * cs as usize), cs as usize, "nothing drained or in flight");
            w.abort();
            let _ = w.await;
        }
        hole.abort();
    }

    /// write()'s batch flush keeps the drained batch visible in flight
    /// (async_commit: `async_write_returns_while_the_batch_uploads`).
    #[tokio::test]
    async fn write_batch_flush_stays_visible_in_flight() {
        let (client, hole) = black_hole_client().await;
        for async_on in [false] {
            let (fs, fh, cs) = writer_fs(&client, async_on, 0, 0, 0);
            let w = stuck_write(&fs, fh, 0, vec![3; 4 * cs as usize]).await;
            assert_eq!(fs.write_buffer.dirty_bytes("/f"), 0, "batch drained");
            assert_eq!(fs.write_buffer.visible_size("/f"), Some(4 * cs), "async={async_on}: drained batch invisible");
            assert_eq!(overlay_len(&fs, 0, 4 * cs as usize), 4 * cs as usize);
            w.abort();
            let _ = w.await;
        }
        hole.abort();
    }

    /// Async fs on a black-hole manager with write()'s 4-chunk batch of `/f`
    /// uploading in the background (stuck): write() must have returned.
    async fn bg_batch_in_flight(client: &DtfsClient) -> (Arc<DtfsFilesystem>, u64, u64) {
        let (fs, fh, cs) = writer_fs(client, true, 0, 0, 0);
        let w = tokio::time::timeout(Duration::from_secs(2), fs.write(fh, 0, &vec![3; 4 * cs as usize])).await;
        assert_eq!(w.expect("write must return before the upload finishes").unwrap(), 4 * cs as u32);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(fs.bg_uploads.contains_key("/f"), "batch uploads in the background");
        (fs, fh, cs)
    }

    /// The handed-off batch is visible the moment write() returns, before the
    /// upload task ever runs: else the next write's EOF (`write_eof` =
    /// visible size) is too low and its hole fill pads zeros into chunks
    /// that are still uploading (and a reader sees the file short).
    #[tokio::test]
    async fn async_batch_is_visible_before_the_upload_task_runs() {
        let (client, hole) = black_hole_client().await;
        let (fs, fh, cs) = writer_fs(&client, true, 0, 0, 0);
        assert_eq!(fs.write(fh, 0, &vec![3; 4 * cs as usize]).await.unwrap(), 4 * cs as u32);
        // No await since write() returned: the spawned task has not run.
        assert!(fs.bg_uploads.contains_key("/f"), "batch handed off");
        assert_eq!(fs.write_buffer.dirty_bytes("/f"), 0, "batch drained");
        assert_eq!(fs.write_buffer.visible_size("/f"), Some(4 * cs), "drained batch invisible");
        assert_eq!(overlay_len(&fs, 0, 4 * cs as usize), 4 * cs as usize);
        hole.abort();
    }

    /// `op` must still be pending after 300 ms (it waits for the upload).
    async fn pending<T>(op: impl std::future::Future<Output = T>) -> bool {
        tokio::time::timeout(Duration::from_millis(300), op).await.is_err()
    }

    /// Prod (NFS re-export, one knfsd thread): write() awaiting each batch
    /// upload capped a 70 GB copy at 9-20 MiB/s. async_commit: write() hands
    /// the batch off and returns; the bytes stay visible in flight.
    #[tokio::test]
    async fn async_write_returns_while_the_batch_uploads() {
        let (client, hole) = black_hole_client().await;
        let (fs, fh, cs) = bg_batch_in_flight(&client).await;
        assert_eq!(fs.write_buffer.dirty_bytes("/f"), 0, "batch drained");
        assert_eq!(fs.write_buffer.visible_size("/f"), Some(4 * cs));
        assert_eq!(overlay_len(&fs, 0, 4 * cs as usize), 4 * cs as usize);
        // Appending after the in-flight batch neither waits nor zero-fills it.
        let w = tokio::time::timeout(Duration::from_secs(2), fs.write(fh, 4 * cs, &[5; 10])).await;
        assert_eq!(w.expect("a write above the in-flight batch must not wait").unwrap(), 10);
        assert_eq!(fs.write_buffer.dirty_bytes("/f"), 10, "no hole fill over the in-flight chunks");
        hole.abort();
    }

    /// The last writer's close (flush/release) and a write into an in-flight
    /// chunk wait for the background upload; fsync does not (prod: knfsd's
    /// per-COMMIT fsync waited on every batch, 29 -> 20 MiB/s). (truncate,
    /// rename, unlink and the timer issue RPCs the black hole also stalls,
    /// so this harness cannot tell their wait apart; same `wait_bg_uploads`.)
    #[tokio::test]
    async fn async_close_and_overlapping_write_wait_for_the_upload_fsync_does_not() {
        let (client, hole) = black_hole_client().await;
        let (fs, fh, cs) = bg_batch_in_flight(&client).await;
        assert!(!pending(fs.fsync(fh)).await, "async fsync must not wait for the upload");
        assert!(fs.bg_uploads.contains_key("/f"), "upload still in flight");
        assert!(pending(fs.flush(fh)).await, "last writer's flush must wait");
        assert!(pending(fs.release(fh)).await, "last writer's release must wait");
        assert!(pending(fs.write(fh, 10, &[1; 10])).await, "write into in-flight chunk 0 must wait");
        assert!(pending(fs.write(fh, 3 * cs + 1, &[1; 10])).await, "chunk 3 (last in flight) too");
        assert_eq!(fs.write_buffer.dirty_bytes("/f"), 0, "nothing buffered over in-flight chunks");
        // A non-last writer's close commits nothing and does not wait.
        // (fh's flush above set its `close_seen`; fh3 stays open.)
        let fh2 = open(&fs, "/f", OpenFlags::ReadWrite);
        let _fh3 = open(&fs, "/f", OpenFlags::ReadWrite);
        assert!(!pending(fs.flush(fh2)).await, "deferred close must not wait");
        hole.abort();
    }

    /// Writer `fh` on `/f` holding lease "L1" of owner "h:1" (black hole: any
    /// manager RPC hangs).
    async fn leased_writer(client: &DtfsClient) -> (Arc<DtfsFilesystem>, u64) {
        let (fs, fh, _) = writer_fs(client, true, 3, 5, 0);
        set_lease(&fs, fh, "h:1", "L1");
        fs.open_files.get_mut(&fh).unwrap().lease_deadline = Some(Instant::now() + AGE);
        (fs, fh)
    }

    /// knfsd opens a struct file per NFS COMMIT (prod: 13/20 knfsd stack
    /// samples in fuse_send_open, 29 -> 20 MiB/s as the file grew): a second
    /// writable open of a leased path sends no RPC (no O(file) fetch, no
    /// Raft lease write) and shares the lease and the snapshot Arc.
    #[tokio::test]
    async fn writable_reopen_of_a_leased_path_reuses_lease_and_snapshot() {
        let (client, hole) = black_hole_client().await;
        let (fs, fh) = leased_writer(&client).await;
        let open2 = tokio::time::timeout(Duration::from_secs(2), fs.open_with_append("/f", OpenFlags::WriteOnly, false)).await;
        let fh2 = open2.expect("reopen must not issue a manager RPC").unwrap();
        let (a, b) = (fs.open_files.get(&fh).unwrap(), fs.open_files.get(&fh2).unwrap());
        assert_eq!(b.lease_id.as_deref(), Some("L1"));
        assert_eq!(b.lease_owner_id.as_deref(), Some("h:1"));
        assert_eq!(b.lease_deadline, a.lease_deadline);
        assert!(Arc::ptr_eq(a.locations.as_ref().unwrap(), b.locations.as_ref().unwrap()), "snapshot not shared");
        drop((a, b));
        assert!(fs.metadata_cache.get("/f").is_none(), "a reused snapshot is not a fetch: not cached");
        // Not the last handle of the owner: no commit, no lease release RPC.
        let r = tokio::time::timeout(Duration::from_secs(2), fs.release(fh2)).await;
        r.expect("release of a shared-owner handle must not call the manager").unwrap();
        assert_eq!(lease_id(&fs, fh).as_deref(), Some("L1"));
        hole.abort();
    }

    /// R2-1: an O_APPEND reopen while a writer is open must not seed the tail
    /// chunk at open (no path_guard, no bg wait: the chunk may sit in an
    /// in-flight batch and the stale seed later truncates it). The tail
    /// chunk's replica is a black hole: a seed read would hang.
    #[tokio::test]
    async fn append_reopen_with_an_open_writer_does_not_seed_the_tail() {
        let (client, hole) = black_hole_client().await;
        let (fs, fh) = leased_writer(&client).await;
        let data = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", data.local_addr().unwrap());
        let cs = fs.write_buffer.chunk_size() as u64;
        let mut snap = holes("/f", 3 * cs + 5, cs);
        snap.chunks.push(committed_chunk_info(3, &"a".repeat(64), 5, vec![addr], "warm"));
        fs.open_files.get_mut(&fh).unwrap().locations = Some(Arc::new(snap));
        let open2 = tokio::time::timeout(Duration::from_secs(2), fs.open_with_append("/f", OpenFlags::WriteOnly, true)).await;
        open2.expect("append reopen must not read the tail chunk").unwrap();
        assert!(!fs.write_buffer.has_chunk("/f", 3), "tail seeded at open");
        drop(data);
        hole.abort();
    }

    /// R2-2: a commit in flight while the reopen copies the lender's snapshot
    /// may merge into the lender before the new fh is inserted (its token
    /// still alive, so the old `unchanged()` held): the copy must not be kept.
    #[tokio::test]
    async fn reopen_during_an_in_flight_commit_keeps_no_snapshot() {
        let (client, hole) = black_hole_client().await;
        let (fs, _fh) = leased_writer(&client).await;
        let commit = fs.commit_tracker.begin("/f");
        let open2 = tokio::time::timeout(Duration::from_secs(2), fs.open_with_append("/f", OpenFlags::WriteOnly, false)).await;
        let fh2 = open2.expect("reopen must not issue a manager RPC").unwrap();
        assert!(fs.open_files.get(&fh2).unwrap().locations.is_none(), "kept a snapshot copied under an in-flight commit");
        drop(commit);
        hole.abort();
    }

    /// The first handle's snapshot is stale (None): the reopen falls back to
    /// the fetch (hangs on the black hole). Readers always fetch.
    #[tokio::test]
    async fn reopen_falls_back_to_fetch_when_stale_and_readers_still_fetch() {
        let (client, hole) = black_hole_client().await;
        let (fs, fh) = leased_writer(&client).await;
        assert!(pending(fs.open_with_append("/f", OpenFlags::ReadOnly, false)).await, "reader must fetch");
        fs.invalidate_fh_locations(fh);
        assert!(pending(fs.open_with_append("/f", OpenFlags::WriteOnly, false)).await, "stale snapshot must fetch");
        hole.abort();
    }

    /// Backpressure: dirty + in-flight bytes count against DIRTY_BYTES_CAP.
    /// Just under the cap the write returns; meeting it waits for the upload.
    #[tokio::test]
    async fn async_write_at_the_cap_waits_for_the_in_flight_upload() {
        let (client, hole) = black_hole_client().await;
        let (fs, fh, cs) = bg_batch_in_flight(&client).await;
        let cap_chunks = (crate::writebuf::DIRTY_BYTES_CAP as u64 / cs) as u32;
        for i in 4..cap_chunks - 1 {
            fs.write_buffer.write("/f", i as u64 * cs, &vec![1; cs as usize]);
        }
        let last = (cap_chunks - 1) as u64 * cs;
        let w = tokio::time::timeout(Duration::from_secs(2), fs.write(fh, last, &vec![2; cs as usize - 1])).await;
        assert_eq!(w.expect("cap - 1: must not wait").unwrap(), cs as u32 - 1);
        assert!(pending(fs.write(fh, last + cs - 1, &[2; 1])).await, "dirty + in-flight at the cap must wait");
        hole.abort();
    }

    /// No managers (fail fast): the background failure restores the batch,
    /// arms every writer's sticky error (EIO once), and the last writer's
    /// close waits for it and returns an error instead of success.
    #[tokio::test]
    async fn async_failed_background_upload_restores_and_is_reported() {
        let fs = Arc::new(fs(true));
        let cs = fs.write_buffer.chunk_size() as u64;
        let fh = open(&fs, "/f", OpenFlags::ReadWrite);
        let fh2 = open(&fs, "/f", OpenFlags::WriteOnly);
        fs.open_files.get_mut(&fh).unwrap().locations = Some(Arc::new(holes("/f", 0, cs)));
        assert_eq!(fs.write(fh, 0, &vec![3; 4 * cs as usize]).await.unwrap(), 4 * cs as u32);
        fs.wait_bg_uploads("/f").await;
        assert_eq!(fs.write_buffer.dirty_bytes("/f"), 4 * cs as usize, "restored");
        let err = fs.fsync(fh).await.unwrap_err();
        assert!(format!("{err:#}").contains("async commit of /f failed"), "{err:#}");
        fs.fsync(fh).await.expect("reported once");
        assert!(fs.open_files.get(&fh2).unwrap().write_error.is_some(), "every writer armed");
        // Retried by the next write's flush; close waits for it and fails.
        fs.write(fh, 4 * cs, &[1; 10]).await.unwrap();
        let _ = fs.release(fh2).await; // not the last writer: commits nothing
        let err = fs.flush(fh).await.unwrap_err();
        assert!(format!("{err:#}").contains("allocate_chunks failed for /f"), "{err:#}");
        assert_eq!(fs.write_buffer.dirty_bytes("/f"), 4 * cs as usize + 10, "kept for retry");
    }

    /// async_commit off: write() still uploads its batch inline and returns
    /// the error (bytes restored, nothing in the background).
    #[tokio::test]
    async fn sync_write_still_uploads_the_batch_inline() {
        let fs = Arc::new(fs(false));
        let cs = fs.write_buffer.chunk_size() as u64;
        let fh = open(&fs, "/f", OpenFlags::ReadWrite);
        fs.open_files.get_mut(&fh).unwrap().locations = Some(Arc::new(holes("/f", 0, cs)));
        let err = fs.write(fh, 0, &vec![3; 4 * cs as usize]).await.unwrap_err();
        assert!(format!("{err:#}").contains("allocate_chunks failed for /f"), "{err:#}");
        assert!(fs.bg_uploads.is_empty());
        assert_eq!(fs.write_buffer.dirty_bytes("/f"), 4 * cs as usize);
    }

    #[test]
    fn write_range_hits_upload_bounds() {
        let cs = 4u64 << 20;
        let up = &[2, 3];
        assert!(!range_hits_upload(up, 0, (2 * cs) as usize, cs), "ends right below chunk 2");
        assert!(range_hits_upload(up, 0, (2 * cs) as usize + 1, cs));
        assert!(range_hits_upload(up, 4 * cs - 1, 1, cs), "last byte of chunk 3");
        assert!(!range_hits_upload(up, 4 * cs, 100, cs), "above the batch");
        assert!(!range_hits_upload(up, 2 * cs, 0, cs), "empty write");
        // Interleaved NFS streams: the batch is a sparse set, not a range.
        let sparse = &[2, 19, 36];
        assert!(!range_hits_upload(sparse, 3 * cs, (16 * cs) as usize, cs), "chunks 3..=18 between");
        assert!(range_hits_upload(sparse, 3 * cs, (16 * cs) as usize + 1, cs), "reaches chunk 19");
        assert!(range_hits_upload(sparse, 36 * cs, 1, cs), "first byte of chunk 36");
        assert!(!range_hits_upload(&[], 0, 100, cs), "no batch");
    }

    /// FUSE-3: a write into a chunk between two chunks of the in-flight
    /// batch must not wait for the upload; one into a batch chunk must.
    #[tokio::test]
    async fn async_write_between_batch_chunks_does_not_wait() {
        let fs = Arc::new(fs(true));
        let cs = fs.write_buffer.chunk_size() as u64;
        let fh = open(&fs, "/f", OpenFlags::ReadWrite);
        fs.open_files.get_mut(&fh).unwrap().locations = Some(Arc::new(holes("/f", 40 * cs, cs)));
        fs.bg_uploads.insert("/f".to_string(), (vec![2, 19], 2 * cs as usize));
        let w = tokio::time::timeout(Duration::from_secs(2), fs.write(fh, 10 * cs + 1, &[5; 10])).await;
        assert_eq!(w.expect("chunk 10 is not in the batch {2, 19}: must not wait").unwrap(), 10);
        assert!(pending(fs.write(fh, 19 * cs + 1, &[5; 10])).await, "chunk 19 is in the batch");
        assert!(pending(fs.write(fh, 19 * cs - 1, &[5; 2])).await, "straddles 18/19");
    }

    /// `/f` with a 4-chunk batch in flight (overlay only). The returned hook
    /// retires it like `bg_upload`/`commit_all` do: merge into every handle
    /// (size 4 chunks), then drop the overlay.
    fn retiring_batch(fs: &DtfsFilesystem, cs: u64) -> Box<dyn FnOnce(&DtfsFilesystem) + Send> {
        fs.write_buffer.write("/f", 0, &vec![3; 4 * cs as usize]);
        let segs = fs.write_buffer.flush_all("/f");
        let id = fs.write_buffer.begin_inflight("/f", &segs).into_id();
        Box::new(move |fs: &DtfsFilesystem| {
            for mut f in fs.open_files.iter_mut() {
                if f.path == "/f" {
                    f.locations = Some(Arc::new(holes("/f", 4 * cs, cs)));
                }
            }
            drop(fs.write_buffer.adopt_inflight(id));
        })
    }

    /// FUSE-1: a batch commits and retires between write()'s snapshot read
    /// and its EOF uses (renew/refetch/seed awaits). Its bytes are then in
    /// none of buffer, overlay or the stale clone: O_APPEND must still land
    /// at its end and the sparse-write size pin must not drop it.
    #[tokio::test]
    async fn write_eof_counts_a_batch_that_retired_after_the_snapshot_read() {
        for async_on in [true, false] {
            let fs = Arc::new(fs(async_on));
            let cs = fs.write_buffer.chunk_size() as u64;
            for append in [false, true] {
                let fh = open(&fs, "/f", OpenFlags::ReadWrite);
                {
                    let mut f = fs.open_files.get_mut(&fh).unwrap();
                    f.append = append;
                    f.locations = Some(Arc::new(holes("/f", 0, cs)));
                }
                *fs.write_race_hook.lock().unwrap() = Some(retiring_batch(&fs, cs));
                if append {
                    assert_eq!(fs.write(fh, 0, &[5; 10]).await.unwrap(), 10);
                    assert!(!fs.write_buffer.has_chunk("/f", 0), "async={async_on}: O_APPEND landed over the batch");
                    assert!(fs.write_buffer.has_chunk("/f", 4), "async={async_on}: append not at the batch end");
                } else {
                    let err = fs.write(fh, 10 * cs + 5, &[5; 10]).await.unwrap_err();
                    let want = format!("set_size {} failed", 4 * cs);
                    assert!(format!("{err:#}").contains(&want), "async={async_on}: pin below the batch: {err:#}");
                }
                fs.write_buffer.remove("/f");
                fs.open_files.remove(&fh);
            }
        }
    }

    /// FUSE-2: fallocate's size floor counts in-flight bytes, also when the
    /// batch retires during the manager fetch; else the manager size wins.
    #[tokio::test]
    async fn fallocate_size_floor_counts_in_flight_bytes() {
        let fs = fs(true);
        let cs = fs.write_buffer.chunk_size() as u64;
        let _fh = open(&fs, "/f", OpenFlags::ReadWrite);
        let hook = retiring_batch(&fs, cs);
        let floor = fs.size_floor("/f", async { Ok::<u64, anyhow::Error>(0) }).await.unwrap();
        assert_eq!(floor, 4 * cs, "in-flight batch not counted");
        let fsr = &fs;
        let floor = fs.size_floor("/f", async move { hook(fsr); Ok::<u64, anyhow::Error>(0) }).await.unwrap();
        assert_eq!(floor, 4 * cs, "batch retired during the fetch");
        let _g = open(&fs, "/g", OpenFlags::ReadWrite);
        assert_eq!(fs.size_floor("/g", async { Ok::<u64, anyhow::Error>(100) }).await.unwrap(), 100);
    }

    /// The config bound must stay at half the write lease (renewed only by
    /// write() and commits, not by the timer's wait).
    #[test]
    fn async_commit_secs_bound_is_half_the_write_lease() {
        let mut w = dtfs_common::config::FuseWriteConfig::default();
        w.async_commit = true;
        w.async_commit_secs = WRITE_LEASE_MS / 2000;
        assert!(w.validate().is_ok());
        w.async_commit_secs += 1;
        assert!(w.validate().is_err());
    }

    #[test]
    fn rename_lock_paths_sorted_unique_writers_only() {
        let w = OpenFlags::ReadWrite;
        let open = vec![
            ("/e/z".to_string(), w),
            ("/d/y".to_string(), w),
            ("/d/x".to_string(), w),
            ("/d/x".to_string(), w),
            ("/d/r".to_string(), OpenFlags::ReadOnly),
            ("/dd".to_string(), w),
            ("/e".to_string(), OpenFlags::ReadOnly),
        ];
        assert_eq!(rename_lock_paths(&open, "/d", "/e"), ["/d", "/d/x", "/d/y", "/e/z"]);
        assert_eq!(rename_lock_paths(&[], "/a", "/b"), ["/a"]);
    }

    #[tokio::test]
    async fn async_rename_does_not_wait_on_unrelated_or_reader_only_paths() {
        let fs = fs(true);
        open(&fs, "/c", OpenFlags::ReadWrite);
        open(&fs, "/b", OpenFlags::ReadOnly);
        open(&fs, "/ab", OpenFlags::ReadWrite);
        let gc = fs.path_guard("/c").await;
        let gb = fs.path_guard("/b").await;
        let gab = fs.path_guard("/ab").await;
        let r = tokio::time::timeout(Duration::from_secs(5), fs.rename("/a", "/b")).await;
        assert!(r.expect("must not wait").is_err(), "no managers: the rename RPC fails");
        fs.drop_path_guard("/c", gc);
        fs.drop_path_guard("/b", gb);
        fs.drop_path_guard("/ab", gab);
    }

    /// A later timer retry that succeeds clears the sticky error: the bytes
    /// it reported are committed now.
    #[tokio::test]
    async fn successful_timer_retry_clears_the_sticky_error() {
        let fs = fs(true);
        let a = open(&fs, "/f", OpenFlags::ReadWrite);
        let b = open(&fs, "/f", OpenFlags::WriteOnly);
        fs.record_aged_commit("/f", Err(anyhow::anyhow!("put failed")));
        assert!(fs.open_files.get(&a).unwrap().write_error.is_some());
        fs.record_aged_commit("/f", Ok(()));
        assert!(fs.open_files.get(&a).unwrap().write_error.is_none(), "retry succeeded");
        assert!(fs.open_files.get(&b).unwrap().write_error.is_none());
        fs.fsync(a).await.expect("nothing pending after a successful retry");
    }

    #[derive(Clone, Default)]
    struct Logs(Arc<std::sync::Mutex<Vec<u8>>>);
    impl std::io::Write for Logs {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Ghost unlink of a never-committed file follows release's own commit
    /// result only, not a stale background error the commit already fixed.
    #[tokio::test]
    async fn release_ghost_unlink_ignores_a_pending_background_error() {
        let logs = Logs::default();
        let w = logs.clone();
        let sub = tracing_subscriber::fmt().with_writer(move || w.clone()).with_ansi(false).finish();
        let _g = tracing::subscriber::set_default(sub);
        let ghost = "unlinking ghost entry";
        for (buffered, pending, expect_unlink) in [(false, true, false), (true, false, true)] {
            logs.0.lock().unwrap().clear();
            let fs = fs(true);
            let fh = open(&fs, "/g", OpenFlags::ReadWrite);
            fs.open_files.get_mut(&fh).unwrap().committed_any = false;
            if pending {
                fs.open_files.get_mut(&fh).unwrap().write_error = Some("old timer failure".into());
            }
            if buffered {
                fs.write_buffer.write("/g", 0, b"new");
            }
            // A pending error the successful commit superseded is cleared (FUSE-4).
            assert_eq!(fs.release(fh).await.is_err(), buffered, "only the failed commit is reported");
            let out = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
            assert_eq!(out.contains(ghost), expect_unlink, "buffered={buffered}: {out}");
        }
    }

    #[tokio::test]
    async fn async_release_still_commits_the_partial_tail() {
        let fs = fs(true);
        let fh = open(&fs, "/f", OpenFlags::ReadWrite);
        fs.write_buffer.write("/f", 0, &[7; 100]);
        let err = fs.release(fh).await.unwrap_err();
        assert!(format!("{err:#}").contains("allocate_chunks failed for /f"), "{err:#}");
        assert!(!fs.open_files.contains_key(&fh));
    }

    /// Dev-cluster repro: knfsd's close drained the buffer (flush_all) and a
    /// reader that opened the file before the upload finished saw size 0 /
    /// EOF. A manager that accepts but never answers keeps the commit in
    /// flight; the drained bytes must stay visible to lookup() and read().
    #[tokio::test]
    async fn drained_bytes_stay_visible_while_the_commit_is_in_flight() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let hole = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((s, _)) = l.accept().await {
                held.push(s);
            }
        });
        let client = DtfsClient::new();
        client.connect(&addr).await.expect("black-hole manager accepts");
        for async_on in [false, true] {
            let mut cfg: FuseConfig =
                toml::from_str(include_str!("../../../deploy/examples/fuse.toml")).unwrap();
            cfg.write.async_commit = async_on;
            let fs = Arc::new(DtfsFilesystem::new(client.clone(), cfg.location.clone(), cfg));
            let cs = fs.write_buffer.chunk_size() as u64;
            let w = open(&fs, "/v", OpenFlags::ReadWrite);
            let r = open(&fs, "/v", OpenFlags::ReadOnly);
            for fh in [w, r] {
                fs.open_files.get_mut(&fh).unwrap().locations = Some(Arc::new(holes("/v", 0, cs)));
            }
            fs.cache_metadata("/v", Arc::new(holes("/v", 0, cs)));
            fs.write_buffer.write("/v", 0, b"visible");
            let f2 = fs.clone();
            let commit = tokio::spawn(async move { f2.flush(w).await });
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(!commit.is_finished(), "the commit must still be in flight");
            assert_eq!(fs.write_buffer.dirty_bytes("/v"), 0, "drained by flush_all");
            assert_eq!(fs.lookup("/v").await.unwrap().size, 7, "async={async_on}");
            assert_eq!(&fs.read(r, 0, 100).await.unwrap()[..], b"visible", "async={async_on}");
            commit.abort();
            let _ = commit.await;
            assert_eq!(fs.lookup("/v").await.unwrap().size, 0, "overlay ends with the commit");
        }
        hole.abort();
    }
}
