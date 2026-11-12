// Configurable read-ahead engine — Phase 2 (FUSE Client).
//
// Prefetches upcoming chunks in the background based on detected access
// patterns (sequential file reads, folder-sequential reads, etc.). Cached
// data is served from memory on subsequent reads, eliminating round-trips
// to data nodes.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{oneshot, watch, Semaphore};
use tokio::task::JoinHandle;
use tracing::{debug, instrument, warn};

use dtfs_client::client::DtfsClient;

/// Read-ahead mode determines the prefetch strategy.
#[derive(Debug, Clone)]
pub enum ReadAheadMode {
    /// Prefetch the next N chunks within the same file.
    FileSequential {
        prefetch_chunks: u32,
    },
    /// Config name `folder-sequential`. Cross-file prefetch is NOT
    /// implemented (`readahead.prefetch_next_files` is parsed and ignored):
    /// behaves like `FileSequential`.
    FolderSequential {
        prefetch_chunks: u32,
    },
    /// Prefetch only within the current file, no cross-file prefetching.
    FileOnly {
        prefetch_chunks: u32,
    },
    /// Read-ahead disabled.
    Disabled,
}

impl ReadAheadMode {
    /// Parse a mode string from configuration into a `ReadAheadMode`.
    pub fn from_config(mode: &str, prefetch_chunks: u32) -> Self {
        match mode {
            "file-sequential" => ReadAheadMode::FileSequential { prefetch_chunks },
            "folder-sequential" => ReadAheadMode::FolderSequential { prefetch_chunks },
            "file-only" => ReadAheadMode::FileOnly { prefetch_chunks },
            "disabled" => ReadAheadMode::Disabled,
            other => {
                warn!(mode = other, "unknown readahead mode, defaulting to folder-sequential");
                ReadAheadMode::FolderSequential { prefetch_chunks }
            }
        }
    }

    /// Number of chunks to prefetch ahead of the current read position.
    fn prefetch_chunks(&self) -> u32 {
        match self {
            ReadAheadMode::FileSequential { prefetch_chunks } => *prefetch_chunks,
            ReadAheadMode::FolderSequential { prefetch_chunks, .. } => *prefetch_chunks,
            ReadAheadMode::FileOnly { prefetch_chunks } => *prefetch_chunks,
            ReadAheadMode::Disabled => 0,
        }
    }
}

/// REV-2-3 — pick what to prefetch for chunk index `idx`.
///
/// Two things used to be wrong here. The list was indexed POSITIONALLY, but
/// the manager's array is dense while a file's chunk index space may be sparse
/// (see REV-2-1), so a prefetch could fetch a completely different chunk and
/// cache it under this index. And the address was always `node_addresses[0]`,
/// the manager's arbitrary first replica, ignoring every locality rule the
/// foreground read applies — which both wasted a WAN link and cached the chunk
/// under a key the foreground read never looks up.
///
/// The caller now supplies `(chunk_index, chunk_id, candidates)` where
/// `candidates` is exactly `read_candidates`' output, so the first entry is
/// the same replica the foreground read would pick.
fn prefetch_target(
    chunk_locations: &[(u64, String, Vec<String>)],
    idx: u64,
) -> Option<(&String, &String)> {
    let (_, chunk_id, addrs) = chunk_locations.iter().find(|(i, _, _)| *i == idx)?;
    let addr = addrs.first()?;
    Some((chunk_id, addr))
}

/// WI-05 / B2 — hard cap on how long a foreground read waits for an
/// already-on-the-wire prefetch of the same chunk. Deliberately a small
/// constant and NOT `client.chunk_read_timeout_secs` (default 60s): the
/// join is a pure optimisation (avoid a duplicate `GetChunk` over the same
/// link), so it must never eat into the P0-2 replica-fallover budget
/// release case 69 asserts (`ELAPSED <= TMO*3`, `TMO=12`). Worst case it
/// adds 2s once per chunk in front of the unchanged candidate loop.
pub const PREFETCH_JOIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Key for cached chunk data: "node_addr|chunk_id".
fn cache_key(node_addr: &str, chunk_id: &str) -> String {
    format!("{node_addr}|{chunk_id}")
}

/// WI-05 — a prefetch task's join handle plus a "done" signal. The signal
/// lets a foreground read racing an in-flight prefetch for the same chunk
/// wait for it (`ReadAheadEngine::join_pending`) instead of issuing a
/// second `GetChunk` for the same bytes over the same link. `done` starts
/// `false` and the task sets it `true` on EITHER success or failure, just
/// before removing its own `pending` entry.
struct PendingPrefetch {
    handle: JoinHandle<()>,
    done: watch::Receiver<bool>,
    /// WI-05 / B1 — set true by the prefetch task the instant it ACQUIRES
    /// its `prefetch_slots` permit, i.e. once the fetch is genuinely on the
    /// wire. A queued task (parked behind the `prev_acquired` chain, or
    /// behind the semaphore) stays false. `join_pending` refuses to wait on
    /// a false entry, so a foreground read never queues behind speculative
    /// I/O, and a test can read the flag to assert both the in-flight cap
    /// and the near-first ordering without any unbounded production
    /// bookkeeping.
    started: Arc<AtomicBool>,
    /// #151: the file whose read spawned it (queued-window cancel scope).
    file: String,
}

/// Background read-ahead engine.
///
/// When a chunk is read, the engine inspects the access pattern and
/// speculatively fetches upcoming chunks in background tokio tasks. Fetched
/// data is stored in an in-memory `DashMap` cache and can be retrieved
/// with [`get_cached`](Self::get_cached).
pub struct ReadAheadEngine {
    mode: ReadAheadMode,
    client: DtfsClient,
    /// FUSE-2 — per-fetch bound for a background prefetch (`None` =
    /// unbounded, the operator's `chunk_read_timeout_secs = 0`). Pre-fix
    /// the prefetch `read_chunk` had no deadline at all, so two hung data
    /// nodes pinned both `prefetch_max_inflight` permits for the life of
    /// the mount and readahead was silently off from then on.
    fetch_timeout: Option<std::time::Duration>,
    /// Currently pending prefetch tasks keyed by cache key.
    pending: Arc<DashMap<String, PendingPrefetch>>,
    /// Cached prefetched chunk data keyed by cache key.
    cache: Arc<DashMap<String, (u64, Bytes)>>,
    /// Bug CE — running total of bytes held in `cache`, kept in sync with
    /// every insert/evict so the cache is bounded by SIZE, not entry count.
    /// A whole chunk can be up to 128 MB; the old 256-entry count bound x
    /// whole-chunk values OOM-killed the 1 GB FUSE client on a full-file read.
    cache_bytes: Arc<std::sync::atomic::AtomicUsize>,
    /// Soft byte budget for `cache`. Inserts evict arbitrary entries until
    /// the total fits (best-effort; never rejects a read — § A.5).
    ///
    /// CF-5 — runtime-adjustable (was a fixed `usize`) so the memory-pressure
    /// ladder can halve it at the Warn level and restore it on recovery. An
    /// `Arc<AtomicUsize>` so the cheap `ReadAheadCacheController` handle can
    /// shrink it without owning the engine (the engine is moved into the
    /// fuse adapter at mount time).
    max_cache_bytes: Arc<AtomicUsize>,
    /// P0-11 / "next episode survives" — when set, prefetched chunks are
    /// also mirrored into the persistent disk cache so they survive
    /// in-memory eviction, FUSE restart, and scanner storms. Setting an
    /// initial atime on the disk-cache entry keeps the next-episode
    /// chunks near the top of the size-LRU.
    local_cache: parking_lot::RwLock<Option<Arc<crate::local_cache::LocalChunkCache>>>,
    /// WI-05 — bounds how many prefetch `GetChunk` fetches are on the wire
    /// at once (`readahead.prefetch_max_inflight`). The prefetch WINDOW
    /// (`prefetch_chunks`) stays deep; this only bounds concurrency.
    prefetch_slots: Arc<Semaphore>,
    /// CF-5 / H2 — pressure-ladder gate on spawning NEW prefetch. Pinned
    /// false at Shed (`ReadAheadCacheController::set_prefetch_enabled`) so
    /// the `drop_unconsumed()` rung is not immediately undone by the next
    /// `on_chunk_read`. Replaces the deleted `cache_bytes() >=
    /// max_cache_bytes` gate, which observed only STORED bytes: do NOT
    /// re-add a byte gate here (A.3.2 — in-flight bytes are bounded by
    /// `prefetch_slots`).
    prefetch_enabled: Arc<AtomicBool>,
    /// #151: the last `RECENT_PREFETCH_FILES` distinct files read, newest
    /// first; queued prefetches of other files are dropped.
    recent_files: parking_lot::Mutex<std::collections::VecDeque<String>>,
}

/// Whether chunk `idx` is inside the window `on_chunk_read` prefetches after
/// reading `chunk_index`: `chunk_index+1 ..= chunk_index+n`. Test-only mirror of the
/// `start..end` loop in `on_chunk_read`.
#[cfg(test)]
pub(crate) fn in_prefetch_window(idx: u64, chunk_index: u32, n: u32) -> bool {
    idx > chunk_index as u64 && idx <= chunk_index as u64 + n as u64
}

/// #151: files whose queued prefetch windows survive another file's read
/// (concurrent streams); a scan's older files beyond this lose theirs.
pub(crate) const RECENT_PREFETCH_FILES: usize = 8;

/// Monotonic insertion stamp for the prefetch cache (oldest-first eviction).
static CACHE_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Remove the oldest-inserted entry other than `keep`, subtracting its
/// bytes. Returns false when there is nothing evictable.
fn evict_oldest(
    cache: &DashMap<String, (u64, Bytes)>,
    cache_bytes: &AtomicUsize,
    keep: Option<&str>,
) -> bool {
    let victim = cache
        .iter()
        .filter(|e| Some(e.key().as_str()) != keep)
        .min_by_key(|e| e.value().0)
        .map(|e| e.key().clone());
    match victim {
        Some(k) => {
            if let Some((_, (_, v))) = cache.remove(&k) {
                cache_bytes.fetch_sub(v.len(), Ordering::Relaxed);
            }
            true
        }
        None => false,
    }
}

/// Empty the cache entry by entry, subtracting each removed length, so a
/// concurrent insert/evict keeps `cache_bytes` consistent (a bulk
/// `store(0)` raced a `fetch_sub` and wrapped the counter).
fn drain_cache(cache: &DashMap<String, (u64, Bytes)>, cache_bytes: &AtomicUsize) {
    cache.retain(|_, (_, v)| {
        cache_bytes.fetch_sub(v.len(), Ordering::Relaxed);
        false
    });
}

/// FUSE-2 — `0` keeps the documented "unbounded" meaning of
/// `client.chunk_read_timeout_secs`; anything else is the prefetch bound.
pub(crate) fn prefetch_timeout(secs: u64) -> Option<std::time::Duration> {
    (secs > 0).then(|| std::time::Duration::from_secs(secs))
}

#[cfg(test)]
mod fuse_2_prefetch_timeout_tests {
    use super::prefetch_timeout;

    #[test]
    fn zero_means_unbounded() {
        assert_eq!(prefetch_timeout(0), None);
    }

    #[test]
    fn positive_is_the_bound() {
        assert_eq!(prefetch_timeout(7), Some(std::time::Duration::from_secs(7)));
    }
}

impl ReadAheadEngine {
    /// Create a new read-ahead engine. `max_cache_bytes` is the soft byte
    /// budget for the in-memory prefetch cache (Bug CE). `max_inflight` is
    /// the WI-05 cap on concurrent prefetch fetches (`.max(1)`, so `0`
    /// degrades to serial prefetch rather than "unbounded").
    pub fn new(
        mode: ReadAheadMode,
        client: DtfsClient,
        fetch_timeout_secs: u64,
        max_cache_bytes: usize,
        max_inflight: usize,
    ) -> Self {
        Self {
            mode,
            client,
            fetch_timeout: prefetch_timeout(fetch_timeout_secs),
            pending: Arc::new(DashMap::new()),
            cache: Arc::new(DashMap::new()),
            cache_bytes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            max_cache_bytes: Arc::new(AtomicUsize::new(max_cache_bytes)),
            local_cache: parking_lot::RwLock::new(None),
            prefetch_slots: Arc::new(Semaphore::new(max_inflight.max(1))),
            prefetch_enabled: Arc::new(AtomicBool::new(true)),
            recent_files: parking_lot::Mutex::new(std::collections::VecDeque::new()),
        }
    }

    /// CF-5 — a cheap clonable handle that lets a background task drive the
    /// memory-pressure ladder (shrink the byte budget, drop unconsumed
    /// prefetch) without owning the engine, which is moved into the fuse
    /// adapter at mount time. Captures the Arcs the engine already shares
    /// internally plus the operator-configured budget so Normal can restore
    /// the full size.
    pub fn cache_controller(&self) -> ReadAheadCacheController {
        ReadAheadCacheController {
            cache: Arc::clone(&self.cache),
            cache_bytes: Arc::clone(&self.cache_bytes),
            pending: Arc::clone(&self.pending),
            max_cache_bytes: Arc::clone(&self.max_cache_bytes),
            configured_budget: self.max_cache_bytes.load(Ordering::Relaxed),
            prefetch_enabled: Arc::clone(&self.prefetch_enabled),
        }
    }

    /// Bug CE — insert into the byte-bounded cache, evicting arbitrary other
    /// entries until the running byte total fits `max_cache_bytes`. Associated
    /// fn (not `&self`) so the prefetch task can call it with cloned `Arc`s.
    /// Keeps at least the just-stored entry; best-effort, never rejects.
    fn store_in_cache(
        cache: &DashMap<String, (u64, Bytes)>,
        cache_bytes: &std::sync::atomic::AtomicUsize,
        max_cache_bytes: usize,
        key: String,
        data: Bytes,
    ) {
        use std::sync::atomic::Ordering;
        let incoming = data.len();
        // Add before insert so a concurrent drain that removes the entry
        // and subtracts its length never underflows the counter.
        cache_bytes.fetch_add(incoming, Ordering::Relaxed);
        let seq = CACHE_SEQ.fetch_add(1, Ordering::Relaxed);
        if let Some((_, old)) = cache.insert(key.clone(), (seq, data)) {
            cache_bytes.fetch_sub(old.len(), Ordering::Relaxed);
        }
        // Evict oldest-inserted first until under budget, never dropping
        // the entry just stored.
        while cache_bytes.load(Ordering::Relaxed) > max_cache_bytes {
            if !evict_oldest(cache, cache_bytes, Some(&key)) {
                break; // only the fresh entry remains
            }
        }
    }

    /// Number of chunks after the one just read that `on_chunk_read` may prefetch.
    pub fn prefetch_window(&self) -> u32 {
        self.mode.prefetch_chunks()
    }

    /// Bytes currently held in the prefetch cache (Bug CE accounting).
    #[cfg(test)]
    pub fn cache_bytes(&self) -> usize {
        self.cache_bytes.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// CR2-8-3 (FUSE-11) — whether readahead is actively prefetching for
    /// this mount: the configured mode isn't `Disabled` AND the memory
    /// ladder hasn't pinned prefetch off at Shed. Used as a viewer-read
    /// signal for the disk cache's atime hint (a scanner's stat+header
    /// access never drives sequential prefetch).
    pub fn is_active(&self) -> bool {
        !matches!(self.mode, ReadAheadMode::Disabled)
            && self.prefetch_enabled.load(Ordering::Relaxed)
    }

    /// Attach a disk-backed cache. Prefetched chunks will be mirrored to
    /// it after a successful fetch.
    pub fn set_local_cache(&self, cache: Arc<crate::local_cache::LocalChunkCache>) {
        *self.local_cache.write() = Some(cache);
    }

    /// Called when a chunk is read. Triggers background prefetch of upcoming
    /// chunks based on the configured read-ahead mode.
    ///
    /// # Parameters
    /// - `file_path`: path of the file being read
    /// - `chunk_index`: zero-based index of the chunk just read
    /// - `total_chunks`: total number of chunks in the file
    /// - `chunk_locations`: `(chunk_index, chunk_id, node_addresses)` for the
    ///   chunks of this file. REV-2-3 — `node_addresses` arrives already
    ///   ordered by the caller's `read_candidates`, i.e. by the same U4 rules
    ///   the foreground read applies (`read_location_priority`, then physical
    ///   distance, then the tier tie-break), so taking the first entry here
    ///   prefetches from the same replica the foreground read would choose and
    ///   caches it under the key that read looks up. It used to be a bare
    ///   positional `(chunk_id, addrs)` list from which this function took
    ///   `addrs[0]`. The explicit index also removes the positional aliasing
    ///   REV-2-1 fixed on the foreground path: a sparse file's
    ///   `chunk_locations[i]` is not chunk `i`.
    pub fn on_chunk_read(
        &self,
        file_path: &str,
        chunk_index: u32,
        total_chunks: u32,
        chunk_locations: &[(u64, String, Vec<String>)],
    ) {
        self.on_chunk_read_as(file_path, chunk_index, total_chunks, chunk_locations, true);
    }

    /// `on_chunk_read`; `track = false` makes the prefetches it spawns fetch
    /// with no requester DC (an untracked fh's readahead).
    #[instrument(skip(self, chunk_locations), fields(path = %file_path, chunk = chunk_index, total = total_chunks))]
    pub fn on_chunk_read_as(
        &self,
        file_path: &str,
        chunk_index: u32,
        total_chunks: u32,
        chunk_locations: &[(u64, String, Vec<String>)],
        track: bool,
    ) {
        let n = self.mode.prefetch_chunks();
        if n == 0 {
            return;
        }
        // CF-5 / H2 — the memory-pressure ladder pins this false at Shed, so
        // the `drop_unconsumed()` rung is not immediately undone by the next
        // read. This replaces the deleted `cache_bytes() >= max_cache_bytes`
        // gate, which observed only STORED bytes: do NOT re-add a byte gate
        // here (A.3.2 — in-flight bytes are bounded by `prefetch_slots`).
        if !self.prefetch_enabled.load(Ordering::Relaxed) {
            return;
        }

        // Prefetch the next `n` chunks after the one just read.
        let start = chunk_index + 1;
        let end = total_chunks.min(start + n);

        // WI-05 — chain each newly spawned prefetch off the previous one's
        // "acquired a permit" signal, so the ascending (nearest-first) order
        // of this loop is binding on the wire, not just on scheduling. A
        // bare FIFO semaphore only orders whichever task calls `acquire`
        // first, which is not guaranteed to match spawn order once several
        // tasks are polled in the same pass.
        // #151: prefetches still queued (no slot yet) of this file belong to
        // a position that moved on (a seek), and those of files outside the
        // last RECENT_PREFETCH_FILES read to a scan that moved on; drop
        // them. Other recent files are concurrent streams: keep their
        // windows. Started ones finish. This window is re-spawned below.
        let recent = {
            let mut r = self.recent_files.lock();
            if let Some(i) = r.iter().position(|f| f == file_path) {
                r.remove(i);
            }
            r.push_front(file_path.to_string());
            r.truncate(RECENT_PREFETCH_FILES);
            r.clone()
        };
        self.pending.retain(|_, p| {
            let keep = p.started.load(Ordering::Relaxed)
                || (p.file != file_path && recent.contains(&p.file));
            if !keep {
                p.handle.abort();
            }
            keep
        });
        let mut prev: Option<oneshot::Receiver<()>> = None;
        for idx in start..end {
            if let Some((chunk_id, addr)) = prefetch_target(chunk_locations, idx as u64) {
                let key = cache_key(addr, chunk_id);

                // Skip if already cached or being fetched.
                if self.cache.contains_key(&key) || self.pending.contains_key(&key) {
                    continue;
                }

                prev = Some(self.spawn_prefetch(
                    key,
                    addr.clone(),
                    chunk_id.clone(),
                    file_path,
                    prev.take(),
                    track,
                ));
            }
        }

        debug!(
            file_path,
            chunk_index,
            prefetch_start = start,
            prefetch_end = end,
            "readahead triggered"
        );
    }

    /// Get a previously prefetched chunk from the cache.
    ///
    /// Returns `Some(data)` if the chunk was prefetched and is available,
    /// or `None` if it has not been fetched yet.
    pub fn get_cached(&self, node_addr: &str, chunk_id: &str) -> Option<Bytes> {
        let key = cache_key(node_addr, chunk_id);
        self.cache.get(&key).map(|entry| entry.value().1.clone())
    }

    /// Insert a chunk that the foreground read path just fetched. Without
    /// this, every FUSE read of a fresh chunk (128 KB at a time across a
    /// 128 MB chunk) re-issues a full chunk fetch — observed ~1.2 MB/s.
    /// Storing the chunk here lets the next FUSE read hit the cache.
    pub fn insert_cached(&self, node_addr: &str, chunk_id: &str, data: Bytes) {
        let key = cache_key(node_addr, chunk_id);
        Self::store_in_cache(
            &self.cache,
            &self.cache_bytes,
            self.max_cache_bytes.load(Ordering::Relaxed),
            key,
            data,
        );
    }

    /// WI-05 / B2 — if this chunk is already being prefetched AND that
    /// prefetch has already acquired its `prefetch_slots` permit (i.e. it
    /// is genuinely on the wire, not queued behind the `prev_acquired`
    /// chain or the semaphore), wait for it instead of issuing a second
    /// `GetChunk` for the same bytes over the same link. Returns `None`
    /// when nothing is pending, the pending prefetch has not STARTED, the
    /// prefetch failed, or `timeout` elapsed; the caller then runs its own
    /// candidate loop (P0-2 replica fallback must stay reachable).
    pub async fn join_pending(
        &self,
        node_addr: &str,
        chunk_id: &str,
        timeout: Duration,
    ) -> Option<Bytes> {
        let key = cache_key(node_addr, chunk_id);
        // CO-3-12 — clone the watch receiver and drop the DashMap guard
        // before the await below; never hold a DashMap guard across an
        // `.await`.
        let (started, mut rx) = {
            let entry = self.pending.get(&key)?;
            (Arc::clone(&entry.started), entry.done.clone())
        };
        if !started.load(Ordering::Relaxed) {
            // B2 — queued behind the prev_acquired chain / the semaphore,
            // not on the wire. Waiting would put this user-visible read
            // behind speculative I/O; fall through to our own fetch
            // instead.
            return None;
        }
        match tokio::time::timeout(timeout, rx.wait_for(|done| *done)).await {
            Ok(Ok(_)) => self.get_cached(node_addr, chunk_id),
            _ => None,
        }
    }

    /// Number of entries currently in the prefetch cache.
    #[cfg(test)]
    pub fn cache_len(&self) -> usize {
        self.cache.len()
    }

    /// Spawn a background task to fetch a single chunk. `prev_acquired`,
    /// when set, is awaited before this task's own semaphore permit, so
    /// prefetch fetches acquire their in-flight slot in the SAME order
    /// `on_chunk_read`'s ascending loop spawned them (WI-05 near-first).
    /// Returns a receiver the NEXT chunk in the chain should await.
    ///
    /// WI-05 — the old byte-budget gate here (`cache_bytes() >=
    /// max_cache_bytes`) only observed bytes already STORED, never bytes
    /// in flight, so it could not bound concurrency. Under the shipped
    /// power-of-two defaults (64 MB cache / 4 MB chunks) `store_in_cache`
    /// evicts down to exactly the budget, so the gate went permanently
    /// true after the first `cache_mb` of reads and silently killed all
    /// further prefetch (with ragged sizing the total settles just below
    /// the budget and the old gate never fired at all). Do not re-add a
    /// `cache_bytes` gate here — memory is now bounded by
    /// `store_in_cache`'s byte eviction (stored bytes, Bug CE, unchanged)
    /// plus the `prefetch_slots` semaphore acquired below (in-flight
    /// bytes, capped at `prefetch_max_inflight * chunk_size`). The CF-5
    /// Shed rung is served instead by `prefetch_enabled` (H2), checked in
    /// `on_chunk_read` before this function is ever called.
    fn spawn_prefetch(
        &self,
        key: String,
        node_addr: String,
        chunk_id: String,
        file_path: &str,
        prev_acquired: Option<oneshot::Receiver<()>>,
        track: bool,
    ) -> oneshot::Receiver<()> {
        let client = self.client.clone();
        let fetch_timeout = self.fetch_timeout;
        let cache = Arc::clone(&self.cache);
        let cache_bytes = Arc::clone(&self.cache_bytes);
        let max_cache_bytes = self.max_cache_bytes.load(Ordering::Relaxed);
        let pending_key = key.clone();
        let pending_ref = Arc::clone(&self.pending);
        let local_cache = self.local_cache.read().clone();
        let slots = Arc::clone(&self.prefetch_slots);
        let started = Arc::new(AtomicBool::new(false));
        let started_flag = Arc::clone(&started);

        // CO-5-17 — gate the spawned task on a one-shot `started` signal so
        // its terminal `pending.remove(key)` cannot run before the parent has
        // installed the real handle. Pre-fix the task removed the key at its
        // end while the parent inserted it AFTER the spawn; a fast task could
        // remove before the insert landed, stranding the handle in `pending`
        // forever and permanently suppressing this chunk's prefetch (the
        // `pending.contains_key` guard in `on_chunk_read` then always skips
        // it). With the gate, the parent inserts, then releases the task, so
        // the remove is always well-ordered after the insert — no leak.
        let (started_tx, started_rx) = oneshot::channel::<()>();
        let pending_key2 = pending_key.clone();

        // WI-05 — fires once this task has acquired its `prefetch_slots`
        // permit, so the next chunk in the ascending window chains its own
        // `acquire` strictly after this one (near-first is then binding on
        // the wire, not just on spawn order).
        let (acquired_tx, acquired_rx) = oneshot::channel::<()>();

        // WI-05 — fires `true` when this task reaches a terminal state
        // (success or failure), just before it removes its own `pending`
        // entry, so `join_pending` (Step 4) can wait for a chunk that is
        // already being fetched instead of duplicating the fetch.
        let (done_tx, done_rx) = watch::channel(false);

        let handle = tokio::spawn(async move {
            // Wait until the parent has installed our handle in `pending`.
            let _ = started_rx.await;

            // WI-05 — wait for the previous chunk in the chain to have
            // acquired ITS permit first. A cancelled/aborted predecessor
            // drops its sender without sending, which we treat as
            // "proceed" so an aborted chain link can never wedge its
            // successors.
            if let Some(prev) = prev_acquired {
                let _ = prev.await;
            }

            // WI-05 — bound concurrent prefetch fetches
            // (`readahead.prefetch_max_inflight`). The semaphore is never
            // closed, so `Err` only happens if the engine itself is being
            // torn down mid-acquire; treat that like any other abort.
            let permit = match slots.acquire_owned().await {
                Ok(p) => p,
                Err(_) => {
                    let _ = acquired_tx.send(());
                    let _ = done_tx.send(true);
                    pending_ref.remove(&pending_key2);
                    return;
                }
            };
            // WI-05 / B1 — set true the instant the fetch is genuinely on
            // the wire (permit acquired), BEFORE releasing the next chunk
            // in the chain. `join_pending` refuses to wait on a false
            // entry.
            started_flag.store(true, Ordering::Relaxed);
            // Release the next chunk in the chain now that our permit is
            // held — its own `acquire` call can now queue behind ours.
            let _ = acquired_tx.send(());

            // #151: a foreground read may have cached it while we queued.
            if cache.contains_key(&pending_key) {
                drop(permit);
                let _ = done_tx.send(true);
                pending_ref.remove(&pending_key2);
                return;
            }

            // FUSE-2 — bound the fetch so a hung replica releases the permit.
            let fetch = match fetch_timeout {
                None => client.read_chunk_as(&node_addr, &chunk_id, track, true).await,
                Some(t) => match tokio::time::timeout(t, client.read_chunk_as(&node_addr, &chunk_id, track, true)).await {
                    Ok(res) => res,
                    Err(_) => Err(anyhow::anyhow!(
                        "prefetch of {chunk_id} from {node_addr} timed out after {}s",
                        t.as_secs()
                    )),
                },
            };
            // Free the slot before the (purely local) cache store + disk
            // mirror below, so the next queued fetch can start immediately.
            drop(permit);

            match fetch {
                Ok(data) => {
                    debug!(
                        chunk_id = %chunk_id,
                        node = %node_addr,
                        size = data.len(),
                        "readahead prefetch complete"
                    );
                    // Bug CE — byte-bounded insert (was an unbounded
                    // `cache.insert`, the worst offender for the OOM).
                    Self::store_in_cache(
                        &cache,
                        &cache_bytes,
                        max_cache_bytes,
                        pending_key.clone(),
                        data.clone(),
                    );
                    // #151: release joined readers before the disk write.
                    let _ = done_tx.send(true);
                    // Mirror to disk cache so the prefetched chunk
                    // survives in-mem eviction + FUSE restart. Best-
                    // effort; disk-cache failures are logged, not fatal.
                    if let Some(lc) = local_cache {
                        if let Err(e) = lc.put(&chunk_id, &data).await {
                            debug!(
                                chunk_id = %chunk_id,
                                error = %e,
                                "readahead disk-cache mirror failed (non-fatal)"
                            );
                        }
                    }
                }
                Err(e) => {
                    // Prefetch failures are non-fatal — the read path will
                    // fetch on demand.
                    debug!(
                        chunk_id = %chunk_id,
                        node = %node_addr,
                        error = %e,
                        "readahead prefetch failed (non-fatal)"
                    );
                }
            }
            // WI-05 — signal any foreground reader waiting in
            // `join_pending` BEFORE removing ourselves from `pending`, so a
            // waiter's `get_cached` call reliably finds the stored bytes on
            // success, or reliably finds nothing (and falls through to its
            // own fetch) on failure. On success it was sent after the
            // in-memory store, before the disk mirror (a no-op resend here).
            let _ = done_tx.send(true);
            // Remove from pending set. Ordered strictly after the parent's
            // insert via the `started` gate above, so it can never leak.
            pending_ref.remove(&pending_key2);
        });

        // Install the handle, THEN release the task so its terminal remove
        // is well-ordered after this insert.
        self.pending.insert(
            key,
            PendingPrefetch {
                handle,
                done: done_rx,
                started,
                file: file_path.to_string(),
            },
        );
        let _ = started_tx.send(());
        acquired_rx
    }
}

impl Drop for ReadAheadEngine {
    fn drop(&mut self) {
        // Abort all pending tasks on drop.
        for entry in self.pending.iter() {
            entry.value().handle.abort();
        }
    }
}

/// CF-5 — cheap clonable handle to the readahead engine's cache, used by the
/// FUSE memory-pressure ladder. The engine is moved into the fuse adapter at
/// mount time, so a background sampler task can't hold `&ReadAheadEngine`;
/// this captures the same internal `Arc`s and drives the two ladder rungs:
///
///  * Warn → `shrink_to_bytes(configured / 2)`: halve the live byte budget
///    and evict down to it. Prefetch still runs, just bounded smaller.
///  * Shed → `drop_unconsumed()`: cancel pending prefetch + clear the cache.
///  * Normal → `shrink_to_bytes(configured)`: restore the operator budget.
#[derive(Clone)]
pub struct ReadAheadCacheController {
    cache: Arc<DashMap<String, (u64, Bytes)>>,
    cache_bytes: Arc<AtomicUsize>,
    pending: Arc<DashMap<String, PendingPrefetch>>,
    max_cache_bytes: Arc<AtomicUsize>,
    /// The operator-configured budget (Bug CE `cache_mb`), captured at
    /// construction so `Normal` can restore it after pressure clears.
    configured_budget: usize,
    /// CF-5 / H2 — shared handle to the engine's `prefetch_enabled` gate.
    prefetch_enabled: Arc<AtomicBool>,
}

impl ReadAheadCacheController {
    /// The operator-configured byte budget (Bug CE `cache_mb`).
    pub fn configured_budget(&self) -> usize {
        self.configured_budget
    }

    /// Current live byte budget.
    #[cfg(test)]
    pub fn current_budget(&self) -> usize {
        self.max_cache_bytes.load(Ordering::Relaxed)
    }

    /// Bytes currently held in the prefetch cache.
    pub fn cache_bytes(&self) -> usize {
        self.cache_bytes.load(Ordering::Relaxed)
    }

    /// Set the live byte budget to `target` and evict arbitrary entries until
    /// the cache fits. Best-effort, never rejects a read (§ A.5). Returns the
    /// number of entries evicted.
    pub fn shrink_to_bytes(&self, target: usize) -> usize {
        self.max_cache_bytes.store(target, Ordering::Relaxed);
        let mut evicted = 0usize;
        while self.cache_bytes.load(Ordering::Relaxed) > target {
            if !evict_oldest(&self.cache, &self.cache_bytes, None) {
                break;
            }
            evicted += 1;
        }
        evicted
    }

    /// Shed rung — cancel every pending prefetch and clear the in-memory
    /// cache. The disk-backed local cache (P0-11) is untouched, so warm
    /// chunks still survive; only the volatile RAM cache is dropped.
    pub fn drop_unconsumed(&self) {
        for entry in self.pending.iter() {
            entry.value().handle.abort();
        }
        self.pending.clear();
        drain_cache(&self.cache, &self.cache_bytes);
    }

    /// CF-5 — Shed pins prefetch off; Normal/Warn turn it back on. Warn only
    /// halves the byte budget (§ A.5: slow down, don't stop).
    pub fn set_prefetch_enabled(&self, on: bool) {
        self.prefetch_enabled.store(on, Ordering::Relaxed);
    }

    /// Whether new prefetch spawns are currently allowed.
    pub fn prefetch_enabled(&self) -> bool {
        self.prefetch_enabled.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod rev_2_3_prefetch_target_tests {
    use super::prefetch_target;

    fn locs() -> Vec<(u64, String, Vec<String>)> {
        vec![
            (
                0,
                "h0".to_string(),
                vec!["http://near:9500".to_string(), "http://far:9500".to_string()],
            ),
            // index 1 is a hole — a sparse file, or a chunk whose only
            // location row was removed.
            (2, "h2".to_string(), vec!["http://near2:9500".to_string()]),
            (3, "h3".to_string(), Vec::new()),
        ]
    }

    /// REV-2-3: the caller pre-orders candidates with the same U4 rules the
    /// foreground read applies, so the head is the replica the read would
    /// pick — not the manager's arbitrary `node_addresses[0]`.
    #[test]
    fn rev_2_3_prefetches_the_locality_winner_not_the_managers_first_replica() {
        let l = locs();
        let (id, addr) = prefetch_target(&l, 0).unwrap();
        assert_eq!(id, "h0");
        assert_eq!(addr, "http://near:9500");
    }

    /// REV-2-3: the same positional aliasing REV-2-1 fixed on the read path.
    #[test]
    fn rev_2_3_a_hole_prefetches_nothing_rather_than_a_neighbour() {
        let l = locs();
        assert!(
            prefetch_target(&l, 1).is_none(),
            "positional lookup used to return h2 for index 1"
        );
        assert_eq!(prefetch_target(&l, 2).unwrap().0, "h2");
    }

    #[test]
    fn rev_2_3_a_chunk_with_no_replicas_is_skipped() {
        let l = locs();
        assert!(prefetch_target(&l, 3).is_none());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_readahead_mode_from_config() {
        let mode = ReadAheadMode::from_config("disabled", 8);
        assert!(matches!(mode, ReadAheadMode::Disabled));
        assert_eq!(mode.prefetch_chunks(), 0);

        let mode = ReadAheadMode::from_config("file-sequential", 4);
        assert!(matches!(
            mode,
            ReadAheadMode::FileSequential { prefetch_chunks: 4 }
        ));

        let mode = ReadAheadMode::from_config("folder-sequential", 8);
        assert!(matches!(
            mode,
            ReadAheadMode::FolderSequential { prefetch_chunks: 8 }
        ));

        let mode = ReadAheadMode::from_config("file-only", 2);
        assert!(matches!(
            mode,
            ReadAheadMode::FileOnly { prefetch_chunks: 2 }
        ));

        // Unknown defaults to folder-sequential.
        let mode = ReadAheadMode::from_config("unknown", 8);
        assert!(matches!(mode, ReadAheadMode::FolderSequential { .. }));
    }

    #[test]
    fn test_cache_key_format() {
        assert_eq!(
            cache_key("node1:9000", "abc123"),
            "node1:9000|abc123"
        );
    }

    /// Bug CE — the in-memory prefetch cache must be bounded by BYTES, not
    /// entry count. With a 4 MB budget, inserting eight 1 MB chunks must
    /// evict down to <= 4 MB (the old count bound kept all 8 MB, which on
    /// the 1 GB client with whole-chunk values OOM-killed FUSE).
    #[test]
    fn bug_ce_cache_is_byte_bounded() {
        let client = DtfsClient::new();
        let engine = ReadAheadEngine::new(ReadAheadMode::Disabled, client, 60, 4 * 1024 * 1024, 2);
        let one_mb = Bytes::from(vec![0u8; 1024 * 1024]);
        for i in 0..8 {
            engine.insert_cached("node:9000", &format!("chunk{i}"), one_mb.clone());
        }
        assert!(
            engine.cache_bytes() <= 4 * 1024 * 1024,
            "cache must stay within the 4 MB byte budget, got {} bytes",
            engine.cache_bytes()
        );
        assert!(
            engine.cache_bytes() > 0,
            "cache should retain the most-recent chunks, not drop everything"
        );
        // Byte accounting stays consistent with the entry count.
        assert!(
            engine.cache_len() <= 4,
            "at most four 1 MB chunks fit in a 4 MB budget, got {} entries",
            engine.cache_len()
        );
    }

    #[test]
    fn prefetch_window_bounds() {
        assert!(!in_prefetch_window(5, 5, 4), "current chunk is not prefetched");
        assert!(in_prefetch_window(6, 5, 4));
        assert!(in_prefetch_window(9, 5, 4), "last chunk of the window");
        assert!(!in_prefetch_window(10, 5, 4), "one past the window");
        assert!(!in_prefetch_window(2, 5, 4), "earlier chunk");
        assert!(!in_prefetch_window(6, 5, 0), "window 0 = nothing");
    }

    /// Eviction drops the oldest-inserted entries first (not DashMap
    /// iteration order): the newest chunks, i.e. the ones being read and
    /// other streams' fresh prefetches, survive.
    #[test]
    fn eviction_is_oldest_inserted_first() {
        let engine = ReadAheadEngine::new(
            ReadAheadMode::Disabled,
            DtfsClient::new(),
            60,
            3 * 1024 * 1024,
            2,
        );
        let one_mb = Bytes::from(vec![0u8; 1024 * 1024]);
        for i in 0..6 {
            engine.insert_cached("node:9000", &format!("chunk{i}"), one_mb.clone());
        }
        for i in 0..3 {
            assert!(engine.get_cached("node:9000", &format!("chunk{i}")).is_none(), "chunk{i} is oldest, must be evicted");
        }
        for i in 3..6 {
            assert!(engine.get_cached("node:9000", &format!("chunk{i}")).is_some(), "chunk{i} is newest, must survive");
        }
        // shrink also goes oldest-first
        engine.cache_controller().shrink_to_bytes(1024 * 1024);
        assert!(engine.get_cached("node:9000", "chunk5").is_some());
        assert!(engine.get_cached("node:9000", "chunk4").is_none());
    }

    /// Draining while inserts race must leave the byte counter equal to
    /// the surviving entries (never wrapped by a bulk `store(0)`).
    #[test]
    fn drop_unconsumed_keeps_cache_bytes_consistent_under_race() {
        let engine = Arc::new(ReadAheadEngine::new(
            ReadAheadMode::Disabled,
            DtfsClient::new(),
            60,
            usize::MAX / 2,
            2,
        ));
        let ctrl = engine.cache_controller();
        let e2 = Arc::clone(&engine);
        let t = std::thread::spawn(move || {
            for i in 0..2000 {
                e2.insert_cached("n:1", &format!("c{}", i % 16), Bytes::from(vec![0u8; 1024]));
            }
        });
        for _ in 0..200 {
            ctrl.drop_unconsumed();
        }
        t.join().unwrap();
        assert!(engine.cache_bytes() <= 16 * 1024, "counter wrapped: {}", engine.cache_bytes());
        assert_eq!(engine.cache_bytes(), engine.cache_len() * 1024);
        ctrl.drop_unconsumed();
        assert_eq!(engine.cache_bytes(), 0);
    }

    /// CF-5 — the controller's `shrink_to_bytes` halves the live budget and
    /// evicts down to it; `drop_unconsumed` clears the cache entirely. This
    /// is the FUSE memory-pressure ladder's Warn and Shed rungs.
    #[test]
    fn cf5_controller_shrink_and_drop() {
        let client = DtfsClient::new();
        let engine = ReadAheadEngine::new(ReadAheadMode::Disabled, client, 60, 8 * 1024 * 1024, 2);
        let one_mb = Bytes::from(vec![0u8; 1024 * 1024]);
        for i in 0..8 {
            engine.insert_cached("node:9000", &format!("chunk{i}"), one_mb.clone());
        }
        let ctrl = engine.cache_controller();
        assert_eq!(ctrl.configured_budget(), 8 * 1024 * 1024);
        assert!(ctrl.cache_bytes() > 4 * 1024 * 1024);

        // Warn rung — halve the budget; cache must evict down to <= 4 MB.
        let evicted = ctrl.shrink_to_bytes(4 * 1024 * 1024);
        assert!(evicted > 0, "halving the budget must evict entries");
        assert!(
            ctrl.cache_bytes() <= 4 * 1024 * 1024,
            "cache must fit the halved 4 MB budget, got {} bytes",
            ctrl.cache_bytes()
        );

        // Shed rung — drop everything.
        ctrl.drop_unconsumed();
        assert_eq!(ctrl.cache_bytes(), 0, "drop_unconsumed clears the cache");
        assert_eq!(engine.cache_len(), 0);

        // Normal rung — restore the operator budget.
        ctrl.shrink_to_bytes(ctrl.configured_budget());
        assert_eq!(ctrl.current_budget(), 8 * 1024 * 1024);
    }

    /// CO-5-17 — a prefetch task must not leave a stranded key in `pending`.
    /// Pre-fix the parent inserted the handle AFTER the spawn while the task
    /// removed the key at its end; a fast task (here the read fails instantly
    /// against an unreachable node) removed before the insert landed, so the
    /// handle leaked and the `pending.contains_key` guard then permanently
    /// suppressed that chunk's prefetch. With the started-gate fix the
    /// remove is ordered strictly after the insert, so `pending` drains.
    #[tokio::test]
    async fn co_5_17_completed_prefetch_does_not_leak_pending_key() {
        let client = DtfsClient::new();
        let engine =
            ReadAheadEngine::new(ReadAheadMode::FileSequential { prefetch_chunks: 1 }, client, 60, 64 * 1024 * 1024, 2);

        // Two chunks; reading index 0 prefetches index 1 from an
        // unreachable node so `read_chunk` errors out near-instantly.
        let locations = vec![
            (0u64, "chunk0".to_string(), vec!["127.0.0.1:1".to_string()]),
            (1u64, "chunk1".to_string(), vec!["127.0.0.1:1".to_string()]),
        ];
        engine.on_chunk_read("/movie.mkv", 0, 2, &locations);

        // The prefetch task should run to completion and remove its own
        // pending entry. Wait (bounded) for the map to drain.
        let drained = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if engine.pending.is_empty() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await;

        assert!(
            drained.is_ok(),
            "pending map never drained — a completed prefetch leaked its key"
        );
        assert_eq!(
            engine.pending.len(),
            0,
            "pending must be empty after the prefetch task completes"
        );
    }
}

#[cfg(test)]
mod wi_05_prefetch_cap_tests {
    use super::*;

    /// WI-05 — the prefetch WINDOW stays `prefetch_chunks` (8) deep — all 8
    /// candidates land in `pending` — but only `prefetch_max_inflight` (2)
    /// fetches are actually on the wire at once, and the two that start are
    /// the NEAREST two (chunk1, chunk2), not an arbitrary pair.
    ///
    /// The listener here accepts TCP connections and holds the sockets
    /// without ever speaking HTTP/2, so each fetch's h2 handshake hangs and
    /// holds its `prefetch_slots` permit for the whole test window (well
    /// inside tonic's 5s `connect_timeout`). An unreachable address like
    /// `127.0.0.1:1` would NOT work here: it errors instantly, the permit
    /// releases immediately, and all 8 fetches would start.
    #[tokio::test]
    async fn wi_05_prefetch_fan_out_is_capped_and_near_first() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stalling listener");
        let addr = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((sock, _)) = listener.accept().await {
                held.push(sock);
            }
        });

        let client = DtfsClient::new();
        let engine = ReadAheadEngine::new(
            ReadAheadMode::FileSequential { prefetch_chunks: 8 },
            client,
            60,
            64 * 1024 * 1024,
            2,
        );

        let locations: Vec<(u64, String, Vec<String>)> = (0..=8u64)
            .map(|i| (i, format!("chunk{i}"), vec![addr.clone()]))
            .collect();

        engine.on_chunk_read("/movie.mkv", 0, 9, &locations);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        assert_eq!(
            engine.pending.len(),
            8,
            "prefetch window must stay 8 deep regardless of the in-flight cap"
        );
        let started: Vec<String> = (1..=8u64)
            .map(|i| format!("chunk{i}"))
            .filter(|id| {
                engine
                    .pending
                    .get(&cache_key(&addr, id))
                    .map(|e| e.started.load(std::sync::atomic::Ordering::Relaxed))
                    .unwrap_or(false)
            })
            .collect();
        assert!(
            started.len() <= 2,
            "at most prefetch_max_inflight (2) fetches may be on the wire, got {started:?}"
        );
        assert_eq!(
            started,
            vec!["chunk1".to_string(), "chunk2".to_string()],
            "the two started fetches must be the NEAREST two"
        );
    }

    fn scan_locs(f: usize, n: u64) -> Vec<(u64, String, Vec<String>)> {
        (0..n).map(|i| (i, format!("f{f}c{i}"), vec!["127.0.0.1:1".to_string()])).collect()
    }

    fn queued_engine() -> (ReadAheadEngine, tokio::sync::OwnedSemaphorePermit) {
        let engine = ReadAheadEngine::new(
            ReadAheadMode::FileSequential { prefetch_chunks: 8 },
            DtfsClient::new(),
            60,
            64 * 1024 * 1024,
            1,
        );
        // Hold the only slot: every prefetch stays queued (not started).
        let slot = engine.prefetch_slots.clone().try_acquire_owned().unwrap();
        (engine, slot)
    }

    /// #151: a library scan reads one chunk per file. Queued prefetches of
    /// files outside the last `RECENT_PREFETCH_FILES` read are dropped, so
    /// `pending` stays at that many windows instead of one per file.
    #[tokio::test]
    async fn queued_prefetches_of_earlier_reads_are_dropped() {
        let (engine, _slot) = queued_engine();
        for f in 0..100 {
            engine.on_chunk_read(&format!("/f{f}"), 0, 9, &scan_locs(f, 9));
            tokio::task::yield_now().await;
        }
        assert_eq!(engine.pending.len(), RECENT_PREFETCH_FILES * 8, "boundary: N windows");
        assert!(engine.pending.contains_key(&cache_key("127.0.0.1:1", "f99c1")));
        assert!(engine.pending.contains_key(&cache_key("127.0.0.1:1", "f92c1")));
        assert!(!engine.pending.contains_key(&cache_key("127.0.0.1:1", "f91c1")));
    }

    /// #151 review: concurrent streams (several Plex viewers) each keep
    /// their queued window while the others advance.
    #[tokio::test]
    async fn concurrent_streams_keep_their_queued_windows() {
        let (engine, _slot) = queued_engine();
        for c in 0..4u32 {
            for f in 0..6 {
                engine.on_chunk_read(&format!("/f{f}"), c, 100, &scan_locs(f, 100));
                tokio::task::yield_now().await;
            }
        }
        for f in 0..6 {
            let key = cache_key("127.0.0.1:1", &format!("f{f}c4"));
            assert!(engine.pending.contains_key(&key), "stream {f} lost its window");
        }
        assert_eq!(engine.pending.len(), 6 * 8);
    }

    /// #151: a seek drops the queued window of the old position in the
    /// same file.
    #[tokio::test]
    async fn seek_drops_the_same_files_queued_window() {
        let (engine, _slot) = queued_engine();
        engine.on_chunk_read("/f0", 0, 100, &scan_locs(0, 100));
        tokio::task::yield_now().await;
        engine.on_chunk_read("/f0", 50, 100, &scan_locs(0, 100));
        tokio::task::yield_now().await;
        assert!(!engine.pending.contains_key(&cache_key("127.0.0.1:1", "f0c1")));
        assert!(engine.pending.contains_key(&cache_key("127.0.0.1:1", "f0c51")));
        assert_eq!(engine.pending.len(), 8);
    }

    /// WI-05 — the old gate (`cache_bytes() >= max_cache_bytes`) went
    /// permanently true once the cache settled at exactly its byte budget
    /// (which `store_in_cache` always evicts down to), silently killing all
    /// further prefetch. With the gate removed, prefetch must still spawn
    /// while the cache sits exactly at budget. Deterministic and network-
    /// free: on the current_thread test runtime, spawned tasks are not
    /// polled until this function's next `.await`, and `pending.insert`
    /// happens synchronously inside `spawn_prefetch`.
    #[tokio::test]
    async fn wi_05_prefetch_runs_when_cache_is_at_budget() {
        let client = DtfsClient::new();
        let engine = ReadAheadEngine::new(
            ReadAheadMode::FileSequential { prefetch_chunks: 2 },
            client,
            60,
            4 * 1024 * 1024,
            2,
        );
        let one_mb = Bytes::from(vec![0u8; 1024 * 1024]);
        for i in 0..4 {
            engine.insert_cached("node:9000", &format!("cached{i}"), one_mb.clone());
        }
        assert_eq!(
            engine.cache_bytes(),
            4 * 1024 * 1024,
            "cache must sit exactly at its byte budget"
        );

        let locations = vec![
            (0u64, "chunk0".to_string(), vec!["node:9000".to_string()]),
            (1u64, "next1".to_string(), vec!["127.0.0.1:1".to_string()]),
            (2u64, "next2".to_string(), vec!["127.0.0.1:1".to_string()]),
        ];
        engine.on_chunk_read("/movie.mkv", 0, 3, &locations);

        assert!(
            !engine.pending.is_empty(),
            "prefetch must still spawn when the cache is exactly at its byte budget"
        );
    }

    /// WI-05 Step 4 — `join_pending` returns the bytes an in-flight
    /// prefetch stored once it signals done, and returns `None` (not a
    /// hang) when the prefetch's sender is dropped without the cache ever
    /// being populated (a failed/aborted fetch).
    #[tokio::test]
    async fn wi_05_foreground_join_returns_prefetched_bytes() {
        let client = DtfsClient::new();
        let engine = ReadAheadEngine::new(ReadAheadMode::Disabled, client, 60, 4 * 1024 * 1024, 2);

        // Success case: the watch sender flips AFTER the bytes land in the
        // cache, mirroring spawn_prefetch's own ordering.
        let (done_tx, done_rx) = watch::channel(false);
        let handle = tokio::spawn(async {});
        engine.pending.insert(
            "node:9000|chunkA".to_string(),
            PendingPrefetch {
                handle,
                done: done_rx,
                started: Arc::new(AtomicBool::new(true)),
                file: String::new(),
            },
        );
        let data = Bytes::from_static(b"hello");
        engine.insert_cached("node:9000", "chunkA", data.clone());
        let _ = done_tx.send(true);

        let joined = engine
            .join_pending("node:9000", "chunkA", std::time::Duration::from_secs(1))
            .await;
        assert_eq!(
            joined,
            Some(data),
            "join_pending must return the bytes the prefetch stored"
        );

        // Failure case: the sender is dropped without ever sending `true`
        // and without the cache being populated.
        let (done_tx2, done_rx2) = watch::channel(false);
        let handle2 = tokio::spawn(async {});
        engine.pending.insert(
            "node:9000|chunkB".to_string(),
            PendingPrefetch {
                handle: handle2,
                done: done_rx2,
                started: Arc::new(AtomicBool::new(true)),
                file: String::new(),
            },
        );
        drop(done_tx2);

        let joined2 = engine
            .join_pending(
                "node:9000",
                "chunkB",
                std::time::Duration::from_millis(200),
            )
            .await;
        assert_eq!(
            joined2, None,
            "join_pending must return None, not hang, when the prefetch never completes"
        );
    }

    /// B2 — a prefetch that has NOT yet acquired its `prefetch_slots`
    /// permit (still queued behind the `prev_acquired` chain / the
    /// semaphore) must not be waited on: `join_pending` returns `None`
    /// immediately, not after `timeout` elapses.
    #[tokio::test]
    async fn b2_join_pending_refuses_a_queued_prefetch() {
        let client = DtfsClient::new();
        let engine = ReadAheadEngine::new(ReadAheadMode::Disabled, client, 60, 4 * 1024 * 1024, 2);

        let (_done_tx, done_rx) = watch::channel(false);
        let handle = tokio::spawn(async {});
        engine.pending.insert(
            "node:9000|chunkQ".to_string(),
            PendingPrefetch {
                handle,
                done: done_rx,
                started: Arc::new(AtomicBool::new(false)),
                file: String::new(),
            },
        );

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            engine.join_pending("node:9000", "chunkQ", std::time::Duration::from_secs(30)),
        )
        .await;
        assert!(
            result.is_ok(),
            "join_pending must return immediately for a not-yet-started prefetch, not wait out the timeout"
        );
        assert_eq!(
            result.unwrap(),
            None,
            "a queued-but-not-started prefetch must never be joined"
        );
    }

    /// B2 — a started-but-stalled prefetch is bounded by the fixed
    /// `PREFETCH_JOIN_TIMEOUT` (2s) constant, never by a longer caller-
    /// supplied budget such as `client.chunk_read_timeout_secs`.
    #[tokio::test]
    async fn b2_join_pending_is_bounded_by_the_two_second_constant() {
        let client = DtfsClient::new();
        let engine = ReadAheadEngine::new(ReadAheadMode::Disabled, client, 60, 4 * 1024 * 1024, 2);

        let (_done_tx, done_rx) = watch::channel(false);
        let handle = tokio::spawn(async {});
        engine.pending.insert(
            "node:9000|chunkS".to_string(),
            PendingPrefetch {
                handle,
                done: done_rx,
                started: Arc::new(AtomicBool::new(true)),
                file: String::new(),
            },
        );

        let start = std::time::Instant::now();
        let joined = engine
            .join_pending("node:9000", "chunkS", PREFETCH_JOIN_TIMEOUT)
            .await;
        let elapsed = start.elapsed();
        assert_eq!(joined, None, "a stalled started prefetch must yield None on timeout");
        assert!(
            elapsed >= std::time::Duration::from_secs(2),
            "must wait out the full 2s constant, elapsed {elapsed:?}"
        );
        assert!(
            elapsed < std::time::Duration::from_secs(4),
            "must not wait meaningfully longer than the 2s constant, elapsed {elapsed:?}"
        );
    }

    /// H2 — the CF-5 Shed rung pins `prefetch_enabled` false, so
    /// `on_chunk_read` spawns nothing while it is set; flipping it back on
    /// restores prefetch. Deterministic and network-free: `pending.insert`
    /// happens synchronously inside `spawn_prefetch` on the current-thread
    /// test runtime.
    #[tokio::test]
    async fn h2_shed_gate_stops_new_prefetch() {
        let client = DtfsClient::new();
        let engine = ReadAheadEngine::new(
            ReadAheadMode::FileSequential { prefetch_chunks: 2 },
            client,
            60,
            4 * 1024 * 1024,
            2,
        );
        let ctrl = engine.cache_controller();

        let locations = vec![
            (0u64, "chunk0".to_string(), vec!["127.0.0.1:1".to_string()]),
            (1u64, "chunk1".to_string(), vec!["127.0.0.1:1".to_string()]),
            (2u64, "chunk2".to_string(), vec!["127.0.0.1:1".to_string()]),
        ];

        ctrl.set_prefetch_enabled(false);
        engine.on_chunk_read("/movie.mkv", 0, 3, &locations);
        assert!(
            engine.pending.is_empty(),
            "on_chunk_read must spawn nothing while prefetch_enabled is false"
        );

        ctrl.set_prefetch_enabled(true);
        engine.on_chunk_read("/movie.mkv", 0, 3, &locations);
        assert!(
            !engine.pending.is_empty(),
            "on_chunk_read must resume spawning once prefetch_enabled is true again"
        );
    }
}
