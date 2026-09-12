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
    /// Prefetch the next N chunks in this file AND the first chunks of the
    /// next M files in the same directory (sorted lexicographically).
    FolderSequential {
        prefetch_chunks: u32,
        prefetch_next_files: u32,
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
    pub fn from_config(mode: &str, prefetch_chunks: u32, prefetch_next_files: u32) -> Self {
        match mode {
            "file-sequential" => ReadAheadMode::FileSequential { prefetch_chunks },
            "folder-sequential" => ReadAheadMode::FolderSequential {
                prefetch_chunks,
                prefetch_next_files,
            },
            "file-only" => ReadAheadMode::FileOnly { prefetch_chunks },
            "disabled" => ReadAheadMode::Disabled,
            other => {
                warn!(mode = other, "unknown readahead mode, defaulting to folder-sequential");
                ReadAheadMode::FolderSequential {
                    prefetch_chunks,
                    prefetch_next_files,
                }
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
    /// Currently pending prefetch tasks keyed by cache key.
    pending: Arc<DashMap<String, PendingPrefetch>>,
    /// Cached prefetched chunk data keyed by cache key.
    cache: Arc<DashMap<String, Bytes>>,
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
}

impl ReadAheadEngine {
    /// Create a new read-ahead engine. `max_cache_bytes` is the soft byte
    /// budget for the in-memory prefetch cache (Bug CE). `max_inflight` is
    /// the WI-05 cap on concurrent prefetch fetches (`.max(1)`, so `0`
    /// degrades to serial prefetch rather than "unbounded").
    pub fn new(
        mode: ReadAheadMode,
        client: DtfsClient,
        max_cache_bytes: usize,
        max_inflight: usize,
    ) -> Self {
        Self {
            mode,
            client,
            pending: Arc::new(DashMap::new()),
            cache: Arc::new(DashMap::new()),
            cache_bytes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            max_cache_bytes: Arc::new(AtomicUsize::new(max_cache_bytes)),
            local_cache: parking_lot::RwLock::new(None),
            prefetch_slots: Arc::new(Semaphore::new(max_inflight.max(1))),
            prefetch_enabled: Arc::new(AtomicBool::new(true)),
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
        cache: &DashMap<String, Bytes>,
        cache_bytes: &std::sync::atomic::AtomicUsize,
        max_cache_bytes: usize,
        key: String,
        data: Bytes,
    ) {
        use std::sync::atomic::Ordering;
        let incoming = data.len();
        if let Some(old) = cache.insert(key.clone(), data) {
            cache_bytes.fetch_sub(old.len(), Ordering::Relaxed);
        }
        cache_bytes.fetch_add(incoming, Ordering::Relaxed);
        // Evict until under budget, never dropping the entry just stored.
        while cache_bytes.load(Ordering::Relaxed) > max_cache_bytes {
            let victim = cache
                .iter()
                .find(|e| e.key() != &key)
                .map(|e| e.key().clone());
            match victim {
                Some(k) => {
                    if let Some((_, v)) = cache.remove(&k) {
                        cache_bytes.fetch_sub(v.len(), Ordering::Relaxed);
                    }
                }
                None => break, // only the fresh entry remains
            }
        }
    }

    /// Bytes currently held in the prefetch cache (Bug CE accounting).
    pub fn cache_bytes(&self) -> usize {
        self.cache_bytes.load(std::sync::atomic::Ordering::Relaxed)
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
    #[instrument(skip(self, chunk_locations), fields(path = %file_path, chunk = chunk_index, total = total_chunks))]
    pub fn on_chunk_read(
        &self,
        file_path: &str,
        chunk_index: u32,
        total_chunks: u32,
        chunk_locations: &[(u64, String, Vec<String>)],
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
        let mut prev: Option<oneshot::Receiver<()>> = None;
        for idx in start..end {
            if let Some((chunk_id, addr)) = prefetch_target(chunk_locations, idx as u64) {
                let key = cache_key(addr, chunk_id);

                // Skip if already cached or being fetched.
                if self.cache.contains_key(&key) || self.pending.contains_key(&key) {
                    continue;
                }

                prev = Some(self.spawn_prefetch(key, addr.clone(), chunk_id.clone(), prev.take()));
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
        self.cache.get(&key).map(|entry| entry.value().clone())
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

    /// Remove a cached entry (e.g., after it has been consumed).
    pub fn evict(&self, node_addr: &str, chunk_id: &str) {
        let key = cache_key(node_addr, chunk_id);
        if let Some((_, v)) = self.cache.remove(&key) {
            self.cache_bytes
                .fetch_sub(v.len(), std::sync::atomic::Ordering::Relaxed);
        }
        // Also cancel pending task if any.
        if let Some((_, p)) = self.pending.remove(&key) {
            p.handle.abort();
        }
    }

    /// Cancel all pending prefetch tasks and clear the cache.
    pub fn clear(&self) {
        for entry in self.pending.iter() {
            entry.value().handle.abort();
        }
        self.pending.clear();
        self.cache.clear();
        self.cache_bytes
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    /// Number of entries currently in the prefetch cache.
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
        prev_acquired: Option<oneshot::Receiver<()>>,
    ) -> oneshot::Receiver<()> {
        let client = self.client.clone();
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

            let fetch = client.read_chunk(&node_addr, &chunk_id).await;
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
            // own fetch) on failure.
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
    cache: Arc<DashMap<String, Bytes>>,
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
            let victim = self.cache.iter().next().map(|e| e.key().clone());
            match victim {
                Some(k) => {
                    if let Some((_, v)) = self.cache.remove(&k) {
                        self.cache_bytes.fetch_sub(v.len(), Ordering::Relaxed);
                        evicted += 1;
                    }
                }
                None => break,
            }
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
        self.cache.clear();
        self.cache_bytes.store(0, Ordering::Relaxed);
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
        let mode = ReadAheadMode::from_config("disabled", 8, 3);
        assert!(matches!(mode, ReadAheadMode::Disabled));
        assert_eq!(mode.prefetch_chunks(), 0);

        let mode = ReadAheadMode::from_config("file-sequential", 4, 0);
        assert!(matches!(
            mode,
            ReadAheadMode::FileSequential { prefetch_chunks: 4 }
        ));

        let mode = ReadAheadMode::from_config("folder-sequential", 8, 3);
        assert!(matches!(
            mode,
            ReadAheadMode::FolderSequential {
                prefetch_chunks: 8,
                prefetch_next_files: 3,
            }
        ));

        let mode = ReadAheadMode::from_config("file-only", 2, 0);
        assert!(matches!(
            mode,
            ReadAheadMode::FileOnly { prefetch_chunks: 2 }
        ));

        // Unknown defaults to folder-sequential.
        let mode = ReadAheadMode::from_config("unknown", 8, 3);
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
        let engine = ReadAheadEngine::new(ReadAheadMode::Disabled, client, 4 * 1024 * 1024, 2);
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

    /// CF-5 — the controller's `shrink_to_bytes` halves the live budget and
    /// evicts down to it; `drop_unconsumed` clears the cache entirely. This
    /// is the FUSE memory-pressure ladder's Warn and Shed rungs.
    #[test]
    fn cf5_controller_shrink_and_drop() {
        let client = DtfsClient::new();
        let engine = ReadAheadEngine::new(ReadAheadMode::Disabled, client, 8 * 1024 * 1024, 2);
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
            ReadAheadEngine::new(ReadAheadMode::FileSequential { prefetch_chunks: 1 }, client, 64 * 1024 * 1024, 2);

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
        let engine = ReadAheadEngine::new(ReadAheadMode::Disabled, client, 4 * 1024 * 1024, 2);

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
        let engine = ReadAheadEngine::new(ReadAheadMode::Disabled, client, 4 * 1024 * 1024, 2);

        let (_done_tx, done_rx) = watch::channel(false);
        let handle = tokio::spawn(async {});
        engine.pending.insert(
            "node:9000|chunkQ".to_string(),
            PendingPrefetch {
                handle,
                done: done_rx,
                started: Arc::new(AtomicBool::new(false)),
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
        let engine = ReadAheadEngine::new(ReadAheadMode::Disabled, client, 4 * 1024 * 1024, 2);

        let (_done_tx, done_rx) = watch::channel(false);
        let handle = tokio::spawn(async {});
        engine.pending.insert(
            "node:9000|chunkS".to_string(),
            PendingPrefetch {
                handle,
                done: done_rx,
                started: Arc::new(AtomicBool::new(true)),
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
