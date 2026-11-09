// Write buffering aligned to chunk boundaries — Phase 2.
//
// Accumulates writes in memory and produces chunk-aligned data segments
// ready to be sent to data nodes. This allows small writes to be coalesced
// and chunk boundaries to be respected without requiring callers to manage
// alignment.
//
// CR-1-1 (FUSE-1): the buffer is keyed PER CHUNK (`BTreeMap<u32, ChunkBuf>`),
// never one contiguous `Vec<u8>` spanning the whole file. A write is split
// at chunk boundaries before it is merged, so a write below an
// already-flushed chunk, or one that lands past the highest resident chunk,
// can never zero-fill (and therefore re-emit as real data) a chunk it did
// not touch. A write that lands in a chunk which is not yet resident, or
// does not cover that chunk in full, marks the chunk `needs_seed` instead
// of gap-filling — the caller (filesystem.rs, CR-1-2) is responsible for
// reading the cluster's existing bytes and seeding the chunk before it is
// safe to commit as the chunk's whole content.

use std::collections::BTreeMap;
use std::ops::{Range, RangeInclusive};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;
use tracing::{debug, instrument};

/// WI-01 — upper clamp on `flush_batch_chunks`. Far below the manager's
/// `MAX_CHUNKS_PER_ALLOCATION` (65_536, `metadata.rs`), it just bounds how
/// much un-committed data one open file can hold in client heap
/// (`flush_batch_chunks * chunk_size`; see CLAUDE.md A.3.2).
const MAX_FLUSH_BATCH_CHUNKS: usize = 64;

/// WI-01 / H1 — floor of the per-file batch byte limit: any batch up to
/// 64 MB (the default 4 x 4 MB included) keeps the pre-bigbatch limits,
/// 64 MB batch and 128 MB dirty cap.
const FLUSH_BATCH_MIN_BYTES: usize = 64 * 1024 * 1024;

/// Hard ceiling of the batch byte limit: nothing in the dtfs-fuse memory
/// ladder (A.3.2 / A.5) sheds write buffers, and several files can be open
/// at once during a torrent landing, so batch x chunk size stays bounded.
const FLUSH_BATCH_MAX_BYTES: usize = 256 * 1024 * 1024;
/// Reference chunk size for `batch_limits` (the default `chunk_size_mb`).
const BATCH_REF_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// CR-1-1 — default (and minimum) ceiling on total dirty bytes buffered per
/// file, independent of how many distinct chunk indices are touched. The
/// configured cap is `batch_limits(..).1` (`WriteBuffer::dirty_cap`); this
/// is its value for any batch up to 64 MB. Enforcement is caller-side
/// (filesystem.rs forces a flush before a write that would exceed it).
pub const DIRTY_BYTES_CAP: usize = FLUSH_BATCH_MIN_BYTES * 2;

/// `(batch byte limit, per-file dirty cap)` for a configured
/// `flush_batch_chunks` (clamped `1..=MAX_FLUSH_BATCH_CHUNKS`) and chunk
/// size: batch = `max(64 MB, chunks x chunk_size)` capped at 256 MB; dirty
/// cap = `max(128 MB, 2 x batch)`, so in async_commit mode one batch can
/// upload (in-flight bytes count against the cap) while the next fills.
pub(crate) fn batch_limits(configured: usize) -> (usize, usize) {
    // Sized against a 4 MiB reference chunk, not the volume's: only raising
    // `flush_batch_chunks` grows the limits, so a large-chunk volume with the
    // default batch keeps the old 64 MiB batch / 128 MiB cap (per-chunk
    // flush) instead of up to 512 MiB of dirty data per writer.
    let want = configured.clamp(1, MAX_FLUSH_BATCH_CHUNKS).saturating_mul(BATCH_REF_CHUNK_BYTES);
    let batch = want.clamp(FLUSH_BATCH_MIN_BYTES, FLUSH_BATCH_MAX_BYTES);
    (batch, (2 * batch).max(DIRTY_BYTES_CAP))
}

/// Per-file dirty cap with `inflight` background batches per file
/// (`write.max_inflight_batches`, async_commit): `max(128 MB, (inflight + 1)
/// x batch limit)`, so `inflight` batches can upload while the next fills.
/// `inflight = 1` is `batch_limits(..).1`.
pub(crate) fn dirty_cap_for(configured: usize, inflight: usize) -> usize {
    (inflight.max(1) + 1).saturating_mul(batch_limits(configured).0).max(DIRTY_BYTES_CAP)
}

/// WI-01 / H1 — resolve the effective `flush_batch_chunks` for a given
/// configured value and chunk size: clamp the count into
/// `1..=MAX_FLUSH_BATCH_CHUNKS`, then clamp again so `batch * chunk_size`
/// never exceeds the `batch_limits` byte limit. Never goes below 1 — a single
/// chunk always flushes whatever its size (pre-WI-01 behaviour); holding it
/// back would strand the write.
pub(crate) fn effective_batch_chunks(configured: usize, chunk_size: usize) -> usize {
    let by_count = configured.clamp(1, MAX_FLUSH_BATCH_CHUNKS);
    if chunk_size == 0 {
        // Degenerate; avoids a divide by zero.
        return by_count;
    }
    by_count.min(batch_limits(configured).0 / chunk_size).max(1)
}

/// Zero fill for a write at `offset` past the logical `end`: only the edge
/// chunks get bytes, `[end, end of its chunk)` (old partial tail) and
/// `[start of the write's chunk, offset)`. Chunk indices entirely inside the
/// hole get nothing: a missing index below EOF reads as zeros. The two ranges
/// are contiguous when the write lands in or right after the tail chunk;
/// `r[1].start > r[0].end` means whole chunks were skipped.
pub(crate) fn hole_fill_ranges(end: u64, offset: u64, chunk_size: u64) -> [Range<u64>; 2] {
    if offset <= end || chunk_size == 0 {
        return [end..end, end..end];
    }
    let tail_end = end.next_multiple_of(chunk_size).min(offset);
    let head = (offset / chunk_size * chunk_size).max(tail_end);
    [end..tail_end, head..offset]
}

/// If `path` is `from` or lies below `from/`, the same path re-rooted under
/// `to`; `None` otherwise. Shared by the write buffer and the open-handle
/// table so a rename re-keys both identically.
pub(crate) fn rekey_path(path: &str, from: &str, to: &str) -> Option<String> {
    if path == from {
        return Some(to.to_string());
    }
    let from_dir = from.trim_end_matches('/');
    let rest = path.strip_prefix(from_dir)?.strip_prefix('/')?;
    Some(format!("{}/{}", to.trim_end_matches('/'), rest))
}

/// Write buffer that accumulates data and flushes chunk-aligned segments.
///
/// Each file path gets an independent [`FileWriteBuffer`] that tracks dirty
/// data per chunk index. When enough complete chunks have accumulated,
/// those chunks can be extracted via [`flush`](Self::flush). Remaining
/// (possibly partial) chunks can be extracted with
/// [`flush_all`](Self::flush_all).
pub struct WriteBuffer {
    /// Chunk size in bytes (default 128 MB).
    chunk_size: usize,
    /// WI-01 — `flush()` holds back complete chunks until at least this many
    /// are available, amortizing the allocate+commit round trip over
    /// several chunks. `1` (the default from `new()`) reproduces the
    /// pre-WI-01 per-chunk-flush behaviour exactly.
    flush_batch_chunks: usize,
    /// Per-file dirty cap (`batch_limits(..).1`; `DIRTY_BYTES_CAP` from `new()`).
    dirty_cap: usize,
    /// Per-file write buffers, keyed by file path.
    buffers: DashMap<String, FileWriteBuffer>,
    /// Drained segments whose commit is in flight, by `begin_inflight` id:
    /// (path, [(absolute offset, bytes)]). Read-only overlay so a reader
    /// between the drain and the commit's return still sees the bytes
    /// (`Bytes` clones of the segments being uploaded: no extra copy).
    inflight: DashMap<u64, (String, Vec<(u64, Bytes)>)>,
    next_inflight: AtomicU64,
    /// Gap ranges of chunks drained with gaps (committed with zeros there),
    /// per path and chunk index. `seed_chunk` re-applies them so a re-seeded
    /// chunk stays out of `flush()` until the gap is written. Flush hints
    /// only, never content. Dropped on a gap-free drain, when writes cover
    /// them, on `remove`; re-keyed by `rename`, clipped by `truncate`.
    gap_hints: DashMap<String, BTreeMap<u32, Vec<Range<usize>>>>,
}

/// Remove `r` from every range in `gaps`.
fn subtract(gaps: &mut Vec<Range<usize>>, r: Range<usize>) {
    let mut out = Vec::with_capacity(gaps.len() + 1);
    for g in gaps.drain(..) {
        for p in [g.start..g.end.min(r.start), g.start.max(r.end)..g.end] {
            if p.start < p.end {
                out.push(p);
            }
        }
    }
    *gaps = out;
}

/// Ends an in-flight overlay on drop: after the commit's caller restored the
/// segments (failure) or merged the committed chunks (success).
pub struct InflightGuard<'a> {
    wb: &'a WriteBuffer,
    id: u64,
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.wb.inflight.remove(&self.id);
    }
}

impl InflightGuard<'_> {
    /// Keep the overlay registered past this guard (a background upload
    /// registers it before spawning, then `adopt_inflight`s it in the task).
    pub fn into_id(self) -> u64 {
        let id = self.id;
        std::mem::forget(self);
        id
    }
}

/// Internal per-file write buffer: a sparse map from chunk index to the
/// dirty bytes buffered for that chunk (CR-1-1).
struct FileWriteBuffer {
    #[allow(dead_code)] // kept for parity/debuggability; not read internally
    path: String,
    chunks: BTreeMap<u32, ChunkBuf>,
    /// Logical end of the buffered writes: highest `offset + len` written,
    /// kept across drains (whole-hole chunks have no `ChunkBuf`, the top
    /// chunk may already be committed); set by `truncate`.
    extent: u64,
}

impl FileWriteBuffer {
    fn new(path: &str) -> Self {
        Self { path: path.to_string(), chunks: BTreeMap::new(), extent: 0 }
    }
}

/// CR-1-1 — buffered bytes for one chunk index. `data.len() == dirty.end -
/// dirty.start`; bytes outside `dirty` within the chunk are simply not
/// resident (never materialised as zeros), so a partially-written chunk
/// costs only the bytes actually written, not a full `chunk_size` Vec.
struct ChunkBuf {
    /// Bytes covering `dirty`.
    data: Vec<u8>,
    /// Byte range within the chunk (`0..chunk_size`) that `data` covers.
    dirty: Range<usize>,
    /// True once `seed_chunk` merged the cluster's existing bytes in: the
    /// chunk is then safe to commit as a whole even though it is partial,
    /// and must not be seeded again (a re-seed used to overwrite newer
    /// buffered bytes with stale cluster bytes).
    seeded: bool,
    /// When this chunk's oldest uncommitted byte was buffered
    /// (`write.async_commit` age trigger, `aged_paths`).
    since: Instant,
    /// Zero-filled ranges inside `dirty` that no write covered yet: a later
    /// write landed beyond them (hole fill, merge gap). Such a chunk is
    /// complete but not `flush()`-eligible: the pending write would replace
    /// the uploaded zeros (knfsd out-of-order WRITEs). Bytes seeded from an
    /// existing cluster chunk are gap only where its `gap_hints` say so; a
    /// missing-index zero seed (`seed_hole`) is gap.
    gaps: Vec<Range<usize>>,
}

impl ChunkBuf {
    fn empty() -> Self {
        Self { data: Vec::new(), dirty: 0..0, seeded: false, since: Instant::now(), gaps: Vec::new() }
    }

    /// True iff this chunk's buffered data fully covers `0..chunk_size` —
    /// i.e. it is safe to commit as the chunk's whole content without an
    /// RMW seed first.
    fn is_complete(&self, chunk_size: usize) -> bool {
        self.dirty.start == 0 && self.dirty.end == chunk_size
    }

    /// Complete and no unwritten gap: eligible for the batch `flush()`.
    fn is_flushable(&self, chunk_size: usize) -> bool {
        self.is_complete(chunk_size) && self.gaps.is_empty()
    }

    /// `r` was written: it is no longer gap.
    fn cover(&mut self, r: Range<usize>) {
        subtract(&mut self.gaps, r);
    }

    fn add_gap(&mut self, r: Range<usize>) {
        if r.start < r.end {
            self.gaps.push(r);
        }
    }

    /// Merge a write at `offset_in_chunk` (always `< chunk_size`; the caller
    /// splits writes at chunk boundaries before calling this) into this
    /// chunk's buffer. Mirrors the pre-CR-1-1 contiguous-buffer merge logic,
    /// but every gap it can zero-fill is bounded to `0..chunk_size` — it can
    /// never reach into a neighbouring chunk.
    fn merge(&mut self, offset_in_chunk: usize, data: &[u8]) {
        let write_end = offset_in_chunk + data.len();
        self.cover(offset_in_chunk..write_end);

        if self.data.is_empty() {
            self.dirty = offset_in_chunk..write_end;
            self.data = data.to_vec();
            return;
        }

        let cur_start = self.dirty.start;
        let cur_end = self.dirty.end;

        if offset_in_chunk >= cur_start && offset_in_chunk <= cur_end {
            // Write starts within or immediately after the current range.
            let rel = offset_in_chunk - cur_start;
            let needed = rel + data.len();
            if needed > self.data.len() {
                self.data.resize(needed, 0);
            }
            self.data[rel..rel + data.len()].copy_from_slice(data);
            self.dirty = cur_start..cur_end.max(write_end);
        } else if offset_in_chunk < cur_start {
            // Write starts before the current range.
            if write_end >= cur_start {
                // Overlaps or is adjacent: merge into one contiguous range.
                let new_start = offset_in_chunk;
                let new_end = cur_end.max(write_end);
                let new_len = new_end - new_start;
                let mut new_data = vec![0u8; new_len];
                new_data[0..data.len()].copy_from_slice(data);

                let existing_offset = cur_start - new_start;
                let overwritten_end = write_end - cur_start;
                if overwritten_end < self.data.len() {
                    let dest_start = existing_offset + overwritten_end;
                    new_data[dest_start..existing_offset + self.data.len()]
                        .copy_from_slice(&self.data[overwritten_end..]);
                }

                self.dirty = new_start..new_end;
                self.data = new_data;
            } else {
                // Disjoint gap, but bounded within this one chunk (never
                // crosses a chunk boundary) — zero-fill just this gap.
                let new_start = offset_in_chunk;
                let new_end = cur_end;
                let new_len = new_end - new_start;
                let mut new_data = vec![0u8; new_len];
                new_data[0..data.len()].copy_from_slice(data);
                let existing_offset = cur_start - new_start;
                new_data[existing_offset..existing_offset + self.data.len()]
                    .copy_from_slice(&self.data);
                self.add_gap(write_end..cur_start);
                self.dirty = new_start..new_end;
                self.data = new_data;
            }
        } else {
            // Write starts strictly after the current range, with a gap —
            // again bounded within this chunk.
            let new_end = write_end;
            let new_len = new_end - cur_start;
            self.data.resize(new_len, 0);
            let rel = offset_in_chunk - cur_start;
            self.data[rel..rel + data.len()].copy_from_slice(data);
            self.add_gap(cur_end..offset_in_chunk);
            self.dirty.end = new_end;
        }
    }
}

/// A chunk-aligned segment ready for writing to a data node.
#[derive(Debug)]
pub struct ChunkSegment {
    /// Zero-based chunk index within the file.
    pub chunk_index: u32,
    /// The data for this chunk. Covers `[offset_in_chunk, offset_in_chunk +
    /// data.len())` within the chunk — the whole chunk when `needs_seed` is
    /// false.
    pub data: Bytes,
    /// Byte offset within the chunk where `data` begins. Always 0 for a
    /// complete (fully-covered) chunk; may be non-zero for a partial chunk
    /// drained by `flush_all` without full coverage.
    pub offset_in_chunk: usize,
    /// CR-1-1 (FUSE-1/FUSE-7) — true when `data` does NOT fully cover
    /// `0..chunk_size` and was not seeded via `seed_chunk`. The caller must never commit a segment with
    /// `needs_seed == true` as the chunk's whole content without first
    /// RMW-seeding it against the cluster's existing bytes (CR-1-2) — doing
    /// so would silently truncate or shift already-committed data.
    pub needs_seed: bool,
    /// Age of the chunk's oldest byte; `restore_segments` keeps it so a
    /// failed age-triggered commit retries on the next tick.
    pub since: Option<Instant>,
    /// Chunk-relative ranges of `data` that are zeros only because a later
    /// write landed beyond them (not yet written, above the cluster's
    /// content); `restore_segments` keeps them.
    pub gaps: Vec<Range<usize>>,
}

impl WriteBuffer {
    /// Create a new write buffer with the given chunk size in bytes.
    ///
    /// Flushes one complete chunk at a time (`flush_batch_chunks = 1`) — the
    /// original behaviour. Use [`new_with_batch`](Self::new_with_batch) to
    /// hold back several complete chunks per flush.
    pub fn new(chunk_size: usize) -> Self {
        Self {
            chunk_size,
            flush_batch_chunks: 1,
            dirty_cap: DIRTY_BYTES_CAP,
            buffers: DashMap::new(),
            inflight: DashMap::new(),
            next_inflight: AtomicU64::new(0),
            gap_hints: DashMap::new(),
        }
    }

    /// WI-01 — create a new write buffer that holds back complete chunks
    /// until at least `flush_batch_chunks` are available, so `flush()`
    /// amortizes the allocate+commit round trip over a batch instead of
    /// firing once per chunk. Clamped into `1..=MAX_FLUSH_BATCH_CHUNKS` AND
    /// down to whatever keeps `batch * chunk_size` within the
    /// `batch_limits` byte limit (64..=256 MB), which also sets the per-file
    /// dirty cap. `1` reproduces `new()`'s exact behaviour (the rollback value)
    /// at the default 4 MB chunk size. `flush_all` is unaffected — it
    /// always drains everything, batched or not, so held-back chunks still
    /// leave at fsync/close.
    pub fn new_with_batch(chunk_size: usize, flush_batch_chunks: usize) -> Self {
        Self::new_with_batch_inflight(chunk_size, flush_batch_chunks, 1)
    }

    /// `new_with_batch` with the dirty cap sized for `inflight` background
    /// batches per file (`dirty_cap_for`).
    pub fn new_with_batch_inflight(chunk_size: usize, flush_batch_chunks: usize, inflight: usize) -> Self {
        Self {
            chunk_size,
            flush_batch_chunks: effective_batch_chunks(flush_batch_chunks, chunk_size),
            dirty_cap: dirty_cap_for(flush_batch_chunks, inflight),
            buffers: DashMap::new(),
            inflight: DashMap::new(),
            next_inflight: AtomicU64::new(0),
            gap_hints: DashMap::new(),
        }
    }

    /// Write data into the buffer for the given file path at the specified
    /// offset. Splits the write at chunk boundaries and merges each piece
    /// into its own chunk's buffer — never touching, let alone zero-filling,
    /// a chunk the write doesn't overlap.
    ///
    /// Returns the number of bytes buffered (always equal to `data.len()`).
    #[instrument(skip(self, data), fields(path = %path, offset, len = data.len()))]
    pub fn write(&self, path: &str, offset: u64, data: &[u8]) -> usize {
        let len = data.len();
        if len == 0 || self.chunk_size == 0 {
            return 0;
        }

        let mut entry = self.buffers.entry(path.to_string()).or_insert_with(|| FileWriteBuffer::new(path));
        let buf = entry.value_mut();
        // A fully drained buffer's end is the snapshot's (or in-flight) now.
        let prev = if buf.chunks.is_empty() { 0 } else { buf.extent };
        buf.extent = prev.max(offset + len as u64);

        let chunk_size = self.chunk_size as u64;
        let mut remaining = data;
        let mut cur_offset = offset;
        while !remaining.is_empty() {
            let chunk_index = (cur_offset / chunk_size) as u32;
            let offset_in_chunk = (cur_offset % chunk_size) as usize;
            let take = remaining.len().min(self.chunk_size - offset_in_chunk);
            let (piece, rest) = remaining.split_at(take);
            buf.chunks
                .entry(chunk_index)
                .or_insert_with(ChunkBuf::empty)
                .merge(offset_in_chunk, piece);
            remaining = rest;
            cur_offset += take as u64;
        }
        drop(entry);
        self.cover_hints(path, offset, len);

        debug!(path, offset, len, "buffered write");
        len
    }

    /// `[offset, offset+len)` was written: drop it from the gap hints.
    fn cover_hints(&self, path: &str, offset: u64, len: usize) {
        let Some(mut h) = self.gap_hints.get_mut(path) else { return };
        let cs = self.chunk_size as u64;
        let end = offset + len as u64;
        let (first, last) = ((offset / cs) as u32, ((end - 1) / cs) as u32);
        let hit: Vec<u32> = h.range(first..=last).map(|(k, _)| *k).collect();
        for k in hit {
            let base = k as u64 * cs;
            let lo = offset.saturating_sub(base) as usize;
            let hi = (end - base).min(cs) as usize;
            let g = h.get_mut(&k).expect("key just listed");
            subtract(g, lo..hi);
            if g.is_empty() {
                h.remove(&k);
            }
        }
        if h.is_empty() {
            drop(h);
            self.gap_hints.remove_if(path, |_, h| h.is_empty());
        }
    }

    /// Drained `segs` of `path` (about to be committed): a segment with gaps
    /// leaves its gap ranges as the chunk's hint; a gap-free complete or
    /// seeded one drops it; an unseeded partial one adds its gaps to it.
    fn note_drained(&self, path: &str, segs: &[ChunkSegment]) {
        if segs.is_empty() || (segs.iter().all(|s| s.gaps.is_empty()) && !self.gap_hints.contains_key(path)) {
            return;
        }
        let mut h = self.gap_hints.entry(path.to_string()).or_default();
        for s in segs {
            let whole = s.offset_in_chunk == 0 && !s.needs_seed;
            let mut g = if whole { Vec::new() } else { h.remove(&s.chunk_index).unwrap_or_default() };
            g.extend(s.gaps.iter().cloned());
            if g.is_empty() {
                h.remove(&s.chunk_index);
            } else {
                h.insert(s.chunk_index, g);
            }
        }
        if h.is_empty() {
            drop(h);
            self.gap_hints.remove_if(path, |_, h| h.is_empty());
        }
    }

    /// Zero-fill `len` bytes at `offset`: the sparse gap below a write that
    /// landed past the logical end.
    /// The zeros are marked as gap: a chunk holding them stays out of
    /// `flush()` until a write covers them.
    pub fn write_hole(&self, path: &str, offset: u64, len: usize) {
        if self.write(path, offset, &vec![0u8; len]) == 0 {
            return;
        }
        let Some(mut e) = self.buffers.get_mut(path) else { return };
        let cs = self.chunk_size as u64;
        let (mut at, end) = (offset, offset + len as u64);
        while at < end {
            let (i, o) = ((at / cs) as u32, (at % cs) as usize);
            let n = ((end - at) as usize).min(self.chunk_size - o);
            if let Some(cb) = e.value_mut().chunks.get_mut(&i) {
                cb.add_gap(o..o + n);
            }
            at += n as u64;
        }
    }

    /// Extract all complete (chunk-aligned) segments from the buffer.
    ///
    /// Any chunk that is not yet fully covered remains in the buffer.
    /// Returns an empty `Vec` if fewer than `flush_batch_chunks` complete
    /// chunks are available.
    #[instrument(skip(self), fields(path = %path))]
    pub fn flush(&self, path: &str) -> Vec<ChunkSegment> {
        self.drain(path, false, |buf| self.flush_locked(buf)).0
    }

    /// `flush` + `begin_inflight` in one step (write()'s batch flush and
    /// `spawn_bg_upload`); no overlay when nothing was drained.
    pub fn flush_inflight(&self, path: &str) -> (Vec<ChunkSegment>, Option<InflightGuard<'_>>) {
        self.drain(path, true, |buf| self.flush_locked(buf))
    }

    fn flush_locked(&self, buf: &mut FileWriteBuffer) -> Vec<ChunkSegment> {
        let ready = buf
            .chunks
            .values()
            .filter(|cb| cb.is_flushable(self.chunk_size))
            .count();
        if ready < self.flush_batch_chunks {
            return Vec::new();
        }
        // One batch only: `flush_batch_chunks` complete chunks are within the
        // `batch_limits` byte limit (`effective_batch_chunks`).
        let segments = buf.extract_complete_chunks(self.chunk_size, self.flush_batch_chunks);
        debug!(
            path = %buf.path,
            num_segments = segments.len(),
            remaining = buf.chunks.len(),
            "flushed complete chunks"
        );
        segments
    }

    /// Drain `path` with `f` under its buffer entry's write lock. With
    /// `inflight`, the drained segments are registered as in-flight BEFORE
    /// that lock drops: `visible_size`/`overlay` read the overlay under the
    /// entry lock, so they never see the bytes in neither place.
    fn drain(
        &self,
        path: &str,
        inflight: bool,
        f: impl FnOnce(&mut FileWriteBuffer) -> Vec<ChunkSegment>,
    ) -> (Vec<ChunkSegment>, Option<InflightGuard<'_>>) {
        let Some(mut entry) = self.buffers.get_mut(path) else { return (Vec::new(), None) };
        let segments = f(entry.value_mut());
        // Lock order buffers -> inflight (as in visible_size/overlay).
        let guard = (inflight && !segments.is_empty()).then(|| self.begin_inflight(path, &segments));
        drop(entry);
        self.note_drained(path, &segments);
        (segments, guard)
    }

    /// Extract every buffered chunk, including partial (not fully covered)
    /// ones. This should be called when the file is being closed or when
    /// all buffered data must be persisted regardless of alignment.
    #[instrument(skip(self), fields(path = %path))]
    pub fn flush_all(&self, path: &str) -> Vec<ChunkSegment> {
        self.drain(path, false, |buf| self.flush_all_locked(buf)).0
    }

    /// `flush_all` + `begin_inflight` in one step (commit_all).
    pub fn flush_all_inflight(&self, path: &str) -> (Vec<ChunkSegment>, Option<InflightGuard<'_>>) {
        self.drain(path, true, |buf| self.flush_all_locked(buf))
    }

    fn flush_all_locked(&self, buf: &mut FileWriteBuffer) -> Vec<ChunkSegment> {
        let segments = buf.extract_all_chunks(self.chunk_size);
        debug!(
            path = %buf.path,
            num_segments = segments.len(),
            "flushed all chunks (including partial)"
        );
        segments
    }

    /// `write.async_commit` age trigger: up to `limit` paths whose oldest
    /// buffered byte is at least `max_age` old at `now`, oldest first.
    pub fn aged_paths(&self, now: Instant, max_age: Duration, limit: usize) -> Vec<String> {
        let mut aged: Vec<(Instant, String)> = self
            .buffers
            .iter()
            .filter_map(|e| {
                let oldest = e.value().chunks.values().map(|cb| cb.since).min()?;
                (now.saturating_duration_since(oldest) >= max_age).then(|| (oldest, e.key().clone()))
            })
            .collect();
        aged.sort();
        aged.into_iter().take(limit).map(|(_, p)| p).collect()
    }

    /// Dirty-cap (`dirty_cap()`) release valve: if `dirty + incoming >= cap`, drain
    /// every gap-free complete chunk outside `keep` (the chunks the pending
    /// write touches and has just seeded), then gappy complete ones until
    /// under the cap. With `partial` (async_commit), if still at the cap,
    /// also partial ones outside `keep`. Ascending by chunk index.
    pub fn cap_drain(
        &self,
        path: &str,
        incoming: usize,
        cap: usize,
        partial: bool,
        keep: RangeInclusive<u32>,
    ) -> Vec<ChunkSegment> {
        self.drain(path, false, |buf| self.cap_drain_locked(buf, incoming, cap, partial, keep)).0
    }

    /// `cap_drain` + `begin_inflight` in one step (write()'s cap drains).
    pub fn cap_drain_inflight(
        &self,
        path: &str,
        incoming: usize,
        cap: usize,
        partial: bool,
        keep: RangeInclusive<u32>,
    ) -> (Vec<ChunkSegment>, Option<InflightGuard<'_>>) {
        self.drain(path, true, |buf| self.cap_drain_locked(buf, incoming, cap, partial, keep))
    }

    fn cap_drain_locked(
        &self,
        buf: &mut FileWriteBuffer,
        incoming: usize,
        cap: usize,
        partial: bool,
        keep: RangeInclusive<u32>,
    ) -> Vec<ChunkSegment> {
        let mut dirty: usize = buf.chunks.values().map(|cb| cb.data.len()).sum();
        if dirty.saturating_add(incoming) < cap {
            return Vec::new();
        }
        let mut segs = Vec::new();
        // Pass 0: complete gap-free chunks (all). Pass 1, still at the cap:
        // complete chunks with an unwritten gap, only until under the cap
        // (each one is likely re-uploaded once the gap is written). Pass 2
        // (async only), still at the cap: partial ones.
        // Never a `keep` chunk: draining a chunk the pending write just
        // seeded would leave its bytes in a fresh unseeded chunk.
        let class = |cb: &ChunkBuf| match (cb.is_complete(self.chunk_size), cb.gaps.is_empty()) {
            (true, true) => 0,
            (true, false) => 1,
            (false, _) => 2,
        };
        for pass in 0..3 {
            if (pass > 0 && dirty.saturating_add(incoming) < cap) || (pass == 2 && !partial) {
                break;
            }
            let drain: Vec<u32> = buf
                .chunks
                .iter()
                .filter(|(k, cb)| !keep.contains(k) && class(cb) == pass)
                .map(|(k, _)| *k)
                .collect();
            for k in drain {
                if pass == 1 && dirty.saturating_add(incoming) < cap {
                    break;
                }
                if let Some(cb) = buf.chunks.remove(&k) {
                    dirty -= cb.data.len();
                    segs.push(ChunkSegment {
                        chunk_index: k,
                        offset_in_chunk: cb.dirty.start,
                        needs_seed: pass == 2 && !cb.seeded,
                        since: Some(cb.since),
                        gaps: cb.gaps,
                        data: Bytes::from(cb.data),
                    });
                }
            }
        }
        segs.sort_by_key(|s| s.chunk_index);
        segs
    }

    /// Keep `segs` (just drained from `path`) visible to `overlay` and
    /// `visible_size` until the returned guard drops. Hold it across the
    /// upload and the restore or post-commit merge.
    pub fn begin_inflight(&self, path: &str, segs: &[ChunkSegment]) -> InflightGuard<'_> {
        let id = self.next_inflight.fetch_add(1, Ordering::Relaxed);
        let cs = self.chunk_size as u64;
        let parts = segs
            .iter()
            .map(|s| (s.chunk_index as u64 * cs + s.offset_in_chunk as u64, s.data.clone()))
            .collect();
        self.inflight.insert(id, (path.to_string(), parts));
        InflightGuard { wb: self, id }
    }

    /// The id the next `begin_inflight` takes. Ids of one path are taken
    /// under its buffer entry lock (`drain`), which `restore_segments` also
    /// takes, so read after a restore every batch of the path drained before
    /// it has a lower id and every one drained after it a higher or equal one.
    pub fn next_inflight_id(&self) -> u64 {
        self.next_inflight.load(Ordering::Relaxed)
    }

    /// Guard for an overlay registered earlier (`InflightGuard::into_id`).
    pub fn adopt_inflight(&self, id: u64) -> InflightGuard<'_> {
        InflightGuard { wb: self, id }
    }

    /// Remove the buffer for a given path entirely, and the in-flight overlay
    /// of the path and everything below it (unlink / rename-over: the old
    /// file's drained bytes must not show under the path).
    pub fn remove(&self, path: &str) {
        self.buffers.remove(path);
        self.gap_hints.retain(|p, _| rekey_path(p, path, path).is_none());
        self.inflight.retain(|_, (p, _)| rekey_path(p, path, path).is_none());
    }

    /// True iff `chunk_index` of `path` has any buffered bytes.
    pub fn has_chunk(&self, path: &str, chunk_index: u32) -> bool {
        self.buffers.get(path).is_some_and(|e| e.value().chunks.contains_key(&chunk_index))
    }

    /// Drop the given chunks of `path` (a failed write's own seeds).
    pub fn remove_chunks(&self, path: &str, chunk_indices: &[u32]) {
        let Some(mut e) = self.buffers.get_mut(path) else { return };
        for k in chunk_indices {
            e.value_mut().chunks.remove(k);
        }
        if e.value().chunks.is_empty() {
            drop(e);
            self.buffers.remove_if(path, |_, b| b.chunks.is_empty());
        }
    }

    /// Move the buffer of `from` (and, for a directory rename, of every
    /// path below `from/`) to the matching key under `to`, so a rename of an
    /// open file (`a.part` -> `a`) keeps its unflushed bytes. An existing
    /// buffer under a destination key is replaced (rename overwrites).
    pub fn rename(&self, from: &str, to: &str) {
        let keys: Vec<String> = self
            .buffers
            .iter()
            .map(|e| e.key().clone())
            .filter(|k| rekey_path(k, from, to).is_some())
            .collect();
        for k in keys {
            let (Some(new_key), Some((_, mut buf))) =
                (rekey_path(&k, from, to), self.buffers.remove(&k))
            else {
                continue;
            };
            buf.path = new_key.clone();
            self.buffers.insert(new_key, buf);
        }
        let keys: Vec<String> =
            self.gap_hints.iter().map(|e| e.key().clone()).filter(|k| rekey_path(k, from, to).is_some()).collect();
        for k in keys {
            if let (Some(new_key), Some((_, h))) = (rekey_path(&k, from, to), self.gap_hints.remove(&k)) {
                self.gap_hints.insert(new_key, h);
            }
        }
        for mut e in self.inflight.iter_mut() {
            if let Some(np) = rekey_path(&e.value().0, from, to) {
                e.value_mut().0 = np;
            }
        }
    }

    /// True iff `chunk_index` still needs an RMW seed before a partial
    /// write: it is neither fully covered nor already seeded.
    pub fn chunk_needs_seed(&self, path: &str, chunk_index: u32) -> bool {
        !self
            .buffers
            .get(path)
            .and_then(|e| {
                e.value()
                    .chunks
                    .get(&chunk_index)
                    .map(|cb| cb.seeded || cb.is_complete(self.chunk_size))
            })
            .unwrap_or(false)
    }

    /// Merge the cluster's existing bytes of `chunk_index` UNDER whatever is
    /// already buffered for it (buffered bytes win), then mark it seeded.
    /// Idempotent: a second call is a no-op, so a seed can never overwrite
    /// a newer write. Bytes between the end of `existing` and the buffered
    /// range read as zeros (the file ended inside this chunk). The chunk's
    /// gap hint (it was committed with gaps) becomes gap again.
    pub fn seed_chunk(&self, path: &str, chunk_index: u32, existing: &[u8]) {
        self.seed_inner(path, chunk_index, existing, false)
    }

    /// `seed_chunk` for an index the cluster has no chunk at (hole below
    /// EOF): `zeros` are the hole's content but never written, so the
    /// seeded range outside the buffered bytes is gap.
    pub fn seed_hole(&self, path: &str, chunk_index: u32, zeros: &[u8]) {
        self.seed_inner(path, chunk_index, zeros, true)
    }

    fn seed_inner(&self, path: &str, chunk_index: u32, existing: &[u8], hole: bool) {
        if existing.is_empty() || self.chunk_size == 0 {
            return;
        }
        let existing = &existing[..existing.len().min(self.chunk_size)];
        let hint = self.gap_hints.get(path).and_then(|h| h.get(&chunk_index).cloned()).unwrap_or_default();
        let mut entry = self.buffers.entry(path.to_string()).or_insert_with(|| FileWriteBuffer::new(path));
        if entry.chunks.is_empty() {
            entry.extent = 0;
        }
        let cb = entry
            .value_mut()
            .chunks
            .entry(chunk_index)
            .or_insert_with(ChunkBuf::empty);
        if cb.seeded {
            return;
        }
        // Never-written seed bytes: all of a hole seed, else the gaps the
        // chunk was last committed with. Buffered bytes are not gap.
        let mut unwritten = if hole { vec![0..existing.len()] } else { hint };
        for g in &mut unwritten {
            g.end = g.end.min(existing.len());
        }
        if !cb.data.is_empty() {
            subtract(&mut unwritten, cb.dirty.clone());
        }
        let mut merged = existing.to_vec();
        if !cb.data.is_empty() {
            // Zeros between the cluster's end and the buffered bytes: unwritten.
            cb.add_gap(merged.len()..cb.dirty.start);
            if merged.len() < cb.dirty.end {
                merged.resize(cb.dirty.end, 0);
            }
            merged[cb.dirty.clone()].copy_from_slice(&cb.data);
        }
        for g in unwritten {
            cb.add_gap(g);
        }
        cb.dirty = 0..merged.len();
        cb.data = merged;
        cb.seeded = true;
    }

    /// CO-1-8 (b) — drop buffered bytes at or above `new_size`, keeping the
    /// buffered prefix `[0..new_size)` intact.
    pub fn truncate(&self, path: &str, new_size: u64) {
        // In-flight parts too: drop those at/above new_size, shorten a straddler.
        for mut e in self.inflight.iter_mut() {
            let (p, parts) = e.value_mut();
            if p == path {
                parts.retain_mut(|(start, data)| {
                    data.truncate(new_size.saturating_sub(*start) as usize);
                    !data.is_empty()
                });
            }
        }
        if self.chunk_size > 0 {
            if let Some(mut h) = self.gap_hints.get_mut(path) {
                let cs = self.chunk_size as u64;
                h.retain(|&k, g| {
                    subtract(g, new_size.saturating_sub(k as u64 * cs).min(cs) as usize..usize::MAX);
                    !g.is_empty()
                });
                if h.is_empty() {
                    drop(h);
                    self.gap_hints.remove_if(path, |_, h| h.is_empty());
                }
            }
        }
        let mut entry = match self.buffers.get_mut(path) {
            Some(e) => e,
            None => return,
        };
        let buf = entry.value_mut();
        buf.extent = new_size;
        let chunk_size = self.chunk_size;
        if chunk_size == 0 {
            return;
        }
        let cutoff_chunk = (new_size / chunk_size as u64) as u32;
        let cutoff_in_chunk = (new_size % chunk_size as u64) as usize;

        let at_or_above: Vec<u32> = buf
            .chunks
            .range(cutoff_chunk..)
            .map(|(k, _)| *k)
            .collect();
        for k in at_or_above {
            if k == cutoff_chunk && cutoff_in_chunk > 0 {
                let mut drop_entirely = false;
                if let Some(cb) = buf.chunks.get_mut(&k) {
                    // A fully buffered chunk is committable without a seed;
                    // cutting it makes it partial, so remember the
                    // completeness or the commit guard refuses it later.
                    cb.seeded |= cb.is_complete(chunk_size);
                    if cb.dirty.start >= cutoff_in_chunk {
                        drop_entirely = true;
                    } else {
                        let keep_end = cutoff_in_chunk.min(cb.dirty.end);
                        let keep_len = keep_end - cb.dirty.start;
                        cb.data.truncate(keep_len);
                        cb.dirty.end = keep_end;
                        cb.cover(keep_end..chunk_size);
                    }
                }
                if drop_entirely {
                    buf.chunks.remove(&k);
                }
            } else {
                buf.chunks.remove(&k);
            }
        }

        if buf.chunks.is_empty() {
            drop(entry);
            self.buffers.remove(path);
        }
    }

    /// CO-1-8 (a) — re-insert segments that were extracted by `flush` /
    /// `flush_all` but failed to reach the cluster, so a later flush re-sends
    /// them instead of dropping the bytes. Restores each segment at its
    /// original absolute offset (`chunk_index * chunk_size +
    /// offset_in_chunk`), so a partial (needs_seed) segment round-trips to
    /// exactly the range it covered, not the whole chunk.
    pub fn restore_segments(&self, path: &str, segments: &[ChunkSegment]) {
        let chunk_size = self.chunk_size as u64;
        for seg in segments {
            let abs_offset = seg.chunk_index as u64 * chunk_size + seg.offset_in_chunk as u64;
            self.write(path, abs_offset, seg.data.as_ref());
            if let Some(mut e) = self.buffers.get_mut(path) {
                if let Some(cb) = e.value_mut().chunks.get_mut(&seg.chunk_index) {
                    if !seg.needs_seed && seg.offset_in_chunk == 0 {
                        // A seeded partial chunk stays seeded across the restore.
                        cb.seeded = true;
                    }
                    if let Some(since) = seg.since {
                        cb.since = cb.since.min(since);
                    }
                    // The write above covered the segment; its gaps are still unwritten.
                    for g in &seg.gaps {
                        cb.add_gap(g.clone());
                    }
                }
            }
        }
    }

    /// Resident chunk indices and total buffered bytes for `path`; used to
    /// report exactly what a refused commit loses when the buffer is dropped.
    pub fn pending_summary(&self, path: &str) -> (Vec<u32>, usize) {
        self.buffers
            .get(path)
            .map(|e| {
                let c = &e.value().chunks;
                (c.keys().copied().collect(), c.values().map(|cb| cb.data.len()).sum())
            })
            .unwrap_or_default()
    }

    /// Check whether a file has any buffered dirty data.
    #[cfg(test)]
    pub fn has_dirty_data(&self, path: &str) -> bool {
        self.buffers
            .get(path)
            .map(|e| !e.value().chunks.is_empty())
            .unwrap_or(false)
    }

    #[cfg(test)]
    fn gap_hint(&self, path: &str, chunk_index: u32) -> Option<Vec<Range<usize>>> {
        self.gap_hints.get(path).and_then(|h| h.get(&chunk_index).cloned())
    }

    /// Total buffered bytes for `path`, summed across every resident chunk
    /// (CR-1-1) — used by callers to enforce `dirty_cap()`.
    pub fn dirty_bytes(&self, path: &str) -> usize {
        self.buffers
            .get(path)
            .map(|e| e.value().chunks.values().map(|cb| cb.data.len()).sum())
            .unwrap_or(0)
    }

    /// Write-buffer memory of the whole process: buffered bytes of every
    /// path plus every in-flight drain (background batches and commits).
    /// Summed on demand (no counter to drift across the many buffer
    /// mutations); O(buffered chunks), like `dirty_bytes`.
    pub fn total_bytes(&self) -> usize {
        let buffered: usize = self
            .buffers
            .iter()
            .map(|e| e.value().chunks.values().map(|cb| cb.data.len()).sum::<usize>())
            .sum();
        let inflight: usize = self
            .inflight
            .iter()
            .map(|e| e.value().1.iter().map(|(_, d)| d.len()).sum::<usize>())
            .sum();
        buffered + inflight
    }

    /// CR-1-1/CR-1-2 — true iff `chunk_index` is currently buffered AND its
    /// buffered range fully covers `0..chunk_size`, i.e. it is already safe
    /// to commit without an RMW seed.
    #[cfg(test)]
    pub fn chunk_is_fully_buffered(&self, path: &str, chunk_index: u32) -> bool {
        self.buffers
            .get(path)
            .and_then(|e| {
                e.value()
                    .chunks
                    .get(&chunk_index)
                    .map(|cb| cb.is_complete(self.chunk_size))
            })
            .unwrap_or(false)
    }

    /// Logical end of the file as seen by the buffer: the buffered end
    /// (`FileWriteBuffer.extent`, kept across partial drains: the top chunk
    /// may be committed, whole-hole chunks are never buffered) or the end of
    /// a resident chunk (a seed), whichever is larger. `None` when nothing
    /// is buffered (fully drained: in-flight overlay, then the snapshot).
    ///
    /// Used by the F-021/F-022 fix to decide where O_APPEND writes land and
    /// to size sparse-hole zero fills.
    pub fn logical_size(&self, path: &str) -> Option<u64> {
        let entry = self.buffers.get(path)?;
        self.buffered_end(entry.value())
    }

    fn buffered_end(&self, buf: &FileWriteBuffer) -> Option<u64> {
        if buf.chunks.is_empty() {
            return None;
        }
        let chunk_size = self.chunk_size as u64;
        let end = buf
            .chunks
            .iter()
            .map(|(idx, cb)| *idx as u64 * chunk_size + cb.dirty.end as u64)
            .max();
        (buf.extent > 0).then_some(buf.extent).max(end)
    }

    /// What a reader sees as the end of `path` beyond the committed size:
    /// `logical_size` or the end of an in-flight drained segment.
    /// The overlay is read under the buffer entry's lock: a drain
    /// (`flush_all_inflight`) registers its overlay and a restore refills
    /// the buffer under the write lock, both before the overlay drops, so
    /// the bytes are always in one of the two. No entry: no drain can run,
    /// and the overlay read first covers a restore that creates one.
    pub fn visible_size(&self, path: &str) -> Option<u64> {
        let inflight = || {
            self.inflight
                .iter()
                .filter(|e| e.value().0 == path)
                .flat_map(|e| e.value().1.iter().map(|(o, d)| o + d.len() as u64).collect::<Vec<_>>())
                .max()
        };
        let before = inflight();
        let Some(entry) = self.buffers.get(path) else { return before };
        self.buffered_end(entry.value()).max(inflight())
    }

    /// Overlay any buffered data for `path` that overlaps the byte range
    /// `[abs_offset, abs_offset + dst.len())` onto `dst`.
    ///
    /// F-029 — without this, a read on an open fd that follows a write on
    /// the same fd returns the *pre-write* chunk content (or empty for a
    /// brand-new file), because writes only live in this buffer until they
    /// flush at chunk-boundary or release-time. SQLite (and every database)
    /// breaks on the very first `CREATE TABLE` because the schema page is
    /// written, then read back, then nothing is there.
    ///
    /// Returns the number of bytes the buffer covered.
    pub fn overlay(&self, path: &str, abs_offset: u64, dst: &mut [u8]) -> usize {
        if dst.is_empty() {
            return 0;
        }
        // In-flight drained segments first (oldest first), the buffer on top:
        // its bytes are newer. Both under the buffer entry's read lock (order
        // buffers -> inflight, as `visible_size`): a drain registers its
        // overlay and a restore refills the buffer under the write lock, both
        // while the overlay exists, so a read never sees the bytes in neither.
        loop {
            let Some(entry) = self.buffers.get(path) else {
                // No entry: no drain can run; a restore creating one drops its
                // overlay only after, so re-check the entry.
                let n = self.overlay_inflight(path, abs_offset, dst);
                if !self.buffers.contains_key(path) {
                    return n;
                }
                continue;
            };
            return self.overlay_inflight(path, abs_offset, dst) + self.overlay_buffer(entry.value(), abs_offset, dst);
        }
    }

    fn overlay_inflight(&self, path: &str, abs_offset: u64, dst: &mut [u8]) -> usize {
        let mut ids: Vec<u64> = self
            .inflight
            .iter()
            .filter(|e| e.value().0 == path)
            .map(|e| *e.key())
            .collect();
        ids.sort_unstable();
        let req_end = abs_offset + dst.len() as u64;
        let mut total = 0usize;
        for id in ids {
            let Some(e) = self.inflight.get(&id) else { continue };
            for (start, data) in &e.value().1 {
                let lo = abs_offset.max(*start);
                let hi = req_end.min(start + data.len() as u64);
                if hi > lo {
                    let n = (hi - lo) as usize;
                    let d = (lo - abs_offset) as usize;
                    let s = (lo - start) as usize;
                    dst[d..d + n].copy_from_slice(&data[s..s + n]);
                    total += n;
                }
            }
        }
        total
    }

    fn overlay_buffer(&self, buf: &FileWriteBuffer, abs_offset: u64, dst: &mut [u8]) -> usize {
        if buf.chunks.is_empty() {
            return 0;
        }
        let chunk_size = self.chunk_size as u64;
        let req_start = abs_offset;
        let req_end = abs_offset + dst.len() as u64;
        let start_chunk = (req_start / chunk_size) as u32;
        let end_chunk = ((req_end - 1) / chunk_size) as u32;

        let mut total = 0usize;
        for chunk_index in start_chunk..=end_chunk {
            let Some(cb) = buf.chunks.get(&chunk_index) else { continue };
            let chunk_base = chunk_index as u64 * chunk_size;
            let buf_start = chunk_base + cb.dirty.start as u64;
            let buf_end = chunk_base + cb.dirty.end as u64;
            let overlap_start = req_start.max(buf_start);
            let overlap_end = req_end.min(buf_end);
            if overlap_end <= overlap_start {
                continue;
            }
            let src_off = (overlap_start - buf_start) as usize;
            let dst_off = (overlap_start - req_start) as usize;
            let n = (overlap_end - overlap_start) as usize;
            dst[dst_off..dst_off + n].copy_from_slice(&cb.data[src_off..src_off + n]);
            total += n;
        }
        total
    }

    /// Get the current chunk size.
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// Per-file dirty-byte cap (dirty + in-flight in async_commit mode).
    pub fn dirty_cap(&self) -> usize {
        self.dirty_cap
    }
}

impl FileWriteBuffer {
    /// Extract up to `limit` complete (fully-covered) chunk-aligned
    /// segments, lowest chunk indices first, removing them from the map.
    fn extract_complete_chunks(&mut self, chunk_size: usize, limit: usize) -> Vec<ChunkSegment> {
        let complete_keys: Vec<u32> = self
            .chunks
            .iter()
            .filter(|(_, cb)| cb.is_flushable(chunk_size))
            .map(|(k, _)| *k)
            .take(limit)
            .collect();

        let mut segments = Vec::with_capacity(complete_keys.len());
        for k in complete_keys {
            if let Some(cb) = self.chunks.remove(&k) {
                segments.push(ChunkSegment {
                    chunk_index: k,
                    data: Bytes::from(cb.data),
                    offset_in_chunk: 0,
                    needs_seed: false,
                    since: Some(cb.since),
                    gaps: Vec::new(),
                });
            }
        }
        segments
    }

    /// Extract every resident chunk (complete or partial), ascending by
    /// chunk index, draining the map entirely.
    fn extract_all_chunks(&mut self, chunk_size: usize) -> Vec<ChunkSegment> {
        let keys: Vec<u32> = self.chunks.keys().copied().collect();
        let mut segments = Vec::with_capacity(keys.len());
        for k in keys {
            if let Some(cb) = self.chunks.remove(&k) {
                let complete = cb.is_complete(chunk_size);
                segments.push(ChunkSegment {
                    chunk_index: k,
                    offset_in_chunk: cb.dirty.start,
                    needs_seed: !complete && !cb.seeded,
                    since: Some(cb.since),
                    gaps: cb.gaps,
                    data: Bytes::from(cb.data),
                });
            }
        }
        segments
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Bug BW — a write larger than chunk_size MUST split into
    // ceil(N/chunk_size) chunk-aligned segments, with the tail carrying the
    // remainder. The writebuf logic was always correct here; the corruption
    // came from the FUSE filesystem constructing it with a 128 MB chunk size
    // while file metadata recorded the volume's 4 MB (filesystem.rs now drives
    // it from config.client.chunk_size_mb). This guards the segmentation
    // contract at a realistic 4 MB chunk size.
    #[test]
    fn bw_large_write_splits_into_chunk_size_segments() {
        let chunk = 4 * 1024 * 1024; // 4 MB
        let wb = WriteBuffer::new(chunk);
        let data = vec![7u8; 10 * 1024 * 1024]; // 10 MB
        wb.write("/big", 0, &data);

        let segs = wb.flush_all("/big");
        assert_eq!(segs.len(), 3, "10 MB at 4 MB chunks -> 3 segments");
        assert_eq!(segs[0].chunk_index, 0);
        assert_eq!(segs[1].chunk_index, 1);
        assert_eq!(segs[2].chunk_index, 2);
        assert_eq!(segs[0].data.len(), chunk);
        assert_eq!(segs[1].data.len(), chunk);
        assert_eq!(segs[2].data.len(), 2 * 1024 * 1024, "tail carries the remainder");
        let total: usize = segs.iter().map(|s| s.data.len()).sum();
        assert_eq!(total, data.len(), "no bytes lost across the split");
    }

    #[test]
    fn test_simple_sequential_write() {
        let wb = WriteBuffer::new(10); // 10-byte chunks for testing
        wb.write("file1", 0, &[1, 2, 3, 4, 5]);
        wb.write("file1", 5, &[6, 7, 8, 9, 10]);

        let segments = wb.flush("file1");
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].chunk_index, 0);
        assert_eq!(segments[0].data.as_ref(), &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn test_partial_chunk_not_flushed() {
        let wb = WriteBuffer::new(10);
        wb.write("file1", 0, &[1, 2, 3]);

        let segments = wb.flush("file1");
        assert!(segments.is_empty());
        assert!(wb.has_dirty_data("file1"));
    }

    #[test]
    fn test_flush_all_includes_partial() {
        let wb = WriteBuffer::new(10);
        wb.write("file1", 0, &[1, 2, 3]);

        let segments = wb.flush_all("file1");
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].chunk_index, 0);
        assert_eq!(segments[0].data.as_ref(), &[1, 2, 3]);
    }

    #[test]
    fn test_multiple_chunks() {
        let wb = WriteBuffer::new(4);
        wb.write("file1", 0, &[1, 2, 3, 4, 5, 6, 7, 8, 9]);

        // Batch of 1 (`new`): one chunk per flush.
        let mut segments = wb.flush("file1");
        segments.extend(wb.flush("file1"));
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].chunk_index, 0);
        assert_eq!(segments[0].data.as_ref(), &[1, 2, 3, 4]);
        assert_eq!(segments[1].chunk_index, 1);
        assert_eq!(segments[1].data.as_ref(), &[5, 6, 7, 8]);

        // Remaining byte
        assert!(wb.has_dirty_data("file1"));
        let remaining = wb.flush_all("file1");
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].data.as_ref(), &[9]);
    }

    #[test]
    fn test_overlapping_write() {
        let wb = WriteBuffer::new(10);
        wb.write("file1", 0, &[1, 2, 3, 4, 5]);
        wb.write("file1", 3, &[30, 40, 50, 60]); // overlap at 3..7

        let segments = wb.flush_all("file1");
        assert_eq!(segments.len(), 1);
        assert_eq!(
            segments[0].data.as_ref(),
            &[1, 2, 3, 30, 40, 50, 60]
        );
    }

    #[test]
    fn test_no_data_for_path() {
        let wb = WriteBuffer::new(10);
        let segments = wb.flush("nonexistent");
        assert!(segments.is_empty());
        assert!(!wb.has_dirty_data("nonexistent"));
    }

    #[test]
    fn test_remove() {
        let wb = WriteBuffer::new(10);
        wb.write("file1", 0, &[1, 2, 3]);
        assert!(wb.has_dirty_data("file1"));
        wb.remove("file1");
        assert!(!wb.has_dirty_data("file1"));
    }

    /// Regression for F-029: writes that haven't flushed yet must be visible
    /// to reads on the same fd, otherwise SQLite (and every DB engine) sees
    /// an empty file after `CREATE TABLE`.
    #[test]
    fn overlay_returns_buffered_bytes_to_reader() {
        let wb = WriteBuffer::new(4096);
        wb.write("db", 0, b"PAGE1-000");
        wb.write("db", 4096, b"PAGE2-000");

        // Read a window covering both pages with a fresh dst buffer.
        let mut dst = vec![0u8; 9]; // first 9 bytes of file
        let n = wb.overlay("db", 0, &mut dst);
        assert_eq!(n, 9);
        assert_eq!(&dst, b"PAGE1-000");

        // Read at offset 4096 — should see the second write.
        let mut dst = vec![0u8; 9];
        let n = wb.overlay("db", 4096, &mut dst);
        assert_eq!(n, 9);
        assert_eq!(&dst, b"PAGE2-000");

        // Read past the buffered region returns zero coverage.
        let mut dst = vec![0u8; 9];
        let n = wb.overlay("db", 8192, &mut dst);
        assert_eq!(n, 0);
        assert_eq!(&dst, &[0u8; 9]);

        // Partial overlap with a SEPARATE buffer (no zero-fill artefact):
        // single 9-byte write at offset 0, then ask for [4..12) — buffer
        // covers [0..9), so 5 bytes overlap.
        let wb2 = WriteBuffer::new(4096);
        wb2.write("x", 0, b"PAGE1-000");
        let mut dst = vec![0u8; 8];
        let n = wb2.overlay("x", 4, &mut dst);
        assert_eq!(n, 5, "5 buffered bytes overlap the request");
        assert_eq!(&dst[..5], b"1-000", "the trailing 5 bytes of PAGE1-000");
    }

    /// Regression for F-021/F-022: `logical_size` must report `offset + len`
    /// so the filesystem layer knows where to append and where to zero-fill.
    #[test]
    fn logical_size_tracks_end_of_buffered_writes() {
        let wb = WriteBuffer::new(1024);
        assert_eq!(wb.logical_size("x"), None, "no buffer yet");
        wb.write("x", 0, &[0u8; 50]);
        assert_eq!(wb.logical_size("x"), Some(50), "initial write");
        wb.write("x", 50, &[0u8; 10]);
        assert_eq!(wb.logical_size("x"), Some(60), "appended write extends size");
        wb.write("x", 0, &[0u8; 5]);
        assert_eq!(
            wb.logical_size("x"),
            Some(60),
            "overlapping write does not shrink logical end"
        );
    }

    #[test]
    fn wi01_flush_holds_chunks_until_batch_is_full() {
        let chunk = 4 * 1024 * 1024; // 4 MiB
        let wb = WriteBuffer::new_with_batch(chunk, 4);
        let mib = 1024 * 1024;

        // Feed 16 MiB as 16 sequential 1 MiB writes, byte value = write index.
        for i in 0..16u8 {
            let data = vec![i; mib];
            wb.write("/f", (i as u64) * mib as u64, &data);
            let segs = wb.flush("/f");
            if i < 15 {
                assert!(
                    segs.is_empty(),
                    "flush after write {} must hold back (only {} complete chunk(s) so far)",
                    i + 1,
                    (i as usize + 1) * mib / chunk
                );
            } else {
                assert_eq!(segs.len(), 4, "batch of 4 emitted once the 16th MiB completes it");
                let mibs_per_chunk = chunk / mib; // 4
                for (idx, seg) in segs.iter().enumerate() {
                    assert_eq!(seg.chunk_index, idx as u32);
                    assert_eq!(seg.data.len(), chunk);
                    // Each chunk is `mibs_per_chunk` sequential 1 MiB writes,
                    // each filled with its own write index as the byte value.
                    for k in 0..mibs_per_chunk {
                        let write_idx = (idx * mibs_per_chunk + k) as u8;
                        let piece = &seg.data.as_ref()[k * mib..(k + 1) * mib];
                        assert!(
                            piece.iter().all(|&b| b == write_idx),
                            "chunk {idx} sub-piece {k} must carry write-index byte {write_idx}"
                        );
                    }
                }
            }
        }
        assert!(!wb.has_dirty_data("/f"), "batch fully drained, nothing left dirty");
    }

    // Audit 8 — a batch carries at most `flush_batch_chunks` complete chunks
    // (lowest indices first), however many are ready: else one background
    // batch held the whole backlog (up to the dirty cap). flush_all still
    // drains everything.
    #[test]
    fn flush_takes_at_most_one_batch_lowest_indices_first() {
        let chunk = 4 * 1024 * 1024;
        let wb = WriteBuffer::new_with_batch(chunk, 4);
        for i in (0..10u32).rev() {
            wb.write("/f", i as u64 * chunk as u64, &vec![i as u8; chunk]);
        }
        let idx = |s: Vec<ChunkSegment>| s.iter().map(|s| s.chunk_index).collect::<Vec<u32>>();
        assert_eq!(idx(wb.flush("/f")), vec![0, 1, 2, 3], "10 ready: one batch of 4");
        assert_eq!(idx(wb.flush("/f")), vec![4, 5, 6, 7], "boundary: exactly 4 ready");
        assert!(wb.flush("/f").is_empty(), "2 < 4 ready: held back");
        assert_eq!(idx(wb.flush_all("/f")), vec![8, 9]);
        let bytes: usize = WriteBuffer::new_with_batch(chunk, 64).flush_batch_chunks * chunk;
        assert!(bytes <= batch_limits(64).0, "a batch never exceeds the byte limit");
    }

    // WI-01 — flush_all must still drain held-back chunks at fsync/close,
    // even when the batch threshold was never reached.
    #[test]
    fn wi01_flush_all_drains_held_back_chunks() {
        let chunk = 4 * 1024 * 1024;
        let wb = WriteBuffer::new_with_batch(chunk, 8);
        let data = vec![9u8; chunk * 3]; // 3 complete chunks, batch is 8
        wb.write("/f", 0, &data);

        let held = wb.flush("/f");
        assert!(held.is_empty(), "3 < 8, flush must hold back");

        let drained = wb.flush_all("/f");
        assert_eq!(drained.len(), 3, "flush_all must not strand held-back chunks");
        for (idx, seg) in drained.iter().enumerate() {
            assert_eq!(seg.chunk_index, idx as u32);
        }
        assert!(!wb.has_dirty_data("/f"));
    }

    // WI-01 / CO-1-8a — restore_segments must round-trip a MULTI-segment
    // batch (previously only a single held-back chunk, or the final
    // flush_all drain, exercised this path). Write 4 full chunks + a
    // partial 5th, flush the batch out, restore it, then flush_all and
    // confirm the exact byte sequence comes back.
    #[test]
    fn wi01_restore_segments_round_trips_a_batch() {
        let chunk = 4 * 1024 * 1024;
        let wb = WriteBuffer::new_with_batch(chunk, 4);

        let mut full = Vec::new();
        for i in 0..4u8 {
            full.extend(std::iter::repeat(i).take(chunk));
        }
        let tail = vec![4u8; 1024 * 1024]; // partial 5th chunk
        wb.write("/f", 0, &full);
        wb.write("/f", full.len() as u64, &tail);

        let segments = wb.flush("/f");
        assert_eq!(segments.len(), 4, "batch of 4 full chunks emitted");
        assert!(wb.has_dirty_data("/f"), "partial 5th chunk still buffered");

        // Simulate a cluster-write failure: restore the drained batch.
        wb.restore_segments("/f", &segments);
        assert!(wb.has_dirty_data("/f"));

        let mut emitted: Vec<(u32, Vec<u8>)> = Vec::new();
        for seg in wb.flush_all("/f") {
            emitted.push((seg.chunk_index, seg.data.to_vec()));
        }

        assert_eq!(emitted.len(), 5, "4 restored full chunks + the partial 5th");
        let mut expected: Vec<u8> = full.clone();
        expected.extend_from_slice(&tail);
        let mut got: Vec<u8> = Vec::new();
        for (idx, (chunk_index, bytes)) in emitted.iter().enumerate() {
            assert_eq!(*chunk_index, idx as u32, "chunk indices ascend 0..=4");
            got.extend_from_slice(bytes);
        }
        assert_eq!(got, expected, "restored batch re-emits every original byte");
        assert!(!wb.has_dirty_data("/f"));
    }

    // WI-01 / H1 — effective_batch_chunks must clamp both by count
    // (1..=MAX_FLUSH_BATCH_CHUNKS) and by byte budget (FLUSH_BATCH_MAX_BYTES),
    // and the resulting batch * chunk_size must never exceed the budget for
    // any chunk size up to the budget itself.
    #[test]
    fn wi01_h1_effective_batch_is_byte_bounded() {
        let mib = 1024 * 1024;
        assert_eq!(effective_batch_chunks(4, 4 * mib), 4);
        assert_eq!(effective_batch_chunks(32, 4 * mib), 32);
        // Default batch on large chunks: unchanged from before the 64-chunk
        // limits (64 MiB batch), only a raised count grows it.
        assert_eq!(effective_batch_chunks(4, 32 * mib), 2);
        assert_eq!(effective_batch_chunks(4, 64 * mib), 1);
        assert_eq!(effective_batch_chunks(4, 128 * mib), 1);
        assert_eq!(effective_batch_chunks(64, 4 * mib), 64);
        assert_eq!(effective_batch_chunks(0, 4 * mib), 1);
        assert_eq!(effective_batch_chunks(4, 0), 4);

        let cases: &[(usize, usize)] = &[
            (4, 4 * mib),
            (32, 4 * mib),
            (4, 32 * mib),
            (4, 64 * mib),
            (4, 128 * mib),
            (64, 4 * mib),
        ];
        for &(configured, size) in cases {
            if size == 0 || size > FLUSH_BATCH_MAX_BYTES {
                continue;
            }
            let effective = effective_batch_chunks(configured, size);
            assert!(
                effective * size <= FLUSH_BATCH_MAX_BYTES,
                "batch {effective} * chunk_size {size} must stay within {FLUSH_BATCH_MAX_BYTES}"
            );
        }
    }

    /// multibatch — the cap holds `n` uploading batches plus one filling;
    /// `n = 1` (and a nonsense 0) is the single-batch cap.
    #[test]
    fn dirty_cap_scales_with_inflight_batches() {
        let mib = 1024 * 1024;
        for fbc in [0, 4, 16, 17, 64, 128] {
            assert_eq!(dirty_cap_for(fbc, 1), batch_limits(fbc).1, "fbc {fbc}");
            assert_eq!(dirty_cap_for(fbc, 0), batch_limits(fbc).1, "fbc {fbc}");
        }
        assert_eq!(dirty_cap_for(16, 4), 320 * mib);
        assert_eq!(dirty_cap_for(64, 16), 17 * 256 * mib);
        assert_eq!(WriteBuffer::new_with_batch_inflight(4 * mib, 16, 4).dirty_cap(), 320 * mib);
        assert_eq!(WriteBuffer::new_with_batch_inflight(4 * mib, 4, 1).dirty_cap(), DIRTY_BYTES_CAP);
    }

    /// bigbatch — batch byte limit / dirty cap derive from the configured
    /// batch: floors 64/128 MiB (defaults unchanged), ceiling 256/512 MiB.
    #[test]
    fn bigbatch_limits_derive_from_configured_batch() {
        let mib = 1024 * 1024;
        assert_eq!(batch_limits(4), (64 * mib, 128 * mib), "defaults unchanged");
        assert_eq!(batch_limits(16), (64 * mib, 128 * mib));
        assert_eq!(batch_limits(17), (68 * mib, 136 * mib), "just above the floor");
        assert_eq!(batch_limits(64), (256 * mib, 512 * mib));
        assert_eq!(batch_limits(128), (256 * mib, 512 * mib), "count clamped to 64");
        assert_eq!(batch_limits(0), (64 * mib, 128 * mib));
        assert_eq!(effective_batch_chunks(64, 4 * mib), 64, "64 is in range");
        assert_eq!(effective_batch_chunks(65, 4 * mib), 64, "65 clamped");
        // A large-chunk volume with the default batch keeps the old per-chunk
        // flush and 128 MiB cap: the limits follow the configured count only.
        assert_eq!(effective_batch_chunks(4, 64 * mib), 1, "64 MiB batch / 64 MiB chunk");
        assert_eq!(WriteBuffer::new_with_batch(64 * mib, 4).dirty_cap(), DIRTY_BYTES_CAP);
        assert_eq!(effective_batch_chunks(64, 64 * mib), 4, "raised batch: 256 MiB / 64 MiB");
        let wb = WriteBuffer::new_with_batch(4 * mib, 64);
        assert_eq!((wb.flush_batch_chunks, wb.dirty_cap()), (64, 512 * mib));
        assert_eq!(WriteBuffer::new_with_batch(4 * mib, 4).dirty_cap(), DIRTY_BYTES_CAP);
        assert_eq!(WriteBuffer::new(4 * mib).dirty_cap(), DIRTY_BYTES_CAP);
    }

    // WI-01 / H1 — a chunk at or above the 256 MB batch ceiling must degrade
    // to per-chunk flushing regardless of the configured batch; the
    // reference-deployment 4 MB chunk size keeps the configured default of 4.
    #[test]
    fn wi01_h1_large_chunk_volume_flushes_per_chunk() {
        assert_eq!(WriteBuffer::new_with_batch(256 * 1024 * 1024, 4).flush_batch_chunks, 1);
        assert_eq!(WriteBuffer::new_with_batch(512 * 1024 * 1024, 4).flush_batch_chunks, 1);
        assert_eq!(WriteBuffer::new_with_batch(4 * 1024 * 1024, 4).flush_batch_chunks, 4);
    }

    // R1 (FUSE-1 memory half) — the cap drain must take EVERY complete
    // chunk immediately, ignoring flush_batch_chunks entirely. A caller
    // reacting to memory pressure (DIRTY_BYTES_CAP) can't wait for a batch
    // to fill.
    #[test]
    fn r1_flush_complete_ignores_batch_threshold() {
        let chunk = 4 * 1024 * 1024;
        let wb = WriteBuffer::new_with_batch(chunk, 4);
        for i in 0..40u32 {
            wb.write("/f", (i as u64) * chunk as u64, &vec![i as u8; chunk]);
        }
        assert_eq!(wb.dirty_bytes("/f"), 40 * chunk);

        // A normal flush() would emit exactly one batch of 4 (holding back
        // the rest); the cap drain must take everything at once.
        #[allow(clippy::reversed_empty_ranges)]
        let segs = wb.cap_drain("/f", 0, 0, false, 1..=0);
        assert_eq!(segs.len(), 40, "the cap drain must take every complete chunk");
        for (idx, seg) in segs.iter().enumerate() {
            assert_eq!(seg.chunk_index, idx as u32);
            assert!(!seg.needs_seed);
        }
        assert_eq!(wb.dirty_bytes("/f"), 0, "nothing left resident");
        assert!(!wb.has_dirty_data("/f"));
    }

    // CR-1-1 (FUSE-1) test (a): after a flushed sequential write of 3
    // chunks, a NEW write of 100 bytes at offset 0 must create ONLY a
    // chunk-0 entry marked `needs_seed`; chunks 1 and 2 (already flushed and
    // gone from the buffer) must never be re-emitted or re-materialised.
    #[test]
    fn cr_1_1_rewrite_offset_zero_marks_only_that_chunk_needs_seed() {
        let chunk = 1024;
        let wb = WriteBuffer::new(chunk);
        let data = vec![1u8; chunk * 3];
        wb.write("/f", 0, &data);
        let flushed = wb.flush_all("/f");
        assert_eq!(flushed.len(), 3, "3 full chunks flushed and drained");
        assert!(!wb.has_dirty_data("/f"));

        // Simulate a patch write at offset 0 after the flush (e.g.
        // `dd conv=notrunc` rewriting a header).
        wb.write("/f", 0, &vec![2u8; 100]);

        let segs = wb.flush_all("/f");
        assert_eq!(segs.len(), 1, "only chunk 0 is resident again");
        assert_eq!(segs[0].chunk_index, 0);
        assert!(segs[0].needs_seed, "partial rewrite must be marked needs_seed");
        assert_eq!(segs[0].data.len(), 100);
        assert!(
            segs.iter().all(|s| s.chunk_index != 1 && s.chunk_index != 2),
            "already-flushed chunks 1 and 2 must never be re-emitted"
        );
    }

    // CR-1-1 (FUSE-1) test (b): a write far into a large sparse file
    // followed by a write near the start must hold exactly two small
    // chunk-buf entries — never a multi-hundred-MB contiguous allocation.
    #[test]
    fn cr_1_1_scattered_writes_hold_only_touched_chunks() {
        let chunk = 4 * 1024 * 1024; // 4 MiB
        let wb = WriteBuffer::new(chunk);
        wb.write("/f", 100 * 1024 * 1024, &[9u8; 16]); // far chunk
        wb.write("/f", 4 * 1024 * 1024, &[8u8; 16]); // near chunk

        assert!(wb.dirty_bytes("/f") < 2 * chunk, "must not materialise the gap between them");
        assert_eq!(wb.dirty_bytes("/f"), 32, "only the actually-written bytes are resident");

        let segs = wb.flush_all("/f");
        assert_eq!(segs.len(), 2, "exactly the two touched chunks, nothing in between");
        let far_idx = (100 * 1024 * 1024u64) / chunk as u64;
        let near_idx = (4 * 1024 * 1024u64) / chunk as u64;
        let indices: Vec<u32> = segs.iter().map(|s| s.chunk_index).collect();
        assert!(indices.contains(&(far_idx as u32)));
        assert!(indices.contains(&(near_idx as u32)));
    }
}

#[cfg(test)]
mod audit_co_tests {
    use super::*;

    // CO-1-7: mid-chunk `extract_complete_chunks` sets `consumed = pos` and
    // `data.drain(..consumed)`, dropping the leading partial bytes
    // [0..bytes_to_complete_first] that were never emitted as a segment. Those
    // bytes (file offsets in the first, partially-buffered chunk) are silently
    // lost. The buffered bytes for every file offset MUST survive across the
    // full flush()/flush_all() drain path.
    #[test]
    fn co_1_7_midchunk_flush_does_not_drop_leading_partial_bytes() {
        // 10-byte chunks; write 20 bytes starting at file offset 5. This puts
        // the first touched chunk mid-chunk (offset_in_chunk = 5).
        let chunk = 10;
        let wb = WriteBuffer::new(chunk);
        let pattern: Vec<u8> = (0u8..20).collect(); // distinct bytes 0..19
        wb.write("/f", 5, &pattern);

        // Drive the same flush path the production code uses: flush() extracts
        // complete chunks, then flush_all() drains the remainder at close.
        let mut emitted: Vec<u8> = Vec::new();
        for seg in wb.flush("/f") {
            emitted.extend_from_slice(seg.data.as_ref());
        }
        for seg in wb.flush_all("/f") {
            emitted.extend_from_slice(seg.data.as_ref());
        }

        // The bytes written for file offsets 5..10 are pattern[0..5].
        // Assert they appear somewhere in the emitted output; today they are
        // drained without ever being emitted, so this fails (data loss).
        let leading = &pattern[0..5];
        let found = emitted
            .windows(leading.len())
            .any(|w| w == leading);
        assert!(
            found,
            "leading partial-chunk bytes (file offsets 5..10) must be emitted, not silently dropped; emitted={emitted:?}"
        );
    }

    // CO-1-8 (b): truncate must keep buffered bytes BELOW the new EOF. The old
    // filesystem path removed the whole buffer whenever buffered_eof > new_size,
    // discarding bytes the truncation never deleted.
    #[test]
    fn co_1_8b_truncate_keeps_bytes_below_new_size() {
        let wb = WriteBuffer::new(4096);
        let data: Vec<u8> = (0u8..100).collect(); // 100 distinct bytes @ offset 0
        wb.write("/f", 0, &data);
        assert_eq!(wb.logical_size("/f"), Some(100));

        wb.truncate("/f", 50);
        assert_eq!(
            wb.logical_size("/f"),
            Some(50),
            "buffer must shrink to the new size, not vanish"
        );

        // flush_all must still emit exactly the first 50 bytes, in order.
        let mut emitted: Vec<u8> = Vec::new();
        for seg in wb.flush_all("/f") {
            emitted.extend_from_slice(seg.data.as_ref());
        }
        assert_eq!(emitted, (0u8..50).collect::<Vec<u8>>());
    }

    // CO-1-8 (b): a truncate at/below the buffer start drops everything; a
    // truncate at/above the buffer end is a no-op.
    #[test]
    fn co_1_8b_truncate_edges() {
        let wb = WriteBuffer::new(4096);
        wb.write("/f", 10, &[1u8; 20]); // buffer covers [10, 30)

        // Cut at 10 (== buf_start) removes the whole buffer.
        wb.truncate("/f", 10);
        assert!(!wb.has_dirty_data("/f"), "cut at buffer start clears it");

        // Re-populate, then cut above the end: untouched.
        wb.write("/f", 10, &[2u8; 20]);
        wb.truncate("/f", 1000);
        assert_eq!(wb.dirty_bytes("/f"), 20, "cut above end keeps the bytes");
        assert_eq!(wb.logical_size("/f"), Some(1000), "and extends the end");
    }

    // CO-1-8 (a): on a cluster-flush error the FUSE layer must restore the
    // already-drained segments into the buffer so a later flush re-sends them
    // (a dropped restore loses the bytes while fsync returns Ok). Restoring the
    // segments must reproduce exactly the bytes a subsequent flush_all emits.
    #[test]
    fn co_1_8a_restore_segments_round_trips_through_flush() {
        let chunk = 10;
        let wb = WriteBuffer::new(chunk);
        let data: Vec<u8> = (0u8..25).collect(); // 2 full chunks + a 5-byte tail
        wb.write("/f", 0, &data);

        // Drain everything as the flush path does.
        let segments = wb.flush_all("/f");
        assert!(!segments.is_empty());
        assert!(!wb.has_dirty_data("/f"), "flush_all drained the buffer");

        // Simulate the cluster write failing: restore the drained segments.
        wb.restore_segments("/f", &segments);
        assert!(wb.has_dirty_data("/f"), "restored bytes are buffered again");
        assert_eq!(wb.logical_size("/f"), Some(25));

        // A retry flush must emit the original bytes, in order.
        let mut emitted: Vec<u8> = Vec::new();
        for seg in wb.flush_all("/f") {
            emitted.extend_from_slice(seg.data.as_ref());
        }
        assert_eq!(emitted, data, "restored buffer re-sends every original byte");
    }

    /// Mimics `DtfsFilesystem::write`'s RMW step for a 7-byte cluster chunk.
    fn rmw_write(wb: &WriteBuffer, path: &str, offset: u64, data: &[u8], seed_calls: &mut u32) {
        if wb.chunk_needs_seed(path, 0) {
            *seed_calls += 1;
            wb.seed_chunk(path, 0, &[7u8; 7]);
        }
        wb.write(path, offset, data);
    }

    #[test]
    fn seed_once_two_writes_into_partial_chunk_keep_both_payloads() {
        let wb = WriteBuffer::new(16);
        let mut seeds = 0;
        rmw_write(&wb, "/f", 0, b"AA", &mut seeds);
        rmw_write(&wb, "/f", 5, b"BB", &mut seeds);
        assert_eq!(seeds, 1, "a partial chunk is seeded at most once");
        let segs = wb.flush_all("/f");
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].offset_in_chunk, 0);
        assert!(!segs[0].needs_seed, "seeded chunk is committable whole");
        assert_eq!(segs[0].data.as_ref(), b"AABB");
    }

    #[test]
    fn seed_never_overwrites_buffered_bytes_and_is_idempotent() {
        let wb = WriteBuffer::new(16);
        wb.write("/f", 2, b"AA");
        assert!(wb.chunk_needs_seed("/f", 0), "unseeded partial needs a seed");
        wb.seed_chunk("/f", 0, &[1u8; 10]);
        wb.seed_chunk("/f", 0, &[9u8; 10]);
        assert!(!wb.chunk_needs_seed("/f", 0));
        let mut dst = [0u8; 10];
        assert_eq!(wb.overlay("/f", 0, &mut dst), 10);
        assert_eq!(&dst, &[1, 1, b'A', b'A', 1, 1, 1, 1, 1, 1]);
    }

    #[test]
    fn unseeded_partial_segment_still_needs_seed() {
        let wb = WriteBuffer::new(16);
        wb.write("/f", 0, b"AA");
        let segs = wb.flush_all("/f");
        assert!(segs[0].needs_seed);
    }

    #[test]
    fn seeded_flag_survives_restore_segments() {
        let wb = WriteBuffer::new(16);
        wb.seed_chunk("/f", 0, &[7u8; 7]);
        wb.write("/f", 1, b"A");
        let segs = wb.flush_all("/f");
        wb.restore_segments("/f", &segs);
        assert!(!wb.chunk_needs_seed("/f", 0));
        assert!(!wb.flush_all("/f")[0].needs_seed);
    }

    #[test]
    fn rename_moves_buffer_and_children_not_siblings() {
        let wb = WriteBuffer::new(16);
        wb.write("/d/a.part", 0, b"one");
        wb.write("/d/sub/x", 0, b"two");
        wb.write("/dd/y", 0, b"sib");
        wb.write("/e", 0, b"old");
        wb.rename("/d", "/e");
        assert!(!wb.has_dirty_data("/d/a.part"));
        let mut b = [0u8; 3];
        assert_eq!(wb.overlay("/e/a.part", 0, &mut b), 3);
        assert_eq!(&b, b"one");
        assert_eq!(wb.overlay("/e/sub/x", 0, &mut b), 3);
        assert_eq!(&b, b"two");
        assert_eq!(wb.overlay("/dd/y", 0, &mut b), 3, "sibling prefix untouched");
        assert_eq!(wb.overlay("/e", 0, &mut b), 3, "unrelated /e kept (not a rename target key)");
        assert_eq!(&b, b"old");
    }

    #[test]
    fn rename_file_keeps_unflushed_data_and_overwrites_target() {
        let wb = WriteBuffer::new(16);
        wb.write("/a.part", 0, b"payload");
        wb.write("/a", 0, b"stale!!");
        wb.rename("/a.part", "/a");
        assert!(!wb.has_dirty_data("/a.part"));
        let segs = wb.flush_all("/a");
        assert_eq!(segs[0].data.as_ref(), b"payload");
    }

    #[test]
    fn rekey_path_boundaries() {
        assert_eq!(rekey_path("/a", "/a", "/b").as_deref(), Some("/b"));
        assert_eq!(rekey_path("/a/c", "/a", "/b").as_deref(), Some("/b/c"));
        assert_eq!(rekey_path("/a/c", "/a/", "/b/").as_deref(), Some("/b/c"));
        assert_eq!(rekey_path("/ab", "/a", "/b"), None);
        assert_eq!(rekey_path("/x", "/a", "/b"), None);
    }

    // Truncating a fully buffered chunk used to leave it partial AND
    // unseeded; the commit guard then refused it (data loss at close).
    #[test]
    fn truncate_of_complete_chunk_stays_committable() {
        let wb = WriteBuffer::new(8);
        wb.write("/f", 0, &[7u8; 8]);
        wb.truncate("/f", 3);
        let segs = wb.flush_all("/f");
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].data.as_ref(), &[7u8; 3]);
        assert_eq!(segs[0].offset_in_chunk, 0);
        assert!(!segs[0].needs_seed, "complete-then-cut chunk must be committable");
    }

    // Negative: a genuinely partial chunk stays unseeded after a cut.
    #[test]
    fn truncate_of_partial_chunk_stays_unseeded() {
        let wb = WriteBuffer::new(8);
        wb.write("/f", 0, &[7u8; 5]);
        wb.truncate("/f", 3);
        assert!(wb.flush_all("/f")[0].needs_seed);
    }

    // Hole below EOF: a zero base of min(chunk, eof - start) bytes makes a
    // partial write into the hole chunk commit as one zero-padded chunk.
    #[test]
    fn zero_seed_makes_hole_chunk_committable() {
        let wb = WriteBuffer::new(8);
        // eof 20: chunk 1 covers 8..16, fully below EOF.
        let base = vec![0u8; seed_len_within_eof_for_test(8, 1, 20)];
        wb.write("/f", 10, b"ab");
        assert!(wb.chunk_needs_seed("/f", 1));
        wb.seed_chunk("/f", 1, &base);
        let segs = wb.flush_all("/f");
        assert!(!segs[0].needs_seed);
        assert_eq!(segs[0].offset_in_chunk, 0);
        assert_eq!(segs[0].data.as_ref(), &[0, 0, b'a', b'b', 0, 0, 0, 0]);
    }

    fn seed_len_within_eof_for_test(cs: u64, idx: u64, eof: u64) -> usize {
        eof.saturating_sub(idx * cs).min(cs) as usize
    }

    #[test]
    fn pending_summary_reports_indices_and_bytes() {
        let wb = WriteBuffer::new(8);
        assert_eq!(wb.pending_summary("/f"), (vec![], 0));
        wb.write("/f", 0, &[1u8; 10]);
        assert_eq!(wb.pending_summary("/f"), (vec![0, 1], 10));
    }
}

/// `write.async_commit`: age trigger, cap drain of partial chunks, and a
/// model check of the buffer under the async policy.
#[cfg(test)]
mod async_commit_tests {
    use super::*;
    use std::collections::BTreeMap;

    const CS: usize = 8;
    const AGE: Duration = Duration::from_secs(30);

    #[test]
    fn aged_paths_waits_for_the_oldest_byte_and_drops_drained_paths() {
        let wb = WriteBuffer::new(CS);
        let t0 = Instant::now();
        wb.write("/a", 0, b"abc");
        wb.write("/b", 0, &[1u8; CS]);
        assert!(wb.aged_paths(t0 + AGE - Duration::from_secs(1), AGE, 8).is_empty());
        let mut aged = wb.aged_paths(Instant::now() + AGE, AGE, 8);
        aged.sort();
        assert_eq!(aged, vec!["/a".to_string(), "/b".to_string()]);
        assert_eq!(wb.aged_paths(Instant::now() + AGE, AGE, 1).len(), 1, "limit per tick");
        let _ = wb.flush_all("/b");
        assert_eq!(wb.aged_paths(Instant::now() + AGE, AGE, 8), vec!["/a".to_string()]);
    }

    #[test]
    fn restore_keeps_the_original_age_so_a_failed_commit_retries_next_tick() {
        let wb = WriteBuffer::new(CS);
        wb.write("/a", 0, b"abc");
        let mut segs = wb.flush_all("/a");
        let t0 = Instant::now();
        segs[0].since = Some(t0);
        while Instant::now() <= t0 {}
        wb.restore_segments("/a", &segs);
        assert_eq!(wb.aged_paths(t0 + AGE, AGE, 8), vec!["/a".to_string()]);
        // A fresh write after t0 is not aged at t0 + AGE.
        wb.write("/c", 0, b"x");
        assert!(!wb.aged_paths(t0 + AGE, AGE, 8).contains(&"/c".to_string()));
    }

    #[test]
    fn cap_drain_async_also_drains_partials_outside_keep() {
        for partial in [false, true] {
            let wb = WriteBuffer::new_with_batch(CS, 4);
            wb.write("/f", 0, &[1; CS]); // complete chunk 0
            wb.write("/f", CS as u64 + 1, &[2; 5]); // partial chunk 1
            wb.write("/f", 3 * CS as u64, &[3; 4]); // partial chunk 3 (pending write)
            assert_eq!(wb.dirty_bytes("/f"), 17);
            assert!(wb.cap_drain("/f", 0, 18, partial, 3..=3).is_empty(), "17 < 18: nothing");
            // 17 + 4 >= 13: drain complete; 9 + 4 still meets 13 exactly.
            let segs = wb.cap_drain("/f", 4, 13, partial, 3..=3);
            let idx: Vec<u32> = segs.iter().map(|s| s.chunk_index).collect();
            if partial {
                assert_eq!(idx, vec![0, 1]);
                assert_eq!(wb.dirty_bytes("/f"), 4, "only the kept chunk stays");
            } else {
                assert_eq!(idx, vec![0], "sync mode: complete chunks only");
                assert_eq!(wb.dirty_bytes("/f"), 9);
            }
        }
    }

    /// Found by the model below: a chunk the pending write just seeded to
    /// full is complete; draining it would leave the write's bytes in a fresh
    /// unseeded chunk that the commit guard then refuses.
    #[test]
    fn cap_drain_async_keeps_the_seeded_chunk_of_the_pending_write() {
        let wb = WriteBuffer::new_with_batch(CS, 4);
        wb.seed_chunk("/f", 2, &[9; CS]);
        wb.write("/f", 0, &[1; 3]);
        let segs = wb.cap_drain("/f", 1, 1, true, 2..=2);
        assert_eq!(segs.iter().map(|s| s.chunk_index).collect::<Vec<_>>(), vec![0]);
        assert!(!wb.chunk_needs_seed("/f", 2), "still seeded for the pending write");
    }

    /// Default mode: the cap drain takes complete chunks only, and never the
    /// one the pending write just seeded to full (its bytes would land in a
    /// fresh unseeded chunk and be refused at close).
    #[test]
    fn cap_drain_default_keeps_the_seeded_chunk_of_the_pending_write() {
        let wb = WriteBuffer::new_with_batch(CS, 4);
        wb.write("/f", 0, &[1; CS]); // complete chunk 0
        wb.seed_chunk("/f", 2, &[9; CS]); // complete after the seed
        wb.write("/f", 3 * CS as u64, &[3; 2]); // partial: never in default mode
        let segs = wb.cap_drain("/f", 1, 1, false, 2..=2);
        assert_eq!(segs.iter().map(|s| s.chunk_index).collect::<Vec<_>>(), vec![0]);
        assert!(!wb.chunk_needs_seed("/f", 2), "still seeded for the pending write");
        assert_eq!(wb.dirty_bytes("/f"), CS + 2);
    }

    #[test]
    fn inflight_overlay_until_the_guard_drops_buffer_wins_rename_follows() {
        let wb = WriteBuffer::new(CS);
        wb.write("/f", 0, &[1; CS]);
        wb.write("/f", CS as u64, &[2; 3]);
        let segs = wb.flush_all("/f");
        let g = wb.begin_inflight("/f", &segs);
        assert_eq!(wb.logical_size("/f"), None, "drained");
        assert_eq!(wb.visible_size("/f"), Some(CS as u64 + 3));
        wb.write("/f", 1, &[9; 2]); // newer write over the in-flight chunk
        let mut v = vec![0u8; CS + 4];
        wb.overlay("/f", 0, &mut v);
        assert_eq!(&v[..4], &[1, 9, 9, 1]);
        assert_eq!(&v[CS..], &[2, 2, 2, 0]);
        wb.rename("/f", "/g");
        assert_eq!(wb.visible_size("/f"), None);
        assert_eq!(wb.visible_size("/g"), Some(CS as u64 + 3));
        drop(g);
        assert_eq!(wb.visible_size("/g"), Some(3), "only the newer write is left");
        let mut v = vec![0u8; 4];
        wb.overlay("/g", 0, &mut v);
        assert_eq!(v, [0, 9, 9, 0]);
    }

    fn drain_inflight<'a>(wb: &'a WriteBuffer, path: &str, data: &[u8]) -> InflightGuard<'a> {
        wb.write(path, 0, data);
        let segs = wb.flush_all(path);
        wb.begin_inflight(path, &segs)
    }

    /// unlink: the old file's in-flight bytes must not show under the path
    /// (or below it), a sibling sharing the prefix keeps its own; the
    /// guard of a removed entry drops as a no-op.
    #[test]
    fn remove_ends_the_inflight_overlay_of_path_and_subtree() {
        let wb = WriteBuffer::new(CS);
        let gf = drain_inflight(&wb, "/f", &[1; 3]);
        let gx = drain_inflight(&wb, "/d/x", &[1; 3]);
        let gdd = drain_inflight(&wb, "/dd", &[1; 3]);
        wb.remove("/f");
        wb.remove("/d");
        assert_eq!(wb.visible_size("/f"), None, "unlinked file still visible");
        assert_eq!(wb.visible_size("/d/x"), None);
        let mut v = [0u8; 3];
        assert_eq!(wb.overlay("/f", 0, &mut v), 0);
        assert_eq!(wb.visible_size("/dd"), Some(3), "sibling prefix kept");
        drop((gf, gx));
        assert_eq!(wb.visible_size("/dd"), Some(3));
        drop(gdd);
        assert_eq!(wb.visible_size("/dd"), None);
    }

    /// rename-over (rename_inner: remove(to) then rename(from, to)): only
    /// the moved file's in-flight bytes show under `to`.
    #[test]
    fn rename_over_drops_the_replaced_files_inflight_overlay() {
        let wb = WriteBuffer::new(CS);
        let _ga = drain_inflight(&wb, "/a", &[1; 3]);
        let _gb = drain_inflight(&wb, "/b", &[2; 5]);
        wb.remove("/b");
        wb.rename("/a", "/b");
        assert_eq!(wb.visible_size("/b"), Some(3), "replaced file's size leaks");
        let mut v = [0u8; 5];
        assert_eq!(wb.overlay("/b", 0, &mut v), 3);
        assert_eq!(v, [1, 1, 1, 0, 0]);
    }

    /// truncate clips in-flight parts: dropped at/above new_size, the
    /// straddling one shortened; growing is a no-op.
    #[test]
    fn truncate_clips_the_inflight_overlay() {
        let wb = WriteBuffer::new(CS);
        wb.write("/t", 0, &[1; CS]);
        wb.write("/t", CS as u64, &[2; 3]);
        let segs = wb.flush_all("/t");
        let _g = wb.begin_inflight("/t", &segs);
        wb.truncate("/t", CS as u64 + 10);
        assert_eq!(wb.visible_size("/t"), Some(CS as u64 + 3), "grow keeps it");
        wb.truncate("/t", CS as u64 + 1);
        assert_eq!(wb.visible_size("/t"), Some(CS as u64 + 1), "straddling part shortened");
        wb.truncate("/t", CS as u64);
        assert_eq!(wb.visible_size("/t"), Some(CS as u64), "part starting at new_size dropped");
        wb.truncate("/t", 3);
        let mut v = vec![0u8; CS + 4];
        assert_eq!(wb.overlay("/t", 0, &mut v), 3);
        assert!(v[..3].iter().all(|b| *b == 1) && v[3..].iter().all(|b| *b == 0));
        wb.truncate("/t", 0);
        assert_eq!(wb.visible_size("/t"), None);
    }

    #[test]
    fn cap_drain_async_stops_once_complete_chunks_suffice() {
        let wb = WriteBuffer::new_with_batch(CS, 4);
        wb.write("/f", 0, &[1; CS]);
        wb.write("/f", CS as u64, &[2; 3]);
        let segs = wb.cap_drain("/f", 1, 11, true, 5..=5);
        assert_eq!(segs.len(), 1, "3 + 1 < 11 after the complete drain");
        assert_eq!(wb.dirty_bytes("/f"), 3);
    }

    const P: &str = "/f";

    fn idx(segs: &[ChunkSegment]) -> Vec<u32> {
        segs.iter().map(|s| s.chunk_index).collect()
    }

    /// prod dtfs-lm-data01 (knfsd, out-of-order WRITEs): a write past the
    /// buffered end zero-fills the gap; that must not make chunks 0/1
    /// flushable and upload zeros the late write then replaces.
    #[test]
    fn gap_chunks_wait_for_the_late_write_then_flush_like_sequential() {
        let wb = WriteBuffer::new(CS);
        wb.write(P, 0, &[1; 4]);
        wb.write_hole(P, 4, 12);
        wb.write(P, 16, &[3; 2]);
        assert!(wb.flush(P).is_empty(), "gap chunks flushed while the gap is open");
        wb.write(P, 4, &[2; 4]);
        let first = wb.flush(P);
        assert_eq!(idx(&first), vec![0], "chunk 0 closed, chunk 1 still gappy");
        wb.write(P, 8, &[2; 8]);
        let second = wb.flush(P);
        assert_eq!(idx(&second), vec![1]);
        let seq = WriteBuffer::new(CS);
        seq.write(P, 0, &[1; 4]);
        seq.write(P, 4, &[2; 12]);
        let mut want = seq.flush(P);
        want.extend(seq.flush(P));
        assert_eq!(idx(&want), vec![0, 1]);
        assert_eq!(first[0].data, want[0].data);
        assert_eq!(second[0].data, want[1].data);
        assert!(first.iter().chain(&second).all(|s| !s.needs_seed && s.gaps.is_empty()));
    }

    /// Same for the in-chunk zero fill of `ChunkBuf::merge`.
    #[test]
    fn merge_gap_keeps_a_full_chunk_out_of_flush() {
        let wb = WriteBuffer::new(CS);
        wb.write(P, 0, &[1; 2]);
        wb.write(P, 5, &[1; 3]);
        assert!(wb.flush(P).is_empty(), "merge gap chunk flushed");
        wb.write(P, 2, &[2; 3]);
        assert_eq!(wb.flush(P)[0].data.as_ref(), &[1, 1, 2, 2, 2, 1, 1, 1]);
    }

    /// Unwritten = zeros: close/fsync/timer (`flush_all`) commit the gap.
    #[test]
    fn flush_all_commits_a_gappy_chunk_with_zeros() {
        let wb = WriteBuffer::new(CS);
        wb.write(P, 0, &[1; 2]);
        wb.write(P, 6, &[1; 2]);
        let segs = wb.flush_all(P);
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].data.as_ref(), &[1, 1, 0, 0, 0, 0, 1, 1]);
        assert!(!segs[0].needs_seed);
        assert_eq!(segs[0].gaps, vec![2..6]);
    }

    /// Bytes seeded from an EXISTING cluster chunk are content (even zeros):
    /// the chunk flushes. Zeros for a MISSING index are a gap; flush_all
    /// still commits them as the hole's zeros.
    #[test]
    fn existing_chunk_seed_is_content_missing_chunk_zeros_are_gap() {
        let wb = WriteBuffer::new(CS);
        wb.seed_chunk(P, 1, &[0; CS]);
        wb.write(P, 10, &[5; 2]);
        let segs = wb.flush(P);
        assert_eq!(idx(&segs), vec![1]);
        assert_eq!(segs[0].data.as_ref(), &[0, 0, 5, 5, 0, 0, 0, 0]);

        let wb = WriteBuffer::new(CS);
        wb.seed_hole(P, 1, &[0; CS]);
        wb.write(P, 10, &[5; 2]);
        assert!(wb.flush(P).is_empty(), "missing-chunk zero seed flushed as content");
        let segs = wb.flush_all(P);
        assert_eq!(segs[0].data.as_ref(), &[0, 0, 5, 5, 0, 0, 0, 0]);
        assert!(!segs[0].needs_seed);
        assert_eq!(segs[0].gaps, vec![0..2, 4..CS]);
    }

    /// prod dtfs-lm-data01 (x1.6): a write below EOF into an index the
    /// cluster has no chunk at (later chunks committed first) must not
    /// upload the zero-seeded chunk until the rest of it is written.
    #[test]
    fn missing_index_zero_seed_waits_for_the_rest_then_flushes_once() {
        let wb = WriteBuffer::new(CS);
        wb.seed_hole(P, 1, &[0; CS]);
        wb.write(P, 10, &[5; 2]);
        assert!(wb.flush(P).is_empty(), "zero seed of a missing chunk flushed as content");
        wb.write(P, 8, &[4; 2]);
        wb.write(P, 12, &[6; 4]);
        let segs = wb.flush(P);
        assert_eq!(idx(&segs), vec![1]);
        assert_eq!(segs[0].data.as_ref(), &[4, 4, 5, 5, 6, 6, 6, 6]);
        assert!(wb.flush_all(P).is_empty(), "uploaded more than once");

        // Bytes buffered before the hole seed are not gap.
        let wb = WriteBuffer::new(CS);
        wb.write(P, 8, &[4; 2]);
        wb.seed_hole(P, 1, &[0; CS]);
        wb.write(P, 10, &[5; 6]);
        assert_eq!(idx(&wb.flush(P)), vec![1], "buffered bytes marked as gap");
    }

    fn gappy(wb: &WriteBuffer, path: &str, i: u64) {
        wb.write(path, i * CS as u64, &[1; 2]);
        wb.write_hole(path, i * CS as u64 + 2, CS - 2);
    }

    /// A chunk committed with a gap (flush_all) and RMW-seeded again from
    /// the committed bytes (zeros at the gap) keeps the gap: no upload per
    /// later write. The hint goes away with the gap-free commit.
    #[test]
    fn reseeded_gappy_commit_stays_out_of_flush_until_the_gap_is_written() {
        let wb = WriteBuffer::new(CS);
        wb.write(P, 0, &[1; 2]);
        wb.write_hole(P, 2, 4);
        wb.write(P, 6, &[1; 2]);
        let committed = wb.flush_all(P);
        assert_eq!(committed[0].gaps, vec![2..6]);
        assert_eq!(wb.gap_hint(P, 0), Some(vec![2..6]));
        wb.seed_chunk(P, 0, &committed[0].data);
        wb.write(P, 2, &[2; 2]);
        assert!(wb.flush(P).is_empty(), "re-seeded committed chunk flushed with the gap open");
        wb.write(P, 4, &[3; 2]);
        let segs = wb.flush(P);
        assert_eq!(idx(&segs), vec![0]);
        assert_eq!(segs[0].data.as_ref(), &[1, 1, 2, 2, 3, 3, 1, 1]);
        assert_eq!(wb.gap_hint(P, 0), None, "gap-free commit keeps the hint");

        // Still gappy at the next flush_all: the hint is the remaining gap.
        let wb = WriteBuffer::new(CS);
        gappy(&wb, P, 0);
        let committed = wb.flush_all(P);
        wb.seed_chunk(P, 0, &committed[0].data);
        wb.write(P, 2, &[2; 2]);
        let _ = wb.flush_all(P);
        assert_eq!(wb.gap_hint(P, 0), Some(vec![4..CS]));
        // A write covering the rest drops it without a seed.
        wb.write(P, 4, &[2; CS - 4]);
        assert_eq!(wb.gap_hint(P, 0), None, "covered hint kept");
    }

    #[test]
    fn gap_hints_follow_remove_truncate_and_rename() {
        let wb = WriteBuffer::new(CS);
        for p in ["/a", "/d/x", "/dd", "/r", "/d2/y"] {
            gappy(&wb, p, 0);
            gappy(&wb, p, 1);
            let _ = wb.flush_all(p);
            assert_eq!(wb.gap_hint(p, 1), Some(vec![2..CS]), "{p}");
        }
        wb.remove("/a");
        wb.remove("/d");
        assert_eq!(wb.gap_hint("/a", 0), None, "unlink kept the hint");
        assert_eq!(wb.gap_hint("/d/x", 1), None, "subtree kept the hint");
        assert_eq!(wb.gap_hint("/dd", 0), Some(vec![2..CS]), "sibling prefix dropped");

        wb.truncate("/dd", CS as u64 + 4);
        assert_eq!(wb.gap_hint("/dd", 1), Some(vec![2..4]), "straddling hint not clipped");
        assert_eq!(wb.gap_hint("/dd", 0), Some(vec![2..CS]));
        wb.truncate("/dd", CS as u64);
        assert_eq!(wb.gap_hint("/dd", 1), None, "hint at new_size kept");
        wb.truncate("/dd", 3);
        assert_eq!(wb.gap_hint("/dd", 0), Some(vec![2..3]));
        wb.truncate("/dd", 2);
        assert_eq!(wb.gap_hint("/dd", 0), None);

        wb.rename("/r", "/s");
        wb.rename("/d2", "/e");
        assert_eq!(wb.gap_hint("/r", 0), None);
        assert_eq!(wb.gap_hint("/s", 0), Some(vec![2..CS]), "rename lost the hint");
        assert_eq!(wb.gap_hint("/d2/y", 1), None);
        assert_eq!(wb.gap_hint("/e/y", 1), Some(vec![2..CS]), "dir rename lost the hint");
    }

    /// Memory bound: the cap drain still drains re-seeded hinted (gappy)
    /// chunks when nothing else gets the buffer under the cap.
    #[test]
    fn cap_drain_drains_reseeded_hinted_chunks_when_only_they_remain() {
        let wb = WriteBuffer::new_with_batch(CS, 4);
        gappy(&wb, P, 0);
        gappy(&wb, P, 1);
        for s in wb.flush_all(P) {
            wb.seed_chunk(P, s.chunk_index, &s.data);
        }
        wb.write(P, 0, &[3; 1]);
        wb.write(P, CS as u64, &[3; 1]);
        assert!(wb.flush(P).is_empty());
        let segs = wb.cap_drain(P, 1, 2 * CS, true, 9..=9);
        assert_eq!(idx(&segs), vec![0], "hinted chunks drained as gap-free");
        assert_eq!(segs[0].gaps, vec![2..CS]);
        assert_eq!(wb.dirty_bytes(P), CS);
    }

    /// The cap drain still drains gappy chunks when nothing else brings the
    /// buffer under the cap (memory bound), but gap-free ones first.
    #[test]
    fn cap_drain_drains_gappy_chunks_only_when_needed() {
        for partial in [false, true] {
            let gappy = |wb: &WriteBuffer, i: u64| {
                wb.write(P, i * CS as u64, &[1; 2]);
                wb.write_hole(P, i * CS as u64 + 2, CS - 2);
            };
            let wb = WriteBuffer::new_with_batch(CS, 4);
            gappy(&wb, 0);
            gappy(&wb, 1);
            let segs = wb.cap_drain(P, 1, 2 * CS, partial, 9..=9);
            assert_eq!(idx(&segs), vec![0], "only gappy chunks: drain until under the cap");
            assert!(!segs[0].needs_seed);
            assert_eq!(segs[0].gaps, vec![2..CS]);
            assert_eq!(wb.dirty_bytes(P), CS);

            let wb = WriteBuffer::new_with_batch(CS, 4);
            gappy(&wb, 0);
            wb.write(P, CS as u64, &[2; CS]);
            assert_eq!(idx(&wb.cap_drain(P, 1, 2 * CS, partial, 9..=9)), vec![1], "gap-free first");
            assert_eq!(wb.dirty_bytes(P), CS);
        }
    }

    #[test]
    fn restore_after_failed_upload_keeps_the_gap() {
        let wb = WriteBuffer::new(CS);
        wb.write(P, 0, &[1; 2]);
        wb.write_hole(P, 2, CS - 2);
        let segs = wb.flush_all(P);
        wb.restore_segments(P, &segs);
        assert!(wb.flush(P).is_empty(), "restored chunk lost its gap marker");
        wb.write(P, 2, &[2; CS - 2]);
        assert_eq!(idx(&wb.flush(P)), vec![0]);
    }

    /// Model of the cluster plus filesystem.rs write/fsync/timer/truncate/
    /// release steps on top of the real `WriteBuffer` (cap scaled down).
    struct Sim {
        wb: WriteBuffer,
        async_on: bool,
        cap: usize,
        chunks: BTreeMap<u32, Vec<u8>>,
        size: u64,
        model: Vec<u8>,
        fail_next: bool,
        sticky: bool,
        clock: Duration,
        trace: Vec<String>,
        /// A write's cap drain failed: it returned EIO after dropping its own
        /// fresh seeds, so the bound is the cap plus one drain's overshoot.
        drain_failed: bool,
        /// Chunks the current write seeded that held no bytes before.
        fresh: Vec<u32>,
        /// Committed uploads per chunk index.
        uploads: BTreeMap<u32, u32>,
    }

    impl Sim {
        fn new(async_on: bool) -> Self {
            Self {
                wb: WriteBuffer::new_with_batch(CS, 2),
                async_on,
                cap: 40,
                chunks: BTreeMap::new(),
                size: 0,
                model: Vec::new(),
                fail_next: false,
                sticky: false,
                clock: Duration::ZERO,
                trace: Vec::new(),
                drain_failed: false,
                fresh: Vec::new(),
                uploads: BTreeMap::new(),
            }
        }

        /// flush_segments_to_cluster + segments_committable; the caller's
        /// restore on error.
        fn commit(&mut self, segs: Vec<ChunkSegment>) -> bool {
            if segs.is_empty() {
                return true;
            }
            let refused = segs.iter().any(|s| {
                s.offset_in_chunk > 0 || (s.needs_seed && self.chunks.contains_key(&s.chunk_index))
            });
            assert!(
                !refused,
                "policy produced an uncommittable segment: async={} {:?} trace={:?}",
                self.async_on,
                segs.iter()
                    .map(|s| (s.chunk_index, s.offset_in_chunk, s.needs_seed, s.data.len()))
                    .collect::<Vec<_>>(),
                self.trace
            );
            if std::mem::take(&mut self.fail_next) {
                self.wb.restore_segments(P, &segs);
                return false;
            }
            for s in segs {
                *self.uploads.entry(s.chunk_index).or_default() += 1;
                let end = s.chunk_index as u64 * CS as u64 + s.data.len() as u64;
                self.chunks.insert(s.chunk_index, s.data.to_vec());
                self.size = self.size.max(end);
            }
            true
        }

        fn committed_view(&self) -> Vec<u8> {
            (0..self.size)
                .map(|i| {
                    let (c, o) = ((i / CS as u64) as u32, (i % CS as u64) as usize);
                    self.chunks.get(&c).and_then(|d| d.get(o).copied()).unwrap_or(0)
                })
                .collect()
        }

        /// read(): committed bytes overlaid with the buffer.
        fn view(&self) -> Vec<u8> {
            let len = self.size.max(self.wb.logical_size(P).unwrap_or(0));
            let mut v = self.committed_view();
            v.resize(len as usize, 0);
            self.wb.overlay(P, 0, &mut v);
            v
        }

        /// read_seed_bytes clamped to `eof`; a hole below EOF seeds zeros
        /// (write(): up to the buffered end, `hole_eof`).
        fn seed(&mut self, idx: u32, eof: u64, hole_eof: u64) {
            let base = idx as u64 * CS as u64;
            let eof = if self.chunks.contains_key(&idx) { eof } else { hole_eof };
            if !self.wb.chunk_needs_seed(P, idx) || base >= eof {
                return;
            }
            if !self.wb.has_chunk(P, idx) {
                self.fresh.push(idx);
            }
            let clamp = (eof - base).min(CS as u64) as usize;
            match self.chunks.get(&idx) {
                Some(d) => self.wb.seed_chunk(P, idx, &d[..d.len().min(clamp)]),
                None => self.wb.seed_hole(P, idx, &vec![0; clamp]),
            }
        }

        fn span(off: u64, len: usize) -> RangeInclusive<u32> {
            (off / CS as u64) as u32..=((off + len as u64 - 1) / CS as u64) as u32
        }

        fn cap_drain(&mut self, incoming: usize, keep: RangeInclusive<u32>) -> bool {
            let segs = self.wb.cap_drain(P, incoming, self.cap, self.async_on, keep);
            let ok = self.commit(segs);
            if !ok {
                self.wb.remove_chunks(P, &self.fresh);
            }
            self.drain_failed |= !ok;
            ok
        }

        fn write(&mut self, off: u64, data: &[u8]) {
            self.fresh.clear();
            let eof = self.size.max(self.wb.logical_size(P).unwrap_or(0));
            for idx in Self::span(off, data.len()) {
                self.seed(idx, self.size, eof);
            }
            if off > eof && eof % CS as u64 != 0 {
                // The hole tail seed is kept on a failed drain (filesystem.rs).
                let in_range = std::mem::take(&mut self.fresh);
                self.seed((eof / CS as u64) as u32, self.size, eof);
                self.fresh = in_range;
            }
            let fill = hole_fill_ranges(eof, off, CS as u64);
            for (i, r) in fill.iter().enumerate() {
                if i == 1 && r.start > fill[0].end {
                    // set_size pin (SetFileSize keeps every chunk below it).
                    self.size = self.size.max(eof);
                }
                let mut at = r.start;
                while at < r.end {
                    let n = ((r.end - at) as usize).min(2 * CS);
                    if !self.cap_drain(n, Self::span(at, n)) {
                        self.model.resize(self.model.len().max(at as usize), 0);
                        return;
                    }
                    self.wb.write_hole(P, at, n);
                    at += n as u64;
                }
                if r.start < r.end {
                    self.model.resize(self.model.len().max(r.end as usize), 0);
                }
            }
            if !self.cap_drain(data.len(), Self::span(off, data.len())) {
                return;
            }
            self.wb.write(P, off, data);
            let end = off as usize + data.len();
            if self.model.len() < end {
                self.model.resize(end, 0);
            }
            self.model[off as usize..end].copy_from_slice(data);
            let segs = self.wb.flush(P);
            self.commit(segs);
        }

        /// fsync / flush: async only reports the sticky error.
        fn fsync(&mut self) {
            if self.async_on {
                self.sticky = false;
            } else {
                let segs = self.wb.flush_all(P);
                self.commit(segs);
            }
        }

        fn tick(&mut self, adv: Duration) {
            self.clock += adv;
            if !self.async_on {
                return;
            }
            for p in self.wb.aged_paths(Instant::now() + self.clock, AGE, 8) {
                assert_eq!(p, P);
                let segs = self.wb.flush_all(P);
                if !self.commit(segs) {
                    self.sticky = true;
                }
            }
        }

        /// truncate_inner with a writer (SetFileSize to `new`).
        fn truncate(&mut self, new: u64) {
            if new < self.size && new % CS as u64 != 0 {
                self.seed((new / CS as u64) as u32, new, new);
            }
            self.size = new;
            self.chunks.retain(|&c, _| (c as u64) * (CS as u64) < new);
            self.wb.truncate(P, new);
            self.model.resize(new as usize, 0);
        }

        fn release(&mut self) {
            let segs = self.wb.flush_all(P);
            if self.commit(segs) {
                assert_eq!(self.committed_view(), self.model, "release commits the model");
            } else {
                self.wb.remove(P);
                self.model = self.committed_view();
            }
            self.sticky = false;
        }

        fn check(&mut self) {
            assert_eq!(self.view(), self.model, "async={} {:?}", self.async_on, self.trace);
            if self.async_on {
                let d = self.wb.dirty_bytes(P);
                let bound = if self.drain_failed {
                    self.cap + 4 * CS
                } else {
                    self.cap
                };
                let tail = &self.trace[self.trace.len().saturating_sub(6)..];
                assert!(d <= bound, "{d} buffered > {bound} {tail:?}");
                // The overshoot lasts until a write's drain succeeds.
                self.drain_failed &= d > self.cap;
            }
        }
    }

    /// knfsd writeback model: 2-byte pieces of a 12-chunk file delivered
    /// out of order within a window of about 3 chunks, one commit-all (aged
    /// timer / fsync) in the middle. Each chunk is uploaded at most twice
    /// (once by the commit-all with zeros at its gaps, once complete) and
    /// the file equals the model; in order, exactly once.
    #[test]
    fn out_of_order_window_uploads_each_chunk_at_most_twice() {
        const N: u64 = 12 * CS as u64;
        for async_on in [true, false] {
            for seed in 0..=200u64 {
                let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
                let mut next = |m: u64| {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    rng % m
                };
                let mut order: Vec<(u64, u64)> = (0..N / 2).map(|i| (i, i)).collect();
                if seed > 0 {
                    for o in &mut order {
                        o.0 += next(12);
                    }
                }
                order.sort();
                let mid = if seed > 0 { Some(next(order.len() as u64) as usize) } else { None };
                let mut sim = Sim::new(async_on);
                sim.cap = 1 << 20;
                for (k, &(_, i)) in order.iter().enumerate() {
                    if mid == Some(k) {
                        sim.trace.push("commit-all".into());
                        if async_on {
                            sim.tick(AGE);
                        } else {
                            sim.fsync();
                        }
                    }
                    sim.trace.push(format!("write {}", i * 2));
                    sim.write(i * 2, &[(i % 251 + 1) as u8; 2]);
                    sim.check();
                }
                sim.release();
                let max = if seed == 0 { 1 } else { 2 };
                for (c, n) in &sim.uploads {
                    assert!(
                        *n <= max,
                        "seed {seed} async={async_on}: chunk {c} uploaded {n}x {:?}",
                        sim.uploads
                    );
                }
                assert_eq!(sim.uploads.len(), 12);
            }
        }
    }

    /// Only the edge chunks of a hole get zero padding; whole chunks inside
    /// it are skipped (`r[1].start > r[0].end`).
    #[test]
    fn hole_fill_ranges_pads_only_the_edge_chunks() {
        assert_eq!(hole_fill_ranges(10, 100, 8), [10..16, 96..100]);
        assert_eq!(hole_fill_ranges(10, 14, 8), [10..14, 14..14], "write inside the tail chunk");
        assert_eq!(hole_fill_ranges(10, 20, 8), [10..16, 16..20], "next chunk: contiguous, no skip");
        assert_eq!(hole_fill_ranges(16, 96, 8), [16..16, 96..96], "aligned both ends: nothing padded");
        assert_eq!(hole_fill_ranges(16, 100, 8), [16..16, 96..100]);
        assert_eq!(hole_fill_ranges(16, 16, 8), [16..16, 16..16], "write at the end: no hole");
        assert_eq!(hole_fill_ranges(20, 16, 8), [20..20, 20..20], "write below the end: no hole");
    }

    /// The buffered end is the logical end: no zero buffers for a sparse
    /// write, it survives draining the top chunk while a lower one stays
    /// buffered, and truncate clips or extends it. Fully drained: `None`
    /// (in-flight overlay, then the snapshot).
    #[test]
    fn buffered_end_survives_drains_and_follows_truncate() {
        let wb = WriteBuffer::new(CS);
        wb.write(P, 0, &[1; 3]);
        wb.write(P, 20 * CS as u64, &[2; CS]);
        assert_eq!(wb.dirty_bytes(P), CS + 3, "no zeros buffered for the hole");
        assert_eq!(wb.visible_size(P), Some(21 * CS as u64));
        assert_eq!(idx(&wb.flush(P)), vec![20]);
        assert_eq!(wb.logical_size(P), Some(21 * CS as u64), "end lost by the drain");
        let _segs = wb.flush_all(P);
        assert_eq!(wb.logical_size(P), None, "fully drained");
        wb.write(P, 1, &[3; 1]);
        assert_eq!(wb.logical_size(P), Some(2), "a drained end does not come back");

        wb.write(P, 0, &[1; 3]);
        wb.truncate(P, 10 * CS as u64 + 4);
        assert_eq!(wb.visible_size(P), Some(10 * CS as u64 + 4), "truncate-extend");
        wb.truncate(P, 2);
        assert_eq!(wb.visible_size(P), Some(2), "truncate-shrink");
    }

    /// commit_all's drain into the in-flight overlay (and a failed upload's
    /// restore back) is atomic for `visible_size`: a concurrent EOF reader
    /// never sees the file shorter (FUSE-1: a SetFileSize pin below it drops
    /// committed chunks).
    #[test]
    fn drain_to_inflight_and_restore_never_hide_the_bytes() {
        let wb = WriteBuffer::new(CS);
        // Every drain into an overlay: commit_all, write()'s batch flush (and
        // spawn_bg_upload), the cap drains; read via size and via content.
        let mut misses = Vec::new();
        for drain in ["flush_all", "flush", "cap_drain"] {
            // flush leaves a partial tail buffered, which would mask the size.
            let end = if drain == "flush" { 4 * CS as u64 } else { 3 * CS as u64 + 5 };
            wb.remove(P);
            wb.write(P, 0, &vec![1; end as usize]);
            for read in ["visible_size", "overlay"] {
                let stop = std::sync::atomic::AtomicBool::new(false);
                std::thread::scope(|s| {
                    let reader = s.spawn(|| {
                        let mut n = 0u32;
                        let mut dst = vec![0u8; end as usize];
                        while !stop.load(Ordering::Relaxed) {
                            let ok = if read == "overlay" {
                                dst.fill(0);
                                wb.overlay(P, 0, &mut dst);
                                dst.iter().all(|&b| b == 1)
                            } else {
                                wb.visible_size(P) == Some(end)
                            };
                            n += u32::from(!ok);
                        }
                        n
                    });
                    for _ in 0..30_000 {
                        let (segs, g) = match drain {
                            "flush_all" => wb.flush_all_inflight(P),
                            "flush" => wb.flush_inflight(P),
                            _ => wb.cap_drain_inflight(P, 0, 0, true, u32::MAX..=u32::MAX),
                        };
                        assert!(!segs.is_empty() && g.is_some(), "{drain}: nothing drained");
                        wb.restore_segments(P, &segs);
                        drop(g);
                    }
                    stop.store(true, Ordering::Relaxed);
                    misses.push((drain, read, reader.join().unwrap()));
                });
            }
        }
        assert!(misses.iter().all(|m| m.2 == 0), "bytes hidden by a drain/restore: {misses:?}");
    }

    /// Write at 0, then far past the end, close: only the two written chunks
    /// upload (edge-padded), the hole reads zeros, the size is the end.
    #[test]
    fn sparse_write_uploads_only_written_chunks() {
        for async_on in [true, false] {
            let mut sim = Sim::new(async_on);
            sim.cap = 1 << 20;
            sim.write(0, &[1; 3]);
            sim.write(10 * CS as u64 + 3, &[2; 5]);
            sim.check();
            assert_eq!(sim.wb.dirty_bytes(P), 2 * CS, "async={async_on}: only edge padding");
            assert_eq!(sim.wb.visible_size(P), Some(10 * CS as u64 + 8));
            sim.release();
            assert_eq!(sim.uploads.keys().copied().collect::<Vec<_>>(), vec![0, 10]);
            assert_eq!(sim.size, 10 * CS as u64 + 8);
        }
    }

    /// Truncate-extend then close: the end is a hole, size is the truncate
    /// size, the hole reads zeros. (A write past EOF always ends with data.)
    #[test]
    fn truncate_extend_then_close_keeps_the_size() {
        for async_on in [true, false] {
            let mut sim = Sim::new(async_on);
            sim.write(0, &[1; 5]);
            sim.truncate(10 * CS as u64 + 4);
            sim.check();
            sim.release();
            assert_eq!(sim.size, 10 * CS as u64 + 4);
            assert_eq!(sim.uploads.keys().copied().collect::<Vec<_>>(), vec![0]);
        }
    }

    /// Prod knfsd pattern (NFSv4.1 rsync, async_commit): ~6 sequential
    /// writeback streams over adjacent, chunk-unaligned regions, round-robin
    /// in small writes, each starting past the current end. Every chunk
    /// uploads exactly once and the file equals the model; the dirty cap
    /// (16 chunks) is never hit. Before: each stream start zero-filled the
    /// whole gap, the cap drain uploaded gappy chunks, x1.6.
    #[test]
    fn interleaved_streams_upload_each_chunk_once() {
        const REG: u64 = 16 * CS as u64 + 3;
        const ARRIVAL: [u64; 6] = [1, 5, 2, 0, 3, 4];
        for async_on in [true, false] {
            let mut sim = Sim::new(async_on);
            sim.cap = 16 * CS;
            let mut pos: Vec<u64> = (0..6).map(|s| s * REG).collect();
            let mut k = 0u64;
            while pos.iter().enumerate().any(|(s, &p)| p < (s as u64 + 1) * REG) {
                for &s in &ARRIVAL {
                    let end = (s + 1) * REG;
                    let p = pos[s as usize];
                    if p < end {
                        let n = (end - p).min(2) as usize;
                        k += 1;
                        sim.trace.push(format!("write {p} {n}"));
                        sim.write(p, &vec![(k % 251 + 1) as u8; n]);
                        sim.check();
                        pos[s as usize] += n as u64;
                    }
                }
            }
            sim.release();
            let chunks = (6 * REG).div_ceil(CS as u64) as usize;
            assert_eq!(sim.uploads.len(), chunks, "async={async_on}");
            assert!(sim.uploads.values().all(|&n| n == 1), "async={async_on}: {:?}", sim.uploads);
        }
    }

    #[test]
    fn async_commit_random_ops_keep_reads_equal_to_model() {
        for async_on in [true, false] {
            for seed in 1..=400u64 {
                let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
                let mut next = |m: u64| {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    rng % m
                };
                let mut sim = Sim::new(async_on);
                for _ in 0..80 {
                    let logical = sim.model.len() as u64;
                    sim.trace.push(format!("size={} buf={:?}", sim.size, sim.wb.logical_size(P)));
                    match next(10) {
                        0..=3 => {
                            let off = next(logical + 3 * CS as u64 + 1);
                            let len = 1 + next(12) as usize;
                            let b = (next(255) + 1) as u8;
                            sim.trace.push(format!("write {off} {len}"));
                            sim.write(off, &vec![b; len]);
                        }
                        4 | 9 => {
                            sim.trace.push("fsync".into());
                            sim.fsync();
                        }
                        5 => {
                            let a = next(21);
                            sim.trace.push(format!("tick {a}"));
                            sim.tick(Duration::from_secs(a));
                        }
                        6 => {
                            let n = next(logical + 1);
                            sim.trace.push(format!("truncate {n}"));
                            sim.truncate(n);
                        }
                        7 => {
                            sim.trace.push("fail next upload".into());
                            sim.fail_next = true;
                        }
                        _ => {
                            sim.trace.push("release".into());
                            sim.release();
                        }
                    }
                    sim.check();
                }
                sim.fail_next = false;
                sim.release();
                assert_eq!(sim.wb.dirty_bytes(P), 0);
            }
        }
    }
}
