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
use std::ops::Range;

use bytes::Bytes;
use dashmap::DashMap;
use tracing::{debug, instrument};

/// WI-01 — upper clamp on `flush_batch_chunks`. Far below the manager's
/// `MAX_CHUNKS_PER_ALLOCATION` (65_536, `metadata.rs`), it just bounds how
/// much un-committed data one open file can hold in client heap
/// (`flush_batch_chunks * chunk_size`; see AGENTS.md A.3.2).
const MAX_FLUSH_BATCH_CHUNKS: usize = 32;

/// WI-01 / H1 — hard byte ceiling on un-committed data one open file may hold
/// because of batching. `flush_batch_chunks` bounds a chunk COUNT, so at a
/// 128 MB volume chunk size the default batch of 4 would buffer 512 MB per
/// open write and the count clamp would permit 4 GB — with nothing in the
/// dtfs-fuse memory ladder (A.3.2 / A.5) shedding write buffers and several
/// files open at once during a torrent landing. 64 MB keeps the default
/// 4 x 4 MB batch intact and degrades a large-chunk volume to per-chunk
/// flushing (the pre-WI-01 behaviour) instead of to an OOM.
const FLUSH_BATCH_MAX_BYTES: usize = 64 * 1024 * 1024;

/// CR-1-1 — hard ceiling on total dirty bytes buffered per file, independent
/// of how many distinct chunk indices are touched (a write far into a large
/// sparse file plus a write near offset 0 now holds two small `ChunkBuf`
/// entries rather than a multi-hundred-MB contiguous `Vec`, but callers must
/// still not be able to pin unbounded heap by scattering tiny writes across
/// many chunk indices). Set to twice `FLUSH_BATCH_MAX_BYTES` — comfortably
/// above one batch's worth of complete chunks plus whatever partial chunks
/// are still accumulating. Enforcement is caller-side (filesystem.rs forces
/// a flush before a write that would exceed it); `dirty_bytes()` is the
/// query callers use to check.
pub const DIRTY_BYTES_CAP: usize = FLUSH_BATCH_MAX_BYTES * 2;

/// WI-01 / H1 — resolve the effective `flush_batch_chunks` for a given
/// configured value and chunk size: clamp the count into
/// `1..=MAX_FLUSH_BATCH_CHUNKS`, then clamp again so `batch * chunk_size`
/// never exceeds `FLUSH_BATCH_MAX_BYTES`. Never goes below 1 — a single
/// chunk always flushes whatever its size (pre-WI-01 behaviour); holding it
/// back would strand the write.
pub(crate) fn effective_batch_chunks(configured: usize, chunk_size: usize) -> usize {
    let by_count = configured.clamp(1, MAX_FLUSH_BATCH_CHUNKS);
    if chunk_size == 0 {
        // Degenerate; complete_chunks_available already returns 0. Also
        // avoids a divide by zero.
        return by_count;
    }
    by_count.min(FLUSH_BATCH_MAX_BYTES / chunk_size).max(1)
}

/// Pre-CR-1-1 pure helper retained for its direct unit test and any external
/// caller: number of whole, chunk-aligned chunks extractable from a
/// contiguous run that starts at `dirty_offset` and holds `len` bytes. The
/// sparse per-chunk buffer below no longer uses this internally (batching
/// now counts complete `ChunkBuf` entries directly — see
/// `FileWriteBuffer::complete_chunk_count`), but the single-buffer semantics
/// this encodes are still a useful, independently-testable invariant.
pub(crate) fn complete_chunks_available(dirty_offset: u64, len: usize, chunk_size: usize) -> usize {
    if chunk_size == 0 || dirty_offset % chunk_size as u64 != 0 {
        return 0;
    }
    len / chunk_size
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
    /// Per-file write buffers, keyed by file path.
    buffers: DashMap<String, FileWriteBuffer>,
}

/// Internal per-file write buffer: a sparse map from chunk index to the
/// dirty bytes buffered for that chunk (CR-1-1).
struct FileWriteBuffer {
    #[allow(dead_code)] // kept for parity/debuggability; not read internally
    path: String,
    chunks: BTreeMap<u32, ChunkBuf>,
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
}

impl ChunkBuf {
    /// True iff this chunk's buffered data fully covers `0..chunk_size` —
    /// i.e. it is safe to commit as the chunk's whole content without an
    /// RMW seed first.
    fn is_complete(&self, chunk_size: usize) -> bool {
        self.dirty.start == 0 && self.dirty.end == chunk_size
    }

    /// Merge a write at `offset_in_chunk` (always `< chunk_size`; the caller
    /// splits writes at chunk boundaries before calling this) into this
    /// chunk's buffer. Mirrors the pre-CR-1-1 contiguous-buffer merge logic,
    /// but every gap it can zero-fill is bounded to `0..chunk_size` — it can
    /// never reach into a neighbouring chunk.
    fn merge(&mut self, offset_in_chunk: usize, data: &[u8]) {
        let write_end = offset_in_chunk + data.len();

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
    /// `0..chunk_size`. The caller must never commit a segment with
    /// `needs_seed == true` as the chunk's whole content without first
    /// RMW-seeding it against the cluster's existing bytes (CR-1-2) — doing
    /// so would silently truncate or shift already-committed data.
    pub needs_seed: bool,
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
            buffers: DashMap::new(),
        }
    }

    /// WI-01 — create a new write buffer that holds back complete chunks
    /// until at least `flush_batch_chunks` are available, so `flush()`
    /// amortizes the allocate+commit round trip over a batch instead of
    /// firing once per chunk. Clamped into `1..=MAX_FLUSH_BATCH_CHUNKS` AND
    /// down to whatever keeps `batch * chunk_size` within
    /// `FLUSH_BATCH_MAX_BYTES` (64 MB), so a large-chunk volume degrades to
    /// per-chunk flushing rather than buffering hundreds of MB per open
    /// file. `1` reproduces `new()`'s exact behaviour (the rollback value)
    /// at the default 4 MB chunk size. `flush_all` is unaffected — it
    /// always drains everything, batched or not, so held-back chunks still
    /// leave at fsync/close.
    pub fn new_with_batch(chunk_size: usize, flush_batch_chunks: usize) -> Self {
        Self {
            chunk_size,
            flush_batch_chunks: effective_batch_chunks(flush_batch_chunks, chunk_size),
            buffers: DashMap::new(),
        }
    }

    /// Create a new write buffer with the default 128 MB chunk size.
    pub fn with_default_chunk_size() -> Self {
        Self::new(128 * 1024 * 1024)
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

        let mut entry = self.buffers.entry(path.to_string()).or_insert_with(|| {
            FileWriteBuffer {
                path: path.to_string(),
                chunks: BTreeMap::new(),
            }
        });
        let buf = entry.value_mut();

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
                .or_insert_with(|| ChunkBuf {
                    data: Vec::new(),
                    dirty: 0..0,
                })
                .merge(offset_in_chunk, piece);
            remaining = rest;
            cur_offset += take as u64;
        }

        debug!(path, offset, len, "buffered write");
        len
    }

    /// Extract all complete (chunk-aligned) segments from the buffer.
    ///
    /// Any chunk that is not yet fully covered remains in the buffer.
    /// Returns an empty `Vec` if fewer than `flush_batch_chunks` complete
    /// chunks are available.
    #[instrument(skip(self), fields(path = %path))]
    pub fn flush(&self, path: &str) -> Vec<ChunkSegment> {
        let mut entry = match self.buffers.get_mut(path) {
            Some(e) => e,
            None => return Vec::new(),
        };
        let buf = entry.value_mut();

        let ready = buf
            .chunks
            .values()
            .filter(|cb| cb.is_complete(self.chunk_size))
            .count();
        if ready < self.flush_batch_chunks {
            return Vec::new();
        }

        let segments = buf.extract_complete_chunks(self.chunk_size);
        debug!(
            path,
            num_segments = segments.len(),
            remaining = buf.chunks.len(),
            "flushed complete chunks"
        );
        segments
    }

    /// R1 (FUSE-1 memory half) — drain EVERY complete chunk right now,
    /// ignoring `flush_batch_chunks`. Unlike `flush()`, which deliberately
    /// holds back below the batch threshold to amortize the
    /// allocate+commit round trip, this exists purely as a memory-pressure
    /// release valve: a caller nearing `DIRTY_BYTES_CAP` (e.g. the F-022
    /// sparse-hole fill loop in `filesystem.rs::write()`) needs to commit
    /// and drop whatever is already flushable RIGHT NOW, not wait for a
    /// batch to fill. Partial (incomplete) chunks are left resident.
    #[instrument(skip(self), fields(path = %path))]
    pub fn flush_complete(&self, path: &str) -> Vec<ChunkSegment> {
        let mut entry = match self.buffers.get_mut(path) {
            Some(e) => e,
            None => return Vec::new(),
        };
        let buf = entry.value_mut();
        let segments = buf.extract_complete_chunks(self.chunk_size);
        debug!(
            path,
            num_segments = segments.len(),
            remaining = buf.chunks.len(),
            "flush_complete: force-drained complete chunks (memory pressure)"
        );
        segments
    }

    /// Extract every buffered chunk, including partial (not fully covered)
    /// ones. This should be called when the file is being closed or when
    /// all buffered data must be persisted regardless of alignment.
    #[instrument(skip(self), fields(path = %path))]
    pub fn flush_all(&self, path: &str) -> Vec<ChunkSegment> {
        let mut entry = match self.buffers.get_mut(path) {
            Some(e) => e,
            None => return Vec::new(),
        };
        let buf = entry.value_mut();
        let segments = buf.extract_all_chunks(self.chunk_size);
        debug!(
            path,
            num_segments = segments.len(),
            "flushed all chunks (including partial)"
        );
        segments
    }

    /// Remove the buffer for a given path entirely.
    pub fn remove(&self, path: &str) {
        self.buffers.remove(path);
    }

    /// CO-1-8 (b) — drop buffered bytes at or above `new_size`, keeping the
    /// buffered prefix `[0..new_size)` intact.
    pub fn truncate(&self, path: &str, new_size: u64) {
        let mut entry = match self.buffers.get_mut(path) {
            Some(e) => e,
            None => return,
        };
        let buf = entry.value_mut();
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
                    if cb.dirty.start >= cutoff_in_chunk {
                        drop_entirely = true;
                    } else {
                        let keep_end = cutoff_in_chunk.min(cb.dirty.end);
                        let keep_len = keep_end - cb.dirty.start;
                        cb.data.truncate(keep_len);
                        cb.dirty.end = keep_end;
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
        }
    }

    /// Check whether a file has any buffered dirty data.
    pub fn has_dirty_data(&self, path: &str) -> bool {
        self.buffers
            .get(path)
            .map(|e| !e.value().chunks.is_empty())
            .unwrap_or(false)
    }

    /// Total buffered bytes for `path`, summed across every resident chunk
    /// (CR-1-1) — used by callers to enforce `DIRTY_BYTES_CAP`.
    pub fn dirty_bytes(&self, path: &str) -> usize {
        self.buffers
            .get(path)
            .map(|e| e.value().chunks.values().map(|cb| cb.data.len()).sum())
            .unwrap_or(0)
    }

    /// CR-1-1/CR-1-2 — true iff `chunk_index` is currently buffered AND its
    /// buffered range fully covers `0..chunk_size`, i.e. it is already safe
    /// to commit without an RMW seed.
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

    /// Logical end of the file as seen by the buffer (i.e. the highest
    /// `chunk_index * chunk_size + dirty.end` of any resident chunk).
    /// Returns `None` if the buffer has never seen a write for this path.
    ///
    /// Used by the F-021/F-022 fix to decide where O_APPEND writes land and
    /// to size sparse-hole zero fills.
    pub fn logical_size(&self, path: &str) -> Option<u64> {
        let entry = self.buffers.get(path)?;
        let buf = entry.value();
        if buf.chunks.is_empty() {
            return None;
        }
        let chunk_size = self.chunk_size as u64;
        buf.chunks
            .iter()
            .map(|(idx, cb)| *idx as u64 * chunk_size + cb.dirty.end as u64)
            .max()
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
        let Some(entry) = self.buffers.get(path) else { return 0 };
        let buf = entry.value();
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
}

impl FileWriteBuffer {
    /// Extract complete (fully-covered) chunk-aligned segments, ascending
    /// by chunk index, removing them from the map.
    fn extract_complete_chunks(&mut self, chunk_size: usize) -> Vec<ChunkSegment> {
        let complete_keys: Vec<u32> = self
            .chunks
            .iter()
            .filter(|(_, cb)| cb.is_complete(chunk_size))
            .map(|(k, _)| *k)
            .collect();

        let mut segments = Vec::with_capacity(complete_keys.len());
        for k in complete_keys {
            if let Some(cb) = self.chunks.remove(&k) {
                segments.push(ChunkSegment {
                    chunk_index: k,
                    data: Bytes::from(cb.data),
                    offset_in_chunk: 0,
                    needs_seed: false,
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
                    needs_seed: !complete,
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

        let segments = wb.flush("file1");
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

    // WI-01 — direct unit on the pure batch-threshold helper: a buffer that
    // starts mid-chunk must report 0 available chunks regardless of how much
    // data it holds (mirrors the CO-1-7 rule in extract_complete_chunks).
    // Retained unchanged post-CR-1-1: this exercises the pure
    // single-contiguous-buffer helper directly, not the sparse per-chunk
    // buffer's own (now internal) batching count.
    #[test]
    fn wi01_complete_chunks_available_is_zero_when_midchunk() {
        let chunk = 10;
        // Aligned start: 25 bytes -> 2 complete chunks.
        assert_eq!(complete_chunks_available(0, 25, chunk), 2);
        assert_eq!(complete_chunks_available(20, 25, chunk), 2);
        // Mid-chunk start (dirty_offset % chunk_size != 0) -> always 0.
        assert_eq!(complete_chunks_available(5, 25, chunk), 0);
        assert_eq!(complete_chunks_available(5, 1000, chunk), 0);
    }

    // WI-01 — flush() must hold back complete chunks until the batch
    // threshold is reached, then emit exactly the batch, ascending, and
    // leave nothing dirty.
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
        assert_eq!(effective_batch_chunks(32, 4 * mib), 16);
        assert_eq!(effective_batch_chunks(4, 32 * mib), 2);
        assert_eq!(effective_batch_chunks(4, 64 * mib), 1);
        assert_eq!(effective_batch_chunks(4, 128 * mib), 1);
        assert_eq!(effective_batch_chunks(64, 4 * mib), 16);
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

    // WI-01 / H1 — a large-chunk volume (>= 64 MB) must degrade to per-chunk
    // flushing regardless of the configured batch; the reference-deployment
    // 4 MB chunk size keeps the configured default of 4.
    #[test]
    fn wi01_h1_large_chunk_volume_flushes_per_chunk() {
        assert_eq!(WriteBuffer::new_with_batch(128 * 1024 * 1024, 4).flush_batch_chunks, 1);
        assert_eq!(WriteBuffer::new_with_batch(4 * 1024 * 1024, 4).flush_batch_chunks, 4);
    }

    // R1 (FUSE-1 memory half) — flush_complete() must drain EVERY complete
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
        // the rest); flush_complete() must take everything at once.
        let segs = wb.flush_complete("/f");
        assert_eq!(segs.len(), 40, "flush_complete must drain every complete chunk");
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
        assert_eq!(wb.logical_size("/f"), Some(30), "cut above end is a no-op");
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
}
