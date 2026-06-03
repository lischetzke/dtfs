// Write buffering aligned to chunk boundaries — Phase 2.
//
// Accumulates writes in memory and produces chunk-aligned data segments
// ready to be sent to data nodes. This allows small writes to be coalesced
// and chunk boundaries to be respected without requiring callers to manage
// alignment.

use std::collections::BTreeMap;

use bytes::Bytes;
use dashmap::DashMap;
use tracing::{debug, instrument};

/// Write buffer that accumulates data and flushes chunk-aligned segments.
///
/// Each file path gets an independent [`FileWriteBuffer`] that tracks dirty
/// data. When enough data has been accumulated to fill one or more complete
/// chunks, those chunks can be extracted via [`flush`](Self::flush). A final
/// partial chunk can be extracted with [`flush_all`](Self::flush_all).
pub struct WriteBuffer {
    /// Chunk size in bytes (default 128 MB).
    chunk_size: usize,
    /// Per-file write buffers, keyed by file path.
    buffers: DashMap<String, FileWriteBuffer>,
}

/// Internal per-file write buffer.
///
/// Data is stored in a `BTreeMap<u64, u8>` sparse representation would be
/// wasteful, so we keep a contiguous `Vec<u8>` and track the starting offset
/// of the dirty region.
struct FileWriteBuffer {
    path: String,
    /// Contiguous buffer of dirty data.
    data: Vec<u8>,
    /// The byte offset in the file where `data[0]` maps to.
    dirty_offset: u64,
}

/// A chunk-aligned segment ready for writing to a data node.
#[derive(Debug)]
pub struct ChunkSegment {
    /// Zero-based chunk index within the file.
    pub chunk_index: u32,
    /// The data for this chunk (up to `chunk_size` bytes).
    pub data: Bytes,
}

impl WriteBuffer {
    /// Create a new write buffer with the given chunk size in bytes.
    pub fn new(chunk_size: usize) -> Self {
        Self {
            chunk_size,
            buffers: DashMap::new(),
        }
    }

    /// Create a new write buffer with the default 128 MB chunk size.
    pub fn with_default_chunk_size() -> Self {
        Self::new(128 * 1024 * 1024)
    }

    /// Write data into the buffer for the given file path at the specified offset.
    ///
    /// Returns the number of bytes buffered (always equal to `data.len()`).
    #[instrument(skip(self, data), fields(path = %path, offset, len = data.len()))]
    pub fn write(&self, path: &str, offset: u64, data: &[u8]) -> usize {
        let len = data.len();
        if len == 0 {
            return 0;
        }

        let mut entry = self.buffers.entry(path.to_string()).or_insert_with(|| {
            FileWriteBuffer {
                path: path.to_string(),
                data: Vec::new(),
                dirty_offset: offset,
            }
        });

        let buf = entry.value_mut();
        buf.merge_write(offset, data);

        debug!(path, offset, len, "buffered write");
        len
    }

    /// Extract all complete (chunk-aligned) segments from the buffer.
    ///
    /// Any trailing data that does not fill a complete chunk remains in the
    /// buffer. Returns an empty `Vec` if no complete chunks are available.
    #[instrument(skip(self), fields(path = %path))]
    pub fn flush(&self, path: &str) -> Vec<ChunkSegment> {
        let mut entry = match self.buffers.get_mut(path) {
            Some(e) => e,
            None => return Vec::new(),
        };

        let buf = entry.value_mut();
        let segments = buf.extract_complete_chunks(self.chunk_size);

        debug!(
            path,
            num_segments = segments.len(),
            remaining = buf.data.len(),
            "flushed complete chunks"
        );
        segments
    }

    /// Extract all data from the buffer, including a partial final chunk.
    ///
    /// This should be called when the file is being closed or when all
    /// buffered data must be persisted regardless of alignment.
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

    /// Check whether a file has any buffered dirty data.
    pub fn has_dirty_data(&self, path: &str) -> bool {
        self.buffers
            .get(path)
            .map(|e| !e.value().data.is_empty())
            .unwrap_or(false)
    }

    /// Logical end of the file as seen by the buffer (i.e. the highest
    /// `offset + data.len()` of any write merged so far). Returns `None` if
    /// the buffer has never seen a write for this path.
    ///
    /// Used by the F-021/F-022 fix to decide where O_APPEND writes land and
    /// to size sparse-hole zero fills.
    pub fn logical_size(&self, path: &str) -> Option<u64> {
        self.buffers
            .get(path)
            .map(|e| e.value().dirty_offset + e.value().data.len() as u64)
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
    /// Returns the number of bytes the buffer covered. Callers can use this
    /// to decide whether to short-circuit cluster fetches (when buffered
    /// data fully covers the request, no chunk read is needed).
    pub fn overlay(&self, path: &str, abs_offset: u64, dst: &mut [u8]) -> usize {
        let Some(entry) = self.buffers.get(path) else { return 0 };
        let buf = entry.value();
        if buf.data.is_empty() {
            return 0;
        }
        let buf_start = buf.dirty_offset;
        let buf_end = buf_start + buf.data.len() as u64;
        let req_start = abs_offset;
        let req_end = abs_offset + dst.len() as u64;
        let overlap_start = req_start.max(buf_start);
        let overlap_end = req_end.min(buf_end);
        if overlap_end <= overlap_start {
            return 0;
        }
        let src_off = (overlap_start - buf_start) as usize;
        let dst_off = (overlap_start - req_start) as usize;
        let n = (overlap_end - overlap_start) as usize;
        dst[dst_off..dst_off + n]
            .copy_from_slice(&buf.data[src_off..src_off + n]);
        n
    }

    /// Get the current chunk size.
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }
}

impl FileWriteBuffer {
    /// Merge a write at `offset` into the contiguous buffer.
    ///
    /// If the write is contiguous with (or overlaps) the existing dirty
    /// region, the buffer is extended or overwritten as needed. If the write
    /// is entirely disjoint, the gap is zero-filled (this is the simplest
    /// approach; a sparse representation could be used for very large gaps
    /// but is not needed for the typical sequential-write pattern).
    fn merge_write(&mut self, offset: u64, data: &[u8]) {
        if self.data.is_empty() {
            // First write — just store it.
            self.dirty_offset = offset;
            self.data = data.to_vec();
            return;
        }

        let buf_start = self.dirty_offset;
        let buf_end = buf_start + self.data.len() as u64;
        let write_end = offset + data.len() as u64;

        if offset >= buf_start && offset <= buf_end {
            // Write starts within or immediately after current buffer.
            let relative = (offset - buf_start) as usize;
            let needed = relative + data.len();
            if needed > self.data.len() {
                self.data.resize(needed, 0);
            }
            self.data[relative..relative + data.len()].copy_from_slice(data);
        } else if offset < buf_start {
            // Write starts before the current buffer — prepend.
            if write_end >= buf_start {
                // Overlaps: merge.
                let new_start = offset;
                let new_end = buf_end.max(write_end);
                let new_len = (new_end - new_start) as usize;
                let mut new_data = vec![0u8; new_len];

                // Copy the new write data.
                new_data[0..data.len()].copy_from_slice(data);

                // Copy existing buffer data into the correct position.
                let existing_offset = (buf_start - new_start) as usize;
                // Only copy the part of the existing buffer that is not
                // overwritten by the new write.
                let overwritten_end = (write_end - buf_start) as usize;
                if overwritten_end < self.data.len() {
                    let dest_start = existing_offset + overwritten_end;
                    new_data[dest_start..existing_offset + self.data.len()]
                        .copy_from_slice(&self.data[overwritten_end..]);
                }

                self.dirty_offset = new_start;
                self.data = new_data;
            } else {
                // Disjoint before — zero-fill the gap.
                let new_start = offset;
                let new_end = buf_end;
                let new_len = (new_end - new_start) as usize;
                let mut new_data = vec![0u8; new_len];
                new_data[0..data.len()].copy_from_slice(data);
                let existing_offset = (buf_start - new_start) as usize;
                new_data[existing_offset..existing_offset + self.data.len()]
                    .copy_from_slice(&self.data);
                self.dirty_offset = new_start;
                self.data = new_data;
            }
        } else {
            // Write starts after the current buffer with a gap — zero-fill.
            let new_end = write_end;
            let new_len = (new_end - buf_start) as usize;
            self.data.resize(new_len, 0);
            let relative = (offset - buf_start) as usize;
            self.data[relative..relative + data.len()].copy_from_slice(data);
        }
    }

    /// Extract complete chunk-aligned segments from the front of the buffer.
    fn extract_complete_chunks(&mut self, chunk_size: usize) -> Vec<ChunkSegment> {
        let mut segments = Vec::new();

        // Calculate which chunk the dirty_offset falls into, and how much
        // data is needed before we complete that first chunk.
        let first_chunk_idx = (self.dirty_offset / chunk_size as u64) as u32;
        let offset_in_chunk = (self.dirty_offset % chunk_size as u64) as usize;

        // Bytes needed to complete the first (partial) chunk.
        let bytes_to_complete_first = chunk_size - offset_in_chunk;

        if self.data.len() < bytes_to_complete_first {
            // Not enough data to complete even the first chunk.
            return segments;
        }

        // If offset_in_chunk > 0, we cannot extract the first chunk as a
        // "complete" chunk because we only have partial data for it.
        // We can only extract chunks where we have the full chunk_size bytes.
        //
        // Strategy: find the first chunk boundary within the buffer, then
        // extract all full chunks from that point onward.
        let mut consumed = 0usize;

        if offset_in_chunk == 0 {
            // Buffer starts on a chunk boundary — extract full chunks.
            let mut pos = 0usize;
            let mut chunk_idx = first_chunk_idx;
            while pos + chunk_size <= self.data.len() {
                let chunk_data = Bytes::copy_from_slice(&self.data[pos..pos + chunk_size]);
                segments.push(ChunkSegment {
                    chunk_index: chunk_idx,
                    data: chunk_data,
                });
                pos += chunk_size;
                chunk_idx += 1;
            }
            consumed = pos;
        } else {
            // Buffer starts mid-chunk. The first complete chunk we can
            // extract starts at `bytes_to_complete_first` offset in the
            // buffer. But we must also include that first partial portion
            // as part of the first chunk — so we extract it if we have
            // enough data for a full chunk that starts at chunk boundary.

            // Actually, for the first chunk we need to include the data
            // from offset_in_chunk. We produce a segment with only our
            // portion; the filesystem layer will need to handle combining
            // with existing data. For simplicity, we just extract full
            // chunks starting from the next boundary.
            let mut pos = bytes_to_complete_first;
            let next_chunk_idx = first_chunk_idx + 1;
            let mut chunk_idx = next_chunk_idx;

            while pos + chunk_size <= self.data.len() {
                let chunk_data = Bytes::copy_from_slice(&self.data[pos..pos + chunk_size]);
                segments.push(ChunkSegment {
                    chunk_index: chunk_idx,
                    data: chunk_data,
                });
                pos += chunk_size;
                chunk_idx += 1;
            }
            // Don't consume the leading partial data — it stays in the
            // buffer since we didn't extract a segment for it.
            consumed = pos;
        }

        if consumed > 0 {
            // Remove consumed data from the front of the buffer.
            self.data.drain(..consumed);
            self.dirty_offset += consumed as u64;
        }

        segments
    }

    /// Extract all segments, including partial final chunk.
    fn extract_all_chunks(&mut self, chunk_size: usize) -> Vec<ChunkSegment> {
        if self.data.is_empty() {
            return Vec::new();
        }

        let mut segments = Vec::new();
        let first_chunk_idx = (self.dirty_offset / chunk_size as u64) as u32;
        let offset_in_chunk = (self.dirty_offset % chunk_size as u64) as usize;

        if offset_in_chunk == 0 {
            // Aligned start — straightforward.
            let mut pos = 0usize;
            let mut chunk_idx = first_chunk_idx;
            while pos < self.data.len() {
                let end = (pos + chunk_size).min(self.data.len());
                let chunk_data = Bytes::copy_from_slice(&self.data[pos..end]);
                segments.push(ChunkSegment {
                    chunk_index: chunk_idx,
                    data: chunk_data,
                });
                pos = end;
                chunk_idx += 1;
            }
        } else {
            // Starts mid-chunk. Emit the leading partial piece as a
            // segment for the first chunk.
            let first_end = (chunk_size - offset_in_chunk).min(self.data.len());
            segments.push(ChunkSegment {
                chunk_index: first_chunk_idx,
                data: Bytes::copy_from_slice(&self.data[..first_end]),
            });

            let mut pos = first_end;
            let mut chunk_idx = first_chunk_idx + 1;
            while pos < self.data.len() {
                let end = (pos + chunk_size).min(self.data.len());
                let chunk_data = Bytes::copy_from_slice(&self.data[pos..end]);
                segments.push(ChunkSegment {
                    chunk_index: chunk_idx,
                    data: chunk_data,
                });
                pos = end;
                chunk_idx += 1;
            }
        }

        // Clear the buffer.
        self.data.clear();
        self.dirty_offset = 0;

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
}
