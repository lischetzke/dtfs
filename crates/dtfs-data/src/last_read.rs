//! Per-disk coarse "last read" index (cache eviction / retention input).
//!
//! One index per non-cold disk. Entry per chunk: `last_read_ms` + size.
//! Persisted to `<disk>/.dtfs/last_read.tsv` (`<id hex>\t<ms>` per line) by a
//! periodic tmp + `sync_all` + rename writer. Memory is bounded by the chunks
//! on the disk (~60 B/entry). Best-effort: a corrupt or missing file only
//! loses retention precision, never blocks startup. Scrub never touches it.

use std::collections::{BinaryHeap, HashMap};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

/// A read refreshes the entry only when the old value is at least this old.
pub const REFRESH_GRANULARITY_MS: u64 = 10 * 60 * 1000;
/// Hard cap on one `ListOldestChunks` response.
pub const LIST_MAX: usize = 4096;

type Key = [u8; 32];

#[derive(Debug, PartialEq, Eq)]
pub enum ListError {
    /// Seeding has not finished yet.
    NotReady,
}

pub struct LastReadIndex {
    file: PathBuf,
    map: parking_lot::Mutex<HashMap<Key, (u64, u64)>>,
    dirty: AtomicBool,
    ready: AtomicBool,
}

impl LastReadIndex {
    pub fn new(disk_mount: &Path) -> Self {
        Self {
            file: disk_mount.join(".dtfs").join("last_read.tsv"),
            map: parking_lot::Mutex::new(HashMap::new()),
            dirty: AtomicBool::new(false),
            ready: AtomicBool::new(false),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// A client (non-scanner) read: refresh only an existing entry whose value
    /// is >= 10 min older than `now_ms`. Before seeding finished, a read known
    /// to be served from this disk (`on_disk_size` = its `.dat` size) inserts
    /// the missing entry so seeding keeps it. Returns true when it changed.
    pub fn touch(&self, id: &Key, now_ms: u64, scanner: bool, on_disk_size: Option<u64>) -> bool {
        if scanner {
            return false;
        }
        let mut m = self.map.lock();
        let changed = match m.get_mut(id) {
            Some(e) if now_ms.saturating_sub(e.0) >= REFRESH_GRANULARITY_MS => {
                e.0 = now_ms;
                true
            }
            Some(_) => false,
            None => match on_disk_size {
                Some(size) if !self.is_ready() => {
                    m.insert(*id, (now_ms, size));
                    true
                }
                _ => false,
            },
        };
        if changed {
            self.dirty.store(true, Ordering::Release);
        }
        changed
    }

    /// A commit: the chunk starts at `now_ms`.
    pub fn insert(&self, id: Key, now_ms: u64, size: u64) {
        self.map.lock().insert(id, (now_ms, size));
        self.dirty.store(true, Ordering::Release);
    }

    /// Delete or quarantine.
    pub fn remove(&self, id: &Key) {
        if self.map.lock().remove(id).is_some() {
            self.dirty.store(true, Ordering::Release);
        }
    }

    /// Entries with `last_read_ms <= older_than_ms` (0 = no filter), oldest
    /// first, at most `min(limit, LIST_MAX)` (limit 0 = LIST_MAX).
    /// Ordered by `(last_read_ms, id)`; `after` = exclusive cursor.
    pub fn oldest(&self, limit: usize, older_than_ms: u64, after: Option<(u64, Key)>) -> Result<Vec<(Key, u64, u64)>, ListError> {
        if !self.is_ready() {
            return Err(ListError::NotReady);
        }
        let limit = if limit == 0 { LIST_MAX } else { limit.min(LIST_MAX) };
        // Max-heap of the `limit` smallest (ms, id, size): bounded, no copy of the map under the lock.
        let mut heap: BinaryHeap<(u64, Key, u64)> = BinaryHeap::with_capacity(limit);
        for (k, e) in self.map.lock().iter() {
            if (older_than_ms != 0 && e.0 > older_than_ms) || after.is_some_and(|(ms, id)| (e.0, *k) <= (ms, id)) {
                continue;
            }
            let item = (e.0, *k, e.1);
            if heap.len() < limit {
                heap.push(item);
            } else if let Some(mut top) = heap.peek_mut() {
                if item < *top {
                    *top = item;
                }
            }
        }
        Ok(heap.into_sorted_vec().into_iter().map(|(ms, k, size)| (k, ms, size)).collect())
    }

    /// Seed from the on-disk chunk walk (`walked`: id, `.dat` mtime, size) and
    /// the persisted file: persisted value wins for chunks still present,
    /// persisted entries for absent chunks are dropped. A chunk with no
    /// persisted entry counts as just read: `max(mtime, seed_now_ms)` (the
    /// mtime is the copy's creation, not a read). A chunk committed or read
    /// while the walk ran keeps its live entry if newer.
    pub fn seed(&self, walked: Vec<(Key, u64, u64)>, seed_now_ms: u64) {
        let persisted = self.load_file();
        let mut m = self.map.lock();
        for (id, mtime, size) in walked {
            let ms = persisted.get(&id).copied().unwrap_or(mtime.max(seed_now_ms));
            m.entry(id).and_modify(|e| e.0 = e.0.max(ms)).or_insert((ms, size));
        }
        drop(m);
        self.dirty.store(true, Ordering::Release);
        self.ready.store(true, Ordering::Release);
    }

    fn load_file(&self) -> HashMap<Key, u64> {
        let mut out = HashMap::new();
        let text = match std::fs::read_to_string(&self.file) {
            Ok(t) => t,
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(path = %self.file.display(), error = %e, "last_read index unreadable; seeding every chunk as just read");
                }
                return out;
            }
        };
        let mut bad = 0usize;
        for line in text.lines().filter(|l| !l.is_empty()) {
            let parsed = line.split_once('\t').and_then(|(h, ms)| {
                let mut k = [0u8; 32];
                hex::decode_to_slice(h, &mut k).ok()?;
                Some((k, ms.parse::<u64>().ok()?))
            });
            match parsed {
                Some((k, ms)) => {
                    out.insert(k, ms);
                }
                None => bad += 1,
            }
        }
        if bad > 0 {
            tracing::warn!(path = %self.file.display(), bad_lines = bad, "last_read index has corrupt lines; ignored");
        }
        out
    }

    /// Write the file (tmp + `sync_all` + atomic rename) when dirty and
    /// seeded. Blocking: call from `spawn_blocking`. A failure re-arms dirty.
    pub fn save_if_dirty(&self) -> std::io::Result<()> {
        if !self.is_ready() || !self.dirty.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        let r = self.write_file();
        if r.is_err() {
            self.dirty.store(true, Ordering::Release);
        }
        r
    }

    fn write_file(&self) -> std::io::Result<()> {
        // Snapshot under the lock, format after: `touch` takes it on the read path.
        let snap: Vec<(Key, u64)> = self.map.lock().iter().map(|(k, e)| (*k, e.0)).collect();
        let mut text = String::with_capacity(snap.len() * 80);
        for (k, ms) in snap {
            text.push_str(&format!("{}\t{}\n", hex::encode(k), ms));
        }
        let tmp = self.file.with_extension("tsv.tmp");
        let mut f = std::fs::File::create(&tmp)?;
        let res = f.write_all(text.as_bytes()).and_then(|_| f.sync_all());
        if let Err(e) = res.and_then(|_| std::fs::rename(&tmp, &self.file)) {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
        Ok(())
    }
}

/// Walk `<disk>/chunks/<aa>/<bb>/<id>.dat` and return `(id, mtime_ms, size)`.
/// Blocking: call from `spawn_blocking`. Unreadable entries are skipped.
pub fn walk_dat_files(disk_mount: &Path) -> Vec<(Key, u64, u64)> {
    let mut out = Vec::new();
    let subdirs = |p: &Path| -> Vec<PathBuf> {
        std::fs::read_dir(p)
            .into_iter()
            .flatten()
            .flatten()
            .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
            .map(|e| e.path())
            .collect()
    };
    for d1 in subdirs(&disk_mount.join("chunks")) {
        for d2 in subdirs(&d1) {
            for e in std::fs::read_dir(&d2).into_iter().flatten().flatten() {
                let p = e.path();
                if p.extension().is_none_or(|x| x != "dat") {
                    continue;
                }
                let mut k = [0u8; 32];
                let Some(stem) = p.file_stem().and_then(|s| s.to_str()) else { continue };
                if hex::decode_to_slice(stem, &mut k).is_err() {
                    continue;
                }
                let Ok(md) = e.metadata() else { continue };
                let mtime = md
                    .modified()
                    .ok()
                    .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
                    .map_or(0, |d| d.as_millis() as u64);
                out.push((k, mtime, md.len()));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN: u64 = 60 * 1000;

    fn key(b: u8) -> Key {
        [b; 32]
    }

    fn ready_index(dir: &Path) -> LastReadIndex {
        std::fs::create_dir_all(dir.join(".dtfs")).unwrap();
        let ix = LastReadIndex::new(dir);
        ix.seed(vec![], 0);
        ix.dirty.store(false, Ordering::Release);
        ix
    }

    #[test]
    fn touch_is_coarse_and_sets_dirty() {
        let t = tempfile::tempdir().unwrap();
        let ix = ready_index(t.path());
        ix.insert(key(1), 1_000_000, 10);
        ix.dirty.store(false, Ordering::Release);
        assert!(!ix.touch(&key(1), 1_000_000 + 10 * MIN - 1, false, Some(10)), "< 10 min: no update");
        assert!(!ix.dirty.load(Ordering::Acquire));
        assert!(ix.touch(&key(1), 1_000_000 + 10 * MIN, false, None), ">= 10 min: update");
        assert!(ix.dirty.load(Ordering::Acquire));
        assert_eq!(ix.oldest(0, 0, None).unwrap()[0].1, 1_000_000 + 10 * MIN);
        assert!(!ix.touch(&key(9), 99 * MIN * 1000, false, Some(10)), "ready: unknown chunk is not inserted");
    }

    #[test]
    fn scanner_read_does_not_refresh() {
        let t = tempfile::tempdir().unwrap();
        let ix = ready_index(t.path());
        ix.insert(key(1), 1_000, 10);
        assert!(!ix.touch(&key(1), 1_000 + 60 * MIN, true, Some(10)));
        assert_eq!(ix.oldest(0, 0, None).unwrap()[0].1, 1_000);
    }

    #[test]
    fn insert_and_remove() {
        let t = tempfile::tempdir().unwrap();
        let ix = ready_index(t.path());
        ix.insert(key(1), 5, 10);
        assert_eq!(ix.oldest(0, 0, None).unwrap(), vec![(key(1), 5, 10)]);
        ix.remove(&key(1));
        assert!(ix.oldest(0, 0, None).unwrap().is_empty());
    }

    #[test]
    fn oldest_orders_limits_and_filters() {
        let t = tempfile::tempdir().unwrap();
        let ix = ready_index(t.path());
        for i in 0..10u8 {
            ix.insert(key(i), 100 + (9 - i as u64) * 10, 1); // key 9 oldest
        }
        let v = ix.oldest(3, 0, None).unwrap();
        assert_eq!(v.iter().map(|x| x.1).collect::<Vec<_>>(), vec![100, 110, 120]);
        assert_eq!(v[0].0, key(9));
        let f = ix.oldest(0, 120, None).unwrap();
        assert_eq!(f.len(), 3, "older_than is inclusive");
        assert_eq!(ix.oldest(0, 99, None).unwrap().len(), 0);
    }

    #[test]
    fn cursor_pages_are_disjoint_ordered_and_split_ties_by_id() {
        let t = tempfile::tempdir().unwrap();
        let ix = ready_index(t.path());
        // 10 entries, 4 distinct times: ties straddle page boundaries of 3.
        for i in 0..10u8 {
            ix.insert(key(i), 100 + (i as u64 / 3) * 10, 1);
        }
        let all = ix.oldest(0, 0, None).unwrap();
        assert_eq!(all.len(), 10);
        assert!(all.windows(2).all(|w| (w[0].1, w[0].0) < (w[1].1, w[1].0)), "total order");
        let mut got = Vec::new();
        let mut cur = None;
        loop {
            let p = ix.oldest(3, 0, cur).unwrap();
            if p.is_empty() {
                break;
            }
            cur = p.last().map(|l| (l.1, l.0));
            got.extend(p);
        }
        assert_eq!(got, all, "none lost or duplicated across page boundaries");
        assert_eq!(ix.oldest(3, 0, None).unwrap(), all[..3], "no cursor = first page");
        assert!(ix.oldest(3, 0, Some((u64::MAX, key(0)))).unwrap().is_empty());
    }

    #[test]
    fn oldest_caps_at_4096() {
        let t = tempfile::tempdir().unwrap();
        let ix = ready_index(t.path());
        for i in 0..(LIST_MAX as u32 + 50) {
            let mut k = [0u8; 32];
            k[..4].copy_from_slice(&i.to_be_bytes());
            ix.insert(k, 1000 + i as u64, 1);
        }
        assert_eq!(ix.oldest(0, 0, None).unwrap().len(), LIST_MAX);
        assert_eq!(ix.oldest(1_000_000, 0, None).unwrap().len(), LIST_MAX);
        assert_eq!(ix.oldest(7, 0, None).unwrap().len(), 7);
    }

    #[test]
    fn not_ready_is_an_error() {
        let t = tempfile::tempdir().unwrap();
        let ix = LastReadIndex::new(t.path());
        assert_eq!(ix.oldest(0, 0, None).unwrap_err(), ListError::NotReady);
    }

    #[test]
    fn load_ignores_corrupt_lines_drops_absent_and_overrides_mtime() {
        let t = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(t.path().join(".dtfs")).unwrap();
        let present = hex::encode(key(1));
        let absent = hex::encode(key(2));
        let text = format!("{present}\t777\ngarbage\n{absent}\t5\nzz\t1\n{}\tnotnum\n", hex::encode(key(3)));
        std::fs::write(t.path().join(".dtfs/last_read.tsv"), text).unwrap();
        let ix = LastReadIndex::new(t.path());
        ix.seed(vec![(key(1), 100, 4), (key(3), 200, 8)], 150);
        let v = ix.oldest(0, 0, None).unwrap();
        assert_eq!(v, vec![(key(3), 200, 8), (key(1), 777, 4)], "persisted overrides mtime; absent dropped; bad line falls back to mtime");
    }

    #[test]
    fn save_round_trips_and_skips_when_clean() {
        let t = tempfile::tempdir().unwrap();
        let ix = ready_index(t.path());
        ix.save_if_dirty().unwrap();
        assert!(!t.path().join(".dtfs/last_read.tsv").exists(), "clean: nothing written");
        ix.insert(key(1), 42, 9);
        ix.save_if_dirty().unwrap();
        assert!(!t.path().join(".dtfs/last_read.tsv.tmp").exists());
        let ix2 = LastReadIndex::new(t.path());
        ix2.seed(vec![(key(1), 1, 9)], 1_000);
        assert_eq!(ix2.oldest(0, 0, None).unwrap(), vec![(key(1), 42, 9)]);
    }

    /// DATA-1: no persisted entry (upgrade, lost or corrupt file) means "just
    /// read": seeded at the seed time, not the `.dat` mtime; a persisted
    /// entry still wins, and an mtime after the seed time is kept.
    #[test]
    fn unknown_chunk_seeds_at_seed_time_and_persisted_wins() {
        let t = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(t.path().join(".dtfs")).unwrap();
        std::fs::write(t.path().join(".dtfs/last_read.tsv"), format!("{}\t42\n", hex::encode(key(1)))).unwrap();
        let ix = LastReadIndex::new(t.path());
        ix.seed(vec![(key(1), 5, 1), (key(2), 5, 1), (key(3), 9_000, 1)], 1_000);
        let v = ix.oldest(0, 0, None).unwrap();
        assert_eq!(v, vec![(key(1), 42, 1), (key(2), 1_000, 1), (key(3), 9_000, 1)]);
    }

    /// DATA-1: a client read served from this disk before seeding finished is
    /// kept (and beats the persisted value); a read not known to be on this
    /// disk (`None`, the cache-hit path) inserts nothing.
    #[test]
    fn read_during_seeding_is_kept_only_when_on_disk() {
        let t = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(t.path().join(".dtfs")).unwrap();
        std::fs::write(t.path().join(".dtfs/last_read.tsv"), format!("{}\t42\n", hex::encode(key(1)))).unwrap();
        let ix = LastReadIndex::new(t.path());
        assert!(ix.touch(&key(1), 500, false, Some(7)));
        assert!(!ix.touch(&key(2), 500, false, None));
        assert!(!ix.touch(&key(3), 500, true, Some(7)), "scanner read");
        ix.seed(vec![(key(1), 5, 7), (key(2), 5, 7), (key(3), 5, 7)], 1_000);
        let v = ix.oldest(0, 0, None).unwrap();
        assert_eq!(v, vec![(key(1), 500, 7), (key(2), 1_000, 7), (key(3), 1_000, 7)]);
    }

    #[test]
    fn save_before_seed_writes_nothing() {
        let t = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(t.path().join(".dtfs")).unwrap();
        let ix = LastReadIndex::new(t.path());
        ix.insert(key(1), 1, 1);
        ix.save_if_dirty().unwrap();
        assert!(!t.path().join(".dtfs/last_read.tsv").exists());
    }
}
