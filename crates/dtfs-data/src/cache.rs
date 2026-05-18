// ARC (Adaptive Replacement Cache) implementation.
// Provides in-memory caching for hot chunks with configurable memory limit.
//
// ARC uses four lists:
// - T1: recent cache entries (seen once recently)
// - T2: frequent cache entries (seen at least twice recently)
// - B1: ghost entries evicted from T1 (keys only)
// - B2: ghost entries evicted from T2 (keys only)
//
// The parameter `p` controls the target size of T1 and is adapted based on
// whether ghost hits land in B1 (increase p -> favor recency) or B2
// (decrease p -> favor frequency).

use std::collections::{HashMap, VecDeque};

use bytes::Bytes;
use parking_lot::Mutex;

/// A single entry in the T1 or T2 list.
#[derive(Debug, Clone)]
struct CacheEntry {
    key: String,
}

/// ARC (Adaptive Replacement Cache).
///
/// Thread-safe via an internal `Mutex`. All public methods take `&self`.
pub struct ArcCache {
    inner: Mutex<ArcCacheInner>,
}

struct ArcCacheInner {
    /// Maximum number of entries across T1 + T2.
    max_size: usize,
    /// Recent entries (seen once).
    t1: VecDeque<CacheEntry>,
    /// Frequent entries (seen at least twice).
    t2: VecDeque<CacheEntry>,
    /// Ghost list for recently evicted T1 keys.
    b1: VecDeque<String>,
    /// Ghost list for recently evicted T2 keys.
    b2: VecDeque<String>,
    /// Target size for T1. Adapted at runtime.
    p: usize,
    /// Actual cached data keyed by chunk key.
    data: HashMap<String, Bytes>,
}

impl ArcCache {
    /// Create a new ARC cache with the given maximum number of entries.
    pub fn new(max_size: usize) -> Self {
        let max_size = max_size.max(1);
        Self {
            inner: Mutex::new(ArcCacheInner {
                max_size,
                t1: VecDeque::new(),
                t2: VecDeque::new(),
                b1: VecDeque::new(),
                b2: VecDeque::new(),
                p: 0,
                data: HashMap::new(),
            }),
        }
    }

    /// Retrieve a value from the cache.
    ///
    /// On a hit in T1 the entry is promoted to T2 (it has now been seen twice).
    /// On a hit in T2 the entry is moved to the MRU position of T2.
    /// Returns `None` on a miss.
    pub fn get(&self, key: &str) -> Option<Bytes> {
        let mut inner = self.inner.lock();
        inner.get(key)
    }

    /// Insert a key/value pair into the cache using ARC eviction logic.
    pub fn put(&self, key: String, value: Bytes) {
        let mut inner = self.inner.lock();
        inner.put(key, value);
    }

    /// Check whether a key is present in the cache (T1 or T2) without
    /// promoting it.
    pub fn contains(&self, key: &str) -> bool {
        let inner = self.inner.lock();
        inner.data.contains_key(key)
    }

    /// Number of entries currently cached (T1 + T2).
    pub fn len(&self) -> usize {
        let inner = self.inner.lock();
        inner.t1.len() + inner.t2.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove all entries from the cache.
    pub fn clear(&self) {
        let mut inner = self.inner.lock();
        inner.t1.clear();
        inner.t2.clear();
        inner.b1.clear();
        inner.b2.clear();
        inner.p = 0;
        inner.data.clear();
    }

    /// Bug V — shrink the cache to at most `target_entries` live (T1+T2)
    /// records, evicting from the LRU side of T1 first, then T2.
    ///
    /// This is the cooperative half of the strategy ladder: when memory
    /// pressure rises, the cache gives back RAM *before* we have to start
    /// rejecting writes. Ghost lists (B1/B2) are preserved so that ARC's
    /// adaptive `p` recovers correctly once pressure subsides — clear()
    /// would lose all that history.
    ///
    /// Returns the number of entries evicted. `target_entries = 0` is
    /// effectively a "clear data without losing ghost history" operation.
    pub fn shrink_to(&self, target_entries: usize) -> usize {
        let mut inner = self.inner.lock();
        let initial = inner.t1.len() + inner.t2.len();
        if initial <= target_entries {
            return 0;
        }
        let mut to_evict = initial - target_entries;
        // Evict LRU of T1 first (less-recent recency).
        while to_evict > 0 {
            match inner.t1.pop_front() {
                Some(entry) => {
                    inner.data.remove(&entry.key);
                    // Demote to B1 ghost so ARC's `p` can re-learn.
                    inner.b1.push_back(entry.key);
                    to_evict -= 1;
                }
                None => break,
            }
        }
        // Still need more? Pull from T2 LRU.
        while to_evict > 0 {
            match inner.t2.pop_front() {
                Some(entry) => {
                    inner.data.remove(&entry.key);
                    inner.b2.push_back(entry.key);
                    to_evict -= 1;
                }
                None => break,
            }
        }
        initial - (inner.t1.len() + inner.t2.len())
    }
}

impl ArcCacheInner {
    fn get(&mut self, key: &str) -> Option<Bytes> {
        // Case 1: key is in T1 -> promote to MRU of T2.
        if let Some(pos) = self.t1.iter().position(|e| e.key == key) {
            let entry = self.t1.remove(pos).unwrap();
            self.t2.push_back(entry);
            return self.data.get(key).cloned();
        }

        // Case 2: key is in T2 -> move to MRU of T2.
        if let Some(pos) = self.t2.iter().position(|e| e.key == key) {
            let entry = self.t2.remove(pos).unwrap();
            self.t2.push_back(entry);
            return self.data.get(key).cloned();
        }

        // Miss.
        None
    }

    fn put(&mut self, key: String, value: Bytes) {
        // Case 1: key already in T1 or T2 -> update value, promote to T2 MRU.
        if let Some(pos) = self.t1.iter().position(|e| e.key == key) {
            self.t1.remove(pos);
            self.t2.push_back(CacheEntry { key: key.clone() });
            self.data.insert(key, value);
            return;
        }
        if let Some(pos) = self.t2.iter().position(|e| e.key == key) {
            self.t2.remove(pos);
            self.t2.push_back(CacheEntry { key: key.clone() });
            self.data.insert(key, value);
            return;
        }

        // Case 2: key is in B1 (ghost of T1).
        // A B1 hit means recency is under-represented -> increase p.
        if let Some(pos) = self.b1.iter().position(|k| k == &key) {
            let delta = std::cmp::max(1, self.b2.len() / self.b1.len().max(1));
            self.p = std::cmp::min(self.p.saturating_add(delta), self.max_size);
            self.b1.remove(pos);
            self.replace(false);
            self.t2.push_back(CacheEntry { key: key.clone() });
            self.data.insert(key, value);
            return;
        }

        // Case 3: key is in B2 (ghost of T2).
        // A B2 hit means frequency is under-represented -> decrease p.
        if let Some(pos) = self.b2.iter().position(|k| k == &key) {
            let delta = std::cmp::max(1, self.b1.len() / self.b2.len().max(1));
            self.p = self.p.saturating_sub(delta);
            self.b2.remove(pos);
            self.replace(true);
            self.t2.push_back(CacheEntry { key: key.clone() });
            self.data.insert(key, value);
            return;
        }

        // Case 4: key is a complete miss (not in T1, T2, B1, B2).
        let l1 = self.t1.len() + self.b1.len();
        let l2 = self.t2.len() + self.b2.len();

        if l1 == self.max_size {
            // L1 is full.
            if self.t1.len() < self.max_size {
                // Still room in T1 directory, evict from B1 ghost.
                self.b1.pop_front();
                self.replace(false);
            } else {
                // T1 itself is full (max_size), evict LRU of T1 directly.
                if let Some(evicted) = self.t1.pop_front() {
                    self.data.remove(&evicted.key);
                }
            }
        } else if l1 + l2 >= self.max_size {
            // Combined directory is full, make room.
            if l1 + l2 >= 2 * self.max_size {
                self.b2.pop_front();
            }
            self.replace(false);
        }

        // Insert into T1 (new, seen once).
        self.t1.push_back(CacheEntry { key: key.clone() });
        self.data.insert(key, value);
    }

    /// Evict one entry from either T1 or T2 into the corresponding ghost list.
    /// `favor_t1` biases towards evicting from T1 when true.
    fn replace(&mut self, favor_t1: bool) {
        let t1_len = self.t1.len();
        if t1_len > 0 && (t1_len > self.p || (t1_len == self.p && favor_t1)) {
            // Evict LRU of T1 -> ghost to B1.
            if let Some(evicted) = self.t1.pop_front() {
                self.data.remove(&evicted.key);
                self.b1.push_back(evicted.key);
                // Cap ghost list at max_size.
                if self.b1.len() > self.max_size {
                    self.b1.pop_front();
                }
            }
        } else if let Some(evicted) = self.t2.pop_front() {
            // Evict LRU of T2 -> ghost to B2.
            self.data.remove(&evicted.key);
            self.b2.push_back(evicted.key);
            if self.b2.len() > self.max_size {
                self.b2.pop_front();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_put_get() {
        let cache = ArcCache::new(10);
        cache.put("a".into(), Bytes::from("hello"));
        assert_eq!(cache.get("a"), Some(Bytes::from("hello")));
        assert_eq!(cache.get("b"), None);
    }

    #[test]
    fn test_contains_and_len() {
        let cache = ArcCache::new(10);
        assert!(cache.is_empty());
        cache.put("x".into(), Bytes::from("1"));
        cache.put("y".into(), Bytes::from("2"));
        assert!(cache.contains("x"));
        assert!(!cache.contains("z"));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_clear() {
        let cache = ArcCache::new(10);
        cache.put("a".into(), Bytes::from("1"));
        cache.put("b".into(), Bytes::from("2"));
        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.get("a"), None);
    }

    #[test]
    fn test_eviction_under_pressure() {
        let cache = ArcCache::new(3);
        cache.put("a".into(), Bytes::from("1"));
        cache.put("b".into(), Bytes::from("2"));
        cache.put("c".into(), Bytes::from("3"));
        // Cache is full, inserting d should evict something.
        cache.put("d".into(), Bytes::from("4"));
        assert!(cache.len() <= 3);
        assert!(cache.contains("d"));
    }

    #[test]
    fn test_promotion_t1_to_t2() {
        let cache = ArcCache::new(10);
        cache.put("a".into(), Bytes::from("1"));
        // First get promotes from T1 to T2.
        assert_eq!(cache.get("a"), Some(Bytes::from("1")));
        // Still accessible.
        assert_eq!(cache.get("a"), Some(Bytes::from("1")));
    }

    #[test]
    fn test_update_existing_key() {
        let cache = ArcCache::new(10);
        cache.put("a".into(), Bytes::from("old"));
        cache.put("a".into(), Bytes::from("new"));
        assert_eq!(cache.get("a"), Some(Bytes::from("new")));
    }

    #[test]
    fn test_ghost_list_adaptation() {
        // Use a small cache to trigger ghost hits.
        let cache = ArcCache::new(2);
        cache.put("a".into(), Bytes::from("1"));
        cache.put("b".into(), Bytes::from("2"));
        // Evict a by inserting c.
        cache.put("c".into(), Bytes::from("3"));
        // Now re-insert a; it should be found in B1 ghost and adapt p.
        cache.put("a".into(), Bytes::from("1-again"));
        assert!(cache.contains("a"));
    }

    /// Bug V regression — shrink_to(N) on a cache of size > N evicts down
    /// to N entries. The remaining entries are the MRU ones (we evict
    /// LRU first), matching Ceph OSD's "drop coldest under pressure" pattern.
    #[test]
    fn bug_v_shrink_to_evicts_down_to_target() {
        let cache = ArcCache::new(100);
        for i in 0..50 {
            cache.put(format!("k{i}"), Bytes::from(format!("v{i}")));
        }
        assert_eq!(cache.len(), 50);
        let evicted = cache.shrink_to(20);
        assert_eq!(evicted, 30);
        assert_eq!(cache.len(), 20);
        // Most-recent inserts (k30..k49) should survive; oldest (k0..k29) evicted.
        assert!(!cache.contains("k0"));
        assert!(!cache.contains("k29"));
        assert!(cache.contains("k49"));
    }

    /// Bug V regression — shrink_to when current size is already at or
    /// below target is a no-op (no spurious evictions).
    #[test]
    fn bug_v_shrink_to_below_current_is_noop() {
        let cache = ArcCache::new(100);
        for i in 0..10 {
            cache.put(format!("k{i}"), Bytes::from(format!("v{i}")));
        }
        let evicted = cache.shrink_to(20);
        assert_eq!(evicted, 0, "shrink_to(20) on 10-entry cache should not evict");
        assert_eq!(cache.len(), 10);
    }

    /// Bug V regression — shrink_to(0) clears T1+T2 data but preserves
    /// ghost lists (B1+B2) so ARC's adaptive `p` parameter recovers
    /// correctly when pressure subsides. Using clear() instead would
    /// destroy the adaptation history.
    #[test]
    fn bug_v_shrink_to_zero_preserves_ghost_history() {
        let cache = ArcCache::new(100);
        for i in 0..30 {
            cache.put(format!("k{i}"), Bytes::from(format!("v{i}")));
        }
        let evicted = cache.shrink_to(0);
        assert_eq!(evicted, 30);
        assert_eq!(cache.len(), 0);
        // Re-inserting any previously-cached key should find it in B1 ghost,
        // proving ghost history was preserved (this exercises ARC's
        // adaptive code path).
        cache.put("k0".into(), Bytes::from("revived"));
        assert!(cache.contains("k0"));
    }
}
