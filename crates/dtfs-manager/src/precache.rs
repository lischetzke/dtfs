//! Predictive pre-caching, pure kernels (no I/O, no async, no locks):
//! playback ordering, series-root detection, selection and play detection.

use dtfs_raft::state_machine::FileMetadata;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// Distinct chunks remembered per (file, dc) before `overflow` is set.
pub(crate) const MAX_ACTIVITY_CHUNKS: usize = 64;
/// (file, dc) keys kept in the activity map; the stalest 10% go at the cap.
pub(crate) const MAX_ACTIVITY_KEYS: usize = 1024;
/// FUSE readahead / header probes touch chunks 0..8; only later reads count as playing.
pub(crate) const PLAY_SKIP_HEAD_CHUNKS: u64 = 9;
/// Tail probes (index containers) touch the last chunks.
pub(crate) const PLAY_SKIP_TAIL_CHUNKS: u64 = 2;

fn runs(s: &str) -> Vec<(bool, &str)> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut cur: Option<bool> = None;
    for (i, c) in s.char_indices() {
        let d = c.is_ascii_digit();
        if cur != Some(d) {
            if let Some(k) = cur {
                out.push((k, &s[start..i]));
            }
            start = i;
            cur = Some(d);
        }
    }
    if let Some(k) = cur {
        out.push((k, &s[start..]));
    }
    out
}

/// Natural order: digit runs numerically (no overflow), text case-insensitively, raw bytes last so it is total.
pub(crate) fn natural_cmp(a: &str, b: &str) -> Ordering {
    let (ra, rb) = (runs(a), runs(b));
    for (x, y) in ra.iter().zip(rb.iter()) {
        let ord = match (x.0, y.0) {
            (true, true) => {
                let (xa, ya) = (x.1.trim_start_matches('0'), y.1.trim_start_matches('0'));
                xa.len().cmp(&ya.len()).then_with(|| xa.cmp(ya))
            }
            (false, false) => x.1.to_lowercase().cmp(&y.1.to_lowercase()),
            // digits before text
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    ra.len()
        .cmp(&rb.len())
        .then_with(|| a.as_bytes().cmp(b.as_bytes()))
}

/// Orders relative paths component by component so Season 2/E10 < Season 10/E01.
pub(crate) fn playback_cmp(rel_a: &str, rel_b: &str) -> Ordering {
    let mut a = rel_a.split('/');
    let mut b = rel_b.split('/');
    loop {
        match (a.next(), b.next()) {
            (None, None) => return Ordering::Equal,
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (Some(x), Some(y)) => {
                let o = natural_cmp(x, y);
                if o != Ordering::Equal {
                    return o;
                }
            }
        }
    }
}

/// Highest ancestor within 2 levels (parent, grandparent) holding a regular-file `.plexmatch`.
pub(crate) fn series_root(files: &BTreeMap<String, FileMetadata>, path: &str) -> Option<String> {
    let mut dir = path;
    let mut found = None;
    for _ in 0..2 {
        dir = dir.rsplit_once('/')?.0;
        if dir.is_empty() {
            break;
        }
        let marker = format!("{dir}/.plexmatch");
        if files
            .get(&marker)
            .is_some_and(|f| !f.is_directory && f.symlink_target.is_none())
        {
            found = Some(dir.to_string());
        }
    }
    found
}

/// Folders that hold extras rather than episodes, excluded from playback order.
pub(crate) fn is_extras_dir(name: &str) -> bool {
    const EXTRAS: [&str; 12] = [
        "specials",
        "season 00",
        "season 0",
        "extras",
        "featurettes",
        "behind the scenes",
        "deleted scenes",
        "interviews",
        "scenes",
        "shorts",
        "trailers",
        "other",
    ];
    EXTRAS.contains(&name.to_lowercase().as_str())
}

/// How much of a file to pre-cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Take {
    Whole,
    Head(u64),
}

/// Greedy contiguous pick; 0 means "no limit" for either bound, both 0 selects nothing.
/// The first file that does not fit the byte budget gets a head of the leftover, then we stop.
pub(crate) fn select_following(sizes: &[u64], max_files: u32, max_bytes: u64) -> Vec<Take> {
    if max_files == 0 && max_bytes == 0 {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut left = if max_bytes == 0 { u64::MAX } else { max_bytes };
    for &size in sizes {
        if max_files != 0 && out.len() as u32 >= max_files {
            break;
        }
        if size <= left {
            out.push(Take::Whole);
            left -= size;
        } else {
            if left > 0 {
                out.push(Take::Head(left));
            }
            break;
        }
    }
    out
}

/// Leading whole chunks (index, bytes) that fit in `head_bytes`.
pub(crate) fn head_chunk_indexes(chunk_sizes: &[(u64, u64)], head_bytes: u64) -> Vec<u64> {
    let mut used = 0u64;
    let mut out = Vec::new();
    for &(idx, bytes) in chunk_sizes {
        used = used.saturating_add(bytes);
        if used > head_bytes {
            break;
        }
        out.push(idx);
    }
    out
}

/// A read beyond the header probe and before the tail probe, or many distinct chunks, means playback.
pub(crate) fn is_played(chunk_count: u64, read_indexes: &BTreeSet<u64>, overflow: bool) -> bool {
    overflow
        || read_indexes
            .iter()
            .any(|&i| i >= PLAY_SKIP_HEAD_CHUNKS && i + PLAY_SKIP_TAIL_CHUNKS < chunk_count)
}

/// Recent reads of one file in one DC.
#[derive(Debug, Clone, Default)]
pub(crate) struct FileActivity {
    pub last_read_ms: u64,
    pub chunk_ids: HashSet<String>,
    pub overflow: bool,
}

/// Records reads per (file, lowercased dc); empty and "unknown" DCs never promote, so are ignored.
pub(crate) fn note_read(
    activity: &mut HashMap<(String, String), FileActivity>,
    file_path: &str,
    chunk_id: &str,
    reads_by_dc: &HashMap<String, u32>,
    now_ms: u64,
) {
    for (dc, &n) in reads_by_dc {
        let dc = dc.to_lowercase();
        if n == 0 || dc.is_empty() || dc == "unknown" {
            continue;
        }
        let key = (file_path.to_string(), dc);
        if !activity.contains_key(&key) && activity.len() >= MAX_ACTIVITY_KEYS {
            let mut ages: Vec<((String, String), u64)> = activity
                .iter()
                .map(|(k, v)| (k.clone(), v.last_read_ms))
                .collect();
            ages.sort_by_key(|(_, t)| *t);
            for (k, _) in ages.into_iter().take(MAX_ACTIVITY_KEYS / 10) {
                activity.remove(&k);
            }
        }
        let a = activity.entry(key).or_default();
        a.last_read_ms = a.last_read_ms.max(now_ms);
        if !a.chunk_ids.contains(chunk_id) {
            if a.chunk_ids.len() >= MAX_ACTIVITY_CHUNKS {
                a.overflow = true;
            } else {
                a.chunk_ids.insert(chunk_id.to_string());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sorted(mut v: Vec<&str>, f: fn(&str, &str) -> Ordering) -> Vec<&str> {
        v.sort_by(|a, b| f(a, b));
        v
    }

    fn fm(path: &str, dir: bool, symlink: Option<&str>) -> FileMetadata {
        serde_json::from_value(serde_json::json!({
            "path": path, "volume_id": "v", "chunk_size": 1, "chunks": [],
            "created_at_ms": 0, "updated_at_ms": 0,
            "is_directory": dir, "symlink_target": symlink,
        }))
        .unwrap()
    }

    fn tree(entries: &[(&str, bool)]) -> BTreeMap<String, FileMetadata> {
        entries
            .iter()
            .map(|(p, d)| (p.to_string(), fm(p, *d, None)))
            .collect()
    }

    #[test]
    fn natural_cmp_orders_numbers_and_case() {
        assert_eq!(sorted(vec!["E10", "E2", "E1"], natural_cmp), ["E1", "E2", "E10"]);
        assert_eq!(
            sorted(vec!["S02E01", "S01E10.720p", "S01E09.1080p"], natural_cmp),
            ["S01E09.1080p", "S01E10.720p", "S02E01"]
        );
        assert_eq!(sorted(vec!["Season 10", "Season 2"], natural_cmp), ["Season 2", "Season 10"]);
        assert_eq!(natural_cmp("a.mkv", "B.mkv"), Ordering::Less);
        assert_ne!(natural_cmp("ep01", "ep1"), Ordering::Equal);
        assert_eq!(natural_cmp("ep01", "ep1"), natural_cmp("ep1", "ep01").reverse());
        let big = "9".repeat(25);
        let bigger = format!("1{}", "0".repeat(25));
        assert_eq!(natural_cmp(&big, &bigger), Ordering::Less);
    }

    #[test]
    fn playback_cmp_orders_by_component() {
        assert_eq!(
            sorted(
                vec!["Season 10/E01", "Season 2/E10", "Season 2/E2", "Season 2/E01"],
                playback_cmp
            ),
            ["Season 2/E01", "Season 2/E2", "Season 2/E10", "Season 10/E01"]
        );
    }

    #[test]
    fn series_root_cases() {
        let f = "/tv/Show/Season 1/e1.mkv";
        let t = tree(&[(f, false), ("/tv/Show/.plexmatch", false)]);
        assert_eq!(series_root(&t, f).as_deref(), Some("/tv/Show"));
        let m = "/tv/Mini/e1.mkv";
        let t = tree(&[(m, false), ("/tv/Mini/.plexmatch", false)]);
        assert_eq!(series_root(&t, m).as_deref(), Some("/tv/Mini"));
        let t = tree(&[
            (f, false),
            ("/tv/Show/.plexmatch", false),
            ("/tv/Show/Season 1/.plexmatch", false),
        ]);
        assert_eq!(series_root(&t, f).as_deref(), Some("/tv/Show"));
        let deep = "/tv/Show/Season 1/Disc 1/e1.mkv";
        let t = tree(&[(deep, false), ("/tv/Show/.plexmatch", false)]);
        assert_eq!(series_root(&t, deep), None);
        let t = tree(&[(f, false), ("/tv/Show/.plexmatch", true)]);
        assert_eq!(series_root(&t, f), None);
        let t = tree(&[(f, false)]);
        assert_eq!(series_root(&t, f), None);
        // a symlink marker is not a regular file
        let mut t = tree(&[(f, false)]);
        t.insert(
            "/tv/Show/.plexmatch".into(),
            fm("/tv/Show/.plexmatch", false, Some("/x")),
        );
        assert_eq!(series_root(&t, f), None);
    }

    #[test]
    fn extras_dir_detection() {
        assert!(is_extras_dir("SPECIALS"));
        assert!(is_extras_dir("Season 00"));
        assert!(!is_extras_dir("Season 1"));
    }

    #[test]
    fn select_following_cases() {
        use Take::*;
        assert_eq!(select_following(&[5, 5, 5], 0, 12), [Whole, Whole, Head(2)]);
        assert_eq!(select_following(&[5, 5, 5], 0, 10), [Whole, Whole]);
        assert_eq!(select_following(&[30], 0, 12), [Head(12)]);
        assert_eq!(select_following(&[1, 1, 1], 2, 0), [Whole, Whole]);
        assert_eq!(select_following(&[3, 3, 3], 3, 7), [Whole, Whole, Head(1)]);
        assert_eq!(select_following(&[3, 3, 3], 0, 0), Vec::<Take>::new());
    }

    #[test]
    fn head_chunk_indexes_cases() {
        let mib = 1u64 << 20;
        let c = [(0, 4 * mib), (1, 4 * mib), (2, 4 * mib), (3, 2 * mib)];
        assert_eq!(head_chunk_indexes(&c, 9 * mib), [0, 1]);
        assert_eq!(head_chunk_indexes(&c, 3 * mib), Vec::<u64>::new());
    }

    #[test]
    fn is_played_cases() {
        let s = |v: &[u64]| v.iter().copied().collect::<BTreeSet<u64>>();
        assert!(!is_played(20, &s(&[0, 1, 2, 3, 4, 5, 6, 7, 8]), false));
        assert!(!is_played(20, &s(&[0, 19]), false));
        assert!(is_played(20, &s(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 10]), false));
        assert!(!is_played(20, &s(&[18]), false));
        assert!(is_played(20, &s(&[]), true));
        assert!(!is_played(11, &s(&[9]), false));
    }

    #[test]
    fn note_read_cases() {
        let dcs = |d: &str| HashMap::from([(d.to_string(), 1u32)]);
        let mut a = HashMap::new();
        note_read(&mut a, "/f", "c0", &dcs("unknown"), 1);
        note_read(&mut a, "/f", "c0", &dcs(""), 1);
        assert!(a.is_empty());

        note_read(&mut a, "/f", "c0", &dcs("TORO"), 1);
        assert!(a.contains_key(&("/f".to_string(), "toro".to_string())));

        for i in 1..=64 {
            note_read(&mut a, "/f", &format!("c{i}"), &dcs("toro"), 2);
        }
        let act = &a[&("/f".to_string(), "toro".to_string())];
        assert_eq!(act.chunk_ids.len(), 64);
        assert!(act.overflow, "the 65th distinct chunk sets overflow");

        let mut a = HashMap::new();
        for i in 0..MAX_ACTIVITY_KEYS {
            note_read(&mut a, &format!("/f{i}"), "c", &dcs("toro"), 10 + i as u64);
        }
        assert_eq!(a.len(), MAX_ACTIVITY_KEYS);
        note_read(&mut a, "/new", "c", &dcs("toro"), 99_999);
        assert!(!a.contains_key(&("/f0".to_string(), "toro".to_string())));
        assert!(a.contains_key(&("/new".to_string(), "toro".to_string())));
        assert!(a.len() <= MAX_ACTIVITY_KEYS);
    }
}
