//! Predictive pre-caching, pure kernels (no I/O, no async, no locks):
//! playback ordering, series-root detection, selection and play detection.

use crate::scheduler::{path_matches_prefix, survivor_replica_tier};
use crate::tiering::{
    node_selection_view, select_promotion_target, tier_rank,
};
use dtfs_common::config::PlacementConfig;
use dtfs_common::types::{DiskStatus, DiskTier, PrecacheMode, VolumeConfig};
use dtfs_raft::state_machine::{FileMetadata, StateMachineData};
use serde::Serialize;
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Bound;

/// Distinct chunks remembered per (file, dc) before `overflow` is set.
pub(crate) const MAX_ACTIVITY_CHUNKS: usize = 64;
/// (file, dc) keys kept in the activity map; the stalest 10% go at the cap.
pub(crate) const MAX_ACTIVITY_KEYS: usize = 1024;
/// FUSE readahead (default 8 prefetch chunks; measured on the dev cluster: a
/// one-chunk `dd` reaches chunk index 9) and header probes touch the first
/// chunks; only later reads count as playing.
pub(crate) const PLAY_SKIP_HEAD_CHUNKS: u64 = 12;
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

// ---------------------------------------------------------------------------
// Planner: pure, one pass over a short SM read lock held by the caller.
// ---------------------------------------------------------------------------

const GIB: u64 = 1 << 30;
/// Files below this are sidecars (subtitles, nfo, artwork), never pre-cached.
pub(crate) const SIDECAR_MAX_BYTES: u64 = 16 << 20;
/// File-mode cursors kept per (volume, dc), newest first.
pub(crate) const MAX_FILE_CURSORS_PER_DC: usize = 8;
/// Directory entries scanned per listing.
pub(crate) const MAX_DIR_ENTRIES: usize = 10_000;
/// Share of a node's tier capacity pre-caching never fills.
pub(crate) const PRECACHE_RESERVE_PCT: u64 = 10;
/// Eligible files listed per pin per pass.
pub(crate) const MAX_PIN_FILES: usize = 5000;
/// Chunk entries one pass may walk under the SM read lock, cursors and pins together.
/// 200k entries (about 12 TiB of 64 MiB chunks on one replica) is tens of ms of pure
/// CPU (no I/O), so Raft apply never waits long on the lock; a typical library of a
/// few TiB fits, a pin larger than that is flagged `truncated` instead of walked.
pub(crate) const MAX_PLAN_CHUNK_VISITS: usize = 200_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SeriesEntry {
    pub cursor: String,
    pub last_read_ms: u64,
}

/// Leader-memory input: recent reads and the series LRU kept across passes.
#[derive(Debug, Clone, Default)]
pub(crate) struct PrecacheSnapshot {
    /// (file path, lowercased dc) -> reads, as built by `note_read`.
    pub activity: HashMap<(String, String), FileActivity>,
    /// (volume, dc, series root) -> cursor.
    pub series_lru: BTreeMap<(String, String, String), SeriesEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrecacheCopy {
    pub chunk_id: String,
    pub file_path: String,
    pub chunk_index: u64,
    pub source_node: u64,
    pub target_node: u64,
    pub tier: DiskTier,
    pub bytes: u64,
    /// (position in the cursor's selection, cursor recency rank, chunk index).
    pub rank: (u32, u32, u64),
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct LastBatch {
    pub new: u64,
    pub already: u64,
    pub deferred: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DcStatus {
    pub dc: String,
    pub tier: DiskTier,
    pub room_bytes: u64,
    pub desired_bytes: u64,
    pub cached_bytes: u64,
    pub capacity_limited: bool,
    pub no_target: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct FileStatus {
    pub path: String,
    pub bytes: u64,
    pub desired_bytes: u64,
    pub cached_bytes: u64,
    pub head: bool,
}

/// One series (`root`) or one file-mode cursor (`dir`).
#[derive(Debug, Clone, Serialize)]
pub(crate) struct GroupStatus {
    pub volume: String,
    pub dc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dir: Option<String>,
    pub cursor: String,
    pub last_read_ms: u64,
    pub budget_bytes: u64,
    pub desired_bytes: u64,
    pub cached_bytes: u64,
    pub files: Vec<FileStatus>,
}

/// A volume whose enabled policy no longer validates (set-cache / set-mandatory after set-precache).
#[derive(Debug, Clone, Serialize)]
pub(crate) struct SkippedVolume {
    pub volume: String,
    pub reason: &'static str,
    pub detail: String,
}

/// One pinned prefix in one DC.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct PinStatus {
    pub volume: String,
    pub prefix: String,
    pub dc: String,
    pub bytes: u64,
    pub cached_bytes: u64,
    pub truncated: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct PrecacheStatus {
    pub is_leader: bool,
    pub updated_at_ms: u64,
    pub rate_mbps: u64,
    /// Pre-cache scheduled nothing this pass because `live_tasks` (any class) reached the ceiling.
    pub yielded_live_tasks: bool,
    /// All live scheduler tasks of every class (what the yield decision looks at).
    pub live_tasks: u64,
    /// Pre-cache's own live copy tasks.
    pub inflight_tasks: u64,
    pub last_batch: LastBatch,
    pub dcs: Vec<DcStatus>,
    pub series: Vec<GroupStatus>,
    pub cursors: Vec<GroupStatus>,
    pub pins: Vec<PinStatus>,
    pub skipped_volumes: Vec<SkippedVolume>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PrecachePlan {
    /// Sorted by rank.
    pub copies: Vec<PrecacheCopy>,
    /// (chunk_id, node_id, disk_id) existing cache copies the idle demote must leave alone.
    pub protected: HashSet<(String, u64, u32)>,
    pub series_lru: BTreeMap<(String, String, String), SeriesEntry>,
    pub status: PrecacheStatus,
}

fn within_window(now_ms: u64, last_read_ms: u64, vol: &VolumeConfig) -> bool {
    now_ms.saturating_sub(last_read_ms) <= vol.cache_replicas.promote_window_secs.saturating_mul(1000)
}

fn dir_of(path: &str) -> &str {
    path.rsplit_once('/').map_or("", |(d, _)| d)
}

/// True when a directory component between `root` and the file is an extras folder.
fn in_extras(root: &str, path: &str) -> bool {
    let rel = path.strip_prefix(root).unwrap_or(path).trim_start_matches('/');
    let mut comps: Vec<&str> = rel.split('/').collect();
    comps.pop();
    comps.into_iter().any(is_extras_dir)
}

fn upsert(
    map: &mut BTreeMap<(String, String, String), SeriesEntry>,
    key: (String, String, String),
    path: &str,
    last_read_ms: u64,
) {
    let newer = map
        .get(&key)
        .map_or(true, |e| (last_read_ms, path) > (e.last_read_ms, e.cursor.as_str()));
    if newer {
        map.insert(key, SeriesEntry { cursor: path.to_string(), last_read_ms });
    }
}

/// Regular, unleased, non-hidden, non-sidecar file with chunks.
fn eligible_file(sm: &StateMachineData, path: &str, fm: &FileMetadata, now_ms: u64) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path);
    !fm.is_directory
        && fm.symlink_target.is_none()
        && !name.starts_with('.')
        && !fm.chunks.is_empty()
        && fm.reported_size() >= SIDECAR_MAX_BYTES
        && !sm.write_leases.get(path).is_some_and(|l| l.expires_at_ms > now_ms)
}

/// Entries under `prefix` (ends with '/'), bounded. `children_only` jumps over
/// each child directory's subtree so the cap counts direct children.
fn scan<'a>(
    sm: &'a StateMachineData,
    prefix: &str,
    children_only: bool,
) -> Vec<(&'a String, &'a FileMetadata)> {
    let mut out = Vec::new();
    let mut from = prefix.to_string();
    while out.len() < MAX_DIR_ENTRIES {
        let Some((k, fm)) = sm
            .files
            .range::<str, _>((Bound::Included(from.as_str()), Bound::Unbounded))
            .next()
            .filter(|(k, _)| k.starts_with(prefix))
        else {
            break;
        };
        let rel = &k[prefix.len()..];
        if let (true, Some((child, _))) = (children_only, rel.split_once('/')) {
            // '0' is the byte after '/': first key past "prefix/child/..."
            from = format!("{prefix}{child}0");
            continue;
        }
        out.push((k, fm));
        from = format!("{k}\0");
    }
    out
}

/// Eligible files in playback order that come after `cursor`.
fn following<'a>(
    sm: &'a StateMachineData,
    now_ms: u64,
    series_root: Option<&str>,
    dir: &str,
    cursor: &str,
    vid: &str,
) -> Vec<(&'a String, &'a FileMetadata)> {
    let base = series_root.unwrap_or(dir);
    let prefix = format!("{base}/");
    let Some(crel) = cursor.strip_prefix(prefix.as_str()) else {
        return Vec::new();
    };
    let mut v: Vec<(&str, &String, &FileMetadata)> = scan(sm, &prefix, series_root.is_none())
        .into_iter()
        .filter(|(_, fm)| fm.volume_id == vid)
        .filter_map(|(p, fm)| Some((p.strip_prefix(prefix.as_str())?, p, fm)))
        .filter(|(rel, p, fm)| {
            let in_scope = if series_root.is_some() {
                !in_extras(base, p)
            } else {
                !rel.contains('/')
            };
            in_scope && eligible_file(sm, p, fm, now_ms)
        })
        .collect();
    let cmp = |a: &str, b: &str| {
        if series_root.is_some() {
            playback_cmp(a, b)
        } else {
            natural_cmp(a, b)
        }
    };
    v.sort_by(|a, b| cmp(a.0, b.0));
    v.into_iter()
        .filter(|(rel, _, _)| cmp(rel, crel) == Ordering::Greater)
        .map(|(_, p, fm)| (p, fm))
        .collect()
}

/// Room pre-caching may use on one node's tier: (pooled free bytes minus the
/// reserve share of the tier total, free bytes of the largest single disk).
/// Both over serviceable, non-draining disks, minus headroom.
fn node_room(info: &dtfs_raft::NodeInfo, tier: DiskTier, cfg: &PlacementConfig) -> (u64, u64) {
    let (mut avail, mut total, mut biggest) = (0u64, 0u64, 0u64);
    for d in info.disks.iter().filter(|d| {
        d.tier == tier
            && !matches!(d.status, DiskStatus::Failed | DiskStatus::Removed | DiskStatus::Draining)
    }) {
        total = total.saturating_add(d.total_bytes);
        let free = d
            .total_bytes
            .saturating_sub(d.used_bytes)
            .saturating_sub(cfg.headroom_bytes(d.total_bytes));
        avail = avail.saturating_add(free);
        biggest = biggest.max(free);
    }
    (avail.saturating_sub(total.saturating_mul(PRECACHE_RESERVE_PCT) / 100), biggest)
}

struct Ctx<'a> {
    sm: &'a StateMachineData,
    now_ms: u64,
    placement: &'a PlacementConfig,
    room: HashMap<(u64, DiskTier), (u64, u64)>,
    seen: HashSet<(String, String)>,
    stopped: HashSet<String>,
    copies: Vec<PrecacheCopy>,
    protected: HashSet<(String, u64, u32)>,
    dcs: BTreeMap<String, DcStatus>,
    /// Chunk entries walked so far this pass (see `MAX_PLAN_CHUNK_VISITS`).
    visits: usize,
}

/// `pos` of pinned files (ranked after every cursor copy).
const PIN_POS: u32 = u32::MAX;

impl Ctx<'_> {
    fn room(&mut self, nid: u64, tier: DiskTier) -> u64 {
        let (sm, cfg) = (self.sm, self.placement);
        let (pooled, biggest) = *self.room.entry((nid, tier)).or_insert_with(|| {
            sm.nodes.get(&nid).map_or((0, 0), |n| node_room(n, tier, cfg))
        });
        pooled.min(biggest)
    }
}

fn cache_tiers(vol: &VolumeConfig) -> Vec<DiskTier> {
    let mand = vol.effective_mandatory_replicas();
    let c = &vol.cache_replicas;
    [
        (DiskTier::Hot, &c.hot),
        (DiskTier::Warm, &c.warm),
        (DiskTier::Cold, &c.cold),
        (DiskTier::Tape, &c.tape),
    ]
    .into_iter()
    .filter(|(t, cfg)| !cfg.is_disabled() && mand.for_tier(*t) == 0)
    .map(|(t, _)| t)
    .collect()
}

struct Group {
    vid: String,
    dc: String,
    last_read_ms: u64,
    cursor: String,
    root: Option<String>,
    dir: Option<String>,
}

impl Group {
    fn key(&self) -> &str {
        self.root.as_deref().or(self.dir.as_deref()).unwrap_or("")
    }
}

/// Register the status row of `dc` once: the hottest eligible tier with a usable target there.
fn ensure_dc(ctx: &mut Ctx, elig: &[DiskTier], dc: &str) {
    if ctx.dcs.contains_key(dc) {
        return;
    }
    let (sm, placement) = (ctx.sm, ctx.placement);
    let tier = elig
        .iter()
        .copied()
        .find(|t| {
            sm.nodes.iter().any(|(id, n)| {
                let v = node_selection_view(*id, n, *t, placement);
                v.datacenter.eq_ignore_ascii_case(dc) && v.has_target_tier_disk && !v.draining
            })
        })
        .unwrap_or(elig[0]);
    ctx.dcs.insert(
        dc.to_string(),
        DcStatus {
            dc: dc.to_string(),
            tier,
            room_bytes: 0,
            desired_bytes: 0,
            cached_bytes: 0,
            capacity_limited: false,
            no_target: false,
        },
    );
}

/// True when the volume has no anchor or a live replica sits in the anchor DC (and
/// on its `required_tier` when set): the `anchor_satisfied_by` rule of api/cluster.rs.
fn anchor_satisfied(live: &[(u64, u32, String, DiskTier)], vol: &VolumeConfig) -> bool {
    let Some(a) = vol.effective_anchor() else { return true };
    if a.datacenter.is_empty() {
        return true;
    }
    live.iter().any(|(_, _, dc, tier)| {
        dc.eq_ignore_ascii_case(&a.datacenter) && a.required_tier.map_or(true, |t| *tier == t)
    })
}

fn plan_file(
    ctx: &mut Ctx,
    vol: &VolumeConfig,
    elig: &[DiskTier],
    dc: &str,
    path: &str,
    fm: &FileMetadata,
    take: Take,
    pos: u32,
    rank: u32,
) -> FileStatus {
    ctx.visits = ctx.visits.saturating_add(fm.chunks.len());
    let mut by_idx: BTreeMap<u64, Vec<&dtfs_raft::ChunkLocationEntry>> = BTreeMap::new();
    for e in &fm.chunks {
        by_idx.entry(e.chunk_index).or_default().push(e);
    }
    let sizes: Vec<(u64, u64)> = by_idx.iter().map(|(i, e)| (*i, e[0].size_bytes)).collect();
    let wanted: Vec<u64> = match take {
        Take::Whole => sizes.iter().map(|c| c.0).collect(),
        Take::Head(n) => head_chunk_indexes(&sizes, n),
    };
    let mut fs = FileStatus {
        path: path.to_string(),
        bytes: fm.reported_size(),
        desired_bytes: 0,
        cached_bytes: 0,
        head: matches!(take, Take::Head(_)),
    };
    for idx in wanted {
        // A pin past its DC's room gains nothing from the rest of the file (cursors keep
        // walking: their cached chunks still need protection).
        if pos == PIN_POS && ctx.stopped.contains(dc) {
            break;
        }
        let entries = &by_idx[&idx];
        let (chunk_id, bytes) = (entries[0].chunk_hash.clone(), entries[0].size_bytes);
        fs.desired_bytes += bytes;
        ctx.dcs.get_mut(dc).expect("dc status").desired_bytes += bytes;

        let sm = ctx.sm;
        let mut live: Vec<(u64, u32, String, DiskTier)> = Vec::new();
        let mut holders: HashSet<u64> = HashSet::new();
        for e in entries.iter() {
            let Ok(nid) = e.node_id.parse::<u64>() else { continue };
            holders.insert(nid);
            let info = sm.nodes.get(&nid);
            if let (Some(info), Some(tier)) = (info, survivor_replica_tier(info, e.disk_id)) {
                live.push((nid, e.disk_id, info.location.datacenter.clone(), tier));
            }
        }
        if live.is_empty() {
            continue;
        }
        // Anchor-pending (no live replica satisfying the anchor's DC and required tier,
        // same test as `anchor_satisfied_by`): the anchor-lag copy comes first.
        if !anchor_satisfied(&live, vol) {
            continue;
        }
        // Already cached in this DC: protect it, plan nothing.
        let cached: Vec<(u64, u32)> = live
            .iter()
            .filter(|(_, _, d, t)| d.eq_ignore_ascii_case(dc) && elig.contains(t))
            .map(|(n, disk, _, _)| (*n, *disk))
            .collect();
        if !cached.is_empty() {
            fs.cached_bytes += bytes;
            ctx.dcs.get_mut(dc).expect("dc status").cached_bytes += bytes;
            for (n, disk) in cached {
                ctx.protected.insert((chunk_id.clone(), n, disk));
            }
            continue;
        }
        if fm.cache_cooldown_until_ms.get(&idx).copied().unwrap_or(0) > ctx.now_ms {
            continue;
        }
        if ctx.stopped.contains(dc) || !ctx.seen.insert((chunk_id.clone(), dc.to_string())) {
            continue;
        }

        let mut chosen: Option<(u64, DiskTier)> = None;
        let mut room_short = false;
        for &tier in elig {
            let mut views = Vec::new();
            for (nid, info) in sm.nodes.iter() {
                if !info.location.datacenter.eq_ignore_ascii_case(dc) {
                    continue;
                }
                let mut v = node_selection_view(*nid, info, tier, ctx.placement);
                // Same rule as `survivor_replica_tier`: such a copy would not count as cached.
                if info.quarantine.is_some() || info.health == dtfs_common::types::HealthStatus::Maintenance {
                    v.draining = true;
                }
                v.already_holds_chunk = holders.contains(nid);
                views.push(v);
            }
            let mut gated = views.clone();
            for v in gated.iter_mut() {
                if v.has_target_tier_disk && ctx.room(v.node_id, tier) < bytes {
                    v.has_target_tier_disk = false;
                }
            }
            if let Some(t) = select_promotion_target(&gated, dc) {
                chosen = Some((t, tier));
                break;
            }
            room_short |= select_promotion_target(&views, dc).is_some();
        }
        let Some((target, tier)) = chosen else {
            let st = ctx.dcs.get_mut(dc).expect("dc status");
            if room_short {
                st.capacity_limited = true;
                ctx.stopped.insert(dc.to_string());
            } else {
                st.no_target = true;
            }
            continue;
        };
        let left = ctx.room.get_mut(&(target, tier)).expect("room initialised");
        // shortcut: the copy is charged to the largest disk, the real pick may differ.
        left.0 -= bytes;
        left.1 -= bytes;
        let source = live
            .iter()
            .min_by_key(|(n, _, _, t)| (tier_rank(*t), *n))
            .map(|(n, _, _, _)| *n)
            .expect("live non-empty");
        ctx.copies.push(PrecacheCopy {
            chunk_id,
            file_path: path.to_string(),
            chunk_index: idx,
            source_node: source,
            target_node: target,
            tier,
            bytes,
            rank: (pos, rank, idx),
        });
    }
    fs
}

/// Decide which cache copies to add. Pure: the caller holds one short SM read
/// lock and feeds the leader-memory snapshot; nothing is awaited or mutated.
/// Never removes anything and never targets a mandatory tier.
pub(crate) fn plan_precache(
    sm: &StateMachineData,
    snap: &PrecacheSnapshot,
    now_ms: u64,
    placement: &PlacementConfig,
) -> PrecachePlan {
    plan_precache_budget(sm, snap, now_ms, placement, MAX_PLAN_CHUNK_VISITS)
}

fn plan_precache_budget(
    sm: &StateMachineData,
    snap: &PrecacheSnapshot,
    now_ms: u64,
    placement: &PlacementConfig,
    budget: usize,
) -> PrecachePlan {
    let mut plan = PrecachePlan::default();
    plan.status.updated_at_ms = now_ms;

    // 1. Active volumes; an enabled policy that stopped validating is skipped, not planned.
    let mut vols: BTreeMap<&str, &VolumeConfig> = BTreeMap::new();
    for (vid, v) in &sm.volumes {
        if v.precache.mode == PrecacheMode::Off && v.precache.pins.is_empty() {
            continue;
        }
        match v.precache.validate(&v.cache_replicas, v.effective_mandatory_replicas()) {
            Ok(()) => {
                vols.insert(vid.as_str(), v);
            }
            Err(detail) => plan.status.skipped_volumes.push(SkippedVolume {
                volume: vid.clone(),
                reason: "policy_invalid",
                detail,
            }),
        }
    }

    // 2-3. Played files inside the window become cursors.
    let mut series: BTreeMap<(String, String, String), SeriesEntry> = BTreeMap::new();
    let mut files: BTreeMap<(String, String, String), SeriesEntry> = BTreeMap::new();
    for ((path, dc), act) in &snap.activity {
        let Some(fm) = sm.files.get(path) else { continue };
        let Some(vol) = vols.get(fm.volume_id.as_str()) else { continue };
        // Mode off with pins only: reads create no cursors.
        if vol.precache.mode == PrecacheMode::Off {
            continue;
        }
        if fm.is_directory || fm.symlink_target.is_some() || !within_window(now_ms, act.last_read_ms, vol) {
            continue;
        }
        let read: BTreeSet<u64> = fm
            .chunks
            .iter()
            .filter(|c| act.chunk_ids.contains(&c.chunk_hash))
            .map(|c| c.chunk_index)
            .collect();
        let count = fm.chunks.iter().map(|c| c.chunk_index).collect::<BTreeSet<_>>().len() as u64;
        if !is_played(count, &read, act.overflow) {
            continue;
        }
        let dc = dc.to_lowercase();
        let root = (vol.precache.mode == PrecacheMode::Plex)
            .then(|| series_root(&sm.files, path))
            .flatten()
            .filter(|r| !in_extras(r, path));
        match root {
            Some(r) => upsert(&mut series, (fm.volume_id.clone(), dc, r), path, act.last_read_ms),
            None => upsert(
                &mut files,
                (fm.volume_id.clone(), dc, dir_of(path).to_string()),
                path,
                act.last_read_ms,
            ),
        }
    }

    // 4. Merge the series LRU (entries still in the window whose cursor exists), keep the top max_series.
    for (k, e) in &snap.series_lru {
        let Some(vol) = vols.get(k.0.as_str()) else { continue };
        if vol.precache.mode == PrecacheMode::Plex
            && sm.files.contains_key(&e.cursor)
            && within_window(now_ms, e.last_read_ms, vol)
        {
            upsert(&mut series, k.clone(), &e.cursor, e.last_read_ms);
        }
    }
    let mut ordered: Vec<_> = series.into_iter().collect();
    ordered.sort_by(|a, b| {
        (&a.0 .0, &a.0 .1, Reverse(a.1.last_read_ms), &a.0 .2)
            .cmp(&(&b.0 .0, &b.0 .1, Reverse(b.1.last_read_ms), &b.0 .2))
    });
    let mut groups: Vec<Group> = Vec::new();
    let mut run: (String, String, usize) = (String::new(), String::new(), 0);
    for (k, e) in ordered {
        if (&run.0, &run.1) != (&k.0, &k.1) {
            run = (k.0.clone(), k.1.clone(), 0);
        }
        run.2 += 1;
        if run.2 > vols[k.0.as_str()].precache.max_series as usize {
            continue;
        }
        plan.series_lru.insert(k.clone(), e.clone());
        groups.push(Group {
            vid: k.0,
            dc: k.1,
            last_read_ms: e.last_read_ms,
            cursor: e.cursor,
            root: Some(k.2),
            dir: None,
        });
    }
    // 5. File cursors: newest MAX_FILE_CURSORS_PER_DC per (volume, dc).
    let mut ordered: Vec<_> = files.into_iter().collect();
    ordered.sort_by(|a, b| {
        (&a.0 .0, &a.0 .1, Reverse(a.1.last_read_ms), &a.0 .2)
            .cmp(&(&b.0 .0, &b.0 .1, Reverse(b.1.last_read_ms), &b.0 .2))
    });
    let mut run: (String, String, usize) = (String::new(), String::new(), 0);
    for (k, e) in ordered {
        if (&run.0, &run.1) != (&k.0, &k.1) {
            run = (k.0.clone(), k.1.clone(), 0);
        }
        run.2 += 1;
        if run.2 > MAX_FILE_CURSORS_PER_DC {
            continue;
        }
        groups.push(Group {
            vid: k.0,
            dc: k.1,
            last_read_ms: e.last_read_ms,
            cursor: e.cursor,
            root: None,
            dir: Some(k.2),
        });
    }
    groups.sort_by(|a, b| {
        (&a.vid, &a.dc, Reverse(a.last_read_ms), a.root.is_none(), a.key())
            .cmp(&(&b.vid, &b.dc, Reverse(b.last_read_ms), b.root.is_none(), b.key()))
    });

    // 6-9. Select, expand to chunks, place.
    let mut ctx = Ctx {
        sm,
        now_ms,
        placement,
        room: HashMap::new(),
        seen: HashSet::new(),
        stopped: HashSet::new(),
        copies: Vec::new(),
        protected: HashSet::new(),
        dcs: BTreeMap::new(),
        visits: 0,
    };
    let mut rank_run: (String, String, u32) = (String::new(), String::new(), 0);
    for g in groups {
        let vol = vols[g.vid.as_str()];
        let elig = cache_tiers(vol);
        let pol = &vol.precache;
        if (&rank_run.0, &rank_run.1) != (&g.vid, &g.dc) {
            rank_run = (g.vid.clone(), g.dc.clone(), 0);
        }
        let rank = rank_run.2;
        rank_run.2 += 1;

        ensure_dc(&mut ctx, &elig, &g.dc);
        let (max_files, max_bytes) = if g.root.is_some() {
            (0, u64::from(pol.series_max_gb) * GIB)
        } else {
            (pol.next_files, u64::from(pol.next_max_gb) * GIB)
        };
        let dir = g.dir.clone().unwrap_or_default();
        let next = following(sm, now_ms, g.root.as_deref(), &dir, &g.cursor, &g.vid);
        let sizes: Vec<u64> = next.iter().map(|(_, f)| f.reported_size()).collect();
        let takes = select_following(&sizes, max_files, max_bytes);
        let mut st = GroupStatus {
            volume: g.vid.clone(),
            dc: g.dc.clone(),
            root: g.root.clone(),
            dir: g.dir.clone(),
            cursor: g.cursor.clone(),
            last_read_ms: g.last_read_ms,
            budget_bytes: max_bytes,
            desired_bytes: 0,
            cached_bytes: 0,
            files: Vec::new(),
        };
        for (pos, (take, (path, fm))) in takes.into_iter().zip(next).enumerate() {
            let fs = plan_file(&mut ctx, vol, &elig, &g.dc, path, fm, take, pos as u32, rank);
            st.desired_bytes += fs.desired_bytes;
            st.cached_bytes += fs.cached_bytes;
            st.files.push(fs);
        }
        if g.root.is_some() {
            plan.status.series.push(st);
        } else {
            plan.status.cursors.push(st);
        }
    }

    // Pins: whole files under each pinned prefix, ranked after every cursor copy.
    let mut pin_ord: u32 = 0;
    for (vid, vol) in &vols {
        let elig = cache_tiers(vol);
        for pin in &vol.precache.pins {
            let dc = pin.datacenter.to_lowercase();
            ensure_dc(&mut ctx, &elig, &dc);
            let mut ps = PinStatus {
                volume: (*vid).to_string(),
                prefix: pin.prefix.clone(),
                dc: dc.clone(),
                bytes: 0,
                cached_bytes: 0,
                truncated: false,
            };
            // Budget gone: do not even list the pin.
            if ctx.visits >= budget {
                ps.truncated = true;
                plan.status.pins.push(ps);
                pin_ord += 1;
                continue;
            }
            let base = pin.prefix.trim_end_matches('/');
            let mut listed: Vec<(&String, &FileMetadata)> = sm
                .files
                .range::<str, _>((Bound::Included(base), Bound::Unbounded))
                .take_while(|(k, _)| k.starts_with(base))
                .filter(|(k, fm)| {
                    path_matches_prefix(k, base)
                        && fm.volume_id == *vid
                        && eligible_file(sm, k, fm, now_ms)
                })
                .take(MAX_PIN_FILES + 1)
                .collect();
            ps.truncated = listed.len() > MAX_PIN_FILES;
            listed.truncate(MAX_PIN_FILES);
            for (path, fm) in listed {
                // Budget spent or the DC is out of room: the rest of the pin is not walked.
                if ctx.visits >= budget || ctx.stopped.contains(&dc) {
                    ps.truncated = true;
                    break;
                }
                let fs = plan_file(&mut ctx, vol, &elig, &dc, path, fm, Take::Whole, PIN_POS, pin_ord);
                ps.bytes += fs.desired_bytes;
                ps.cached_bytes += fs.cached_bytes;
            }
            pin_ord += 1;
            plan.status.pins.push(ps);
        }
    }

    // 10. Final order and status.
    let mut dcs: Vec<DcStatus> = ctx.dcs.values().cloned().collect();
    for d in dcs.iter_mut() {
        let ids: Vec<u64> = sm
            .nodes
            .iter()
            .filter(|(_, n)| n.location.datacenter.eq_ignore_ascii_case(&d.dc))
            .map(|(id, _)| *id)
            .collect();
        d.room_bytes = ids.into_iter().map(|id| ctx.room(id, d.tier)).sum();
    }
    plan.status.dcs = dcs;
    plan.protected = ctx.protected;
    plan.copies = ctx.copies;
    plan.copies.sort_by(|a, b| {
        a.rank
            .cmp(&b.rank)
            .then_with(|| a.chunk_id.cmp(&b.chunk_id))
            .then_with(|| a.target_node.cmp(&b.target_node))
    });
    plan
}

// ---------------------------------------------------------------------------
// Engine glue kernels: rate limit, yield, protection, leader-local state.
// ---------------------------------------------------------------------------

/// Seconds between pre-cache passes.
pub(crate) const PRECACHE_TICK_SECS: u64 = 30;
/// Start offset of the pre-cache loop. The scheduler ticks every 30 s too and
/// holds the migration lock per tick; started in phase, every pass was Deferred.
pub(crate) const PRECACHE_PHASE_SECS: u64 = 15;
/// Scheduler live tasks (any class) at or above which pre-cache schedules nothing.
pub(crate) const PRECACHE_YIELD_LIVE_TASKS: usize = 24;
/// Token-bucket depth in seconds of `precache_max_mbps`.
pub(crate) const PRECACHE_BURST_SECS: u64 = 120;

/// Leader-memory state of the pre-cache loop. Never held across an await.
#[derive(Debug, Default)]
pub(crate) struct PrecacheState {
    /// (file, dc) reads fed by `TieringEngine::report_access`; kept across leader changes.
    pub activity: HashMap<(String, String), FileActivity>,
    pub series_lru: BTreeMap<(String, String, String), SeriesEntry>,
    /// (chunk_id, node_id, disk_id) existing cache copies the idle demote must leave alone.
    pub protected: HashSet<(String, u64, u32)>,
    /// (chunk_id, target node) pairs this loop has a live task for (also protected from
    /// idle demote: the target holds no copy yet, so any disk of it is covered).
    pub scheduled: HashSet<(String, u64)>,
    /// Token balance in bytes and when it was last refilled.
    pub tokens: u64,
    pub tokens_at_ms: u64,
    pub status: PrecacheStatus,
    /// Volumes already warned about as `policy_invalid` (log once per transition).
    pub invalid_logged: HashSet<String>,
}

impl PrecacheState {
    /// Leadership lost: drop everything derived from leader tenure; activity stays.
    pub(crate) fn reset_leader_state(&mut self) {
        self.protected.clear();
        self.scheduled.clear();
        self.series_lru.clear();
        self.invalid_logged.clear();
        self.status = PrecacheStatus::default();
    }
}

/// Add `rate_mbps` worth of bytes for the elapsed time, capped at `burst_secs` of rate.
pub(crate) fn refill_tokens(
    tokens: u64,
    last_ms: u64,
    now_ms: u64,
    rate_mbps: u64,
    burst_secs: u64,
    min_cap: u64,
) -> u64 {
    if now_ms < last_ms {
        return tokens;
    }
    let per_sec = u128::from(rate_mbps) * 125_000;
    // A chunk larger than the burst must still fit, or it would never schedule.
    let cap = (per_sec * u128::from(burst_secs)).max(u128::from(min_cap));
    let add = per_sec * u128::from(now_ms - last_ms) / 1000;
    (u128::from(tokens) + add).min(cap) as u64
}

/// Leading copies (in plan order) that fit the token balance, skipping pairs with a live
/// task. Stops at the first copy that does not fit so rank order is never skipped over.
pub(crate) fn pick_within_budget<'a>(
    copies: &'a [PrecacheCopy],
    active: &HashSet<(String, u64)>,
    tokens: u64,
) -> Vec<&'a PrecacheCopy> {
    let mut left = tokens;
    let mut out = Vec::new();
    for c in copies {
        if active.contains(&(c.chunk_id.clone(), c.target_node)) {
            continue;
        }
        if c.bytes > left {
            break;
        }
        left -= c.bytes;
        out.push(c);
    }
    out
}

pub(crate) fn should_yield(live_tasks: usize) -> bool {
    live_tasks >= PRECACHE_YIELD_LIVE_TASKS
}

/// New tasks pre-cache may add this pass: the room left under its yield ceiling.
pub(crate) fn precache_max_new(live_tasks: usize) -> usize {
    PRECACHE_YIELD_LIVE_TASKS.saturating_sub(live_tasks)
}

/// Drop idle demotions of protected copies; returns how many were dropped.
pub(crate) fn retain_unprotected(
    demotions: &mut Vec<crate::tiering::TickDemotion>,
    protected: &HashSet<(String, u64, u32)>,
    scheduled: &HashSet<(String, u64)>,
) -> usize {
    let before = demotions.len();
    demotions.retain(|d| {
        !protected.contains(&(d.chunk_id.clone(), d.node_id, d.disk_id))
            && !scheduled.contains(&(d.chunk_id.clone(), d.node_id))
    });
    before - demotions.len()
}

/// Pre-cache's own tasks still live: pairs it scheduled earlier that the scheduler still
/// holds in flight (the yield decision looks at ALL live tasks, the metric only at these).
pub(crate) fn own_inflight(prev_scheduled: &HashSet<(String, u64)>, live: &HashSet<(String, u64)>) -> u64 {
    prev_scheduled.iter().filter(|p| live.contains(*p)).count() as u64
}

/// Volumes newly `policy_invalid` since the last pass; `logged` becomes the current set.
pub(crate) fn newly_invalid<'a>(
    logged: &mut HashSet<String>,
    skipped: &'a [SkippedVolume],
) -> Vec<&'a SkippedVolume> {
    let fresh = skipped.iter().filter(|s| !logged.contains(&s.volume)).collect();
    *logged = skipped.iter().map(|s| s.volume.clone()).collect();
    fresh
}

#[cfg(test)]
mod tests {
    #[test]
    fn precache_loop_is_out_of_phase_with_scheduler_tick() {
        assert_eq!(super::PRECACHE_TICK_SECS % 30, 0, "tick must equal the scheduler tick for the offset to matter");
        assert!(super::PRECACHE_PHASE_SECS > 0 && super::PRECACHE_PHASE_SECS < super::PRECACHE_TICK_SECS);
    }

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
        assert!(!is_played(20, &s(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]), false));
        assert!(!is_played(20, &s(&[0, 19]), false));
        assert!(is_played(20, &s(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 12]), false));
        assert!(!is_played(20, &s(&[18]), false));
        assert!(is_played(20, &s(&[]), true));
        assert!(!is_played(14, &s(&[12]), false));
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

#[cfg(test)]
mod planner_tests {
    use super::*;
    use dtfs_common::topology::Location;
    use dtfs_common::types::{
        AnchorPolicy, CacheTierConfig, DiskInfo, HealthStatus, NodeRole, TierReplication,
    };
    use dtfs_raft::state_machine::WriteLease;
    use dtfs_raft::NodeInfo;
    use serde_json::json;

    const MIB: u64 = 1 << 20;
    const NOW: u64 = 1_000_000;
    const CHUNK: u64 = 4 * MIB;

    fn cfg() -> PlacementConfig {
        PlacementConfig { min_free_bytes: 0, min_free_pct: 0.0, ..Default::default() }
    }

    fn node(id: u64, dc: &str, tier: DiskTier, total: u64, used: u64) -> NodeInfo {
        NodeInfo {
            role: NodeRole::Data,
            hostname: format!("node{id}"),
            grpc_addr: format!("http://10.0.0.{id}:9500"),
            location: Location::new("r", dc, "rack", &format!("host{id}")),
            disks: vec![DiskInfo {
                disk_id: 0,
                uuid: format!("u{id}"),
                mount_path: "/d".into(),
                tier,
                total_bytes: total,
                used_bytes: used,
                status: DiskStatus::Healthy,
            }],
            total_capacity: total,
            used_capacity: used,
            health: HealthStatus::Healthy,
            last_seen_ms: 0,
            total_bytes_read: 0,
            total_bytes_written: 0,
            client_ip: None,
            version: String::new(),
            build_date: String::new(),
            quarantine: None,
        }
    }

    struct W {
        sm: StateMachineData,
    }

    /// leli/cold(1), toro/warm(2), nrb/warm(3); anchor leli/cold; mandatory cold=1; warm cache 1/1 per_dc; plex.
    fn world() -> W {
        let mut sm = StateMachineData::default();
        let tib = 1u64 << 40;
        sm.nodes.insert(1, node(1, "leli", DiskTier::Cold, tib, 0));
        sm.nodes.insert(2, node(2, "toro", DiskTier::Warm, tib, 0));
        sm.nodes.insert(3, node(3, "nrb", DiskTier::Warm, tib, 0));
        let mut v = VolumeConfig::default();
        v.mandatory_replicas = TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 };
        v.cache_replicas.hot = CacheTierConfig { min: 0, max: 0 };
        v.cache_replicas.warm = CacheTierConfig { min: 1, max: 1 };
        v.cache_replicas.cold = CacheTierConfig { min: 0, max: 0 };
        v.cache_replicas.tape = CacheTierConfig { min: 0, max: 0 };
        v.cache_replicas.per_dc = true;
        v.cache_replicas.promote_window_secs = 4 * 3600;
        v.anchor = Some(AnchorPolicy { datacenter: "leli".into(), required_tier: Some(DiskTier::Cold) });
        v.precache.mode = PrecacheMode::Plex;
        v.precache.next_files = 0;
        v.precache.next_max_gb = 0;
        v.precache.series_max_gb = 1;
        v.precache.max_series = 3;
        sm.volumes.insert("v".into(), v);
        W { sm }
    }

    impl W {
        fn vol(&mut self) -> &mut VolumeConfig {
            self.sm.volumes.get_mut("v").unwrap()
        }
        fn file(&mut self, path: &str, nchunks: u64, holders: &[u64]) {
            let mut chunks = Vec::new();
            for i in 0..nchunks {
                for h in holders {
                    chunks.push(json!({"chunk_index": i, "chunk_hash": format!("{path}#{i}"),
                        "node_id": h.to_string(), "disk_id": 0, "size_bytes": CHUNK}));
                }
            }
            let fm: FileMetadata = serde_json::from_value(json!({
                "path": path, "volume_id": "v", "chunk_size": CHUNK, "chunks": chunks,
                "created_at_ms": 0, "updated_at_ms": 0,
            }))
            .unwrap();
            self.sm.files.insert(path.to_string(), fm);
        }
        /// 300 MiB episodes on node 1 plus a `.plexmatch` at `root`.
        fn series(&mut self, root: &str, eps: &[&str]) {
            self.file(&format!("{root}/.plexmatch"), 0, &[]);
            for e in eps {
                self.file(&format!("{root}/{e}"), 75, &[1]);
            }
        }
        fn set_disk(&mut self, id: u64, total: u64, used: u64) {
            let d = &mut self.sm.nodes.get_mut(&id).unwrap().disks[0];
            d.total_bytes = total;
            d.used_bytes = used;
        }
        fn plan(&self, reads: &[(&str, &str)]) -> PrecachePlan {
            let mut snap = PrecacheSnapshot::default();
            for (path, dc) in reads {
                let a = FileActivity {
                    last_read_ms: NOW,
                    chunk_ids: [format!("{path}#20")].into_iter().collect(),
                    overflow: false,
                };
                snap.activity.insert((path.to_string(), dc.to_string()), a);
            }
            plan_precache(&self.sm, &snap, NOW, &cfg())
        }
    }

    fn per_path(p: &PrecachePlan) -> BTreeMap<String, usize> {
        let mut m = BTreeMap::new();
        for c in &p.copies {
            *m.entry(c.file_path.clone()).or_insert(0) += 1;
        }
        m
    }

    const S1: &str = "/tv/Show/Season 01";

    fn show5() -> W {
        let mut w = world();
        w.series("/tv/Show", &["Season 01/E01.mkv", "Season 01/E02.mkv", "Season 01/E03.mkv",
            "Season 01/E04.mkv", "Season 01/E05.mkv"]);
        w
    }

    #[test]
    fn plex_selects_following_episodes_with_head_of_next() {
        let w = show5();
        let p = w.plan(&[("/tv/Show/Season 01/E01.mkv", "toro")]);
        let m = per_path(&p);
        assert_eq!(m.get(&format!("{S1}/E01.mkv")), None, "watched file excluded");
        for e in ["E02", "E03", "E04"] {
            assert_eq!(m[&format!("{S1}/{e}.mkv")], 75, "{e} whole");
        }
        assert_eq!(m[&format!("{S1}/E05.mkv")], 31, "124 MiB head = 31 chunks");
        assert!(p.copies.iter().all(|c| c.target_node == 2 && c.tier == DiskTier::Warm && c.source_node == 1));
    }

    #[test]
    fn plex_playback_order_crosses_seasons_naturally() {
        let mut w = world();
        w.series("/tv/Show", &["Season 01/E10.mkv", "Season 2/E01.mkv", "Season 10/E01.mkv"]);
        let p = w.plan(&[("/tv/Show/Season 01/E10.mkv", "toro")]);
        let at = |pos: u32| p.copies.iter().find(|c| c.rank.0 == pos).unwrap().file_path.clone();
        assert_eq!(at(0), "/tv/Show/Season 2/E01.mkv");
        assert_eq!(at(1), "/tv/Show/Season 10/E01.mkv");
    }

    #[test]
    fn plex_without_plexmatch_falls_back_to_next_files() {
        let mut w = world();
        w.vol().precache.next_files = 2;
        for n in ["A", "B", "C", "D"] {
            w.file(&format!("/movies/{n}.mkv"), 75, &[1]);
        }
        let p = w.plan(&[("/movies/A.mkv", "toro")]);
        let m = per_path(&p);
        assert_eq!(m.keys().cloned().collect::<Vec<_>>(), ["/movies/B.mkv", "/movies/C.mkv"]);
    }

    #[test]
    fn files_mode_ignores_plexmatch() {
        let mut w = show5();
        w.file("/tv/Show/Season 02/E01.mkv", 75, &[1]);
        w.vol().precache.mode = PrecacheMode::Files;
        w.vol().precache.next_files = 1;
        let p = w.plan(&[("/tv/Show/Season 01/E01.mkv", "toro")]);
        assert_eq!(per_path(&p).keys().cloned().collect::<Vec<_>>(), [format!("{S1}/E02.mkv")]);
        assert!(p.status.series.is_empty());
    }

    #[test]
    fn series_lru_drops_oldest_and_its_protection() {
        let mut w = world();
        for s in ["A", "B", "C"] {
            w.file(&format!("/tv/{s}/.plexmatch"), 0, &[]);
            w.file(&format!("/tv/{s}/E01.mkv"), 75, &[1]);
            w.file(&format!("/tv/{s}/E02.mkv"), 75, &[1, 2]);
        }
        w.vol().precache.max_series = 2;
        let mut snap = PrecacheSnapshot::default();
        for (i, s) in ["A", "B", "C"].iter().enumerate() {
            snap.series_lru.insert(
                ("v".into(), "toro".into(), format!("/tv/{s}")),
                SeriesEntry { cursor: format!("/tv/{s}/E01.mkv"), last_read_ms: i as u64 + 1 },
            );
        }
        let p = plan_precache(&w.sm, &snap, NOW, &cfg());
        let roots: Vec<&str> = p.series_lru.keys().map(|k| k.2.as_str()).collect();
        assert_eq!(roots, ["/tv/B", "/tv/C"]);
        let sroots: BTreeSet<_> = p.status.series.iter().filter_map(|s| s.root.clone()).collect();
        assert_eq!(sroots, ["/tv/B".to_string(), "/tv/C".to_string()].into_iter().collect());
        assert!(p.protected.contains(&("/tv/B/E02.mkv#0".to_string(), 2, 0)));
        assert!(!p.protected.contains(&("/tv/A/E02.mkv#0".to_string(), 2, 0)), "evicted series loses protection");
    }

    #[test]
    fn already_cached_chunk_is_protected_not_copied() {
        let mut w = world();
        w.file("/tv/S/.plexmatch", 0, &[]);
        w.file("/tv/S/E01.mkv", 75, &[1]);
        w.file("/tv/S/E02.mkv", 75, &[1, 2]);
        let p = w.plan(&[("/tv/S/E01.mkv", "toro")]);
        assert!(p.copies.is_empty());
        assert!(p.protected.contains(&("/tv/S/E02.mkv#0".to_string(), 2, 0)));
        assert_eq!(p.status.series[0].cached_bytes, 300 * MIB);
    }

    #[test]
    fn anchor_pending_chunk_is_neither_copied_nor_protected() {
        let mut w = world();
        w.file("/tv/S/.plexmatch", 0, &[]);
        w.file("/tv/S/E01.mkv", 75, &[1]);
        w.file("/tv/S/E02.mkv", 75, &[3]);
        let p = w.plan(&[("/tv/S/E01.mkv", "toro")]);
        assert!(p.copies.is_empty(), "no copy for an anchor-pending chunk");
        assert!(p.protected.is_empty());
    }

    #[test]
    fn cooldown_blocks_copy_until_it_expires() {
        let mut w = show5();
        let e2 = format!("{S1}/E02.mkv");
        w.sm.files.get_mut(&e2).unwrap().cache_cooldown_until_ms.insert(0, NOW + 1);
        let p = w.plan(&[("/tv/Show/Season 01/E01.mkv", "toro")]);
        assert_eq!(per_path(&p)[&e2], 74);
        assert!(!p.copies.iter().any(|c| c.file_path == e2 && c.chunk_index == 0));
        w.sm.files.get_mut(&e2).unwrap().cache_cooldown_until_ms.insert(0, NOW - 1);
        let p = w.plan(&[("/tv/Show/Season 01/E01.mkv", "toro")]);
        assert_eq!(per_path(&p)[&e2], 75);
    }

    fn pin(w: &mut W, prefix: &str, dc: &str) {
        w.vol().precache.pins.push(dtfs_common::types::PrecachePin {
            prefix: prefix.into(),
            datacenter: dc.into(),
        });
    }

    #[test]
    fn pin_copies_every_chunk_ranked_after_cursor_copies() {
        let mut w = show5();
        for n in ["a", "b"] {
            w.file(&format!("/tv/Bluey/{n}.mkv"), 75, &[1]);
        }
        pin(&mut w, "/tv/Bluey", "TORO");
        let p = w.plan(&[("/tv/Show/Season 01/E01.mkv", "toro")]);
        let m = per_path(&p);
        assert_eq!((m["/tv/Bluey/a.mkv"], m["/tv/Bluey/b.mkv"]), (75, 75));
        assert!(p.copies.iter().all(|c| c.target_node == 2));
        let first_pin = p.copies.iter().position(|c| c.file_path.starts_with("/tv/Bluey")).unwrap();
        assert!(first_pin > 0 && p.copies[..first_pin].iter().all(|c| c.file_path.starts_with("/tv/Show")));
        assert!(p.copies[first_pin..].iter().all(|c| c.file_path.starts_with("/tv/Bluey")));
        let ps = &p.status.pins[0];
        assert_eq!((ps.prefix.as_str(), ps.dc.as_str(), ps.bytes, ps.cached_bytes, ps.truncated),
            ("/tv/Bluey", "toro", 600 * MIB, 0, false));
    }

    #[test]
    fn pin_of_cached_chunk_is_protected_not_copied() {
        let mut w = world();
        w.file("/tv/Bluey/a.mkv", 75, &[1, 2]);
        pin(&mut w, "/tv/Bluey", "toro");
        let p = w.plan(&[]);
        assert!(p.copies.is_empty());
        assert!(p.protected.contains(&("/tv/Bluey/a.mkv#0".to_string(), 2, 0)));
        assert_eq!(p.status.pins[0].cached_bytes, 300 * MIB);
    }

    #[test]
    fn pin_acts_with_mode_off_and_ignores_reads() {
        let mut w = world();
        w.vol().precache.mode = PrecacheMode::Off;
        w.series("/tv/Show", &["E01.mkv", "E02.mkv"]);
        w.file("/tv/Bluey/a.mkv", 75, &[1]);
        pin(&mut w, "/tv/Bluey", "toro");
        let p = w.plan(&[("/tv/Show/E01.mkv", "toro")]);
        assert_eq!(per_path(&p).keys().cloned().collect::<Vec<_>>(), ["/tv/Bluey/a.mkv"]);
        assert!(p.status.series.is_empty() && p.status.cursors.is_empty());
        // Without the pin, mode off plans nothing.
        w.vol().precache.pins.clear();
        assert!(w.plan(&[]).copies.is_empty());
    }

    #[test]
    fn pin_prefix_matches_whole_components_and_own_volume_only() {
        let mut w = world();
        w.file("/tv/Bluey/a.mkv", 75, &[1]);
        w.file("/tv/Bluey2/a.mkv", 75, &[1]);
        w.file("/tv/Bluey/other.mkv", 75, &[1]);
        w.sm.files.get_mut("/tv/Bluey/other.mkv").unwrap().volume_id = "w".into();
        pin(&mut w, "/tv/Blue", "toro");
        assert!(w.plan(&[]).copies.is_empty(), "/tv/Blue must not match /tv/Bluey");
        w.vol().precache.pins.clear();
        pin(&mut w, "/tv/Bluey/", "toro");
        let p = w.plan(&[]);
        assert_eq!(per_path(&p).keys().cloned().collect::<Vec<_>>(), ["/tv/Bluey/a.mkv"]);
    }

    #[test]
    fn pin_respects_anchor_pending_and_already_held_node() {
        let mut w = world();
        w.file("/tv/Bluey/pending.mkv", 75, &[3]); // no anchor replica yet
        w.file("/tv/Bluey/ok.mkv", 75, &[1]);
        pin(&mut w, "/tv/Bluey", "toro");
        let p = w.plan(&[]);
        assert_eq!(per_path(&p).keys().cloned().collect::<Vec<_>>(), ["/tv/Bluey/ok.mkv"]);
        assert!(!p.protected.iter().any(|(c, _, _)| c.contains("pending")));
    }

    #[test]
    fn pin_listing_is_capped_and_flags_truncated() {
        let mut w = world();
        w.vol().precache.mode = PrecacheMode::Off;
        for i in 0..MAX_PIN_FILES {
            w.file(&format!("/big/{i:05}.mkv"), 4, &[1]);
        }
        pin(&mut w, "/big", "toro");
        let p = w.plan(&[]);
        assert!(!p.status.pins[0].truncated, "exactly the cap is not truncated");
        assert_eq!(per_path(&p).len(), MAX_PIN_FILES);
        w.file("/big/zzzzz.mkv", 4, &[1]);
        let p = w.plan(&[]);
        assert!(p.status.pins[0].truncated);
        assert_eq!(per_path(&p).len(), MAX_PIN_FILES);
    }

    /// Anchor requires cold in leli; a warm leli copy does not satisfy it (`anchor_satisfied_by`).
    #[test]
    fn anchor_required_tier_is_honoured() {
        let mut w = world();
        w.sm.nodes.insert(4, node(4, "leli", DiskTier::Warm, 1 << 40, 0));
        w.file("/tv/Bluey/warm_only.mkv", 4, &[4]);
        w.file("/tv/Bluey/warm_and_toro.mkv", 4, &[4, 2]);
        w.file("/tv/Bluey/cold.mkv", 4, &[1]);
        pin(&mut w, "/tv/Bluey", "toro");
        let p = w.plan(&[]);
        assert_eq!(per_path(&p).keys().cloned().collect::<Vec<_>>(), ["/tv/Bluey/cold.mkv"]);
        assert!(!p.protected.iter().any(|(c, _, _)| c.contains("warm")), "anchor-pending: no protection");
    }

    /// 20 pinned 4-chunk files after a cursor of ~300 chunk entries, budget 350.
    #[test]
    fn pin_planning_stops_at_the_chunk_visit_budget() {
        let mut w = show5();
        for i in 0..20 {
            w.file(&format!("/tv/Bluey/{i:02}.mkv"), 4, &[1]);
        }
        pin(&mut w, "/tv/Bluey", "toro");
        let mut snap = PrecacheSnapshot::default();
        let a = FileActivity {
            last_read_ms: NOW,
            chunk_ids: ["/tv/Show/Season 01/E01.mkv#20".to_string()].into_iter().collect(),
            overflow: false,
        };
        snap.activity.insert(("/tv/Show/Season 01/E01.mkv".into(), "toro".into()), a);
        let p = plan_precache_budget(&w.sm, &snap, NOW, &cfg(), 350);
        let ps = &p.status.pins[0];
        assert!(ps.truncated, "budget exhausted mid-pin");
        let pinned = p.copies.iter().filter(|c| c.file_path.starts_with("/tv/Bluey")).count();
        assert!(pinned > 0 && pinned < 80, "pin partly planned, got {pinned} chunks");
        assert!(p.copies.iter().any(|c| c.file_path.starts_with("/tv/Show")), "cursor copies still planned");
        assert!(p.copies[0].file_path.starts_with("/tv/Show"), "cursor copies rank first");
        // Plenty of budget: nothing truncated.
        let p = plan_precache_budget(&w.sm, &snap, NOW, &cfg(), 1_000_000);
        assert!(!p.status.pins[0].truncated);
        assert_eq!(p.copies.iter().filter(|c| c.file_path.starts_with("/tv/Bluey")).count(), 80);
    }

    /// Cursors alone use the budget up: later pins are listed as truncated and plan nothing.
    #[test]
    fn exhausted_budget_skips_later_pins() {
        let mut w = world();
        w.vol().precache.mode = PrecacheMode::Off;
        w.file("/a/x.mkv", 4, &[1]);
        w.file("/b/y.mkv", 4, &[1]);
        pin(&mut w, "/a", "toro");
        pin(&mut w, "/b", "toro");
        let p = plan_precache_budget(&w.sm, &PrecacheSnapshot::default(), NOW, &cfg(), 4);
        assert_eq!(per_path(&p).keys().cloned().collect::<Vec<_>>(), ["/a/x.mkv"]);
        assert_eq!(p.status.pins.len(), 2);
        assert!(!p.status.pins[0].truncated && p.status.pins[1].truncated);
    }

    /// A pin stops once the DC's room is gone instead of walking every remaining file.
    #[test]
    fn pin_stops_when_dc_room_is_exhausted() {
        let mut w = world();
        w.vol().precache.mode = PrecacheMode::Off;
        // room: 1000 - 100 reserve = 900 MiB -> 225 chunks; 20 files x 75 chunks = 1500.
        w.set_disk(2, 1000 * MIB, 0);
        for i in 0..20 {
            w.file(&format!("/big/{i:02}.mkv"), 75, &[1]);
        }
        pin(&mut w, "/big", "toro");
        let budget = 20 * 75;
        let p = plan_precache_budget(&w.sm, &PrecacheSnapshot::default(), NOW, &cfg(), budget);
        assert!(p.status.dcs[0].capacity_limited);
        assert_eq!(p.copies.len(), 225);
        // Files after the one that hit the limit are not walked: desired stops near the room.
        assert!(p.status.pins[0].bytes <= 225 * CHUNK + 75 * CHUNK, "walked {} bytes", p.status.pins[0].bytes);
        assert!(p.status.pins[0].truncated);
    }

    #[test]
    fn capacity_stops_planning_and_flags_dc() {
        let mut w = show5();
        // total 1000 MiB, used 300 MiB, reserve 100 MiB => room 600 MiB = 150 chunks.
        w.set_disk(2, 1000 * MIB, 300 * MIB);
        let p = w.plan(&[("/tv/Show/Season 01/E01.mkv", "toro")]);
        assert_eq!(p.copies.len(), 150);
        let toro = p.status.dcs.iter().find(|d| d.dc == "toro").unwrap();
        assert!(toro.capacity_limited);
        assert!(!toro.no_target);
    }

    #[test]
    fn capacity_reserve_boundary_takes_exactly_one_chunk() {
        let mut w = show5();
        // avail = reserve + exactly one chunk.
        w.set_disk(2, 1000 * MIB, 1000 * MIB - 100 * MIB - CHUNK);
        let p = w.plan(&[("/tv/Show/Season 01/E01.mkv", "toro")]);
        assert_eq!(p.copies.len(), 1);
        assert!(p.status.dcs.iter().find(|d| d.dc == "toro").unwrap().capacity_limited);
        // One byte less: nothing fits.
        w.set_disk(2, 1000 * MIB, 1000 * MIB - 100 * MIB - CHUNK + 1);
        assert!(w.plan(&[("/tv/Show/Season 01/E01.mkv", "toro")]).copies.is_empty());
    }

    #[test]
    fn no_warm_node_in_dc_reports_no_target() {
        let mut w = show5();
        w.sm.nodes.remove(&2);
        w.sm.nodes.remove(&3);
        let p = w.plan(&[("/tv/Show/Season 01/E01.mkv", "toro")]);
        assert!(p.copies.is_empty());
        assert!(p.status.dcs.iter().find(|d| d.dc == "toro").unwrap().no_target);
    }

    #[test]
    fn header_and_tail_probe_is_not_playing() {
        let w = show5();
        let path = "/tv/Show/Season 01/E01.mkv";
        let mut snap = PrecacheSnapshot::default();
        let ids = (0..=8).chain([74]).map(|i| format!("{path}#{i}")).collect();
        snap.activity.insert(
            (path.into(), "toro".into()),
            FileActivity { last_read_ms: NOW, chunk_ids: ids, overflow: false },
        );
        let p = plan_precache(&w.sm, &snap, NOW, &cfg());
        assert!(p.copies.is_empty());
        assert!(p.status.series.is_empty() && p.status.cursors.is_empty());
    }

    #[test]
    fn write_leased_file_is_skipped() {
        let mut w = show5();
        w.vol().precache.mode = PrecacheMode::Files;
        w.vol().precache.next_files = 1;
        w.sm.write_leases.insert(
            format!("{S1}/E02.mkv"),
            WriteLease { owner_id: "o".into(), lease_id: "l".into(), expires_at_ms: NOW + 60_000 },
        );
        let p = w.plan(&[("/tv/Show/Season 01/E01.mkv", "toro")]);
        assert_eq!(per_path(&p).keys().cloned().collect::<Vec<_>>(), [format!("{S1}/E03.mkv")]);
    }

    #[test]
    fn cursors_in_two_dcs_target_their_own_dc() {
        let w = show5();
        let f = "/tv/Show/Season 01/E01.mkv";
        let p = w.plan(&[(f, "toro"), (f, "nrb")]);
        let targets: BTreeSet<u64> = p
            .copies
            .iter()
            .filter(|c| c.chunk_id == format!("{S1}/E02.mkv#0"))
            .map(|c| c.target_node)
            .collect();
        assert_eq!(targets, [2, 3].into_iter().collect());
    }

    #[test]
    fn per_dc_false_skips_volume_with_policy_invalid() {
        let mut w = show5();
        w.vol().cache_replicas.per_dc = false;
        let p = w.plan(&[("/tv/Show/Season 01/E01.mkv", "toro")]);
        assert!(p.copies.is_empty());
        assert_eq!(p.status.skipped_volumes.len(), 1);
        assert_eq!(p.status.skipped_volumes[0].reason, "policy_invalid");
        assert!(p.status.skipped_volumes[0].detail.contains("per_dc"));
    }

    #[test]
    fn two_cursors_in_one_dc_interleave_by_file_position() {
        let mut w = world();
        w.series("/tv/A", &["E01.mkv", "E02.mkv", "E03.mkv"]);
        w.series("/tv/B", &["E01.mkv", "E02.mkv", "E03.mkv"]);
        let p = w.plan(&[("/tv/A/E01.mkv", "toro"), ("/tv/B/E01.mkv", "toro")]);
        let last0 = p.copies.iter().rposition(|c| c.rank.0 == 0).unwrap();
        let first1 = p.copies.iter().position(|c| c.rank.0 == 1).unwrap();
        assert!(last0 < first1);
        let firsts: BTreeSet<&str> =
            p.copies.iter().filter(|c| c.rank.0 == 0).map(|c| &c.file_path[..5]).collect();
        assert_eq!(firsts.len(), 2, "both series get their next file before either gets its second");
    }

    #[test]
    fn warm_mandatory_never_planned_and_invalid_policy_reported() {
        let mut w = show5();
        w.vol().mandatory_replicas.warm = 1;
        let f = [("/tv/Show/Season 01/E01.mkv", "toro")];
        let p = w.plan(&f);
        assert!(p.copies.is_empty());
        assert!(p.status.skipped_volumes[0].detail.contains("cache tier"));
        // A usable hot cache keeps the policy valid; the mandatory warm tier must still never be chosen.
        w.vol().cache_replicas.hot = CacheTierConfig { min: 1, max: 1 };
        let p = w.plan(&f);
        assert!(p.status.skipped_volumes.is_empty());
        assert!(p.copies.is_empty());
        assert!(p.status.dcs.iter().find(|d| d.dc == "toro").unwrap().no_target);
    }

    #[test]
    fn new_episode_after_cursor_is_selected() {
        let mut w = show5();
        w.file(&format!("{S1}/E06.mkv"), 75, &[1]);
        let p = w.plan(&[("/tv/Show/Season 01/E05.mkv", "toro")]);
        assert_eq!(per_path(&p).keys().cloned().collect::<Vec<_>>(), [format!("{S1}/E06.mkv")]);
    }

    #[test]
    fn planner_is_deterministic() {
        let w = show5();
        let f = [("/tv/Show/Season 01/E01.mkv", "toro"), ("/tv/Show/Season 01/E01.mkv", "nrb")];
        let (a, b) = (w.plan(&f), w.plan(&f));
        assert!(!a.copies.is_empty());
        assert_eq!(a.copies, b.copies);
    }

    fn qinfo() -> dtfs_common::types::QuarantineInfo {
        dtfs_common::types::QuarantineInfo {
            since_ms: 1,
            reason: dtfs_common::types::QuarantineReason::OperatorRequested,
            triggering_chunk_id: None,
            fault_count_total: 1,
            last_fault_at_ms: 1,
            last_fault_summary: String::new(),
        }
    }

    #[test]
    fn next_files_ignore_other_volumes() {
        let mut w = world();
        w.vol().precache.mode = PrecacheMode::Files;
        w.vol().precache.next_files = 1;
        for n in ["A", "B", "C"] {
            w.file(&format!("/m/{n}.mkv"), 75, &[1]);
        }
        let mut other = w.sm.volumes["v"].clone();
        other.mandatory_replicas.warm = 1;
        w.sm.volumes.insert("w".into(), other);
        w.sm.files.get_mut("/m/B.mkv").unwrap().volume_id = "w".into();
        let p = w.plan(&[("/m/A.mkv", "toro")]);
        assert_eq!(per_path(&p).keys().cloned().collect::<Vec<_>>(), ["/m/C.mkv"]);
    }

    #[test]
    fn maintenance_and_quarantined_nodes_are_never_targets() {
        let mut w = show5();
        w.sm.nodes.get_mut(&2).unwrap().health = HealthStatus::Maintenance;
        w.sm.nodes.insert(4, node(4, "toro", DiskTier::Warm, 1 << 40, 0));
        w.sm.nodes.get_mut(&4).unwrap().quarantine = Some(qinfo());
        let p = w.plan(&[("/tv/Show/Season 01/E01.mkv", "toro")]);
        assert!(p.copies.is_empty());
        assert!(p.status.dcs.iter().find(|d| d.dc == "toro").unwrap().no_target);
        // A healthy peer is still used.
        w.sm.nodes.insert(5, node(5, "toro", DiskTier::Warm, 1 << 40, 0));
        let p = w.plan(&[("/tv/Show/Season 01/E01.mkv", "toro")]);
        assert!(!p.copies.is_empty() && p.copies.iter().all(|c| c.target_node == 5));
    }

    #[test]
    fn files_mode_cap_counts_direct_children_only() {
        let mut w = world();
        w.vol().precache.mode = PrecacheMode::Files;
        w.vol().precache.next_files = 1;
        for i in 0..(MAX_DIR_ENTRIES + 5) {
            w.file(&format!("/m/A/f{i:06}.mkv"), 0, &[]);
        }
        w.file("/m/Z1.mkv", 75, &[1]);
        w.file("/m/Z2.mkv", 75, &[1]);
        let p = w.plan(&[("/m/Z1.mkv", "toro")]);
        assert_eq!(per_path(&p).keys().cloned().collect::<Vec<_>>(), ["/m/Z2.mkv"]);
    }

    #[test]
    fn room_needs_one_disk_and_skips_draining() {
        let mut w = show5();
        let mut d = w.sm.nodes[&2].disks[0].clone();
        d.total_bytes = 3 * MIB;
        d.used_bytes = 0;
        let mut d2 = d.clone();
        d2.disk_id = 1;
        w.sm.nodes.get_mut(&2).unwrap().disks = vec![d, d2];
        let f = [("/tv/Show/Season 01/E01.mkv", "toro")];
        let p = w.plan(&f);
        assert!(p.copies.is_empty(), "two 3 MiB disks cannot hold a 4 MiB chunk");
        assert!(p.status.dcs.iter().find(|d| d.dc == "toro").unwrap().capacity_limited);
        // Only a draining disk: no room at all.
        let mut w = show5();
        w.sm.nodes.get_mut(&2).unwrap().disks[0].status = DiskStatus::Draining;
        assert!(w.plan(&f).copies.is_empty());
    }

    #[test]
    fn anchor_pending_chunk_in_cursor_dc_is_not_protected() {
        let mut w = world();
        w.file("/tv/S/.plexmatch", 0, &[]);
        w.file("/tv/S/E01.mkv", 75, &[1]);
        w.file("/tv/S/E02.mkv", 75, &[2]);
        let p = w.plan(&[("/tv/S/E01.mkv", "toro")]);
        assert!(p.copies.is_empty());
        assert!(p.protected.is_empty());
    }

    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self, n: u64) -> u64 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (self.0 >> 33) % n
        }
    }

    #[test]
    fn property_random_trees_keep_invariants() {
        let mut r = Lcg(0xD7F5);
        for _ in 0..200 {
            let mut w = world();
            let total = 1000 * MIB;
            let mut room = BTreeMap::new();
            for id in [2u64, 3] {
                let used = r.next(250) * CHUNK;
                w.set_disk(id, total, used);
                room.insert(id, (total - used).saturating_sub(total / 10));
                match r.next(5) {
                    0 => w.sm.nodes.get_mut(&id).unwrap().health = HealthStatus::Maintenance,
                    1 => w.sm.nodes.get_mut(&id).unwrap().quarantine = Some(qinfo()),
                    _ => {}
                }
            }
            let other = w.sm.volumes["v"].clone();
            w.sm.volumes.insert("w".into(), other);
            let plex = r.next(2) == 0;
            w.vol().precache.mode = if plex { PrecacheMode::Plex } else { PrecacheMode::Files };
            w.vol().precache.next_files = 1 + r.next(3) as u32;
            w.vol().precache.series_max_gb = 1 + r.next(2) as u32;
            w.vol().precache.max_series = 1 + r.next(3) as u32;
            let mut reads: Vec<(String, String)> = Vec::new();
            for s in 0..=r.next(3) {
                let root = format!("/tv/S{s}");
                if r.next(4) != 0 {
                    w.file(&format!("{root}/.plexmatch"), 0, &[]);
                }
                for e in 0..(3 + r.next(4)) {
                    let holders: Vec<u64> = [1u64, 2, 3].into_iter().filter(|_| r.next(3) == 0).collect();
                    let holders = if holders.is_empty() { vec![1] } else { holders };
                    let path = format!("{root}/E{e:02}.mkv");
                    w.file(&path, 4 + r.next(40), &holders);
                    if r.next(4) == 0 {
                        w.sm.files.get_mut(&path).unwrap().volume_id = "w".into();
                        continue;
                    }
                    if r.next(5) == 0 {
                        w.sm.files.get_mut(&path).unwrap().cache_cooldown_until_ms.insert(0, NOW + 1);
                    }
                    if r.next(3) == 0 {
                        let dc = ["toro", "nrb", "leli"][r.next(3) as usize];
                        reads.push((path, dc.to_string()));
                    }
                }
            }
            let mut snap = PrecacheSnapshot::default();
            for (path, dc) in &reads {
                snap.activity.insert(
                    (path.clone(), dc.clone()),
                    FileActivity { last_read_ms: NOW, chunk_ids: HashSet::new(), overflow: true },
                );
            }
            let p = plan_precache(&w.sm, &snap, NOW, &cfg());
            let dcs: BTreeSet<&str> = reads.iter().map(|(_, d)| d.as_str()).collect();
            let mut planned: BTreeMap<u64, u64> = BTreeMap::new();
            let mut seen = HashSet::new();
            for c in &p.copies {
                let dc = &w.sm.nodes[&c.target_node].location.datacenter;
                assert!(dcs.contains(dc.as_str()), "target DC has a cursor");
                let tn = &w.sm.nodes[&c.target_node];
                assert!(tn.quarantine.is_none() && tn.health == HealthStatus::Healthy, "excluded target");
                let root = c.file_path.rsplit_once('/').unwrap().0;
                assert!(
                    reads.iter().any(|(p, d)| p.starts_with(root) && d.eq_ignore_ascii_case(dc)),
                    "target DC matches the cursor that produced the copy"
                );
                let fm = &w.sm.files[&c.file_path];
                assert_eq!(fm.volume_id, "v", "copy from another volume");
                let rows: Vec<_> = fm.chunks.iter().filter(|e| e.chunk_index == c.chunk_index).collect();
                let holders: Vec<u64> = rows.iter().map(|e| e.node_id.parse().unwrap()).collect();
                assert!(!holders.contains(&c.target_node), "target already holds the chunk");
                assert!(holders.contains(&c.source_node));
                assert!(holders.contains(&1), "anchor copy is live");
                assert_eq!(c.tier, DiskTier::Warm);
                assert_eq!(w.sm.volumes["v"].effective_mandatory_replicas().for_tier(c.tier), 0);
                assert!(seen.insert((c.chunk_id.clone(), c.target_node)), "no duplicate copy");
                *planned.entry(c.target_node).or_default() += c.bytes;
            }
            for (n, b) in planned {
                assert!(b <= room[&n], "node {n} planned {b} > room {}", room[&n]);
            }
        }
    }
}

#[cfg(test)]
mod engine_tests {
    use super::*;
    use crate::tiering::TickDemotion;

    fn copy(chunk: &str, target: u64, bytes: u64) -> PrecacheCopy {
        PrecacheCopy {
            chunk_id: chunk.into(),
            file_path: "/f".into(),
            chunk_index: 0,
            source_node: 1,
            target_node: target,
            tier: DiskTier::Warm,
            bytes,
            rank: (0, 0, 0),
        }
    }

    fn demotion(chunk: &str, node: u64) -> TickDemotion {
        demotion_on(chunk, node, 0)
    }

    fn demotion_on(chunk: &str, node: u64, disk: u32) -> TickDemotion {
        TickDemotion {
            chunk_id: chunk.into(),
            chunk_index: 0,
            file_path: "/f".into(),
            node_id: node,
            disk_id: disk,
            tier: DiskTier::Warm,
            reason: "idle",
            cooldown_until_ms_at_snapshot: 0,
            enforce_anchor: None,
        }
    }

    #[test]
    fn refill_tokens_rate_cap_and_clock_skew() {
        assert_eq!(refill_tokens(0, 0, 30_000, 10, 120, 0), 37_500_000);
        assert_eq!(refill_tokens(0, 0, 10_000_000_000, 10, 120, 0), 150_000_000);
        assert_eq!(refill_tokens(5, 10_000, 9_000, 10, 120, 0), 5, "now < last leaves tokens unchanged");
    }

    /// 4 Mbps x 120 s = 60 MB < one 64 MiB chunk: the bucket must still be able to hold it.
    #[test]
    fn bucket_cap_never_below_one_chunk() {
        let chunk = 64 * 1024 * 1024;
        let full = refill_tokens(0, 0, 10_000_000_000, 4, 120, chunk);
        assert_eq!(full, chunk);
        let copies = vec![copy("big", 2, chunk)];
        assert_eq!(pick_within_budget(&copies, &HashSet::new(), full).len(), 1);
        // A bigger rate cap still wins over the chunk floor.
        assert_eq!(refill_tokens(0, 0, 10_000_000_000, 10, 120, chunk), 150_000_000);
    }

    #[test]
    fn own_inflight_counts_only_precache_pairs() {
        let live = HashSet::from([("a".to_string(), 2u64), ("repair".to_string(), 3)]);
        let mine = HashSet::from([("a".to_string(), 2u64), ("done".to_string(), 2)]);
        assert_eq!(own_inflight(&mine, &live), 1);
        assert_eq!(own_inflight(&HashSet::new(), &live), 0);
    }

    /// Failed pairs are in the skip set: not charged to the budget, later copies still picked.
    #[test]
    fn pick_skips_blocking_pairs_without_charge() {
        let mb = 1_000_000;
        let copies = vec![copy("failed", 2, 50 * mb), copy("ok", 2, 10 * mb)];
        let blocking = HashSet::from([("failed".to_string(), 2u64)]);
        let ids: Vec<_> = pick_within_budget(&copies, &blocking, 20 * mb).iter().map(|c| c.chunk_id.clone()).collect();
        assert_eq!(ids, vec!["ok"]);
    }

    #[test]
    fn precache_max_new_is_room_under_the_ceiling() {
        assert_eq!(precache_max_new(0), PRECACHE_YIELD_LIVE_TASKS);
        assert_eq!(precache_max_new(PRECACHE_YIELD_LIVE_TASKS - 1), 1);
        assert_eq!(precache_max_new(PRECACHE_YIELD_LIVE_TASKS), 0);
        assert_eq!(precache_max_new(PRECACHE_YIELD_LIVE_TASKS + 9), 0);
    }

    #[test]
    fn pick_within_budget_skips_active_and_stops_at_first_overrun() {
        let mb = 1_000_000;
        let copies = vec![copy("a", 2, 10 * mb), copy("b", 2, 50 * mb), copy("c", 2, mb)];
        let none = HashSet::new();
        let ids: Vec<_> = pick_within_budget(&copies, &none, 20 * mb).iter().map(|c| c.chunk_id.clone()).collect();
        assert_eq!(ids, vec!["a"], "c fits but must not jump ahead of b");
        assert!(pick_within_budget(&copies, &none, 0).is_empty());
        let active = HashSet::from([("a".to_string(), 2u64)]);
        let ids: Vec<_> = pick_within_budget(&copies, &active, 100 * mb).iter().map(|c| c.chunk_id.clone()).collect();
        assert_eq!(ids, vec!["b", "c"]);
    }

    #[test]
    fn yield_boundary() {
        assert!(should_yield(PRECACHE_YIELD_LIVE_TASKS));
        assert!(!should_yield(PRECACHE_YIELD_LIVE_TASKS - 1));
        assert_eq!(PRECACHE_YIELD_LIVE_TASKS, 24);
    }

    #[test]
    fn retain_unprotected_removes_exactly_protected_pairs() {
        let mut d = vec![demotion("a", 2), demotion("a", 3), demotion("b", 2)];
        let protected = HashSet::from([("a".to_string(), 2u64, 0u32)]);
        let none = HashSet::new();
        assert_eq!(retain_unprotected(&mut d, &protected, &none), 1);
        assert_eq!(d.len(), 2);
        assert!(d.iter().all(|x| (x.chunk_id.as_str(), x.node_id) != ("a", 2)));
        assert_eq!(retain_unprotected(&mut d, &HashSet::new(), &none), 0);
    }

    /// Protecting one cache disk's copy leaves a second copy on another disk of the node demotable.
    #[test]
    fn retain_unprotected_is_keyed_by_disk() {
        let mut d = vec![demotion_on("a", 2, 0), demotion_on("a", 2, 1)];
        let protected = HashSet::from([("a".to_string(), 2u64, 0u32)]);
        assert_eq!(retain_unprotected(&mut d, &protected, &HashSet::new()), 1);
        assert_eq!((d[0].node_id, d[0].disk_id), (2, 1));
    }

    /// A pair with a live copy task protects every disk of the target (it holds no copy yet).
    #[test]
    fn retain_unprotected_keeps_scheduled_target_on_any_disk() {
        let mut d = vec![demotion_on("a", 2, 0), demotion_on("a", 2, 7), demotion_on("b", 2, 0)];
        let scheduled = HashSet::from([("a".to_string(), 2u64)]);
        assert_eq!(retain_unprotected(&mut d, &HashSet::new(), &scheduled), 2);
        assert_eq!(d.len(), 1);
    }

    #[test]
    fn reset_leader_state_keeps_activity() {
        let mut st = PrecacheState::default();
        let reads = HashMap::from([("toro".to_string(), 1u32)]);
        note_read(&mut st.activity, "/tv/a.mkv", "c1", &reads, 5);
        st.protected.insert(("c1".into(), 2, 0));
        st.scheduled.insert(("c1".into(), 2));
        st.series_lru.insert(
            ("v".into(), "toro".into(), "/tv".into()),
            SeriesEntry { cursor: "/tv/a.mkv".into(), last_read_ms: 5 },
        );
        st.status.is_leader = true;
        st.reset_leader_state();
        assert!(st.protected.is_empty() && st.scheduled.is_empty() && st.series_lru.is_empty());
        assert!(!st.status.is_leader);
        assert_eq!(st.activity.len(), 1);
    }

    #[test]
    fn newly_invalid_logs_once_per_transition() {
        let sk = |v: &str| SkippedVolume { volume: v.into(), reason: "policy_invalid", detail: String::new() };
        let mut logged = HashSet::new();
        let first = vec![sk("m")];
        assert_eq!(newly_invalid(&mut logged, &first).len(), 1);
        assert!(newly_invalid(&mut logged, &first).is_empty(), "same volume is not re-logged");
        assert!(newly_invalid(&mut logged, &[]).is_empty());
        assert_eq!(newly_invalid(&mut logged, &first).len(), 1, "valid then invalid again logs again");
    }
}
