// DTFS fuse3 adapter — Phase 2 Linux FUSE mount.
//
// Bridges the DtfsFilesystem abstraction layer (async, path-based) to the
// fuse3 raw Filesystem trait (inode-based).
//
// Inode mapping:
//   - FUSE root inode (1) ↔ "/"
//   - Other inodes are minted monotonically on first lookup and cached in
//     bidirectional DashMaps.  The mapping is in-memory only and does NOT
//     survive a remount, which is fine because POSIX only requires stability
//     within a single mount session.
//
// Compiled for Linux only — fuse3 requires libfuse3 and the Linux kernel FUSE
// module.
#![cfg(target_os = "linux")]

use std::ffi::OsStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use fuse3::raw::prelude::*;
use fuse3::{Errno, Result as FuseResult, Timestamp};
use futures_util::StreamExt;
use tracing::{debug, warn};

use crate::filesystem::{DtfsFilesystem, FileAttr as DtfsAttr, OpenFlags};

// Kernel TTLs for cached attributes and directory entries.
//
// F-023: these were 1 s / 5 s, which meant a `for i in …; echo > file-$i; done`
// followed by an immediate `ls` could miss every file just written because the
// kernel held onto the pre-write directory cache for 5 seconds. The shell
// glob in `rm /mnt/dtfs/small-*.txt` then expanded to nothing and the unlink
// RPCs never fired — which is what made F-025 look like a separate bug.
//
// Sub-second TTLs cost an extra round-trip on rapid stat-loops but the cluster
// API handles single-digit-ms list calls comfortably and the trade-off is
// worth it for write/read-back correctness.
const ATTR_TTL: Duration = Duration::from_millis(200);
const ENTRY_TTL: Duration = Duration::from_millis(200);

/// Root inode in FUSE (always 1).
const FUSE_ROOT_ID: u64 = 1;

// ---------------------------------------------------------------------------
// Bidirectional inode ↔ path mapping
// ---------------------------------------------------------------------------

struct InodeMap {
    inode_to_path: DashMap<u64, String>,
    path_to_inode: DashMap<String, u64>,
    next: AtomicU64,
    /// Kernel lookup count per inode (+ time of the last bump). Entries are
    /// dropped when `forget` brings the count to 0.
    refs: DashMap<u64, (u64, Instant)>,
}

/// fuse3 0.7 hands `batch_forget` bare inode numbers (the per-inode
/// `nlookup` is discarded), so a batched forget only drops an inode that
/// has not been looked up for this long: a lookup racing the forget keeps
/// its mapping and is forgotten by its own later FORGET.
const BATCH_FORGET_QUIET: Duration = Duration::from_secs(5);

impl InodeMap {
    fn new() -> Self {
        let m = Self {
            inode_to_path: DashMap::new(),
            path_to_inode: DashMap::new(),
            next: AtomicU64::new(2),
            refs: DashMap::new(),
        };
        m.inode_to_path.insert(FUSE_ROOT_ID, "/".to_string());
        m.path_to_inode.insert("/".to_string(), FUSE_ROOT_ID);
        m
    }

    /// Get or mint an inode for `path`.
    fn inode_for(&self, path: &str) -> u64 {
        // FUSE-4 — mint under the entry lock. The old get-then-insert let
        // two concurrent lookups (Plex scanning directories in parallel)
        // both miss and both mint, handing the kernel two nodeids for one
        // name: two page caches, stale reads through the loser.
        match self.path_to_inode.entry(path.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(e) => *e.get(),
            dashmap::mapref::entry::Entry::Vacant(v) => {
                let ino = self.next.fetch_add(1, Ordering::Relaxed);
                self.inode_to_path.insert(ino, path.to_string());
                v.insert(ino);
                ino
            }
        }
    }

    /// Count one kernel lookup reference on `ino` (every reply that hands
    /// the kernel an entry: lookup, create, mkdir, symlink, link, readdirplus).
    fn add_ref(&self, ino: u64) {
        if ino == FUSE_ROOT_ID {
            return;
        }
        let mut e = self.refs.entry(ino).or_insert((0, Instant::now()));
        e.0 += 1;
        e.1 = Instant::now();
    }

    /// `inode_for` + `add_ref`, re-minting if a concurrent forget dropped
    /// the mapping between the two.
    fn lookup_ref(&self, path: &str) -> u64 {
        loop {
            let ino = self.inode_for(path);
            self.add_ref(ino);
            if self.inode_to_path.contains_key(&ino) {
                return ino;
            }
        }
    }

    /// Drop both directions of `ino`'s mapping; returns the path it had.
    fn drop_mapping(&self, ino: u64) -> Option<String> {
        let (_, path) = self.inode_to_path.remove(&ino)?;
        // Only if the path still points at this inode (unlink + recreate
        // may have re-minted the path for a different inode).
        self.path_to_inode.remove_if(&path, |_, v| *v == ino);
        Some(path)
    }

    /// FUSE_FORGET: subtract `n` lookups; at 0 drop the mapping. Returns the
    /// dropped path so the caller can clear path-keyed state.
    fn forget(&self, ino: u64, n: u64) -> Option<String> {
        if ino == FUSE_ROOT_ID {
            return None;
        }
        match self.refs.entry(ino) {
            dashmap::mapref::entry::Entry::Occupied(mut e) => {
                let left = e.get().0.saturating_sub(n);
                if left > 0 {
                    e.get_mut().0 = left;
                    return None;
                }
                let dropped = self.drop_mapping(ino);
                e.remove();
                dropped
            }
            dashmap::mapref::entry::Entry::Vacant(_) => None,
        }
    }

    /// FUSE_BATCH_FORGET (count unknown, see `BATCH_FORGET_QUIET`).
    fn forget_quiet(&self, ino: u64, quiet: Duration) -> Option<String> {
        if ino == FUSE_ROOT_ID {
            return None;
        }
        match self.refs.entry(ino) {
            dashmap::mapref::entry::Entry::Occupied(e) if e.get().1.elapsed() >= quiet => {
                let dropped = self.drop_mapping(ino);
                e.remove();
                dropped
            }
            _ => None,
        }
    }

    /// Resolve an inode to its path, or return ENOENT.
    fn path_for(&self, ino: u64) -> FuseResult<String> {
        if ino == FUSE_ROOT_ID {
            return Ok("/".to_string());
        }
        self.inode_to_path
            .get(&ino)
            .map(|p| p.clone())
            .ok_or_else(|| Errno::from(libc::ENOENT))
    }

    /// Remove path/inode entries (called on unlink/rmdir).
    fn remove(&self, path: &str) {
        if let Some((_, ino)) = self.path_to_inode.remove(path) {
            self.inode_to_path.remove(&ino);
        }
    }

    /// Migrate the inode of `from` to point at `to`. Used by rename so the
    /// open FDs keep working (F-034).
    fn rename(&self, from: &str, to: &str) {
        // Descendants of a renamed directory move with it; collect first so
        // no DashMap guard is held while mutating.
        let prefix = format!("{}/", from.trim_end_matches('/'));
        let children: Vec<(String, u64)> = self
            .path_to_inode
            .iter()
            .filter(|e| e.key().starts_with(&prefix))
            .map(|e| (e.key().clone(), *e.value()))
            .collect();
        // A replaced destination loses its inode, else the old inode would
        // keep resolving to the path now owned by the moved one.
        if let Some((_, old)) = self.path_to_inode.remove(to) {
            self.inode_to_path.remove(&old);
        }
        if let Some((_, ino)) = self.path_to_inode.remove(from) {
            self.inode_to_path.insert(ino, to.to_string());
            self.path_to_inode.insert(to.to_string(), ino);
        }
        for (old_path, ino) in children {
            let new_path = format!("{}/{}", to.trim_end_matches('/'), &old_path[prefix.len()..]);
            self.path_to_inode.remove(&old_path);
            if let Some((_, stale)) = self.path_to_inode.remove(&new_path) {
                self.inode_to_path.remove(&stale);
            }
            self.inode_to_path.insert(ino, new_path.clone());
            self.path_to_inode.insert(new_path, ino);
        }
    }

    /// Return the inode of the parent path (best-effort; falls back to root).
    fn parent_inode(&self, path: &str) -> u64 {
        let parent = parent_of(path);
        *self
            .path_to_inode
            .get(&parent)
            .unwrap_or_else(|| self.path_to_inode.get("/").unwrap())
    }
}

// ---------------------------------------------------------------------------
// DtfsFuse3Adapter — wraps DtfsFilesystem for fuse3
// ---------------------------------------------------------------------------

pub struct DtfsFuse3Adapter {
    fs: Arc<DtfsFilesystem>,
    inodes: InodeMap,
    /// F-020: per-path mode overrides set via `setattr` (chmod). The manager
    /// does not track per-file UNIX permissions, so we hold the most recent
    /// `chmod` value process-locally and surface it from `getattr`. This
    /// keeps the usual `chmod 600 f && stat -c %a f → 600` workflow honest
    /// without changing cluster-wide metadata. The map is lost on unmount.
    mode_overrides: DashMap<String, u16>,
    /// Directory listings snapshotted at opendir, dropped at releasedir.
    dirs: DirSnapshots,
    /// F-031: per-inode POSIX advisory + flock lock table, with one
    /// `tokio::sync::Notify` per inode so blocking-acquire (`F_SETLKW`,
    /// `flock(LOCK_EX|0)`) waits until the holder releases instead of
    /// returning EAGAIN. The lock table is wrapped in a `tokio::Mutex` so
    /// the critical section can `await` the notify without holding the
    /// lock across the wait.
    locks: Arc<tokio::sync::Mutex<std::collections::HashMap<u64, InodeLocks>>>,
    /// F-031: per-inode `Notify` used to wake blocked-waiters when any lock
    /// for that inode is released. `DashMap` so we don't need to lock the
    /// lock table merely to find the Notify.
    lock_notify: Arc<DashMap<u64, Arc<tokio::sync::Notify>>>,
}

/// Directory listings frozen at `opendir` (fh -> entries). Cookies are
/// positions in the frozen list, so pages stay consistent while the live
/// directory changes.
struct DirSnapshots {
    next: AtomicU64,
    map: DashMap<u64, Arc<Vec<crate::filesystem::DirEntry>>>,
}

impl DirSnapshots {
    fn new() -> Self {
        // fh 0 means "no snapshot" (fallback to a live listing).
        Self { next: AtomicU64::new(1), map: DashMap::new() }
    }

    fn open(&self, entries: Vec<crate::filesystem::DirEntry>) -> u64 {
        let fh = self.next.fetch_add(1, Ordering::Relaxed);
        self.map.insert(fh, Arc::new(dedup_entries(entries)));
        fh
    }

    fn get(&self, fh: u64) -> Option<Arc<Vec<crate::filesystem::DirEntry>>> {
        self.map.get(&fh).map(|e| Arc::clone(e.value()))
    }

    fn close(&self, fh: u64) {
        self.map.remove(&fh);
    }
}

/// Keep the first entry per name (defensive; see F-027).
fn dedup_entries(entries: Vec<crate::filesystem::DirEntry>) -> Vec<crate::filesystem::DirEntry> {
    let mut seen = std::collections::HashSet::new();
    entries.into_iter().filter(|e| seen.insert(e.name.clone())).collect()
}

/// Entries of a snapshot after cookie `offset`, with their cookies.
fn snapshot_page(
    entries: &[crate::filesystem::DirEntry],
    offset: u64,
) -> impl Iterator<Item = (u64, &crate::filesystem::DirEntry)> {
    entries
        .iter()
        .enumerate()
        .map(|(idx, e)| (dir_entry_cookie(idx), e))
        .filter(move |(cookie, _)| *cookie > offset)
}

/// fuse3 `FileAttr` for a manager attribute with the given permission bits.
/// Single source for getattr/lookup and readdirplus so both agree on nlink.
fn build_attr(ino: u64, a: &DtfsAttr, perm: u16) -> FileAttr {
    FileAttr {
        ino,
        size: a.size,
        blocks: (a.size + 511) / 512,
        atime: to_timestamp(a.modified_at),
        mtime: to_timestamp(a.modified_at),
        ctime: to_timestamp(a.created_at),
        kind: if a.is_directory {
            FileType::Directory
        } else {
            FileType::RegularFile
        },
        perm,
        nlink: if a.is_directory { 2 } else { 1 },
        uid: unsafe { libc::getuid() },
        gid: unsafe { libc::getgid() },
        rdev: 0,
        blksize: 4096,
    }
}

/// All lock state for one inode (F-031).
#[derive(Debug, Default)]
struct InodeLocks {
    /// POSIX byte-range locks granted via `fcntl(F_SETLK/F_SETLKW)`.
    posix: Vec<HeldLock>,
    /// BSD-style whole-file lock held via `flock(2)`. There is at most one
    /// exclusive flock; multiple shared flocks can coexist. The bool is
    /// `true` for exclusive (LOCK_EX), `false` for shared (LOCK_SH).
    flock_owners: Vec<(u64, bool)>,
}

/// One granted byte-range lock entry in the per-inode lock table (F-031).
#[derive(Debug, Clone)]
struct HeldLock {
    owner: u64,
    start: u64,
    end: u64,
    write: bool,
}

impl HeldLock {
    /// Kernel lock ranges are inclusive on both ends (`end` is the last
    /// locked byte; whole file is `end == OFFSET_MAX`).
    fn overlaps(&self, start: u64, end: u64) -> bool {
        start <= self.end && self.start <= end
    }
}

/// Remove `[start, end]` (inclusive) from every lock `owner` holds,
/// keeping the parts outside it as separate locks of the same type.
fn carve_owner_range(posix: &mut Vec<HeldLock>, owner: u64, start: u64, end: u64) {
    let mut kept = Vec::with_capacity(posix.len() + 1);
    for h in posix.drain(..) {
        if h.owner != owner || !h.overlaps(start, end) {
            kept.push(h);
            continue;
        }
        if h.start < start {
            kept.push(HeldLock { end: start - 1, ..h.clone() });
        }
        if h.end > end {
            kept.push(HeldLock { start: end + 1, ..h });
        }
    }
    *posix = kept;
}

impl DtfsFuse3Adapter {
    pub fn new(fs: DtfsFilesystem) -> Self {
        Self::from_shared(Arc::new(fs))
    }

    /// CR-6-5 — accept an already-`Arc`-wrapped filesystem so callers can
    /// keep a clone (e.g. to spawn the metadata-cache sweep task) before
    /// handing ownership to the adapter.
    pub fn from_shared(fs: Arc<DtfsFilesystem>) -> Self {
        Self {
            fs,
            inodes: InodeMap::new(),
            mode_overrides: DashMap::new(),
            dirs: DirSnapshots::new(),
            locks: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            lock_notify: Arc::new(DashMap::new()),
        }
    }

    /// FUSE-3 — drop every POSIX lock `owner` holds on `inode` and wake
    /// blocked `F_SETLKW` waiters. No-op when the owner holds none.
    async fn release_posix_locks_for_owner(&self, inode: u64, owner: u64) {
        let released = {
            let mut guard = self.locks.lock().await;
            match guard.get_mut(&inode) {
                Some(t) => {
                    let before = t.posix.len();
                    t.posix.retain(|h| h.owner != owner);
                    before != t.posix.len()
                }
                None => false,
            }
        };
        if released {
            self.notify_for(inode).notify_waiters();
        }
    }

    /// The snapshot opendir took for `fh`, or a fresh listing when the
    /// kernel passes an fh we never issued.
    async fn listing(
        &self,
        fh: u64,
        path: &str,
    ) -> FuseResult<Arc<Vec<crate::filesystem::DirEntry>>> {
        if let Some(s) = self.dirs.get(fh) {
            return Ok(s);
        }
        let entries = self
            .fs
            .readdir(path)
            .await
            .map_err(|e| cluster_err_to_errno(&e))?;
        Ok(Arc::new(dedup_entries(entries)))
    }

    fn notify_for(&self, inode: u64) -> Arc<tokio::sync::Notify> {
        if let Some(n) = self.lock_notify.get(&inode) {
            return n.clone();
        }
        let n = Arc::new(tokio::sync::Notify::new());
        self.lock_notify.insert(inode, n.clone());
        n
    }

    fn override_perm(&self, path: &str, is_dir: bool) -> u16 {
        self.mode_overrides
            .get(path)
            .map(|m| *m)
            .unwrap_or(if is_dir { 0o755 } else { 0o644 })
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// Build an absolute DTFS path from a parent inode + child OsStr name.
    fn child_path(&self, parent_ino: u64, name: &OsStr) -> FuseResult<String> {
        let parent = self.inodes.path_for(parent_ino)?;
        let name_str = name.to_str().ok_or_else(|| Errno::from(libc::EINVAL))?;
        if parent == "/" {
            Ok(format!("/{}", name_str))
        } else {
            Ok(format!("{}/{}", parent, name_str))
        }
    }

    /// Convert a DtfsAttr + inode to a fuse3 `FileAttr`.
    fn to_fuse_attr(&self, ino: u64, a: &DtfsAttr) -> FileAttr {
        // Use the per-path chmod override if one has been recorded; otherwise
        // fall back to the role default (F-020).
        let path = self
            .inodes
            .path_for(ino)
            .unwrap_or_else(|_| "/".to_string());
        build_attr(ino, a, self.override_perm(&path, a.is_directory))
    }

    /// Synthesise a fuse3 `FileAttr` for a newly-created file or directory.
    fn synthetic_attr(&self, ino: u64, is_dir: bool, perm: u16) -> FileAttr {
        let ts = to_timestamp(SystemTime::now());
        FileAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: ts,
            mtime: ts,
            ctime: ts,
            kind: if is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            },
            perm,
            nlink: if is_dir { 2 } else { 1 },
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 4096,
        }
    }

    /// Map a raw POSIX `O_*` flag word to `OpenFlags`.
    fn decode_open_flags(flags: u32) -> OpenFlags {
        match flags as libc::c_int & libc::O_ACCMODE {
            libc::O_WRONLY => OpenFlags::WriteOnly,
            libc::O_RDWR => OpenFlags::ReadWrite,
            _ => OpenFlags::ReadOnly,
        }
    }

    /// Returns true when the kernel passed `O_APPEND` in the open-flag word.
    /// Used to route every write to the current end-of-file (F-021).
    fn has_append(flags: u32) -> bool {
        (flags as libc::c_int & libc::O_APPEND) != 0
    }
}

// ---------------------------------------------------------------------------
// fuse3::raw::Filesystem implementation
// ---------------------------------------------------------------------------

impl Filesystem for DtfsFuse3Adapter {
    async fn init(&self, _req: Request) -> FuseResult<ReplyInit> {
        Ok(ReplyInit {
            max_write: std::num::NonZeroU32::new(128 * 1024 * 1024).unwrap(),
        })
    }

    async fn destroy(&self, _req: Request) {}

    async fn lookup(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
    ) -> FuseResult<ReplyEntry> {
        let path = self.child_path(parent, name)?;
        debug!("fuse3:lookup {}", path);
        match self.fs.lookup(&path).await {
            Ok(attrs) => {
                let ino = self.inodes.lookup_ref(&path);
                Ok(ReplyEntry {
                    ttl: ENTRY_TTL,
                    attr: self.to_fuse_attr(ino, &attrs),
                    generation: 0,
                })
            }
            // CO-4-16: ENOENT only on genuine NotFound; transient -> EIO.
            Err(e) => Err(cluster_err_to_errno(&e)),
        }
    }

    async fn forget(&self, _req: Request, inode: u64, nlookup: u64) {
        if let Some(path) = self.inodes.forget(inode, nlookup) {
            self.mode_overrides.remove(&path);
        }
    }

    async fn batch_forget(&self, _req: Request, inodes: &[u64]) {
        for &ino in inodes {
            if let Some(path) = self.inodes.forget_quiet(ino, BATCH_FORGET_QUIET) {
                self.mode_overrides.remove(&path);
            }
        }
    }

    async fn getattr(
        &self,
        _req: Request,
        inode: u64,
        _fh: Option<u64>,
        _flags: u32,
    ) -> FuseResult<ReplyAttr> {
        let path = self.inodes.path_for(inode)?;
        debug!("fuse3:getattr {} (ino={})", path, inode);
        match self.fs.lookup(&path).await {
            Ok(attrs) => Ok(ReplyAttr {
                ttl: ATTR_TTL,
                attr: self.to_fuse_attr(inode, &attrs),
            }),
            // CO-4-16: ENOENT only on genuine NotFound; transient -> EIO.
            Err(e) => Err(cluster_err_to_errno(&e)),
        }
    }

    async fn setattr(
        &self,
        _req: Request,
        inode: u64,
        fh: Option<u64>,
        set_attr: SetAttr,
    ) -> FuseResult<ReplyAttr> {
        let path = self.inodes.path_for(inode)?;

        // F-020: persist `chmod`-requested mode in the process-local override
        // map so subsequent `stat` reports the requested value instead of the
        // default. We don't push this through to the cluster — the manager
        // doesn't track per-file UNIX permissions, and a media-server use case
        // does not need cluster-wide enforcement.
        if let Some(mode) = set_attr.mode {
            let perm = (mode & 0o7777) as u16;
            self.mode_overrides.insert(path.clone(), perm);
            debug!("setattr chmod {} -> {:o}", path, perm);
        }

        // F-037: truncate (both shrink and grow). When the caller is
        // `truncate -s N file`, `ftruncate(fd, N)`, or SQLite invalidating
        // a journal by truncating it, we honour the new size — including
        // dropping chunks past the new EOF on a shrink.
        if let Some(new_size) = set_attr.size {
            if let Err(e) = self.fs.truncate(&path, fh, new_size).await {
                warn!(path = %path, error = %e, "fuse3 setattr truncate failed");
                return Err(Errno::from(libc::EIO));
            }
        }

        match self.fs.lookup(&path).await {
            Ok(attrs) => Ok(ReplyAttr {
                ttl: ATTR_TTL,
                attr: self.to_fuse_attr(inode, &attrs),
            }),
            // CO-4-16: ENOENT only on genuine NotFound; transient -> EIO.
            Err(e) => Err(cluster_err_to_errno(&e)),
        }
    }

    async fn open(
        &self,
        _req: Request,
        inode: u64,
        flags: u32,
    ) -> FuseResult<ReplyOpen> {
        let path = self.inodes.path_for(inode)?;
        debug!("fuse3:open {} (ino={}, flags={:#x})", path, inode, flags);
        let open_flags = Self::decode_open_flags(flags);
        let append = Self::has_append(flags);
        match self
            .fs
            .open_with_append(&path, open_flags, append)
            .await
        {
            Ok(fh) => Ok(ReplyOpen { fh, flags: 0 }),
            Err(e) => {
                // P0-1: surface write-lease conflicts as EBUSY so apps see
                // "Resource busy" instead of a generic ENOENT.
                if e.downcast_ref::<crate::filesystem::LeaseBusy>().is_some() {
                    warn!(path = %path, "fuse3 open: write lease busy");
                    return Err(Errno::from(libc::EBUSY));
                }
                // CO-4-16: ENOENT only on genuine NotFound; transient -> EIO.
                Err(cluster_err_to_errno(&e))
            }
        }
    }

    async fn release(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> FuseResult<()> {
        debug!("fuse3:release fh={}", fh);
        self.fs.release(fh).await.map_err(|e| {
            warn!(fh, error = %e, "fuse3 release failed");
            Errno::from(libc::EIO)
        })
    }

    async fn read(
        &self,
        _req: Request,
        inode: u64,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> FuseResult<ReplyData> {
        debug!(
            "fuse3:read ino={} fh={} offset={} size={}",
            inode, fh, offset, size
        );
        match self.fs.read(fh, offset, size).await {
            Ok(data) => Ok(ReplyData { data }),
            Err(e) => {
                warn!(fh, error = %e, "fuse3 read failed");
                Err(Errno::from(libc::EIO))
            }
        }
    }

    async fn write(
        &self,
        _req: Request,
        inode: u64,
        fh: u64,
        offset: u64,
        data: &[u8],
        _write_flags: u32,
        _flags: u32,
    ) -> FuseResult<ReplyWrite> {
        debug!(
            "fuse3:write ino={} fh={} offset={} len={}",
            inode,
            fh,
            offset,
            data.len()
        );
        match self.fs.write(fh, offset, data).await {
            Ok(written) => Ok(ReplyWrite { written }),
            Err(e) => {
                warn!(fh, error = %e, "fuse3 write failed");
                Err(Errno::from(libc::EIO))
            }
        }
    }

    async fn flush(
        &self,
        _req: Request,
        inode: u64,
        fh: u64,
        lock_owner: u64,
    ) -> FuseResult<()> {
        debug!("fuse3:flush fh={}", fh);
        // FUSE-3 — the kernel sends FUSE_FLUSH with `lock_owner` on every
        // close(2) precisely so the server drops that owner's POSIX record
        // locks. Pre-fix nothing did, so a writer that exited while holding
        // an fcntl lock left it resident forever and the next F_SETLKW on
        // the inode blocked without end (a mount hang with no cluster
        // fault). Same shape as the F_UNLCK arm of `setlk`.
        self.release_posix_locks_for_owner(inode, lock_owner).await;
        self.fs.flush(fh).await.map_err(|e| {
            warn!(fh, error = %e, "fuse3 flush failed");
            Errno::from(libc::EIO)
        })
    }

    /// Durable flush handler. SQLite (and every database/log writer) calls
    /// `fsync()` to know its data is safely persisted before continuing.
    /// Without this method the kernel returns OK locally and the writes may
    /// still be sitting in our process-local `write_buffer` — i.e. a power
    /// loss before the next release() would lose committed transactions.
    /// We route fsync to the same chunk-flush path used by close().
    async fn fsync(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        _datasync: bool,
    ) -> FuseResult<()> {
        debug!("fuse3:fsync fh={}", fh);
        self.fs.flush(fh).await.map_err(|e| {
            warn!(fh, error = %e, "fuse3 fsync failed");
            Errno::from(libc::EIO)
        })
    }

    /// POSIX advisory byte-range lock check (`fcntl(F_GETLK)`) — F-031.
    async fn getlk(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        r#type: u32,
        pid: u32,
    ) -> FuseResult<ReplyLock> {
        let want_write = (r#type as i32) == libc::F_WRLCK;
        let guard = self.locks.lock().await;
        if let Some(held) = guard.get(&inode) {
            for h in &held.posix {
                if h.owner == lock_owner {
                    continue;
                }
                if !h.overlaps(start, end) {
                    continue;
                }
                if h.write || want_write {
                    return Ok(ReplyLock {
                        start: h.start,
                        end: h.end,
                        r#type: if h.write {
                            libc::F_WRLCK as u32
                        } else {
                            libc::F_RDLCK as u32
                        },
                        pid,
                    });
                }
            }
        }
        Ok(ReplyLock {
            start,
            end,
            r#type: libc::F_UNLCK as u32,
            pid,
        })
    }

    /// POSIX advisory byte-range lock acquire/release (`fcntl(F_SETLK)` and
    /// `fcntl(F_SETLKW)`) — F-031.
    ///
    /// When `block` is true (`F_SETLKW`), we actually wait for the lock to
    /// become available rather than returning `EAGAIN`. Waiters subscribe
    /// to a per-inode `Notify`; any release pulses it so blocked acquirers
    /// re-check and try again.
    async fn setlk(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        r#type: u32,
        _pid: u32,
        block: bool,
    ) -> FuseResult<()> {
        let lt = r#type as i32;

        if lt == libc::F_UNLCK {
            let mut guard = self.locks.lock().await;
            if let Some(t) = guard.get_mut(&inode) {
                carve_owner_range(&mut t.posix, lock_owner, start, end);
            }
            drop(guard);
            self.notify_for(inode).notify_waiters();
            return Ok(());
        }

        let want_write = lt == libc::F_WRLCK;
        let notify = self.notify_for(inode);

        loop {
            // Subscribe before taking the lock so a release between our
            // check and the await call still wakes us.
            let waiter = notify.notified();
            tokio::pin!(waiter);
            waiter.as_mut().enable();

            {
                let mut guard = self.locks.lock().await;
                let table = guard.entry(inode).or_default();
                let conflict = table.posix.iter().any(|h| {
                    h.owner != lock_owner
                        && h.overlaps(start, end)
                        && (h.write || want_write)
                });
                if !conflict {
                    carve_owner_range(&mut table.posix, lock_owner, start, end);
                    table.posix.push(HeldLock {
                        owner: lock_owner,
                        start,
                        end,
                        write: want_write,
                    });
                    return Ok(());
                }
            }

            if !block {
                return Err(Errno::from(libc::EAGAIN));
            }
            // Bounded wait so a buggy peer can't pin a thread forever.
            // SQLite's busy_timeout retries us anyway.
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                waiter,
            )
            .await;
        }
    }

    // Note: fuse3 0.7.3 does not expose a separate `flock` trait method;
    // Linux passes `flock(2)` calls through the same FUSE_LK opcode that
    // our `setlk` already implements (with the whole-file range and
    // F_WRLCK / F_RDLCK type). The `flock_owners` field on `InodeLocks`
    // is reserved for a future explicit handler.

    async fn mkdir(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
    ) -> FuseResult<ReplyEntry> {
        let path = self.child_path(parent, name)?;
        debug!("fuse3:mkdir {} (mode={:#o})", path, mode);
        match self.fs.mkdir(&path).await {
            Ok(()) => {
                let ino = self.inodes.lookup_ref(&path);
                Ok(ReplyEntry {
                    ttl: ENTRY_TTL,
                    attr: self.synthetic_attr(ino, true, (mode & 0o777) as u16),
                    generation: 0,
                })
            }
            // CR-1-6 (FUSE-4/CO-4-16): route through cluster_err_to_errno,
            // never a bare EIO/ENOENT — a manager blip during a mkdir must
            // not be misreported.
            Err(e) => {
                warn!(path = %path, error = %e, "fuse3 mkdir failed");
                Err(cluster_err_to_errno(&e))
            }
        }
    }

    async fn unlink(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
    ) -> FuseResult<()> {
        let path = self.child_path(parent, name)?;
        debug!("fuse3:unlink {}", path);
        // Snapshot the inode (if any) before we drop the path mapping, so we
        // can clear any lingering byte-range locks for that inode (F-031).
        let inode_for_locks = self.inodes.path_to_inode.get(&path).map(|i| *i);
        match self.fs.unlink(&path).await {
            Ok(()) => {
                self.inodes.remove(&path);
                self.mode_overrides.remove(&path);
                if let Some(ino) = inode_for_locks {
                    self.locks.lock().await.remove(&ino);
                    self.lock_notify.remove(&ino);
                }
                Ok(())
            }
            // CR-1-6 (FUSE-4/CO-4-16): a transient manager error must map
            // to EIO, not ENOENT — `cluster_err_to_errno` makes ENOENT
            // conditional on a genuine NotFound.
            Err(e) => {
                warn!(path = %path, error = %e, "fuse3 unlink failed");
                Err(cluster_err_to_errno(&e))
            }
        }
    }

    async fn rmdir(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
    ) -> FuseResult<()> {
        let path = self.child_path(parent, name)?;
        debug!("fuse3:rmdir {}", path);
        match self.fs.unlink(&path).await {
            Ok(()) => {
                self.inodes.remove(&path);
                Ok(())
            }
            // CR-1-6 (FUSE-4/CO-4-16): same rationale as unlink above.
            Err(e) => {
                warn!(path = %path, error = %e, "fuse3 rmdir failed");
                Err(cluster_err_to_errno(&e))
            }
        }
    }

    async fn create(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _flags: u32,
    ) -> FuseResult<ReplyCreated> {
        let path = self.child_path(parent, name)?;
        debug!("fuse3:create {} (mode={:#o})", path, mode);
        match self.fs.create(&path, mode).await {
            Ok(fh) => {
                let ino = self.inodes.lookup_ref(&path);
                let attr = self.synthetic_attr(ino, false, (mode & 0o777) as u16);
                Ok(ReplyCreated {
                    ttl: ENTRY_TTL,
                    attr,
                    generation: 0,
                    fh,
                    flags: 0,
                })
            }
            Err(e) => {
                // P0-1: write-lease conflict on create → EBUSY.
                if e.downcast_ref::<crate::filesystem::LeaseBusy>().is_some() {
                    warn!(path = %path, "fuse3 create: write lease busy");
                    return Err(Errno::from(libc::EBUSY));
                }
                // Bug Y diagnostic — include the anyhow error chain so we
                // see the actual manager-side reject (e.g. Raft proposal
                // failure, no eligible data node, etc.) instead of just
                // "create_file failed".
                // CR-1-6 (FUSE-4/CO-4-16): route the generic case through
                // cluster_err_to_errno too.
                warn!(path = %path, error = %format!("{:#}", e), "fuse3 create failed");
                Err(cluster_err_to_errno(&e))
            }
        }
    }

    async fn opendir(
        &self,
        _req: Request,
        inode: u64,
        flags: u32,
    ) -> FuseResult<ReplyOpen> {
        let path = self.inodes.path_for(inode)?;
        debug!("fuse3:opendir {} (ino={})", path, inode);
        match self.fs.lookup(&path).await {
            Ok(a) if a.is_directory => {
                // Snapshot the listing once per opendir; pages are served
                // from it so a concurrent delete cannot shift cookies and
                // skip an unrelated entry.
                let entries = self
                    .fs
                    .readdir(&path)
                    .await
                    .map_err(|e| cluster_err_to_errno(&e))?;
                Ok(ReplyOpen { fh: self.dirs.open(entries), flags })
            }
            Ok(_) => Err(Errno::from(libc::ENOTDIR)),
            // CO-4-16: ENOENT only on genuine NotFound; transient -> EIO.
            Err(e) => Err(cluster_err_to_errno(&e)),
        }
    }

    async fn releasedir(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        _flags: u32,
    ) -> FuseResult<()> {
        self.dirs.close(fh);
        Ok(())
    }

    type DirEntryStream<'a>
        = futures_util::stream::Iter<std::vec::IntoIter<FuseResult<DirectoryEntry>>>
    where
        Self: 'a;

    async fn readdir<'a>(
        &'a self,
        _req: Request,
        inode: u64,
        fh: u64,
        offset: i64,
    ) -> FuseResult<ReplyDirectory<Self::DirEntryStream<'a>>> {
        let path = self.inodes.path_for(inode)?;
        debug!("fuse3:readdir {} (ino={}, offset={})", path, inode, offset);

        // CR-1-6 (FUSE-4/CO-4-16): a manager blip during a Plex library
        // scan must not read as "directory does not exist" — only a
        // genuine NotFound may map to ENOENT.
        let entries = self.listing(fh, &path).await?;

        // Cookie scheme — Bug F fix:
        //
        // The kernel calls readdirplus first (no "." / "..", entries get
        // cookies `idx + 1` starting at 1) and then readdir(offset=last)
        // to verify there's nothing after. If readdir uses a DIFFERENT
        // cookie scheme (e.g. reserving 1 and 2 for "." and "..") the
        // kernel ends up with two cookies for the same name — one from
        // readdirplus and a fresh one from readdir — and ls / find see
        // the entry twice. Lab symptom: "The Nun II (2023)" and
        // "Wicked (2024)" appeared duplicated after every `ls` until the
        // SOAK-RECOVERY-2026-05-18 bug F fix.
        //
        // The kernel synthesises "." and ".." for us on the userspace
        // side — we MUST NOT emit them here, and the cookie for the
        // first real entry must be 1, matching readdirplus.
        let mut items: Vec<FuseResult<DirectoryEntry>> = Vec::new();

        for (cookie, entry) in snapshot_page(&entries, offset.max(0) as u64) {
            let entry_offset = cookie as i64;
            let child_path = if path == "/" {
                format!("/{}", entry.name)
            } else {
                format!("{}/{}", path, entry.name)
            };
            let child_ino = self.inodes.inode_for(&child_path);
            items.push(Ok(DirectoryEntry {
                inode: child_ino,
                offset: entry_offset,
                kind: if entry.is_directory {
                    FileType::Directory
                } else {
                    FileType::RegularFile
                },
                name: entry.name.clone().into(),
            }));
        }

        Ok(ReplyDirectory {
            entries: futures_util::stream::iter(items),
        })
    }

    type DirEntryPlusStream<'a>
        = futures_util::stream::BoxStream<'a, FuseResult<DirectoryEntryPlus>>
    where
        Self: 'a;

    async fn readdirplus<'a>(
        &'a self,
        _req: Request,
        inode: u64,
        fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> FuseResult<ReplyDirectoryPlus<Self::DirEntryPlusStream<'a>>> {
        let path = self.inodes.path_for(inode)?;
        debug!("fuse3:readdirplus {} (ino={}, offset={})", path, inode, offset);
        // CR-1-6 (FUSE-4/CO-4-16): same rationale as readdir above.
        let entries = self.listing(fh, &path).await?;

        let mut items: Vec<FuseResult<DirectoryEntryPlus>> = Vec::new();

        for (entry_offset, entry) in snapshot_page(&entries, offset) {
            let child_path = if path == "/" {
                format!("/{}", entry.name)
            } else {
                format!("{}/{}", path, entry.name)
            };
            let child_ino = self.inodes.inode_for(&child_path);
            let kind = if entry.is_directory {
                FileType::Directory
            } else {
                FileType::RegularFile
            };
            let attr = build_attr(
                child_ino,
                &DtfsAttr {
                    size: entry.size,
                    is_directory: entry.is_directory,
                    created_at: entry.created_at,
                    modified_at: entry.modified_at,
                },
                self.override_perm(&child_path, entry.is_directory),
            );
            items.push(Ok(DirectoryEntryPlus {
                inode: child_ino,
                generation: 0,
                kind,
                name: entry.name.clone().into(),
                offset: entry_offset as i64,
                attr,
                entry_ttl: ENTRY_TTL,
                attr_ttl: ATTR_TTL,
            }));
        }

        // Each entry the kernel actually receives is a lookup reference;
        // count lazily as fuse3 pulls items so entries it never reads (page
        // full) are not counted (at most the one item pulled and dropped).
        let inodes = &self.inodes;
        Ok(ReplyDirectoryPlus {
            entries: futures_util::stream::iter(items)
                .map(move |r| {
                    if let Ok(e) = &r {
                        inodes.add_ref(e.inode);
                    }
                    r
                })
                .boxed(),
        })
    }

    /// `statvfs` — return cluster-aware total/used/free capacity so
    /// `df /mnt/dtfs` shows real numbers. Falls back to the historic
    /// pseudo values if the manager is unreachable.
    async fn statfs(&self, _req: Request, _inode: u64) -> FuseResult<ReplyStatFs> {
        // 4 KiB blocks across the board.
        const BSIZE: u32 = 4096;
        let bsize_u64 = BSIZE as u64;
        let (total_bytes, used_bytes, file_count) = match self.fs.cluster_capacity().await {
            Ok(c) => c,
            Err(_) => {
                return Ok(ReplyStatFs {
                    blocks: 1_000_000_000,
                    bfree: 900_000_000,
                    bavail: 900_000_000,
                    files: 1_000_000,
                    ffree: 999_900,
                    bsize: BSIZE,
                    namelen: 255,
                    frsize: BSIZE,
                });
            }
        };
        let blocks = total_bytes / bsize_u64;
        let used_blocks = used_bytes / bsize_u64;
        let bfree = blocks.saturating_sub(used_blocks);
        Ok(ReplyStatFs {
            blocks,
            bfree,
            bavail: bfree,
            files: file_count.max(1),
            ffree: u64::MAX / 2,
            bsize: BSIZE,
            namelen: 255,
            frsize: BSIZE,
        })
    }

    /// `fallocate(2)` — pre-allocate space. DTFS stores files as sparse
    /// chunk lists so pre-allocation is conceptually free. We treat both
    /// FALLOC_FL_KEEP_SIZE and the default mode as a no-op success. (F-033)
    async fn fallocate(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        offset: u64,
        length: u64,
        mode: u32,
    ) -> FuseResult<()> {
        debug!(
            "fuse3:fallocate fh={} off={} len={} mode={}",
            fh, offset, length, mode
        );
        if length == 0 {
            return Ok(());
        }
        // CO-5-18 — honour the mode: FALLOC_FL_KEEP_SIZE must NOT grow the
        // file, and `offset + length` must not silently overflow u64.
        let new_size = match fallocate_target_size(offset, length, mode)? {
            Some(size) => size,
            None => return Ok(()), // KEEP_SIZE: reserve space, don't grow.
        };
        // Grow the file via setattr-style size update. The sparse-fill path
        // turns the trailing bytes into a hole, matching what other
        // filesystems do for default-mode fallocate.
        if let Err(e) = self.fs.set_size(fh, new_size).await {
            warn!(error = %e, "fuse3 fallocate set_size failed");
            return Err(Errno::from(libc::EIO));
        }
        Ok(())
    }

    // Note: we intentionally do NOT override `getxattr` / `setxattr` /
    // `listxattr` / `removexattr` here. fuse3 0.7.3's xattr reply encoding
    // tears the session down on Debian 13 (libfuse3-4) when the userspace
    // returns a custom `ReplyXAttr::Size` — the kernel rejects the
    // malformed reply and disconnects. Keeping the default trait impls
    // (which return ENOSYS straight through the session) lets the kernel
    // cache ENOSYS once and short-circuit future calls. Net effect:
    // `setfattr` fails fast with "operation not supported" instead of
    // crashing the mount. Linux xattrs are not part of POSIX-1-2024 and
    // are not a hard requirement for the workloads DTFS targets.

    /// `rename(2)` — F-034. Atomic same-directory and cross-directory move.
    async fn rename(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
    ) -> FuseResult<()> {
        let from = self.child_path(parent, name)?;
        let to = self.child_path(new_parent, new_name)?;
        debug!("fuse3:rename {} -> {}", from, to);
        match self.fs.rename(&from, &to).await {
            Ok(()) => {
                // Migrate adapter-local state.
                self.inodes.rename(&from, &to);
                if let Some((_, v)) = self.mode_overrides.remove(&from) {
                    self.mode_overrides.insert(to.clone(), v);
                }
                Ok(())
            }
            // CR-1-6 (FUSE-4/CO-4-16): route through cluster_err_to_errno.
            Err(e) => {
                warn!(from = %from, to = %to, error = %e, "fuse3 rename failed");
                Err(cluster_err_to_errno(&e))
            }
        }
    }

    /// `symlink(2)` — F-035. Manager stores the link target string as
    /// metadata; readlink retrieves it.
    async fn symlink(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        target: &OsStr,
    ) -> FuseResult<ReplyEntry> {
        let path = self.child_path(parent, name)?;
        let tgt = target
            .to_str()
            .ok_or_else(|| Errno::from(libc::EINVAL))?
            .to_string();
        debug!("fuse3:symlink {} -> {}", path, tgt);
        match self.fs.symlink(&path, &tgt).await {
            Ok(()) => {
                let ino = self.inodes.lookup_ref(&path);
                Ok(ReplyEntry {
                    ttl: ENTRY_TTL,
                    attr: FileAttr {
                        ino,
                        size: tgt.len() as u64,
                        blocks: 0,
                        atime: to_timestamp(SystemTime::now()),
                        mtime: to_timestamp(SystemTime::now()),
                        ctime: to_timestamp(SystemTime::now()),
                        kind: FileType::Symlink,
                        perm: 0o777,
                        nlink: 1,
                        uid: unsafe { libc::getuid() },
                        gid: unsafe { libc::getgid() },
                        rdev: 0,
                        blksize: 4096,
                    },
                    generation: 0,
                })
            }
            // CR-1-6 (FUSE-4/CO-4-16): route through cluster_err_to_errno.
            Err(e) => {
                warn!(path = %path, error = %e, "fuse3 symlink failed");
                Err(cluster_err_to_errno(&e))
            }
        }
    }

    /// `readlink(2)` — F-035.
    async fn readlink(&self, _req: Request, inode: u64) -> FuseResult<ReplyData> {
        let path = self.inodes.path_for(inode)?;
        match self.fs.readlink(&path).await {
            Ok(t) => Ok(ReplyData {
                data: t.into_bytes().into(),
            }),
            // CO-4-16: ENOENT only on genuine NotFound; transient -> EIO.
            Err(e) => Err(cluster_err_to_errno(&e)),
        }
    }

    /// `link(2)` — F-036. Hard link. We implement it by copying the source
    /// file's chunk-location list to a new path entry; the chunks remain
    /// shared. This gives the same observable behaviour as a hard link
    /// (both names see identical bytes) but with snapshot semantics — a
    /// subsequent write through one name diverges. SQLite WAL recovery
    /// uses link() once at open and doesn't write through the link, so
    /// this is sufficient for the common case.
    async fn link(
        &self,
        _req: Request,
        inode: u64,
        new_parent: u64,
        new_name: &OsStr,
    ) -> FuseResult<ReplyEntry> {
        let src = self.inodes.path_for(inode)?;
        let dst = self.child_path(new_parent, new_name)?;
        debug!("fuse3:link {} -> {}", src, dst);
        match self.fs.link(&src, &dst).await {
            Ok(attr) => {
                let ino = self.inodes.lookup_ref(&dst);
                Ok(ReplyEntry {
                    ttl: ENTRY_TTL,
                    attr: self.to_fuse_attr(ino, &attr),
                    generation: 0,
                })
            }
            // CR-1-6 (FUSE-4/CO-4-16): route through cluster_err_to_errno.
            Err(e) => {
                warn!(src = %src, dst = %dst, error = %e, "fuse3 link failed");
                Err(cluster_err_to_errno(&e))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// CO-4-16 — map a cluster-side error to the errno a metadata op should
/// return. Return `ENOENT` ONLY for a genuine gRPC `NotFound`; map every
/// other failure (manager outage, election window, timeout, internal error,
/// or an error we can't classify) to `EIO`.
///
/// Why this matters: the old code mapped *every* error to `ENOENT`. During a
/// manager blip a `lookup`/`getattr` then reported "file does not exist", and
/// Plex treats that as a deleted file and drops the library entry — a
/// transient outage turned into a permanent-looking data loss to the app.
/// `EIO` is the correct "try again, the file may still be there" signal.
///
/// The `tonic::Status` is preserved in the anyhow error chain by the client
/// (`call_manager` wraps it via `anyhow::Error::new(status).context(...)`),
/// possibly under one or more `.context(...)` layers, so we walk the whole
/// chain rather than only inspecting the top error.
fn cluster_err_to_errno(err: &anyhow::Error) -> Errno {
    for cause in err.chain() {
        if let Some(status) = cause.downcast_ref::<tonic::Status>() {
            if status.code() == tonic::Code::NotFound {
                return Errno::from(libc::ENOENT);
            }
            // CR-2-4 (RAFT-7) — DeleteFile refuses to remove a directory
            // that still has children with FailedPrecondition; rmdir must
            // surface that as ENOTEMPTY, not a generic EIO, or `rm -r`
            // reports the tree gone while it is still there.
            if status.code() == tonic::Code::FailedPrecondition {
                return Errno::from(libc::ENOTEMPTY);
            }
            // Any other status (Unavailable, DeadlineExceeded, Internal, ...)
            // is transient or non-NotFound/non-FailedPrecondition — never
            // ENOENT.
            return Errno::from(libc::EIO);
        }
    }
    // No gRPC status in the chain: an error we can't classify. Treat as
    // transient (EIO), never "file gone".
    Errno::from(libc::EIO)
}

/// Convert a `SystemTime` to fuse3's `Timestamp`.
fn to_timestamp(st: SystemTime) -> Timestamp {
    let d = st.duration_since(UNIX_EPOCH).unwrap_or_default();
    Timestamp::new(d.as_secs() as i64, d.subsec_nanos())
}

/// Return the parent path of a given absolute path string.
fn parent_of(path: &str) -> String {
    if path == "/" {
        return "/".to_string();
    }
    match path.rfind('/') {
        Some(0) | None => "/".to_string(),
        Some(idx) => path[..idx].to_string(),
    }
}

/// Cookie that fuse3 emits for the directory entry at position `idx`
/// (0-based). Used by BOTH `readdir` and `readdirplus`. The two paths
/// MUST use the same scheme — see Bug F in
/// `plans/incidents/SOAK-RECOVERY-2026-05-18.md`. If readdir emitted a
/// different cookie for the same idx, the kernel would assemble two
/// distinct dirents for the same name and `ls` would show duplicates.
#[inline]
pub(crate) fn dir_entry_cookie(idx: usize) -> u64 {
    (idx as u64) + 1
}

/// CO-5-18 — resolve the target file size for a `fallocate(2)` request.
///
/// Returns:
///   * `Ok(None)`   when `FALLOC_FL_KEEP_SIZE` is set — space is reserved
///     but the file MUST NOT grow, so the caller skips the `set_size`.
///   * `Err(EINVAL)` when `offset + length` overflows `u64`.
///   * `Ok(Some(offset + length))` otherwise — the new file size.
///
/// The kernel passes other mode bits (e.g. PUNCH_HOLE) too, but DTFS only
/// distinguishes "grow" vs "keep size"; KEEP_SIZE is the only bit that
/// suppresses the grow.
pub(crate) fn fallocate_target_size(
    offset: u64,
    length: u64,
    mode: u32,
) -> Result<Option<u64>, Errno> {
    if mode & (libc::FALLOC_FL_KEEP_SIZE as u32) != 0 {
        return Ok(None);
    }
    match offset.checked_add(length) {
        Some(size) => Ok(Some(size)),
        None => Err(Errno::from(libc::EINVAL)),
    }
}

#[cfg(test)]
mod fuse_readdir_regression {
    use super::*;

    /// Bug F — readdir + readdirplus MUST agree on the cookie for the
    /// same entry. Pre-fix readdir reserved cookies 1 & 2 for "." and
    /// ".." and emitted children at `idx + 3`; readdirplus emitted
    /// children at `idx + 1`. The kernel saw two different cookies for
    /// the same name and ls/find showed duplicates.
    ///
    /// Live-lab symptom: `ls /mnt/dtfs/` produced 9 lines for 7 entries
    /// ("The Nun II (2023)" and "Wicked (2024)" duplicated). Verified
    /// fixed on the 3-DC lab post-deploy.
    #[test]
    fn bug_f_readdir_and_readdirplus_share_cookie_scheme() {
        // The helper is the single source of truth for the cookie scheme.
        // Both readdir and readdirplus use it.
        assert_eq!(dir_entry_cookie(0), 1);
        assert_eq!(dir_entry_cookie(1), 2);
        assert_eq!(dir_entry_cookie(6), 7);
        // The kernel synthesises "." and ".." for us — we MUST NOT
        // reserve cookies 1/2 for them in readdir output. If a future
        // change tries to push children into 3.. the test below catches
        // the regression.
        // (We can't unit-test the actual readdir impl without a fuse3
        // harness, but we can lock the cookie scheme.)
        for idx in 0..32 {
            assert_eq!(dir_entry_cookie(idx), (idx as u64) + 1,
                "cookie scheme must remain `idx + 1` for both readdir and \
                 readdirplus — see Bug F in SOAK-RECOVERY-2026-05-18.md");
        }
    }

    // CR-1-6 (FUSE-4) — readdir/readdirplus now map their manager error
    // through `cluster_err_to_errno` instead of a bare `|_| ENOENT`. There
    // is no fuse3 test harness to drive the trait methods themselves (see
    // the comment above), so this locks the mapping function they now both
    // call: a transient `Unavailable` (manager blip during a Plex library
    // scan) must surface as EIO, never ENOENT — the old code reported
    // "directory does not exist" for ANY error, including this one.
    #[test]
    fn cr_1_6_transient_manager_error_is_not_enoent() {
        let transient = anyhow::Error::new(tonic::Status::unavailable("manager unreachable"))
            .context("readdir failed for /movies");
        assert_eq!(cluster_err_to_errno(&transient), Errno::from(libc::EIO));
        assert_ne!(cluster_err_to_errno(&transient), Errno::from(libc::ENOENT));
    }

    // CR-2-4 (RAFT-7) — DeleteFile refuses a non-empty directory with
    // FailedPrecondition; rmdir must map that to ENOTEMPTY so `rm -r`
    // reports the tree is still there instead of a generic I/O error.
    #[test]
    fn cr_2_4_failed_precondition_maps_to_enotempty() {
        let refused =
            anyhow::Error::new(tonic::Status::failed_precondition("directory not empty"))
                .context("rmdir failed for /movies");
        assert_eq!(cluster_err_to_errno(&refused), Errno::from(libc::ENOTEMPTY));
    }
}

#[cfg(test)]
mod audit_co_tests {
    use super::*;

    /// CO-5-18 — FALLOC_FL_KEEP_SIZE reserves space without growing the
    /// file, so the helper returns `None` and the caller skips `set_size`.
    /// Pre-fix `fallocate` always grew to `offset + length`, wrongly
    /// extending the file.
    #[test]
    fn co_5_18_keep_size_does_not_grow() {
        let mode = libc::FALLOC_FL_KEEP_SIZE as u32;
        assert_eq!(fallocate_target_size(100, 50, mode).unwrap(), None);
        // KEEP_SIZE combined with other bits still suppresses the grow.
        let combined = (libc::FALLOC_FL_KEEP_SIZE | libc::FALLOC_FL_PUNCH_HOLE) as u32;
        assert_eq!(fallocate_target_size(100, 50, combined).unwrap(), None);
    }

    /// CO-5-18 — `offset + length` overflowing u64 must surface as EINVAL,
    /// not wrap around to a tiny/zero size. Pre-fix `offset + length`
    /// panicked in debug or wrapped in release.
    #[test]
    fn co_5_18_overflow_is_einval() {
        let err = fallocate_target_size(u64::MAX, 1, 0).unwrap_err();
        assert_eq!(err, Errno::from(libc::EINVAL));
    }

    /// CO-5-18 — a normal default-mode request grows the file to
    /// `offset + length`.
    #[test]
    fn co_5_18_default_mode_grows() {
        assert_eq!(fallocate_target_size(100, 50, 0).unwrap(), Some(150));
    }

    /// CO-4-16 — a genuine `NotFound` status maps to ENOENT (the file
    /// really is gone).
    #[test]
    fn co_4_16_not_found_maps_to_enoent() {
        let err = anyhow::Error::new(tonic::Status::not_found("no such file"));
        assert_eq!(cluster_err_to_errno(&err), Errno::from(libc::ENOENT));
    }

    /// CO-4-16 — a transient manager outage (`Unavailable`) must NOT map to
    /// ENOENT, or Plex sees "file does not exist" during a blip and drops
    /// the library entry. It maps to EIO instead.
    #[test]
    fn co_4_16_transient_unavailable_maps_to_eio_not_enoent() {
        let err = anyhow::Error::new(tonic::Status::unavailable("manager down"));
        let errno = cluster_err_to_errno(&err);
        assert_eq!(errno, Errno::from(libc::EIO));
        assert_ne!(errno, Errno::from(libc::ENOENT));
    }

    /// CO-4-16 — a timeout (`DeadlineExceeded`) is also transient -> EIO.
    #[test]
    fn co_4_16_deadline_exceeded_maps_to_eio() {
        let err = anyhow::Error::new(tonic::Status::deadline_exceeded("timed out"));
        assert_eq!(cluster_err_to_errno(&err), Errno::from(libc::EIO));
    }

    /// CO-4-16 — the status is preserved under `.context(...)` layers (the
    /// client wraps it that way, and `fetch_file_locations` adds another).
    /// The helper must walk the whole chain, not just the top error.
    #[test]
    fn co_4_16_finds_status_under_context_layers() {
        let not_found = anyhow::Error::new(tonic::Status::not_found("gone"))
            .context("get_file_locations RPC failed")
            .context("get_file_locations failed for /Movies/x.mkv");
        assert_eq!(cluster_err_to_errno(&not_found), Errno::from(libc::ENOENT));

        let unavailable = anyhow::Error::new(tonic::Status::unavailable("down"))
            .context("get_file_locations RPC failed (all managers exhausted)");
        assert_eq!(cluster_err_to_errno(&unavailable), Errno::from(libc::EIO));
    }

    /// CO-4-16 — an error with no gRPC status in the chain is unclassifiable
    /// and must be treated as transient (EIO), never "file gone".
    #[test]
    fn co_4_16_unknown_error_maps_to_eio() {
        let err = anyhow::anyhow!("some local error with no tonic status");
        assert_eq!(cluster_err_to_errno(&err), Errno::from(libc::EIO));
    }
}

#[cfg(test)]
mod inode_map_rename_tests {
    use super::*;

    #[test]
    fn rename_rekeys_children_of_a_renamed_directory() {
        let m = InodeMap::new();
        let d = m.inode_for("/a");
        let f = m.inode_for("/a/f.mkv");
        let deep = m.inode_for("/a/sub/g");
        let sibling = m.inode_for("/ab/f");
        m.rename("/a", "/b");
        assert_eq!(m.path_for(d).unwrap(), "/b");
        assert_eq!(m.path_for(f).unwrap(), "/b/f.mkv");
        assert_eq!(m.path_for(deep).unwrap(), "/b/sub/g");
        assert_eq!(m.inode_for("/b/f.mkv"), f);
        // old child path no longer maps to the moved inode
        assert_ne!(m.inode_for("/a/f.mkv"), f);
        // a path that merely shares the string prefix is untouched
        assert_eq!(m.path_for(sibling).unwrap(), "/ab/f");
    }

    #[test]
    fn rename_drops_the_replaced_destination_inode() {
        let m = InodeMap::new();
        let src = m.inode_for("/x");
        let victim = m.inode_for("/y");
        m.rename("/x", "/y");
        assert_eq!(m.path_for(src).unwrap(), "/y");
        assert_eq!(m.inode_for("/y"), src);
        assert!(m.path_for(victim).is_err(), "replaced inode must not resolve");
    }
}

#[cfg(test)]
mod posix_lock_range_tests {
    use super::*;

    fn lk(owner: u64, start: u64, end: u64) -> HeldLock {
        HeldLock { owner, start, end, write: true }
    }

    #[test]
    fn one_byte_locks_on_same_byte_overlap_adjacent_do_not() {
        // fcntl lock of byte 5 arrives as start=5,end=5 (inclusive).
        let h = lk(1, 5, 5);
        assert!(h.overlaps(5, 5), "same byte must conflict");
        assert!(!h.overlaps(6, 6));
        assert!(!h.overlaps(4, 4));
        assert!(h.overlaps(0, 5));
        assert!(h.overlaps(5, u64::MAX));
    }

    #[test]
    fn whole_file_lock_overlaps_everything() {
        let h = lk(1, 0, u64::MAX);
        assert!(h.overlaps(0, 0));
        assert!(h.overlaps(u64::MAX, u64::MAX));
    }

    #[test]
    fn unlock_in_the_middle_splits_the_lock() {
        let mut v = vec![lk(1, 0, 99), lk(2, 0, 99)];
        carve_owner_range(&mut v, 1, 40, 59);
        let mine: Vec<_> = v.iter().filter(|h| h.owner == 1).map(|h| (h.start, h.end)).collect();
        assert_eq!(mine, vec![(0, 39), (60, 99)]);
        // other owner untouched
        assert!(v.iter().any(|h| h.owner == 2 && h.start == 0 && h.end == 99));
    }

    #[test]
    fn unlock_whole_file_range_removes_without_overflow() {
        let mut v = vec![lk(1, 10, 20), lk(1, 0, u64::MAX)];
        carve_owner_range(&mut v, 1, 0, u64::MAX);
        assert!(v.is_empty());
    }

    #[test]
    fn unlock_one_byte_at_edges_keeps_the_rest() {
        let mut v = vec![lk(1, 0, 9)];
        carve_owner_range(&mut v, 1, 0, 0);
        assert_eq!((v[0].start, v[0].end), (1, 9));
        carve_owner_range(&mut v, 1, 9, 9);
        assert_eq!((v[0].start, v[0].end), (1, 8));
        assert_eq!(v.len(), 1);
    }

    #[test]
    fn non_overlapping_range_is_untouched() {
        let mut v = vec![lk(1, 0, 9)];
        carve_owner_range(&mut v, 1, 10, 20);
        assert_eq!((v[0].start, v[0].end), (0, 9));
    }
}

#[cfg(test)]
mod forget_and_snapshot_tests {
    use super::*;
    use crate::filesystem::DirEntry;

    fn de(name: &str, dir: bool) -> DirEntry {
        DirEntry {
            name: name.to_string(),
            size: 10,
            is_directory: dir,
            created_at: SystemTime::UNIX_EPOCH,
            modified_at: SystemTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn forget_drops_mapping_only_when_count_reaches_zero() {
        let m = InodeMap::new();
        let a = m.lookup_ref("/a");
        assert_eq!(m.lookup_ref("/a"), a);
        assert_eq!(m.forget(a, 1), None, "one lookup still outstanding");
        assert_eq!(m.path_for(a).unwrap(), "/a");
        assert_eq!(m.forget(a, 1), Some("/a".to_string()));
        assert!(m.path_for(a).is_err());
        assert!(!m.path_to_inode.contains_key("/a"));
        assert!(m.refs.is_empty());
    }

    #[test]
    fn forget_more_than_count_saturates_and_unknown_inode_is_noop() {
        let m = InodeMap::new();
        let a = m.lookup_ref("/a");
        assert_eq!(m.forget(a, 99), Some("/a".to_string()));
        assert_eq!(m.forget(a, 1), None, "second forget is a no-op");
        assert_eq!(m.forget(12345, 1), None);
    }

    #[test]
    fn root_is_never_forgotten() {
        let m = InodeMap::new();
        assert_eq!(m.forget(FUSE_ROOT_ID, 1), None);
        assert_eq!(m.forget_quiet(FUSE_ROOT_ID, Duration::ZERO), None);
        assert_eq!(m.path_for(FUSE_ROOT_ID).unwrap(), "/");
    }

    #[test]
    fn forgetting_an_unlinked_inode_keeps_the_recreated_path() {
        let m = InodeMap::new();
        let old = m.lookup_ref("/f");
        m.remove("/f"); // unlink
        let new = m.lookup_ref("/f"); // recreate: new inode
        assert_ne!(old, new);
        m.forget(old, 1);
        assert_eq!(m.inode_for("/f"), new, "new file keeps its mapping");
        assert_eq!(m.path_for(new).unwrap(), "/f");
    }

    #[test]
    fn batch_forget_spares_recently_looked_up_inodes() {
        let m = InodeMap::new();
        let a = m.lookup_ref("/a");
        assert_eq!(m.forget_quiet(a, Duration::from_secs(3600)), None);
        assert_eq!(m.path_for(a).unwrap(), "/a");
        assert_eq!(m.forget_quiet(a, Duration::ZERO), Some("/a".to_string()));
        assert!(m.path_for(a).is_err());
    }

    #[test]
    fn snapshot_pages_survive_a_concurrent_delete() {
        let snaps = DirSnapshots::new();
        let fh = snaps.open(vec![de("a", false), de("b", false), de("c", false)]);
        let snap = snaps.get(fh).unwrap();
        // First page ended after "a" (cookie 1); "a" is deleted meanwhile.
        let names: Vec<_> = snapshot_page(&snap, 1).map(|(_, e)| e.name.clone()).collect();
        assert_eq!(names, vec!["b", "c"], "b must not be skipped");
        // A live re-list ([b, c]) paged from cookie 1 would have lost "b":
        let live = vec![de("b", false), de("c", false)];
        let live_names: Vec<_> = snapshot_page(&live, 1).map(|(_, e)| e.name.clone()).collect();
        assert_eq!(live_names, vec!["c"]);
        snaps.close(fh);
        assert!(snaps.get(fh).is_none());
        assert!(snaps.get(0).is_none(), "fh 0 means no snapshot");
    }

    #[test]
    fn snapshot_dedups_names_and_numbers_cookies_from_one() {
        let snaps = DirSnapshots::new();
        let fh = snaps.open(vec![de("a", false), de("a", true), de("b", false)]);
        let snap = snaps.get(fh).unwrap();
        let page: Vec<_> = snapshot_page(&snap, 0).map(|(c, e)| (c, e.name.clone())).collect();
        assert_eq!(page, vec![(1, "a".to_string()), (2, "b".to_string())]);
        assert_eq!(snapshot_page(&snap, 2).count(), 0);
    }

    #[test]
    fn readdirplus_attr_matches_getattr_rules() {
        let a = DtfsAttr {
            size: 0,
            is_directory: true,
            created_at: SystemTime::UNIX_EPOCH,
            modified_at: SystemTime::UNIX_EPOCH,
        };
        let d = build_attr(5, &a, 0o700);
        assert_eq!(d.nlink, 2, "directories report nlink 2");
        assert_eq!(d.perm, 0o700, "chmod override is honoured");
        let f = build_attr(6, &DtfsAttr { is_directory: false, ..a }, 0o600);
        assert_eq!((f.nlink, f.perm), (1, 0o600));
    }
}
