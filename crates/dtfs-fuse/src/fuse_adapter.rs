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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use fuse3::raw::prelude::*;
use fuse3::{Errno, Result as FuseResult, Timestamp};
use tracing::{debug, warn};

use crate::filesystem::{DtfsFilesystem, FileAttr as DtfsAttr, OpenFlags};

// Kernel TTLs for cached attributes and directory entries.
const ATTR_TTL: Duration = Duration::from_secs(1);
const ENTRY_TTL: Duration = Duration::from_secs(5);

/// Root inode in FUSE (always 1).
const FUSE_ROOT_ID: u64 = 1;

// ---------------------------------------------------------------------------
// Bidirectional inode ↔ path mapping
// ---------------------------------------------------------------------------

struct InodeMap {
    inode_to_path: DashMap<u64, String>,
    path_to_inode: DashMap<String, u64>,
    next: AtomicU64,
}

impl InodeMap {
    fn new() -> Self {
        let m = Self {
            inode_to_path: DashMap::new(),
            path_to_inode: DashMap::new(),
            next: AtomicU64::new(2),
        };
        m.inode_to_path.insert(FUSE_ROOT_ID, "/".to_string());
        m.path_to_inode.insert("/".to_string(), FUSE_ROOT_ID);
        m
    }

    /// Get or mint an inode for `path`.
    fn inode_for(&self, path: &str) -> u64 {
        if let Some(ino) = self.path_to_inode.get(path) {
            return *ino;
        }
        let ino = self.next.fetch_add(1, Ordering::Relaxed);
        self.path_to_inode.insert(path.to_string(), ino);
        self.inode_to_path.insert(ino, path.to_string());
        ino
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
}

impl DtfsFuse3Adapter {
    pub fn new(fs: DtfsFilesystem) -> Self {
        Self {
            fs: Arc::new(fs),
            inodes: InodeMap::new(),
        }
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
            perm: if a.is_directory { 0o755 } else { 0o644 },
            nlink: if a.is_directory { 2 } else { 1 },
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 4096,
        }
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
                let ino = self.inodes.inode_for(&path);
                Ok(ReplyEntry {
                    ttl: ENTRY_TTL,
                    attr: self.to_fuse_attr(ino, &attrs),
                    generation: 0,
                })
            }
            Err(_) => Err(Errno::from(libc::ENOENT)),
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
            Err(_) => Err(Errno::from(libc::ENOENT)),
        }
    }

    async fn setattr(
        &self,
        _req: Request,
        inode: u64,
        _fh: Option<u64>,
        _set_attr: SetAttr,
    ) -> FuseResult<ReplyAttr> {
        // Minimal: return current attributes.
        let path = self.inodes.path_for(inode)?;
        match self.fs.lookup(&path).await {
            Ok(attrs) => Ok(ReplyAttr {
                ttl: ATTR_TTL,
                attr: self.to_fuse_attr(inode, &attrs),
            }),
            Err(_) => Err(Errno::from(libc::ENOENT)),
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
        match self.fs.open(&path, open_flags).await {
            Ok(fh) => Ok(ReplyOpen { fh, flags: 0 }),
            Err(_) => Err(Errno::from(libc::ENOENT)),
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
            warn!("fuse3 release fh={}: {}", fh, e);
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
                warn!("fuse3 read fh={}: {}", fh, e);
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
                warn!("fuse3 write fh={}: {}", fh, e);
                Err(Errno::from(libc::EIO))
            }
        }
    }

    async fn flush(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        _lock_owner: u64,
    ) -> FuseResult<()> {
        debug!("fuse3:flush fh={}", fh);
        self.fs.flush(fh).await.map_err(|e| {
            warn!("fuse3 flush fh={}: {}", fh, e);
            Errno::from(libc::EIO)
        })
    }

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
                let ino = self.inodes.inode_for(&path);
                Ok(ReplyEntry {
                    ttl: ENTRY_TTL,
                    attr: self.synthetic_attr(ino, true, (mode & 0o777) as u16),
                    generation: 0,
                })
            }
            Err(e) => {
                warn!("fuse3 mkdir {}: {}", path, e);
                Err(Errno::from(libc::EIO))
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
        match self.fs.unlink(&path).await {
            Ok(()) => {
                self.inodes.remove(&path);
                Ok(())
            }
            Err(e) => {
                warn!("fuse3 unlink {}: {}", path, e);
                Err(Errno::from(libc::EIO))
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
            Err(e) => {
                warn!("fuse3 rmdir {}: {}", path, e);
                Err(Errno::from(libc::EIO))
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
                let ino = self.inodes.inode_for(&path);
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
                warn!("fuse3 create {}: {}", path, e);
                Err(Errno::from(libc::EIO))
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
            Ok(a) if a.is_directory => Ok(ReplyOpen { fh: 0, flags }),
            Ok(_) => Err(Errno::from(libc::ENOTDIR)),
            Err(_) => Err(Errno::from(libc::ENOENT)),
        }
    }

    async fn releasedir(
        &self,
        _req: Request,
        _inode: u64,
        _fh: u64,
        _flags: u32,
    ) -> FuseResult<()> {
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
        _fh: u64,
        offset: i64,
    ) -> FuseResult<ReplyDirectory<Self::DirEntryStream<'a>>> {
        let path = self.inodes.path_for(inode)?;
        debug!("fuse3:readdir {} (ino={}, offset={})", path, inode, offset);

        let entries = self
            .fs
            .readdir(&path)
            .await
            .map_err(|_| Errno::from(libc::ENOENT))?;

        let parent_ino = if inode == FUSE_ROOT_ID {
            FUSE_ROOT_ID
        } else {
            self.inodes.parent_inode(&path)
        };

        let mut items: Vec<FuseResult<DirectoryEntry>> = Vec::new();

        // Offsets: 0→return ".", 1→return "..", ≥2→return children at (offset-2).
        if offset == 0 {
            items.push(Ok(DirectoryEntry {
                inode,
                offset: 1,
                kind: FileType::Directory,
                name: ".".into(),
            }));
        }
        if offset <= 1 {
            items.push(Ok(DirectoryEntry {
                inode: parent_ino,
                offset: 2,
                kind: FileType::Directory,
                name: "..".into(),
            }));
        }

        for (idx, entry) in entries.iter().enumerate() {
            let entry_offset = (idx as i64) + 3;
            if entry_offset <= offset {
                continue;
            }
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
        = futures_util::stream::Iter<std::vec::IntoIter<FuseResult<DirectoryEntryPlus>>>
    where
        Self: 'a;

    async fn readdirplus<'a>(
        &'a self,
        _req: Request,
        inode: u64,
        _fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> FuseResult<ReplyDirectoryPlus<Self::DirEntryPlusStream<'a>>> {
        let path = self.inodes.path_for(inode)?;
        let entries = self
            .fs
            .readdir(&path)
            .await
            .map_err(|_| Errno::from(libc::ENOENT))?;

        let mut items: Vec<FuseResult<DirectoryEntryPlus>> = Vec::new();

        for (idx, entry) in entries.iter().enumerate() {
            let entry_offset = (idx as u64) + 1;
            if entry_offset <= offset {
                continue;
            }
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
            let attr = FileAttr {
                ino: child_ino,
                size: entry.size,
                blocks: (entry.size + 511) / 512,
                atime: to_timestamp(entry.modified_at),
                mtime: to_timestamp(entry.modified_at),
                ctime: to_timestamp(entry.created_at),
                kind,
                perm: if entry.is_directory { 0o755 } else { 0o644 },
                nlink: 1,
                uid: unsafe { libc::getuid() },
                gid: unsafe { libc::getgid() },
                rdev: 0,
                blksize: 4096,
            };
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

        Ok(ReplyDirectoryPlus {
            entries: futures_util::stream::iter(items),
        })
    }

    async fn statfs(&self, _req: Request, _inode: u64) -> FuseResult<ReplyStatFs> {
        // Pseudo statfs — a full implementation would query the manager for
        // actual cluster-wide capacity figures.
        Ok(ReplyStatFs {
            blocks: 1_000_000_000,
            bfree: 900_000_000,
            bavail: 900_000_000,
            files: 1_000_000,
            ffree: 999_900,
            bsize: 1024,
            namelen: 255,
            frsize: 1024,
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
