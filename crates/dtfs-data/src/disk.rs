use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use dtfs_common::config::DiskConfig;
use dtfs_common::types::{DiskStatus, DiskTier};

/// Information about a managed disk.
#[derive(Debug, Clone)]
pub struct ManagedDisk {
    pub disk_id: u32,
    pub mount_path: PathBuf,
    pub tier: DiskTier,
    pub status: DiskStatus,
    pub total_bytes: u64,
    pub used_bytes: u64,
}

/// Manages all disks on this data node.
pub struct DiskManager {
    disks: Vec<ManagedDisk>,
    /// Per-disk (by position = disk_id) failed flag, set on EIO/EROFS from a
    /// put or commit. Never cleared at runtime; a restart clears it.
    failed: Vec<AtomicBool>,
}

impl DiskManager {
    pub fn new(disk_configs: &[DiskConfig]) -> anyhow::Result<Self> {
        let mut disks = Vec::new();

        for (i, dc) in disk_configs.iter().enumerate() {
            // Create DTFS directory structure if it doesn't exist
            let dtfs_dir = dc.path.join(".dtfs");
            let chunks_dir = dc.path.join("chunks");
            let tmp_dir = dc.path.join("tmp");
            let wal_dir = dtfs_dir.join("wal");

            std::fs::create_dir_all(&dtfs_dir)?;
            std::fs::create_dir_all(&chunks_dir)?;
            std::fs::create_dir_all(&tmp_dir)?;
            std::fs::create_dir_all(&wal_dir)?;

            ensure_persisted_disk_id(&dc.path, i as u32)?;

            // Get disk capacity (best effort, default to 0 if can't determine)
            let (total, used) = get_disk_usage(&dc.path);

            disks.push(ManagedDisk {
                disk_id: i as u32,
                mount_path: dc.path.clone(),
                tier: dc.tier,
                status: DiskStatus::Healthy,
                total_bytes: total,
                used_bytes: used,
            });

            tracing::info!(
                "Disk {}: {} (tier={}, total={}GB, used={}GB)",
                i,
                dc.path.display(),
                dc.tier,
                total / (1024 * 1024 * 1024),
                used / (1024 * 1024 * 1024),
            );
        }

        let failed = disks.iter().map(|_| AtomicBool::new(false)).collect();
        Ok(Self { disks, failed })
    }

    fn is_failed(&self, d: &ManagedDisk) -> bool {
        self.failed.get(d.disk_id as usize).is_some_and(|f| f.load(Ordering::Relaxed))
    }

    /// Status as seen now: `Failed` once a write hit EIO/EROFS on the disk.
    pub fn disk_status(&self, d: &ManagedDisk) -> DiskStatus {
        if self.is_failed(d) { DiskStatus::Failed } else { d.status }
    }

    /// A put/commit on the disk mounted at `mount` failed with `e`. EIO and
    /// EROFS mark the disk Failed so `select_disk_for_tier` stops choosing
    /// it; other errors (ENOSPC, ...) do not. Returns true when this call
    /// flagged the disk.
    pub fn note_write_error(&self, mount: &Path, e: &std::io::Error) -> bool {
        // 5 = EIO, 30 = EROFS on Linux.
        if !matches!(e.raw_os_error(), Some(5) | Some(30)) {
            return false;
        }
        let Some(d) = self.disks.iter().find(|d| d.mount_path == mount) else {
            return false;
        };
        let Some(flag) = self.failed.get(d.disk_id as usize) else { return false };
        if flag.swap(true, Ordering::Relaxed) {
            return false;
        }
        tracing::error!(
            disk_id = d.disk_id, path = %d.mount_path.display(), error = %e,
            "data disk marked Failed after write I/O error; no new chunks placed on it until restart"
        );
        true
    }

    /// Number of managed disks.
    pub fn disk_count(&self) -> usize {
        self.disks.len()
    }

    /// Select a healthy disk of the given tier with the most free space.
    ///
    /// If this node has no disk of the requested tier (common in 3-DC
    /// topologies where DC-B/C nodes are warm-only and DC-A nodes are
    /// hot+cold), fall back to whichever healthy disk has the most free
    /// space.
    ///
    /// Bug AD — re-stat each candidate disk's free space at selection
    /// time. `used_bytes` is only set at startup via `DiskManager::new`,
    /// so once a disk fills up, manager view never updates and selection
    /// keeps picking the same full disk. Symptom: `data-primary01`'s
    /// cold-4 disk filled to 100% while cold-1/2/3 stayed at 2%. Every
    /// subsequent put_chunk failed with ENOSPC, FUSE unlinked the file,
    /// and the cluster appeared "broken" with no migrations running.
    /// `statvfs` is cheap (~1 µs); calling it 1–4 times per put_chunk
    /// is acceptable.
    pub fn select_disk_for_tier(&self, tier: DiskTier) -> Option<&ManagedDisk> {
        let live_free = |d: &ManagedDisk| -> u64 {
            let (total, used) = get_disk_usage(&d.mount_path);
            if total == 0 {
                // statvfs failed — fall back to cached value
                d.total_bytes.saturating_sub(d.used_bytes)
            } else {
                total.saturating_sub(used)
            }
        };

        let exact = self
            .disks
            .iter()
            .filter(|d| d.tier == tier && d.status == DiskStatus::Healthy && !self.is_failed(d))
            .max_by_key(|d| live_free(d));
        if exact.is_some() {
            return exact;
        }
        // Fallback: the placement engine picked this node for tier T but
        // we have no tier-T disk. Use any healthy disk so the chunk lands
        // rather than failing the whole replica set.
        self.disks
            .iter()
            .filter(|d| d.status == DiskStatus::Healthy && !self.is_failed(d))
            .max_by_key(|d| live_free(d))
    }

    /// Get all managed disks.
    pub fn all_disks(&self) -> &[ManagedDisk] {
        &self.disks
    }

    /// Bug AU — live (disk_id, total_bytes, used_bytes) snapshot per
    /// configured disk. Each call statvfs's every disk independently of
    /// the cached `ManagedDisk` record. The heartbeat loop pulls this
    /// once per tick and ships the result to the manager so the per-disk
    /// usage shown on the Nodes page (Bug AR) tracks reality instead of
    /// the at-boot snapshot.
    ///
    /// Cheap: a single statvfs() per disk = ~1µs each, called at the
    /// heartbeat interval (default 5s). statvfs failure → returns the
    /// cached values so the API stays populated rather than reporting 0.
    pub fn live_disk_usage(&self) -> Vec<(u32, u64, u64)> {
        self.disks
            .iter()
            .map(|d| {
                let (total, used) = get_disk_usage(&d.mount_path);
                if total == 0 {
                    // statvfs failed (umounted, permission denied, …);
                    // fall back to the cached boot-time values so the
                    // dashboard doesn't go to 0 on a transient glitch.
                    (d.disk_id, d.total_bytes, d.used_bytes)
                } else {
                    (d.disk_id, total, used)
                }
            })
            .collect()
    }

}

/// Replica identity is `(node_id, disk_id)` and `disk_id` is the position in
/// `[[disks]]`. Persist it in `<mount>/.dtfs-disk-id` on first start and
/// refuse to start when the disk now sits at a different position, since
/// the manager would attribute its chunks to the wrong disk (and the orphan
/// reaper would then delete the wrong copies).
fn ensure_persisted_disk_id(mount: &std::path::Path, disk_id: u32) -> anyhow::Result<()> {
    let file = mount.join(".dtfs-disk-id");
    match std::fs::read_to_string(&file) {
        Ok(s) => {
            let persisted: u32 = s.trim().parse().map_err(|_| {
                anyhow::anyhow!("{} is not a valid disk id: {:?}", file.display(), s.trim())
            })?;
            if persisted != disk_id {
                tracing::error!(
                    path = %mount.display(), persisted, configured = disk_id,
                    "disk position in [[disks]] changed; refusing to start"
                );
                anyhow::bail!(
                    "disk {} was disk_id {} but is now at [[disks]] position {}. Keep \
                     [[disks]] positions stable (do not remove or reorder entries); \
                     replace a drained disk in place instead",
                    mount.display(), persisted, disk_id
                );
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let tmp = mount.join(".dtfs-disk-id.tmp");
            std::fs::write(&tmp, format!("{disk_id}\n"))?;
            std::fs::File::open(&tmp)?.sync_all()?;
            std::fs::rename(&tmp, &file)?;
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(p: &std::path::Path) -> DiskConfig {
        DiskConfig { path: p.to_path_buf(), tier: DiskTier::Hot }
    }

    #[test]
    fn first_start_persists_position_and_same_position_restarts() {
        let a = tempfile::TempDir::new().unwrap();
        let b = tempfile::TempDir::new().unwrap();
        let cfgs = [cfg(a.path()), cfg(b.path())];
        let dm = DiskManager::new(&cfgs).unwrap();
        assert_eq!(dm.all_disks()[1].disk_id, 1);
        assert_eq!(std::fs::read_to_string(b.path().join(".dtfs-disk-id")).unwrap().trim(), "1");
        // Same positions again: fine.
        DiskManager::new(&cfgs).unwrap();
    }

    #[test]
    fn moved_position_is_refused_with_clear_error() {
        let a = tempfile::TempDir::new().unwrap();
        let b = tempfile::TempDir::new().unwrap();
        DiskManager::new(&[cfg(a.path()), cfg(b.path())]).unwrap();
        // Remove the first entry: b moves from position 1 to 0.
        let err = DiskManager::new(&[cfg(b.path())]).err().expect("must refuse");
        let msg = err.to_string();
        assert!(msg.contains("was disk_id 1 but is now at [[disks]] position 0"), "{msg}");
    }

    #[test]
    fn eio_or_erofs_marks_disk_failed_and_selection_skips_it() {
        let a = tempfile::TempDir::new().unwrap();
        let b = tempfile::TempDir::new().unwrap();
        let dm = DiskManager::new(&[cfg(a.path()), cfg(b.path())]).unwrap();
        let eio = std::io::Error::from_raw_os_error(5);
        // Same filesystem, equal free space: max_by_key picks the LAST disk,
        // so fail that one to prove the skip.
        assert!(dm.note_write_error(b.path(), &eio));
        assert!(!dm.note_write_error(b.path(), &eio), "flag (and error log) only once");
        assert_eq!(dm.disk_status(&dm.all_disks()[1]), DiskStatus::Failed);
        assert_eq!(dm.select_disk_for_tier(DiskTier::Hot).unwrap().disk_id, 0);
        assert!(dm.note_write_error(a.path(), &std::io::Error::from_raw_os_error(30)));
        assert!(dm.select_disk_for_tier(DiskTier::Hot).is_none(), "every disk failed");
        assert!(dm.select_disk_for_tier(DiskTier::Cold).is_none(), "fallback skips failed too");
    }

    #[test]
    fn enospc_and_other_errors_do_not_mark_disk_failed() {
        let a = tempfile::TempDir::new().unwrap();
        let dm = DiskManager::new(&[cfg(a.path())]).unwrap();
        for e in [
            std::io::Error::from_raw_os_error(28),
            std::io::Error::from(std::io::ErrorKind::NotFound),
        ] {
            assert!(!dm.note_write_error(a.path(), &e));
        }
        let other = tempfile::TempDir::new().unwrap();
        assert!(!dm.note_write_error(other.path(), &std::io::Error::from_raw_os_error(5)));
        assert_eq!(dm.disk_status(&dm.all_disks()[0]), DiskStatus::Healthy);
        assert_eq!(dm.select_disk_for_tier(DiskTier::Hot).unwrap().disk_id, 0);
    }

    #[test]
    fn unparseable_id_file_is_refused() {
        let a = tempfile::TempDir::new().unwrap();
        std::fs::write(a.path().join(".dtfs-disk-id"), "garbage").unwrap();
        assert!(DiskManager::new(&[cfg(a.path())]).is_err());
    }
}

/// Get disk usage for a path. Returns (total_bytes, used_bytes).
fn get_disk_usage(path: &PathBuf) -> (u64, u64) {
    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let c_path = match CString::new(path.as_os_str().as_bytes()) {
            Ok(p) => p,
            Err(_) => return (0, 0),
        };

        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };

        if rc == 0 {
            let block_size = stat.f_frsize as u64;
            let total = block_size * stat.f_blocks;
            let free  = block_size * stat.f_bfree;
            let used  = total.saturating_sub(free);
            return (total, used);
        }
    }

    // Non-Linux or statvfs failure: return zeros (best effort).
    let _ = path;
    (0, 0)
}
