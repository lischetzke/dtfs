use std::path::PathBuf;

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

        Ok(Self { disks })
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
    /// space. The placement engine has already decided this node should
    /// hold a replica; we honour that decision rather than rejecting the
    /// write. Tier semantics still drive cluster-wide placement, just not
    /// per-disk on each node.
    pub fn select_disk_for_tier(&self, tier: DiskTier) -> Option<&ManagedDisk> {
        let exact = self
            .disks
            .iter()
            .filter(|d| d.tier == tier && d.status == DiskStatus::Healthy)
            .max_by_key(|d| d.total_bytes.saturating_sub(d.used_bytes));
        if exact.is_some() {
            return exact;
        }
        // Fallback: the placement engine picked this node for tier T but
        // we have no tier-T disk. Use any healthy disk so the chunk lands
        // rather than failing the whole replica set.
        self.disks
            .iter()
            .filter(|d| d.status == DiskStatus::Healthy)
            .max_by_key(|d| d.total_bytes.saturating_sub(d.used_bytes))
    }

    /// Get all managed disks.
    pub fn all_disks(&self) -> &[ManagedDisk] {
        &self.disks
    }

    /// Get disks of a specific tier.
    pub fn disks_for_tier(&self, tier: DiskTier) -> Vec<&ManagedDisk> {
        self.disks.iter().filter(|d| d.tier == tier).collect()
    }

    /// Mark a disk as failed.
    pub fn mark_disk_failed(&mut self, disk_id: u32) {
        if let Some(disk) = self.disks.iter_mut().find(|d| d.disk_id == disk_id) {
            disk.status = DiskStatus::Failed;
            tracing::warn!("Disk {} marked as FAILED: {}", disk_id, disk.mount_path.display());
        }
    }

    /// Mark a disk as draining (for removal or maintenance).
    pub fn mark_disk_draining(&mut self, disk_id: u32) {
        if let Some(disk) = self.disks.iter_mut().find(|d| d.disk_id == disk_id) {
            disk.status = DiskStatus::Draining;
            tracing::info!("Disk {} marked as DRAINING: {}", disk_id, disk.mount_path.display());
        }
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
