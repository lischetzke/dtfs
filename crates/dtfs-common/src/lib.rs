pub mod checksum;
pub mod config;
pub mod crypto;
pub mod error;
pub mod hash_ring;
pub mod memory;
pub mod reconfig;
pub mod replication;
pub mod tls;
pub mod topology;
pub mod types;
pub mod version;

/// AZ-4 — canonical repo URL. Single source of truth for every in-tree
/// link (docs, error messages, the updater default). The project moved
/// from GitHub (`lischetzke/dtfs`) to the self-hosted GitLab in 2026-09;
/// `dtfs-cli update` derives the GitLab API project path from this URL
/// (`updater::release_source`).
pub const REPO_URL: &str = "https://github.com/lischetzke/dtfs";

/// Deterministic `u64` id from a hostname (data nodes, manager Raft peers).
#[inline]
pub fn node_id_from_hostname(hostname: &str) -> u64 {
    let hash = xxhash_rust::xxh3::xxh3_64(hostname.as_bytes());
    if hash == 0 { 1 } else { hash }
}
