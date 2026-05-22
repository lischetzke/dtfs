pub mod checksum;
pub mod config;
pub mod crypto;
pub mod error;
pub mod hash_ring;
pub mod reconfig;
pub mod topology;
pub mod types;
pub mod version;

/// Deterministic `u64` id from a hostname (data nodes, manager Raft peers).
#[inline]
pub fn node_id_from_hostname(hostname: &str) -> u64 {
    let hash = xxhash_rust::xxh3::xxh3_64(hostname.as_bytes());
    if hash == 0 { 1 } else { hash }
}
