use anyhow::Result;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Generate a self-signed CA certificate and key for cluster TLS.
pub fn generate_cluster_ca(cluster_name: &str) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut params = rcgen::CertificateParams::new(vec![])
        .map_err(|e| anyhow::anyhow!("Failed to create cert params: {}", e))?;
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, format!("DTFS CA - {}", cluster_name));
    params
        .distinguished_name
        .push(rcgen::DnType::OrganizationName, "DTFS");

    let key_pair = rcgen::KeyPair::generate()
        .map_err(|e| anyhow::anyhow!("Failed to generate key pair: {}", e))?;

    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| anyhow::anyhow!("Failed to self-sign CA cert: {}", e))?;

    Ok((cert.pem().into_bytes(), key_pair.serialize_pem().into_bytes()))
}

/// Generate a server certificate signed by the cluster CA.
/// Takes the CA key pair and generates a new node cert signed by it.
pub fn generate_node_cert(
    ca_cert_pem: &[u8],
    ca_key_pem: &[u8],
    hostname: &str,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let ca_key_str = std::str::from_utf8(ca_key_pem)?;
    let ca_key = rcgen::KeyPair::from_pem(ca_key_str)
        .map_err(|e| anyhow::anyhow!("Failed to parse CA key: {}", e))?;

    // CF-6 — reconstruct the issuer from the REAL CA certificate. The
    // previous code rebuilt CA params from scratch with CN "DTFS CA",
    // which differs from the issued CA's CN ("DTFS CA - <cluster>"), so
    // the node cert's issuer DN never matched the trust anchor and chain
    // validation failed as soon as the certs were used for actual TLS.
    let ca_cert_str = std::str::from_utf8(ca_cert_pem)?;
    let ca_params = rcgen::CertificateParams::from_ca_cert_pem(ca_cert_str)
        .map_err(|e| anyhow::anyhow!("Failed to parse CA cert: {}", e))?;
    let ca_cert = ca_params
        .self_signed(&ca_key)
        .map_err(|e| anyhow::anyhow!("Failed to recreate CA cert: {}", e))?;

    // CF-6 — nodes are usually dialed by IP (registry grpc_addr), and
    // rustls verifies the dialed host against the SAN list: an IP-shaped
    // hostname must become an IP SAN, not a DNS SAN.
    let san = match hostname.parse::<std::net::IpAddr>() {
        Ok(ip) => rcgen::SanType::IpAddress(ip),
        Err(_) => rcgen::SanType::DnsName(
            hostname
                .try_into()
                .map_err(|e| anyhow::anyhow!("Invalid hostname for SAN: {}", e))?,
        ),
    };

    let mut node_params = rcgen::CertificateParams::new(vec![])
        .map_err(|e| anyhow::anyhow!("Failed to create node cert params: {}", e))?;
    node_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, hostname.to_string());
    node_params.subject_alt_names.push(san);

    let node_key = rcgen::KeyPair::generate()
        .map_err(|e| anyhow::anyhow!("Failed to generate node key pair: {}", e))?;

    let node_cert = node_params
        .signed_by(&node_key, &ca_cert, &ca_key)
        .map_err(|e| anyhow::anyhow!("Failed to sign node cert: {}", e))?;

    Ok((
        node_cert.pem().into_bytes(),
        node_key.serialize_pem().into_bytes(),
    ))
}

/// Constant-time byte-slice comparison to prevent timing attacks. Used for
/// HMAC verification (join handshake) and shared-secret checks (CO-2-2
/// forwarded-write auth). Compares content in constant time; a length
/// mismatch short-circuits (length is not itself secret here).
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut result = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        result |= x ^ y;
    }
    result == 0
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.is_ascii() {
        return None;
    }
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

// ---------------------------------------------------------------------------
// LIS-19 / LIS-55 / LIS-83 — data-plane RPC auth (mutate + read)
// ---------------------------------------------------------------------------

/// gRPC metadata key: unix-seconds timestamp for the data-RPC MAC.
pub const DATA_RPC_AUTH_TS: &str = "x-dtfs-rpc-ts";
/// gRPC metadata key: hex-encoded HMAC-SHA256 of the canonical message.
pub const DATA_RPC_AUTH_MAC: &str = "x-dtfs-rpc-mac";
/// Reject MACs whose timestamp is older/newer than this many seconds.
pub const DATA_RPC_MAX_SKEW_SECS: u64 = 300;

/// Canonical MAC message for a data-plane RPC.
///
/// `method` is `PutChunk`, `DeleteChunk`, `ReplicateChunk`, `GetChunk`,
/// `ListChunks`, or `VerifyChunk`. `binding` is:
/// - empty for PutChunk / ListChunks
/// - `chunk_id` for GetChunk / VerifyChunk
/// - [`data_rpc_delete_binding`] for DeleteChunk (`chunk_id|disk_id|if_mtime_le_ms`)
/// - [`data_rpc_replicate_binding`] for ReplicateChunk (`chunk_id|target_address`)
pub fn data_rpc_mac_message(method: &str, ts_unix: u64, binding: &str) -> String {
    format!("dtfs-data-rpc\nv1\n{method}\n{ts_unix}\n{binding}\n")
}

/// MAC binding for `ReplicateChunk` — pins both chunk and outbound target
/// so a stolen stamp cannot retarget SSRF within the skew window (LIS-55).
pub fn data_rpc_replicate_binding(chunk_id: &str, target_address: &str) -> String {
    format!("{chunk_id}|{target_address}")
}

/// MAC binding for `DeleteChunk` — pins chunk, target disk and the mtime
/// guard so a captured stamp cannot be replayed with `disk_id` stripped
/// (first-found copy, possibly the anchor) or the mtime guard removed.
pub fn data_rpc_delete_binding(
    chunk_id: &str,
    disk_id: Option<u32>,
    if_mtime_le_ms: Option<u64>,
) -> String {
    format!(
        "{chunk_id}|{}|{}",
        disk_id.map(|d| d.to_string()).unwrap_or_default(),
        if_mtime_le_ms.map(|m| m.to_string()).unwrap_or_default()
    )
}

/// Compute hex HMAC-SHA256 for a data-plane RPC auth stamp.
pub fn compute_data_rpc_mac_hex(
    secret: &str,
    method: &str,
    ts_unix: u64,
    binding: &str,
) -> String {
    let msg = data_rpc_mac_message(method, ts_unix, binding);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .expect("HMAC can take key of any size");
    mac.update(msg.as_bytes());
    hex_encode(&mac.finalize().into_bytes())
}

/// Stamp headers for an authenticated data-plane RPC.
///
/// Returns `None` when `secret` is empty (legacy plaintext cluster — callers
/// skip stamping; the server skips verification). Otherwise
/// `(ts_unix_string, mac_hex)`.
pub fn stamp_data_rpc_auth(
    secret: &str,
    method: &str,
    binding: &str,
) -> Option<(String, String)> {
    if secret.is_empty() {
        return None;
    }
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mac = compute_data_rpc_mac_hex(secret, method, ts, binding);
    Some((ts.to_string(), mac))
}

/// Verify data-plane RPC auth headers.
///
/// Empty `secret` → always true (legacy skip). Otherwise requires a parseable
/// timestamp within [`DATA_RPC_MAX_SKEW_SECS`] of `now_unix` and a matching
/// constant-time MAC.
pub fn verify_data_rpc_auth(
    secret: &str,
    method: &str,
    ts_str: &str,
    mac_hex: &str,
    binding: &str,
    now_unix: u64,
) -> bool {
    if secret.is_empty() {
        return true;
    }
    let Ok(ts) = ts_str.parse::<u64>() else {
        return false;
    };
    let skew = if now_unix >= ts {
        now_unix - ts
    } else {
        ts - now_unix
    };
    if skew > DATA_RPC_MAX_SKEW_SECS {
        return false;
    }
    let Some(provided) = hex_decode(mac_hex) else {
        return false;
    };
    let expected_hex = compute_data_rpc_mac_hex(secret, method, ts, binding);
    let Some(expected) = hex_decode(&expected_hex) else {
        return false;
    };
    constant_time_eq(&provided, &expected)
}

// ---------------------------------------------------------------------------
// LIS-56 / Sec HIGH — manager metadata mutating-RPC authentication
// ---------------------------------------------------------------------------
//
// Same header keys + skew as LIS-19 (`x-dtfs-rpc-ts` / `x-dtfs-rpc-mac`), but
// a distinct MAC plane (`dtfs-mgr-rpc`) so a data-plane stamp cannot be
// replayed against ManagerService (and vice versa).

/// Canonical MAC message for a manager metadata mutating RPC.
///
/// `method` is the ManagerService RPC name (`DeleteFile`, `CreateFile`, …).
/// `binding` is typically the target path (or `from\\nto` for RenameFile) so a
/// stamp cannot be replayed against a different object within the skew window.
pub fn mgr_rpc_mac_message(method: &str, ts_unix: u64, binding: &str) -> String {
    format!("dtfs-mgr-rpc\nv1\n{method}\n{ts_unix}\n{binding}\n")
}

/// Compute hex HMAC-SHA256 for a manager metadata RPC auth stamp.
pub fn compute_mgr_rpc_mac_hex(
    secret: &str,
    method: &str,
    ts_unix: u64,
    binding: &str,
) -> String {
    let msg = mgr_rpc_mac_message(method, ts_unix, binding);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .expect("HMAC can take key of any size");
    mac.update(msg.as_bytes());
    hex_encode(&mac.finalize().into_bytes())
}

/// Stamp headers for an authenticated manager metadata RPC.
///
/// Returns `None` when `secret` is empty (legacy plaintext cluster). Otherwise
/// `(ts_unix_string, mac_hex)`.
pub fn stamp_mgr_rpc_auth(
    secret: &str,
    method: &str,
    binding: &str,
) -> Option<(String, String)> {
    if secret.is_empty() {
        return None;
    }
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mac = compute_mgr_rpc_mac_hex(secret, method, ts, binding);
    Some((ts.to_string(), mac))
}

/// Verify manager metadata RPC auth headers.
///
/// Empty `secret` → always true (legacy skip). Otherwise requires a parseable
/// timestamp within [`DATA_RPC_MAX_SKEW_SECS`] of `now_unix` and a matching
/// constant-time MAC over the `dtfs-mgr-rpc` plane.
pub fn verify_mgr_rpc_auth(
    secret: &str,
    method: &str,
    ts_str: &str,
    mac_hex: &str,
    binding: &str,
    now_unix: u64,
) -> bool {
    if secret.is_empty() {
        return true;
    }
    let Ok(ts) = ts_str.parse::<u64>() else {
        return false;
    };
    let skew = if now_unix >= ts {
        now_unix - ts
    } else {
        ts - now_unix
    };
    if skew > DATA_RPC_MAX_SKEW_SECS {
        return false;
    }
    let Some(provided) = hex_decode(mac_hex) else {
        return false;
    };
    let expected_hex = compute_mgr_rpc_mac_hex(secret, method, ts, binding);
    let Some(expected) = hex_decode(&expected_hex) else {
        return false;
    };
    constant_time_eq(&provided, &expected)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hex_decode_non_ascii_returns_none() {
        // CMN-4: a non-ASCII MAC header must not panic on the byte-slice
        // indexing inside hex_decode (char boundaries != byte offsets).
        assert_eq!(hex_decode("\u{20ac}\u{20ac}"), None);
    }

    #[test]
    fn test_generate_ca() {
        let (cert, key) = generate_cluster_ca("test-cluster").unwrap();
        assert!(!cert.is_empty());
        assert!(!key.is_empty());
        assert!(std::str::from_utf8(&cert).unwrap().contains("BEGIN CERTIFICATE"));
        assert!(std::str::from_utf8(&key).unwrap().contains("BEGIN PRIVATE KEY"));
    }

    #[test]
    fn test_generate_node_cert() {
        let (ca_cert, ca_key) = generate_cluster_ca("test-cluster").unwrap();
        let (node_cert, node_key) = generate_node_cert(&ca_cert, &ca_key, "node-01.example.com").unwrap();
        assert!(!node_cert.is_empty());
        assert!(!node_key.is_empty());
    }

    #[test]
    fn test_generate_node_cert_ip_hostname() {
        // CF-6 — an IP-shaped hostname yields an IP SAN cert without error.
        let (ca_cert, ca_key) = generate_cluster_ca("test-cluster").unwrap();
        let (node_cert, node_key) = generate_node_cert(&ca_cert, &ca_key, "127.0.0.1").unwrap();
        assert!(!node_cert.is_empty());
        assert!(!node_key.is_empty());
    }

    #[test]
    fn data_rpc_mac_roundtrip_accepts_valid() {
        let secret = "cluster-secret";
        let method = "DeleteChunk";
        let binding = "aabbccdd";
        let ts = 1_700_000_000u64;
        let mac = compute_data_rpc_mac_hex(secret, method, ts, binding);
        assert!(verify_data_rpc_auth(
            secret,
            method,
            &ts.to_string(),
            &mac,
            binding,
            ts
        ));
    }

    #[test]
    fn data_rpc_mac_rejects_wrong_secret() {
        let ts = 1_700_000_000u64;
        let mac = compute_data_rpc_mac_hex("right", "DeleteChunk", ts, "cid");
        assert!(!verify_data_rpc_auth(
            "wrong",
            "DeleteChunk",
            &ts.to_string(),
            &mac,
            "cid",
            ts
        ));
    }

    #[test]
    fn data_rpc_mac_rejects_wrong_binding() {
        let secret = "s";
        let ts = 1_700_000_000u64;
        let mac = compute_data_rpc_mac_hex(secret, "DeleteChunk", ts, "cid-a");
        assert!(!verify_data_rpc_auth(
            secret,
            "DeleteChunk",
            &ts.to_string(),
            &mac,
            "cid-b",
            ts
        ));
    }

    #[test]
    fn data_rpc_mac_rejects_skew() {
        let secret = "s";
        let ts = 1_700_000_000u64;
        let mac = compute_data_rpc_mac_hex(secret, "PutChunk", ts, "");
        assert!(!verify_data_rpc_auth(
            secret,
            "PutChunk",
            &ts.to_string(),
            &mac,
            "",
            ts + DATA_RPC_MAX_SKEW_SECS + 1
        ));
    }

    #[test]
    fn data_rpc_mac_empty_secret_is_legacy_skip() {
        assert!(verify_data_rpc_auth("", "DeleteChunk", "", "", "x", 0));
        assert!(stamp_data_rpc_auth("", "PutChunk", "").is_none());
    }

    #[test]
    fn stamp_data_rpc_auth_produces_verifiable_pair() {
        let secret = "live-secret";
        let (ts, mac) = stamp_data_rpc_auth(secret, "PutChunk", "").unwrap();
        let now: u64 = ts.parse().unwrap();
        assert!(verify_data_rpc_auth(
            secret, "PutChunk", &ts, &mac, "", now
        ));
    }

    #[test]
    fn data_rpc_replicate_binding_pins_target() {
        let secret = "s";
        let ts = 1_700_000_000u64;
        let binding = data_rpc_replicate_binding("cid", "http://peer:9500");
        let mac = compute_data_rpc_mac_hex(secret, "ReplicateChunk", ts, &binding);
        assert!(verify_data_rpc_auth(
            secret,
            "ReplicateChunk",
            &ts.to_string(),
            &mac,
            &binding,
            ts
        ));
        let wrong = data_rpc_replicate_binding("cid", "http://evil:9500");
        assert!(!verify_data_rpc_auth(
            secret,
            "ReplicateChunk",
            &ts.to_string(),
            &mac,
            &wrong,
            ts
        ));
    }

    #[test]
    fn data_rpc_delete_binding_pins_disk_and_mtime() {
        assert_eq!(data_rpc_delete_binding("cid", Some(1), Some(42)), "cid|1|42");
        assert_eq!(data_rpc_delete_binding("cid", None, None), "cid||");
        // Never collides with the legacy chunk_id-only binding.
        assert_ne!(data_rpc_delete_binding("cid", None, None), "cid");
        assert_ne!(
            data_rpc_delete_binding("cid", Some(0), None),
            data_rpc_delete_binding("cid", None, Some(0))
        );
    }

    #[test]
    fn mgr_rpc_mac_roundtrip_accepts_valid() {
        let secret = "cluster-secret";
        let method = "DeleteFile";
        let binding = "/media/a.mkv";
        let ts = 1_700_000_000u64;
        let mac = compute_mgr_rpc_mac_hex(secret, method, ts, binding);
        assert!(verify_mgr_rpc_auth(
            secret,
            method,
            &ts.to_string(),
            &mac,
            binding,
            ts
        ));
    }

    #[test]
    fn mgr_rpc_mac_rejects_data_plane_replay() {
        // A valid data-plane MAC must not verify under the mgr plane.
        let secret = "s";
        let ts = 1_700_000_000u64;
        let mac = compute_data_rpc_mac_hex(secret, "DeleteChunk", ts, "cid");
        assert!(!verify_mgr_rpc_auth(
            secret,
            "DeleteFile",
            &ts.to_string(),
            &mac,
            "/x",
            ts
        ));
    }

    #[test]
    fn mgr_rpc_mac_empty_secret_is_legacy_skip() {
        assert!(verify_mgr_rpc_auth("", "DeleteFile", "", "", "/x", 0));
        assert!(stamp_mgr_rpc_auth("", "CreateFile", "/x").is_none());
    }

    #[test]
    fn stamp_mgr_rpc_auth_produces_verifiable_pair() {
        let secret = "s";
        let (ts, mac) = stamp_mgr_rpc_auth(secret, "CreateFile", "/y").unwrap();
        let now: u64 = ts.parse().unwrap();
        assert!(verify_mgr_rpc_auth(
            secret, "CreateFile", &ts, &mac, "/y", now
        ));
    }
}
