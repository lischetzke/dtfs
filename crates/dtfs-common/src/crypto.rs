use anyhow::Result;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Generate an HMAC-SHA256 of a shared secret with a nonce for cluster join authentication.
pub fn compute_join_hmac(secret: &str, nonce: &str) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .expect("HMAC can take key of any size");
    mac.update(nonce.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

/// Verify an HMAC-SHA256 for cluster join authentication.
pub fn verify_join_hmac(secret: &str, nonce: &str, expected_hmac: &[u8]) -> bool {
    let computed = compute_join_hmac(secret, nonce);
    constant_time_eq(&computed, expected_hmac)
}

/// Generate a random nonce string.
pub fn generate_nonce() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let bytes: Vec<u8> = (0..32).map(|_| rng.r#gen()).collect();
    hex_encode(&bytes)
}

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

/// Constant-time comparison to prevent timing attacks.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hmac_roundtrip() {
        let secret = "my-cluster-secret";
        let nonce = generate_nonce();
        let hmac = compute_join_hmac(secret, &nonce);
        assert!(verify_join_hmac(secret, &nonce, &hmac));
        assert!(!verify_join_hmac("wrong-secret", &nonce, &hmac));
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
    fn test_nonce_uniqueness() {
        let n1 = generate_nonce();
        let n2 = generate_nonce();
        assert_ne!(n1, n2);
    }
}
