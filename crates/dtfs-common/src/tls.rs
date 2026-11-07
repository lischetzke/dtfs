//! CF-6 — gRPC transport security (mTLS via the cluster CA).
//!
//! `TlsContext` resolves a `[tls]` config block ONCE at startup into either
//! plaintext (today's default) or full mutual TLS, and is then threaded to
//! every tonic server builder and client connect site. Both directions are
//! covered: servers present the node certificate and REQUIRE client
//! certificates signed by the cluster CA; clients verify the server against
//! the CA and present their own node certificate.
//!
//! Mode semantics (back-compat is load-bearing — a default config must keep
//! a mixed/plaintext cluster working unchanged):
//! * `disabled` / `disable` / `off` — plaintext, certificate paths ignored.
//! * `require` / `required` — `cert_file`, `key_file`, `ca_file` must all be
//!   configured and readable; any problem is a hard startup error.
//! * `auto` (default) — TLS only when all three files are configured AND
//!   present on disk. Configured-but-MISSING files stay plaintext with a
//!   startup warning (rollout-friendly: paths may be set before the certs
//!   land, § rollout step 2 in docs/src/security/tls.md — but note a node
//!   that loses its cert file restarts into plaintext and partitions from a
//!   TLS cluster; switch to `require` after the rollout to pin it).
//!   Present-but-unreadable files are a hard error.
//! * anything else — hard startup error (a typo must not silently run
//!   plaintext).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tonic::transport::{
    Certificate, Channel, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig,
};

use crate::config::TlsConfig;

/// Default TCP/TLS connect budget for [`TlsContext::connect`].
///
/// Matches the Raft peer channel (Bug AC): a blackhole / unroutable peer
/// must fail fast so callers can walk the next endpoint. Bug BR-1's manager
/// self-register keepalive hung forever on dead `initial_voters` entries
/// when this was unbounded (LIS-77 / DC1 sole-voter restart).
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Resolved per-process TLS material. Cheap to clone — the PEM blobs sit
/// behind one `Arc` (the context is cloned per component at startup and per
/// cached channel in dtfs-client).
#[derive(Clone, Default)]
pub struct TlsContext {
    material: Option<Arc<Material>>,
}

struct Material {
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
    ca_pem: Vec<u8>,
}

impl std::fmt::Debug for TlsContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never debug-print key material.
        f.debug_struct("TlsContext")
            .field("enabled", &self.is_enabled())
            .finish()
    }
}

impl TlsContext {
    /// A context that always speaks plaintext.
    pub fn plaintext() -> Self {
        Self { material: None }
    }

    /// Resolve a `[tls]` block (see module docs for the mode semantics).
    pub fn from_config(cfg: &TlsConfig) -> Result<Self> {
        let mode = cfg.mode.trim().to_ascii_lowercase();
        match mode.as_str() {
            "disabled" | "disable" | "off" => return Ok(Self::plaintext()),
            "require" | "required" => {
                let (cert, key, ca) = match (&cfg.cert_file, &cfg.key_file, &cfg.ca_file) {
                    (Some(c), Some(k), Some(a)) => (c, k, a),
                    _ => anyhow::bail!(
                        "[tls] mode = \"require\" needs cert_file, key_file and ca_file all set"
                    ),
                };
                return Ok(Self {
                    material: Some(Arc::new(Material::load(cert, key, ca)?)),
                });
            }
            "auto" => {}
            other => anyhow::bail!(
                "unknown [tls] mode {other:?} (expected auto|require|disabled)"
            ),
        }
        // auto
        match (&cfg.cert_file, &cfg.key_file, &cfg.ca_file) {
            (None, None, None) => Ok(Self::plaintext()),
            (Some(cert), Some(key), Some(ca)) => {
                let missing: Vec<String> = [cert, key, ca]
                    .into_iter()
                    .filter(|p| !p.exists())
                    .map(|p| p.display().to_string())
                    .collect();
                if missing.is_empty() {
                    Ok(Self {
                        material: Some(Arc::new(Material::load(cert, key, ca)?)),
                    })
                } else {
                    tracing::warn!(
                        missing = ?missing,
                        "[tls] mode auto: certificate files not present on disk; staying PLAINTEXT"
                    );
                    Ok(Self::plaintext())
                }
            }
            _ => anyhow::bail!(
                "[tls] is partially configured; set all of cert_file, key_file, ca_file (or none)"
            ),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.material.is_some()
    }

    /// One startup log line describing the TLS decision for this process.
    pub fn log_startup(&self, component: &str, mode: &str) {
        let m = mode.trim().to_ascii_lowercase();
        if self.is_enabled() {
            tracing::info!(
                component,
                mode = %m,
                "gRPC TLS enabled: mutual TLS, peers verified against the cluster CA"
            );
        } else if matches!(m.as_str(), "disabled" | "disable" | "off") {
            tracing::info!(component, "gRPC TLS disabled by config; plaintext");
        } else {
            tracing::warn!(
                component,
                mode = %m,
                "gRPC runs in PLAINTEXT (no certificates configured); set [tls] \
                 cert_file/key_file/ca_file on every node to enable mTLS"
            );
        }
    }

    /// Server-side tonic config: present the node certificate and require
    /// client certificates signed by the cluster CA. `None` = plaintext.
    pub fn server_tls(&self) -> Option<ServerTlsConfig> {
        self.material.as_ref().map(|m| {
            ServerTlsConfig::new()
                .identity(Identity::from_pem(&m.cert_pem, &m.key_pem))
                .client_ca_root(Certificate::from_pem(&m.ca_pem))
        })
    }

    /// Client-side tonic config: verify the server against the cluster CA
    /// and present the node certificate for the server's client-auth check.
    pub fn client_tls(&self) -> Option<ClientTlsConfig> {
        self.material.as_ref().map(|m| {
            ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(&m.ca_pem))
                .identity(Identity::from_pem(&m.cert_pem, &m.key_pem))
        })
    }

    /// Scheme-aware endpoint URL for a stored node address. Accepts a bare
    /// `host:port` as well as `http://`/`https://` forms (registry
    /// `grpc_addr` carries a scheme since Bug W) and rewrites the scheme to
    /// match THIS process's TLS state.
    pub fn endpoint_url(&self, addr: &str) -> String {
        let bare = addr
            .strip_prefix("http://")
            .or_else(|| addr.strip_prefix("https://"))
            .unwrap_or(addr);
        let scheme = if self.is_enabled() { "https" } else { "http" };
        format!("{scheme}://{bare}")
    }

    /// tonic `Endpoint` for `addr` with the scheme and (when enabled) the
    /// client TLS config applied. Callers chain their own timeouts before
    /// `.connect()`.
    pub fn endpoint(&self, addr: &str) -> Result<Endpoint> {
        let url = self.endpoint_url(addr);
        let mut ep = Endpoint::from_shared(url.clone())
            .with_context(|| format!("invalid endpoint {url}"))?;
        if let Some(tls) = self.client_tls() {
            ep = ep
                .tls_config(tls)
                .with_context(|| format!("building TLS config for {url}"))?;
        }
        ep = apply_keepalive(ep);
        Ok(ep)
    }

    /// Connect a channel to `addr` (scheme + TLS handled here).
    ///
    /// Applies [`CONNECT_TIMEOUT`] so unreachable peers fail fast. Callers
    /// that need a different budget should use [`Self::endpoint`] and chain
    /// their own `.connect_timeout(...)` before `.connect()` (Raft does).
    pub async fn connect(&self, addr: &str) -> Result<Channel> {
        let url = self.endpoint_url(addr);
        self.endpoint(addr)?
            .connect_timeout(CONNECT_TIMEOUT)
            .connect()
            .await
            .with_context(|| format!("connect to {url}"))
    }
}

/// DATA-6 — every peer channel gets an HTTP/2 keepalive so a half-open
/// TCP connection (peer crashed/rebooted without a clean FIN, e.g. a
/// manager behind a NAT/firewall) is detected and torn down instead of
/// silently absorbing every RPC until the OS-level TCP timeout (minutes).
/// `keep_alive_while_idle(true)` so the ping fires even when this
/// connection has no in-flight request (control-plane channels like the
/// data-node heartbeat loop are idle most of the time between ticks).
fn apply_keepalive(ep: Endpoint) -> Endpoint {
    ep.http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(5))
        .keep_alive_while_idle(true)
}

impl Material {
    fn load(cert: &PathBuf, key: &PathBuf, ca: &PathBuf) -> Result<Self> {
        let read = |p: &PathBuf, what: &str| {
            std::fs::read(p).with_context(|| format!("[tls] reading {what} {}", p.display()))
        };
        Ok(Self {
            cert_pem: read(cert, "cert_file")?,
            key_pem: read(key, "key_file")?,
            ca_pem: read(ca, "ca_file")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_6_apply_keepalive_does_not_panic_and_returns_usable_endpoint() {
        // DATA-6 — tonic's `Endpoint` exposes no getters for keepalive
        // settings, so this is a builder-smoke test (doesn't panic, and
        // the endpoint can still build a channel-less client). The actual
        // values (10s interval / 5s timeout / while_idle) are verified by
        // inspection of `apply_keepalive` and exercised end-to-end by the
        // existing `tls_loopback` integration tests, which route through
        // `TlsContext::connect` -> `endpoint()` -> `apply_keepalive`.
        let ep = Endpoint::from_shared("http://127.0.0.1:1".to_string()).unwrap();
        let _ep = apply_keepalive(ep);
    }

    fn cfg(mode: &str, dir: Option<&std::path::Path>) -> TlsConfig {
        TlsConfig {
            mode: mode.to_string(),
            cert_file: dir.map(|d| d.join("node.crt")),
            key_file: dir.map(|d| d.join("node.key")),
            ca_file: dir.map(|d| d.join("ca.crt")),
        }
    }

    /// Write a real CA + node cert into `dir` as node.crt/node.key/ca.crt.
    fn write_material(dir: &std::path::Path) {
        let (ca_cert, ca_key) = crate::crypto::generate_cluster_ca("t").unwrap();
        let (cert, key) = crate::crypto::generate_node_cert(&ca_cert, &ca_key, "127.0.0.1").unwrap();
        std::fs::write(dir.join("node.crt"), cert).unwrap();
        std::fs::write(dir.join("node.key"), key).unwrap();
        std::fs::write(dir.join("ca.crt"), ca_cert).unwrap();
    }

    #[test]
    fn default_config_is_plaintext() {
        let ctx = TlsContext::from_config(&TlsConfig::default()).unwrap();
        assert!(!ctx.is_enabled());
        assert!(ctx.server_tls().is_none());
        assert!(ctx.client_tls().is_none());
    }

    #[test]
    fn disabled_ignores_certificates() {
        let dir = tempfile::tempdir().unwrap();
        write_material(dir.path());
        let ctx = TlsContext::from_config(&cfg("disabled", Some(dir.path()))).unwrap();
        assert!(!ctx.is_enabled());
        // legacy spelling
        let ctx = TlsContext::from_config(&cfg("off", Some(dir.path()))).unwrap();
        assert!(!ctx.is_enabled());
    }

    #[test]
    fn auto_enables_when_files_exist() {
        let dir = tempfile::tempdir().unwrap();
        write_material(dir.path());
        let ctx = TlsContext::from_config(&cfg("auto", Some(dir.path()))).unwrap();
        assert!(ctx.is_enabled());
        assert!(ctx.server_tls().is_some());
        assert!(ctx.client_tls().is_some());
    }

    #[test]
    fn auto_falls_back_to_plaintext_when_files_missing() {
        let dir = tempfile::tempdir().unwrap();
        // Paths configured but nothing on disk → plaintext (back-compat).
        let ctx = TlsContext::from_config(&cfg("auto", Some(dir.path()))).unwrap();
        assert!(!ctx.is_enabled());
    }

    #[test]
    fn auto_partial_config_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        write_material(dir.path());
        let mut c = cfg("auto", Some(dir.path()));
        c.ca_file = None;
        assert!(TlsContext::from_config(&c).is_err());
    }

    #[test]
    fn require_errors_without_files() {
        // Not configured at all.
        assert!(TlsContext::from_config(&cfg("require", None)).is_err());
        // Configured but missing on disk.
        let dir = tempfile::tempdir().unwrap();
        assert!(TlsContext::from_config(&cfg("require", Some(dir.path()))).is_err());
    }

    #[test]
    fn require_enables_with_files() {
        let dir = tempfile::tempdir().unwrap();
        write_material(dir.path());
        let ctx = TlsContext::from_config(&cfg("require", Some(dir.path()))).unwrap();
        assert!(ctx.is_enabled());
    }

    #[test]
    fn unknown_mode_is_an_error() {
        let err = TlsContext::from_config(&cfg("frobnicate", None)).unwrap_err();
        assert!(err.to_string().contains("unknown [tls] mode"), "{err}");
    }

    #[test]
    fn endpoint_url_rewrites_scheme() {
        let plain = TlsContext::plaintext();
        assert_eq!(plain.endpoint_url("10.0.0.1:9500"), "http://10.0.0.1:9500");
        assert_eq!(plain.endpoint_url("http://10.0.0.1:9500"), "http://10.0.0.1:9500");
        // A stored https addr is rewritten down when this process has no TLS.
        assert_eq!(plain.endpoint_url("https://10.0.0.1:9500"), "http://10.0.0.1:9500");

        let dir = tempfile::tempdir().unwrap();
        write_material(dir.path());
        let tls = TlsContext::from_config(&cfg("auto", Some(dir.path()))).unwrap();
        assert_eq!(tls.endpoint_url("10.0.0.1:9500"), "https://10.0.0.1:9500");
        assert_eq!(tls.endpoint_url("http://10.0.0.1:9500"), "https://10.0.0.1:9500");
        assert_eq!(tls.endpoint_url("https://host:9500"), "https://host:9500");
    }

    /// LIS-77 / BR-1 — connecting to an unroutable TEST-NET address must
    /// fail within a small multiple of CONNECT_TIMEOUT, not hang for the
    /// OS TCP blackhole (often minutes).
    #[tokio::test]
    async fn connect_to_blackhole_fails_within_bound() {
        use std::time::Instant;

        let ctx = TlsContext::plaintext();
        let start = Instant::now();
        let result = ctx.connect("192.0.2.1:9401").await;
        let elapsed = start.elapsed();
        assert!(result.is_err(), "blackhole connect should fail, got Ok");
        assert!(
            elapsed < Duration::from_secs(8),
            "connect hung longer than CONNECT_TIMEOUT slack: {elapsed:?}"
        );
        assert!(
            elapsed >= Duration::from_millis(50),
            "expected a real connect attempt, elapsed={elapsed:?}"
        );
    }
}
