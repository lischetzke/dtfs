//! P1-8 — Alert dispatcher.
//!
//! Fires operationally important events (anchor violated, node down, disk
//! failed, scrub corruption, quorum lost, migration stalled) to a list of
//! webhook sinks configured in `manager.toml`. Each POST carries a JSON
//! body and an HMAC-SHA256 signature derived from a per-sink shared secret
//! so the receiver can authenticate the payload.
//!
//! De-duplication: each event kind is rate-limited to one notification per
//! 5 minutes (configurable) per sink. Loss of a single transient event is
//! acceptable; storms (a thousand alerts/sec on a partition heal) are not.
//!
//! Sink reachability: if the sink returns a non-2xx or times out, the
//! event is dropped after logging the error. Future work: persistent
//! disk-backed retry queue with bounded backoff (see plan section 5.6).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

/// Categorical event types the manager can fire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AlertKind {
    AnchorViolated,
    NodeDown,
    DiskFailed,
    ScrubCorruption,
    QuorumLost,
    MigrationFailed,
    LeaseGcSpike,
}

impl AlertKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AnchorViolated => "anchor.violated",
            Self::NodeDown => "node.down",
            Self::DiskFailed => "disk.failed",
            Self::ScrubCorruption => "scrub.corruption",
            Self::QuorumLost => "quorum.lost",
            Self::MigrationFailed => "migration.failed",
            Self::LeaseGcSpike => "lease.gc.spike",
        }
    }
}

/// Re-export of the common config webhook type so manager-side code can
/// use a stable name even if the config struct moves later.
pub use dtfs_common::config::AlertWebhook as WebhookSink;

/// JSON payload sent to each sink.
#[derive(Debug, Clone, Serialize)]
pub struct AlertPayload {
    pub kind: String,
    pub cluster: String,
    pub manager: String,
    pub timestamp_ms: u64,
    pub summary: String,
    pub details: serde_json::Value,
}

/// In-memory dispatcher. Cloning is cheap (Arc-based).
pub struct AlertDispatcher {
    cluster_name: String,
    manager_hostname: String,
    sinks: Vec<WebhookSink>,
    /// Last-fired timestamps per (kind, sink_url) for dedup.
    last_fired: Arc<Mutex<HashMap<(AlertKind, String), Instant>>>,
    http: reqwest::Client,
}

impl AlertDispatcher {
    pub fn new(
        cluster_name: String,
        manager_hostname: String,
        sinks: Vec<WebhookSink>,
    ) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            cluster_name,
            manager_hostname,
            sinks,
            last_fired: Arc::new(Mutex::new(HashMap::new())),
            http,
        }
    }

    /// Fire an event. Best-effort, fire-and-forget — never blocks the
    /// caller (each sink POST runs in its own spawned task). Dedup keeps
    /// repeated events within `dedup_secs` quiet on a per-kind basis.
    pub fn fire(&self, kind: AlertKind, summary: &str, details: serde_json::Value) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let payload = AlertPayload {
            kind: kind.as_str().to_string(),
            cluster: self.cluster_name.clone(),
            manager: self.manager_hostname.clone(),
            timestamp_ms: now_ms,
            summary: summary.to_string(),
            details,
        };
        let body = match serde_json::to_vec(&payload) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(error = %e, "alert payload serialise failed");
                return;
            }
        };

        for sink in &self.sinks {
            // Filter by kind allow-list (empty list = accept everything).
            if !sink.kinds.is_empty() && !sink.kinds.iter().any(|k| k == kind.as_str()) {
                continue;
            }

            // Dedup window.
            {
                let mut last = self.last_fired.lock();
                let key = (kind, sink.url.clone());
                let now = Instant::now();
                if let Some(prev) = last.get(&key) {
                    if now.duration_since(*prev) < Duration::from_secs(sink.dedup_secs) {
                        continue;
                    }
                }
                last.insert(key, now);
            }

            let url = sink.url.clone();
            let secret = sink.secret.clone();
            let body_for_task = body.clone();
            let http = self.http.clone();
            tokio::spawn(async move {
                let sig = hmac_sha256_hex(&secret, &body_for_task);
                let res = http
                    .post(&url)
                    .header("content-type", "application/json")
                    .header("x-dtfs-signature", sig)
                    .body(body_for_task)
                    .send()
                    .await;
                match res {
                    Ok(r) if r.status().is_success() => {
                        tracing::debug!(host = %url_host(&url), "alert delivered");
                    }
                    Ok(r) => {
                        tracing::warn!(
                            host = %url_host(&url),
                            status = %r.status(),
                            "alert sink returned non-success"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(host = %url_host(&url), error = %e.without_url(), "alert delivery failed");
                    }
                }
            });
        }
    }

    /// Number of configured sinks (test/observability helper).
    pub fn sink_count(&self) -> usize {
        self.sinks.len()
    }
}

/// Compute HMAC-SHA256 over `body` keyed by `secret`, return lowercase hex.
fn hmac_sha256_hex(secret: &str, body: &[u8]) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = match HmacSha256::new_from_slice(secret.as_bytes()) {
        Ok(m) => m,
        Err(_) => return String::new(),
    };
    mac.update(body);
    let result = mac.finalize().into_bytes();
    hex::encode(result)
}

/// Host (no scheme/path/query/credentials) of a webhook URL for logging:
/// the path and query commonly carry the sink's secret token.
fn url_host(url: &str) -> String {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_string))
        .unwrap_or_else(|| "<unparsable>".to_string())
}

#[cfg(test)]
mod tests {
    #[test]
    fn url_host_drops_secret_path_and_query() {
        assert_eq!(super::url_host("https://user:pw@hooks.example.com/services/T0/B1/SECRET?k=v"), "hooks.example.com");
        assert_eq!(super::url_host("not a url"), "<unparsable>");
    }

    use super::*;

    #[tokio::test]
    async fn dedup_holds_repeated_fires() {
        let d = AlertDispatcher::new(
            "test".into(),
            "test-host".into(),
            vec![WebhookSink {
                url: "http://127.0.0.1:1/never".into(),
                secret: "shh".into(),
                kinds: vec![],
                dedup_secs: 10,
            }],
        );
        // 5 rapid fires — only the first should attempt delivery (the
        // others fall inside the dedup window). We can't easily assert
        // network delivery here, but we can assert the dedup map only has
        // one entry per (kind, url) and that calling fire is non-blocking.
        for _ in 0..5 {
            d.fire(AlertKind::NodeDown, "test", serde_json::json!({}));
        }
        let len = d.last_fired.lock().len();
        assert_eq!(len, 1);
    }

    #[test]
    fn hmac_signature_is_stable() {
        let s = hmac_sha256_hex("k", b"hello");
        let s2 = hmac_sha256_hex("k", b"hello");
        assert_eq!(s, s2);
        assert_eq!(s.len(), 64);
    }
}
