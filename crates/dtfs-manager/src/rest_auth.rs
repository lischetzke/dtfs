//! LIS-18 / Sec R1 — shared-secret gate for mutating manager REST.
//!
//! When `[cluster].shared_secret` is non-empty, every state-mutating
//! REST call under `/api/v1/*` must present that secret via
//! `X-Dtfs-Shared-Secret` or `Authorization: Bearer <secret>`.
//! Fail closed: missing → 401, mismatch → 403.
//!
//! Empty `shared_secret` keeps the legacy open path (same contract as
//! RegisterNode on master). LIS-20 tightens empty-secret startup; when
//! that lands, empty + no escape hatch never reaches this middleware.
//!
//! Exempt (own auth or non-mutating):
//! - GET / HEAD / OPTIONS
//! - `/healthz`, `/readyz`, `/metrics` (probes / scrapers)

use axum::{
    extract::{Request, State},
    http::{header, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::sync::Arc;

use crate::api::AppState;

/// One `auth.denied` audit row per (source, route template) per window; the rest are
/// counted and reported on the next row. Without this an unauthenticated
/// flood costs a blocking disk append + a Raft AppendAudit per request and
/// evicts real audit history from the capped ring.
pub struct DenyCoalescer {
    window_ms: u64,
    max_keys: usize,
    /// (source, path) -> (window start ms, suppressed since)
    seen: parking_lot::Mutex<std::collections::HashMap<(String, String), (u64, u64)>>,
}

impl DenyCoalescer {
    pub fn new(window_ms: u64, max_keys: usize) -> Self {
        Self { window_ms, max_keys, seen: Default::default() }
    }

    /// `Some(n)`: write a row now, `n` earlier denials were suppressed.
    /// `None`: coalesced into the open window (or table full of live keys).
    pub fn admit(&self, src: &str, path: &str, now_ms: u64) -> Option<u64> {
        let mut seen = self.seen.lock();
        let key = (src.to_string(), path.to_string());
        if let Some((start, suppressed)) = seen.get_mut(&key) {
            if now_ms.saturating_sub(*start) < self.window_ms {
                *suppressed += 1;
                return None;
            }
            let n = *suppressed;
            *start = now_ms;
            *suppressed = 0;
            return Some(n);
        }
        if seen.len() >= self.max_keys {
            let w = self.window_ms;
            seen.retain(|_, (start, _)| now_ms.saturating_sub(*start) < w);
            if seen.len() >= self.max_keys {
                return None; // flood of distinct keys: drop rather than grow
            }
        }
        seen.insert(key, (now_ms, 0));
        Some(0)
    }
}

static DENY_COALESCER: std::sync::OnceLock<DenyCoalescer> = std::sync::OnceLock::new();

/// Header operators / CLI / release suite send on mutating calls.
pub const SHARED_SECRET_HEADER: &str = "x-dtfs-shared-secret";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationAuthError {
    Missing,
    Mismatch,
}

impl MutationAuthError {
    pub fn status(self) -> StatusCode {
        match self {
            Self::Missing => StatusCode::UNAUTHORIZED,
            Self::Mismatch => StatusCode::FORBIDDEN,
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::Missing => {
                "shared_secret required (X-Dtfs-Shared-Secret or Authorization: Bearer)"
            }
            Self::Mismatch => "shared_secret mismatch",
        }
    }
}

/// Pure check used by the middleware and unit tests.
pub fn check_mutation_auth(
    expected_secret: &str,
    headers: &axum::http::HeaderMap,
) -> Result<(), MutationAuthError> {
    if expected_secret.is_empty() {
        return Ok(());
    }
    let provided = extract_provided_secret(headers);
    match provided {
        None => Err(MutationAuthError::Missing),
        Some(p)
            if dtfs_common::crypto::constant_time_eq(p.as_bytes(), expected_secret.as_bytes()) =>
        {
            Ok(())
        }
        Some(_) => Err(MutationAuthError::Mismatch),
    }
}

fn extract_provided_secret(headers: &axum::http::HeaderMap) -> Option<String> {
    if let Some(v) = headers
        .get(SHARED_SECRET_HEADER)
        .and_then(|v| v.to_str().ok())
    {
        let t = v.trim();
        if !t.is_empty() {
            return Some(t.to_string());
        }
    }
    if let Some(v) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        let t = v.trim();
        if let Some(rest) = t
            .strip_prefix("Bearer ")
            .or_else(|| t.strip_prefix("bearer "))
        {
            let secret = rest.trim();
            if !secret.is_empty() {
                return Some(secret.to_string());
            }
        }
    }
    None
}

/// Paths that must not go through the shared-secret mutation gate.
pub fn mutation_auth_exempt(method: &Method, path: &str) -> bool {
    if matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS) {
        return true;
    }
    if path == "/healthz" || path == "/readyz" || path == "/metrics" {
        return true;
    }
    false
}

/// Axum middleware: reject unauthenticated mutating `/api/v1` calls.
pub async fn require_mutation_auth(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    // Coalesce denials per route template (#127): raw paths carry free
    // parameters, so keying on them lets random ids flood the audit ring.
    let route = req
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map_or_else(|| path.clone(), |m| m.as_str().to_string());
    let peer = req
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|c| c.0);

    if mutation_auth_exempt(&method, &path) {
        return next.run(req).await;
    }

    if !path.starts_with("/api/v1/") {
        return next.run(req).await;
    }

    match check_mutation_auth(&state.config.cluster.shared_secret, req.headers()) {
        Ok(()) => next.run(req).await,
        Err(err) => {
            let src = peer
                .map(|a| a.ip().to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let co = DENY_COALESCER.get_or_init(|| DenyCoalescer::new(60_000, 1024));
            if let Some(suppressed) = co.admit(&src, &route, now_ms) {
                state.audit_log.log(
                    "api",
                    "auth.denied",
                    &format!("method={method} path={path} src={src} suppressed={suppressed}"),
                    err.message(),
                );
            }
            (
                err.status(),
                Json(json!({
                    "error": err.message(),
                })),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue, Method};

    fn headers_with(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    #[test]
    fn deny_coalescer_one_row_per_source_path_per_window() {
        let c = DenyCoalescer::new(60_000, 4);
        assert_eq!(c.admit("1.1.1.1", "/api/v1/scrub", 0), Some(0));
        assert_eq!(c.admit("1.1.1.1", "/api/v1/scrub", 10), None);
        assert_eq!(c.admit("1.1.1.1", "/api/v1/scrub", 59_999), None);
        // Different source or path is its own key.
        assert_eq!(c.admit("2.2.2.2", "/api/v1/scrub", 20), Some(0));
        assert_eq!(c.admit("1.1.1.1", "/api/v1/config", 20), Some(0));
        // Window elapsed: row again, carrying the suppressed count.
        assert_eq!(c.admit("1.1.1.1", "/api/v1/scrub", 60_000), Some(2));
        assert_eq!(c.admit("1.1.1.1", "/api/v1/scrub", 60_001), None);
    }

    #[test]
    fn deny_coalescer_table_is_bounded() {
        let c = DenyCoalescer::new(60_000, 2);
        assert_eq!(c.admit("a", "/x", 0), Some(0));
        assert_eq!(c.admit("b", "/x", 0), Some(0));
        // Full of live keys: a third distinct key is dropped, not stored.
        assert_eq!(c.admit("c", "/x", 1), None);
        // Once the old keys expire there is room again.
        assert_eq!(c.admit("c", "/x", 70_000), Some(0));
    }

    #[test]
    fn empty_secret_allows_without_header() {
        assert!(check_mutation_auth("", &HeaderMap::new()).is_ok());
    }

    #[test]
    fn missing_header_is_unauthorized() {
        let err = check_mutation_auth("s3cret", &HeaderMap::new()).unwrap_err();
        assert_eq!(err, MutationAuthError::Missing);
        assert_eq!(err.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn wrong_header_is_forbidden() {
        let h = headers_with(&[("x-dtfs-shared-secret", "nope")]);
        let err = check_mutation_auth("s3cret", &h).unwrap_err();
        assert_eq!(err, MutationAuthError::Mismatch);
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn matching_x_header_ok() {
        let h = headers_with(&[("x-dtfs-shared-secret", "s3cret")]);
        assert!(check_mutation_auth("s3cret", &h).is_ok());
    }

    #[test]
    fn matching_bearer_ok() {
        let h = headers_with(&[("authorization", "Bearer s3cret")]);
        assert!(check_mutation_auth("s3cret", &h).is_ok());
    }

    #[test]
    fn bearer_mismatch_forbidden() {
        let h = headers_with(&[("authorization", "Bearer wrong")]);
        assert_eq!(
            check_mutation_auth("s3cret", &h).unwrap_err(),
            MutationAuthError::Mismatch
        );
    }

    #[test]
    fn exempt_gets_and_probes_not_join() {
        assert!(mutation_auth_exempt(&Method::GET, "/api/v1/scrub"));
        // Audit #12 — the REST join was removed; no exemption remains.
        assert!(!mutation_auth_exempt(&Method::POST, "/api/v1/join/request"));
        assert!(mutation_auth_exempt(&Method::POST, "/healthz"));
        assert!(!mutation_auth_exempt(&Method::POST, "/api/v1/scrub"));
        assert!(!mutation_auth_exempt(
            &Method::POST,
            "/api/v1/volumes/default/set-mandatory"
        ));
    }
}
