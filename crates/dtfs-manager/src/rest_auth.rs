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
//! - `/api/v1/join/*` (HMAC challenge already proves the secret)
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
    if path.starts_with("/api/v1/join/") {
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

    if mutation_auth_exempt(&method, &path) {
        return next.run(req).await;
    }

    if !path.starts_with("/api/v1/") {
        return next.run(req).await;
    }

    match check_mutation_auth(&state.config.cluster.shared_secret, req.headers()) {
        Ok(()) => next.run(req).await,
        Err(err) => {
            state.audit_log.log(
                "api",
                "auth.denied",
                &format!("method={method} path={path}"),
                err.message(),
            );
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
    fn exempt_gets_and_join() {
        assert!(mutation_auth_exempt(&Method::GET, "/api/v1/scrub"));
        assert!(mutation_auth_exempt(&Method::POST, "/api/v1/join/request"));
        assert!(mutation_auth_exempt(&Method::POST, "/healthz"));
        assert!(!mutation_auth_exempt(&Method::POST, "/api/v1/scrub"));
        assert!(!mutation_auth_exempt(
            &Method::POST,
            "/api/v1/volumes/default/set-mandatory"
        ));
    }
}
