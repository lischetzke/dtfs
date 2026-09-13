//! REST handlers: audit log and integrity-fault views. Split out of api.rs on 2026-09-13; every
//! item sees api.rs's imports and private helpers via the glob.
#![allow(unused_imports)]

use super::*;

#[derive(Deserialize, Default)]
pub(super) struct AuditQuery {
    pub(super) limit: Option<usize>,
    pub(super) action: Option<String>,
}

/// Bug BC-12b — list of action prefixes the integrity feed surfaces.
/// Order matches the section in plans/completed/2026-05-28-PLAN-BUG-BC-SCRUB.md § 4.5.
pub(super) const INTEGRITY_ACTIONS: &[&str] = &[
    "node.quarantine",
    "node.unquarantine",
    "scrub.quick.fault",
    "scrub.deep.corrupt",
    "read.integrity_fault",
    "orphan.in_state_missing",
    // CF-1b — delete fan-out failures (Bug N): a data node was
    // unreachable or rejected DeleteChunk, so the deleted file's chunks
    // are orphaned until the reconciler reclaims them. Surfacing it here
    // makes the leak visible in the WebUI integrity feed.
    "delete.fanout_failed",
];

/// Bug BC-12b / CF-1b — does an audit entry belong in the integrity feed?
/// True when either the structured `action` or the legacy `summary`
/// (state-machine-internal entries have no action field) starts with one
/// of the `INTEGRITY_ACTIONS` prefixes. Pass `""` for `summary` on the
/// local-ring branch, which has no summary column.
pub(super) fn action_is_integrity_fault(action: &str, summary: &str) -> bool {
    INTEGRITY_ACTIONS
        .iter()
        .any(|p| action.starts_with(p) || summary.starts_with(p))
}

/// Bug BC-12b — returns all audit entries whose `action` (or, for
/// legacy state-machine entries, `summary`) starts with one of the
/// integrity prefixes. Same wire shape as `/api/v1/audit` rows so the
/// WebUI consumer can reuse its render code.
pub(super) async fn get_integrity_faults(
    State(state): State<Arc<AppState>>,
    Query(query): Query<AuditQuery>,
) -> Json<Vec<Value>> {
    let limit = query.limit.unwrap_or(200);

    if state.config.audit.replicate_via_raft {
        let sm = state.sm_data.read().await;
        let entries: Vec<Value> = sm
            .audit_log
            .iter()
            .rev()
            .filter(|e| action_is_integrity_fault(&e.action, &e.summary))
            .take(limit)
            .map(|e| {
                let (actor, action, target, result) =
                    backfill_audit_fields(&e.actor, &e.action, &e.target, &e.result, &e.summary);
                json!({
                    "timestamp": e.timestamp_ms / 1_000,
                    "actor": actor,
                    "action": action,
                    "target": target,
                    "result": result,
                    "summary": e.summary,
                    "origin_manager": e.origin_manager,
                })
            })
            .collect();
        Json(entries)
    } else {
        // Local-ring AuditEntry has no `summary` field (that lives on
        // the replicated state_machine variant). Match against action
        // only. Over-fetch so the filter has enough material before
        // truncation.
        let filter = crate::audit::AuditFilter {
            since: None,
            until: None,
            action: None,
            limit: Some(limit.saturating_mul(4).max(limit)),
        };
        let entries: Vec<Value> = state
            .audit_log
            .query(&filter)
            .into_iter()
            .filter(|e| action_is_integrity_fault(&e.action, ""))
            .take(limit)
            .map(|e| {
                json!({
                    "timestamp": e.timestamp,
                    "actor": e.actor,
                    "action": e.action,
                    "target": e.target,
                    "result": e.result,
                    "summary": "",
                    "origin_manager": "",
                })
            })
            .collect();
        Json(entries)
    }
}

pub(super) async fn get_audit_log(
    State(state): State<Arc<AppState>>,
    Query(query): Query<AuditQuery>,
) -> Json<Vec<Value>> {
    // Bug AH — when Raft audit fan-out is enabled, read from the REPLICATED
    // state-machine audit_log so every manager returns the same view. When
    // disabled (the safe default during rolling upgrades), fall back to the
    // per-manager local ring as before. Both branches emit the same wire
    // shape: `timestamp` in SECONDS-since-epoch (preserves WebUI Bug AG
    // `*1000` fix and CLI's `from_timestamp(ts, 0)` consumer); `summary`
    // and `origin_manager` are surfaced as new fields, empty when reading
    // from the local ring (which has no notion of either).
    let limit = query.limit.unwrap_or(50);
    let action_filter = query.action.as_deref();

    if state.config.audit.replicate_via_raft {
        let sm = state.sm_data.read().await;
        // Newest-first: replicated log appends in Raft commit order (≈
        // wall-clock). Reverse, filter, limit.
        let entries: Vec<Value> = sm
            .audit_log
            .iter()
            .rev()
            .filter(|e| match action_filter {
                Some(prefix) => {
                    e.action.starts_with(prefix)
                        // Legacy entries (pre-Bug-AH) only have
                        // `summary`; match against that too.
                        || e.summary.starts_with(prefix)
                }
                None => true,
            })
            .take(limit)
            .map(|e| {
                // Bug AI — `apply_request`'s own `audit()` helper produces
                // entries with empty structured fields and a summary like
                // "CreateFile: /foo (directory)". Without backfill, the
                // WebUI Audit page renders these as four em-dashes
                // because the table draws from actor/action/target/result
                // and never surfaces the summary. Delegate to the pure
                // helper so the parse is unit-tested.
                let (actor, action, target, result) =
                    backfill_audit_fields(&e.actor, &e.action, &e.target, &e.result, &e.summary);
                json!({
                    "timestamp": e.timestamp_ms / 1_000,
                    "actor": actor,
                    "action": action,
                    "target": target,
                    "result": result,
                    "summary": e.summary,
                    "origin_manager": e.origin_manager,
                })
            })
            .collect();
        Json(entries)
    } else {
        // Pre-Bug-AH behaviour — local per-manager view.
        let filter = crate::audit::AuditFilter {
            limit: Some(limit),
            action: query.action,
            ..Default::default()
        };
        let entries = state.audit_log.query(&filter);
        let result: Vec<Value> = entries
            .into_iter()
            .map(|e| {
                json!({
                    "timestamp": e.timestamp,
                    "actor": e.actor,
                    "action": e.action,
                    "target": e.target,
                    "result": e.result,
                    "summary": "",
                    "origin_manager": "",
                })
            })
            .collect();
        Json(result)
    }
}

// ---------------------------------------------------------------------------
// Config endpoints
// ---------------------------------------------------------------------------
