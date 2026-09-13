//! Embedded manager WebUI.
//!
//! Serves a functional single-page dashboard from the manager binary.
//! In a future production build this would serve a compiled React/Vite SPA
//! via `rust-embed`; for now it renders a self-contained HTML page with
//! vanilla JS that fetches data from the REST API.
//!
//! **Home page sections (matching the plan):**
//! - Health overview banner (HEALTHY / DEGRADED / CRITICAL)
//! - Production Signals strip (anchor pending, missing chunks, quarantine,
//!   failed migrations, placement undergoal/missing) — real API fields only
//! - Summary counters: Managers, Data Nodes, Disks, FUSE Clients
//! - Capacity cards: Raw (total/used), Net usable, I/O throughput
//! - Last scrub card (level, time, results)
//! - Node table with health badges

use axum::{
    extract::State,
    http::{header, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};
use std::sync::Arc;

/// Content-Security-Policy sent with every WebUI document.
///
/// The dashboard is fully self-contained: no remote scripts, styles, fonts
/// or images, and no `eval`. The policy therefore names no remote origin at
/// all, which blocks script loading and pixel exfiltration even if markup
/// were ever injected. `script-src 'unsafe-inline'` is required because the
/// page is a single inline bundle *and* wires its controls through inline
/// `on*` attributes; a nonce would not cover those (nonces apply to script
/// elements, not event handlers), so escaping — not CSP — is the primary
/// control against injected markup here.
const CONTENT_SECURITY_POLICY: &str = "default-src 'self'; \
script-src 'unsafe-inline'; \
style-src 'self' 'unsafe-inline'; \
img-src 'self' data:; \
font-src 'self'; \
connect-src 'self'; \
object-src 'none'; \
base-uri 'none'; \
form-action 'none'; \
frame-ancestors 'none'";

/// HTML-escape a value for interpolation into element text or a quoted
/// attribute. Used for every value the manager does not itself control.
fn escape_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

/// Shared state for WebUI routes.
#[derive(Clone)]
pub struct WebUiState {
    pub cluster_name: String,
    pub manager_hostname: String,
}

/// Build the Axum router for the WebUI.
pub fn router(state: Arc<WebUiState>) -> Router {
    Router::new()
        .route("/", get(index_page))
        .route("/ui", get(index_page))
        .route("/ui/{*path}", get(index_page))
        .route("/assets/styles.css", get(stylesheet))
        .route("/favicon.ico", get(favicon))
        .with_state(state)
}

async fn index_page(State(state): State<Arc<WebUiState>>) -> Response {
    let body = render_dashboard(&state.cluster_name, &state.manager_hostname);
    (
        StatusCode::OK,
        [
            (header::CONTENT_SECURITY_POLICY, CONTENT_SECURITY_POLICY),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (header::REFERRER_POLICY, "no-referrer"),
        ],
        Html(body),
    )
        .into_response()
}

async fn stylesheet() -> Response {
    let css = include_str!("webui_assets/styles.css");
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/css"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        css.to_string(),
    )
        .into_response()
}

async fn favicon() -> Response {
    let svg = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
        <rect width="64" height="64" rx="12" fill="#2563eb"/>
        <text x="32" y="44" font-size="32" fill="white" text-anchor="middle" font-family="monospace">D</text>
    </svg>"##;
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "image/svg+xml"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        svg.to_string(),
    )
        .into_response()
}

fn render_dashboard(cluster_name: &str, hostname: &str) -> String {
    // The page lives in webui_assets/dashboard.html as plain HTML/JS (no
    // format! brace doubling); only three tokens are substituted.
    include_str!("webui_assets/dashboard.html")
        .replace("__DTFS_CLUSTER_NAME__", &escape_html(cluster_name))
        .replace("__DTFS_HOSTNAME__", &escape_html(hostname))
        .replace("__DTFS_REPO_URL__", dtfs_common::REPO_URL)
}

#[cfg(test)]
mod bug_bi_nav_caret_tests {
    use super::*;

    /// Bug BI — the nav-group dropdown caret must NOT be emitted as a
    /// double-backslash CSS escape. The page is a Rust raw string, so
    /// `\25BE` in source survives verbatim and the browser renders the
    /// literal text `\25BE`. The fix uses the literal ▾ glyph.
    #[test]
    fn nav_caret_not_double_escaped() {
        let html = render_dashboard("test-cluster", "mgr-test");
        assert!(
            !html.contains(r"\25BE"),
            "nav caret must not emit a double-backslash CSS escape (raw-string trap)"
        );
        assert!(
            !html.contains("25BE"),
            "no leftover CSS hex escape for the caret"
        );
        assert!(
            html.contains("button::after"),
            "nav-group caret rule should be present"
        );
        assert!(
            html.contains('▾'),
            "nav caret should render the literal ▾ glyph"
        );
    }
}

#[cfg(test)]
mod bug_bk_scrub_tests {
    use super::*;

    /// Bug BK — the scrub card must list every retained result (per node +
    /// level), not just the single most-recent. The render path iterates
    /// `stats.scrub_results_by_node` and emits one card per entry, each
    /// carrying its own Quick/Deep badge. Assert the template wires the
    /// array (not the scalar `last_scrub`) and carries both badge labels.
    #[test]
    fn scrub_lists_all_levels() {
        let html = render_dashboard("test-cluster", "mgr-test");
        assert!(
            html.contains("renderScrub(stats.scrub_results_by_node"),
            "scrub render must consume the per-(node,level) array, not last_scrub"
        );
        assert!(
            html.contains(">Deep<") && html.contains(">Quick<"),
            "both Quick and Deep level badges must be present in the scrub card template"
        );
        assert!(
            html.contains("renderScrubCard"),
            "per-entry card renderer should be present"
        );
    }

    /// Bug BT-4 — the home page renders the chunk-placement distribution
    /// (Bug BX-6 `stats.chunk_placement`): a card container, a render fn
    /// wired into the stats refresh, and the six buckets in the legend.
    #[test]
    fn chunk_placement_panel_present() {
        let html = render_dashboard("test-cluster", "mgr-test");
        assert!(html.contains("id=\"chunk-placement-content\""), "panel container present");
        assert!(html.contains("renderChunkPlacement(stats.chunk_placement)"), "render fn wired to stats");
        for bucket in ["stable", "misplaced", "undergoal", "overgoal", "missing", "pending_deletion"] {
            assert!(
                html.contains(&format!("'{bucket}'")),
                "bucket {bucket} must appear in the chunk-placement legend"
            );
        }
    }

    /// Bug BT-1/BT-2 — the volume-edit modal exposes mandatory replica
    /// inputs (per tier) + an anchor section, and the submit posts to
    /// set-mandatory / set-anchor in addition to set-cache.
    #[test]
    fn volume_modal_edits_mandatory_and_anchor() {
        let html = render_dashboard("test-cluster", "mgr-test");
        for id in ["ve-mand-hot", "ve-mand-warm", "ve-mand-cold", "ve-mand-tape", "ve-anchor-dc", "ve-anchor-tier"] {
            assert!(html.contains(id), "modal must contain input {id}");
        }
        assert!(html.contains("post('set-mandatory'"), "submit posts set-mandatory");
        assert!(html.contains("post('set-anchor'"), "submit posts set-anchor");
        assert!(html.contains("post('set-cache'"), "submit still posts set-cache");
    }

    /// Bug BZ — the volume-edit modal exposes the spill-policy controls
    /// (allow override + forbidden tiers/datacenters) and the submit posts
    /// to set-spill.
    #[test]
    fn volume_modal_edits_spill_policy() {
        let html = render_dashboard("test-cluster", "mgr-test");
        for id in [
            "ve-spill-allow",
            "ve-spill-forbid-hot",
            "ve-spill-forbid-warm",
            "ve-spill-forbid-cold",
            "ve-spill-forbid-tape",
            "ve-spill-forbid-dcs",
        ] {
            assert!(html.contains(id), "modal must contain spill control {id}");
        }
        assert!(html.contains("post('set-spill'"), "submit posts set-spill");
    }

    /// UIP-1-2 — the volume-edit modal exposes a write-safety section
    /// (min_replicas_before_ack + write_timeout_secs) and the submit
    /// posts to set-write-safety only when one of those fields was
    /// actually touched by the operator.
    #[test]
    fn volume_modal_edits_write_safety() {
        let html = render_dashboard("test-cluster", "mgr-test");
        for id in ["ve-write-safety-min-replicas", "ve-write-safety-timeout"] {
            assert!(html.contains(id), "modal must contain write-safety input {id}");
        }
        assert!(
            html.contains("post('set-write-safety'"),
            "submit posts set-write-safety"
        );
        assert!(
            html.contains("wsMinEl.dataset.orig"),
            "write-safety fields must be tracked against their prefilled value"
        );
    }

    /// UIP-1-3 — the volume-edit modal exposes a one-shot "Migrate policy"
    /// action button (Bug AV legacy→mandatory migration) that POSTs (no
    /// body) to `/api/v1/volumes/{id}/migrate-policy` via `mutateFetch`.
    #[test]
    fn volume_modal_migrate_policy_button_present() {
        let html = render_dashboard("test-cluster", "mgr-test");
        assert!(
            html.contains(r#"id="ve-migrate-btn""#),
            "modal must contain the migrate-policy button"
        );
        assert!(
            html.contains("onclick=\"migrateVolumePolicy()\""),
            "migrate-policy button must be wired to migrateVolumePolicy()"
        );
        assert!(
            html.contains("async function migrateVolumePolicy"),
            "migrateVolumePolicy handler must exist"
        );
        assert!(
            html.contains("mutateFetch(\n                `/api/v1/volumes/${encodeURIComponent(vid)}/migrate-policy`"),
            "migrate-policy must POST via mutateFetch with a template-literal volume id"
        );
        assert!(
            html.contains("json.mandatory_replicas"),
            "migrate-policy result must surface the returned mandatory_replicas summary"
        );
    }

    /// Home Production Signals strip: real API fields only so operators
    /// see anchor pending / quarantine / failed migrations without logs.
    #[test]
    fn production_signals_strip_present() {
        let html = render_dashboard("test-cluster", "mgr-test");
        assert!(
            html.contains("id=\"signal-strip\""),
            "Home must render the production-signals container"
        );
        assert!(
            html.contains("renderProductionSignals(stats, migrations, cluster)"),
            "fetchStats must pass stats + migrations + cluster/status into the strip"
        );
        assert!(
            html.contains("stats.anchor_pending"),
            "strip must read stats.anchor_pending"
        );
        assert!(
            html.contains("stats.quarantined_nodes"),
            "strip must read stats.quarantined_nodes"
        );
        assert!(
            html.contains("files_missing_chunks"),
            "strip must read cluster/status.files_missing_chunks"
        );
        assert!(
            html.contains("sig-anchor") && html.contains("sig-mig-failed"),
            "chip ids for anchor + failed migrations must be present"
        );
        assert!(
            html.contains("id=\"mig-summary\""),
            "Migrations page must show a status summary strip"
        );
    }

    /// LIS-35 — mutating POSTs must send X-Dtfs-Shared-Secret from
    /// sessionStorage (never embedded in HTML); GETs stay plain fetch;
    /// 401/403 get actionable copy.
    #[test]
    fn lis35_session_secret_on_mutating_fetches() {
        let html = render_dashboard("test-cluster", "mgr-test");
        assert!(
            html.contains("DTFS_SECRET_KEY = 'dtfs.shared_secret'"),
            "sessionStorage key constant present"
        );
        assert!(
            html.contains("sessionStorage.setItem(DTFS_SECRET_KEY"),
            "secret must be written to sessionStorage"
        );
        assert!(
            html.contains("h['X-Dtfs-Shared-Secret'] = s"),
            "authHeaders must attach X-Dtfs-Shared-Secret"
        );
        assert!(
            html.contains("async function mutateFetch"),
            "mutateFetch helper must exist"
        );
        assert!(
            html.contains("401 Unauthorized - shared_secret required"),
            "401 copy must be actionable"
        );
        assert!(
            html.contains("403 Forbidden - shared_secret mismatch"),
            "403 copy must be actionable"
        );
        assert!(
            html.contains("id=\"session-secret-input\""),
            "Settings must expose a session secret field"
        );
        assert!(
            html.contains("id=\"auth-badge\""),
            "header auth badge must be present"
        );
        // Riv LIS-65: Settings tier readouts must stay (not a LIS-35 regression).
        for needle in [
            "tier.frozen_threshold_days",
            "tier.evaluation_interval_secs",
            "tier.enable_cache_redundancy_escalation",
        ] {
            assert!(
                html.contains(needle),
                "Settings must retain tier readout: {needle}"
            );
        }
        // Mutating endpoints must be invoked via mutateFetch.
        for needle in [
            "mutateFetch('/api/v1/migrations'",
            "mutateFetch('/api/v1/files/flush_cache'",
            "mutateFetch('/api/v1/config'",
            "'/api/v1/nodes/' + nodeId + '/maintenance'",
            "'/api/v1/nodes/' + nid + '/unquarantine'",
            "'/api/v1/nodes/' + nid + '/decommission'",
            "/${suffix}`",
        ] {
            assert!(html.contains(needle), "expected mutate path fragment: {needle}");
        }
        assert!(
            html.contains("await mutateFetch("),
            "maintenance/unquarantine/volume posts must await mutateFetch"
        );
        // No bare POST fetch left for the gated paths.
        assert!(
            !html.contains("method: 'POST'"),
            "all POSTs should go through mutateFetch (no inline method: POST)"
        );
        // GETs must remain unauthenticated plain fetch.
        assert!(
            html.contains("fetch('/api/v1/stats')"),
            "GET /api/v1/stats must stay a plain fetch"
        );
        assert!(
            !html.contains("mutateFetch('/api/v1/stats'"),
            "GET stats must not go through mutateFetch"
        );
        assert!(
            !html.contains("shared_secret = \""),
            "must not embed a shared_secret value in HTML"
        );
    }
}

/// UIP-1-1 — the Volumes page's old placeholder pointed operators at
/// `dtfs-cli volume create`, but the webui had no create-volume path of its
/// own. Replaced with a modal that POSTs `CreateVolumeRequest` to
/// `/api/v1/volumes` via `mutateFetch`.
#[cfg(test)]
mod create_volume_modal_tests {
    use super::*;

    #[test]
    fn create_volume_modal_present_and_wired_through_mutate_fetch() {
        let html = render_dashboard("test-cluster", "mgr-test");
        // Modal container + every CreateVolumeRequest field must be present.
        for needle in [
            "id=\"create-volume-modal\"",
            "id=\"cv-name\"",
            "id=\"cv-chunk-size-mb\"",
            "id=\"cv-replication-hot\"",
            "id=\"cv-replication-warm\"",
            "id=\"cv-replication-cold\"",
            "id=\"cv-anchor-datacenter\"",
        ] {
            assert!(html.contains(needle), "create-volume modal missing: {needle}");
        }
        // The submit path must go through mutateFetch, not an inline fetch.
        assert!(
            html.contains("mutateFetch(\n                '/api/v1/volumes',"),
            "create-volume submit must POST via mutateFetch"
        );
        assert!(
            html.contains("function submitCreateVolume"),
            "expected a submitCreateVolume() handler"
        );
        assert!(
            html.contains("openCreateVolume()"),
            "Volumes page must expose a way to open the create-volume modal"
        );
    }

    #[test]
    fn old_cli_creation_placeholder_is_gone() {
        let html = render_dashboard("test-cluster", "mgr-test");
        assert!(
            !html.contains("Create one with <code>dtfs-cli volume create</code>"),
            "old dtfs-cli-only volume creation placeholder must be removed"
        );
    }
}

#[cfg(test)]
mod uip_2_webui_ops_tests {
    use super::*;

    /// UIP-2-5 — node cards offer Decommission; the dialog POSTs through
    /// mutateFetch and prints the server's 409 message verbatim.
    #[test]
    fn decommission_dialog_wired_through_mutate_fetch() {
        let html = render_dashboard("test-cluster", "mgr-test");
        for needle in [
            "id=\"decommission-modal\"",
            "id=\"dc-node-id\"",
            "id=\"dc-operator\"",
            "id=\"dc-reason\"",
            "id=\"dc-submit\"",
            "id=\"dc-result\"",
            "openDecommissionModal('${escArg(n.node_id)}'",
        ] {
            assert!(html.contains(needle), "decommission dialog missing: {needle}");
        }
        let fn_body = html
            .split("async function submitDecommission()")
            .nth(1)
            .expect("submitDecommission must be defined")
            .split("window.openDecommissionModal")
            .next()
            .unwrap();
        assert!(
            fn_body.contains("mutateFetch(") && fn_body.contains("'/api/v1/nodes/' + nid + '/decommission'"),
            "decommission must POST /api/v1/nodes/<id>/decommission through mutateFetch"
        );
        assert!(
            fn_body.contains("json.error) ? json.error"),
            "server error (409 replicas remain) must be shown verbatim"
        );
    }

    /// UIP-2-4 — membership forms POST the three raft endpoints through
    /// mutateFetch and surface the 409 leader hint.
    #[test]
    fn raft_membership_forms_wired_through_mutate_fetch() {
        let html = render_dashboard("test-cluster", "mgr-test");
        for needle in [
            "id=\"raft-op-hostname\"",
            "id=\"raft-op-addr\"",
            "onclick=\"raftMembershipOp('add-learner')\"",
            "onclick=\"raftMembershipOp('promote')\"",
            "onclick=\"raftMembershipOp('remove')\"",
            "id=\"raft-op-result\"",
        ] {
            assert!(html.contains(needle), "raft membership form missing: {needle}");
        }
        let fn_body = html
            .split("async function raftMembershipOp(op)")
            .nth(1)
            .expect("raftMembershipOp must be defined")
            .split("window.raftMembershipOp")
            .next()
            .unwrap();
        assert!(
            fn_body.contains("mutateFetch(") && fn_body.contains("'/api/v1/raft/' + op"),
            "membership ops must POST /api/v1/raft/<op> through mutateFetch"
        );
        assert!(
            fn_body.contains("shared_secret: getSharedSecret()"),
            "body must carry the session secret for the API's second check"
        );
        assert!(
            fn_body.contains("resp.status === 409") && fn_body.contains("leader_hostname"),
            "409 must surface the leader hint, not a generic error"
        );
    }

    /// UIP-2-3 — read-only Raft page: nav entry, route, state/members
    /// containers and table headers; GET raft/state is a plain fetch.
    #[test]
    fn raft_page_read_only_markup_present() {
        let html = render_dashboard("test-cluster", "mgr-test");
        for needle in [
            "data-route=\"/ui/raft\"",
            "'/ui/raft':       renderRaft",
            "async function renderRaft()",
            "id=\"raft-state\"",
            "id=\"raft-members\"",
            "id=\"raft-members-table\"",
            "<th>Hostname</th><th>Node id</th><th>Addr</th><th>Role</th>",
            "fetch('/api/v1/raft/state')",
        ] {
            assert!(html.contains(needle), "raft page missing: {needle}");
        }
        assert!(
            !html.contains("mutateFetch('/api/v1/raft/state'"),
            "GET raft/state must stay a plain fetch"
        );
    }

    /// UIP-2-2 — manager reachability comes from a plain GET
    /// /api/v1/health next to the stats poll, never via mutateFetch.
    #[test]
    fn manager_health_indicator_uses_plain_fetch() {
        let html = render_dashboard("test-cluster", "mgr-test");
        assert!(html.contains("id=\"manager-health\""), "indicator span missing");
        assert!(
            html.contains("fetch('/api/v1/health')"),
            "GET /api/v1/health must be a plain fetch"
        );
        assert!(
            !html.contains("mutateFetch('/api/v1/health'"),
            "GET health must not go through mutateFetch"
        );
        assert!(html.contains("function setManagerHealth("), "indicator setter missing");
    }

    /// UIP-2-1 — Integrity page carries a "Scrub all" control wired
    /// through mutateFetch; the old CLI-punting placeholder is gone.
    #[test]
    fn scrub_all_button_present_and_wired_through_mutate_fetch() {
        let html = render_dashboard("test-cluster", "mgr-test");
        for needle in [
            "id=\"scrub-all-level\"",
            "<option value=\"quick\">",
            "<option value=\"deep\">",
            "id=\"scrub-all-btn\"",
            "onclick=\"runScrubAll()\"",
            "id=\"scrub-all-result\"",
        ] {
            assert!(html.contains(needle), "scrub-all control missing: {needle}");
        }
        let fn_body = html
            .split("async function runScrubAll()")
            .nth(1)
            .expect("runScrubAll must be defined")
            .split("finally")
            .next()
            .unwrap();
        assert!(
            fn_body.contains("mutateFetch(") && fn_body.contains("'/api/v1/scrub'"),
            "runScrubAll must POST /api/v1/scrub through mutateFetch"
        );
        assert!(
            fn_body.contains("target: '*'") && fn_body.contains("target_type: 'all'"),
            "scrub-all must send the whole-cluster target the API accepts"
        );
        assert!(
            !html.contains("Run <code>dtfs-cli scrub --level deep</code>"),
            "CLI-punting scrub placeholder must be gone"
        );
    }
}

#[cfg(test)]
mod rendered_text_no_bug_refs {
    use super::*;

    /// Internal bug-tracker IDs (e.g. "Bug BX", "Bug BC") must never leak
    /// into the served WebUI — neither rendered, operator-facing text nor
    /// the CSS/HTML/JS comments shipped inside the page (View Source). The
    /// whole `render_dashboard` output is the served document, so a blanket
    /// scan catches any future regression in label or comment. Rust source
    /// comments (this test module) are not part of the output and unaffected.
    #[test]
    fn no_bug_ids_in_served_dashboard() {
        let html = render_dashboard("test-cluster", "mgr-test");
        assert!(
            !html.contains("Bug "),
            "internal bug-tracker IDs must not appear in the served WebUI (rendered text or shipped comments)"
        );
    }
}

/// REV-4-1 — the WebUI is served to an operator whose browser session holds
/// unauthenticated control of the whole REST API, and it renders strings the
/// cluster does not control: file paths (anything that can write to the FUSE
/// mount picks the filename), node hostnames, disk mount paths, volume ids,
/// audit actor/action/target, quarantine reasons and migration errors.
/// Every one of those must be escaped before it reaches `innerHTML`.
#[cfg(test)]
mod xss_escaping_tests {
    use super::*;

    /// The server-side escape helper must neutralise every character that
    /// can break out of an HTML text node or a quoted attribute.
    #[test]
    fn escape_html_neutralises_markup() {
        assert_eq!(escape_html("<script>"), "&lt;script&gt;");
        assert_eq!(escape_html("\""), "&quot;");
        assert_eq!(escape_html("'"), "&#39;");
        assert_eq!(escape_html("&"), "&amp;");
        assert_eq!(escape_html("a&b"), "a&amp;b");
        let hostile = "/media/<img src=x onerror=alert(1)>.mkv";
        let out = escape_html(hostile);
        assert_eq!(
            out,
            "/media/&lt;img src=x onerror=alert(1)&gt;.mkv",
            "path markup must be escaped"
        );
        assert!(!out.contains('<'), "no raw < may survive escaping");
        assert!(!out.contains('>'), "no raw > may survive escaping");
        // Escaping never manufactures new markup.
        assert!(!escape_html("&lt;").contains('<'));
    }

    /// A hostile cluster name / manager hostname must not be able to inject
    /// markup into the server-rendered shell.
    #[test]
    fn hostile_identity_strings_render_escaped() {
        let html = render_dashboard(
            "<script>alert(1)</script>",
            "\"><img src=x onerror=alert(2)>",
        );
        assert!(
            !html.contains("<script>alert(1)</script>"),
            "hostile cluster name must not render as live markup"
        );
        assert!(
            !html.contains("onerror=alert(2)>"),
            "hostile hostname must not render an event handler"
        );
        assert!(
            html.contains("&lt;script&gt;alert(1)&lt;/script&gt;"),
            "hostile cluster name must render escaped"
        );
        // Exactly one real <script element: the page's own bundle.
        assert_eq!(
            html.matches("<script").count(),
            1,
            "the page must contain exactly one script element"
        );
    }

    /// Every value that originates outside the manager must be routed
    /// through the client-side escape helper before it is concatenated into
    /// innerHTML. Each entry below is a raw interpolation form that was
    /// previously injected verbatim; its continued absence from the served
    /// page is the regression guard.
    #[test]
    fn tainted_interpolations_are_escaped_in_page_script() {
        let html = render_dashboard("test-cluster", "mgr-test");
        assert!(
            html.contains("function esc("),
            "the page must define the client-side escape helper"
        );
        assert!(
            html.contains("function escArg("),
            "the page must define the inline-handler-argument escape helper"
        );
        // escArg must JS-escape the backslash BEFORE the quote, then hand
        // the result to esc(). Reversing the order lets a trailing
        // backslash escape the closing quote of the JS string literal and
        // run the rest of the filename as code, and skipping esc() leaves
        // the attribute itself breakable.
        let arg_body = html
            .split("function escArg(")
            .nth(1)
            .expect("escArg body")
            .split("}")
            .next()
            .expect("escArg body end");
        let bs = arg_body.find(r"/\\/g").expect("escArg must escape backslashes");
        let quote = arg_body.find(r"/'/g").expect("escArg must escape quotes");
        assert!(bs < quote, "escArg must escape backslash before quote");
        assert!(
            arg_body.contains("return esc("),
            "escArg must HTML-escape its JS-escaped result: {arg_body}"
        );
        let raw_forms = [
            // Storage overview + scrub card
            "${t.tier}",
            "${s.node_id}",
            "${s.disk_path}",
            // Node table
            "${n.node_id||'—'}",
            "${n.hostname||'—'}",
            "${n.location||'—'}",
            "${versionText}",
            "${n.health||'unknown'}",
            "${n.version || '—'}",
            "' / ' + n.build_date",
            "${rc}",
            "${hc}",
            // Integrity feed
            "${action}",
            "${r.actor||'—'}",
            "${r.target||'—'}",
            "${r.summary||r.result||'—'}",
            // Topology
            "${region}",
            "${dc}",
            "${rack}",
            "${h}",
            "${role}",
            // Node cards + disk table
            "${d.disk_id}",
            "${d.tier||'—'}",
            "${d.mount_path||''}",
            "${d.mount_path||'—'}",
            "${d.status||'—'}",
            "${dstatus}",
            "${(d.tier||'').toLowerCase()}",
            "${n.client_ip || '—'}",
            "${n.grpc_addr||'—'}",
            "${n.quarantine.reason}",
            "${n.quarantine.triggering_chunk_id}",
            "${n.quarantine.last_fault_summary||'—'}",
            "${(n.node_id||'').slice(0,12)}",
            "${n.node_id}",
            // Volumes
            "${v.anchor.datacenter||'—'}",
            "${v.anchor.required_tier||'(any)'}",
            "${v.volume_id}",
            "${vid}",
            // Explorer
            "${seg}",
            "${filter}",
            "${cwd}",
            "${d.name}",
            "${f.name}",
            "${f.path||''}",
            "${d.path.replace",
            "${full.replace",
            // Migration tasks
            "${t.task_id||'—'}",
            "${t.chunk_id||''}",
            "${(t.chunk_id||'').slice(0,16)}",
            "${t.source_node||'?'}",
            "${t.target_node||'?'}",
            "${t.tier||'—'}",
            "${(t.tier||'').toLowerCase()}",
            "${t.status||'—'}",
            "${t.last_error||''}",
            "${t.last_error||'—'}",
            // Audit log
            "${e.actor||'—'}",
            "${e.action||'—'}",
            "${e.target||''}",
            "${e.target||'—'}",
            "${e.result||'—'}",
            // Config table
            "${r.name}",
            "${r.scope}",
            "${cur||'(null)'}",
            "${def}",
            "String(pending).replace(/\"/g,'&quot;')",
        ];
        for form in raw_forms {
            assert!(
                !html.contains(form),
                "unescaped interpolation still present in page script: {form}"
            );
        }
        // Locals interpolated into inline handler arguments must be escaped
        // where they are built, not where they are used.
        for construction in [
            "const safePath = escArg(",
            "const safe = escArg(",
            "const safeParent = escArg(",
            "const safeVid = escArg(",
            "const safeName = escArg(",
        ] {
            assert!(
                html.contains(construction),
                "handler argument must be escaped at construction: {construction}"
            );
        }
        // The old hand-rolled quote-stripping is not an escaper: it leaves
        // `<` and `"` intact. None of it may survive.
        assert!(
            !html.contains(".replace(/'/g, \"&#39;\")")
                && !html.contains(".replace(/'/g,'&#39;')"),
            "hand-rolled quote stripping must be replaced by escArg()"
        );
    }

    /// A hostile filename flowing through the explorer row builder must not
    /// be able to open a script element.
    #[test]
    fn hostile_filename_produces_no_script_element() {
        let hostile = "/media/<script>alert(1)</script>.mkv";
        let row = format!(
            "<td title=\"{}\">{}</td>",
            escape_html(hostile),
            escape_html(hostile)
        );
        assert!(
            !row.contains("<script"),
            "escaped filename must not contain an unescaped script element: {row}"
        );
        assert!(row.contains("&lt;script&gt;"));
    }

    /// The WebUI response must carry a Content-Security-Policy that blocks
    /// remote script, plugins, framing and cross-origin form posts.
    #[test]
    fn dashboard_declares_restrictive_csp() {
        let csp = CONTENT_SECURITY_POLICY;
        assert!(csp.contains("default-src 'self'"), "csp: {csp}");
        assert!(csp.contains("object-src 'none'"), "csp: {csp}");
        assert!(csp.contains("base-uri 'none'"), "csp: {csp}");
        assert!(csp.contains("frame-ancestors 'none'"), "csp: {csp}");
        assert!(csp.contains("connect-src 'self'"), "csp: {csp}");
        assert!(!csp.contains("unsafe-eval"), "csp must not permit eval: {csp}");
        assert!(
            !csp.contains("http:") && !csp.contains("https:") && !csp.contains('*'),
            "csp must not allow any remote origin: {csp}"
        );
    }
}
