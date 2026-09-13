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
    format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DTFS — {cluster_name}</title>
    <style>
        :root {{
            --bg:          #0f172a;
            --surface:     #1e293b;
            --surface2:    #273449;
            --border:      #334155;
            --text:        #e2e8f0;
            --text-muted:  #94a3b8;
            --primary:     #3b82f6;
            --success:     #22c55e;
            --warning:     #f59e0b;
            --danger:      #ef4444;
            --tape:        #a855f7;
        }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--bg); color: var(--text); min-height: 100vh;
        }}

        /* ── Header ── */
        .header {{
            background: var(--surface);
            border-bottom: 1px solid var(--border);
            padding: .5rem 1.25rem;
            display: flex; align-items: center; justify-content: space-between;
        }}
        .header-brand {{ display: flex; align-items: center; gap: .75rem; }}
        .header-brand svg {{ flex-shrink: 0; }}
        .header-title {{ font-size: 1.1rem; font-weight: 700; }}
        .header-title span {{ color: var(--primary); }}
        .header-meta {{ font-size: .8rem; color: var(--text-muted); text-align: right; }}
        .header-meta b {{ color: var(--text); }}
        /* LIS-35 — session shared_secret status (not the secret value) */
        .auth-badge {{
            display: inline-block; margin-top: .25rem;
            font-size: .72rem; font-weight: 600; letter-spacing: .02em;
            padding: .15rem .45rem; border-radius: .25rem; cursor: pointer;
            border: 1px solid var(--border); background: var(--surface2);
            color: var(--text-muted); font-family: inherit;
        }}
        .auth-badge.auth-set {{
            border-color: #166534; color: var(--success); background: #14532d33;
        }}
        .auth-badge.auth-missing {{
            border-color: #92400e; color: var(--warning); background: #78350f33;
        }}
        .auth-badge:hover {{ border-color: var(--primary); color: var(--text); }}

        /* ── Nav ── */
        .nav {{
            background: var(--surface);
            border-bottom: 1px solid var(--border);
            padding: 0 1.25rem;
            display: flex; gap: 0;
            /* overflow MUST be visible so the absolute-positioned
               .nav-menu dropdowns aren't clipped at the nav bottom.
               Horizontal scroll on tiny viewports is handled by the
               page-level overflow-x; this nav is short enough that
               no clipping is needed on standard desktop widths. */
            overflow: visible;
        }}
        .nav a, .nav .nav-group > button {{
            color: var(--text-muted); text-decoration: none;
            padding: .55rem .9rem; font-size: .85rem;
            border-bottom: 2px solid transparent; white-space: nowrap;
            transition: color .15s, border-color .15s;
            background: transparent; border-top: none; border-left: none;
            border-right: none; cursor: pointer; font-family: inherit;
        }}
        .nav a:hover, .nav .nav-group > button:hover {{
            color: var(--text); border-bottom-color: var(--border);
        }}
        .nav a.active, .nav .nav-group.active > button {{
            color: var(--text); border-bottom-color: var(--primary);
        }}
        .nav .nav-group {{ position: relative; }}
        .nav .nav-group > button::after {{
            content: ' ▾'; font-size: .7rem; margin-left: .3rem;
            color: var(--text-muted);
        }}
        .nav .nav-menu {{
            display: none; position: absolute; top: 100%; left: 0;
            background: var(--surface); border: 1px solid var(--border);
            border-top: none; min-width: 11rem; z-index: 50;
            box-shadow: 0 4px 12px rgba(0,0,0,.25);
        }}
        .nav .nav-group:hover .nav-menu,
        .nav .nav-group.open .nav-menu {{ display: block; }}
        .nav .nav-menu a {{
            display: block; padding: .5rem .9rem; border-bottom: none;
            font-size: .85rem; color: var(--text-muted);
        }}
        .nav .nav-menu a:hover {{
            background: var(--bg); color: var(--text); border-bottom: none;
        }}
        .nav .nav-menu a.active {{
            color: var(--text); background: var(--bg);
            border-left: 2px solid var(--primary);
        }}

        /* ── Health banner ── */
        .health-banner {{
            padding: .6rem 1.25rem;
            display: flex; align-items: center; justify-content: space-between;
            font-size: .9rem; font-weight: 600;
            border-bottom: 1px solid var(--border);
        }}
        .health-banner.healthy  {{ background: #14532d44; }}
        .health-banner.degraded {{ background: #78350f44; }}
        .health-banner.critical {{ background: #7f1d1d44; }}
        .health-banner.unknown  {{ background: var(--surface); }}
        .health-left {{ display: flex; align-items: center; gap: .75rem; }}
        .health-dot {{
            width: 12px; height: 12px; border-radius: 50%;
            animation: pulse 2s infinite;
        }}
        .health-dot.healthy  {{ background: var(--success); box-shadow: 0 0 8px var(--success); }}
        .health-dot.degraded {{ background: var(--warning); box-shadow: 0 0 8px var(--warning); }}
        .health-dot.critical {{ background: var(--danger);  box-shadow: 0 0 8px var(--danger);  animation: pulse-fast 1s infinite; }}
        .health-dot.unknown  {{ background: var(--text-muted); animation: none; }}
        @keyframes pulse      {{ 0%,100% {{ opacity:1 }} 50% {{ opacity:.5 }} }}
        @keyframes pulse-fast {{ 0%,100% {{ opacity:1 }} 50% {{ opacity:.3 }} }}
        .health-label {{ font-size: 1rem; text-transform: uppercase; letter-spacing: .08em; }}
        .health-right {{ font-size: .8rem; color: var(--text-muted); font-weight: 400; }}

        /* ── Production signals (Home) ── */
        .signal-strip {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            gap: .5rem; margin-bottom: 1rem;
        }}
        .signal-chip {{
            background: var(--surface); border: 1px solid var(--border);
            border-radius: .4rem; padding: .55rem .7rem;
            cursor: pointer; text-align: left; font-family: inherit; color: inherit;
            min-height: 3.4rem;
        }}
        .signal-chip:hover {{ border-color: var(--primary); }}
        .signal-chip .signal-val {{
            font-size: 1.25rem; font-weight: 700; line-height: 1.1;
            font-variant-numeric: tabular-nums;
        }}
        .signal-chip .signal-lbl {{
            font-size: .7rem; color: var(--text-muted); margin-top: .2rem;
            text-transform: uppercase; letter-spacing: .04em;
        }}
        .signal-chip.ok .signal-val {{ color: var(--text-muted); }}
        .signal-chip.warn {{ border-color: #92400e88; background: #78350f22; }}
        .signal-chip.warn .signal-val {{ color: var(--warning); }}
        .signal-chip.danger {{ border-color: #7f1d1d88; background: #7f1d1d22; }}
        .signal-chip.danger .signal-val {{ color: var(--danger); }}
        .signal-chip .signal-hint {{
            font-size: .65rem; color: var(--text-muted); margin-top: .15rem;
        }}

        /* ── Main layout ── */
        .container {{ max-width: none; margin: 0; padding: 1rem 1.25rem; }}
        .section-title {{
            font-size: .7rem; font-weight: 600; text-transform: uppercase;
            letter-spacing: .08em; color: var(--text-muted); margin-bottom: .75rem;
        }}

        /* ── Count cards (auto-fit so wide displays show more per row) ── */
        .counts-row {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: .75rem; margin-bottom: 1rem;
        }}
        .count-card {{
            background: var(--surface); border: 1px solid var(--border);
            border-radius: .5rem; padding: .65rem .9rem;
            display: flex; align-items: center; gap: .75rem;
        }}
        .count-icon {{
            width: 40px; height: 40px; border-radius: .5rem;
            display: flex; align-items: center; justify-content: center;
            font-size: 1.25rem; flex-shrink: 0;
        }}
        .count-icon.managers   {{ background: #1d4ed822; color: var(--primary); }}
        .count-icon.datanodes  {{ background: #15803d22; color: var(--success); }}
        .count-icon.disks      {{ background: #92400e22; color: var(--warning); }}
        .count-icon.clients    {{ background: #6b21a822; color: var(--tape); }}
        .count-body h4 {{ font-size: 1.6rem; font-weight: 700; line-height: 1; }}
        .count-body p  {{ font-size: .75rem; color: var(--text-muted); margin-top: .2rem; }}

        /* ── Metric cards (capacity + I/O) ── */
        .metrics-row {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
            gap: .75rem; margin-bottom: 1rem;
        }}
        .metric-card {{
            background: var(--surface); border: 1px solid var(--border);
            border-radius: .5rem; padding: .85rem 1rem;
        }}
        .metric-card h3 {{
            font-size: .75rem; font-weight: 600; text-transform: uppercase;
            letter-spacing: .06em; color: var(--text-muted); margin-bottom: .875rem;
        }}
        .metric-row {{ display: flex; justify-content: space-between; align-items: baseline; margin-bottom: .4rem; }}
        .metric-label {{ font-size: .8rem; color: var(--text-muted); }}
        .metric-value {{ font-size: .95rem; font-weight: 600; }}
        .progress-wrap {{ margin: .5rem 0 .75rem; }}
        .progress-bar {{
            height: 6px; background: var(--border); border-radius: 3px; overflow: hidden;
        }}
        .progress-fill {{
            height: 100%; border-radius: 3px; transition: width .5s ease;
        }}
        .progress-fill.primary {{ background: var(--primary); }}
        .progress-fill.success {{ background: var(--success); }}
        .progress-fill.warning {{ background: var(--warning); }}
        .io-row {{ display: flex; gap: 1rem; margin-top: .25rem; }}
        .io-item {{ flex: 1; }}
        .io-item .io-dir {{ font-size: .7rem; text-transform: uppercase; letter-spacing: .06em; color: var(--text-muted); margin-bottom: .2rem; }}
        .io-item .io-val {{ font-size: 1.4rem; font-weight: 700; }}
        .io-item .io-unit {{ font-size: .75rem; color: var(--text-muted); margin-left: .2rem; }}

        /* ── Scrub card ── */
        .scrub-card {{
            background: var(--surface); border: 1px solid var(--border);
            border-radius: .5rem; padding: .85rem 1rem;
            /* WebUI bug — was missing the top margin so the card
               sat flush against Tier Distribution above. */
            margin-top: .75rem;
            margin-bottom: 1rem;
        }}
        .scrub-card h3 {{
            font-size: .75rem; font-weight: 600; text-transform: uppercase;
            letter-spacing: .06em; color: var(--text-muted); margin-bottom: .875rem;
        }}
        .scrub-result {{ padding: .75rem 0; }}
        .scrub-result + .scrub-result {{ border-top: 1px solid var(--border); margin-top: .25rem; }}
        .scrub-meta {{ display: flex; flex-wrap: wrap; gap: .5rem 2rem; margin-bottom: .875rem; }}
        .scrub-meta-item {{ font-size: .85rem; }}
        .scrub-meta-item span {{ color: var(--text-muted); margin-right: .3rem; }}
        .scrub-stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: .75rem; }}
        .scrub-stat {{ text-align: center; }}
        .scrub-stat .stat-val {{ font-size: 1.4rem; font-weight: 700; }}
        .scrub-stat .stat-lbl {{ font-size: .7rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: .05em; margin-top: .15rem; }}
        .scrub-stat.corrupt .stat-val {{ color: var(--danger); }}
        .scrub-stat.missing .stat-val {{ color: var(--warning); }}
        .scrub-placeholder {{ font-size: .85rem; color: var(--text-muted); font-style: italic; }}

        /* ── Nodes table ── */
        .table-card {{
            background: var(--surface); border: 1px solid var(--border);
            border-radius: .5rem; overflow: hidden; margin-bottom: 1rem;
        }}
        .table-card-header {{
            padding: .55rem 1rem;
            display: flex; align-items: center; justify-content: space-between;
            border-bottom: 1px solid var(--border);
        }}
        .table-card-header h3 {{
            font-size: .8rem; font-weight: 600; text-transform: uppercase;
            letter-spacing: .06em; color: var(--text-muted);
        }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{
            text-align: left;
            padding: .4rem .85rem;
            border-bottom: 1px solid var(--border);
        }}
        th {{
            font-size: .7rem; color: var(--text-muted);
            text-transform: uppercase; letter-spacing: .05em;
            background: var(--surface2);
        }}
        td {{ font-size: .8rem; }}
        tr:last-child td {{ border-bottom: none; }}
        tr:hover td {{ background: var(--surface2); }}

        /* ── Badges ── */
        .badge {{
            display: inline-block; padding: .15rem .55rem;
            border-radius: 9999px; font-size: .7rem; font-weight: 600;
        }}
        .badge-healthy    {{ background: #22c55e22; color: var(--success); }}
        .badge-suspect    {{ background: #f59e0b22; color: var(--warning); }}
        .badge-degraded   {{ background: #f59e0b22; color: var(--warning); }}
        .badge-down       {{ background: #ef444422; color: var(--danger); }}
        .badge-failed     {{ background: #ef444422; color: var(--danger); }}
        /* Quarantined nodes hard-flag in deep red so an
           operator scanning the page can't miss a data-integrity event. */
        .badge-quarantined {{ background: #b91c1c; color: white; font-weight: 700; letter-spacing: .04em; }}
        /* CF-4c — a Manager-role node that is configured/registered but
           NOT in raft membership (voter or learner). Amber so it reads as
           a warning distinct from the red integrity quarantine. */
        .badge-unjoined {{ background: #d97706; color: white; font-weight: 700; letter-spacing: .04em; }}
        .badge-maintenance {{ background: #3b82f622; color: var(--primary); }}
        .badge-draining   {{ background: #3b82f622; color: var(--primary); }}
        .badge-hot  {{ background: #ef444422; color: var(--danger); }}
        .badge-warm {{ background: #f59e0b22; color: var(--warning); }}
        .badge-cold {{ background: #3b82f622; color: var(--primary); }}
        .badge-tape {{ background: #a855f722; color: var(--tape); }}
        .badge-manager {{ background: #6366f122; color: #818cf8; }}
        /* Raft-leader marker on the Nodes page. Bright accent so
           it stands out next to the normal role/health badges. */
        .badge-leader {{ background: var(--primary); color: white; letter-spacing: .04em; }}
        .badge-data    {{ background: #22c55e22; color: var(--success); }}
        /* clients previously had no badge style, so the role
           column on Nodes rendered an unstyled "client" text without
           the pill that data/manager rows have. */
        .badge-client  {{ background: #06b6d422; color: #22d3ee; }}
        .badge-unknown {{ background: #6b728022; color: var(--text-muted); }}
        /* right-click context menu for the Data Explorer. */
        .ctxmenu {{ position: absolute; z-index: 1000; min-width: 180px;
                    background: var(--surface); border: 1px solid var(--border);
                    border-radius: .375rem; box-shadow: 0 4px 12px #0008;
                    padding: .25rem 0; font-size: .82rem; }}
        .ctxmenu-item {{ padding: .35rem .75rem; cursor: pointer; color: var(--text); }}
        .ctxmenu-item:hover {{ background: var(--surface2); }}
        .explorer-row:hover {{ background: var(--surface2); }}

        .placeholder {{
            text-align: center; color: var(--text-muted);
            padding: 2.5rem; font-style: italic; font-size: .85rem;
        }}

        /* ── Page header ── */
        .page-header {{ margin-bottom: 1rem; }}
        .page-header h2 {{ font-size: 1.25rem; font-weight: 700; margin-bottom: .25rem; }}
        .page-desc {{ font-size: .85rem; color: var(--text-muted); }}

        /* ── Topology ── */
        .topo-root {{ display: flex; flex-direction: column; gap: 1rem; }}
        .topo-region {{ background: var(--surface); border: 1px solid var(--border); border-radius: .625rem; padding: 1rem 1.25rem; }}
        .topo-region-label {{ font-size: .9rem; font-weight: 700; margin-bottom: .75rem; }}
        .topo-dc {{ background: var(--surface2); border: 1px solid var(--border); border-radius: .5rem; padding: .875rem 1rem; margin-bottom: .75rem; }}
        .topo-dc-label {{ font-size: .85rem; font-weight: 600; margin-bottom: .5rem; color: var(--primary); }}
        /* racks lay out HORIZONTALLY inside a DC (left-to-right);
           hosts inside a rack stack VERTICALLY (top-to-bottom). Previously
           racks stacked vertically and hosts wrapped in a grid, which
           collapsed the rack-vs-host hierarchy for any DC with more
           than one rack. flex-wrap so very wide DCs still fit small
           screens; min-width on each rack so columns stay readable. */
        .topo-racks {{ display: flex; flex-direction: row; flex-wrap: wrap; gap: .75rem; align-items: flex-start; }}
        .topo-rack {{ display: flex; flex-direction: column; flex: 1 1 240px; min-width: 220px; background: var(--surface); border: 1px solid var(--border); border-radius: .5rem; padding: .75rem .85rem; }}
        .topo-rack-label {{ font-size: .78rem; color: var(--text-muted); margin-bottom: .5rem; text-transform: uppercase; letter-spacing: .04em; }}
        .topo-nodes {{ display: flex; flex-direction: column; gap: .5rem; }}
        .topo-node {{ background: var(--surface2); border: 1px solid var(--border); border-radius: .4rem; padding: .6rem .75rem; font-size: .83rem; }}

        /* ── Node cards ── */
        .node-card {{ background: var(--surface); border: 1px solid var(--border); border-radius: .5rem; margin-bottom: .75rem; overflow: hidden; }}
        .node-card-header {{ padding: .6rem 1rem; display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border); background: var(--surface2); }}
        .node-card-body {{ padding: .6rem 1rem; }}
        .node-meta {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: .35rem .75rem; margin-bottom: .5rem; }}
        .node-meta-item {{ font-size: .8rem; }}
        .node-meta-item span {{ color: var(--text-muted); margin-right: .3rem; font-size: .75rem; }}
        .btn-maint {{
            background: transparent; border: 1px solid var(--border); color: var(--text-muted);
            padding: .25rem .75rem; border-radius: .375rem; font-size: .75rem; cursor: pointer;
            transition: all .15s;
        }}
        .btn-maint:hover {{ border-color: var(--primary); color: var(--primary); }}

        /* ── Settings ── */
        .settings-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: .75rem; }}
        .settings-section {{
            background: var(--surface); border: 1px solid var(--border);
            border-radius: .5rem; padding: .9rem 1.1rem;
        }}
        .settings-section h3 {{
            font-size: .8rem; font-weight: 700; text-transform: uppercase;
            letter-spacing: .06em; margin-bottom: .5rem; color: var(--text-muted);
        }}
        .settings-desc {{ font-size: .8rem; color: var(--text-muted); margin-bottom: .875rem; }}
        .settings-row {{ display: flex; justify-content: space-between; align-items: center; padding: .35rem 0; border-bottom: 1px solid var(--border); }}
        .settings-row:last-of-type {{ border-bottom: none; }}
        .settings-row label {{ font-size: .82rem; color: var(--text-muted); }}
        .settings-val {{ font-size: .9rem; font-weight: 600; }}
        .settings-note {{ font-size: .73rem; color: var(--text-muted); margin-top: .75rem; font-style: italic; }}
        .form-input {{
            background: var(--surface2); border: 1px solid var(--border); color: var(--text);
            padding: .4rem .7rem; border-radius: .375rem; font-size: .85rem; width: 100%;
        }}
        .form-textarea {{
            background: var(--surface2); border: 1px solid var(--border); color: var(--text);
            padding: .5rem .75rem; border-radius: .375rem; font-size: .78rem;
            width: 100%; font-family: monospace; resize: vertical;
        }}
        .btn-primary {{
            background: var(--primary); border: none; color: white;
            padding: .4rem 1rem; border-radius: .375rem; font-size: .85rem; cursor: pointer;
            transition: opacity .15s; white-space: nowrap;
        }}
        .btn-primary:hover {{ opacity: .85; }}
        .btn-secondary {{
            background: var(--surface2); border: 1px solid var(--border); color: var(--text);
            padding: .35rem .75rem; border-radius: .375rem; font-size: .78rem; cursor: pointer;
            transition: background .15s; white-space: nowrap;
        }}
        .btn-secondary:hover {{ background: var(--border); }}

        /* ── Modal (Migrate dialog) ── */
        .modal-backdrop {{
            position: fixed; inset: 0; background: rgba(0,0,0,.55); z-index: 99;
            display: flex; align-items: center; justify-content: center;
        }}
        .modal-backdrop.hidden {{ display: none; }}
        .modal-card {{
            background: var(--surface); border: 1px solid var(--border);
            border-radius: .5rem; padding: 1.25rem 1.5rem; min-width: 460px;
            max-width: 640px; box-shadow: 0 10px 40px rgba(0,0,0,.4);
            max-height: 80vh; overflow-y: auto;
        }}
        .modal-card h3 {{ margin: 0 0 1rem; font-size: 1.05rem; }}
        .modal-row {{ display: flex; flex-direction: column; gap: .25rem; margin-bottom: .85rem; }}
        .modal-row label {{ font-size: .78rem; color: var(--text-muted); }}
        .modal-actions {{ display: flex; gap: .5rem; justify-content: flex-end; margin-top: .25rem; }}
        .modal-result {{
            margin-top: .85rem; padding: .65rem .8rem; border-radius: .375rem;
            background: var(--surface2); border: 1px solid var(--border);
            font-family: monospace; font-size: .78rem; white-space: pre-wrap;
            max-height: 240px; overflow: auto;
        }}
        .modal-result.success {{ border-color: var(--success); }}
        .modal-result.error   {{ border-color: var(--danger); color: var(--danger); }}

        /* ── Footer ── */
        .footer {{
            text-align: center; color: var(--text-muted);
            font-size: .75rem; padding: .75rem 1.25rem 1.25rem;
        }}
        @media (max-width: 900px) {{
            .counts-row {{ grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); }}
            .metrics-row {{ grid-template-columns: 1fr; }}
            .scrub-stats {{ grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); }}
            .container {{ padding: .75rem .75rem; }}
            .header, .nav, .health-banner {{ padding-left: .75rem; padding-right: .75rem; }}
        }}
    </style>
</head>
<body>

<div class="header">
    <div class="header-brand">
        <svg width="28" height="28" viewBox="0 0 64 64" xmlns="http://www.w3.org/2000/svg">
            <rect width="64" height="64" rx="10" fill="#2563eb"/>
            <text x="32" y="44" font-size="34" fill="white" text-anchor="middle" font-family="monospace" font-weight="bold">D</text>
        </svg>
        <span class="header-title"><span>DTFS</span> — {cluster_name}</span>
    </div>
    <div class="header-meta">
        Manager: <b>{hostname}</b><br>
        <span id="refresh-time">Loading…</span><br>
        <button type="button" class="auth-badge auth-missing" id="auth-badge"
                title="Mutating POSTs need X-Dtfs-Shared-Secret when the cluster secret is set"
                onclick="promptSharedSecret('Set session shared_secret for mutating WebUI actions')">Secret: not set</button>
    </div>
</div>

<nav class="nav" id="topnav">
    <a href="/ui" class="active" data-route="/ui">Home</a>
    <div class="nav-group" data-group="cluster">
        <button type="button" onclick="toggleNavGroup(event,'cluster')">Cluster</button>
        <div class="nav-menu">
            <a href="/ui/topology"   data-route="/ui/topology">Topology</a>
            <a href="/ui/nodes"      data-route="/ui/nodes">Nodes</a>
            <a href="/ui/integrity"  data-route="/ui/integrity">Integrity</a>
            <a href="/ui/audit"      data-route="/ui/audit">Audit Log</a>
            <a href="/ui/config"     data-route="/ui/config">Config</a>
            <a href="/ui/settings"   data-route="/ui/settings">Settings</a>
        </div>
    </div>
    <div class="nav-group" data-group="data">
        <button type="button" onclick="toggleNavGroup(event,'data')">Data</button>
        <div class="nav-menu">
            <a href="/ui/volumes"    data-route="/ui/volumes">Volumes</a>
            <a href="/ui/explorer"   data-route="/ui/explorer">Data Explorer</a>
            <a href="/ui/migrations" data-route="/ui/migrations">Migrations</a>
        </div>
    </div>
    <a href="{repo}/tree/master/docs" target="_blank">Docs ↗</a>
</nav>

<div class="health-banner unknown" id="health-banner">
    <div class="health-left">
        <div class="health-dot unknown" id="health-dot"></div>
        <span class="health-label" id="health-label">Loading…</span>
        <span id="health-detail" style="font-weight:400;color:var(--text-muted);font-size:.85rem"></span>
    </div>
    <div class="health-right" id="health-right">Fetching cluster status…</div>
</div>

<div class="container" id="page-content">
    <!-- Populated by SPA router -->
</div>

<!-- ── Migrate dialog (operator-driven tier/DC moves) ── -->
<div id="migrate-modal" class="modal-backdrop hidden" onclick="if(event.target===this)closeMigrate()">
    <div class="modal-card">
        <h3>Migrate data to another tier / datacenter</h3>
        <div style="font-size:.78rem;color:var(--text-muted);margin-bottom:1rem">
            Adds a replica satisfying the chosen tier (and, when set, datacenter) for every chunk
            whose file path begins with the prefix. Existing replicas are left in place — this
            is additive and never deletes data.
        </div>
        <div class="modal-row">
            <label>Path prefix</label>
            <input id="m-path" class="form-input" placeholder="/movies/Season 1/">
        </div>
        <div class="modal-row">
            <label>Target tier</label>
            <select id="m-tier" class="form-input">
                <option value="hot">hot</option>
                <option value="warm" selected>warm</option>
                <option value="cold">cold</option>
                <option value="tape">tape</option>
            </select>
        </div>
        <div class="modal-row">
            <label>Target datacenter (optional)</label>
            <select id="m-dc" class="form-input">
                <option value="">Any datacenter</option>
            </select>
        </div>
        <div class="modal-row" id="m-empty-row" style="display:none">
            <label style="color:var(--warning)">
                <input id="m-allow-empty" type="checkbox"> Allow empty prefix (matches every file — destructive on big clusters!)
            </label>
        </div>
        <div class="modal-actions">
            <button class="btn-secondary" onclick="closeMigrate()">Cancel</button>
            <button class="btn-primary"  onclick="submitMigrate()" id="m-submit">Schedule migration</button>
        </div>
        <div id="m-result" class="modal-result" style="display:none"></div>
    </div>
</div>

<!-- ── Flush cache dialog (operator-driven cache eviction) ── -->
<div id="flush-modal" class="modal-backdrop hidden" onclick="if(event.target===this)closeFlushCache()">
    <div class="modal-card">
        <h3>Flush cache replicas</h3>
        <div style="font-size:.78rem;color:var(--text-muted);margin-bottom:1rem">
            Drops every <em>cache</em>-class replica (warm/hot, depending on the volume policy)
            for files under the path prefix and sets a per-chunk cooldown to prevent immediate
            re-promotion. Mandatory replicas (the durable anchor) are preserved — any chunk
            whose flush would strand the anchor is refused and audited.
        </div>
        <div class="modal-row">
            <label>Path prefix</label>
            <input id="fc-path" class="form-input" placeholder="/movies/Season 1/">
        </div>
        <div class="modal-row">
            <label>Cache tiers to flush</label>
            <select id="fc-tiers" class="form-input">
                <option value="warm" selected>warm</option>
                <option value="hot">hot</option>
                <option value="warm,hot">warm + hot</option>
            </select>
        </div>
        <div class="modal-row">
            <label>Cooldown (seconds)</label>
            <input id="fc-cooldown" class="form-input" type="number" min="0" step="60" value="300">
        </div>
        <div class="modal-row" id="fc-empty-row" style="display:none">
            <label style="color:var(--warning)">
                <input id="fc-allow-empty" type="checkbox"> Allow empty prefix (matches every file — destructive!)
            </label>
        </div>
        <div class="modal-actions">
            <button class="btn-secondary" onclick="closeFlushCache()">Cancel</button>
            <button class="btn-primary"  onclick="submitFlushCache()" id="fc-submit">Flush cache</button>
        </div>
        <div id="fc-result" class="modal-result" style="display:none"></div>
    </div>
</div>

<!-- ── Create volume dialog (UIP-1-1) ── -->
<div id="create-volume-modal" class="modal-backdrop hidden" onclick="if(event.target===this)closeCreateVolume()">
    <div class="modal-card">
        <h3>Create volume</h3>
        <div style="font-size:.78rem;color:var(--text-muted);margin-bottom:1rem">
            Posts <code>CreateVolumeRequest</code> to <code>/api/v1/volumes</code>.
            Only <code>name</code> is required; every other field falls back
            to the cluster default when left empty.
        </div>
        <div class="modal-row">
            <label>name</label>
            <input id="cv-name" class="form-input" type="text" placeholder="e.g. media">
        </div>
        <div class="modal-row">
            <label>chunk_size_mb</label>
            <input id="cv-chunk-size-mb" class="form-input" type="number" min="1" step="1" placeholder="(cluster default)">
        </div>
        <div style="font-weight:600;font-size:.8rem;margin:.6rem 0 .2rem">Replication</div>
        <div class="modal-row">
            <label>replication_hot</label>
            <input id="cv-replication-hot" class="form-input" type="number" min="0" max="9" step="1" placeholder="(default)">
        </div>
        <div class="modal-row">
            <label>replication_warm</label>
            <input id="cv-replication-warm" class="form-input" type="number" min="0" max="9" step="1" placeholder="(default)">
        </div>
        <div class="modal-row">
            <label>replication_cold</label>
            <input id="cv-replication-cold" class="form-input" type="number" min="0" max="9" step="1" placeholder="(default)">
        </div>
        <div class="modal-row">
            <label>anchor_datacenter</label>
            <input id="cv-anchor-datacenter" class="form-input" type="text" placeholder="e.g. dc-a">
        </div>
        <div class="modal-actions">
            <button class="btn-secondary" onclick="closeCreateVolume()">Cancel</button>
            <button class="btn-primary"  onclick="submitCreateVolume()" id="cv-submit">Create volume</button>
        </div>
        <div id="cv-result" class="modal-result" style="display:none"></div>
    </div>
</div>

<!-- ── Volume edit dialog (inline policy editor) ── -->
<div id="volume-edit-modal" class="modal-backdrop hidden" onclick="if(event.target===this)closeVolumeEdit()">
    <div class="modal-card">
        <h3>Edit volume policy</h3>
        <div style="font-size:.78rem;color:var(--text-muted);margin-bottom:1rem">
            Edits mandatory replicas (<code>set-mandatory</code>), the anchor
            policy (<code>set-anchor</code>), cache replicas
            (<code>set-cache</code>) and write safety
            (<code>set-write-safety</code>). Empty fields are left unchanged; each
            section posts only when you change it. Server-side validation
            (e.g. unsatisfiable tape / anchor) is shown below.
        </div>
        <div class="modal-row">
            <label>Volume ID</label>
            <input id="ve-volume-id" class="form-input" readonly>
        </div>
        <div style="font-weight:600;font-size:.8rem;margin:.6rem 0 .2rem">Mandatory replicas</div>
        <div class="modal-row">
            <label>hot</label>
            <input id="ve-mand-hot" class="form-input" type="number" min="0" max="9" step="1">
        </div>
        <div class="modal-row">
            <label>warm</label>
            <input id="ve-mand-warm" class="form-input" type="number" min="0" max="9" step="1">
        </div>
        <div class="modal-row">
            <label>cold</label>
            <input id="ve-mand-cold" class="form-input" type="number" min="0" max="9" step="1">
        </div>
        <div class="modal-row">
            <label>tape</label>
            <input id="ve-mand-tape" class="form-input" type="number" min="0" max="9" step="1">
        </div>
        <div style="font-weight:600;font-size:.8rem;margin:.6rem 0 .2rem">Anchor</div>
        <div class="modal-row">
            <label>datacenter</label>
            <input id="ve-anchor-dc" class="form-input" type="text" placeholder="e.g. dc-a">
        </div>
        <div class="modal-row">
            <label>required_tier</label>
            <select id="ve-anchor-tier" class="form-input">
                <option value="">(unchanged)</option>
                <option value="hot">hot</option>
                <option value="warm">warm</option>
                <option value="cold">cold</option>
                <option value="tape">tape</option>
            </select>
        </div>
        <div style="font-weight:600;font-size:.8rem;margin:.6rem 0 .2rem">Spill policy</div>
        <div class="modal-row">
            <label>allow spill</label>
            <select id="ve-spill-allow" class="form-input">
                <option value="">(inherit cluster default)</option>
                <option value="true">yes — spill allowed</option>
                <option value="false">no — fail the write instead</option>
            </select>
        </div>
        <div class="modal-row">
            <label>forbid tiers</label>
            <label style="font-weight:normal;display:flex;gap:.6rem;flex-wrap:wrap">
                <span><input id="ve-spill-forbid-hot"  type="checkbox"> hot</span>
                <span><input id="ve-spill-forbid-warm" type="checkbox"> warm</span>
                <span><input id="ve-spill-forbid-cold" type="checkbox"> cold</span>
                <span><input id="ve-spill-forbid-tape" type="checkbox"> tape</span>
            </label>
        </div>
        <div class="modal-row">
            <label>forbid datacenters</label>
            <input id="ve-spill-forbid-dcs" class="form-input" type="text" placeholder="comma-separated, e.g. dc-b, dc-c">
        </div>
        <div style="font-weight:600;font-size:.8rem;margin:.6rem 0 .2rem">Cache replicas</div>
        <div class="modal-row">
            <label>warm.min</label>
            <input id="ve-warm-min" class="form-input" type="number" min="0" max="9" step="1">
        </div>
        <div class="modal-row">
            <label>warm.max</label>
            <input id="ve-warm-max" class="form-input" type="number" min="0" max="9" step="1">
        </div>
        <div class="modal-row">
            <label>hot.min (advanced)</label>
            <input id="ve-hot-min" class="form-input" type="number" min="0" max="9" step="1">
        </div>
        <div class="modal-row">
            <label>hot.max (advanced)</label>
            <input id="ve-hot-max" class="form-input" type="number" min="0" max="9" step="1">
        </div>
        <div class="modal-row">
            <label>cold.min (advanced)</label>
            <input id="ve-cold-min" class="form-input" type="number" min="0" max="9" step="1">
        </div>
        <div class="modal-row">
            <label>cold.max (advanced)</label>
            <input id="ve-cold-max" class="form-input" type="number" min="0" max="9" step="1">
        </div>
        <div class="modal-row">
            <label>tape.min (advanced)</label>
            <input id="ve-tape-min" class="form-input" type="number" min="0" max="9" step="1">
        </div>
        <div class="modal-row">
            <label>tape.max (advanced)</label>
            <input id="ve-tape-max" class="form-input" type="number" min="0" max="9" step="1">
        </div>
        <div class="modal-row">
            <label>per_dc</label>
            <label style="font-weight:normal">
                <input id="ve-per-dc" type="checkbox"> apply caps per datacenter
            </label>
        </div>
        <div class="modal-row">
            <label>promote_reads</label>
            <input id="ve-promote-reads" class="form-input" type="number" min="1" step="1">
        </div>
        <div class="modal-row">
            <label>promote_window_secs</label>
            <input id="ve-promote-window" class="form-input" type="number" min="1" step="1">
        </div>
        <div class="modal-row">
            <label>cooldown_secs</label>
            <input id="ve-cooldown" class="form-input" type="number" min="0" step="1">
        </div>
        <div style="font-weight:600;font-size:.8rem;margin:.6rem 0 .2rem">Write safety</div>
        <div class="modal-row">
            <label>min_replicas_before_ack</label>
            <input id="ve-write-safety-min-replicas" class="form-input" type="number" min="1" step="1">
        </div>
        <div class="modal-row">
            <label>write_timeout_secs</label>
            <input id="ve-write-safety-timeout" class="form-input" type="number" min="1" step="1">
        </div>
        <div style="font-weight:600;font-size:.8rem;margin:.6rem 0 .2rem">Legacy policy migration</div>
        <div style="font-size:.78rem;color:var(--text-muted);margin-bottom:.4rem">
            One-shot: clones the legacy <code>replication</code> field into
            <code>mandatory_replicas</code>. Idempotent — safe to run again.
        </div>
        <div class="modal-row">
            <button class="btn-secondary" onclick="migrateVolumePolicy()" id="ve-migrate-btn">Migrate policy…</button>
        </div>
        <div id="ve-migrate-result" class="modal-result" style="display:none"></div>
        <div class="modal-actions">
            <button class="btn-secondary" onclick="closeVolumeEdit()">Cancel</button>
            <button class="btn-primary"  onclick="submitVolumeEdit()" id="ve-submit">Apply</button>
        </div>
        <div id="ve-result" class="modal-result" style="display:none"></div>
    </div>
</div>

<!-- AZ-1 — Schedules edit dialog -->
<div id="volume-schedules-modal" class="modal-backdrop hidden" onclick="if(event.target===this)closeVolumeSchedules()">
    <div class="modal-card">
        <h3>Edit volume schedules</h3>
        <div style="font-size:.78rem;color:var(--text-muted);margin-bottom:1rem">
            Per-volume background-job cadence overrides. Empty input
            preserves the current value; <code>0</code> clears the
            override (back to cluster default). Minimum: 60 s.
        </div>
        <div class="modal-row">
            <label>Volume ID</label>
            <input id="vs-volume-id" class="form-input" readonly>
        </div>
        <div class="modal-row">
            <label>scrub_interval_secs <span style="color:var(--warning);font-size:.7rem">(deprecated)</span></label>
            <input id="vs-scrub-secs" class="form-input" type="number" min="0" step="60" placeholder="(ignored — set cluster [scrub] block instead)">
            <div style="font-size:.7rem;color:var(--text-muted);margin-top:.2rem">
                Per-volume scrub cadence is no longer effective: data nodes scrub by disk,
                not by volume. Set cluster-wide values in Config → scrub.* instead.
            </div>
        </div>
        <div class="modal-row">
            <label>fsck_interval_secs</label>
            <input id="vs-fsck-secs" class="form-input" type="number" min="0" step="60" placeholder="(inherit; reserved for future fsck)">
        </div>
        <div class="modal-actions">
            <button class="btn-secondary" onclick="closeVolumeSchedules()">Cancel</button>
            <button class="btn-primary"  onclick="submitVolumeSchedules()" id="vs-submit">Apply</button>
        </div>
        <div id="vs-result" class="modal-result" style="display:none"></div>
    </div>
</div>

<!-- Unquarantine confirmation dialog -->
<div id="unquarantine-modal" class="modal-backdrop hidden" onclick="if(event.target===this)closeUnquarantineModal()">
    <div class="modal-card" style="max-width:32rem">
        <h3 style="color:var(--danger)">Unquarantine <span id="uq-hostname"></span></h3>
        <div style="font-size:.78rem;color:var(--text-muted);margin-bottom:.8rem">
            This re-admits the node to all serving paths (reads, replication source, allocate target).
            Do this only AFTER you have personally verified the chunk integrity or replaced the failing
            disk. Both fields below land in the audit log and are required.
        </div>
        <input type="hidden" id="uq-node-id">
        <div style="background:#b91c1c14;border:1px solid #b91c1c;border-radius:.4rem;padding:.5rem;margin-bottom:.8rem;font-size:.8rem">
            <div><b>Reason:</b> <span id="uq-reason"></span></div>
            <div><b>Triggering chunk:</b> <span id="uq-chunk" style="font-family:monospace;font-size:.7rem"></span></div>
            <div><b>Total faults:</b> <span id="uq-fault-count"></span></div>
            <div><b>Last summary:</b> <span id="uq-summary"></span></div>
        </div>
        <div class="modal-row">
            <label>operator_id (your identity)</label>
            <input id="uq-operator" class="form-input" oninput="onUnquarantineInput()" placeholder="alice / ticket-123 / on-call">
        </div>
        <div class="modal-row">
            <label>reason_acknowledged (≥ 16 chars; what you verified)</label>
            <input id="uq-ack" class="form-input" oninput="onUnquarantineInput()" placeholder="e.g. disk swapped + bitrot rescan clean">
        </div>
        <div class="modal-actions">
            <button class="btn-secondary" onclick="closeUnquarantineModal()">Cancel</button>
            <button class="btn-primary" style="background:#b91c1c" onclick="submitUnquarantine()" id="uq-submit" disabled>Unquarantine</button>
        </div>
        <div id="uq-result" class="modal-result" style="display:none"></div>
    </div>
</div>

<div class="footer">
    DTFS — Distributed Tiered Filesystem &nbsp;·&nbsp;
    <a href="/api/v1/cluster/status" style="color:var(--primary)">API</a> &nbsp;·&nbsp;
    <a href="{repo}/tree/master/docs" style="color:var(--primary)" target="_blank">Docs</a>
</div>

<script>
    // ── Utilities ────────────────────────────────────────────────────────────

    // Escape a value for interpolation into element text or into a quoted
    // HTML attribute. EVERY value that originates outside the manager
    // (file paths, hostnames, mount paths, volume ids, audit fields,
    // error strings) must pass through this before it reaches innerHTML.
    function esc(v) {{
        if (v === null || v === undefined) return '';
        return String(v).replace(/[&<>"']/g, c => (
            {{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[c]
        ));
    }}

    // Escape a value that is embedded in a single-quoted JS string which
    // itself sits inside a double-quoted HTML attribute, e.g.
    // onclick="fn('...')". JS-escape first, then HTML-escape, so the HTML
    // parser hands the JS parser a properly terminated string literal.
    // esc() alone is not enough here: a backslash would escape the closing
    // quote of the JS literal and let the argument run into code.
    function escArg(v) {{
        return esc(String(v === null || v === undefined ? '' : v)
            .replace(/\\/g, '\\\\').replace(/'/g, "\\'"));
    }}

    function fmtBytes(b) {{
        if (b === 0) return '0 B';
        const u = ['B','KB','MB','GB','TB','PB'];
        const i = Math.min(Math.floor(Math.log(b) / Math.log(1024)), u.length - 1);
        return (b / Math.pow(1024, i)).toFixed(i === 0 ? 0 : 1) + ' ' + u[i];
    }}

    function fmtBytesUnit(b) {{
        // Returns {{ val, unit }} for separate display
        if (b === 0) return {{ val: '0', unit: 'B/s' }};
        const u = ['B/s','KB/s','MB/s','GB/s','TB/s'];
        const i = Math.min(Math.floor(Math.log(b) / Math.log(1024)), u.length - 1);
        return {{ val: (b / Math.pow(1024, i)).toFixed(1), unit: u[i] }};
    }}

    function fmtPct(used, total) {{
        if (!total) return '—';
        return Math.round(used / total * 100) + '%';
    }}

    function barColor(pct) {{
        if (pct > 90) return 'var(--danger)';
        if (pct > 75) return 'var(--warning)';
        return null; // keep default class
    }}

    // set the innerHTML of an element while preserving the
    // window's scroll position. Replacing innerHTML on an in-view
    // element resets scrollTop to 0 when the new content is shorter
    // than the scroll offset, which looks to the user like the page
    // "jumped back". All periodic refreshes that swap a large element
    // (migrations list, audit log, nodes detail, topology) should
    // wrap their innerHTML write in this helper.
    function setHtmlPreserveScroll(el, html) {{
        if (!el) return;
        const sx = window.scrollX;
        const sy = window.scrollY;
        el.innerHTML = html;
        window.scrollTo(sx, sy);
    }}

    // ── LIS-35: session shared_secret for mutating REST ─────────────────────
    // Secret lives in sessionStorage only — never embedded in HTML/page source.
    // GETs stay unauthenticated; POSTs attach X-Dtfs-Shared-Secret when set.
    const DTFS_SECRET_KEY = 'dtfs.shared_secret';

    function getSharedSecret() {{
        try {{ return sessionStorage.getItem(DTFS_SECRET_KEY) || ''; }}
        catch (_) {{ return ''; }}
    }}

    function setSharedSecret(value) {{
        try {{
            const v = (value || '').trim();
            if (v) sessionStorage.setItem(DTFS_SECRET_KEY, v);
            else sessionStorage.removeItem(DTFS_SECRET_KEY);
        }} catch (_) {{}}
        updateAuthBadge();
        const status = document.getElementById('session-secret-status');
        if (status) {{
            status.textContent = getSharedSecret()
                ? 'Stored in sessionStorage for this browser tab.'
                : 'Not set - mutating POSTs will 401 if the cluster secret is configured.';
            status.style.color = getSharedSecret() ? 'var(--success)' : 'var(--text-muted)';
        }}
    }}

    function clearSharedSecret() {{
        setSharedSecret('');
        const input = document.getElementById('session-secret-input');
        if (input) input.value = '';
    }}

    function updateAuthBadge() {{
        const el = document.getElementById('auth-badge');
        if (!el) return;
        const set = !!getSharedSecret();
        el.textContent = set ? 'Secret: session' : 'Secret: not set';
        el.classList.toggle('auth-set', set);
        el.classList.toggle('auth-missing', !set);
        el.title = set
            ? 'X-Dtfs-Shared-Secret will be sent on mutating POSTs (sessionStorage). Click to change.'
            : 'Mutating POSTs get 401 if shared_secret is set. Click to store secret in sessionStorage.';
    }}

    function promptSharedSecret(reason) {{
        const msg = (reason || 'Cluster shared_secret')
            + '\n\nStored in sessionStorage only (cleared when the tab closes). Not written into the page.';
        const entered = window.prompt(msg, '');
        if (entered === null) return getSharedSecret();
        setSharedSecret(entered);
        return getSharedSecret();
    }}

    function saveSessionSecretFromSettings() {{
        const input = document.getElementById('session-secret-input');
        setSharedSecret(input ? input.value : '');
        if (input) input.value = '';
    }}

    /** Merge Content-Type + X-Dtfs-Shared-Secret when a session secret exists. */
    function authHeaders(extra) {{
        const h = Object.assign({{ 'Content-Type': 'application/json' }}, extra || {{}});
        const s = getSharedSecret();
        if (s) h['X-Dtfs-Shared-Secret'] = s;
        return h;
    }}

    /**
     * Human-readable 401/403 copy for operators (Recognition over Recall).
     * Returns null when status is not an auth denial.
     */
    function formatAuthError(status, bodyText, parsed) {{
        if (status === 401) {{
            return '401 Unauthorized - shared_secret required. '
                + 'Set Session secret (header badge or Settings), then retry.';
        }}
        if (status === 403) {{
            let detail = '';
            if (parsed && parsed.error) detail = ' (' + parsed.error + ')';
            else if (bodyText && bodyText.length < 200 && bodyText.indexOf('<') === -1)
                detail = ' (' + bodyText + ')';
            return '403 Forbidden - shared_secret mismatch' + detail + '. '
                + 'Update Session secret (header badge or Settings), then retry.';
        }}
        return null;
    }}

    /**
     * Mutating fetch: prompt once if secret empty, attach auth header, return
     * {{ resp, text, json, authError }}. Body is consumed once.
     */
    async function mutateFetch(url, options) {{
        if (!getSharedSecret()) {{
            promptSharedSecret('Mutating API call needs the cluster shared_secret.');
        }}
        const opts = Object.assign({{}}, options || {{}});
        opts.method = opts.method || 'POST';
        opts.headers = authHeaders(opts.headers);
        const resp = await fetch(url, opts);
        const text = await resp.text();
        let json = null;
        try {{ json = text ? JSON.parse(text) : null; }} catch (_) {{}}
        const authError = formatAuthError(resp.status, text, json);
        return {{ resp, text, json, authError }};
    }}

    function fmtDuration(secs) {{
        if (secs < 60) return secs.toFixed(1) + 's';
        if (secs < 3600) return Math.round(secs / 60) + 'm';
        return (secs / 3600).toFixed(1) + 'h';
    }}

    function timeAgo(ms) {{
        const diff = Date.now() - ms;
        const s = Math.floor(diff / 1000);
        if (s < 60)   return s + 's ago';
        if (s < 3600) return Math.floor(s / 60) + 'm ago';
        if (s < 86400) return Math.floor(s / 3600) + 'h ago';
        return Math.floor(s / 86400) + 'd ago';
    }}

    function healthClass(h) {{
        const m = (h || '').toLowerCase();
        if (m === 'healthy') return 'healthy';
        if (['suspect','degraded'].includes(m)) return 'degraded';
        if (['down','failed'].includes(m)) return 'danger';
        return '';
    }}

    // ── Stats fetch ──────────────────────────────────────────────────────────

    async function fetchStats() {{
        const [stats, nodes, migrations, cluster] = await Promise.all([
            fetch('/api/v1/stats').then(r => r.json()).catch(() => null),
            fetch('/api/v1/nodes').then(r => r.json()).catch(() => []),
            fetch('/api/v1/migrations').then(r => r.json()).catch(() => ({{count:0,tasks:[]}})),
            fetch('/api/v1/cluster/status').then(r => r.json()).catch(() => null),
        ]);

        document.getElementById('refresh-time').textContent =
            'Updated ' + new Date().toLocaleTimeString();

        if (!stats) {{
            setHealthBanner('unknown', 'Unknown', 'Could not reach API', '');
            renderProductionSignals(null, null, null);
            return;
        }}

        // ── Health banner
        const h = stats.health || 'unknown';
        // count managers + data only (infra_*); clients are
        // consumers and must not drag the ratio. Fall back to the legacy
        // all-node fields if an older manager hasn't shipped infra_*.
        const infraHealthy = stats.infra_healthy ?? stats.healthy_nodes;
        const infraTotal   = stats.infra_total   ?? stats.total_nodes;
        const detail = infraHealthy + '/' + infraTotal + ' nodes healthy';
        const right = 'Config v' + (stats.config_version || 0) +
                      ' · ' + (stats.file_count || 0) + ' files';
        setHealthBanner(h, h.toUpperCase(), detail, right);

        // ── Production signals (anchor / integrity / migrations)
        renderProductionSignals(stats, migrations, cluster);

        // ── Count cards
        document.getElementById('cnt-managers').textContent  = stats.manager_count   ?? '0';
        document.getElementById('cnt-datanodes').textContent = stats.data_node_count ?? '0';
        document.getElementById('cnt-disks').textContent     = stats.disk_count      ?? '0';
        document.getElementById('cnt-clients').textContent   = stats.client_count    ?? '0';

        // ── Raw capacity
        const rawTotal = stats.raw_total_bytes || 0;
        const rawUsed  = stats.raw_used_bytes  || 0;
        const rawFree  = rawTotal - rawUsed;
        const rawPct   = rawTotal ? Math.round(rawUsed / rawTotal * 100) : 0;
        document.getElementById('raw-total').textContent = fmtBytes(rawTotal);
        document.getElementById('raw-used').textContent  = fmtBytes(rawUsed);
        document.getElementById('raw-free').textContent  = fmtBytes(rawFree);
        document.getElementById('raw-pct').textContent   = rawPct + '%';
        const rawBar = document.getElementById('raw-bar');
        rawBar.style.width = rawPct + '%';
        if (barColor(rawPct)) rawBar.style.background = barColor(rawPct);

        // ── Net capacity
        const netUsable = stats.net_usable_bytes || 0;
        const netUsed   = stats.net_used_bytes   || 0;
        const netFree   = netUsable - netUsed;
        const netPct    = netUsable ? Math.round(netUsed / netUsable * 100) : 0;
        document.getElementById('net-usable').textContent = fmtBytes(netUsable);
        document.getElementById('net-used').textContent   = fmtBytes(netUsed);
        document.getElementById('net-free').textContent   = fmtBytes(netFree);
        document.getElementById('net-pct').textContent    = netPct + '%';
        const netBar = document.getElementById('net-bar');
        netBar.style.width = netPct + '%';
        if (barColor(netPct)) netBar.style.background = barColor(netPct);

        // ── logical claimed bytes vs phantom (metadata without on-disk data).
        const logical = stats.logical_bytes || 0;
        const phantom = stats.phantom_bytes_estimate || 0;
        document.getElementById('logical-bytes').textContent = fmtBytes(logical);
        const phantomEl = document.getElementById('phantom-bytes');
        phantomEl.textContent = fmtBytes(phantom);
        // Color the phantom number based on severity relative to logical:
        // 0 → muted; <10% logical → warning; ≥10% logical → danger.
        if (phantom === 0) {{
            phantomEl.style.color = 'var(--text-muted)';
        }} else if (logical > 0 && phantom / logical < 0.1) {{
            phantomEl.style.color = 'var(--warning)';
        }} else {{
            phantomEl.style.color = 'var(--danger)';
        }}

        // ── I/O
        const rd = fmtBytesUnit(stats.io_read_bytes_per_sec  || 0);
        const wr = fmtBytesUnit(stats.io_write_bytes_per_sec || 0);
        document.getElementById('io-read').textContent      = rd.val;
        document.getElementById('io-read-unit').textContent = rd.unit;
        document.getElementById('io-write').textContent     = wr.val;
        document.getElementById('io-write-unit').textContent = wr.unit;
        document.getElementById('file-count').textContent   = stats.file_count ?? 0;

        // ── Tier breakdown
        renderTierBreakdown(stats.tier_breakdown || []);

        // ── chunk placement distribution
        renderChunkPlacement(stats.chunk_placement);

        // ── Scrub results (one card per node + level)
        renderScrub(stats.scrub_results_by_node || []);

        // ── Nodes table
        renderNodes(nodes);
    }}

    // Home strip: surface production lies from real API fields only.
    // Clicking a chip navigates to the page that owns the drill-down.
    function renderProductionSignals(stats, migrations, cluster) {{
        const el = document.getElementById('signal-strip');
        if (!el) return;
        if (!stats) {{
            el.innerHTML = '<div class="placeholder">Signals unavailable (stats fetch failed).</div>';
            return;
        }}
        const tasks = (migrations && migrations.tasks) || [];
        const failedMig = tasks.filter(t => (t.status||'').toLowerCase() === 'failed').length;
        const activeMig = tasks.filter(t => {{
            const s = (t.status||'').toLowerCase();
            return s && s !== 'done' && s !== 'failed';
        }}).length;
        const cp = stats.chunk_placement || {{}};
        const missingChunks = (cluster && typeof cluster.files_missing_chunks === 'number')
            ? cluster.files_missing_chunks : null;
        const chips = [
            {{
                id: 'sig-anchor',
                val: stats.anchor_pending ?? 0,
                lbl: 'Anchor pending',
                hint: 'Files missing mandatory anchor copy',
                sev: (stats.anchor_pending || 0) > 0 ? 'warn' : 'ok',
                href: '/ui/volumes',
            }},
            {{
                id: 'sig-missing-files',
                val: missingChunks === null ? '—' : missingChunks,
                lbl: 'Files missing chunks',
                hint: missingChunks === null
                    ? 'cluster/status unavailable'
                    : 'Realized data loss (chunk bindings empty)',
                sev: (missingChunks || 0) > 0 ? 'danger' : (missingChunks === null ? 'ok' : 'ok'),
                href: '/ui/integrity',
            }},
            {{
                id: 'sig-quarantine',
                val: stats.quarantined_nodes ?? 0,
                lbl: 'Quarantined nodes',
                hint: 'Excluded from serving paths',
                sev: (stats.quarantined_nodes || 0) > 0 ? 'danger' : 'ok',
                href: '/ui/integrity',
            }},
            {{
                id: 'sig-mig-failed',
                val: failedMig,
                lbl: 'Migrations failed',
                hint: activeMig ? (activeMig + ' in flight') : 'Exhausted retries',
                sev: failedMig > 0 ? 'danger' : 'ok',
                href: '/ui/migrations',
            }},
            {{
                id: 'sig-undergoal',
                val: cp.undergoal ?? 0,
                lbl: 'Undergoal chunks',
                hint: 'Below volume replica goal',
                sev: (cp.undergoal || 0) > 0 ? 'warn' : 'ok',
                href: '/ui/',
            }},
            {{
                id: 'sig-missing-chunks',
                val: cp.missing ?? 0,
                lbl: 'Missing chunks',
                hint: 'Placement state: missing',
                sev: (cp.missing || 0) > 0 ? 'danger' : 'ok',
                href: '/ui/',
            }},
        ];
        el.innerHTML = chips.map(c => `
            <button type="button" class="signal-chip ${{c.sev}}" id="${{c.id}}"
                    title="${{c.hint}}" onclick="navigate('${{c.href}}')">
                <div class="signal-val">${{c.val}}</div>
                <div class="signal-lbl">${{c.lbl}}</div>
                <div class="signal-hint">${{c.hint}}</div>
            </button>`).join('');
    }}

    // render the chunk placement-state distribution
    // (`/api/v1/stats.chunk_placement`) as a stacked bar + colour legend.
    function renderChunkPlacement(cp) {{
        const el = document.getElementById('chunk-placement-content');
        if (!el) return;
        if (!cp) {{ el.innerHTML = '<div class="placeholder">No chunk-placement data</div>'; return; }}
        const buckets = [
            ['stable','Stable','var(--success)'],
            ['misplaced','Misplaced','var(--warning)'],
            ['undergoal','Undergoal','#e67e22'],
            ['overgoal','Overgoal','var(--primary)'],
            ['missing','Missing','var(--danger)'],
            ['pending_deletion','Pending deletion','var(--text-muted)'],
        ];
        const total = buckets.reduce((a, [k]) => a + (cp[k] || 0), 0) || 1;
        let bar = '<div style="display:flex;height:14px;border-radius:6px;overflow:hidden;margin-bottom:.6rem;background:var(--border)">';
        let legend = '<div style="display:flex;flex-wrap:wrap;gap:.75rem;font-size:.78rem">';
        for (const [k, label, color] of buckets) {{
            const n = cp[k] || 0;
            const pct = n / total * 100;
            if (n > 0) bar += `<div title="${{label}}: ${{n}}" style="width:${{pct}}%;background:${{color}}"></div>`;
            legend += `<span style="display:inline-flex;align-items:center;gap:.3rem">`
                + `<span style="width:.7rem;height:.7rem;border-radius:2px;background:${{color}};display:inline-block"></span>`
                + `${{label}}: <strong>${{n.toLocaleString()}}</strong></span>`;
        }}
        bar += '</div>'; legend += '</div>';
        el.innerHTML = bar + legend;
    }}

    function renderTierBreakdown(tiers) {{
        const tbody = document.getElementById('tier-table');
        if (!tbody) return;
        if (!tiers.length) {{
            tbody.innerHTML = '<tr><td colspan="5" class="placeholder">No tier data</td></tr>';
            return;
        }}
        // Order: hot → warm → cold → tape (display order, not alphabetical).
        const order = {{hot:0, warm:1, cold:2, tape:3}};
        const sorted = [...tiers].sort((a, b) =>
            (order[a.tier] ?? 99) - (order[b.tier] ?? 99));
        let html = '';
        for (const t of sorted) {{
            const total = t.total_bytes || 0;
            const used  = t.used_bytes  || 0;
            const pct   = total ? Math.round(used / total * 100) : 0;
            const color = barColor(pct) || 'var(--primary)';
            const badge = `<span class="badge badge-${{esc(t.tier)}}" style="text-transform:capitalize">${{esc(t.tier)}}</span>`;
            html += `<tr>
                <td style="padding:.4rem">${{badge}}</td>
                <td style="padding:.4rem">${{t.disks||0}}</td>
                <td style="padding:.4rem">${{fmtBytes(total)}}</td>
                <td style="padding:.4rem">${{fmtBytes(used)}}</td>
                <td style="padding:.4rem">
                    <div style="display:flex;align-items:center;gap:.5rem">
                        <div class="progress-bar" style="flex:1">
                            <div class="progress-fill" style="width:${{pct}}%;background:${{color}}"></div>
                        </div>
                        <span style="font-size:.75rem;color:var(--text-muted);min-width:3em;text-align:right">${{pct}}%</span>
                    </div>
                </td>
            </tr>`;
        }}
        tbody.innerHTML = html;
    }}

    function setHealthBanner(cls, label, detail, right) {{
        const banner = document.getElementById('health-banner');
        banner.className = 'health-banner ' + cls;
        document.getElementById('health-dot').className = 'health-dot ' + cls;
        document.getElementById('health-label').textContent = label;
        document.getElementById('health-detail').textContent = detail ? '· ' + detail : '';
        document.getElementById('health-right').textContent = right;
    }}

    function renderScrubCard(s) {{
        const when = new Date(s.completed_at_ms).toLocaleString();
        const ago  = timeAgo(s.completed_at_ms);
        const dur  = fmtDuration(s.duration_secs);
        const lvlBadge = s.level === 'deep'
            ? '<span class="badge badge-healthy">Deep</span>'
            : '<span class="badge badge-warm">Quick</span>';

        return `
            <div class="scrub-result">
            <div class="scrub-meta">
                <div class="scrub-meta-item">${{lvlBadge}}</div>
                <div class="scrub-meta-item"><span>Node:</span> ${{esc(s.node_id)}}</div>
                <div class="scrub-meta-item"><span>Disk:</span> ${{esc(s.disk_path)}}</div>
                <div class="scrub-meta-item"><span>Completed:</span> ${{when}} (${{ago}})</div>
                <div class="scrub-meta-item"><span>Duration:</span> ${{dur}}</div>
            </div>
            <div class="scrub-stats">
                <div class="scrub-stat">
                    <div class="stat-val">${{s.chunks_checked.toLocaleString()}}</div>
                    <div class="stat-lbl">Checked</div>
                </div>
                <div class="scrub-stat">
                    <div class="stat-val" style="color:var(--success)">${{s.chunks_healthy.toLocaleString()}}</div>
                    <div class="stat-lbl">Healthy</div>
                </div>
                <div class="scrub-stat corrupt">
                    <div class="stat-val">${{s.chunks_corrupt.toLocaleString()}}</div>
                    <div class="stat-lbl">Corrupt</div>
                </div>
                <div class="scrub-stat missing">
                    <div class="stat-val">${{s.chunks_missing.toLocaleString()}}</div>
                    <div class="stat-lbl">Missing</div>
                </div>
            </div>
            </div>`;
    }}

    // BK — list every retained scrub result (one card per node + level),
    // not just the single most-recent. Sort by node_id then level so the
    // Quick / Deep pair for each node renders together.
    function renderScrub(list) {{
        const el = document.getElementById('scrub-content');
        if (!list || !list.length) {{
            el.innerHTML = '<div class="scrub-placeholder">No scrub result recorded yet. ' +
                'Use "Scrub all" above to trigger one.</div>';
            return;
        }}
        const sorted = [...list].sort((a, b) =>
            (a.node_id - b.node_id) || String(a.level).localeCompare(String(b.level)));
        el.innerHTML = sorted.map(renderScrubCard).join('');
    }}

    // UIP-2-1 — cluster-wide scrub trigger. The API only accepts a
    // whole-cluster target ("*"/"all"); per-target scrub stays a 400 at
    // the API layer, so the webui never offers a per-target control.
    async function runScrubAll() {{
        const level = document.getElementById('scrub-all-level').value;
        const btn = document.getElementById('scrub-all-btn');
        const result = document.getElementById('scrub-all-result');
        btn.disabled = true; btn.textContent = 'Scrubbing…';
        result.style.display = 'block';
        result.className = '';
        result.textContent = 'Starting scrub…';
        try {{
            const {{ resp, json, authError }} = await mutateFetch(
                '/api/v1/scrub',
                {{ body: JSON.stringify({{ target: '*', target_type: 'all', level }}) }}
            );
            if (!resp.ok) {{
                result.textContent = authError
                    || ('Error ' + resp.status + ': ' + ((json && json.error) || JSON.stringify(json)));
            }} else {{
                result.textContent = 'Scrub ' + (json && json.status || 'started')
                    + ' (level: ' + (json && json.level || level) + ')';
            }}
        }} catch (e) {{
            result.textContent = 'Network error: ' + e;
        }} finally {{
            btn.disabled = false; btn.textContent = 'Scrub all';
        }}
    }}

    // stable sort + per-role grouping for the Home page "Data &
    // Manager Nodes" table. Order:
    //   1. role (manager → data → client → other), so each kind clusters
    //   2. datacenter/location (groups within DC together)
    //   3. hostname (deterministic within DC)
    // Without this the table follows whatever order /api/v1/nodes
    // returned (BTreeMap by u64 node_id — neither human-readable nor
    // stable across renames). On every refresh, rows visually swap.
    const NODE_ROLE_ORDER = {{ manager: 0, data: 1, client: 2 }};
    function sortNodesForHome(nodes) {{
        return [...nodes].sort((a, b) => {{
            const ra = NODE_ROLE_ORDER[(a.role||'').toLowerCase()] ?? 9;
            const rb = NODE_ROLE_ORDER[(b.role||'').toLowerCase()] ?? 9;
            if (ra !== rb) return ra - rb;
            const la = (a.location||'').toLowerCase();
            const lb = (b.location||'').toLowerCase();
            if (la !== lb) return la.localeCompare(lb);
            return (a.hostname||'').localeCompare(b.hostname||'');
        }});
    }}

    function renderNodes(nodes) {{
        const tbody = document.getElementById('nodes-table');
        if (!nodes || nodes.length === 0) {{
            tbody.innerHTML = '<tr><td colspan="10" class="placeholder">No nodes registered yet.</td></tr>';
            document.getElementById('nodes-summary').textContent = '';
            return;
        }}

        const sorted = sortNodesForHome(nodes);
        const healthy = sorted.filter(n => (n.health||'').toLowerCase() === 'healthy').length;
        document.getElementById('nodes-summary').textContent =
            healthy + '/' + sorted.length + ' healthy';

        // split the "Used" column into separate cells for bar
        // and value. Previously they shared one flex row inside a single
        // <td>, so variable text widths shifted the bars left/right and
        // they didn't line up vertically down the column.
        tbody.innerHTML = sorted.map(n => {{
            const h  = (n.health || 'unknown').toLowerCase();
            const hc = 'badge badge-' + h;
            const role = (n.role || 'data').toLowerCase();
            const rc = 'badge badge-' + role;
            const isManager = role === 'manager';
            const isClient  = role === 'client';
            const usedPct = n.total_capacity
                ? Math.round(n.used_capacity / n.total_capacity * 100) : 0;
            const barFill = usedPct > 90 ? 'var(--danger)'
                          : usedPct > 75 ? 'var(--warning)' : 'var(--primary)';
            // manager nodes have no disks of their own; the
            // utilization bar would always read 0% which is misleading.
            // Render an em-dash instead so it's visually obvious that
            // the metric doesn't apply.
            const usedBarCell = (isManager || isClient)
                ? `<td style="text-align:center;color:var(--text-muted)">—</td>`
                : `<td style="width:120px">
                       <div style="height:6px;background:var(--border);border-radius:3px;width:100%">
                           <div style="height:100%;border-radius:3px;background:${{barFill}};width:${{usedPct}}%"></div>
                       </div>
                   </td>`;
            const usedValCell = (isManager || isClient)
                ? `<td style="font-size:.78rem;white-space:nowrap;color:var(--text-muted)">—</td>`
                : `<td style="font-size:.78rem;white-space:nowrap">${{fmtBytes(n.used_capacity||0)}} <span style="color:var(--text-muted)">(${{usedPct}}%)</span></td>`;
            const capCell = (isManager || isClient)
                ? `<td style="color:var(--text-muted)">—</td>`
                : `<td>${{fmtBytes(n.total_capacity||0)}}</td>`;
            const diskCell = (isManager || isClient)
                ? `<td style="color:var(--text-muted)">—</td>`
                : `<td>${{n.disk_count||0}}</td>`;
            // Show full node_id (no slice). Location + Version columns
            // get explicit min-widths (×4 + ×2 of the previous implicit
            // width) so the full location string and the version+build
            // pair fit without truncation or wrapping.
            const versionText = (n.version || n.build_date)
                ? `${{esc(n.version || '—')}}${{n.build_date ? ' / ' + esc(n.build_date) : ''}}`
                : '—';
            // CF-4c — a manager that's not a raft voter/learner is
            // configured/registered but never joined; flag it so it doesn't
            // masquerade as a plain Healthy member (the production dc1 case).
            const unjoinedBadge = (isManager && n.raft_member === false)
                ? '<span class="badge badge-unjoined" style="margin-right:.4rem" title="Manager is not a raft member (voter or learner)">UNJOINED</span>'
                : '';
            return `<tr>
                <td style="font-size:.75rem;color:var(--text-muted);font-family:monospace;white-space:nowrap">
                    ${{esc(n.node_id||'—')}}</td>
                <td><b>${{esc(n.hostname||'—')}}</b></td>
                <td><span class="${{esc(rc)}}">${{esc(role)}}</span></td>
                <td style="font-size:.78rem;color:var(--text-muted);white-space:nowrap">${{esc(n.location||'—')}}</td>
                <td style="font-size:.78rem;color:var(--text-muted);white-space:nowrap">${{esc(versionText)}}</td>
                ${{diskCell}}
                ${{capCell}}
                ${{usedBarCell}}
                ${{usedValCell}}
                <td>${{unjoinedBadge}}<span class="${{esc(hc)}}">${{esc(n.health||'unknown')}}</span></td>
            </tr>`;
        }}).join('');
    }}

    // ── SPA Router ───────────────────────────────────────────────────────────

    // Map dotted route → which top-level nav group should light up.
    // Lets the active highlight cascade onto the group button when a
    // child route is active.
    const NAV_GROUP_OF = {{
        '/ui/topology':   'cluster',
        '/ui/nodes':      'cluster',
        '/ui/integrity':  'cluster',
        '/ui/audit':      'cluster',
        '/ui/config':     'cluster',
        '/ui/settings':   'cluster',
        '/ui/volumes':    'data',
        '/ui/explorer':   'data',
        '/ui/migrations': 'data',
    }};

    const ROUTES = {{
        '/ui':            renderHome,
        '/ui/':           renderHome,
        '/ui/topology':   renderTopology,
        '/ui/nodes':      renderNodesList,
        '/ui/integrity':  renderIntegrityFeed,
        '/ui/volumes':    renderVolumes,
        '/ui/explorer':   renderExplorer,
        '/ui/migrations': renderMigrations,
        '/ui/audit':      renderAudit,
        '/ui/config':     renderConfig,
        '/ui/settings':   renderSettings,
    }};

    // Integrity feed page. Lists every integrity-related
    // audit row (quarantine, unquarantine, scrub faults, read mismatch,
    // orphan in-state-missing). Polls every 5s while the page is open.
    let _integrityTimer = null;
    async function renderIntegrityFeed() {{
        document.getElementById('page-content').innerHTML = `
            <div class="page-header">
                <h2>Integrity</h2>
                <p class="page-desc">
                    Data-integrity events across the cluster. Quarantined nodes,
                    scrub faults, read-hash mismatches, orphan reaps. Click a row to jump
                    to the node detail panel.
                </p>
            </div>
            <div id="integrity-summary" style="margin-bottom:.6rem;font-size:.85rem;color:var(--text-muted)">Loading…</div>
            <div class="table-card"><table>
                <thead><tr>
                    <th style="min-width:11rem">Time (UTC)</th>
                    <th style="min-width:9rem">Action</th>
                    <th style="min-width:9rem">Actor</th>
                    <th style="min-width:9rem">Target</th>
                    <th>Result / Summary</th>
                </tr></thead>
                <tbody id="integrity-body"><tr><td colspan="5" class="placeholder">Fetching…</td></tr></tbody>
            </table></div>`;
        const tick = async () => {{
            const rows = await fetch('/api/v1/integrity/faults?limit=200')
                .then(r => r.json()).catch(() => []);
            const body = document.getElementById('integrity-body');
            const sum  = document.getElementById('integrity-summary');
            if (!rows.length) {{
                body.innerHTML = '<tr><td colspan="5" class="placeholder">No integrity events. Cluster is clean.</td></tr>';
                sum.textContent = '0 events';
                return;
            }}
            sum.textContent = rows.length + ' event(s); newest first';
            body.innerHTML = rows.map(r => {{
                const ts = new Date((r.timestamp||0)*1000).toISOString().replace('T',' ').replace(/\..*/, '');
                const action = r.action||'';
                const isQuar = action.startsWith('node.quarantine');
                const isUnq  = action.startsWith('node.unquarantine');
                const rowStyle = isQuar ? 'background:#b91c1c14' : (isUnq ? 'background:#22c55e10' : '');
                return `<tr style="${{rowStyle}}">
                    <td style="font-family:monospace;font-size:.78rem">${{ts}}</td>
                    <td><b>${{esc(action)}}</b></td>
                    <td style="font-size:.78rem;color:var(--text-muted)">${{esc(r.actor||'—')}}</td>
                    <td style="font-size:.78rem;font-family:monospace">${{esc(r.target||'—')}}</td>
                    <td style="font-size:.78rem">${{esc(r.summary||r.result||'—')}}</td>
                </tr>`;
            }}).join('');
        }};
        await tick();
        if (refreshTimer) {{ clearInterval(refreshTimer); }}
        refreshTimer = setInterval(tick, 5000);
    }}

    function toggleNavGroup(ev, name) {{
        ev.stopPropagation();
        document.querySelectorAll('.nav .nav-group').forEach(g => {{
            if (g.dataset.group === name) {{
                g.classList.toggle('open');
            }} else {{
                g.classList.remove('open');
            }}
        }});
    }}
    document.addEventListener('click', () => {{
        document.querySelectorAll('.nav .nav-group.open').forEach(g =>
            g.classList.remove('open'));
    }});
    window.toggleNavGroup = toggleNavGroup;

    let refreshTimer = null;

    function navigate(path) {{
        history.pushState({{}}, '', path);
        route();
    }}

    function route() {{
        const path = window.location.pathname.replace(/\/$/, '') || '/ui';
        const normPath = path === '' ? '/ui' : path;

        // Update active nav link (top-level + dropdown children).
        document.querySelectorAll('.nav a').forEach(a => {{
            const href = (a.getAttribute('href')||'').replace(/\/$/, '');
            a.classList.toggle('active', href === normPath);
        }});

        // Cascade active highlight onto the parent nav-group button.
        const groupName = NAV_GROUP_OF[normPath] || null;
        document.querySelectorAll('.nav .nav-group').forEach(g => {{
            g.classList.toggle('active', g.dataset.group === groupName);
            // Close any open dropdowns when route changes.
            g.classList.remove('open');
        }});

        // Clear existing auto-refresh
        if (refreshTimer) {{ clearInterval(refreshTimer); refreshTimer = null; }}

        const renderFn = ROUTES[normPath] || renderHome;
        renderFn();
    }}

    // Intercept nav clicks for SPA navigation
    document.querySelectorAll('.nav a[href^="/ui"]').forEach(a => {{
        a.addEventListener('click', e => {{
            e.preventDefault();
            navigate(a.getAttribute('href'));
        }});
    }});

    window.addEventListener('popstate', () => route());

    // ── Home page ─────────────────────────────────────────────────────────────

    function renderHome() {{
        document.getElementById('page-content').innerHTML = `
        <div class="section-title">Production signals</div>
        <div class="signal-strip" id="signal-strip">
            <div class="placeholder">Loading signals…</div>
        </div>
        <div class="counts-row">
            <div class="count-card">
                <div class="count-icon managers">⬡</div>
                <div class="count-body"><h4 id="cnt-managers">—</h4><p>Manager nodes</p></div>
            </div>
            <div class="count-card">
                <div class="count-icon datanodes">◈</div>
                <div class="count-body"><h4 id="cnt-datanodes">—</h4><p>Data nodes</p></div>
            </div>
            <div class="count-card">
                <div class="count-icon disks">▨</div>
                <div class="count-body"><h4 id="cnt-disks">—</h4><p>Disks registered</p></div>
            </div>
            <div class="count-card">
                <div class="count-icon clients">⌗</div>
                <div class="count-body"><h4 id="cnt-clients">—</h4><p>FUSE clients</p></div>
            </div>
        </div>
        <div class="metrics-row">
            <div class="metric-card">
                <h3>Raw Capacity</h3>
                <div class="metric-row"><span class="metric-label">Total</span><span class="metric-value" id="raw-total">—</span></div>
                <div class="metric-row"><span class="metric-label">Used</span><span class="metric-value" id="raw-used">—</span></div>
                <div class="progress-wrap">
                    <div style="display:flex;justify-content:space-between;margin-bottom:.3rem">
                        <span style="font-size:.75rem;color:var(--text-muted)">Utilisation</span>
                        <span style="font-size:.75rem;color:var(--text-muted)" id="raw-pct">—</span>
                    </div>
                    <div class="progress-bar"><div class="progress-fill primary" id="raw-bar" style="width:0%"></div></div>
                </div>
                <div class="metric-row" style="margin-top:.25rem"><span class="metric-label">Free</span><span class="metric-value" id="raw-free" style="color:var(--success)">—</span></div>
            </div>
            <!-- drop the "After replication factor (÷ hot RF)"
                 subheader (the math is per-tier now anyway, and the title
                 already conveys the intent). Card body structurally mirrors
                 Raw Capacity so the rows line up at the same vertical
                 baseline across both cards. -->
            <div class="metric-card">
                <h3>Net Usable Capacity</h3>
                <div class="metric-row"><span class="metric-label">Total</span><span class="metric-value" id="net-usable">—</span></div>
                <div class="metric-row"><span class="metric-label">Used</span><span class="metric-value" id="net-used">—</span></div>
                <div class="progress-wrap">
                    <div style="display:flex;justify-content:space-between;margin-bottom:.3rem">
                        <span style="font-size:.75rem;color:var(--text-muted)">Utilisation</span>
                        <span style="font-size:.75rem;color:var(--text-muted)" id="net-pct">—</span>
                    </div>
                    <div class="progress-bar"><div class="progress-fill success" id="net-bar" style="width:0%"></div></div>
                </div>
                <div class="metric-row" style="margin-top:.25rem"><span class="metric-label">Free</span><span class="metric-value" id="net-free" style="color:var(--success)">—</span></div>
                <!-- show logical bytes claimed by file metadata vs net actually on disk. -->
                <div class="metric-row" style="margin-top:.6rem;border-top:1px solid var(--border-color);padding-top:.4rem"><span class="metric-label" title="Sum of file sizes in metadata (replicas deduped). Should match Net Used in a healthy cluster.">Logical</span><span class="metric-value" id="logical-bytes">—</span></div>
                <div class="metric-row"><span class="metric-label" title="Logical − Net Used. Non-zero indicates file metadata referencing chunks that aren't on any disk (data-node wipe, ENOSPC unlink cascade, etc.). A scrub-driven GC would reclaim these.">Phantom</span><span class="metric-value" id="phantom-bytes" style="color:var(--text-muted)">—</span></div>
            </div>
            <div class="metric-card">
                <h3>I/O Throughput</h3>
                <div class="io-row">
                    <div class="io-item">
                        <div class="io-dir">↓ Read</div>
                        <div><span class="io-val" id="io-read">0</span><span class="io-unit" id="io-read-unit">B/s</span></div>
                    </div>
                    <div class="io-item">
                        <div class="io-dir">↑ Write</div>
                        <div><span class="io-val" id="io-write">0</span><span class="io-unit" id="io-write-unit">B/s</span></div>
                    </div>
                </div>
                <div style="margin-top:1.25rem;font-size:.72rem;color:var(--text-muted)">Files tracked</div>
                <div style="font-size:1.4rem;font-weight:700;margin-top:.15rem" id="file-count">—</div>
                <div style="font-size:.72rem;color:var(--text-muted);margin-top:.1rem">across all volumes</div>
            </div>
        </div>
        <div class="metric-card" style="margin-top:1rem">
            <h3>Tier Distribution</h3>
            <div style="font-size:.72rem;color:var(--text-muted);margin-bottom:.75rem">
                Capacity and usage per storage tier across all data nodes
            </div>
            <table style="width:100%;font-size:.85rem">
                <thead>
                    <tr style="text-align:left;border-bottom:1px solid var(--border)">
                        <th style="padding:.4rem">Tier</th>
                        <th style="padding:.4rem">Disks</th>
                        <th style="padding:.4rem">Total</th>
                        <th style="padding:.4rem">Used</th>
                        <th style="padding:.4rem;width:40%">Utilisation</th>
                    </tr>
                </thead>
                <tbody id="tier-table"><tr><td colspan="5" class="placeholder">Loading…</td></tr></tbody>
            </table>
        </div>
        <div class="metric-card" style="margin-top:1rem">
            <h3>Chunk Placement</h3>
            <div style="font-size:.72rem;color:var(--text-muted);margin-bottom:.75rem">
                Every logical chunk classified by its replica placement vs the volume goal
            </div>
            <div id="chunk-placement-content"><div class="placeholder">Loading…</div></div>
        </div>
        <div class="scrub-card">
            <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:.5rem">
                <h3 style="margin:0">Last Scrub</h3>
                <div style="display:flex;align-items:center;gap:.4rem">
                    <select id="scrub-all-level" class="form-input" style="width:auto">
                        <option value="quick">Quick</option>
                        <option value="deep">Deep</option>
                    </select>
                    <button class="btn-primary" onclick="runScrubAll()" id="scrub-all-btn">Scrub all</button>
                </div>
            </div>
            <div id="scrub-all-result" style="display:none;margin:.5rem 0;font-size:.8rem"></div>
            <div id="scrub-content"><div class="scrub-placeholder">No scrub result recorded yet.</div></div>
        </div>
        <div class="table-card">
            <div class="table-card-header">
                <h3>Data &amp; Manager Nodes</h3>
                <span style="font-size:.75rem;color:var(--text-muted)" id="nodes-summary"></span>
            </div>
            <table><thead><tr>
                <th style="min-width:14rem">ID</th>
                <th>Hostname</th>
                <th>Role</th>
                <th style="min-width:16rem">Location</th>
                <th style="min-width:10rem">Version</th>
                <th>Disks</th>
                <th>Capacity</th>
                <th style="width:120px">Utilization</th>
                <th>Used</th>
                <th>Health</th>
            </tr></thead>
            <tbody id="nodes-table"><tr><td colspan="10" class="placeholder">Fetching node list…</td></tr></tbody></table>
        </div>`;
        fetchStats();
        refreshTimer = setInterval(fetchStats, 8000);
    }}

    // ── Topology page ─────────────────────────────────────────────────────────

    async function renderTopology() {{
        document.getElementById('page-content').innerHTML =
            '<div class="page-header"><h2>Topology</h2><p class="page-desc">Region → DC → Rack → Host hierarchy</p></div>' +
            '<div id="topo-tree" class="topo-root"><div class="placeholder">Loading topology…</div></div>';
        await refreshTopology();
        // refresh ONLY the topo-tree element on each tick (not
        // the whole page). The previous code recursively called
        // renderTopology which replaces page-content innerHTML every
        // 15s, blowing away scroll position and any user interaction
        // state. Also: that previous self-call set a NEW setInterval
        // without clearing the old one, so timers stacked up over time.
        refreshTimer = setInterval(refreshTopology, 15000);
    }}

    async function refreshTopology() {{
        const topo = document.getElementById('topo-tree');
        if (!topo) return; // user navigated away mid-fetch
        const nodes = await fetch('/api/v1/nodes').then(r => r.json()).catch(() => []);
        if (!document.getElementById('topo-tree')) return; // checked again after await

        // Group nodes: region → dc → rack → [nodes]
        const tree = {{}};
        for (const n of nodes) {{
            const loc = n.location || '—/—/—/—';
            const [region, dc, rack] = loc.split('/');
            if (!tree[region]) tree[region] = {{}};
            if (!tree[region][dc]) tree[region][dc] = {{}};
            if (!tree[region][dc][rack]) tree[region][dc][rack] = [];
            tree[region][dc][rack].push(n);
        }}

        if (nodes.length === 0) {{
            topo.innerHTML = '<div class="placeholder">No nodes registered yet.</div>';
            return;
        }}

        // Helper: sorted entries with locale-aware ASC ordering. Used to
        // render Region → DC → Rack → Host predictably regardless of the
        // order nodes happen to register in.
        const sortedEntries = (obj) =>
            Object.entries(obj).sort(([a], [b]) =>
                a.localeCompare(b, undefined, {{sensitivity: 'base', numeric: true}}));

        let html = '';
        for (const [region, dcs] of sortedEntries(tree)) {{
            html += `<div class="topo-region"><div class="topo-region-label">🌐 Region: <b>${{esc(region)}}</b></div>`;
            for (const [dc, racks] of sortedEntries(dcs)) {{
                // wrap all racks of this DC in a horizontal flex
                // container (.topo-racks); each .topo-rack inside is a
                // vertical column of hosts. Yields the visual layout:
                //
                //   [Rack A]   [Rack B]   [Rack C]
                //   host-1      host-1     host-1
                //   host-2      host-2     host-2
                //
                // Matches operator mental model: racks are physical
                // siblings; hosts within a rack are stacked.
                html += `<div class="topo-dc"><div class="topo-dc-label">🏢 Datacenter: <b>${{esc(dc)}}</b></div><div class="topo-racks">`;
                for (const [rack, rnodes] of sortedEntries(racks)) {{
                    html += `<div class="topo-rack"><div class="topo-rack-label">🗄 Rack: <b>${{esc(rack)}}</b></div><div class="topo-nodes">`;
                    // Sort hosts (nodes) inside the rack by hostname ASC.
                    const sortedNodes = [...rnodes].sort((a, b) =>
                        (a.hostname || '').localeCompare(b.hostname || '', undefined,
                            {{sensitivity: 'base', numeric: true}}));
                    for (const n of sortedNodes) {{
                        const h = (n.health||'unknown').toLowerCase();
                        const role = (n.role||'data').toLowerCase();
                        html += `<div class="topo-node">
                            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:.4rem">
                                <b>${{esc(n.hostname||'—')}}</b>
                                <span class="badge badge-${{esc(h)}}">${{esc(n.health||'unknown')}}</span>
                            </div>
                            <div style="font-size:.75rem;color:var(--text-muted);margin-bottom:.3rem">
                                <span class="badge badge-${{esc(role)}}">${{esc(role)}}</span>
                                &nbsp; ${{n.disk_count||0}} disk${{n.disk_count!==1?'s':''}}
                            </div>
                            <div style="font-size:.75rem;color:var(--text-muted)">
                                Cap: ${{fmtBytes(n.total_capacity||0)}} · Used: ${{fmtBytes(n.used_capacity||0)}}
                            </div>
                        </div>`;
                    }}
                    html += '</div></div>';
                }}
                html += '</div></div>';
            }}
            html += '</div>';
        }}
        // preserve scroll position across refresh. innerHTML
        // assignment moves scrollTop back to 0 on long pages, making
        // the page look like it "jumped back" to the user. Snapshot
        // before, restore after.
        const savedScroll = window.scrollY;
        topo.innerHTML = html;
        window.scrollTo(0, savedScroll);
    }}

    // ── Nodes list page ────────────────────────────────────────────────────────

    async function renderNodesList() {{
        document.getElementById('page-content').innerHTML =
            '<div class="page-header"><h2>Nodes</h2><p class="page-desc">All cluster nodes with disk detail</p></div>' +
            '<div id="nodes-detail-list"><div class="placeholder">Loading…</div></div>';
        await refreshNodesList();
        refreshTimer = setInterval(refreshNodesList, 10000);
    }}

    async function refreshNodesList() {{
        const nodes = await fetch('/api/v1/nodes').then(r => r.json()).catch(() => []);
        const el = document.getElementById('nodes-detail-list');
        if (!el) return;
        if (!nodes || nodes.length === 0) {{
            el.innerHTML = '<div class="placeholder">No nodes registered.</div>';
            return;
        }}

        // preserve scroll position across the periodic refresh
        // (was setInterval 10s). innerHTML reset of the whole list resets
        // scrollTop to 0 which felt like a "jump back" on long node lists.
        const savedScroll = window.scrollY;

        // Use the same role-aware sort as the Home page so the user
        // sees a consistent order on both pages: manager → data → client,
        // then by location then hostname.
        const sorted = sortNodesForHome(nodes);

        el.innerHTML = sorted.map(n => {{
            const h = (n.health||'unknown').toLowerCase();
            const role = (n.role||'data').toLowerCase();
            const isManager = role === 'manager';
            const isClient  = role === 'client';
            const isStorageRole = !isManager && !isClient;
            const pct = n.total_capacity ? Math.round(n.used_capacity/n.total_capacity*100) : 0;
            const barCol = pct>90?'var(--danger)':pct>75?'var(--warning)':'var(--primary)';
            const isMaintenance = h === 'maintenance';

            // manager + client nodes have no disks of their own;
            // showing a "Used / Utilisation" row that always reads 0
            // (or — / —) is misleading clutter. For those roles, drop the
            // size/util rows entirely. For data nodes keep the full
            // breakdown.
            const sizeMetaRows = isStorageRole
                ? `<div class="node-meta-item"><span>Total</span>${{fmtBytes(n.total_capacity||0)}}</div>
                   <div class="node-meta-item"><span>Used</span>${{fmtBytes(n.used_capacity||0)}} (${{pct}}%)</div>`
                : '';
            const utilBar = isStorageRole
                ? `<div style="margin:.5rem 0">
                       <div class="progress-bar" style="height:8px">
                           <div style="height:100%;border-radius:4px;background:${{barCol}};width:${{pct}}%"></div>
                       </div>
                   </div>`
                : '';

            // per-disk table for data nodes. Lets the operator
            // spot a single full disk (which would otherwise be hidden
            // behind the node-level aggregate, e.g. data-primary01 with
            // 4 cold disks where one fills to 100% while the others stay
            // at 2% — the symptom that drove the earlier fix).
            const disks = Array.isArray(n.disks) ? n.disks : [];
            const diskTable = isStorageRole && disks.length > 0
                ? `<div style="margin-top:.5rem">
                       <div style="font-size:.75rem;color:var(--text-muted);margin-bottom:.3rem;text-transform:uppercase;letter-spacing:.04em">Disks (${{disks.length}})</div>
                       <table style="width:100%;font-size:.78rem;border-collapse:collapse">
                           <thead>
                               <tr style="text-align:left;color:var(--text-muted);border-bottom:1px solid var(--border)">
                                   <th style="padding:.25rem .4rem">ID</th>
                                   <th style="padding:.25rem .4rem">Tier</th>
                                   <th style="padding:.25rem .4rem">Mount</th>
                                   <th style="padding:.25rem .4rem">Total</th>
                                   <th style="padding:.25rem .4rem">Used</th>
                                   <th style="padding:.25rem .4rem;width:140px">Utilisation</th>
                                   <th style="padding:.25rem .4rem">Status</th>
                               </tr>
                           </thead>
                           <tbody>
                               ${{disks.map(d => {{
                                   const dpct = d.total_bytes ? Math.round(d.used_bytes/d.total_bytes*100) : 0;
                                   const dcol = dpct>90?'var(--danger)':dpct>75?'var(--warning)':'var(--primary)';
                                   const dstatus = (d.status||'healthy').toLowerCase();
                                   return `<tr>
                                       <td style="padding:.25rem .4rem;font-family:monospace">${{esc(d.disk_id)}}</td>
                                       <td style="padding:.25rem .4rem"><span class="badge badge-${{esc((d.tier||'').toLowerCase())}}">${{esc(d.tier||'—')}}</span></td>
                                       <td style="padding:.25rem .4rem;font-family:monospace;color:var(--text-muted);max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${{esc(d.mount_path||'')}}">${{esc(d.mount_path||'—')}}</td>
                                       <td style="padding:.25rem .4rem">${{fmtBytes(d.total_bytes||0)}}</td>
                                       <td style="padding:.25rem .4rem">${{fmtBytes(d.used_bytes||0)}}</td>
                                       <td style="padding:.25rem .4rem">
                                           <div style="display:flex;align-items:center;gap:.5rem">
                                               <div style="flex:1;height:5px;background:var(--border);border-radius:3px;min-width:60px">
                                                   <div style="height:100%;border-radius:3px;background:${{dcol}};width:${{dpct}}%"></div>
                                               </div>
                                               <span style="font-size:.72rem;color:var(--text-muted);min-width:2.5em;text-align:right">${{dpct}}%</span>
                                           </div>
                                       </td>
                                       <td style="padding:.25rem .4rem"><span class="badge badge-${{esc(dstatus)}}">${{esc(d.status||'—')}}</span></td>
                                   </tr>`;
                               }}).join('')}}
                           </tbody>
                       </table>
                   </div>`
                : '';

            // show the manager's Raft-leader status, render
            // a client's source IP (clients have no gRPC listener), and
            // surface last_seen on every node so operators can see how
            // fresh the heartbeat is at a glance.
            const leaderBadge = (isManager && n.is_leader)
                ? '<span class="badge badge-leader" style="margin-right:.4rem" title="Current Raft leader">LEADER</span>'
                : '';
            // CF-4c — manager registered/configured but not a raft member
            // (voter or learner). The production dc1 symptom rendered such a
            // node as plain "Healthy"; flag it explicitly.
            const unjoinedBadge = (isManager && n.raft_member === false)
                ? '<span class="badge badge-unjoined" style="margin-right:.4rem" title="Manager is not a raft member (voter or learner)">UNJOINED</span>'
                : '';
            const addrRow = isClient
                ? `<div class="node-meta-item"><span>Client IP</span>${{esc(n.client_ip || '—')}}</div>`
                : `<div class="node-meta-item"><span>gRPC</span>${{esc(n.grpc_addr||'—')}}</div>`;
            const lastSeenRow = n.last_seen_ms
                ? `<div class="node-meta-item"><span>Last seen</span>${{timeAgo(n.last_seen_ms)}}</div>`
                : `<div class="node-meta-item"><span>Last seen</span>—</div>`;
            // show binary version + build date. Empty values
            // mean the node pre-dates that field or wasn't queried directly
            // (older clients, peer managers).
            const versionRow = (n.version || n.build_date)
                ? `<div class="node-meta-item"><span>Version</span>${{esc(n.version || '—')}}${{n.build_date ? ' / ' + esc(n.build_date) : ''}}</div>`
                : '';
            // Quarantined badge + details panel. The
            // operator never needs to curl: the reason, triggering
            // chunk, fault count, and unquarantine action all live
            // in the card.
            const isQuarantined = !!n.quarantine;
            const quarBadge = isQuarantined
                ? `<span class="badge badge-quarantined" title="Click to inspect">QUARANTINED</span>`
                : '';
            const quarPanel = isQuarantined ? `
                <div style="margin-top:.6rem;padding:.6rem;border:1px solid #b91c1c;border-radius:.4rem;background:#b91c1c14">
                    <div style="font-weight:700;color:var(--danger);margin-bottom:.3rem">
                        Integrity fault — node excluded from all serving paths.
                    </div>
                    <div class="node-meta-item"><span>Reason</span><b>${{esc(n.quarantine.reason)}}</b></div>
                    ${{n.quarantine.triggering_chunk_id ? `<div class="node-meta-item"><span>Triggering chunk</span><span style="font-family:monospace;font-size:.75rem">${{esc(n.quarantine.triggering_chunk_id)}}</span></div>` : ''}}
                    <div class="node-meta-item"><span>Since</span>${{new Date(n.quarantine.since_ms).toISOString()}}</div>
                    <div class="node-meta-item"><span>Last fault</span>${{new Date(n.quarantine.last_fault_at_ms).toISOString()}}</div>
                    <div class="node-meta-item"><span>Total faults</span>${{n.quarantine.fault_count_total||0}}</div>
                    <div class="node-meta-item"><span>Summary</span>${{esc(n.quarantine.last_fault_summary||'—')}}</div>
                    <div style="margin-top:.6rem;display:flex;gap:.4rem">
                        <button class="btn-primary" style="background:#b91c1c"
                            onclick="openUnquarantineModal('${{escArg(n.node_id)}}','${{escArg(n.hostname||'')}}','${{escArg(n.quarantine.reason)}}','${{escArg(n.quarantine.triggering_chunk_id||'')}}','${{escArg(n.quarantine.fault_count_total||0)}}','${{escArg(n.quarantine.last_fault_summary||'')}}')">
                            Unquarantine…
                        </button>
                    </div>
                </div>` : '';
            return `<div class="node-card">
                <div class="node-card-header">
                    <div>
                        <span class="badge badge-${{esc(role)}}" style="margin-right:.4rem">${{esc(role)}}</span>
                        ${{leaderBadge}}
                        ${{unjoinedBadge}}
                        <b style="font-size:1rem">${{esc(n.hostname||'—')}}</b>
                        <span style="font-size:.78rem;color:var(--text-muted);margin-left:.5rem;font-family:monospace">
                            id:${{esc((n.node_id||'').slice(0,12))}}…
                        </span>
                    </div>
                    <div style="display:flex;gap:.5rem;align-items:center">
                        ${{quarBadge}}
                        ${{isQuarantined ? '' : `<span class="badge badge-${{esc(h)}}">${{esc(n.health||'unknown')}}</span>`}}
                        ${{isStorageRole ? `<button class="btn-maint" onclick="toggleMaintenance('${{escArg(n.node_id)}}', ${{!isMaintenance}})">
                            ${{isMaintenance ? 'Exit Maintenance' : 'Set Maintenance'}}
                        </button>` : ''}}
                    </div>
                </div>
                <div class="node-card-body">
                    <div class="node-meta">
                        ${{addrRow}}
                        <div class="node-meta-item"><span>Location</span>${{esc(n.location||'—')}}</div>
                        ${{lastSeenRow}}
                        ${{versionRow}}
                        ${{sizeMetaRows}}
                    </div>
                    ${{quarPanel}}
                    ${{utilBar}}
                    ${{diskTable}}
                </div>
            </div>`;
        }}).join('');

        window.scrollTo(0, savedScroll);
    }}

    async function toggleMaintenance(nodeId, enable) {{
        try {{
            const {{ resp, text, json, authError }} = await mutateFetch(
                '/api/v1/nodes/' + nodeId + '/maintenance',
                {{ body: JSON.stringify({{ enable }}) }}
            );
            if (!resp.ok) {{
                alert(authError || ((json && json.error) ? json.error : (text || ('HTTP ' + resp.status))));
            }}
        }} catch (e) {{
            alert('Maintenance toggle failed: ' + e);
        }}
        await refreshNodesList();
    }}

    // ── Unquarantine confirmation modal ────────────────
    function openUnquarantineModal(nodeId, hostname, reason, chunkId, faultCount, summary) {{
        document.getElementById('uq-node-id').value   = nodeId;
        document.getElementById('uq-hostname').textContent = hostname;
        document.getElementById('uq-reason').textContent   = reason;
        document.getElementById('uq-chunk').textContent    = chunkId || '—';
        document.getElementById('uq-fault-count').textContent = faultCount;
        document.getElementById('uq-summary').textContent  = summary || '—';
        document.getElementById('uq-operator').value = '';
        document.getElementById('uq-ack').value = '';
        document.getElementById('uq-result').style.display = 'none';
        document.getElementById('uq-submit').disabled = true;
        document.getElementById('unquarantine-modal').classList.remove('hidden');
    }}
    function closeUnquarantineModal() {{
        document.getElementById('unquarantine-modal').classList.add('hidden');
    }}
    function onUnquarantineInput() {{
        const op  = document.getElementById('uq-operator').value.trim();
        const ack = document.getElementById('uq-ack').value.trim();
        document.getElementById('uq-submit').disabled = !(op.length > 0 && ack.length >= 16);
    }}
    async function submitUnquarantine() {{
        const nid = document.getElementById('uq-node-id').value;
        const body = {{
            operator_id:        document.getElementById('uq-operator').value.trim(),
            reason_acknowledged: document.getElementById('uq-ack').value.trim(),
        }};
        const {{ resp, text, json, authError }} = await mutateFetch(
            '/api/v1/nodes/' + nid + '/unquarantine',
            {{ body: JSON.stringify(body) }}
        );
        const result = document.getElementById('uq-result');
        result.style.display = 'block';
        if (resp.ok) {{
            result.style.color = 'var(--success)';
            result.textContent = 'Unquarantined.';
            setTimeout(async () => {{
                closeUnquarantineModal();
                await refreshNodesList();
            }}, 800);
        }} else {{
            result.style.color = 'var(--danger)';
            result.textContent = authError || ('Failed: ' + (text || JSON.stringify(json)));
        }}
    }}
    window.openUnquarantineModal = openUnquarantineModal;
    window.closeUnquarantineModal = closeUnquarantineModal;
    window.onUnquarantineInput   = onUnquarantineInput;
    window.submitUnquarantine    = submitUnquarantine;

    // ── Volumes page ──────────────────────────────────────────────────────────

    // Volumes page extended with a per-row
    // policy detail card and an inline "Edit policy" button. The list
    // endpoint now embeds the full AV-3-5 projection (AV-4-2), so the
    // card data comes from the same fetch — no per-volume GET needed.
    window._volumesData = [];
    window._volumesExpanded = new Set();

    function renderVolumeCard(v) {{
        const tier = (t) => {{
            const m = (v.mandatory_replicas && v.mandatory_replicas[t]) || 0;
            const c = (v.cache_replicas && v.cache_replicas[t]) || {{min:0, max:0}};
            return `${{t}}: mandatory=${{m}}, cache.min=${{c.min||0}}, cache.max=${{c.max||0}}`;
        }};
        const cr = v.cache_replicas || {{}};
        const anchor = v.anchor
            ? `dc=${{esc(v.anchor.datacenter||'—')}}, required_tier=${{esc(v.anchor.required_tier||'(any)')}}`
            : '(none)';
        // AZ-1 — per-volume background-job schedules.
        const sch = v.schedules || {{}};
        const scrubLine = (sch.scrub_interval_secs != null)
            ? `${{sch.scrub_interval_secs}}s (override)`
            : '(inherit cluster default)';
        const fsckLine = (sch.fsck_interval_secs != null)
            ? `${{sch.fsck_interval_secs}}s (override)`
            : '(inherit cluster default)';
        const customMap = sch.custom || {{}};
        const customCount = Object.keys(customMap).length;
        return `<div class="placeholder" style="text-align:left;padding:1rem;background:rgba(0,0,0,.04);border-radius:.5rem">
            <div style="font-weight:bold;margin-bottom:.5rem">Policy for ${{esc(v.volume_id)}}</div>
            <div style="font-family:monospace;font-size:.78rem;line-height:1.6">
                <div>Mandatory replicas: hot=${{(v.mandatory_replicas||{{}}).hot||0}}, warm=${{(v.mandatory_replicas||{{}}).warm||0}}, cold=${{(v.mandatory_replicas||{{}}).cold||0}}, tape=${{(v.mandatory_replicas||{{}}).tape||0}}</div>
                <div>Cache replicas (per_dc=${{cr.per_dc===true?'true':'false'}}, promote_reads=${{cr.promote_reads||0}}, promote_window_secs=${{cr.promote_window_secs||0}}, cooldown_secs=${{cr.cooldown_secs||0}}):</div>
                <div style="margin-left:1rem">${{tier('hot')}}</div>
                <div style="margin-left:1rem">${{tier('warm')}}</div>
                <div style="margin-left:1rem">${{tier('cold')}}</div>
                <div style="margin-left:1rem">${{tier('tape')}}</div>
                <div>Anchor: ${{anchor}}</div>
                <div>Chunk size: ${{fmtBytes(v.chunk_size_bytes||0)}}</div>
                <div style="margin-top:.5rem;font-weight:bold">Schedules (AZ-1)</div>
                <div style="margin-left:1rem">scrub_interval_secs: ${{scrubLine}}</div>
                <div style="margin-left:1rem">fsck_interval_secs: ${{fsckLine}}</div>
                <div style="margin-left:1rem">custom jobs: ${{customCount}}</div>
            </div>
        </div>`;
    }}

    function toggleVolumeRow(volumeId) {{
        if (window._volumesExpanded.has(volumeId)) {{
            window._volumesExpanded.delete(volumeId);
        }} else {{
            window._volumesExpanded.add(volumeId);
        }}
        rerenderVolumesTable();
    }}

    function rerenderVolumesTable() {{
        const el = document.getElementById('volumes-list');
        if (!el) return;
        const vols = window._volumesData;
        if (!vols.length) {{
            el.innerHTML = '<div class="placeholder">No volumes yet. Use "+ New volume" above to create one.</div>';
            return;
        }}
        // Actions layout — each action gets its own column so the
        // table column widths are stable. Button widths are fixed via
        // min-width sized to the LONGEST text variant ("Details" wins
        // over "Hide"; modal-opener buttons keep their full label
        // including the trailing ellipsis). Without this, the Details
        // toggle would shrink the Actions cell whenever it flipped to
        // "Hide", causing the surrounding columns to jitter.
        const ACT_BTN = 'btn-secondary';
        const ACT_STYLE = 'padding:.25rem .5rem;font-size:.78rem;width:100%';
        const ACT_TD = 'style="white-space:nowrap;width:1%;text-align:center"';
        el.innerHTML = '<div class="table-card"><table>' +
            '<thead><tr>' +
                '<th>Volume ID</th>' +
                '<th>Files</th>' +
                '<th>Total Size</th>' +
                '<th>Avg File Size</th>' +
                '<th>Cache</th>' +
                '<th>Anchor</th>' +
                '<th style="min-width:5.5rem">Details</th>' +
                '<th style="min-width:7.5rem">Edit policy</th>' +
                '<th style="min-width:7rem">Schedules</th>' +
                '<th style="min-width:8rem">Flush cache</th>' +
            '</tr></thead><tbody>' +
            vols.flatMap(v => {{
                const vid = v.volume_id||'—';
                const safeVid = escArg(vid);
                const cacheLabel = (v.cache_replicas && v.cache_replicas.warm)
                    ? `warm min=${{v.cache_replicas.warm.min||0}}/max=${{v.cache_replicas.warm.max||0}}${{v.cache_replicas.per_dc?' per-dc':''}}`
                    : '—';
                const anchorLabel = esc(v.anchor ? (v.anchor.datacenter||'—') : '—');
                const isOpen = window._volumesExpanded.has(vid);
                const rows = [`<tr>
                    <td><b style="font-family:monospace">${{esc(vid)}}</b></td>
                    <td>${{(v.file_count||0).toLocaleString()}}</td>
                    <td>${{fmtBytes(v.total_size||0)}}</td>
                    <td>${{v.file_count ? fmtBytes(Math.round((v.total_size||0)/v.file_count)) : '—'}}</td>
                    <td>${{cacheLabel}}</td>
                    <td>${{anchorLabel}}</td>
                    <td ${{ACT_TD}}><button class="${{ACT_BTN}}" style="${{ACT_STYLE}}"
                        onclick="toggleVolumeRow('${{safeVid}}')">${{isOpen?'Hide':'Details'}}</button></td>
                    <td ${{ACT_TD}}><button class="${{ACT_BTN}}" style="${{ACT_STYLE}}"
                        onclick="openVolumeEdit('${{safeVid}}')">Edit policy…</button></td>
                    <td ${{ACT_TD}}><button class="${{ACT_BTN}}" style="${{ACT_STYLE}}"
                        onclick="openVolumeSchedules('${{safeVid}}')">Schedules…</button></td>
                    <td ${{ACT_TD}}><button class="${{ACT_BTN}}" style="${{ACT_STYLE}}"
                        onclick="openFlushCache('/')">Flush cache…</button></td>
                </tr>`];
                if (isOpen) {{
                    rows.push(`<tr><td colspan="10">${{renderVolumeCard(v)}}</td></tr>`);
                }}
                return rows;
            }}).join('') +
            '</tbody></table></div>';
    }}

    async function renderVolumes() {{
        document.getElementById('page-content').innerHTML =
            '<div class="page-header"><h2>Volumes</h2><p class="page-desc">Storage volumes, file distribution, and cache policy</p></div>' +
            '<div style="display:flex;gap:.75rem;margin-bottom:1rem;flex-wrap:wrap">' +
                '<button class="btn-secondary" onclick="openCreateVolume()">+ New volume</button>' +
            '</div>' +
            '<div id="volumes-list"><div class="placeholder">Loading…</div></div>';
        const data = await fetch('/api/v1/volumes').then(r => r.json()).catch(() => ({{count:0, volumes:[]}}));
        window._volumesData = data.volumes || [];
        rerenderVolumesTable();
    }}

    // ── Create volume modal (UIP-1-1) ───────────────────────────────
    function openCreateVolume() {{
        document.getElementById('cv-name').value = '';
        document.getElementById('cv-chunk-size-mb').value = '';
        document.getElementById('cv-replication-hot').value = '';
        document.getElementById('cv-replication-warm').value = '';
        document.getElementById('cv-replication-cold').value = '';
        document.getElementById('cv-anchor-datacenter').value = '';
        document.getElementById('cv-result').style.display = 'none';
        document.getElementById('create-volume-modal').classList.remove('hidden');
    }}

    function closeCreateVolume() {{
        document.getElementById('create-volume-modal').classList.add('hidden');
    }}

    async function submitCreateVolume() {{
        const name = document.getElementById('cv-name').value.trim();
        if (!name) {{ alert('name is required'); return; }}

        const intOrNull = (id) => {{
            const raw = document.getElementById(id).value.trim();
            return raw === '' ? null : parseInt(raw, 10);
        }};
        const dc = document.getElementById('cv-anchor-datacenter').value.trim();
        const body = {{
            name: name,
            chunk_size_mb: intOrNull('cv-chunk-size-mb'),
            replication_hot: intOrNull('cv-replication-hot'),
            replication_warm: intOrNull('cv-replication-warm'),
            replication_cold: intOrNull('cv-replication-cold'),
            anchor_datacenter: dc === '' ? null : dc,
        }};

        const btn = document.getElementById('cv-submit');
        btn.disabled = true; btn.textContent = 'Creating…';
        const result = document.getElementById('cv-result');
        result.style.display = 'block';
        result.textContent = 'Submitting…';

        try {{
            const {{ resp, text, json, authError }} = await mutateFetch(
                '/api/v1/volumes',
                {{ body: JSON.stringify(body) }}
            );
            if (resp.ok) {{
                result.style.color = 'var(--success)';
                result.textContent = 'Created volume ' + ((json && json.volume_id) || name) + '.';
                setTimeout(async () => {{
                    closeCreateVolume();
                    await renderVolumes();
                }}, 800);
            }} else {{
                result.style.color = 'var(--danger)';
                result.textContent = authError || ('Failed: ' + (text || JSON.stringify(json)));
            }}
        }} catch (e) {{
            result.style.color = 'var(--danger)';
            result.textContent = 'network error: ' + e;
        }} finally {{
            btn.disabled = false; btn.textContent = 'Create volume';
        }}
    }}

    // ── Volume policy edit modal (AV-4-4) ─────────────────────────
    function openVolumeEdit(volumeId) {{
        const v = (window._volumesData||[]).find(x => x.volume_id === volumeId);
        document.getElementById('ve-volume-id').value = volumeId;
        if (v) {{
            const cr = v.cache_replicas || {{}};
            document.getElementById('ve-warm-min').value = (cr.warm && cr.warm.min) || 0;
            document.getElementById('ve-warm-max').value = (cr.warm && cr.warm.max) || 0;
            document.getElementById('ve-hot-min').value  = (cr.hot  && cr.hot.min)  || 0;
            document.getElementById('ve-hot-max').value  = (cr.hot  && cr.hot.max)  || 0;
            document.getElementById('ve-cold-min').value = (cr.cold && cr.cold.min) || 0;
            document.getElementById('ve-cold-max').value = (cr.cold && cr.cold.max) || 0;
            document.getElementById('ve-tape-min').value = (cr.tape && cr.tape.min) || 0;
            document.getElementById('ve-tape-max').value = (cr.tape && cr.tape.max) || 0;
            document.getElementById('ve-per-dc').checked = cr.per_dc === true;
            document.getElementById('ve-promote-reads').value = cr.promote_reads || '';
            document.getElementById('ve-promote-window').value = cr.promote_window_secs || '';
            document.getElementById('ve-cooldown').value = cr.cooldown_secs || '';
            // prefill mandatory replicas.
            const mr = v.mandatory_replicas || {{}};
            document.getElementById('ve-mand-hot').value  = mr.hot  || 0;
            document.getElementById('ve-mand-warm').value = mr.warm || 0;
            document.getElementById('ve-mand-cold').value = mr.cold || 0;
            document.getElementById('ve-mand-tape').value = mr.tape || 0;
            // prefill anchor.
            const an = v.anchor || {{}};
            document.getElementById('ve-anchor-dc').value = an.datacenter || '';
            document.getElementById('ve-anchor-tier').value = an.required_tier || '';
            // prefill spill policy.
            const sp = v.spill || {{}};
            document.getElementById('ve-spill-allow').value =
                (sp.allow === true) ? 'true' : (sp.allow === false) ? 'false' : '';
            const ft = sp.forbidden_tiers || [];
            document.getElementById('ve-spill-forbid-hot').checked  = ft.includes('hot');
            document.getElementById('ve-spill-forbid-warm').checked = ft.includes('warm');
            document.getElementById('ve-spill-forbid-cold').checked = ft.includes('cold');
            document.getElementById('ve-spill-forbid-tape').checked = ft.includes('tape');
            document.getElementById('ve-spill-forbid-dcs').value =
                (sp.forbidden_datacenters || []).join(', ');
            // prefill write safety; remember the prefilled strings so
            // submit only posts a field the operator actually edited
            // (never silently reasserts/resets an untouched value).
            const ws = v.write_safety || {{}};
            const wsMinEl = document.getElementById('ve-write-safety-min-replicas');
            const wsTimeoutEl = document.getElementById('ve-write-safety-timeout');
            wsMinEl.value = (ws.min_replicas_before_ack != null) ? ws.min_replicas_before_ack : '';
            wsTimeoutEl.value = (ws.write_timeout_secs != null) ? ws.write_timeout_secs : '';
            wsMinEl.dataset.orig = wsMinEl.value;
            wsTimeoutEl.dataset.orig = wsTimeoutEl.value;
        }}
        document.getElementById('ve-result').style.display = 'none';
        document.getElementById('volume-edit-modal').classList.remove('hidden');
    }}

    function closeVolumeEdit() {{
        document.getElementById('volume-edit-modal').classList.add('hidden');
    }}

    async function submitVolumeEdit() {{
        const vid = document.getElementById('ve-volume-id').value.trim();
        if (!vid) {{ alert('volume id is required'); return; }}

        const intOrNull = (id) => {{
            const raw = document.getElementById(id).value.trim();
            return raw === '' ? null : parseInt(raw, 10);
        }};
        const cacheBody = {{
            warm_min: intOrNull('ve-warm-min'),
            warm_max: intOrNull('ve-warm-max'),
            hot_min:  intOrNull('ve-hot-min'),
            hot_max:  intOrNull('ve-hot-max'),
            cold_min: intOrNull('ve-cold-min'),
            cold_max: intOrNull('ve-cold-max'),
            tape_min: intOrNull('ve-tape-min'),
            tape_max: intOrNull('ve-tape-max'),
            per_dc:   document.getElementById('ve-per-dc').checked,
            promote_reads:       intOrNull('ve-promote-reads'),
            promote_window_secs: intOrNull('ve-promote-window'),
            cooldown_secs:       intOrNull('ve-cooldown'),
        }};
        // Client-side guard: warm.max >= warm.min when both provided.
        if (cacheBody.warm_min != null && cacheBody.warm_max != null
            && cacheBody.warm_max < cacheBody.warm_min) {{
            alert('warm.max must be >= warm.min');
            return;
        }}
        // mandatory replicas (per tier).
        const mandBody = {{
            hot_replicas:  intOrNull('ve-mand-hot'),
            warm_replicas: intOrNull('ve-mand-warm'),
            cold_replicas: intOrNull('ve-mand-cold'),
            tape_replicas: intOrNull('ve-mand-tape'),
        }};
        // anchor policy.
        const dc = document.getElementById('ve-anchor-dc').value.trim();
        const rt = document.getElementById('ve-anchor-tier').value.trim();
        const anchorBody = {{}};
        if (dc !== '') anchorBody.datacenter = dc;
        if (rt !== '') anchorBody.required_tier = rt;
        // spill policy body.
        const spillAllow = document.getElementById('ve-spill-allow').value;
        const forbidTiers = ['hot','warm','cold','tape']
            .filter(t => document.getElementById('ve-spill-forbid-' + t).checked);
        const forbidDcs = document.getElementById('ve-spill-forbid-dcs').value
            .split(',').map(s => s.trim()).filter(s => s !== '');
        const spillBody = {{
            allow: spillAllow === '' ? null : (spillAllow === 'true'),
            forbidden_tiers: forbidTiers,
            forbidden_datacenters: forbidDcs,
        }};
        // write safety: only include a field the operator actually
        // changed from what was prefilled — these fields always carry
        // a concrete current value (never blank), so an unconditional
        // post would silently reassert/reset them on every save.
        const wsMinEl = document.getElementById('ve-write-safety-min-replicas');
        const wsTimeoutEl = document.getElementById('ve-write-safety-timeout');
        const writeSafetyBody = {{}};
        if (wsMinEl.value.trim() !== (wsMinEl.dataset.orig || '')) {{
            writeSafetyBody.min_replicas_before_ack = intOrNull('ve-write-safety-min-replicas');
        }}
        if (wsTimeoutEl.value.trim() !== (wsTimeoutEl.dataset.orig || '')) {{
            writeSafetyBody.write_timeout_secs = intOrNull('ve-write-safety-timeout');
        }}

        const btn = document.getElementById('ve-submit');
        btn.disabled = true; btn.textContent = 'Applying…';
        const result = document.getElementById('ve-result');
        result.style.display = 'block';
        result.textContent = 'Submitting…';

        // POST one section; return a human-readable line. Each endpoint
        // surfaces its own server-side validation error (BM-3 tape, anchor).
        const post = async (suffix, body) => {{
            try {{
                const {{ resp, json, authError }} = await mutateFetch(
                    `/api/v1/volumes/${{encodeURIComponent(vid)}}/${{suffix}}`,
                    {{ body: JSON.stringify(body) }}
                );
                if (!resp.ok) {{
                    return `${{suffix}}: ` + (authError
                        || (`Error ${{resp.status}}: ${{(json && json.error) || JSON.stringify(json)}}`));
                }}
                return `${{suffix}}: ${{json && json.changed ? 'updated' : 'no change'}}`;
            }} catch (e) {{
                return `${{suffix}}: network error: ${{e}}`;
            }}
        }};

        const lines = [];
        // Only post mandatory when at least one field is set (avoid an
        // all-null no-op being rejected as all-zero by the validator).
        if (Object.values(mandBody).some(x => x != null)) {{
            lines.push(await post('set-mandatory', mandBody));
        }}
        if (Object.keys(anchorBody).length > 0) {{
            lines.push(await post('set-anchor', anchorBody));
        }}
        // post spill policy when the operator set anything.
        if (spillBody.allow !== null || forbidTiers.length > 0 || forbidDcs.length > 0) {{
            lines.push(await post('set-spill', spillBody));
        }}
        if (Object.keys(writeSafetyBody).length > 0) {{
            lines.push(await post('set-write-safety', writeSafetyBody));
        }}
        lines.push(await post('set-cache', cacheBody));

        result.textContent = lines.join('\n');
        await renderVolumes();
        btn.disabled = false; btn.textContent = 'Apply';
    }}

    // UIP-1-3 — legacy→mandatory policy migration, one-shot (no request body).
    async function migrateVolumePolicy() {{
        const vid = document.getElementById('ve-volume-id').value.trim();
        if (!vid) {{ alert('volume id is required'); return; }}

        const btn = document.getElementById('ve-migrate-btn');
        const result = document.getElementById('ve-migrate-result');
        btn.disabled = true; btn.textContent = 'Migrating…';
        result.style.display = 'block';
        result.className = 'modal-result';
        result.textContent = 'Migrating…';

        try {{
            const {{ resp, json, authError }} = await mutateFetch(
                `/api/v1/volumes/${{encodeURIComponent(vid)}}/migrate-policy`,
                {{}}
            );
            if (!resp.ok) {{
                result.className = 'modal-result error';
                result.textContent = authError
                    || ('Error ' + resp.status + ': ' + ((json && json.error) || JSON.stringify(json)));
            }} else {{
                result.className = 'modal-result success';
                result.textContent = json && json.changed
                    ? 'Migrated. mandatory_replicas: ' + JSON.stringify(json.mandatory_replicas)
                    : 'No change (already migrated). mandatory_replicas: ' + JSON.stringify(json && json.mandatory_replicas);
                await renderVolumes();
            }}
        }} catch (e) {{
            result.className = 'modal-result error';
            result.textContent = 'Network error: ' + e;
        }} finally {{
            btn.disabled = false; btn.textContent = 'Migrate policy…';
        }}
    }}

    // ── AZ-1 Schedules modal ──────────────────────────────────────
    function openVolumeSchedules(volumeId) {{
        const v = (window._volumesData||[]).find(x => x.volume_id === volumeId);
        document.getElementById('vs-volume-id').value = volumeId;
        const sch = (v && v.schedules) || {{}};
        document.getElementById('vs-scrub-secs').value = sch.scrub_interval_secs || '';
        document.getElementById('vs-fsck-secs').value = sch.fsck_interval_secs || '';
        document.getElementById('vs-result').style.display = 'none';
        document.getElementById('volume-schedules-modal').classList.remove('hidden');
    }}

    function closeVolumeSchedules() {{
        document.getElementById('volume-schedules-modal').classList.add('hidden');
    }}

    async function submitVolumeSchedules() {{
        const vid = document.getElementById('vs-volume-id').value.trim();
        if (!vid) {{ alert('volume id is required'); return; }}

        const intOrNull = (id) => {{
            const raw = document.getElementById(id).value.trim();
            return raw === '' ? null : parseInt(raw, 10);
        }};
        const body = {{
            scrub_interval_secs: intOrNull('vs-scrub-secs'),
            fsck_interval_secs:  intOrNull('vs-fsck-secs'),
        }};

        const btn = document.getElementById('vs-submit');
        btn.disabled = true; btn.textContent = 'Applying…';
        const result = document.getElementById('vs-result');
        result.style.display = 'block';
        result.textContent = 'Submitting…';

        try {{
            const {{ resp, json, authError }} = await mutateFetch(
                `/api/v1/volumes/${{encodeURIComponent(vid)}}/set-schedules`,
                {{ body: JSON.stringify(body) }}
            );
            if (!resp.ok) {{
                result.textContent = authError
                    || ('Error ' + resp.status + ': ' + ((json && json.error) || JSON.stringify(json)));
            }} else {{
                result.textContent = json && json.changed
                    ? 'Updated. New schedules:\n' + JSON.stringify(json.config.schedules, null, 2)
                    : 'No change (request was a no-op).';
                await renderVolumes();
            }}
        }} catch (e) {{
            result.textContent = 'Network error: ' + e;
        }} finally {{
            btn.disabled = false; btn.textContent = 'Apply';
        }}
    }}

    // ── Data Explorer page ────────────────────────────────────────────────────

    // folder-aware data explorer.
    //
    // Pre-fix the explorer dumped every file in the cluster (paginated to
    // the first 1000 by /api/v1/files) as a flat table with the whole
    // path in one column. Useless on a media library with thousands of
    // files under nested `/movies/Season N/` paths.
    //
    // New behaviour:
    //   * Browse a single directory at a time, starting at `/`.
    //   * Subdirectories shown first as folder rows; clicking a folder
    //     descends into it. Breadcrumb above lets you climb back.
    //   * Files shown after folders with size + chunk count.
    //   * Right-click on either a folder or a file opens a context menu
    //     with operator actions: "Migrate this path…" prefilled. Falls
    //     back to standard click handlers for keyboard users (the menu
    //     also opens on Enter when the row is focused).
    //
    // The grouping is computed client-side from the full file list (we
    // fetch up to 100k files in one shot — adequate for the household
    // Plex scale). For huge clusters this should be replaced with a
    // server-side `/api/v1/files/browse?path=...&depth=1` endpoint.
    let explorerCwd = '/';
    let explorerLastFiles = [];

    async function renderExplorer() {{
        document.getElementById('page-content').innerHTML =
            `<div class="page-header">
                <h2>Data Explorer</h2>
                <p class="page-desc">Browse files and folders cluster-wide. Right-click a row for actions.</p>
            </div>
            <div style="display:flex;gap:.75rem;margin-bottom:.5rem;flex-wrap:wrap;align-items:center">
                <div id="explorer-breadcrumb" style="flex:1;min-width:200px;font-family:monospace;font-size:.85rem;color:var(--text-muted)">/</div>
                <input id="explorer-filter" type="text" class="form-input" placeholder="Filter by name…" style="max-width:240px">
                <button class="btn-primary"   onclick="loadExplorer()">Refresh</button>
                <button class="btn-secondary" onclick="openMigrate(window.explorerCwd || '/')" title="Migrate the current folder">Migrate folder…</button>
            </div>
            <div id="explorer-list"><div class="placeholder">Loading…</div></div>
            <div id="explorer-ctxmenu" class="ctxmenu" style="display:none"></div>`;
        document.getElementById('explorer-filter').addEventListener('input', () => renderExplorerCwd());
        // Hide the context menu on any outside click / Escape.
        document.addEventListener('click', hideCtxMenu);
        document.addEventListener('keydown', e => {{ if (e.key === 'Escape') hideCtxMenu(); }});
        explorerCwd = '/';
        window.explorerCwd = '/';
        await loadExplorer();
    }}

    async function loadExplorer() {{
        // Fetch a large slice once; folder rendering is purely client-side.
        const data = await fetch('/api/v1/files?limit=100000').then(r => r.json()).catch(() => ({{count:0,files:[]}}));
        explorerLastFiles = data.files || [];
        renderExplorerCwd();
    }}

    /// Render the current working directory (`explorerCwd`) by grouping
    /// `explorerLastFiles` into direct-child folders and direct-child files.
    function renderExplorerCwd() {{
        const el = document.getElementById('explorer-list');
        if (!el) return;
        const cwd = explorerCwd.endsWith('/') ? explorerCwd : explorerCwd + '/';
        window.explorerCwd = cwd;
        const filter = (document.getElementById('explorer-filter')?.value || '').toLowerCase();

        // Breadcrumb
        const crumbs = ['/', ...cwd.split('/').filter(Boolean)];
        document.getElementById('explorer-breadcrumb').innerHTML = crumbs.map((seg, idx) => {{
            if (idx === 0) {{
                const onClick = `event.preventDefault();explorerNavigate('/')`;
                return `<a href="#" onclick="${{onClick}}" style="color:var(--primary);text-decoration:none">/</a>`;
            }}
            const full = '/' + crumbs.slice(1, idx + 1).join('/') + '/';
            const onClick = `event.preventDefault();explorerNavigate('${{escArg(full)}}')`;
            return `<a href="#" onclick="${{onClick}}" style="color:var(--primary);text-decoration:none">${{esc(seg)}}</a>/`;
        }}).join('');

        // Group direct children: folders (have at least one descendant) vs files.
        const folders = new Map(); // name → {{ name, path, fileCount, totalSize }}
        const files = [];
        for (const f of explorerLastFiles) {{
            const p = f.path || '';
            if (!p.startsWith(cwd)) continue;
            const tail = p.slice(cwd.length);
            const slash = tail.indexOf('/');
            if (slash === -1) {{
                // Direct file under cwd
                if (!filter || tail.toLowerCase().includes(filter)) {{
                    files.push({{ ...f, name: tail }});
                }}
            }} else {{
                const name = tail.slice(0, slash);
                if (!folders.has(name)) {{
                    folders.set(name, {{
                        name,
                        path: cwd + name + '/',
                        fileCount: 0,
                        totalSize: 0,
                    }});
                }}
                const entry = folders.get(name);
                entry.fileCount += 1;
                entry.totalSize += (f.total_size || 0);
            }}
        }}
        let folderList = [...folders.values()];
        if (filter) folderList = folderList.filter(f => f.name.toLowerCase().includes(filter));
        folderList.sort((a, b) => a.name.localeCompare(b.name, undefined, {{numeric:true}}));
        files.sort((a, b) => a.name.localeCompare(b.name, undefined, {{numeric:true}}));

        if (folderList.length === 0 && files.length === 0) {{
            el.innerHTML = '<div class="placeholder">' +
                (filter ? `No entries match "${{esc(filter)}}" in ${{esc(cwd)}}.` : `Empty folder: ${{esc(cwd)}}`) +
                '</div>';
            return;
        }}

        const folderRows = folderList.map(d => `<tr class="explorer-row" oncontextmenu="event.preventDefault();showCtxMenu(event,'${{escArg(d.path)}}',true)">
            <td style="font-size:1rem;width:1.5rem">📁</td>
            <td>
                <a href="#" onclick="event.preventDefault();explorerNavigate('${{escArg(d.path)}}')" style="color:var(--primary);text-decoration:none;font-family:monospace">${{esc(d.name)}}/</a>
            </td>
            <td style="font-size:.78rem;color:var(--text-muted)">${{d.fileCount.toLocaleString()}} file${{d.fileCount!==1?'s':''}}</td>
            <td>${{fmtBytes(d.totalSize)}}</td>
            <td></td>
            <td style="white-space:nowrap">
                <button class="btn-secondary" style="font-size:.72rem;padding:.2rem .5rem"
                        onclick="openMigrate('${{escArg(d.path)}}')" title="Migrate this folder">⋯</button>
            </td>
        </tr>`).join('');

        const fileRows = files.map(f => {{
            const safePath = escArg(f.path || '');
            return `<tr class="explorer-row" oncontextmenu="event.preventDefault();showCtxMenu(event,'${{safePath}}',false)">
                <td style="font-size:1rem;width:1.5rem">📄</td>
                <td style="font-family:monospace;font-size:.85rem;max-width:340px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${{esc(f.path||'')}}">${{esc(f.name)}}</td>
                <td style="font-size:.78rem;color:var(--text-muted)">${{f.chunk_count||0}} chunk${{(f.chunk_count||0)!==1?'s':''}}</td>
                <td>${{fmtBytes(f.total_size||0)}}</td>
                <td style="font-size:.78rem;color:var(--text-muted)">${{f.created_at ? timeAgo(f.created_at) : '—'}}</td>
                <td style="white-space:nowrap">
                    <button class="btn-secondary" style="font-size:.72rem;padding:.2rem .5rem"
                            onclick="openMigrate('${{safePath}}')" title="Migrate just this file">⋯</button>
                </td>
            </tr>`;
        }}).join('');

        const savedScroll = window.scrollY;
        el.innerHTML = '<div class="table-card"><table>' +
            '<thead><tr><th></th><th>Name</th><th>Contents</th><th>Size</th><th>Created</th><th></th></tr></thead><tbody>' +
            folderRows + fileRows +
            `</tbody></table>` +
            `<div style="padding:.75rem 1.25rem;font-size:.78rem;color:var(--text-muted);border-top:1px solid var(--border)">
                ${{folderList.length}} folder${{folderList.length!==1?'s':''}} · ${{files.length}} file${{files.length!==1?'s':''}} in <code>${{esc(cwd)}}</code> · right-click for actions
            </div></div>`;
        window.scrollTo(0, savedScroll);
    }}

    function explorerNavigate(path) {{
        explorerCwd = path || '/';
        renderExplorerCwd();
    }}

    function showCtxMenu(evt, path, isFolder) {{
        const menu = document.getElementById('explorer-ctxmenu');
        if (!menu) return;
        const safe = escArg(path || '');
        const parent = isFolder ? path : path.replace(/[^/]*$/, '');
        const safeParent = escArg(parent);
        const items = isFolder
            ? [
                {{ label: 'Open folder',           onClick: `explorerNavigate('${{safe}}')` }},
                {{ label: 'Migrate this folder…',  onClick: `openMigrate('${{safe}}')` }},
                {{ label: 'Flush cache (folder)…', onClick: `openFlushCache('${{safe}}')` }},
                {{ label: 'Copy path',             onClick: `navigator.clipboard&&navigator.clipboard.writeText('${{safe}}')` }},
              ]
            : [
                {{ label: 'Migrate this file…',    onClick: `openMigrate('${{safe}}')` }},
                {{ label: 'Migrate parent folder…',onClick: `openMigrate('${{safeParent}}')` }},
                {{ label: 'Flush cache (file)…',   onClick: `openFlushCache('${{safe}}')` }},
                {{ label: 'Flush cache (parent)…', onClick: `openFlushCache('${{safeParent}}')` }},
                {{ label: 'Copy path',             onClick: `navigator.clipboard&&navigator.clipboard.writeText('${{safe}}')` }},
              ];
        menu.innerHTML = items.map(it =>
            `<div class="ctxmenu-item" onclick="${{it.onClick}};hideCtxMenu()">${{it.label}}</div>`
        ).join('');
        menu.style.display = 'block';
        menu.style.left = evt.pageX + 'px';
        menu.style.top  = evt.pageY + 'px';
    }}

    function hideCtxMenu() {{
        const m = document.getElementById('explorer-ctxmenu');
        if (m) m.style.display = 'none';
    }}

    // ── Migrate dialog (operator-driven path migration) ──────────────────────

    async function openMigrate(prefix) {{
        // Populate the DC picker from the live node list so the user only sees
        // datacenters that actually exist.
        const nodes = await fetch('/api/v1/nodes').then(r => r.json()).catch(() => []);
        const dcs = Array.from(new Set(nodes
            .map(n => (n.location || '').split('/')[1])
            .filter(Boolean)
        )).sort();
        const dcSel = document.getElementById('m-dc');
        dcSel.innerHTML = '<option value="">Any datacenter</option>' +
            dcs.map(dc => `<option value="${{esc(dc)}}">${{esc(dc)}}</option>`).join('');

        document.getElementById('m-path').value = prefix || '';
        document.getElementById('m-result').style.display = 'none';
        document.getElementById('m-result').textContent = '';
        document.getElementById('m-empty-row').style.display = (prefix === '') ? 'block' : 'none';
        document.getElementById('m-allow-empty').checked = false;
        document.getElementById('m-submit').disabled = false;
        document.getElementById('migrate-modal').classList.remove('hidden');
        document.getElementById('m-path').focus();
    }}

    function closeMigrate() {{
        document.getElementById('migrate-modal').classList.add('hidden');
    }}

    async function submitMigrate() {{
        const path        = document.getElementById('m-path').value;
        const tier        = document.getElementById('m-tier').value;
        const dcRaw       = document.getElementById('m-dc').value;
        const allowEmpty  = document.getElementById('m-allow-empty').checked;
        const result      = document.getElementById('m-result');
        const submitBtn   = document.getElementById('m-submit');

        submitBtn.disabled = true;
        result.style.display = 'block';
        result.className = 'modal-result';
        result.textContent = 'Scheduling…';

        const body = {{
            path_prefix: path,
            target_tier: tier,
            allow_empty_prefix: allowEmpty,
        }};
        if (dcRaw) body.target_dc = dcRaw;

        try {{
            const {{ resp, json, authError }} = await mutateFetch('/api/v1/migrations', {{
                body: JSON.stringify(body),
            }});
            const data = json || {{}};
            if (!resp.ok) {{
                result.className = 'modal-result error';
                result.textContent = authError
                    || ((data && data.error) ? data.error : ('HTTP ' + resp.status));
                submitBtn.disabled = false;
                return;
            }}
            result.className = 'modal-result success';
            result.textContent =
                'Files matched: '       + (data.files_matched     ?? 0) + '\n' +
                'Chunks total: '        + (data.chunks_total      ?? 0) + '\n' +
                'Already satisfied: '   + (data.already_satisfied ?? 0) + '\n' +
                'Tasks scheduled: '     + (data.tasks_scheduled   ?? 0) + '\n' +
                'No healthy source: '   + (data.no_source         ?? 0) + '\n' +
                'No matching target: '  + (data.no_target         ?? 0) +
                (data.truncated_chunks ? ('\nTruncated (re-run): ' + data.truncated_chunks) : '') +
                ((data.examples && data.examples.length)
                    ? '\n\nExamples:\n' + data.examples.map(e =>
                        '  · ' + e.path + ' [chunk ' + e.chunk_index + '] → ' + e.reason
                      ).join('\n')
                    : ''
                );
        }} catch (e) {{
            result.className = 'modal-result error';
            result.textContent = 'Request failed: ' + e;
            submitBtn.disabled = false;
        }}
    }}

    // ── Flush cache modal ──────────────────────────────────

    function openFlushCache(prefix) {{
        document.getElementById('fc-path').value = prefix || '';
        document.getElementById('fc-tiers').value = 'warm';
        document.getElementById('fc-cooldown').value = 300;
        document.getElementById('fc-result').style.display = 'none';
        document.getElementById('fc-result').textContent = '';
        document.getElementById('fc-empty-row').style.display =
            (prefix === '' || prefix === '/') ? 'block' : 'none';
        document.getElementById('fc-allow-empty').checked = false;
        document.getElementById('fc-submit').disabled = false;
        document.getElementById('flush-modal').classList.remove('hidden');
        document.getElementById('fc-path').focus();
    }}

    function closeFlushCache() {{
        document.getElementById('flush-modal').classList.add('hidden');
    }}

    async function submitFlushCache() {{
        const path        = document.getElementById('fc-path').value;
        const tiersRaw    = document.getElementById('fc-tiers').value;
        const cooldown    = parseInt(document.getElementById('fc-cooldown').value, 10);
        const allowEmpty  = document.getElementById('fc-allow-empty').checked;
        const result      = document.getElementById('fc-result');
        const submitBtn   = document.getElementById('fc-submit');

        submitBtn.disabled = true;
        result.style.display = 'block';
        result.className = 'modal-result';
        result.textContent = 'Flushing…';

        const body = {{
            path_prefix: path,
            cache_tiers: tiersRaw.split(','),
            cooldown_secs: Number.isFinite(cooldown) ? cooldown : 300,
            allow_empty_prefix: allowEmpty,
        }};

        try {{
            const {{ resp, json, authError }} = await mutateFetch('/api/v1/files/flush_cache', {{
                body: JSON.stringify(body),
            }});
            const data = json || {{}};
            if (!resp.ok) {{
                result.className = 'modal-result error';
                result.textContent = authError
                    || ((data && data.error) ? data.error : ('HTTP ' + resp.status));
                submitBtn.disabled = false;
                return;
            }}
            result.className = 'modal-result success';
            result.textContent =
                'Files matched: '       + (data.files_matched      ?? 0) + '\n' +
                'Chunks examined: '     + (data.chunks_examined    ?? 0) + '\n' +
                'Replicas dropped: '    + (data.replicas_dropped   ?? 0) + '\n' +
                'Mandatory refusals: '  + (data.mandatory_refusals ?? 0) +
                ((data.mandatory_refusals > 0)
                    ? '  ⚠ check audit log: action=cache.flush_refused'
                    : '') + '\n' +
                'Unreachable nodes: '   + (data.unreachable_nodes  ?? 0) + '\n' +
                'RPC failures: '        + (data.rpc_failures       ?? 0) + '\n' +
                'Raft failures: '       + (data.raft_failures      ?? 0) +
                (data.truncated ? ('\nTruncated (re-run): ' + data.truncated) : '');
        }} catch (e) {{
            result.className = 'modal-result error';
            result.textContent = 'Request failed: ' + e;
            submitBtn.disabled = false;
        }}
    }}

    // ── Migrations page ──────────────────────────────────────────────────────

    async function renderMigrations() {{
        document.getElementById('page-content').innerHTML =
            `<div class="page-header">
                <h2>Migrations</h2>
                <p class="page-desc">Manual + automatic chunk migration tasks (refreshed every 5s)</p>
            </div>
            <div id="mig-summary" class="signal-strip" style="margin-bottom:.75rem">
                <div class="placeholder">Counting…</div>
            </div>
            <div style="display:flex;gap:.75rem;margin-bottom:1rem;flex-wrap:wrap">
                <button class="btn-primary"   onclick="loadMigrations()">Refresh</button>
                <button class="btn-secondary" onclick="openMigrate('')">+ New migration</button>
            </div>
            <div id="migrations-list"><div class="placeholder">Loading…</div></div>`;
        await loadMigrations();
        refreshTimer = setInterval(loadMigrations, 5000);
    }}

    async function loadMigrations() {{
        const data = await fetch('/api/v1/migrations').then(r => r.json()).catch(() => ({{count:0,tasks:[]}}));
        const el = document.getElementById('migrations-list');
        if (!el) return;
        const tasks = data.tasks || [];
        const sumEl = document.getElementById('mig-summary');
        if (sumEl) {{
            let failed = 0, done = 0, active = 0, pending = 0;
            for (const t of tasks) {{
                const s = (t.status||'').toLowerCase();
                if (s === 'failed') failed++;
                else if (s === 'done') done++;
                else if (s === 'pending') pending++;
                else active++;
            }}
            const chip = (val, lbl, sev) =>
                `<div class="signal-chip ${{sev}}" style="cursor:default">
                    <div class="signal-val">${{val}}</div>
                    <div class="signal-lbl">${{lbl}}</div>
                </div>`;
            sumEl.innerHTML =
                chip(failed, 'Failed', failed > 0 ? 'danger' : 'ok') +
                chip(active, 'In flight', active > 0 ? 'warn' : 'ok') +
                chip(pending, 'Pending', 'ok') +
                chip(done, 'Done', 'ok') +
                chip(tasks.length, 'Total tracked', 'ok');
        }}
        if (tasks.length === 0) {{
            setHtmlPreserveScroll(el, '<div class="placeholder">No migration tasks. Schedule one from Data Explorer or the “+ New migration” button.</div>');
            return;
        }}
        const statusColor = (s) => {{
            if (s === 'done')      return 'var(--success)';
            if (s === 'failed')    return 'var(--danger)';
            if (s === 'pending')   return 'var(--text-muted)';
            return 'var(--warning)'; // copying/verifying/completing
        }};
        // wrap the innerHTML write in setHtmlPreserveScroll so
        // the 5s auto-refresh doesn't yank the user back to the top of
        // a long migration list. The server-side sort plus
        // scroll preservation here means the table stays readable
        // during sustained migration storms.
        setHtmlPreserveScroll(el,
            '<div class="table-card"><table>' +
            '<thead><tr><th>Task</th><th>Chunk</th><th>Src → Tgt</th><th>Tier</th><th>Status</th><th>Attempts</th><th>Updated</th><th>Last error</th></tr></thead><tbody>' +
            tasks.map(t => `<tr>
                <td style="font-family:monospace;font-size:.75rem">${{esc(t.task_id||'—')}}</td>
                <td style="font-family:monospace;font-size:.75rem;max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${{esc(t.chunk_id||'')}}">${{esc((t.chunk_id||'').slice(0,16))}}…</td>
                <td style="font-family:monospace;font-size:.78rem">${{esc(t.source_node||'?')}} → ${{esc(t.target_node||'?')}}</td>
                <td><span class="badge badge-${{esc((t.tier||'').toLowerCase())}}">${{esc(t.tier||'—')}}</span></td>
                <td style="color:${{statusColor((t.status||'').toLowerCase())}};font-weight:600">${{esc(t.status||'—')}}</td>
                <td>${{t.attempts ?? 0}}</td>
                <td style="font-size:.75rem;color:var(--text-muted)">${{t.updated_at_ms ? timeAgo(t.updated_at_ms) : '—'}}</td>
                <td style="font-size:.75rem;color:var(--text-muted);max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${{esc(t.last_error||'')}}">${{esc(t.last_error||'—')}}</td>
            </tr>`).join('') +
            `</tbody></table>` +
            `<div style="padding:.75rem 1.25rem;font-size:.78rem;color:var(--text-muted);border-top:1px solid var(--border)">
                ${{data.count}} task(s) tracked
            </div></div>`
        );
    }}

    // ── Audit Log page ────────────────────────────────────────────────────────

    async function renderAudit() {{
        document.getElementById('page-content').innerHTML =
            `<div class="page-header">
                <h2>Audit Log</h2>
                <p class="page-desc">All mutating operations recorded cluster-wide</p>
            </div>
            <div style="display:flex;gap:.75rem;margin-bottom:1rem;flex-wrap:wrap">
                <input id="audit-action-filter" type="text" class="form-input" placeholder="Filter by action (e.g. tier, scrub)…" style="max-width:260px">
                <select id="audit-limit" class="form-input" style="max-width:120px">
                    <option value="50">Last 50</option>
                    <option value="100">Last 100</option>
                    <option value="200">Last 200</option>
                    <option value="500">Last 500</option>
                </select>
                <button class="btn-primary" onclick="loadAudit()">Refresh</button>
            </div>
            <div id="audit-list"><div class="placeholder">Loading…</div></div>`;
        document.getElementById('audit-action-filter').addEventListener('input', () => loadAudit());
        document.getElementById('audit-limit').addEventListener('change', () => loadAudit());
        await loadAudit();
        refreshTimer = setInterval(loadAudit, 15000);
    }}

    async function loadAudit() {{
        const action = document.getElementById('audit-action-filter')?.value || '';
        const limit  = document.getElementById('audit-limit')?.value || '50';
        let url = '/api/v1/audit?limit=' + limit;
        if (action) url += '&action=' + encodeURIComponent(action);
        const entries = await fetch(url).then(r => r.json()).catch(() => []);
        const el = document.getElementById('audit-list');
        if (!el) return;
        if (!entries || entries.length === 0) {{
            el.innerHTML = '<div class="placeholder">No audit entries found.</div>';
            return;
        }}
        el.innerHTML = '<div class="table-card"><table>' +
            '<thead><tr><th>Time</th><th>Actor</th><th>Action</th><th>Target</th><th>Result</th></tr></thead><tbody>' +
            entries.map(e => {{
                const ok = (e.result||'').toLowerCase() === 'success' || (e.result||'').toLowerCase() === 'ok';
                const resultColor = ok ? 'var(--success)' : e.result === 'pending' ? 'var(--warning)' : 'var(--text-muted)';
                return `<tr>
                    <td style="font-size:.78rem;color:var(--text-muted);white-space:nowrap">
                        ${{/* AuditEntry.timestamp is SECONDS-since-epoch (chrono::Utc::now().timestamp() in audit.rs). JS new Date() expects MS. Without *1000 every entry rendered as 1970-01-2x. */ e.timestamp ? new Date(e.timestamp * 1000).toLocaleString() : '—'}}
                    </td>
                    <td style="font-size:.8rem">${{esc(e.actor||'—')}}</td>
                    <td style="font-family:monospace;font-size:.78rem">${{esc(e.action||'—')}}</td>
                    <td style="font-size:.78rem;color:var(--text-muted);max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${{esc(e.target||'')}}">
                        ${{esc(e.target||'—')}}
                    </td>
                    <td style="font-size:.78rem;color:${{resultColor}}">${{esc(e.result||'—')}}</td>
                </tr>`;
            }}).join('') +
            '</tbody></table></div>';
    }}

    // ── Config page (AZ-5) ────────────────────────────────────────────────────
    // Raw cluster-config viewer/editor. Every [section.key] from the
    // ManagerConfig is listed with name / current / default; operators
    // can edit and Apply (live-reload + write-to-disk). Validation
    // failures surface inline and the input reverts to the
    // pre-attempt value (AZ-5-A3 boundary fallback).
    window._configRows = [];
    window._configFilter = '';
    window._configPending = {{}};

    window._configTab = 'cluster';
    async function renderConfig() {{
        document.getElementById('page-content').innerHTML = `
            <div class="page-header"><h2>Config</h2>
                <p class="page-desc">Cluster-wide keys must match across every manager. Node-specific
                keys (hostname, listen addrs, TLS material) live in the per-host tab. Edit a field
                and click <b>Apply</b> to live-reload + persist.</p>
            </div>
            <div style="display:flex;gap:.5rem;margin-bottom:.5rem;flex-wrap:wrap;align-items:center">
                <button id="cfg-tab-cluster" class="btn-secondary"
                        onclick="onConfigTab('cluster')">Cluster (—)</button>
                <button id="cfg-tab-node" class="btn-secondary"
                        onclick="onConfigTab('node')">Node: <span id="cfg-tab-node-host">—</span> (—)</button>
                <input id="cfg-filter" type="text" class="form-input" placeholder="Filter by name…" style="max-width:260px"
                       oninput="onConfigFilter(this.value)">
                <button class="btn-secondary" onclick="renderConfig()">Refresh</button>
                <button class="btn-primary"   onclick="submitConfigDiff(false)">Apply (live only)</button>
                <button class="btn-primary"   onclick="submitConfigDiff(true)">Apply + write to disk</button>
                <span id="cfg-pending-count" style="font-size:.85rem;color:var(--text-muted)"></span>
            </div>
            <div id="cfg-result" class="modal-result" style="display:none"></div>
            <div id="cfg-list"><div class="placeholder">Loading…</div></div>`;
        const data = await fetch('/api/v1/config/effective').then(r => r.json())
            .catch(() => ({{entries: [], scope: 'unknown', node_hostname: 'unknown'}}));
        window._configRows = data.entries || [];
        window._configHost = data.node_hostname || '—';
        window._configPending = {{}};
        document.getElementById('cfg-tab-node-host').textContent = window._configHost;
        // Counts per scope_kind.
        const cn = window._configRows.filter(r => r.scope_kind === 'cluster').length;
        const nn = window._configRows.filter(r => r.scope_kind === 'node').length;
        document.getElementById('cfg-tab-cluster').textContent = 'Cluster (' + cn + ')';
        document.getElementById('cfg-tab-node').textContent =
            'Node: ' + window._configHost + ' (' + nn + ')';
        rerenderConfigTable();
    }}

    function onConfigTab(name) {{
        window._configTab = name;
        const ct = document.getElementById('cfg-tab-cluster');
        const nt = document.getElementById('cfg-tab-node');
        if (ct) ct.classList.toggle('btn-primary', name === 'cluster');
        if (nt) nt.classList.toggle('btn-primary', name === 'node');
        rerenderConfigTable();
    }}
    window.onConfigTab = onConfigTab;

    function onConfigFilter(v) {{
        window._configFilter = (v||'').toLowerCase();
        rerenderConfigTable();
    }}

    function rerenderConfigTable() {{
        const el = document.getElementById('cfg-list');
        if (!el) return;
        const rows = window._configRows;
        const filter = window._configFilter;
        const tab = window._configTab || 'cluster';
        const scoped = rows.filter(r => (r.scope_kind||'node') === tab);
        const filtered = filter
            ? scoped.filter(r => (r.name||'').toLowerCase().includes(filter))
            : scoped;
        if (!filtered.length) {{
            el.innerHTML = '<div class="placeholder">No matching config keys.</div>';
            return;
        }}
        const pendingCount = Object.keys(window._configPending).length;
        document.getElementById('cfg-pending-count').textContent =
            pendingCount ? `${{pendingCount}} pending change${{pendingCount===1?'':'s'}}` : '';
        el.innerHTML = '<div class="table-card"><table>' +
            '<thead><tr><th>Name</th><th>Scope</th><th>Current</th><th>Default</th><th>Edit</th></tr></thead><tbody>' +
            filtered.map(r => {{
                const safeName = escArg(r.name);
                const cur = (r.current === null || r.current === undefined)
                    ? '' : JSON.stringify(r.current);
                const def = (r.default === null || r.default === undefined)
                    ? '(operator-set)' : JSON.stringify(r.default);
                const pending = window._configPending[r.name];
                const pendingFlag = (pending !== undefined)
                    ? `<span style="color:var(--warning);font-weight:bold">●</span>` : '';
                const isDefault = r.is_default
                    ? `<span style="color:var(--text-muted);font-size:.75rem">(default)</span>` : '';
                return `<tr>
                    <td style="font-family:monospace;font-size:.78rem">${{esc(r.name)}} ${{pendingFlag}} ${{isDefault}}</td>
                    <td>${{esc(r.scope)}}</td>
                    <td style="font-family:monospace;font-size:.78rem">${{esc(cur||'(null)')}}</td>
                    <td style="font-family:monospace;font-size:.78rem;color:var(--text-muted)">${{esc(def)}}</td>
                    <td><input class="form-input" style="font-family:monospace;font-size:.78rem;width:14rem"
                               value="${{esc(pending !== undefined ? String(pending) : cur)}}"
                               oninput="onConfigEdit('${{safeName}}', this.value)"></td>
                </tr>`;
            }}).join('') +
            '</tbody></table></div>';
    }}

    function onConfigEdit(name, value) {{
        if (value === '') {{
            delete window._configPending[name];
        }} else {{
            // Attempt JSON parse so numbers/bools/arrays don't get
            // string-quoted. Fall back to literal string on failure.
            try {{
                window._configPending[name] = JSON.parse(value);
            }} catch (_) {{
                window._configPending[name] = value;
            }}
        }}
        const pendingCount = Object.keys(window._configPending).length;
        document.getElementById('cfg-pending-count').textContent =
            pendingCount ? `${{pendingCount}} pending change${{pendingCount===1?'':'s'}}` : '';
    }}

    async function submitConfigDiff(writeToDisk) {{
        const pending = window._configPending;
        const keys = Object.keys(pending);
        if (!keys.length) {{
            alert('No pending changes.');
            return;
        }}
        // Build a JSON-merge from the dotted-path names. Drop the
        // "manager." prefix to match the ManagerConfig shape.
        const merged = {{}};
        for (const dotted of keys) {{
            const parts = dotted.replace(/^manager\./, '').split('.');
            let cursor = merged;
            for (let i = 0; i < parts.length - 1; i++) {{
                cursor[parts[i]] = cursor[parts[i]] || {{}};
                cursor = cursor[parts[i]];
            }}
            cursor[parts[parts.length - 1]] = pending[dotted];
        }}
        const result = document.getElementById('cfg-result');
        result.style.display = 'block';
        result.textContent = 'Submitting…';
        try {{
            const {{ resp, json, authError }} = await mutateFetch('/api/v1/config', {{
                body: JSON.stringify({{
                    config_json: JSON.stringify(merged),
                    write_to_disk: writeToDisk,
                }}),
            }});
            const j = json || {{error: 'invalid JSON response'}};
            if (!resp.ok) {{
                result.textContent = (authError
                    || ('Error ' + resp.status + ': ' + (j.error || JSON.stringify(j))))
                    + (j.preserved_current ? ' (current values preserved)' : '');
                // AZ-5-A3 — reload to revert pending UI state to
                // server's current values.
                await renderConfig();
                return;
            }}
            result.textContent = `Applied. version=${{j.version}}, write_to_disk=${{j.write_to_disk}}`;
            // Re-fetch to reflect post-apply state.
            await renderConfig();
        }} catch (e) {{
            result.textContent = 'Network error: ' + e;
        }}
    }}

    // ── Settings page ─────────────────────────────────────────────────────────

    async function renderSettings() {{
        document.getElementById('page-content').innerHTML =
            '<div class="page-header"><h2>Settings</h2><p class="page-desc">Cluster-wide configuration (live reload)</p></div>' +
            '<div id="settings-body"><div class="placeholder">Loading…</div></div>';
        await loadSettings();
    }}

    async function loadSettings() {{
        const cfg = await fetch('/api/v1/config').then(r => r.json()).catch(() => null);
        const el = document.getElementById('settings-body');
        if (!el) return;
        if (!cfg) {{
            el.innerHTML = '<div class="placeholder">Could not load configuration.</div>';
            return;
        }}
        const d = cfg.defaults || {{}};
        const ws = d.write_safety || {{}};
        const rep = d.replication || {{}};
        const tier = cfg.tiering || {{}};

        el.innerHTML = `
        <div class="settings-section" style="margin-bottom:1.25rem">
            <h3>Session shared secret</h3>
            <p class="settings-desc">Required for mutating WebUI actions when the cluster
            <code>[cluster].shared_secret</code> is set. Stored in <code>sessionStorage</code>
            only - never embedded in HTML. GETs stay open without it.</p>
            <div class="settings-row" style="flex-direction:column;align-items:stretch;gap:.5rem;border-bottom:none">
                <label for="session-secret-input">Shared secret</label>
                <input id="session-secret-input" class="form-input" type="password"
                       autocomplete="off" placeholder="Paste cluster shared_secret">
                <div style="display:flex;gap:.5rem;flex-wrap:wrap;align-items:center">
                    <button type="button" class="btn-primary" onclick="saveSessionSecretFromSettings()">Save to session</button>
                    <button type="button" class="btn-secondary" onclick="clearSharedSecret()">Clear</button>
                    <span id="session-secret-status" class="settings-note" style="margin:0;font-style:normal"></span>
                </div>
            </div>
            <p class="settings-note">401 = missing header · 403 = wrong secret. Header badge shows whether a session secret is set (not the value).</p>
        </div>
        <div class="settings-grid">
            <div class="settings-section">
                <h3>Write Safety</h3>
                <p class="settings-desc">Controls how many replicas must confirm a write before the client is acknowledged.</p>
                <div class="settings-row">
                    <label>Min replicas before ack</label>
                    <span class="settings-val">${{ws.min_replicas_before_ack ?? '—'}}</span>
                </div>
                <div class="settings-row">
                    <label>Write timeout (secs)</label>
                    <span class="settings-val">${{ws.write_timeout_secs ?? '—'}}</span>
                </div>
                <p class="settings-note">Config version: <b>v${{cfg.version ?? 0}}</b> · Change via <code>dtfs-cli config set</code> or POST /api/v1/config</p>
            </div>

            <div class="settings-section">
                <h3>Default Replication</h3>
                <p class="settings-desc">Per-tier replication factors applied to new volumes unless overridden.</p>
                <div class="settings-row">
                    <label><span class="badge badge-hot">hot</span> RF</label>
                    <span class="settings-val">${{rep.hot ?? '—'}}</span>
                </div>
                <div class="settings-row">
                    <label><span class="badge badge-warm">warm</span> RF</label>
                    <span class="settings-val">${{rep.warm ?? '—'}}</span>
                </div>
                <div class="settings-row">
                    <label><span class="badge badge-cold">cold</span> RF</label>
                    <span class="settings-val">${{rep.cold ?? '—'}}</span>
                </div>
            </div>

            <div class="settings-section">
                <h3>Tiering Engine</h3>
                <p class="settings-desc">Automatic hot/cold data classification and migration settings.</p>
                <div class="settings-row">
                    <label>Enabled</label>
                    <span class="settings-val">${{tier.enabled ? '✅ Yes' : '❌ No'}}</span>
                </div>
                <div class="settings-row">
                    <label>Hot threshold (reads/hr)</label>
                    <span class="settings-val">${{tier.hot_threshold_reads_per_hour ?? '—'}}</span>
                </div>
                <div class="settings-row">
                    <label>Cold threshold (days)</label>
                    <span class="settings-val">${{tier.cold_threshold_days ?? '—'}}</span>
                </div>
                <div class="settings-row">
                    <label>Frozen threshold (days)</label>
                    <span class="settings-val">${{tier.frozen_threshold_days ?? '—'}}</span>
                </div>
                <div class="settings-row">
                    <label>Evaluation interval (secs)</label>
                    <span class="settings-val">${{tier.evaluation_interval_secs ?? '—'}}</span>
                </div>
                <div class="settings-row">
                    <label>Migration bandwidth (MB/s)</label>
                    <span class="settings-val">${{tier.migration_bandwidth_limit_mbps ?? '—'}}</span>
                </div>
                <div class="settings-row">
                    <label>Cache redundancy escalation</label>
                    <span class="settings-val">${{tier.enable_cache_redundancy_escalation ? '✅ Yes' : '❌ No'}}</span>
                </div>
            </div>

            <div class="settings-section">
                <h3>Chunk Size</h3>
                <p class="settings-desc">Default chunk size for new volumes.</p>
                <div class="settings-row">
                    <label>Chunk size</label>
                    <span class="settings-val">${{d.chunk_size_mb ?? 128}} MB</span>
                </div>
                <p class="settings-note">Per-volume overrides available via <code>dtfs-cli volume create --chunk-size</code></p>
            </div>
        </div>

        <div class="settings-section" style="margin-top:1.25rem">
            <h3>Raw Config JSON</h3>
            <p class="settings-desc">Current cluster config blob stored in Raft state. Edit carefully.</p>
            <textarea id="cfg-json" class="form-textarea" rows="6">${{
                esc(cfg.config ? JSON.stringify(cfg.config, null, 2) : '(empty)')
            }}</textarea>
            <div style="margin-top:.75rem;display:flex;gap:.75rem;align-items:center">
                <button class="btn-primary" onclick="saveConfigJson()">Apply Config JSON</button>
                <span id="cfg-save-status" style="font-size:.8rem;color:var(--text-muted)"></span>
            </div>
        </div>`;
        // Refresh session-secret status after Settings DOM is built.
        setSharedSecret(getSharedSecret());
    }}

    async function saveConfigJson() {{
        const json = document.getElementById('cfg-json')?.value || '';
        const status = document.getElementById('cfg-save-status');
        if (status) status.textContent = 'Saving…';
        try {{
            // Validate JSON first
            JSON.parse(json === '(empty)' ? '{{}}' : json);
            const {{ resp, json: result, authError }} = await mutateFetch('/api/v1/config', {{
                body: JSON.stringify({{ config_json: json === '(empty)' ? '' : json }}),
            }});
            if (status) status.textContent = resp.ok
                ? 'Applied (v' + (result && result.version) + ')'
                : (authError || ('Error: ' + JSON.stringify(result)));
        }} catch(e) {{
            if (status) status.textContent = String(e.message || e);
        }}
    }}

    // ── Bootstrap ────────────────────────────────────────────────────────────
    updateAuthBadge();
    route();
</script>
</body>
</html>"##,
        cluster_name = escape_html(cluster_name),
        hostname = escape_html(hostname),
        repo = dtfs_common::REPO_URL,
    )
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
            "/set-schedules`",
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
