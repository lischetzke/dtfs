//! Embedded manager WebUI.
//!
//! Serves a functional single-page dashboard from the manager binary.
//! In a future production build this would serve a compiled React/Vite SPA
//! via `rust-embed`; for now it renders a self-contained HTML page with
//! vanilla JS that fetches data from the REST API.
//!
//! **Home page sections (matching the plan):**
//! - Health overview banner (HEALTHY / DEGRADED / CRITICAL)
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

async fn index_page(State(state): State<Arc<WebUiState>>) -> Html<String> {
    Html(render_dashboard(&state.cluster_name, &state.manager_hostname))
}

async fn stylesheet() -> Response {
    let css = include_str!("webui_assets/styles.css");
    (StatusCode::OK, [(header::CONTENT_TYPE, "text/css")], css.to_string()).into_response()
}

async fn favicon() -> Response {
    let svg = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
        <rect width="64" height="64" rx="12" fill="#2563eb"/>
        <text x="32" y="44" font-size="32" fill="white" text-anchor="middle" font-family="monospace">D</text>
    </svg>"##;
    (StatusCode::OK, [(header::CONTENT_TYPE, "image/svg+xml")], svg.to_string()).into_response()
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

        /* ── Nav ── */
        .nav {{
            background: var(--surface);
            border-bottom: 1px solid var(--border);
            padding: 0 1.25rem;
            display: flex; gap: 0; overflow-x: auto;
        }}
        .nav a {{
            color: var(--text-muted); text-decoration: none;
            padding: .55rem .9rem; font-size: .85rem;
            border-bottom: 2px solid transparent; white-space: nowrap;
            transition: color .15s, border-color .15s;
        }}
        .nav a:hover {{ color: var(--text); border-bottom-color: var(--border); }}
        .nav a.active {{ color: var(--text); border-bottom-color: var(--primary); }}

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
        .badge-maintenance {{ background: #3b82f622; color: var(--primary); }}
        .badge-draining   {{ background: #3b82f622; color: var(--primary); }}
        .badge-hot  {{ background: #ef444422; color: var(--danger); }}
        .badge-warm {{ background: #f59e0b22; color: var(--warning); }}
        .badge-cold {{ background: #3b82f622; color: var(--primary); }}
        .badge-tape {{ background: #a855f722; color: var(--tape); }}
        .badge-manager {{ background: #6366f122; color: #818cf8; }}
        /* Bug AX — Raft-leader marker on the Nodes page. Bright accent so
           it stands out next to the normal role/health badges. */
        .badge-leader {{ background: var(--primary); color: white; letter-spacing: .04em; }}
        .badge-data    {{ background: #22c55e22; color: var(--success); }}
        /* Bug AN — clients previously had no badge style, so the role
           column on Nodes rendered an unstyled "client" text without
           the pill that data/manager rows have. */
        .badge-client  {{ background: #06b6d422; color: #22d3ee; }}
        .badge-unknown {{ background: #6b728022; color: var(--text-muted); }}
        /* Bug AS — right-click context menu for the Data Explorer. */
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
        /* Bug AO — racks lay out HORIZONTALLY inside a DC (left-to-right);
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
        <span id="refresh-time">Loading…</span>
    </div>
</div>

<nav class="nav">
    <a href="/ui" class="active">Home</a>
    <a href="/ui/topology">Topology</a>
    <a href="/ui/nodes">Nodes</a>
    <a href="/ui/volumes">Volumes</a>
    <a href="/ui/explorer">Data Explorer</a>
    <a href="/ui/migrations">Migrations</a>
    <a href="/ui/audit">Audit Log</a>
    <a href="/ui/settings">Settings</a>
    <a href="https://github.com/leduc/dtfs2/tree/master/docs" target="_blank">Docs ↗</a>
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

<!-- ── Flush cache dialog (Bug AV — operator-driven cache eviction) ── -->
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

<!-- ── Volume edit dialog (Bug AV — AV-4-4 inline policy editor) ── -->
<div id="volume-edit-modal" class="modal-backdrop hidden" onclick="if(event.target===this)closeVolumeEdit()">
    <div class="modal-card">
        <h3>Edit volume cache policy</h3>
        <div style="font-size:.78rem;color:var(--text-muted);margin-bottom:1rem">
            Updates <code>cache_replicas</code> for the volume via
            <code>POST /api/v1/volumes/{{id}}/set-cache</code>. Empty fields are left
            unchanged. Mandatory replicas and the legacy <code>replication</code>
            block are NOT touched here — use the CLI for those.
        </div>
        <div class="modal-row">
            <label>Volume ID</label>
            <input id="ve-volume-id" class="form-input" readonly>
        </div>
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
        <div class="modal-actions">
            <button class="btn-secondary" onclick="closeVolumeEdit()">Cancel</button>
            <button class="btn-primary"  onclick="submitVolumeEdit()" id="ve-submit">Apply</button>
        </div>
        <div id="ve-result" class="modal-result" style="display:none"></div>
    </div>
</div>

<div class="footer">
    DTFS — Distributed Tiered Filesystem &nbsp;·&nbsp;
    <a href="/api/v1/cluster/status" style="color:var(--primary)">API</a> &nbsp;·&nbsp;
    <a href="https://github.com/leduc/dtfs2/tree/master/docs" style="color:var(--primary)" target="_blank">Docs</a>
</div>

<script>
    // ── Utilities ────────────────────────────────────────────────────────────

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

    // Bug AT — set the innerHTML of an element while preserving the
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
        const [stats, nodes] = await Promise.all([
            fetch('/api/v1/stats').then(r => r.json()).catch(() => null),
            fetch('/api/v1/nodes').then(r => r.json()).catch(() => []),
        ]);

        document.getElementById('refresh-time').textContent =
            'Updated ' + new Date().toLocaleTimeString();

        if (!stats) {{
            setHealthBanner('unknown', 'Unknown', 'Could not reach API', '');
            return;
        }}

        // ── Health banner
        const h = stats.health || 'unknown';
        const detail = stats.healthy_nodes + '/' + stats.total_nodes + ' nodes healthy';
        const right = 'Config v' + (stats.config_version || 0) +
                      ' · ' + (stats.file_count || 0) + ' files';
        setHealthBanner(h, h.toUpperCase(), detail, right);

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

        // ── Bug AK — logical claimed bytes vs phantom (metadata without on-disk data).
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

        // ── Last scrub
        renderScrub(stats.last_scrub);

        // ── Nodes table
        renderNodes(nodes);
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
            const badge = `<span class="badge badge-${{t.tier}}" style="text-transform:capitalize">${{t.tier}}</span>`;
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

    function renderScrub(s) {{
        const el = document.getElementById('scrub-content');
        if (!s) {{
            el.innerHTML = '<div class="scrub-placeholder">No scrub result recorded yet. ' +
                'Run <code>dtfs-cli scrub --level deep</code> to trigger one.</div>';
            return;
        }}
        const when = new Date(s.completed_at_ms).toLocaleString();
        const ago  = timeAgo(s.completed_at_ms);
        const dur  = fmtDuration(s.duration_secs);
        const lvlBadge = s.level === 'deep'
            ? '<span class="badge badge-healthy">Deep</span>'
            : '<span class="badge badge-warm">Quick</span>';

        el.innerHTML = `
            <div class="scrub-meta">
                <div class="scrub-meta-item">${{lvlBadge}}</div>
                <div class="scrub-meta-item"><span>Node:</span> ${{s.node_id}}</div>
                <div class="scrub-meta-item"><span>Disk:</span> ${{s.disk_path}}</div>
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
            </div>`;
    }}

    // Bug AN — stable sort + per-role grouping for the Home page "Data &
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
            tbody.innerHTML = '<tr><td colspan="9" class="placeholder">No nodes registered yet.</td></tr>';
            document.getElementById('nodes-summary').textContent = '';
            return;
        }}

        const sorted = sortNodesForHome(nodes);
        const healthy = sorted.filter(n => (n.health||'').toLowerCase() === 'healthy').length;
        document.getElementById('nodes-summary').textContent =
            healthy + '/' + sorted.length + ' healthy';

        // Bug AN — split the "Used" column into separate cells for bar
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
            // Bug AP — manager nodes have no disks of their own; the
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
            return `<tr>
                <td style="font-size:.78rem;color:var(--text-muted);font-family:monospace">
                    ${{(n.node_id||'').slice(0,8)}}…</td>
                <td><b>${{n.hostname||'—'}}</b></td>
                <td><span class="${{rc}}">${{role}}</span></td>
                <td style="font-size:.78rem;color:var(--text-muted)">${{n.location||'—'}}</td>
                ${{diskCell}}
                ${{capCell}}
                ${{usedBarCell}}
                ${{usedValCell}}
                <td><span class="${{hc}}">${{n.health||'unknown'}}</span></td>
            </tr>`;
        }}).join('');
    }}

    // ── SPA Router ───────────────────────────────────────────────────────────

    const ROUTES = {{
        '/ui':            renderHome,
        '/ui/':           renderHome,
        '/ui/topology':   renderTopology,
        '/ui/nodes':      renderNodesList,
        '/ui/volumes':    renderVolumes,
        '/ui/explorer':   renderExplorer,
        '/ui/migrations': renderMigrations,
        '/ui/audit':      renderAudit,
        '/ui/settings':   renderSettings,
    }};

    let refreshTimer = null;

    function navigate(path) {{
        history.pushState({{}}, '', path);
        route();
    }}

    function route() {{
        const path = window.location.pathname.replace(/\/$/, '') || '/ui';
        const normPath = path === '' ? '/ui' : path;

        // Update active nav link
        document.querySelectorAll('.nav a').forEach(a => {{
            const href = a.getAttribute('href').replace(/\/$/, '');
            a.classList.toggle('active', href === normPath);
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
            <!-- Bug AN — drop the "After replication factor (÷ hot RF)"
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
                <!-- Bug AK — show logical bytes claimed by file metadata vs net actually on disk. -->
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
        <div class="scrub-card"><h3>Last Scrub</h3><div id="scrub-content"><div class="scrub-placeholder">No scrub result recorded yet.</div></div></div>
        <div class="table-card">
            <div class="table-card-header">
                <h3>Data &amp; Manager Nodes</h3>
                <span style="font-size:.75rem;color:var(--text-muted)" id="nodes-summary"></span>
            </div>
            <table><thead><tr><th>ID</th><th>Hostname</th><th>Role</th><th>Location</th><th>Disks</th><th>Capacity</th><th style="width:120px">Utilization</th><th>Used</th><th>Health</th></tr></thead>
            <tbody id="nodes-table"><tr><td colspan="9" class="placeholder">Fetching node list…</td></tr></tbody></table>
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
        // Bug AT — refresh ONLY the topo-tree element on each tick (not
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
            html += `<div class="topo-region"><div class="topo-region-label">🌐 Region: <b>${{region}}</b></div>`;
            for (const [dc, racks] of sortedEntries(dcs)) {{
                // Bug AO — wrap all racks of this DC in a horizontal flex
                // container (.topo-racks); each .topo-rack inside is a
                // vertical column of hosts. Yields the visual layout:
                //
                //   [Rack A]   [Rack B]   [Rack C]
                //   host-1      host-1     host-1
                //   host-2      host-2     host-2
                //
                // Matches operator mental model: racks are physical
                // siblings; hosts within a rack are stacked.
                html += `<div class="topo-dc"><div class="topo-dc-label">🏢 Datacenter: <b>${{dc}}</b></div><div class="topo-racks">`;
                for (const [rack, rnodes] of sortedEntries(racks)) {{
                    html += `<div class="topo-rack"><div class="topo-rack-label">🗄 Rack: <b>${{rack}}</b></div><div class="topo-nodes">`;
                    // Sort hosts (nodes) inside the rack by hostname ASC.
                    const sortedNodes = [...rnodes].sort((a, b) =>
                        (a.hostname || '').localeCompare(b.hostname || '', undefined,
                            {{sensitivity: 'base', numeric: true}}));
                    for (const n of sortedNodes) {{
                        const h = (n.health||'unknown').toLowerCase();
                        const role = (n.role||'data').toLowerCase();
                        html += `<div class="topo-node">
                            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:.4rem">
                                <b>${{n.hostname||'—'}}</b>
                                <span class="badge badge-${{h}}">${{n.health||'unknown'}}</span>
                            </div>
                            <div style="font-size:.75rem;color:var(--text-muted);margin-bottom:.3rem">
                                <span class="badge badge-${{role}}">${{role}}</span>
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
        // Bug AT — preserve scroll position across refresh. innerHTML
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

        // Bug AT — preserve scroll position across the periodic refresh
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

            // Bug AP — manager + client nodes have no disks of their own;
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

            // Bug AR — per-disk table for data nodes. Lets the operator
            // spot a single full disk (which would otherwise be hidden
            // behind the node-level aggregate, e.g. data-primary01 with
            // 4 cold disks where one fills to 100% while the others stay
            // at 2% — the symptom that drove the Bug AD fix).
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
                                       <td style="padding:.25rem .4rem;font-family:monospace">${{d.disk_id}}</td>
                                       <td style="padding:.25rem .4rem"><span class="badge badge-${{(d.tier||'').toLowerCase()}}">${{d.tier||'—'}}</span></td>
                                       <td style="padding:.25rem .4rem;font-family:monospace;color:var(--text-muted);max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${{d.mount_path||''}}">${{d.mount_path||'—'}}</td>
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
                                       <td style="padding:.25rem .4rem"><span class="badge badge-${{dstatus}}">${{d.status||'—'}}</span></td>
                                   </tr>`;
                               }}).join('')}}
                           </tbody>
                       </table>
                   </div>`
                : '';

            // Bug AX — show the manager's Raft-leader status, render
            // a client's source IP (clients have no gRPC listener), and
            // surface last_seen on every node so operators can see how
            // fresh the heartbeat is at a glance.
            const leaderBadge = (isManager && n.is_leader)
                ? '<span class="badge badge-leader" style="margin-right:.4rem" title="Current Raft leader">LEADER</span>'
                : '';
            const addrRow = isClient
                ? `<div class="node-meta-item"><span>Client IP</span>${{n.client_ip || '—'}}</div>`
                : `<div class="node-meta-item"><span>gRPC</span>${{n.grpc_addr||'—'}}</div>`;
            const lastSeenRow = n.last_seen_ms
                ? `<div class="node-meta-item"><span>Last seen</span>${{timeAgo(n.last_seen_ms)}}</div>`
                : `<div class="node-meta-item"><span>Last seen</span>—</div>`;
            // Bug AY — show binary version + build date. Empty values
            // mean the node pre-dates Bug AY or wasn't queried directly
            // (older clients, peer managers).
            const versionRow = (n.version || n.build_date)
                ? `<div class="node-meta-item"><span>Version</span>${{n.version || '—'}}${{n.build_date ? ' / ' + n.build_date : ''}}</div>`
                : '';
            return `<div class="node-card">
                <div class="node-card-header">
                    <div>
                        <span class="badge badge-${{role}}" style="margin-right:.4rem">${{role}}</span>
                        ${{leaderBadge}}
                        <b style="font-size:1rem">${{n.hostname||'—'}}</b>
                        <span style="font-size:.78rem;color:var(--text-muted);margin-left:.5rem;font-family:monospace">
                            id:${{(n.node_id||'').slice(0,12)}}…
                        </span>
                    </div>
                    <div style="display:flex;gap:.5rem;align-items:center">
                        <span class="badge badge-${{h}}">${{n.health||'unknown'}}</span>
                        ${{isStorageRole ? `<button class="btn-maint" onclick="toggleMaintenance('${{n.node_id}}', ${{!isMaintenance}})">
                            ${{isMaintenance ? 'Exit Maintenance' : 'Set Maintenance'}}
                        </button>` : ''}}
                    </div>
                </div>
                <div class="node-card-body">
                    <div class="node-meta">
                        ${{addrRow}}
                        <div class="node-meta-item"><span>Location</span>${{n.location||'—'}}</div>
                        ${{lastSeenRow}}
                        ${{versionRow}}
                        ${{sizeMetaRows}}
                    </div>
                    ${{utilBar}}
                    ${{diskTable}}
                </div>
            </div>`;
        }}).join('');

        window.scrollTo(0, savedScroll);
    }}

    async function toggleMaintenance(nodeId, enable) {{
        await fetch('/api/v1/nodes/' + nodeId + '/maintenance', {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/json' }},
            body: JSON.stringify({{ enable }}),
        }}).catch(() => {{}});
        await refreshNodesList();
    }}

    // ── Volumes page ──────────────────────────────────────────────────────────

    // Bug AV (AV-4-3 / AV-4-4) — Volumes page extended with a per-row
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
            ? `dc=${{v.anchor.datacenter||'—'}}, required_tier=${{v.anchor.required_tier||'(any)'}}`
            : '(none)';
        return `<div class="placeholder" style="text-align:left;padding:1rem;background:rgba(0,0,0,.04);border-radius:.5rem">
            <div style="font-weight:bold;margin-bottom:.5rem">Policy for ${{v.volume_id}}</div>
            <div style="font-family:monospace;font-size:.78rem;line-height:1.6">
                <div>Mandatory replicas: hot=${{(v.mandatory_replicas||{{}}).hot||0}}, warm=${{(v.mandatory_replicas||{{}}).warm||0}}, cold=${{(v.mandatory_replicas||{{}}).cold||0}}, tape=${{(v.mandatory_replicas||{{}}).tape||0}}</div>
                <div>Cache replicas (per_dc=${{cr.per_dc===true?'true':'false'}}, promote_reads=${{cr.promote_reads||0}}, promote_window_secs=${{cr.promote_window_secs||0}}, cooldown_secs=${{cr.cooldown_secs||0}}):</div>
                <div style="margin-left:1rem">${{tier('hot')}}</div>
                <div style="margin-left:1rem">${{tier('warm')}}</div>
                <div style="margin-left:1rem">${{tier('cold')}}</div>
                <div style="margin-left:1rem">${{tier('tape')}}</div>
                <div>Anchor: ${{anchor}}</div>
                <div>Chunk size: ${{fmtBytes(v.chunk_size_bytes||0)}}</div>
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
            el.innerHTML = '<div class="placeholder">No volumes yet. Create one with <code>dtfs-cli volume create</code>.</div>';
            return;
        }}
        el.innerHTML = '<div class="table-card"><table>' +
            '<thead><tr><th>Volume ID</th><th>Files</th><th>Total Size</th><th>Avg File Size</th><th>Cache</th><th>Anchor</th><th>Actions</th></tr></thead><tbody>' +
            vols.flatMap(v => {{
                const vid = v.volume_id||'—';
                const safeVid = vid.replace(/'/g, "&#39;");
                const cacheLabel = (v.cache_replicas && v.cache_replicas.warm)
                    ? `warm min=${{v.cache_replicas.warm.min||0}}/max=${{v.cache_replicas.warm.max||0}}${{v.cache_replicas.per_dc?' per-dc':''}}`
                    : '—';
                const anchorLabel = v.anchor ? (v.anchor.datacenter||'—') : '—';
                const isOpen = window._volumesExpanded.has(vid);
                const rows = [`<tr>
                    <td><b style="font-family:monospace">${{vid}}</b></td>
                    <td>${{(v.file_count||0).toLocaleString()}}</td>
                    <td>${{fmtBytes(v.total_size||0)}}</td>
                    <td>${{v.file_count ? fmtBytes(Math.round((v.total_size||0)/v.file_count)) : '—'}}</td>
                    <td>${{cacheLabel}}</td>
                    <td>${{anchorLabel}}</td>
                    <td style="white-space:nowrap">
                        <button class="btn-secondary" style="padding:.25rem .5rem;font-size:.78rem"
                            onclick="toggleVolumeRow('${{safeVid}}')">${{isOpen?'Hide':'Details'}}</button>
                        <button class="btn-secondary" style="padding:.25rem .5rem;font-size:.78rem"
                            onclick="openVolumeEdit('${{safeVid}}')">Edit policy…</button>
                        <button class="btn-secondary" style="padding:.25rem .5rem;font-size:.78rem"
                            onclick="openFlushCache('/')">Flush cache…</button>
                    </td>
                </tr>`];
                if (isOpen) {{
                    rows.push(`<tr><td colspan="7">${{renderVolumeCard(v)}}</td></tr>`);
                }}
                return rows;
            }}).join('') +
            '</tbody></table></div>';
    }}

    async function renderVolumes() {{
        document.getElementById('page-content').innerHTML =
            '<div class="page-header"><h2>Volumes</h2><p class="page-desc">Storage volumes, file distribution, and cache policy (Bug AV)</p></div>' +
            '<div id="volumes-list"><div class="placeholder">Loading…</div></div>';
        const data = await fetch('/api/v1/volumes').then(r => r.json()).catch(() => ({{count:0, volumes:[]}}));
        window._volumesData = data.volumes || [];
        rerenderVolumesTable();
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
            document.getElementById('ve-per-dc').checked = cr.per_dc === true;
            document.getElementById('ve-promote-reads').value = cr.promote_reads || '';
            document.getElementById('ve-promote-window').value = cr.promote_window_secs || '';
            document.getElementById('ve-cooldown').value = cr.cooldown_secs || '';
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
        const body = {{
            warm_min: intOrNull('ve-warm-min'),
            warm_max: intOrNull('ve-warm-max'),
            hot_min:  intOrNull('ve-hot-min'),
            hot_max:  intOrNull('ve-hot-max'),
            per_dc:   document.getElementById('ve-per-dc').checked,
            promote_reads:       intOrNull('ve-promote-reads'),
            promote_window_secs: intOrNull('ve-promote-window'),
            cooldown_secs:       intOrNull('ve-cooldown'),
        }};
        // Client-side guard: warm.max >= warm.min when both provided.
        if (body.warm_min != null && body.warm_max != null
            && body.warm_max < body.warm_min) {{
            alert('warm.max must be >= warm.min');
            return;
        }}

        const btn = document.getElementById('ve-submit');
        btn.disabled = true; btn.textContent = 'Applying…';
        const result = document.getElementById('ve-result');
        result.style.display = 'block';
        result.textContent = 'Submitting…';

        try {{
            const r = await fetch(`/api/v1/volumes/${{encodeURIComponent(vid)}}/set-cache`, {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify(body),
            }});
            const j = await r.json().catch(() => ({{error: 'invalid JSON response'}}));
            if (!r.ok) {{
                result.textContent = 'Error ' + r.status + ': ' + (j.error || JSON.stringify(j));
            }} else {{
                result.textContent = j.changed
                    ? 'Updated. New policy:\n' + JSON.stringify(j.config, null, 2)
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

    // Bug AS — folder-aware data explorer.
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
            const onClick = `event.preventDefault();explorerNavigate('${{full.replace(/'/g, "&#39;")}}')`;
            return `<a href="#" onclick="${{onClick}}" style="color:var(--primary);text-decoration:none">${{seg}}</a>/`;
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
                (filter ? `No entries match "${{filter}}" in ${{cwd}}.` : `Empty folder: ${{cwd}}`) +
                '</div>';
            return;
        }}

        const folderRows = folderList.map(d => `<tr class="explorer-row" oncontextmenu="event.preventDefault();showCtxMenu(event,'${{d.path.replace(/'/g, "&#39;")}}',true)">
            <td style="font-size:1rem;width:1.5rem">📁</td>
            <td>
                <a href="#" onclick="event.preventDefault();explorerNavigate('${{d.path.replace(/'/g, "&#39;")}}')" style="color:var(--primary);text-decoration:none;font-family:monospace">${{d.name}}/</a>
            </td>
            <td style="font-size:.78rem;color:var(--text-muted)">${{d.fileCount.toLocaleString()}} file${{d.fileCount!==1?'s':''}}</td>
            <td>${{fmtBytes(d.totalSize)}}</td>
            <td></td>
            <td style="white-space:nowrap">
                <button class="btn-secondary" style="font-size:.72rem;padding:.2rem .5rem"
                        onclick="openMigrate('${{d.path.replace(/'/g, "&#39;")}}')" title="Migrate this folder">⋯</button>
            </td>
        </tr>`).join('');

        const fileRows = files.map(f => {{
            const safePath = (f.path || '').replace(/'/g, "&#39;");
            return `<tr class="explorer-row" oncontextmenu="event.preventDefault();showCtxMenu(event,'${{safePath}}',false)">
                <td style="font-size:1rem;width:1.5rem">📄</td>
                <td style="font-family:monospace;font-size:.85rem;max-width:340px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${{f.path||''}}">${{f.name}}</td>
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
                ${{folderList.length}} folder${{folderList.length!==1?'s':''}} · ${{files.length}} file${{files.length!==1?'s':''}} in <code>${{cwd}}</code> · right-click for actions
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
        const safe = (path || '').replace(/'/g, "&#39;");
        const parent = isFolder ? path : path.replace(/[^/]*$/, '');
        const safeParent = parent.replace(/'/g, "&#39;");
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
            dcs.map(dc => `<option value="${{dc}}">${{dc}}</option>`).join('');

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
            const resp = await fetch('/api/v1/migrations', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify(body),
            }});
            const data = await resp.json();
            if (!resp.ok) {{
                result.className = 'modal-result error';
                result.textContent = (data && data.error) ? data.error : ('HTTP ' + resp.status);
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

    // ── Bug AV (AV-3-4) — Flush cache modal ──────────────────────────────────

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
            const resp = await fetch('/api/v1/files/flush_cache', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify(body),
            }});
            const data = await resp.json();
            if (!resp.ok) {{
                result.className = 'modal-result error';
                result.textContent = (data && data.error) ? data.error : ('HTTP ' + resp.status);
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
        // Bug AT — wrap the innerHTML write in setHtmlPreserveScroll so
        // the 5s auto-refresh doesn't yank the user back to the top of
        // a long migration list. The server-side sort (Bug AM) plus
        // scroll preservation here means the table stays readable
        // during sustained migration storms.
        setHtmlPreserveScroll(el,
            '<div class="table-card"><table>' +
            '<thead><tr><th>Task</th><th>Chunk</th><th>Src → Tgt</th><th>Tier</th><th>Status</th><th>Attempts</th><th>Updated</th><th>Last error</th></tr></thead><tbody>' +
            tasks.map(t => `<tr>
                <td style="font-family:monospace;font-size:.75rem">${{t.task_id||'—'}}</td>
                <td style="font-family:monospace;font-size:.75rem;max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${{t.chunk_id||''}}">${{(t.chunk_id||'').slice(0,16)}}…</td>
                <td style="font-family:monospace;font-size:.78rem">${{t.source_node||'?'}} → ${{t.target_node||'?'}}</td>
                <td><span class="badge badge-${{(t.tier||'').toLowerCase()}}">${{t.tier||'—'}}</span></td>
                <td style="color:${{statusColor((t.status||'').toLowerCase())}};font-weight:600">${{t.status||'—'}}</td>
                <td>${{t.attempts ?? 0}}</td>
                <td style="font-size:.75rem;color:var(--text-muted)">${{t.updated_at_ms ? timeAgo(t.updated_at_ms) : '—'}}</td>
                <td style="font-size:.75rem;color:var(--text-muted);max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${{t.last_error||''}}">${{t.last_error||'—'}}</td>
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
                        ${{/* Bug AG — AuditEntry.timestamp is SECONDS-since-epoch (chrono::Utc::now().timestamp() in audit.rs). JS new Date() expects MS. Without *1000 every entry rendered as 1970-01-2x. */ e.timestamp ? new Date(e.timestamp * 1000).toLocaleString() : '—'}}
                    </td>
                    <td style="font-size:.8rem">${{e.actor||'—'}}</td>
                    <td style="font-family:monospace;font-size:.78rem">${{e.action||'—'}}</td>
                    <td style="font-size:.78rem;color:var(--text-muted);max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${{e.target||''}}">
                        ${{e.target||'—'}}
                    </td>
                    <td style="font-size:.78rem;color:${{resultColor}}">${{e.result||'—'}}</td>
                </tr>`;
            }}).join('') +
            '</tbody></table></div>';
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
                    <label>Migration bandwidth (MB/s)</label>
                    <span class="settings-val">${{tier.migration_bandwidth_limit_mbps ?? '—'}}</span>
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
                cfg.config ? JSON.stringify(cfg.config, null, 2) : '(empty)'
            }}</textarea>
            <div style="margin-top:.75rem;display:flex;gap:.75rem;align-items:center">
                <button class="btn-primary" onclick="saveConfigJson()">Apply Config JSON</button>
                <span id="cfg-save-status" style="font-size:.8rem;color:var(--text-muted)"></span>
            </div>
        </div>`;
    }}

    async function saveConfigJson() {{
        const json = document.getElementById('cfg-json')?.value || '';
        const status = document.getElementById('cfg-save-status');
        if (status) status.textContent = 'Saving…';
        try {{
            // Validate JSON first
            JSON.parse(json === '(empty)' ? '{{}}' : json);
            const resp = await fetch('/api/v1/config', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify({{ config_json: json === '(empty)' ? '' : json }}),
            }});
            const result = await resp.json();
            if (status) status.textContent = resp.ok
                ? '✅ Applied (v' + result.version + ')'
                : '❌ Error: ' + JSON.stringify(result);
        }} catch(e) {{
            if (status) status.textContent = '❌ ' + e.message;
        }}
    }}

    // ── Bootstrap ────────────────────────────────────────────────────────────
    route();
</script>
</body>
</html>"##,
        cluster_name = cluster_name,
        hostname = hostname,
    )
}
