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
            padding: .75rem 2rem;
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
            padding: 0 2rem;
            display: flex; gap: 0; overflow-x: auto;
        }}
        .nav a {{
            color: var(--text-muted); text-decoration: none;
            padding: .7rem 1.1rem; font-size: .85rem;
            border-bottom: 2px solid transparent; white-space: nowrap;
            transition: color .15s, border-color .15s;
        }}
        .nav a:hover {{ color: var(--text); border-bottom-color: var(--border); }}
        .nav a.active {{ color: var(--text); border-bottom-color: var(--primary); }}

        /* ── Health banner ── */
        .health-banner {{
            padding: .9rem 2rem;
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
        .container {{ max-width: 1440px; margin: 0 auto; padding: 1.5rem 2rem; }}
        .section-title {{
            font-size: .7rem; font-weight: 600; text-transform: uppercase;
            letter-spacing: .08em; color: var(--text-muted); margin-bottom: .75rem;
        }}

        /* ── Count cards (row of 4) ── */
        .counts-row {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 1rem; margin-bottom: 1.25rem;
        }}
        .count-card {{
            background: var(--surface); border: 1px solid var(--border);
            border-radius: .625rem; padding: 1rem 1.25rem;
            display: flex; align-items: center; gap: 1rem;
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
            grid-template-columns: 1fr 1fr 1fr;
            gap: 1rem; margin-bottom: 1.25rem;
        }}
        .metric-card {{
            background: var(--surface); border: 1px solid var(--border);
            border-radius: .625rem; padding: 1.25rem;
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
            border-radius: .625rem; padding: 1.25rem;
            margin-bottom: 1.25rem;
        }}
        .scrub-card h3 {{
            font-size: .75rem; font-weight: 600; text-transform: uppercase;
            letter-spacing: .06em; color: var(--text-muted); margin-bottom: .875rem;
        }}
        .scrub-meta {{ display: flex; flex-wrap: wrap; gap: .5rem 2rem; margin-bottom: .875rem; }}
        .scrub-meta-item {{ font-size: .85rem; }}
        .scrub-meta-item span {{ color: var(--text-muted); margin-right: .3rem; }}
        .scrub-stats {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: .75rem; }}
        .scrub-stat {{ text-align: center; }}
        .scrub-stat .stat-val {{ font-size: 1.4rem; font-weight: 700; }}
        .scrub-stat .stat-lbl {{ font-size: .7rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: .05em; margin-top: .15rem; }}
        .scrub-stat.corrupt .stat-val {{ color: var(--danger); }}
        .scrub-stat.missing .stat-val {{ color: var(--warning); }}
        .scrub-placeholder {{ font-size: .85rem; color: var(--text-muted); font-style: italic; }}

        /* ── Nodes table ── */
        .table-card {{
            background: var(--surface); border: 1px solid var(--border);
            border-radius: .625rem; overflow: hidden; margin-bottom: 1.25rem;
        }}
        .table-card-header {{
            padding: .875rem 1.25rem;
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
            padding: .625rem 1.25rem;
            border-bottom: 1px solid var(--border);
        }}
        th {{
            font-size: .7rem; color: var(--text-muted);
            text-transform: uppercase; letter-spacing: .05em;
            background: var(--surface2);
        }}
        td {{ font-size: .83rem; }}
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
        .badge-data    {{ background: #22c55e22; color: var(--success); }}

        .placeholder {{
            text-align: center; color: var(--text-muted);
            padding: 2.5rem; font-style: italic; font-size: .85rem;
        }}

        /* ── Page header ── */
        .page-header {{ margin-bottom: 1.5rem; }}
        .page-header h2 {{ font-size: 1.25rem; font-weight: 700; margin-bottom: .25rem; }}
        .page-desc {{ font-size: .85rem; color: var(--text-muted); }}

        /* ── Topology ── */
        .topo-root {{ display: flex; flex-direction: column; gap: 1rem; }}
        .topo-region {{ background: var(--surface); border: 1px solid var(--border); border-radius: .625rem; padding: 1rem 1.25rem; }}
        .topo-region-label {{ font-size: .9rem; font-weight: 700; margin-bottom: .75rem; }}
        .topo-dc {{ background: var(--surface2); border: 1px solid var(--border); border-radius: .5rem; padding: .875rem 1rem; margin-bottom: .75rem; }}
        .topo-dc-label {{ font-size: .85rem; font-weight: 600; margin-bottom: .5rem; color: var(--primary); }}
        .topo-rack {{ margin: .5rem 0; }}
        .topo-rack-label {{ font-size: .78rem; color: var(--text-muted); margin-bottom: .4rem; text-transform: uppercase; letter-spacing: .04em; }}
        .topo-nodes {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: .75rem; }}
        .topo-node {{ background: var(--surface); border: 1px solid var(--border); border-radius: .5rem; padding: .75rem 1rem; font-size: .83rem; }}

        /* ── Node cards ── */
        .node-card {{ background: var(--surface); border: 1px solid var(--border); border-radius: .625rem; margin-bottom: 1rem; overflow: hidden; }}
        .node-card-header {{ padding: .875rem 1.25rem; display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border); background: var(--surface2); }}
        .node-card-body {{ padding: .875rem 1.25rem; }}
        .node-meta {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: .4rem .75rem; margin-bottom: .5rem; }}
        .node-meta-item {{ font-size: .8rem; }}
        .node-meta-item span {{ color: var(--text-muted); margin-right: .3rem; font-size: .75rem; }}
        .btn-maint {{
            background: transparent; border: 1px solid var(--border); color: var(--text-muted);
            padding: .25rem .75rem; border-radius: .375rem; font-size: .75rem; cursor: pointer;
            transition: all .15s;
        }}
        .btn-maint:hover {{ border-color: var(--primary); color: var(--primary); }}

        /* ── Settings ── */
        .settings-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 1rem; }}
        .settings-section {{
            background: var(--surface); border: 1px solid var(--border);
            border-radius: .625rem; padding: 1.25rem;
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

        /* ── Footer ── */
        .footer {{
            text-align: center; color: var(--text-muted);
            font-size: .75rem; padding: 1rem 2rem 2rem;
        }}
        @media (max-width: 900px) {{
            .counts-row {{ grid-template-columns: repeat(2, 1fr); }}
            .metrics-row {{ grid-template-columns: 1fr; }}
            .scrub-stats {{ grid-template-columns: repeat(2, 1fr); }}
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
    <a href="/ui/audit">Audit Log</a>
    <a href="/ui/settings">Settings</a>
    <a href="/docs/" target="_blank">Docs ↗</a>
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

<div class="footer">
    DTFS — Distributed Tiered Filesystem &nbsp;·&nbsp;
    <a href="/api/v1/cluster/status" style="color:var(--primary)">API</a> &nbsp;·&nbsp;
    <a href="/docs/" style="color:var(--primary)">Docs</a>
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

    function renderNodes(nodes) {{
        const tbody = document.getElementById('nodes-table');
        if (!nodes || nodes.length === 0) {{
            tbody.innerHTML = '<tr><td colspan="8" class="placeholder">No nodes registered yet.</td></tr>';
            document.getElementById('nodes-summary').textContent = '';
            return;
        }}

        const healthy = nodes.filter(n => (n.health||'').toLowerCase() === 'healthy').length;
        document.getElementById('nodes-summary').textContent =
            healthy + '/' + nodes.length + ' healthy';

        tbody.innerHTML = nodes.map(n => {{
            const h  = (n.health || 'unknown').toLowerCase();
            const hc = 'badge badge-' + h;
            const role = (n.role || 'data').toLowerCase();
            const rc = 'badge badge-' + role;
            const usedPct = n.total_capacity
                ? Math.round(n.used_capacity / n.total_capacity * 100) : 0;
            const barFill = usedPct > 90 ? 'var(--danger)'
                          : usedPct > 75 ? 'var(--warning)' : 'var(--primary)';
            return `<tr>
                <td style="font-size:.78rem;color:var(--text-muted);font-family:monospace">
                    ${{(n.node_id||'').slice(0,8)}}…</td>
                <td><b>${{n.hostname||'—'}}</b></td>
                <td><span class="${{rc}}">${{role}}</span></td>
                <td style="font-size:.78rem;color:var(--text-muted)">${{n.location||'—'}}</td>
                <td>${{n.disk_count||0}}</td>
                <td>${{fmtBytes(n.total_capacity||0)}}</td>
                <td>
                    <div style="display:flex;align-items:center;gap:.5rem">
                        <div style="flex:1;height:4px;background:var(--border);border-radius:2px;min-width:48px">
                            <div style="height:100%;border-radius:2px;background:${{barFill}};width:${{usedPct}}%"></div>
                        </div>
                        <span style="font-size:.78rem;white-space:nowrap">${{fmtBytes(n.used_capacity||0)}}</span>
                    </div>
                </td>
                <td><span class="${{hc}}">${{n.health||'unknown'}}</span></td>
            </tr>`;
        }}).join('');
    }}

    // ── SPA Router ───────────────────────────────────────────────────────────

    const ROUTES = {{
        '/ui':          renderHome,
        '/ui/':         renderHome,
        '/ui/topology': renderTopology,
        '/ui/nodes':    renderNodesList,
        '/ui/volumes':  renderVolumes,
        '/ui/explorer': renderExplorer,
        '/ui/audit':    renderAudit,
        '/ui/settings': renderSettings,
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
            <div class="metric-card">
                <h3>Net Usable Capacity</h3>
                <div style="font-size:.72rem;color:var(--text-muted);margin-bottom:.75rem">After replication factor (÷ hot RF)</div>
                <div class="metric-row"><span class="metric-label">Usable</span><span class="metric-value" id="net-usable">—</span></div>
                <div class="metric-row"><span class="metric-label">Used</span><span class="metric-value" id="net-used">—</span></div>
                <div class="progress-wrap">
                    <div style="display:flex;justify-content:space-between;margin-bottom:.3rem">
                        <span style="font-size:.75rem;color:var(--text-muted)">Utilisation</span>
                        <span style="font-size:.75rem;color:var(--text-muted)" id="net-pct">—</span>
                    </div>
                    <div class="progress-bar"><div class="progress-fill success" id="net-bar" style="width:0%"></div></div>
                </div>
                <div class="metric-row" style="margin-top:.25rem"><span class="metric-label">Free</span><span class="metric-value" id="net-free" style="color:var(--success)">—</span></div>
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
            <table><thead><tr><th>ID</th><th>Hostname</th><th>Role</th><th>Location</th><th>Disks</th><th>Capacity</th><th>Used</th><th>Health</th></tr></thead>
            <tbody id="nodes-table"><tr><td colspan="8" class="placeholder">Fetching node list…</td></tr></tbody></table>
        </div>`;
        fetchStats();
        refreshTimer = setInterval(fetchStats, 8000);
    }}

    // ── Topology page ─────────────────────────────────────────────────────────

    async function renderTopology() {{
        document.getElementById('page-content').innerHTML =
            '<div class="page-header"><h2>Topology</h2><p class="page-desc">Region → DC → Rack → Host hierarchy</p></div>' +
            '<div id="topo-tree" class="topo-root"><div class="placeholder">Loading topology…</div></div>';

        const nodes = await fetch('/api/v1/nodes').then(r => r.json()).catch(() => []);

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

        const topo = document.getElementById('topo-tree');
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
                html += `<div class="topo-dc"><div class="topo-dc-label">🏢 Datacenter: <b>${{dc}}</b></div>`;
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
                html += '</div>';
            }}
            html += '</div>';
        }}
        topo.innerHTML = html;
        refreshTimer = setInterval(renderTopology, 15000);
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
        el.innerHTML = nodes.map(n => {{
            const h = (n.health||'unknown').toLowerCase();
            const role = (n.role||'data').toLowerCase();
            const pct = n.total_capacity ? Math.round(n.used_capacity/n.total_capacity*100) : 0;
            const barCol = pct>90?'var(--danger)':pct>75?'var(--warning)':'var(--primary)';
            const isMaintenance = h === 'maintenance';
            return `<div class="node-card">
                <div class="node-card-header">
                    <div>
                        <span class="badge badge-${{role}}" style="margin-right:.4rem">${{role}}</span>
                        <b style="font-size:1rem">${{n.hostname||'—'}}</b>
                        <span style="font-size:.78rem;color:var(--text-muted);margin-left:.5rem;font-family:monospace">
                            id:${{(n.node_id||'').slice(0,12)}}…
                        </span>
                    </div>
                    <div style="display:flex;gap:.5rem;align-items:center">
                        <span class="badge badge-${{h}}">${{n.health||'unknown'}}</span>
                        <button class="btn-maint" onclick="toggleMaintenance('${{n.node_id}}', ${{!isMaintenance}})">
                            ${{isMaintenance ? 'Exit Maintenance' : 'Set Maintenance'}}
                        </button>
                    </div>
                </div>
                <div class="node-card-body">
                    <div class="node-meta">
                        <div class="node-meta-item"><span>gRPC</span>${{n.grpc_addr||'—'}}</div>
                        <div class="node-meta-item"><span>Location</span>${{n.location||'—'}}</div>
                        <div class="node-meta-item"><span>Total</span>${{fmtBytes(n.total_capacity||0)}}</div>
                        <div class="node-meta-item"><span>Used</span>${{fmtBytes(n.used_capacity||0)}} (${{pct}}%)</div>
                    </div>
                    <div style="margin:.5rem 0">
                        <div class="progress-bar" style="height:8px">
                            <div style="height:100%;border-radius:4px;background:${{barCol}};width:${{pct}}%"></div>
                        </div>
                    </div>
                </div>
            </div>`;
        }}).join('');
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

    async function renderVolumes() {{
        document.getElementById('page-content').innerHTML =
            '<div class="page-header"><h2>Volumes</h2><p class="page-desc">Storage volumes and their file distribution</p></div>' +
            '<div id="volumes-list"><div class="placeholder">Loading…</div></div>';
        const data = await fetch('/api/v1/volumes').then(r => r.json()).catch(() => ({{count:0, volumes:[]}}));
        const el = document.getElementById('volumes-list');
        if (!el) return;
        const vols = data.volumes || [];
        if (vols.length === 0) {{
            el.innerHTML = '<div class="placeholder">No volumes yet. Create one with <code>dtfs-cli volume create</code>.</div>';
            return;
        }}
        el.innerHTML = '<div class="table-card"><table>' +
            '<thead><tr><th>Volume ID</th><th>Files</th><th>Total Size</th><th>Avg File Size</th></tr></thead><tbody>' +
            vols.map(v => `<tr>
                <td><b style="font-family:monospace">${{v.volume_id||'—'}}</b></td>
                <td>${{(v.file_count||0).toLocaleString()}}</td>
                <td>${{fmtBytes(v.total_size||0)}}</td>
                <td>${{v.file_count ? fmtBytes(Math.round((v.total_size||0)/v.file_count)) : '—'}}</td>
            </tr>`).join('') +
            '</tbody></table></div>';
    }}

    // ── Data Explorer page ────────────────────────────────────────────────────

    async function renderExplorer() {{
        document.getElementById('page-content').innerHTML =
            `<div class="page-header">
                <h2>Data Explorer</h2>
                <p class="page-desc">Browse files across all volumes</p>
            </div>
            <div style="display:flex;gap:.75rem;margin-bottom:1rem">
                <input id="explorer-filter" type="text" class="form-input" placeholder="Filter by path…" style="max-width:320px">
                <button class="btn-primary" onclick="loadExplorer()">Refresh</button>
            </div>
            <div id="explorer-list"><div class="placeholder">Loading…</div></div>`;
        document.getElementById('explorer-filter').addEventListener('input', () => loadExplorer());
        await loadExplorer();
    }}

    async function loadExplorer() {{
        const filter = (document.getElementById('explorer-filter')?.value || '').toLowerCase();
        const data = await fetch('/api/v1/files').then(r => r.json()).catch(() => ({{count:0,files:[]}}));
        const el = document.getElementById('explorer-list');
        if (!el) return;
        let files = data.files || [];
        if (filter) files = files.filter(f => (f.path||'').toLowerCase().includes(filter));
        if (files.length === 0) {{
            el.innerHTML = '<div class="placeholder">' + (filter ? 'No files match "' + filter + '".' : 'No files stored yet.') + '</div>';
            return;
        }}
        el.innerHTML = '<div class="table-card"><table>' +
            '<thead><tr><th>Path</th><th>Volume</th><th>Chunks</th><th>Size</th><th>Created</th></tr></thead><tbody>' +
            files.map(f => `<tr>
                <td style="font-family:monospace;font-size:.8rem;max-width:360px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${{f.path||''}}">
                    ${{f.path||'—'}}
                </td>
                <td style="font-family:monospace;font-size:.78rem;color:var(--text-muted)">${{f.volume_id||'—'}}</td>
                <td>${{f.chunk_count||0}}</td>
                <td>${{fmtBytes(f.total_size||0)}}</td>
                <td style="font-size:.78rem;color:var(--text-muted)">${{f.created_at ? timeAgo(f.created_at) : '—'}}</td>
            </tr>`).join('') +
            `</tbody></table>` +
            `<div style="padding:.75rem 1.25rem;font-size:.78rem;color:var(--text-muted);border-top:1px solid var(--border)">
                Showing ${{files.length}} of ${{data.count||0}} files
            </div></div>`;
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
                        ${{e.timestamp ? new Date(e.timestamp).toLocaleString() : '—'}}
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
