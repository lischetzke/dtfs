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

<div class="container">

    <!-- Count cards -->
    <div class="counts-row">
        <div class="count-card">
            <div class="count-icon managers">⬡</div>
            <div class="count-body">
                <h4 id="cnt-managers">—</h4>
                <p>Manager nodes</p>
            </div>
        </div>
        <div class="count-card">
            <div class="count-icon datanodes">◈</div>
            <div class="count-body">
                <h4 id="cnt-datanodes">—</h4>
                <p>Data nodes</p>
            </div>
        </div>
        <div class="count-card">
            <div class="count-icon disks">▨</div>
            <div class="count-body">
                <h4 id="cnt-disks">—</h4>
                <p>Disks registered</p>
            </div>
        </div>
        <div class="count-card">
            <div class="count-icon clients">⌗</div>
            <div class="count-body">
                <h4 id="cnt-clients">—</h4>
                <p>FUSE clients</p>
            </div>
        </div>
    </div>

    <!-- Capacity + I/O metrics -->
    <div class="metrics-row">
        <div class="metric-card">
            <h3>Raw Capacity</h3>
            <div class="metric-row">
                <span class="metric-label">Total</span>
                <span class="metric-value" id="raw-total">—</span>
            </div>
            <div class="metric-row">
                <span class="metric-label">Used</span>
                <span class="metric-value" id="raw-used">—</span>
            </div>
            <div class="progress-wrap">
                <div style="display:flex;justify-content:space-between;margin-bottom:.3rem">
                    <span style="font-size:.75rem;color:var(--text-muted)">Utilisation</span>
                    <span style="font-size:.75rem;color:var(--text-muted)" id="raw-pct">—</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill primary" id="raw-bar" style="width:0%"></div>
                </div>
            </div>
            <div class="metric-row" style="margin-top:.25rem">
                <span class="metric-label">Free</span>
                <span class="metric-value" id="raw-free" style="color:var(--success)">—</span>
            </div>
        </div>

        <div class="metric-card">
            <h3>Net Usable Capacity</h3>
            <div style="font-size:.72rem;color:var(--text-muted);margin-bottom:.75rem">
                After replication factor (÷ hot RF)
            </div>
            <div class="metric-row">
                <span class="metric-label">Usable</span>
                <span class="metric-value" id="net-usable">—</span>
            </div>
            <div class="metric-row">
                <span class="metric-label">Used</span>
                <span class="metric-value" id="net-used">—</span>
            </div>
            <div class="progress-wrap">
                <div style="display:flex;justify-content:space-between;margin-bottom:.3rem">
                    <span style="font-size:.75rem;color:var(--text-muted)">Utilisation</span>
                    <span style="font-size:.75rem;color:var(--text-muted)" id="net-pct">—</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill success" id="net-bar" style="width:0%"></div>
                </div>
            </div>
            <div class="metric-row" style="margin-top:.25rem">
                <span class="metric-label">Free</span>
                <span class="metric-value" id="net-free" style="color:var(--success)">—</span>
            </div>
        </div>

        <div class="metric-card">
            <h3>I/O Throughput</h3>
            <div class="io-row">
                <div class="io-item">
                    <div class="io-dir">↓ Read</div>
                    <div>
                        <span class="io-val" id="io-read">0</span>
                        <span class="io-unit" id="io-read-unit">B/s</span>
                    </div>
                </div>
                <div class="io-item">
                    <div class="io-dir">↑ Write</div>
                    <div>
                        <span class="io-val" id="io-write">0</span>
                        <span class="io-unit" id="io-write-unit">B/s</span>
                    </div>
                </div>
            </div>
            <div style="margin-top:1.25rem;font-size:.72rem;color:var(--text-muted)">
                Files tracked
            </div>
            <div style="font-size:1.4rem;font-weight:700;margin-top:.15rem" id="file-count">—</div>
            <div style="font-size:.72rem;color:var(--text-muted);margin-top:.1rem">
                across all volumes
            </div>
        </div>
    </div>

    <!-- Last scrub -->
    <div class="scrub-card">
        <h3>Last Scrub</h3>
        <div id="scrub-content">
            <div class="scrub-placeholder">No scrub result recorded yet. Run
                <code>dtfs-cli scrub --level deep</code> to trigger one.</div>
        </div>
    </div>

    <!-- Nodes table -->
    <div class="table-card">
        <div class="table-card-header">
            <h3>Data &amp; Manager Nodes</h3>
            <span style="font-size:.75rem;color:var(--text-muted)" id="nodes-summary"></span>
        </div>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Hostname</th>
                    <th>Role</th>
                    <th>Location</th>
                    <th>Disks</th>
                    <th>Capacity</th>
                    <th>Used</th>
                    <th>Health</th>
                </tr>
            </thead>
            <tbody id="nodes-table">
                <tr><td colspan="8" class="placeholder">Fetching node list…</td></tr>
            </tbody>
        </table>
    </div>

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

        // ── Last scrub
        renderScrub(stats.last_scrub);

        // ── Nodes table
        renderNodes(nodes);
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

    // ── Bootstrap ────────────────────────────────────────────────────────────
    fetchStats();
    setInterval(fetchStats, 8000);
</script>
</body>
</html>"##,
        cluster_name = cluster_name,
        hostname = hostname,
    )
}
