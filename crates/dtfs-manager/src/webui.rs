//! Embedded SPA serving for the Manager WebUI.
//!
//! In development, this module serves a placeholder HTML page. In production
//! (when the `webui` feature is enabled and `webui/dist/` has been built),
//! it serves the static React SPA assets embedded in the binary via
//! `rust-embed`.
//!
//! The WebUI is mounted at `/` on the API server and falls back to
//! `index.html` for client-side routing.

use axum::{
    extract::State,
    http::{header, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};
use std::sync::Arc;

/// Shared state for WebUI routes (can reference cluster state as needed).
#[derive(Clone)]
pub struct WebUiState {
    pub cluster_name: String,
    pub manager_hostname: String,
}

/// Build the Axum router for the WebUI.
///
/// This is mounted at `/` on the manager API server. API routes (`/api/...`)
/// take precedence when merged.
pub fn router(state: Arc<WebUiState>) -> Router {
    Router::new()
        .route("/", get(index_page))
        .route("/ui", get(index_page))
        .route("/ui/{*path}", get(index_page))
        .route("/assets/styles.css", get(stylesheet))
        .route("/favicon.ico", get(favicon))
        .with_state(state)
}

/// Serve the main SPA page.
///
/// In a production build, this would serve the embedded React build.
/// For now, it serves a functional placeholder dashboard.
async fn index_page(State(state): State<Arc<WebUiState>>) -> Html<String> {
    Html(render_dashboard(&state.cluster_name, &state.manager_hostname))
}

/// Serve a basic CSS stylesheet.
async fn stylesheet() -> Response {
    let css = include_str!("webui_assets/styles.css");
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/css")],
        css.to_string(),
    )
        .into_response()
}

/// Serve a favicon (SVG).
async fn favicon() -> Response {
    let svg = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
        <rect width="64" height="64" rx="12" fill="#2563eb"/>
        <text x="32" y="44" font-size="32" fill="white" text-anchor="middle" font-family="monospace">D</text>
    </svg>"##;
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "image/svg+xml")],
        svg.to_string(),
    )
        .into_response()
}

/// Render the placeholder dashboard HTML.
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
            --bg: #0f172a;
            --surface: #1e293b;
            --border: #334155;
            --text: #e2e8f0;
            --text-muted: #94a3b8;
            --primary: #3b82f6;
            --success: #22c55e;
            --warning: #f59e0b;
            --danger: #ef4444;
        }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--bg);
            color: var(--text);
            min-height: 100vh;
        }}
        .header {{
            background: var(--surface);
            border-bottom: 1px solid var(--border);
            padding: 1rem 2rem;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }}
        .header h1 {{
            font-size: 1.25rem;
            font-weight: 600;
        }}
        .header h1 span {{
            color: var(--primary);
        }}
        .header .meta {{
            color: var(--text-muted);
            font-size: 0.875rem;
        }}
        .nav {{
            background: var(--surface);
            border-bottom: 1px solid var(--border);
            padding: 0 2rem;
            display: flex;
            gap: 0;
        }}
        .nav a {{
            color: var(--text-muted);
            text-decoration: none;
            padding: 0.75rem 1.25rem;
            font-size: 0.875rem;
            border-bottom: 2px solid transparent;
            transition: all 0.2s;
        }}
        .nav a:hover, .nav a.active {{
            color: var(--text);
            border-bottom-color: var(--primary);
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 2rem;
        }}
        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }}
        .card {{
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 0.75rem;
            padding: 1.5rem;
        }}
        .card h3 {{
            font-size: 0.875rem;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 0.5rem;
        }}
        .card .value {{
            font-size: 2rem;
            font-weight: 700;
        }}
        .card .sub {{
            font-size: 0.875rem;
            color: var(--text-muted);
            margin-top: 0.25rem;
        }}
        .status-dot {{
            display: inline-block;
            width: 8px;
            height: 8px;
            border-radius: 50%;
            margin-right: 0.5rem;
        }}
        .status-dot.healthy {{ background: var(--success); }}
        .status-dot.warning {{ background: var(--warning); }}
        .status-dot.danger {{ background: var(--danger); }}
        .table-card {{
            grid-column: 1 / -1;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        th, td {{
            text-align: left;
            padding: 0.75rem 1rem;
            border-bottom: 1px solid var(--border);
        }}
        th {{
            font-size: 0.75rem;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}
        td {{
            font-size: 0.875rem;
        }}
        .badge {{
            display: inline-block;
            padding: 0.125rem 0.5rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 600;
        }}
        .badge-hot {{ background: #ef444433; color: var(--danger); }}
        .badge-warm {{ background: #f59e0b33; color: var(--warning); }}
        .badge-cold {{ background: #3b82f633; color: var(--primary); }}
        .badge-healthy {{ background: #22c55e33; color: var(--success); }}
        .placeholder {{
            text-align: center;
            color: var(--text-muted);
            padding: 3rem;
            font-style: italic;
        }}
        #status-indicator {{
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1><span>DTFS</span> — {cluster_name}</h1>
        <div class="meta">
            <div id="status-indicator">
                <span class="status-dot healthy"></span>
                Manager: {hostname}
            </div>
        </div>
    </div>
    <div class="nav">
        <a href="/ui" class="active">Dashboard</a>
        <a href="/ui/topology">Topology</a>
        <a href="/ui/nodes">Nodes</a>
        <a href="/ui/volumes">Volumes</a>
        <a href="/ui/explorer">Data Explorer</a>
        <a href="/ui/audit">Audit Log</a>
        <a href="/ui/settings">Settings</a>
    </div>
    <div class="container">
        <div class="grid">
            <div class="card">
                <h3>Cluster Status</h3>
                <div class="value" id="cluster-status">Loading...</div>
                <div class="sub" id="cluster-sub"></div>
            </div>
            <div class="card">
                <h3>Data Nodes</h3>
                <div class="value" id="node-count">—</div>
                <div class="sub" id="node-sub"></div>
            </div>
            <div class="card">
                <h3>Total Capacity</h3>
                <div class="value" id="total-capacity">—</div>
                <div class="sub" id="capacity-sub"></div>
            </div>
            <div class="card">
                <h3>Files</h3>
                <div class="value" id="file-count">—</div>
                <div class="sub" id="file-sub"></div>
            </div>
        </div>

        <div class="grid">
            <div class="card table-card">
                <h3>Recent Nodes</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Node ID</th>
                            <th>Hostname</th>
                            <th>Location</th>
                            <th>Disks</th>
                            <th>Capacity</th>
                            <th>Health</th>
                        </tr>
                    </thead>
                    <tbody id="nodes-table">
                        <tr><td colspan="6" class="placeholder">Loading node data...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>

        <div class="grid">
            <div class="card table-card">
                <h3>Recent Audit Events</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Time</th>
                            <th>Actor</th>
                            <th>Action</th>
                            <th>Target</th>
                            <th>Result</th>
                        </tr>
                    </thead>
                    <tbody id="audit-table">
                        <tr><td colspan="5" class="placeholder">Loading audit log...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <script>
        async function fetchData() {{
            try {{
                const status = await fetch('/api/v1/cluster/status').then(r => r.json());
                document.getElementById('cluster-status').innerHTML =
                    '<span class="status-dot healthy"></span>' + (status.status || 'unknown');
                document.getElementById('cluster-sub').textContent =
                    'Cluster: ' + (status.cluster_name || '—');

                const nodes = await fetch('/api/v1/nodes').then(r => r.json()).catch(() => []);
                document.getElementById('node-count').textContent = nodes.length || '0';
                document.getElementById('node-sub').textContent =
                    nodes.filter(n => n.health === 'healthy' || n.health === 'Healthy').length + ' healthy';

                if (nodes.length > 0) {{
                    let totalCap = 0, usedCap = 0;
                    nodes.forEach(n => {{ totalCap += n.total_capacity || 0; usedCap += n.used_capacity || 0; }});
                    document.getElementById('total-capacity').textContent = formatBytes(totalCap);
                    document.getElementById('capacity-sub').textContent = formatBytes(usedCap) + ' used';

                    const tbody = document.getElementById('nodes-table');
                    tbody.innerHTML = nodes.map(n => `
                        <tr>
                            <td>${{n.node_id || '—'}}</td>
                            <td>${{n.hostname || '—'}}</td>
                            <td>${{n.location || '—'}}</td>
                            <td>${{n.disk_count || 0}}</td>
                            <td>${{formatBytes(n.total_capacity || 0)}}</td>
                            <td><span class="badge badge-healthy">${{n.health || '—'}}</span></td>
                        </tr>
                    `).join('');
                }}

                const files = await fetch('/api/v1/files').then(r => r.json()).catch(() => ({{count: 0}}));
                document.getElementById('file-count').textContent = files.count || '0';

                const audit = await fetch('/api/v1/audit?limit=10').then(r => r.json()).catch(() => []);
                if (audit.length > 0) {{
                    const auditTbody = document.getElementById('audit-table');
                    auditTbody.innerHTML = audit.map(e => `
                        <tr>
                            <td>${{new Date(e.timestamp * 1000).toLocaleString()}}</td>
                            <td>${{e.actor || '—'}}</td>
                            <td>${{e.action || '—'}}</td>
                            <td>${{e.target || '—'}}</td>
                            <td>${{e.result || '—'}}</td>
                        </tr>
                    `).join('');
                }}
            }} catch (err) {{
                console.error('Dashboard fetch error:', err);
            }}
        }}

        function formatBytes(bytes) {{
            if (bytes === 0) return '0 B';
            const k = 1024;
            const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
        }}

        fetchData();
        setInterval(fetchData, 10000);
    </script>
</body>
</html>"##
    )
}
