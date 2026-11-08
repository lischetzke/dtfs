#!/usr/bin/env bash
# Syntax-check every inline <script> body of the dashboard asset with node.
# A parse error blanks the whole UI, and no Rust test notices without this.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
asset="$here/../../crates/dtfs-manager/src/webui_assets/dashboard.html"
command -v node >/dev/null || { echo "check-webui-js: node not found" >&2; exit 1; }
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
python3 - "$asset" "$tmp" <<'PY'
import re, sys
html = open(sys.argv[1], encoding="utf-8").read()
for i, m in enumerate(re.finditer(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", html, re.S | re.I)):
    open(f"{sys.argv[2]}/s{i}.js", "w", encoding="utf-8").write(m.group(1))
PY
n=0
for f in "$tmp"/s*.js; do node --check "$f"; n=$((n+1)); done
echo "check-webui-js: $n script(s) OK"
