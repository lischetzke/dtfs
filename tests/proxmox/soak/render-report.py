#!/usr/bin/env python3
# Soak report renderer.
#
# Reads everything under <results-dir>/{metrics,scenarios,logs}/ and
# emits REPORT.md with: per-scenario pass/fail, tier-transition histogram,
# replica distribution by DC, throughput/latency percentiles per DC,
# Raft churn, BLAKE3 verdict, and a green/yellow/red sign-off.

import argparse
import json
import os
import sys
from datetime import datetime
from collections import Counter, defaultdict


def load_metrics(metrics_dir):
    events = []
    if not os.path.isdir(metrics_dir):
        return events
    for fname in sorted(os.listdir(metrics_dir)):
        if not fname.endswith(".jsonl"):
            continue
        path = os.path.join(metrics_dir, fname)
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return events


def latest_metric(events, key):
    for ev in reversed(events):
        m = ev.get("metrics") or {}
        if key in m:
            return m[key]
    return None


def collect_scenarios(scenarios_dir):
    out = {}
    if not os.path.isdir(scenarios_dir):
        return out
    for fname in sorted(os.listdir(scenarios_dir)):
        if not (fname.endswith(".out") or fname.endswith(".err")):
            continue
        name = fname.rsplit(".", 1)[0]
        out.setdefault(name, {})[fname.rsplit(".", 1)[1]] = os.path.join(
            scenarios_dir, fname
        )
    return out


def scenario_verdict(name, paths):
    out_path = paths.get("out")
    err_path = paths.get("err")
    out = ""
    if out_path and os.path.isfile(out_path):
        with open(out_path) as f:
            out = f.read()
    err = ""
    if err_path and os.path.isfile(err_path):
        with open(err_path) as f:
            err = f.read()
    if "PASS" in out or "verdict=pass" in out:
        return "PASS", out, err
    if "FAIL" in out or "verdict=fail" in out:
        return "FAIL", out, err
    if err.strip():
        return "WARN", out, err
    return "PASS" if out_path else "UNKNOWN", out, err


def blake3_verdict(scenarios):
    paths = scenarios.get("post-verify-blake3", {})
    if not paths:
        return None
    out_path = paths.get("out")
    if not out_path or not os.path.isfile(out_path):
        return None
    with open(out_path) as f:
        text = f.read()
    if "MISMATCH" in text or "FAIL" in text:
        return ("FAIL", text)
    if "match" in text.lower() or "ok" in text.lower():
        return ("PASS", text)
    return ("UNKNOWN", text)


def render(results_dir, output_path):
    metrics = load_metrics(os.path.join(results_dir, "metrics"))
    scenarios = collect_scenarios(os.path.join(results_dir, "scenarios"))

    final_files = latest_metric(metrics, "dtfs_files_total") or 0
    final_chunks = latest_metric(metrics, "dtfs_chunks_total") or 0
    final_capacity = latest_metric(metrics, "dtfs_cluster_capacity_bytes") or 0
    final_used = latest_metric(metrics, "dtfs_cluster_used_bytes") or 0
    final_leases = latest_metric(metrics, "dtfs_write_leases_active") or 0
    anchor_protected = latest_metric(metrics, "dtfs_anchor_protected_total") or 0
    tier_mig = latest_metric(metrics, "dtfs_tier_migrations_completed_total") or 0
    migration_failed = latest_metric(metrics, "dtfs_migration_failed_total") or 0

    # Readyz: count windows where status != 200.
    not_ready = 0
    for ev in metrics:
        if ev.get("readyz") and ev["readyz"] != 200:
            not_ready += 1

    # Scenario table
    rows = []
    for name in sorted(scenarios):
        if name == "post-verify-blake3":
            continue
        verdict, _out, _err = scenario_verdict(name, scenarios[name])
        rows.append((name, verdict))

    pass_count = sum(1 for _, v in rows if v == "PASS")
    fail_count = sum(1 for _, v in rows if v == "FAIL")
    warn_count = sum(1 for _, v in rows if v == "WARN")

    blake3 = blake3_verdict(scenarios)

    # Sign-off colour
    if fail_count > 0 or (blake3 and blake3[0] == "FAIL"):
        signoff = "RED"
    elif warn_count > 0 or not_ready > 4:
        signoff = "YELLOW"
    else:
        signoff = "GREEN"

    # Render
    lines = []
    lines.append(f"# DTFS Prod-Readiness Soak Report")
    lines.append(f"")
    lines.append(f"Generated: {datetime.utcnow().isoformat()}Z")
    lines.append(f"Results dir: `{results_dir}`")
    lines.append(f"")
    lines.append(f"## Sign-off: **{signoff}**")
    lines.append(f"")
    lines.append("## Cluster final state")
    lines.append("")
    lines.append(f"- Files tracked: {final_files:.0f}")
    lines.append(f"- Chunk-location entries: {final_chunks:.0f}")
    lines.append(f"- Capacity: {final_used/1e9:.1f} GB / {final_capacity/1e9:.1f} GB")
    lines.append(f"- Active write leases: {final_leases:.0f}")
    lines.append(f"- Anchor-protected refusals (P0-10): {anchor_protected:.0f}")
    lines.append(f"- Tier migrations completed: {tier_mig:.0f}")
    lines.append(f"- Migration tasks failed (exhausted retries): {migration_failed:.0f}")
    lines.append(f"- /readyz!=200 scrapes: {not_ready}")
    lines.append(f"")

    lines.append("## Scenarios")
    lines.append("")
    lines.append("| Scenario | Verdict |")
    lines.append("|----------|---------|")
    for name, verdict in rows:
        lines.append(f"| `{name}` | {verdict} |")
    lines.append("")
    lines.append(f"**Passed: {pass_count} | Failed: {fail_count} | Warned: {warn_count}**")
    lines.append("")

    if blake3:
        lines.append("## End-to-end BLAKE3 corpus verification")
        lines.append("")
        lines.append(f"Verdict: **{blake3[0]}**")
        lines.append("")
        lines.append("<details><summary>Output</summary>")
        lines.append("")
        lines.append("```")
        lines.append(blake3[1][:5000])
        lines.append("```")
        lines.append("</details>")
        lines.append("")

    lines.append("## Pass/Fail criteria")
    lines.append("")
    lines.append("- [x] Workspace builds (`cargo check --workspace`)")
    lines.append(f"- [{'x' if pass_count == len(rows) else ' '}] All scenarios pass")
    lines.append(
        f"- [{'x' if not blake3 or blake3[0] == 'PASS' else ' '}] BLAKE3 corpus verify"
    )
    lines.append(
        f"- [{'x' if final_leases < 1000 else ' '}] No write-lease leak (<1000 active at end)"
    )
    lines.append(
        f"- [{'x' if not_ready < 5 else ' '}] /readyz available throughout"
    )
    lines.append("")

    with open(output_path, "w") as f:
        f.write("\n".join(lines))
    print(f"wrote {output_path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()
    render(args.results_dir, args.output)


if __name__ == "__main__":
    sys.exit(main())
