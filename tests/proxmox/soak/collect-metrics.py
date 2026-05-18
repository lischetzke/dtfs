#!/usr/bin/env python3
# Soak metrics collector.
#
# Scrapes the manager /metrics endpoint and a couple of REST APIs on a
# fixed interval, writing one JSON event per scrape to a per-host JSONL
# file under <output>/metrics-<host>.jsonl. The render-report script
# consumes these files.
#
# Designed to be resilient: a single failed scrape logs to stderr but
# does not stop the loop.

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone


def parse_prometheus(text: str) -> dict:
    """Parse Prometheus text-format into a flat {metric: value} dict."""
    out = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # `metric value` or `metric{labels} value`
        parts = line.rsplit(None, 1)
        if len(parts) != 2:
            continue
        key, val = parts
        try:
            out[key] = float(val)
        except ValueError:
            pass
    return out


def fetch_json(url: str, timeout: float = 5.0):
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def fetch_text(url: str, timeout: float = 5.0) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def scrape_one(mgr: str):
    out = {"mgr": mgr, "ts": datetime.now(timezone.utc).isoformat()}

    # /metrics Prometheus scrape
    try:
        text = fetch_text(f"{mgr}/metrics")
        out["metrics"] = parse_prometheus(text)
    except Exception as e:
        out["metrics_err"] = str(e)

    # Cluster status (compact, used for liveness)
    try:
        out["status"] = fetch_json(f"{mgr}/api/v1/cluster/status")
    except Exception as e:
        out["status_err"] = str(e)

    # Nodes
    try:
        out["nodes"] = fetch_json(f"{mgr}/api/v1/nodes")
    except Exception as e:
        out["nodes_err"] = str(e)

    # /readyz (boolean health probe — 200 vs 503)
    try:
        with urllib.request.urlopen(f"{mgr}/readyz", timeout=2) as r:
            out["readyz"] = r.status
    except urllib.error.HTTPError as e:
        out["readyz"] = e.code
    except Exception as e:
        out["readyz_err"] = str(e)

    return out


def host_label(url: str) -> str:
    # http://10.150.0.151:9402 -> 10.150.0.151_9402
    no_scheme = url.split("//", 1)[-1]
    return no_scheme.replace(":", "_").replace("/", "_")


def main():
    ap = argparse.ArgumentParser(description="DTFS soak metrics scraper")
    ap.add_argument("--mgrs", nargs="+", required=True, help="manager base URLs")
    ap.add_argument("--output", required=True, help="output dir for jsonl")
    ap.add_argument("--interval", type=float, default=30.0, help="seconds")
    args = ap.parse_args()

    os.makedirs(args.output, exist_ok=True)
    file_handles = {
        m: open(os.path.join(args.output, f"metrics-{host_label(m)}.jsonl"), "a")
        for m in args.mgrs
    }

    try:
        while True:
            start = time.time()
            for mgr in args.mgrs:
                event = scrape_one(mgr)
                file_handles[mgr].write(json.dumps(event) + "\n")
                file_handles[mgr].flush()
            elapsed = time.time() - start
            time.sleep(max(0, args.interval - elapsed))
    except KeyboardInterrupt:
        pass
    finally:
        for fh in file_handles.values():
            fh.close()


if __name__ == "__main__":
    sys.exit(main())
