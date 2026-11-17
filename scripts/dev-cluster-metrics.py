#!/usr/bin/env python3
"""Helper of `scripts/dev-cluster.sh metrics-check`; runs in a container on dtfs-dev-net.

Subcommands (PASS/FAIL lines on stdout, exit 1 when any FAIL):
  series            role/node series present in InfluxDB v1 and v2
  proxy             metrics_proxy_received_batches > 0 and rejected == 0 per manager
  gzip              manager points arrive in v1 and v2 (mgr01/02 gzip, mgr03 none)
  target <secs>     which manager the proxied batches go to (counter deltas), last line TARGET <mgr>
  fresh <epoch>     every data node and FUSE client has a point newer than <epoch>
  dash              import the Grafana dashboards, run every panel query, DATA/EMPTY/FAIL
"""
import csv, datetime, glob, io, json, os, re, sys, time, urllib.error, urllib.parse, urllib.request

I1 = "http://dtfs-dev-influx1:8086"
I2 = "http://dtfs-dev-influx2:8086"
GF = "http://dtfs-dev-grafana:3000"
DB, USER, PASS = "dtfs", "admin", "dtfs-dev-pass"
ORG, BUCKET, TOKEN = "dtfs", "dtfs", "dtfs-dev-token"
GF_USER, GF_PASS = "admin", "dtfs-dev-grafana"
MGRS = ["mgr01", "mgr02", "mgr03"]
EXPECT = {"manager": MGRS, "data": ["data01", "data02", "data03"], "fuse": ["fuse01_cln"]}
SENT = "dtfs_metrics_proxy_sent_bytes"
rc = 0


def ck(ok, msg):
    global rc
    print(("PASS: " if ok else "FAIL: ") + msg, flush=True)
    if not ok:
        rc = 1


def http(method, url, body=None, headers=None, timeout=90):
    req = urllib.request.Request(url, method=method, data=body, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def q1(q):
    """InfluxQL; returns [{tags, columns, values}]."""
    url = "%s/query?%s" % (I1, urllib.parse.urlencode({"db": DB, "u": USER, "p": PASS, "q": q, "epoch": "s"}))
    st, b = http("GET", url)
    j = json.loads(b)
    res = j["results"][0]
    if st != 200 or "error" in res:
        raise RuntimeError("v1 %s: %s" % (st, res.get("error") or b[:200]))
    return res.get("series", [])


def q2(flux):
    """Flux; returns list of row dicts (annotation lines dropped)."""
    st, b = http("POST", "%s/api/v2/query?org=%s" % (I2, ORG), flux.encode(),
                 {"Authorization": "Token " + TOKEN, "Content-Type": "application/vnd.flux", "Accept": "application/csv"})
    if st != 200:
        raise RuntimeError("v2 %s: %s" % (st, b[:200]))
    rows = []
    for block in re.split(r"\r?\n\r?\n", b.decode()):
        lines = [l for l in block.splitlines() if l and not l.startswith("#")]
        if len(lines) > 1:
            rows += list(csv.DictReader(io.StringIO("\n".join(lines))))
    return rows


def ts(s):
    s = re.sub(r"(\.\d{6})\d*Z$", r"\1Z", s)
    return datetime.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def flux_last(measurements, rng, keep):
    cond = " or ".join('r._measurement == "%s"' % m for m in measurements)
    return ('from(bucket: "%s") |> range(start: %s) |> filter(fn: (r) => (%s) and r._field == "value") '
            '|> last() |> group() |> keep(columns: %s)' % (BUCKET, rng, cond, json.dumps(keep)))


def last_by(ver, measurements, rng, tags):
    """{tuple(tag values): (epoch time, value)} of the last point per tag set."""
    out = {}
    if ver == 1:
        ms = ",".join('"%s"' % m for m in measurements)
        by = ",".join('"%s"' % t for t in tags)
        for s in q1('SELECT last("value") FROM %s WHERE time > now() - %s GROUP BY %s' % (ms, rng, by)):
            out[tuple(s["tags"].get(t, "") for t in tags)] = (s["values"][0][0], s["values"][0][1])
    else:
        for r in q2(flux_last(measurements, "-" + rng, tags + ["_time", "_value"])):
            out[tuple(r.get(t, "") for t in tags)] = (ts(r["_time"]), float(r["_value"]))
    return out


def cmd_series():
    for ver in (1, 2):
        got = {}
        for (role, node), _ in last_by(ver, ["dtfs_raft_current_term", SENT], "2m", ["role", "node"]).items():
            got.setdefault(role, set()).add(node)
        for role, nodes in EXPECT.items():
            have = got.get(role, set())
            ck(set(nodes) <= have, "v%d role=%s series from %s (got %s)" % (ver, role, nodes, sorted(have)))
    n1 = len(q1("SHOW MEASUREMENTS")[0]["values"])
    n2 = len(q2('import "influxdata/influxdb/schema"\nschema.measurements(bucket: "%s")' % BUCKET))
    print("info: measurements v1=%d v2=%d" % (n1, n2))
    ck(n1 > 20 and n2 > 20, "both versions hold many dtfs_* measurements")


def cmd_proxy():
    for ver in (1, 2):
        recv = last_by(ver, ["dtfs_metrics_proxy_received_batches"], "3m", ["node"])
        rej = last_by(ver, ["dtfs_metrics_proxy_rejected"], "30m", ["node", "reason"])
        # nodes stick to one manager (the first in their address list), so only that one counts batches
        for m in MGRS:
            n = recv.get((m,), (0, 0))[1]
            bad = {k[1]: v[1] for k, v in rej.items() if k[0] == m and v[1] > 0}
            ck(not bad, "v%d %s metrics_proxy_rejected=%s (received_batches=%d)" % (ver, m, bad or 0, n))
        ck(sum(v[1] for v in recv.values()) > 0, "v%d some manager has metrics_proxy_received_batches > 0 (%s)"
           % (ver, {k[0]: int(v[1]) for k, v in recv.items()}))


def cmd_gzip():
    now = time.time()
    for ver in (1, 2):
        pts = last_by(ver, ["dtfs_raft_current_term"], "2m", ["node"])
        for m, enc in (("mgr01", "gzip"), ("mgr02", "gzip"), ("mgr03", "none")):
            age = now - pts[(m,)][0] if (m,) in pts else None
            ck(age is not None and age < 60, "v%d: POST body %s from %s accepted (newest point age %s s)"
               % (ver, enc, m, None if age is None else int(age)))


def received():
    return {k[0]: v[1] for k, v in last_by(1, ["dtfs_metrics_proxy_received_batches"], "3m", ["node"]).items()}


def cmd_target(secs):
    a = received()
    time.sleep(secs)
    b = received()
    d = {m: b.get(m, 0) - a.get(m, 0) for m in MGRS}
    print("received_batches delta over %ds: %s" % (secs, d))
    print("TARGET " + max(d, key=d.get))
    ck(max(d.values()) > 0, "some manager received proxied batches during the window")


def cmd_fresh(since):
    for ver in (1, 2):
        pts = last_by(ver, [SENT], "10m", ["role", "node"])
        for role in ("data", "fuse"):
            for node in EXPECT[role]:
                t = pts.get((role, node), (0, 0))[0]
                ck(t >= since, "v%d %s %s newest point %+ds vs the stop mark" % (ver, role, node, int(t - since)))


# ---- dashboards -------------------------------------------------------------

def gf(method, path, obj=None):
    h = {"Content-Type": "application/json", "Authorization": "Basic " + __import__("base64").b64encode(
        ("%s:%s" % (GF_USER, GF_PASS)).encode()).decode()}
    st, b = http(method, GF + path, None if obj is None else json.dumps(obj).encode(), h)
    try:
        return st, json.loads(b or b"{}")
    except ValueError:
        return st, {"raw": b[:200].decode(errors="replace")}


def add_ds(name, body):
    gf("DELETE", "/api/datasources/name/" + name)
    st, r = gf("POST", "/api/datasources", dict(body, name=name, type="influxdb", access="proxy"))
    if st != 200:
        print("FAIL: create datasource %s: %s %s" % (name, st, r))
        sys.exit(1)
    return r["datasource"]["uid"]


def ds_query(uid, ver, q):
    t = {"refId": "A", "datasource": {"type": "influxdb", "uid": uid}, "query": q, "intervalMs": 15000, "maxDataPoints": 600}
    if ver == 1:
        t.update(rawQuery=True, resultFormat="time_series")
    st, b = http("POST", GF + "/api/ds/query", json.dumps({"queries": [t], "from": "now-30m", "to": "now"}).encode(),
                 {"Content-Type": "application/json", "Authorization": "Basic " + __import__("base64").b64encode(
                     ("%s:%s" % (GF_USER, GF_PASS)).encode()).decode()})
    try:
        j = json.loads(b)
    except ValueError:
        return "FAIL", "http %s %s" % (st, b[:150])
    r = (j.get("results") or {}).get("A")
    if r is None:
        return "FAIL", "http %s %s" % (st, j.get("message") or str(b[:150]))
    if r.get("error"):
        return "FAIL", str(r["error"])[:300]
    cols = []
    for f in r.get("frames") or []:
        vals = f["data"]["values"]
        # column 0 is time for series frames; any later column with a non-null value is data
        cols += [c for c in vals[1:] if any(x is not None for x in c)] or ([vals[0]] if len(vals) == 1 and vals[0] else [])
    return ("DATA" if cols else "EMPTY"), r.get("frames") or []


VAR = re.compile(r"\$\{?(dc|node|disk|bucket)(:json)?\}?")
MACRO = re.compile(r"\$\{?(?!timeFilter\b|__interval\b|__interval_ms\b)[A-Za-z_]")


def subst(q, ver, vars_):
    if ver == 1:
        q = VAR.sub(".*", q)  # every variable is "All" (allValue .*)
    else:
        q = VAR.sub(lambda m: json.dumps(vars_.get(m.group(1), [])) if m.group(2) else vars_[m.group(1)][0], q)
    return q


def panels(items):
    for p in items:
        if p.get("type") == "row":
            yield from panels(p.get("panels", []))
        else:
            yield p


EXPECTED_EMPTY = re.compile(r"corrupt|quarantin|fail|error|reject|drop|mismatch|fill|evict|retr|timeout|deadline|forward|"
                            r"leader change|stall|backpressure|shed|unavail|lost|bad|conflict|refus|invalid|scrub", re.I)


def cmd_dash():
    uids = {
        1: add_ds("dtfs-influx-v1", {"url": I1, "database": DB, "user": USER, "secureJsonData": {"password": PASS},
                                    "jsonData": {"httpMode": "GET"}}),
        2: add_ds("dtfs-influx-v2", {"url": I2, "secureJsonData": {"token": TOKEN},
                                    "jsonData": {"version": "Flux", "organization": ORG, "defaultBucket": BUCKET}}),
    }
    summary = []
    for ver in (1, 2):
        for path in sorted(glob.glob("/grafana/influxdb-v%d/*.json" % ver)):
            name = os.path.basename(path)[:-5]
            d = json.load(open(path))
            d["id"] = None
            for v in d["templating"]["list"]:
                if v["name"] == "DS":
                    v["current"] = {"text": "dtfs-influx-v%d" % ver, "value": uids[ver]}
            st, r = gf("POST", "/api/dashboards/db", {"dashboard": d, "overwrite": True})
            ck(st == 200, "v%d %s imported into Grafana (%s)" % (ver, name, r.get("status") or r))
            vars_ = {"bucket": [BUCKET]}
            for v in d["templating"]["list"]:
                if v["type"] != "query" or v["name"] == "bucket":
                    continue
                q = subst(v["query"], ver, vars_)
                res, detail = ds_query(uids[ver], ver, q)
                ck(res != "FAIL", "v%d %s variable %s query %s" % (ver, name, v["name"], res if res != "FAIL" else "FAIL " + detail))
                vals = []
                if res != "FAIL":
                    for f in detail:
                        vals += [x for x in f["data"]["values"][-1] if x is not None]
                vars_[v["name"]] = sorted(set(vals))
            print("info: v%d %s variables %s" % (ver, name, {k: v for k, v in vars_.items() if k != "bucket"}))
            count = {"DATA": 0, "EMPTY": 0, "FAIL": 0}
            for p in panels(d["panels"]):
                res_all = []
                for t in p.get("targets", []):
                    if t.get("hide") or not t.get("query"):
                        continue
                    q = subst(t["query"], ver, vars_)
                    if MACRO.search(q):
                        res_all.append(("FAIL", "unresolved variable in query"))
                        continue
                    r_, det = ds_query(uids[ver], ver, q)
                    res_all.append((r_, det if r_ == "FAIL" else ""))
                states = [s for s, _ in res_all]
                st_ = "FAIL" if "FAIL" in states else ("DATA" if "DATA" in states else "EMPTY")
                if not states:
                    st_ = "EMPTY"
                extra = "; ".join(x for s, x in res_all if s == "FAIL")
                if st_ == "EMPTY":
                    extra = "expected?" if EXPECTED_EMPTY.search(p.get("title", "")) else "UNEXPECTED?"
                count[st_] += 1
                print("PANEL v%d %s | %s | %s %s | targets %d" % (ver, name, p.get("title"), st_, extra, len(states)), flush=True)
            summary.append((ver, name, count))
            ck(count["FAIL"] == 0, "v%d %s panels: %d DATA, %d EMPTY, %d FAIL" % (ver, name, count["DATA"], count["EMPTY"], count["FAIL"]))
    print("SUMMARY (version dashboard DATA/EMPTY/FAIL)")
    for ver, name, c in summary:
        print("  v%d %-14s %d/%d/%d" % (ver, name, c["DATA"], c["EMPTY"], c["FAIL"]))


if __name__ == "__main__":
    cmd = sys.argv[1]
    {"series": cmd_series, "proxy": cmd_proxy, "gzip": cmd_gzip, "dash": cmd_dash,
     "target": lambda: cmd_target(int(sys.argv[2])), "fresh": lambda: cmd_fresh(float(sys.argv[2]))}[cmd]()
    sys.exit(rc)
