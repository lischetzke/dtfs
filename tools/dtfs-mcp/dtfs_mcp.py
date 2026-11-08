# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "mcp>=1.27,<2",
#   "httpx>=0.28",
# ]
# ///
"""DTFS diagnostics MCP server (stdio).

Read tools talk to the manager REST API (:9402) and, over the system `ssh`
binary, run a fixed set of allowlisted read-only command templates on cluster
hosts. Three gated ops (scrub, maintenance, unquarantine) mutate the cluster
and only act when called with confirm=true.

Config: %USERPROFILE%\\.dtfs\\clusters.toml (or env DTFS_MCP_CONFIG). See
clusters.example.toml. Documentation: docs/src/operations/diagnostics-mcp.md.

Nothing here writes to stdout except the MCP protocol itself.
"""
from __future__ import annotations

import asyncio
import functools
import inspect
import json
import os
import re
import shlex
import subprocess
import time
import tomllib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import httpx

OUTPUT_CAP = 60_000
REST_TIMEOUT = 10.0
SSH_TIMEOUT = 30
SSH_LOG_TIMEOUT = 60
SECRET_HEADER = "X-Dtfs-Shared-Secret"
REST_PREFIXES = ("/api/v1/",)
REST_EXACT = ("/metrics", "/readyz", "/healthz")
DEFAULT_MOUNT = "/mnt/dtfs"


class SetupError(Exception):
    """Config missing/invalid: surfaced to the caller as a setup message."""


class ValidationError(ValueError):
    """An argument failed strict validation."""


# --------------------------------------------------------------------------
# Output helpers
# --------------------------------------------------------------------------

def scrub(text: Any, secrets: list[str] | tuple[str, ...] = ()) -> str:
    s = str(text)
    for sec in secrets:
        if sec:
            s = s.replace(sec, "***")
    return s


def cap_output(text: str, limit: int = OUTPUT_CAP) -> str:
    if len(text) <= limit:
        return text
    dropped = len(text) - limit
    return text[:limit] + f"\n...[truncated {dropped} bytes; narrow the query]"


def render(obj: Any, secrets: list[str] | tuple[str, ...] = ()) -> str:
    return cap_output(scrub(json.dumps(obj, default=str, indent=1), secrets))


# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

def config_path() -> Path:
    env = os.environ.get("DTFS_MCP_CONFIG")
    if env:
        return Path(env).expanduser()
    return Path.home() / ".dtfs" / "clusters.toml"


def load_config(path: Path | None = None) -> dict:
    p = path or config_path()
    if not p.is_file():
        raise SetupError(
            f"DTFS MCP is not configured: no config file at {p}. Copy "
            "tools/dtfs-mcp/clusters.example.toml to that path (or set "
            "DTFS_MCP_CONFIG) and fill in managers, shared_secret(_env), "
            "ssh_user/ssh_key and hosts."
        )
    try:
        with open(p, "rb") as f:
            return tomllib.load(f)
    except tomllib.TOMLDecodeError as e:
        raise SetupError(f"config file {p} is not valid TOML: {e}") from e


class Cluster:
    def __init__(self, name: str, raw: dict):
        self.name = name
        self.raw = raw
        self.managers: list[str] = list(raw.get("managers") or [])
        if not self.managers:
            raise SetupError(f"cluster '{name}' has no managers configured")
        env_name = raw.get("shared_secret_env") or ""
        self.secret: str = raw.get("shared_secret") or (
            os.environ.get(env_name, "") if env_name else ""
        )
        self.ssh_user: str = raw.get("ssh_user", "root")
        self.ssh_key: str = raw.get("ssh_key", "")
        self.ssh_port: int = int(raw.get("ssh_port", 22))
        self.hosts: list[dict] = list(raw.get("hosts") or [])

    def secrets(self) -> list[str]:
        return [self.secret] if self.secret else []

    def host(self, name: str) -> dict:
        validate_hostname(name)
        for h in self.hosts:
            if str(h.get("name", "")).lower() == name.lower():
                return h
        known = ", ".join(str(h.get("name")) for h in self.hosts) or "(none)"
        raise ValidationError(
            f"host '{name}' is not in the cluster config; configured hosts: {known}"
        )


def get_cluster(cluster: str | None = None, cfg: dict | None = None) -> Cluster:
    cfg = cfg if cfg is not None else load_config()
    clusters = cfg.get("clusters") or {}
    if not clusters:
        raise SetupError("config has no [clusters.<name>] tables")
    name = cluster or cfg.get("default_cluster") or (
        next(iter(clusters)) if len(clusters) == 1 else None
    )
    if not name or name not in clusters:
        raise SetupError(
            f"unknown or unspecified cluster {name!r}; available: {', '.join(clusters)}"
        )
    return Cluster(name, clusters[name])


# --------------------------------------------------------------------------
# Validators (every value interpolated into a remote command goes through one)
# --------------------------------------------------------------------------

_HOSTNAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,62}")
_REL_TIME = re.compile(r"\d{1,6}[smhd] ago")
_ABS_TIME = re.compile(r"\d{4}-\d\d-\d\d( \d\d:\d\d(:\d\d)?)?")
_CHUNK_ID = re.compile(r"[0-9a-f]{8,128}")
_DEVICE = re.compile(r"/dev/[A-Za-z0-9._-]{1,40}")
_TARGET = re.compile(r"([A-Za-z0-9][A-Za-z0-9.-]{0,252}):(\d{1,5})")
_DISK_PATH = re.compile(r"/[A-Za-z0-9._@+/-]{1,200}")
_NODE_ID = re.compile(r"\d{1,20}")
_OPERATOR = re.compile(r"[A-Za-z0-9._@ -]{1,64}")
ROLES = ("manager", "data", "fuse")
UNITS = ("manager", "data", "fuse", "all", "kernel")


def _need(rx: re.Pattern, value: Any, what: str) -> str:
    if not isinstance(value, str) or not rx.fullmatch(value):
        raise ValidationError(f"invalid {what}: {value!r}")
    return value


def validate_hostname(v: str) -> str:
    return _need(_HOSTNAME, v, "host name")


def validate_time(v: str) -> str:
    if isinstance(v, str) and (_REL_TIME.fullmatch(v) or _ABS_TIME.fullmatch(v)):
        return v
    raise ValidationError(
        f"invalid time {v!r}: use '<N>[smhd] ago' or 'YYYY-MM-DD[ HH:MM[:SS]]'"
    )


def validate_chunk_id(v: str) -> str:
    return _need(_CHUNK_ID, v.lower() if isinstance(v, str) else v, "chunk id")


def validate_device(v: str) -> str:
    _need(_DEVICE, v, "device")
    return v


def validate_target(v: str) -> str:
    m = _need(_TARGET, v, "host:port target")
    if int(_TARGET.fullmatch(m).group(2)) > 65535:  # type: ignore[union-attr]
        raise ValidationError(f"invalid port in target: {v!r}")
    return m


def validate_disk_path(v: str) -> str:
    _need(_DISK_PATH, v, "disk path")
    if ".." in v.split("/"):
        raise ValidationError(f"invalid disk path: {v!r}")
    return v


def validate_node_id(v: Any) -> str:
    return _need(_NODE_ID, str(v), "node id")


def validate_role(v: str) -> str:
    if v not in ROLES:
        raise ValidationError(f"invalid role {v!r}: one of {', '.join(ROLES)}")
    return v


def validate_unit(v: str) -> str:
    if v not in UNITS:
        raise ValidationError(f"invalid unit {v!r}: one of {', '.join(UNITS)}")
    return v


def validate_lines(v: Any, maximum: int = 5000) -> int:
    if isinstance(v, bool) or not isinstance(v, int) or not 1 <= v <= maximum:
        raise ValidationError(f"lines must be an integer in 1..{maximum}")
    return v


def validate_rest_path(path: str) -> str:
    if (
        not isinstance(path, str)
        or not re.fullmatch(r"/[A-Za-z0-9_./:-]{0,300}", path)
        or ".." in path
        or "//" in path
    ):
        raise ValidationError(f"rest path rejected: {path!r}")
    if path in REST_EXACT or any(path.startswith(p) for p in REST_PREFIXES):
        return path
    raise ValidationError(
        f"rest path {path!r} not allowed; must start with /api/v1/ or be one of "
        f"{', '.join(REST_EXACT)}"
    )


# --------------------------------------------------------------------------
# REST
# --------------------------------------------------------------------------

def _base(addr: str) -> str:
    return addr if "://" in addr else f"http://{addr}"


def rest_call(
    cl: Cluster,
    addr: str,
    path: str,
    params: dict | None = None,
    method: str = "GET",
    body: dict | None = None,
    auth: bool = False,
) -> dict:
    """One REST call. Never raises: returns {"status", "data"} or {"__error__"}."""
    headers = {SECRET_HEADER: cl.secret} if (auth and cl.secret) else {}
    try:
        with httpx.Client(timeout=REST_TIMEOUT) as c:
            r = c.request(
                method, _base(addr) + path, params=params, json=body, headers=headers
            )
    except Exception as e:  # noqa: BLE001 - unreachable must become a finding
        return {"__error__": scrub(f"{addr}: {type(e).__name__}: {e}", cl.secrets()),
                "addr": addr}
    try:
        data: Any = r.json()
    except ValueError:
        data = scrub(r.text[:4000], cl.secrets())
    if r.status_code >= 400:
        return {"__error__": f"{addr}: HTTP {r.status_code}", "status": r.status_code,
                "data": data, "addr": addr}
    return {"status": r.status_code, "data": data, "addr": addr}


def is_err(x: Any) -> bool:
    return isinstance(x, dict) and "__error__" in x


def payload(x: Any) -> Any:
    return None if (x is None or is_err(x)) else x.get("data")


def unwrap(x: Any) -> Any:
    """rest_call wrapper -> payload; already-bare payloads pass through; errors -> None."""
    if x is None or is_err(x):
        return None
    if isinstance(x, dict) and "data" in x and isinstance(x.get("status"), int):
        return x["data"]
    return x


def find_leader(cl: Cluster, raft: dict[str, dict] | None = None) -> str | None:
    """Manager address whose own /raft/state says it is the leader."""
    if raft is None:
        raft = fetch_all(cl, "/api/v1/raft/state")
    for addr, r in raft.items():
        d = payload(r)
        if isinstance(d, dict) and d.get("is_leader") is True:
            return addr
    return None


def fetch_all(cl: Cluster, path: str, params: dict | None = None) -> dict[str, dict]:
    with ThreadPoolExecutor(max_workers=max(1, len(cl.managers))) as ex:
        results = list(ex.map(lambda a: rest_call(cl, a, path, params), cl.managers))
    return dict(zip(cl.managers, results))


def pick_manager(cl: Cluster, manager: str | None) -> str:
    if manager:
        if manager not in cl.managers:
            raise ValidationError(
                f"manager {manager!r} not configured; configured: {', '.join(cl.managers)}"
            )
        return manager
    return find_leader(cl) or cl.managers[0]


# --------------------------------------------------------------------------
# Findings (pure)
# --------------------------------------------------------------------------

_BAD_HEALTH = {"down": "critical", "failed": "critical", "quarantined": "critical",
               "suspect": "warning", "degraded": "warning"}
_BAD_DISK = ("failed", "removed", "degraded", "offline", "readonly", "read_only")


def _f(sev: str, code: str, msg: str, **extra: Any) -> dict:
    return {"severity": sev, "code": code, "message": msg, **extra}


def compute_findings(
    raft: dict[str, Any],
    ready: dict[str, Any] | None = None,
    status: Any = None,
    stats: Any = None,
    nodes: Any = None,
    memory: Any = None,
    faults: Any = None,
    migrations: Any = None,
    now_ms: int | None = None,
) -> list[dict]:
    """Derive findings from already-fetched JSON. Each input is the raw payload
    (or None / {"__error__": ...} when unavailable). `raft`/`ready` map manager
    address -> payload-or-error."""
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    out: list[dict] = []
    ready = ready or {}

    reach: dict[str, dict] = {}
    for addr, r in raft.items():
        if is_err(r) or not isinstance(r, dict) or r.get("data") is None and "state" not in r:
            msg = (r or {}).get("__error__", "no data") if isinstance(r, dict) else "no data"
            out.append(_f("critical", "manager_unreachable",
                          f"manager {addr} unreachable: {msg}", manager=addr))
        else:
            d = r.get("data", r)
            if isinstance(d, dict):
                reach[addr] = d

    for addr, d in reach.items():
        rs = str(d.get("running_state", "ok"))
        if rs.startswith("fatal"):
            out.append(_f("critical", "raft_fatal", f"{addr} raft {rs}", manager=addr))
        lag = d.get("applied_log_lag") or 0
        if lag > 1000:
            out.append(_f("critical", "applied_lag", f"{addr} applied_log_lag={lag}", manager=addr))
        elif lag > 50:
            out.append(_f("warning", "applied_lag", f"{addr} applied_log_lag={lag}", manager=addr))
        repl = d.get("replication")
        if isinstance(repl, list):
            for p in repl:
                pl = (p or {}).get("lag") or 0
                if pl > 100:
                    out.append(_f("warning", "replication_lag",
                                  f"follower {p.get('node_id')} lag={pl} behind leader {addr}",
                                  manager=addr))

    if reach:
        leaders = {d.get("current_leader") for d in reach.values()}
        if None in leaders:
            out.append(_f("critical", "no_leader",
                          "at least one reachable manager reports no leader (quorum lost or electing)"))
        known = {l for l in leaders if l is not None}
        if len(known) > 1:
            out.append(_f("critical", "leader_disagreement",
                          f"managers disagree on leader: {sorted(map(str, known))}"))
        terms = {d.get("current_term") for d in reach.values()}
        if len(terms) > 1:
            out.append(_f("warning", "term_split",
                          f"managers report different terms: {sorted(map(str, terms))}"))
        if not any(d.get("is_leader") for d in reach.values()) and len(known) == 1:
            out.append(_f("warning", "leader_unreachable",
                          f"leader {next(iter(known))} is not among reachable managers"))

    for addr, r in ready.items():
        if is_err(r):
            if not is_err(raft.get(addr)):
                out.append(_f("warning", "not_ready", f"{addr} /readyz: {r['__error__']}",
                              manager=addr))

    st = unwrap(status)
    if isinstance(st, dict):
        fm = st.get("files_missing_chunks") or 0
        if fm > 0:
            out.append(_f("critical", "files_missing_chunks",
                          f"{fm} file(s) reference chunks with no live replica"))
        ap = st.get("anchor_pending") or 0
        if ap > 0:
            out.append(_f("warning", "anchor_pending",
                          f"{ap} file(s) not yet anchored on the primary DC"))
        if st.get("status") not in (None, "ok", "healthy"):
            out.append(_f("warning", "cluster_status", f"cluster status = {st.get('status')}"))

    sd = unwrap(stats)
    if isinstance(sd, dict):
        cp = sd.get("chunk_placement") or {}
        if (cp.get("missing") or 0) > 0:
            out.append(_f("critical", "chunks_missing", f"{cp['missing']} chunk(s) with no live replica"))
        if (cp.get("undergoal") or 0) > 0:
            out.append(_f("warning", "chunks_undergoal", f"{cp['undergoal']} chunk(s) under-replicated"))
        if (cp.get("misplaced") or 0) > 0:
            out.append(_f("info", "chunks_misplaced", f"{cp['misplaced']} chunk(s) misplaced"))

    nd = unwrap(nodes)
    if isinstance(nd, list):
        versions: dict[str, list[str]] = {}
        for n in nd:
            if not isinstance(n, dict):
                continue
            name = f"{n.get('hostname')}({n.get('role')})"
            h = str(n.get("health", "")).lower()
            if h in _BAD_HEALTH:
                out.append(_f(_BAD_HEALTH[h], f"node_{h}", f"node {name} health={n.get('health')}",
                              node=n.get("node_id")))
            elif h in ("maintenance", "draining"):
                out.append(_f("info", f"node_{h}", f"node {name} is {n.get('health')}",
                              node=n.get("node_id")))
            if n.get("quarantine"):
                out.append(_f("critical", "node_quarantine",
                              f"node {name} quarantined: {json.dumps(n['quarantine'], default=str)[:300]}",
                              node=n.get("node_id")))
            for d in n.get("disks") or []:
                ds = str(d.get("status", "")).lower()
                if any(b in ds for b in _BAD_DISK):
                    out.append(_f("warning", "disk_bad",
                                  f"node {name} disk {d.get('mount_path')} status={d.get('status')}",
                                  node=n.get("node_id")))
            seen = n.get("last_seen_ms") or 0
            if n.get("role") in ("Data", "data") and seen and now_ms - seen > 60_000:
                out.append(_f("warning", "stale_last_seen",
                              f"node {name} last seen {(now_ms - seen) // 1000}s ago",
                              node=n.get("node_id")))
            if n.get("version"):
                versions.setdefault(str(n["version"]), []).append(name)
            if n.get("raft_member") is False:
                out.append(_f("warning", "manager_unjoined",
                              f"manager {name} registered but not in raft membership"))
        if len(versions) > 1:
            out.append(_f("warning", "version_skew",
                          "nodes run different versions: "
                          + "; ".join(f"{v}: {', '.join(ns)}" for v, ns in sorted(versions.items()))))

    mem = unwrap(memory)
    if isinstance(mem, dict):
        lvl = str(mem.get("pressure_level", "")).lower()
        if lvl and lvl not in ("ok", "normal", "none", "green"):
            sev = "critical" if any(k in lvl for k in ("shed", "critical", "reject")) else "warning"
            out.append(_f(sev, "memory_pressure", f"manager memory pressure level = {mem.get('pressure_level')}"))

    mg = unwrap(migrations)
    if isinstance(mg, dict):
        failed = [t for t in (mg.get("tasks") or [])
                  if "fail" in str((t or {}).get("status", "")).lower()]
        if failed:
            out.append(_f("warning", "migrations_failed",
                          f"{len(failed)} failed migration/repair task(s); first error: "
                          f"{str(failed[0].get('last_error'))[:200]}"))

    fl = unwrap(faults)
    if isinstance(fl, list) and fl:
        newest = fl[0] if isinstance(fl[0], dict) else {}
        out.append(_f("warning", "integrity_faults",
                      f"{len(fl)} recent integrity fault audit row(s); newest: "
                      f"{newest.get('action')} {newest.get('target')}"))

    order = {"critical": 0, "warning": 1, "info": 2}
    out.sort(key=lambda f: order.get(f["severity"], 3))
    return out


def cluster_overview_impl(cl: Cluster, now_ms: int | None = None) -> dict:
    raft = fetch_all(cl, "/api/v1/raft/state")
    ready = fetch_all(cl, "/readyz")
    leader = find_leader(cl, raft)
    target = leader or next((a for a, r in raft.items() if not is_err(r)), None)
    data: dict[str, Any] = {}
    if target:
        for key, path, params in (
            ("status", "/api/v1/cluster/status", None),
            ("stats", "/api/v1/stats", None),
            ("nodes", "/api/v1/nodes", None),
            ("memory", "/api/v1/memory", None),
            ("faults", "/api/v1/integrity/faults", {"limit": 20}),
            ("migrations", "/api/v1/migrations", None),
        ):
            data[key] = rest_call(cl, target, path, params)
    findings = compute_findings(
        raft=raft, ready=ready,
        status=data.get("status"), stats=data.get("stats"), nodes=data.get("nodes"),
        memory=data.get("memory"), faults=data.get("faults"),
        migrations=data.get("migrations"), now_ms=now_ms,
    )
    if not target:
        findings.insert(0, _f("critical", "no_reachable_manager",
                              "no manager answered /raft/state; check network/Tailscale and config"))
    notes = []
    for k, v in data.items():
        if is_err(v) and v.get("status") == 404:
            notes.append(f"{k}: 404 (older manager build without this route)")
    return {
        "cluster": cl.name,
        "leader": leader,
        "queried": target,
        "findings": findings,
        "notes": notes,
        "raft": {a: (payload(r) if not is_err(r) else r["__error__"]) for a, r in raft.items()},
        "data": {k: (payload(v) if not is_err(v) else v["__error__"]) for k, v in data.items()},
    }


# --------------------------------------------------------------------------
# Audit merge / Prometheus
# --------------------------------------------------------------------------

def merge_audit(per_manager: dict[str, Any], limit: int, since_ms: int | None = None) -> list[dict]:
    seen: set[tuple] = set()
    rows: list[dict] = []
    for addr, r in per_manager.items():
        d = payload(r)
        if not isinstance(d, list):
            continue
        for e in d:
            if not isinstance(e, dict):
                continue
            ts = e.get("timestamp") or 0  # seconds since epoch
            if since_ms is not None and ts * 1000 < since_ms:
                continue
            key = (ts, e.get("actor"), e.get("action"), e.get("target"), e.get("summary"))
            if key in seen:
                continue
            seen.add(key)
            rows.append({**e, "seen_on": addr})
    rows.sort(key=lambda e: e.get("timestamp") or 0, reverse=True)
    return rows[:limit]


_PROM = re.compile(r"([A-Za-z_:][A-Za-z0-9_:]*)(\{.*\})?\s+(\S+)(?:\s+-?\d+)?")


def parse_prometheus(text: str, match: str | None = None) -> dict[str, float | str]:
    out: dict[str, float | str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = _PROM.fullmatch(line)
        if not m:
            continue
        key = m.group(1) + (m.group(2) or "")
        if match and match not in key:
            continue
        try:
            out[key] = float(m.group(3))
        except ValueError:
            out[key] = m.group(3)
    return out


# --------------------------------------------------------------------------
# SSH
# --------------------------------------------------------------------------

def ssh_argv(cl: Cluster, host: dict, remote_cmd: str) -> list[str]:
    argv = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
            "-o", "StrictHostKeyChecking=accept-new"]
    if cl.ssh_key:
        argv += ["-i", os.path.expanduser(cl.ssh_key)]
    argv += ["-p", str(cl.ssh_port), f"{cl.ssh_user}@{host['address']}", remote_cmd]
    return argv


def run_ssh(cl: Cluster, host: dict, remote_cmd: str, timeout: int = SSH_TIMEOUT) -> dict:
    argv = ssh_argv(cl, host, remote_cmd)
    try:
        p = subprocess.run(argv, capture_output=True, text=True, errors="replace",
                           timeout=timeout, stdin=subprocess.DEVNULL)
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": f"ssh to {host.get('name')} timed out after {timeout}s"}
    except FileNotFoundError:
        return {"ok": False, "error": "system `ssh` binary not found on PATH"}
    except OSError as e:
        return {"ok": False, "error": scrub(f"ssh failed: {e}", cl.secrets())}
    res = {"ok": p.returncode == 0, "rc": p.returncode,
           "stdout": scrub(p.stdout, cl.secrets()), "stderr": scrub(p.stderr, cl.secrets())[:4000]}
    if p.returncode == 255:
        res["error"] = f"ssh to {host.get('name')} failed (unreachable/auth): {res['stderr'][:300]}"
    return res


def _q(v: str) -> str:
    return shlex.quote(v)


def cmd_logs(unit: str, since: str, until: str | None, lines: int) -> str:
    validate_unit(unit)
    validate_time(since)
    if until is not None:
        validate_time(until)
    validate_lines(lines)
    if unit == "kernel":
        sel = "-k"
    else:
        names = {"manager": ["dtfs-manager"], "data": ["dtfs-data"], "fuse": ["dtfs-fuse@*"],
                 "all": ["dtfs-manager", "dtfs-data", "dtfs-fuse@*"]}[unit]
        sel = " ".join(f"-u {_q(n)}" for n in names)
    cmd = f"journalctl {sel} --since {_q(since)}"
    if until is not None:
        cmd += f" --until {_q(until)}"
    return cmd + f" -o short-iso --no-pager -n {int(lines)}"


def cmd_status() -> str:
    parts = [
        "echo '### systemctl'",
        "systemctl show dtfs-manager dtfs-data 'dtfs-fuse@*' -p Id,ActiveState,SubState,"
        "NRestarts,MemoryCurrent,MemoryMax,ExecMainStartTimestamp",
        "echo '### versions'",
        "for b in dtfs-manager dtfs-data dtfs-fuse dtfs-cli; do "
        "[ -x /usr/local/bin/$b ] && /usr/local/bin/$b --version; done",
        "echo '### uptime'", "uptime",
        "echo '### memory'", "free -b",
        "echo '### df'", "df -B1",
        "echo '### clock'", "chronyc tracking || timedatectl",
        "echo '### kernel'", "journalctl -k --since '24 hours ago' --no-pager -n 5000",
    ]
    return " ; ".join(parts)


_OOM = re.compile(r"out of memory|oom-kill|oom_reaper|killed process|invoked oom", re.I)


def split_sections(text: str) -> dict[str, str]:
    secs: dict[str, list[str]] = {}
    cur = "preamble"
    for line in text.splitlines():
        if line.startswith("### "):
            cur = line[4:].strip()
            secs[cur] = []
        else:
            secs.setdefault(cur, []).append(line)
    return {k: "\n".join(v).strip() for k, v in secs.items() if v or k != "preamble"}


def process_status(stdout: str) -> dict:
    secs = split_sections(stdout)
    kern = secs.pop("kernel", "")
    secs["kernel_oom"] = "\n".join([l for l in kern.splitlines() if _OOM.search(l)][-50:])
    return secs


def cmd_disks_for(paths: list[str], count_chunks: bool) -> str:
    parts = ["echo '### lsblk'", "lsblk -J -o NAME,TYPE,SIZE,MOUNTPOINT,MODEL"]
    for p in paths:
        validate_disk_path(p)
        d = _q(p)
        parts += [
            f"echo '### disk {p}'",
            f"df -B1 {d}",
            f"findmnt -n -o SOURCE,FSTYPE,OPTIONS --target {d}",
            f"cat {d}/.dtfs-disk-uuid",
            f"echo '-- wal'; ls -la {d}/.dtfs/wal | head -n 200",
            f"echo '-- quarantine'; ls -la --time-style=long-iso {d}/.dtfs/quarantine | head -n 200",
            f"echo '-- tmp'; find {d}/tmp -type f | wc -l; "
            f"find {d}/tmp -type f -printf '%T+ %s %p\\n' | sort | head -n 1",
        ]
        if count_chunks:
            parts.append(f"echo '-- chunks'; find {d}/chunks -name '*.dat' | wc -l")
    return " ; ".join(parts)


def cmd_cat_config(role: str) -> str:
    return f"cat /etc/dtfs/{validate_role(role)}.toml"


def cmd_smart(device: str) -> str:
    return f"smartctl -a -j {_q(validate_device(device))}"


def cmd_chunk_for(paths: list[str], chunk_id: str) -> str:
    cid = validate_chunk_id(chunk_id)
    p1, p2 = cid[0:2], cid[2:4]  # ChunkId::dir_prefix_1/2: first two bytes, 2 hex each
    parts = []
    for p in paths:
        validate_disk_path(p)
        d = _q(p)
        base = f"{d}/chunks/{p1}/{p2}/{cid}"
        parts += [
            f"echo '### disk {p}'",
            f"stat -c '%n %s bytes mtime=%y' {base}.dat {base}.meta 2>&1",
            f"echo '-- meta'; head -c 4096 {base}.meta 2>&1",
            f"echo '-- quarantine'; ls -la {d}/.dtfs/quarantine/{cid}.dat.bad 2>&1",
            f"echo '-- wal'; ls -la {d}/.dtfs/wal/{cid}.* 2>&1",
        ]
    return " ; ".join(parts)


def cmd_net(targets: list[str]) -> str:
    parts = ["echo '### listening'", "ss -tlnp"]
    for t in targets:
        validate_target(t)
        h, p = t.rsplit(":", 1)
        parts.append(
            f"echo -n '{t} '; timeout 3 bash -c {_q(f'</dev/tcp/{h}/{p}')} 2>/dev/null "
            "&& echo OPEN || echo CLOSED"
        )
    return " ; ".join(parts)


_SECRET_LINE = re.compile(
    r'^(\s*"?[A-Za-z0-9_.\-]*(?:secret|token|password|key_file)[A-Za-z0-9_.\-]*"?\s*=\s*).*$',
    re.I | re.M,
)


_TABLE_HDR = re.compile(r'^\s*\[+\s*([^\]]+?)\s*\]+\s*(?:#.*)?$')
_KV_LINE = re.compile(r'^(\s*"?([A-Za-z0-9_.\-]*)"?\s*=\s*)(.*)$')
# [[alerts.webhooks]] keys that carry no secret; every other key is redacted
# (the webhook URL itself is the credential for Slack/Discord-style sinks).
_WEBHOOK_SAFE_KEYS = {"kinds", "dedup_secs"}


def redact_config(text: str) -> str:
    out: list[str] = []
    in_webhook = False
    for line in _SECRET_LINE.sub(lambda m: m.group(1) + '"***"', text).splitlines(keepends=True):
        body = line.rstrip("\r\n")
        h = _TABLE_HDR.match(body)
        if h:
            in_webhook = h.group(1) == "alerts.webhooks"
        else:
            kv = _KV_LINE.match(body)
            if kv:
                key, val = kv.group(2).lower(), kv.group(3)
                # URL userinfo / query strings carry credentials anywhere.
                url_cred = "url" in key and ("@" in val or "?" in val)
                if url_cred or (in_webhook and key not in _WEBHOOK_SAFE_KEYS):
                    line = kv.group(1) + '"***"' + line[len(body):]
        out.append(line)
    return "".join(out)


def disk_paths_from_config(text: str) -> list[str]:
    try:
        cfg = tomllib.loads(text)
    except tomllib.TOMLDecodeError as e:
        raise ValidationError(f"remote data.toml is not valid TOML: {e}") from e
    return [validate_disk_path(str(d["path"])) for d in cfg.get("disks", []) if "path" in d]


def summarize_smart(raw: str) -> dict:
    try:
        j = json.loads(raw)
    except ValueError:
        return {"raw": raw[:2000]}
    keep = {5, 177, 187, 188, 196, 197, 198, 199}
    attrs = [
        {"id": a.get("id"), "name": a.get("name"), "raw": (a.get("raw") or {}).get("value"),
         "value": a.get("value"), "thresh": a.get("thresh")}
        for a in (j.get("ata_smart_attributes") or {}).get("table", []) if a.get("id") in keep
    ]
    return {
        "model": j.get("model_name"), "serial_tail": str(j.get("serial_number", ""))[-4:],
        "passed": (j.get("smart_status") or {}).get("passed"),
        "temperature_c": (j.get("temperature") or {}).get("current"),
        "power_on_hours": (j.get("power_on_time") or {}).get("hours"),
        "attributes": attrs,
        "nvme_health": j.get("nvme_smart_health_information_log"),
        "messages": (j.get("smartctl") or {}).get("messages"),
    }


def _ssh_tool(cl: Cluster, host_name: str, cmd: str, timeout: int = SSH_TIMEOUT) -> tuple[dict, dict]:
    host = cl.host(host_name)
    return host, run_ssh(cl, host, cmd, timeout)


def _unreachable(host: str, res: dict) -> dict:
    return {"host": host, "findings": [_f("critical", "host_unreachable",
            res.get("error") or f"ssh rc={res.get('rc')}: {res.get('stderr', '')[:300]}")]}


def _remote_disks(cl: Cluster, host_name: str) -> tuple[list[str] | None, dict | None]:
    host, res = _ssh_tool(cl, host_name, cmd_cat_config("data"))
    if not res.get("ok"):
        return None, _unreachable(host_name, res)
    return disk_paths_from_config(res["stdout"]), None


def host_logs_impl(cl: Cluster, host: str, unit: str, since: str, until: str | None,
                   grep: str | None, lines: int) -> dict:
    cmd = cmd_logs(unit, since, until, lines)
    h, res = _ssh_tool(cl, host, cmd, SSH_LOG_TIMEOUT)
    if not res.get("ok"):
        return _unreachable(host, res)
    out = res["stdout"].splitlines()
    if grep:
        try:
            rx = re.compile(grep[:200], re.I)
            out = [l for l in out if rx.search(l)]
        except re.error:
            out = [l for l in out if grep.lower() in l.lower()]
    return {"host": host, "unit": unit, "lines": len(out), "log": "\n".join(out)}


# --------------------------------------------------------------------------
# Gated ops
# --------------------------------------------------------------------------

def _hint_addr(cl: Cluster, hint: Any) -> str | None:
    if not isinstance(hint, dict):
        return None
    hn = str(hint.get("leader_hostname") or "").lower()
    for m in cl.managers:
        if hn and m.split(":")[0].lower().split(".")[0] == hn.split(".")[0]:
            return m
    la = str(hint.get("leader_addr") or "")
    if la:
        port = cl.managers[0].rsplit(":", 1)[-1]
        return f"{la.rsplit(':', 1)[0]}:{port}"
    return None


def gated_post(cl: Cluster, path: str, body: dict, confirm: bool, what: str) -> dict:
    if confirm is not True:
        return {"dry_run": True, "action": what, "would_send": {"method": "POST", "path": path, "body": body},
                "note": "nothing was changed; re-call with confirm=true after the user approves"}
    return _post_with_leader(cl, path, body, what)


def _post_with_leader(cl: Cluster, path: str, body: dict, what: str) -> dict:
    addr = find_leader(cl) or cl.managers[0]
    r = rest_call(cl, addr, path, method="POST", body=body, auth=True)
    if is_err(r) and r.get("status") == 409:
        d = r.get("data")
        hint = d.get("leader") if isinstance(d, dict) else None
        alt = _hint_addr(cl, hint)
        if alt and alt != addr:
            r = rest_call(cl, alt, path, method="POST", body=body, auth=True)
            addr = alt
    return {"action": what, "manager": addr, "executed": not is_err(r),
            "response": r.get("data") if "data" in r else r.get("__error__")}


def op_scrub_impl(cl: Cluster, level: str, confirm: bool) -> dict:
    if level not in ("quick", "deep"):
        raise ValidationError("level must be 'quick' or 'deep'")
    body = {"target_type": "all", "target": "*", "level": level}
    return gated_post(cl, "/api/v1/scrub", body, confirm, f"trigger {level} scrub of all disks")


def op_maintenance_impl(cl: Cluster, node_id: Any, enabled: bool, confirm: bool) -> dict:
    nid = validate_node_id(node_id)
    if not isinstance(enabled, bool):
        raise ValidationError("enabled must be a boolean")
    return gated_post(cl, f"/api/v1/nodes/{nid}/maintenance", {"enable": enabled}, confirm,
                      f"{'enable' if enabled else 'disable'} maintenance on node {nid}")


def op_unquarantine_impl(cl: Cluster, node_id: Any, operator_id: str, reason: str,
                         confirm: bool) -> dict:
    nid = validate_node_id(node_id)
    _need(_OPERATOR, operator_id, "operator_id")
    if not isinstance(reason, str) or not 16 <= len(reason) <= 500 or not reason.isprintable():
        raise ValidationError("reason must be 16..500 printable characters (server requires >= 16)")
    body = {"operator_id": operator_id, "reason_acknowledged": reason}
    return gated_post(cl, f"/api/v1/nodes/{nid}/unquarantine", body, confirm,
                      f"clear integrity quarantine on node {nid}")


# --------------------------------------------------------------------------
# MCP wiring
# --------------------------------------------------------------------------

def _guard(fn):
    """Run a sync impl in a thread; turn setup/validation/other errors into text."""
    @functools.wraps(fn)
    async def wrapper(*a, **kw):
        secrets: list[str] = []
        try:
            def run():
                return fn(*a, **kw)
            result = await asyncio.to_thread(run)
            try:
                secrets = get_cluster(kw.get("cluster")).secrets()
            except Exception:  # noqa: BLE001
                pass
            return render(result, secrets)
        except SetupError as e:
            return render({"setup_required": str(e)})
        except ValidationError as e:
            return render({"error": f"invalid argument: {e}"})
        except Exception as e:  # noqa: BLE001
            try:
                secrets = get_cluster(kw.get("cluster")).secrets()
            except Exception:  # noqa: BLE001
                pass
            return render({"error": f"{type(e).__name__}: {e}"}, secrets)
    wrapper.__signature__ = inspect.signature(fn).replace(return_annotation=str)  # type: ignore[attr-defined]
    wrapper.__annotations__ = {**fn.__annotations__, "return": str}
    return wrapper


def build_server():
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP("dtfs")

    def tool(fn):
        g = _guard(fn)
        mcp.tool()(g)
        return g

    @tool
    def cluster_overview(cluster: str | None = None) -> dict:
        """Fan out to every manager (raft state, readyz) and the leader (status, stats,
        nodes, memory, faults, migrations); returns raw data plus computed findings."""
        return cluster_overview_impl(get_cluster(cluster))

    @tool
    def rest_get(path: str, params: dict | None = None, manager: str | None = None,
                 cluster: str | None = None) -> dict:
        """GET a manager REST path (/api/v1/..., /metrics, /readyz, /healthz). Defaults to the leader."""
        validate_rest_path(path)
        cl = get_cluster(cluster)
        r = rest_call(cl, pick_manager(cl, manager), path, params)
        return r

    @tool
    def audit_merged(limit: int = 100, action: str | None = None, since_ms: int | None = None,
                     cluster: str | None = None) -> dict:
        """Merge /api/v1/audit from every manager, de-duplicated, newest first."""
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValidationError("limit must be 1..1000")
        cl = get_cluster(cluster)
        params: dict = {"limit": limit}
        if action:
            _need(re.compile(r"[A-Za-z0-9_.-]{1,64}"), action, "action")
            params["action"] = action
        per = fetch_all(cl, "/api/v1/audit", params)
        errs = {a: r["__error__"] for a, r in per.items() if is_err(r)}
        return {"entries": merge_audit(per, limit, since_ms), "unreachable": errs}

    @tool
    def metrics(match: str | None = None, manager: str | None = None,
                cluster: str | None = None) -> dict:
        """Prometheus /metrics from the leader parsed to {name{labels}: value}; optional substring filter."""
        cl = get_cluster(cluster)
        addr = pick_manager(cl, manager)
        r = rest_call(cl, addr, "/metrics")
        if is_err(r):
            return {"error": r["__error__"]}
        return {"manager": addr, "metrics": parse_prometheus(str(r["data"]), match)}

    @tool
    def host_logs(host: str, unit: str = "all", since: str = "1h ago", until: str | None = None,
                  grep: str | None = None, lines: int = 500, cluster: str | None = None) -> dict:
        """journalctl for dtfs units on a configured host. unit: manager|data|fuse|all|kernel.
        since/until: '<N>[smhd] ago' or 'YYYY-MM-DD[ HH:MM[:SS]]'. grep is applied locally."""
        return host_logs_impl(get_cluster(cluster), host, unit, since, until, grep, lines)

    @tool
    def host_status(host: str, cluster: str | None = None) -> dict:
        """Service state, versions, uptime, memory, df, clock sync and kernel OOM lines (24h)."""
        cl = get_cluster(cluster)
        h, res = _ssh_tool(cl, host, cmd_status())
        if not res.get("ok") and not res.get("stdout"):
            return _unreachable(host, res)
        return {"host": host, **process_status(res["stdout"])}

    @tool
    def host_disks(host: str, count_chunks: bool = False, cluster: str | None = None) -> dict:
        """Per data disk (from /etc/dtfs/data.toml): df, WAL, quarantine, tmp, uuid, backing device."""
        cl = get_cluster(cluster)
        paths, err = _remote_disks(cl, host)
        if err:
            return err
        h, res = _ssh_tool(cl, host, cmd_disks_for(paths or [], bool(count_chunks)))
        if not res.get("stdout") and not res.get("ok"):
            return _unreachable(host, res)
        return {"host": host, "disks": paths, **split_sections(res["stdout"]),
                "stderr": res.get("stderr", "")[:1000]}

    @tool
    def host_smart(host: str, device: str | None = None, cluster: str | None = None) -> dict:
        """smartctl summary for a block device (must appear in the host's lsblk), or all disks."""
        cl = get_cluster(cluster)
        if device is not None:
            validate_device(device)
        h, res = _ssh_tool(cl, host, "lsblk -J -o NAME,TYPE")
        if not res.get("ok"):
            return _unreachable(host, res)
        try:
            devs = json.loads(res["stdout"]).get("blockdevices", [])
        except ValueError:
            return {"host": host, "error": "could not parse lsblk output"}
        flat: list[dict] = []

        def walk(ns):
            for n in ns:
                flat.append(n)
                walk(n.get("children") or [])
        walk(devs)
        known = {f"/dev/{n['name']}" for n in flat}
        targets = [device] if device else [f"/dev/{n['name']}" for n in devs if n.get("type") == "disk"]
        if device and device not in known:
            raise ValidationError(f"{device} is not a block device on {host}")
        out = {}
        for t in targets[:12]:
            r = run_ssh(cl, h, cmd_smart(t))
            out[t] = summarize_smart(r.get("stdout", "")) if r.get("stdout") else {
                "error": r.get("error") or r.get("stderr", "")[:300]}
        return {"host": host, "devices": out}

    @tool
    def host_config(host: str, role: str, cluster: str | None = None) -> dict:
        """/etc/dtfs/<role>.toml with secret/token/password/key_file values redacted."""
        cl = get_cluster(cluster)
        h, res = _ssh_tool(cl, host, cmd_cat_config(role))
        if not res.get("ok"):
            return _unreachable(host, res)
        return {"host": host, "role": role, "config": redact_config(res["stdout"])}

    @tool
    def host_chunk(host: str, chunk_id: str, cluster: str | None = None) -> dict:
        """Locate a chunk on a data host's disks: .dat/.meta stat, .meta content, quarantine, WAL."""
        cl = get_cluster(cluster)
        validate_chunk_id(chunk_id)
        paths, err = _remote_disks(cl, host)
        if err:
            return err
        h, res = _ssh_tool(cl, host, cmd_chunk_for(paths or [], chunk_id))
        if not res.get("stdout") and not res.get("ok"):
            return _unreachable(host, res)
        return {"host": host, "chunk_id": chunk_id.lower(), **split_sections(res["stdout"])}

    @tool
    def host_net(host: str, targets: list[str] | None = None, cluster: str | None = None) -> dict:
        """Listening sockets (ss -tlnp) plus optional host:port reachability probes from that host."""
        cl = get_cluster(cluster)
        tg = [validate_target(t) for t in (targets or [])][:20]
        h, res = _ssh_tool(cl, host, cmd_net(tg), SSH_TIMEOUT + 3 * len(tg))
        if not res.get("stdout") and not res.get("ok"):
            return _unreachable(host, res)
        return {"host": host, **split_sections(res["stdout"])}

    @tool
    def op_scrub(level: str = "deep", confirm: bool = False, cluster: str | None = None) -> dict:
        """GATED. Trigger a full scrub (quick|deep). Dry-run unless confirm=true."""
        return op_scrub_impl(get_cluster(cluster), level, confirm)

    @tool
    def op_maintenance(node_id: str, enabled: bool, confirm: bool = False,
                       cluster: str | None = None) -> dict:
        """GATED. Enable/disable maintenance mode on a node (numeric node_id). Dry-run unless confirm=true."""
        return op_maintenance_impl(get_cluster(cluster), node_id, enabled, confirm)

    @tool
    def op_unquarantine(node_id: str, operator_id: str, reason_acknowledged: str,
                        confirm: bool = False, cluster: str | None = None) -> dict:
        """GATED. Clear a node's integrity quarantine (reason >= 16 chars, audited). Dry-run unless confirm=true."""
        return op_unquarantine_impl(get_cluster(cluster), node_id, operator_id,
                                    reason_acknowledged, confirm)

    return mcp


def main() -> None:
    build_server().run()


if __name__ == "__main__":
    main()
