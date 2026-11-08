import asyncio
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
import dtfs_mcp as m  # noqa: E402

INJECTIONS = ["; rm -rf /", "`id`", "$(id)", "a\nb", "a b", "x|y", "a&&b", "'", '"']


def cluster(**over):
    raw = {
        "managers": ["mgr01:9402", "mgr02:9402"],
        "shared_secret": "S3CRET",
        "ssh_user": "root",
        "ssh_key": "/k/id",
        "ssh_port": 2222,
        "hosts": [{"name": "toro-plex", "address": "100.64.0.4"}],
    }
    raw.update(over)
    return m.Cluster("prod", raw)


# ---- validators reject injection for every argument ----------------------

@pytest.mark.parametrize("bad", INJECTIONS)
def test_validators_reject_injection(bad):
    for fn in (m.validate_hostname, m.validate_time, m.validate_chunk_id, m.validate_device,
               m.validate_target, m.validate_disk_path, m.validate_node_id, m.validate_role,
               m.validate_unit, m.validate_rest_path):
        with pytest.raises(m.ValidationError):
            fn(bad)
    with pytest.raises(m.ValidationError):
        m.cmd_logs("manager", bad, None, 10)
    with pytest.raises(m.ValidationError):
        m.cmd_logs("manager", "1h ago", bad, 10)
    with pytest.raises(m.ValidationError):
        m.cmd_chunk_for(["/mnt/hdd0"], bad)
    with pytest.raises(m.ValidationError):
        m.cmd_chunk_for([bad], "ab" * 32)
    with pytest.raises(m.ValidationError):
        m.cmd_net([bad])
    with pytest.raises(m.ValidationError):
        m.cmd_disks_for([bad], False)
    with pytest.raises(m.ValidationError):
        m.cmd_smart(bad)
    with pytest.raises(m.ValidationError):
        m.cmd_cat_config(bad)
    with pytest.raises(m.ValidationError):
        cluster().host(bad)


def test_trailing_newline_rejected():
    with pytest.raises(m.ValidationError):
        m.validate_hostname("host\n")
    with pytest.raises(m.ValidationError):
        m.validate_chunk_id("abcdef12\n")


def test_lines_bounds():
    for bad in (0, 5001, -1, True, "5", None):
        with pytest.raises(m.ValidationError):
            m.validate_lines(bad)
    assert m.validate_lines(5000) == 5000


def test_good_values():
    m.validate_time("10m ago")
    m.validate_time("2026-10-19")
    m.validate_time("2026-10-19 12:30:05")
    m.validate_target("100.64.0.4:9402")
    with pytest.raises(m.ValidationError):
        m.validate_target("h:99999")
    with pytest.raises(m.ValidationError):
        m.validate_disk_path("/mnt/../etc")


# ---- exact argv / templates ----------------------------------------------

def test_ssh_argv_exact():
    cl = cluster()
    argv = m.ssh_argv(cl, cl.hosts[0], "uptime")
    assert argv == ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
                    "-o", "StrictHostKeyChecking=accept-new", "-i", "/k/id", "-p", "2222",
                    "root@100.64.0.4", "uptime"]


def test_logs_commands_exact():
    assert m.cmd_logs("manager", "1h ago", None, 100) == (
        "journalctl -u dtfs-manager --since '1h ago' -o short-iso --no-pager -n 100")
    assert m.cmd_logs("fuse", "2026-10-19 10:00", "2026-10-19 11:00", 5) == (
        "journalctl -u 'dtfs-fuse@*' --since '2026-10-19 10:00' --until '2026-10-19 11:00' "
        "-o short-iso --no-pager -n 5")
    assert m.cmd_logs("kernel", "5m ago", None, 7).startswith("journalctl -k --since '5m ago'")
    assert m.cmd_logs("all", "1d ago", None, 1).count("-u ") == 3


def test_chunk_paths_use_first_two_bytes():
    cid = "ab12" + "0" * 60
    cmd = m.cmd_chunk_for(["/mnt/hdd0"], cid)
    assert f"/mnt/hdd0/chunks/ab/12/{cid}.dat" in cmd
    assert f"/mnt/hdd0/.dtfs/quarantine/{cid}.dat.bad" in cmd
    assert f"/mnt/hdd0/.dtfs/wal/{cid}.*" in cmd


def test_net_and_smart_and_config():
    assert "bash -c '</dev/tcp/h1.example/9402'" in m.cmd_net(["h1.example:9402"])
    assert m.cmd_smart("/dev/sda") == "smartctl -a -j /dev/sda"
    assert m.cmd_cat_config("data") == "cat /etc/dtfs/data.toml"


def test_disks_template_and_toml_parse():
    toml = '[[disks]]\npath = "/mnt/hdd0"\ntier = "cold"\n[[disks]]\npath = "/mnt/ssd0"\ntier = "warm"\n'
    paths = m.disk_paths_from_config(toml)
    assert paths == ["/mnt/hdd0", "/mnt/ssd0"]
    cmd = m.cmd_disks_for(paths, True)
    assert "/mnt/hdd0/.dtfs/wal" in cmd and "/mnt/ssd0/.dtfs/quarantine" in cmd
    assert "find /mnt/hdd0/chunks -name '*.dat'" in cmd
    assert "chunks -name" not in m.cmd_disks_for(paths, False)


def test_hostile_remote_disk_path_rejected():
    with pytest.raises(m.ValidationError):
        m.disk_paths_from_config('[[disks]]\npath = "/mnt/x; reboot"\n')


def test_unknown_host_rejected():
    with pytest.raises(m.ValidationError):
        cluster().host("evil")


def test_run_ssh_uses_argv_list(monkeypatch):
    seen = {}

    class P:
        returncode = 0
        stdout = "ok S3CRET"
        stderr = ""

    def fake(argv, **kw):
        seen["argv"], seen["kw"] = argv, kw
        return P()

    monkeypatch.setattr(m.subprocess, "run", fake)
    cl = cluster()
    res = m.run_ssh(cl, cl.hosts[0], "uptime")
    assert isinstance(seen["argv"], list) and "shell" not in seen["kw"]
    assert seen["kw"]["timeout"] == 30
    assert res["stdout"] == "ok ***"


def test_logs_grep_is_local(monkeypatch):
    cl = cluster()
    sent = []

    def fake(cl_, host, cmd, timeout=30):
        sent.append((cmd, timeout))
        return {"ok": True, "stdout": "a ERROR x\nb info\nc error y\n"}

    monkeypatch.setattr(m, "run_ssh", fake)
    out = m.host_logs_impl(cl, "toro-plex", "data", "1h ago", None, "error", 100)
    assert out["lines"] == 2
    assert "error" not in sent[0][0].lower()
    assert sent[0][1] == 60


def test_ssh_unreachable_is_finding(monkeypatch):
    monkeypatch.setattr(m, "run_ssh", lambda *a, **k: {"ok": False, "error": "timed out"})
    out = m.host_logs_impl(cluster(), "toro-plex", "data", "1h ago", None, None, 10)
    assert out["findings"][0]["code"] == "host_unreachable"


def test_status_oom_filtered_locally():
    out = m.process_status("### uptime\nup 1 day\n### kernel\nfoo\nOut of memory: Killed process 1 (x)\nbar\n")
    assert "Killed process" in out["kernel_oom"] and "foo" not in out["kernel_oom"]
    assert "kernel" not in out and out["uptime"] == "up 1 day"


# ---- redaction / scrub / cap ---------------------------------------------

def test_redact_config():
    txt = ('shared_secret = "abc"\n[tls]\nkey_file = "/etc/k.pem"\napi_token="x"\n'
           'Password = "p"\nhostname = "h"\n')
    out = m.redact_config(txt)
    for leak in ("abc", "/etc/k.pem", '"x"', '"p"'):
        assert leak not in out
    assert 'hostname = "h"' in out and 'shared_secret = "***"' in out


def test_redact_config_urls_and_webhooks():
    txt = ('[updater]\nrepo_url = "https://gitlab.example/example/dtfs"\n'
           'proxy_url = "http://user:pw@proxy:3128"\n'
           '[[alerts.webhooks]]\nurl = "https://hooks.slack.com/services/T0/B0/XYZ"\n'
           'secret = "s"\nkinds = ["node_down"]\ndedup_secs = 300\n'
           '[[alerts.webhooks]]\nurl = "https://x.example/h"  # c\n'
           '[grpc]\nlisten_addr = "0.0.0.0:9401"\n')
    out = m.redact_config(txt)
    for leak in ("XYZ", "user:pw", "x.example/h", '"s"'):
        assert leak not in out
    # non-sensitive values survive, including after the webhook tables end
    for keep in ('repo_url = "https://gitlab.example/example/dtfs"', 'kinds = ["node_down"]',
                 "dedup_secs = 300", 'listen_addr = "0.0.0.0:9401"'):
        assert keep in out
    assert out.count('url = "***"') == 3  # proxy_url + two webhook urls


def test_scrub_and_cap():
    assert m.scrub("boom S3CRET boom", ["S3CRET"]) == "boom *** boom"
    out = m.cap_output("x" * 70000)
    assert len(out) < 60200 and "truncated" in out
    assert m.cap_output("small") == "small"


# ---- rest_get path validation --------------------------------------------

@pytest.mark.parametrize("p", ["/api/v1/nodes", "/metrics", "/readyz", "/healthz",
                               "/api/v1/chunks/abcdef12"])
def test_rest_path_allowed(p):
    assert m.validate_rest_path(p) == p


@pytest.mark.parametrize("p", ["/etc/passwd", "/api/v2/x", "/api/v1/../x", "api/v1/x",
                               "/api/v1/x?y=1", "//api/v1/x", "/metrics/x", "http://evil/api/v1/x",
                               "/api/v1/a b", "/api/v1/%2e%2e"])
def test_rest_path_rejected(p):
    with pytest.raises(m.ValidationError):
        m.validate_rest_path(p)


def test_pick_manager_rejects_unconfigured():
    with pytest.raises(m.ValidationError):
        m.pick_manager(cluster(), "evil:1")
    assert m.pick_manager(cluster(), "mgr02:9402") == "mgr02:9402"


# ---- gated ops -----------------------------------------------------------

def _boom(*a, **k):
    raise AssertionError("network used without confirm")


def test_ops_refuse_without_confirm(monkeypatch):
    monkeypatch.setattr(m, "rest_call", _boom)
    monkeypatch.setattr(m, "find_leader", _boom)
    cl = cluster()
    r = m.op_scrub_impl(cl, "deep", False)
    assert r["dry_run"] and r["would_send"]["path"] == "/api/v1/scrub"
    assert r["would_send"]["body"] == {"target_type": "all", "target": "*", "level": "deep"}
    r = m.op_maintenance_impl(cl, "12", True, False)
    assert r["dry_run"] and r["would_send"]["body"] == {"enable": True}
    assert r["would_send"]["path"] == "/api/v1/nodes/12/maintenance"
    r = m.op_unquarantine_impl(cl, "12", "ops1", "disk replaced and verified ok", False)
    assert r["dry_run"] and r["would_send"]["path"] == "/api/v1/nodes/12/unquarantine"
    assert r["would_send"]["body"]["operator_id"] == "ops1"
    assert m.op_scrub_impl(cl, "deep", "yes")["dry_run"]  # truthy but not True


def test_ops_validate_args():
    cl = cluster()
    with pytest.raises(m.ValidationError):
        m.op_maintenance_impl(cl, "1; rm", True, True)
    with pytest.raises(m.ValidationError):
        m.op_unquarantine_impl(cl, "1", "op", "short", True)
    with pytest.raises(m.ValidationError):
        m.op_scrub_impl(cl, "fast", True)


def test_op_confirm_sends_secret_and_follows_409_once(monkeypatch):
    calls = []

    def fake(cl, addr, path, params=None, method="GET", body=None, auth=False):
        calls.append((addr, path, method, body, auth))
        if addr == "mgr01:9402":
            return {"__error__": "x: HTTP 409", "status": 409, "addr": addr,
                    "data": {"leader": {"leader_hostname": "mgr02", "leader_addr": "mgr02:9401"}}}
        return {"status": 200, "data": {"status": "accepted"}, "addr": addr}

    monkeypatch.setattr(m, "rest_call", fake)
    monkeypatch.setattr(m, "find_leader", lambda cl, raft=None: "mgr01:9402")
    r = m.op_scrub_impl(cluster(), "quick", True)
    assert [c[0] for c in calls] == ["mgr01:9402", "mgr02:9402"]
    assert all(c[2] == "POST" and c[4] is True for c in calls)
    assert r["executed"] and r["manager"] == "mgr02:9402"


def test_rest_call_header_and_error_scrub(monkeypatch):
    seen = {}

    class C:
        def __init__(self, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def request(self, method, url, params=None, json=None, headers=None):
            seen.update(method=method, url=url, headers=headers)
            raise RuntimeError("connect failed S3CRET")

    monkeypatch.setattr(m.httpx, "Client", C)
    r = m.rest_call(cluster(), "mgr01:9402", "/api/v1/scrub", method="POST", body={}, auth=True)
    assert seen["headers"] == {"X-Dtfs-Shared-Secret": "S3CRET"}
    assert seen["url"] == "http://mgr01:9402/api/v1/scrub"
    assert "S3CRET" not in r["__error__"]
    m.rest_call(cluster(), "mgr01:9402", "/api/v1/nodes")
    assert seen["headers"] == {}


# ---- findings ------------------------------------------------------------

def raft(term=5, leader="1", is_leader=False, lag=0, **kw):
    return {"status": 200, "data": {"current_term": term, "current_leader": leader,
                                    "is_leader": is_leader, "applied_log_lag": lag,
                                    "running_state": "ok", "state": "Follower", **kw}}


def codes(fs):
    return {f["code"] for f in fs}


def test_findings_healthy_is_empty():
    fs = m.compute_findings({"a": raft(is_leader=True), "b": raft()}, status={"status": "ok"},
                            stats={"chunk_placement": {"missing": 0, "undergoal": 0}},
                            nodes=[{"hostname": "n", "role": "Data", "health": "Healthy",
                                    "version": "1", "last_seen_ms": 1000}], now_ms=2000)
    assert fs == []


def test_findings_unreachable_and_leader_split_and_term():
    fs = m.compute_findings({"a": raft(leader="1", term=5), "b": raft(leader="2", term=6),
                             "c": {"__error__": "c: ConnectError"}})
    assert {"manager_unreachable", "leader_disagreement", "term_split"} <= codes(fs)
    assert fs[0]["severity"] == "critical"


def test_findings_no_leader_and_lag():
    fs = m.compute_findings({"a": raft(leader=None, lag=5000)})
    assert {"no_leader", "applied_lag"} <= codes(fs)


def test_findings_cluster_data():
    nodes = [
        {"hostname": "d1", "role": "Data", "health": "Down", "version": "1.0", "last_seen_ms": 0},
        {"hostname": "d2", "role": "Data", "health": "Healthy", "version": "1.1",
         "last_seen_ms": 1_000_000, "disks": [{"mount_path": "/mnt/a", "status": "Failed"}]},
        {"hostname": "d3", "role": "Data", "health": "Healthy", "version": "1.1",
         "last_seen_ms": 1, "quarantine": {"reason": "x"}},
    ]
    fs = m.compute_findings(
        {"a": raft(is_leader=True)},
        status={"files_missing_chunks": 2, "anchor_pending": 1, "status": "ok"},
        stats={"chunk_placement": {"missing": 1, "undergoal": 4, "misplaced": 2}},
        nodes=nodes, memory={"pressure_level": "Warn"},
        faults=[{"action": "scrub.deep.corrupt", "target": "c1"}],
        migrations={"tasks": [{"status": "failed", "last_error": "boom"}]},
        now_ms=1_000_000)
    assert {"files_missing_chunks", "chunks_missing", "chunks_undergoal", "node_down",
            "node_quarantine", "disk_bad", "version_skew", "stale_last_seen",
            "memory_pressure", "integrity_faults", "migrations_failed",
            "anchor_pending"} <= codes(fs)


def test_findings_tolerate_404_inputs():
    err = {"__error__": "HTTP 404", "status": 404}
    fs = m.compute_findings({"a": raft(is_leader=True)}, memory=err, stats=err, nodes=err,
                            faults=err, migrations=err, status=err)
    assert fs == []


def test_findings_replication_lag():
    fs = m.compute_findings({"a": raft(is_leader=True, replication=[{"node_id": "2", "lag": 900}])})
    assert "replication_lag" in codes(fs)


def test_overview_unreachable_managers(monkeypatch):
    def fake(cl, addr, path, params=None, method="GET", body=None, auth=False):
        return {"__error__": f"{addr}: ConnectError", "addr": addr}

    monkeypatch.setattr(m, "rest_call", fake)
    out = m.cluster_overview_impl(cluster())
    c = codes(out["findings"])
    assert "no_reachable_manager" in c and "manager_unreachable" in c


# ---- audit merge / prometheus --------------------------------------------

def test_merge_audit_dedups_and_sorts():
    e1 = {"timestamp": 10, "actor": "a", "action": "x", "target": "t", "summary": "s"}
    e2 = {"timestamp": 20, "actor": "a", "action": "y", "target": "t", "summary": "s"}
    per = {"m1": {"data": [e2, e1]}, "m2": {"data": [e1]}, "m3": {"__error__": "down"}}
    out = m.merge_audit(per, 10)
    assert [e["timestamp"] for e in out] == [20, 10]
    assert m.merge_audit(per, 10, since_ms=15_000) == [out[0]]
    assert len(m.merge_audit(per, 1)) == 1


def test_parse_prometheus():
    text = ('# HELP x y\n# TYPE x counter\ndtfs_files 12\n'
            'dtfs_node_up{node="a b",dc="LELI"} 1\ndtfs_ts 3.5 1700000000\nbad line here\n')
    out = m.parse_prometheus(text)
    assert out["dtfs_files"] == 12.0
    assert out['dtfs_node_up{node="a b",dc="LELI"}'] == 1.0
    assert out["dtfs_ts"] == 3.5 and len(out) == 3
    assert list(m.parse_prometheus(text, "node_up")) == ['dtfs_node_up{node="a b",dc="LELI"}']


# ---- config / setup ------------------------------------------------------

def test_missing_config_message(tmp_path, monkeypatch):
    monkeypatch.setenv("DTFS_MCP_CONFIG", str(tmp_path / "nope.toml"))
    with pytest.raises(m.SetupError) as e:
        m.load_config()
    assert "clusters.example.toml" in str(e.value)


def test_config_secret_env(tmp_path, monkeypatch):
    p = tmp_path / "c.toml"
    p.write_text('default_cluster="prod"\n[clusters.prod]\nmanagers=["a:1"]\nshared_secret_env="X_SEC"\n')
    monkeypatch.setenv("X_SEC", "fromenv")
    monkeypatch.setenv("DTFS_MCP_CONFIG", str(p))
    cl = m.get_cluster()
    assert cl.secret == "fromenv" and cl.ssh_user == "root"


def test_tool_returns_setup_message_when_unconfigured(tmp_path, monkeypatch):
    monkeypatch.setenv("DTFS_MCP_CONFIG", str(tmp_path / "nope.toml"))
    mcp = m.build_server()
    res = asyncio.run(mcp.call_tool("cluster_overview", {}))
    assert "setup_required" in json.dumps(res, default=str)


def test_tool_registry():
    mcp = m.build_server()
    names = {t.name for t in asyncio.run(mcp.list_tools())}
    assert names == {"cluster_overview", "rest_get", "audit_merged", "metrics", "host_logs",
                     "host_status", "host_disks", "host_smart", "host_config", "host_chunk",
                     "host_net", "op_scrub", "op_maintenance", "op_unquarantine",
                     "files_list", "file_lookup", "chunk_detail", "volume_detail", "node_detail",
                     "migrations_status", "integrity_faults", "memory_status", "raft_replication",
                     "config_diff", "host_disk_ids", "host_quarantine", "op_flush_cache",
                     "op_migrate", "op_decommission"}


# ---- extended read tools ---------------------------------------------------

def fake_rest(routes, calls=None):
    def fake(cl, addr, path, params=None, method="GET", body=None, auth=False):
        if calls is not None:
            calls.append((addr, path, params, method, body))
        if path == "/api/v1/raft/state":
            return {"status": 200, "data": {"is_leader": addr == "mgr01:9402"}, "addr": addr}
        r = routes.get(path)
        if r is None:
            return {"__error__": f"{addr}: HTTP 404", "status": 404, "data": {"error": "nf"}}
        return {"status": 200, "data": r, "addr": addr}
    return fake


NODES = [
    {"node_id": "1", "hostname": "leli-d", "health": "Healthy", "location": "eu/LELI/r/leli-d",
     "disks": [{"disk_id": 0, "status": "active"}]},
    {"node_id": "2", "hostname": "toro-d", "health": "Down", "location": "na/TORO/r/toro-d",
     "disks": [{"disk_id": 0, "status": "active"}]},
]
INSPECT = {"path": "/a/b.mkv", "volume_id": "default", "total_size": 9, "chunk_count": 2, "chunks": [
    {"chunk_index": 0, "chunk_id": "aa", "size": 4, "replicas": [
        {"node_id": "1", "disk_id": 0, "tier": "cold"}, {"node_id": "2", "disk_id": 0, "tier": "warm"}]},
    {"chunk_index": 1, "chunk_id": "bb", "size": 5, "replicas": [
        {"node_id": "2", "disk_id": 0, "tier": "warm"}]},
]}


def test_file_lookup_anchor_and_liveness(monkeypatch):
    monkeypatch.setattr(m, "rest_call", fake_rest({
        "/api/v1/files/inspect": INSPECT, "/api/v1/nodes": NODES,
        "/api/v1/volumes/default": {"anchor": {"datacenter": "leli", "required_tier": "cold"}}}))
    out = m.file_lookup_impl(cluster(), "/a/b.mkv")
    assert out["problem_chunk_count"] == 1
    bad = out["problem_chunks"][0]
    assert bad["chunk_id"] == "bb" and bad["problem"] == "no live replica"
    c0 = out["chunks"][0]
    assert c0["live_replicas"] == 1 and c0["replicas"][0]["anchor"] and not c0["replicas"][1]["live"]


def test_file_lookup_not_found_and_validation(monkeypatch):
    monkeypatch.setattr(m, "rest_call", fake_rest({}))
    out = m.file_lookup_impl(cluster(), "/missing")
    assert out["error"].endswith("404") and "note" in out and out["server_error"] == "nf"
    for bad in ("rel/path", "/a\nb", "/" + "x" * 2000):
        with pytest.raises(m.ValidationError):
            m.file_lookup_impl(cluster(), bad)


def test_prefix_validation():
    for bad in ("", "/", "//", "x"):
        with pytest.raises(m.ValidationError):
            m.validate_prefix(bad)
    assert m.validate_prefix("", allow_empty=True) == ""
    assert m.validate_prefix("/tv/") == "/tv/"


def test_summaries():
    mg = m.summarize_migrations({"tasks": [{"status": "failed", "last_error": "e"},
                                           {"status": "pending"}, {"status": "pending"}]})
    assert mg["by_status"] == {"failed": 1, "pending": 2} and mg["failed"][0]["last_error"] == "e"
    r = m.summarize_raft({"a": {"data": {"is_leader": True, "current_term": 3, "voters": [1],
                                         "replication": [{"node_id": "2", "lag": 4}], "junk": 1}},
                          "b": {"__error__": "down"}})
    assert "junk" not in r["a"] and r["a"]["replication"][0]["lag"] == 4 and r["b"]["error"] == "down"


def test_config_diff_redacts_and_filters():
    eff = {"node_hostname": "mgr01", "entries": [
        {"name": "manager.cluster.shared_secret", "current": "hunter2", "default": "", "is_default": False},
        {"name": "manager.alerts.webhooks", "current": [{"url": "https://hook/x"}], "default": [],
         "is_default": False},
        {"name": "manager.scrub.interval", "current": 5, "default": 3, "is_default": False},
        {"name": "manager.x", "current": 1, "default": 1, "is_default": True}]}
    out = m.config_diff_impl(eff)
    assert out["total_non_default"] == 3
    dumped = json.dumps(out)
    assert "hunter2" not in dumped and "hook/x" not in dumped and '"current": 5' in dumped


def test_disk_ids_command_and_check():
    cmd = m.cmd_disk_ids(["/mnt/hdd0", "/mnt/ssd0"])
    assert "cat /mnt/hdd0/.dtfs-disk-id" in cmd and "/mnt/ssd0/.dtfs-disk-id" in cmd
    with pytest.raises(m.ValidationError):
        m.cmd_disk_ids(["/mnt/x; id"])
    secs = {"disk /mnt/hdd0": "0", "disk /mnt/ssd0": "0", "disk /mnt/n": "cat: x: No such file or directory"}
    rows = m.check_disk_ids(["/mnt/hdd0", "/mnt/ssd0", "/mnt/n"], secs)
    assert [r["ok"] for r in rows] == [True, False, True] and rows[1]["persisted_id"] == 0


def test_quarantine_command():
    cmd = m.cmd_quarantine(["/mnt/hdd0"])
    assert "ls -la --time-style=long-iso /mnt/hdd0/.dtfs/quarantine" in cmd and "head -n 300" in cmd
    with pytest.raises(m.ValidationError):
        m.cmd_quarantine(["/mnt/../etc"])


def test_host_disk_ids_tool_end_to_end(monkeypatch):
    def fake(cl, host, cmd, timeout=30):
        if cmd.startswith("cat /etc/dtfs"):
            return {"ok": True, "stdout": '[[disks]]\npath = "/mnt/hdd0"\n[[disks]]\npath = "/mnt/b"\n'}
        return {"ok": True, "stdout": "### disk /mnt/hdd0\n0\n### disk /mnt/b\n0\n"}
    monkeypatch.setattr(m, "run_ssh", fake)
    monkeypatch.setattr(m, "get_cluster", lambda c=None, cfg=None: cluster())
    mcp = m.build_server()
    res = asyncio.run(mcp.call_tool("host_disk_ids", {"host": "toro-plex"}))
    text = json.dumps(res, default=str)
    assert '\\"ok\\": false' in text or '"ok": false' in text


def test_ops_new_dry_run_preview_and_validation(monkeypatch):
    calls = []
    monkeypatch.setattr(m, "rest_call", fake_rest({
        "/api/v1/files": {"total": 7, "files": [{"path": "/tv/a"}]},
        "/api/v1/nodes/9": {"node_id": "9", "used_capacity": 5}}, calls))
    cl = cluster()
    r = m.op_flush_cache_impl(cl, "/tv/", None, 60, False)
    assert r["dry_run"] and r["preview"]["matching_files"] == 7
    assert r["would_send"]["path"] == "/api/v1/files/flush_cache"
    assert r["would_send"]["body"] == {"path_prefix": "/tv/", "cache_tiers": ["warm", "hot"],
                                       "cooldown_secs": 60}
    r = m.op_migrate_impl(cl, "/tv/", "cold", "LELI", 100, False)
    assert r["dry_run"] and r["would_send"]["body"]["target_dc"] == "LELI"
    r = m.op_decommission_impl(cl, "9", "ops1", "", False)
    assert r["dry_run"] and r["preview"]["node"]["used_capacity"] == 5
    assert not [c for c in calls if c[3] == "POST"]
    for fn in (lambda: m.op_flush_cache_impl(cl, "/", None, None, True),
               lambda: m.op_flush_cache_impl(cl, "/tv/", ["cold"], None, True),
               lambda: m.op_flush_cache_impl(cl, "/tv/", [], None, True),
               lambda: m.op_migrate_impl(cl, "", "cold", None, 10, True),
               lambda: m.op_migrate_impl(cl, "/tv/", "ice", None, 10, True),
               lambda: m.op_migrate_impl(cl, "/tv/", "cold", "a b", 10, True),
               lambda: m.op_migrate_impl(cl, "/tv/", "cold", None, 0, True),
               lambda: m.op_decommission_impl(cl, "1;x", "ops1", "", True),
               lambda: m.op_decommission_impl(cl, "1", "ops1", "x" * 501, True)):
        with pytest.raises(m.ValidationError):
            fn()


def test_new_ops_confirm_posts(monkeypatch):
    calls = []
    monkeypatch.setattr(m, "rest_call", fake_rest({"/api/v1/migrations": {"queued": 1}}, calls))
    r = m.op_migrate_impl(cluster(), "/tv/", "warm", None, 5, True)
    assert r["executed"]
    post = [c for c in calls if c[3] == "POST"][0]
    assert post[1] == "/api/v1/migrations" and post[4]["target_tier"] == "warm"
