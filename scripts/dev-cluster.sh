#!/usr/bin/env bash
# Local Docker dev cluster: 3 managers + 3 data nodes (+ optional FUSE/NFS
# clients) for runtime checks. Only needs docker and bash. See --help.
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash on Windows must not rewrite /paths

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOST_ROOT="$ROOT"; if pwd -W >/dev/null 2>&1; then HOST_ROOT="$(cd "$ROOT" && pwd -W)"; fi
IMG="${DTFS_BUILDENV_IMAGE:-dtfs-buildenv:latest}"
TARGET_VOL="${DTFS_DEV_TARGET_VOLUME:-dtfs-target}"
NET=dtfs-dev-net; SUBNET=172.31.250.0/24
BINVOL=dtfs-dev-bin; ETCVOL=dtfs-dev-etc
SECRET="dev-cluster-secret-not-for-production"
BINS="dtfs-manager dtfs-data dtfs-fuse dtfs-cli"
MGRS="mgr01 mgr02 mgr03"; DATAS="data01 data02 data03"
mgr_dc()  { case $1 in mgr01) echo leli;; mgr02) echo toro;; *) echo nrb;; esac; }
mgr_ip()  { echo "172.31.250.1${1#mgr0}"; }
mgr_port(){ case $1 in mgr01) echo 29402;; mgr02) echo 29412;; *) echo 29422;; esac; }
data_dc() { case $1 in data01) echo leli;; data02) echo toro;; *) echo nrb;; esac; }
data_tier(){ [ "$1" = data01 ] && echo cold || echo warm; }
data_ip() { echo "172.31.250.2${1#data0}"; }

die()  { echo "error: $*" >&2; exit 1; }
log()  { echo "[dev-cluster] $*"; }
pass() { echo "PASS: $*"; }
fail() { echo "FAIL: $*"; }

usage() { cat <<'EOF'
Usage: scripts/dev-cluster.sh <command>

  up [--no-build]   build, copy binaries, write configs, start 3 managers and
                    3 data nodes, wait for 3 healthy data nodes
  deploy            rebuild, copy binaries, restart all daemons, wait healthy
  status            container states and a cluster summary from the REST API
  fuse [cmd...]     start (or reuse) the FUSE client container; with args, run
                    them inside it: fuse sh -c 'echo hi > /mnt/dtfs/x'
  nfs-check         NFS re-export of a FUSE mount: PASS/FAIL per step
  empty-files       empty/sparse files (FUSE + NFS create paths) must not make
                    cluster/status degraded on any manager; mtime not 1970
  big-file <GiB>    large-file check through FUSE (write, read back, in-DTFS
                    copy; MB/s per phase, peak RSS). Needs free disk of about
                    size x replicas x 2
  precache-check    pre-cache runtime check (volume media, Plex policy, toro FUSE
                    client, series E01 played): PASS/FAIL per criterion. Needs a
                    cluster built with DTFS_DEV_VERSION=v0.1.16 (else 409)
  cache-check       cache retention/eviction/scanner check (~45 min): PASS/FAIL per
                    criterion. Needs DTFS_DEV_VERSION=v0.1.16 DTFS_DEV_CACHE=1 up
  down              remove all dtfs-dev-* containers, network and volumes

Topology: mgr01/02/03 (leli/toro/nrb), data01 leli cold, data02 toro warm,
data03 nrb warm. Network dtfs-dev-net 172.31.250.0/24. REST published on host
ports 29402 (mgr01), 29412 (mgr02), 29422 (mgr03). No TLS; fixed dev secret.
Env: DTFS_DEV_TARGET_VOLUME (cargo target volume, default dtfs-target).
DTFS_DEV_ASYNC_COMMIT=<secs> writes [write] async_commit = true with that
async_commit_secs into the client configs (written by up; up --no-build to switch).
DTFS_DEV_VERSION=<tag> stamps DTFS_VERSION into the build (v0.1.16 opens the
pre-caching version gate; unset = v0.0.0-dev).
DTFS_DEV_CACHE=1 (with up) sets 20 s tiering evaluation and 60/50 cache watermarks,
makes data02's /mnt/d0 a 512 MiB tmpfs volume (placement min_free_bytes 1 MiB: the 1 GiB
default headroom would exclude it) and writes nfs-toro.toml/fuse-bulk.toml.
tmpfs content is lost when data02 restarts (deploy), so cache mode needs a fresh
down + up.
DTFS_DEV_RUST_LOG (default info) sets RUST_LOG for the FUSE/NFS client containers.
No systemd, journald or sshd in the containers: MCP host_* tools and Ansible
do not apply.
EOF
}

drun() { docker run --rm "$@"; }

build() {
  log "building release binaries (target volume $TARGET_VOL)"
  drun -e DTFS_VERSION="${DTFS_DEV_VERSION:-}" -v "$HOST_ROOT:/build" -v "$TARGET_VOL:/target" -v dtfs-registry:/usr/local/cargo/registry \
    -w /build "$IMG" cargo build --release -p dtfs-manager -p dtfs-data -p dtfs-fuse -p dtfs-cli 2>&1 | tail -5
  [ "${PIPESTATUS[0]}" = 0 ] || die "build failed"
}

copy_bins() {
  log "copying binaries to volume $BINVOL"
  # cp to tmp + mv so running daemons keep their old inode
  drun -v "$TARGET_VOL:/target:ro" -v "$BINVOL:/opt/dtfs" "$IMG" sh -c \
    "set -e; for b in $BINS; do cp /target/release/\$b /opt/dtfs/\$b.new && mv /opt/dtfs/\$b.new /opt/dtfs/\$b; done"
}

put_cfg() { # put_cfg <file>  (content on stdin)
  docker run --rm -i -v "$ETCVOL:/etc/dtfs" "$IMG" sh -c "cat > /etc/dtfs/$1"
}

mgr_cfg() {
  local n=$1 boot=false; [ "$n" = mgr01 ] && boot=true
  cat <<EOF
[node]
id = "auto"
hostname = "$n"
data_dir = "/var/lib/dtfs"
[location]
region = "dev"
datacenter = "$(mgr_dc $n)"
rack = "r1"
host = "$n"
[cluster]
name = "dtfs-dev"
shared_secret = "$SECRET"
bootstrap = $boot
EOF
  for m in $MGRS; do printf '[[cluster.initial_voters]]\nhostname = "%s"\naddr = "%s:9401"\n' "$m" "$m"; done
  cat <<EOF
[raft]
[grpc]
listen_addr = "0.0.0.0:9401"
[api]
listen_addr = "0.0.0.0:9402"
[tls]
mode = "disabled"
[defaults]
anchor_datacenter = "leli"
[tiering]
precache_max_mbps = 200
$([ "${DTFS_DEV_CACHE:-}" = 1 ] && printf 'evaluation_interval_secs = 20\ncache_high_watermark_pct = 60\ncache_low_watermark_pct = 50\n')
[monitoring]
$([ "${DTFS_DEV_CACHE:-}" = 1 ] && printf '[placement]
min_free_bytes = 1048576
')
EOF
}

data_cfg() {
  local n=$1
  cat <<EOF
[node]
hostname = "$n"
data_dir = "/var/lib/dtfs"
[location]
region = "dev"
datacenter = "$(data_dc $n)"
rack = "r1"
host = "$n"
[cluster]
manager_addr = "mgr01:9401"
manager_addrs = ["mgr02:9401", "mgr03:9401"]
shared_secret = "$SECRET"
[grpc]
listen_addr = "$(data_ip $n):9500"
[tls]
mode = "disabled"
[[disks]]
path = "/mnt/d0"
tier = "$(data_tier $n)"
[scrub]
$([ "${DTFS_DEV_CACHE:-}" = 1 ] && printf '[placement]
min_free_bytes = 1048576
')
EOF
}

fuse_cfg() { # fuse_cfg <host> <allow_other> [dc] [volume_id] [chunk_size_mb]
  cat <<EOF
[client]
manager_addr = "mgr01:9401"
manager_addrs = ["mgr02:9401", "mgr03:9401"]
shared_secret = "$SECRET"
$([ -n "${4:-}" ] && echo "volume_id = \"$4\"")
$([ -n "${5:-}" ] && echo "chunk_size_mb = $5")
[location]
region = "dev"
datacenter = "${3:-leli}"
rack = "r1"
host = "$1"
[mount]
mountpoint = "/mnt/dtfs"
allow_other = $2
[readahead]
[cache]
[partition_safety]
EOF
  if [ -n "${DTFS_DEV_ASYNC_COMMIT:-}" ]; then
    printf '[write]\nasync_commit = true\nasync_commit_secs = %s\n' "$DTFS_DEV_ASYNC_COMMIT"
  fi
}

write_configs() {
  log "writing configs to volume $ETCVOL"
  for n in $MGRS; do mgr_cfg $n | put_cfg $n.toml; done
  for n in $DATAS; do data_cfg $n | put_cfg $n.toml; done
  fuse_cfg fuse01_cln false | put_cfg fuse.toml
  fuse_cfg nfs01_cln true | put_cfg nfs.toml
  fuse_cfg fuse02_cln false toro media | put_cfg fuse-toro.toml
  if [ "${DTFS_DEV_CACHE:-}" = 1 ]; then
    fuse_cfg nfs02_cln true toro wan | put_cfg nfs-toro.toml
    fuse_cfg fuse03_cln false leli bulk 1 | put_cfg fuse-bulk.toml
  fi
}

# cache mode: data02's disk is a 512 MiB tmpfs so the watermarks are reachable
ensure_cache_vol() {
  [ "${DTFS_DEV_CACHE:-}" = 1 ] || return 0
  docker volume inspect dtfs-dev-data02-d0 >/dev/null 2>&1 && return 0
  docker volume create --driver local --opt type=tmpfs --opt device=tmpfs --opt o=size=512m dtfs-dev-data02-d0 >/dev/null
}

ensure_net() {
  docker network inspect $NET >/dev/null 2>&1 || docker network create --subnet $SUBNET $NET >/dev/null
}

exists() { docker inspect "$1" >/dev/null 2>&1; }
running() { [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = true ]; }

start_daemon() { # name ip bin [extra docker args...]
  local n=$1 ip=$2 bin=$3; shift 3
  exists dtfs-dev-$n && docker rm -f dtfs-dev-$n >/dev/null
  docker run -d --name dtfs-dev-$n --hostname $n --network $NET --ip $ip --network-alias $n \
    -e RUST_LOG=info -v $BINVOL:/opt/dtfs:ro -v $ETCVOL:/etc/dtfs:ro "$@" \
    "$IMG" /opt/dtfs/$bin --config /etc/dtfs/$n.toml >/dev/null
}

start_all() {
  ensure_net
  for n in $MGRS; do
    start_daemon $n $(mgr_ip $n) dtfs-manager -p $(mgr_port $n):9402 -v dtfs-dev-$n-data:/var/lib/dtfs
  done
  for n in $DATAS; do
    start_daemon $n $(data_ip $n) dtfs-data -v dtfs-dev-$n-data:/var/lib/dtfs -v dtfs-dev-$n-d0:/mnt/d0
  done
}

summary() { curl -s --max-time 3 "http://127.0.0.1:29402/api/v1/cluster/status" 2>/dev/null | tr -d ' \n' || true; }

wait_healthy() {
  log "waiting for 3 healthy data nodes (max 180s)"
  local i s
  for i in $(seq 1 90); do
    s="$(summary)"
    case "$s" in *'"data_nodes_healthy":3'*) log "cluster healthy: $s"; return 0;; esac
    sleep 2
  done
  echo "timeout; last status: ${s:-none}" >&2
  for n in $MGRS $DATAS; do echo "--- dtfs-dev-$n"; docker logs --tail 15 dtfs-dev-$n 2>&1 || true; done
  die "cluster did not become healthy"
}

cmd_up() {
  local nb=0; [ "${1:-}" = --no-build ] && nb=1
  docker image inspect "$IMG" >/dev/null 2>&1 || die "image $IMG missing (see CLAUDE.md A.4)"
  if [ $nb = 0 ]; then build; fi
  copy_bins; write_configs; ensure_cache_vol; start_all; wait_healthy
  log "REST: mgr01 http://127.0.0.1:29402 mgr02 :29412 mgr03 :29422"
}

cmd_deploy() {
  build; copy_bins
  for n in $MGRS $DATAS; do exists dtfs-dev-$n || die "cluster not up (run: up)"; done
  for n in fuse nfs; do exists dtfs-dev-$n && docker rm -f dtfs-dev-$n >/dev/null; done
  docker restart $(for n in $MGRS $DATAS; do echo dtfs-dev-$n; done) >/dev/null
  wait_healthy
}

cmd_status() {
  docker ps -a --filter name=dtfs-dev- --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
  local s; s="$(summary)"; echo "cluster: ${s:-REST not reachable on 29402}"
}

# start a client container: name cfg privileged(0/1)
start_client() {
  local name=dtfs-dev-$1 cfg=$2 priv=$3 args
  running $name && return 0
  exists $name && docker rm -f $name >/dev/null
  running dtfs-dev-mgr01 || die "cluster not up (run: up)"
  if [ "$priv" = 1 ]; then args="--privileged"
  else args="--device /dev/fuse --cap-add SYS_ADMIN --security-opt apparmor:unconfined"; fi
  # shellcheck disable=SC2086
  docker run -d --name $name --hostname $1 --network $NET $args -e RUST_LOG="${DTFS_DEV_RUST_LOG:-info}" \
    -v $BINVOL:/opt/dtfs:ro -v $ETCVOL:/etc/dtfs:ro "$IMG" sh -c \
    "mkdir -p /mnt/dtfs && exec /opt/dtfs/dtfs-fuse --config /etc/dtfs/$cfg" >/dev/null
  local i
  for i in $(seq 1 30); do
    docker exec $name grep -q ' /mnt/dtfs fuse' /proc/mounts 2>/dev/null && return 0
    running $name || break
    sleep 1
  done
  docker logs --tail 20 $name 2>&1 || true
  die "FUSE mount did not appear"
}

cmd_fuse() {
  start_client fuse fuse.toml 0
  if [ $# -gt 0 ]; then docker exec dtfs-dev-fuse "$@"; else log "FUSE mounted at /mnt/dtfs in dtfs-dev-fuse"; fi
}

NFS_STEPS='
set -u; rc=0
ck() { if [ "$1" = 0 ]; then echo "PASS: $2"; else echo "FAIL: $2"; rc=1; fi; }
mount -t nfsd nfsd /proc/fs/nfsd 2>/dev/null; rpcbind; sleep 1
echo "/mnt/dtfs *(rw,sync,no_subtree_check,no_root_squash,fsid=4242)" > /etc/exports
exportfs -ra && rpc.mountd && rpc.nfsd 4; ck $? "nfs server up"
mkdir -p /mnt/nfs
mount -t nfs -o vers=3,nolock,actimeo=0 127.0.0.1:/mnt/dtfs /mnt/nfs; ck $? "nfs mount"
echo hello > /mnt/nfs/small; [ "$(cat /mnt/dtfs/small)" = hello ]; ck $? "small file visible via FUSE"
head -c 10485760 /dev/urandom > /tmp/r10; cp /tmp/r10 /mnt/nfs/r10
cmp /tmp/r10 /mnt/nfs/r10; ck $? "10MB cmp via NFS"
cmp /tmp/r10 /mnt/dtfs/r10; ck $? "10MB cmp via FUSE"
cp /tmp/r10 /mnt/nfs/n1; cmp /tmp/r10 /mnt/dtfs/n1; ck $? "cmp via FUSE right after NFS cp"
echo 3 > /proc/sys/vm/drop_caches; cmp /tmp/r10 /mnt/dtfs/n1; ck $? "cmp via FUSE after cache drop, before the commit timer"
rm /mnt/nfs/n1; [ ! -e /mnt/dtfs/n1 ]; ck $? "rm n1"
mv /mnt/nfs/r10 /mnt/nfs/r10b; [ -e /mnt/dtfs/r10b ] && [ ! -e /mnt/dtfs/r10 ]; ck $? "rename"
echo 3 > /proc/sys/vm/drop_caches
cmp /tmp/r10 /mnt/nfs/r10b; ck $? "cmp via NFS after cache drop"
cmp /tmp/r10 /mnt/dtfs/r10b; ck $? "cmp via FUSE after cache drop"
rm /mnt/nfs/r10b /mnt/nfs/small; [ ! -e /mnt/dtfs/r10b ] && [ ! -e /mnt/dtfs/small ]; ck $? "rm"
# knfsd holds the FUSE mount while exported: a later docker rm -f leaves a zombie.
umount /mnt/nfs; exportfs -ua; exportfs -f; ck $? "unexport"
exit $rc
'

cmd_nfs_check() {
  exists dtfs-dev-nfs && docker rm -f dtfs-dev-nfs >/dev/null
  start_client nfs nfs.toml 1
  log "installing nfs packages in dtfs-dev-nfs"
  docker exec dtfs-dev-nfs sh -c 'apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq nfs-kernel-server nfs-common >/dev/null' \
    || { fail "apt install nfs packages"; exit 1; }
  docker exec dtfs-dev-nfs sh -c "$NFS_STEPS"
}

EMPTY_MAKE='
set -u; D=/mnt/dtfs/$1; rc=0
ck() { if [ "$1" = 0 ]; then echo "PASS: $2"; else echo "FAIL: $2"; rc=1; fi; }
mount -t nfsd nfsd /proc/fs/nfsd 2>/dev/null; rpcbind; sleep 1
echo "/mnt/dtfs *(rw,sync,no_subtree_check,no_root_squash,fsid=4242)" > /etc/exports
exportfs -ra && rpc.mountd && rpc.nfsd 4; ck $? "nfs server up"
mkdir -p /mnt/nfs
mount -t nfs -o vers=3,nolock,actimeo=0 127.0.0.1:/mnt/dtfs /mnt/nfs; ck $? "nfs mount"
mkdir "$D"; ck $? "mkdir $D"
cd "$D" || exit 1
touch a; ck $? "FUSE touch a"
: > b; ck $? "FUSE : > b (O_TRUNC create)"
echo x > c; : > c; ck $? "FUSE write then truncate c"
truncate -s 10M d; ck $? "FUSE truncate -s 10M d (sparse)"
python3 -c "import os,stat; os.mknod(\"e\", 0o644|stat.S_IFREG)"; ck $? "FUSE mknod e"
N=/mnt/nfs/$1
touch $N/f; ck $? "NFS touch f"
: > $N/g; ck $? "NFS : > g"
touch $N/h; : > $N/h; ck $? "NFS touch + truncate h"
exit $rc
'

EMPTY_FINISH='
set -u; D=/mnt/dtfs/$1; rc=0
ck() { if [ "$1" = 0 ]; then echo "PASS: $2"; else echo "FAIL: $2"; rc=1; fi; }
for f in a e; do
  m=$(stat -c %Y "$D/$f" 2>/dev/null || echo 0)
  [ "$m" -gt 1600000000 ]; ck $? "mtime of $f is not 1970 ($m)"
done
cd /
rm -rf "$D"; ck $? "cleanup files"
exportfs -u "*:/mnt/dtfs"; exportfs -f
umount /mnt/nfs; ck $? "umount nfs"
fusermount3 -u /mnt/dtfs; ck $? "fusermount3 -u /mnt/dtfs"
exit $rc
'

# status "ok" and files_missing_chunks 0 on every manager; sets rc=1 on FAIL
empty_status_check() { # empty_status_check <label>
  local m s
  for m in mgr01 mgr02 mgr03; do
    s="$(curl -s --max-time 5 "http://127.0.0.1:$(mgr_port $m)/api/v1/cluster/status" | tr -d ' \n')"
    case "$s" in
      *'"status":"ok"'*'"files_missing_chunks":0'*|*'"files_missing_chunks":0'*'"status":"ok"'*) pass "$1: $m status ok, files_missing_chunks 0";;
      *) fail "$1: $m status not ok or files_missing_chunks != 0 (${s:-no REST reply})"; rc=1;;
    esac
  done
}

cmd_empty_files() {
  local rc=0 dir; dir="empty-check-$(date +%s)"
  empty_status_check "baseline (pre-existing cluster state, not caused by this check)"
  [ $rc = 0 ] || exit 1
  exists dtfs-dev-nfs && docker rm -f dtfs-dev-nfs >/dev/null
  start_client nfs nfs.toml 1
  log "installing nfs and python3 packages in dtfs-dev-nfs"
  docker exec dtfs-dev-nfs sh -c 'apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq nfs-kernel-server nfs-common python3-minimal >/dev/null' \
    || { fail "apt install packages"; exit 1; }
  docker exec dtfs-dev-nfs bash -c "$EMPTY_MAKE" _ "$dir" || rc=1
  sleep 5
  empty_status_check "after empty files"
  docker exec dtfs-dev-nfs bash -c "$EMPTY_FINISH" _ "$dir" || rc=1
  [ $rc = 0 ] && log "empty-files: all PASS" || log "empty-files: FAILED"
  exit $rc
}

BIG_STEPS='
set -u; N=$1; B=/mnt/dtfs/big-$$
t() { date +%s.%N; }
mbs() { awk -v n="$N" -v a="$1" -v b="$2" "BEGIN{printf \"%.1f MB/s\", n*1024/(b-a)}"; }
rss() { grep VmHWM /proc/$(pidof dtfs-fuse | cut -d" " -f1)/status; }
s=$(t); head -c "${N}G" /dev/urandom | tee >(sha256sum | cut -d" " -f1 > /tmp/h1) | dd of=$B bs=1M conv=fsync 2>/dev/null; e=$(t)
echo "write: $(mbs $s $e)"; sleep 1
s=$(t); h2=$(sha256sum < $B | cut -d" " -f1); e=$(t); echo "read:  $(mbs $s $e)"
[ "$h2" = "$(cat /tmp/h1)" ] && echo "PASS: read-back hash matches" || { echo "FAIL: read-back hash mismatch"; rc=1; }
s=$(t); cp $B $B.cp; e=$(t); echo "copy:  $(mbs $s $e)"
h3=$(sha256sum < $B.cp | cut -d" " -f1)
[ "$h3" = "$h2" ] && echo "PASS: in-DTFS copy hash matches" || { echo "FAIL: copy hash mismatch"; rc=1; }
echo "dtfs-fuse peak $(rss | tr -s " ")"
rm -f $B $B.cp
exit ${rc:-0}
'

cmd_big_file() {
  local g="${1:-}"; case "$g" in ''|*[!0-9]*) die "usage: big-file <GiB> (integer)";; esac
  log "WARNING: needs about $((g * 3 * 2)) GiB free disk for $g GiB (size x 3 replicas worst case x 2 for the copy)"
  start_client fuse fuse.toml 0
  docker exec dtfs-dev-fuse bash -c "$BIG_STEPS" _ "$g"
}

PRECACHE_PY='
import json, os, subprocess, sys, time, urllib.request, urllib.error
M = "/mnt/dtfs"; MIB = 1 << 20; rc = 0
def ck(ok, msg):
    global rc
    print(("PASS: " if ok else "FAIL: ") + msg, flush=True)
    if not ok: rc = 1
def http(base, method, path, body=None):
    req = urllib.request.Request(base + path, method=method, data=None if body is None else json.dumps(body).encode(),
                                 headers={"content-type": "application/json", "X-Dtfs-Shared-Secret": os.environ["DTFS_SECRET"]})
    try:
        with urllib.request.urlopen(req, timeout=30) as r: return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try: return e.code, json.loads(e.read() or b"{}")
        except ValueError: return e.code, {}
leader = None
for m in ("mgr01", "mgr02", "mgr03"):
    base = "http://%s:9402" % m
    try:
        if http(base, "GET", "/api/v1/tiering/precache")[0] == 200: leader = base; break
    except OSError: pass
if not leader: print("FAIL: no leader answered /api/v1/tiering/precache"); sys.exit(1)
print("leader", leader)
def post(path, body):
    st, r = http(leader, "POST", path, body)
    if st == 409 and "v0.1.16" in json.dumps(r): print("FAIL: %s -> 409 %s (rebuild with DTFS_DEV_VERSION=v0.1.16)" % (path, r)); sys.exit(1)
    if st != 200: print("FAIL: %s -> %s %s" % (path, st, r)); sys.exit(1)
    return r
if sys.argv[1] == "write":
  if http(leader, "GET", "/api/v1/volumes/media")[0] != 200:
      post("/api/v1/volumes", {"name": "media", "chunk_size_mb": 4, "anchor_datacenter": "leli"})
  post("/api/v1/volumes/media/set-mandatory", {"hot_replicas": 0, "warm_replicas": 0, "cold_replicas": 1})
  post("/api/v1/volumes/media/set-anchor", {"datacenter": "leli", "required_tier": "cold"})
  post("/api/v1/volumes/media/set-cache", {"warm_min": 1, "warm_max": 1, "per_dc": True, "promote_window_secs": 600})
  post("/api/v1/volumes/media/set-precache", {"mode": "plex", "series_max_gb": 1, "max_series": 1, "next_files": 1})
  ck(http(leader, "GET", "/api/v1/volumes/media")[1].get("precache", {}).get("mode") == "plex", "volume media precache.mode == plex")
def mk(path, mib):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        for _ in range(mib // 4): f.write(os.urandom(4 * MIB))
def tree():
    for d in ("Show", "Other"): os.makedirs("%s/tv/%s" % (M, d), exist_ok=True); open("%s/tv/%s/.plexmatch" % (M, d), "w").write("title: %s\n" % d)
    for i in range(1, 6): mk("%s/tv/Show/Season 01/E%02d.mkv" % (M, i), 300)
    for i in (1, 2): mk("%s/tv/Other/Season 01/E%02d.mkv" % (M, i), 100)
    for n in "ABC": mk("%s/movies/%s.mkv" % (M, n), 64)
if sys.argv[1] == "write": tree(); sys.exit(0)
def play(path, skip, count=5):
    subprocess.run(["dd", "if=" + M + path, "of=/dev/null", "bs=4M", "skip=%d" % skip, "count=%d" % count], check=True, stderr=subprocess.DEVNULL)
def status(): return http(leader, "GET", "/api/v1/tiering/precache")[1]
hosts = {n["node_id"]: n["hostname"] for n in http(leader, "GET", "/api/v1/nodes")[1] if n.get("role", "").lower() == "data"}
def rows(path):
    st, r = http(leader, "GET", "/api/v1/files/inspect?path=" + urllib.request.quote(path))
    if st != 200: return None
    return [{hosts.get(x["node_id"], x["node_id"]) for x in c["replicas"]} for c in r["chunks"]]
def frac(path, host):
    r = rows(path)
    return (sum(host in c for c in r), len(r)) if r else (0, 0)
show = "/tv/Show/Season 01/E%02d.mkv"
play(show % 1, 20)
print("played E01", flush=True)
end = time.time() + 480; ok = False
while time.time() < end:
    f2, f3 = frac(show % 2, "data02"), frac(show % 3, "data02")
    st = status(); print("  %s E02 %d/%d E03 %d/%d inflight %s series %s cursors %s" % ((time.strftime("%H:%M:%S"),) + f2 + f3 + (st.get("inflight_tasks"), [g.get("root") for g in st.get("series", [])], [g.get("cursor") for g in st.get("cursors", [])])), flush=True)
    if f2[1] and f2[0] == f2[1] and f3[1] and f3[0] == f3[1]: ok = True; break
    time.sleep(10)
ck(ok, "E02 and E03: every chunk has a data02 (toro warm) row (%s %s)" % (frac(show % 2, "data02"), frac(show % 3, "data02")))
st = status(); print(json.dumps({k: st.get(k) for k in ("is_leader", "rate_mbps", "inflight_tasks", "series")})[:1500])
ser = [g for g in st.get("series", []) if g["volume"] == "media"]
ck(len(ser) == 1 and ser[0].get("root") == "/tv/Show", "series root is /tv/Show (%s)" % [g.get("root") for g in ser])
ck(ser and ser[0]["cursor"] == show % 1 and show % 1 not in [f["path"] for f in ser[0]["files"]], "E01 (cursor) is not in series[0].files")
ck(ser and {show % 2, show % 3} <= {f["path"] for f in ser[0]["files"]}, "E02 and E03 are in series[0].files")
play("/movies/A.mkv", 0, 1)
time.sleep(120)
st = status()
ck(not [g for g in st.get("series", []) + st.get("cursors", []) if "/movies" in (g.get("root") or g.get("dir") or "")], "header-only read of /movies/A.mkv: no /movies cursor")
play("/tv/Other/Season 01/E01.mkv", 14, 5)
end = time.time() + 240; ok = False
while time.time() < end:
    st = status(); roots = [g.get("root") for g in st.get("series", []) if g["volume"] == "media"]
    if roots == ["/tv/Other"]: ok = True; break
    time.sleep(10)
ck(ok, "LRU: second series replaces first under max-series 1 (series roots %s)" % roots)
paths = [show % i for i in range(1, 6)] + ["/tv/Other/Season 01/E%02d.mkv" % i for i in (1, 2)] + ["/movies/%s.mkv" % n for n in "ABC"]
bad = [p for p in paths if not rows(p) or not all("data01" in c for c in rows(p))]
ck(not bad, "every media file still has a data01 anchor row on every chunk (bad: %s)" % bad)
sys.exit(rc)
'

cmd_precache_check() {
  exists dtfs-dev-fuse-toro && docker rm -f dtfs-dev-fuse-toro >/dev/null
  start_client fuse-toro fuse-toro.toml 0
  docker exec -i -e DTFS_SECRET="$SECRET" dtfs-dev-fuse-toro python3 -c "$PRECACHE_PY" write || { fail "writing the Plex tree"; exit 1; }
  # fresh client: no local cache, so the reads below reach the data nodes and are reported
  docker rm -f dtfs-dev-fuse-toro >/dev/null
  start_client fuse-toro fuse-toro.toml 0
  docker exec -i -e DTFS_SECRET="$SECRET" dtfs-dev-fuse-toro python3 -c "$PRECACHE_PY" check
}

CACHE_PY='
import json, os, sys, time, urllib.request, urllib.error
rc = 0
def ck(ok, msg):
    global rc
    print(("PASS: " if ok else "FAIL: ") + msg, flush=True)
    if not ok: rc = 1
def http(base, method, path, body=None):
    req = urllib.request.Request(base + path, method=method, data=None if body is None else json.dumps(body).encode(),
                                 headers={"content-type": "application/json", "X-Dtfs-Shared-Secret": os.environ["DTFS_SECRET"]})
    try:
        with urllib.request.urlopen(req, timeout=30) as r: return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try: return e.code, json.loads(e.read() or b"{}")
        except ValueError: return e.code, {}
def find_leader():
    for m in ("mgr01", "mgr02", "mgr03"):
        base = "http://%s:9402" % m
        try:
            if http(base, "GET", "/api/v1/tiering/precache")[0] == 200: return base
        except OSError: pass
    return None
def now(): return time.strftime("%H:%M:%S")
cmd = sys.argv[1]; args = sys.argv[2:]
if cmd == "leader":
    l = find_leader(); print(l.split("//")[1].split(":")[0] if l else ""); sys.exit(0 if l else 1)
if cmd == "healthy":
    t0 = time.time()
    while time.time() - t0 < 300:
        l = find_leader()
        if l:
            try: st = http(l, "GET", "/api/v1/cluster/status")[1]
            except OSError: st = {}
            if "\"data_nodes_healthy\":3" in json.dumps(st, separators=(",", ":")):
                print("LEADER=%s ELECT_SECS=%d" % (l.split("//")[1].split(":")[0], time.time() - t0)); sys.exit(0)
        time.sleep(2)
    print("FAIL: no leader with 3 healthy data nodes within 300 s"); sys.exit(1)
leader = find_leader()
if not leader: print("FAIL: no leader answered /api/v1/tiering/precache"); sys.exit(1)
def post(path, body):
    st, r = http(leader, "POST", path, body)
    for _ in range(12):   # the version gate opens once every manager has registered its version after a fresh up
        if st != 409: break
        time.sleep(10); st, r = http(leader, "POST", path, body)
    if st == 409: print("FAIL: %s -> 409 %s (wrong build: rebuild with DTFS_DEV_VERSION=v0.1.16)" % (path, r)); sys.exit(1)
    if st != 200: print("FAIL: %s -> %s %s" % (path, st, r)); sys.exit(1)
if cmd == "setup":
    if http(leader, "GET", "/api/v1/volumes/wan")[0] != 200:
        post("/api/v1/volumes", {"name": "wan", "chunk_size_mb": 4, "anchor_datacenter": "leli"})
    post("/api/v1/volumes/wan/set-mandatory", {"hot_replicas": 0, "warm_replicas": 0, "cold_replicas": 1})
    post("/api/v1/volumes/wan/set-anchor", {"datacenter": "leli", "required_tier": "cold"})
    post("/api/v1/volumes/wan/set-cache", {"warm_min": 1, "warm_max": 1, "per_dc": True, "promote_reads": 1, "promote_window_secs": 60, "retention_secs": 1200})
    if http(leader, "GET", "/api/v1/volumes/bulk")[0] != 200:
        post("/api/v1/volumes", {"name": "bulk", "chunk_size_mb": 1, "anchor_datacenter": "leli"})
    post("/api/v1/volumes/bulk/set-mandatory", {"hot_replicas": 0, "warm_replicas": 0, "cold_replicas": 1})
    post("/api/v1/volumes/bulk/set-anchor", {"datacenter": "leli", "required_tier": "cold"})
    print("volumes wan and bulk configured"); sys.exit(0)
def rows(path):
    hosts = {n["node_id"]: n["hostname"] for n in http(leader, "GET", "/api/v1/nodes")[1] if n.get("role", "").lower() == "data"}
    st, r = http(leader, "GET", "/api/v1/files/inspect?path=" + urllib.request.quote(path))
    if st != 200: return None
    return [{hosts.get(x["node_id"], x["node_id"]) for x in c["replicas"]} for c in r["chunks"]]
def cnt(path):
    r = rows(path) or []
    return sum("data02" in c for c in r[1:]), max(len(r) - 1, 0)
if cmd == "promo":   # promo <path> <timeout_s>: chunks 1..N all on data02
    p = args[0]; end = time.time() + int(args[1]); r = None
    while time.time() < end:
        r = rows(p)
        if r:
            n = sum("data02" in c for c in r[1:]); print("  %s %s promoted %d/%d (chunk0 on data02: %s)" % (now(), p, n, len(r) - 1, "data02" in r[0]), flush=True)
            if n == len(r) - 1 and len(r) > 1: break
        time.sleep(10)
    ok = bool(r) and len(r) > 1 and all("data02" in c for c in r[1:])
    ck(ok, "%s: chunks 1..%d have a data02 row" % (p, len(r) - 1 if r else -1))
    if ok: print("TA=%d" % time.time()); print("INFO: %s chunk 0 (scanner read) promoted: %s" % (p, "data02" in r[0]))
    sys.exit(rc)
if cmd == "none":    # none <paths...>: no data02 row on any chunk
    for p in args:
        r = rows(p); ck(bool(r) and not any("data02" in c for c in r), "%s: no data02 row on any chunk (%s)" % (p, [i for i, c in enumerate(r or []) if "data02" in c]))
    sys.exit(rc)
if cmd == "present": # present <path>: chunks 1..N all still on data02
    p = args[0]; r = rows(p) or []
    ck(len(r) > 1 and all("data02" in c for c in r[1:]), "%s: all promoted chunks still on data02 (%d/%d)" % (p, cnt(p)[0], cnt(p)[1]))
    sys.exit(rc)
if cmd == "anchors": # anchors <paths...>: every chunk has data01
    for p in args:
        r = rows(p) or []; ck(len(r) > 0 and all("data01" in c for c in r), "%s: every chunk has its data01 row (%d chunks)" % (p, len(r)))
    sys.exit(rc)
if cmd == "cnt":
    b, c = cnt(args[0]), cnt(args[1]); print("COUNTS %d %d %d %d" % (b + c)); sys.exit(0)
if cmd == "retention": # retention <path> <ta_epoch>
    p = args[0]; ta = int(args[1]); deadline = ta + 2400; td = None
    while time.time() < deadline:
        r = rows(p) or []
        if len(r) > 2:
            mid = sum("data02" in c for c in r[1:-1]); last = "data02" in r[-1]
            print("  %s +%.1f min: middle chunks on data02 %d/%d, last chunk on data02 %s" % (now(), (time.time() - ta) / 60, mid, len(r) - 2, last), flush=True)
            if mid == 0: td = time.time(); break
        time.sleep(30)
    if td is None: ck(False, "middle chunks of %s never lost their data02 row by tA+40 min" % p); sys.exit(1)
    m = (td - ta) / 60
    ck(20 <= m <= 31, "tD = tA+%.1f min, within [tA+20, tA+31] (retention honoured, node-local path)" % m)
    last = "data02" in (rows(p) or [{"data02"}])[-1]
    if last:
        time.sleep(30); last = "data02" in (rows(p) or [{"data02"}])[-1]
    ck(not last, "last chunk of %s lost its data02 row within one poll of the middle chunks (tA+11 scan did not refresh it)" % p)
    sys.exit(rc)
print("unknown command " + cmd); sys.exit(2)
'

cmd_cache_check() {
  set +e
  local LOG RC=0 TA t T0=$SECONDS
  LOG="$(mktemp)"
  tlog() { echo "[$(date +%H:%M:%S)] $*"; }
  # py <args>: run a CACHE_PY stage in nfs-toro, tee to $LOG, RC=1 on non-zero exit
  py() { docker exec -i -e DTFS_SECRET="$SECRET" dtfs-dev-nfs-toro python3 -c "$CACHE_PY" "$@" | tee -a "$LOG"; [ "${PIPESTATUS[0]}" = 0 ] || RC=1; }
  sleep_until() { local d=$(( $1 - $(date +%s) )); [ $d -gt 0 ] && { tlog "waiting ${d}s until $2"; sleep $d; }; return 0; }
  nfsx() { docker exec dtfs-dev-nfs-toro sh -c "$1"; }

  running dtfs-dev-mgr01 || die "cluster not up (run: DTFS_DEV_VERSION=v0.1.16 DTFS_DEV_CACHE=1 up)"
  docker exec dtfs-dev-data02 df -T /mnt/d0 | grep -q tmpfs || die "data02 /mnt/d0 is not tmpfs: needs DTFS_DEV_CACHE=1 on a fresh down + up"

  tlog "setup: volumes wan and bulk"
  docker run --rm -i --network $NET -e DTFS_SECRET="$SECRET" "$IMG" python3 -c "$CACHE_PY" setup || { fail "volume setup"; exit 1; }

  tlog "starting fuse-bulk and writing bulk.bin (10.5 GiB, urandom: zeros would dedupe to one chunk) in the background"
  exists dtfs-dev-fuse-bulk && docker rm -f dtfs-dev-fuse-bulk >/dev/null
  start_client fuse-bulk fuse-bulk.toml 0
  docker exec dtfs-dev-fuse-bulk dd if=/dev/urandom of=/mnt/dtfs/bulk.bin bs=1M count=10752 iflag=fullblock conv=fsync 2>/dev/null &
  local BULKPID=$!
  sleep 10; kill -0 $BULKPID 2>/dev/null || { wait $BULKPID; fail "bulk.bin write died early (dd exit $?; see docker logs dtfs-dev-fuse-bulk)"; exit 1; }

  # nfs_up: fresh nfs-toro container with the NFS server, export and mount
  nfs_up() {
    exists dtfs-dev-nfs-toro && docker rm -f dtfs-dev-nfs-toro >/dev/null
    start_client nfs-toro nfs-toro.toml 1
    tlog "installing nfs packages in dtfs-dev-nfs-toro"
    docker exec dtfs-dev-nfs-toro sh -c 'apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq nfs-kernel-server nfs-common >/dev/null'       || { fail "apt install nfs packages"; exit 1; }
    nfsx 'mount -t nfsd nfsd /proc/fs/nfsd 2>/dev/null; rpcbind; sleep 1
      echo "/mnt/dtfs *(rw,sync,no_subtree_check,no_root_squash,fsid=4242)" > /etc/exports
      exportfs -ra && rpc.mountd && rpc.nfsd 4 && mkdir -p /mnt/nfs &&
      mount -t nfs -o vers=3,nolock,actimeo=0 127.0.0.1:/mnt/dtfs /mnt/nfs' || { fail "nfs server/mount in nfs-toro"; exit 1; }
  }
  nfs_up
  # knfsd holds the FUSE mount while exported: never leave it exported on an early exit
  trap 'docker exec dtfs-dev-nfs-toro sh -c "umount /mnt/nfs; exportfs -ua; exportfs -f; rpc.nfsd 0" >/dev/null 2>&1' EXIT
  tlog "writing A (64 MiB), B (128 MiB), C (64 MiB) via NFS"
  nfsx 'mkdir -p /mnt/nfs/wan-check && cd /mnt/nfs/wan-check &&
    dd if=/dev/urandom of=A bs=1M count=64 conv=fsync 2>/dev/null && dd if=/dev/urandom of=B bs=1M count=128 conv=fsync 2>/dev/null &&
    dd if=/dev/urandom of=C bs=1M count=64 conv=fsync 2>/dev/null' || { fail "writing A/B/C via NFS"; exit 1; }
  # fresh client: the writes warmed its local cache, so reads below would never reach the data nodes
  tlog "restarting nfs-toro for a cold client cache"
  nfsx 'umount /mnt/nfs; exportfs -ua; exportfs -f; rpc.nfsd 0'
  nfs_up
  local P=/wan-check D=/mnt/nfs/wan-check

  tlog "stage 1: viewer read of A"
  nfsx "echo 3 > /proc/sys/vm/drop_caches; dd if=$D/A of=/dev/null bs=1M 2>/dev/null" || { fail "viewer read of A"; exit 1; }
  py promo $P/A 300
  TA="$(grep '^TA=' "$LOG" | tail -1 | cut -d= -f2)"
  [ -n "$TA" ] || { fail "A never promoted: cannot run the timeline"; exit 1; }
  tlog "tA = $TA ($(date -d @$TA +%H:%M:%S 2>/dev/null))"
  tlog "stage 1: scanner passes over B and C"
  for t in 1 2 3; do
    nfsx "echo 3 > /proc/sys/vm/drop_caches; for f in B C; do head -c 131072 $D/\$f >/dev/null; tail -c 131072 $D/\$f >/dev/null; done"
    sleep 5
  done
  sleep 90
  py none $P/B $P/C

  sleep_until $((TA + 660)) "tA+11 min (scan of A)"
  nfsx "echo 3 > /proc/sys/vm/drop_caches; head -c 131072 $D/A >/dev/null; tail -c 131072 $D/A >/dev/null"
  tlog "scanned A header and tail"

  sleep_until $((TA + 720)) "tA+12 min (bulk read + leader restart)"
  wait $BULKPID || { fail "bulk.bin write (dd exit $?)"; exit 1; }
  tlog "reading bulk.bin via fuse-bulk (10.5k distinct 1 MiB chunks)"
  docker exec dtfs-dev-fuse-bulk dd if=/mnt/dtfs/bulk.bin of=/dev/null bs=1M 2>&1 | tail -1
  [ "${PIPESTATUS[0]}" = 0 ] || { fail "bulk.bin read"; RC=1; }
  local OLD; OLD="$(docker exec -e DTFS_SECRET="$SECRET" dtfs-dev-nfs-toro python3 -c "$CACHE_PY" leader)"
  [ -n "$OLD" ] || { fail "no leader to restart"; exit 1; }
  # stop (not restart): a restarted leader reclaims leadership from its own vote
  tlog "stopping leader $OLD until another manager leads"
  docker stop dtfs-dev-$OLD >/dev/null
  local NEW="" i
  for i in $(seq 1 60); do
    NEW="$(docker exec -e DTFS_SECRET="$SECRET" dtfs-dev-nfs-toro python3 -c "$CACHE_PY" leader 2>/dev/null || true)"
    [ -n "$NEW" ] && [ "$NEW" != "$OLD" ] && break; sleep 2
  done
  docker start dtfs-dev-$OLD >/dev/null
  py healthy
  [ -n "$NEW" ] && [ "$NEW" != "$OLD" ] && pass "leader changed $OLD -> $NEW" || { fail "no other leader after stopping $OLD"; RC=1; }

  sleep_until $((TA + 900)) "tA+15 min"
  py present $P/A

  tlog "stage 3: waiting for retention demotion of A (up to tA+40 min)"
  py retention $P/A "$TA"
  py anchors $P/A

  tlog "stage 4: eviction"
  nfsx "echo 3 > /proc/sys/vm/drop_caches; dd if=$D/B of=/dev/null bs=1M 2>/dev/null"
  py promo $P/B 300
  sleep 60
  nfsx "echo 3 > /proc/sys/vm/drop_caches; dd if=$D/C of=/dev/null bs=1M 2>/dev/null"
  py promo $P/C 300
  set -- $(py cnt $P/B $P/C | grep '^COUNTS' | cut -d' ' -f2-)
  local B0=$1 BT=$2 C0=$3 CT=$4 SZ USED NEED
  tlog "baseline promoted chunks: B $B0/$BT C $C0/$CT"
  read -r SZ USED < <(docker exec dtfs-dev-data02 df -B1 --output=size,used /mnt/d0 | tail -1)
  NEED=$(( SZ * 62 / 100 - USED ))
  tlog "data02 tmpfs size $SZ used $USED; junk bytes to reach 62%: $NEED"
  if [ $NEED -le 0 ]; then fail "usage already above 62% before the junk file"; exit 1; fi
  docker exec dtfs-dev-data02 dd if=/dev/zero of=/mnt/d0/junk bs=1M count=$(( NEED / 1048576 )) 2>/dev/null
  local i B1 C1 PCT BELOW=0 MINPCT=100
  for i in $(seq 1 20); do
    read -r _ USED < <(docker exec dtfs-dev-data02 df -B1 --output=size,used /mnt/d0 | tail -1)
    PCT=$(( USED * 100 / SZ ))
    set -- $(py cnt $P/B $P/C | grep '^COUNTS' | cut -d' ' -f2-); B1=$1; C1=$3
    tlog "usage ${PCT}% ($USED bytes) B $B1/$BT C $C1/$CT"
    [ $PCT -lt 60 ] && { BELOW=1; break; }
    sleep 15
  done
  if [ $BELOW = 1 ]; then pass "usage dropped below 60% (high watermark)"; else fail "usage still ${PCT}% after 5 min"; fi
  if [ $BELOW = 1 ]; then
    sleep 75   # let the eviction round settle
    read -r _ USED < <(docker exec dtfs-dev-data02 df -B1 --output=size,used /mnt/d0 | tail -1)
    PCT=$(( USED * 100 / SZ ))
    set -- $(py cnt $P/B $P/C | grep '^COUNTS' | cut -d' ' -f2-); B1=$1; C1=$3
    tlog "settled: usage ${PCT}% B $B1/$BT C $C1/$CT"
    [ $PCT -ge 45 ] && pass "usage settled at ${PCT}% (>= 45%, no gross over-eviction)" || fail "usage settled at ${PCT}% (< 45%: over-eviction)"
  fi
  [ "$C1" = "$C0" ] && pass "C data02 rows all intact ($C1/$C0)" || fail "C lost data02 rows ($C1/$C0)"
  [ "$B1" -lt "$B0" ] && pass "B lost data02 rows ($B1/$B0): oldest-read evicted first" || fail "B lost no data02 rows ($B1/$B0)"
  py anchors $P/A $P/B $P/C

  tlog "cleanup"
  docker exec dtfs-dev-data02 rm -f /mnt/d0/junk
  # knfsd holds the FUSE mount while exported: a later docker rm -f leaves a zombie.
  nfsx 'umount /mnt/nfs; exportfs -ua; exportfs -f; rpc.nfsd 0'
  grep -c '^FAIL' "$LOG" | grep -qx 0 || RC=1
  tlog "cache-check finished in $(( (SECONDS - T0) / 60 )) min: $(grep -c '^PASS' "$LOG") PASS, $(grep -c '^FAIL' "$LOG") FAIL"
  exit $RC
}

cmd_down() {
  local c; c="$(docker ps -aq --filter name=dtfs-dev-)"
  # shellcheck disable=SC2086
  [ -n "$c" ] && docker rm -f $c >/dev/null
  docker network rm $NET >/dev/null 2>&1 || true
  local v; v="$(docker volume ls -q --filter name=dtfs-dev-)"
  # shellcheck disable=SC2086
  [ -n "$v" ] && docker volume rm $v >/dev/null
  log "removed dtfs-dev-* containers, network and volumes"
}

case "${1:-}" in
  up) shift; cmd_up "$@";;
  deploy) cmd_deploy;;
  status) cmd_status;;
  fuse) shift; cmd_fuse "$@";;
  nfs-check) cmd_nfs_check;;
  empty-files) cmd_empty_files;;
  big-file) shift; cmd_big_file "$@";;
  precache-check) cmd_precache_check;;
  cache-check) cmd_cache_check;;
  down) cmd_down;;
  -h|--help|help|"") usage;;
  *) usage >&2; exit 2;;
esac
