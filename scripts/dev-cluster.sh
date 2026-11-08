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
  down              remove all dtfs-dev-* containers, network and volumes

Topology: mgr01/02/03 (leli/toro/nrb), data01 leli cold, data02 toro warm,
data03 nrb warm. Network dtfs-dev-net 172.31.250.0/24. REST published on host
ports 29402 (mgr01), 29412 (mgr02), 29422 (mgr03). No TLS; fixed dev secret.
Env: DTFS_DEV_TARGET_VOLUME (cargo target volume, default dtfs-target).
DTFS_DEV_ASYNC_COMMIT=<secs> writes [write] async_commit = true with that
async_commit_secs into the client configs (written by up; up --no-build to switch).
No systemd, journald or sshd in the containers: MCP host_* tools and Ansible
do not apply.
EOF
}

drun() { docker run --rm "$@"; }

build() {
  log "building release binaries (target volume $TARGET_VOL)"
  drun -v "$HOST_ROOT:/build" -v "$TARGET_VOL:/target" -v dtfs-registry:/usr/local/cargo/registry \
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
[monitoring]
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
EOF
}

fuse_cfg() { # fuse_cfg <host> <allow_other>
  cat <<EOF
[client]
manager_addr = "mgr01:9401"
manager_addrs = ["mgr02:9401", "mgr03:9401"]
shared_secret = "$SECRET"
[location]
region = "dev"
datacenter = "leli"
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
  copy_bins; write_configs; start_all; wait_healthy
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
  docker run -d --name $name --hostname $1 --network $NET $args -e RUST_LOG=info \
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
mv /mnt/nfs/r10 /mnt/nfs/r10b; [ -e /mnt/dtfs/r10b ] && [ ! -e /mnt/dtfs/r10 ]; ck $? "rename"
echo 3 > /proc/sys/vm/drop_caches
cmp /tmp/r10 /mnt/nfs/r10b; ck $? "cmp via NFS after cache drop"
cmp /tmp/r10 /mnt/dtfs/r10b; ck $? "cmp via FUSE after cache drop"
rm /mnt/nfs/r10b /mnt/nfs/small; [ ! -e /mnt/dtfs/r10b ] && [ ! -e /mnt/dtfs/small ]; ck $? "rm"
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
  down) cmd_down;;
  -h|--help|help|"") usage;;
  *) usage >&2; exit 2;;
esac
