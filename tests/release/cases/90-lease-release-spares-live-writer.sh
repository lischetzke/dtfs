#!/usr/bin/env bash
# CD: ReleaseLeasesByHost (the FUSE startup/shutdown orphan-lease cleanup) must
# NOT drop a LIVE concurrent writer's lease on the same host. Pre-CD it dropped
# every host lease except the caller's own pid, so a second live FUSE process
# (or a duplicate / crash-looping mount unit) lost its in-flight lease whenever
# another FUSE process on the host (re)started -> the live writer's next commit
# hit WriteLeaseConflict -> EIO. Post-CD the cleanup reaps only EXPIRED leases,
# and a live client renews at half-life so it is never expired.
#
# A faithful e2e needs TWO independent FUSE pids on ONE host (the bug is
# host-scoped, pid-discriminated). The default minicluster runs a single FUSE
# unit per client, which cannot stage two concurrent pids, so this case SKIPs
# there. Set CLIENT2 + MOUNT2 to a second mount on the SAME host (different
# mountpoint, own unit) to run the real scenario. The pure-layer proof is the
# unit test state_machine::...::bug_cd_release_by_host_spares_live_peer.
source "$(dirname "$0")/../lib/common.sh"

CLIENT2=${CLIENT2:-}
MOUNT2=${MOUNT2:-}
FUSE_UNIT2=${FUSE_UNIT2:-dtfs-fuse@-mnt-dtfs2.service}

[ -n "$CLIENT2" ] && [ -n "$MOUNT2" ] || \
    skip "no second co-located FUSE mount configured (set CLIENT2+MOUNT2); single-FUSE topology cannot stage two concurrent pids on one host"

# Both mounts must live on the SAME host for the host-scoped cleanup to apply.
[ "$CLIENT2" = "$CLIENT" ] || \
    skip "CLIENT2 ($CLIENT2) must be the same host as CLIENT ($CLIENT) — the cleanup is host-scoped"

A=""; B=""
case_cleanup() { rm_fuse_file "$A"; client_run "sudo rm -f '$B'" >/dev/null 2>&1 || true; }
trap case_cleanup EXIT

fuse2_active() { ssh_run "$CLIENT" "systemctl is-active $FUSE_UNIT2" 2>/dev/null | grep -q '^active'; }

echo "[1/5] preconditions: two FUSE mounts active on $CLIENT"
fuse2_active || skip "second FUSE unit $FUSE_UNIT2 not active on $CLIENT"

echo "[2/5] start a live write on the SECOND mount (holds a lease, host=$CLIENT)"
B="$MOUNT2/release/90-cd-live-$$.bin"
# Long-running streaming write keeps the lease held + renewing for the window.
ssh_run "$CLIENT" "sudo sh -c 'dd if=/dev/zero of=\"$B\" bs=1M count=2048 oflag=direct status=none' >/dev/null 2>&1 &"
sleep 3

echo "[3/5] (re)start the FIRST mount's FUSE -> runs ReleaseLeasesByHost(host=$CLIENT, except_pid=NEWPID)"
ssh_run "$CLIENT" "sudo systemctl restart ${FUSE_UNIT:-dtfs-fuse@-mnt-dtfs.service}"
wait_for 40 "first mount back" bash -c "ssh ${SSH_OPTS[*]} debian@$CLIENT 'systemctl is-active ${FUSE_UNIT:-dtfs-fuse@-mnt-dtfs.service}' | grep -q active" || true

echo "[4/5] the live write on the second mount must still complete (its lease survived)"
if wait_for_soft 60 bash -c "ssh ${SSH_OPTS[*]} debian@$CLIENT 'test -f \"$B\" && ! pgrep -f \"of=$B\" >/dev/null'"; then
    echo "  live write finished"
else
    fail "live write on second mount did not finish — its lease was likely dropped by the first mount's cleanup (CD regression)"
fi

echo "[5/5] the live file is readable + non-empty (write was not corrupted by a lost lease)"
SZ=$(ssh_run "$CLIENT" "stat -c %s '$B' 2>/dev/null" | tr -d '\r')
assert_ge "${SZ:-0}" $((2048*1024*1024 - 1)) "live file size"
pass
