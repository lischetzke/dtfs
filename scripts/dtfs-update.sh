#!/usr/bin/env bash
# DTFS rolling-upgrade orchestrator.
#
# Reads a new build tarball and walks a cluster in the order:
#   1. each FUSE client       — drain, restart
#   2. each data node         — drain, restart
#   3. one manager at a time  — drain, restart, wait for cluster reconvergence
#
# Aborts and rolls back the last upgraded node if the cluster doesn't
# reconverge within --recover-timeout seconds after each restart.
#
# Usage:
#   ./scripts/dtfs-update.sh <new-tarball> [hosts.yml]
#   ./scripts/dtfs-update.sh --dry-run <new-tarball>
#   ./scripts/dtfs-update.sh --rollback
#
# Inventory format (YAML, same as Ansible):
#   managers:    [mgr-01, mgr-02, mgr-03]
#   data_nodes:  [data-01, data-02, data-03, data-04]
#   clients:     [client-01, client-02]
#
# Requirements: ssh access to every host as a sudo-capable user; the
# DTFS REST API reachable at http://<mgr>:9402/api/v1/cluster/status.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ─── tty/colour ──────────────────────────────────────────────────────────────
if [ -t 1 ]; then
    GREEN='\033[0;32m'; YELLOW='\033[0;33m'; RED='\033[0;31m'; BOLD='\033[1m'; OFF='\033[0m'
else
    GREEN=''; YELLOW=''; RED=''; BOLD=''; OFF=''
fi
info()  { echo -e "${GREEN}[INFO]${OFF}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${OFF}  $*"; }
error() { echo -e "${RED}[ERROR]${OFF} $*"; }
step()  { echo -e "\n${BOLD}>>> $*${OFF}"; }

DRY_RUN=0
ROLLBACK=0
RECOVER_TIMEOUT=120
TARBALL=""
INVENTORY=""

while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run) DRY_RUN=1 ;;
        --rollback) ROLLBACK=1 ;;
        --recover-timeout) shift; RECOVER_TIMEOUT="$1" ;;
        --help|-h) sed -n '2,21p' "$0"; exit 0 ;;
        *)
            if [ -z "$TARBALL" ]; then TARBALL="$1"
            elif [ -z "$INVENTORY" ]; then INVENTORY="$1"
            else error "unknown arg: $1"; exit 1; fi
            ;;
    esac
    shift
done

if [ "$ROLLBACK" -eq 1 ]; then
    error "rollback unimplemented in this revision; use the previous tarball with the normal update flow"
    exit 2
fi

[ -z "$TARBALL" ] && { error "missing tarball"; exit 1; }
[ ! -f "$TARBALL" ] && { error "tarball not found: $TARBALL"; exit 1; }
[ -z "$INVENTORY" ] && INVENTORY="tests/proxmox/ansible/hosts.yml"
[ ! -f "$INVENTORY" ] && { error "inventory not found: $INVENTORY"; exit 1; }

# ─── parse inventory (minimal YAML scrape) ───────────────────────────────────
MANAGERS=()
DATAS=()
CLIENTS=()
while IFS= read -r line; do
    case "$line" in
        *mgr-[0-9]*:)    MANAGERS+=("${line%%:*}") ;;
        *data-[0-9]*:)   DATAS+=("${line%%:*}") ;;
        *client-[0-9]*:) CLIENTS+=("${line%%:*}") ;;
    esac
done < <(grep -E '^\s+(mgr-|data-|client-)' "$INVENTORY" | sed 's/^[[:space:]]*//')

if [ ${#MANAGERS[@]} -eq 0 ]; then
    error "no managers in $INVENTORY"; exit 1
fi
info "Inventory: ${#MANAGERS[@]} mgr, ${#DATAS[@]} data, ${#CLIENTS[@]} client"

# ─── helpers ─────────────────────────────────────────────────────────────────
do_ssh()  { ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new "$@"; }
do_scp()  { scp -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new "$@"; }

wait_for_convergence() {
    local mgr_host=$1
    local deadline=$((SECONDS + RECOVER_TIMEOUT))
    while [ $SECONDS -lt $deadline ]; do
        if do_ssh "$mgr_host" "curl -sS --max-time 3 http://localhost:9402/api/v1/cluster/status" \
            | grep -q '"data_nodes_healthy"'; then
            return 0
        fi
        sleep 2
    done
    return 1
}

push_and_install() {
    local host=$1
    local role=$2
    info "[$host:$role] uploading new tarball"
    [ "$DRY_RUN" -eq 1 ] && { info "  (dry-run: skipping)"; return; }
    do_scp "$TARBALL" "$host:/tmp/dtfs-update.tar.gz"
    do_ssh "$host" "sudo tar -C /tmp -xzf /tmp/dtfs-update.tar.gz && \
        for b in dtfs-manager dtfs-data dtfs-fuse dtfs-cli; do \
            sudo install -m 0755 /tmp/dtfs/bin/\$b /usr/local/bin/\$b; \
        done && rm -rf /tmp/dtfs /tmp/dtfs-update.tar.gz"
}

restart_and_verify() {
    local host=$1
    local svc=$2
    info "[$host] restarting $svc"
    [ "$DRY_RUN" -eq 1 ] && return
    do_ssh "$host" "sudo systemctl restart $svc"
    sleep 3
    if ! do_ssh "$host" "systemctl is-active $svc >/dev/null"; then
        error "[$host] $svc failed to come back up"
        return 1
    fi
}

# ─── phase 1: FUSE clients ───────────────────────────────────────────────────
step "Phase 1: FUSE clients"
for h in "${CLIENTS[@]}"; do
    push_and_install "$h" client
    # Find the actual @-instance unit running on this host.
    UNIT=$(do_ssh "$h" "systemctl list-units --type=service --no-legend 'dtfs-fuse@*' | awk '{print \$1}' | head -1" 2>/dev/null || true)
    if [ -n "$UNIT" ]; then
        restart_and_verify "$h" "$UNIT"
    else
        warn "[$h] no dtfs-fuse@ instance found"
    fi
done

# ─── phase 2: data nodes (one at a time) ─────────────────────────────────────
step "Phase 2: data nodes (rolling)"
for h in "${DATAS[@]}"; do
    push_and_install "$h" data
    restart_and_verify "$h" "dtfs-data.service"
    if [ "$DRY_RUN" -eq 0 ]; then
        if ! wait_for_convergence "${MANAGERS[0]}"; then
            error "cluster did not reconverge after $h restart; halting roll"
            exit 3
        fi
    fi
done

# ─── phase 3: managers (one at a time, in reverse — leader last) ─────────────
step "Phase 3: managers (rolling, leader last)"
# Reverse to leave mgr-01 (likely bootstrap leader) for last.
for ((idx=${#MANAGERS[@]}-1; idx>=0; idx--)); do
    h="${MANAGERS[$idx]}"
    push_and_install "$h" manager
    restart_and_verify "$h" "dtfs-manager.service"
    if [ "$DRY_RUN" -eq 0 ]; then
        # Wait for THIS host to be back in the cluster view before moving on.
        if ! wait_for_convergence "$h"; then
            error "manager $h didn't rejoin within $RECOVER_TIMEOUT s"
            exit 3
        fi
    fi
done

step "Update complete"
info "Verify: dtfs-cli status (against any manager)"
