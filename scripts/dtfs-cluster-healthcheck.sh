#!/usr/bin/env bash
# DTFS cluster health check — read-only, runs from any host that can reach
# the manager REST API. Used by the GitLab pipeline's verify stage and as
# the final play of deploy/ansible/update-cluster.yml.
#
# Passes when, within --timeout seconds, ALL of the following hold:
#   * every listed manager answers /api/v1/raft/state with
#     running_state == "ok" and agrees on one current_leader;
#   * /api/v1/cluster/status (asked on the leader) reports status == "ok",
#     data_nodes_healthy == data_nodes, files_missing_chunks == 0;
#   * with --expect-version: every node in /api/v1/nodes that reports a
#     version reports exactly that version;
#   * with --min-data-nodes: at least that many data nodes are registered.
#
# Usage:
#   scripts/dtfs-cluster-healthcheck.sh --managers mgr01:9402,mgr02:9402,mgr03:9402 \
#       [--expect-version v0.2.0] [--min-data-nodes 3] [--timeout 300] [--interval 10]
#
# Managers may be given as host:port or as full http(s)://host:port URLs.
# Exit codes: 0 healthy, 1 unhealthy after timeout, 3 bad arguments.

set -uo pipefail

MANAGERS=""
EXPECT_VERSION=""
MIN_DATA_NODES=0
TIMEOUT=300
INTERVAL=10

while [ $# -gt 0 ]; do
    case "$1" in
        --managers) shift; MANAGERS="${1:-}" ;;
        --expect-version) shift; EXPECT_VERSION="${1:-}" ;;
        --min-data-nodes) shift; MIN_DATA_NODES="${1:-0}" ;;
        --timeout) shift; TIMEOUT="${1:-300}" ;;
        --interval) shift; INTERVAL="${1:-10}" ;;
        --help|-h) sed -n '2,22p' "$0"; exit 0 ;;
        *) echo "error: unknown argument: $1" >&2; exit 3 ;;
    esac
    shift
done

[ -n "$MANAGERS" ] || { echo "error: --managers is required" >&2; exit 3; }
for tool in curl jq; do
    command -v "$tool" >/dev/null 2>&1 || { echo "error: $tool not found" >&2; exit 3; }
done

url_of() {
    case "$1" in
        http://*|https://*) printf '%s' "${1%/}" ;;
        *) printf 'http://%s' "$1" ;;
    esac
}

IFS=',' read -r -a MGR_LIST <<< "$MANAGERS"

# One pass; prints the first failing reason to stdout and returns 1.
check_once() {
    local leader="" leader_url="" problems=()
    for m in "${MGR_LIST[@]}"; do
        local base; base="$(url_of "$m")"
        local rs
        if ! rs="$(curl -fsS --max-time 8 "$base/api/v1/raft/state" 2>/dev/null)"; then
            problems+=("$m: raft/state unreachable")
            continue
        fi
        local running state cur
        running="$(jq -r '.running_state // "?"' <<< "$rs")"
        cur="$(jq -r '.current_leader // empty' <<< "$rs")"
        state="$(jq -r '.state // "?"' <<< "$rs")"
        if [ "$running" != "ok" ]; then
            problems+=("$m: running_state=$running")
        fi
        if [ -z "$cur" ] || [ "$cur" = "null" ]; then
            problems+=("$m: no current_leader (state=$state)")
        else
            if [ -z "$leader" ]; then
                leader="$cur"
            elif [ "$leader" != "$cur" ]; then
                problems+=("$m: leader disagreement ($cur vs $leader)")
            fi
            if [ "$(jq -r '.is_leader' <<< "$rs")" = "true" ]; then
                leader_url="$base"
            fi
        fi
    done
    if [ ${#problems[@]} -gt 0 ]; then
        printf '%s\n' "${problems[@]}"
        return 1
    fi
    if [ -z "$leader_url" ]; then
        echo "no listed manager is the leader ($leader); list every voter"
        return 1
    fi

    local cs
    if ! cs="$(curl -fsS --max-time 8 "$leader_url/api/v1/cluster/status" 2>/dev/null)"; then
        echo "leader $leader_url: cluster/status unreachable"
        return 1
    fi
    local status dn dnh missing
    status="$(jq -r '.status // "?"' <<< "$cs")"
    dn="$(jq -r '.data_nodes // 0' <<< "$cs")"
    dnh="$(jq -r '.data_nodes_healthy // 0' <<< "$cs")"
    missing="$(jq -r '.files_missing_chunks // 0' <<< "$cs")"
    if [ "$status" != "ok" ]; then
        echo "cluster status=$status (data_nodes=$dn healthy=$dnh files_missing_chunks=$missing)"
        return 1
    fi
    if [ "$dnh" != "$dn" ]; then
        echo "data nodes healthy $dnh/$dn"
        return 1
    fi
    if [ "$missing" != "0" ]; then
        echo "files_missing_chunks=$missing"
        return 1
    fi
    if [ "$dn" -lt "$MIN_DATA_NODES" ]; then
        echo "only $dn data nodes registered (need >= $MIN_DATA_NODES)"
        return 1
    fi

    if [ -n "$EXPECT_VERSION" ]; then
        local nodes
        if ! nodes="$(curl -fsS --max-time 8 "$leader_url/api/v1/nodes" 2>/dev/null)"; then
            echo "leader $leader_url: nodes unreachable"
            return 1
        fi
        local stale
        stale="$(jq -r --arg v "$EXPECT_VERSION" '
            (if type == "array" then . else (.nodes // []) end)
            | map(select((.version // "") != "" and .version != $v))
            | map("\(.hostname // .node_id)=\(.version)") | join(", ")' <<< "$nodes")"
        if [ -n "$stale" ]; then
            echo "nodes not on $EXPECT_VERSION: $stale"
            return 1
        fi
    fi
    return 0
}

deadline=$(( $(date +%s) + TIMEOUT ))
attempt=0
while :; do
    attempt=$((attempt + 1))
    if out="$(check_once)"; then
        echo "healthy: leader agreed, status ok, all data nodes healthy${EXPECT_VERSION:+, all nodes on $EXPECT_VERSION}"
        exit 0
    fi
    now=$(date +%s)
    if [ "$now" -ge "$deadline" ]; then
        echo "UNHEALTHY after ${TIMEOUT}s (${attempt} attempts):"
        printf '%s\n' "$out" | sed 's/^/  /'
        exit 1
    fi
    echo "attempt $attempt not healthy yet: $(printf '%s' "$out" | tr '\n' ';'); retrying in ${INTERVAL}s"
    sleep "$INTERVAL"
done
