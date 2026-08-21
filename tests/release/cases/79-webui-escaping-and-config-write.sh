#!/usr/bin/env bash
# REV-4-1 / REV-4-3 — the WebUI must not execute names that came from the
# filesystem, and a config write that would not parse must never reach disk.
#
# REV-4-1: file paths, node hostnames, audit targets, quarantine reasons and
# migration task fields were interpolated raw into innerHTML with no escaping
# and no CSP. Anything that can write to the FUSE mount chooses a filename, so
# a filename was a script-injection vector into the operator's browser - a
# session that holds unauthenticated control of the whole REST API.
#
# REV-4-3: CO-4-3 already merges and validates the sparse override, but the
# rendered TOML was never re-parsed before replacing manager.toml, the write
# was not fsynced, and the temp file leaked on error paths.
source "$(dirname "$0")/../lib/common.sh"

HOSTILE=""
case_cleanup() {
    [ -n "$HOSTILE" ] && client_run "sudo rm -f \"\$(printf '%s' '$HOSTILE')\"" >/dev/null 2>&1
    return 0
}
trap case_cleanup EXIT

LEADER=""
for m in $(mgrs_list); do
    if curl "${CURL_OPTS[@]}" "http://$m:$MGR_PORT/api/v1/raft/state" 2>/dev/null \
        | grep -q '"is_leader":[[:space:]]*true'; then
        LEADER="$m"; break
    fi
done
[ -n "$LEADER" ] || fail "no raft leader reachable"

echo "[1/4] the WebUI ships a restrictive CSP and nosniff"
HEADERS=$(curl "${CURL_OPTS[@]}" -D - -o /dev/null "http://$LEADER:$MGR_PORT/ui" 2>/dev/null)
echo "$HEADERS" | grep -qi 'content-security-policy' \
    || fail "no Content-Security-Policy header on /ui"
echo "$HEADERS" | grep -qi 'x-content-type-options: *nosniff' \
    || fail "no X-Content-Type-Options: nosniff on /ui"
CSP=$(echo "$HEADERS" | grep -i 'content-security-policy' | head -1)
echo "$CSP" | grep -qi "object-src 'none'" || fail "CSP does not block plugin content: $CSP"
# The policy must not name a remote origin: inline handlers force
# script-src 'unsafe-inline', so "no remote origin" is what makes it useful.
echo "$CSP" | grep -qiE 'https?://' && fail "CSP names a remote origin: $CSP"

echo "[2/4] create a file whose NAME is a script payload"
HOSTILE="$MOUNT/release/79-<img src=x onerror=alert(1)>.bin"
client_run "sudo mkdir -p $MOUNT/release" >/dev/null 2>&1 || true
if ! client_run "sudo dd if=/dev/urandom of=\"\$(printf '%s' '$HOSTILE')\" bs=1K count=8 status=none && sudo sync" >/dev/null 2>&1; then
    skip "the mount rejected a filename containing < > - nothing to escape"
fi
sleep 3

echo "[3/4] the served page carries no raw payload and defines both escapers"
PAGE=$(curl "${CURL_OPTS[@]}" "http://$LEADER:$MGR_PORT/ui" 2>/dev/null)
echo "$PAGE" | grep -q '<img src=x onerror=' \
    && fail "the raw payload is present in the served page"
echo "$PAGE" | grep -q 'function esc(' || fail "page script has no esc() helper"
# A single escaper is not sufficient for values inside inline onclick args.
echo "$PAGE" | grep -q 'function escArg(' || fail "page script has no escArg() helper"

echo "[4/4] a config write that would not parse is refused, file untouched"
BEFORE=$(ssh_run "$LEADER" "sudo sha256sum /etc/dtfs/manager.toml 2>/dev/null | awk '{print \$1}'" | tr -d '\r')
[ -n "$BEFORE" ] || skip "no /etc/dtfs/manager.toml on $LEADER"
CODE=$(curl "${CURL_OPTS[@]}" -o /dev/null -w '%{http_code}' \
    -X POST -H 'Content-Type: application/json' \
    -d '{"config_json":"{\"raft\":{\"heartbeat_interval_ms\":\"not-a-number\"}}","write_to_disk":true}' \
    "http://$LEADER:$MGR_PORT/api/v1/config" 2>/dev/null)
case "$CODE" in
    400|422) echo "  refused with HTTP $CODE" ;;
    *) fail "expected a 4xx refusal for an invalid write_to_disk body, got $CODE" ;;
esac
AFTER=$(ssh_run "$LEADER" "sudo sha256sum /etc/dtfs/manager.toml 2>/dev/null | awk '{print \$1}'" | tr -d '\r')
assert_eq "$BEFORE" "$AFTER" "manager.toml unchanged after a refused write"

# And no temp file was left beside it.
LEFT=$(ssh_run "$LEADER" "sudo ls /etc/dtfs/ 2>/dev/null | grep -c 'manager.toml\..*tmp'" | tr -d '\r')
[ "${LEFT:-0}" -eq 0 ] || fail "a temp config file leaked beside manager.toml"
pass
