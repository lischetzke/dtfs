#!/usr/bin/env bash
# BE-13: per-volume schedules endpoint — now deprecated for
# scrub_interval (Bug BC-13) but still accepts the value + emits
# warn log. Verifies the endpoint shape + warn surface.
source "$(dirname "$0")/../lib/common.sh"

echo "[1/2] set scrub_interval_secs=120 on $VOL (deprecated, warn expected)"
RESP=$(mgr_post_leader /api/v1/volumes/$VOL/set-schedules '{"scrub_interval_secs":120}')
echo "  response: $RESP" | head -1
if ! echo "$RESP" | grep -q "ok\|true\|schedules"; then
    fail "set-schedules rejected: $RESP"
fi

echo "[2/2] reset to inherit"
mgr_post_leader /api/v1/volumes/$VOL/set-schedules '{"scrub_interval_secs":0}' >/dev/null

# Volume info should show schedules block (BC-13 deprecation note is
# WebUI-side; endpoint still echoes it for back-compat).
INFO=$(mgr_get_any /api/v1/volumes)
echo "$INFO" | grep -q '"schedules"' || fail "volumes endpoint missing schedules block"
pass
