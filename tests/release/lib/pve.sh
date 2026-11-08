# tests/release/lib/pve.sh — hypervisor chaos wrappers for the release suite.
# Thin shims over $PVE_CHAOS_PS1 (PVE REST verbs; lab tooling, not in the repo) invoked through
# pwsh; the heavy/tested PVE logic stays in pve.ps1. Sourced by chaos cases.
# Depends on common.sh (fail, skip) + health.sh (wait_cluster_ready,
# wait_all_healthy). All verbs no-op-SKIP a case unless ALLOW_PVE_CHAOS=1.

_PVE_LIBDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PVE_CHAOS_PS1=${PVE_CHAOS_PS1:-}
ANSIBLE_DIR=${ANSIBLE_DIR:-}
INV3DC=${INV3DC:-$ANSIBLE_DIR/inventories/3dc/hosts.yml}

# Gate: destructive hypervisor ops require an explicit opt-in + a token.
pve_guard()   { [ "${ALLOW_PVE_CHAOS:-0}" = "1" ] && [ -n "${PVE_TOKEN:-}" ]; }
require_pve() { pve_guard || skip "PVE chaos disabled (set ALLOW_PVE_CHAOS=1 and PVE_TOKEN)"; }

_pvechaos() {
    command -v pwsh >/dev/null 2>&1 || fail "pwsh not found (needed for PVE chaos)"
    [ -f "$PVE_CHAOS_PS1" ] || fail "pve-chaos.ps1 not found at $PVE_CHAOS_PS1"
    PVE_TOKEN="${PVE_TOKEN:-}" pwsh -NoProfile -File "$PVE_CHAOS_PS1" "$@"
}

# ─── Power a whole DC / single VM off and on ─────────────────────
power_dc_off()   { _pvechaos -Action power-off -Dc "$1" -Force; }
power_dc_on()    { _pvechaos -Action power-on  -Dc "$1"; wait_cluster_ready 180; wait_all_healthy 180; }
power_host_off() { _pvechaos -Action power-off -Vm "$1" -Force; }
power_host_on()  { _pvechaos -Action power-on  -Vm "$1"; }

# ─── Disk yank + blank install ───────────────────────────────────
# detach_disk simulates a dead disk; attach_blank_disk installs a fresh one
# and runs mount-data-disks so the data node formats+mounts+rejoins it.
detach_disk() { _pvechaos -Action detach-disk -Vm "$1" -Slot "$2"; }
attach_blank_disk() {
    _pvechaos -Action attach-disk -Vm "$1" -Slot "$2" -SizeGb "${3:-16}"
    mount_data_disks_on "$1"
}

# Format + mount any new blank disk on a host and restart dtfs-data (reuses
# the deploy-time playbook). Requires ansible on the runner.
mount_data_disks_on() {
    command -v ansible-playbook >/dev/null 2>&1 || fail "ansible-playbook not found (needed to mount a new disk)"
    ansible-playbook -i "$INV3DC" "$ANSIBLE_DIR/playbooks/mount-data-disks.yml" --limit "$1"
}
