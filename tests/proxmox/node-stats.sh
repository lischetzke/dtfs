#!/usr/bin/env bash
# node-stats.sh — quick "top of htop" snapshot for a DTFS lab/prod node.
#
# Prints per-core CPU %, memory usage, swap usage (the three htop header
# gauges), then the state of any ENABLED DTFS services on this host
# (dtfs-manager, dtfs-data, and every dtfs-fuse@<mount> instance).
#
# One-shot, read-only, no args. Run locally on a node, over ssh, or via the
# `!` prompt. Dependency-light: bash + awk + /proc + systemctl (no htop,
# mpstat, or python needed).
#
#   tests/proxmox/node-stats.sh
#   ssh root@<node> 'bash -s' < tests/proxmox/node-stats.sh
set -u

# ── per-core CPU % (sample /proc/stat twice over a short window) ──────────
declare -A BUSY0 TOTAL0
while read -r cpu user nice sys idle iowait irq softirq steal _; do
    case "$cpu" in cpu[0-9]*) ;; *) continue ;; esac   # per-core only, skip the "cpu" total
    busy=$((user + nice + sys + irq + softirq + steal))
    BUSY0[$cpu]=$busy
    TOTAL0[$cpu]=$((busy + idle + iowait))
done < /proc/stat
sleep 0.5

echo "== CPU per core =="
while read -r cpu user nice sys idle iowait irq softirq steal _; do
    case "$cpu" in cpu[0-9]*) ;; *) continue ;; esac
    busy=$((user + nice + sys + irq + softirq + steal))
    total=$((busy + idle + iowait))
    db=$((busy - ${BUSY0[$cpu]:-0}))
    dt=$((total - ${TOTAL0[$cpu]:-0}))
    awk -v c="$cpu" -v db="$db" -v dt="$dt" 'BEGIN{
        p = (dt > 0) ? 100.0 * db / dt : 0.0
        bars = int(p / 5)               # 20-wide bar, one block per 5%
        s = ""; for (i = 0; i < bars; i++) s = s "|"
        printf "  %-6s %5.1f%%  [%-20s]\n", c, p, s
    }'
done < /proc/stat

# ── memory + swap (the other two htop gauges) ────────────────────────────
echo "== Memory / Swap =="
awk '
    /^MemTotal:/      { mt = $2 }
    /^MemAvailable:/  { ma = $2 }
    /^SwapTotal:/     { st = $2 }
    /^SwapFree:/      { sf = $2 }
    END {
        mu = mt - ma
        printf "  Mem :  %6.2f / %6.2f GiB  (%.0f%%)\n", mu/1048576, mt/1048576, (mt>0)?100*mu/mt:0
        if (st > 0) {
            su = st - sf
            printf "  Swap:  %6.2f / %6.2f GiB  (%.0f%%)\n", su/1048576, st/1048576, 100*su/st
        } else {
            printf "  Swap:  none configured\n"
        }
    }' /proc/meminfo

# ── DTFS service state (only units that are ENABLED) ─────────────────────
echo "== DTFS services (enabled only) =="
if ! command -v systemctl >/dev/null 2>&1; then
    echo "  (systemctl not available)"
    exit 0
fi

print_unit() {   # print one row if the unit is enabled
    local u="$1"
    systemctl is-enabled --quiet "$u" 2>/dev/null || return 1
    printf "  %-26s %-9s (%s)  since %s\n" \
        "$u" \
        "$(systemctl is-active "$u" 2>/dev/null)" \
        "$(systemctl show -p SubState --value "$u" 2>/dev/null)" \
        "$(systemctl show -p ActiveEnterTimestamp --value "$u" 2>/dev/null)"
    return 0
}

any=0
print_unit dtfs-manager && any=1
print_unit dtfs-data && any=1

# dtfs-fuse is templated (dtfs-fuse@<mount>.service) — enumerate every
# instance, whether currently mounted (list-units) or just enabled but
# inactive (list-unit-files), dedup, and print the enabled ones.
fuse_units=$(
    {
        systemctl list-units      --all --no-legend 'dtfs-fuse@*.service' 2>/dev/null | awk '{print $1}'
        systemctl list-unit-files --no-legend       'dtfs-fuse@*.service' 2>/dev/null | awk '{print $1}'
    } | sort -u
)
for u in $fuse_units; do
    inst="${u#dtfs-fuse@}"; inst="${inst%.service}"
    [ -z "$inst" ] && continue   # skip the bare template dtfs-fuse@.service
    print_unit "$u" && any=1
done

[ "$any" -eq 0 ] && echo "  (none enabled on this host)"
exit 0
