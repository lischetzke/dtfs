#!/usr/bin/env bash
# DC-A client workload: single Plex-rate stream over the DSL uplink.
# Picks a random media file under /mnt/dtfs and streams it at ~32 Mbps
# (4 MB/s). Restart immediately on completion so the workload is
# continuous for the soak duration.

set -uo pipefail

MOUNT=/mnt/dtfs
LOG=/var/log/dtfs-workload-a.log
RATE_BPS=$((4 * 1024 * 1024))  # 4 MB/s ≈ 32 Mbps transcode rate

log() { echo "$(date -Iseconds) $*" >> "$LOG"; }

log "playback-client-a starting"
while true; do
    file=$(find "$MOUNT" -type f \( -name '*.mkv' -o -name '*.mp4' \) 2>/dev/null \
        | shuf -n 1)
    if [ -z "$file" ]; then
        log "no media files yet; sleeping 30s"
        sleep 30
        continue
    fi
    size=$(stat -c%s "$file" 2>/dev/null || echo 0)
    log "playing $file (${size} bytes)"
    if ! pv -q -L "$RATE_BPS" "$file" > /dev/null 2>&1; then
        log "ERR pv failed for $file"
        sleep 5
    fi
done
