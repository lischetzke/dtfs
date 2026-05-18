#!/usr/bin/env bash
# DC-B client workload: two concurrent streams + a folder-walk every 10 min.
# Folder-walk simulates the Plex library scanner so we can validate
# folder-sequential readahead in the metrics.

set -uo pipefail

MOUNT=/mnt/dtfs
LOG=/var/log/dtfs-workload-b.log
RATE_BPS=$((4 * 1024 * 1024))
SCANNER_INTERVAL=$((10 * 60))  # 10 minutes

log() { echo "$(date -Iseconds) $*" >> "$LOG"; }

play_one() {
    while true; do
        file=$(find "$MOUNT" -type f \( -name '*.mkv' -o -name '*.mp4' \) 2>/dev/null \
            | shuf -n 1)
        if [ -z "$file" ]; then sleep 15; continue; fi
        pv -q -L "$RATE_BPS" "$file" > /dev/null 2>&1 || true
    done
}

scanner_loop() {
    while true; do
        log "scanner walk"
        # Top-level folder walk like Plex does.
        find "$MOUNT" -maxdepth 2 -type d -exec ls -lt {} + \
            > /dev/null 2>&1 || true
        # Stat the first 50 files (touches metadata).
        find "$MOUNT" -type f \( -name '*.mkv' -o -name '*.mp4' \) \
            2>/dev/null | head -50 | xargs -r stat > /dev/null 2>&1 || true
        sleep "$SCANNER_INTERVAL"
    done
}

log "playback-client-b starting 2 streams + scanner"
play_one &
play_one &
scanner_loop &
wait
