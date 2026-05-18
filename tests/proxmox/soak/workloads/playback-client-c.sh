#!/usr/bin/env bash
# DC-C client workload: three concurrent streams + new-file ingest every
# 30 minutes. C is the fastest DC, so this is the heaviest read and write
# generator.

set -uo pipefail

MOUNT=/mnt/dtfs
LOG=/var/log/dtfs-workload-c.log
RATE_BPS=$((4 * 1024 * 1024))
INGEST_INTERVAL=$((30 * 60))
INGEST_DIR="$MOUNT/NewSeason"

log() { echo "$(date -Iseconds) $*" >> "$LOG"; }

play_one() {
    while true; do
        file=$(find "$MOUNT" -type f \( -name '*.mkv' -o -name '*.mp4' \) 2>/dev/null \
            | shuf -n 1)
        if [ -z "$file" ]; then sleep 15; continue; fi
        pv -q -L "$RATE_BPS" "$file" > /dev/null 2>&1 || true
    done
}

ingest_loop() {
    mkdir -p "$INGEST_DIR" 2>/dev/null || true
    local n=1
    while true; do
        local target="$INGEST_DIR/Ep${n}.mkv"
        log "ingesting $target (512 MB)"
        if ! dd if=/dev/urandom of="$target" bs=1M count=512 status=none 2>/dev/null; then
            log "ERR ingest failed for $target"
        fi
        n=$((n + 1))
        sleep "$INGEST_INTERVAL"
    done
}

log "playback-client-c starting 3 streams + ingest"
play_one &
play_one &
play_one &
ingest_loop &
wait
