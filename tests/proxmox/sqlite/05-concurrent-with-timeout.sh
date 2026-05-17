#!/usr/bin/env bash
# Concurrent SQLite writers with busy_timeout so they wait politely.
set -u
M=${MOUNT:-/mnt/dtfs}
DB=$M/sqlconc-$(date +%s).db

sudo rm -f "$DB" "$DB-journal" "$DB-wal" "$DB-shm" 2>/dev/null || true

sudo sqlite3 "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, src TEXT, v TEXT);" 2>&1

echo "=== Two writers with 30-s busy_timeout, same client ==="
(
    for i in $(seq 1 50); do
        out=$(sudo sqlite3 "$DB" ".timeout 30000" "INSERT INTO t(src,v) VALUES('w1','row-$i');" 2>&1)
        if [ -n "$out" ]; then echo "w1-$i: $out"; fi
    done
    echo "w1-done"
) &
P1=$!
(
    for i in $(seq 1 50); do
        out=$(sudo sqlite3 "$DB" ".timeout 30000" "INSERT INTO t(src,v) VALUES('w2','row-$i');" 2>&1)
        if [ -n "$out" ]; then echo "w2-$i: $out"; fi
    done
    echo "w2-done"
) &
P2=$!
wait $P1 $P2

sudo sqlite3 "$DB" "SELECT src, COUNT(*) FROM t GROUP BY src; SELECT COUNT(*) AS total FROM t;" 2>&1

sudo rm -f "$DB" "$DB-journal" "$DB-wal" "$DB-shm" 2>/dev/null || true
