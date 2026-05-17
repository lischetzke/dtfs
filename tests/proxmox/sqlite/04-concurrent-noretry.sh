#!/usr/bin/env bash
# Concurrent SQLite writers on the *same* client (intra-process locking)
# and across two clients (cross-client locking — expected to be unsafe
# until we wire fcntl through to the manager).
set -u
M=${MOUNT:-/mnt/dtfs}
DB=$M/sqlconc-$(date +%s).db

sudo rm -f "$DB" "$DB-journal" "$DB-wal" "$DB-shm" 2>/dev/null || true

echo "=== Setup table ==="
sudo sqlite3 "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY, src TEXT, v TEXT);" 2>&1

echo ""
echo "=== Two writers, same client (single process FUSE) ==="
# Two concurrent sqlite3 processes each insert 50 rows.
(
    for i in $(seq 1 50); do
        sudo sqlite3 "$DB" "INSERT INTO t(src,v) VALUES('w1','row-$i');" 2>/dev/null || echo "w1-fail-$i"
    done
) &
P1=$!
(
    for i in $(seq 1 50); do
        sudo sqlite3 "$DB" "INSERT INTO t(src,v) VALUES('w2','row-$i');" 2>/dev/null || echo "w2-fail-$i"
    done
) &
P2=$!
wait $P1 $P2 2>/dev/null

sudo sqlite3 "$DB" "SELECT src, COUNT(*) FROM t GROUP BY src; SELECT COUNT(*) AS total FROM t;" 2>&1

echo ""
echo "=== Verify each row has a sequential pk (no gaps from rolled-back txns) ==="
sudo sqlite3 "$DB" "SELECT MIN(id) AS mn, MAX(id) AS mx, COUNT(DISTINCT id) AS uniq FROM t;" 2>&1

sudo rm -f "$DB" "$DB-journal" "$DB-wal" "$DB-shm" 2>/dev/null || true
