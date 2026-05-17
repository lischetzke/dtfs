#!/usr/bin/env bash
# Try exclusive locking mode + various journal modes
set -u
M=${MOUNT:-/mnt/dtfs}
DB=$M/sql4-$(date +%s).db

sudo rm -f "$DB" "$DB-journal" "$DB-wal" "$DB-shm" 2>/dev/null || true

echo "=== EXCLUSIVE locking + MEMORY journal (no on-disk journal) ==="
sudo sqlite3 "$DB" <<'SQL' 2>&1
PRAGMA locking_mode=EXCLUSIVE;
PRAGMA journal_mode=MEMORY;
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t(v) VALUES('a');
SELECT 'after-a' AS phase, COUNT(*) AS n FROM t;
INSERT INTO t(v) VALUES('b');
SELECT 'after-b' AS phase, COUNT(*) AS n FROM t;
INSERT INTO t(v) VALUES('c');
SELECT 'after-c' AS phase, COUNT(*) AS n FROM t;
SELECT GROUP_CONCAT(v) FROM t;
SQL

echo ""
echo "=== reopen ==="
sudo sqlite3 "$DB" "SELECT COUNT(*) AS reopen_count, GROUP_CONCAT(v) FROM t;" 2>&1

echo ""
echo "=== Now with default rollback journal ==="
sudo rm -f "$DB" "$DB-journal" "$DB-wal" "$DB-shm" 2>/dev/null || true
sudo sqlite3 "$DB" <<'SQL' 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t(v) VALUES('a');
INSERT INTO t(v) VALUES('b');
INSERT INTO t(v) VALUES('c');
SELECT COUNT(*), GROUP_CONCAT(v) FROM t;
SQL

sudo rm -f "$DB" "$DB-journal" "$DB-wal" "$DB-shm" 2>/dev/null || true
