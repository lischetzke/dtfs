#!/usr/bin/env bash
# More focused SQLite test — do everything in ONE sqlite3 invocation,
# verify within-session counts grow correctly. Then verify persistence.
set -u
M=${MOUNT:-/mnt/dtfs}
DB=$M/sqltest3-$(date +%s).db

sudo rm -f "$DB" "$DB-journal" "$DB-wal" "$DB-shm" 2>/dev/null || true

echo "=== within-session inserts + counts ==="
sudo sqlite3 "$DB" <<'SQL' 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t(v) VALUES('one');
SELECT 'after-1' AS phase, COUNT(*) AS n FROM t;
INSERT INTO t(v) VALUES('two');
SELECT 'after-2' AS phase, COUNT(*) AS n FROM t;
INSERT INTO t(v) VALUES('three');
SELECT 'after-3' AS phase, COUNT(*) AS n FROM t;
SELECT 'rows' AS phase, GROUP_CONCAT(v, ',') FROM t;
SQL

echo ""
echo "=== persistence (separate sqlite3 process) ==="
sudo sqlite3 "$DB" "SELECT COUNT(*) FROM t; SELECT v FROM t ORDER BY id;" 2>&1

echo ""
echo "=== bigger workload: 200 rows then COUNT ==="
sudo sqlite3 "$DB" <<'SQL' 2>&1
BEGIN;
WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<200)
INSERT INTO t(v) SELECT 'row-'||i FROM c;
COMMIT;
SELECT COUNT(*) FROM t;
SQL

echo ""
echo "=== reopen count ==="
sudo sqlite3 "$DB" "SELECT COUNT(*) FROM t;" 2>&1

sudo rm -f "$DB" "$DB-journal" "$DB-wal" "$DB-shm" 2>/dev/null || true
