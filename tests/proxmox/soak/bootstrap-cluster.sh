#!/usr/bin/env bash
# Post-deploy cluster bootstrap. Run AFTER ansible-playbook full-deploy.yml
# completes. Sets up the volume with anchor_datacenter=dc-a and verifies
# the cluster is ready for the smoke + soak runs.

set -euo pipefail
MGR="${MGR:-http://10.150.0.151:9402}"

echo "=== waiting for /readyz on mgr-01 ==="
for try in $(seq 1 60); do
    if curl -fsS -o /dev/null "$MGR/readyz"; then
        echo "ready after ${try}×5s"
        break
    fi
    sleep 5
done

echo ""
echo "=== cluster status ==="
curl -fsS "$MGR/api/v1/cluster/status" | python3 -m json.tool

echo ""
echo "=== nodes ==="
curl -fsS "$MGR/api/v1/nodes" | python3 -m json.tool

echo ""
echo "=== creating default volume with anchor_datacenter=dc-a ==="
curl -fsS -X POST "$MGR/api/v1/volumes" \
    -H 'content-type: application/json' \
    -d '{
        "name": "default",
        "chunk_size_mb": 128,
        "replication_hot": 2,
        "replication_warm": 2,
        "replication_cold": 1,
        "anchor_datacenter": "dc-a"
    }' | python3 -m json.tool

echo ""
echo "=== volumes ==="
curl -fsS "$MGR/api/v1/volumes" | python3 -m json.tool

echo ""
echo "=== /metrics sample ==="
curl -fsS "$MGR/metrics" | grep -E "dtfs_(files|chunks|nodes|capacity|leases|anchor)" | head -20

echo ""
echo "bootstrap complete"
