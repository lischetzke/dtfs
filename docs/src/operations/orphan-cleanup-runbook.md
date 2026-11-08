# Orphan-chunk cleanup runbook (Bug BC / BF-13)

Deleting a file (`unlink`/`rm` through FUSE, or the delete RPC) removes only its
metadata. DTFS deliberately has no immediate chunk delete: a content-identical
re-upload can land its bytes before its metadata commit, so an eager "nobody
references this chunk" check would race it. The orphan reaper below is therefore
the only thing that frees the space of deleted files, once each chunk's on-disk
`.dat` is older than `orphan_grace_secs`. Expect space to come back one to two
days after a bulk delete with the default cadence (Option B shortens that).

`orphan_scrub` (Bug BC-7) reaps chunks present on disk but not in
`state_machine.chunk_locations`. Default cadence is conservative
(`orphan_interval_secs = 86400` = 1 d, `orphan_grace_secs = 86400` =
1 d more before reap). On a freshly-upgraded cluster you may have
weeks of accumulated orphans (in the lab: 24+ GB of 128 MB chunks
from prior test campaigns). Two paths to drain them.

## Option A — wait

Do nothing. `orphan_scrub` ticks every `orphan_interval_secs`.
After two intervals (≈ 48 h on defaults) the orphans have been
seen + reaped. Audit `/api/v1/integrity/faults` filters this.

```
curl -s http://mgr01:9402/api/v1/audit?action=orphan.reaped&limit=200 \
  | python3 -m json.tool
```

## Option B — accelerate

Temporarily lower the cadence + grace via the live-reload Config
pane, observe the reap audit lines, then restore defaults.

1. **Snapshot current `[scrub]` values** before changing anything:
   ```
   curl -s http://mgr01:9402/api/v1/config/effective \
     | python3 -c "import json,sys; print({e['name']: e['current']
       for e in json.load(sys.stdin)['entries']
       if e['name'].startswith('manager.scrub.')})"
   ```

2. **POST a temporary lower cadence + ack flag**. Use the same
   `config_json` shape `update_config` accepts (full ManagerConfig
   JSON; reuse the snapshot from step 1, mutate just the two
   fields):
   ```
   curl -s -X POST http://mgr01:9402/api/v1/config \
     -H 'content-type: application/json' \
     -d '{"config_json":"<full-config>","write_to_disk":false,
          "acknowledge_cluster_wide":true}'
   ```
   The two fields to lower:
   - `scrub.orphan_interval_secs = 300` (5 min tick)
   - `scrub.orphan_grace_secs    = 21600`  (6 h, the enforced minimum)
   
   `write_to_disk:false` keeps the change live-only — a manager
   restart returns defaults. Operationally safer for a one-shot
   drain.

3. **Watch the reaps stream**:
   ```
   watch -n 30 'curl -s "http://mgr01:9402/api/v1/audit?action=orphan.reaped&limit=20" \
     | python3 -c "import json,sys; ts=json.load(sys.stdin); print(len(ts),
       \"reaped, latest:\", ts[0][\"summary\"] if ts else None)"'
   ```

4. **Verify disk usage matches expectations**:
   ```
   ssh root@<data-node> "du -sh <mount>/chunks"   # one per [[disks]] path
   ```
   Compare against `logical_bytes × replication_factor` from
   `/api/v1/stats`. After full drain, on-disk should be within
   ~5 % of expected.

5. **Restore defaults**. Either restart the manager (live-only
   wipe revert) or POST again with original values.

## Safety notes

- `orphan_scrub` respects `enforce_anchor`: a chunk that LOOKS
  orphan but matches a mandatory-anchor policy is never reaped
  (`state_machine.rs` apply path).
- Bug CB — the reap grace is measured against each chunk's durable
  on-disk `.dat` mtime (`scheduler.rs::orphan_reap_due`), reported by
  `ListChunks`, NOT a manager-local "first observed orphan" clock.
  A chunk is reaped only when it is unreferenced AND its mtime is at
  least `orphan_grace_secs` old. This survives manager restarts and
  leadership changes, so an accelerated drain still completes even if a
  manager bounces mid-drain. A chunk re-attached (re-referenced by a
  file) before the reap is preserved by the orphan check.
- A data node that does not report chunk mtimes (pre-Bug-CB binary)
  makes the manager DEFER reaping — `orphan.reaped` will not advance
  until the data nodes are upgraded. If a drain stalls with zero reaps,
  confirm the data-node build carries Bug CB (`dtfs-data --version`).
- Lowering `orphan_grace_secs` below 21600 s (6 h) is rejected at startup because it risks reaping
  in-flight writes (commit-to-raft-apply window). Don't go lower.
- This is a CLUSTER-WIDE knob. Every data node will run the
  same accelerated cadence while the override is live.

## When to repeat

After every multi-week pause in cluster maintenance, OR after a
data-node disk wipe + bootstrap (mgr state retained, but the new
disk holds none of the chunk_ids it used to). The wipe path
specifically generates a large in-state-not-on-disk delta that
Bug BF-8 escalates to `Quarantined`; in that case Drain Option B
should be paired with operator-driven unquarantine via
`/ui/integrity`.
