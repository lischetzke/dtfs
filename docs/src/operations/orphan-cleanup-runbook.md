# Orphan-chunk cleanup runbook (Bug BC / BF-13)

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
   - `scrub.orphan_grace_secs    = 60`  (1 min before reap)
   
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
   ssh debian@<data-node> "du -sh /mnt/*-*/chunks"
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
- Mid-reap the grace tracker (`scheduler.rs::orphan_seen`) only
  fires after seeing the same orphan TWICE across two consecutive
  ticks. A chunk re-attached between ticks is preserved.
- Lowering `orphan_grace_secs` below 60 s risks reaping
  in-flight writes (WAL+commit window). Don't go lower.
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
