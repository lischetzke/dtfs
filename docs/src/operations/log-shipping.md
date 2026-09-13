# Log Shipping (Filebeat → Elastic)

This guide is a **worked example** for shipping DTFS logs from every cluster node to an Elastic Stack (Elasticsearch + Kibana) using Filebeat as the on-node agent. It is tuned for the multi-DC Plexmedia topology described in [CLAUDE.md § A.1.1](../../../CLAUDE.md) — bandwidth-asymmetric DCs where naive log shipping can saturate the 40 Mbps LELI uplink during an incident.

Adapt the version numbers, hostnames, and credentials to your environment. Tested with Elastic Stack 8.13 and Filebeat 8.13.

## Architecture

```
┌─────────────────────────────────────────┐
│  DTFS node (manager / data / fuse)      │
│  ┌─────────────────────────────────┐    │
│  │ dtfs-* binary                   │    │
│  │   tracing-subscriber → stdout   │    │
│  └──────────────┬──────────────────┘    │
│                 │                       │
│  ┌──────────────▼──────────────────┐    │
│  │ systemd-journald (per-unit log) │    │
│  └──────────────┬──────────────────┘    │
│                 │                       │
│  ┌──────────────▼──────────────────┐    │
│  │ filebeat (local agent)          │    │
│  │   journald input → ES output    │    │
│  └──────────────┬──────────────────┘    │
└─────────────────┼───────────────────────┘
                  │ HTTPS (compressed)
                  ▼
        ┌───────────────────┐
        │ Elasticsearch     │
        │ + Kibana          │
        └───────────────────┘
```

Each DTFS node runs its own Filebeat. The agent reads only the DTFS systemd units from the journal (no host-wide log noise), parses the JSON payload tracing-subscriber emits, and ships to a central Elastic cluster.

## Prerequisites

- **Elastic Stack 8.x** (Elasticsearch + Kibana). 8.13+ recommended for ECS 8 mappings.
- **Filebeat 8.x**, matching your Elasticsearch major version.
- **Network**: outbound TCP 9200 (ES direct) or 5044 (via Logstash) from each DTFS node to the Elastic cluster. Add this to your shaping exemption list alongside SSH/22, manager gRPC/9401, manager API/9402, data gRPC/9500, NTP/123 (CLAUDE.md § A.7) — otherwise Filebeat backoff will mask real incidents.
- **Disk**: ~1 GB per node for Filebeat's local queue (see § Bandwidth and noise control).
- **Credentials**: a dedicated `filebeat_writer` Elasticsearch role with `create_doc` on `filebeat-dtfs-*` and `monitor` on the cluster. Do not reuse the superuser.

For multi-DC deployments with the Plexmedia profile, prefer a per-DC ingest path (LELI ingest node, TORO ingest node, NRB ingest node) so LELI's 40 Mbps uplink isn't the bottleneck for warm/hot DCs' chattier logs.

## Step 1 — Configure DTFS to emit JSON logs

DTFS components use `tracing-subscriber` with the `json` feature enabled (see workspace `Cargo.toml`). Switch your binaries to JSON output via a systemd drop-in so the change survives package upgrades.

Create the drop-in for each role:

```ini
# /etc/systemd/system/dtfs-manager.service.d/logging.conf
[Service]
Environment="RUST_LOG=info,dtfs_manager=debug,dtfs_raft=info,openraft=info"
Environment="DTFS_LOG_FORMAT=json"
```

```ini
# /etc/systemd/system/dtfs-data.service.d/logging.conf
[Service]
Environment="RUST_LOG=info,dtfs_data=debug"
Environment="DTFS_LOG_FORMAT=json"
```

```ini
# /etc/systemd/system/dtfs-fuse.service.d/logging.conf
[Service]
Environment="RUST_LOG=info,dtfs_fuse=info"
Environment="DTFS_LOG_FORMAT=json"
```

Reload and restart:

```bash
sudo systemctl daemon-reload
sudo systemctl restart dtfs-manager dtfs-data dtfs-fuse
```

Verify each unit is emitting one JSON object per line:

```bash
journalctl -u dtfs-manager -n 3 -o cat
# {"timestamp":"2026-05-21T08:14:02.317Z","level":"INFO","target":"dtfs_manager::raft","fields":{"message":"Raft leader confirmed","leader":"mgr01","term":42},...}
```

> **If your build does not honour `DTFS_LOG_FORMAT`:** fall back to the default tracing-subscriber text format and use Filebeat's `dissect` processor to parse it. JSON is strongly preferred — text parsing breaks every time a format string changes.

## Step 2 — Install Filebeat

Per node (manager, data, fuse client):

```bash
# Debian / Ubuntu
curl -fsSL https://artifacts.elastic.co/GPG-KEY-elasticsearch \
  | sudo gpg --dearmor -o /usr/share/keyrings/elastic-keyring.gpg

echo "deb [signed-by=/usr/share/keyrings/elastic-keyring.gpg] \
https://artifacts.elastic.co/packages/8.x/apt stable main" \
  | sudo tee /etc/apt/sources.list.d/elastic-8.x.list

sudo apt-get update
sudo apt-get install -y filebeat=8.13.*
sudo systemctl enable filebeat
```

Filebeat needs to read the journal, so its user must be in the `systemd-journal` group:

```bash
sudo usermod -a -G systemd-journal filebeat
```

## Step 3 — Configure Filebeat

Replace `/etc/filebeat/filebeat.yml` with:

```yaml
# /etc/filebeat/filebeat.yml
filebeat.inputs:
  - type: journald
    id: dtfs
    # Only the three DTFS units — no host noise.
    include_matches.match:
      - _SYSTEMD_UNIT=dtfs-manager.service
      - _SYSTEMD_UNIT=dtfs-data.service
      - _SYSTEMD_UNIT=dtfs-fuse.service
    fields:
      log_source: dtfs
    fields_under_root: true

processors:
  # Parse the JSON object emitted by tracing-subscriber.
  - decode_json_fields:
      fields: ["message"]
      target: "dtfs"
      overwrite_keys: true
      add_error_key: true

  # Tag every event with cluster identity. Set via systemd env (see below).
  - add_fields:
      target: dtfs
      fields:
        cluster: "${DTFS_CLUSTER:plexmedia}"
        dc: "${DTFS_DC:unknown}"
        role: "${DTFS_ROLE:unknown}"
        node: "${DTFS_NODE:${HOSTNAME}}"

  # Drop noisy dependency targets. tonic/h2/hyper at INFO are usually
  # uninteresting once a cluster is stable; raise per-target later if needed.
  - drop_event:
      when:
        regexp:
          dtfs.target: "^(hyper|tonic|h2|tower)"

output.elasticsearch:
  hosts: ["https://elastic.example.internal:9200"]
  username: "${ELASTIC_USER}"
  password: "${ELASTIC_PASSWORD}"
  ssl.verification_mode: "full"
  preset: balanced
  index: "filebeat-dtfs-%{[dtfs.cluster]}-%{+yyyy.MM.dd}"

setup.template.enabled: false   # we manage the template ourselves (Step 4)
setup.ilm.enabled: false

queue.mem:
  events: 4096
  flush.min_events: 512
  flush.timeout: 5s

# JSON compresses well; spend a little CPU to save uplink bytes.
http.compression_level: 6

logging.level: info
logging.json: true
```

### Per-DC overrides for the LELI link

LELI is bandwidth-constrained (40 Mbps shared with Plex egress). On LELI nodes, add `/etc/filebeat/filebeat.d/leli.yml` (Filebeat auto-merges files in `filebeat.d/` if `path.config` includes it; otherwise inline):

```yaml
output.elasticsearch:
  bulk_max_size: 50      # smaller bulks → smaller bursts
  worker: 1
  backoff.init: 5s
  backoff.max: 600s

# Spool to disk so a transient ES outage doesn't drop events while the
# slow uplink catches up.
queue.disk:
  max_size: 1GB
  path: /var/lib/filebeat/queue

# On LELI, ship WARN+ only by default. Raise at incident time.
processors:
  - drop_event:
      when:
        not:
          regexp:
            dtfs.level: "^(WARN|ERROR)"
```

### Wire up per-node identity

Each node needs to know its DC + role for the `add_fields` processor:

```bash
sudo systemctl edit filebeat
```

```ini
[Service]
Environment="DTFS_CLUSTER=plexmedia"
Environment="DTFS_DC=leli"           # one of: leli, toro, nrb
Environment="DTFS_ROLE=manager"      # one of: manager, data, fuse
Environment="DTFS_NODE=mgr01"
Environment="ELASTIC_USER=filebeat_writer"
Environment="ELASTIC_PASSWORD=<from secret store>"
```

Restart:

```bash
sudo systemctl restart filebeat
sudo systemctl status filebeat --no-pager
journalctl -u filebeat -n 50 --no-pager
```

You should see Filebeat connect to Elasticsearch and start the journal input.

## Step 4 — Index template and ILM

Filebeat 8 ships a sensible default template, but DTFS adds custom fields (`dtfs.node_id`, `dtfs.chunk_id`, `dtfs.volume`, …) that we want to map as `keyword` for aggregation rather than letting Elastic infer `text`.

### Component template (DTFS fields)

```bash
curl -sS -u admin -X PUT \
  "https://elastic.example.internal:9200/_component_template/filebeat-dtfs-fields" \
  -H 'Content-Type: application/json' -d @- <<'EOF'
{
  "template": {
    "mappings": {
      "properties": {
        "dtfs": {
          "properties": {
            "level":     { "type": "keyword" },
            "target":    { "type": "keyword" },
            "cluster":   { "type": "keyword" },
            "dc":        { "type": "keyword" },
            "role":      { "type": "keyword" },
            "node":      { "type": "keyword" },
            "node_id":   { "type": "keyword" },
            "chunk_id":  { "type": "keyword" },
            "volume":    { "type": "keyword" },
            "term":      { "type": "long"    },
            "leader":    { "type": "keyword" }
          }
        }
      }
    }
  }
}
EOF
```

### Index template

```bash
curl -sS -u admin -X PUT \
  "https://elastic.example.internal:9200/_index_template/filebeat-dtfs" \
  -H 'Content-Type: application/json' -d @- <<'EOF'
{
  "index_patterns": ["filebeat-dtfs-*"],
  "priority": 200,
  "composed_of": ["filebeat-dtfs-fields"],
  "template": {
    "settings": {
      "index.lifecycle.name": "filebeat-dtfs-30d",
      "index.number_of_shards": 1,
      "index.number_of_replicas": 1
    }
  }
}
EOF
```

### ILM policy (30-day retention)

```bash
curl -sS -u admin -X PUT \
  "https://elastic.example.internal:9200/_ilm/policy/filebeat-dtfs-30d" \
  -H 'Content-Type: application/json' -d @- <<'EOF'
{
  "policy": {
    "phases": {
      "hot": {
        "actions": {
          "rollover": { "max_age": "1d", "max_primary_shard_size": "20gb" }
        }
      },
      "warm": {
        "min_age": "1d",
        "actions": { "forcemerge": { "max_num_segments": 1 } }
      },
      "delete": {
        "min_age": "30d",
        "actions": { "delete": {} }
      }
    }
  }
}
EOF
```

## Step 5 — Verify in Kibana

1. **Create a data view**. Kibana → Stack Management → Data Views → Create.
   - Name: `filebeat-dtfs`
   - Index pattern: `filebeat-dtfs-*`
   - Timestamp field: `@timestamp`

2. **Sanity check** in Discover. You should see one document per DTFS log line, with parsed fields under `dtfs.*`. Filter `dtfs.role: "manager"` to confirm role tagging works.

3. **Sample saved searches** (paste into Discover, save with the listed name):

   | Saved search | KQL query |
   |---|---|
   | Raft leader changes | `dtfs.target: "openraft" and dtfs.fields.message: *Leader*` |
   | Memory shedding (Bug G) | `dtfs.fields.message: "RESOURCE_EXHAUSTED"` |
   | Scrub corruption (§ 7 #7) | `dtfs.target: "dtfs_data::scrub" and dtfs.level: "ERROR"` |
   | FUSE cookie path (Bug F) | `dtfs.target: "dtfs_fuse::fuse_adapter" and dtfs.fields.message: *cookie*` |
   | Anchor invariant violation (LELI replica missing) | `dtfs.fields.message: *anchor* and dtfs.dc: "leli" and dtfs.level: (WARN or ERROR)` |
   | Write-lease churn | `dtfs.target: "dtfs_manager::lease" and dtfs.level: "WARN"` |

4. **Recommended dashboards**. One per concern, each pinned to a 24-hour default window:

   - **Cluster health**: manager count per DC, Raft term over time, leader uptime, log volume per role.
   - **Data plane**: `put_chunk` rate, `get_chunk` rate, error count per data node.
   - **Memory pressure**: per-role RSS samples, shed events per minute.
   - **Plex playback events** (FUSE-side): cache hit ratio, read-ahead prefetch rate, replica fallback rate.

## Bandwidth and noise control

These knobs all matter most for LELI. Apply selectively:

- **Drop noisy dependency targets at the agent.** The `drop_event` processor in Step 3 strips `hyper`, `tonic`, `h2`, `tower`. These are high-volume, low-value once the cluster is stable.
- **Filter by level for the constrained DC.** LELI ships WARN+ only by default (the `leli.yml` drop-in). Operators can temporarily set `RUST_LOG=debug` on a single node and ship INFO+ for that incident.
- **Use the disk-backed queue.** `queue.disk` lets Filebeat survive an ES outage without dropping events; events drain over the next hour as the uplink permits.
- **Compress.** `http.compression_level: 6` reduces wire volume ~4–6× on JSON.
- **Never ship DEBUG across DCs by default.** Keep DEBUG local (journald retention is ~7 days at default config); only ship temporarily when an operator opts in via an env-var bump.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Filebeat connects but no events flow | journald input doesn't match the unit name | Run `journalctl -o json -n 1 -u dtfs-manager` and confirm `_SYSTEMD_UNIT` matches exactly. Service prefixes (e.g. `dtfs-manager@instance.service`) need an updated matcher. |
| `message` arrives as a single opaque string | DTFS not emitting JSON, or `decode_json_fields` failed | Check `journalctl -u dtfs-manager -n 1 -o cat` is valid JSON. If not, verify `DTFS_LOG_FORMAT=json` is in effect (`systemctl show dtfs-manager -p Environment`). |
| All numeric fields show as `text` in Kibana | First doc landed before the index template was installed | Delete the misindexed index (`DELETE filebeat-dtfs-...`) and let Filebeat rewrite it; the disk queue replays the events. |
| LELI uplink saturates during an incident | Filebeat shipping DEBUG from a temporary investigation | Lower `RUST_LOG` back to `info`, restart the affected DTFS unit. Filebeat's disk queue drains over the next ~30 min. |
| Filebeat fails to read journal with EACCES | `filebeat` user not in `systemd-journal` group | `sudo usermod -a -G systemd-journal filebeat && sudo systemctl restart filebeat`. |
| Mapping explosion on `dtfs.fields.*` | Tracing fields with high-cardinality keys promoted to mapping | Promote only the well-known keys (Step 4 component template) and route the rest via `flattened` field: add `"dtfs.fields": { "type": "flattened" }` to the component template. |

## See also

- [Monitoring](monitoring.md) — Prometheus metrics scrape (complement to log shipping).
- [Alerting](alerting.md) — alert rules; consider adding log-derived alerts for the saved searches above.
- [Troubleshooting](troubleshooting.md) — on-cluster diagnostic commands that complement centralised logs.
- [CLAUDE.md § A.7](../../../CLAUDE.md) — DTFS bandwidth-shaping and port exemption rules.
