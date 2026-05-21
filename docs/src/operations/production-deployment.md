# DTFS Production Deployment & Soak Runbook

Operator-facing playbook for the first DTFS prod-ish deployment. Covers
provisioning, smoke validation, the 48h soak campaign, and the explicit
gaps that ship in this build.

## What ships in this build

**Closed P0 (silent-data-loss class)**

- P0-1/P0-6 single-writer lease (manager Raft + FUSE EBUSY + crash recovery)
- P0-2 replica fallback on read failure (try every replica before erroring)
- P0-3 multi-manager runtime failover on the client
- P0-4 migration alternate-source retry with circuit breaker
- P0-5 scrub skips chunks with outstanding WAL entries
- P0-7 serde-default on every new state-machine field
- P0-8 audit log JSONL archive (`<data_dir>/audit/audit-YYYYMMDD.jsonl`)

**Distinctive features (now functional, not just configured)**

- P0-9 `reads_by_dc` end-to-end → auto-move-replica-to-active-DC works
- P0-10 DC-A anchor placement (tiering refuses to strand the last anchor copy)
- P0-11 FUSE disk-backed cache with 90-day `max_age_days` eviction

**Operations**

- P1-7 Prometheus `/metrics` scrape on the manager REST listener
- P1-8 alert webhooks (HMAC-signed POSTs, per-kind dedup)
- P1-10 `/healthz` (liveness) + `/readyz` (FSM-init + ≥1 healthy node)
- P1-11 RUST_LOG + systemd journald rate-limit defaults

**Known gaps that ship with caveats**

| ID | Item | Mitigation for first deploy |
|----|------|------------------------------|
| P1-1 | No TLS on gRPC / REST / Raft | Restrict to a private LAN; firewall everything that's not SSH/9402/9403. |
| P1-2 | No authentication (REST + WebUI fully open) | Same as above. Bind REST/WebUI to a management VLAN. |
| P1-9 | SMART data is best-effort (heartbeat enum only) | Monitor SMART externally (e.g. Prometheus node-exporter on each data node). |
| P1-12 | No graceful node decommission RPC | To remove a node: drain manually via `nftables` block, then `dtfs-cli node remove`. |
| P1-14 | No metadata backup CLI | `tar -czf metadata.tgz <data_dir>/raft-{log,sm}` on the leader. |

These are documented in the plan at `~/.claude/plans/are-all-function-relevant-eventual-bubble.md` and tracked as Phase D/E follow-ups.

## Lab campaign — 3-DC topology

Cluster name: `Lab`. Topology:

| DC | Bandwidth (down/up) | Role | Storage |
|----|---------------------|------|---------|
| dc-a | 400 / 40 Mbps (DSL) | Anchor — full corpus | 500 GB hot (SSD) + 300 GB cold (USB) |
| dc-b | 800 / 400 Mbps | Warm + cache, 90-day TTL | 200 GB warm (USB) |
| dc-c | 1 G / 1 G | Warm-only, fastest path | 50 GB warm (SSD partition) |

Volume `default`: `anchor_datacenter=dc-a`, `rf hot=2, warm=2, cold=1`,
`chunk_size_mb=128`.

## Provisioning (Proxmox)

```powershell
# From the Windows control host with PVE API token in env.
pwsh tests/proxmox/pve.ps1 -ConfigJson tests/proxmox/inventory-3dc.json -CreateAll
```

This clones the Debian 13 cloud-init template into 13 VMs at
10.150.0.150-162 with the right disk topology.

## Build & deploy

```bash
# On the builder VM (10.150.0.150 — kept alive across the soak):
ssh debian@10.150.0.150
cd ~/dtfs2 && git pull
cargo build --release --workspace
# Releases at target/release/dtfs-{manager,data,fuse,cli}

# Push binaries from builder to the Ansible control host, then deploy.
ansible-playbook -i tests/proxmox/ansible/inventories/3dc/hosts.yml \
    tests/proxmox/ansible/playbooks/full-deploy.yml

# Apply bandwidth shaping LAST (so deploy traffic isn't slowed).
ansible-playbook -i tests/proxmox/ansible/inventories/3dc/hosts.yml \
    tests/proxmox/ansible/playbooks/apply-shaping.yml
```

## Cluster bootstrap & volume

```bash
# From any host that can reach the managers:
dtfs-cli cluster status
dtfs-cli volume update default \
    --anchor-dc dc-a \
    --replication hot=2,warm=2,cold=1 \
    --chunk-size-mb 128
```

### Tier policy bootstrap (Bug AV)

After a fresh deploy or when upgrading an existing cluster from the
pre-Bug-AV schema, apply the two-class tier lifecycle policy in this
order. Each step is idempotent and safe to re-run.

```bash
# 1. (Upgrades only) Migrate the legacy `replication` field into
#    `mandatory_replicas`. Idempotent; preserves the legacy field as a
#    rollback path.
dtfs-cli volumes migrate-policy default

# 2. Inspect what landed — the new fields should now be populated.
dtfs-cli volumes show default

# 3. Set the cache policy. Plexmedia-style: 1 warm cache replica per DC,
#    promoted after 2 reads in 4 h, with a 5-min cooldown after operator
#    flushes.
dtfs-cli volumes set-cache default \
    --warm-min 1 --warm-max 1 --per-dc true \
    --promote-reads 2 --promote-window-secs 14400 --cooldown-secs 300

# 4. (Optional) Verify the round-trip.
dtfs-cli volumes show default
```

If `step 3` returns a `400 Bad Request` complaining about `max < min`,
fix the offending tier and re-run. The validator catches the
misconfiguration at the REST boundary so a malformed policy can't
land on the Raft state.

#### Rolling-upgrade gating

* **Before rollout** — the new manager binary safely loads pre-AV
  snapshots (`#[serde(default)]` on every new field). Old managers
  cannot load AV-written snapshots cleanly; complete the rolling
  upgrade *before* running `migrate-policy`.
* **During rollout** — leave volumes on the legacy `replication`
  field. The under-replication scheduler falls back to it when
  `mandatory_replicas` is all-zero. New writes work either way.
* **After rollout** — run `migrate-policy` once the leader is on the
  new binary. The Raft commit replicates the new schema to every
  follower, including any node still on a slightly older binary
  (they'll apply the new fields blindly; reads use the new code
  paths only on follower-elections, which happen post-rollout).

#### Rollback path

`migrate-policy` is additive: it never clears `replication`. Reverting
to pre-AV behaviour is "stop using `mandatory_replicas`", which
operationally means: don't run `migrate-policy` again on rolled-back
binaries (a downgrade is rare in this codebase and isn't a supported
flow — see § Operator dial-ins after a problem). If a downgrade
becomes necessary, deploy old binaries, then manually edit each
volume's `mandatory_replicas` back to zero via the REST UpsertVolume
path; legacy `replication` then drives the scheduler.

See [Storage → Tiering → Cache tier semantics (Bug AV)](../storage/tiering.md#cache-tier-semantics-bug-av)
for the runtime semantics that this bootstrap enables.

## Smoke checks

```bash
# Liveness / readiness probes
curl -fsS http://10.150.0.151:9402/healthz
curl -fsS http://10.150.0.151:9402/readyz
curl -fsS http://10.150.0.151:9402/metrics | grep dtfs_

# All 6 data nodes registered + healthy
curl -fsS http://10.150.0.151:9402/api/v1/nodes | jq '.[] | {hostname,health,dc}'

# Sentinel write + cross-DC read
ssh debian@10.150.0.162 'dd if=/dev/urandom of=/mnt/dtfs/sentinel.dat bs=1M count=64'
ssh debian@10.150.0.160 'cat /mnt/dtfs/sentinel.dat | b3sum'  # over DSL
ssh debian@10.150.0.162 'cat /mnt/dtfs/sentinel.dat | b3sum'  # over 1G
# Verify the two BLAKE3 hashes match.

# Anchor invariant — chunks of `sentinel.dat` should include a DC-A replica
curl -fsS 'http://10.150.0.151:9402/api/v1/files?path=/sentinel.dat' \
    | jq '.chunks[].node_addresses'

# Write-lease busy
ssh debian@10.150.0.160 'dd if=/dev/urandom of=/mnt/dtfs/lease-test.dat bs=1M count=1 &'
ssh debian@10.150.0.162 'dd if=/dev/urandom of=/mnt/dtfs/lease-test.dat bs=1M count=1'
# One of the two writes should produce "Device or resource busy".
```

## Preload corpus

```bash
# Copy the 641 GB / 290-file dummy_files corpus from Windows host via
# client-c (fastest DC, bypasses DC-A's DSL cap).
rsync -aP /mnt/c/Users/leduc/git_github.com/dtfs2/dummy_files/ \
    debian@10.150.0.162:/mnt/dtfs/
# Expected ~3h at 80 MB/s sustained.

# Baseline BLAKE3 of source
find /mnt/c/Users/leduc/git_github.com/dtfs2/dummy_files \
    -type f \( -name '*.mkv' -o -name '*.mp4' \) -print0 \
    | xargs -0 b3sum > corpus-source.blake3
```

## Run the 48h soak

```bash
# From the Ansible control host:
./tests/proxmox/soak/run-soak.sh \
    --duration 48h \
    --corpus /mnt/c/Users/leduc/git_github.com/dtfs2/dummy_files \
    --results-dir ./tests/proxmox/soak/results/$(date +%Y%m%dT%H%M)
```

Workloads run continuously on each client; the driver injects scheduled
failures (S13 partition at 8h, S14 disk-full at 16h, S15 corruption at
20h, S17 rolling-restart at 30h, etc.). Metrics are scraped every 30s.

## Final verification

```bash
# Post-soak BLAKE3 of every file in the corpus.
bash tests/proxmox/soak/scenarios/post-verify-blake3.sh \
    /mnt/c/Users/leduc/git_github.com/dtfs2/dummy_files \
    ./tests/proxmox/soak/results/<ts>

# Sign-off report
cat ./tests/proxmox/soak/results/<ts>/REPORT.md
```

## Pass / fail

PASS iff:

- corpus BLAKE3 100% match end-to-end
- anchor invariant held throughout (`dtfs_anchor_protected_total` does
  NOT include violations — only refusals; check that no `anchor.violated`
  events fired)
- lease invariant held (no observed simultaneous double-writer commit)
- `/readyz` was 200 for ≥98% of the run
- all S01-S03 + S08-S10 + S13 + S15 + S17 scenarios PASS in the report

FAIL on any:

- unrecovered corrupt chunk
- silent data loss observed by sentinel checks
- BLAKE3 mismatch on any source file

## Operator dial-ins after a problem

Common knobs you might tune mid-soak (live-reloaded via REST):

```bash
# Quieter or louder logging without a restart
ssh debian@10.150.0.151 'sudo systemctl set-environment RUST_LOG=dtfs=debug && \
    sudo systemctl reload dtfs-manager'

# Drop a single sink that's misbehaving (e.g. flooding Slack):
# edit /etc/dtfs/manager.toml, remove the offending [[alerts.webhooks]],
# then sudo systemctl reload dtfs-manager.

# Tighten or relax tiering thresholds
curl -fsS -X POST http://10.150.0.151:9402/api/v1/config \
    -d '{"tiering": {"evaluation_interval_secs": 30}}'
```

## When you're done

```bash
# Save the report off-cluster
rsync -avz debian@10.150.0.121:tests/proxmox/soak/results/ ./local-results/

# Drop bandwidth shaping (returns full wire speed for fast teardown)
ansible-playbook tests/proxmox/ansible/playbooks/revert-shaping.yml

# Teardown
ansible-playbook tests/proxmox/ansible/playbooks/teardown.yml
```

---

## Plexmedia — real-world prod target

The Plexmedia inventory (`tests/proxmox/inventory-plexmedia.json`) is the
deployment shape closest to actual production. Three DCs:

| DC   | Link    | Role            | Storage                                |
|------|---------|-----------------|----------------------------------------|
| LELI | 40 Mbps | Anchor (DSL)    | data-primary01: 4×150 GB cold (~50 TB at real scale) |
| TORO | 400 Mbps| SSD warm+cache  | data-warm01: 180 GB SSD                |
| NRB  | 1 Gbps  | Ingest          | data-warm02: 180 GB                    |

5 managers: 3 voters in LELI (so quorum can never partition without
LELI), 2 observers in TORO + NRB for local dashboard fast-path.

3 clients: Plex viewer in LELI (LAN to primary), Plex viewer in TORO
(40 GB disk cache + folder readahead, 90-day TTL), download ingest in
NRB.

Plex workload constants: each viewer ≤ 5 streams × 60 Mbps = 300 Mbps.
This is why TORO at 400 Mbps is the highest sustained sink and why LELI
viewers MUST hit the local primary — LELI's 40 Mbps uplink can't
sustain even one cross-DC Plex stream.

### Provision + deploy

```powershell
# Windows host
pwsh tests/proxmox/provision-plexmedia.ps1
```

```bash
# On the builder (10.150.0.150)
cd /home/debian/dtfs2 && cargo build --release --workspace
# Stage binaries under /home/debian/dtfs2-deploy/bin/
cp target/release/{dtfs-manager,dtfs-data,dtfs-fuse,dtfs-cli} \
   /home/debian/dtfs2-deploy/bin/

# Run the Plexmedia deploy
cd tests/proxmox/ansible
ANSIBLE_ROLES_PATH=./roles \
  ansible-playbook -i inventories/plexmedia/hosts.yml \
  playbooks/mount-data-disks.yml \
  playbooks/full-deploy-plexmedia.yml \
  playbooks/apply-shaping.yml
```

### Soak chain — 1h → 4h → 8h

```bash
ssh debian@10.150.0.150 \
  'bash /home/debian/dtfs2/tests/proxmox/soak/run-plexmedia.sh chain'
```

Chain aborts on first stage FAIL. Each stage's results dir holds the
metrics + scenario logs for forensic review.

---

## Bug U — graceful shutdown

All three daemons now drain on SIGTERM (systemd `stop`) instead of
losing in-flight work to SIGKILL:

- **dtfs-data**: waits up to 30s for in-flight `put_chunk` slots to
  drain, then flushes the pending-corruption queue.
- **dtfs-manager**: triggers a final Raft snapshot (5s budget) so the
  on-disk state captures every committed entry; emits a "manager.stop"
  audit event.
- **dtfs-fuse**: calls `MountHandle::unmount()` so the kernel issues
  flush+release on every open inode, then releases this PID's write
  leases on the manager.

systemd units now declare `TimeoutStopSec=30s` (mgr) / `45s` (data,fuse)
with `KillSignal=SIGTERM`. Verify on a running service:

```bash
systemctl show dtfs-data | grep -E 'TimeoutStop|KillSignal'
# Should show TimeoutStopUSec=45s, KillSignal=SIGTERM.

# Verify a clean stop completes within budget:
time sudo systemctl stop dtfs-data
# Expect <30s; if you hit TimeoutStop the drain helper is stuck and
# logs at WARN level — investigate before relying on graceful shutdown.
```

## Bug V — cooperative memory ladder

In addition to Bug G's "shed at threshold → reject new put_chunk", the
data node now also runs a Ceph OSD–style cooperative ladder:

| Pressure | RSS                       | Action                                |
|----------|---------------------------|---------------------------------------|
| Normal   | < warn_threshold          | Full ARC cache                        |
| Warn     | [warn, shed)              | Shrink ARC to 50% (LRU first)         |
| Shed     | >= shed_threshold         | Shrink ARC to 10%, refuse put_chunk   |

The shrink demotes evicted keys to ARC's B1/B2 ghost lists so the
adaptive `p` parameter recovers once pressure subsides. No client sees
errors until the Shed rung — clients first observe the cache shrink as
slightly higher read latency.

### Composite-host budgeting

Hosts like `toro-plex` (running mgr + data + client + Plex) need
per-role caps so Plex isn't starved. Drop-in templates under
`/etc/systemd/system/<service>.service.d/memory.conf`:

```ini
# dtfs-data.service.d/memory.conf
[Service]
MemoryHigh=1800M
MemoryMax=2048M
```

```ini
# dtfs-manager.service.d/memory.conf
[Service]
MemoryHigh=400M
MemoryMax=512M
```

```ini
# dtfs-fuse@.service.d/memory.conf
[Service]
MemoryHigh=800M
MemoryMax=1024M
```

The `data.toml` `[memory]` block independently configures the in-process
ladder; set `hard_cap_mb` to the same value as `MemoryMax` so both
defenses fire at the same point.

---

## Known operational gaps to plumb after first prod

- Anchor placement is enforced per-volume (`VolumeConfig.anchor_datacenter`)
  but `dtfs-cli volumes create` doesn't yet accept `--anchor-datacenter`.
  Workaround: pre-create the volume with the manager API directly.
- `manager_count` in `/api/v1/stats` only counts voters (Plexmedia: 3,
  not 5). Non-voting observers are intentional, so this is correct —
  but the "WARN node_count=5 expected ≥3" alert in the runbook needs
  adjusting for inventories with observers.
- The dtfs2-deploy staging dir on the builder (`/home/debian/dtfs2-deploy/`)
  isn't auto-populated by `cargo build`; the runbook above includes the
  cp step but a `scripts/stage-deploy.sh` would be a nice ergonomic.
