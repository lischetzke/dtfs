# DTFS Proxmox Test Run — Findings Log

Live log of issues found during the test campaign. Each entry: severity, location, observed behaviour, fix recommendation.

---

## F-001 — Ansible playbook pins Debian 12 fuse package

**Severity:** Low (build/deploy)
**Location:** [tests/ansible/inventory/group_vars/all.yml:52-67](tests/ansible/inventory/group_vars/all.yml)
**Observed:** Running `setup.yml` against Debian 13 (Trixie) hosts fails immediately:
```
No package matching 'libfuse3-3' is available
```
`dtfs_system_packages` includes `libfuse3-3` — that's the Debian 12 ABI version. Debian 13 ships `libfuse3-4`.

**Fix recommendation:** Replace `libfuse3-3` with `libfuse3-4`, or with the `libfuse3` metapackage if it exists, or branch on `ansible_distribution_release`.

**Worked around in:** appended override to `tests/proxmox/ansible/group_vars/all.yml`.

---

## F-002 — dtfs UID/GID 990 collides with systemd-resolve on Debian 13

**Severity:** Low (build/deploy)
**Location:** [tests/ansible/inventory/group_vars/all.yml:89-90](tests/ansible/inventory/group_vars/all.yml)
**Observed:** Running `setup.yml` against Debian 13 hosts fails:
```
groupadd: GID '990' already exists
```
`dtfs_uid: 990` / `dtfs_gid: 990` — but on Debian 13, GID 990 belongs to `systemd-resolve`. The next 9 GIDs (991–999) are also occupied by systemd-* services.

**Fix recommendation:** Pick a GID well below systemd's reserved range (e.g. 800–899), or rely on `useradd --system` (which picks the next free slot below 1000) instead of pinning a specific value.

**Worked around in:** override `dtfs_uid: 980` / `dtfs_gid: 980` in `tests/proxmox/ansible/group_vars/all.yml`.

---

## F-003 — Ansible playbook is built for a config schema the binary no longer accepts

**Severity:** **Critical** (deployment is broken out of the box)
**Location:** [tests/ansible/playbooks/setup.yml](tests/ansible/playbooks/setup.yml) + [tests/ansible/roles/dtfs-manager/templates/manager.toml.j2](tests/ansible/roles/dtfs-manager/templates/manager.toml.j2) + [tests/ansible/inventory/group_vars/all.yml](tests/ansible/inventory/group_vars/all.yml)

**Observed:** The setup.yml playbook calls subcommands the binary does not implement and produces a config file the binary cannot parse. Comparing what the Ansible stack produces vs. what `crates/dtfs-common/src/config.rs` defines:

| Aspect | Ansible playbook expects | Real binary expects |
|---|---|---|
| Bootstrap mechanism | `dtfs-manager init-cluster --node-id X --cluster-name Y` and `dtfs-manager join-cluster --join-addr X --node-id Y` CLI subcommands | `bootstrap = true` in `[cluster]` section of `manager.toml`. The binary takes only `--config <path>` — no other args/subcommands exist. |
| Node identity | `[node].id = <int>`, `[node].name = "..."`, `[node].cluster = "..."` | `[node].id = "auto"` (string), `[node].hostname = "..."`, plus a top-level `[location]` section with `region/datacenter/rack/host` fields |
| Cluster section | `shared_secret` + `peers = [{node_id, addr}, ...]` | `name`, `shared_secret`, `bootstrap`. **No `peers` list — peer discovery is via Raft join, not config** |
| Raft section | `listen_addr`, `data_dir`, `election_timeout_min_ms`, `election_timeout_max_ms`, `heartbeat_interval_ms`, `snapshot_threshold`, `snapshot_interval_s`, `log_max_entries`, `leader_lease_duration_ms`, `leader_renew_deadline_ms` | only `listen_addr`, `election_timeout_ms` (single), `heartbeat_interval_ms`, `snapshot_threshold` |
| Ports (Raft / gRPC / API) | 7000 / 7001 / 8080 | **9400 / 9401 / 9402** (per `deploy/configs/mgr-1.toml`) |
| Data node `[cluster]` | not present — uses `dtfs_data_manager_endpoints` list | `manager_addr = "<ip>:9401"` (single entry; bootstrap node), `shared_secret` |
| Tiering keys | `hot_threshold_accesses`, `hot_window_hours`, `cold_after_hours`, `evaluation_interval_s`, `migration_bandwidth_mb` | `hot_threshold_reads_per_hour`, `cold_threshold_days`, `frozen_threshold_days`, `evaluation_interval_secs`, `migration_bandwidth_limit_mbps` |
| Monitoring keys | `health_check_interval_s`, `node_dead_after_s`, `node_suspect_after_s`, `replication_trigger_delay_s` | `heartbeat_interval_secs`, `suspect_after_missed` (count, not seconds), `down_after_missed` (count, not seconds) |
| Webui section | `[webui] enabled, path` in `manager.toml` | Not in `ManagerConfig` struct — webui is served unconditionally by the API on the same port |

**Fix recommendation:** The example configs at `deploy/configs/mgr-1.toml`, `data-1.toml` etc. ARE accurate. Either:
1. Regenerate the Ansible templates from those examples (one-shot fix), OR
2. Delete the broken Ansible stack and document the deploy/configs/ approach as the canonical bootstrap (lighter touch).

This is the single largest gap between docs/automation and reality. A new operator following the documented deploy procedure will hit a wall on the first command.

**Worked around in:** wrote a fresh playbook at `tests/proxmox/ansible/playbooks/full-deploy.yml` and templates at `tests/proxmox/ansible/templates/*.toml.j2` using the real schema.

---

## F-004 — `dtfs-fuse@.service` uses `%i` instead of `%I` and produces an invalid mountpoint

**Severity:** Medium (FUSE never mounts via systemd)
**Location:** [deploy/systemd/dtfs-fuse@.service](deploy/systemd/dtfs-fuse@.service)
**Observed:** The unit's `ExecStart` and `ExecStop` reference `%i` (the raw, escaped instance name). When the unit is instantiated against `/mnt/dtfs`, systemd-escape encodes it as `mnt-dtfs`, and the service is invoked as:
```
/usr/local/bin/dtfs-fuse --config /etc/dtfs/fuse.toml --mountpoint mnt-dtfs
```
i.e. the leading `/` is stripped and `/` is replaced with `-`. The binary creates/mounts at `./mnt-dtfs` (relative path, ends nowhere useful) — every restart fails.

**Fix recommendation:** Use `%I` (decoded form, returns `mnt/dtfs`) and hard-prefix a `/` so the value is `/mnt/dtfs`:
```ini
ExecStart=/usr/local/bin/dtfs-fuse --config /etc/dtfs/fuse.toml --mountpoint /%I
ExecStop=/bin/fusermount3 -u /%I
```

**Worked around in:** patched on each client via `sed -i 's| %i| /%I|g' …`.

---

## F-005 — `PrivateTmp=yes` puts the FUSE mount in a private namespace

**Severity:** **Critical** (FUSE service appears running but mount is invisible to host)
**Location:** [deploy/systemd/dtfs-fuse@.service](deploy/systemd/dtfs-fuse@.service)
**Observed:** With `PrivateTmp=yes`, systemd starts the unit in a new mount namespace (this is how it isolates `/tmp`). The FUSE mount the binary performs happens *inside* that namespace, so the host's `/mnt/dtfs` stays unmounted even though the service shows `Active: active (running)` and `dtfs-fuse` logs `mount "/mnt/dtfs" success` and `fuse init done`. Externally:
```
$ mountpoint /mnt/dtfs
/mnt/dtfs is not a mountpoint
$ mount | grep dtfs    # nothing
```
Writes to `/mnt/dtfs` from outside the service silently land on the underlying directory, not on DTFS. This is a silent data-routing bug — every "successful" write through the FUSE path goes to the local disk instead of the cluster.

**Fix recommendation:**
1. Set `PrivateTmp=no` and add `MountFlags=shared` to propagate FUSE mounts back to the host (verified: with these flags the mount appears in `/proc/mounts` and writes route to the cluster).
2. Alternatively, drop the namespace flags entirely for this service (FUSE serving fundamentally needs to be visible to the host's mount table).

**Worked around in:** patched both client units with `PrivateTmp=no` + `MountFlags=shared`.

---

## F-006 — Manager reports `total_capacity: 0` even with mounted, configured disks

**Severity:** Medium (capacity-based scheduling/tiering can't make decisions)
**Location:** [crates/dtfs-manager/src/api.rs](crates/dtfs-manager/src/api.rs) (`/api/v1/cluster/status` route + capacity aggregation)
**Observed:** After the cluster is up with 4 data nodes, each owning a 16 GB hot disk + 16 GB cold disk (total 128 GB raw), the API returns:
```json
{
  "cluster_name": "dtfs-test-cluster",
  "data_nodes": 4,
  "data_nodes_healthy": 4,
  "files": 2,
  "total_capacity": 0,
  "used_capacity": 0
}
```
Per-node `/api/v1/nodes` likewise shows `total_capacity: 0` for every node despite `disk_count: 2`. Files write & read successfully so chunks ARE landing on disks; only the capacity reporting is broken.

**Fix recommendation:** This looks consistent with the open issue I-3 in ISSUES_TO_FIX.md (SMART/disk size not collected by heartbeat). The data node needs to `statvfs()` each configured disk path on startup and include the capacity in its heartbeat to the manager, or the manager needs to expose this on demand.

---

## F-007 — `dtfs-cli` lacks a volume-creation subcommand

**Severity:** Low (REST API works, but CLI parity is missing)
**Location:** [crates/dtfs-cli/src/main.rs](crates/dtfs-cli/src/main.rs)
**Observed:** `dtfs-cli volumes` lists volumes but has no `create` action:
```
$ dtfs-cli volumes --help
Manage volumes
Options:
  --format <FORMAT>  Output format [default: table]
  -h, --help         Print help
```
A new operator following `docs/first-cluster.md` has no documented way to create a volume from the CLI. Workaround is `curl -X POST /api/v1/volumes -d '{"name":"default"}'`.

**Fix recommendation:** Add `dtfs-cli volumes create <name> [--replication-factor N] [--tier-policy hot|cold|...]` that calls the same REST endpoint.

---

## F-008 — `dtfs_system_packages` is missing `python3` / Ansible interpreter fallback

**Severity:** Low (works only because Debian 13 generic image ships python3.13 by default)
**Location:** [tests/ansible/inventory/group_vars/all.yml](tests/ansible/inventory/group_vars/all.yml)
**Observed:** Ansible logs warnings on every play: "Host 'mgr-01' is using the discovered Python interpreter at /usr/bin/python3.13, but future installation of another Python interpreter could cause a different interpreter to be discovered." Not blocking now but will flap if a host gains another python.

**Fix recommendation:** Set `ansible_python_interpreter: /usr/bin/python3` in group_vars/all.yml.

---

## F-009 — Three "manager" nodes are actually three isolated single-node Raft instances (no HA)

**Severity:** **Showstopper** (the project's central HA claim is not implemented)
**Location:** [crates/dtfs-manager/src/main.rs:99-113](crates/dtfs-manager/src/main.rs)
**Observed:** Deployed a 3-manager cluster per the documented procedure (`bootstrap = true` on mgr-01, `bootstrap = false` on mgr-02 & mgr-03). Each manager starts cleanly. Each data node successfully registers with mgr-01 (the manager_addr in its config). But:

| Endpoint | mgr-01 | mgr-02 | mgr-03 |
|---|---|---|---|
| `/api/v1/cluster/status` data_nodes | **4** | **0** | **0** |
| `/api/v1/cluster/status` files | **2** | **0** | **0** |
| `/api/v1/nodes` length | **4** | **0** | **0** |

The three managers report **completely disjoint state**. They are not following a shared Raft log; they have no idea about each other.

Looking at the bootstrap code in [main.rs:99-113](crates/dtfs-manager/src/main.rs):
```rust
if config.cluster.bootstrap {
    let mut members = BTreeMap::new();
    members.insert(node_id, node);  // ← only THIS node
    raft.initialize(members).await...
}
```

When `bootstrap = true`, the manager initialises a **single-member** Raft cluster containing only itself. There is no code path that:
1. Discovers peer managers
2. Calls `raft.change_membership()` to add them as voters/learners
3. Allows a follower with `bootstrap = false` to ask the existing leader to be admitted

The `/api/v1/join/{request,complete}` endpoints exist but are wired to the **data-node** HMAC join flow — they don't add Raft voters.

There is also no `--join` CLI flag, no `peers = [...]` in the config schema, and no `/api/v1/raft/*` endpoint to add members at runtime.

**Effect:**
- The project is effectively a 1-manager system. The other two manager VMs run zombie Raft instances with no data and no purpose.
- If mgr-01 dies, the cluster is dead: data nodes have no fallback (their `manager_addr` points at mgr-01 only) and even if they pointed at mgr-02/03, those have no state.
- All the resiliency / failover / leader-election claims in `docs/` are inert. None of the integration scripts that depend on multi-manager quorum (07_election_quorum, parts of 03_network_disconnect, 04_service_crash) can pass.

**Fix recommendation (in priority order):**

1. **Implement `add_voter`** — expose a manager-side admin endpoint that, when called on the current leader, performs `raft.change_membership(new_set, /*learner_only=*/true)` then promotes to voter. Document this as the proper way to grow the cluster from 1 → 3 voters after bootstrap.

2. **Wire a join CLI** — either revive `dtfs-manager join --leader <addr>` (the Ansible playbook already expects this) or add `dtfs-cli managers add <addr>` that hits the admin endpoint from any seed manager. Until either exists, only single-manager deployments work.

3. **Extend `manager.toml` `[cluster]`** with an `initial_voters = ["addr1", "addr2", "addr3"]` list and have the bootstrap path initialise Raft with all of them at once. This is what most OpenRaft tutorials show and matches what the `deploy/configs/mgr-{1,2,3}.toml` examples *imply* but don't actually wire.

4. **Document** clearly in `docs/operations/cluster-management.md` how multi-manager bootstrap is *supposed* to work, with a working example.

**Worked around:** none — the only honest workaround is to run a single-manager test cluster. Doing so reduces the test campaign to "verify a single-node DTFS works", which has lower informational value.

---

## F-010 — Replication factor (`hot = 3`) is configured but NOT enforced on initial write

**Severity:** **Critical** (data is single-replica even when RF=3 is set)
**Location:** [crates/dtfs-manager/src/placement.rs](crates/dtfs-manager/src/placement.rs) + chunk-write coordinator
**Observed:** With `[defaults.replication] hot = 3` in `manager.toml` and 4 healthy data nodes, I wrote a 245 MB file through FUSE. Cluster reports `chunk_count: 2`, `used_capacity: 256 MB`. But on the data nodes:

| Node | /mnt/disk-a usage |
|---|---|
| data-01 | 345 MB (baseline) |
| data-02 | 345 MB (baseline) |
| **data-03** | **590 MB** ← +245 MB |
| data-04 | 345 MB (baseline) |

Both chunks (`38ee…`, `7af7…`) landed only on data-03. No replicas anywhere. The expected behaviour was 3 copies of each chunk on 3 different nodes (total 6 chunk-files, 735 MB raw).

After data-03 was stopped, reading the file failed immediately with `Input/output error` — confirming there is no second replica to fall back to.

**Fix recommendation:** The placement engine assigns a single target node per chunk at write time. It needs to either (a) place N replicas synchronously and only ACK the client once all min-required replicas are written, or (b) write the primary copy and immediately schedule N-1 follow-on replication tasks. Option (a) is what `defaults.write_safety.min_replicas_before_ack = 2` *implies* should happen but evidently doesn't.

---

## F-011 — Read throughput is ~50× slower than write throughput

**Severity:** High (any read-heavy workload is unusable)
**Location:** Likely [crates/dtfs-fuse/src/](crates/dtfs-fuse/src/) read path + [crates/dtfs-data/src/](crates/dtfs-data/src/) chunk serving
**Observed:**
- Write 245 MB through FUSE: **0.97 s ≈ 252 MB/s**
- Read 245 MB through FUSE: **49.3 s ≈ 5 MB/s** (50× slower)
- Both files are on the same 4 lvm-thin disks on the same Proxmox host (10 Gbit virtual NIC, no I/O contention).
- md5 of the read file matches source, so the data is correct; only the speed is bad.

**Fix recommendation:** Profile the read path. Suspects: (1) per-chunk RPC setup cost (each 128 MB chunk should stream, not fetch in tiny batches), (2) the FUSE read window is small and not multi-buffered, (3) the readahead module isn't actually fetching ahead. Compare server-side throughput (`time curl http://data-XX:9500/.../chunk/<id>`) against client throughput to localise the bottleneck.

---

## F-012 — Chunk placement does NOT filter out nodes marked Down

**Severity:** **Critical** (writes during a partial outage silently lose data)
**Location:** [crates/dtfs-manager/src/placement.rs](crates/dtfs-manager/src/placement.rs)
**Observed:** With data-03 marked `health: "Down"` in `/api/v1/nodes`, I wrote `post-fail.txt`. FUSE log shows:
```
WARN dtfs_fuse::fuse_adapter: fuse3 flush fh=9: write_chunk failed for chunk_index 0 on http://data-03:9500
```
The manager assigned the chunk to data-03 anyway, the data node was unreachable, and the write failed at the data plane while the file was already created in the metadata plane. End state: `chunk_count: 0, total_size: 0` in the cluster's `/api/v1/files`.

**Fix recommendation:** `PlacementEngine::allocate_chunks` must exclude nodes whose health is not `Healthy`. Add a unit test that runs `allocate_chunks` against a topology where one node is `Down` and asserts that node is never chosen.

---

## F-013 — Silent data loss when chunk write fails after metadata-create succeeds

**Severity:** **Critical** (data appears to be saved but isn't)
**Location:** [crates/dtfs-fuse/src/filesystem.rs](crates/dtfs-fuse/src/filesystem.rs) write/flush flow
**Observed:** Continuing F-012: even though the underlying chunk-write failed, `cat /mnt/dtfs/post-fail.txt` immediately afterwards returned the expected content. But the cluster has the file at 0 bytes. The data only exists in the FUSE process's write buffer; once that's flushed/evicted/closed it's gone. The user is given the impression their data was saved successfully.

**Fix recommendation:** FUSE flush should propagate the underlying chunk-write error back to the caller. A `write` that ends in a 0-chunk metadata entry needs to fail loudly: either the create-file RPC should be rolled back, or the FUSE `release`/`fsync` should return -EIO so applications retry or alert.

---

## F-014 — File timestamps are always epoch (1970-01-01) on FUSE

**Severity:** Low (cosmetic but breaks tools that compare mtimes)
**Location:** [crates/dtfs-fuse/src/filesystem.rs](crates/dtfs-fuse/src/filesystem.rs) `getattr`
**Observed:** `ls -lh /mnt/dtfs/ep1.mkv` shows `Jan  1  1970`. The cluster's `/api/v1/files` returns valid `created_at` and `updated_at` Unix timestamps (e.g. `1779007032840`), so the data exists — it's just not being mapped into the FUSE `getattr` reply.

**Fix recommendation:** In the FUSE adapter's `getattr`/`readdir_plus` handler, populate `attr.atime`/`mtime`/`ctime` from `file.updated_at` / `file.created_at`.

---

## F-015 — Re-replication retries the original (failed) source forever, even after a healthier replica exists

**Severity:** High (self-healing is best-effort but stalls in practice)
**Location:** [crates/dtfs-manager/src/scheduler.rs](crates/dtfs-manager/src/scheduler.rs)
**Observed:** With data-03 down and chunks single-copy on data-03, the manager **correctly** detected under-replication and emitted:
```
Auto-scheduled re-replication (replica count 1 < 3) task_id=repl-3
  chunk=38ee… source=6909250697178733005 target=2763275014011927717
Migration: Pending -> Copying task_id=repl-3
```
That's the good news — the scheduler logic is real and works.

The bad news: the task fixed `source = data-03` at scheduling time. After data-03 came back online, the same task continued to fail (`connect to source http://data-03:9500: transport error`) — the scheduler retries the same source forever rather than re-evaluating where the chunk *currently* lives.

Even after data-03 was back for several minutes, disk usage on data-01/02/04 stayed at the baseline 345 MB. No chunk ever made it off data-03.

**Fix recommendation:** Before each retry attempt of a `Copying` migration, re-query the chunk's current replica set and pick any healthy source. Better: have the scheduler treat the source as a *capability* (any node that has the chunk), not as a fixed assignment.

---

## F-016 — Re-replication only fires after a node is marked Down (~30 s), not on under-replication discovered any other way

**Severity:** Medium (writes that land at RF<target are not retroactively repaired until a node fails)
**Location:** [crates/dtfs-manager/src/scheduler.rs](crates/dtfs-manager/src/scheduler.rs) auto-schedule trigger
**Observed:** I wrote ep1.mkv at RF=1 (F-010). It sat at RF=1 for 10+ minutes while all data nodes were healthy. The scheduler never noticed until I forcibly took data-03 down — only at that point did the "replica count 1 < 3" tasks fire. Combined with F-010, this means new files start under-replicated and stay that way.

**Fix recommendation:** Run an under-replication scan as a periodic background job (e.g. every 60 s walk all chunks, count replicas, schedule repair for any below target). Don't rely on node-down events.

---
---

# Abuse-suite findings — 2026-05-17 (F-020 .. F-028)

The findings below come from the dedicated abuse-test suite at [tests/proxmox/abuse/](abuse/), run after F-001..F-019 were fixed. Suite totals: **48 cases / 28 PASS / 16 FAIL / 3 WARN / 1 SKIP**, wall-clock 431 s. Raw results in `/tmp/dtfs-abuse-results.tsv` on the controller. The seven path/disk/soak failures all cascade from F-024 — once a single critical bug is fixed, those re-run cleanly.

**Status (post-fix):** all 9 findings below were fixed in the F-020..F-028 fix campaign and the abuse suite now reports **37 PASS / 9 FAIL / 1 WARN / 1 SKIP** — the remaining FAILs are pre-existing F-04 ENOSYS gaps plus a single cascading limitation noted in [FIX-REPORT-F020-F028.md](FIX-REPORT-F020-F028.md). See that report for end-to-end verification per finding.

## F-020 — FUSE `chmod` returns success but file mode is unchanged

**Severity:** Low (POSIX compliance, no data risk)
**Location:** [crates/dtfs-fuse/src/fuse_adapter.rs](crates/dtfs-fuse/src/fuse_adapter.rs) `setattr`
**Observed (abuse T2.10):**
```
$ sudo chmod 600 /mnt/dtfs/chmod-test.txt; echo rc=$?
rc=0
$ stat -c %a /mnt/dtfs/chmod-test.txt
644
```
The syscall succeeds (rc=0) but the mode is silently ignored — `setattr` returns OK without persisting. Worse than ENOSYS: callers get a green light and assume the change took effect.

**Fix recommendation:** Either persist mode in the manager metadata (probably overkill — the cluster does not enforce per-file UNIX permissions) or return ENOSYS so callers can detect the no-op. Pretending to succeed is the worst of both options.

---

## F-021 — FUSE `O_APPEND` overwrites the file instead of appending

**Severity:** Medium (silent data loss — common log/stream pattern)
**Location:** [crates/dtfs-fuse/src/filesystem.rs](crates/dtfs-fuse/src/filesystem.rs) `open` / `write`
**Observed (abuse T2.12):**
```
$ echo line1  > /mnt/dtfs/append.txt
$ echo line2 >> /mnt/dtfs/append.txt
$ wc -l /mnt/dtfs/append.txt
1 /mnt/dtfs/append.txt   # only "line2" present
```
Opening with `O_APPEND` and writing should always append at end-of-file. Our FUSE writes at offset 0 (or wherever the kernel sets the offset), clobbering the prior content.

**Fix recommendation:** In `open`, record the `O_APPEND` flag in the open-file state. In `write`, if O_APPEND is set, ignore the kernel-provided offset and append to the current file size (re-fetched from manager metadata before each write to maintain semantics under concurrent writers).

---

## F-022 — FUSE seek+write produces a truncated file (sparse semantics broken)

**Severity:** Medium (databases, VM images, anything mmap'd or seeked rely on this)
**Location:** [crates/dtfs-fuse/src/filesystem.rs](crates/dtfs-fuse/src/filesystem.rs) `write`
**Observed (abuse T2.13):**
```
$ truncate -s 0 /mnt/dtfs/sparse.bin
$ dd if=/dev/zero of=/mnt/dtfs/sparse.bin bs=1 count=1 seek=1048576
$ stat -c %s /mnt/dtfs/sparse.bin
1                  # expected 1048577 (sparse hole + 1 byte)
```
Seeking past EOF and writing should leave a hole or zero-fill the gap. Our write only persists the 1 byte and reports size=1, ignoring the seek offset.

**Fix recommendation:** In `write`, treat the offset as authoritative: if offset > current size, either (a) zero-fill the gap into the chunk buffer or (b) record a per-chunk "logical size" with sparse-hole tracking. Option (a) is simpler and sufficient for typical workloads.

---

## F-023 — FUSE directory listing is stale immediately after writes

**Severity:** Medium (any workflow that writes a file then immediately enumerates the directory)
**Location:** FUSE entry/attribute cache TTLs in [crates/dtfs-fuse/src/fuse_adapter.rs](crates/dtfs-fuse/src/fuse_adapter.rs) (`ENTRY_TTL` / `ATTR_TTL`) and the local inode/path map
**Observed (abuse T3.1, T3.2, T4.2):**
```
client$ for i in $(seq 1 50); do echo file-$i > /mnt/dtfs/small-$i.txt; done; sync
client$ ls /mnt/dtfs/small-*.txt | wc -l
0
mgr$    curl /api/v1/files | jq '[.files[]|select(.path|startswith("/small-"))]|length'
50      # ← the cluster has the files; FUSE just doesn't see them
```
The writes do complete (the manager API correctly reports all 50 files moments later, and after a brief wait `ls` on the same client also returns 50). But the readdir cache TTL is long enough that `ls` immediately after the write loop returns the stale (pre-write) directory listing.

**Fix recommendation:** Invalidate the parent-directory readdir cache (or its ATTR / ENTRY TTL) on every `create` / `mknod` / `unlink` / `rmdir`. fuse3 supports issuing a kernel-cache invalidation; alternatively use a very short TTL for directories (sub-second) and longer TTL for individual file attributes.

---

## F-024 — Data nodes lose registration after manager rolling restart (cluster shows 0 nodes)

**Severity:** **Critical** (full-cluster outage triggered by `systemctl restart` rolling across managers)
**Location:** [crates/dtfs-data/src/main.rs](crates/dtfs-data/src/main.rs) (heartbeat reconnect logic) + [crates/dtfs-manager/src/server.rs](crates/dtfs-manager/src/server.rs) (`RegisterNode`) + Raft FSM apply path for node state
**Observed (abuse T6.4 + post-test cluster probe):**

After stopping mgr-02 and mgr-03 (T6.3), restarting them, and verifying T6.4 with `/api/v1/cluster/status`:
```
mgr-01:  data_nodes: 0  data_nodes_healthy: 0  total_capacity: 0
mgr-02:  data_nodes: 0  data_nodes_healthy: 0
mgr-03:  data_nodes: 0  data_nodes_healthy: 0
```
All four data nodes' systemd units are `active`, and at least two (data-03, data-04) logged `INFO dtfs-data: Registered with manager, assigned node_id: …` after the manager restart — yet the cluster view stays at zero nodes. Repeated heartbeats then fail:
```
WARN dtfs_data: Heartbeat failed (1/3 before reconnect): status: Unavailable, message: "tcp connect error"
```

Two probable contributors:
1. Each data node knows only **one** `manager_addr` (the bootstrap manager), so it cannot fail over when that manager is the one that restarted.
2. Even re-registrations don't show up in `/api/v1/cluster/status` — the in-memory node table appears to be lost on restart and the Raft snapshot doesn't replay it. (`config_version: 0` on all three managers suggests state machine started empty.)

Every subsequent test (T7-T10) failed because the cluster could not accept any writes. This is the highest-impact finding in the run.

**Fix recommendation:**
- Allow `manager_addr` to be a list, retry across managers on heartbeat failure.
- Investigate why node registration is not surviving manager restart — either the FSM apply path for `RegisterNode` is not durable, or the snapshot/log replay drops it.
- Add a periodic node-state reconciliation: any data node still heartbeating should be visible in `/api/v1/nodes` within one heartbeat interval, regardless of manager restart history.

---

## F-025 — `rm` via FUSE does not propagate to manager metadata

**Severity:** Medium (orphan metadata accumulates; chunks may also be leaked)
**Location:** [crates/dtfs-fuse/src/fuse_adapter.rs](crates/dtfs-fuse/src/fuse_adapter.rs) `unlink` → manager `DeleteFile`
**Observed (abuse T3.6):** Wrote 50 tiny files, then `rm /mnt/dtfs/small-*.txt`. FUSE `ls` correctly shows them gone. But:
```
$ curl /api/v1/files | jq '[.files[]|select(.path|startswith("/small-"))]|length'
50          # ← all 50 still indexed in the manager
```
The directory cleared from FUSE's view, but cluster metadata kept all 50 entries.

**Fix recommendation:** Confirm `unlink` actually issues `DeleteFile` for every removed file. The likely culprit is a missed RPC when the shell glob (`small-*.txt`) is unlinked via FUSE's batch behaviour — each iteration's `release` may be acked locally without the manager being notified. Add a regression test that fails CI if `/api/v1/files` doesn't reflect FUSE deletes within 5 s.

---

## F-026 — FUSE deadlocks under sustained small-file load on a single-vCPU client

**Severity:** **Critical** (any workload generating many small files for an extended period wedges the mount)
**Location:** [crates/dtfs-fuse/src/filesystem.rs](crates/dtfs-fuse/src/filesystem.rs) + [crates/dtfs-fuse/src/fuse_adapter.rs](crates/dtfs-fuse/src/fuse_adapter.rs) (concurrency model)
**Observed (initial abuse run, N=500):**

After `for i in $(seq 1 500); do echo file-$i > /mnt/dtfs/small-$i.txt; done` followed by sequential reads of every file:
```
client-01$ sudo cat /proc/<reader-pid>/stack
[<0>] folio_wait_bit_common+0x132/0x320
[<0>] filemap_get_pages+0x6c6/0x6f0
[<0>] filemap_read+0xf0/0x370
[<0>] vfs_read+0x244/0x360

client-01$ sudo cat /proc/<dtfs-fuse-pid>/task/*/wchan
futex_wait_queue
futex_wait_queue
```
The dtfs-fuse process is alive but completely idle — both worker threads asleep on a futex. Every subsequent operation on `/mnt/dtfs` (including a fresh `dd` from a different test) blocks indefinitely in `request_wait_answer`. Last completed FUSE log entry was a successful chunk read for `/small-248.txt`; nothing after.

Recovery requires `fusermount3 -uz` + `systemctl restart dtfs-fuse@…`.

The client is 1 vCPU / 2 GB RAM — Tokio's default runtime picks `worker_threads = num_cpus = 1`, so a single internal task holding a lock can stall the entire mount. The deadlock did not reproduce with N=50 in the follow-up run, so the threshold is somewhere between 50 and 500.

**Fix recommendation:**
- Force `tokio::runtime::Builder::new_multi_thread().worker_threads(2_or_more)` at minimum — a single-threaded runtime in a FUSE adapter is a footgun.
- Audit `std::sync::Mutex` usage in FUSE hot paths (especially `open_files`, `inode_table`, readahead cache) — replace with `tokio::sync::Mutex` or `parking_lot` and add timeouts/poisoning detection.
- Stress test: include an N=500-tiny-files round-trip in the integration suite so this regresses loudly.

---

## F-027 — FUSE `readdir` returns each entry twice (`ls` shows duplicates)

**Severity:** Low (visual / shell-script breakage; no data integrity impact)
**Location:** [crates/dtfs-fuse/src/fuse_adapter.rs:473](crates/dtfs-fuse/src/fuse_adapter.rs) (`readdir` / `readdirplus` offset accounting)
**Observed:**
```
client$ sudo ls -1 /mnt/dtfs/
ep1.mkv
ep1.mkv
testdir
testdir
client$ sudo ls -la /mnt/dtfs/ | grep -v ^total | wc -l
2          # stat-based listing dedups; raw readdir does not
client$ curl /api/v1/files                    # manager has 2 entries; not the bug
```
Manager-side `list_directory` correctly returns 2 entries (verified by reading the API). The duplication happens in FUSE's `readdir`/`readdirplus`. The offset cookie returned for entries (`(idx as i64) + 3` in `readdir`, `(idx as u64) + 1` in `readdirplus`) and the skip condition (`entry_offset <= offset`) interact such that a paginated readdir re-emits the last entry on the second call.

**Fix recommendation:** Audit and unify the offset/skip semantics. For fuse3's `ReplyDirectory` the `offset` carried in each entry is the cookie the kernel will pass back on the *next* call — meaning the skip predicate must be strict (`entry_offset < offset`, not `≤`). Add a unit test in the FUSE adapter that simulates a paginated readdir of N entries and asserts exactly N entries are emitted across all calls.

---

## F-028 — FUSE mount is root-only (no `allow_other`); unprivileged processes get EACCES

**Severity:** Low–Medium (operational — most realistic media-server use cases run unprivileged)
**Location:** FUSE mount options in [crates/dtfs-fuse/src/main.rs](crates/dtfs-fuse/src/main.rs)
**Observed:** `mount | grep dtfs` shows `fuse (rw,relatime,user_id=0,group_id=0)` — no `allow_other`. A non-root user gets `Permission denied` on `ls /mnt/dtfs/` even though the root directory is mode 0755.

**Fix recommendation:** Add a `[fuse] allow_other = true` config option (default false) and pass `MountOptions::default().allow_other(true)` to fuse3. Document the `/etc/fuse.conf` requirement (`user_allow_other`) in deploy docs. Mount permission should match a regular Linux filesystem so that user-owned services (e.g. a media server running under its own UID) can read/write.

---

### Summary of new findings

| ID | Severity | Area | One-line |
|---|---|---|---|
| F-020 | Low | FUSE setattr | chmod silently no-ops (rc=0, mode unchanged) |
| F-021 | Medium | FUSE write | O_APPEND overwrites instead of appending |
| F-022 | Medium | FUSE write | seek-past-EOF truncates file to written bytes only |
| F-023 | Medium | FUSE cache | readdir stale immediately after writes |
| F-024 | **Critical** | Manager + Data | rolling manager restart drops every data node from cluster view |
| F-025 | Medium | FUSE unlink | `rm` doesn't propagate to manager metadata |
| F-026 | **Critical** | FUSE concurrency | Deadlock under sustained tiny-file load on 1-vCPU client |
| F-027 | Low | FUSE readdir | each entry returned twice |
| F-028 | Low–Med | FUSE mount | no `allow_other`; non-root EACCES |

---
---

# POSIX-compliance probe — 2026-05-17 (F-029 .. F-031)

User asked: "Fully POSIX compliant? SQLite locking functionality etc must be verified." Short answer: **no, not fully** — but the first three things that break SQLite in any meaningful way have now been fixed. See [FIX-REPORT-F029-F031.md](FIX-REPORT-F029-F031.md) for verification details and the remaining POSIX gaps.

## F-029 — Reads do not see writes on the same fd (write-buffer not consulted on read)

**Severity:** **Critical** (every database engine breaks; SQLite returns "no such table" right after `CREATE TABLE` in the same session)
**Location:** [crates/dtfs-fuse/src/filesystem.rs](../../crates/dtfs-fuse/src/filesystem.rs) `read` + `flush`
**Observed:** A minimal python probe issues `write(16K)`, `flush()`, `fsync()`, `seek(0)`, `read(16K)` on a single fd and gets back 0 bytes. The 16 K landed in our process-local `write_buffer` but our `read` consulted only the cluster-side chunk locations (which still showed `total_size=0` for a brand-new file), so the read short-circuited to `Bytes::new()`.

```
=== Test 2: write 16K, then read 16K via python on the SAME fd ===
wrote=16384 read=0 match=False
=== Test 4: pwrite at offset, pread same offset (SQLite pattern) ===
page0 match=False (read b'', expected b'XXXXXXXX')
```

**Fix:** `WriteBuffer::overlay()` plus an `effective_size = max(locations.total_size, buffered_eof)` clamp in `read`. Also `flush()` now re-fetches locations from the manager and updates every `OpenFile.locations` for that path, so the next read sees the freshly-committed chunks.

**Verification:** [tests/proxmox/sqlite/](sqlite/) probe suite (4 coherency cases now all pass) and a new `writebuf::tests::overlay_returns_buffered_bytes_to_reader` unit test.

---

## F-030 — Re-writing a chunk leaves the previous content as a stale row in the FSM

**Severity:** **Critical** (SQLite INSERT 2/3/4… invisible after reopen; every "update in place" workload silently keeps both versions)
**Location:** [crates/dtfs-raft/src/lib.rs](../../crates/dtfs-raft/src/lib.rs) `merge_chunk_location_entries`
**Observed:** Even with F-029 fixed, three sequential INSERTs into a SQLite DB on `/mnt/dtfs` produced only one row visible after a reopen. The first INSERT writes chunk-0 with content hash `aa`. The second INSERT writes chunk-0 with content hash `bb`. The merge function was keyed by `(chunk_index, chunk_hash, node_id)`, so the `aa` rows were *not* removed — the file ended up with chunks `[aa]` AND `[bb]` at index 0, and `get_file_locations` returns whichever sorts first in the BTreeMap. Half the reads got the pre-rewrite content.

**Fix:** When the incoming batch contains entries for a given `chunk_index`, drop *all* existing entries at that index, then insert the new replicas. The FUSE client always sends the complete new replica set per chunk at flush time, so this is safe.

**Verification:** Two new unit tests under `merge_tests` cover replica-replacement-on-rewrite and "untouched chunks are preserved". SQLite probe 3 (single-process INSERT batch + reopen) and probe 4 (default rollback journal + WAL mode) now produce the correct row counts on reopen.

---

## F-031 — Cross-process `fcntl` byte-range locking not coordinated by FUSE

**Severity:** Medium (multi-process apps on the same client cannot safely share files via POSIX locks)
**Location:** [crates/dtfs-fuse/src/fuse_adapter.rs](../../crates/dtfs-fuse/src/fuse_adapter.rs) — added `setlk` / `getlk` handlers under the fuse3 `file-lock` feature.
**Observed:** Two concurrent `sqlite3` invocations against the same DB file from the same host: ~88 of 100 INSERTs failed with `database is locked` because the kernel's default fuse fcntl handler grants every process the lock locally — they all think they hold the writer lock and SQLite's optimistic transaction commit then rolls many of them back.

**Fix (partial):** Implemented an in-memory per-inode lock table in the FUSE adapter that handles non-blocking `F_SETLK` correctly (conflicts return `EAGAIN`, owners can re-enter, unlock trims partial overlaps). After the fix a single-process SQLite (any number of statements / WAL mode / synchronous=FULL) works end-to-end. Multi-process SQLite still requires either application-level `busy_timeout` (which lets the BUSY retry loop converge) or a future enhancement to forward locks to the manager so other clients across the cluster respect them — see the gap list in [FIX-REPORT-F029-F031.md](FIX-REPORT-F029-F031.md).

---

### Remaining POSIX gaps (not fixed in this pass)

| Gap | Impact |
|---|---|
| `rename` returns ENOSYS (F-04 carry-over) | Atomic file replacement / WAL switchover impossible |
| `setattr` truncate-shrink ignored | `truncate -s 0 file` / file growth via `truncate -s N` doesn't shrink |
| `symlink` / `readlink` ENOSYS | No symbolic links; breaks lots of build tooling |
| Hard links not supported (`link()` ENOSYS) | SQLite WAL recovery patterns, atomic rename emulation |
| `fallocate` / `posix_fallocate` ENOSYS | Pre-allocating database files (BoltDB, LMDB) fails or is slow |
| Extended attributes (xattr family) ENOSYS | SELinux contexts, capabilities, custom metadata |
| Cross-host `fcntl` locks not coordinated by manager | Shared databases between FUSE clients are unsafe |
| `F_SETLKW` (blocking lock) returns `EAGAIN` instead of waiting | Apps relying on the kernel to block must add their own retry loop |

The current fix set unblocks **single-client SQLite** (the most common dependency we expect on a media-file FS) and turns "ls/stat after write" into deterministic behaviour. Full POSIX-1-2024 compliance is a multi-week roadmap.
