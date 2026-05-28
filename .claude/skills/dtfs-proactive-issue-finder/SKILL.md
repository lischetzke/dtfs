---
name: dtfs-proactive-issue-finder
description: >-
  Inspect a running DTFS cluster and report what's wrong. Use whenever someone
  wants to know if a DTFS cluster is fine — "check the cluster", "is it
  healthy/OK", "make sure nothing's broken", "anything looking off", "do a
  health sweep", "scan the logs", "run the proactive issue finder" — or kicks
  off a monitoring/triage shift with no specific symptom named. Works on any
  DTFS cluster: production, LELI, Minicluster, by name or IP. Also use to confirm
  one live concern across the cluster: migrations stuck or looping, raft
  leader/quorum and manager catch-up, node/disk/volume/capacity usage,
  replication or anchor-policy compliance, integrity or corruption faults.
  Strictly read-only: gathers state via the manager REST API, dtfs-cli, and SSH
  to node logs, and produces a severity-ranked findings report to hand off to the
  triage/intake session; never changes code, config, or cluster state. Do NOT use
  to fix bugs, write or run plans, run tests, debug a single past error, or
  create/modify volumes.
---

# DTFS Proactive Issue Finder

You are the **proactive issue finder** session for DTFS. Two sibling sessions
exist: a triage/intake session that turns reported issues into plans, and an
implementation session that fixes them. Your job is to *find* the issues they
will act on. You discover and report; you do not fix.

## Prime directive: read-only

This session is strictly observational. You may read state, read logs, and run
queries that have no side effects. You must not:

- Edit, create, or delete any source/config file under `crates/`, `proto/`,
  `tests/`, `scripts/`, `deploy/`, or anywhere else in the repo, **except** the
  single findings report you write (see "Output").
- Run any mutating cluster operation: no `dtfs-cli scrub`, no `flush-cache`, no
  `maintenance`, no `volumes create/set-*/migrate-policy`, no POST endpoints, no
  `systemctl restart`, no writes through a FUSE mount.
- Commit, build, or deploy. That belongs to the implementation session.

If a check would require a mutation to confirm, do not perform it — record it as
a hypothesis in the report for the triage session to schedule.

Why this matters: three sessions share one cluster. If the finder mutates state,
it corrupts the very signal it is trying to read and races the other two
sessions. Discipline here keeps the division of labour clean.

## How to access the cluster

You have three read paths. Prefer the cheapest that answers the question, and
cross-check when something looks wrong.

1. **Manager REST API** (primary, no SSH needed). Default manager API is
   `http://<mgr>:9402`. JSON is the machine-readable form — append nothing
   special, the endpoints return JSON. Use `curl -s`.
2. **dtfs-cli** (convenience wrapper over the same API). `dtfs-cli --manager
   <host:9402> --format json <cmd>`. Use `--format json` so output is parseable.
   Only the read subcommands: `status`, `nodes`, `volumes list`, `volumes show`,
   `files list`, `audit`, `config`, `health`. **Never** `scrub`, `maintenance`,
   `files flush-cache`, or any `volumes set-*` / `create` / `migrate-policy`.
3. **SSH to nodes** for logs the API does not expose. Lab is `10.150.0.0/24`,
   key `~/.ssh/dtfs-lab/id_ed25519`. Daemons run under systemd as
   `dtfs-manager` and `dtfs-data`; read their logs with
   `journalctl -u dtfs-manager --no-pager -n <N>` (read-only). Never restart a
   unit or edit a config on the node.

See `references/cluster-surfaces.md` for the full endpoint/command/log cheat
sheet, the audit + integrity action vocabulary, and the JSON field names each
surface returns. Read it before running the sweep so you cite the right keys.

## What "expected" means

Findings are deviations from expectation. You have two baselines and must
cross-check both, because either can be the one that drifted:

- **Documented topology** — the Plexmedia reference deployment in
  `AGENTS.md` § A.1 (3 DCs: LELI/TORO/NRB; node roles, storage classes,
  bandwidth, and the **anchor invariant**: every chunk has at least one replica
  on LELI `data-primary01`). This is the intended shape of the *production*
  cluster. Test/lab clusters differ (e.g. the "Minicluster": `mini-mgr*` /
  `mini-data*` hosts across `dc-a`/`dc-b`), so do NOT hard-fail on the literal
  Plexmedia hostnames — first read `cluster_name` from `/api/v1/cluster/status`
  and treat the topology check as: managers form a quorum, every data node is
  healthy, and the **anchor invariant holds for whatever anchor DC the volumes
  actually declare**. The generic invariant (an anchored volume keeps its
  mandatory copy in its anchor DC/tier) applies on every cluster; the specific
  LELI/data-primary01 names apply only when `cluster_name` is the production one.
- **Live volume configs** — each volume's own `mandatory_replicas`,
  `cache_replicas`, and `anchor` policy from `dtfs-cli volumes show <id>
  --format json` (or `GET /api/v1/volumes/<id>`). This is what the cluster
  currently believes its own rules are.

When live state contradicts the documented topology, flag it. When the volume
config itself contradicts the documented invariants (e.g. an anchor policy that
does not pin LELI, or a mandatory cold count of 0 on a volume that should hold
an anchor), flag the config drift too. Do not assume one baseline is ground
truth; report which one you measured against.

## The sweep

Run these passes in order. Gather first, interpret second — collect all the raw
state, then reason across it (a single symptom often shows up in three places).

### 1. Manager / data-node state

- `dtfs-cli status` / `GET /api/v1/cluster/status`: cluster name, status, data
  node count vs healthy count, total/used capacity, config version.
- `GET /api/v1/health` and `GET /api/v1/raft/state`: is there a leader? Are all
  managers (mgr01-05) present and caught up? A missing or lagging manager, or a
  cluster that cannot hold quorum, is high severity.
- `dtfs-cli nodes --format json` / `GET /api/v1/nodes`: every node's health,
  role, datacenter, disk count, capacity, used. Then per-suspect node
  `GET /api/v1/nodes/<id>` for per-disk tier/usage/SMART/status.
- Compare the live node roster against the Plexmedia roster: are mgr01-05,
  data-primary01, data-warm01, data-warm02 all present and in the right DC with
  the right role? A node that is **quarantined**, in **maintenance**, or
  **unhealthy** is a finding. A node that is *missing entirely* is a bigger one.

### 2. Cluster / volume / disk usage vs expected

- Per-disk: `used_bytes / total_bytes`. Flag disks near full (warm SSDs at TORO
  are only ~1 TB — they fill fast). Flag a disk whose `status` is not healthy.
- Per-volume: `dtfs-cli volumes show <id> --format json`. Confirm
  `mandatory_replicas` and `anchor` match the documented invariants. Confirm the
  anchor DC and tier are set where the topology requires it (LELI/cold on
  production; whatever the volume declares on a lab cluster). **Cross-check every
  mandatory tier against the disk tiers that actually exist** in the cluster
  (`disks[].tier` from `/api/v1/nodes/{id}`): a `mandatory_replicas.tape=1` on a
  cluster whose data nodes only expose Warm+Cold disks is unsatisfiable and will
  queue permanent under-replication the moment the volume gets a file — flag it
  even when `file_count` is currently 0. Also flag drift *between* volumes (one
  volume anchored, a sibling with `anchor=null`).
- Capacity sanity: does `used_capacity` roughly match the sum of file sizes and
  the replication factor? A large gap can mean orphaned chunks (disk used by
  data no file references) or under-replication (files exist but replicas
  missing). Both are findings.
- Anchor coverage: if the cluster reports chunks without a LELI replica, or
  the anchor-lag repair queue is non-empty and not draining, that is a direct
  violation of the prime invariant — high severity.

### 3. Logs: node, audit, migration, integrity

- **Audit log**: `dtfs-cli audit --format json --limit 200` /
  `GET /api/v1/audit`. A successful row's `result` is `ok` **or** `success` —
  do not flag those. Scan instead for genuine failure results
  (`error`, `failed`, `refused`, `denied`) and for *patterns*: the same
  `action`+`target` repeating many times in a short window (e.g. one node
  emitting dozens of `RegisterNode` rows usually means a restart/rejoin loop,
  not a healthy register-once). Pay special attention to
  `cache.flush_refused` — it signals an attempted flush that would have stranded
  a mandatory copy; investigate why a flush was attempted and whether the anchor
  is at risk.
- **Integrity feed**: `GET /api/v1/integrity/faults`. Surfaces
  `scrub.quick.fault`, `scrub.deep.corrupt`, `read.integrity_fault`,
  `orphan.in_state_missing`, `node.quarantine` / `node.unquarantine`. Any
  corruption or read-integrity fault is high severity (data integrity is the
  prime directive — see AGENTS.md § A.3.3). A flapping node
  (quarantine/unquarantine churn) is its own finding.
- **Migrations**: `GET /api/v1/migrations`. Each task has `status`, `attempts`,
  `last_error`, `source_node`, `target_node`, `chunk_id`. Flag: any
  `status=failed` task (especially with rising `attempts`), tasks lingering whose
  `last_error` says the chunk no longer exists (orphan tasks that were never
  reaped), a queue that never drains across two reads minutes apart, and any task
  where `source_node == target_node` (a replication/anchor task copying a chunk
  to the node it already lives on is suspect). A migration loop that never
  completes is a known failure mode (anchor-lag repair on multi-tier topologies
  — AGENTS.md § A.3.4); if you see the same chunk re-queued repeatedly, flag it
  with the chunk id. Distinguish bandwidth-bound-but-progressing from truly
  stuck/looping.
- **Node daemon logs** (SSH + `journalctl`): on any node the above flagged,
  read recent `dtfs-manager` / `dtfs-data` logs for panics, repeated errors,
  WAL-recovery noise, gRPC failures, or memory-pressure / shedding messages
  (the Bug G ladder — AGENTS.md § A.5). Quote the exact log line.

## Output: a findings report, handed to triage

You produce **one report** and nothing else. Write it to
`plans/reports/ISSUE-SCAN-<YYYY-MM-DD-HHmm>.md` (this is the only file you
create; `plans/reports/` is the canonical home for historical lab/scan
artefacts per AGENTS.md § B.6). Do **not** create plans in `plans/active/` and
do **not** touch `ISSUES_TO_FIX.md` — the triage session owns turning these
findings into plans.

For each finding, give the triage session everything it needs to act without
re-running the sweep:

```
## Finding <n>: <one-line title>

- Severity: critical | high | medium | low | info
- Area: node | disk | volume | replication | anchor | integrity | migration | memory | raft
- Evidence: <exact command(s) run + the output snippet / log line that shows it>
- Expected: <what the Plexmedia topology or the volume config says should hold>
- Actual: <what you measured>
- Hypothesis: <likely cause, or "unknown — needs investigation"; never guess as fact>
- Suggested next step: <what the triage session should scope; NOT a code change you make>
```

Severity rubric:

- **critical**: active or imminent data loss / unavailability — corruption
  confirmed, anchor invariant violated, no Raft leader, a disk failed with
  under-replicated chunks on it.
- **high**: integrity faults, node quarantine, stuck/looping migrations,
  near-full warm disks, mandatory replicas below configured floor.
- **medium**: drift between live state and documented topology that is not yet
  hurting (a node in maintenance longer than expected, orphan chunks wasting
  space, config drift).
- **low / info**: cosmetic or informational; worth noting, not urgent.

End the report with a short summary line (counts per severity) and an explicit
handoff note: "Hand to triage session for planning." If the sweep finds nothing,
say so plainly with the evidence that each pass was clean — a clean report is a
valid and useful result.

## Discipline reminders

- Read before asserting. Verify endpoint shapes and field names against
  `references/cluster-surfaces.md` and the live JSON; do not invent keys.
- Cite evidence for every finding. A finding without a command + output is a
  guess, and guesses waste the triage session's time.
- Cross-check across passes before concluding. One root cause (say, a dead data
  node) will surface as a node-health finding, an under-replication finding, and
  a migration-backlog finding — link them rather than filing three unrelated
  reports.
- Stay read-only. If you are ever unsure whether an action mutates state, do not
  run it; note it as something the triage/implementation session should check.
