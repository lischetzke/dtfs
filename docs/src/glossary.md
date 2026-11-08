# Glossary

Terms used across DTFS: its own names and the general distributed-systems words it builds on. Each entry links to the page that explains it in depth.

## Cluster and consensus

- **Anchor datacenter**: The datacenter that must hold at least one live copy of every chunk of a volume (set per volume, `anchor.datacenter`). In the reference deployment this is LELI. See [Replication](./storage/replication.md).
- **Apply**: The step where a manager executes a committed Raft log entry against its state machine. Apply is deterministic: no wall clock, no randomness, no I/O. See [Consensus](./architecture/consensus.md#log-replication).
- **ClientRequest**: One metadata command (create file, add chunk location, register node, ...) proposed to Raft. Every change to cluster metadata is a `ClientRequest`; nothing else mutates replicated state.
- **Commit (Raft)**: A log entry is committed once a quorum of voters has stored it. Only committed entries are applied.
- **Follower**: A voter that is not the leader. It replicates the leader's log, answers reads from its local state and forwards writes to the leader. The web dashboard on a follower points you at the leader for changes.
- **Leader**: The one voter that accepts writes for the current term. All metadata changes, repair scheduling and tiering decisions run on the leader. See [Leader Election](./architecture/consensus.md#leader-election).
- **Learner**: A manager that receives the Raft log but does not vote and does not count toward quorum. New managers join as learners and are then promoted to voters. See [Membership Changes](./architecture/consensus.md#membership-changes).
- **Manager**: The `dtfs-manager` daemon. A Raft peer that owns all metadata (files, chunk locations, nodes, volumes, leases), runs the repair scheduler and tiering engine on the leader, and serves gRPC (9401), the REST API and the web dashboard (9402). Chunk bytes never pass through a manager.
- **Membership**: The set of voters and learners Raft knows about. Changed only through the Raft membership API (`add-learner`, `promote`, `remove`), never by editing config.
- **Quorum**: A majority of voters (2 of 3, 3 of 5). Without a quorum no metadata can change: reads of cached data keep working, writes fail. See [Quorum Behavior](./architecture/consensus.md#quorum-behavior) and [Partition Behavior](./access/partition-behavior.md).
- **Raft**: The consensus protocol that keeps the managers' metadata identical. DTFS uses the openraft library. See [Consensus (Raft)](./architecture/consensus.md).
- **Raft log**: The ordered list of `ClientRequest` entries. The log is the source of truth; the state machine is derived from it.
- **Snapshot**: A serialized copy of the state machine at a log index. Lets the log be truncated and lets a lagging or new manager catch up without replaying everything. See [Snapshots](./architecture/consensus.md#snapshots).
- **State machine**: The in-memory metadata (files, chunk locations, nodes, volumes, leases) that every manager builds by applying the Raft log.
- **Term**: A Raft election epoch. Each new election increments the term; at most one leader exists per term.
- **Voter**: A manager that votes in elections and counts toward quorum. Run 3 or 5.

## Data model

- **Chunk**: A fixed-size piece of a file (default 4 MiB, set per volume). Chunks are immutable once written.
- **Chunk id**: The BLAKE3 hash of a chunk's bytes, written as hex. Because the id is the content hash, the same bytes always get the same id and any copy can be checked against it.
- **Data node**: The `dtfs-data` daemon. Stores chunk bytes on its local disks (one tier per disk), verifies and scrubs them, and streams them to clients and other data nodes over gRPC (9500).
- **Disk id**: The number of a data disk within its node, stored on the disk in `.dtfs-disk-id` on first start. A data node refuses to start if a `[[disks]]` entry would point a known disk at a different id.
- **File metadata**: A file's path, size, mode, volume and ordered list of chunk ids. Lives only in the Raft state machine.
- **Replica**: One copy of a chunk on one disk, identified by `(node_id, disk_id)`. Never by node alone: one node can hold an anchor copy and a cache copy of the same chunk on different disks.
- **Tier**: The class of a disk: `hot` (fast, small), `warm`, `cold` (large, slow, archive), `tape`. A chunk's replicas are placed and moved between tiers. See [Tiering](./storage/tiering.md) and [Tape Storage](./storage/tape.md).
- **Volume**: A named policy that files belong to: chunk size, mandatory replicas per tier, cache replicas, anchor datacenter, spill rules and write safety. Every cluster has a `default` volume. See [Volume Configuration](./configuration/volume-config.md).

## Replication and placement

- **Anchor copy**: The replica in the anchor datacenter. It is always one of the mandatory replicas, and no promote, demote, flush or repair may remove the last one.
- **Anchor pending**: A file with at least one chunk that has no anchor copy yet. Normal for a while after a write that landed outside the anchor datacenter (for example a download at NRB, replicating over the slow LELI link, or any write from a FUSE client with `[write] anchor_sync = false`, which acks on the ack floor of near copies before the anchor copy exists). The dashboard shows this as "Catching up", not as a failure.
- **Cache replica**: An extra, evictable copy created by the tiering engine near readers when a chunk is read often (`cache_replicas.<tier>.{min,max}`). Never counted for durability. See [Replication](./storage/replication.md#two-replication-classes-mandatory-vs-cache).
- **Demote / promote**: Removing a cache replica that is no longer needed, or creating one because of read demand. Both are done by the tiering engine and both check the anchor first.
- **Files missing chunks** (`files_missing_chunks`): Regular files whose committed data is gone: chunk data was committed once, and no chunk rows are left. This is data loss. Empty files and sparse files that were never written have no chunks and are not counted. A file from before v0.1.8 (unknown history) with no chunks still counts unless its size was explicitly set to 0, so real loss is never hidden. A truncate to 0 resets the history.
- **Flush cache**: An operator action that drops cache replicas under a path prefix ahead of time to reclaim space. Mandatory replicas are never touched.
- **Mandatory replica**: A replica required by the volume's `mandatory_replicas.<tier>`. Maintained only by the repair scheduler; this is the durability floor.
- **Migration**: An operator-requested move of the files under a path prefix to another tier or datacenter. A copy is verified on the target before the old one is removed. See [Migration](./storage/migration.md).
- **Placement state**: How a chunk's replicas compare to its volume's goal. One of:
  - **Stable**: matches the goal.
  - **Under-replicated** (code and API: `undergoal`): fewer live mandatory replicas than the goal; the repair scheduler adds copies.
  - **Over-replicated** (`overgoal`): more replicas than the goal; extras are removed once the anchor is safe.
  - **Misplaced**: the right number of copies, but on the wrong tier or datacenter.
  - **Missing**: no live replica at all. This is data loss.
- **Repair scheduler**: The leader-only background loop that keeps mandatory replicas at goal: it copies under-replicated chunks, retires extras, and is the only component allowed to remove a replica after the Raft removal has committed.
- **Spill**: Placing a replica on another tier or datacenter when the preferred one has no room, as allowed by the volume's spill rules. The anchor copy never spills out of the anchor datacenter.
- **Tiering engine**: The leader-only loop that promotes and demotes cache replicas from read statistics. It never creates or removes mandatory replicas. See [Tiering](./storage/tiering.md).
- **Write safety**: `min_replicas_before_ack`: how many distinct `(node, disk)` replicas on maintained tiers a write needs before it is acknowledged (filled across tiers, clamped only to what the cluster can hold). Below that the write fails rather than pretending to be durable. See [Write Safety](./storage/replication.md#write-safety).

## Health and operations

- **Decommission**: Planned removal of a data node: its replicas are moved off first, and the call is refused while any remain. See [Decommission and Replace](./deployment/decommission-replace.md).
- **Draining**: A node state set by the scheduler while a node is being emptied; it receives no new placements. Like Maintenance and Quarantined it is sticky: heartbeats do not clear it.
- **Heartbeat**: The periodic message every data node and FUSE client sends to the managers (default every 5 s). Missing heartbeats drive the health states below.

  - **Healthy**: heartbeats arriving, no faults.
  - **Suspect**: 3 heartbeats missed (default); still used, but watched.
  - **Down**: 6 heartbeats missed (default); no new placements, and its mandatory replicas are repaired elsewhere.
  - **Degraded** / **Failed**: disk or node state reported by the data node itself.
  - **Maintenance**: set by the operator; the node stops serving and receiving chunks until maintenance is ended. See [Maintenance Mode](./operations/maintenance-mode.md).
  - **Quarantined**: an integrity fault was seen on the node; it is excluded from every read, copy and placement until an operator clears it.
- **Lost-and-found (`/.dtfs-lost/`)**: Where a file is moved when removing a dead node had to drop the last copy of one of its chunks, so it never reads back as zeros.
- **Monitor**: The manager component that turns heartbeats into health states. It only detects; it never schedules repair.
- **Orphan chunk**: Chunk bytes on a data node that no file references any more (after an unlink, an overwrite, or an aborted write). The orphan reaper deletes them after a grace period (`orphan_grace_secs`, at least 6 h, default 24 h) measured on the chunk file's age. See [Orphan Chunk Cleanup](./operations/orphan-cleanup-runbook.md).
- **Remove dead node**: Clearing a node that will never come back: shows a dry-run plan first, removes its chunk locations where other live copies exist, and needs explicit consent (`accept_data_loss`) for chunks that would lose their last copy. See [Cluster Management](./operations/cluster-management.md).
- **Scrub**: Background re-reading of stored chunks. A **quick** scrub reads no chunk data: it checks that each chunk file exists, its metadata parses and the sizes agree. A **deep** scrub re-hashes every byte against the chunk id. See [Scrubbing](./storage/scrubbing.md).

## Integrity and security

- **BLAKE3**: The hash function used for chunk ids and every integrity check.
- **Integrity fault**: A chunk whose bytes do not hash to its id, or that is listed but missing from disk. Reported to the managers, repaired from another copy, and (with `quarantine_on_fault`) quarantines the node.
- **Quarantine (chunk)**: Moving a bad chunk file aside to `.dtfs/quarantine/<id>.dat.bad` instead of deleting it, so it can be inspected.
- **Shared secret**: `cluster.shared_secret`, identical on every node. Used to sign data-plane and manager RPCs (HMAC headers), Raft transport (`require_raft_auth`), node join, and REST mutations (`X-Dtfs-Shared-Secret` header). See [Authentication](./security/authentication.md).
- **Verify on read**: Every read is checked against the chunk id, on the data node and again in the client. A mismatch makes the client fail over to another replica.

## Clients

- **FUSE client**: The `dtfs-fuse` daemon that mounts the filesystem (default `/mnt/dtfs`). It talks to managers for metadata and directly to data nodes for bytes. See [FUSE Mount](./access/fuse-mount.md).
- **Local cache**: The FUSE client's on-disk cache of recently read chunks. See [Caching](./storage/caching.md#local-disk-cache).
- **Read-ahead**: Fetching the next chunks of a file before they are requested, so streams do not stall. See [Read-Ahead Modes](./access/read-ahead.md).
- **Read priority**: The order a client picks a replica in: operator priority (`read_location_priority`), then network distance, then tier. See [FUSE Mount](./access/fuse-mount.md).
- **Write lease**: A time-limited lock a writer holds on a path while it writes and commits chunks, so two writers cannot interleave on one file.

## Deployment and configuration

- **Cluster-scoped vs node-scoped key**: Cluster-scoped settings must match on every manager; node-scoped ones (hostname, listen addresses, TLS files) differ per host. See [Cluster vs Node-scoped Keys](./configuration/scope.md).
- **Composite host**: One machine running several roles (for example a manager and a data node). Each role needs its own `[node].hostname` and its own memory cap.
- **Location**: Where a node sits: `region/datacenter/rack/host`. Used for distance-aware reads and for placing replicas in different failure domains.
- **Memory ladder**: The steps every daemon takes as memory runs short, from streaming to disk and shrinking caches up to back-pressure, so DTFS never starves a co-resident application such as Plex. Configured in `[memory]`.
- **MANIFEST / SHA256SUMS / minisign**: Release verification: `SHA256SUMS` lists the tarball's hash, `MANIFEST` inside the tarball lists every binary's hash, and an optional minisign signature (`SHA256SUMS.minisig`) proves who built it. See [Release Artifacts](./deployment/artifacts.md).
- **Release commit**: A commit on `master` whose subject starts with `release vX.Y.Z`; it makes CI build and publish that version. See [CI and Releases](./operations/ci-pipeline.md).

## Reference deployment

- **LELI, TORO, NRB**: The three sites of the reference deployment. LELI (about 40 Mbps) is the archive and anchor datacenter and hosts three of the five managers; TORO (about 400 Mbps) holds the working set on SSD; NRB (about 1 Gbps) is where new downloads land before they replicate to LELI. See the [Introduction](./introduction.md).

## Project conventions

- **Bug and audit ids (`Bug AV`, `LIS-18`, `CR-4-7`, ...)**: Identifiers from the project's bug and audit history, kept in code comments and tests so a guard can be traced back to why it exists. See [Invariants & Bug Archaeology](./internals/invariants.md).
- **Commit types (`bug`, `feat`, `int`, `docs`)**: The first word of every commit subject; release notes are grouped by it. See [Contributing](./development/contributing.md).
