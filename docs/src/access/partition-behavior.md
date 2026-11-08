# Partition Behavior

What a FUSE client can do when it cannot reach a manager quorum, and what the data plane does while the control plane is down.

## What happens

Metadata always goes through managers; chunk bytes go straight to data nodes. A partition therefore affects reads and writes very differently.

| Operation | Client on the minority side or leader unreachable |
|-----------|----------------------------------------------------|
| Read, location cached (within 5 s) | Works if a replica's data node is reachable |
| Read, location not cached | Needs a manager answer. Managers other than the leader can answer reads from their local state, so this works as long as any manager is reachable; with none reachable it fails after the manager walk |
| Chunk fetch | Works against any reachable data node; fails over replica by replica |
| Create, write, rename, unlink, truncate | Fail. Allocation, write leases and commit go through the leader, so nothing can be committed without a quorum. The client retries its manager list for up to 15 s, then returns `EIO` |
| Bytes already streamed to data nodes by an uncommitted write | Not referenced by any file; reclaimed by the orphan reaper after its grace period |

Reads can be briefly stale: a manager that is behind may return an older location or file size, and a client keeps a location for up to 5 s. A stale location only costs a failover, because chunk ids are content hashes and a replica that no longer exists simply errors.

## Client behavior

- **Manager walk.** The client keeps the whole `manager_addr` + `manager_addrs` list. When no usable manager answers (election in progress, rolling restart), it re-walks the list with backoff for up to 15 seconds before giving up.
- **Write lease.** A lapsed lease is re-acquired once under the same owner id. If the lease cannot be renewed because managers are unreachable, a flush continues best effort with the existing lease until it expires; if another writer holds the path, the write aborts.
- **Voters.** The manager list must include at least one voter. A list of learners only supports reads.

## `[partition_safety]` is not enforced

`fuse.toml` accepts a `[partition_safety]` block (`mode` = `strict`, `localreadonly`, `fulllock`; `quorum_loss_timeout_secs`; `show_warning`), and `quorum_loss_timeout_secs` is validated as non-zero at startup. The client does not act on any of these values: there is no quorum-loss state, no marker file and no read blocking. The behavior is the one in the table above regardless of the setting. Do not rely on `strict` or `fulllock` to freeze a mount.

## Manager side

Raft behavior during partitions (leader isolation, minority loss, three-way split) is described in [Consensus](../architecture/consensus.md#network-partition-behavior). A data node that cannot reach any manager keeps serving existing chunks and keeps trying its manager list (re-registering when told it is unknown); the managers mark it Suspect and then Down after 3 and 6 missed heartbeats, which stops new placements on it and starts repair of its mandatory replicas.

## Monitoring

Watch `dtfs_nodes_suspect` and `dtfs_nodes_down` on the manager `/metrics` endpoint and the Raft state under `GET /api/v1/raft/state`. See [Monitoring](../operations/monitoring.md).
