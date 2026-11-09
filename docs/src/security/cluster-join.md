# Cluster Join Protocol

When a data node or FUSE client starts, it registers itself with a manager through the gRPC `ManagerService.RegisterNode` call and must present the cluster's shared secret.

> **Scope.** This covers **data-node and client registration**: the `RegisterNode` write that records the node in the node registry. It does **not** change Raft membership. Adding, promoting, or removing a *manager* in the Raft voter/learner set is a separate operation through the membership API (`/api/v1/raft/{add-learner,promote,remove}` and `dtfs-cli raft …`); see [Cluster Management → Adding a Manager Node](../operations/cluster-management.md#adding-a-manager-node).

## Prerequisites

Before a node can join the cluster, the following must be in place:

- A manager cluster is running and has a leader elected.
- The joining node's configuration file includes the correct `shared_secret` value matching the manager's configuration.
- The joining node can reach the manager's gRPC port (default 9401).

The shared secret is configured in each component's TOML file:

```toml
[cluster]
name = "my-cluster"
shared_secret = "a-strong-random-secret-at-least-32-characters"
```

The same `shared_secret` value must be present on the manager and on every node that will join the cluster.
An empty manager `shared_secret` is refused at startup (LIS-20 / Sec R3) unless
`cluster.allow_insecure_empty_secret = true` is set for lab-only insecure mode.
Data nodes apply the same refuse at startup (LIS-55) so Put/Delete/ReplicateChunk
auth cannot silently skip.

The same secret also authenticates data-plane `PutChunk` / `DeleteChunk`
RPCs when set (LIS-19); see [Authentication → Data-plane PutChunk /
DeleteChunk](./authentication.md#data-plane-putchunk--deletechunk-lis-19--sec-r2).

## RegisterNode

The node sends a `RegisterNodeRequest` (`manager.proto`) with its hostname, role (`data`, `client` or `manager`), gRPC port, location, disk inventory, version and build date, plus the `shared_secret` field.

The manager:

1. Compares `shared_secret` with its own `cluster.shared_secret` in constant time. A mismatch (or a missing value while the cluster secret is set) returns `accepted = false` and performs no Raft write. With an empty cluster secret the request is accepted only when `cluster.allow_insecure_empty_secret = true`.
2. Derives `node_id = node_id_from_hostname(hostname)` server-side. The client never supplies a node id, so it cannot register under an id it does not own.
3. Builds the data node's gRPC address from the connection's peer address and the reported port (clients get no gRPC address and record their peer IP).
4. Commits a `RegisterNode` Raft write. Sticky node states (Quarantined, Maintenance, Draining) survive a re-register.
5. Returns `accepted = true`, the `node_id`, and the current cluster `config_version`.

The node then begins normal operation: heartbeats, chunk placements and replication.

## Security Properties

| Property | Mechanism |
|----------|-----------|
| Proof of possession | `shared_secret` must match, compared in constant time |
| No client-trusted identity | `node_id` is derived server-side from the hostname |
| Transport protection | The secret travels in the request body: enable operator-provisioned mTLS ([TLS](./tls.md)) on any untrusted network |

There is no REST join endpoint; `/api/v1/join/*` was removed.
