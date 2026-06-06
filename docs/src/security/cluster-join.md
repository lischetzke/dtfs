# Cluster Join Protocol

When a new node starts and contacts the manager to register itself, it must prove that it holds the cluster's shared secret. DTFS uses a two-phase handshake based on HMAC-SHA256 to authenticate joining nodes without transmitting the secret over the network.

> **Scope.** This protocol covers **data-node and client registration**: the HMAC handshake, cluster CA issuance, and the `RegisterNode` write that records the node in the node registry. It does **not** change Raft membership. Adding, promoting, or removing a *manager* in the Raft voter/learner set is a separate operation through the membership API (`/api/v1/raft/{add-learner,promote,remove}` and `dtfs-cli raft …`); see [Cluster Management → Adding a Manager Node](../operations/cluster-management.md#adding-a-manager-node).

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

## Protocol Overview

The join handshake consists of two gRPC calls defined in `cluster.proto` via the `ClusterJoin` service:

```text
  Joining Node                              Manager (Leader)
       |                                          |
       |  1. RequestJoin(hmac, nonce, node_info)   |
       |----------------------------------------->|
       |                                          |  Verify HMAC(secret, nonce)
       |                                          |  Generate cluster CA (if needed)
       |                                          |  Generate challenge nonce
       |  2. JoinResponse(accepted, challenge,     |
       |     ca_cert, managers)                    |
       |<-----------------------------------------|
       |                                          |
       |  (Node configures TLS with CA cert)       |
       |                                          |
       |  3. CompleteJoin(node_id, challenge_resp) |
       |----------------------------------------->|
       |                                          |  Verify challenge response
       |                                          |  Register node in Raft state
       |  4. JoinCompletionResponse(success,       |
       |     config_version, config_json)          |
       |<-----------------------------------------|
       |                                          |
       |  (Node begins normal operation)           |
```

## Phase 1: RequestJoin

The joining node initiates the handshake by computing an HMAC-SHA256 over a freshly generated random nonce using the shared secret as the key:

```text
hmac = HMAC-SHA256(key=shared_secret, message=nonce)
```

The node sends a `JoinRequest` containing:

| Field | Description |
|-------|-------------|
| `secret_hmac` | The computed HMAC bytes |
| `nonce` | The random nonce (32 bytes, hex-encoded) |
| `node_info` | Node metadata: ID, role, hostname, gRPC port, location, disk inventory |

### Manager Validation

The manager receives the request and performs the following checks:

1. **HMAC verification** -- Recomputes `HMAC-SHA256(shared_secret, nonce)` using its own copy of the shared secret and compares the result against the provided `secret_hmac` using constant-time comparison to prevent timing attacks.
2. **Duplicate check** -- Verifies the node is not already registered.

If HMAC verification fails, the manager returns a `JoinResponse` with `accepted = false` and a rejection reason. The attempt is recorded in the audit log.

### Successful Response

If validation passes, the manager:

1. Lazily generates a cluster CA certificate if one does not already exist (see [TLS](./tls.md)).
2. Generates a challenge nonce for Phase 2.
3. Returns a `JoinResponse` containing:

| Field | Description |
|-------|-------------|
| `accepted` | `true` |
| `challenge` | A new random nonce for the completion step |
| `cluster_ca_cert` | PEM-encoded CA certificate for TLS setup |
| `managers` | List of all manager node addresses |

## Phase 2: CompleteJoin

After receiving the CA certificate, the joining node configures its TLS stack to trust the cluster CA. It then sends a `JoinCompletionRequest` containing:

| Field | Description |
|-------|-------------|
| `node_id` | The node's assigned or self-generated ID |
| `challenge_response` | Response to the manager's challenge |

### Manager Completion

The manager verifies the challenge response and, if valid, registers the node in the Raft state machine via a `RegisterNode` write. This ensures the new node's membership is replicated across all manager nodes.

On success, the manager returns:

| Field | Description |
|-------|-------------|
| `success` | `true` |
| `config_version` | Current cluster configuration version |
| `config_json` | Full cluster configuration as JSON |

The joining node stores this configuration and begins normal operation: sending heartbeats, accepting chunk placements, and participating in replication.

## Nonce Generation

Nonces are generated using 32 bytes from the system's cryptographically secure random number generator, then hex-encoded to produce a 64-character string. Each nonce is used exactly once.

## Security Properties

| Property | Mechanism |
|----------|-----------|
| Secret never transmitted | Only HMAC(secret, nonce) crosses the wire |
| Replay prevention | Fresh random nonce per request; each nonce is single-use |
| Timing attack resistance | Constant-time HMAC comparison |
| Man-in-the-middle prevention | CA certificate distributed during join; subsequent connections use mTLS |
| Audit trail | All join attempts (accepted and rejected) are recorded in the audit log |

## Rejection Scenarios

A join request is rejected if:

- The HMAC does not match (wrong shared secret).
- The manager cluster has no elected leader (request cannot be processed).
- The node ID is already registered with a different hostname.

Rejected join attempts are logged as `node.join.request` with result `rejected` in the audit log.

## Monitoring Join Activity

Join events can be viewed in the audit log:

```bash
dtfs-cli audit --action node.join --limit 20
```

This shows both `node.join.request` and `node.join.complete` events with their results.
