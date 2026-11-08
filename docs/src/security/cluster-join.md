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
An empty manager `shared_secret` is refused at startup (LIS-20 / Sec R3) unless
`cluster.allow_insecure_empty_secret = true` is set for lab-only insecure mode.
Data nodes apply the same refuse at startup (LIS-55) so Put/Delete/ReplicateChunk
auth cannot silently skip.

The same secret also authenticates data-plane `PutChunk` / `DeleteChunk`
RPCs when set (LIS-19); see [Authentication → Data-plane PutChunk /
DeleteChunk](./authentication.md#data-plane-putchunk--deletechunk-lis-19--sec-r2).

## Protocol Overview

The join handshake consists of two gRPC calls defined in `cluster.proto` via the `ClusterJoin` service:

```text
  Joining Node                              Manager (Leader)
       |                                          |
       |  1. RequestJoin(hmac, nonce, node_info)   |
       |----------------------------------------->|
       |                                          |  Verify HMAC(secret, nonce)
       |                                          |  Generate challenge nonce
       |  2. JoinResponse(accepted, challenge,     |
       |     ca_cert, managers)                    |
       |<-----------------------------------------|
       |                                          |
       |  3. CompleteJoin(challenge,               |
       |       HMAC(secret, challenge), node_info) |
       |----------------------------------------->|
       |                                          |  Look up challenge (TTL, single-use)
       |                                          |  Verify HMAC(secret, challenge)
       |                                          |  Derive node_id from hostname
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

1. Generates a challenge nonce for Phase 2. Challenges are held in memory **per manager** (single-use, 60 s TTL): both phases (`RequestJoin` and `CompleteJoin`) must reach the same manager, or `CompleteJoin` fails with an unknown-challenge rejection and the node must restart the join.
2. Returns a `JoinResponse` containing:

| Field | Description |
|-------|-------------|
| `accepted` | `true` |
| `challenge` | A new random nonce for the completion step |
| `cluster_ca_cert` | Empty. The manager no longer generates a join-time CA; TLS material comes from the `[tls]` config (see [TLS](./tls.md)) |
| `managers` | List of all manager node addresses |

## Phase 2: CompleteJoin

The joining node computes a completion HMAC over the manager-issued `challenge` and sends a `JoinCompletionRequest` containing:

```text
completion_hmac = HMAC-SHA256(key=shared_secret, message=challenge)
```

| Field | Description |
|-------|-------------|
| `challenge` | The server-issued challenge string returned in Phase 1 |
| `completion_hmac` | `HMAC-SHA256(shared_secret, challenge)`, hex-encoded |
| `node_info` | Node metadata: role, hostname, gRPC address, location, disk inventory |

> **The client does not supply a `node_id`.** The manager derives the id server-side as `node_id_from_hostname(hostname)`, so a caller can never register under (or hijack) an id it does not own.

### Manager Completion

The manager (CO-2-3):

1. Looks up the `challenge` in its short-lived challenge store (60 s TTL, **single-use** — consumed on lookup so a replay finds nothing). An unknown or expired challenge is rejected.
2. Verifies `completion_hmac == HMAC-SHA256(shared_secret, challenge)` in constant time. A mismatch is rejected.
3. Derives `node_id = node_id_from_hostname(hostname)`.
4. Registers the node in the Raft state machine via a `RegisterNode` write, replicating membership across all managers.

A rejected completion performs **no Raft write**, so a forged or replayed `CompleteJoin` cannot change an existing node's registered `grpc_addr`. Rejections are recorded as `node.join.complete` with the reason in the audit log.

> **Back-compat downgrade.** When `cluster.shared_secret` is **empty** (a legacy plaintext cluster), the challenge/HMAC checks in Phase 2 are skipped and the join is unauthenticated, exactly as before. Configure a non-empty shared secret on every manager and node to require proof of possession on completion.

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
| Secret never transmitted | Only HMAC(secret, nonce/challenge) crosses the wire |
| Proof of possession on completion | Phase 2 requires HMAC(secret, challenge) over the server-issued, TTL-bounded, single-use challenge |
| No client-trusted identity | `node_id` is derived server-side from the hostname; the client cannot choose it |
| Replay prevention | Fresh random nonce per request; each nonce/challenge is single-use |
| Timing attack resistance | Constant-time HMAC comparison |
| Man-in-the-middle prevention | CA certificate distributed during join; subsequent connections use mTLS |
| Audit trail | All join attempts (accepted and rejected) are recorded in the audit log |

## Rejection Scenarios

A join request is rejected if:

- The HMAC does not match (wrong shared secret) — Phase 1 over the nonce, or Phase 2 over the challenge.
- The Phase 2 challenge is unknown or expired (no matching Phase 1, TTL elapsed, or already consumed).
- The manager cluster has no elected leader (request cannot be processed).

Rejected join attempts are logged as `node.join.request` with result `rejected` in the audit log.

## Monitoring Join Activity

Join events can be viewed in the audit log:

```bash
dtfs-cli audit --action node.join --limit 20
```

This shows both `node.join.request` and `node.join.complete` events with their results.
