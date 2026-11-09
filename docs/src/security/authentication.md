# Authentication

DTFS authenticates control-plane mutations with the cluster
`[cluster].shared_secret`. This is a shared-secret gate, not per-user
JWT (TLS for transport is separate — see [tls.md](./tls.md)).

## Mutating manager REST (LIS-18 / Sec R1)

When `shared_secret` is **non-empty**, every state-mutating call under
`/api/v1/*` must present that secret. Fail closed:

| Condition | HTTP |
|-----------|------|
| Header missing | 401 |
| Header present but wrong | 403 |

Accepted presentation (either):

- Header `X-Dtfs-Shared-Secret: <secret>`
- Header `Authorization: Bearer <secret>`

Comparisons use constant-time equality (same helper as join / ForwardWrite).

Read-only `GET` endpoints stay open so operator dashboards on a
trusted network can poll status without embedding the secret in the
browser. **This does not replace TLS** (R5 / CF-6).

Exempt from the header gate (own proof of the secret):

- `POST /api/v1/join/request` and `/api/v1/join/complete` (HMAC)
- CF-2 raft membership endpoints still also require `shared_secret` in
  the JSON body (defense in depth) **and** the header when the secret
  is configured

When `shared_secret` is **empty**, mutations stay open (legacy plaintext
/ lab downgrade — same contract as `RegisterNode`). Production clusters
must set a non-empty secret. (LIS-20 separately refuses empty secrets at
startup unless an explicit lab escape hatch is set.)

Denied attempts are audited as `auth.denied`.

### Example

```bash
curl -X POST -H 'content-type: application/json' \
  -H "X-Dtfs-Shared-Secret: $DTFS_SHARED_SECRET" \
  -d '{"level":"quick"}' \
  http://mgr01:9402/api/v1/scrub
```

### dtfs-cli

```bash
export DTFS_SHARED_SECRET='...'   # or --secret on each command
dtfs-cli --manager mgr01:9402 scrub --level quick
```

Raft membership still takes `--secret` for the JSON body; the CLI also
sends it as `X-Dtfs-Shared-Secret`.

## Mutating manager metadata gRPC (LIS-56)

When `shared_secret` is **non-empty**, client-facing mutating
`ManagerService` RPCs (`CreateFile`, `DeleteFile`, `RenameFile`,
`AllocateChunks`, `CommitChunkLocations`, write leases, locks, …)
require HMAC metadata (same header names as LIS-19 data-plane auth):

| Metadata | Meaning |
|----------|---------|
| `x-dtfs-rpc-ts` | unix seconds |
| `x-dtfs-rpc-mac` | hex HMAC-SHA256 |

MAC message plane is `dtfs-mgr-rpc` (not `dtfs-data-rpc`) so a data-plane
stamp cannot be replayed against the manager. Binding is typically the
target path. Missing/invalid → gRPC `PERMISSION_DENIED`. Empty secret →
legacy skip.

`dtfs-client` (FUSE / CLI) stamps these from `[client].shared_secret`.
Read-only RPCs (`GetFileLocations`, `ListDirectory`, …) stay open.
`RegisterNode` keeps its body `shared_secret` field. `Heartbeat` and `ReportChunkStatus` are mutating and carry the same stamp.

`ForwardClientWrite` (follower to leader) carries the same stamp with method
`ForwardClientWrite` and binding hex `blake3(client_request_json)`, so a
captured stamp cannot authorize another body; the raw secret is no longer
sent (the legacy `shared_secret` body field is empty). The leader always
verifies a stamped forward. An unstamped forward from a pre-HMAC manager is
accepted on its raw `shared_secret` field only while
`cluster.accept_legacy_forward_secret = true` (default), otherwise refused
with `PERMISSION_DENIED`. Rolling update: managers update leader last, so
until the leader runs the new build it refuses the updated followers' forwards
(heartbeat usage/status writes landing on those followers do not commit for
that window). Once every manager runs the new build, set
`accept_legacy_forward_secret = false` on every manager.

**REST and gRPC are independent gates** — securing only `:9402` leaves
`:9401` open to anyone who can dial the leader.
## WebUI

The embedded WebUI stores the cluster `shared_secret` in browser
`sessionStorage` only (header badge prompt or **Settings → Session
shared secret**). Mutating `fetch` POSTs attach
`X-Dtfs-Shared-Secret`; the secret is never embedded in HTML. GETs stay
open. Missing secret → HTTP 401 with an actionable message; wrong secret
→ HTTP 403. Clear the session value via Settings → Clear (or close the
tab).

## Data-plane PutChunk / DeleteChunk / ReplicateChunk / GetChunk / ListChunks / VerifyChunk (LIS-19 / LIS-55 / LIS-83)

When `[cluster].shared_secret` is **non-empty**, data nodes require
authenticated gRPC metadata on mutating **and** read RPCs:

| Metadata | Meaning |
|----------|---------|
| `x-dtfs-rpc-ts` | Unix seconds |
| `x-dtfs-rpc-mac` | Hex HMAC-SHA256 over `dtfs-data-rpc\nv1\n{method}\n{ts}\n{binding}\n` |

- `method` is `PutChunk`, `DeleteChunk`, `ReplicateChunk`, `GetChunk`,
  `ListChunks`, or `VerifyChunk`.
- `binding` is empty for PutChunk / ListChunks; `chunk_id` for
  DeleteChunk / GetChunk / VerifyChunk; `chunk_id|target_address` for
  ReplicateChunk (LIS-55 — pins outbound SSRF target).
- Max skew 300 seconds. Missing/invalid → `PermissionDenied`.
- Empty `shared_secret` skips verification (join / legacy plaintext parity).
  Data-node startup refuses empty secret unless
  `allow_insecure_empty_secret = true` (LIS-55 / LIS-20).

Callers that stamp: FUSE/client put + get, data→data replication put,
manager scheduler `ReplicateChunk` / `VerifyChunk` / `ListChunks`, scheduler
flush/orphan delete. **Deploy data + manager +
fuse together** when enabling a non-empty secret (update-required). mTLS
(CF-6) is complementary transport auth, not a substitute for this MAC.

## Raft transport

When `cluster.shared_secret` is set, every Raft RPC (AppendEntries, Vote,
InstallSnapshot) between managers carries the `x-dtfs-rpc-ts` /
`x-dtfs-rpc-mac` pair (method `RaftRpc`, `dtfs-mgr-rpc` plane, 300 s skew).
The MAC binding is `<from>\n<to>\n<blake3(body)>`: the sender's node id (sent
in the `x-dtfs-raft-from` header), the receiver's own node id, and the BLAKE3
hash of the request payload, so a captured stamp cannot authorize another body
or another peer. Because the MAC covers the body, the receiver verifies the
stamp after a size-bounded read of the body (a missing or non-numeric
`x-dtfs-raft-from` is treated as a legacy request with no sender binding: it
is rejected when `require_raft_auth = true`, otherwise accepted as unsigned
with the rate-limited warning, even if it carries a stamp).

- The Raft request body is capped at the manager gRPC message limit
  (`[grpc].max_message_size_mb`); larger bodies are rejected before parsing.
- A stamped-but-invalid request (bad MAC, skew, one header only) is always
  rejected with `PermissionDenied`.
- An unsigned request is accepted with a rate-limited warning unless
  `cluster.require_raft_auth = true`, in which case it is rejected.
- Rolling update: deploy the new build to all managers first (they sign but
  still accept unsigned peers), then set `require_raft_auth = true` on every
  manager and restart. Until then any host that can reach the gRPC port can
  forge Raft RPCs (including InstallSnapshot).

## Node join / register

Data nodes and FUSE clients prove the secret via gRPC `RegisterNode`
(`shared_secret` field) or the HMAC join handshake; see
[cluster-join.md](./cluster-join.md).

## Secret handling

- `GET /api/v1/config` and the WebUI editor show every secret leaf (and
  webhook URL) as `***`. Writing `***` back means "unchanged"; the real value
  is never round-tripped.
- When a config update is persisted, `manager.toml` is replaced via a
  `0600` temp file, `fsync`, then atomic rename, because it carries
  `cluster.shared_secret`.
- Rejected REST mutations are audited as `auth.denied`, coalesced per
  (source, path) per 60 s window; see [Audit Log](./audit-log.md).
