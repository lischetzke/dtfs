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

## WebUI

The embedded WebUI stores the cluster `shared_secret` in browser
`sessionStorage` only (header badge prompt or **Settings → Session
shared secret**). Mutating `fetch` POSTs attach
`X-Dtfs-Shared-Secret`; the secret is never embedded in HTML. GETs stay
open. Missing secret → HTTP 401 with an actionable message; wrong secret
→ HTTP 403. Clear the session value via Settings → Clear (or close the
tab).

## Data-plane PutChunk / DeleteChunk / ReplicateChunk (LIS-19 / LIS-55)

When `[cluster].shared_secret` is **non-empty**, data nodes require
authenticated gRPC metadata on mutating RPCs:

| Metadata | Meaning |
|----------|---------|
| `x-dtfs-rpc-ts` | Unix seconds |
| `x-dtfs-rpc-mac` | Hex HMAC-SHA256 over `dtfs-data-rpc\nv1\n{method}\n{ts}\n{binding}\n` |

- `method` is `PutChunk`, `DeleteChunk`, or `ReplicateChunk`.
- `binding` is empty for PutChunk; `chunk_id` for DeleteChunk;
  `chunk_id|target_address` for ReplicateChunk (LIS-55 — pins outbound
  SSRF target).
- Max skew 300 seconds. Missing/invalid → `PermissionDenied`.
- Empty `shared_secret` skips verification (join / legacy plaintext parity).
  Data-node startup refuses empty secret unless
  `allow_insecure_empty_secret = true` (LIS-55 / LIS-20).

Callers that stamp: FUSE/client put, data→data replication put, manager
scheduler `ReplicateChunk`, manager unlink fan-out, scheduler flush/orphan
delete. **Deploy data + manager + fuse together** when enabling a
non-empty secret (update-required). mTLS (CF-6) is complementary transport
auth, not a substitute for this MAC.

**Residual (read path):** `GetChunk` / `ListChunks` / `VerifyChunk` are
not MAC-gated. Compensating control: keep data gRPC off the public
internet and prefer TLS `mode = require` across DCs when certs are
available. Auth-gating reads is a follow-up.

## Node join / register

Data nodes and FUSE clients prove the secret via gRPC `RegisterNode`
(`shared_secret` field) or the HMAC join handshake — see
[cluster-join.md](./cluster-join.md).
