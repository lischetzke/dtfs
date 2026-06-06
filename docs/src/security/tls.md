# TLS

> **Implementation status: gRPC TLS is NOT wired up yet.** Regardless of
> `tls.mode`, every manager and data-node gRPC listener currently serves
> **plaintext** and logs
> `TLS mode 'auto' is set but manager gRPC still listens in plaintext (TLS not yet implemented)`
> at startup. The certificate machinery below (cluster CA, node certs issued
> during the join handshake) exists and runs, but the resulting certificates
> are not yet used to encrypt gRPC channels. Treat the rest of this page as
> the design target; do not rely on transport encryption between DTFS nodes
> today — isolate the cluster network (e.g. WireGuard/Tailscale) instead.

DTFS secures all inter-node communication with TLS. By default, a cluster CA is auto-generated on first node join, and each node receives a certificate signed by that CA. Operators can also provide their own certificates for environments with existing PKI infrastructure.

## Auto-Generated Cluster CA

When the first data node joins the cluster and no custom certificates are configured, the manager lazily generates a self-signed CA certificate using the `rcgen` crate. This CA is used to sign individual node certificates throughout the lifetime of the cluster.

The auto-generated CA has the following properties:

| Field | Value |
|-------|-------|
| Common Name | `DTFS CA - <cluster_name>` |
| Organization | `DTFS` |
| Key Type | ECDSA (P-256, via `rcgen::KeyPair::generate`) |
| CA Constraint | `BasicConstraints::Unconstrained` |

The CA certificate and private key are held in memory on the manager leader and are distributed to joining nodes during the [Cluster Join Protocol](./cluster-join.md).

### Node Certificate Generation

When a node completes the join handshake, the manager generates a node-specific certificate signed by the cluster CA:

1. A new ECDSA key pair is generated for the node.
2. The certificate's Common Name is set to the node's hostname.
3. A Subject Alternative Name (SAN) DNS entry is added for the hostname.
4. The certificate is signed by the cluster CA key.

Both the node certificate and private key are returned to the joining node in PEM format.

## Custom Certificates

For environments with existing certificate infrastructure, operators can provide their own CA and node certificates by configuring the TLS section of each component's TOML file:

```toml
[tls]
mode = "require"                          # "auto" | "require" | "disable"
cert_file = "/etc/dtfs/certs/node.pem"
key_file = "/etc/dtfs/certs/node-key.pem"
ca_file = "/etc/dtfs/certs/ca.pem"
```

When custom certificates are provided, the auto-generation logic is skipped entirely. The CA certificate is still distributed to joining nodes so they can validate peer connections.

### Certificate Requirements

Custom certificates must satisfy these requirements:

- The CA certificate must have the `CA:TRUE` basic constraint.
- Node certificates must include a SAN entry matching the node's hostname or IP address.
- All certificates must be PEM-encoded.
- Key pairs must use an algorithm supported by the `ring` cryptography backend (RSA 2048+, ECDSA P-256/P-384, or Ed25519).

## Mutual TLS (mTLS)

All gRPC connections between DTFS components use mutual TLS authentication. Both sides of every connection present a certificate, and both sides verify the peer certificate against the cluster CA.

```text
  dtfs-data (client)                  dtfs-manager (server)
       |                                     |
       |--- ClientHello ------------------>  |
       |<-- ServerHello + server cert -----  |
       |--- client cert ------------------>  |
       |                                     |
       |  (both sides verify against CA)     |
       |                                     |
       |<== encrypted gRPC channel =======>  |
```

This applies to all communication paths:

| Connection | Direction |
|------------|-----------|
| Data node to Manager | Heartbeats, registration, chunk status |
| Manager to Data node | Replication tasks, scrub triggers |
| Data node to Data node | Chunk replication |
| FUSE client to Manager | Metadata operations |
| FUSE client to Data node | Chunk reads and writes |
| Manager to Manager | Raft consensus (inter-node) |

When a node cannot present a valid certificate signed by the cluster CA, the connection is rejected at the TLS handshake level before any application data is exchanged.

## TLS Implementation

DTFS uses `rustls` with the `ring` cryptographic backend for all TLS operations. This provides a pure-Rust TLS stack with no dependency on OpenSSL.

| Component | Crate | Purpose |
|-----------|-------|---------|
| `rustls` | TLS protocol | Connection establishment, handshake, record layer |
| `ring` | Cryptography | ECDSA signatures, AES-GCM encryption, key agreement |
| `tokio-rustls` | Async integration | TLS streams over Tokio async I/O |
| `rcgen` | Certificate generation | Auto-generated CA and node certificates |
| `rustls-pemfile` | PEM parsing | Loading custom certificates from disk |

### Supported Cipher Suites

The `rustls` defaults are used, which include:

- TLS 1.3: `TLS_AES_256_GCM_SHA384`, `TLS_AES_128_GCM_SHA256`, `TLS_CHACHA20_POLY1305_SHA256`
- TLS 1.2: `TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384`, `TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256`

TLS 1.2 support is enabled for compatibility but TLS 1.3 is preferred.

## Disabling TLS

TLS can be disabled for development or testing by setting:

```toml
[tls]
mode = "disable"
```

When TLS is disabled, all gRPC connections use plaintext. This is not recommended for production deployments. (As noted at the top of this page, gRPC connections are currently plaintext in **every** mode.)

## Certificate Rotation

To rotate certificates without downtime:

1. Generate new certificates signed by the same CA (or a new CA that nodes already trust).
2. Update the `cert_file` and `key_file` paths in each node's configuration file.
3. Restart nodes one at a time, starting with data nodes and finishing with managers.
4. If rotating the CA itself, distribute the new CA to all nodes before rotating any node certificates.

See [Maintenance Mode](../operations/maintenance-mode.md) for the recommended procedure for rolling restarts.
