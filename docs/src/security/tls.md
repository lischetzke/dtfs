# TLS

> **Implementation status: gRPC mutual TLS is implemented (CF-6).** When a
> node's `[tls]` block resolves certificates (see modes below), its gRPC
> listeners (manager `ManagerService` + Raft transport, data-node
> `DataService`) require a mutual-TLS handshake against the cluster CA, and
> all of its outbound gRPC connections (manager-to-data, manager-to-manager
> forwards, Raft peer channels, data-to-manager heartbeats, data-to-data
> replication, FUSE-to-manager and FUSE-to-data) present the node certificate
> and verify the peer against the CA. With no certificates configured
> (the default), every mode except `require` keeps serving **plaintext** for
> back-compat, and the node logs a startup warning. See
> [Rollout](#rollout-enabling-tls-on-an-existing-cluster) before enabling on
> a live cluster.

DTFS can secure all inter-node gRPC with mutual TLS. **The operator
provisions the certificates**: set `[tls]` `cert_file`/`key_file`/`ca_file`
on every node. The manager does not generate or distribute a CA during
node join; with no files configured the cluster stays plaintext.

## Provisioning Certificates

Use your own PKI, or generate a CA and node certificates with any external
tool (openssl, step, cfssl). `dtfs_common::crypto::generate_cluster_ca` and
`generate_node_cert` exist only as test helpers; they are not wired into
the join protocol or the `[tls]` loader.

## Configuration

For environments with existing certificate infrastructure, operators can provide their own CA and node certificates by configuring the TLS section of each component's TOML file:

```toml
[tls]
mode = "require"                          # "auto" | "require" | "disabled"
cert_file = "/etc/dtfs/certs/node.pem"
key_file = "/etc/dtfs/certs/node-key.pem"
ca_file = "/etc/dtfs/certs/ca.pem"
```

Every node needs the CA certificate in `ca_file`; nothing distributes it at join time.

### Mode semantics

| Mode | Behaviour |
|------|-----------|
| `auto` (default) | mTLS only when `cert_file`, `key_file` and `ca_file` are all configured AND all exist on disk; otherwise plaintext with a startup warning. A partially configured block (some paths set, some not) is a startup error. Configured-and-present but unreadable files are also a hard error -- never a silent downgrade. |
| `require` | All three files must be configured and load successfully, or the process fails at startup. |
| `disabled` (also `disable`/`off`) | Plaintext; certificate paths are ignored. |

The decision is made once at startup (`dtfs_common::tls::TlsContext`) and
logged: `gRPC TLS enabled: mutual TLS, peers verified against the cluster CA`
or a plaintext notice/warning. Certificate or mode changes require a process
restart.

### Certificate Requirements

Certificates must satisfy these requirements:

- The CA certificate must have the `CA:TRUE` basic constraint.
- Node certificates must include a SAN entry for **every address peers dial
  this node by** — in practice that means IP SANs: the manager registry
  stores each node's observed IP (`grpc_addr`), raft peers dial
  `initial_voters` addresses, and migrations/reads dial the registry IP.
  A hostname-only SAN fails rustls verification on every IP-dialed
  connection. A single-node manager whose config triggers the
  self-register loopback fallback additionally needs a `127.0.0.1` SAN.
- All certificates must be PEM-encoded.
- Key pairs must use an algorithm supported by the `ring` cryptography backend (RSA 2048+, ECDSA P-256/P-384, or Ed25519).

### Known limitations

- **IPv6 literals**: registry addresses for IPv6 nodes are stored
  bracketed (`http://[2001:db8::1]:9500`); the TLS server-name check does
  not currently strip the brackets, so TLS-enabled clusters must dial
  nodes via IPv4 or DNS names.

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
| `rustls-pemfile` | PEM parsing | Loading custom certificates from disk |

### Supported Cipher Suites

The `rustls` defaults are used, which include:

- TLS 1.3: `TLS_AES_256_GCM_SHA384`, `TLS_AES_128_GCM_SHA256`, `TLS_CHACHA20_POLY1305_SHA256`
- TLS 1.2: `TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384`, `TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256`

TLS 1.2 support is enabled for compatibility but TLS 1.3 is preferred.

## Rollout: enabling TLS on an existing cluster

A TLS-enabled node only speaks `https` and a plaintext node only speaks
`http` -- there is no per-connection negotiation. Mixed TLS state therefore
splits the cluster: a plaintext peer cannot talk to a TLS listener and vice
versa. Enable TLS in this order:

1. **Upgrade every binary first.** Roll the TLS-capable build to all
   managers, data nodes and FUSE clients while leaving `[tls]` unconfigured.
   The default (`mode = "auto"`, no cert paths) keeps the whole cluster on
   plaintext -- a mixed-version cluster keeps working unchanged.
2. **Distribute certificates everywhere.** Place the cluster CA plus a
   per-node certificate/key (SAN must match the address peers dial -- IP SANs
   for IP-dialed nodes) on every node, and set `cert_file`/`key_file`/
   `ca_file` in each `[tls]` block. With `mode = "auto"` the files may land
   ahead of the restarts.
3. **Restart the cluster into TLS.** Because mixed TLS state partitions the
   cluster, schedule a short maintenance window: restart all managers, then
   data nodes, then FUSE clients. Verify each node logs
   `gRPC TLS enabled: mutual TLS, peers verified against the cluster CA`.
4. **Pin it.** Once everything is healthy, switch `mode = "auto"` to
   `mode = "require"` so a missing certificate becomes a startup error
   instead of a silent plaintext downgrade.

Until the rollout completes, isolate the cluster network (e.g.
WireGuard/Tailscale) if transport privacy is required.

### Upgrade notes (pre-CF-6 configs)

Before CF-6 the `[tls]` block was decorative — every mode served plaintext
with at most a warning. The new binary **enforces** it:

- `mode = "require"` without loadable certificates, or a **partially**
  filled block (some paths set, some not), is now a **startup error** —
  the daemon exits. Audit every `[tls]` block before rolling the upgrade;
  a manager restart-looping on a leftover aspirational block can cost
  Raft quorum mid-deploy.
- `mode = "auto"` with all three paths set AND files present **enables
  mTLS on the next restart** — the upgrade itself flips that node to TLS
  while unrestarted peers still speak plaintext. If certificates were
  staged pre-CF-6, either remove the paths until the coordinated
  restart window (step 3) or treat the binary upgrade as that window.
- Note the `auto`-mode hazard between steps 2 and 3: once paths + files
  are in place, ANY restart (crash, OOM-kill, reboot) flips that single
  node to TLS early and partitions it until the rest of the cluster
  follows. Keep the step-2 to step-3 gap short.

## Disabling TLS

TLS can be disabled for development or testing by setting:

```toml
[tls]
mode = "disabled"
```

When TLS is disabled, all gRPC connections use plaintext. This is not recommended for production deployments.

## Certificate Rotation

To rotate certificates without downtime:

1. Generate new certificates signed by the same CA (or a new CA that nodes already trust).
2. Update the `cert_file` and `key_file` paths in each node's configuration file.
3. Restart nodes one at a time, starting with data nodes and finishing with managers.
4. If rotating the CA itself, distribute the new CA to all nodes before rotating any node certificates.

See [Maintenance Mode](../operations/maintenance-mode.md) for the recommended procedure for rolling restarts.
