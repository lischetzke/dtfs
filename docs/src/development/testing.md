# Testing

DTFS uses a multi-layered testing strategy: unit tests, integration tests, chaos tests, and benchmarks. All tests are run through the standard Cargo test infrastructure.

## Unit Tests

Unit tests live alongside the source code in each crate using Rust's built-in `#[cfg(test)]` modules.

### Running All Unit Tests

```bash
cargo test
```

### Running Tests for a Specific Crate

```bash
cargo test -p dtfs-common
cargo test -p dtfs-manager
cargo test -p dtfs-data
cargo test -p dtfs-raft
cargo test -p dtfs-client
```

### Running a Specific Test

```bash
cargo test -p dtfs-common test_hmac_roundtrip
```

### Key Unit Test Areas

| Crate | Test Area | What Is Tested |
|-------|-----------|----------------|
| `dtfs-common` | `crypto` | HMAC-SHA256 roundtrip, CA generation, node certificate signing, nonce uniqueness |
| `dtfs-common` | `hash_ring` | Consistent hashing ring distribution, node addition/removal |
| `dtfs-common` | `checksum` | BLAKE3 checksum computation and verification |
| `dtfs-common` | `config` | TOML parsing, default values, validation |
| `dtfs-manager` | `placement` | Rack-aware placement, failure domain spreading |
| `dtfs-manager` | `audit` | Ring buffer behavior, filtering, capacity limits |
| `dtfs-manager` | `tiering` | Temperature classification, migration decisions |
| `dtfs-data` | `store` | WAL writes, atomic renames, crash recovery |
| `dtfs-data` | `cache` | ARC cache eviction, hit/miss tracking |
| `dtfs-data` | `scrub` | Quick and deep scrub correctness |
| `dtfs-raft` | `state_machine` | Raft state machine apply, snapshot, restore |

## Integration Tests

Integration tests verify interactions between multiple components. They are located in the `tests/` directory of each crate or at the workspace root.

### Running Integration Tests

```bash
cargo test --test '*'
```

Integration tests typically spin up in-process instances of the manager and data nodes to test end-to-end flows without requiring external infrastructure.

### Key Integration Test Scenarios

- **Node join flow** -- A data node joins a manager cluster using the shared secret handshake, receives a CA certificate, and completes registration.
- **File write/read roundtrip** -- A file is created via the manager, chunks are allocated and written to data nodes, then read back and verified.
- **Replication** -- A chunk is written with replication factor 2, and both replicas are verified for correctness.
- **Scrub detection** -- A chunk is intentionally corrupted on disk, a deep scrub detects the corruption, and the system re-replicates from a healthy replica.
- **Configuration propagation** -- A cluster configuration update is applied via the REST API and verified to propagate to data nodes through heartbeat responses.

## Chaos Tests

Chaos tests simulate failure conditions to verify that DTFS recovers correctly. These tests are typically longer-running and are gated behind a feature flag or test filter.

### Running Chaos Tests

```bash
cargo test chaos -- --ignored
```

Chaos tests are marked with `#[ignore]` by default so they do not run in the standard `cargo test` cycle.

### Chaos Test Scenarios

| Test | Description |
|------|-------------|
| Node crash during write | Kill a data node mid-write and verify WAL recovery on restart |
| Leader failover | Stop the Raft leader and verify a new leader is elected and operations resume |
| Network partition | Simulate a partition between data nodes and verify that reads fall back to available replicas |
| Disk failure | Mark a disk as failed and verify chunks are re-replicated to healthy disks |
| Concurrent joins | Multiple nodes attempt to join simultaneously; all should succeed without deadlock |

## Benchmarks

Performance benchmarks use Rust's built-in benchmark framework or `criterion` for more detailed statistical analysis.

### Running Benchmarks

```bash
cargo bench
```

### Running Benchmarks for a Specific Crate

```bash
cargo bench -p dtfs-data
cargo bench -p dtfs-common
```

### Key Benchmark Areas

| Crate | Benchmark | What Is Measured |
|-------|-----------|------------------|
| `dtfs-common` | `checksum` | BLAKE3 hashing throughput at various chunk sizes |
| `dtfs-common` | `hash_ring` | Consistent hash ring lookup latency |
| `dtfs-data` | `store` | Sequential and random chunk write/read throughput |
| `dtfs-data` | `cache` | ARC cache lookup latency under contention |
| `dtfs-raft` | `state_machine` | State machine apply throughput |

## Test Configuration

Some tests require specific environment variables or configuration:

| Variable | Purpose |
|----------|---------|
| `DTFS_TEST_DATA_DIR` | Directory for temporary test data (defaults to a `tempfile`-managed directory) |
| `RUST_LOG` | Log level for test output (e.g., `RUST_LOG=dtfs_manager=debug cargo test`) |

## Continuous Integration

The recommended CI pipeline runs the following stages in order:

1. **Format check** -- `cargo fmt --check`
2. **Lint** -- `cargo clippy -- -D warnings`
3. **Unit tests** -- `cargo test`
4. **Integration tests** -- `cargo test --test '*'`
5. **Chaos tests** (nightly/weekly) -- `cargo test chaos -- --ignored`
6. **Benchmarks** (optional, for regression tracking) -- `cargo bench`
