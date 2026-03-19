# Contributing

This guide covers the development workflow, code conventions, and architecture overview for contributors to DTFS.

## Getting Started

1. Clone the repository and build the project (see [Building from Source](./building.md)).
2. Run the test suite to verify your setup:

```bash
cargo test
```

3. Read the [Architecture Overview](../architecture/overview.md) to understand how the components fit together.

## Code Style

DTFS follows standard Rust conventions enforced by `rustfmt` and `clippy`.

### Formatting

All code must be formatted with `rustfmt` using the project's default settings:

```bash
cargo fmt
```

Check formatting without modifying files:

```bash
cargo fmt --check
```

### Linting

All code must pass `clippy` with no warnings:

```bash
cargo clippy -- -D warnings
```

### Naming Conventions

- **Crates**: `dtfs-<name>` with hyphens (e.g., `dtfs-manager`, `dtfs-common`).
- **Modules**: Snake case (e.g., `state_machine.rs`, `hash_ring.rs`).
- **Types**: Pascal case (e.g., `AuditEntry`, `ChunkPlacement`).
- **Functions**: Snake case (e.g., `compute_join_hmac`, `generate_nonce`).
- **Constants**: Screaming snake case (e.g., `DEFAULT_CHUNK_SIZE`).

### Documentation

- All public items (functions, types, modules) must have a `///` doc comment.
- Module-level documentation uses `//!` at the top of the file.
- Include a brief description of the module's purpose and its role in the broader architecture.

### Error Handling

- Use `anyhow::Result` for application-level error propagation in binaries.
- Use `thiserror` for library crates that define error types consumed by other crates.
- Avoid `.unwrap()` in production code; use `.expect()` only when the invariant is documented.

## Architecture for Contributors

DTFS is structured as a Cargo workspace with 8 crates. Here is a dependency overview:

```text
  dtfs-manager -----> dtfs-raft -----> dtfs-common
       |                  |                |
       |                  v                |
       |             dtfs-proto <----------+
       v
  dtfs-data -------> dtfs-common
       |                  |
       v                  v
  dtfs-proto         dtfs-proto

  dtfs-fuse -------> dtfs-client -----> dtfs-proto
                          |
                          v
                     dtfs-common

  dtfs-cli ---------> dtfs-common
```

### Where Things Live

| What | Crate | Key Files |
|------|-------|-----------|
| Raft consensus, state machine | `dtfs-raft` | `state_machine.rs`, `log_store.rs`, `network.rs` |
| File/chunk metadata | `dtfs-manager` | `metadata.rs` |
| Chunk placement algorithm | `dtfs-manager` | `placement.rs` |
| Node join protocol | `dtfs-manager` | `join.rs` |
| Tiering engine | `dtfs-manager` | `tiering.rs` |
| REST API endpoints | `dtfs-manager` | `api.rs` |
| WebUI | `dtfs-manager` | `webui.rs` |
| Audit log | `dtfs-manager` | `audit.rs` |
| Chunk storage on disk | `dtfs-data` | `store.rs` |
| ARC cache | `dtfs-data` | `cache.rs` |
| Scrub scheduler | `dtfs-data` | `scrub.rs` |
| FUSE filesystem ops | `dtfs-fuse` | `filesystem.rs` |
| Read-ahead engine | `dtfs-fuse` | `readahead.rs` |
| Write buffer | `dtfs-fuse` | `writebuf.rs` |
| CLI commands | `dtfs-cli` | `main.rs` |
| TLS/crypto helpers | `dtfs-common` | `crypto.rs` |
| Configuration parsing | `dtfs-common` | `config.rs` |
| Consistent hash ring | `dtfs-common` | `hash_ring.rs` |
| Protobuf definitions | `proto/` | `manager.proto`, `data.proto`, `cluster.proto`, `admin.proto`, `common.proto` |

## Pull Request Process

### Before Submitting

1. **Run the full test suite:**

```bash
cargo test
```

2. **Run the linter:**

```bash
cargo clippy -- -D warnings
```

3. **Format your code:**

```bash
cargo fmt
```

4. **Write tests** for new functionality. Both unit tests (in `#[cfg(test)]` modules) and integration tests (in `tests/` directories) are expected for non-trivial changes.

### PR Guidelines

- Keep PRs focused on a single change. Avoid mixing unrelated refactors with feature work.
- Write a clear PR description that explains what changed and why.
- Reference any related issues in the description.
- Ensure all CI checks pass before requesting review.

### Review Expectations

- All PRs require at least one approving review before merge.
- Reviewers will check for correctness, test coverage, documentation, and adherence to the code style guidelines.
- Address review comments by pushing additional commits; do not force-push over reviewed commits.

## Commit Messages

Use clear, imperative-mood commit messages:

```
Add rack-aware placement constraint to placement engine

The placement engine now spreads replicas across different racks
within the same datacenter by default. This prevents data loss
when an entire rack fails.
```

- First line: imperative summary, under 72 characters.
- Blank line, then a longer description if needed.
- Reference issue numbers where applicable (e.g., `Fixes #42`).

## Adding a New gRPC RPC

1. Define the RPC and message types in the appropriate `.proto` file under `proto/`.
2. Rebuild `dtfs-proto`: `cargo build -p dtfs-proto`.
3. Implement the server-side handler in the relevant crate (`dtfs-manager` or `dtfs-data`).
4. Add client support in `dtfs-client` if the RPC is called by FUSE clients or the CLI.
5. Add tests covering both success and error paths.
6. Document the RPC in the [gRPC API reference](../api-reference/grpc-api.md).

## Adding a New REST Endpoint

1. Add the route in `dtfs-manager/src/api.rs` using Axum's routing API.
2. Implement the handler function with proper error handling and audit logging.
3. Add corresponding CLI support in `dtfs-cli/src/main.rs` if the endpoint should be accessible via the CLI.
4. Add tests for the endpoint.
5. Document the endpoint in the [REST API reference](../api-reference/rest-api.md).

## Adding a New CLI Command

1. Add a new variant to the `Commands` enum in `dtfs-cli/src/main.rs`.
2. Implement the handler function that calls the appropriate REST API endpoint.
3. Support both `table` and `json` output formats.
4. Document the command in the [CLI reference](../api-reference/cli-reference.md).
