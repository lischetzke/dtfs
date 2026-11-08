# Testing

All verification runs as unit and crate tests inside the Docker build image ([Building from Source](./building.md)). There is no build VM and no lab cluster; behavior that only exists on a real cluster is reasoned about and noted in the commit body, not executed.

## Running tests

```bash
MSYS_NO_PATHCONV=1 docker run --rm -v "<path-to-checkout>:/build" \
  -v dtfs-target:/target -v dtfs-registry:/usr/local/cargo/registry \
  -w /build dtfs-buildenv:latest cargo test -p <crate> --release
```

- Always `--release`. Run the crate you changed; run the whole workspace (`cargo test --workspace --release`) for cross-crate changes.
- After any config struct or example change, run `cargo test -p dtfs-common --release`: the `example_*_toml_parses` tests check every shipped example. If one flakes while another container holds the shared build lock, rerun it once.
- A single test: append the test name after `--` (`cargo test -p dtfs-raft --release posix_path_regression`).

## Where tests live

Unit tests sit beside the code in `#[cfg(test)]` modules or `tests.rs` files (`dtfs-raft/src/state_machine/tests.rs`, `dtfs-manager/src/scheduler/tests.rs`, `dtfs-manager/src/api/tests.rs`). `crates/dtfs-data/tests/` holds loopback integration tests such as mutual TLS.

## Conventions

1. **Test the purest layer.** Prefer, in order: a pure helper, then a state-machine `apply` test, then a manager aggregator, then a FUSE helper. Planners (`plan_cache_tick`, `plan_cache_promotion`) and decision functions (`demote_preserves_anchor`) are pure on purpose.
2. **Red, green, suite.** Write the failing test, watch it fail for the right reason, make the smallest fix, watch it pass, then run the crate suite.
3. **Snapshot compatibility.** Every new field on `StateMachineData` carries `#[serde(default)]` and gets a regression test (`state_machine::posix_path_regression` or `snapshot_persistence_tests`). A new replicated timestamp carries `#[serde(default)] now_ms` and falls back to the log index when `0`.
4. **Durability and anchor invariants** get a property-style test over random operation sequences, not just examples.
5. **Cluster-only behavior** (failover timing, WAN behavior) cannot run here. Say so in the commit body instead of adding a test that cannot execute.
6. **No wall clock in `apply`.** A test that would need `SystemTime` inside apply is a sign the field should come from the proposer.

## Dormant harnesses

`tests/release/`, `tests/chaos/` and `tests/integration/` are shell harnesses for a live cluster or Docker Compose lab. They are not part of the verification loop and need no change to ship a fix. See [Lab: Chaos Testing](../lab/chaos-testing.md) and [Release Testing](../lab/release-testing.md).

The host-side Python tooling under `tools/dtfs-mcp` is tested separately (`uv run --with pytest --with "mcp<2" --with httpx pytest tools/dtfs-mcp`), outside the cargo loop.

## Environment

`RUST_LOG=dtfs_manager=debug` raises log output for a failing test. `DTFS_TEST_DATA_DIR` overrides the temp directory some data-node tests use.
