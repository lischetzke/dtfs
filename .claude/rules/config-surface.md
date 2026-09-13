---
paths:
  - "crates/dtfs-common/src/config.rs"
---

# Config-surface sync (CLAUDE.md § B.5)

Any change to a config struct here (new/renamed/removed option, changed
default) MUST, in the **same commit**:

1. Update the matching `deploy/examples/{manager,data,fuse}.toml` —
   every option present, optional ones commented out with their
   compiled-in default. `example_*_toml_parses` (in this crate) gates
   this: it fails if an example no longer parses against the struct.
2. Update the matching reference page under `docs/src/configuration/`
   (`manager.md`, `data-node.md`, `fuse-client.md`, `cluster-config.md`,
   `volume-config.md`) — one table row per option, no duplicated
   complete-example block (the `deploy/examples/*.toml` files are the
   single source for those).

Run `cargo test -p dtfs-common --release` after any config/example
change — it is cheap and is exactly what would have caught a stale
example. A field documented but not wired to anything real (CMN-3,
`grpc.max_message_size_mb` before CR2-8-1, was exactly that) is
worse than no field: either wire it or delete it, the example, and the
docs row together.

CR2-8-1 fixed CMN-3 itself: `grpc.max_message_size_mb` was parsed and
documented (default 256) but every tonic builder hardcoded a 32 MB
literal instead of reading it. The fix added the shared pure helper
`dtfs_common::config::max_message_bytes(mb)` and a matching
`[client].max_message_size_mb` on `FuseClientConfig` (the client had no
`[grpc]` block of its own) — when a config value has no natural home in
an existing per-role struct, mirror the sibling role's field name/default
rather than inventing a new shape.
