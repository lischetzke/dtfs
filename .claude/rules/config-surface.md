---
paths:
  - "crates/dtfs-common/src/config.rs"
---

# Config-surface sync (AGENTS.md § B.5)

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
example. A field documented but not wired to anything real (see CMN-3 in
`plans/reports/CORE-REVIEW-2026-09-12.md` for the shape of that bug) is
worse than no field: either wire it or delete it, the example, and the
docs row together.
