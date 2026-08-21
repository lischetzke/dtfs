// Data-node memory backpressure (Bug G / Bug V).
//
// The role-agnostic core — `MemoryBudget`, `PressureLevel`, RSS sampling,
// the pure slot-recommendation + threshold helpers, and `run_monitor` —
// moved to `dtfs_common::memory` in CF-5 so the manager and fuse roles
// share one sampler instead of a copy-paste. This module re-exports those
// items so the rest of dtfs-data keeps using `crate::memory::*`.
//
// REV-4-5 — the data-specific rung that used to live here
// (`run_monitor_with_cache`, the Bug V "shrink the ARC cache" step) is
// gone. Nothing in the workspace ever called `ArcCache::put`, so the
// cache was permanently empty and the rung could only ever evict zero
// entries — an advertised ladder step the node did not actually have.
// The data node's real cheapest-first rungs are streaming-to-disk on
// both the write and read paths (Bug I / U2) and, last resort, the
// opt-in put_chunk shed (Bug G) driven by the shared `run_monitor`.

pub use dtfs_common::memory::MemoryBudget;
