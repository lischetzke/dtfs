//! Bug AY — single source of truth for the binary version string that
//! every dtfs-* crate reports to the manager.
//!
//! There is no semver release process yet, so `VERSION` is pinned at
//! `v0.0.0-dev` and `BUILD_DATE` (emitted by `build.rs` from
//! `SystemTime::now()`) is the actually-useful field for telling two
//! builds apart. `version_string()` glues them together for display.

/// Static base version. Bumped manually when we cut a real release.
pub const VERSION: &str = "v0.0.0-dev";

/// Build-time UTC timestamp, format `YYYYMMDD-HHmmss` (e.g.
/// `20260522-141034`). UTC by construction; no trailing zone suffix.
/// Emitted by `dtfs-common/build.rs`.
pub const BUILD_DATE: &str = env!("DTFS_BUILD_DATE");

/// Combined string for logs / WebUI: `"v0.0.0-dev (built 20260522-141034)"`.
pub fn version_string() -> String {
    format!("{VERSION} (built {BUILD_DATE})")
}

/// Compile-time equivalent of [`version_string`] for use where a
/// `&'static str` is required (e.g. clap's `#[command(version = ...)]`),
/// so `dtfs-* --version` reports the build date an operator can compare
/// against `/api/v1/nodes`. The literal must match [`VERSION`]; concat!
/// cannot interpolate a const ident, only literals + macro output.
pub const VERSION_STRING: &str =
    concat!("v0.0.0-dev (built ", env!("DTFS_BUILD_DATE"), ")");
