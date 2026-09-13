//! Bug AY — single source of truth for the binary version string that
//! every dtfs-* crate reports to the manager.
//!
//! `VERSION` defaults to `v0.0.0-dev`; release builds stamp it via the
//! `DTFS_VERSION` env var (see `build.rs` + `scripts/dtfs-release.sh`,
//! CLAUDE.md § A.3.5). `BUILD_DATE` (emitted by `build.rs` from
//! `SystemTime::now()` / `SOURCE_DATE_EPOCH`) still distinguishes two
//! dev builds with the same version. `version_string()` glues them
//! together for display.

/// Base version. Defaults to `v0.0.0-dev`; release wrappers stamp a real
/// tag by exporting `DTFS_VERSION` before `cargo build` (`build.rs`
/// emits it as a `rustc-env`). Plain/dev builds keep `v0.0.0-dev`.
pub const VERSION: &str = env!("DTFS_VERSION");

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
/// so `dtfs-* --version` reports the version + build date an operator can
/// compare against `/api/v1/nodes` (or the latest GitHub release tag).
/// `env!` expands to a string literal, so `concat!` accepts it.
pub const VERSION_STRING: &str =
    concat!(env!("DTFS_VERSION"), " (built ", env!("DTFS_BUILD_DATE"), ")");
