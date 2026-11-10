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

/// CO-5-15 — parse the numeric `(major, minor, patch)` tuple out of a
/// version string. Tolerates a leading `v`, surrounding text (the binary
/// `--version` prints `v0.1.9 (built ...)`), and missing components
/// (`v1` -> `(1, 0, 0)`). Returns `None` if no leading-numeric version
/// token is found. A trailing pre-release/build suffix on the patch
/// component (`v1.2.3-rc1`) is ignored for ordering.
pub fn parse_semver(s: &str) -> Option<(u64, u64, u64)> {
    // Take the first whitespace-delimited token, drop an optional `v`.
    let tok = s.trim().split_whitespace().next()?;
    let tok = tok.strip_prefix('v').or_else(|| tok.strip_prefix('V')).unwrap_or(tok);
    let mut it = tok.split('.');
    let parse_part = |p: Option<&str>| -> u64 {
        p.map(|p| {
            // Stop at the first non-digit (pre-release/build suffix).
            let digits: String = p.chars().take_while(|c| c.is_ascii_digit()).collect();
            digits.parse().unwrap_or(0)
        })
        .unwrap_or(0)
    };
    let first = it.next()?;
    // The major component must start with a digit, else this is not a
    // version token (avoids treating "dtfs-cli" as 0.0.0).
    if !first.starts_with(|c: char| c.is_ascii_digit()) {
        return None;
    }
    let major = parse_part(Some(first));
    let minor = parse_part(it.next());
    let patch = parse_part(it.next());
    Some((major, minor, patch))
}

/// CO-5-15 — semver-aware "is `latest` strictly newer than `installed`?".
/// A naive string compare orders `v0.1.10 < v0.1.9`; this compares the
/// numeric `(major, minor, patch)` tuples. Unparseable versions
/// (e.g. `not installed`, `v0.0.0-dev`) make the answer conservative:
/// if `installed` can't be parsed but `latest` can, an update is
/// available; otherwise (latest unparseable, or both unparseable) false.
pub fn version_is_newer(installed: &str, latest: &str) -> bool {
    match (parse_semver(installed), parse_semver(latest)) {
        (Some(i), Some(l)) => l > i,
        (None, Some(_)) => true,
        _ => false,
    }
}

/// Feature gate on a node's reported `VERSION`: true only for an exact
/// release stamp `vX.Y.Z` (leading `v` optional) at or above `min`. Dev
/// builds (`v0.0.0-dev`), pre-release/build suffixes (`v0.1.14-rc1`), and
/// empty or unparsable strings never pass, so a gated behaviour stays off
/// until every node runs a real release that has it.
pub fn release_at_least(version: &str, min: (u64, u64, u64)) -> bool {
    let v = version.strip_prefix('v').unwrap_or(version);
    let parts: Vec<&str> = v.split('.').collect();
    parts.len() == 3
        && parts.iter().all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
        && parse_semver(v).is_some_and(|t| t >= min)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn co_5_15_version_is_newer_is_semver_aware() {
        // The motivating bug: a naive string compare orders
        // "v0.1.10" < "v0.1.9"; the numeric compare must not.
        assert!(version_is_newer("v0.1.9", "v0.1.10"));
        assert!(!version_is_newer("v0.1.10", "v0.1.9"));
        // Equal -> not newer.
        assert!(!version_is_newer("v0.1.9", "v0.1.9"));
        // Major/minor ordering.
        assert!(version_is_newer("v0.9.9", "v1.0.0"));
        assert!(!version_is_newer("v2.0.0", "v1.9.9"));
        // Tolerates the `v` prefix being absent and trailing build text
        // (the binary `--version` prints "v0.1.9 (built 20260101-000000)").
        assert!(version_is_newer("v0.1.9 (built 20260101-000000)", "0.1.10"));
        // Missing components default to 0: v1 == v1.0.0.
        assert!(!version_is_newer("v1", "v1.0.0"));
        assert!(version_is_newer("v1", "v1.0.1"));
        // Unparseable installed (not yet installed) but a real latest ->
        // update available.
        assert!(version_is_newer("not installed", "v0.1.0"));
        // Unparseable latest -> conservatively not newer.
        assert!(!version_is_newer("v0.1.0", "<unknown>"));
    }

    #[test]
    fn release_at_least_accepts_release_stamps_at_or_above_min() {
        let min = (0, 1, 14);
        assert!(release_at_least("v0.1.14", min));
        assert!(release_at_least("0.1.14", min));
        assert!(release_at_least("v0.1.15", min));
        assert!(release_at_least("v0.2.0", min));
        assert!(release_at_least("v1.0.0", min));
    }

    #[test]
    fn release_at_least_rejects_old_dev_suffixed_and_garbage() {
        let min = (0, 1, 14);
        // Boundary: one patch below.
        assert!(!release_at_least("v0.1.13", min));
        assert!(!release_at_least("v0.0.0-dev", min));
        assert!(!release_at_least("v0.1.14-rc1", min));
        assert!(!release_at_least("v9.9.9-dev", min));
        assert!(!release_at_least("v0.1.14 (built 20261110-000000)", min));
        assert!(!release_at_least("v0.1", min));
        assert!(!release_at_least("", min));
        assert!(!release_at_least("not installed", min));
    }
}
