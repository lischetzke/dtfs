// Bug AY / Bug BS — emit DTFS_BUILD_DATE at compile time so every binary
// carries the date of the build that produced it.
//
// Freshness is driven by SOURCE_DATE_EPOCH, NOT by re-running this script
// on every `cargo build`. The authoritative build wrappers
// (scripts/build-linux-amd64.sh, scripts/dtfs-build.sh) export
// `SOURCE_DATE_EPOCH=$(date -u +%s)` before `cargo build`; because we
// declare `rerun-if-env-changed=SOURCE_DATE_EPOCH`, cargo re-runs this
// script (and re-stamps) whenever that value changes — i.e. every wrapper
// invocation — so two wrapper builds a minute apart get two distinct
// stamps (CLAUDE.md §A.3.6). A plain `cargo build` with the var unset
// leaves the cached stamp untouched, keeping local incremental builds
// fast (the broken pre-BS `rerun-if-changed=build.rs` gating froze the
// stamp at the first-ever build instead).
//
// SOURCE_DATE_EPOCH is ALSO the reproducible-build pin: a caller that
// exports a fixed value gets a fixed, deterministic stamp (the sanctioned
// exception in CLAUDE.md §A.3.6).

use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    let secs = match env::var("SOURCE_DATE_EPOCH").ok().and_then(|s| s.parse::<i64>().ok()) {
        Some(epoch) => epoch,
        None => SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0),
    };
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0)
        .unwrap_or_else(|| chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).unwrap());
    // Format: YYYYMMDD-HHmmss (UTC instant, no trailing zone suffix —
    // the value is always UTC by construction).
    let build_date = dt.format("%Y%m%d-%H%M%S").to_string();
    println!("cargo:rustc-env=DTFS_BUILD_DATE={build_date}");

    // AZ-5 — release version stamp. The release wrapper
    // (scripts/dtfs-release.sh) exports DTFS_VERSION=<tag>; unset/empty
    // keeps the dev default so plain `cargo build` and the dev wrappers
    // report `v0.0.0-dev` byte-for-byte as before. Pair the
    // rerun-if-env-changed so cargo re-stamps when the value changes.
    let version = env::var("DTFS_VERSION")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "v0.0.0-dev".to_string());
    println!("cargo:rustc-env=DTFS_VERSION={version}");

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");
    println!("cargo:rerun-if-env-changed=DTFS_VERSION");
}
