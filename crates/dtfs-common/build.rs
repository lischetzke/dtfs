// Bug AY — emit DTFS_BUILD_DATE at compile time so every binary carries
// the date of the build that produced it. Re-run only when the build
// script itself changes; otherwise cargo would invalidate the cache on
// every invocation and force rebuilds across the workspace.
//
// SOURCE_DATE_EPOCH is honoured for reproducible-build environments;
// when set, we format that fixed instant instead of "now".

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
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");
}
