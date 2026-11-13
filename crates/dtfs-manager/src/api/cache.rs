//! `GET /api/v1/cache/status`: leader-only (409 on a follower, like
//! `/api/v1/tiering/precache`) view of whether the cache meets its goals.

use super::*;

use crate::cache_stats::{collect_disks, STATS};

pub(super) async fn get_cache_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    check_is_leader(&state)?;
    let (high_pct, low_pct) = match &state.tiering {
        Some(t) => t.watermarks().await,
        None => (state.config.tiering.cache_high_watermark_pct, state.config.tiering.cache_low_watermark_pct),
    };
    let rows = collect_disks(&state.sm_data, &state.scheduler).await;
    let now_ms = crate::metadata::stamp_now_ms();
    let disks: Vec<Value> = rows.iter().map(|r| r.to_json(now_ms)).collect();
    STATS.set_view(now_ms, rows);
    Ok(Json(json!({
        "is_leader": true,
        "watermarks": {"high_pct": high_pct, "low_pct": low_pct},
        "disks": disks,
        "counters": STATS.counters().to_json(),
        "link_budget": state.scheduler.link_budget_status(),
    })))
}
