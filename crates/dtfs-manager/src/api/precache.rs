//! `GET /api/v1/tiering/precache`: leader-only view of the pre-cache planner.

use super::*;

pub(super) async fn get_precache_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    check_is_leader(&state)?;
    let status = state.tiering.as_ref().map(|t| t.precache_status()).unwrap_or_default();
    Ok(Json(serde_json::to_value(status).unwrap_or_else(|_| json!({}))))
}
