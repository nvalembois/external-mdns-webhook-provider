use std::sync::Arc;

use axum::{Json, extract::State, http::{StatusCode, header}, response::IntoResponse};
use tracing::debug;

use crate::{model::{configmap::ConfigMapStore, records::{Changes, Endpoint}, state::AppState}, routes::WEBHOOK_CONTENT_TYPE};

#[axum::debug_handler]
pub async fn get_records(
    State(cm_store): State<Arc<ConfigMapStore>>
) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, WEBHOOK_CONTENT_TYPE)], 
        Json(cm_store.into_endpoints().await)
    )
}

#[axum::debug_handler]
pub async fn post_records(
    State(state): State<AppState>,
    Json(mut changes): Json<Changes>
) -> Result <impl IntoResponse, String> {
    debug!("in create records: {:?}", changes.create);
    debug!("in delete records: {:?}", changes.delete);
    debug!("in update new records: {:?}", changes.update_new);
    debug!("in update old records: {:?}", changes.update_old);

    if !state.app_config.dry_run { 
        state.cm_store.appy_changes(&mut changes).await?;
    }

    Ok((
        StatusCode::NO_CONTENT,
        [(header::CONTENT_TYPE, WEBHOOK_CONTENT_TYPE)], 
        Json(changes)
    ))
}

#[axum::debug_handler]
pub async fn post_adjustendpoints(
    Json(mut records): Json<Vec<Endpoint>>,
) -> impl IntoResponse
{
    for record in &mut records {
        record.set_identifier = None;
        record.record_t_t_l = None;
        record.labels = None;
        record.provider_specific = None;
    }
    (
        [(header::CONTENT_TYPE, WEBHOOK_CONTENT_TYPE)],
        Json(records)
    )
}
