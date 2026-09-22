use std::sync::Arc;
use axum::extract::FromRef;
use crate::model::{config::{WebhookConfig, DomainFilter}, configmap::ConfigMapStore};

#[derive(FromRef, Clone)]
pub struct AppState {
    pub cm_store: Arc<ConfigMapStore>,
    pub app_config: Arc<WebhookConfig>,
}

impl FromRef<AppState> for DomainFilter {
    fn from_ref(app_state: &AppState) -> DomainFilter {
        app_state.app_config.domain_filter.clone()
    }
}

