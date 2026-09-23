use axum::routing::{get, post};
use clap::Parser;
use mdns_webhook_provider::model::config::WebhookConfig;
use mdns_webhook_provider::model::configmap::ConfigMapStore;
use mdns_webhook_provider::model::state::AppState;
use mdns_webhook_provider::routes::health::get_healthz;
use mdns_webhook_provider::routes::records::{get_records, post_adjustendpoints, post_records};
use mdns_webhook_provider::routes::root::get_root;
use axum::Router;
use tokio::net::TcpListener;
use tokio::signal;
use std::process::ExitCode;
use std::sync::Arc;
use tracing::{error, info};

#[tokio::main]
async fn main() -> ExitCode {
    let app_config = WebhookConfig::parse();

    tracing_subscriber::fmt()
        .with_max_level(if app_config.debug {tracing::Level::DEBUG} else { tracing::Level::INFO} )
        .init();

    info!("Config: filters={}", &app_config.domain_filter.filters.join(","));
    info!("Config: exclude={}", &app_config.domain_filter.exclude.join(","));
    info!("Config: regex={}", &app_config.domain_filter.regex);
    info!("Config: regex_exclusion={}", &app_config.domain_filter.regex_exclusion);
    info!("Config: host_configmap_name={}", &app_config.host_configmap_name);
    info!("Config: host_configmap_namespace={}", &app_config.host_configmap_namespace);
    info!("Config: host_configmap_key={}", &app_config.host_configmap_key);
    info!("Config: listen_addr={}", &app_config.listen_addr);
    info!("Config: health_listen_addr={}", &app_config.health_listen_addr);
    info!("Config: dry_run={}", &app_config.dry_run);
    info!("Config: debug={}", &app_config.debug);

    match run(app_config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!("{e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(app_config: WebhookConfig) -> Result<(), String> {
    let cm_store = ConfigMapStore::new(
        &app_config.host_configmap_namespace,
        &app_config.host_configmap_name,
        &app_config.host_configmap_key).await
        .map_err(|e| format!("ConfigMap store init error : {e}"))?;
        
    // Create `TcpListener` using tokio
    let webhook_listener = TcpListener::bind(app_config.listen_addr.clone()).await
        .map_err(|e| format!("Listen for webhok error : {e}"))?;
    let health_listener = TcpListener::bind(app_config.health_listen_addr.clone()).await
        .map_err(|e| format!("Listen for health error : {e}"))?;
    let app_state = AppState {
        app_config: Arc::new(app_config),
        cm_store: Arc::new(cm_store),
    };
    // Create `Router`
    let webhook_router = Router::new()
        .route("/records", get(get_records))
        .route("/records", post(post_records))
        .route("/adjustendpoints", post(post_adjustendpoints))
        .route("/", get(get_root))
        .with_state(app_state);
    let health_router = Router::new()
        .route("/healthz", get(get_healthz));

    // Run the servers with graceful shutdown
    let webhook = axum::serve(webhook_listener, webhook_router)
        .with_graceful_shutdown(shutdown_signal_webhook());
    let health = axum::serve(health_listener, health_router)
        .with_graceful_shutdown(shutdown_signal_health());
     
        // Les deux serveurs tournent en parallèle sur la même tâche async
    let (webhook_result, health_result) = tokio::join!(webhook, health);

    if let Err(e) = health_result {
        error!("Shutdown server health error : {e}");
    }
    webhook_result.
        map_err(|e| format!("Shutdown server webhook error : {e}"))
}


async fn shutdown_signal_webhook() {
    shutdown_signal("webhook").await
}

async fn shutdown_signal_health() {
    shutdown_signal("health").await
}

async fn shutdown_signal(server: &str) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("échec de l'installation du handler Ctrl+C");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("échec de l'installation du handler SIGTERM")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Signal Ctrl+C reçu, arrêt de {server} en cours...");
        },
        _ = terminate => {
            info!("Signal SIGTERM reçu, arrêt de {server} en cours...");
        },
    }
}



