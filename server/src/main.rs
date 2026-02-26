#![forbid(unsafe_code)]
//! AIONBD HTTP server.
//!
//! Exposes:
//! - `GET /live`: process liveness
//! - `GET /ready`: readiness (engine/storage checks)
//! - `GET /metrics`: runtime counters and state
//! - `GET /metrics/prometheus`: runtime counters in Prometheus text format
//! - `POST /distance`: validated vector operation endpoint
//! - `POST /collections`: create an in-memory collection
//! - `GET/DELETE /collections/:name`: collection metadata and deletion
//! - `PUT/GET/DELETE /collections/:name/points/:id`: point CRUD
//! - `POST /collections/:name/points`: batch upsert points
//! - `GET /collections/:name/points`: paginated list of point ids
//! - `POST /collections/:name/search`: top-1 nearest/most similar point
//! - `POST /collections/:name/search/topk`: top-k nearest/most similar points
//! - `POST /collections/:name/search/topk/batch`: batched top-k queries

use std::time::Duration;

use aionbd_core::{checkpoint_wal_with_policy, load_collections, CheckpointPolicy};
use anyhow::{Context, Result};
use axum::error_handling::HandleErrorLayer;
use axum::extract::DefaultBodyLimit;
use axum::http::{HeaderName, Request};
use axum::middleware;
use axum::routing::{get, post, put};
use axum::Router;
use tower::limit::ConcurrencyLimitLayer;
use tower::timeout::TimeoutLayer;
use tower::ServiceBuilder;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::trace::{DefaultOnResponse, TraceLayer};
use tower_http::LatencyUnit;
use tracing_subscriber::EnvFilter;

mod auth;
mod config;
mod engine_guard;
mod env_utils;
mod errors;
mod handler_utils;
mod handlers;
mod handlers_health;
mod handlers_metrics;
mod handlers_points;
mod handlers_search;
mod http_metrics;
mod index_manager;
mod ivf_index;
mod models;
mod persistence;
mod persistence_backlog;
mod persistence_queue;
mod prometheus_exporter;
mod resource_manager;
mod state;
mod tenant_quota;
#[cfg(test)]
mod tests;
mod tls;
mod write_path;

use crate::auth::{auth_rate_limit_audit, AuthConfig, AuthMode};
use crate::config::AppConfig;
use crate::engine_guard::require_engine_loaded;
use crate::errors::handle_middleware_error;
use crate::handlers::{
    create_collection, delete_collection, distance, get_collection, list_collections, live, ready,
    upsert_point, upsert_points_batch,
};
use crate::handlers_metrics::{metrics, metrics_prometheus};
use crate::handlers_points::{delete_point, get_point, list_points};
use crate::handlers_search::{
    search_collection, search_collection_top_k, search_collection_top_k_batch,
};
use crate::http_metrics::track_http_metrics;
use crate::index_manager::warmup_l2_indexes;
use crate::state::AppState;
use crate::tls::TlsRuntimeConfig;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let config = AppConfig::from_env().context("invalid configuration")?;
    let tls_config = TlsRuntimeConfig::from_env().context("invalid TLS configuration")?;
    let auth_config = AuthConfig::from_env().context("invalid authentication configuration")?;
    let initial_collections = load_initial_collections(&config)?;
    let bind = config.bind;
    if !bind.ip().is_loopback() && auth_config.mode == AuthMode::Disabled {
        tracing::warn!(
            %bind,
            "authentication is disabled while listening on a non-loopback address"
        );
    }
    if !bind.ip().is_loopback() && !tls_config.enabled() {
        tracing::warn!(
            %bind,
            "TLS is disabled while listening on a non-loopback address"
        );
    }
    let state =
        AppState::try_with_collections_and_auth(config.clone(), initial_collections, auth_config)
            .context("failed to initialize application state")?;
    warmup_l2_indexes(&state);
    let app = build_app(state.clone());

    tracing::info!(
        %bind,
        tls_enabled = tls_config.enabled(),
        tls_cert_path = tls_config
            .cert_path()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "-".to_string()),
        tls_key_path = tls_config
            .key_path()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "-".to_string()),
        max_dimension = config.max_dimension,
        max_points_per_collection = config.max_points_per_collection,
        memory_budget_bytes = config.memory_budget_bytes,
        strict_finite = config.strict_finite,
        timeout_ms = config.request_timeout_ms,
        max_body_bytes = config.max_body_bytes,
        max_concurrency = config.max_concurrency,
        max_page_limit = config.max_page_limit,
        max_topk_limit = config.max_topk_limit,
        checkpoint_interval = config.checkpoint_interval,
        persistence_enabled = config.persistence_enabled,
        wal_sync_on_write = config.wal_sync_on_write,
        wal_sync_every_n_writes = config.wal_sync_every_n_writes,
        wal_sync_interval_seconds = config.wal_sync_interval_seconds,
        wal_group_commit_max_batch = config.wal_group_commit_max_batch,
        wal_group_commit_flush_delay_ms = config.wal_group_commit_flush_delay_ms,
        async_checkpoints = config.async_checkpoints,
        checkpoint_compact_after = config.checkpoint_compact_after,
        snapshot_path = %config.snapshot_path.display(),
        wal_path = %config.wal_path.display(),
        "aionbd server started"
    );

    run_server(app, bind, &tls_config).await?;
    checkpoint_before_exit(&state).await;

    Ok(())
}

async fn run_server(
    app: Router,
    bind: std::net::SocketAddr,
    tls_config: &TlsRuntimeConfig,
) -> Result<()> {
    if let Some(rustls_config) = tls_config.rustls_config().await? {
        let handle = axum_server::Handle::new();
        let shutdown_handle = handle.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            shutdown_handle.graceful_shutdown(Some(Duration::from_secs(30)));
        });
        axum_server::bind_rustls(bind, rustls_config)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .context("TLS server exited unexpectedly")?;
        return Ok(());
    }

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("failed to bind server socket on {bind}"))?;

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server exited unexpectedly")?;
    Ok(())
}

pub(crate) fn build_app(state: AppState) -> Router {
    persistence_backlog::initialize_cache(&state);
    let request_id_header = HeaderName::from_static("x-request-id");
    let http_metrics_layer = middleware::from_fn_with_state(state.clone(), track_http_metrics);
    let auth_layer = middleware::from_fn_with_state(state.clone(), auth_rate_limit_audit);
    let engine_guard_layer = middleware::from_fn_with_state(state.clone(), require_engine_loaded);
    let config = state.config.clone();
    let timeout = Duration::from_millis(config.request_timeout_ms);

    let middleware = ServiceBuilder::new()
        .layer(SetRequestIdLayer::new(
            request_id_header.clone(),
            MakeRequestUuid,
        ))
        .layer(PropagateRequestIdLayer::new(request_id_header.clone()))
        .layer(HandleErrorLayer::new(handle_middleware_error))
        .layer(TimeoutLayer::new(timeout))
        .layer(ConcurrencyLimitLayer::new(config.max_concurrency))
        .layer(RequestBodyLimitLayer::new(config.max_body_bytes))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(move |request: &Request<_>| {
                    let request_id = request
                        .headers()
                        .get(&request_id_header)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or("-");
                    tracing::info_span!(
                        "http_request",
                        method = %request.method(),
                        uri = %request.uri(),
                        request_id = %request_id
                    )
                })
                .on_response(DefaultOnResponse::new().latency_unit(LatencyUnit::Millis)),
        );

    let data_routes = Router::new()
        .route(
            "/collections",
            post(create_collection).get(list_collections),
        )
        .route(
            "/collections/:name",
            get(get_collection).delete(delete_collection),
        )
        .route("/collections/:name/search", post(search_collection))
        .route(
            "/collections/:name/search/topk",
            post(search_collection_top_k),
        )
        .route(
            "/collections/:name/search/topk/batch",
            post(search_collection_top_k_batch),
        )
        .route(
            "/collections/:name/points",
            get(list_points).post(upsert_points_batch),
        )
        .route(
            "/collections/:name/points/:id",
            put(upsert_point).get(get_point).delete(delete_point),
        )
        .layer(engine_guard_layer);

    Router::new()
        .route("/live", get(live))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/metrics/prometheus", get(metrics_prometheus))
        .route("/distance", post(distance))
        .merge(data_routes)
        .layer(DefaultBodyLimit::max(config.max_body_bytes))
        .layer(auth_layer)
        .layer(middleware)
        .layer(http_metrics_layer)
        .with_state(state)
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    if let Err(error) = tracing_subscriber::fmt().with_env_filter(filter).try_init() {
        eprintln!("failed to initialize tracing subscriber: {error}");
    }
}

async fn shutdown_signal() {
    match tokio::signal::ctrl_c().await {
        Ok(()) => tracing::info!("shutdown signal received"),
        Err(error) => tracing::error!(%error, "failed to install Ctrl-C handler"),
    }
}

async fn checkpoint_before_exit(state: &AppState) {
    if !state.config.persistence_enabled {
        return;
    }
    let snapshot_path = state.config.snapshot_path.clone();
    let wal_path = state.config.wal_path.clone();
    let checkpoint_policy = CheckpointPolicy {
        incremental_compact_after: state.config.checkpoint_compact_after,
    };
    let result = tokio::task::spawn_blocking(move || {
        checkpoint_wal_with_policy(&snapshot_path, &wal_path, checkpoint_policy)
    })
    .await;
    match result {
        Ok(Ok(())) => {
            tracing::info!("shutdown checkpoint completed");
        }
        Ok(Err(error)) => {
            tracing::warn!(%error, "shutdown checkpoint failed");
        }
        Err(error) => {
            tracing::warn!(%error, "shutdown checkpoint task failed");
        }
    }
}

fn load_initial_collections(
    config: &AppConfig,
) -> Result<std::collections::BTreeMap<String, aionbd_core::Collection>> {
    if !config.persistence_enabled {
        return Ok(std::collections::BTreeMap::new());
    }

    load_collections(&config.snapshot_path, &config.wal_path).with_context(|| {
        format!(
            "failed to load persisted data from snapshot '{}' and wal '{}'",
            config.snapshot_path.display(),
            config.wal_path.display()
        )
    })
}
