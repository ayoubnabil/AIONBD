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
//! - `GET /collections/:name/points`: paginated list of point ids
//! - `POST /collections/:name/search`: top-1 nearest/most similar point
//! - `POST /collections/:name/search/topk`: top-k nearest/most similar points

use std::time::Duration;

use aionbd_core::{checkpoint_wal, load_collections, PersistOutcome};
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
mod state;
mod tenant_quota;
#[cfg(test)]
mod tests;

use crate::auth::{auth_rate_limit_audit, AuthConfig};
use crate::config::AppConfig;
use crate::errors::handle_middleware_error;
use crate::handlers::{
    create_collection, delete_collection, distance, get_collection, list_collections, live, ready,
    upsert_point,
};
use crate::handlers_metrics::{metrics, metrics_prometheus};
use crate::handlers_points::{delete_point, get_point, list_points};
use crate::handlers_search::{search_collection, search_collection_top_k};
use crate::http_metrics::track_http_metrics;
use crate::index_manager::warmup_l2_indexes;
use crate::state::AppState;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let config = AppConfig::from_env().context("invalid configuration")?;
    let auth_config = AuthConfig::from_env().context("invalid authentication configuration")?;
    let initial_collections = load_initial_collections(&config)?;
    let bind = config.bind;
    let state =
        AppState::with_collections_and_auth(config.clone(), initial_collections, auth_config);
    warmup_l2_indexes(&state);
    let app = build_app(state.clone());

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("failed to bind server socket on {bind}"))?;

    tracing::info!(
        %bind,
        max_dimension = config.max_dimension,
        strict_finite = config.strict_finite,
        timeout_ms = config.request_timeout_ms,
        max_body_bytes = config.max_body_bytes,
        max_concurrency = config.max_concurrency,
        max_page_limit = config.max_page_limit,
        max_topk_limit = config.max_topk_limit,
        checkpoint_interval = config.checkpoint_interval,
        persistence_enabled = config.persistence_enabled,
        snapshot_path = %config.snapshot_path.display(),
        wal_path = %config.wal_path.display(),
        "aionbd server started"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server exited unexpectedly")?;
    checkpoint_before_exit(&state).await;

    Ok(())
}

pub(crate) fn build_app(state: AppState) -> Router {
    let request_id_header = HeaderName::from_static("x-request-id");
    let http_metrics_layer = middleware::from_fn_with_state(state.clone(), track_http_metrics);
    let auth_layer = middleware::from_fn_with_state(state.clone(), auth_rate_limit_audit);
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

    Router::new()
        .route("/live", get(live))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/metrics/prometheus", get(metrics_prometheus))
        .route("/distance", post(distance))
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
        .route("/collections/:name/points", get(list_points))
        .route(
            "/collections/:name/points/:id",
            put(upsert_point).get(get_point).delete(delete_point),
        )
        .layer(DefaultBodyLimit::max(config.max_body_bytes))
        .layer(auth_layer)
        .layer(middleware)
        .layer(http_metrics_layer)
        .with_state(state)
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
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
    let result =
        tokio::task::spawn_blocking(move || checkpoint_wal(&snapshot_path, &wal_path)).await;
    match result {
        Ok(Ok(PersistOutcome::Checkpointed)) => {
            tracing::info!("shutdown checkpoint completed");
        }
        Ok(Ok(PersistOutcome::WalOnly { reason })) => {
            tracing::warn!(%reason, "shutdown checkpoint degraded to wal-only");
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
