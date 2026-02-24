#![forbid(unsafe_code)]
//! AIONBD HTTP server.
//!
//! Exposes:
//! - `GET /live`: process liveness
//! - `GET /ready`: readiness (engine/storage checks)
//! - `POST /distance`: validated vector operation endpoint
//! - `POST /collections`: create an in-memory collection
//! - `GET /collections/:name`: collection metadata
//! - `PUT/GET/DELETE /collections/:name/points/:id`: point CRUD

use std::time::Duration;

use anyhow::{Context, Result};
use axum::error_handling::HandleErrorLayer;
use axum::extract::DefaultBodyLimit;
use axum::http::{HeaderName, Request};
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

mod config;
mod errors;
mod handlers;
mod models;
mod state;

use crate::config::AppConfig;
use crate::errors::handle_middleware_error;
use crate::handlers::{
    create_collection, delete_point, distance, get_collection, get_point, live, ready, upsert_point,
};
use crate::state::AppState;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let config = AppConfig::from_env().context("invalid configuration")?;
    let bind = config.bind;
    let state = AppState::new(config.clone());
    let app = build_app(state);

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
        "aionbd server started"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server exited unexpectedly")?;

    Ok(())
}

fn build_app(state: AppState) -> Router {
    let request_id_header = HeaderName::from_static("x-request-id");
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
        .route("/distance", post(distance))
        .route("/collections", post(create_collection))
        .route("/collections/:name", get(get_collection))
        .route(
            "/collections/:name/points/:id",
            put(upsert_point).get(get_point).delete(delete_point),
        )
        .layer(DefaultBodyLimit::max(config.max_body_bytes))
        .layer(middleware)
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
