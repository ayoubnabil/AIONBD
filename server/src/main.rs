#![forbid(unsafe_code)]
//! AIONBD HTTP server.
//!
//! Exposes:
//! - `GET /live`: process liveness
//! - `GET /ready`: readiness (engine/storage checks)
//! - `POST /distance`: validated vector operation endpoint

use std::env;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aionbd_core::{
    cosine_similarity_with_options, dot_product_with_options, l2_distance_with_options,
    VectorError, VectorValidationOptions,
};
use anyhow::{Context, Result};
use axum::error_handling::HandleErrorLayer;
use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderName, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{BoxError, Json, Router};
use serde::{Deserialize, Serialize};
use tower::limit::ConcurrencyLimitLayer;
use tower::timeout::TimeoutLayer;
use tower::ServiceBuilder;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::trace::{DefaultOnResponse, TraceLayer};
use tower_http::LatencyUnit;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone)]
struct AppConfig {
    bind: SocketAddr,
    max_dimension: usize,
    strict_finite: bool,
    request_timeout_ms: u64,
    max_body_bytes: usize,
    max_concurrency: usize,
}

impl AppConfig {
    fn from_env() -> Result<Self> {
        let bind = parse_socket_addr("AIONBD_BIND", "127.0.0.1:8080")?;
        let max_dimension = parse_usize("AIONBD_MAX_DIMENSION", 4096)?;
        let strict_finite = parse_bool("AIONBD_STRICT_FINITE", true)?;
        let request_timeout_ms = parse_u64("AIONBD_REQUEST_TIMEOUT_MS", 2000)?;
        let max_body_bytes = parse_usize("AIONBD_MAX_BODY_BYTES", 1_048_576)?;
        let max_concurrency = parse_usize("AIONBD_MAX_CONCURRENCY", 256)?;

        if max_dimension == 0 {
            anyhow::bail!("AIONBD_MAX_DIMENSION must be > 0");
        }
        if max_body_bytes == 0 {
            anyhow::bail!("AIONBD_MAX_BODY_BYTES must be > 0");
        }
        if max_concurrency == 0 {
            anyhow::bail!("AIONBD_MAX_CONCURRENCY must be > 0");
        }

        Ok(Self {
            bind,
            max_dimension,
            strict_finite,
            request_timeout_ms,
            max_body_bytes,
            max_concurrency,
        })
    }
}

#[derive(Clone)]
struct AppState {
    started_at: Instant,
    config: Arc<AppConfig>,
    engine_loaded: Arc<AtomicBool>,
    storage_available: Arc<AtomicBool>,
}

impl AppState {
    fn new(config: AppConfig) -> Self {
        Self {
            started_at: Instant::now(),
            config: Arc::new(config),
            engine_loaded: Arc::new(AtomicBool::new(true)),
            storage_available: Arc::new(AtomicBool::new(true)),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
enum Metric {
    #[default]
    Dot,
    L2,
    Cosine,
}

#[derive(Debug, Deserialize)]
struct DistanceRequest {
    left: Vec<f32>,
    right: Vec<f32>,
    #[serde(default)]
    metric: Metric,
}

#[derive(Debug, Serialize)]
struct DistanceResponse {
    metric: Metric,
    value: f32,
}

#[derive(Debug, Serialize)]
struct LiveResponse {
    status: &'static str,
    uptime_ms: u64,
}

#[derive(Debug, Serialize)]
struct ReadyChecks {
    engine_loaded: bool,
    storage_available: bool,
}

#[derive(Debug, Serialize)]
struct ReadyResponse {
    status: &'static str,
    uptime_ms: u64,
    checks: ReadyChecks,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    code: &'static str,
    message: String,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn invalid_argument(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_argument",
            message: message.into(),
        }
    }

    fn payload_too_large(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            code: "payload_too_large",
            message: message.into(),
        }
    }

    fn request_timeout() -> Self {
        Self {
            status: StatusCode::REQUEST_TIMEOUT,
            code: "request_timeout",
            message: "request timed out".to_string(),
        }
    }

    fn service_unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "not_ready",
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal",
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                code: self.code,
                message: self.message,
            }),
        )
            .into_response()
    }
}

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

async fn live(State(state): State<AppState>) -> Json<LiveResponse> {
    Json(LiveResponse {
        status: "live",
        uptime_ms: state.started_at.elapsed().as_millis() as u64,
    })
}

async fn ready(State(state): State<AppState>) -> Result<Json<ReadyResponse>, ApiError> {
    let engine_loaded = state.engine_loaded.load(Ordering::Relaxed);
    let storage_available = state.storage_available.load(Ordering::Relaxed);

    let response = ReadyResponse {
        status: if engine_loaded && storage_available {
            "ready"
        } else {
            "not_ready"
        },
        uptime_ms: state.started_at.elapsed().as_millis() as u64,
        checks: ReadyChecks {
            engine_loaded,
            storage_available,
        },
    };

    if engine_loaded && storage_available {
        Ok(Json(response))
    } else {
        Err(ApiError::service_unavailable(
            "engine or storage is not ready",
        ))
    }
}

async fn distance(
    State(state): State<AppState>,
    payload: Result<Json<DistanceRequest>, JsonRejection>,
) -> Result<Json<DistanceResponse>, ApiError> {
    let Json(payload) = payload.map_err(map_json_rejection)?;
    validate_distance_request(&payload, &state.config)?;

    let options = VectorValidationOptions {
        strict_finite: state.config.strict_finite,
        zero_norm_epsilon: f32::EPSILON,
    };

    let value = match payload.metric {
        Metric::Dot => dot_product_with_options(&payload.left, &payload.right, options),
        Metric::L2 => l2_distance_with_options(&payload.left, &payload.right, options),
        Metric::Cosine => cosine_similarity_with_options(&payload.left, &payload.right, options),
    }
    .map_err(map_vector_error)?;

    Ok(Json(DistanceResponse {
        metric: payload.metric,
        value,
    }))
}

fn validate_distance_request(
    payload: &DistanceRequest,
    config: &AppConfig,
) -> Result<(), ApiError> {
    if payload.left.len() != payload.right.len() {
        return Err(ApiError::invalid_argument(
            "left and right must have the same length",
        ));
    }
    if payload.left.is_empty() {
        return Err(ApiError::invalid_argument("vectors must not be empty"));
    }
    if payload.left.len() > config.max_dimension {
        return Err(ApiError::invalid_argument(format!(
            "vector dimension {} exceeds configured maximum {}",
            payload.left.len(),
            config.max_dimension
        )));
    }

    if config.strict_finite {
        if let Some(index) = first_non_finite_index(&payload.left) {
            return Err(ApiError::invalid_argument(format!(
                "left contains a non-finite value at index {index}"
            )));
        }
        if let Some(index) = first_non_finite_index(&payload.right) {
            return Err(ApiError::invalid_argument(format!(
                "right contains a non-finite value at index {index}"
            )));
        }
    }

    Ok(())
}

fn first_non_finite_index(values: &[f32]) -> Option<usize> {
    values.iter().position(|value| !value.is_finite())
}

fn map_json_rejection(rejection: JsonRejection) -> ApiError {
    let status = rejection.status();
    if status == StatusCode::PAYLOAD_TOO_LARGE {
        return ApiError::payload_too_large("request body exceeds configured size limit");
    }
    if status == StatusCode::UNSUPPORTED_MEDIA_TYPE {
        return ApiError::invalid_argument("content-type must be application/json");
    }
    ApiError::invalid_argument("invalid JSON payload")
}

fn map_vector_error(error: VectorError) -> ApiError {
    match error {
        VectorError::DimensionMismatch { .. } => {
            ApiError::invalid_argument("left and right must have the same length")
        }
        VectorError::EmptyVector => ApiError::invalid_argument("vectors must not be empty"),
        VectorError::ZeroNorm { .. } => {
            ApiError::invalid_argument("cosine similarity is undefined for zero vectors")
        }
        VectorError::NonFinite { .. } => {
            ApiError::invalid_argument("vectors must only contain finite values")
        }
    }
}

async fn handle_middleware_error(error: BoxError) -> Response {
    if error.is::<tower::timeout::error::Elapsed>() {
        return ApiError::request_timeout().into_response();
    }
    tracing::error!(%error, "middleware error");
    ApiError::internal("internal middleware error").into_response()
}

fn parse_socket_addr(key: &str, default: &str) -> Result<SocketAddr> {
    let raw = env::var(key).unwrap_or_else(|_| default.to_string());
    raw.parse()
        .with_context(|| format!("{key} must be a valid socket address, got '{raw}'"))
}

fn parse_usize(key: &str, default: usize) -> Result<usize> {
    let raw = env::var(key).unwrap_or_else(|_| default.to_string());
    raw.parse()
        .with_context(|| format!("{key} must be a positive integer, got '{raw}'"))
}

fn parse_u64(key: &str, default: u64) -> Result<u64> {
    let raw = env::var(key).unwrap_or_else(|_| default.to_string());
    raw.parse()
        .with_context(|| format!("{key} must be a positive integer, got '{raw}'"))
}

fn parse_bool(key: &str, default: bool) -> Result<bool> {
    let raw = env::var(key).unwrap_or_else(|_| {
        if default {
            "true".to_string()
        } else {
            "false".to_string()
        }
    });
    match raw.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => anyhow::bail!("{key} must be a boolean, got '{raw}'"),
    }
}
