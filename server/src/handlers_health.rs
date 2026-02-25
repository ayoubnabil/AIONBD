use std::sync::atomic::Ordering;

use aionbd_core::{
    cosine_similarity_with_options, dot_product_with_options, l2_distance_with_options,
    VectorValidationOptions,
};
use axum::extract::rejection::JsonRejection;
use axum::extract::State;
use axum::Json;

use crate::errors::{map_json_rejection, map_vector_error, ApiError};
use crate::handler_utils::validate_distance_request;
use crate::models::{
    DistanceRequest, DistanceResponse, LiveResponse, Metric, ReadyChecks, ReadyResponse,
};
use crate::state::AppState;

pub(crate) async fn live(State(state): State<AppState>) -> Json<LiveResponse> {
    Json(LiveResponse {
        status: "live",
        uptime_ms: state.started_at.elapsed().as_millis() as u64,
    })
}

pub(crate) async fn ready(State(state): State<AppState>) -> Result<Json<ReadyResponse>, ApiError> {
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

pub(crate) async fn distance(
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
