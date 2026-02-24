use std::sync::atomic::Ordering;

use aionbd_core::{
    cosine_similarity_with_options, dot_product_with_options, l2_distance_with_options, Collection,
    CollectionConfig, VectorValidationOptions,
};
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::Json;

use crate::config::AppConfig;
use crate::errors::{map_collection_error, map_json_rejection, map_vector_error, ApiError};
use crate::models::{
    CollectionResponse, CreateCollectionRequest, DeletePointResponse, DistanceRequest,
    DistanceResponse, ListCollectionsResponse, LiveResponse, Metric, PointResponse, ReadyChecks,
    ReadyResponse, UpsertPointRequest, UpsertPointResponse,
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

pub(crate) async fn create_collection(
    State(state): State<AppState>,
    payload: Result<Json<CreateCollectionRequest>, JsonRejection>,
) -> Result<Json<CollectionResponse>, ApiError> {
    let Json(payload) = payload.map_err(map_json_rejection)?;
    let name = canonical_collection_name(&payload.name)?;

    let config = CollectionConfig::new(payload.dimension, payload.strict_finite)
        .map_err(map_collection_error)?;
    let collection = Collection::new(name.clone(), config).map_err(map_collection_error)?;

    let mut collections = state
        .collections
        .write()
        .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;

    if collections.contains_key(&name) {
        return Err(ApiError::conflict(format!(
            "collection '{name}' already exists"
        )));
    }

    let response = build_collection_response(&collection);
    collections.insert(name, collection);

    Ok(Json(response))
}

pub(crate) async fn list_collections(
    State(state): State<AppState>,
) -> Result<Json<ListCollectionsResponse>, ApiError> {
    let collections = state
        .collections
        .read()
        .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;

    let items = collections
        .values()
        .map(build_collection_response)
        .collect();
    Ok(Json(ListCollectionsResponse { collections: items }))
}

pub(crate) async fn get_collection(
    Path(name): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<CollectionResponse>, ApiError> {
    let name = canonical_collection_name(&name)?;

    let collections = state
        .collections
        .read()
        .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;

    let collection = collections
        .get(&name)
        .ok_or_else(|| ApiError::not_found(format!("collection '{name}' not found")))?;

    Ok(Json(build_collection_response(collection)))
}

pub(crate) async fn upsert_point(
    Path((name, id)): Path<(String, u64)>,
    State(state): State<AppState>,
    payload: Result<Json<UpsertPointRequest>, JsonRejection>,
) -> Result<Json<UpsertPointResponse>, ApiError> {
    let name = canonical_collection_name(&name)?;
    let Json(payload) = payload.map_err(map_json_rejection)?;

    let mut collections = state
        .collections
        .write()
        .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;

    let collection = collections
        .get_mut(&name)
        .ok_or_else(|| ApiError::not_found(format!("collection '{name}' not found")))?;

    let created = collection
        .upsert_point(id, payload.values)
        .map_err(map_collection_error)?;

    Ok(Json(UpsertPointResponse { id, created }))
}

pub(crate) async fn get_point(
    Path((name, id)): Path<(String, u64)>,
    State(state): State<AppState>,
) -> Result<Json<PointResponse>, ApiError> {
    let name = canonical_collection_name(&name)?;

    let collections = state
        .collections
        .read()
        .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;

    let collection = collections
        .get(&name)
        .ok_or_else(|| ApiError::not_found(format!("collection '{name}' not found")))?;

    let values = collection
        .get_point(id)
        .ok_or_else(|| ApiError::not_found(format!("point '{id}' not found")))?;

    Ok(Json(PointResponse {
        id,
        values: values.to_vec(),
    }))
}

pub(crate) async fn delete_point(
    Path((name, id)): Path<(String, u64)>,
    State(state): State<AppState>,
) -> Result<Json<DeletePointResponse>, ApiError> {
    let name = canonical_collection_name(&name)?;

    let mut collections = state
        .collections
        .write()
        .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;

    let collection = collections
        .get_mut(&name)
        .ok_or_else(|| ApiError::not_found(format!("collection '{name}' not found")))?;

    if collection.remove_point(id).is_none() {
        return Err(ApiError::not_found(format!("point '{id}' not found")));
    }

    Ok(Json(DeletePointResponse { id, deleted: true }))
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

fn canonical_collection_name(name: &str) -> Result<String, ApiError> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(ApiError::invalid_argument(
            "collection name must not be empty",
        ));
    }

    Ok(trimmed.to_string())
}

fn build_collection_response(collection: &Collection) -> CollectionResponse {
    CollectionResponse {
        name: collection.name().to_string(),
        dimension: collection.dimension(),
        strict_finite: collection.strict_finite(),
        point_count: collection.len(),
    }
}
