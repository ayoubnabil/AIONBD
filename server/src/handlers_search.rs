use aionbd_core::{
    cosine_similarity_with_options, dot_product_with_options, l2_distance_with_options, Collection,
    VectorValidationOptions,
};
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::Json;

use crate::errors::{map_json_rejection, map_vector_error, ApiError};
use crate::handler_utils::canonical_collection_name;
use crate::models::{
    Metric, SearchHit, SearchRequest, SearchResponse, SearchTopKRequest, SearchTopKResponse,
};
use crate::state::AppState;

pub(crate) async fn search_collection(
    Path(name): Path<String>,
    State(state): State<AppState>,
    payload: Result<Json<SearchRequest>, JsonRejection>,
) -> Result<Json<SearchResponse>, ApiError> {
    let name = canonical_collection_name(&name)?;
    let Json(payload) = payload.map_err(map_json_rejection)?;

    let collections = state
        .collections
        .read()
        .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;

    let collection = collections
        .get(&name)
        .ok_or_else(|| ApiError::not_found(format!("collection '{name}' not found")))?;

    let mut scored = score_collection(collection, &payload.query, payload.metric)?;
    sort_scores(&mut scored, payload.metric);

    let (id, value) = scored
        .into_iter()
        .next()
        .ok_or_else(|| ApiError::invalid_argument("collection contains no points"))?;

    Ok(Json(SearchResponse {
        id,
        metric: payload.metric,
        value,
    }))
}

pub(crate) async fn search_collection_top_k(
    Path(name): Path<String>,
    State(state): State<AppState>,
    payload: Result<Json<SearchTopKRequest>, JsonRejection>,
) -> Result<Json<SearchTopKResponse>, ApiError> {
    let name = canonical_collection_name(&name)?;
    let Json(payload) = payload.map_err(map_json_rejection)?;

    if payload.limit == 0 {
        return Err(ApiError::invalid_argument("limit must be > 0"));
    }

    let collections = state
        .collections
        .read()
        .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;

    let collection = collections
        .get(&name)
        .ok_or_else(|| ApiError::not_found(format!("collection '{name}' not found")))?;

    let mut scored = score_collection(collection, &payload.query, payload.metric)?;
    sort_scores(&mut scored, payload.metric);

    let hits = scored
        .into_iter()
        .take(payload.limit)
        .map(|(id, value)| SearchHit { id, value })
        .collect();

    Ok(Json(SearchTopKResponse {
        metric: payload.metric,
        hits,
    }))
}

fn score_collection(
    collection: &Collection,
    query: &[f32],
    metric: Metric,
) -> Result<Vec<(u64, f32)>, ApiError> {
    if collection.is_empty() {
        return Err(ApiError::invalid_argument("collection contains no points"));
    }
    if query.len() != collection.dimension() {
        return Err(ApiError::invalid_argument(format!(
            "query dimension {} does not match collection dimension {}",
            query.len(),
            collection.dimension()
        )));
    }

    let options = VectorValidationOptions {
        strict_finite: collection.strict_finite(),
        zero_norm_epsilon: f32::EPSILON,
    };

    let mut scored = Vec::with_capacity(collection.len());
    for id in collection.point_ids() {
        let values = collection
            .get_point(id)
            .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;

        let value = match metric {
            Metric::Dot => dot_product_with_options(query, values, options),
            Metric::L2 => l2_distance_with_options(query, values, options),
            Metric::Cosine => cosine_similarity_with_options(query, values, options),
        }
        .map_err(map_vector_error)?;

        scored.push((id, value));
    }

    Ok(scored)
}

fn sort_scores(scored: &mut [(u64, f32)], metric: Metric) {
    scored.sort_by(|left, right| match metric {
        Metric::L2 => left
            .1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0)),
        Metric::Dot | Metric::Cosine => right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0)),
    });
}
