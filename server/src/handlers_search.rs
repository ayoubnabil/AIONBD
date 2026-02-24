use aionbd_core::{
    cosine_similarity_with_options, dot_product_with_options, l2_distance_with_options,
    VectorValidationOptions,
};
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::Json;

use crate::errors::{map_json_rejection, map_vector_error, ApiError};
use crate::handler_utils::canonical_collection_name;
use crate::models::{Metric, SearchRequest, SearchResponse};
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

    if collection.is_empty() {
        return Err(ApiError::invalid_argument("collection contains no points"));
    }
    if payload.query.len() != collection.dimension() {
        return Err(ApiError::invalid_argument(format!(
            "query dimension {} does not match collection dimension {}",
            payload.query.len(),
            collection.dimension()
        )));
    }

    let options = VectorValidationOptions {
        strict_finite: collection.strict_finite(),
        zero_norm_epsilon: f32::EPSILON,
    };

    let mut best: Option<(u64, f32)> = None;
    for id in collection.point_ids() {
        let values = collection
            .get_point(id)
            .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;

        let value = match payload.metric {
            Metric::Dot => dot_product_with_options(&payload.query, values, options),
            Metric::L2 => l2_distance_with_options(&payload.query, values, options),
            Metric::Cosine => cosine_similarity_with_options(&payload.query, values, options),
        }
        .map_err(map_vector_error)?;

        let should_replace = match best {
            None => true,
            Some((_, current)) => match payload.metric {
                Metric::L2 => value < current,
                Metric::Dot | Metric::Cosine => value > current,
            },
        };

        if should_replace {
            best = Some((id, value));
        }
    }

    let (id, value) =
        best.ok_or_else(|| ApiError::invalid_argument("collection contains no points"))?;
    Ok(Json(SearchResponse {
        id,
        metric: payload.metric,
        value,
    }))
}
