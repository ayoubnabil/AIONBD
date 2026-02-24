use axum::extract::{Path, Query, State};
use axum::Json;

use crate::errors::ApiError;
use crate::handler_utils::canonical_collection_name;
use crate::models::{ListPointsQuery, ListPointsResponse, PointIdResponse};
use crate::state::AppState;

pub(crate) async fn list_points(
    Path(name): Path<String>,
    State(state): State<AppState>,
    Query(query): Query<ListPointsQuery>,
) -> Result<Json<ListPointsResponse>, ApiError> {
    let name = canonical_collection_name(&name)?;
    if query.limit == 0 {
        return Err(ApiError::invalid_argument("limit must be > 0"));
    }

    let collections = state
        .collections
        .read()
        .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;

    let collection = collections
        .get(&name)
        .ok_or_else(|| ApiError::not_found(format!("collection '{name}' not found")))?;

    let ids = collection.point_ids();
    let total = ids.len();
    let points: Vec<PointIdResponse> = ids
        .into_iter()
        .skip(query.offset)
        .take(query.limit)
        .map(|id| PointIdResponse { id })
        .collect();

    let next_offset = if query.offset + points.len() < total {
        Some(query.offset + points.len())
    } else {
        None
    };

    Ok(Json(ListPointsResponse {
        points,
        total,
        next_offset,
    }))
}
