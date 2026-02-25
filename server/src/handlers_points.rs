use axum::extract::{Extension, Path, Query, State};
use axum::Json;

use crate::auth::TenantContext;
use crate::errors::ApiError;
use crate::handler_utils::collection_handle;
use crate::models::{ListPointsQuery, ListPointsResponse, PointIdResponse, DEFAULT_PAGE_LIMIT};
use crate::state::AppState;

const MAX_OFFSET_SCAN: usize = 100_000;

pub(crate) async fn list_points(
    Path(name): Path<String>,
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Query(query): Query<ListPointsQuery>,
) -> Result<Json<ListPointsResponse>, ApiError> {
    let max_page_limit = state.config.max_page_limit;
    let requested_limit = query.limit.unwrap_or(DEFAULT_PAGE_LIMIT);

    if requested_limit == 0 {
        return Err(ApiError::invalid_argument("limit must be > 0"));
    }
    if query.limit.is_some_and(|limit| limit > max_page_limit) {
        return Err(ApiError::invalid_argument(format!(
            "limit must be <= {max_page_limit}"
        )));
    }
    if query.after_id.is_some() && query.offset != 0 {
        return Err(ApiError::invalid_argument(
            "offset must be 0 when after_id is provided",
        ));
    }
    if query.after_id.is_none() && query.offset > MAX_OFFSET_SCAN {
        return Err(ApiError::invalid_argument(format!(
            "offset must be <= {MAX_OFFSET_SCAN}; use after_id for deep pagination"
        )));
    }
    let limit = requested_limit.min(max_page_limit);

    let (_, collection) = collection_handle(&state, &name, &tenant)?;
    let collection = collection
        .read()
        .map_err(|_| ApiError::internal("collection lock poisoned"))?;

    let total = collection.len();
    let (ids, next_offset, next_after_id) = if let Some(after_id) = query.after_id {
        let (ids, next_after_id) = collection.point_ids_page_after(Some(after_id), limit);
        (ids, None, next_after_id)
    } else {
        let ids = collection.point_ids_page(query.offset, limit);
        let consumed = query.offset.saturating_add(ids.len());
        let next_offset = if consumed < total {
            Some(consumed)
        } else {
            None
        };
        let next_after_id = if next_offset.is_some() {
            ids.last().copied()
        } else {
            None
        };
        (ids, next_offset, next_after_id)
    };
    let points: Vec<PointIdResponse> = ids.into_iter().map(|id| PointIdResponse { id }).collect();

    Ok(Json(ListPointsResponse {
        points,
        total,
        next_offset,
        next_after_id,
    }))
}
