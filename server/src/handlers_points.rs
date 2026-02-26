use aionbd_core::WalRecord;
use axum::extract::{Extension, Path, Query, State};
use axum::Json;
use tokio::task;

use crate::auth::TenantContext;
use crate::errors::ApiError;
use crate::handler_utils::{
    existing_collection_write_lock, remove_collection_write_lock, scoped_collection_name,
};
use crate::models::{
    DeletePointResponse, ListPointsQuery, ListPointsResponse, PointIdResponse, PointResponse,
    DEFAULT_PAGE_LIMIT,
};
use crate::persistence::persist_change_if_enabled;
use crate::resource_manager::estimated_vector_bytes;
use crate::state::AppState;
use crate::tenant_quota::maybe_acquire_tenant_quota_guard;
use crate::tenant_quota::record_point_deleted;
use crate::write_path::{
    apply_delete, collection_dimension, ensure_point_exists, load_collection_handle,
    load_tenant_collection_handle,
};

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

    let (_, handle) = load_tenant_collection_handle(state.clone(), name, tenant).await?;
    let offset = query.offset;
    let after_id = query.after_id;
    let (total, ids, next_offset, next_after_id) = task::spawn_blocking(move || {
        let collection = handle
            .read()
            .map_err(|_| ApiError::internal("collection lock poisoned"))?;

        let total = collection.len();
        let (ids, next_offset, next_after_id) = if let Some(after_id) = after_id {
            let (ids, next_after_id) = collection.point_ids_page_after(Some(after_id), limit);
            (ids, None, next_after_id)
        } else {
            let ids = collection.point_ids_page(offset, limit);
            let consumed = offset.saturating_add(ids.len());
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
        Ok::<(usize, Vec<u64>, Option<usize>, Option<u64>), ApiError>((
            total,
            ids,
            next_offset,
            next_after_id,
        ))
    })
    .await
    .map_err(|_| ApiError::internal("point listing worker task failed"))??;
    let points: Vec<PointIdResponse> = ids.into_iter().map(|id| PointIdResponse { id }).collect();

    Ok(Json(ListPointsResponse {
        points,
        total,
        next_offset,
        next_after_id,
    }))
}

pub(crate) async fn get_point(
    Path((name, id)): Path<(String, u64)>,
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
) -> Result<Json<PointResponse>, ApiError> {
    let (_, handle) = load_tenant_collection_handle(state, name, tenant).await?;
    let response = task::spawn_blocking(move || {
        let collection = handle
            .read()
            .map_err(|_| ApiError::internal("collection lock poisoned"))?;
        let (values, payload) = collection
            .get_point_record(id)
            .ok_or_else(|| ApiError::not_found(format!("point '{id}' not found")))?;

        Ok::<PointResponse, ApiError>(PointResponse {
            id,
            values: values.to_vec(),
            payload: payload.clone(),
        })
    })
    .await
    .map_err(|_| ApiError::internal("point lookup worker task failed"))??;
    Ok(Json(response))
}

pub(crate) async fn delete_point(
    Path((name, id)): Path<(String, u64)>,
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
) -> Result<Json<DeletePointResponse>, ApiError> {
    let name = scoped_collection_name(&state, &name, &tenant)?;
    let _tenant_quota_guard = maybe_acquire_tenant_quota_guard(&state, &tenant).await?;
    let collection_guard = existing_collection_write_lock(&state, &name)
        .await?
        .acquire_owned()
        .await
        .map_err(|_| ApiError::internal("collection write semaphore closed"))?;
    let handle = match load_collection_handle(state.clone(), name.clone()).await {
        Ok(handle) => handle,
        Err(error) => {
            drop(collection_guard);
            let _ = remove_collection_write_lock(&state, &name).await;
            return Err(error);
        }
    };
    let dimension = collection_dimension(handle.clone()).await?;

    ensure_point_exists(handle.clone(), id).await?;

    persist_change_if_enabled(
        &state,
        &WalRecord::DeletePoint {
            collection: name.clone(),
            id,
        },
    )
    .await?;

    let deleted = apply_delete(handle, id).await?;
    if !deleted {
        state
            .engine_loaded
            .store(false, std::sync::atomic::Ordering::Relaxed);
        tracing::error!(collection = %name, point_id = id, "in-memory delete failed after wal append");
        return Err(ApiError::internal(
            "in-memory state update failed after wal append; restart required",
        ));
    }
    let _ = state.metrics.points_total.fetch_update(
        std::sync::atomic::Ordering::Relaxed,
        std::sync::atomic::Ordering::Relaxed,
        |current| Some(current.saturating_sub(1)),
    );
    state
        .resource_manager
        .release(estimated_vector_bytes(dimension));
    if state.auth_config.tenant_max_points > 0 {
        record_point_deleted(&state, &tenant).await;
    }
    Ok(Json(DeletePointResponse { id, deleted: true }))
}
