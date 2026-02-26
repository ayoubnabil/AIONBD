use aionbd_core::WalRecord;
#[cfg(any(feature = "exp_points_count", feature = "exp_payload_mutation_api"))]
use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Path, Query, State};
use axum::Json;
#[cfg(feature = "exp_payload_mutation_api")]
use std::collections::HashSet;
use tokio::task;

use crate::auth::TenantContext;
#[cfg(any(feature = "exp_points_count", feature = "exp_payload_mutation_api"))]
use crate::errors::map_json_rejection;
use crate::errors::ApiError;
#[cfg(feature = "exp_payload_mutation_api")]
use crate::handler_utils::validate_payload_keys;
use crate::handler_utils::{
    existing_collection_write_lock, remove_collection_write_lock, scoped_collection_name,
};
#[cfg(feature = "exp_points_count")]
use crate::handlers_search::filter::{matches_filter_strict, validate_filter};
#[cfg(feature = "exp_points_count")]
use crate::models::{CountPointsRequest, CountPointsResponse};
#[cfg(feature = "exp_payload_mutation_api")]
use crate::models::{DeletePayloadKeysRequest, PayloadMutationResponse, SetPayloadRequest};
use crate::models::{
    DeletePointResponse, ListPointsQuery, ListPointsResponse, PointIdResponse, PointResponse,
    DEFAULT_PAGE_LIMIT,
};
use crate::persistence::persist_change_if_enabled;
#[cfg(feature = "exp_payload_mutation_api")]
use crate::persistence::persist_changes_if_enabled;
use crate::resource_manager::estimated_vector_bytes;
use crate::state::AppState;
#[cfg(feature = "exp_payload_mutation_api")]
use crate::state::CollectionHandle;
use crate::tenant_quota::maybe_acquire_tenant_quota_guard;
use crate::tenant_quota::record_point_deleted;
#[cfg(feature = "exp_payload_mutation_api")]
use crate::write_path::apply_upsert_batch;
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

#[cfg(feature = "exp_points_count")]
pub(crate) async fn count_points(
    Path(name): Path<String>,
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    payload: Result<Json<CountPointsRequest>, JsonRejection>,
) -> Result<Json<CountPointsResponse>, ApiError> {
    let Json(payload) = payload.map_err(map_json_rejection)?;
    if let Some(filter) = payload.filter.as_ref() {
        validate_filter(filter)?;
    }
    let filter = payload.filter;

    let (_, handle) = load_tenant_collection_handle(state, name, tenant).await?;
    let count = task::spawn_blocking(move || {
        let collection = handle
            .read()
            .map_err(|_| ApiError::internal("collection lock poisoned"))?;
        let count = match filter.as_ref() {
            Some(active_filter) => collection
                .iter_points_with_payload_unordered()
                .filter(|(_, _, payload)| matches_filter_strict(payload, active_filter))
                .count(),
            None => collection.len(),
        };
        Ok::<usize, ApiError>(count)
    })
    .await
    .map_err(|_| ApiError::internal("point count worker task failed"))??;

    Ok(Json(CountPointsResponse { count }))
}

#[cfg(feature = "exp_payload_mutation_api")]
pub(crate) async fn set_payload(
    Path(name): Path<String>,
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    payload: Result<Json<SetPayloadRequest>, JsonRejection>,
) -> Result<Json<PayloadMutationResponse>, ApiError> {
    tenant.require_write()?;
    let Json(payload) = payload.map_err(map_json_rejection)?;
    if payload.points.is_empty() {
        return Err(ApiError::invalid_argument("points must not be empty"));
    }
    if payload.payload.is_empty() {
        return Err(ApiError::invalid_argument("payload must not be empty"));
    }
    validate_payload_keys(&payload.payload)?;
    let point_ids = unique_point_ids(&payload.points);
    let payload_patch = payload.payload;

    let name = scoped_collection_name(&state, &name, &tenant)?;
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

    let wal_records = task::spawn_blocking({
        let name = name.clone();
        let handle = handle.clone();
        move || {
            let collection = handle
                .read()
                .map_err(|_| ApiError::internal("collection lock poisoned"))?;
            let mut wal_records = Vec::with_capacity(point_ids.len());
            for id in point_ids {
                let (values, current_payload) = collection
                    .get_point_record(id)
                    .ok_or_else(|| ApiError::not_found(format!("point '{id}' not found")))?;
                let mut next_payload = current_payload.clone();
                let mut changed = false;
                for (key, value) in &payload_patch {
                    if next_payload.get(key) != Some(value) {
                        changed = true;
                    }
                    next_payload.insert(key.clone(), value.clone());
                }
                if !changed {
                    continue;
                }
                wal_records.push(WalRecord::UpsertPoint {
                    collection: name.clone(),
                    id,
                    values: values.to_vec(),
                    payload: Some(next_payload),
                });
            }
            Ok::<Vec<WalRecord>, ApiError>(wal_records)
        }
    })
    .await
    .map_err(|_| ApiError::internal("payload set worker task failed"))??;

    if wal_records.is_empty() {
        return Ok(Json(PayloadMutationResponse { updated: 0 }));
    }

    let wal_records = persist_changes_if_enabled(&state, wal_records).await?;
    let updated = apply_upsert_records(handle, wal_records).await?;
    Ok(Json(PayloadMutationResponse { updated }))
}

#[cfg(feature = "exp_payload_mutation_api")]
pub(crate) async fn delete_payload_keys(
    Path(name): Path<String>,
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    payload: Result<Json<DeletePayloadKeysRequest>, JsonRejection>,
) -> Result<Json<PayloadMutationResponse>, ApiError> {
    tenant.require_write()?;
    let Json(payload) = payload.map_err(map_json_rejection)?;
    if payload.points.is_empty() {
        return Err(ApiError::invalid_argument("points must not be empty"));
    }
    if payload.keys.is_empty() {
        return Err(ApiError::invalid_argument("keys must not be empty"));
    }
    let point_ids = unique_point_ids(&payload.points);
    let keys = normalized_payload_keys(payload.keys)?;

    let name = scoped_collection_name(&state, &name, &tenant)?;
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

    let wal_records = task::spawn_blocking({
        let name = name.clone();
        let handle = handle.clone();
        move || {
            let collection = handle
                .read()
                .map_err(|_| ApiError::internal("collection lock poisoned"))?;
            let mut wal_records = Vec::with_capacity(point_ids.len());
            for id in point_ids {
                let (values, current_payload) = collection
                    .get_point_record(id)
                    .ok_or_else(|| ApiError::not_found(format!("point '{id}' not found")))?;
                let mut next_payload = current_payload.clone();
                let mut changed = false;
                for key in &keys {
                    changed |= next_payload.remove(key).is_some();
                }
                if !changed {
                    continue;
                }
                wal_records.push(WalRecord::UpsertPoint {
                    collection: name.clone(),
                    id,
                    values: values.to_vec(),
                    payload: Some(next_payload),
                });
            }
            Ok::<Vec<WalRecord>, ApiError>(wal_records)
        }
    })
    .await
    .map_err(|_| ApiError::internal("payload delete worker task failed"))??;

    if wal_records.is_empty() {
        return Ok(Json(PayloadMutationResponse { updated: 0 }));
    }

    let wal_records = persist_changes_if_enabled(&state, wal_records).await?;
    let updated = apply_upsert_records(handle, wal_records).await?;
    Ok(Json(PayloadMutationResponse { updated }))
}

#[cfg(feature = "exp_payload_mutation_api")]
fn unique_point_ids(ids: &[u64]) -> Vec<u64> {
    let mut seen = HashSet::with_capacity(ids.len());
    let mut unique = Vec::with_capacity(ids.len());
    for &id in ids {
        if seen.insert(id) {
            unique.push(id);
        }
    }
    unique
}

#[cfg(feature = "exp_payload_mutation_api")]
fn normalized_payload_keys(raw_keys: Vec<String>) -> Result<Vec<String>, ApiError> {
    let mut seen = HashSet::with_capacity(raw_keys.len());
    let mut keys = Vec::with_capacity(raw_keys.len());
    for raw_key in raw_keys {
        let key = raw_key.trim();
        if key.is_empty() {
            return Err(ApiError::invalid_argument("payload keys must not be empty"));
        }
        if seen.insert(key.to_string()) {
            keys.push(key.to_string());
        }
    }
    Ok(keys)
}

#[cfg(feature = "exp_payload_mutation_api")]
async fn apply_upsert_records(
    handle: CollectionHandle,
    wal_records: Vec<WalRecord>,
) -> Result<usize, ApiError> {
    let mut apply_points = Vec::with_capacity(wal_records.len());
    for record in wal_records {
        match record {
            WalRecord::UpsertPoint {
                id,
                values,
                payload,
                ..
            } => apply_points.push((id, values, payload.unwrap_or_default())),
            _ => {
                return Err(ApiError::internal(
                    "payload mutation path must persist upsert wal records",
                ))
            }
        }
    }

    let created_flags = apply_upsert_batch(handle, apply_points).await?;
    if created_flags.iter().any(|created| *created) {
        return Err(ApiError::internal(
            "in-memory state update failed after wal append; restart required",
        ));
    }
    Ok(created_flags.len())
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
    tenant.require_write()?;
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
