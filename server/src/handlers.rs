use aionbd_core::{Collection, CollectionConfig, WalRecord};
use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Path, State};
use axum::Json;
use std::sync::OnceLock;
use tokio::task;

use crate::auth::TenantContext;
use crate::errors::{map_collection_error, map_json_rejection, ApiError};
use crate::handler_utils::{
    build_collection_response, canonical_collection_name, collection_write_lock,
    existing_collection_write_lock, remove_collection_write_lock, scoped_collection_name,
    validate_upsert_input, visible_collection_name,
};
pub(crate) use crate::handlers_health::{distance, live, ready};
use crate::models::{
    CollectionResponse, CreateCollectionRequest, DeleteCollectionResponse, ListCollectionsResponse,
    UpsertPointRequest, UpsertPointResponse, UpsertPointsBatchRequest, UpsertPointsBatchResponse,
};
use crate::persistence::{
    persist_change_if_enabled, persist_change_owned_if_enabled, persist_changes_if_enabled,
};
use crate::resource_manager::estimated_vector_bytes;
use crate::state::AppState;
use crate::tenant_quota::{
    maybe_acquire_tenant_quota_guard, record_collection_created, record_collection_removed,
    record_point_created, record_points_created, tenant_collection_count, tenant_point_count,
};
use crate::write_path::{
    apply_upsert, apply_upsert_batch, collection_dimension, collection_exists, insert_collection,
    load_collection_handle, load_tenant_collection_handle, precheck_upsert, precheck_upsert_batch,
    remove_collection,
};

const UPSERT_BATCH_MAX_POINTS: usize = 256;
static UPSERT_BATCH_MAX_POINTS_CACHE: OnceLock<usize> = OnceLock::new();

fn upsert_batch_max_points() -> usize {
    *UPSERT_BATCH_MAX_POINTS_CACHE.get_or_init(|| {
        std::env::var("AIONBD_UPSERT_BATCH_MAX_POINTS")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(UPSERT_BATCH_MAX_POINTS)
    })
}

pub(crate) async fn create_collection(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    payload: Result<Json<CreateCollectionRequest>, JsonRejection>,
) -> Result<Json<CollectionResponse>, ApiError> {
    tenant.require_write()?;
    let Json(payload) = payload.map_err(map_json_rejection)?;
    let response_name = canonical_collection_name(&payload.name)?;
    let name = scoped_collection_name(&state, &payload.name, &tenant)?;
    if payload.dimension > state.config.max_dimension {
        return Err(ApiError::invalid_argument(format!(
            "dimension {} exceeds configured maximum {}",
            payload.dimension, state.config.max_dimension
        )));
    }
    let config = CollectionConfig::new(payload.dimension, payload.strict_finite)
        .map_err(map_collection_error)?;
    let collection = Collection::new(name.clone(), config).map_err(map_collection_error)?;
    let handle = std::sync::Arc::new(std::sync::RwLock::new(collection));

    let _tenant_quota_guard = maybe_acquire_tenant_quota_guard(&state, &tenant).await?;
    let collection_guard = collection_write_lock(&state, &name)
        .await?
        .acquire_owned()
        .await
        .map_err(|_| ApiError::internal("collection write semaphore closed"))?;

    if collection_exists(state.clone(), name.clone()).await? {
        return Err(ApiError::conflict(format!(
            "collection '{name}' already exists"
        )));
    }
    if state.auth_config.tenant_max_collections > 0 {
        let tenant_collections = tenant_collection_count(state.clone(), tenant.clone()).await?;
        if tenant_collections >= state.auth_config.tenant_max_collections as usize {
            let _ = state
                .metrics
                .tenant_quota_collection_rejections_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            drop(collection_guard);
            let _ = remove_collection_write_lock(&state, &name).await;
            return Err(ApiError::resource_exhausted(format!(
                "tenant collection limit exceeded ({})",
                state.auth_config.tenant_max_collections
            )));
        }
    }

    if let Err(error) = persist_change_if_enabled(
        &state,
        &WalRecord::CreateCollection {
            name: name.clone(),
            dimension: payload.dimension,
            strict_finite: payload.strict_finite,
        },
    )
    .await
    {
        drop(collection_guard);
        let _ = remove_collection_write_lock(&state, &name).await;
        return Err(error);
    }

    if insert_collection(state.clone(), name.clone(), handle)
        .await
        .is_err()
    {
        state
            .engine_loaded
            .store(false, std::sync::atomic::Ordering::Relaxed);
        let _ = remove_collection_write_lock(&state, &name).await;
        return Err(ApiError::internal(
            "in-memory state update failed after wal append; restart required",
        ));
    }
    let _ = state
        .metrics
        .collections_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if state.auth_config.tenant_max_collections > 0 || state.auth_config.tenant_max_points > 0 {
        record_collection_created(&state, &tenant).await;
    }

    Ok(Json(CollectionResponse {
        name: response_name,
        dimension: payload.dimension,
        strict_finite: payload.strict_finite,
        point_count: 0,
    }))
}

pub(crate) async fn list_collections(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
) -> Result<Json<ListCollectionsResponse>, ApiError> {
    let state_for_list = state.clone();
    let tenant_for_list = tenant.clone();
    let response = task::spawn_blocking(move || {
        let collections = state_for_list
            .collections
            .read()
            .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;

        let mut items = Vec::with_capacity(collections.len());
        for collection in collections.values() {
            let collection = collection
                .read()
                .map_err(|_| ApiError::internal("collection lock poisoned"))?;
            let Some(name) =
                visible_collection_name(&state_for_list, collection.name(), &tenant_for_list)?
            else {
                continue;
            };
            let mut response = build_collection_response(&collection);
            response.name = name;
            items.push(response);
        }
        Ok::<ListCollectionsResponse, ApiError>(ListCollectionsResponse { collections: items })
    })
    .await
    .map_err(|_| ApiError::internal("collection listing worker task failed"))??;
    Ok(Json(response))
}

pub(crate) async fn get_collection(
    Path(name): Path<String>,
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
) -> Result<Json<CollectionResponse>, ApiError> {
    let response_name = canonical_collection_name(&name)?;
    let (_, handle) = load_tenant_collection_handle(state, name, tenant).await?;
    let mut response = task::spawn_blocking(move || {
        let collection = handle
            .read()
            .map_err(|_| ApiError::internal("collection lock poisoned"))?;
        Ok::<CollectionResponse, ApiError>(build_collection_response(&collection))
    })
    .await
    .map_err(|_| ApiError::internal("collection lookup worker task failed"))??;
    response.name = response_name;
    Ok(Json(response))
}

pub(crate) async fn delete_collection(
    Path(name): Path<String>,
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
) -> Result<Json<DeleteCollectionResponse>, ApiError> {
    tenant.require_write()?;
    let response_name = canonical_collection_name(&name)?;
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
    let collection_dimension = collection_dimension(handle).await?;

    persist_change_if_enabled(&state, &WalRecord::DeleteCollection { name: name.clone() }).await?;
    let removed_points = remove_collection(state.clone(), name.clone())
        .await
        .map_err(|_| {
            state
                .engine_loaded
                .store(false, std::sync::atomic::Ordering::Relaxed);
            ApiError::internal("in-memory state update failed after wal append; restart required")
        })?;
    let _ = state.metrics.collections_total.fetch_update(
        std::sync::atomic::Ordering::Relaxed,
        std::sync::atomic::Ordering::Relaxed,
        |current| Some(current.saturating_sub(1)),
    );
    let released_bytes =
        estimated_vector_bytes(collection_dimension).saturating_mul(removed_points as u64);
    state.resource_manager.release(released_bytes);
    decrement_points_total(&state, removed_points);
    if state.auth_config.tenant_max_collections > 0 || state.auth_config.tenant_max_points > 0 {
        record_collection_removed(&state, &tenant, removed_points).await;
    }
    drop(collection_guard);
    let _ = remove_collection_write_lock(&state, &name).await;

    Ok(Json(DeleteCollectionResponse {
        name: response_name,
        deleted: true,
    }))
}

pub(crate) async fn upsert_point(
    Path((name, id)): Path<(String, u64)>,
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    payload: Result<Json<UpsertPointRequest>, JsonRejection>,
) -> Result<Json<UpsertPointResponse>, ApiError> {
    tenant.require_write()?;
    let name = scoped_collection_name(&state, &name, &tenant)?;
    let Json(payload) = payload.map_err(map_json_rejection)?;
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

    let values = payload.values;
    let payload_values = payload.payload;
    let precheck = precheck_upsert(handle.clone(), id).await?;
    validate_upsert_input(
        &values,
        &payload_values,
        precheck.dimension,
        precheck.strict_finite,
    )?;
    let max_points_per_collection = state.config.max_points_per_collection;
    if precheck.creating_point && precheck.point_count >= max_points_per_collection {
        return Err(ApiError::resource_exhausted(format!(
            "collection point limit exceeded ({max_points_per_collection})"
        )));
    }
    if precheck.creating_point && state.auth_config.tenant_max_points > 0 {
        let tenant_points = tenant_point_count(state.clone(), tenant.clone()).await?;
        if tenant_points >= state.auth_config.tenant_max_points as usize {
            let _ = state
                .metrics
                .tenant_quota_point_rejections_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Err(ApiError::resource_exhausted(format!(
                "tenant point limit exceeded ({})",
                state.auth_config.tenant_max_points
            )));
        }
    }
    let reserved_bytes = if precheck.creating_point {
        estimated_vector_bytes(values.len())
    } else {
        0
    };
    if reserved_bytes > 0 && !state.resource_manager.try_reserve(reserved_bytes) {
        return Err(ApiError::resource_exhausted(format!(
            "memory budget exceeded (used {} bytes / budget {} bytes)",
            state.resource_manager.used_bytes(),
            state.resource_manager.budget_bytes()
        )));
    }

    let wal_record = WalRecord::UpsertPoint {
        collection: name.clone(),
        id,
        values,
        payload: Some(payload_values),
    };
    let wal_record = match persist_change_owned_if_enabled(&state, wal_record).await {
        Ok(record) => record,
        Err(error) => {
            state.resource_manager.release(reserved_bytes);
            return Err(error);
        }
    };
    let (values, payload_values) = match wal_record {
        WalRecord::UpsertPoint {
            values, payload, ..
        } => (values, payload.unwrap_or_default()),
        _ => unreachable!("upsert path must persist an upsert wal record"),
    };

    let created = apply_upsert(handle, id, values, payload_values)
        .await
        .map_err(|error| {
            state.resource_manager.release(reserved_bytes);
            state
                .engine_loaded
                .store(false, std::sync::atomic::Ordering::Relaxed);
            tracing::error!(collection = %name, point_id = id, ?error, "in-memory upsert failed after wal append");
            ApiError::internal("in-memory state update failed after wal append; restart required")
        })?;
    if !created {
        state.resource_manager.release(reserved_bytes);
    }
    if created {
        let _ = state
            .metrics
            .points_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if state.auth_config.tenant_max_points > 0 {
            record_point_created(&state, &tenant).await;
        }
    }
    Ok(Json(UpsertPointResponse { id, created }))
}

pub(crate) async fn upsert_points_batch(
    Path(name): Path<String>,
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    payload: Result<Json<UpsertPointsBatchRequest>, JsonRejection>,
) -> Result<Json<UpsertPointsBatchResponse>, ApiError> {
    tenant.require_write()?;
    let name = scoped_collection_name(&state, &name, &tenant)?;
    let Json(payload) = payload.map_err(map_json_rejection)?;
    if payload.points.is_empty() {
        return Err(ApiError::invalid_argument("points must not be empty"));
    }
    let batch_max_points = upsert_batch_max_points();
    if payload.points.len() > batch_max_points {
        return Err(ApiError::invalid_argument(format!(
            "points length must be <= {batch_max_points}"
        )));
    }

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

    let tenant_max_points = state.auth_config.tenant_max_points as usize;
    let mut tenant_points = if tenant_max_points > 0 {
        Some(tenant_point_count(state.clone(), tenant.clone()).await?)
    } else {
        None
    };
    let max_points_per_collection = state.config.max_points_per_collection;

    let batch_precheck = precheck_upsert_batch(
        handle.clone(),
        payload.points.iter().map(|point| point.id).collect(),
    )
    .await?;
    if batch_precheck.creating_points.len() != payload.points.len() {
        return Err(ApiError::internal(
            "in-memory state update failed after wal append; restart required",
        ));
    }

    let mut projected_new_points = 0usize;
    let mut wal_records = Vec::with_capacity(payload.points.len());
    let mut reserved_bytes = Vec::with_capacity(payload.points.len());

    for (index, point) in payload.points.into_iter().enumerate() {
        let id = point.id;
        let values = point.values;
        let payload_values = point.payload;
        validate_upsert_input(
            &values,
            &payload_values,
            batch_precheck.dimension,
            batch_precheck.strict_finite,
        )?;
        let creating_point = batch_precheck.creating_points[index];
        if creating_point {
            if batch_precheck
                .point_count
                .saturating_add(projected_new_points)
                >= max_points_per_collection
            {
                return Err(ApiError::resource_exhausted(format!(
                    "collection point limit exceeded ({max_points_per_collection})"
                )));
            }
            if tenant_points.is_some_and(|value| value >= tenant_max_points) {
                let _ = state
                    .metrics
                    .tenant_quota_point_rejections_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err(ApiError::resource_exhausted(format!(
                    "tenant point limit exceeded ({tenant_max_points})"
                )));
            }
            projected_new_points = projected_new_points.saturating_add(1);
            if let Some(points) = tenant_points.as_mut() {
                *points = points.saturating_add(1);
            }
        }

        reserved_bytes.push(if creating_point {
            estimated_vector_bytes(values.len())
        } else {
            0
        });
        wal_records.push(WalRecord::UpsertPoint {
            collection: name.clone(),
            id,
            values,
            payload: Some(payload_values),
        });
    }

    let reserved_total_bytes = reserved_bytes
        .iter()
        .copied()
        .fold(0u64, |total, bytes| total.saturating_add(bytes));
    if reserved_total_bytes > 0 && !state.resource_manager.try_reserve(reserved_total_bytes) {
        return Err(ApiError::resource_exhausted(format!(
            "memory budget exceeded (used {} bytes / budget {} bytes)",
            state.resource_manager.used_bytes(),
            state.resource_manager.budget_bytes()
        )));
    }

    let wal_records = match persist_changes_if_enabled(&state, wal_records).await {
        Ok(records) => records,
        Err(error) => {
            state.resource_manager.release(reserved_total_bytes);
            return Err(error);
        }
    };

    let mut ids = Vec::with_capacity(wal_records.len());
    let mut apply_points = Vec::with_capacity(wal_records.len());
    for record in wal_records {
        match record {
            WalRecord::UpsertPoint {
                id,
                values,
                payload,
                ..
            } => {
                ids.push(id);
                apply_points.push((id, values, payload.unwrap_or_default()));
            }
            _ => unreachable!("upsert batch path must persist upsert wal records"),
        }
    }
    let created_flags = apply_upsert_batch(handle, apply_points).await.map_err(|error| {
        state.resource_manager.release(reserved_total_bytes);
        state
            .engine_loaded
            .store(false, std::sync::atomic::Ordering::Relaxed);
        tracing::error!(collection = %name, ?error, "in-memory upsert batch failed after wal append");
        ApiError::internal("in-memory state update failed after wal append; restart required")
    })?;
    if created_flags.len() != ids.len() || created_flags.len() != reserved_bytes.len() {
        state.resource_manager.release(reserved_total_bytes);
        state
            .engine_loaded
            .store(false, std::sync::atomic::Ordering::Relaxed);
        return Err(ApiError::internal(
            "in-memory state update failed after wal append; restart required",
        ));
    }

    let mut created_total: usize = 0;
    let mut results = Vec::with_capacity(ids.len());
    for (index, (id, created)) in ids.into_iter().zip(created_flags.into_iter()).enumerate() {
        if created {
            created_total = created_total.saturating_add(1);
        } else {
            state.resource_manager.release(reserved_bytes[index]);
        }
        results.push(UpsertPointResponse { id, created });
    }
    if created_total > 0 {
        let _ = state.metrics.points_total.fetch_add(
            created_total.min(u64::MAX as usize) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        if tenant_max_points > 0 {
            record_points_created(&state, &tenant, created_total).await;
        }
    }

    let updated = results.len().saturating_sub(created_total);
    Ok(Json(UpsertPointsBatchResponse {
        created: created_total,
        updated,
        results,
    }))
}

fn decrement_points_total(state: &AppState, removed: usize) {
    if removed == 0 {
        return;
    }
    let removed = removed.min(u64::MAX as usize) as u64;
    let points_total = &state.metrics.points_total;
    let mut observed = points_total.load(std::sync::atomic::Ordering::Relaxed);
    loop {
        let next = observed.saturating_sub(removed);
        match points_total.compare_exchange_weak(
            observed,
            next,
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
        ) {
            Ok(_) => return,
            Err(actual) => observed = actual,
        }
    }
}
