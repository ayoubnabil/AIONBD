use aionbd_core::{Collection, CollectionConfig, WalRecord};
use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Path, State};
use axum::Json;

use crate::auth::TenantContext;
use crate::errors::{map_collection_error, map_json_rejection, ApiError};
use crate::handler_utils::{
    build_collection_response, canonical_collection_name, collection_handle,
    collection_handle_by_name, collection_write_lock, existing_collection_write_lock,
    remove_collection_write_lock, scoped_collection_name, validate_upsert_input,
    visible_collection_name,
};
pub(crate) use crate::handlers_health::{distance, live, ready};
use crate::index_manager::remove_l2_index_entry;
use crate::models::{
    CollectionResponse, CreateCollectionRequest, DeleteCollectionResponse, DeletePointResponse,
    ListCollectionsResponse, PointResponse, UpsertPointRequest, UpsertPointResponse,
};
use crate::persistence::persist_change_if_enabled;
use crate::state::AppState;

const HARD_MAX_POINTS_PER_COLLECTION: usize = 1_000_000;

pub(crate) async fn create_collection(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    payload: Result<Json<CreateCollectionRequest>, JsonRejection>,
) -> Result<Json<CollectionResponse>, ApiError> {
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

    let collection_guard = collection_write_lock(&state, &name)?
        .acquire_owned()
        .await
        .map_err(|_| ApiError::internal("collection write semaphore closed"))?;

    {
        let collections = state
            .collections
            .read()
            .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;

        if collections.contains_key(&name) {
            return Err(ApiError::conflict(format!(
                "collection '{name}' already exists"
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
        let _ = remove_collection_write_lock(&state, &name);
        return Err(error);
    }

    {
        let mut collections = state
            .collections
            .write()
            .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;
        collections.insert(name, handle);
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
    let collections = state
        .collections
        .read()
        .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;

    let mut items = Vec::with_capacity(collections.len());
    for collection in collections.values() {
        let collection = collection
            .read()
            .map_err(|_| ApiError::internal("collection lock poisoned"))?;
        let Some(name) = visible_collection_name(&state, collection.name(), &tenant)? else {
            continue;
        };
        let mut response = build_collection_response(&collection);
        response.name = name;
        items.push(response);
    }
    Ok(Json(ListCollectionsResponse { collections: items }))
}

pub(crate) async fn get_collection(
    Path(name): Path<String>,
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
) -> Result<Json<CollectionResponse>, ApiError> {
    let response_name = canonical_collection_name(&name)?;
    let (_, handle) = collection_handle(&state, &name, &tenant)?;
    let collection = handle
        .read()
        .map_err(|_| ApiError::internal("collection lock poisoned"))?;
    let mut response = build_collection_response(&collection);
    response.name = response_name;
    Ok(Json(response))
}

pub(crate) async fn delete_collection(
    Path(name): Path<String>,
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
) -> Result<Json<DeleteCollectionResponse>, ApiError> {
    let response_name = canonical_collection_name(&name)?;
    let name = scoped_collection_name(&state, &name, &tenant)?;
    let collection_guard = existing_collection_write_lock(&state, &name)?
        .acquire_owned()
        .await
        .map_err(|_| ApiError::internal("collection write semaphore closed"))?;

    {
        let collections = state
            .collections
            .read()
            .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;
        if !collections.contains_key(&name) {
            return Err(ApiError::not_found(format!(
                "collection '{name}' not found"
            )));
        }
    }

    persist_change_if_enabled(&state, &WalRecord::DeleteCollection { name: name.clone() }).await?;
    {
        let mut collections = state
            .collections
            .write()
            .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;
        let _ = collections.remove(&name);
    }
    drop(collection_guard);
    let _ = remove_collection_write_lock(&state, &name);

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
    let name = scoped_collection_name(&state, &name, &tenant)?;
    let Json(payload) = payload.map_err(map_json_rejection)?;
    let _collection_guard = existing_collection_write_lock(&state, &name)?
        .acquire_owned()
        .await
        .map_err(|_| ApiError::internal("collection write semaphore closed"))?;
    let handle = collection_handle_by_name(&state, &name)?;

    let values = payload.values;
    let payload_values = payload.payload;
    let wal_values = values.clone();
    let wal_payload = payload_values.clone();
    {
        let collection = handle
            .read()
            .map_err(|_| ApiError::internal("collection lock poisoned"))?;
        validate_upsert_input(
            &values,
            &payload_values,
            collection.dimension(),
            collection.strict_finite(),
        )?;
        if collection.get_point(id).is_none() && collection.len() >= HARD_MAX_POINTS_PER_COLLECTION
        {
            return Err(ApiError::resource_exhausted(format!(
                "collection point limit exceeded ({HARD_MAX_POINTS_PER_COLLECTION})"
            )));
        }
    }

    persist_change_if_enabled(
        &state,
        &WalRecord::UpsertPoint {
            collection: name.clone(),
            id,
            values: wal_values,
            payload: Some(wal_payload),
        },
    )
    .await?;

    let created = handle
        .write()
        .map_err(|_| ApiError::internal("collection lock poisoned"))?
        .upsert_point_with_payload(id, values, payload_values)
        .map_err(map_collection_error)?;
    remove_l2_index_entry(&state, &name);

    Ok(Json(UpsertPointResponse { id, created }))
}

pub(crate) async fn get_point(
    Path((name, id)): Path<(String, u64)>,
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
) -> Result<Json<PointResponse>, ApiError> {
    let (_, handle) = collection_handle(&state, &name, &tenant)?;
    let collection = handle
        .read()
        .map_err(|_| ApiError::internal("collection lock poisoned"))?;
    let (values, payload) = collection
        .get_point_record(id)
        .ok_or_else(|| ApiError::not_found(format!("point '{id}' not found")))?;

    Ok(Json(PointResponse {
        id,
        values: values.to_vec(),
        payload: payload.clone(),
    }))
}

pub(crate) async fn delete_point(
    Path((name, id)): Path<(String, u64)>,
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
) -> Result<Json<DeletePointResponse>, ApiError> {
    let name = scoped_collection_name(&state, &name, &tenant)?;
    let _collection_guard = existing_collection_write_lock(&state, &name)?
        .acquire_owned()
        .await
        .map_err(|_| ApiError::internal("collection write semaphore closed"))?;
    let handle = collection_handle_by_name(&state, &name)?;

    {
        let collection = handle
            .read()
            .map_err(|_| ApiError::internal("collection lock poisoned"))?;
        if collection.get_point(id).is_none() {
            return Err(ApiError::not_found(format!("point '{id}' not found")));
        }
    }

    persist_change_if_enabled(
        &state,
        &WalRecord::DeletePoint {
            collection: name.clone(),
            id,
        },
    )
    .await?;

    let deleted = handle
        .write()
        .map_err(|_| ApiError::internal("collection lock poisoned"))?
        .remove_point(id)
        .is_some();
    if !deleted {
        return Err(ApiError::not_found(format!("point '{id}' not found")));
    }
    remove_l2_index_entry(&state, &name);

    Ok(Json(DeletePointResponse { id, deleted: true }))
}
