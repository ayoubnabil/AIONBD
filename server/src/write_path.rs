use tokio::task;

use crate::errors::ApiError;
use crate::handler_utils::validate_upsert_input;
use crate::models::PointPayload;
use crate::state::{AppState, CollectionHandle};

pub(crate) struct UpsertPrecheck {
    pub(crate) creating_point: bool,
    pub(crate) point_count: usize,
}

pub(crate) async fn precheck_upsert(
    handle: CollectionHandle,
    id: u64,
    values: Vec<f32>,
    payload: PointPayload,
) -> Result<UpsertPrecheck, ApiError> {
    run(move || {
        let collection = handle
            .read()
            .map_err(|_| ApiError::internal("collection lock poisoned"))?;
        validate_upsert_input(
            &values,
            &payload,
            collection.dimension(),
            collection.strict_finite(),
        )?;
        Ok(UpsertPrecheck {
            creating_point: collection.get_point(id).is_none(),
            point_count: collection.len(),
        })
    })
    .await
}

pub(crate) async fn apply_upsert(
    handle: CollectionHandle,
    id: u64,
    values: Vec<f32>,
    payload: PointPayload,
) -> Result<bool, ApiError> {
    run(move || {
        handle
            .write()
            .map_err(|_| ApiError::internal("collection lock poisoned"))?
            .upsert_point_with_payload(id, values, payload)
            .map_err(|error| ApiError::invalid_argument(error.to_string()))
    })
    .await
}

pub(crate) async fn load_collection_handle(
    state: AppState,
    canonical_name: String,
) -> Result<CollectionHandle, ApiError> {
    run(move || {
        let collections = state
            .collections
            .read()
            .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;
        collections
            .get(&canonical_name)
            .cloned()
            .ok_or_else(|| ApiError::not_found(format!("collection '{canonical_name}' not found")))
    })
    .await
}

pub(crate) async fn collection_exists(
    state: AppState,
    canonical_name: String,
) -> Result<bool, ApiError> {
    run(move || {
        let collections = state
            .collections
            .read()
            .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;
        Ok(collections.contains_key(&canonical_name))
    })
    .await
}

pub(crate) async fn insert_collection(
    state: AppState,
    canonical_name: String,
    handle: CollectionHandle,
) -> Result<(), ApiError> {
    run(move || {
        let mut collections = state
            .collections
            .write()
            .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;
        collections.insert(canonical_name, handle);
        Ok(())
    })
    .await
}

pub(crate) async fn remove_collection(
    state: AppState,
    canonical_name: String,
) -> Result<(), ApiError> {
    run(move || {
        let mut collections = state
            .collections
            .write()
            .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;
        let _ = collections.remove(&canonical_name);
        Ok(())
    })
    .await
}

pub(crate) async fn ensure_point_exists(handle: CollectionHandle, id: u64) -> Result<(), ApiError> {
    run(move || {
        let collection = handle
            .read()
            .map_err(|_| ApiError::internal("collection lock poisoned"))?;
        if collection.get_point(id).is_none() {
            return Err(ApiError::not_found(format!("point '{id}' not found")));
        }
        Ok(())
    })
    .await
}

pub(crate) async fn apply_delete(handle: CollectionHandle, id: u64) -> Result<bool, ApiError> {
    run(move || {
        Ok(handle
            .write()
            .map_err(|_| ApiError::internal("collection lock poisoned"))?
            .remove_point(id)
            .is_some())
    })
    .await
}

async fn run<T>(
    operation: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
) -> Result<T, ApiError>
where
    T: Send + 'static,
{
    task::spawn_blocking(operation)
        .await
        .map_err(|_| ApiError::internal("collection worker task failed"))?
}
