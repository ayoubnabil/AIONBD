use std::collections::HashSet;

use tokio::task;

use crate::auth::TenantContext;
use crate::errors::ApiError;
use crate::handler_utils::scoped_collection_name;
use crate::models::PointPayload;
use crate::state::{AppState, CollectionHandle};

pub(crate) struct UpsertPrecheck {
    pub(crate) creating_point: bool,
    pub(crate) point_count: usize,
    pub(crate) dimension: usize,
    pub(crate) strict_finite: bool,
}

pub(crate) struct UpsertBatchPrecheck {
    pub(crate) creating_points: Vec<bool>,
    pub(crate) point_count: usize,
    pub(crate) dimension: usize,
    pub(crate) strict_finite: bool,
}

pub(crate) async fn precheck_upsert(
    handle: CollectionHandle,
    id: u64,
) -> Result<UpsertPrecheck, ApiError> {
    if let Ok(collection) = handle.try_read() {
        return Ok(UpsertPrecheck {
            creating_point: collection.get_point(id).is_none(),
            point_count: collection.len(),
            dimension: collection.dimension(),
            strict_finite: collection.strict_finite(),
        });
    }

    run(move || {
        let collection = handle
            .read()
            .map_err(|_| ApiError::internal("collection lock poisoned"))?;
        Ok(UpsertPrecheck {
            creating_point: collection.get_point(id).is_none(),
            point_count: collection.len(),
            dimension: collection.dimension(),
            strict_finite: collection.strict_finite(),
        })
    })
    .await
}

pub(crate) async fn precheck_upsert_batch(
    handle: CollectionHandle,
    ids: Vec<u64>,
) -> Result<UpsertBatchPrecheck, ApiError> {
    if let Ok(collection) = handle.try_read() {
        let mut seen_new_ids = HashSet::with_capacity(ids.len());
        let mut creating_points = Vec::with_capacity(ids.len());
        for id in ids {
            let creating = collection.get_point(id).is_none() && seen_new_ids.insert(id);
            creating_points.push(creating);
        }
        return Ok(UpsertBatchPrecheck {
            creating_points,
            point_count: collection.len(),
            dimension: collection.dimension(),
            strict_finite: collection.strict_finite(),
        });
    }

    run(move || {
        let collection = handle
            .read()
            .map_err(|_| ApiError::internal("collection lock poisoned"))?;
        let mut seen_new_ids = HashSet::with_capacity(ids.len());
        let mut creating_points = Vec::with_capacity(ids.len());
        for id in ids {
            let creating = collection.get_point(id).is_none() && seen_new_ids.insert(id);
            creating_points.push(creating);
        }
        Ok(UpsertBatchPrecheck {
            creating_points,
            point_count: collection.len(),
            dimension: collection.dimension(),
            strict_finite: collection.strict_finite(),
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
    if let Ok(mut collection) = handle.try_write() {
        return Ok(collection.upsert_point_with_payload_unchecked(id, values, payload));
    }

    run(move || {
        Ok(handle
            .write()
            .map_err(|_| ApiError::internal("collection lock poisoned"))?
            .upsert_point_with_payload_unchecked(id, values, payload))
    })
    .await
}

pub(crate) async fn apply_upsert_batch(
    handle: CollectionHandle,
    points: Vec<(u64, Vec<f32>, PointPayload)>,
) -> Result<Vec<bool>, ApiError> {
    if let Ok(mut collection) = handle.try_write() {
        let mut created = Vec::with_capacity(points.len());
        for (id, values, payload) in points {
            created.push(collection.upsert_point_with_payload_unchecked(id, values, payload));
        }
        return Ok(created);
    }

    run(move || {
        let mut collection = handle
            .write()
            .map_err(|_| ApiError::internal("collection lock poisoned"))?;
        let mut created = Vec::with_capacity(points.len());
        for (id, values, payload) in points {
            created.push(collection.upsert_point_with_payload_unchecked(id, values, payload));
        }
        Ok(created)
    })
    .await
}

pub(crate) async fn load_collection_handle(
    state: AppState,
    canonical_name: String,
) -> Result<CollectionHandle, ApiError> {
    if let Ok(collections) = state.collections.try_read() {
        return collections.get(&canonical_name).cloned().ok_or_else(|| {
            ApiError::not_found(format!("collection '{canonical_name}' not found"))
        });
    }

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

pub(crate) async fn load_tenant_collection_handle(
    state: AppState,
    raw_name: String,
    tenant: TenantContext,
) -> Result<(String, CollectionHandle), ApiError> {
    let canonical_name = scoped_collection_name(&state, &raw_name, &tenant)?;
    let handle = load_collection_handle(state, canonical_name.clone()).await?;
    Ok((canonical_name, handle))
}

pub(crate) async fn collection_exists(
    state: AppState,
    canonical_name: String,
) -> Result<bool, ApiError> {
    if let Ok(collections) = state.collections.try_read() {
        return Ok(collections.contains_key(&canonical_name));
    }

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
    if let Ok(mut collections) = state.collections.try_write() {
        collections.insert(canonical_name, handle);
        return Ok(());
    }

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
) -> Result<usize, ApiError> {
    if let Ok(mut collections) = state.collections.try_write() {
        let removed_points = collections
            .remove(&canonical_name)
            .map(|collection| {
                collection
                    .read()
                    .map(|collection| collection.len())
                    .unwrap_or(0)
            })
            .unwrap_or(0);
        return Ok(removed_points);
    }

    run(move || {
        let mut collections = state
            .collections
            .write()
            .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;
        let removed_points = collections
            .remove(&canonical_name)
            .map(|collection| {
                collection
                    .read()
                    .map(|collection| collection.len())
                    .unwrap_or(0)
            })
            .unwrap_or(0);
        Ok(removed_points)
    })
    .await
}

pub(crate) async fn ensure_point_exists(handle: CollectionHandle, id: u64) -> Result<(), ApiError> {
    if let Ok(collection) = handle.try_read() {
        if collection.get_point(id).is_none() {
            return Err(ApiError::not_found(format!("point '{id}' not found")));
        }
        return Ok(());
    }

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

pub(crate) async fn collection_dimension(handle: CollectionHandle) -> Result<usize, ApiError> {
    if let Ok(collection) = handle.try_read() {
        return Ok(collection.dimension());
    }

    run(move || {
        let collection = handle
            .read()
            .map_err(|_| ApiError::internal("collection lock poisoned"))?;
        Ok(collection.dimension())
    })
    .await
}

pub(crate) async fn apply_delete(handle: CollectionHandle, id: u64) -> Result<bool, ApiError> {
    if let Ok(mut collection) = handle.try_write() {
        return Ok(collection.delete_point(id));
    }

    run(move || {
        Ok(handle
            .write()
            .map_err(|_| ApiError::internal("collection lock poisoned"))?
            .delete_point(id))
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
