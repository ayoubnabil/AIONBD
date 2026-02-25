use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task;

use crate::auth::TenantContext;
use crate::errors::ApiError;
use crate::handler_utils::visible_collection_name;
use crate::state::{AppState, CollectionHandle};

pub(crate) async fn acquire_tenant_quota_guard(
    state: &AppState,
    tenant: &TenantContext,
) -> Result<OwnedSemaphorePermit, ApiError> {
    let tenant_key = tenant.tenant_key().to_string();
    let semaphore = {
        let mut locks = state.tenant_quota_locks.lock().await;
        locks.retain(|key, lock| key == &tenant_key || Arc::strong_count(lock) > 1);
        Arc::clone(
            locks
                .entry(tenant_key)
                .or_insert_with(|| Arc::new(Semaphore::new(1))),
        )
    };
    semaphore
        .acquire_owned()
        .await
        .map_err(|_| ApiError::internal("tenant quota semaphore closed"))
}

pub(crate) async fn tenant_collection_count(
    state: AppState,
    tenant: TenantContext,
) -> Result<usize, ApiError> {
    task::spawn_blocking(move || tenant_collection_count_blocking(&state, &tenant))
        .await
        .map_err(|_| ApiError::internal("tenant collection counting worker task failed"))?
}

pub(crate) async fn tenant_point_count(
    state: AppState,
    tenant: TenantContext,
) -> Result<usize, ApiError> {
    task::spawn_blocking(move || tenant_point_count_blocking(&state, &tenant))
        .await
        .map_err(|_| ApiError::internal("tenant point counting worker task failed"))?
}

fn tenant_collection_count_blocking(
    state: &AppState,
    tenant: &TenantContext,
) -> Result<usize, ApiError> {
    let collections = state
        .collections
        .read()
        .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;
    let mut count = 0usize;
    for name in collections.keys() {
        if visible_collection_name(state, name, tenant)?.is_some() {
            count = count.saturating_add(1);
        }
    }
    Ok(count)
}

fn tenant_point_count_blocking(
    state: &AppState,
    tenant: &TenantContext,
) -> Result<usize, ApiError> {
    let handles = visible_collection_handles(state, tenant)?;
    let mut total = 0usize;
    for handle in handles {
        let collection = handle
            .read()
            .map_err(|_| ApiError::internal("collection lock poisoned"))?;
        total = total.saturating_add(collection.len());
    }
    Ok(total)
}

fn visible_collection_handles(
    state: &AppState,
    tenant: &TenantContext,
) -> Result<Vec<CollectionHandle>, ApiError> {
    let collections = state
        .collections
        .read()
        .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;
    let mut handles = Vec::with_capacity(collections.len());
    for (name, handle) in collections.iter() {
        if visible_collection_name(state, name, tenant)?.is_some() {
            handles.push(handle.clone());
        }
    }
    Ok(handles)
}
