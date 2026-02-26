use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::auth::TenantContext;
use crate::errors::ApiError;
use crate::state::{AppState, TenantQuotaUsage};

pub(crate) fn tenant_quotas_enabled(state: &AppState) -> bool {
    state.auth_config.tenant_max_collections > 0 || state.auth_config.tenant_max_points > 0
}

pub(crate) async fn acquire_tenant_quota_guard(
    state: &AppState,
    tenant: &TenantContext,
) -> Result<OwnedSemaphorePermit, ApiError> {
    let tenant_key = tenant.tenant_key().to_string();
    maybe_prune_tenant_quota_locks(state, tenant.tenant_key());
    let semaphore = {
        let entry = state
            .tenant_quota_locks
            .entry(tenant_key)
            .or_insert_with(|| Arc::new(Semaphore::new(1)));
        Arc::clone(entry.value())
    };
    semaphore
        .acquire_owned()
        .await
        .map_err(|_| ApiError::internal("tenant quota semaphore closed"))
}

pub(crate) async fn maybe_acquire_tenant_quota_guard(
    state: &AppState,
    tenant: &TenantContext,
) -> Result<Option<OwnedSemaphorePermit>, ApiError> {
    if !tenant_quotas_enabled(state) {
        return Ok(None);
    }
    acquire_tenant_quota_guard(state, tenant).await.map(Some)
}

pub(crate) async fn tenant_collection_count(
    state: AppState,
    tenant: TenantContext,
) -> Result<usize, ApiError> {
    let usage = tenant_usage(&state, &tenant);
    Ok(usage.collections.min(usize::MAX as u64) as usize)
}

pub(crate) async fn tenant_point_count(
    state: AppState,
    tenant: TenantContext,
) -> Result<usize, ApiError> {
    let usage = tenant_usage(&state, &tenant);
    Ok(usage.points.min(usize::MAX as u64) as usize)
}

pub(crate) async fn record_collection_created(state: &AppState, tenant: &TenantContext) {
    let tenant_key = tenant.tenant_key().to_string();
    let mut entry = state.tenant_quota_usage.entry(tenant_key).or_default();
    entry.collections = entry.collections.saturating_add(1);
}

pub(crate) async fn record_collection_removed(
    state: &AppState,
    tenant: &TenantContext,
    removed_points: usize,
) {
    let tenant_key = tenant.tenant_key().to_string();
    let Some(mut entry) = state.tenant_quota_usage.get_mut(&tenant_key) else {
        return;
    };
    entry.collections = entry.collections.saturating_sub(1);
    entry.points = entry
        .points
        .saturating_sub(removed_points.min(u64::MAX as usize) as u64);
    let should_remove = entry.collections == 0 && entry.points == 0;
    drop(entry);
    if should_remove {
        let _ = state.tenant_quota_usage.remove(&tenant_key);
    }
}

pub(crate) async fn record_point_created(state: &AppState, tenant: &TenantContext) {
    let tenant_key = tenant.tenant_key().to_string();
    let mut entry = state.tenant_quota_usage.entry(tenant_key).or_default();
    entry.points = entry.points.saturating_add(1);
}

pub(crate) async fn record_points_created(
    state: &AppState,
    tenant: &TenantContext,
    created_points: usize,
) {
    if created_points == 0 {
        return;
    }
    let tenant_key = tenant.tenant_key().to_string();
    let mut entry = state.tenant_quota_usage.entry(tenant_key).or_default();
    entry.points = entry
        .points
        .saturating_add(created_points.min(u64::MAX as usize) as u64);
}

pub(crate) async fn record_point_deleted(state: &AppState, tenant: &TenantContext) {
    let tenant_key = tenant.tenant_key().to_string();
    let Some(mut entry) = state.tenant_quota_usage.get_mut(&tenant_key) else {
        return;
    };
    entry.points = entry.points.saturating_sub(1);
    let should_remove = entry.collections == 0 && entry.points == 0;
    drop(entry);
    if should_remove {
        let _ = state.tenant_quota_usage.remove(&tenant_key);
    }
}

fn tenant_usage(state: &AppState, tenant: &TenantContext) -> TenantQuotaUsage {
    state
        .tenant_quota_usage
        .get(tenant.tenant_key())
        .as_deref()
        .copied()
        .unwrap_or_default()
}

fn maybe_prune_tenant_quota_locks(state: &AppState, tenant_key: &str) {
    let now_minute = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        / 60;
    let last_pruned = state
        .tenant_quota_locks_last_prune_minute
        .load(Ordering::Acquire);
    if now_minute <= last_pruned {
        return;
    }
    if state
        .tenant_quota_locks_last_prune_minute
        .compare_exchange(last_pruned, now_minute, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }

    state
        .tenant_quota_locks
        .retain(|key, lock| key == tenant_key || Arc::strong_count(lock) > 1);
}
