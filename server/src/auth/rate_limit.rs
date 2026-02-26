use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::errors::ApiError;
use crate::state::{AppState, TenantRateWindow};

use super::TenantContext;

pub(super) async fn enforce_rate_limit(
    state: &AppState,
    tenant: &TenantContext,
) -> Result<(), ApiError> {
    let limit = state.auth_config.rate_limit_per_minute;
    if limit == 0 {
        return Ok(());
    }

    let now_minute = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        / 60;

    let retention = state.auth_config.rate_window_retention_minutes;
    maybe_prune_rate_windows(state, now_minute, retention);
    let mut entry = state
        .tenant_rate_windows
        .entry(tenant.tenant_key().to_string())
        .or_insert(TenantRateWindow {
            minute: now_minute,
            count: 0,
        });

    if entry.minute != now_minute {
        entry.minute = now_minute;
        entry.count = 0;
    }
    if entry.count >= limit {
        return Err(ApiError::resource_exhausted(
            "tenant request rate exceeded configured limit",
        ));
    }

    entry.count += 1;
    Ok(())
}

fn maybe_prune_rate_windows(state: &AppState, now_minute: u64, retention: u64) {
    let last_pruned = state
        .tenant_rate_windows_last_prune_minute
        .load(Ordering::Acquire);
    if now_minute <= last_pruned {
        return;
    }
    if state
        .tenant_rate_windows_last_prune_minute
        .compare_exchange(last_pruned, now_minute, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }
    state
        .tenant_rate_windows
        .retain(|_, window| now_minute.saturating_sub(window.minute) <= retention);
}
