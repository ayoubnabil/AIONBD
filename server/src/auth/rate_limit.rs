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

    let mut windows = state.tenant_rate_windows.lock().await;
    let retention = state.auth_config.rate_window_retention_minutes;
    windows.retain(|_, window| now_minute.saturating_sub(window.minute) <= retention);
    let entry = windows
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
