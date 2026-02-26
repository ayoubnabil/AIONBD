use std::sync::Arc;

use aionbd_core::Collection;
use tokio::sync::Semaphore;

use crate::auth::{AuthMode, TenantContext};
use crate::config::AppConfig;
use crate::errors::ApiError;
use crate::models::{CollectionResponse, DistanceRequest, PointPayload};
use crate::state::AppState;

pub(crate) fn validate_distance_request(
    payload: &DistanceRequest,
    config: &AppConfig,
) -> Result<(), ApiError> {
    if payload.left.len() != payload.right.len() {
        return Err(ApiError::invalid_argument(
            "left and right must have the same length",
        ));
    }
    if payload.left.is_empty() {
        return Err(ApiError::invalid_argument("vectors must not be empty"));
    }
    if payload.left.len() > config.max_dimension {
        return Err(ApiError::invalid_argument(format!(
            "vector dimension {} exceeds configured maximum {}",
            payload.left.len(),
            config.max_dimension
        )));
    }

    if config.strict_finite {
        if let Some(index) = first_non_finite_index(&payload.left) {
            return Err(ApiError::invalid_argument(format!(
                "left contains a non-finite value at index {index}"
            )));
        }
        if let Some(index) = first_non_finite_index(&payload.right) {
            return Err(ApiError::invalid_argument(format!(
                "right contains a non-finite value at index {index}"
            )));
        }
    }

    Ok(())
}

pub(crate) fn canonical_collection_name(name: &str) -> Result<String, ApiError> {
    const MAX_COLLECTION_NAME_LEN: usize = 128;

    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(ApiError::invalid_argument(
            "collection name must not be empty",
        ));
    }
    if trimmed.len() > MAX_COLLECTION_NAME_LEN {
        return Err(ApiError::invalid_argument(format!(
            "collection name must be <= {MAX_COLLECTION_NAME_LEN} characters"
        )));
    }
    if trimmed == "." || trimmed == ".." || trimmed.contains("..") {
        return Err(ApiError::invalid_argument(
            "collection name contains an invalid path segment",
        ));
    }
    if !trimmed
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(ApiError::invalid_argument(
            "collection name may only contain ASCII letters, numbers, '-', '_' and '.'",
        ));
    }

    Ok(trimmed.to_string())
}

pub(crate) fn validate_upsert_input(
    values: &[f32],
    payload: &PointPayload,
    expected_dimension: usize,
    strict_finite: bool,
) -> Result<(), ApiError> {
    if values.len() != expected_dimension {
        return Err(ApiError::invalid_argument(format!(
            "invalid vector dimension: expected {expected_dimension}, got {}",
            values.len()
        )));
    }

    if strict_finite {
        if let Some(index) = values.iter().position(|value| !value.is_finite()) {
            return Err(ApiError::invalid_argument(format!(
                "vector contains non-finite value at index {index}"
            )));
        }
    }

    if payload.keys().any(|key| key.trim().is_empty()) {
        return Err(ApiError::invalid_argument("payload keys must not be empty"));
    }

    Ok(())
}

pub(crate) async fn collection_write_lock(
    state: &AppState,
    canonical_name: &str,
) -> Result<Arc<Semaphore>, ApiError> {
    if let Some(lock) = state.collection_write_locks.get(canonical_name) {
        return Ok(Arc::clone(lock.value()));
    }

    let lock = state
        .collection_write_locks
        .entry(canonical_name.to_string())
        .or_insert_with(|| Arc::new(Semaphore::new(1)));
    Ok(Arc::clone(&lock))
}

pub(crate) async fn existing_collection_write_lock(
    state: &AppState,
    canonical_name: &str,
) -> Result<Arc<Semaphore>, ApiError> {
    if let Some(lock) = state.collection_write_locks.get(canonical_name) {
        return Ok(Arc::clone(lock.value()));
    }

    let lock = state
        .collection_write_locks
        .entry(canonical_name.to_string())
        .or_insert_with(|| Arc::new(Semaphore::new(1)));
    Ok(Arc::clone(&lock))
}

pub(crate) async fn remove_collection_write_lock(
    state: &AppState,
    canonical_name: &str,
) -> Result<(), ApiError> {
    let should_remove = state
        .collection_write_locks
        .get(canonical_name)
        .is_some_and(|lock| {
            Arc::strong_count(lock.value()) == 1 && lock.value().available_permits() == 1
        });
    if should_remove {
        let _ = state.collection_write_locks.remove(canonical_name);
    }
    Ok(())
}

pub(crate) fn build_collection_response(collection: &Collection) -> CollectionResponse {
    CollectionResponse {
        name: collection.name().to_string(),
        dimension: collection.dimension(),
        strict_finite: collection.strict_finite(),
        point_count: collection.len(),
    }
}

pub(crate) fn scoped_collection_name(
    state: &AppState,
    raw_name: &str,
    tenant: &TenantContext,
) -> Result<String, ApiError> {
    let canonical = canonical_collection_name(raw_name)?;
    let Some(prefix) = tenant_scope_prefix(state, tenant)? else {
        return Ok(canonical);
    };
    Ok(format!("{prefix}{canonical}"))
}

pub(crate) fn visible_collection_name(
    state: &AppState,
    stored_name: &str,
    tenant: &TenantContext,
) -> Result<Option<String>, ApiError> {
    let Some(prefix) = tenant_scope_prefix(state, tenant)? else {
        return Ok(Some(stored_name.to_string()));
    };
    if !stored_name.starts_with(&prefix) {
        return Ok(None);
    }
    Ok(Some(stored_name[prefix.len()..].to_string()))
}

fn tenant_scope_prefix(
    state: &AppState,
    tenant: &TenantContext,
) -> Result<Option<String>, ApiError> {
    if state.auth_config.mode == AuthMode::Disabled {
        return Ok(None);
    }
    let Some(tenant_id) = tenant.tenant_id() else {
        return Err(ApiError::unauthorized("tenant context is missing"));
    };
    Ok(Some(format!("{tenant_id}::")))
}

fn first_non_finite_index(values: &[f32]) -> Option<usize> {
    values.iter().position(|value| !value.is_finite())
}

#[cfg(test)]
mod tests {
    use super::canonical_collection_name;

    #[test]
    fn canonical_collection_name_rejects_path_like_segments() {
        assert!(canonical_collection_name("../demo").is_err());
        assert!(canonical_collection_name("demo/../x").is_err());
        assert!(canonical_collection_name("demo\\x").is_err());
    }

    #[test]
    fn canonical_collection_name_accepts_safe_charset() {
        let name = canonical_collection_name("tenant-a.demo_1").expect("name should be accepted");
        assert_eq!(name, "tenant-a.demo_1");
    }
}
