use aionbd_core::Collection;

use crate::config::AppConfig;
use crate::errors::ApiError;
use crate::models::{CollectionResponse, DistanceRequest};

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
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(ApiError::invalid_argument(
            "collection name must not be empty",
        ));
    }

    Ok(trimmed.to_string())
}

pub(crate) fn build_collection_response(collection: &Collection) -> CollectionResponse {
    CollectionResponse {
        name: collection.name().to_string(),
        dimension: collection.dimension(),
        strict_finite: collection.strict_finite(),
        point_count: collection.len(),
    }
}

fn first_non_finite_index(values: &[f32]) -> Option<usize> {
    values.iter().position(|value| !value.is_finite())
}
