use aionbd_core::{CollectionError, VectorError};
use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{BoxError, Json};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct ErrorResponse {
    code: &'static str,
    message: String,
}

#[derive(Debug)]
pub(crate) struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    pub(crate) fn invalid_argument(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_argument",
            message: message.into(),
        }
    }

    pub(crate) fn payload_too_large(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            code: "payload_too_large",
            message: message.into(),
        }
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: message.into(),
        }
    }

    pub(crate) fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "conflict",
            message: message.into(),
        }
    }

    fn request_timeout() -> Self {
        Self {
            status: StatusCode::REQUEST_TIMEOUT,
            code: "request_timeout",
            message: "request timed out".to_string(),
        }
    }

    pub(crate) fn service_unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "not_ready",
            message: message.into(),
        }
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal",
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                code: self.code,
                message: self.message,
            }),
        )
            .into_response()
    }
}

pub(crate) fn map_json_rejection(rejection: JsonRejection) -> ApiError {
    let status = rejection.status();
    if status == StatusCode::PAYLOAD_TOO_LARGE {
        return ApiError::payload_too_large("request body exceeds configured size limit");
    }
    if status == StatusCode::UNSUPPORTED_MEDIA_TYPE {
        return ApiError::invalid_argument("content-type must be application/json");
    }
    ApiError::invalid_argument("invalid JSON payload")
}

pub(crate) fn map_vector_error(error: VectorError) -> ApiError {
    match error {
        VectorError::DimensionMismatch { .. } => {
            ApiError::invalid_argument("left and right must have the same length")
        }
        VectorError::EmptyVector => ApiError::invalid_argument("vectors must not be empty"),
        VectorError::ZeroNorm { .. } => {
            ApiError::invalid_argument("cosine similarity is undefined for zero vectors")
        }
        VectorError::NonFinite { .. } => {
            ApiError::invalid_argument("vectors must only contain finite values")
        }
    }
}

pub(crate) fn map_collection_error(error: CollectionError) -> ApiError {
    ApiError::invalid_argument(error.to_string())
}

pub(crate) async fn handle_middleware_error(error: BoxError) -> Response {
    if error.is::<tower::timeout::error::Elapsed>() {
        return ApiError::request_timeout().into_response();
    }

    tracing::error!(%error, "middleware error");
    ApiError::internal("internal middleware error").into_response()
}
