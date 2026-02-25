use std::sync::atomic::Ordering;

use axum::body::Body;
use axum::extract::State;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::errors::ApiError;
use crate::state::AppState;

pub(crate) async fn require_engine_loaded(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if !state.engine_loaded.load(Ordering::Relaxed) {
        return ApiError::service_unavailable("engine is not ready; restart required")
            .into_response();
    }
    next.run(request).await
}
