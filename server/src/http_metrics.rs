use std::sync::atomic::Ordering;
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::State;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;

use crate::state::AppState;

pub(crate) async fn track_http_metrics(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    state
        .metrics
        .http_requests_total
        .fetch_add(1, Ordering::Relaxed);
    state
        .metrics
        .http_requests_in_flight
        .fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();

    let response = next.run(request).await;

    state
        .metrics
        .http_requests_in_flight
        .fetch_sub(1, Ordering::Relaxed);
    if response.status().is_success() {
        state
            .metrics
            .http_responses_2xx_total
            .fetch_add(1, Ordering::Relaxed);
    } else if response.status().is_client_error() {
        state
            .metrics
            .http_responses_4xx_total
            .fetch_add(1, Ordering::Relaxed);
    } else if response.status().is_server_error() {
        state
            .metrics
            .http_responses_5xx_total
            .fetch_add(1, Ordering::Relaxed);
    }
    let elapsed_us = started.elapsed().as_micros().min(u64::MAX as u128) as u64;
    state
        .metrics
        .http_request_duration_us_total
        .fetch_add(elapsed_us, Ordering::Relaxed);
    maybe_rotate_max_window(&state);
    update_max(&state.metrics.http_request_duration_us_max, elapsed_us);

    response
}

fn maybe_rotate_max_window(state: &AppState) {
    let now_minute = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        / 60;
    loop {
        let current_minute = state
            .metrics
            .http_request_duration_us_max_window_minute
            .load(Ordering::Relaxed);
        if current_minute >= now_minute {
            return;
        }
        if state
            .metrics
            .http_request_duration_us_max_window_minute
            .compare_exchange_weak(
                current_minute,
                now_minute,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            if current_minute != 0 {
                state
                    .metrics
                    .http_request_duration_us_max
                    .store(0, Ordering::Relaxed);
            }
            return;
        }
    }
}

fn update_max(max: &std::sync::atomic::AtomicU64, observed: u64) {
    loop {
        let current = max.load(Ordering::Relaxed);
        if observed <= current {
            return;
        }
        if max
            .compare_exchange_weak(current, observed, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return;
        }
    }
}
