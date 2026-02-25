use std::sync::atomic::Ordering;

use axum::extract::State;
use axum::http::header;
use axum::Json;
use tokio::task;

use crate::errors::ApiError;
use crate::index_manager::{l2_build_in_flight, l2_cache_hit_ratio};
use crate::models::MetricsResponse;
use crate::state::AppState;

pub(crate) async fn metrics(
    State(state): State<AppState>,
) -> Result<Json<MetricsResponse>, ApiError> {
    let state_for_metrics = state.clone();
    let payload = task::spawn_blocking(move || collect_metrics(&state_for_metrics))
        .await
        .map_err(|_| ApiError::internal("metrics worker task failed"))??;
    Ok(Json(payload))
}

pub(crate) async fn metrics_prometheus(
    State(state): State<AppState>,
) -> Result<impl axum::response::IntoResponse, ApiError> {
    let state_for_metrics = state.clone();
    let metrics = task::spawn_blocking(move || collect_metrics(&state_for_metrics))
        .await
        .map_err(|_| ApiError::internal("metrics worker task failed"))??;
    let body = format!(
        "# HELP aionbd_uptime_ms Process uptime in milliseconds.\n\
# TYPE aionbd_uptime_ms gauge\n\
aionbd_uptime_ms {}\n\
# HELP aionbd_http_requests_total Total number of processed HTTP requests.\n\
# TYPE aionbd_http_requests_total counter\n\
aionbd_http_requests_total {}\n\
# HELP aionbd_http_requests_in_flight Number of HTTP requests currently being processed.\n\
# TYPE aionbd_http_requests_in_flight gauge\n\
aionbd_http_requests_in_flight {}\n\
# HELP aionbd_http_responses_2xx_total Total number of HTTP requests that returned a 2xx status.\n\
# TYPE aionbd_http_responses_2xx_total counter\n\
aionbd_http_responses_2xx_total {}\n\
# HELP aionbd_http_responses_4xx_total Total number of HTTP requests that returned a 4xx status.\n\
# TYPE aionbd_http_responses_4xx_total counter\n\
aionbd_http_responses_4xx_total {}\n\
# HELP aionbd_http_requests_5xx_total Total number of HTTP requests that returned a 5xx status.\n\
# TYPE aionbd_http_requests_5xx_total counter\n\
aionbd_http_requests_5xx_total {}\n\
# HELP aionbd_http_request_duration_us_total Sum of HTTP request processing time in microseconds.\n\
# TYPE aionbd_http_request_duration_us_total counter\n\
aionbd_http_request_duration_us_total {}\n\
# HELP aionbd_http_request_duration_us_max Maximum HTTP request processing time in microseconds.\n\
# TYPE aionbd_http_request_duration_us_max gauge\n\
aionbd_http_request_duration_us_max {}\n\
# HELP aionbd_http_request_duration_us_avg Mean HTTP request processing time in microseconds.\n\
# TYPE aionbd_http_request_duration_us_avg gauge\n\
aionbd_http_request_duration_us_avg {}\n\
# HELP aionbd_ready Server readiness flag (1 ready, 0 not ready).\n\
# TYPE aionbd_ready gauge\n\
aionbd_ready {}\n\
# HELP aionbd_engine_loaded Engine readiness flag (1 ready, 0 not ready).\n\
# TYPE aionbd_engine_loaded gauge\n\
aionbd_engine_loaded {}\n\
# HELP aionbd_storage_available Storage readiness flag (1 ready, 0 not ready).\n\
# TYPE aionbd_storage_available gauge\n\
aionbd_storage_available {}\n\
# HELP aionbd_collections Number of collections currently loaded.\n\
# TYPE aionbd_collections gauge\n\
aionbd_collections {}\n\
# HELP aionbd_points Number of points currently loaded across collections.\n\
# TYPE aionbd_points gauge\n\
aionbd_points {}\n\
# HELP aionbd_l2_indexes Number of cached L2 IVF indexes.\n\
# TYPE aionbd_l2_indexes gauge\n\
aionbd_l2_indexes {}\n\
# HELP aionbd_l2_index_cache_lookups Total L2 index cache lookups.\n\
# TYPE aionbd_l2_index_cache_lookups counter\n\
aionbd_l2_index_cache_lookups {}\n\
# HELP aionbd_l2_index_cache_hits Total L2 index cache hits.\n\
# TYPE aionbd_l2_index_cache_hits counter\n\
aionbd_l2_index_cache_hits {}\n\
# HELP aionbd_l2_index_cache_misses Total L2 index cache misses.\n\
# TYPE aionbd_l2_index_cache_misses counter\n\
aionbd_l2_index_cache_misses {}\n\
# HELP aionbd_l2_index_cache_hit_ratio L2 index cache hit ratio.\n\
# TYPE aionbd_l2_index_cache_hit_ratio gauge\n\
aionbd_l2_index_cache_hit_ratio {}\n\
# HELP aionbd_l2_index_build_requests Total asynchronous L2 index build requests.\n\
# TYPE aionbd_l2_index_build_requests counter\n\
aionbd_l2_index_build_requests {}\n\
# HELP aionbd_l2_index_build_successes Total successful asynchronous L2 index builds.\n\
# TYPE aionbd_l2_index_build_successes counter\n\
aionbd_l2_index_build_successes {}\n\
# HELP aionbd_l2_index_build_failures Total failed asynchronous L2 index builds.\n\
# TYPE aionbd_l2_index_build_failures counter\n\
aionbd_l2_index_build_failures {}\n\
# HELP aionbd_l2_index_build_in_flight Number of currently running asynchronous L2 index builds.\n\
# TYPE aionbd_l2_index_build_in_flight gauge\n\
aionbd_l2_index_build_in_flight {}\n\
# HELP aionbd_persistence_enabled Persistence mode flag (1 enabled, 0 disabled).\n\
# TYPE aionbd_persistence_enabled gauge\n\
aionbd_persistence_enabled {}\n\
# HELP aionbd_persistence_writes Successful persisted writes since startup.\n\
# TYPE aionbd_persistence_writes counter\n\
aionbd_persistence_writes {}\n\
# HELP aionbd_persistence_checkpoint_degraded_total Total checkpoints that fell back to WAL-only mode.\n\
# TYPE aionbd_persistence_checkpoint_degraded_total counter\n\
aionbd_persistence_checkpoint_degraded_total {}\n",
        metrics.uptime_ms,
        metrics.http_requests_total,
        metrics.http_requests_in_flight,
        metrics.http_responses_2xx_total,
        metrics.http_responses_4xx_total,
        metrics.http_requests_5xx_total,
        metrics.http_request_duration_us_total,
        metrics.http_request_duration_us_max,
        metrics.http_request_duration_us_avg,
        bool_as_u8(metrics.ready),
        bool_as_u8(metrics.engine_loaded),
        bool_as_u8(metrics.storage_available),
        metrics.collections,
        metrics.points,
        metrics.l2_indexes,
        metrics.l2_index_cache_lookups,
        metrics.l2_index_cache_hits,
        metrics.l2_index_cache_misses,
        metrics.l2_index_cache_hit_ratio,
        metrics.l2_index_build_requests,
        metrics.l2_index_build_successes,
        metrics.l2_index_build_failures,
        metrics.l2_index_build_in_flight,
        bool_as_u8(metrics.persistence_enabled),
        metrics.persistence_writes,
        metrics.persistence_checkpoint_degraded_total,
    );

    Ok((
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    ))
}

fn collect_metrics(state: &AppState) -> Result<MetricsResponse, ApiError> {
    let collections = state
        .collections
        .read()
        .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;
    let collection_count = collections.len();
    let point_count = collections.values().try_fold(0usize, |total, collection| {
        let collection = collection
            .read()
            .map_err(|_| ApiError::internal("collection lock poisoned"))?;
        Ok(total + collection.len())
    })?;
    drop(collections);

    let l2_indexes = state
        .l2_indexes
        .read()
        .map_err(|_| ApiError::internal("l2 index cache lock poisoned"))?
        .len();
    let (http_requests_total, http_request_duration_us_total) = stable_http_duration_totals(state);
    let http_request_duration_us_avg = if http_requests_total == 0 {
        0.0
    } else {
        http_request_duration_us_total as f64 / http_requests_total as f64
    };
    let engine_loaded = state.engine_loaded.load(Ordering::Relaxed);
    let storage_available = state.storage_available.load(Ordering::Relaxed);

    Ok(MetricsResponse {
        uptime_ms: state.started_at.elapsed().as_millis() as u64,
        ready: engine_loaded && storage_available,
        engine_loaded,
        storage_available,
        http_requests_total,
        http_requests_in_flight: state
            .metrics
            .http_requests_in_flight
            .load(Ordering::Relaxed),
        http_responses_2xx_total: state
            .metrics
            .http_responses_2xx_total
            .load(Ordering::Relaxed),
        http_responses_4xx_total: state
            .metrics
            .http_responses_4xx_total
            .load(Ordering::Relaxed),
        http_requests_5xx_total: state
            .metrics
            .http_requests_5xx_total
            .load(Ordering::Relaxed),
        http_request_duration_us_total,
        http_request_duration_us_max: state
            .metrics
            .http_request_duration_us_max
            .load(Ordering::Relaxed),
        http_request_duration_us_avg,
        collections: collection_count,
        points: point_count,
        l2_indexes,
        l2_index_cache_lookups: state.metrics.l2_index_cache_lookups.load(Ordering::Relaxed),
        l2_index_cache_hits: state.metrics.l2_index_cache_hits.load(Ordering::Relaxed),
        l2_index_cache_misses: state.metrics.l2_index_cache_misses.load(Ordering::Relaxed),
        l2_index_cache_hit_ratio: l2_cache_hit_ratio(state),
        l2_index_build_requests: state
            .metrics
            .l2_index_build_requests
            .load(Ordering::Relaxed),
        l2_index_build_successes: state
            .metrics
            .l2_index_build_successes
            .load(Ordering::Relaxed),
        l2_index_build_failures: state
            .metrics
            .l2_index_build_failures
            .load(Ordering::Relaxed),
        l2_index_build_in_flight: l2_build_in_flight(state),
        persistence_enabled: state.config.persistence_enabled,
        persistence_writes: state.metrics.persistence_writes.load(Ordering::Relaxed),
        persistence_checkpoint_degraded_total: state
            .metrics
            .persistence_checkpoint_degraded_total
            .load(Ordering::Relaxed),
    })
}

fn bool_as_u8(value: bool) -> u8 {
    if value {
        1
    } else {
        0
    }
}

fn stable_http_duration_totals(state: &AppState) -> (u64, u64) {
    let mut fallback = (0u64, 0u64);
    for _ in 0..8 {
        let requests_before = state.metrics.http_requests_total.load(Ordering::Acquire);
        let duration_total = state
            .metrics
            .http_request_duration_us_total
            .load(Ordering::Acquire);
        let requests_after = state.metrics.http_requests_total.load(Ordering::Acquire);
        fallback = (requests_after, duration_total);
        if requests_before == requests_after {
            return (requests_after, duration_total);
        }
    }
    fallback
}
