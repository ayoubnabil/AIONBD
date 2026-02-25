use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::sync::atomic::Ordering;

use aionbd_core::incremental_snapshot_dir;
use axum::extract::State;
use axum::http::header;
use axum::Json;
use tokio::task;

use crate::errors::ApiError;
use crate::index_manager::{
    configured_l2_build_cooldown_ms, configured_l2_warmup_on_boot, l2_build_in_flight,
    l2_cache_hit_ratio,
};
use crate::models::MetricsResponse;
use crate::persistence::{
    configured_async_checkpoints, configured_checkpoint_compact_after,
    configured_wal_sync_every_n_writes,
};
use crate::state::AppState;

mod prometheus;

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
    let body = prometheus::render(&metrics);

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
    let collection_write_lock_entries = state.collection_write_locks.blocking_lock().len();
    let tenant_rate_window_entries = state.tenant_rate_windows.blocking_lock().len();
    let tenant_quota_lock_entries = state.tenant_quota_locks.blocking_lock().len();
    let (
        persistence_wal_size_bytes,
        persistence_wal_tail_open,
        persistence_incremental_segments,
        persistence_incremental_size_bytes,
    ) = persistence_backlog(state);

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
        l2_index_build_cooldown_skips: state
            .metrics
            .l2_index_build_cooldown_skips
            .load(Ordering::Relaxed),
        l2_index_build_cooldown_ms: configured_l2_build_cooldown_ms(),
        l2_index_warmup_on_boot: configured_l2_warmup_on_boot(),
        l2_index_build_in_flight: l2_build_in_flight(state),
        auth_failures_total: state.metrics.auth_failures_total.load(Ordering::Relaxed),
        rate_limit_rejections_total: state
            .metrics
            .rate_limit_rejections_total
            .load(Ordering::Relaxed),
        audit_events_total: state.metrics.audit_events_total.load(Ordering::Relaxed),
        collection_write_lock_entries,
        tenant_rate_window_entries,
        tenant_quota_lock_entries,
        tenant_quota_collection_rejections_total: state
            .metrics
            .tenant_quota_collection_rejections_total
            .load(Ordering::Relaxed),
        tenant_quota_point_rejections_total: state
            .metrics
            .tenant_quota_point_rejections_total
            .load(Ordering::Relaxed),
        persistence_enabled: state.config.persistence_enabled,
        persistence_wal_sync_on_write: state.config.wal_sync_on_write,
        persistence_wal_sync_every_n_writes: configured_wal_sync_every_n_writes(),
        persistence_async_checkpoints: configured_async_checkpoints(),
        persistence_checkpoint_compact_after: configured_checkpoint_compact_after(),
        persistence_writes: state.metrics.persistence_writes.load(Ordering::Relaxed),
        persistence_checkpoint_in_flight: state
            .persistence_checkpoint_in_flight
            .load(Ordering::Relaxed),
        persistence_checkpoint_degraded_total: state
            .metrics
            .persistence_checkpoint_degraded_total
            .load(Ordering::Relaxed),
        persistence_checkpoint_success_total: state
            .metrics
            .persistence_checkpoint_success_total
            .load(Ordering::Relaxed),
        persistence_checkpoint_error_total: state
            .metrics
            .persistence_checkpoint_error_total
            .load(Ordering::Relaxed),
        persistence_wal_size_bytes,
        persistence_wal_tail_open,
        persistence_incremental_segments,
        persistence_incremental_size_bytes,
        search_queries_total: state.metrics.search_queries_total.load(Ordering::Relaxed),
        search_ivf_queries_total: state
            .metrics
            .search_ivf_queries_total
            .load(Ordering::Relaxed),
        search_ivf_fallback_exact_total: state
            .metrics
            .search_ivf_fallback_exact_total
            .load(Ordering::Relaxed),
        max_points_per_collection: state.config.max_points_per_collection,
    })
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

fn persistence_backlog(state: &AppState) -> (u64, bool, u64, u64) {
    if !state.config.persistence_enabled {
        return (0, false, 0, 0);
    }

    let wal_size_bytes = fs::metadata(&state.config.wal_path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let wal_tail_open = wal_tail_is_open(&state.config.wal_path, wal_size_bytes);
    let incremental_dir = incremental_snapshot_dir(&state.config.snapshot_path);
    let mut incremental_segments = 0u64;
    let mut incremental_size_bytes = 0u64;

    if let Ok(entries) = fs::read_dir(incremental_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path
                .extension()
                .is_none_or(|extension| extension != "jsonl")
            {
                continue;
            }
            incremental_segments = incremental_segments.saturating_add(1);
            if let Ok(metadata) = entry.metadata() {
                incremental_size_bytes = incremental_size_bytes.saturating_add(metadata.len());
            }
        }
    }

    (
        wal_size_bytes,
        wal_tail_open,
        incremental_segments,
        incremental_size_bytes,
    )
}

fn wal_tail_is_open(path: &std::path::Path, wal_size_bytes: u64) -> bool {
    if wal_size_bytes == 0 {
        return false;
    }
    let Ok(mut file) = fs::File::open(path) else {
        return false;
    };
    if file.seek(SeekFrom::End(-1)).is_err() {
        return false;
    }
    let mut last = [0u8; 1];
    if file.read_exact(&mut last).is_err() {
        return false;
    }
    last[0] != b'\n'
}
