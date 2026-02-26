use std::sync::atomic::Ordering;

use axum::extract::State;
use axum::http::header;
use axum::Json;

use crate::errors::ApiError;
use crate::index_manager::{
    configured_l2_build_cooldown_ms, configured_l2_build_max_in_flight,
    configured_l2_warmup_on_boot, l2_build_in_flight, l2_cache_hit_ratio,
};
use crate::models::MetricsResponse;
use crate::persistence_backlog;
use crate::state::AppState;

pub(crate) async fn metrics(
    State(state): State<AppState>,
) -> Result<Json<MetricsResponse>, ApiError> {
    let runtime_maps = collect_runtime_map_entries(&state);
    let payload = collect_metrics(&state, runtime_maps);
    Ok(Json(payload))
}

pub(crate) async fn metrics_prometheus(
    State(state): State<AppState>,
) -> Result<impl axum::response::IntoResponse, ApiError> {
    let runtime_maps = collect_runtime_map_entries(&state);
    let metrics = collect_metrics(&state, runtime_maps);
    let body = state
        .prometheus
        .render(metrics)
        .map_err(|error| ApiError::internal(format!("prometheus renderer failed: {error}")))?;

    Ok((
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    ))
}

#[derive(Clone, Copy)]
struct RuntimeMapEntries {
    collection_write_lock_entries: usize,
    tenant_rate_window_entries: usize,
    tenant_quota_lock_entries: usize,
}

fn collect_runtime_map_entries(state: &AppState) -> RuntimeMapEntries {
    RuntimeMapEntries {
        collection_write_lock_entries: state.collection_write_locks.len(),
        tenant_rate_window_entries: state.tenant_rate_windows.len(),
        tenant_quota_lock_entries: state.tenant_quota_locks.len(),
    }
}

fn collect_metrics(state: &AppState, runtime_maps: RuntimeMapEntries) -> MetricsResponse {
    let collection_count = state
        .metrics
        .collections_total
        .load(Ordering::Relaxed)
        .min(usize::MAX as u64) as usize;
    let point_count = state
        .metrics
        .points_total
        .load(Ordering::Relaxed)
        .min(usize::MAX as u64) as usize;

    let l2_indexes = state.l2_indexes.len();
    let (http_requests_total, http_request_duration_us_total) = stable_http_duration_totals(state);
    let http_request_duration_us_avg = if http_requests_total == 0 {
        0.0
    } else {
        http_request_duration_us_total as f64 / http_requests_total as f64
    };
    let engine_loaded = state.engine_loaded.load(Ordering::Relaxed);
    let storage_available = state.storage_available.load(Ordering::Relaxed);
    let (
        persistence_wal_size_bytes,
        persistence_wal_tail_open,
        persistence_incremental_segments,
        persistence_incremental_size_bytes,
    ) = persistence_backlog(state);

    MetricsResponse {
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
        http_responses_5xx_total: state
            .metrics
            .http_responses_5xx_total
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
        l2_index_build_max_in_flight: configured_l2_build_max_in_flight(),
        l2_index_warmup_on_boot: configured_l2_warmup_on_boot(),
        l2_index_build_in_flight: l2_build_in_flight(state),
        auth_failures_total: state.metrics.auth_failures_total.load(Ordering::Relaxed),
        rate_limit_rejections_total: state
            .metrics
            .rate_limit_rejections_total
            .load(Ordering::Relaxed),
        audit_events_total: state.metrics.audit_events_total.load(Ordering::Relaxed),
        collection_write_lock_entries: runtime_maps.collection_write_lock_entries,
        tenant_rate_window_entries: runtime_maps.tenant_rate_window_entries,
        tenant_quota_lock_entries: runtime_maps.tenant_quota_lock_entries,
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
        persistence_wal_sync_every_n_writes: state.config.wal_sync_every_n_writes,
        persistence_wal_sync_interval_seconds: state.config.wal_sync_interval_seconds,
        persistence_wal_group_commit_max_batch: state.config.wal_group_commit_max_batch,
        persistence_wal_group_commit_flush_delay_ms: state.config.wal_group_commit_flush_delay_ms,
        persistence_async_checkpoints: state.config.async_checkpoints,
        persistence_checkpoint_compact_after: state.config.checkpoint_compact_after,
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
        persistence_checkpoint_schedule_skips_total: state
            .metrics
            .persistence_checkpoint_schedule_skips_total
            .load(Ordering::Relaxed),
        persistence_wal_group_commits_total: state
            .metrics
            .persistence_wal_group_commits_total
            .load(Ordering::Relaxed),
        persistence_wal_grouped_records_total: state
            .metrics
            .persistence_wal_grouped_records_total
            .load(Ordering::Relaxed),
        persistence_wal_group_queue_depth: state.wal_group_queue.pending_len(),
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
        memory_budget_bytes: state.resource_manager.budget_bytes(),
        memory_used_bytes: state.resource_manager.used_bytes(),
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

fn persistence_backlog(state: &AppState) -> (u64, bool, u64, u64) {
    if !state.config.persistence_enabled {
        return (0, false, 0, 0);
    }

    let snapshot = persistence_backlog::snapshot(state);

    (
        snapshot.wal_size_bytes,
        snapshot.wal_tail_open,
        snapshot.incremental_segments,
        snapshot.incremental_size_bytes,
    )
}
