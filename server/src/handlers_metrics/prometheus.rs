use crate::models::MetricsResponse;

pub(crate) fn render(metrics: &MetricsResponse) -> String {
    format!(
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
# HELP aionbd_auth_failures_total Total authentication failures.\n\
# TYPE aionbd_auth_failures_total counter\n\
aionbd_auth_failures_total {}\n\
# HELP aionbd_rate_limit_rejections_total Total rate-limited requests.\n\
# TYPE aionbd_rate_limit_rejections_total counter\n\
aionbd_rate_limit_rejections_total {}\n\
# HELP aionbd_audit_events_total Total emitted audit events.\n\
# TYPE aionbd_audit_events_total counter\n\
aionbd_audit_events_total {}\n\
# HELP aionbd_tenant_quota_collection_rejections_total Total collection write rejections due to tenant quota.\n\
# TYPE aionbd_tenant_quota_collection_rejections_total counter\n\
aionbd_tenant_quota_collection_rejections_total {}\n\
# HELP aionbd_tenant_quota_point_rejections_total Total point write rejections due to tenant quota.\n\
# TYPE aionbd_tenant_quota_point_rejections_total counter\n\
aionbd_tenant_quota_point_rejections_total {}\n\
# HELP aionbd_persistence_enabled Persistence mode flag (1 enabled, 0 disabled).\n\
# TYPE aionbd_persistence_enabled gauge\n\
aionbd_persistence_enabled {}\n\
# HELP aionbd_persistence_writes Successful persisted writes since startup.\n\
# TYPE aionbd_persistence_writes counter\n\
aionbd_persistence_writes {}\n\
# HELP aionbd_persistence_checkpoint_degraded_total Total checkpoints that fell back to WAL-only mode.\n\
# TYPE aionbd_persistence_checkpoint_degraded_total counter\n\
aionbd_persistence_checkpoint_degraded_total {}\n\
# HELP aionbd_persistence_checkpoint_success_total Total successful checkpoints.\n\
# TYPE aionbd_persistence_checkpoint_success_total counter\n\
aionbd_persistence_checkpoint_success_total {}\n\
# HELP aionbd_persistence_checkpoint_error_total Total checkpoint attempts that failed with an internal error.\n\
# TYPE aionbd_persistence_checkpoint_error_total counter\n\
aionbd_persistence_checkpoint_error_total {}\n\
# HELP aionbd_persistence_wal_size_bytes Current WAL file size in bytes.\n\
# TYPE aionbd_persistence_wal_size_bytes gauge\n\
aionbd_persistence_wal_size_bytes {}\n\
# HELP aionbd_persistence_incremental_segments Current number of incremental snapshot segment files.\n\
# TYPE aionbd_persistence_incremental_segments gauge\n\
aionbd_persistence_incremental_segments {}\n\
# HELP aionbd_persistence_incremental_size_bytes Current total size of incremental snapshot segment files in bytes.\n\
# TYPE aionbd_persistence_incremental_size_bytes gauge\n\
aionbd_persistence_incremental_size_bytes {}\n\
# HELP aionbd_search_queries_total Total search requests handled.\n\
# TYPE aionbd_search_queries_total counter\n\
aionbd_search_queries_total {}\n\
# HELP aionbd_search_ivf_queries_total Total search requests executed in IVF mode.\n\
# TYPE aionbd_search_ivf_queries_total counter\n\
aionbd_search_ivf_queries_total {}\n\
# HELP aionbd_search_ivf_fallback_exact_total Total explicit IVF searches that fell back to exact scan.\n\
# TYPE aionbd_search_ivf_fallback_exact_total counter\n\
aionbd_search_ivf_fallback_exact_total {}\n",
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
        metrics.auth_failures_total,
        metrics.rate_limit_rejections_total,
        metrics.audit_events_total,
        metrics.tenant_quota_collection_rejections_total,
        metrics.tenant_quota_point_rejections_total,
        bool_as_u8(metrics.persistence_enabled),
        metrics.persistence_writes,
        metrics.persistence_checkpoint_degraded_total,
        metrics.persistence_checkpoint_success_total,
        metrics.persistence_checkpoint_error_total,
        metrics.persistence_wal_size_bytes,
        metrics.persistence_incremental_segments,
        metrics.persistence_incremental_size_bytes,
        metrics.search_queries_total,
        metrics.search_ivf_queries_total,
        metrics.search_ivf_fallback_exact_total,
    )
}

fn bool_as_u8(value: bool) -> u8 {
    if value {
        1
    } else {
        0
    }
}
