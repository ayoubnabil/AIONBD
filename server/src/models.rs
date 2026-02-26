use std::collections::BTreeMap;

use aionbd_core::MetadataValue;
use serde::{Deserialize, Serialize};

pub(crate) const DEFAULT_TOPK_LIMIT: usize = 10;
pub(crate) const DEFAULT_PAGE_LIMIT: usize = 100;
pub(crate) type PointPayload = BTreeMap<String, MetadataValue>;

#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Metric {
    #[default]
    Dot,
    L2,
    Cosine,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SearchMode {
    Exact,
    Ivf,
    #[default]
    Auto,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DistanceRequest {
    pub(crate) left: Vec<f32>,
    pub(crate) right: Vec<f32>,
    #[serde(default)]
    pub(crate) metric: Metric,
}

#[derive(Debug, Serialize)]
pub(crate) struct DistanceResponse {
    pub(crate) metric: Metric,
    pub(crate) value: f32,
}

#[derive(Debug, Serialize)]
pub(crate) struct LiveResponse {
    pub(crate) status: &'static str,
    pub(crate) uptime_ms: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct ReadyChecks {
    pub(crate) engine_loaded: bool,
    pub(crate) storage_available: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct ReadyResponse {
    pub(crate) status: &'static str,
    pub(crate) uptime_ms: u64,
    pub(crate) checks: ReadyChecks,
}

#[derive(Debug, Serialize, Clone, Default)]
pub(crate) struct MetricsResponse {
    pub(crate) uptime_ms: u64,
    pub(crate) ready: bool,
    pub(crate) engine_loaded: bool,
    pub(crate) storage_available: bool,
    pub(crate) http_requests_total: u64,
    pub(crate) http_requests_in_flight: u64,
    pub(crate) http_responses_2xx_total: u64,
    pub(crate) http_responses_4xx_total: u64,
    pub(crate) http_responses_5xx_total: u64,
    pub(crate) http_request_duration_us_total: u64,
    pub(crate) http_request_duration_us_max: u64,
    pub(crate) http_request_duration_us_avg: f64,
    pub(crate) collections: usize,
    pub(crate) points: usize,
    pub(crate) l2_indexes: usize,
    pub(crate) l2_index_cache_lookups: u64,
    pub(crate) l2_index_cache_hits: u64,
    pub(crate) l2_index_cache_misses: u64,
    pub(crate) l2_index_cache_hit_ratio: f64,
    pub(crate) l2_index_build_requests: u64,
    pub(crate) l2_index_build_successes: u64,
    pub(crate) l2_index_build_failures: u64,
    pub(crate) l2_index_build_cooldown_skips: u64,
    pub(crate) l2_index_build_cooldown_ms: u64,
    pub(crate) l2_index_build_max_in_flight: usize,
    pub(crate) l2_index_warmup_on_boot: bool,
    pub(crate) l2_index_build_in_flight: usize,
    pub(crate) auth_failures_total: u64,
    pub(crate) rate_limit_rejections_total: u64,
    pub(crate) audit_events_total: u64,
    pub(crate) collection_write_lock_entries: usize,
    pub(crate) tenant_rate_window_entries: usize,
    pub(crate) tenant_quota_lock_entries: usize,
    pub(crate) tenant_quota_collection_rejections_total: u64,
    pub(crate) tenant_quota_point_rejections_total: u64,
    pub(crate) persistence_enabled: bool,
    pub(crate) persistence_wal_sync_on_write: bool,
    pub(crate) persistence_wal_sync_every_n_writes: u64,
    pub(crate) persistence_wal_sync_interval_seconds: u64,
    pub(crate) persistence_wal_group_commit_max_batch: usize,
    pub(crate) persistence_wal_group_commit_flush_delay_ms: u64,
    pub(crate) persistence_async_checkpoints: bool,
    pub(crate) persistence_checkpoint_compact_after: usize,
    pub(crate) persistence_writes: u64,
    pub(crate) persistence_checkpoint_in_flight: bool,
    pub(crate) persistence_checkpoint_degraded_total: u64,
    pub(crate) persistence_checkpoint_success_total: u64,
    pub(crate) persistence_checkpoint_error_total: u64,
    pub(crate) persistence_checkpoint_schedule_skips_total: u64,
    pub(crate) persistence_wal_group_commits_total: u64,
    pub(crate) persistence_wal_grouped_records_total: u64,
    pub(crate) persistence_wal_group_queue_depth: usize,
    pub(crate) persistence_wal_size_bytes: u64,
    pub(crate) persistence_wal_tail_open: bool,
    pub(crate) persistence_incremental_segments: u64,
    pub(crate) persistence_incremental_size_bytes: u64,
    pub(crate) search_queries_total: u64,
    pub(crate) search_ivf_queries_total: u64,
    pub(crate) search_ivf_fallback_exact_total: u64,
    pub(crate) max_points_per_collection: usize,
    pub(crate) memory_budget_bytes: u64,
    pub(crate) memory_used_bytes: u64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateCollectionRequest {
    pub(crate) name: String,
    pub(crate) dimension: usize,
    #[serde(default = "default_true")]
    pub(crate) strict_finite: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct CollectionResponse {
    pub(crate) name: String,
    pub(crate) dimension: usize,
    pub(crate) strict_finite: bool,
    pub(crate) point_count: usize,
}

#[derive(Debug, Serialize)]
pub(crate) struct ListCollectionsResponse {
    pub(crate) collections: Vec<CollectionResponse>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct UpsertPointRequest {
    pub(crate) values: Vec<f32>,
    #[serde(default)]
    pub(crate) payload: PointPayload,
}

#[derive(Debug, Deserialize)]
pub(crate) struct UpsertPointBatchItem {
    pub(crate) id: u64,
    pub(crate) values: Vec<f32>,
    #[serde(default)]
    pub(crate) payload: PointPayload,
}

#[derive(Debug, Deserialize)]
pub(crate) struct UpsertPointsBatchRequest {
    pub(crate) points: Vec<UpsertPointBatchItem>,
}

#[derive(Debug, Serialize)]
pub(crate) struct UpsertPointResponse {
    pub(crate) id: u64,
    pub(crate) created: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct UpsertPointsBatchResponse {
    pub(crate) created: usize,
    pub(crate) updated: usize,
    pub(crate) results: Vec<UpsertPointResponse>,
}

#[derive(Debug, Serialize)]
pub(crate) struct PointResponse {
    pub(crate) id: u64,
    pub(crate) values: Vec<f32>,
    pub(crate) payload: PointPayload,
}

#[derive(Debug, Serialize)]
pub(crate) struct DeletePointResponse {
    pub(crate) id: u64,
    pub(crate) deleted: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct DeleteCollectionResponse {
    pub(crate) name: String,
    pub(crate) deleted: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListPointsQuery {
    #[serde(default)]
    pub(crate) offset: usize,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
    #[serde(default)]
    pub(crate) after_id: Option<u64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct PointIdResponse {
    pub(crate) id: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct ListPointsResponse {
    pub(crate) points: Vec<PointIdResponse>,
    pub(crate) total: usize,
    pub(crate) next_offset: Option<usize>,
    pub(crate) next_after_id: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SearchRequest {
    pub(crate) query: Vec<f32>,
    #[serde(default)]
    pub(crate) metric: Metric,
    #[serde(default = "default_include_payload")]
    pub(crate) include_payload: bool,
    #[serde(default)]
    pub(crate) mode: SearchMode,
    #[serde(default)]
    pub(crate) target_recall: Option<f32>,
    #[serde(default)]
    pub(crate) filter: Option<SearchFilter>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SearchResponse {
    pub(crate) id: u64,
    pub(crate) metric: Metric,
    pub(crate) value: f32,
    pub(crate) mode: SearchMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) recall_at_k: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) payload: Option<PointPayload>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SearchTopKRequest {
    pub(crate) query: Vec<f32>,
    #[serde(default)]
    pub(crate) metric: Metric,
    #[serde(default = "default_include_payload")]
    pub(crate) include_payload: bool,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
    #[serde(default)]
    pub(crate) mode: SearchMode,
    #[serde(default)]
    pub(crate) target_recall: Option<f32>,
    #[serde(default)]
    pub(crate) filter: Option<SearchFilter>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SearchTopKBatchRequest {
    pub(crate) queries: Vec<Vec<f32>>,
    #[serde(default)]
    pub(crate) metric: Metric,
    #[serde(default = "default_include_payload")]
    pub(crate) include_payload: bool,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
    #[serde(default)]
    pub(crate) mode: SearchMode,
    #[serde(default)]
    pub(crate) target_recall: Option<f32>,
    #[serde(default)]
    pub(crate) filter: Option<SearchFilter>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SearchHit {
    pub(crate) id: u64,
    pub(crate) value: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) payload: Option<PointPayload>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SearchTopKResponse {
    pub(crate) metric: Metric,
    pub(crate) mode: SearchMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) recall_at_k: Option<f32>,
    pub(crate) hits: Vec<SearchHit>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SearchTopKBatchItem {
    pub(crate) mode: SearchMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) recall_at_k: Option<f32>,
    pub(crate) hits: Vec<SearchHit>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SearchTopKBatchResponse {
    pub(crate) metric: Metric,
    pub(crate) results: Vec<SearchTopKBatchItem>,
}

const fn default_include_payload() -> bool {
    true
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct SearchFilter {
    #[serde(default)]
    pub(crate) must: Vec<FilterClause>,
    #[serde(default)]
    pub(crate) should: Vec<FilterClause>,
    #[serde(default)]
    pub(crate) minimum_should_match: Option<usize>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub(crate) enum FilterClause {
    Match(FilterMatchClause),
    Range(FilterRangeClause),
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct FilterMatchClause {
    pub(crate) field: String,
    pub(crate) value: MetadataValue,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct FilterRangeClause {
    pub(crate) field: String,
    #[serde(default)]
    pub(crate) gt: Option<f64>,
    #[serde(default)]
    pub(crate) gte: Option<f64>,
    #[serde(default)]
    pub(crate) lt: Option<f64>,
    #[serde(default)]
    pub(crate) lte: Option<f64>,
}

const fn default_true() -> bool {
    true
}
