export type Metric = "dot" | "l2" | "cosine";
export type SearchMode = "exact" | "ivf" | "auto";

export type MetadataValue = string | number | boolean;
export type PointPayload = Record<string, MetadataValue>;

export interface LiveResponse {
  status: string;
  uptime_ms: number;
}

export interface ReadyChecks {
  engine_loaded: boolean;
  storage_available: boolean;
}

export interface ReadyResponse {
  status: string;
  uptime_ms: number;
  checks: ReadyChecks;
}

export interface DistanceResponse {
  metric: Metric;
  value: number;
}

export interface CollectionResponse {
  name: string;
  dimension: number;
  strict_finite: boolean;
  point_count: number;
}

export interface ListCollectionsResponse {
  collections: CollectionResponse[];
}

export interface SearchHit {
  id: number;
  value: number;
  payload?: PointPayload;
}

export interface SearchResponse {
  id: number;
  metric: Metric;
  value: number;
  mode: SearchMode;
  recall_at_k?: number;
  payload?: PointPayload;
}

export interface SearchTopKResponse {
  metric: Metric;
  mode: SearchMode;
  recall_at_k?: number;
  hits: SearchHit[];
}

export interface SearchTopKBatchItem {
  mode: SearchMode;
  recall_at_k?: number;
  hits: SearchHit[];
}

export interface SearchTopKBatchResponse {
  metric: Metric;
  results: SearchTopKBatchItem[];
}

export interface UpsertPointResponse {
  id: number;
  created: boolean;
}

export interface UpsertPointsBatchResponse {
  created: number;
  updated: number;
  results: UpsertPointResponse[];
}

export interface PointResponse {
  id: number;
  values: number[];
  payload: PointPayload;
}

export interface ListPointsResponse {
  points: Array<{ id: number }>;
  total: number;
  next_offset: number | null;
  next_after_id: number | null;
}

export interface DeletePointResponse {
  id: number;
  deleted: boolean;
}

export interface DeleteCollectionResponse {
  name: string;
  deleted: boolean;
}

export interface FilterMatchClause {
  field: string;
  value: MetadataValue;
}

export interface FilterRangeClause {
  field: string;
  gte?: number;
  lte?: number;
  gt?: number;
  lt?: number;
}

export type FilterClause = FilterMatchClause | FilterRangeClause;

export interface SearchFilter {
  must?: FilterClause[];
  should?: FilterClause[];
  minimum_should_match?: number;
}

export interface SearchOptions {
  metric?: Metric;
  mode?: SearchMode;
  targetRecall?: number;
  filter?: SearchFilter;
  includePayload?: boolean;
}

export interface SearchTopKOptions extends SearchOptions {
  limit?: number | null;
}

export interface SearchTopKBatchOptions extends SearchOptions {
  limit?: number | null;
}

export interface ListPointsOptions {
  offset?: number;
  limit?: number | null;
  afterId?: number | null;
}

export interface ClientOptions {
  baseUrl?: string;
  timeoutMs?: number;
  apiKey?: string | null;
  bearerToken?: string | null;
  headers?: Record<string, string>;
}

export interface RequestErrorOptions {
  cause?: unknown;
  status?: number;
  method?: string;
  path?: string;
  body?: string;
}

export interface MetricsResponse {
  uptime_ms: number;
  ready: boolean;
  engine_loaded: boolean;
  storage_available: boolean;
  http_requests_total: number;
  http_requests_in_flight: number;
  http_responses_2xx_total: number;
  http_responses_4xx_total: number;
  http_responses_5xx_total: number;
  http_request_duration_us_total: number;
  http_request_duration_us_max: number;
  http_request_duration_us_avg: number;
  collections: number;
  points: number;
  l2_indexes: number;
  l2_index_cache_lookups: number;
  l2_index_cache_hits: number;
  l2_index_cache_misses: number;
  l2_index_cache_hit_ratio: number;
  l2_index_build_requests: number;
  l2_index_build_successes: number;
  l2_index_build_failures: number;
  l2_index_build_cooldown_skips: number;
  l2_index_build_cooldown_ms: number;
  l2_index_build_max_in_flight: number;
  l2_index_warmup_on_boot: boolean;
  l2_index_build_in_flight: number;
  auth_failures_total: number;
  rate_limit_rejections_total: number;
  audit_events_total: number;
  collection_write_lock_entries: number;
  tenant_rate_window_entries: number;
  tenant_quota_lock_entries: number;
  tenant_quota_collection_rejections_total: number;
  tenant_quota_point_rejections_total: number;
  persistence_enabled: boolean;
  persistence_wal_sync_on_write: boolean;
  persistence_wal_sync_every_n_writes: number;
  persistence_wal_sync_interval_seconds: number;
  persistence_wal_group_commit_max_batch: number;
  persistence_wal_group_commit_flush_delay_ms: number;
  persistence_async_checkpoints: boolean;
  persistence_checkpoint_compact_after: number;
  persistence_writes: number;
  persistence_checkpoint_in_flight: boolean;
  persistence_checkpoint_degraded_total: number;
  persistence_checkpoint_success_total: number;
  persistence_checkpoint_error_total: number;
  persistence_checkpoint_schedule_skips_total: number;
  persistence_wal_group_commits_total: number;
  persistence_wal_grouped_records_total: number;
  persistence_wal_group_queue_depth: number;
  persistence_wal_size_bytes: number;
  persistence_wal_tail_open: boolean;
  persistence_incremental_segments: number;
  persistence_incremental_size_bytes: number;
  search_queries_total: number;
  search_ivf_queries_total: number;
  search_ivf_fallback_exact_total: number;
  max_points_per_collection: number;
  memory_budget_bytes: number;
  memory_used_bytes: number;
  [key: string]: unknown;
}

export class AionBDError extends Error {
  constructor(message: string, options?: RequestErrorOptions);
  status?: number;
  method?: string;
  path?: string;
  body?: string;
}

export class AionBDClient {
  constructor(baseUrl?: string, options?: ClientOptions);
  constructor(options?: ClientOptions);

  live(): Promise<LiveResponse>;
  ready(): Promise<ReadyResponse>;
  health(): Promise<ReadyResponse>;

  metrics(): Promise<MetricsResponse>;
  metricsPrometheus(): Promise<string>;

  distance(left: number[], right: number[], metric?: Metric): Promise<DistanceResponse>;

  createCollection(
    name: string,
    dimension: number,
    strictFinite?: boolean
  ): Promise<CollectionResponse>;

  listCollections(): Promise<ListCollectionsResponse>;
  getCollection(name: string): Promise<CollectionResponse>;

  searchCollection(
    collection: string,
    query: number[],
    options?: SearchOptions
  ): Promise<SearchResponse>;

  searchCollectionTopK(
    collection: string,
    query: number[],
    options?: SearchTopKOptions
  ): Promise<SearchTopKResponse>;

  searchCollectionTopKBatch(
    collection: string,
    queries: number[][],
    options?: SearchTopKBatchOptions
  ): Promise<SearchTopKBatchResponse>;

  upsertPoint(
    collection: string,
    pointId: number,
    values: number[],
    payload?: PointPayload
  ): Promise<UpsertPointResponse>;

  upsertPointsBatch(
    collection: string,
    points: Array<{ id: number; values: number[]; payload?: PointPayload }>
  ): Promise<UpsertPointsBatchResponse>;

  getPoint(collection: string, pointId: number): Promise<PointResponse>;
  listPoints(collection: string, options?: ListPointsOptions): Promise<ListPointsResponse>;

  deletePoint(collection: string, pointId: number): Promise<DeletePointResponse>;
  deleteCollection(name: string): Promise<DeleteCollectionResponse>;
}

export default AionBDClient;
