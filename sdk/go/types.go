package aionbd

import (
	"net/http"
	"time"
)

const (
	DefaultBaseURL = "http://127.0.0.1:8080"
	DefaultTimeout = 5 * time.Second
)

type Metric string

const (
	MetricDot    Metric = "dot"
	MetricL2     Metric = "l2"
	MetricCosine Metric = "cosine"
)

type SearchMode string

const (
	SearchModeExact SearchMode = "exact"
	SearchModeIVF   SearchMode = "ivf"
	SearchModeAuto  SearchMode = "auto"
)

type PointPayload map[string]any

type LiveResponse struct {
	Status   string `json:"status"`
	UptimeMS uint64 `json:"uptime_ms"`
}

type ReadyChecks struct {
	EngineLoaded     bool `json:"engine_loaded"`
	StorageAvailable bool `json:"storage_available"`
}

type ReadyResponse struct {
	Status   string      `json:"status"`
	UptimeMS uint64      `json:"uptime_ms"`
	Checks   ReadyChecks `json:"checks"`
}

type DistanceResponse struct {
	Metric Metric  `json:"metric"`
	Value  float32 `json:"value"`
}

type CollectionResponse struct {
	Name         string `json:"name"`
	Dimension    int    `json:"dimension"`
	StrictFinite bool   `json:"strict_finite"`
	PointCount   int    `json:"point_count"`
}

type ListCollectionsResponse struct {
	Collections []CollectionResponse `json:"collections"`
}

type SearchResponse struct {
	ID        uint64       `json:"id"`
	Metric    Metric       `json:"metric"`
	Value     float32      `json:"value"`
	Mode      SearchMode   `json:"mode"`
	RecallAtK *float32     `json:"recall_at_k,omitempty"`
	Payload   PointPayload `json:"payload,omitempty"`
}

type SearchHit struct {
	ID      uint64       `json:"id"`
	Value   float32      `json:"value"`
	Payload PointPayload `json:"payload,omitempty"`
}

type SearchTopKResponse struct {
	Metric    Metric      `json:"metric"`
	Mode      SearchMode  `json:"mode"`
	RecallAtK *float32    `json:"recall_at_k,omitempty"`
	Hits      []SearchHit `json:"hits"`
}

type SearchTopKBatchItem struct {
	Mode      SearchMode  `json:"mode"`
	RecallAtK *float32    `json:"recall_at_k,omitempty"`
	Hits      []SearchHit `json:"hits"`
}

type SearchTopKBatchResponse struct {
	Metric  Metric                `json:"metric"`
	Results []SearchTopKBatchItem `json:"results"`
}

type UpsertPointResponse struct {
	ID      uint64 `json:"id"`
	Created bool   `json:"created"`
}

type UpsertPointsBatchItem struct {
	ID      uint64       `json:"id"`
	Values  []float32    `json:"values"`
	Payload PointPayload `json:"payload,omitempty"`
}

type UpsertPointsBatchResponse struct {
	Created int                   `json:"created"`
	Updated int                   `json:"updated"`
	Results []UpsertPointResponse `json:"results"`
}

type PointResponse struct {
	ID      uint64       `json:"id"`
	Values  []float32    `json:"values"`
	Payload PointPayload `json:"payload"`
}

type PointIDResponse struct {
	ID uint64 `json:"id"`
}

type ListPointsResponse struct {
	Points      []PointIDResponse `json:"points"`
	Total       int               `json:"total"`
	NextOffset  *int              `json:"next_offset"`
	NextAfterID *uint64           `json:"next_after_id"`
}

type DeletePointResponse struct {
	ID      uint64 `json:"id"`
	Deleted bool   `json:"deleted"`
}

type DeleteCollectionResponse struct {
	Name    string `json:"name"`
	Deleted bool   `json:"deleted"`
}

type MetricsResponse struct {
	UptimeMS                                uint64  `json:"uptime_ms"`
	Ready                                   bool    `json:"ready"`
	EngineLoaded                            bool    `json:"engine_loaded"`
	StorageAvailable                        bool    `json:"storage_available"`
	HTTPRequestsTotal                       uint64  `json:"http_requests_total"`
	HTTPRequestsInFlight                    uint64  `json:"http_requests_in_flight"`
	HTTPResponses2xxTotal                   uint64  `json:"http_responses_2xx_total"`
	HTTPResponses4xxTotal                   uint64  `json:"http_responses_4xx_total"`
	HTTPRequests5xxTotal                    uint64  `json:"http_responses_5xx_total"`
	HTTPRequestDurationUsTotal              uint64  `json:"http_request_duration_us_total"`
	HTTPRequestDurationUsMax                uint64  `json:"http_request_duration_us_max"`
	HTTPRequestDurationUsAvg                float64 `json:"http_request_duration_us_avg"`
	Collections                             int     `json:"collections"`
	Points                                  int     `json:"points"`
	L2Indexes                               int     `json:"l2_indexes"`
	L2IndexCacheLookups                     uint64  `json:"l2_index_cache_lookups"`
	L2IndexCacheHits                        uint64  `json:"l2_index_cache_hits"`
	L2IndexCacheMisses                      uint64  `json:"l2_index_cache_misses"`
	L2IndexCacheHitRatio                    float64 `json:"l2_index_cache_hit_ratio"`
	L2IndexBuildRequests                    uint64  `json:"l2_index_build_requests"`
	L2IndexBuildSuccesses                   uint64  `json:"l2_index_build_successes"`
	L2IndexBuildFailures                    uint64  `json:"l2_index_build_failures"`
	L2IndexBuildCooldownSkips               uint64  `json:"l2_index_build_cooldown_skips"`
	L2IndexBuildCooldownMS                  uint64  `json:"l2_index_build_cooldown_ms"`
	L2IndexBuildMaxInFlight                 int     `json:"l2_index_build_max_in_flight"`
	L2IndexWarmupOnBoot                     bool    `json:"l2_index_warmup_on_boot"`
	L2IndexBuildInFlight                    int     `json:"l2_index_build_in_flight"`
	AuthFailuresTotal                       uint64  `json:"auth_failures_total"`
	RateLimitRejectionsTotal                uint64  `json:"rate_limit_rejections_total"`
	AuditEventsTotal                        uint64  `json:"audit_events_total"`
	CollectionWriteLockEntries              int     `json:"collection_write_lock_entries"`
	TenantRateWindowEntries                 int     `json:"tenant_rate_window_entries"`
	TenantQuotaLockEntries                  int     `json:"tenant_quota_lock_entries"`
	TenantQuotaCollectionRejectionsTotal    uint64  `json:"tenant_quota_collection_rejections_total"`
	TenantQuotaPointRejectionsTotal         uint64  `json:"tenant_quota_point_rejections_total"`
	PersistenceEnabled                      bool    `json:"persistence_enabled"`
	PersistenceWALSyncOnWrite               bool    `json:"persistence_wal_sync_on_write"`
	PersistenceWALSyncEveryNWrites          uint64  `json:"persistence_wal_sync_every_n_writes"`
	PersistenceWALSyncIntervalSeconds       uint64  `json:"persistence_wal_sync_interval_seconds"`
	PersistenceWALGroupCommitMaxBatch       int     `json:"persistence_wal_group_commit_max_batch"`
	PersistenceWALGroupCommitFlushDelayMS   uint64  `json:"persistence_wal_group_commit_flush_delay_ms"`
	PersistenceAsyncCheckpoints             bool    `json:"persistence_async_checkpoints"`
	PersistenceCheckpointCompactAfter       int     `json:"persistence_checkpoint_compact_after"`
	PersistenceWrites                       uint64  `json:"persistence_writes"`
	PersistenceCheckpointInFlight           bool    `json:"persistence_checkpoint_in_flight"`
	PersistenceCheckpointDegradedTotal      uint64  `json:"persistence_checkpoint_degraded_total"`
	PersistenceCheckpointSuccessTotal       uint64  `json:"persistence_checkpoint_success_total"`
	PersistenceCheckpointErrorTotal         uint64  `json:"persistence_checkpoint_error_total"`
	PersistenceCheckpointScheduleSkipsTotal uint64  `json:"persistence_checkpoint_schedule_skips_total"`
	PersistenceWALGroupCommitsTotal         uint64  `json:"persistence_wal_group_commits_total"`
	PersistenceWALGroupedRecordsTotal       uint64  `json:"persistence_wal_grouped_records_total"`
	PersistenceWALGroupQueueDepth           int     `json:"persistence_wal_group_queue_depth"`
	PersistenceWALSizeBytes                 uint64  `json:"persistence_wal_size_bytes"`
	PersistenceWALTailOpen                  bool    `json:"persistence_wal_tail_open"`
	PersistenceIncrementalSegments          uint64  `json:"persistence_incremental_segments"`
	PersistenceIncrementalSizeBytes         uint64  `json:"persistence_incremental_size_bytes"`
	SearchQueriesTotal                      uint64  `json:"search_queries_total"`
	SearchIVFQueriesTotal                   uint64  `json:"search_ivf_queries_total"`
	SearchIVFFallbackExactTotal             uint64  `json:"search_ivf_fallback_exact_total"`
	MaxPointsPerCollection                  int     `json:"max_points_per_collection"`
	MemoryBudgetBytes                       uint64  `json:"memory_budget_bytes"`
	MemoryUsedBytes                         uint64  `json:"memory_used_bytes"`
}

type SearchOptions struct {
	Metric         Metric
	Mode           SearchMode
	TargetRecall   *float32
	Filter         map[string]any
	IncludePayload *bool
}

type SearchTopKOptions struct {
	SearchOptions
	Limit *int
}

type ListPointsOptions struct {
	Offset  int
	Limit   *int
	AfterID *uint64
}

type ClientOptions struct {
	HTTPClient  *http.Client
	Timeout     time.Duration
	APIKey      string
	BearerToken string
	Headers     map[string]string
}

func IntPtr(value int) *int {
	return &value
}

func Uint64Ptr(value uint64) *uint64 {
	return &value
}

func Float32Ptr(value float32) *float32 {
	return &value
}

func BoolPtr(value bool) *bool {
	return &value
}
