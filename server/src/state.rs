use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::RwLock;
use std::time::Instant;

use aionbd_core::Collection;
use dashmap::DashMap;
use tokio::sync::Semaphore;

use crate::auth::AuthConfig;
use crate::config::AppConfig;
use crate::ivf_index::IvfIndex;
use crate::persistence_queue::WalGroupQueue;
use crate::prometheus_exporter::PrometheusExporter;
use crate::resource_manager::{estimated_vector_bytes, ResourceManager};

pub(crate) type CollectionHandle = Arc<RwLock<Collection>>;
pub(crate) type CollectionRegistry = BTreeMap<String, CollectionHandle>;

#[derive(Debug, Clone, Copy)]
pub(crate) struct TenantRateWindow {
    pub(crate) minute: u64,
    pub(crate) count: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PersistenceBacklogSnapshot {
    pub(crate) wal_size_bytes: u64,
    pub(crate) wal_tail_open: bool,
    pub(crate) incremental_segments: u64,
    pub(crate) incremental_size_bytes: u64,
}

#[derive(Debug, Default)]
pub(crate) struct PersistenceBacklogCache {
    wal_size_bytes: AtomicU64,
    wal_tail_open: AtomicBool,
    incremental_segments: AtomicU64,
    incremental_size_bytes: AtomicU64,
}

impl PersistenceBacklogCache {
    pub(crate) fn snapshot(&self) -> PersistenceBacklogSnapshot {
        PersistenceBacklogSnapshot {
            wal_size_bytes: self.wal_size_bytes.load(Ordering::Relaxed),
            wal_tail_open: self.wal_tail_open.load(Ordering::Relaxed),
            incremental_segments: self.incremental_segments.load(Ordering::Relaxed),
            incremental_size_bytes: self.incremental_size_bytes.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn store_snapshot(&self, snapshot: PersistenceBacklogSnapshot) {
        self.wal_size_bytes
            .store(snapshot.wal_size_bytes, Ordering::Relaxed);
        self.wal_tail_open
            .store(snapshot.wal_tail_open, Ordering::Relaxed);
        self.incremental_segments
            .store(snapshot.incremental_segments, Ordering::Relaxed);
        self.incremental_size_bytes
            .store(snapshot.incremental_size_bytes, Ordering::Relaxed);
    }

    pub(crate) fn update_wal(&self, wal_size_bytes: u64, wal_tail_open: bool) {
        self.wal_size_bytes.store(wal_size_bytes, Ordering::Relaxed);
        self.wal_tail_open.store(wal_tail_open, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct TenantQuotaUsage {
    pub(crate) collections: u64,
    pub(crate) points: u64,
}

#[derive(Debug, Default)]
pub(crate) struct MetricsState {
    pub(crate) http_requests_total: AtomicU64,
    pub(crate) http_requests_in_flight: AtomicU64,
    pub(crate) http_responses_2xx_total: AtomicU64,
    pub(crate) http_responses_4xx_total: AtomicU64,
    pub(crate) http_responses_5xx_total: AtomicU64,
    pub(crate) http_request_duration_us_total: AtomicU64,
    pub(crate) http_request_duration_us_max: AtomicU64,
    pub(crate) http_request_duration_us_max_window_minute: AtomicU64,
    pub(crate) persistence_writes: AtomicU64,
    pub(crate) search_queries_total: AtomicU64,
    pub(crate) search_ivf_queries_total: AtomicU64,
    pub(crate) search_ivf_fallback_exact_total: AtomicU64,
    pub(crate) l2_index_cache_lookups: AtomicU64,
    pub(crate) l2_index_cache_hits: AtomicU64,
    pub(crate) l2_index_cache_misses: AtomicU64,
    pub(crate) l2_index_build_requests: AtomicU64,
    pub(crate) l2_index_build_successes: AtomicU64,
    pub(crate) l2_index_build_failures: AtomicU64,
    pub(crate) l2_index_build_cooldown_skips: AtomicU64,
    pub(crate) auth_failures_total: AtomicU64,
    pub(crate) rate_limit_rejections_total: AtomicU64,
    pub(crate) tenant_quota_collection_rejections_total: AtomicU64,
    pub(crate) tenant_quota_point_rejections_total: AtomicU64,
    pub(crate) audit_events_total: AtomicU64,
    pub(crate) persistence_checkpoint_degraded_total: AtomicU64,
    pub(crate) persistence_checkpoint_success_total: AtomicU64,
    pub(crate) persistence_checkpoint_error_total: AtomicU64,
    pub(crate) persistence_checkpoint_schedule_skips_total: AtomicU64,
    pub(crate) persistence_wal_group_commits_total: AtomicU64,
    pub(crate) persistence_wal_grouped_records_total: AtomicU64,
    pub(crate) collections_total: AtomicU64,
    pub(crate) points_total: AtomicU64,
}

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) started_at: Instant,
    pub(crate) config: Arc<AppConfig>,
    pub(crate) auth_config: Arc<AuthConfig>,
    pub(crate) engine_loaded: Arc<AtomicBool>,
    pub(crate) storage_available: Arc<AtomicBool>,
    pub(crate) resource_manager: Arc<ResourceManager>,
    pub(crate) metrics: Arc<MetricsState>,
    pub(crate) prometheus: Arc<PrometheusExporter>,
    pub(crate) persistence_io_serial: Arc<Semaphore>,
    pub(crate) persistence_checkpoint_in_flight: Arc<AtomicBool>,
    pub(crate) persistence_backlog_cache: Arc<PersistenceBacklogCache>,
    pub(crate) wal_group_queue: Arc<WalGroupQueue>,
    pub(crate) collection_write_locks: Arc<DashMap<String, Arc<Semaphore>>>,
    pub(crate) l2_index_building: Arc<RwLock<BTreeSet<String>>>,
    pub(crate) l2_index_last_started_ms: Arc<StdMutex<BTreeMap<String, u64>>>,
    pub(crate) tenant_rate_windows: Arc<DashMap<String, TenantRateWindow>>,
    pub(crate) tenant_rate_windows_last_prune_minute: Arc<AtomicU64>,
    pub(crate) tenant_quota_locks: Arc<DashMap<String, Arc<Semaphore>>>,
    pub(crate) tenant_quota_locks_last_prune_minute: Arc<AtomicU64>,
    pub(crate) tenant_quota_usage: Arc<DashMap<String, TenantQuotaUsage>>,
    pub(crate) collections: Arc<RwLock<CollectionRegistry>>,
    pub(crate) l2_indexes: Arc<DashMap<String, Arc<IvfIndex>>>,
}

impl AppState {
    #[cfg(test)]
    pub(crate) fn with_collections(
        config: AppConfig,
        collections: BTreeMap<String, Collection>,
    ) -> Self {
        Self::with_collections_and_auth(config, collections, AuthConfig::default())
    }

    #[cfg(test)]
    pub(crate) fn with_collections_and_auth(
        config: AppConfig,
        collections: BTreeMap<String, Collection>,
        auth_config: AuthConfig,
    ) -> Self {
        Self::try_with_collections_and_auth(config, collections, auth_config)
            .expect("failed to initialize application state")
    }

    pub(crate) fn try_with_collections_and_auth(
        config: AppConfig,
        collections: BTreeMap<String, Collection>,
        auth_config: AuthConfig,
    ) -> Result<Self, prometheus::Error> {
        let memory_budget_bytes = config.memory_budget_bytes;
        let collections: CollectionRegistry = collections
            .into_iter()
            .map(|(name, collection)| (name, Arc::new(RwLock::new(collection))))
            .collect();
        let initial_points = collections.values().fold(0usize, |total, collection| {
            let collection = collection
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            total.saturating_add(collection.len())
        });
        let initial_vector_bytes = collections.values().fold(0u64, |total, collection| {
            let collection = collection
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let per_point = estimated_vector_bytes(collection.dimension());
            total.saturating_add((collection.len() as u64).saturating_mul(per_point))
        });
        let collection_write_locks = DashMap::new();
        for name in collections.keys() {
            collection_write_locks.insert(name.clone(), Arc::new(Semaphore::new(1)));
        }
        let tenant_quota_usage = collections.iter().fold(
            BTreeMap::<String, TenantQuotaUsage>::new(),
            |mut usage, (name, handle)| {
                if let Some((tenant_key, _collection_name)) = name.split_once("::") {
                    let points = handle
                        .read()
                        .map(|collection| collection.len() as u64)
                        .unwrap_or(0);
                    let entry = usage.entry(tenant_key.to_string()).or_default();
                    entry.collections = entry.collections.saturating_add(1);
                    entry.points = entry.points.saturating_add(points);
                }
                usage
            },
        );
        let tenant_quota_usage_map = DashMap::new();
        for (tenant_key, usage) in tenant_quota_usage {
            tenant_quota_usage_map.insert(tenant_key, usage);
        }
        let metrics = Arc::new(MetricsState::default());
        metrics.collections_total.store(
            collections.len().min(u64::MAX as usize) as u64,
            Ordering::Relaxed,
        );
        metrics.points_total.store(
            initial_points.min(u64::MAX as usize) as u64,
            Ordering::Relaxed,
        );
        if memory_budget_bytes > 0 && initial_vector_bytes > memory_budget_bytes {
            tracing::warn!(
                configured_budget_bytes = memory_budget_bytes,
                observed_used_bytes = initial_vector_bytes,
                "initial in-memory vector usage exceeds configured memory budget; new point writes may be rejected"
            );
        }

        Ok(Self {
            started_at: Instant::now(),
            config: Arc::new(config),
            auth_config: Arc::new(auth_config),
            engine_loaded: Arc::new(AtomicBool::new(true)),
            storage_available: Arc::new(AtomicBool::new(true)),
            resource_manager: Arc::new(ResourceManager::new(
                memory_budget_bytes,
                initial_vector_bytes,
            )),
            metrics,
            prometheus: Arc::new(PrometheusExporter::new()?),
            persistence_io_serial: Arc::new(Semaphore::new(1)),
            persistence_checkpoint_in_flight: Arc::new(AtomicBool::new(false)),
            persistence_backlog_cache: Arc::new(PersistenceBacklogCache::default()),
            wal_group_queue: Arc::new(WalGroupQueue::new()),
            collection_write_locks: Arc::new(collection_write_locks),
            l2_index_building: Arc::new(RwLock::new(BTreeSet::new())),
            l2_index_last_started_ms: Arc::new(StdMutex::new(BTreeMap::new())),
            tenant_rate_windows: Arc::new(DashMap::new()),
            tenant_rate_windows_last_prune_minute: Arc::new(AtomicU64::new(0)),
            tenant_quota_locks: Arc::new(DashMap::new()),
            tenant_quota_locks_last_prune_minute: Arc::new(AtomicU64::new(0)),
            tenant_quota_usage: Arc::new(tenant_quota_usage_map),
            collections: Arc::new(RwLock::new(collections)),
            l2_indexes: Arc::new(DashMap::new()),
        })
    }
}
