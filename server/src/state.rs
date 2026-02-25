use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::RwLock;
use std::time::Instant;

use aionbd_core::Collection;
use tokio::sync::{Mutex, Semaphore};

use crate::auth::AuthConfig;
use crate::config::AppConfig;
use crate::ivf_index::IvfIndex;
use crate::persistence_queue::WalGroupQueue;

pub(crate) type CollectionHandle = Arc<RwLock<Collection>>;
pub(crate) type CollectionRegistry = BTreeMap<String, CollectionHandle>;

#[derive(Debug, Clone, Copy)]
pub(crate) struct TenantRateWindow {
    pub(crate) minute: u64,
    pub(crate) count: u64,
}

#[derive(Debug, Default)]
pub(crate) struct MetricsState {
    pub(crate) http_requests_total: AtomicU64,
    pub(crate) http_requests_in_flight: AtomicU64,
    pub(crate) http_responses_2xx_total: AtomicU64,
    pub(crate) http_responses_4xx_total: AtomicU64,
    pub(crate) http_requests_5xx_total: AtomicU64,
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
}

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) started_at: Instant,
    pub(crate) config: Arc<AppConfig>,
    pub(crate) auth_config: Arc<AuthConfig>,
    pub(crate) engine_loaded: Arc<AtomicBool>,
    pub(crate) storage_available: Arc<AtomicBool>,
    pub(crate) metrics: Arc<MetricsState>,
    pub(crate) persistence_io_serial: Arc<Semaphore>,
    pub(crate) persistence_checkpoint_in_flight: Arc<AtomicBool>,
    pub(crate) wal_group_queue: Arc<WalGroupQueue>,
    pub(crate) collection_write_locks: Arc<Mutex<BTreeMap<String, Arc<Semaphore>>>>,
    pub(crate) l2_index_building: Arc<RwLock<BTreeSet<String>>>,
    pub(crate) l2_index_last_started_ms: Arc<StdMutex<BTreeMap<String, u64>>>,
    pub(crate) tenant_rate_windows: Arc<Mutex<BTreeMap<String, TenantRateWindow>>>,
    pub(crate) tenant_quota_locks: Arc<Mutex<BTreeMap<String, Arc<Semaphore>>>>,
    pub(crate) collections: Arc<RwLock<CollectionRegistry>>,
    pub(crate) l2_indexes: Arc<RwLock<BTreeMap<String, IvfIndex>>>,
}

impl AppState {
    #[cfg(test)]
    pub(crate) fn with_collections(
        config: AppConfig,
        collections: BTreeMap<String, Collection>,
    ) -> Self {
        Self::with_collections_and_auth(config, collections, AuthConfig::default())
    }

    pub(crate) fn with_collections_and_auth(
        config: AppConfig,
        collections: BTreeMap<String, Collection>,
        auth_config: AuthConfig,
    ) -> Self {
        let collections: CollectionRegistry = collections
            .into_iter()
            .map(|(name, collection)| (name, Arc::new(RwLock::new(collection))))
            .collect();
        let collection_write_locks = collections
            .keys()
            .map(|name| (name.clone(), Arc::new(Semaphore::new(1))))
            .collect();

        Self {
            started_at: Instant::now(),
            config: Arc::new(config),
            auth_config: Arc::new(auth_config),
            engine_loaded: Arc::new(AtomicBool::new(true)),
            storage_available: Arc::new(AtomicBool::new(true)),
            metrics: Arc::new(MetricsState::default()),
            persistence_io_serial: Arc::new(Semaphore::new(1)),
            persistence_checkpoint_in_flight: Arc::new(AtomicBool::new(false)),
            wal_group_queue: Arc::new(WalGroupQueue::new()),
            collection_write_locks: Arc::new(Mutex::new(collection_write_locks)),
            l2_index_building: Arc::new(RwLock::new(BTreeSet::new())),
            l2_index_last_started_ms: Arc::new(StdMutex::new(BTreeMap::new())),
            tenant_rate_windows: Arc::new(Mutex::new(BTreeMap::new())),
            tenant_quota_locks: Arc::new(Mutex::new(BTreeMap::new())),
            collections: Arc::new(RwLock::new(collections)),
            l2_indexes: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}
