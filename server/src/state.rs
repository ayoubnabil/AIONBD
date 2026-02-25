use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Instant;

use aionbd_core::Collection;

use crate::config::AppConfig;
use crate::ivf_index::IvfIndex;

pub(crate) struct AppState {
    pub(crate) started_at: Instant,
    pub(crate) config: Arc<AppConfig>,
    pub(crate) engine_loaded: Arc<AtomicBool>,
    pub(crate) storage_available: Arc<AtomicBool>,
    pub(crate) persistence_writes: Arc<AtomicU64>,
    pub(crate) collections: Arc<RwLock<BTreeMap<String, Collection>>>,
    pub(crate) l2_indexes: Arc<RwLock<BTreeMap<String, IvfIndex>>>,
}

impl Clone for AppState {
    fn clone(&self) -> Self {
        Self {
            started_at: self.started_at,
            config: Arc::clone(&self.config),
            engine_loaded: Arc::clone(&self.engine_loaded),
            storage_available: Arc::clone(&self.storage_available),
            persistence_writes: Arc::clone(&self.persistence_writes),
            collections: Arc::clone(&self.collections),
            l2_indexes: Arc::clone(&self.l2_indexes),
        }
    }
}

impl AppState {
    pub(crate) fn with_collections(
        config: AppConfig,
        collections: BTreeMap<String, Collection>,
    ) -> Self {
        Self {
            started_at: Instant::now(),
            config: Arc::new(config),
            engine_loaded: Arc::new(AtomicBool::new(true)),
            storage_available: Arc::new(AtomicBool::new(true)),
            persistence_writes: Arc::new(AtomicU64::new(0)),
            collections: Arc::new(RwLock::new(collections)),
            l2_indexes: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}
