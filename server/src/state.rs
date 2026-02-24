use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Instant;

use aionbd_core::Collection;

use crate::config::AppConfig;

pub(crate) struct AppState {
    pub(crate) started_at: Instant,
    pub(crate) config: Arc<AppConfig>,
    pub(crate) engine_loaded: Arc<AtomicBool>,
    pub(crate) storage_available: Arc<AtomicBool>,
    pub(crate) collections: Arc<RwLock<BTreeMap<String, Collection>>>,
}

impl Clone for AppState {
    fn clone(&self) -> Self {
        Self {
            started_at: self.started_at,
            config: Arc::clone(&self.config),
            engine_loaded: Arc::clone(&self.engine_loaded),
            storage_available: Arc::clone(&self.storage_available),
            collections: Arc::clone(&self.collections),
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
            collections: Arc::new(RwLock::new(collections)),
        }
    }
}
