use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Instant;

use crate::config::AppConfig;

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) started_at: Instant,
    pub(crate) config: Arc<AppConfig>,
    pub(crate) engine_loaded: Arc<AtomicBool>,
    pub(crate) storage_available: Arc<AtomicBool>,
}

impl AppState {
    pub(crate) fn new(config: AppConfig) -> Self {
        Self {
            started_at: Instant::now(),
            config: Arc::new(config),
            engine_loaded: Arc::new(AtomicBool::new(true)),
            storage_available: Arc::new(AtomicBool::new(true)),
        }
    }
}
