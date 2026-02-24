use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Metric {
    #[default]
    Dot,
    L2,
    Cosine,
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
