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
}

#[derive(Debug, Serialize)]
pub(crate) struct UpsertPointResponse {
    pub(crate) id: u64,
    pub(crate) created: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct PointResponse {
    pub(crate) id: u64,
    pub(crate) values: Vec<f32>,
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
pub(crate) struct SearchRequest {
    pub(crate) query: Vec<f32>,
    #[serde(default)]
    pub(crate) metric: Metric,
}

#[derive(Debug, Serialize)]
pub(crate) struct SearchResponse {
    pub(crate) id: u64,
    pub(crate) metric: Metric,
    pub(crate) value: f32,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SearchTopKRequest {
    pub(crate) query: Vec<f32>,
    #[serde(default)]
    pub(crate) metric: Metric,
    #[serde(default = "default_limit")]
    pub(crate) limit: usize,
}

#[derive(Debug, Serialize)]
pub(crate) struct SearchHit {
    pub(crate) id: u64,
    pub(crate) value: f32,
}

#[derive(Debug, Serialize)]
pub(crate) struct SearchTopKResponse {
    pub(crate) metric: Metric,
    pub(crate) hits: Vec<SearchHit>,
}

const fn default_true() -> bool {
    true
}

const fn default_limit() -> usize {
    10
}
