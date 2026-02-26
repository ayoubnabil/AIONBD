use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Path, State};
use axum::Json;
use std::sync::atomic::Ordering;
use std::sync::OnceLock;
use tokio::task;

use crate::auth::TenantContext;
use crate::errors::{map_json_rejection, ApiError};
use crate::models::{
    Metric, SearchFilter, SearchMode, SearchRequest, SearchResponse, SearchTopKBatchItem,
    SearchTopKBatchRequest, SearchTopKBatchResponse, SearchTopKRequest, SearchTopKResponse,
    DEFAULT_TOPK_LIMIT,
};
use crate::state::{AppState, CollectionHandle};
use crate::write_path::load_tenant_collection_handle;

mod engine;
mod engine_batch;
mod filter;
mod scoring;

const SEARCH_INLINE_MAX_POINTS: usize = 8_192;
const SEARCH_INLINE_MAX_WORK: usize = 1_000_000;
const SEARCH_INLINE_LIGHT_LOAD_MAX_WORK: usize = 20_000_000;
const SEARCH_INLINE_LIGHT_LOAD_MAX_IN_FLIGHT: usize = 1;
const SEARCH_BATCH_MAX_QUERIES: usize = 256;
static SEARCH_INLINE_MAX_POINTS_CACHE: OnceLock<usize> = OnceLock::new();
static SEARCH_INLINE_MAX_WORK_CACHE: OnceLock<usize> = OnceLock::new();
static SEARCH_INLINE_LIGHT_LOAD_MAX_WORK_CACHE: OnceLock<usize> = OnceLock::new();
static SEARCH_INLINE_LIGHT_LOAD_MAX_IN_FLIGHT_CACHE: OnceLock<usize> = OnceLock::new();
static SEARCH_BATCH_MAX_QUERIES_CACHE: OnceLock<usize> = OnceLock::new();

fn search_inline_max_points() -> usize {
    *SEARCH_INLINE_MAX_POINTS_CACHE.get_or_init(|| {
        std::env::var("AIONBD_SEARCH_INLINE_MAX_POINTS")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .unwrap_or(SEARCH_INLINE_MAX_POINTS)
    })
}

fn search_inline_max_work() -> usize {
    *SEARCH_INLINE_MAX_WORK_CACHE.get_or_init(|| {
        std::env::var("AIONBD_SEARCH_INLINE_MAX_WORK")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .unwrap_or(SEARCH_INLINE_MAX_WORK)
    })
}

fn search_inline_light_load_max_work() -> usize {
    *SEARCH_INLINE_LIGHT_LOAD_MAX_WORK_CACHE.get_or_init(|| {
        std::env::var("AIONBD_SEARCH_INLINE_LIGHT_LOAD_MAX_WORK")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .unwrap_or(SEARCH_INLINE_LIGHT_LOAD_MAX_WORK)
    })
}

fn search_inline_light_load_max_in_flight() -> usize {
    *SEARCH_INLINE_LIGHT_LOAD_MAX_IN_FLIGHT_CACHE.get_or_init(|| {
        std::env::var("AIONBD_SEARCH_INLINE_LIGHT_LOAD_MAX_IN_FLIGHT")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .unwrap_or(SEARCH_INLINE_LIGHT_LOAD_MAX_IN_FLIGHT)
    })
}

fn search_batch_max_queries() -> usize {
    *SEARCH_BATCH_MAX_QUERIES_CACHE.get_or_init(|| {
        std::env::var("AIONBD_SEARCH_BATCH_MAX_QUERIES")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(SEARCH_BATCH_MAX_QUERIES)
    })
}

fn should_inline_search(points: usize, dimension: usize, query_count: usize) -> bool {
    let inline_max_points = search_inline_max_points();
    if inline_max_points == 0 || points > inline_max_points {
        return false;
    }
    let inline_max_work = search_inline_max_work();
    if inline_max_work == 0 {
        return true;
    }
    let work = points
        .saturating_mul(dimension)
        .saturating_mul(query_count.max(1));
    work <= inline_max_work
}

fn should_inline_search_with_load(
    state: &AppState,
    points: usize,
    dimension: usize,
    query_count: usize,
) -> bool {
    let in_flight = state
        .metrics
        .http_requests_in_flight
        .load(Ordering::Relaxed);
    should_inline_search_under_light_load(
        points,
        dimension,
        query_count,
        in_flight,
        search_inline_light_load_max_in_flight(),
        search_inline_light_load_max_work(),
    )
}

fn should_inline_search_under_light_load(
    points: usize,
    dimension: usize,
    query_count: usize,
    in_flight: u64,
    max_in_flight: usize,
    max_work: usize,
) -> bool {
    if should_inline_search(points, dimension, query_count) {
        return true;
    }
    if max_work == 0 || max_in_flight == 0 {
        return false;
    }
    let work = points
        .saturating_mul(dimension)
        .saturating_mul(query_count.max(1));
    if work > max_work {
        return false;
    }

    in_flight <= max_in_flight as u64
}

fn normalized_topk_limit(
    requested_limit: Option<usize>,
    max_topk_limit: usize,
) -> Result<usize, ApiError> {
    let requested = requested_limit.unwrap_or(DEFAULT_TOPK_LIMIT);
    if requested == 0 {
        return Err(ApiError::invalid_argument("limit must be > 0"));
    }
    if requested_limit.is_some_and(|explicit| explicit > max_topk_limit) {
        return Err(ApiError::invalid_argument(format!(
            "limit must be <= {max_topk_limit}"
        )));
    }
    Ok(requested.min(max_topk_limit))
}

pub(crate) async fn search_collection(
    Path(name): Path<String>,
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    payload: Result<Json<SearchRequest>, JsonRejection>,
) -> Result<Json<SearchResponse>, ApiError> {
    let Json(payload) = payload.map_err(map_json_rejection)?;
    let metric = payload.metric;
    let (name, handle) = load_tenant_collection_handle(state.clone(), name, tenant.clone()).await?;
    let selected = run_search(
        state.clone(),
        name,
        handle,
        OwnedSearchPlan {
            query: payload.query,
            metric,
            include_payload: payload.include_payload,
            limit: 1,
            mode: payload.mode,
            target_recall: payload.target_recall,
            filter: payload.filter,
        },
    )
    .await?;

    let best = selected
        .hits
        .into_iter()
        .next()
        .ok_or_else(|| ApiError::invalid_argument("collection contains no points"))?;

    Ok(Json(SearchResponse {
        id: best.id,
        metric,
        value: best.value,
        mode: selected.mode,
        recall_at_k: selected.recall_at_k,
        payload: best.payload,
    }))
}

pub(crate) async fn search_collection_top_k(
    Path(name): Path<String>,
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    payload: Result<Json<SearchTopKRequest>, JsonRejection>,
) -> Result<Json<SearchTopKResponse>, ApiError> {
    let Json(payload) = payload.map_err(map_json_rejection)?;
    let metric = payload.metric;
    let limit = normalized_topk_limit(payload.limit, state.config.max_topk_limit)?;

    let (name, handle) = load_tenant_collection_handle(state.clone(), name, tenant.clone()).await?;
    let selected = run_search(
        state.clone(),
        name,
        handle,
        OwnedSearchPlan {
            query: payload.query,
            metric,
            include_payload: payload.include_payload,
            limit,
            mode: payload.mode,
            target_recall: payload.target_recall,
            filter: payload.filter,
        },
    )
    .await?;

    Ok(Json(SearchTopKResponse {
        metric,
        mode: selected.mode,
        recall_at_k: selected.recall_at_k,
        hits: selected.hits,
    }))
}

pub(crate) async fn search_collection_top_k_batch(
    Path(name): Path<String>,
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    payload: Result<Json<SearchTopKBatchRequest>, JsonRejection>,
) -> Result<Json<SearchTopKBatchResponse>, ApiError> {
    let Json(payload) = payload.map_err(map_json_rejection)?;
    let metric = payload.metric;
    let limit = normalized_topk_limit(payload.limit, state.config.max_topk_limit)?;
    if payload.queries.is_empty() {
        return Err(ApiError::invalid_argument("queries must not be empty"));
    }
    let batch_max_queries = search_batch_max_queries();
    if payload.queries.len() > batch_max_queries {
        return Err(ApiError::invalid_argument(format!(
            "queries length must be <= {batch_max_queries}"
        )));
    }

    let (name, handle) = load_tenant_collection_handle(state.clone(), name, tenant.clone()).await?;
    let selected = run_search_batch(
        state.clone(),
        name,
        handle,
        OwnedSearchBatchPlan {
            queries: payload.queries,
            metric,
            include_payload: payload.include_payload,
            limit,
            mode: payload.mode,
            target_recall: payload.target_recall,
            filter: payload.filter,
        },
    )
    .await?;

    Ok(Json(SearchTopKBatchResponse {
        metric,
        results: selected
            .into_iter()
            .map(|result| SearchTopKBatchItem {
                mode: result.mode,
                recall_at_k: result.recall_at_k,
                hits: result.hits,
            })
            .collect(),
    }))
}

#[derive(Debug)]
struct OwnedSearchPlan {
    query: Vec<f32>,
    metric: Metric,
    include_payload: bool,
    limit: usize,
    mode: SearchMode,
    target_recall: Option<f32>,
    filter: Option<SearchFilter>,
}

#[derive(Debug)]
struct OwnedSearchBatchPlan {
    queries: Vec<Vec<f32>>,
    metric: Metric,
    include_payload: bool,
    limit: usize,
    mode: SearchMode,
    target_recall: Option<f32>,
    filter: Option<SearchFilter>,
}

async fn run_search(
    state: AppState,
    collection_name: String,
    handle: CollectionHandle,
    plan: OwnedSearchPlan,
) -> Result<engine::SearchSelection, ApiError> {
    if let Ok(collection) = handle.try_read() {
        if should_inline_search_with_load(&state, collection.len(), collection.dimension(), 1) {
            return engine::select_top_k(
                &state,
                &collection_name,
                &collection,
                engine::SearchPlan {
                    query: &plan.query,
                    metric: plan.metric,
                    include_payload: plan.include_payload,
                    limit: plan.limit,
                    mode: plan.mode,
                    target_recall: plan.target_recall,
                    filter: plan.filter.as_ref(),
                },
            );
        }
    }

    task::spawn_blocking(move || {
        let collection = handle
            .read()
            .map_err(|_| ApiError::internal("collection lock poisoned"))?;
        engine::select_top_k(
            &state,
            &collection_name,
            &collection,
            engine::SearchPlan {
                query: &plan.query,
                metric: plan.metric,
                include_payload: plan.include_payload,
                limit: plan.limit,
                mode: plan.mode,
                target_recall: plan.target_recall,
                filter: plan.filter.as_ref(),
            },
        )
    })
    .await
    .map_err(|_| ApiError::internal("search worker task failed"))?
}

async fn run_search_batch(
    state: AppState,
    collection_name: String,
    handle: CollectionHandle,
    plan: OwnedSearchBatchPlan,
) -> Result<Vec<engine::SearchSelection>, ApiError> {
    if let Ok(collection) = handle.try_read() {
        if should_inline_search_with_load(
            &state,
            collection.len(),
            collection.dimension(),
            plan.queries.len(),
        ) {
            return engine_batch::select_top_k_batch(
                &state,
                &collection_name,
                &collection,
                engine_batch::SearchBatchPlan {
                    queries: &plan.queries,
                    metric: plan.metric,
                    include_payload: plan.include_payload,
                    limit: plan.limit,
                    mode: plan.mode,
                    target_recall: plan.target_recall,
                    filter: plan.filter.as_ref(),
                },
            );
        }
    }

    task::spawn_blocking(move || {
        let collection = handle
            .read()
            .map_err(|_| ApiError::internal("collection lock poisoned"))?;
        engine_batch::select_top_k_batch(
            &state,
            &collection_name,
            &collection,
            engine_batch::SearchBatchPlan {
                queries: &plan.queries,
                metric: plan.metric,
                include_payload: plan.include_payload,
                limit: plan.limit,
                mode: plan.mode,
                target_recall: plan.target_recall,
                filter: plan.filter.as_ref(),
            },
        )
    })
    .await
    .map_err(|_| ApiError::internal("search worker task failed"))?
}

#[cfg(test)]
mod tests {
    use super::{should_inline_search, should_inline_search_under_light_load};

    #[test]
    fn inline_search_rejects_collections_over_point_threshold() {
        assert!(!should_inline_search(8_193, 128, 1));
    }

    #[test]
    fn inline_search_rejects_work_over_threshold() {
        assert!(!should_inline_search(4_096, 512, 1));
    }

    #[test]
    fn inline_search_accepts_small_workload() {
        assert!(should_inline_search(1_024, 128, 1));
    }

    #[test]
    fn inline_search_light_load_accepts_medium_work_when_in_flight_is_low() {
        assert!(should_inline_search_under_light_load(
            5_000, 784, 1, 1, 2, 4_000_000,
        ));
    }

    #[test]
    fn inline_search_light_load_rejects_medium_work_when_in_flight_is_high() {
        assert!(!should_inline_search_under_light_load(
            5_000, 784, 1, 6, 2, 4_000_000,
        ));
    }
}
