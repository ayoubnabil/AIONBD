use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Path, State};
use axum::Json;
use tokio::task;

use crate::auth::TenantContext;
use crate::errors::{map_json_rejection, ApiError};
use crate::models::{
    Metric, SearchFilter, SearchMode, SearchRequest, SearchResponse, SearchTopKRequest,
    SearchTopKResponse, DEFAULT_TOPK_LIMIT,
};
use crate::state::{AppState, CollectionHandle};
use crate::write_path::load_tenant_collection_handle;

mod engine;
mod filter;
mod scoring;

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
    let max_topk_limit = state.config.max_topk_limit;
    let requested_limit = payload.limit.unwrap_or(DEFAULT_TOPK_LIMIT);

    if requested_limit == 0 {
        return Err(ApiError::invalid_argument("limit must be > 0"));
    }
    if payload.limit.is_some_and(|limit| limit > max_topk_limit) {
        return Err(ApiError::invalid_argument(format!(
            "limit must be <= {max_topk_limit}"
        )));
    }
    let limit = requested_limit.min(max_topk_limit);

    let (name, handle) = load_tenant_collection_handle(state.clone(), name, tenant.clone()).await?;
    let selected = run_search(
        state.clone(),
        name,
        handle,
        OwnedSearchPlan {
            query: payload.query,
            metric,
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

#[derive(Debug)]
struct OwnedSearchPlan {
    query: Vec<f32>,
    metric: Metric,
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
