use aionbd_core::{
    cosine_similarity_with_options, dot_product_with_options, l2_distance_with_options, Collection,
    VectorValidationOptions,
};
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::Json;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::errors::{map_json_rejection, map_vector_error, ApiError};
use crate::handler_utils::canonical_collection_name;
use crate::ivf_index::IvfIndex;
use crate::models::{
    Metric, SearchHit, SearchRequest, SearchResponse, SearchTopKRequest, SearchTopKResponse,
};
use crate::state::AppState;

pub(crate) async fn search_collection(
    Path(name): Path<String>,
    State(state): State<AppState>,
    payload: Result<Json<SearchRequest>, JsonRejection>,
) -> Result<Json<SearchResponse>, ApiError> {
    let name = canonical_collection_name(&name)?;
    let Json(payload) = payload.map_err(map_json_rejection)?;

    let collections = state
        .collections
        .read()
        .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;

    let collection = collections
        .get(&name)
        .ok_or_else(|| ApiError::not_found(format!("collection '{name}' not found")))?;

    let best = select_top_k(&state, &name, collection, &payload.query, payload.metric, 1)?
        .into_iter()
        .next()
        .ok_or_else(|| ApiError::invalid_argument("collection contains no points"))?;

    Ok(Json(SearchResponse {
        id: best.id,
        metric: payload.metric,
        value: best.value,
    }))
}

pub(crate) async fn search_collection_top_k(
    Path(name): Path<String>,
    State(state): State<AppState>,
    payload: Result<Json<SearchTopKRequest>, JsonRejection>,
) -> Result<Json<SearchTopKResponse>, ApiError> {
    let name = canonical_collection_name(&name)?;
    let Json(payload) = payload.map_err(map_json_rejection)?;

    if payload.limit == 0 {
        return Err(ApiError::invalid_argument("limit must be > 0"));
    }

    let collections = state
        .collections
        .read()
        .map_err(|_| ApiError::internal("collection registry lock poisoned"))?;

    let collection = collections
        .get(&name)
        .ok_or_else(|| ApiError::not_found(format!("collection '{name}' not found")))?;

    let hits = select_top_k(
        &state,
        &name,
        collection,
        &payload.query,
        payload.metric,
        payload.limit,
    )?;

    Ok(Json(SearchTopKResponse {
        metric: payload.metric,
        hits,
    }))
}

fn select_top_k(
    state: &AppState,
    collection_name: &str,
    collection: &Collection,
    query: &[f32],
    metric: Metric,
    limit: usize,
) -> Result<Vec<SearchHit>, ApiError> {
    if collection.is_empty() {
        return Err(ApiError::invalid_argument("collection contains no points"));
    }
    if query.len() != collection.dimension() {
        return Err(ApiError::invalid_argument(format!(
            "query dimension {} does not match collection dimension {}",
            query.len(),
            collection.dimension()
        )));
    }
    if limit == 0 {
        return Ok(Vec::new());
    }

    let options = VectorValidationOptions {
        strict_finite: collection.strict_finite(),
        zero_norm_epsilon: f32::EPSILON,
    };

    let keep = limit.min(collection.len());
    let mut heap = BinaryHeap::with_capacity(keep);
    let candidate_ids =
        select_candidate_ids(state, collection_name, collection, query, metric, keep);

    if let Some(candidate_ids) = candidate_ids.filter(|ids| ids.len() >= keep) {
        for id in candidate_ids {
            let values = collection
                .get_point(id)
                .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;
            score_candidate(
                &mut heap,
                metric,
                keep,
                id,
                score_point(metric, query, values, options)?,
            );
        }
    } else {
        for (id, values) in collection.iter_points() {
            score_candidate(
                &mut heap,
                metric,
                keep,
                id,
                score_point(metric, query, values, options)?,
            );
        }
    }

    let mut scored: Vec<(u64, f32)> = heap
        .into_iter()
        .map(|candidate| (candidate.id, candidate.value))
        .collect();
    sort_scores(&mut scored, metric);

    Ok(scored
        .into_iter()
        .map(|(id, value)| SearchHit { id, value })
        .collect())
}

fn score_candidate(
    heap: &mut BinaryHeap<HeapCandidate>,
    metric: Metric,
    keep: usize,
    id: u64,
    value: f32,
) {
    let candidate = HeapCandidate { id, value, metric };
    if heap.len() < keep {
        heap.push(candidate);
        return;
    }

    let should_replace = heap.peek().is_some_and(|worst| {
        compare_scores(
            &(candidate.id, candidate.value),
            &(worst.id, worst.value),
            metric,
        )
        .is_lt()
    });
    if should_replace {
        let _ = heap.pop();
        heap.push(candidate);
    }
}

fn select_candidate_ids(
    state: &AppState,
    collection_name: &str,
    collection: &Collection,
    query: &[f32],
    metric: Metric,
    limit: usize,
) -> Option<Vec<u64>> {
    if !matches!(metric, Metric::L2) || collection.len() < IvfIndex::min_indexed_points() {
        return None;
    }

    if let Ok(index_cache) = state.l2_indexes.read() {
        if let Some(index) = index_cache.get(collection_name) {
            if index.is_compatible(collection) {
                return Some(index.candidate_ids(query, limit));
            }
        }
    }

    let index = IvfIndex::build(collection)?;
    let candidate_ids = index.candidate_ids(query, limit);
    if let Ok(mut index_cache) = state.l2_indexes.write() {
        index_cache.insert(collection_name.to_string(), index);
    }
    Some(candidate_ids)
}

fn score_point(
    metric: Metric,
    query: &[f32],
    values: &[f32],
    options: VectorValidationOptions,
) -> Result<f32, ApiError> {
    match metric {
        Metric::Dot => dot_product_with_options(query, values, options),
        Metric::L2 => l2_distance_with_options(query, values, options),
        Metric::Cosine => cosine_similarity_with_options(query, values, options),
    }
    .map_err(map_vector_error)
}

fn sort_scores(scored: &mut [(u64, f32)], metric: Metric) {
    scored.sort_by(|left, right| match metric {
        Metric::L2 => left
            .1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0)),
        Metric::Dot | Metric::Cosine => right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0)),
    });
}

#[derive(Debug, Clone, Copy)]
struct HeapCandidate {
    id: u64,
    value: f32,
    metric: Metric,
}

impl PartialEq for HeapCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.value.to_bits() == other.value.to_bits()
    }
}

impl Eq for HeapCandidate {}

impl PartialOrd for HeapCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_scores(
            &(self.id, self.value),
            &(other.id, other.value),
            self.metric,
        )
    }
}

fn compare_scores(left: &(u64, f32), right: &(u64, f32), metric: Metric) -> std::cmp::Ordering {
    match metric {
        Metric::L2 => left
            .1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0)),
        Metric::Dot | Metric::Cosine => right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0)),
    }
}
