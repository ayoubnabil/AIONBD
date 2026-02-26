use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::{Arc, OnceLock};

use aionbd_core::{
    Collection, PreparedCosineQuery, PreparedDotQuery, PreparedL2Query, VectorError,
    VectorValidationOptions,
};
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

use crate::errors::{map_vector_error, ApiError};
use crate::ivf_index::IvfIndex;
use crate::models::{Metric, PointPayload, SearchFilter, SearchHit};

use super::filter::matches_filter_strict;

const PARALLEL_SCORE_MIN_POINTS: usize = 256;
const PARALLEL_CANDIDATE_IDS_MIN_LEN: usize = 256;
const PARALLEL_SCORE_MIN_WORK: usize = 200_000;
const PARALLEL_TOP1_MIN_POINTS: usize = 8_192;
const PARALLEL_TOP1_MIN_WORK: usize = 4_000_000;
const PARALLEL_CANDIDATE_MIN_WORK: usize = 200_000;
const PARALLEL_SCORE_MIN_CHUNK_LEN: usize = 32;
const SMALL_TOPK_LINEAR_LIMIT: usize = 64;
static PARALLEL_SCORE_MIN_POINTS_CACHE: OnceLock<usize> = OnceLock::new();
static PARALLEL_CANDIDATE_IDS_MIN_LEN_CACHE: OnceLock<usize> = OnceLock::new();
static PARALLEL_SCORE_MIN_WORK_CACHE: OnceLock<usize> = OnceLock::new();
static PARALLEL_TOP1_MIN_POINTS_CACHE: OnceLock<usize> = OnceLock::new();
static PARALLEL_TOP1_MIN_WORK_CACHE: OnceLock<usize> = OnceLock::new();
static PARALLEL_CANDIDATE_MIN_WORK_CACHE: OnceLock<usize> = OnceLock::new();
static PARALLEL_SCORE_MIN_CHUNK_LEN_CACHE: OnceLock<usize> = OnceLock::new();

fn parallel_score_min_points() -> usize {
    *PARALLEL_SCORE_MIN_POINTS_CACHE.get_or_init(|| {
        std::env::var("AIONBD_PARALLEL_SCORE_MIN_POINTS")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .unwrap_or(PARALLEL_SCORE_MIN_POINTS)
    })
}

fn parallel_candidate_ids_min_len() -> usize {
    *PARALLEL_CANDIDATE_IDS_MIN_LEN_CACHE.get_or_init(|| {
        std::env::var("AIONBD_PARALLEL_CANDIDATE_IDS_MIN_LEN")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .unwrap_or(PARALLEL_CANDIDATE_IDS_MIN_LEN)
    })
}

fn parallel_score_min_chunk_len() -> usize {
    *PARALLEL_SCORE_MIN_CHUNK_LEN_CACHE.get_or_init(|| {
        std::env::var("AIONBD_PARALLEL_SCORE_MIN_CHUNK_LEN")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(PARALLEL_SCORE_MIN_CHUNK_LEN)
    })
}

fn parallel_score_min_work() -> usize {
    *PARALLEL_SCORE_MIN_WORK_CACHE.get_or_init(|| {
        std::env::var("AIONBD_PARALLEL_SCORE_MIN_WORK")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .unwrap_or(PARALLEL_SCORE_MIN_WORK)
    })
}

fn parallel_top1_min_points() -> usize {
    *PARALLEL_TOP1_MIN_POINTS_CACHE.get_or_init(|| {
        std::env::var("AIONBD_PARALLEL_TOP1_MIN_POINTS")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .unwrap_or(PARALLEL_TOP1_MIN_POINTS)
    })
}

fn parallel_top1_min_work() -> usize {
    *PARALLEL_TOP1_MIN_WORK_CACHE.get_or_init(|| {
        std::env::var("AIONBD_PARALLEL_TOP1_MIN_WORK")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .unwrap_or(PARALLEL_TOP1_MIN_WORK)
    })
}

fn parallel_candidate_min_work() -> usize {
    *PARALLEL_CANDIDATE_MIN_WORK_CACHE.get_or_init(|| {
        std::env::var("AIONBD_PARALLEL_CANDIDATE_MIN_WORK")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .unwrap_or(PARALLEL_CANDIDATE_MIN_WORK)
    })
}

fn estimated_work(dimension: usize, count: usize) -> usize {
    dimension.saturating_mul(count)
}

fn should_parallel_all_for(dimension: usize, count: usize, keep: usize) -> bool {
    let work = estimated_work(dimension, count);
    if keep <= 1 {
        count >= parallel_top1_min_points() || work >= parallel_top1_min_work()
    } else {
        count >= parallel_score_min_points() || work >= parallel_score_min_work()
    }
}

fn should_parallel_all(collection: &Collection, keep: usize) -> bool {
    should_parallel_all_for(collection.dimension(), collection.len(), keep)
}

fn should_parallel_candidates(dimension: usize, candidate_len: usize) -> bool {
    candidate_len >= parallel_candidate_ids_min_len()
        || estimated_work(dimension, candidate_len) >= parallel_candidate_min_work()
}

pub(crate) enum ScoreSource {
    All,
    #[allow(dead_code)]
    CandidateIds(Vec<u64>),
    #[allow(dead_code)]
    CandidateSlots(Vec<usize>),
    IvfCentroids {
        index: Arc<IvfIndex>,
        centroids: Vec<usize>,
        candidate_count: usize,
    },
}

enum PreparedNonL2Query {
    Dot(PreparedDotQuery),
    Cosine(PreparedCosineQuery),
}

#[allow(clippy::collapsible_else_if, clippy::too_many_arguments)]
pub(crate) fn score_points(
    collection: &Collection,
    query: &[f32],
    metric: Metric,
    include_payload: bool,
    keep: usize,
    options: VectorValidationOptions,
    filter: Option<&SearchFilter>,
    source: ScoreSource,
) -> Result<Vec<SearchHit>, ApiError> {
    validate_query(query)?;
    if matches!(metric, Metric::L2) {
        return score_points_l2(collection, query, include_payload, keep, filter, source);
    }
    let prepared_query = match metric {
        Metric::Dot => PreparedNonL2Query::Dot(PreparedDotQuery::new(query)),
        Metric::Cosine => {
            let prepared = PreparedCosineQuery::new(query);
            if prepared.query_sq_sum() <= options.zero_norm_epsilon.max(0.0) {
                return Err(map_vector_error(VectorError::ZeroNorm {
                    epsilon: options.zero_norm_epsilon,
                }));
            }
            PreparedNonL2Query::Cosine(prepared)
        }
        Metric::L2 => unreachable!("L2 path is handled separately"),
    };

    let score_options = VectorValidationOptions {
        strict_finite: false,
        zero_norm_epsilon: options.zero_norm_epsilon,
    };
    let include_payload = include_payload && collection.has_payload_points();
    let use_parallel_all =
        matches!(source, ScoreSource::All) && should_parallel_all(collection, keep);
    let use_small_topk_fast_path =
        matches!(source, ScoreSource::All) && filter.is_none() && keep <= SMALL_TOPK_LINEAR_LIMIT;
    if use_small_topk_fast_path {
        if use_parallel_all {
            let hits = score_all_non_l2_small_topk_parallel(
                collection,
                &prepared_query,
                metric,
                keep,
                score_options,
            )?;
            return attach_payloads_to_small_topk_hits(collection, hits, include_payload);
        }
        let hits =
            score_all_non_l2_small_topk(collection, &prepared_query, metric, keep, score_options)?;
        return attach_payloads_to_small_topk_hits(collection, hits, include_payload);
    }
    if matches!(source, ScoreSource::All) && keep == 1 && !use_parallel_all {
        return score_all_non_l2_top1(
            collection,
            &prepared_query,
            metric,
            include_payload,
            score_options,
            filter,
        );
    }
    let mut heap = if use_parallel_all {
        score_all_parallel(
            collection,
            &prepared_query,
            metric,
            keep,
            score_options,
            filter,
        )?
    } else {
        BinaryHeap::with_capacity(keep)
    };

    match source {
        ScoreSource::All => {
            if !use_parallel_all {
                if let Some(active_filter) = filter {
                    for (id, values, payload) in collection.iter_points_with_payload_unordered() {
                        if !matches_filter_strict(payload, active_filter) {
                            continue;
                        }
                        if let Some(score) =
                            score_point_for_candidate(&prepared_query, values, score_options)
                        {
                            score_candidate(
                                &mut heap,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: rank_key(metric, score),
                                },
                                keep,
                            );
                        }
                    }
                } else {
                    for (id, values) in collection.iter_points_unordered() {
                        if let Some(score) =
                            score_point_for_candidate(&prepared_query, values, score_options)
                        {
                            score_candidate(
                                &mut heap,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: rank_key(metric, score),
                                },
                                keep,
                            );
                        }
                    }
                }
            }
        }
        ScoreSource::CandidateIds(candidate_ids) => {
            let use_small_topk_candidate_ids_fast_path =
                filter.is_none() && keep <= SMALL_TOPK_LINEAR_LIMIT;
            if use_small_topk_candidate_ids_fast_path {
                if should_parallel_candidates(collection.dimension(), candidate_ids.len()) {
                    let hits = score_candidate_ids_non_l2_small_topk_parallel(
                        collection,
                        &prepared_query,
                        metric,
                        keep,
                        score_options,
                        &candidate_ids,
                    )?;
                    return attach_payloads_to_small_topk_hits(collection, hits, include_payload);
                }
                let hits = score_candidate_ids_non_l2_small_topk(
                    collection,
                    &prepared_query,
                    metric,
                    keep,
                    score_options,
                    &candidate_ids,
                )?;
                return attach_payloads_to_small_topk_hits(collection, hits, include_payload);
            } else if should_parallel_candidates(collection.dimension(), candidate_ids.len()) {
                heap = score_candidate_ids_parallel(
                    collection,
                    &prepared_query,
                    metric,
                    keep,
                    score_options,
                    filter,
                    &candidate_ids,
                )?;
            } else if let Some(active_filter) = filter {
                for id in candidate_ids {
                    let (values, payload) = collection
                        .get_point_record(id)
                        .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;
                    if !matches_filter_strict(payload, active_filter) {
                        continue;
                    }
                    if let Some(score) =
                        score_point_for_candidate(&prepared_query, values, score_options)
                    {
                        score_candidate(
                            &mut heap,
                            HeapCandidate {
                                id,
                                value: score,
                                rank_key: rank_key(metric, score),
                            },
                            keep,
                        );
                    }
                }
            } else {
                for id in candidate_ids {
                    let values = collection
                        .get_point(id)
                        .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;
                    if let Some(score) =
                        score_point_for_candidate(&prepared_query, values, score_options)
                    {
                        score_candidate(
                            &mut heap,
                            HeapCandidate {
                                id,
                                value: score,
                                rank_key: rank_key(metric, score),
                            },
                            keep,
                        );
                    }
                }
            }
        }
        ScoreSource::CandidateSlots(candidate_slots) => {
            let use_small_topk_candidate_slots_fast_path =
                filter.is_none() && keep <= SMALL_TOPK_LINEAR_LIMIT;
            let dense_slots = collection.slots_dense();

            if use_small_topk_candidate_slots_fast_path {
                if should_parallel_candidates(collection.dimension(), candidate_slots.len()) {
                    let hits = score_candidate_slots_non_l2_small_topk_parallel(
                        collection,
                        &prepared_query,
                        metric,
                        keep,
                        score_options,
                        &candidate_slots,
                    )?;
                    return attach_payloads_to_small_topk_hits(collection, hits, include_payload);
                }
                let hits = score_candidate_slots_non_l2_small_topk(
                    collection,
                    &prepared_query,
                    metric,
                    keep,
                    score_options,
                    &candidate_slots,
                )?;
                return attach_payloads_to_small_topk_hits(collection, hits, include_payload);
            } else if should_parallel_candidates(collection.dimension(), candidate_slots.len()) {
                heap = score_candidate_slots_parallel(
                    collection,
                    &prepared_query,
                    metric,
                    keep,
                    score_options,
                    filter,
                    &candidate_slots,
                )?;
            } else if let Some(active_filter) = filter {
                if dense_slots {
                    for slot in candidate_slots {
                        let (id, values, payload) =
                            collection.point_with_payload_at_dense_slot(slot);
                        if !matches_filter_strict(payload, active_filter) {
                            continue;
                        }
                        if let Some(score) =
                            score_point_for_candidate(&prepared_query, values, score_options)
                        {
                            score_candidate(
                                &mut heap,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: rank_key(metric, score),
                                },
                                keep,
                            );
                        }
                    }
                } else {
                    for slot in candidate_slots {
                        let (id, values, payload) =
                            collection.point_with_payload_at_slot(slot).ok_or_else(|| {
                                ApiError::internal("point slot index is inconsistent")
                            })?;
                        if !matches_filter_strict(payload, active_filter) {
                            continue;
                        }
                        if let Some(score) =
                            score_point_for_candidate(&prepared_query, values, score_options)
                        {
                            score_candidate(
                                &mut heap,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: rank_key(metric, score),
                                },
                                keep,
                            );
                        }
                    }
                }
            } else {
                if dense_slots {
                    for slot in candidate_slots {
                        let (id, values) = collection.point_at_dense_slot(slot);
                        if let Some(score) =
                            score_point_for_candidate(&prepared_query, values, score_options)
                        {
                            score_candidate(
                                &mut heap,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: rank_key(metric, score),
                                },
                                keep,
                            );
                        }
                    }
                } else {
                    for slot in candidate_slots {
                        let (id, values) = collection.point_at_slot(slot).ok_or_else(|| {
                            ApiError::internal("point slot index is inconsistent")
                        })?;
                        if let Some(score) =
                            score_point_for_candidate(&prepared_query, values, score_options)
                        {
                            score_candidate(
                                &mut heap,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: rank_key(metric, score),
                                },
                                keep,
                            );
                        }
                    }
                }
            }
        }
        ScoreSource::IvfCentroids {
            index,
            centroids,
            candidate_count,
        } => {
            let use_small_topk_ivf_fast_path = filter.is_none() && keep <= SMALL_TOPK_LINEAR_LIMIT;
            if use_small_topk_ivf_fast_path {
                if should_parallel_candidates(collection.dimension(), candidate_count) {
                    let hits = score_ivf_centroids_non_l2_small_topk_parallel(
                        collection,
                        &index,
                        &centroids,
                        &prepared_query,
                        keep,
                        score_options,
                    )?;
                    return attach_payloads_to_small_topk_hits(collection, hits, include_payload);
                }
                let hits = score_ivf_centroids_non_l2_small_topk(
                    collection,
                    &index,
                    &centroids,
                    &prepared_query,
                    keep,
                    score_options,
                )?;
                return attach_payloads_to_small_topk_hits(collection, hits, include_payload);
            } else if should_parallel_candidates(collection.dimension(), candidate_count) {
                heap = score_ivf_centroids_parallel_non_l2(
                    collection,
                    &index,
                    &centroids,
                    &prepared_query,
                    metric,
                    keep,
                    score_options,
                    filter,
                )?;
            } else {
                score_ivf_centroids_into_heap_non_l2(
                    &mut heap,
                    collection,
                    &index,
                    &centroids,
                    &prepared_query,
                    metric,
                    keep,
                    score_options,
                    filter,
                )?;
            }
        }
    }

    heap.into_sorted_vec()
        .into_iter()
        .map(|candidate| {
            Ok(SearchHit {
                id: candidate.id,
                value: metric_value_for_response(metric, candidate.value),
                payload: if include_payload {
                    let payload = collection
                        .get_payload(candidate.id)
                        .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;
                    payload_for_response(payload)
                } else {
                    None
                },
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()
}

fn score_all_non_l2_small_topk(
    collection: &Collection,
    prepared: &PreparedNonL2Query,
    metric: Metric,
    keep: usize,
    score_options: VectorValidationOptions,
) -> Result<Vec<SearchHit>, ApiError> {
    debug_assert!(matches!(metric, Metric::Dot | Metric::Cosine));
    let mut topk = SmallTopKNonL2::new(keep);

    if collection.slots_dense() {
        for slot in 0..collection.slot_count() {
            let (id, values) = collection.point_at_dense_slot(slot);
            if let Some(score) = score_point_for_candidate(prepared, values, score_options) {
                topk.push_candidate(id, score);
            }
        }
    } else {
        for (id, values) in collection.iter_points_unordered() {
            if let Some(score) = score_point_for_candidate(prepared, values, score_options) {
                topk.push_candidate(id, score);
            }
        }
    }

    build_small_topk_hits_non_l2(&topk)
}

fn score_all_non_l2_small_topk_parallel(
    collection: &Collection,
    prepared: &PreparedNonL2Query,
    metric: Metric,
    keep: usize,
    score_options: VectorValidationOptions,
) -> Result<Vec<SearchHit>, ApiError> {
    debug_assert!(matches!(metric, Metric::Dot | Metric::Cosine));
    let merged = if collection.slots_dense() {
        (0..collection.slot_count())
            .into_par_iter()
            .with_min_len(parallel_score_min_chunk_len())
            .fold(
                || SmallTopKNonL2::new(keep),
                |mut local, slot| {
                    let (id, values) = collection.point_at_dense_slot(slot);
                    if let Some(score) = score_point_for_candidate(prepared, values, score_options)
                    {
                        local.push_candidate(id, score);
                    }
                    local
                },
            )
            .reduce(
                || SmallTopKNonL2::new(keep),
                |mut combined, local| {
                    combined.merge_from(&local);
                    combined
                },
            )
    } else {
        (0..collection.slot_count())
            .into_par_iter()
            .with_min_len(parallel_score_min_chunk_len())
            .fold(
                || SmallTopKNonL2::new(keep),
                |mut local, slot| {
                    if let Some((id, values)) = collection.point_at_slot(slot) {
                        if let Some(score) =
                            score_point_for_candidate(prepared, values, score_options)
                        {
                            local.push_candidate(id, score);
                        }
                    }
                    local
                },
            )
            .reduce(
                || SmallTopKNonL2::new(keep),
                |mut combined, local| {
                    combined.merge_from(&local);
                    combined
                },
            )
    };

    build_small_topk_hits_non_l2(&merged)
}

fn score_all_non_l2_top1(
    collection: &Collection,
    prepared: &PreparedNonL2Query,
    metric: Metric,
    include_payload: bool,
    score_options: VectorValidationOptions,
    filter: Option<&SearchFilter>,
) -> Result<Vec<SearchHit>, ApiError> {
    let mut best: Option<(u64, f32)> = None;
    if let Some(active_filter) = filter {
        for (id, values, payload) in collection.iter_points_with_payload_unordered() {
            if !matches_filter_strict(payload, active_filter) {
                continue;
            }
            if let Some(score) = score_point_for_candidate(prepared, values, score_options) {
                if best.as_ref().is_none_or(|(best_id, best_score)| {
                    compare_non_l2(score, id, *best_score, *best_id).is_lt()
                }) {
                    best = Some((id, score));
                }
            }
        }
    } else {
        for (id, values) in collection.iter_points_unordered() {
            if let Some(score) = score_point_for_candidate(prepared, values, score_options) {
                if best.as_ref().is_none_or(|(best_id, best_score)| {
                    compare_non_l2(score, id, *best_score, *best_id).is_lt()
                }) {
                    best = Some((id, score));
                }
            }
        }
    }

    if let Some((id, ranked_value)) = best {
        let payload = if include_payload {
            let payload = collection
                .get_payload(id)
                .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;
            payload_for_response(payload)
        } else {
            None
        };
        return Ok(vec![SearchHit {
            id,
            value: metric_value_for_response(metric, ranked_value),
            payload,
        }]);
    }

    Ok(Vec::new())
}

fn score_candidate_ids_non_l2_small_topk(
    collection: &Collection,
    prepared: &PreparedNonL2Query,
    metric: Metric,
    keep: usize,
    score_options: VectorValidationOptions,
    candidate_ids: &[u64],
) -> Result<Vec<SearchHit>, ApiError> {
    debug_assert!(matches!(metric, Metric::Dot | Metric::Cosine));
    let mut topk = SmallTopKNonL2::new(keep);

    for id in candidate_ids {
        let values = collection
            .get_point(*id)
            .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;
        if let Some(score) = score_point_for_candidate(prepared, values, score_options) {
            topk.push_candidate(*id, score);
        }
    }

    build_small_topk_hits_non_l2(&topk)
}

fn score_candidate_ids_non_l2_small_topk_parallel(
    collection: &Collection,
    prepared: &PreparedNonL2Query,
    metric: Metric,
    keep: usize,
    score_options: VectorValidationOptions,
    candidate_ids: &[u64],
) -> Result<Vec<SearchHit>, ApiError> {
    debug_assert!(matches!(metric, Metric::Dot | Metric::Cosine));
    let merged = candidate_ids
        .into_par_iter()
        .with_min_len(parallel_score_min_chunk_len())
        .try_fold(
            || SmallTopKNonL2::new(keep),
            |mut local, id| {
                let values = collection
                    .get_point(*id)
                    .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;
                if let Some(score) = score_point_for_candidate(prepared, values, score_options) {
                    local.push_candidate(*id, score);
                }
                Ok(local)
            },
        )
        .try_reduce(
            || SmallTopKNonL2::new(keep),
            |mut combined, local| {
                combined.merge_from(&local);
                Ok(combined)
            },
        )?;

    build_small_topk_hits_non_l2(&merged)
}

fn score_candidate_slots_non_l2_small_topk(
    collection: &Collection,
    prepared: &PreparedNonL2Query,
    metric: Metric,
    keep: usize,
    score_options: VectorValidationOptions,
    candidate_slots: &[usize],
) -> Result<Vec<SearchHit>, ApiError> {
    debug_assert!(matches!(metric, Metric::Dot | Metric::Cosine));
    let mut topk = SmallTopKNonL2::new(keep);

    if collection.slots_dense() {
        for slot in candidate_slots {
            let (id, values) = collection.point_at_dense_slot(*slot);
            if let Some(score) = score_point_for_candidate(prepared, values, score_options) {
                topk.push_candidate(id, score);
            }
        }
    } else {
        for slot in candidate_slots {
            let (id, values) = collection
                .point_at_slot(*slot)
                .ok_or_else(|| ApiError::internal("point slot index is inconsistent"))?;
            if let Some(score) = score_point_for_candidate(prepared, values, score_options) {
                topk.push_candidate(id, score);
            }
        }
    }

    build_small_topk_hits_non_l2(&topk)
}

fn score_candidate_slots_non_l2_small_topk_parallel(
    collection: &Collection,
    prepared: &PreparedNonL2Query,
    metric: Metric,
    keep: usize,
    score_options: VectorValidationOptions,
    candidate_slots: &[usize],
) -> Result<Vec<SearchHit>, ApiError> {
    debug_assert!(matches!(metric, Metric::Dot | Metric::Cosine));
    let merged = if collection.slots_dense() {
        candidate_slots
            .into_par_iter()
            .with_min_len(parallel_score_min_chunk_len())
            .fold(
                || SmallTopKNonL2::new(keep),
                |mut local, slot| {
                    let (id, values) = collection.point_at_dense_slot(*slot);
                    if let Some(score) = score_point_for_candidate(prepared, values, score_options)
                    {
                        local.push_candidate(id, score);
                    }
                    local
                },
            )
            .reduce(
                || SmallTopKNonL2::new(keep),
                |mut combined, local| {
                    combined.merge_from(&local);
                    combined
                },
            )
    } else {
        candidate_slots
            .into_par_iter()
            .with_min_len(parallel_score_min_chunk_len())
            .try_fold(
                || SmallTopKNonL2::new(keep),
                |mut local, slot| {
                    let (id, values) = collection
                        .point_at_slot(*slot)
                        .ok_or_else(|| ApiError::internal("point slot index is inconsistent"))?;
                    if let Some(score) = score_point_for_candidate(prepared, values, score_options)
                    {
                        local.push_candidate(id, score);
                    }
                    Ok(local)
                },
            )
            .try_reduce(
                || SmallTopKNonL2::new(keep),
                |mut combined, local| {
                    combined.merge_from(&local);
                    Ok(combined)
                },
            )?
    };

    build_small_topk_hits_non_l2(&merged)
}

fn score_ivf_centroids_non_l2_small_topk(
    collection: &Collection,
    index: &IvfIndex,
    centroids: &[usize],
    prepared: &PreparedNonL2Query,
    keep: usize,
    score_options: VectorValidationOptions,
) -> Result<Vec<SearchHit>, ApiError> {
    let mut topk = SmallTopKNonL2::new(keep);

    if collection.slots_dense() {
        for centroid_idx in centroids {
            let slots = index
                .slots_for_centroid(*centroid_idx)
                .ok_or_else(|| ApiError::internal("ivf centroid list index is inconsistent"))?;
            for slot in slots {
                let (id, values) = collection.point_at_dense_slot(*slot);
                if let Some(score) = score_point_for_candidate(prepared, values, score_options) {
                    topk.push_candidate(id, score);
                }
            }
        }
    } else {
        for centroid_idx in centroids {
            let slots = index
                .slots_for_centroid(*centroid_idx)
                .ok_or_else(|| ApiError::internal("ivf centroid list index is inconsistent"))?;
            for slot in slots {
                let (id, values) = collection
                    .point_at_slot(*slot)
                    .ok_or_else(|| ApiError::internal("point slot index is inconsistent"))?;
                if let Some(score) = score_point_for_candidate(prepared, values, score_options) {
                    topk.push_candidate(id, score);
                }
            }
        }
    }

    build_small_topk_hits_non_l2(&topk)
}

fn score_ivf_centroids_non_l2_small_topk_parallel(
    collection: &Collection,
    index: &IvfIndex,
    centroids: &[usize],
    prepared: &PreparedNonL2Query,
    keep: usize,
    score_options: VectorValidationOptions,
) -> Result<Vec<SearchHit>, ApiError> {
    let merged = if collection.slots_dense() {
        centroids
            .into_par_iter()
            .with_min_len(parallel_score_min_chunk_len())
            .try_fold(
                || SmallTopKNonL2::new(keep),
                |mut local, centroid_idx| {
                    let slots = index.slots_for_centroid(*centroid_idx).ok_or_else(|| {
                        ApiError::internal("ivf centroid list index is inconsistent")
                    })?;
                    for slot in slots {
                        let (id, values) = collection.point_at_dense_slot(*slot);
                        if let Some(score) =
                            score_point_for_candidate(prepared, values, score_options)
                        {
                            local.push_candidate(id, score);
                        }
                    }
                    Ok(local)
                },
            )
            .try_reduce(
                || SmallTopKNonL2::new(keep),
                |mut combined, local| {
                    combined.merge_from(&local);
                    Ok(combined)
                },
            )?
    } else {
        centroids
            .into_par_iter()
            .with_min_len(parallel_score_min_chunk_len())
            .try_fold(
                || SmallTopKNonL2::new(keep),
                |mut local, centroid_idx| {
                    let slots = index.slots_for_centroid(*centroid_idx).ok_or_else(|| {
                        ApiError::internal("ivf centroid list index is inconsistent")
                    })?;
                    for slot in slots {
                        let (id, values) = collection.point_at_slot(*slot).ok_or_else(|| {
                            ApiError::internal("point slot index is inconsistent")
                        })?;
                        if let Some(score) =
                            score_point_for_candidate(prepared, values, score_options)
                        {
                            local.push_candidate(id, score);
                        }
                    }
                    Ok(local)
                },
            )
            .try_reduce(
                || SmallTopKNonL2::new(keep),
                |mut combined, local| {
                    combined.merge_from(&local);
                    Ok(combined)
                },
            )?
    };

    build_small_topk_hits_non_l2(&merged)
}

fn score_candidate_ids_l2_small_topk(
    collection: &Collection,
    prepared: &PreparedL2Query,
    keep: usize,
    assume_finite: bool,
    candidate_ids: &[u64],
) -> Result<Vec<SearchHit>, ApiError> {
    let mut topk = SmallTopK::new(keep);

    for id in candidate_ids {
        let values = collection
            .get_point(*id)
            .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;
        if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite) {
            topk.push_candidate(*id, score);
        }
    }

    build_small_topk_hits(&topk)
}

fn score_candidate_ids_l2_small_topk_parallel(
    collection: &Collection,
    prepared: &PreparedL2Query,
    keep: usize,
    assume_finite: bool,
    candidate_ids: &[u64],
) -> Result<Vec<SearchHit>, ApiError> {
    let merged = candidate_ids
        .into_par_iter()
        .with_min_len(parallel_score_min_chunk_len())
        .try_fold(
            || SmallTopK::new(keep),
            |mut local, id| {
                let values = collection
                    .get_point(*id)
                    .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;
                if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite) {
                    local.push_candidate(*id, score);
                }
                Ok(local)
            },
        )
        .try_reduce(
            || SmallTopK::new(keep),
            |mut combined, local| {
                combined.merge_from(&local);
                Ok(combined)
            },
        )?;

    build_small_topk_hits(&merged)
}

fn score_candidate_slots_l2_small_topk(
    collection: &Collection,
    prepared: &PreparedL2Query,
    keep: usize,
    assume_finite: bool,
    candidate_slots: &[usize],
) -> Result<Vec<SearchHit>, ApiError> {
    let mut topk = SmallTopK::new(keep);

    if collection.slots_dense() {
        for slot in candidate_slots {
            let (id, values) = collection.point_at_dense_slot(*slot);
            if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite) {
                topk.push_candidate(id, score);
            }
        }
    } else {
        for slot in candidate_slots {
            let (id, values) = collection
                .point_at_slot(*slot)
                .ok_or_else(|| ApiError::internal("point slot index is inconsistent"))?;
            if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite) {
                topk.push_candidate(id, score);
            }
        }
    }

    build_small_topk_hits(&topk)
}

fn score_candidate_slots_l2_small_topk_parallel(
    collection: &Collection,
    prepared: &PreparedL2Query,
    keep: usize,
    assume_finite: bool,
    candidate_slots: &[usize],
) -> Result<Vec<SearchHit>, ApiError> {
    let merged = if collection.slots_dense() {
        candidate_slots
            .into_par_iter()
            .with_min_len(parallel_score_min_chunk_len())
            .fold(
                || SmallTopK::new(keep),
                |mut local, slot| {
                    let (id, values) = collection.point_at_dense_slot(*slot);
                    if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite) {
                        local.push_candidate(id, score);
                    }
                    local
                },
            )
            .reduce(
                || SmallTopK::new(keep),
                |mut combined, local| {
                    combined.merge_from(&local);
                    combined
                },
            )
    } else {
        candidate_slots
            .into_par_iter()
            .with_min_len(parallel_score_min_chunk_len())
            .try_fold(
                || SmallTopK::new(keep),
                |mut local, slot| {
                    let (id, values) = collection
                        .point_at_slot(*slot)
                        .ok_or_else(|| ApiError::internal("point slot index is inconsistent"))?;
                    if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite) {
                        local.push_candidate(id, score);
                    }
                    Ok(local)
                },
            )
            .try_reduce(
                || SmallTopK::new(keep),
                |mut combined, local| {
                    combined.merge_from(&local);
                    Ok(combined)
                },
            )?
    };

    build_small_topk_hits(&merged)
}

fn score_ivf_centroids_l2_small_topk(
    collection: &Collection,
    index: &IvfIndex,
    centroids: &[usize],
    prepared: &PreparedL2Query,
    keep: usize,
    assume_finite: bool,
) -> Result<Vec<SearchHit>, ApiError> {
    let mut topk = SmallTopK::new(keep);

    if collection.slots_dense() {
        for centroid_idx in centroids {
            let slots = index
                .slots_for_centroid(*centroid_idx)
                .ok_or_else(|| ApiError::internal("ivf centroid list index is inconsistent"))?;
            for slot in slots {
                let (id, values) = collection.point_at_dense_slot(*slot);
                if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite) {
                    topk.push_candidate(id, score);
                }
            }
        }
    } else {
        for centroid_idx in centroids {
            let slots = index
                .slots_for_centroid(*centroid_idx)
                .ok_or_else(|| ApiError::internal("ivf centroid list index is inconsistent"))?;
            for slot in slots {
                let (id, values) = collection
                    .point_at_slot(*slot)
                    .ok_or_else(|| ApiError::internal("point slot index is inconsistent"))?;
                if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite) {
                    topk.push_candidate(id, score);
                }
            }
        }
    }

    build_small_topk_hits(&topk)
}

fn score_ivf_centroids_l2_small_topk_parallel(
    collection: &Collection,
    index: &IvfIndex,
    centroids: &[usize],
    prepared: &PreparedL2Query,
    keep: usize,
    assume_finite: bool,
) -> Result<Vec<SearchHit>, ApiError> {
    let merged = if collection.slots_dense() {
        centroids
            .into_par_iter()
            .with_min_len(parallel_score_min_chunk_len())
            .try_fold(
                || SmallTopK::new(keep),
                |mut local, centroid_idx| {
                    let slots = index.slots_for_centroid(*centroid_idx).ok_or_else(|| {
                        ApiError::internal("ivf centroid list index is inconsistent")
                    })?;
                    for slot in slots {
                        let (id, values) = collection.point_at_dense_slot(*slot);
                        if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite)
                        {
                            local.push_candidate(id, score);
                        }
                    }
                    Ok(local)
                },
            )
            .try_reduce(
                || SmallTopK::new(keep),
                |mut combined, local| {
                    combined.merge_from(&local);
                    Ok(combined)
                },
            )?
    } else {
        centroids
            .into_par_iter()
            .with_min_len(parallel_score_min_chunk_len())
            .try_fold(
                || SmallTopK::new(keep),
                |mut local, centroid_idx| {
                    let slots = index.slots_for_centroid(*centroid_idx).ok_or_else(|| {
                        ApiError::internal("ivf centroid list index is inconsistent")
                    })?;
                    for slot in slots {
                        let (id, values) = collection.point_at_slot(*slot).ok_or_else(|| {
                            ApiError::internal("point slot index is inconsistent")
                        })?;
                        if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite)
                        {
                            local.push_candidate(id, score);
                        }
                    }
                    Ok(local)
                },
            )
            .try_reduce(
                || SmallTopK::new(keep),
                |mut combined, local| {
                    combined.merge_from(&local);
                    Ok(combined)
                },
            )?
    };

    build_small_topk_hits(&merged)
}

#[allow(clippy::collapsible_else_if, clippy::too_many_arguments)]
fn score_ivf_centroids_into_heap_non_l2(
    heap: &mut BinaryHeap<HeapCandidate>,
    collection: &Collection,
    index: &IvfIndex,
    centroids: &[usize],
    prepared: &PreparedNonL2Query,
    metric: Metric,
    keep: usize,
    score_options: VectorValidationOptions,
    filter: Option<&SearchFilter>,
) -> Result<(), ApiError> {
    let dense_slots = collection.slots_dense();
    if let Some(active_filter) = filter {
        if dense_slots {
            for centroid_idx in centroids {
                let slots = index
                    .slots_for_centroid(*centroid_idx)
                    .ok_or_else(|| ApiError::internal("ivf centroid list index is inconsistent"))?;
                for slot in slots {
                    let (id, values, payload) = collection.point_with_payload_at_dense_slot(*slot);
                    if !matches_filter_strict(payload, active_filter) {
                        continue;
                    }
                    if let Some(score) = score_point_for_candidate(prepared, values, score_options)
                    {
                        score_candidate(
                            heap,
                            HeapCandidate {
                                id,
                                value: score,
                                rank_key: rank_key(metric, score),
                            },
                            keep,
                        );
                    }
                }
            }
        } else {
            for centroid_idx in centroids {
                let slots = index
                    .slots_for_centroid(*centroid_idx)
                    .ok_or_else(|| ApiError::internal("ivf centroid list index is inconsistent"))?;
                for slot in slots {
                    let (id, values, payload) = collection
                        .point_with_payload_at_slot(*slot)
                        .ok_or_else(|| ApiError::internal("point slot index is inconsistent"))?;
                    if !matches_filter_strict(payload, active_filter) {
                        continue;
                    }
                    if let Some(score) = score_point_for_candidate(prepared, values, score_options)
                    {
                        score_candidate(
                            heap,
                            HeapCandidate {
                                id,
                                value: score,
                                rank_key: rank_key(metric, score),
                            },
                            keep,
                        );
                    }
                }
            }
        }
    } else {
        if dense_slots {
            for centroid_idx in centroids {
                let slots = index
                    .slots_for_centroid(*centroid_idx)
                    .ok_or_else(|| ApiError::internal("ivf centroid list index is inconsistent"))?;
                for slot in slots {
                    let (id, values) = collection.point_at_dense_slot(*slot);
                    if let Some(score) = score_point_for_candidate(prepared, values, score_options)
                    {
                        score_candidate(
                            heap,
                            HeapCandidate {
                                id,
                                value: score,
                                rank_key: rank_key(metric, score),
                            },
                            keep,
                        );
                    }
                }
            }
        } else {
            for centroid_idx in centroids {
                let slots = index
                    .slots_for_centroid(*centroid_idx)
                    .ok_or_else(|| ApiError::internal("ivf centroid list index is inconsistent"))?;
                for slot in slots {
                    let (id, values) = collection
                        .point_at_slot(*slot)
                        .ok_or_else(|| ApiError::internal("point slot index is inconsistent"))?;
                    if let Some(score) = score_point_for_candidate(prepared, values, score_options)
                    {
                        score_candidate(
                            heap,
                            HeapCandidate {
                                id,
                                value: score,
                                rank_key: rank_key(metric, score),
                            },
                            keep,
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

#[allow(clippy::collapsible_else_if, clippy::too_many_arguments)]
fn score_ivf_centroids_parallel_non_l2(
    collection: &Collection,
    index: &IvfIndex,
    centroids: &[usize],
    prepared: &PreparedNonL2Query,
    metric: Metric,
    keep: usize,
    score_options: VectorValidationOptions,
    filter: Option<&SearchFilter>,
) -> Result<BinaryHeap<HeapCandidate>, ApiError> {
    let dense_slots = collection.slots_dense();
    if let Some(active_filter) = filter {
        if dense_slots {
            centroids
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, centroid_idx| {
                        let slots = index.slots_for_centroid(*centroid_idx).ok_or_else(|| {
                            ApiError::internal("ivf centroid list index is inconsistent")
                        })?;
                        for slot in slots {
                            let (id, values, payload) =
                                collection.point_with_payload_at_dense_slot(*slot);
                            if !matches_filter_strict(payload, active_filter) {
                                continue;
                            }
                            if let Some(score) =
                                score_point_for_candidate(prepared, values, score_options)
                            {
                                score_candidate(
                                    &mut local,
                                    HeapCandidate {
                                        id,
                                        value: score,
                                        rank_key: rank_key(metric, score),
                                    },
                                    keep,
                                );
                            }
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        } else {
            centroids
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, centroid_idx| {
                        let slots = index.slots_for_centroid(*centroid_idx).ok_or_else(|| {
                            ApiError::internal("ivf centroid list index is inconsistent")
                        })?;
                        for slot in slots {
                            let (id, values, payload) = collection
                                .point_with_payload_at_slot(*slot)
                                .ok_or_else(|| {
                                    ApiError::internal("point slot index is inconsistent")
                                })?;
                            if !matches_filter_strict(payload, active_filter) {
                                continue;
                            }
                            if let Some(score) =
                                score_point_for_candidate(prepared, values, score_options)
                            {
                                score_candidate(
                                    &mut local,
                                    HeapCandidate {
                                        id,
                                        value: score,
                                        rank_key: rank_key(metric, score),
                                    },
                                    keep,
                                );
                            }
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        }
    } else {
        if dense_slots {
            centroids
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, centroid_idx| {
                        let slots = index.slots_for_centroid(*centroid_idx).ok_or_else(|| {
                            ApiError::internal("ivf centroid list index is inconsistent")
                        })?;
                        for slot in slots {
                            let (id, values) = collection.point_at_dense_slot(*slot);
                            if let Some(score) =
                                score_point_for_candidate(prepared, values, score_options)
                            {
                                score_candidate(
                                    &mut local,
                                    HeapCandidate {
                                        id,
                                        value: score,
                                        rank_key: rank_key(metric, score),
                                    },
                                    keep,
                                );
                            }
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        } else {
            centroids
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, centroid_idx| {
                        let slots = index.slots_for_centroid(*centroid_idx).ok_or_else(|| {
                            ApiError::internal("ivf centroid list index is inconsistent")
                        })?;
                        for slot in slots {
                            let (id, values) =
                                collection.point_at_slot(*slot).ok_or_else(|| {
                                    ApiError::internal("point slot index is inconsistent")
                                })?;
                            if let Some(score) =
                                score_point_for_candidate(prepared, values, score_options)
                            {
                                score_candidate(
                                    &mut local,
                                    HeapCandidate {
                                        id,
                                        value: score,
                                        rank_key: rank_key(metric, score),
                                    },
                                    keep,
                                );
                            }
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        }
    }
}

#[allow(clippy::collapsible_else_if, clippy::too_many_arguments)]
fn score_ivf_centroids_into_heap_l2(
    heap: &mut BinaryHeap<HeapCandidate>,
    collection: &Collection,
    index: &IvfIndex,
    centroids: &[usize],
    prepared: &PreparedL2Query,
    keep: usize,
    assume_finite: bool,
    filter: Option<&SearchFilter>,
) -> Result<(), ApiError> {
    let dense_slots = collection.slots_dense();
    if let Some(active_filter) = filter {
        if dense_slots {
            for centroid_idx in centroids {
                let slots = index
                    .slots_for_centroid(*centroid_idx)
                    .ok_or_else(|| ApiError::internal("ivf centroid list index is inconsistent"))?;
                for slot in slots {
                    let (id, values, payload) = collection.point_with_payload_at_dense_slot(*slot);
                    if !matches_filter_strict(payload, active_filter) {
                        continue;
                    }
                    if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite) {
                        score_candidate(
                            heap,
                            HeapCandidate {
                                id,
                                value: score,
                                rank_key: score,
                            },
                            keep,
                        );
                    }
                }
            }
        } else {
            for centroid_idx in centroids {
                let slots = index
                    .slots_for_centroid(*centroid_idx)
                    .ok_or_else(|| ApiError::internal("ivf centroid list index is inconsistent"))?;
                for slot in slots {
                    let (id, values, payload) = collection
                        .point_with_payload_at_slot(*slot)
                        .ok_or_else(|| ApiError::internal("point slot index is inconsistent"))?;
                    if !matches_filter_strict(payload, active_filter) {
                        continue;
                    }
                    if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite) {
                        score_candidate(
                            heap,
                            HeapCandidate {
                                id,
                                value: score,
                                rank_key: score,
                            },
                            keep,
                        );
                    }
                }
            }
        }
    } else {
        if dense_slots {
            for centroid_idx in centroids {
                let slots = index
                    .slots_for_centroid(*centroid_idx)
                    .ok_or_else(|| ApiError::internal("ivf centroid list index is inconsistent"))?;
                for slot in slots {
                    let (id, values) = collection.point_at_dense_slot(*slot);
                    if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite) {
                        score_candidate(
                            heap,
                            HeapCandidate {
                                id,
                                value: score,
                                rank_key: score,
                            },
                            keep,
                        );
                    }
                }
            }
        } else {
            for centroid_idx in centroids {
                let slots = index
                    .slots_for_centroid(*centroid_idx)
                    .ok_or_else(|| ApiError::internal("ivf centroid list index is inconsistent"))?;
                for slot in slots {
                    let (id, values) = collection
                        .point_at_slot(*slot)
                        .ok_or_else(|| ApiError::internal("point slot index is inconsistent"))?;
                    if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite) {
                        score_candidate(
                            heap,
                            HeapCandidate {
                                id,
                                value: score,
                                rank_key: score,
                            },
                            keep,
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

#[allow(clippy::collapsible_else_if, clippy::too_many_arguments)]
fn score_ivf_centroids_parallel_l2(
    collection: &Collection,
    index: &IvfIndex,
    centroids: &[usize],
    prepared: &PreparedL2Query,
    keep: usize,
    assume_finite: bool,
    filter: Option<&SearchFilter>,
) -> Result<BinaryHeap<HeapCandidate>, ApiError> {
    let dense_slots = collection.slots_dense();
    if let Some(active_filter) = filter {
        if dense_slots {
            centroids
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, centroid_idx| {
                        let slots = index.slots_for_centroid(*centroid_idx).ok_or_else(|| {
                            ApiError::internal("ivf centroid list index is inconsistent")
                        })?;
                        for slot in slots {
                            let (id, values, payload) =
                                collection.point_with_payload_at_dense_slot(*slot);
                            if !matches_filter_strict(payload, active_filter) {
                                continue;
                            }
                            if let Some(score) =
                                score_l2_for_candidate(prepared, values, assume_finite)
                            {
                                score_candidate(
                                    &mut local,
                                    HeapCandidate {
                                        id,
                                        value: score,
                                        rank_key: score,
                                    },
                                    keep,
                                );
                            }
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        } else {
            centroids
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, centroid_idx| {
                        let slots = index.slots_for_centroid(*centroid_idx).ok_or_else(|| {
                            ApiError::internal("ivf centroid list index is inconsistent")
                        })?;
                        for slot in slots {
                            let (id, values, payload) = collection
                                .point_with_payload_at_slot(*slot)
                                .ok_or_else(|| {
                                    ApiError::internal("point slot index is inconsistent")
                                })?;
                            if !matches_filter_strict(payload, active_filter) {
                                continue;
                            }
                            if let Some(score) =
                                score_l2_for_candidate(prepared, values, assume_finite)
                            {
                                score_candidate(
                                    &mut local,
                                    HeapCandidate {
                                        id,
                                        value: score,
                                        rank_key: score,
                                    },
                                    keep,
                                );
                            }
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        }
    } else {
        if dense_slots {
            centroids
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, centroid_idx| {
                        let slots = index.slots_for_centroid(*centroid_idx).ok_or_else(|| {
                            ApiError::internal("ivf centroid list index is inconsistent")
                        })?;
                        for slot in slots {
                            let (id, values) = collection.point_at_dense_slot(*slot);
                            if let Some(score) =
                                score_l2_for_candidate(prepared, values, assume_finite)
                            {
                                score_candidate(
                                    &mut local,
                                    HeapCandidate {
                                        id,
                                        value: score,
                                        rank_key: score,
                                    },
                                    keep,
                                );
                            }
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        } else {
            centroids
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, centroid_idx| {
                        let slots = index.slots_for_centroid(*centroid_idx).ok_or_else(|| {
                            ApiError::internal("ivf centroid list index is inconsistent")
                        })?;
                        for slot in slots {
                            let (id, values) =
                                collection.point_at_slot(*slot).ok_or_else(|| {
                                    ApiError::internal("point slot index is inconsistent")
                                })?;
                            if let Some(score) =
                                score_l2_for_candidate(prepared, values, assume_finite)
                            {
                                score_candidate(
                                    &mut local,
                                    HeapCandidate {
                                        id,
                                        value: score,
                                        rank_key: score,
                                    },
                                    keep,
                                );
                            }
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        }
    }
}

#[allow(clippy::collapsible_else_if)]
fn score_points_l2(
    collection: &Collection,
    query: &[f32],
    include_payload: bool,
    keep: usize,
    filter: Option<&SearchFilter>,
    source: ScoreSource,
) -> Result<Vec<SearchHit>, ApiError> {
    let prepared = PreparedL2Query::new(query);
    let assume_finite = collection.strict_finite();
    let include_payload = include_payload && collection.has_payload_points();
    let use_parallel_all =
        matches!(source, ScoreSource::All) && should_parallel_all(collection, keep);
    let use_small_topk_fast_path =
        matches!(source, ScoreSource::All) && filter.is_none() && keep <= SMALL_TOPK_LINEAR_LIMIT;
    if use_small_topk_fast_path {
        if use_parallel_all {
            let hits =
                score_all_l2_small_topk_parallel(collection, &prepared, keep, assume_finite)?;
            return attach_payloads_to_small_topk_hits(collection, hits, include_payload);
        }
        let hits = score_all_l2_small_topk(collection, &prepared, keep, assume_finite)?;
        return attach_payloads_to_small_topk_hits(collection, hits, include_payload);
    }
    if matches!(source, ScoreSource::All) && keep == 1 && !use_parallel_all {
        return score_all_l2_top1(
            collection,
            &prepared,
            include_payload,
            filter,
            assume_finite,
        );
    }
    let mut heap = if use_parallel_all {
        score_all_parallel_l2(collection, &prepared, keep, filter, assume_finite)?
    } else {
        BinaryHeap::with_capacity(keep)
    };

    match source {
        ScoreSource::All => {
            if !use_parallel_all {
                if let Some(active_filter) = filter {
                    for (id, values, payload) in collection.iter_points_with_payload_unordered() {
                        if !matches_filter_strict(payload, active_filter) {
                            continue;
                        }
                        if let Some(score) =
                            score_l2_for_candidate(&prepared, values, assume_finite)
                        {
                            score_candidate(
                                &mut heap,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: score,
                                },
                                keep,
                            );
                        }
                    }
                } else {
                    for (id, values) in collection.iter_points_unordered() {
                        if let Some(score) =
                            score_l2_for_candidate(&prepared, values, assume_finite)
                        {
                            score_candidate(
                                &mut heap,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: score,
                                },
                                keep,
                            );
                        }
                    }
                }
            }
        }
        ScoreSource::CandidateIds(candidate_ids) => {
            let use_small_topk_candidate_ids_fast_path =
                filter.is_none() && keep <= SMALL_TOPK_LINEAR_LIMIT;
            if use_small_topk_candidate_ids_fast_path {
                if should_parallel_candidates(collection.dimension(), candidate_ids.len()) {
                    let hits = score_candidate_ids_l2_small_topk_parallel(
                        collection,
                        &prepared,
                        keep,
                        assume_finite,
                        &candidate_ids,
                    )?;
                    return attach_payloads_to_small_topk_hits(collection, hits, include_payload);
                }
                let hits = score_candidate_ids_l2_small_topk(
                    collection,
                    &prepared,
                    keep,
                    assume_finite,
                    &candidate_ids,
                )?;
                return attach_payloads_to_small_topk_hits(collection, hits, include_payload);
            } else if should_parallel_candidates(collection.dimension(), candidate_ids.len()) {
                heap = score_candidate_ids_parallel_l2(
                    collection,
                    &prepared,
                    keep,
                    filter,
                    assume_finite,
                    &candidate_ids,
                )?;
            } else if let Some(active_filter) = filter {
                for id in candidate_ids {
                    let (values, payload) = collection
                        .get_point_record(id)
                        .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;
                    if !matches_filter_strict(payload, active_filter) {
                        continue;
                    }
                    if let Some(score) = score_l2_for_candidate(&prepared, values, assume_finite) {
                        score_candidate(
                            &mut heap,
                            HeapCandidate {
                                id,
                                value: score,
                                rank_key: score,
                            },
                            keep,
                        );
                    }
                }
            } else {
                for id in candidate_ids {
                    let values = collection
                        .get_point(id)
                        .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;
                    if let Some(score) = score_l2_for_candidate(&prepared, values, assume_finite) {
                        score_candidate(
                            &mut heap,
                            HeapCandidate {
                                id,
                                value: score,
                                rank_key: score,
                            },
                            keep,
                        );
                    }
                }
            }
        }
        ScoreSource::CandidateSlots(candidate_slots) => {
            let use_small_topk_candidate_slots_fast_path =
                filter.is_none() && keep <= SMALL_TOPK_LINEAR_LIMIT;
            let dense_slots = collection.slots_dense();

            if use_small_topk_candidate_slots_fast_path {
                if should_parallel_candidates(collection.dimension(), candidate_slots.len()) {
                    let hits = score_candidate_slots_l2_small_topk_parallel(
                        collection,
                        &prepared,
                        keep,
                        assume_finite,
                        &candidate_slots,
                    )?;
                    return attach_payloads_to_small_topk_hits(collection, hits, include_payload);
                }
                let hits = score_candidate_slots_l2_small_topk(
                    collection,
                    &prepared,
                    keep,
                    assume_finite,
                    &candidate_slots,
                )?;
                return attach_payloads_to_small_topk_hits(collection, hits, include_payload);
            } else if should_parallel_candidates(collection.dimension(), candidate_slots.len()) {
                heap = score_candidate_slots_parallel_l2(
                    collection,
                    &prepared,
                    keep,
                    filter,
                    assume_finite,
                    &candidate_slots,
                )?;
            } else if let Some(active_filter) = filter {
                if dense_slots {
                    for slot in candidate_slots {
                        let (id, values, payload) =
                            collection.point_with_payload_at_dense_slot(slot);
                        if !matches_filter_strict(payload, active_filter) {
                            continue;
                        }
                        if let Some(score) =
                            score_l2_for_candidate(&prepared, values, assume_finite)
                        {
                            score_candidate(
                                &mut heap,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: score,
                                },
                                keep,
                            );
                        }
                    }
                } else {
                    for slot in candidate_slots {
                        let (id, values, payload) =
                            collection.point_with_payload_at_slot(slot).ok_or_else(|| {
                                ApiError::internal("point slot index is inconsistent")
                            })?;
                        if !matches_filter_strict(payload, active_filter) {
                            continue;
                        }
                        if let Some(score) =
                            score_l2_for_candidate(&prepared, values, assume_finite)
                        {
                            score_candidate(
                                &mut heap,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: score,
                                },
                                keep,
                            );
                        }
                    }
                }
            } else {
                if dense_slots {
                    for slot in candidate_slots {
                        let (id, values) = collection.point_at_dense_slot(slot);
                        if let Some(score) =
                            score_l2_for_candidate(&prepared, values, assume_finite)
                        {
                            score_candidate(
                                &mut heap,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: score,
                                },
                                keep,
                            );
                        }
                    }
                } else {
                    for slot in candidate_slots {
                        let (id, values) = collection.point_at_slot(slot).ok_or_else(|| {
                            ApiError::internal("point slot index is inconsistent")
                        })?;
                        if let Some(score) =
                            score_l2_for_candidate(&prepared, values, assume_finite)
                        {
                            score_candidate(
                                &mut heap,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: score,
                                },
                                keep,
                            );
                        }
                    }
                }
            }
        }
        ScoreSource::IvfCentroids {
            index,
            centroids,
            candidate_count,
        } => {
            let use_small_topk_ivf_fast_path = filter.is_none() && keep <= SMALL_TOPK_LINEAR_LIMIT;
            if use_small_topk_ivf_fast_path {
                if should_parallel_candidates(collection.dimension(), candidate_count) {
                    let hits = score_ivf_centroids_l2_small_topk_parallel(
                        collection,
                        &index,
                        &centroids,
                        &prepared,
                        keep,
                        assume_finite,
                    )?;
                    return attach_payloads_to_small_topk_hits(collection, hits, include_payload);
                }
                let hits = score_ivf_centroids_l2_small_topk(
                    collection,
                    &index,
                    &centroids,
                    &prepared,
                    keep,
                    assume_finite,
                )?;
                return attach_payloads_to_small_topk_hits(collection, hits, include_payload);
            } else if should_parallel_candidates(collection.dimension(), candidate_count) {
                heap = score_ivf_centroids_parallel_l2(
                    collection,
                    &index,
                    &centroids,
                    &prepared,
                    keep,
                    assume_finite,
                    filter,
                )?;
            } else {
                score_ivf_centroids_into_heap_l2(
                    &mut heap,
                    collection,
                    &index,
                    &centroids,
                    &prepared,
                    keep,
                    assume_finite,
                    filter,
                )?;
            }
        }
    }

    heap.into_sorted_vec()
        .into_iter()
        .map(|candidate| {
            Ok(SearchHit {
                id: candidate.id,
                value: candidate.value.sqrt(),
                payload: if include_payload {
                    let payload = collection
                        .get_payload(candidate.id)
                        .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;
                    payload_for_response(payload)
                } else {
                    None
                },
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()
}

#[allow(clippy::collapsible_else_if)]
fn score_all_parallel_l2(
    collection: &Collection,
    prepared: &PreparedL2Query,
    keep: usize,
    filter: Option<&SearchFilter>,
    assume_finite: bool,
) -> Result<BinaryHeap<HeapCandidate>, ApiError> {
    let slot_count = collection.slot_count();
    let dense_slots = collection.slots_dense();
    if let Some(active_filter) = filter {
        if dense_slots {
            (0..slot_count)
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, slot| {
                        let (id, values, payload) =
                            collection.point_with_payload_at_dense_slot(slot);
                        if !matches_filter_strict(payload, active_filter) {
                            return Ok(local);
                        }
                        if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite)
                        {
                            score_candidate(
                                &mut local,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: score,
                                },
                                keep,
                            );
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        } else {
            (0..slot_count)
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, slot| {
                        let Some((id, values, payload)) =
                            collection.point_with_payload_at_slot(slot)
                        else {
                            return Ok(local);
                        };
                        if !matches_filter_strict(payload, active_filter) {
                            return Ok(local);
                        }
                        if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite)
                        {
                            score_candidate(
                                &mut local,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: score,
                                },
                                keep,
                            );
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        }
    } else {
        if dense_slots {
            (0..slot_count)
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, slot| {
                        let (id, values) = collection.point_at_dense_slot(slot);
                        if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite)
                        {
                            score_candidate(
                                &mut local,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: score,
                                },
                                keep,
                            );
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        } else {
            (0..slot_count)
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, slot| {
                        let Some((id, values)) = collection.point_at_slot(slot) else {
                            return Ok(local);
                        };
                        if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite)
                        {
                            score_candidate(
                                &mut local,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: score,
                                },
                                keep,
                            );
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        }
    }
}

fn score_candidate_ids_parallel_l2(
    collection: &Collection,
    prepared: &PreparedL2Query,
    keep: usize,
    filter: Option<&SearchFilter>,
    assume_finite: bool,
    candidate_ids: &[u64],
) -> Result<BinaryHeap<HeapCandidate>, ApiError> {
    if let Some(active_filter) = filter {
        candidate_ids
            .into_par_iter()
            .with_min_len(parallel_score_min_chunk_len())
            .try_fold(
                || BinaryHeap::with_capacity(keep),
                |mut local, id| {
                    let (values, payload) = collection
                        .get_point_record(*id)
                        .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;
                    if !matches_filter_strict(payload, active_filter) {
                        return Ok(local);
                    }
                    if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite) {
                        score_candidate(
                            &mut local,
                            HeapCandidate {
                                id: *id,
                                value: score,
                                rank_key: score,
                            },
                            keep,
                        );
                    }
                    Ok(local)
                },
            )
            .try_reduce(
                || BinaryHeap::with_capacity(keep),
                |mut combined, mut local| {
                    while let Some(candidate) = local.pop() {
                        score_candidate(&mut combined, candidate, keep);
                    }
                    Ok(combined)
                },
            )
    } else {
        candidate_ids
            .into_par_iter()
            .with_min_len(parallel_score_min_chunk_len())
            .try_fold(
                || BinaryHeap::with_capacity(keep),
                |mut local, id| {
                    let values = collection
                        .get_point(*id)
                        .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;
                    if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite) {
                        score_candidate(
                            &mut local,
                            HeapCandidate {
                                id: *id,
                                value: score,
                                rank_key: score,
                            },
                            keep,
                        );
                    }
                    Ok(local)
                },
            )
            .try_reduce(
                || BinaryHeap::with_capacity(keep),
                |mut combined, mut local| {
                    while let Some(candidate) = local.pop() {
                        score_candidate(&mut combined, candidate, keep);
                    }
                    Ok(combined)
                },
            )
    }
}

#[allow(clippy::collapsible_else_if)]
fn score_candidate_slots_parallel_l2(
    collection: &Collection,
    prepared: &PreparedL2Query,
    keep: usize,
    filter: Option<&SearchFilter>,
    assume_finite: bool,
    candidate_slots: &[usize],
) -> Result<BinaryHeap<HeapCandidate>, ApiError> {
    let dense_slots = collection.slots_dense();
    if let Some(active_filter) = filter {
        if dense_slots {
            candidate_slots
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, slot| {
                        let (id, values, payload) =
                            collection.point_with_payload_at_dense_slot(*slot);
                        if !matches_filter_strict(payload, active_filter) {
                            return Ok(local);
                        }
                        if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite)
                        {
                            score_candidate(
                                &mut local,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: score,
                                },
                                keep,
                            );
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        } else {
            candidate_slots
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, slot| {
                        let (id, values, payload) = collection
                            .point_with_payload_at_slot(*slot)
                            .ok_or_else(|| {
                                ApiError::internal("point slot index is inconsistent")
                            })?;
                        if !matches_filter_strict(payload, active_filter) {
                            return Ok(local);
                        }
                        if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite)
                        {
                            score_candidate(
                                &mut local,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: score,
                                },
                                keep,
                            );
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        }
    } else {
        if dense_slots {
            candidate_slots
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, slot| {
                        let (id, values) = collection.point_at_dense_slot(*slot);
                        if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite)
                        {
                            score_candidate(
                                &mut local,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: score,
                                },
                                keep,
                            );
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        } else {
            candidate_slots
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, slot| {
                        let (id, values) = collection.point_at_slot(*slot).ok_or_else(|| {
                            ApiError::internal("point slot index is inconsistent")
                        })?;
                        if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite)
                        {
                            score_candidate(
                                &mut local,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: score,
                                },
                                keep,
                            );
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        }
    }
}

fn score_all_l2_small_topk(
    collection: &Collection,
    prepared: &PreparedL2Query,
    keep: usize,
    assume_finite: bool,
) -> Result<Vec<SearchHit>, ApiError> {
    let mut topk = SmallTopK::new(keep);

    if collection.slots_dense() {
        for slot in 0..collection.slot_count() {
            let (id, values) = collection.point_at_dense_slot(slot);
            let score = prepared.l2_squared(values);
            if !assume_finite && !score.is_finite() {
                continue;
            }
            topk.push_candidate(id, score);
        }
    } else {
        for (id, values) in collection.iter_points_unordered() {
            let score = prepared.l2_squared(values);
            if !assume_finite && !score.is_finite() {
                continue;
            }
            topk.push_candidate(id, score);
        }
    }

    build_small_topk_hits(&topk)
}

fn score_all_l2_small_topk_parallel(
    collection: &Collection,
    prepared: &PreparedL2Query,
    keep: usize,
    assume_finite: bool,
) -> Result<Vec<SearchHit>, ApiError> {
    let merged = if collection.slots_dense() {
        (0..collection.slot_count())
            .into_par_iter()
            .with_min_len(parallel_score_min_chunk_len())
            .fold(
                || SmallTopK::new(keep),
                |mut local, slot| {
                    let (id, values) = collection.point_at_dense_slot(slot);
                    let score = prepared.l2_squared(values);
                    if assume_finite || score.is_finite() {
                        local.push_candidate(id, score);
                    }
                    local
                },
            )
            .reduce(
                || SmallTopK::new(keep),
                |mut combined, local| {
                    combined.merge_from(&local);
                    combined
                },
            )
    } else {
        (0..collection.slot_count())
            .into_par_iter()
            .with_min_len(parallel_score_min_chunk_len())
            .fold(
                || SmallTopK::new(keep),
                |mut local, slot| {
                    if let Some((id, values)) = collection.point_at_slot(slot) {
                        let score = prepared.l2_squared(values);
                        if assume_finite || score.is_finite() {
                            local.push_candidate(id, score);
                        }
                    }
                    local
                },
            )
            .reduce(
                || SmallTopK::new(keep),
                |mut combined, local| {
                    combined.merge_from(&local);
                    combined
                },
            )
    };

    build_small_topk_hits(&merged)
}

fn score_all_l2_top1(
    collection: &Collection,
    prepared: &PreparedL2Query,
    include_payload: bool,
    filter: Option<&SearchFilter>,
    assume_finite: bool,
) -> Result<Vec<SearchHit>, ApiError> {
    let mut best: Option<(u64, f32)> = None;
    if let Some(active_filter) = filter {
        for (id, values, payload) in collection.iter_points_with_payload_unordered() {
            if !matches_filter_strict(payload, active_filter) {
                continue;
            }
            if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite) {
                if best.as_ref().is_none_or(|(best_id, best_score)| {
                    compare_l2(score, id, *best_score, *best_id).is_lt()
                }) {
                    best = Some((id, score));
                }
            }
        }
    } else {
        for (id, values) in collection.iter_points_unordered() {
            if let Some(score) = score_l2_for_candidate(prepared, values, assume_finite) {
                if best.as_ref().is_none_or(|(best_id, best_score)| {
                    compare_l2(score, id, *best_score, *best_id).is_lt()
                }) {
                    best = Some((id, score));
                }
            }
        }
    }

    if let Some((id, squared_value)) = best {
        let payload = if include_payload {
            let payload = collection
                .get_payload(id)
                .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;
            payload_for_response(payload)
        } else {
            None
        };
        return Ok(vec![SearchHit {
            id,
            value: squared_value.sqrt(),
            payload,
        }]);
    }

    Ok(Vec::new())
}

fn attach_payloads_to_small_topk_hits(
    collection: &Collection,
    mut hits: Vec<SearchHit>,
    include_payload: bool,
) -> Result<Vec<SearchHit>, ApiError> {
    if !include_payload {
        return Ok(hits);
    }

    for hit in &mut hits {
        let payload = collection
            .get_payload(hit.id)
            .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;
        hit.payload = payload_for_response(payload);
    }

    Ok(hits)
}

fn build_small_topk_hits(topk: &SmallTopK) -> Result<Vec<SearchHit>, ApiError> {
    let mut order: Vec<usize> = (0..topk.ids.len()).collect();
    order.sort_unstable_by(|left, right| {
        compare_l2(
            topk.scores[*left],
            topk.ids[*left],
            topk.scores[*right],
            topk.ids[*right],
        )
    });

    order
        .into_iter()
        .map(|idx| {
            Ok(SearchHit {
                id: topk.ids[idx],
                value: topk.scores[idx].sqrt(),
                payload: None,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()
}

fn build_small_topk_hits_non_l2(topk: &SmallTopKNonL2) -> Result<Vec<SearchHit>, ApiError> {
    let mut order: Vec<usize> = (0..topk.ids.len()).collect();
    order.sort_unstable_by(|left, right| {
        compare_non_l2(
            topk.scores[*left],
            topk.ids[*left],
            topk.scores[*right],
            topk.ids[*right],
        )
    });

    order
        .into_iter()
        .map(|idx| {
            Ok(SearchHit {
                id: topk.ids[idx],
                value: topk.scores[idx],
                payload: None,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()
}

#[derive(Debug, Clone)]
struct SmallTopK {
    keep: usize,
    ids: Vec<u64>,
    scores: Vec<f32>,
    worst_idx: usize,
}

impl SmallTopK {
    fn new(keep: usize) -> Self {
        Self {
            keep,
            ids: Vec::with_capacity(keep),
            scores: Vec::with_capacity(keep),
            worst_idx: 0,
        }
    }

    fn push_candidate(&mut self, id: u64, score: f32) {
        if self.ids.len() < self.keep {
            self.ids.push(id);
            self.scores.push(score);
            let last_idx = self.ids.len() - 1;
            if self.ids.len() == 1
                || compare_l2(
                    self.scores[last_idx],
                    self.ids[last_idx],
                    self.scores[self.worst_idx],
                    self.ids[self.worst_idx],
                )
                .is_gt()
            {
                self.worst_idx = last_idx;
            }
            return;
        }

        if compare_l2(
            score,
            id,
            self.scores[self.worst_idx],
            self.ids[self.worst_idx],
        )
        .is_lt()
        {
            self.ids[self.worst_idx] = id;
            self.scores[self.worst_idx] = score;
            self.recompute_worst();
        }
    }

    fn merge_from(&mut self, other: &Self) {
        for (id, score) in other.ids.iter().zip(other.scores.iter()) {
            self.push_candidate(*id, *score);
        }
    }

    fn recompute_worst(&mut self) {
        let mut worst_idx = 0usize;
        for idx in 1..self.ids.len() {
            if compare_l2(
                self.scores[idx],
                self.ids[idx],
                self.scores[worst_idx],
                self.ids[worst_idx],
            )
            .is_gt()
            {
                worst_idx = idx;
            }
        }
        self.worst_idx = worst_idx;
    }
}

#[derive(Debug, Clone)]
struct SmallTopKNonL2 {
    keep: usize,
    ids: Vec<u64>,
    scores: Vec<f32>,
    worst_idx: usize,
}

impl SmallTopKNonL2 {
    fn new(keep: usize) -> Self {
        Self {
            keep,
            ids: Vec::with_capacity(keep),
            scores: Vec::with_capacity(keep),
            worst_idx: 0,
        }
    }

    fn push_candidate(&mut self, id: u64, score: f32) {
        if self.ids.len() < self.keep {
            self.ids.push(id);
            self.scores.push(score);
            let last_idx = self.ids.len() - 1;
            if self.ids.len() == 1
                || compare_non_l2(
                    self.scores[last_idx],
                    self.ids[last_idx],
                    self.scores[self.worst_idx],
                    self.ids[self.worst_idx],
                )
                .is_gt()
            {
                self.worst_idx = last_idx;
            }
            return;
        }

        if compare_non_l2(
            score,
            id,
            self.scores[self.worst_idx],
            self.ids[self.worst_idx],
        )
        .is_lt()
        {
            self.ids[self.worst_idx] = id;
            self.scores[self.worst_idx] = score;
            self.recompute_worst();
        }
    }

    fn merge_from(&mut self, other: &Self) {
        for (id, score) in other.ids.iter().zip(other.scores.iter()) {
            self.push_candidate(*id, *score);
        }
    }

    fn recompute_worst(&mut self) {
        let mut worst_idx = 0usize;
        for idx in 1..self.ids.len() {
            if compare_non_l2(
                self.scores[idx],
                self.ids[idx],
                self.scores[worst_idx],
                self.ids[worst_idx],
            )
            .is_gt()
            {
                worst_idx = idx;
            }
        }
        self.worst_idx = worst_idx;
    }
}

fn compare_l2(left_score: f32, left_id: u64, right_score: f32, right_id: u64) -> Ordering {
    left_score
        .total_cmp(&right_score)
        .then_with(|| left_id.cmp(&right_id))
}

fn compare_non_l2(left_score: f32, left_id: u64, right_score: f32, right_id: u64) -> Ordering {
    right_score
        .total_cmp(&left_score)
        .then_with(|| left_id.cmp(&right_id))
}

#[allow(clippy::collapsible_else_if)]
fn score_all_parallel(
    collection: &Collection,
    prepared: &PreparedNonL2Query,
    metric: Metric,
    keep: usize,
    score_options: VectorValidationOptions,
    filter: Option<&SearchFilter>,
) -> Result<BinaryHeap<HeapCandidate>, ApiError> {
    let slot_count = collection.slot_count();
    let dense_slots = collection.slots_dense();
    if let Some(active_filter) = filter {
        if dense_slots {
            (0..slot_count)
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, slot| {
                        let (id, values, payload) =
                            collection.point_with_payload_at_dense_slot(slot);
                        if !matches_filter_strict(payload, active_filter) {
                            return Ok(local);
                        }
                        if let Some(score) =
                            score_point_for_candidate(prepared, values, score_options)
                        {
                            score_candidate(
                                &mut local,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: rank_key(metric, score),
                                },
                                keep,
                            );
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        } else {
            (0..slot_count)
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, slot| {
                        let Some((id, values, payload)) =
                            collection.point_with_payload_at_slot(slot)
                        else {
                            return Ok(local);
                        };
                        if !matches_filter_strict(payload, active_filter) {
                            return Ok(local);
                        }
                        if let Some(score) =
                            score_point_for_candidate(prepared, values, score_options)
                        {
                            score_candidate(
                                &mut local,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: rank_key(metric, score),
                                },
                                keep,
                            );
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        }
    } else {
        if dense_slots {
            (0..slot_count)
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, slot| {
                        let (id, values) = collection.point_at_dense_slot(slot);
                        if let Some(score) =
                            score_point_for_candidate(prepared, values, score_options)
                        {
                            score_candidate(
                                &mut local,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: rank_key(metric, score),
                                },
                                keep,
                            );
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        } else {
            (0..slot_count)
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, slot| {
                        let Some((id, values)) = collection.point_at_slot(slot) else {
                            return Ok(local);
                        };
                        if let Some(score) =
                            score_point_for_candidate(prepared, values, score_options)
                        {
                            score_candidate(
                                &mut local,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: rank_key(metric, score),
                                },
                                keep,
                            );
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        }
    }
}

fn score_candidate_ids_parallel(
    collection: &Collection,
    prepared: &PreparedNonL2Query,
    metric: Metric,
    keep: usize,
    score_options: VectorValidationOptions,
    filter: Option<&SearchFilter>,
    candidate_ids: &[u64],
) -> Result<BinaryHeap<HeapCandidate>, ApiError> {
    if let Some(active_filter) = filter {
        candidate_ids
            .into_par_iter()
            .with_min_len(parallel_score_min_chunk_len())
            .try_fold(
                || BinaryHeap::with_capacity(keep),
                |mut local, id| {
                    let (values, payload) = collection
                        .get_point_record(*id)
                        .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;
                    if !matches_filter_strict(payload, active_filter) {
                        return Ok(local);
                    }
                    if let Some(score) = score_point_for_candidate(prepared, values, score_options)
                    {
                        score_candidate(
                            &mut local,
                            HeapCandidate {
                                id: *id,
                                value: score,
                                rank_key: rank_key(metric, score),
                            },
                            keep,
                        );
                    }
                    Ok(local)
                },
            )
            .try_reduce(
                || BinaryHeap::with_capacity(keep),
                |mut combined, mut local| {
                    while let Some(candidate) = local.pop() {
                        score_candidate(&mut combined, candidate, keep);
                    }
                    Ok(combined)
                },
            )
    } else {
        candidate_ids
            .into_par_iter()
            .with_min_len(parallel_score_min_chunk_len())
            .try_fold(
                || BinaryHeap::with_capacity(keep),
                |mut local, id| {
                    let values = collection
                        .get_point(*id)
                        .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;
                    if let Some(score) = score_point_for_candidate(prepared, values, score_options)
                    {
                        score_candidate(
                            &mut local,
                            HeapCandidate {
                                id: *id,
                                value: score,
                                rank_key: rank_key(metric, score),
                            },
                            keep,
                        );
                    }
                    Ok(local)
                },
            )
            .try_reduce(
                || BinaryHeap::with_capacity(keep),
                |mut combined, mut local| {
                    while let Some(candidate) = local.pop() {
                        score_candidate(&mut combined, candidate, keep);
                    }
                    Ok(combined)
                },
            )
    }
}

#[allow(clippy::collapsible_else_if)]
fn score_candidate_slots_parallel(
    collection: &Collection,
    prepared: &PreparedNonL2Query,
    metric: Metric,
    keep: usize,
    score_options: VectorValidationOptions,
    filter: Option<&SearchFilter>,
    candidate_slots: &[usize],
) -> Result<BinaryHeap<HeapCandidate>, ApiError> {
    let dense_slots = collection.slots_dense();
    if let Some(active_filter) = filter {
        if dense_slots {
            candidate_slots
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, slot| {
                        let (id, values, payload) =
                            collection.point_with_payload_at_dense_slot(*slot);
                        if !matches_filter_strict(payload, active_filter) {
                            return Ok(local);
                        }
                        if let Some(score) =
                            score_point_for_candidate(prepared, values, score_options)
                        {
                            score_candidate(
                                &mut local,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: rank_key(metric, score),
                                },
                                keep,
                            );
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        } else {
            candidate_slots
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, slot| {
                        let (id, values, payload) = collection
                            .point_with_payload_at_slot(*slot)
                            .ok_or_else(|| {
                                ApiError::internal("point slot index is inconsistent")
                            })?;
                        if !matches_filter_strict(payload, active_filter) {
                            return Ok(local);
                        }
                        if let Some(score) =
                            score_point_for_candidate(prepared, values, score_options)
                        {
                            score_candidate(
                                &mut local,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: rank_key(metric, score),
                                },
                                keep,
                            );
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        }
    } else {
        if dense_slots {
            candidate_slots
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, slot| {
                        let (id, values) = collection.point_at_dense_slot(*slot);
                        if let Some(score) =
                            score_point_for_candidate(prepared, values, score_options)
                        {
                            score_candidate(
                                &mut local,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: rank_key(metric, score),
                                },
                                keep,
                            );
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        } else {
            candidate_slots
                .into_par_iter()
                .with_min_len(parallel_score_min_chunk_len())
                .try_fold(
                    || BinaryHeap::with_capacity(keep),
                    |mut local, slot| {
                        let (id, values) = collection.point_at_slot(*slot).ok_or_else(|| {
                            ApiError::internal("point slot index is inconsistent")
                        })?;
                        if let Some(score) =
                            score_point_for_candidate(prepared, values, score_options)
                        {
                            score_candidate(
                                &mut local,
                                HeapCandidate {
                                    id,
                                    value: score,
                                    rank_key: rank_key(metric, score),
                                },
                                keep,
                            );
                        }
                        Ok(local)
                    },
                )
                .try_reduce(
                    || BinaryHeap::with_capacity(keep),
                    |mut combined, mut local| {
                        while let Some(candidate) = local.pop() {
                            score_candidate(&mut combined, candidate, keep);
                        }
                        Ok(combined)
                    },
                )
        }
    }
}

fn validate_query(query: &[f32]) -> Result<(), ApiError> {
    if let Some(index) = query.iter().position(|value| !value.is_finite()) {
        return Err(ApiError::invalid_argument(format!(
            "query contains non-finite value at index {index}"
        )));
    }
    Ok(())
}

fn payload_for_response(payload: &PointPayload) -> Option<PointPayload> {
    if payload.is_empty() {
        None
    } else {
        Some(payload.clone())
    }
}

fn score_point_for_candidate(
    prepared: &PreparedNonL2Query,
    values: &[f32],
    options: VectorValidationOptions,
) -> Option<f32> {
    let value = match prepared {
        PreparedNonL2Query::Dot(prepared) => prepared.dot_unchecked(values),
        PreparedNonL2Query::Cosine(prepared) => {
            prepared.cosine_unchecked(values, options.zero_norm_epsilon)?
        }
    };

    if value.is_finite() {
        Some(value)
    } else {
        None
    }
}

fn score_l2_for_candidate(
    prepared: &PreparedL2Query,
    values: &[f32],
    assume_finite: bool,
) -> Option<f32> {
    let value = prepared.l2_squared(values);
    if assume_finite || value.is_finite() {
        Some(value)
    } else {
        None
    }
}

fn metric_value_for_response(metric: Metric, ranked_value: f32) -> f32 {
    match metric {
        Metric::L2 => ranked_value.sqrt(),
        Metric::Dot | Metric::Cosine => ranked_value,
    }
}

fn score_candidate(heap: &mut BinaryHeap<HeapCandidate>, candidate: HeapCandidate, keep: usize) {
    if heap.len() < keep {
        heap.push(candidate);
        return;
    }

    let should_replace = heap
        .peek()
        .is_some_and(|worst| candidate.cmp(worst).is_lt());
    if should_replace {
        let _ = heap.pop();
        heap.push(candidate);
    }
}

#[derive(Debug, Clone)]
struct HeapCandidate {
    id: u64,
    value: f32,
    rank_key: f32,
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
        self.rank_key
            .total_cmp(&other.rank_key)
            .then_with(|| self.id.cmp(&other.id))
    }
}

fn rank_key(metric: Metric, value: f32) -> f32 {
    match metric {
        Metric::L2 => value,
        Metric::Dot | Metric::Cosine => -value,
    }
}

#[cfg(test)]
mod tests {
    use aionbd_core::{Collection, CollectionConfig, VectorValidationOptions};

    use crate::models::Metric;

    use super::{score_points, ScoreSource, PARALLEL_SCORE_MIN_POINTS};

    fn occupied_slots(collection: &Collection) -> Vec<usize> {
        (0..collection.slot_count())
            .filter(|slot| collection.point_at_slot(*slot).is_some())
            .collect()
    }

    #[test]
    fn parallel_all_source_matches_candidate_ids_source() {
        let config = CollectionConfig::new(8, true).expect("config should be valid");
        let mut collection = Collection::new("bench", config).expect("collection should build");
        let total = PARALLEL_SCORE_MIN_POINTS + 32;
        for id in 0..total {
            let values = (0..8)
                .map(|offset| ((id * 31 + offset * 17) % 1000) as f32 / 1000.0)
                .collect();
            collection
                .upsert_point(id as u64, values)
                .expect("upsert should work");
        }
        let query = vec![0.11, 0.22, 0.33, 0.44, 0.55, 0.66, 0.77, 0.88];
        let keep = 32;

        let all_hits = score_points(
            &collection,
            &query,
            Metric::L2,
            true,
            keep,
            VectorValidationOptions::strict(),
            None,
            ScoreSource::All,
        )
        .expect("parallel scoring should succeed");
        let candidate_hits = score_points(
            &collection,
            &query,
            Metric::L2,
            true,
            keep,
            VectorValidationOptions::strict(),
            None,
            ScoreSource::CandidateIds(collection.point_ids()),
        )
        .expect("candidate scoring should succeed");

        assert_eq!(all_hits.len(), candidate_hits.len());
        for (left, right) in all_hits.iter().zip(candidate_hits.iter()) {
            assert_eq!(left.id, right.id);
            assert!((left.value - right.value).abs() <= f32::EPSILON);
        }
    }

    #[test]
    fn parallel_small_topk_non_l2_matches_candidate_ids_source() {
        let config = CollectionConfig::new(8, true).expect("config should be valid");
        let mut collection = Collection::new("bench", config).expect("collection should build");
        let total = PARALLEL_SCORE_MIN_POINTS + 64;
        for id in 0..total {
            let values = (0..8)
                .map(|offset| (((id * 37 + offset * 19) % 1000) as f32 + 1.0) / 1000.0)
                .collect();
            collection
                .upsert_point(id as u64, values)
                .expect("upsert should work");
        }
        let query = vec![0.15, 0.23, 0.31, 0.49, 0.57, 0.68, 0.79, 0.91];
        let keep = 16;

        for metric in [Metric::Dot, Metric::Cosine] {
            let all_hits = score_points(
                &collection,
                &query,
                metric,
                true,
                keep,
                VectorValidationOptions::strict(),
                None,
                ScoreSource::All,
            )
            .expect("parallel scoring should succeed");
            let candidate_hits = score_points(
                &collection,
                &query,
                metric,
                true,
                keep,
                VectorValidationOptions::strict(),
                None,
                ScoreSource::CandidateIds(collection.point_ids()),
            )
            .expect("candidate scoring should succeed");

            assert_eq!(all_hits.len(), candidate_hits.len());
            for (left, right) in all_hits.iter().zip(candidate_hits.iter()) {
                assert_eq!(left.id, right.id);
                assert!((left.value - right.value).abs() <= f32::EPSILON);
            }
        }
    }

    #[test]
    fn parallel_sources_match_candidate_slots_source() {
        let config = CollectionConfig::new(8, true).expect("config should be valid");
        let mut collection = Collection::new("bench", config).expect("collection should build");
        let total = PARALLEL_SCORE_MIN_POINTS + 48;
        for id in 0..total {
            let values = (0..8)
                .map(|offset| (((id * 41 + offset * 23) % 1000) as f32 + 0.5) / 1000.0)
                .collect();
            collection
                .upsert_point(id as u64, values)
                .expect("upsert should work");
        }
        let slots = occupied_slots(&collection);
        let query = vec![0.12, 0.21, 0.32, 0.45, 0.56, 0.63, 0.74, 0.83];

        for (metric, keep) in [(Metric::L2, 32), (Metric::Dot, 16), (Metric::Cosine, 16)] {
            let all_hits = score_points(
                &collection,
                &query,
                metric,
                true,
                keep,
                VectorValidationOptions::strict(),
                None,
                ScoreSource::All,
            )
            .expect("all-source scoring should succeed");
            let slot_hits = score_points(
                &collection,
                &query,
                metric,
                true,
                keep,
                VectorValidationOptions::strict(),
                None,
                ScoreSource::CandidateSlots(slots.clone()),
            )
            .expect("slot-source scoring should succeed");

            assert_eq!(all_hits.len(), slot_hits.len());
            for (left, right) in all_hits.iter().zip(slot_hits.iter()) {
                assert_eq!(left.id, right.id);
                assert!((left.value - right.value).abs() <= f32::EPSILON);
            }
        }
    }

    #[test]
    fn top1_l2_matches_topk_first_hit() {
        let config = CollectionConfig::new(4, true).expect("config should be valid");
        let mut collection = Collection::new("bench", config).expect("collection should build");
        for id in 0..64u64 {
            let values = vec![
                id as f32 / 100.0,
                (id % 7) as f32 / 10.0,
                (id % 11) as f32 / 10.0,
                (id % 13) as f32 / 10.0,
            ];
            collection
                .upsert_point(id, values)
                .expect("upsert should work");
        }
        let query = vec![0.12, 0.25, 0.44, 0.73];
        let top1 = score_points(
            &collection,
            &query,
            Metric::L2,
            false,
            1,
            VectorValidationOptions::strict(),
            None,
            ScoreSource::All,
        )
        .expect("top1 scoring should succeed");
        let topk = score_points(
            &collection,
            &query,
            Metric::L2,
            false,
            4,
            VectorValidationOptions::strict(),
            None,
            ScoreSource::All,
        )
        .expect("topk scoring should succeed");

        assert_eq!(top1.len(), 1);
        assert!(!topk.is_empty());
        assert_eq!(top1[0].id, topk[0].id);
        assert!((top1[0].value - topk[0].value).abs() <= f32::EPSILON);
    }

    #[test]
    fn top1_dot_matches_topk_first_hit() {
        let config = CollectionConfig::new(4, true).expect("config should be valid");
        let mut collection = Collection::new("bench", config).expect("collection should build");
        for id in 0..64u64 {
            let values = vec![
                id as f32 / 150.0,
                (id % 5) as f32 / 10.0,
                (id % 9) as f32 / 10.0,
                (id % 17) as f32 / 10.0,
            ];
            collection
                .upsert_point(id, values)
                .expect("upsert should work");
        }
        let query = vec![0.17, 0.33, 0.49, 0.71];
        let top1 = score_points(
            &collection,
            &query,
            Metric::Dot,
            false,
            1,
            VectorValidationOptions::strict(),
            None,
            ScoreSource::All,
        )
        .expect("top1 scoring should succeed");
        let topk = score_points(
            &collection,
            &query,
            Metric::Dot,
            false,
            4,
            VectorValidationOptions::strict(),
            None,
            ScoreSource::All,
        )
        .expect("topk scoring should succeed");

        assert_eq!(top1.len(), 1);
        assert!(!topk.is_empty());
        assert_eq!(top1[0].id, topk[0].id);
        assert!((top1[0].value - topk[0].value).abs() <= f32::EPSILON);
    }
}
