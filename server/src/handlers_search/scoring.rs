use std::cmp::Ordering;
use std::collections::BinaryHeap;

use aionbd_core::{
    cosine_similarity_with_options, dot_product_with_options, l2_distance_with_options, Collection,
    VectorError, VectorValidationOptions,
};

use crate::errors::{map_vector_error, ApiError};
use crate::models::{Metric, PointPayload, SearchFilter, SearchHit};

use super::filter::matches_filter;

pub(crate) enum ScoreSource {
    All,
    CandidateIds(Vec<u64>),
}

pub(crate) fn score_points(
    collection: &Collection,
    query: &[f32],
    metric: Metric,
    keep: usize,
    options: VectorValidationOptions,
    filter: Option<&SearchFilter>,
    source: ScoreSource,
) -> Result<Vec<SearchHit>, ApiError> {
    validate_query(query, metric, options)?;
    let score_options = VectorValidationOptions {
        strict_finite: false,
        zero_norm_epsilon: options.zero_norm_epsilon,
    };
    let mut heap = BinaryHeap::with_capacity(keep);

    match source {
        ScoreSource::All => {
            for (id, values, payload) in collection.iter_points_with_payload() {
                if !matches_filter(payload, filter)? {
                    continue;
                }
                if let Some(score) =
                    score_point_for_candidate(metric, query, values, score_options)?
                {
                    score_candidate(&mut heap, metric, keep, id, score);
                }
            }
        }
        ScoreSource::CandidateIds(candidate_ids) => {
            for id in candidate_ids {
                let (values, payload) = collection
                    .get_point_record(id)
                    .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;
                if !matches_filter(payload, filter)? {
                    continue;
                }
                if let Some(score) =
                    score_point_for_candidate(metric, query, values, score_options)?
                {
                    score_candidate(&mut heap, metric, keep, id, score);
                }
            }
        }
    }

    let mut scored: Vec<(u64, f32)> = heap
        .into_iter()
        .map(|candidate| (candidate.id, candidate.value))
        .collect();
    sort_scores(&mut scored, metric);

    scored
        .into_iter()
        .map(|(id, value)| {
            let payload = collection
                .get_payload(id)
                .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;
            Ok(SearchHit {
                id,
                value,
                payload: payload_for_response(payload),
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()
}

fn validate_query(
    query: &[f32],
    metric: Metric,
    options: VectorValidationOptions,
) -> Result<(), ApiError> {
    if let Some(index) = query.iter().position(|value| !value.is_finite()) {
        return Err(ApiError::invalid_argument(format!(
            "query contains non-finite value at index {index}"
        )));
    }
    if matches!(metric, Metric::Cosine) {
        let norm_sq = query.iter().fold(0.0f32, |acc, value| acc + value * value);
        if norm_sq <= options.zero_norm_epsilon.max(0.0) {
            return Err(map_vector_error(VectorError::ZeroNorm {
                epsilon: options.zero_norm_epsilon,
            }));
        }
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

fn score_point(
    metric: Metric,
    query: &[f32],
    values: &[f32],
    options: VectorValidationOptions,
) -> Result<f32, VectorError> {
    match metric {
        Metric::Dot => dot_product_with_options(query, values, options),
        Metric::L2 => l2_distance_with_options(query, values, options),
        Metric::Cosine => cosine_similarity_with_options(query, values, options),
    }
}

fn score_point_for_candidate(
    metric: Metric,
    query: &[f32],
    values: &[f32],
    options: VectorValidationOptions,
) -> Result<Option<f32>, ApiError> {
    match score_point(metric, query, values, options) {
        Ok(value) if value.is_finite() => Ok(Some(value)),
        Ok(_) => Ok(None),
        Err(VectorError::ZeroNorm { .. } | VectorError::NonFinite { .. }) => Ok(None),
        Err(error) => Err(map_vector_error(error)),
    }
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

fn score_candidate(
    heap: &mut BinaryHeap<HeapCandidate>,
    metric: Metric,
    keep: usize,
    id: u64,
    value: f32,
) {
    let candidate = HeapCandidate {
        id,
        value,
        rank_key: rank_key(metric, value),
    };
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
