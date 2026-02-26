use std::sync::OnceLock;

use aionbd_core::{Collection, PreparedL2Query};
use rayon::prelude::*;

use crate::models::{Metric, SearchFilter, SearchHit};

#[path = "engine_batch_exact_transposed.rs"]
mod transposed;

const EXACT_BATCH_SMALL_TOPK_LIMIT: usize = 64;
const EXACT_BATCH_PARALLEL_MIN_POINTS: usize = 2_048;
const EXACT_BATCH_PARALLEL_MIN_WORK: usize = 2_000_000;
const EXACT_BATCH_TRANSPOSE_MIN_QUERIES: usize = 160;
static EXACT_BATCH_SMALL_TOPK_LIMIT_CACHE: OnceLock<usize> = OnceLock::new();
static EXACT_BATCH_TRANSPOSE_MIN_QUERIES_CACHE: OnceLock<usize> = OnceLock::new();

fn exact_batch_small_topk_limit() -> usize {
    *EXACT_BATCH_SMALL_TOPK_LIMIT_CACHE.get_or_init(|| {
        std::env::var("AIONBD_EXACT_BATCH_SMALL_TOPK_LIMIT")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .unwrap_or(EXACT_BATCH_SMALL_TOPK_LIMIT)
    })
}

fn exact_batch_transpose_min_queries() -> usize {
    *EXACT_BATCH_TRANSPOSE_MIN_QUERIES_CACHE.get_or_init(|| {
        std::env::var("AIONBD_EXACT_BATCH_TRANSPOSE_MIN_QUERIES")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|value| *value > 1)
            .unwrap_or(EXACT_BATCH_TRANSPOSE_MIN_QUERIES)
    })
}

pub(super) fn should_use_exact_batch_fast_path(
    metric: Metric,
    filter: Option<&SearchFilter>,
    keep: usize,
    query_count: usize,
) -> bool {
    let fast_path_limit = exact_batch_small_topk_limit();
    matches!(metric, Metric::L2)
        && filter.is_none()
        && fast_path_limit > 0
        && keep <= fast_path_limit.min(EXACT_BATCH_SMALL_TOPK_LIMIT)
        && query_count > 1
}

pub(super) fn score_exact_l2_batch_small_topk(
    collection: &Collection,
    queries: &[Vec<f32>],
    keep: usize,
) -> Vec<Vec<SearchHit>> {
    let assume_finite = collection.strict_finite();
    let use_parallel = collection.slots_dense()
        && should_parallel_exact_l2_batch(
            collection.slot_count(),
            collection.dimension(),
            queries.len(),
        );

    if queries.len() >= exact_batch_transpose_min_queries() {
        return transposed::score_exact_l2_batch_small_topk_transposed(
            collection,
            queries,
            keep,
            assume_finite,
            use_parallel,
        );
    }

    let prepared_queries: Vec<PreparedL2Query> = queries
        .iter()
        .map(|query| PreparedL2Query::new(query))
        .collect();

    if use_parallel {
        return score_exact_l2_batch_small_topk_parallel(
            collection,
            &prepared_queries,
            keep,
            assume_finite,
        );
    }

    let mut topks: Vec<BatchSmallTopK> = (0..queries.len())
        .map(|_| BatchSmallTopK::new(keep))
        .collect();
    if collection.slots_dense() {
        for slot in 0..collection.slot_count() {
            let (id, values) = collection.point_at_dense_slot(slot);
            score_point_for_all_queries(&prepared_queries, &mut topks, id, values, assume_finite);
        }
    } else {
        for (id, values) in collection.iter_points_unordered() {
            score_point_for_all_queries(&prepared_queries, &mut topks, id, values, assume_finite);
        }
    }

    topks.iter().map(build_small_topk_hits).collect()
}

fn should_parallel_exact_l2_batch(points: usize, dimension: usize, query_count: usize) -> bool {
    if points < EXACT_BATCH_PARALLEL_MIN_POINTS {
        return false;
    }
    points
        .saturating_mul(dimension)
        .saturating_mul(query_count.max(1))
        >= EXACT_BATCH_PARALLEL_MIN_WORK
}

fn score_exact_l2_batch_small_topk_parallel(
    collection: &Collection,
    prepared_queries: &[PreparedL2Query],
    keep: usize,
    assume_finite: bool,
) -> Vec<Vec<SearchHit>> {
    let slot_count = collection.slot_count();
    let query_count = prepared_queries.len();
    let threads = rayon::current_num_threads().max(1);
    let chunk_len = (slot_count / threads).max(128);
    let ranges = chunk_ranges(slot_count, chunk_len);

    let partial_topks: Vec<Vec<BatchSmallTopK>> = ranges
        .into_par_iter()
        .map(|(start, end)| {
            let mut local_topks: Vec<BatchSmallTopK> = (0..query_count)
                .map(|_| BatchSmallTopK::new(keep))
                .collect();
            for slot in start..end {
                let (id, values) = collection.point_at_dense_slot(slot);
                score_point_for_all_queries(
                    prepared_queries,
                    &mut local_topks,
                    id,
                    values,
                    assume_finite,
                );
            }
            local_topks
        })
        .collect();

    let merged_topks: Vec<BatchSmallTopK> = (0..query_count)
        .into_par_iter()
        .map(|query_idx| {
            let mut merged = BatchSmallTopK::new(keep);
            for local_topks in &partial_topks {
                let local = &local_topks[query_idx];
                for idx in 0..local.len {
                    merged.push(local.ids[idx], local.scores[idx]);
                }
            }
            merged
        })
        .collect();

    merged_topks.iter().map(build_small_topk_hits).collect()
}

fn chunk_ranges(len: usize, chunk_len: usize) -> Vec<(usize, usize)> {
    let mut ranges = Vec::with_capacity(len.div_ceil(chunk_len.max(1)));
    let mut start = 0usize;
    let chunk_len = chunk_len.max(1);
    while start < len {
        let end = start.saturating_add(chunk_len).min(len);
        ranges.push((start, end));
        start = end;
    }
    ranges
}

fn score_point_for_all_queries(
    prepared_queries: &[PreparedL2Query],
    topks: &mut [BatchSmallTopK],
    id: u64,
    values: &[f32],
    assume_finite: bool,
) {
    for (prepared, topk) in prepared_queries.iter().zip(topks.iter_mut()) {
        let score = prepared.l2_squared(values);
        if !assume_finite && !score.is_finite() {
            continue;
        }
        topk.push(id, score);
    }
}

fn build_small_topk_hits(topk: &BatchSmallTopK) -> Vec<SearchHit> {
    let mut order = [0usize; BATCH_SMALL_TOPK_STACK_CAPACITY];
    for (idx, slot) in order.iter_mut().enumerate().take(topk.len) {
        *slot = idx;
    }
    order[..topk.len].sort_unstable_by(|left, right| {
        topk.scores[*left]
            .total_cmp(&topk.scores[*right])
            .then_with(|| topk.ids[*left].cmp(&topk.ids[*right]))
    });

    let mut hits = Vec::with_capacity(topk.len);
    for idx in order[..topk.len].iter().copied() {
        hits.push(SearchHit {
            id: topk.ids[idx],
            value: topk.scores[idx].sqrt(),
            payload: None,
        });
    }
    hits
}

const BATCH_SMALL_TOPK_STACK_CAPACITY: usize = EXACT_BATCH_SMALL_TOPK_LIMIT;

struct BatchSmallTopK {
    keep: usize,
    len: usize,
    ids: [u64; BATCH_SMALL_TOPK_STACK_CAPACITY],
    scores: [f32; BATCH_SMALL_TOPK_STACK_CAPACITY],
    worst_idx: usize,
}

impl BatchSmallTopK {
    fn new(keep: usize) -> Self {
        debug_assert!(keep <= BATCH_SMALL_TOPK_STACK_CAPACITY);
        Self {
            keep,
            len: 0,
            ids: [0u64; BATCH_SMALL_TOPK_STACK_CAPACITY],
            scores: [0.0f32; BATCH_SMALL_TOPK_STACK_CAPACITY],
            worst_idx: 0,
        }
    }

    fn push(&mut self, id: u64, score: f32) {
        if self.len < self.keep {
            self.ids[self.len] = id;
            self.scores[self.len] = score;
            self.len += 1;
            if self.len == self.keep {
                self.recompute_worst();
            }
            return;
        }
        let worst_idx = self.worst_idx;
        let worst_id = self.ids[worst_idx];
        let worst_score = self.scores[worst_idx];
        if !is_better_candidate(score, id, worst_score, worst_id) {
            return;
        }
        self.ids[worst_idx] = id;
        self.scores[worst_idx] = score;
        self.recompute_worst();
    }

    fn recompute_worst(&mut self) {
        let mut worst_idx = 0usize;
        for idx in 1..self.len {
            let current_score = self.scores[idx];
            let current_id = self.ids[idx];
            let worst_score = self.scores[worst_idx];
            let worst_id = self.ids[worst_idx];
            if is_worse_candidate(current_score, current_id, worst_score, worst_id) {
                worst_idx = idx;
            }
        }
        self.worst_idx = worst_idx;
    }
}

fn is_better_candidate(score: f32, id: u64, other_score: f32, other_id: u64) -> bool {
    match score.total_cmp(&other_score) {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Equal => id < other_id,
        std::cmp::Ordering::Greater => false,
    }
}

fn is_worse_candidate(score: f32, id: u64, other_score: f32, other_id: u64) -> bool {
    match score.total_cmp(&other_score) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Equal => id > other_id,
        std::cmp::Ordering::Less => false,
    }
}
