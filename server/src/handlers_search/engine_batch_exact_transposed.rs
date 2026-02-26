use aionbd_core::Collection;
use rayon::prelude::*;
use wide::f32x8;

use crate::models::SearchHit;

const QUERY_SIMD_WIDTH: usize = 8;

pub(super) fn score_exact_l2_batch_small_topk_transposed(
    collection: &Collection,
    queries: &[Vec<f32>],
    keep: usize,
    assume_finite: bool,
    use_parallel: bool,
) -> Vec<Vec<SearchHit>> {
    let prepared = BatchPreparedL2::new(queries);
    if use_parallel {
        return score_parallel(collection, &prepared, keep, assume_finite);
    }

    let mut topks: Vec<BatchSmallTopK> = (0..prepared.query_count)
        .map(|_| BatchSmallTopK::new(keep))
        .collect();
    let mut dots_simd = vec![f32x8::ZERO; prepared.query_simd_chunks];
    let mut dots_tail = vec![0.0f32; prepared.query_tail_len];
    if collection.slots_dense() {
        for slot in 0..collection.slot_count() {
            let (id, values) = collection.point_at_dense_slot(slot);
            score_point_for_all_queries(
                &prepared,
                &mut topks,
                id,
                values,
                assume_finite,
                &mut dots_simd,
                &mut dots_tail,
            );
        }
    } else {
        for (id, values) in collection.iter_points_unordered() {
            score_point_for_all_queries(
                &prepared,
                &mut topks,
                id,
                values,
                assume_finite,
                &mut dots_simd,
                &mut dots_tail,
            );
        }
    }

    topks.iter().map(build_small_topk_hits).collect()
}

fn score_parallel(
    collection: &Collection,
    prepared: &BatchPreparedL2,
    keep: usize,
    assume_finite: bool,
) -> Vec<Vec<SearchHit>> {
    let slot_count = collection.slot_count();
    let query_count = prepared.query_count;
    let threads = rayon::current_num_threads().max(1);
    let chunk_len = (slot_count / threads).max(128);
    let ranges = chunk_ranges(slot_count, chunk_len);

    let partial_topks: Vec<Vec<BatchSmallTopK>> = ranges
        .into_par_iter()
        .map(|(start, end)| {
            let mut local_topks: Vec<BatchSmallTopK> = (0..query_count)
                .map(|_| BatchSmallTopK::new(keep))
                .collect();
            let mut dots_simd = vec![f32x8::ZERO; prepared.query_simd_chunks];
            let mut dots_tail = vec![0.0f32; prepared.query_tail_len];
            for slot in start..end {
                let (id, values) = collection.point_at_dense_slot(slot);
                score_point_for_all_queries(
                    prepared,
                    &mut local_topks,
                    id,
                    values,
                    assume_finite,
                    &mut dots_simd,
                    &mut dots_tail,
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
    prepared: &BatchPreparedL2,
    topks: &mut [BatchSmallTopK],
    id: u64,
    values: &[f32],
    assume_finite: bool,
    dots_simd: &mut [f32x8],
    dots_tail: &mut [f32],
) {
    let candidate_sq = prepared.accumulate_dots_and_candidate_sq(values, dots_simd, dots_tail);
    for (chunk_idx, dot_chunk) in dots_simd.iter().enumerate() {
        let dot_values = dot_chunk.to_array();
        let base = chunk_idx * QUERY_SIMD_WIDTH;
        for (lane, dot) in dot_values.iter().copied().enumerate() {
            let idx = base + lane;
            let score = prepared.query_sq_sums[idx] + candidate_sq - 2.0 * dot;
            if !assume_finite && !score.is_finite() {
                continue;
            }
            topks[idx].push(id, score);
        }
    }
    let tail_base = prepared.query_simd_chunks * QUERY_SIMD_WIDTH;
    for (tail_idx, dot) in dots_tail.iter().copied().enumerate() {
        let idx = tail_base + tail_idx;
        let score = prepared.query_sq_sums[idx] + candidate_sq - 2.0 * dot;
        if !assume_finite && !score.is_finite() {
            continue;
        }
        topks[idx].push(id, score);
    }
}

struct BatchPreparedL2 {
    query_count: usize,
    query_simd_chunks: usize,
    query_tail_len: usize,
    query_sq_sums: Vec<f32>,
    transposed: Vec<f32>,
}

impl BatchPreparedL2 {
    fn new(queries: &[Vec<f32>]) -> Self {
        let query_count = queries.len();
        let query_simd_chunks = query_count / QUERY_SIMD_WIDTH;
        let query_tail_len = query_count % QUERY_SIMD_WIDTH;
        let dimension = queries.first().map_or(0, Vec::len);
        let mut query_sq_sums = vec![0.0f32; query_count];
        let mut transposed = vec![0.0f32; dimension.saturating_mul(query_count)];

        for (query_idx, query) in queries.iter().enumerate() {
            let mut sum = 0.0f32;
            for (dim, value) in query.iter().copied().enumerate() {
                sum += value * value;
                transposed[dim * query_count + query_idx] = value;
            }
            query_sq_sums[query_idx] = sum;
        }

        Self {
            query_count,
            query_simd_chunks,
            query_tail_len,
            query_sq_sums,
            transposed,
        }
    }

    fn accumulate_dots_and_candidate_sq(
        &self,
        values: &[f32],
        dots_simd: &mut [f32x8],
        dots_tail: &mut [f32],
    ) -> f32 {
        dots_simd.fill(f32x8::ZERO);
        dots_tail.fill(0.0);
        let mut candidate_sq = 0.0f32;
        for (dim, value) in values.iter().copied().enumerate() {
            candidate_sq += value * value;
            let row = &self.transposed[dim * self.query_count..(dim + 1) * self.query_count];
            let query_value = f32x8::splat(value);
            let mut offset = 0usize;
            for dot in dots_simd.iter_mut() {
                let query_row = load_f32x8(&row[offset..offset + QUERY_SIMD_WIDTH]);
                *dot += query_row * query_value;
                offset += QUERY_SIMD_WIDTH;
            }
            for (dot, query_value) in dots_tail.iter_mut().zip(row[offset..].iter().copied()) {
                *dot += query_value * value;
            }
        }
        candidate_sq
    }
}

fn load_f32x8(values: &[f32]) -> f32x8 {
    debug_assert_eq!(values.len(), QUERY_SIMD_WIDTH);
    f32x8::new([
        values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7],
    ])
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

const BATCH_SMALL_TOPK_STACK_CAPACITY: usize = 64;

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
