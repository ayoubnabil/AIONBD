use aionbd_core::{l2_squared_unchecked, Collection};
use rayon::prelude::*;
use std::sync::OnceLock;

const MIN_INDEXED_POINTS: usize = 2_048;
const MAX_LISTS: usize = 256;
const MIN_LISTS: usize = 8;
const KMEANS_ITERS: usize = 4;
const KMEANS_MAX_TRAINING_POINTS: usize = 8_192;
const DEFAULT_NPROBE: usize = 8;
static IVF_NPROBE_DEFAULT_CACHE: OnceLock<usize> = OnceLock::new();
static IVF_KMEANS_MAX_TRAINING_POINTS_CACHE: OnceLock<usize> = OnceLock::new();

#[derive(Debug, Clone)]
pub(crate) struct IvfIndex {
    dimension: usize,
    len: usize,
    mutation_version: u64,
    nlist: usize,
    nprobe: usize,
    centroids: Vec<Vec<f32>>,
    lists: Vec<Vec<usize>>,
}

impl IvfIndex {
    pub(crate) fn min_indexed_points() -> usize {
        MIN_INDEXED_POINTS
    }

    #[cfg(test)]
    pub(crate) fn build(collection: &Collection) -> Option<Self> {
        if collection.len() < MIN_INDEXED_POINTS {
            return None;
        }

        let points: Vec<(usize, &[f32])> = (0..collection.slot_count())
            .filter_map(|slot| {
                collection
                    .point_at_slot(slot)
                    .map(|(_, values)| (slot, values))
            })
            .collect();
        Self::build_from_refs(
            collection.dimension(),
            collection.len(),
            collection.mutation_version(),
            &points,
        )
    }

    pub(crate) fn build_from_snapshot(
        dimension: usize,
        len: usize,
        mutation_version: u64,
        points: &[(usize, Vec<f32>)],
    ) -> Option<Self> {
        let refs: Vec<(usize, &[f32])> = points
            .iter()
            .map(|(slot, values)| (*slot, values.as_slice()))
            .collect();
        Self::build_from_refs(dimension, len, mutation_version, &refs)
    }

    fn build_from_refs(
        dimension: usize,
        len: usize,
        mutation_version: u64,
        points: &[(usize, &[f32])],
    ) -> Option<Self> {
        if points.len() < MIN_INDEXED_POINTS {
            return None;
        }

        let nlist = choose_nlist(points.len());
        if nlist == 0 {
            return None;
        }

        let training_points =
            sample_points_for_training(points, configured_kmeans_max_training_points());
        let mut centroids = initial_centroids(&training_points, nlist);
        let mut training_assignments = vec![0usize; training_points.len()];

        for _ in 0..KMEANS_ITERS {
            for (idx, (_, values)) in training_points.iter().enumerate() {
                training_assignments[idx] = nearest_centroid(values, &centroids);
            }
            recompute_centroids(&training_points, &training_assignments, &mut centroids);
        }

        let assignments: Vec<usize> = points
            .par_iter()
            .map(|(_, values)| nearest_centroid(values, &centroids))
            .collect();

        let mut lists = vec![Vec::new(); nlist];
        for (idx, (slot, _)) in points.iter().enumerate() {
            lists[assignments[idx]].push(*slot);
        }

        Some(Self {
            dimension,
            len,
            mutation_version,
            nlist,
            nprobe: configured_default_nprobe().min(nlist).max(1),
            centroids,
            lists,
        })
    }

    pub(crate) fn is_compatible(&self, collection: &Collection) -> bool {
        self.dimension == collection.dimension()
            && self.len == collection.len()
            && self.mutation_version == collection.mutation_version()
    }

    #[allow(dead_code)]
    pub(crate) fn candidate_slots_with_target_recall(
        &self,
        query: &[f32],
        limit: usize,
        target_recall: Option<f32>,
    ) -> Vec<usize> {
        let centroid_ids = self.centroid_ids_with_target_recall(query, limit, target_recall);
        let capacity = self.candidate_slot_count(&centroid_ids);
        let mut candidate_slots = Vec::with_capacity(capacity);
        for centroid_idx in centroid_ids {
            candidate_slots.extend(self.lists[centroid_idx].iter().copied());
        }
        candidate_slots
    }

    pub(crate) fn centroid_ids_with_target_recall(
        &self,
        query: &[f32],
        limit: usize,
        target_recall: Option<f32>,
    ) -> Vec<usize> {
        let mut centroid_scores: Vec<(usize, f32)> = self
            .centroids
            .iter()
            .enumerate()
            .map(|(idx, centroid)| (idx, l2_squared(query, centroid)))
            .collect();

        let probe = self.probe_for_request(limit, target_recall);
        if centroid_scores.len() > probe {
            let nth = probe - 1;
            centroid_scores.select_nth_unstable_by(nth, |left, right| left.1.total_cmp(&right.1));
            centroid_scores.truncate(probe);
        }

        centroid_scores.into_iter().map(|(idx, _)| idx).collect()
    }

    pub(crate) fn candidate_slot_count(&self, centroid_ids: &[usize]) -> usize {
        centroid_ids
            .iter()
            .map(|centroid_idx| self.lists[*centroid_idx].len())
            .sum()
    }

    pub(crate) fn slots_for_centroid(&self, centroid_idx: usize) -> Option<&[usize]> {
        self.lists.get(centroid_idx).map(Vec::as_slice)
    }

    fn probe_for_request(&self, limit: usize, target_recall: Option<f32>) -> usize {
        let required_lists = limit.saturating_mul(self.nlist).div_ceil(self.len.max(1));
        let mut probe = self.nprobe.max(required_lists).min(self.nlist).max(1);

        if let Some(target_recall) = target_recall {
            let target_lists =
                ((self.nlist as f32) * target_recall.clamp(0.0, 1.0)).ceil() as usize;
            probe = probe.max(target_lists.max(1)).min(self.nlist);
        }

        probe
    }
}

fn configured_default_nprobe() -> usize {
    *IVF_NPROBE_DEFAULT_CACHE.get_or_init(|| {
        std::env::var("AIONBD_IVF_NPROBE_DEFAULT")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_NPROBE)
    })
}

fn configured_kmeans_max_training_points() -> usize {
    *IVF_KMEANS_MAX_TRAINING_POINTS_CACHE.get_or_init(|| {
        std::env::var("AIONBD_IVF_KMEANS_MAX_TRAINING_POINTS")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(KMEANS_MAX_TRAINING_POINTS)
    })
}

fn choose_nlist(total_points: usize) -> usize {
    let sqrt = (total_points as f64).sqrt().round() as usize;
    sqrt.clamp(MIN_LISTS, MAX_LISTS).min(total_points)
}

fn sample_points_for_training<'a>(
    points: &'a [(usize, &'a [f32])],
    max_points: usize,
) -> Vec<(usize, &'a [f32])> {
    if points.len() <= max_points {
        return points.to_vec();
    }
    let step = points.len().div_ceil(max_points);
    points.iter().step_by(step.max(1)).copied().collect()
}

fn initial_centroids(points: &[(usize, &[f32])], nlist: usize) -> Vec<Vec<f32>> {
    let mut centroids = Vec::with_capacity(nlist);
    let mut selected = vec![false; points.len()];

    let first_idx = seeded_start_index(points);
    centroids.push(points[first_idx].1.to_vec());
    selected[first_idx] = true;

    while centroids.len() < nlist {
        let mut best_idx = None;
        let mut best_distance = f32::NEG_INFINITY;

        for (idx, (_, values)) in points.iter().enumerate() {
            if selected[idx] {
                continue;
            }
            let nearest_distance = centroids
                .iter()
                .map(|centroid| l2_squared(values, centroid))
                .fold(f32::INFINITY, f32::min);
            if nearest_distance > best_distance {
                best_distance = nearest_distance;
                best_idx = Some(idx);
            }
        }

        let source_idx = best_idx
            .or_else(|| selected.iter().position(|flag| !*flag))
            .unwrap_or(0);
        selected[source_idx] = true;
        centroids.push(points[source_idx].1.to_vec());
    }
    centroids
}

fn seeded_start_index(points: &[(usize, &[f32])]) -> usize {
    let stride = (points.len() / 64).max(1);
    let mut hash = 0x9E37_79B9_7F4A_7C15_u64 ^ points.len() as u64;
    for (slot, values) in points.iter().step_by(stride) {
        hash ^= (*slot as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        if let Some(value) = values.first() {
            hash ^= (value.to_bits() as u64).wrapping_mul(0x94D0_49BB_1331_11EB);
        }
        hash = hash.rotate_left(17).wrapping_mul(0x9E37_79B1_85EB_CA87);
    }
    (hash as usize) % points.len()
}

fn nearest_centroid(values: &[f32], centroids: &[Vec<f32>]) -> usize {
    let mut best_idx = 0usize;
    let mut best_dist = l2_squared(values, &centroids[0]);
    for (idx, centroid) in centroids.iter().enumerate().skip(1) {
        let distance = l2_squared(values, centroid);
        if distance < best_dist {
            best_dist = distance;
            best_idx = idx;
        }
    }
    best_idx
}

fn recompute_centroids(
    points: &[(usize, &[f32])],
    assignments: &[usize],
    centroids: &mut [Vec<f32>],
) {
    let dimension = centroids[0].len();
    let mut sums = vec![vec![0.0f32; dimension]; centroids.len()];
    let mut counts = vec![0usize; centroids.len()];

    for (idx, (_, values)) in points.iter().enumerate() {
        let centroid_idx = assignments[idx];
        counts[centroid_idx] += 1;
        for (dim, value) in values.iter().enumerate() {
            sums[centroid_idx][dim] += *value;
        }
    }

    for (centroid_idx, centroid) in centroids.iter_mut().enumerate() {
        let count = counts[centroid_idx];
        if count == 0 {
            continue;
        }

        for dim in 0..dimension {
            centroid[dim] = sums[centroid_idx][dim] / count as f32;
        }
    }
}

fn l2_squared(left: &[f32], right: &[f32]) -> f32 {
    debug_assert_eq!(left.len(), right.len());
    if left.len() != right.len() || left.is_empty() {
        return f32::INFINITY;
    }
    l2_squared_unchecked(left, right)
}

#[cfg(test)]
mod tests;
