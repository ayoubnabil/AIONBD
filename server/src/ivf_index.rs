use aionbd_core::{l2_squared_with_options, Collection, VectorValidationOptions};

const MIN_INDEXED_POINTS: usize = 2_048;
const MAX_LISTS: usize = 256;
const MIN_LISTS: usize = 8;
const KMEANS_ITERS: usize = 4;
const DEFAULT_NPROBE: usize = 8;

#[derive(Debug, Clone)]
pub(crate) struct IvfIndex {
    dimension: usize,
    len: usize,
    mutation_version: u64,
    nlist: usize,
    nprobe: usize,
    centroids: Vec<Vec<f32>>,
    lists: Vec<Vec<u64>>,
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

        let points: Vec<(u64, &[f32])> = collection.iter_points().collect();
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
        points: &[(u64, Vec<f32>)],
    ) -> Option<Self> {
        let refs: Vec<(u64, &[f32])> = points
            .iter()
            .map(|(id, values)| (*id, values.as_slice()))
            .collect();
        Self::build_from_refs(dimension, len, mutation_version, &refs)
    }

    fn build_from_refs(
        dimension: usize,
        len: usize,
        mutation_version: u64,
        points: &[(u64, &[f32])],
    ) -> Option<Self> {
        if points.len() < MIN_INDEXED_POINTS {
            return None;
        }

        let nlist = choose_nlist(points.len());
        if nlist == 0 {
            return None;
        }

        let mut centroids = initial_centroids(points, nlist);
        let mut assignments = vec![0usize; points.len()];

        for _ in 0..KMEANS_ITERS {
            for (idx, (_, values)) in points.iter().enumerate() {
                assignments[idx] = nearest_centroid(values, &centroids);
            }
            recompute_centroids(points, &assignments, &mut centroids);
        }

        let mut lists = vec![Vec::new(); nlist];
        for (idx, (id, _)) in points.iter().enumerate() {
            lists[assignments[idx]].push(*id);
        }

        Some(Self {
            dimension,
            len,
            mutation_version,
            nlist,
            nprobe: DEFAULT_NPROBE.min(nlist),
            centroids,
            lists,
        })
    }

    pub(crate) fn is_compatible(&self, collection: &Collection) -> bool {
        self.dimension == collection.dimension()
            && self.len == collection.len()
            && self.mutation_version == collection.mutation_version()
    }

    pub(crate) fn candidate_ids(&self, query: &[f32], limit: usize) -> Vec<u64> {
        let mut centroid_scores: Vec<(usize, f32)> = self
            .centroids
            .iter()
            .enumerate()
            .map(|(idx, centroid)| (idx, l2_squared(query, centroid)))
            .collect();

        let required_lists = limit.saturating_mul(self.nlist).div_ceil(self.len.max(1));
        let probe = self.nprobe.max(required_lists).min(self.nlist).max(1);
        if centroid_scores.len() > probe {
            let nth = probe - 1;
            centroid_scores.select_nth_unstable_by(nth, |left, right| left.1.total_cmp(&right.1));
            centroid_scores.truncate(probe);
        }
        centroid_scores.sort_by(|left, right| left.1.total_cmp(&right.1));

        let capacity = centroid_scores
            .iter()
            .map(|(idx, _)| self.lists[*idx].len())
            .sum();
        let mut candidate_ids = Vec::with_capacity(capacity);
        for (centroid_idx, _) in centroid_scores {
            candidate_ids.extend(self.lists[centroid_idx].iter().copied());
        }
        candidate_ids
    }
}

fn choose_nlist(total_points: usize) -> usize {
    let sqrt = (total_points as f64).sqrt().round() as usize;
    sqrt.clamp(MIN_LISTS, MAX_LISTS).min(total_points)
}

fn initial_centroids(points: &[(u64, &[f32])], nlist: usize) -> Vec<Vec<f32>> {
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

fn seeded_start_index(points: &[(u64, &[f32])]) -> usize {
    let stride = (points.len() / 64).max(1);
    let mut hash = 0x9E37_79B9_7F4A_7C15_u64 ^ points.len() as u64;
    for (id, values) in points.iter().step_by(stride) {
        hash ^= id.wrapping_mul(0xBF58_476D_1CE4_E5B9);
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
    points: &[(u64, &[f32])],
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
    l2_squared_with_options(left, right, VectorValidationOptions::permissive())
        .unwrap_or(f32::INFINITY)
}

#[cfg(test)]
mod tests {
    use aionbd_core::CollectionConfig;

    use super::*;

    #[test]
    fn index_becomes_incompatible_for_same_len_updates() {
        let mut collection = Collection::new(
            "demo",
            CollectionConfig::new(2, true).expect("config should be valid"),
        )
        .expect("collection should be valid");
        for id in 0..MIN_INDEXED_POINTS as u64 {
            collection
                .upsert_point(id, vec![id as f32, 0.0])
                .expect("upsert should succeed");
        }

        let index = IvfIndex::build(&collection).expect("index should build");
        assert!(index.is_compatible(&collection));

        collection
            .upsert_point(1, vec![1234.0, 0.0])
            .expect("update should succeed");
        assert!(!index.is_compatible(&collection));
    }

    #[test]
    fn index_becomes_incompatible_when_collection_len_changes() {
        let mut collection = Collection::new(
            "demo",
            CollectionConfig::new(2, true).expect("config should be valid"),
        )
        .expect("collection should be valid");
        for id in 0..MIN_INDEXED_POINTS as u64 {
            collection
                .upsert_point(id, vec![id as f32, 0.0])
                .expect("upsert should succeed");
        }

        let index = IvfIndex::build(&collection).expect("index should build");
        collection
            .upsert_point(MIN_INDEXED_POINTS as u64 + 1, vec![0.0, 0.0])
            .expect("insert should succeed");
        assert!(!index.is_compatible(&collection));
    }

    #[test]
    fn candidate_ids_reduce_search_space() {
        let mut collection = Collection::new(
            "demo",
            CollectionConfig::new(2, true).expect("config should be valid"),
        )
        .expect("collection should be valid");
        for id in 0..MIN_INDEXED_POINTS as u64 {
            let cluster_shift = if id < (MIN_INDEXED_POINTS / 2) as u64 {
                0.0
            } else {
                1_000.0
            };
            collection
                .upsert_point(id, vec![cluster_shift + (id % 10) as f32, 0.0])
                .expect("upsert should succeed");
        }

        let index = IvfIndex::build(&collection).expect("index should build");
        let candidate_ids = index.candidate_ids(&[1_005.0, 0.0], 10);
        assert!(!candidate_ids.is_empty());
        assert!(candidate_ids.len() < collection.len());
    }
}
