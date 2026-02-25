const KMEANS_ITERS: usize = 4;
const MIN_LISTS: usize = 8;
const MAX_LISTS: usize = 256;
const DEFAULT_NPROBE: usize = 8;

#[derive(Debug, Clone)]
pub(crate) struct IvfBenchIndex {
    len: usize,
    nlist: usize,
    nprobe: usize,
    centroids: Vec<Vec<f32>>,
    lists: Vec<Vec<usize>>,
}

impl IvfBenchIndex {
    pub(crate) fn build(points: &[(u64, Vec<f32>)]) -> Option<Self> {
        if points.is_empty() {
            return None;
        }

        let nlist = choose_nlist(points.len());
        let mut centroids = initial_centroids(points, nlist);
        let mut assignments = vec![0usize; points.len()];

        for _ in 0..KMEANS_ITERS {
            for (point_idx, (_, values)) in points.iter().enumerate() {
                assignments[point_idx] = nearest_centroid(values, &centroids);
            }
            recompute_centroids(points, &assignments, &mut centroids);
        }

        let mut lists = vec![Vec::new(); nlist];
        for (point_idx, centroid_idx) in assignments.into_iter().enumerate() {
            lists[centroid_idx].push(point_idx);
        }

        Some(Self {
            len: points.len(),
            nlist,
            nprobe: DEFAULT_NPROBE.min(nlist),
            centroids,
            lists,
        })
    }

    pub(crate) fn candidate_indices(&self, query: &[f32], limit: usize) -> Vec<usize> {
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
            .map(|(centroid_idx, _)| self.lists[*centroid_idx].len())
            .sum();
        let mut candidates = Vec::with_capacity(capacity);
        for (centroid_idx, _) in centroid_scores {
            candidates.extend(self.lists[centroid_idx].iter().copied());
        }
        candidates
    }

    pub(crate) fn estimated_memory_bytes(&self) -> usize {
        let centroid_bytes: usize = self
            .centroids
            .iter()
            .map(|centroid| centroid.len() * std::mem::size_of::<f32>())
            .sum();
        let list_bytes: usize = self
            .lists
            .iter()
            .map(|list| list.len() * std::mem::size_of::<usize>())
            .sum();
        centroid_bytes + list_bytes
    }
}

fn choose_nlist(total_points: usize) -> usize {
    let sqrt = (total_points as f64).sqrt().round() as usize;
    sqrt.clamp(MIN_LISTS, MAX_LISTS).min(total_points)
}

fn initial_centroids(points: &[(u64, Vec<f32>)], nlist: usize) -> Vec<Vec<f32>> {
    let mut centroids = Vec::with_capacity(nlist);
    for slot in 0..nlist {
        let source_idx = slot.saturating_mul(points.len()) / nlist;
        let (_, values) = &points[source_idx];
        centroids.push(values.clone());
    }
    centroids
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
    points: &[(u64, Vec<f32>)],
    assignments: &[usize],
    centroids: &mut [Vec<f32>],
) {
    let dimension = centroids[0].len();
    let mut sums = vec![vec![0.0f32; dimension]; centroids.len()];
    let mut counts = vec![0usize; centroids.len()];

    for (point_idx, (_, values)) in points.iter().enumerate() {
        let centroid_idx = assignments[point_idx];
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
    left.iter()
        .zip(right.iter())
        .map(|(left, right)| {
            let delta = left - right;
            delta * delta
        })
        .sum()
}
