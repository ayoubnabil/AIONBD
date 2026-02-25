use std::time::Instant;

use aionbd_core::l2_distance;

use crate::search_quality_ivf::IvfBenchIndex;

const TOPK: usize = 10;
const QUERY_COUNT: usize = 128;

#[derive(Clone, Copy)]
enum DatasetKind {
    Uniform,
    Clustered,
}

#[derive(Clone, Copy)]
struct DatasetSpec {
    name: &'static str,
    points: usize,
    dimension: usize,
    kind: DatasetKind,
}

#[derive(Clone, Copy)]
enum SearchMode {
    Exact,
    Ivf,
    Auto,
}

impl SearchMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Ivf => "ivf",
            Self::Auto => "auto",
        }
    }
}

struct BenchRow {
    dataset: &'static str,
    mode: SearchMode,
    recall_at_k: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    memory_bytes: usize,
}

pub(crate) fn run_search_quality_bench(mode: &str) -> bool {
    let specs = [
        DatasetSpec {
            name: "uniform_20000_d128",
            points: 20_000,
            dimension: 128,
            kind: DatasetKind::Uniform,
        },
        DatasetSpec {
            name: "clustered_20000_d128",
            points: 20_000,
            dimension: 128,
            kind: DatasetKind::Clustered,
        },
    ];

    println!("bench=search_quality_perf mode={mode} query_count={QUERY_COUNT} topk={TOPK}");

    for spec in specs {
        let Some(rows) = run_dataset(spec) else {
            return false;
        };

        println!("dataset={}", spec.name);
        println!("| dataset | mode | recall@{TOPK} | p50_ms | p95_ms | p99_ms | memory_mb |");
        println!("|---|---:|---:|---:|---:|---:|---:|");
        for row in rows {
            println!(
                "| {} | {} | {:.4} | {:.6} | {:.6} | {:.6} | {:.3} |",
                row.dataset,
                row.mode.as_str(),
                row.recall_at_k,
                row.p50_ms,
                row.p95_ms,
                row.p99_ms,
                row.memory_bytes as f64 / (1024.0 * 1024.0)
            );
            println!(
                "bench=search_quality_row dataset={} mode={} recall_at_k={:.6} p50_ms={:.6} p95_ms={:.6} p99_ms={:.6} memory_bytes={}",
                row.dataset,
                row.mode.as_str(),
                row.recall_at_k,
                row.p50_ms,
                row.p95_ms,
                row.p99_ms,
                row.memory_bytes
            );
        }
    }

    true
}

fn run_dataset(spec: DatasetSpec) -> Option<Vec<BenchRow>> {
    let points = generate_points(spec);
    let queries = generate_queries(spec);
    let exact_references: Vec<Vec<(u64, f32)>> = queries
        .iter()
        .map(|query| exact_topk(&points, query, TOPK))
        .collect::<Option<Vec<_>>>()?;

    let ivf_index = IvfBenchIndex::build(&points)?;
    let dataset_memory_bytes = points.len() * (spec.dimension * std::mem::size_of::<f32>() + 8);
    let index_memory_bytes = ivf_index.estimated_memory_bytes();

    let mut rows = Vec::new();
    for mode in [SearchMode::Exact, SearchMode::Ivf, SearchMode::Auto] {
        let mut latencies_ms = Vec::with_capacity(queries.len());
        let mut recall_sum = 0.0f64;

        for (query_idx, query) in queries.iter().enumerate() {
            let started = Instant::now();
            let measured = match mode {
                SearchMode::Exact => exact_topk(&points, query, TOPK)?,
                SearchMode::Ivf | SearchMode::Auto => ivf_topk(&points, &ivf_index, query, TOPK)?,
            };
            latencies_ms.push(started.elapsed().as_secs_f64() * 1_000.0);
            recall_sum += recall_at_k(&measured, &exact_references[query_idx], TOPK);
        }

        let (p50_ms, p95_ms, p99_ms) = summarize_percentiles(&latencies_ms);
        let memory_bytes = match mode {
            SearchMode::Exact => dataset_memory_bytes,
            SearchMode::Ivf | SearchMode::Auto => dataset_memory_bytes + index_memory_bytes,
        };
        rows.push(BenchRow {
            dataset: spec.name,
            mode,
            recall_at_k: recall_sum / queries.len() as f64,
            p50_ms,
            p95_ms,
            p99_ms,
            memory_bytes,
        });
    }

    Some(rows)
}

fn generate_points(spec: DatasetSpec) -> Vec<(u64, Vec<f32>)> {
    match spec.kind {
        DatasetKind::Uniform => (0..spec.points)
            .map(|id| (id as u64, deterministic_vector(id as u64, spec.dimension)))
            .collect(),
        DatasetKind::Clustered => (0..spec.points)
            .map(|id| {
                let cluster = (id % 8) as f32;
                let mut values = deterministic_vector((id * 17) as u64, spec.dimension);
                for value in &mut values {
                    *value += cluster * 0.25;
                }
                (id as u64, values)
            })
            .collect(),
    }
}

fn generate_queries(spec: DatasetSpec) -> Vec<Vec<f32>> {
    match spec.kind {
        DatasetKind::Uniform => (0..QUERY_COUNT)
            .map(|idx| deterministic_vector((idx * 101 + 7) as u64, spec.dimension))
            .collect(),
        DatasetKind::Clustered => (0..QUERY_COUNT)
            .map(|idx| {
                let cluster = (idx % 8) as f32;
                let mut query = deterministic_vector((idx * 53 + 11) as u64, spec.dimension);
                for value in &mut query {
                    *value += cluster * 0.25;
                }
                query
            })
            .collect(),
    }
}

fn exact_topk(points: &[(u64, Vec<f32>)], query: &[f32], limit: usize) -> Option<Vec<(u64, f32)>> {
    let mut scored = Vec::with_capacity(points.len());
    for (id, values) in points {
        let distance = l2_distance(query, values).ok()?;
        scored.push((*id, distance));
    }
    scored.sort_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    scored.truncate(limit.min(scored.len()));
    Some(scored)
}

fn ivf_topk(
    points: &[(u64, Vec<f32>)],
    index: &IvfBenchIndex,
    query: &[f32],
    limit: usize,
) -> Option<Vec<(u64, f32)>> {
    let mut scored = Vec::new();
    for point_idx in index.candidate_indices(query, limit) {
        let (id, values) = &points[point_idx];
        let distance = l2_distance(query, values).ok()?;
        scored.push((*id, distance));
    }
    scored.sort_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    scored.truncate(limit.min(scored.len()));
    Some(scored)
}

fn recall_at_k(measured: &[(u64, f32)], exact: &[(u64, f32)], k: usize) -> f64 {
    if k == 0 || exact.is_empty() {
        return 1.0;
    }

    let exact_ids: std::collections::HashSet<u64> =
        exact.iter().take(k).map(|(id, _)| *id).collect();
    let measured_ids: std::collections::HashSet<u64> =
        measured.iter().take(k).map(|(id, _)| *id).collect();
    let overlap = measured_ids.intersection(&exact_ids).count();
    overlap as f64 / exact_ids.len() as f64
}

fn summarize_percentiles(samples_ms: &[f64]) -> (f64, f64, f64) {
    (
        percentile(samples_ms, 0.50),
        percentile(samples_ms, 0.95),
        percentile(samples_ms, 0.99),
    )
}

fn percentile(samples_ms: &[f64], quantile: f64) -> f64 {
    if samples_ms.is_empty() {
        return 0.0;
    }

    let mut sorted = samples_ms.to_vec();
    sorted.sort_by(f64::total_cmp);
    let last_index = sorted.len().saturating_sub(1);
    let position = (quantile.clamp(0.0, 1.0) * last_index as f64).round() as usize;
    sorted[position]
}

fn deterministic_vector(seed: u64, dimension: usize) -> Vec<f32> {
    (0..dimension)
        .map(|index| {
            let mixed = (seed as usize)
                .wrapping_mul(31)
                .wrapping_add(index.wrapping_mul(17))
                % 10_000;
            mixed as f32 / 10_000.0
        })
        .collect()
}
