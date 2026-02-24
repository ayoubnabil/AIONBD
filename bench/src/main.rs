#![forbid(unsafe_code)]
//! Core math benchmark for AIONBD.
//!
//! Scope:
//! - Baseline CPU cost for vector dot-product scoring
//! - Deterministic dataset and reproducible runs
//!
//! Non-scope:
//! - Index build/query benchmarks
//! - WAL/storage/recovery performance

use std::env;
use std::process;
use std::time::{Duration, Instant};

use aionbd_core::dot_product;

const DIMENSION: usize = 128;
const DATASET_SIZE: usize = 10_000;
const WARMUP_RUNS: usize = 8;
const MEASURED_RUNS: usize = 50;

fn main() {
    if cfg!(debug_assertions) && env::var("AIONBD_ALLOW_DEBUG_BENCH").as_deref() != Ok("1") {
        eprintln!(
            "error=debug_build_not_allowed message=\"run `cargo run --release -p aionbd-bench`\""
        );
        process::exit(2);
    }

    let mode = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };

    let query = deterministic_vector(42, DIMENSION);
    let dataset = generate_dataset(DATASET_SIZE, DIMENSION);

    for _ in 0..WARMUP_RUNS {
        if run_once(&query, &dataset).is_none() {
            process::exit(1);
        }
    }

    let mut elapsed_samples = Vec::with_capacity(MEASURED_RUNS);
    let mut last_best_id = 0usize;
    let mut last_best_score = f32::MIN;
    let mut total_duration = Duration::from_secs(0);

    for _ in 0..MEASURED_RUNS {
        let Some((elapsed, best_id, best_score)) = run_once(&query, &dataset) else {
            process::exit(1);
        };
        elapsed_samples.push(elapsed.as_secs_f64() * 1_000.0);
        total_duration += elapsed;
        last_best_id = best_id;
        last_best_score = best_score;
    }

    let p50_ms = percentile_ms(&elapsed_samples, 0.50);
    let p95_ms = percentile_ms(&elapsed_samples, 0.95);
    let avg_ms = elapsed_samples.iter().sum::<f64>() / elapsed_samples.len() as f64;
    let total_ops = (MEASURED_RUNS * DATASET_SIZE) as f64;
    let qps = total_ops / total_duration.as_secs_f64();

    // Stable key=value output for CI parsing and perf gates.
    println!(
        "bench=core_dot mode={mode} dataset_size={DATASET_SIZE} dimension={DIMENSION} warmup_runs={WARMUP_RUNS} measured_runs={MEASURED_RUNS} p50_ms={p50_ms:.6} p95_ms={p95_ms:.6} avg_ms={avg_ms:.6} qps={qps:.2} best_id={last_best_id} best_score={last_best_score:.6}"
    );
}

fn run_once(query: &[f32], dataset: &[Vec<f32>]) -> Option<(Duration, usize, f32)> {
    let started_at = Instant::now();
    let mut best_id = 0usize;
    let mut best_score = f32::MIN;

    for (id, candidate) in dataset.iter().enumerate() {
        let score = match dot_product(query, candidate) {
            Ok(value) => value,
            Err(error) => {
                eprintln!("error=dot_product_failed detail=\"{error}\"");
                return None;
            }
        };

        if score > best_score {
            best_score = score;
            best_id = id;
        }
    }

    Some((started_at.elapsed(), best_id, best_score))
}

fn generate_dataset(size: usize, dimension: usize) -> Vec<Vec<f32>> {
    (0..size)
        .map(|id| deterministic_vector(id as u64, dimension))
        .collect()
}

fn percentile_ms(samples_ms: &[f64], quantile: f64) -> f64 {
    if samples_ms.is_empty() {
        return 0.0;
    }

    let mut sorted = samples_ms.to_vec();
    sorted.sort_by(f64::total_cmp);
    let last_index = sorted.len().saturating_sub(1);
    let position = (quantile.clamp(0.0, 1.0) * last_index as f64).round() as usize;
    sorted[position]
}

/// Creates a deterministic vector from a seed.
///
/// This avoids external randomness so benchmark results stay reproducible.
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
