#![forbid(unsafe_code)]
//! Core and in-memory collection benchmarks for AIONBD.
//!
//! Scenarios:
//! - `core_dot`: scan + top-1 by dot product
//! - `core_l2`: scan + top-1 by L2 distance
//! - `collection_memory_crud`: upsert/get throughput for in-memory collection

use std::env;
use std::process;
use std::time::{Duration, Instant};

use aionbd_core::{dot_product, l2_distance, Collection, CollectionConfig};

const DIMENSION: usize = 128;
const DATASET_SIZE: usize = 10_000;
const COLLECTION_POINTS: usize = 10_000;
const WARMUP_RUNS: usize = 8;
const MEASURED_RUNS: usize = 50;
const COLLECTION_WARMUP_RUNS: usize = 3;
const COLLECTION_MEASURED_RUNS: usize = 15;

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

    let scenario = env::var("AIONBD_BENCH_SCENARIO").unwrap_or_else(|_| "all".to_string());
    let ok = match scenario.as_str() {
        "all" => run_dot_bench(mode) && run_l2_bench(mode) && run_collection_bench(mode),
        "dot" => run_dot_bench(mode),
        "l2" => run_l2_bench(mode),
        "collection" => run_collection_bench(mode),
        _ => {
            eprintln!(
                "error=invalid_scenario scenario=\"{}\" allowed=\"all,dot,l2,collection\"",
                scenario
            );
            false
        }
    };

    if !ok {
        process::exit(1);
    }
}

fn run_dot_bench(mode: &str) -> bool {
    let query = deterministic_vector(42, DIMENSION);
    let dataset = generate_dataset(DATASET_SIZE, DIMENSION);

    for _ in 0..WARMUP_RUNS {
        if run_dot_once(&query, &dataset).is_none() {
            return false;
        }
    }

    let mut elapsed_samples = Vec::with_capacity(MEASURED_RUNS);
    let mut total_duration = Duration::from_secs(0);
    let mut last_best_id = 0usize;
    let mut last_best_score = f32::MIN;

    for _ in 0..MEASURED_RUNS {
        let Some((elapsed, best_id, best_score)) = run_dot_once(&query, &dataset) else {
            return false;
        };
        elapsed_samples.push(elapsed.as_secs_f64() * 1_000.0);
        total_duration += elapsed;
        last_best_id = best_id;
        last_best_score = best_score;
    }

    let (p50_ms, p95_ms, avg_ms) = summarize_ms(&elapsed_samples);
    let total_ops = (MEASURED_RUNS * DATASET_SIZE) as f64;
    let qps = total_ops / total_duration.as_secs_f64();

    println!(
        "bench=core_dot mode={mode} dataset_size={DATASET_SIZE} dimension={DIMENSION} warmup_runs={WARMUP_RUNS} measured_runs={MEASURED_RUNS} p50_ms={p50_ms:.6} p95_ms={p95_ms:.6} avg_ms={avg_ms:.6} qps={qps:.2} best_id={last_best_id} best_score={last_best_score:.6}"
    );

    true
}

fn run_l2_bench(mode: &str) -> bool {
    let query = deterministic_vector(1337, DIMENSION);
    let dataset = generate_dataset(DATASET_SIZE, DIMENSION);

    for _ in 0..WARMUP_RUNS {
        if run_l2_once(&query, &dataset).is_none() {
            return false;
        }
    }

    let mut elapsed_samples = Vec::with_capacity(MEASURED_RUNS);
    let mut total_duration = Duration::from_secs(0);
    let mut last_best_id = 0usize;
    let mut last_best_distance = f32::MAX;

    for _ in 0..MEASURED_RUNS {
        let Some((elapsed, best_id, best_distance)) = run_l2_once(&query, &dataset) else {
            return false;
        };
        elapsed_samples.push(elapsed.as_secs_f64() * 1_000.0);
        total_duration += elapsed;
        last_best_id = best_id;
        last_best_distance = best_distance;
    }

    let (p50_ms, p95_ms, avg_ms) = summarize_ms(&elapsed_samples);
    let total_ops = (MEASURED_RUNS * DATASET_SIZE) as f64;
    let qps = total_ops / total_duration.as_secs_f64();

    println!(
        "bench=core_l2 mode={mode} dataset_size={DATASET_SIZE} dimension={DIMENSION} warmup_runs={WARMUP_RUNS} measured_runs={MEASURED_RUNS} p50_ms={p50_ms:.6} p95_ms={p95_ms:.6} avg_ms={avg_ms:.6} qps={qps:.2} best_id={last_best_id} best_distance={last_best_distance:.6}"
    );

    true
}

fn run_collection_bench(mode: &str) -> bool {
    for _ in 0..COLLECTION_WARMUP_RUNS {
        if run_collection_once().is_none() {
            return false;
        }
    }

    let mut upsert_ms = Vec::with_capacity(COLLECTION_MEASURED_RUNS);
    let mut get_ms = Vec::with_capacity(COLLECTION_MEASURED_RUNS);
    let mut total_upsert = Duration::from_secs(0);
    let mut total_get = Duration::from_secs(0);
    let mut checksum = 0usize;

    for _ in 0..COLLECTION_MEASURED_RUNS {
        let Some((upsert_elapsed, get_elapsed, sample_checksum)) = run_collection_once() else {
            return false;
        };

        upsert_ms.push(upsert_elapsed.as_secs_f64() * 1_000.0);
        get_ms.push(get_elapsed.as_secs_f64() * 1_000.0);
        total_upsert += upsert_elapsed;
        total_get += get_elapsed;
        checksum = sample_checksum;
    }

    let (upsert_p50_ms, upsert_p95_ms, upsert_avg_ms) = summarize_ms(&upsert_ms);
    let (get_p50_ms, get_p95_ms, get_avg_ms) = summarize_ms(&get_ms);
    let ops = (COLLECTION_MEASURED_RUNS * COLLECTION_POINTS) as f64;
    let upsert_qps = ops / total_upsert.as_secs_f64();
    let get_qps = ops / total_get.as_secs_f64();

    println!(
        "bench=collection_memory_crud mode={mode} points={COLLECTION_POINTS} dimension={DIMENSION} warmup_runs={COLLECTION_WARMUP_RUNS} measured_runs={COLLECTION_MEASURED_RUNS} upsert_p50_ms={upsert_p50_ms:.6} upsert_p95_ms={upsert_p95_ms:.6} upsert_avg_ms={upsert_avg_ms:.6} upsert_qps={upsert_qps:.2} get_p50_ms={get_p50_ms:.6} get_p95_ms={get_p95_ms:.6} get_avg_ms={get_avg_ms:.6} get_qps={get_qps:.2} checksum={checksum}"
    );

    true
}

fn run_dot_once(query: &[f32], dataset: &[Vec<f32>]) -> Option<(Duration, usize, f32)> {
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

fn run_l2_once(query: &[f32], dataset: &[Vec<f32>]) -> Option<(Duration, usize, f32)> {
    let started_at = Instant::now();
    let mut best_id = 0usize;
    let mut best_distance = f32::MAX;

    for (id, candidate) in dataset.iter().enumerate() {
        let distance = match l2_distance(query, candidate) {
            Ok(value) => value,
            Err(error) => {
                eprintln!("error=l2_distance_failed detail=\"{error}\"");
                return None;
            }
        };

        if distance < best_distance {
            best_distance = distance;
            best_id = id;
        }
    }

    Some((started_at.elapsed(), best_id, best_distance))
}

fn run_collection_once() -> Option<(Duration, Duration, usize)> {
    let config = match CollectionConfig::new(DIMENSION, true) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("error=collection_config_failed detail=\"{error}\"");
            return None;
        }
    };

    let mut collection = match Collection::new("bench", config) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("error=collection_create_failed detail=\"{error}\"");
            return None;
        }
    };

    let upsert_started = Instant::now();
    for id in 0..COLLECTION_POINTS {
        let values = deterministic_vector(id as u64, DIMENSION);
        if let Err(error) = collection.upsert_point(id as u64, values) {
            eprintln!("error=collection_upsert_failed id={id} detail=\"{error}\"");
            return None;
        }
    }
    let upsert_elapsed = upsert_started.elapsed();

    let get_started = Instant::now();
    let mut checksum = 0usize;
    for id in 0..COLLECTION_POINTS {
        let Some(values) = collection.get_point(id as u64) else {
            eprintln!("error=collection_get_missing id={id}");
            return None;
        };
        checksum = checksum.wrapping_add(values.len());
    }
    let get_elapsed = get_started.elapsed();

    Some((upsert_elapsed, get_elapsed, checksum))
}

fn summarize_ms(samples_ms: &[f64]) -> (f64, f64, f64) {
    let p50_ms = percentile_ms(samples_ms, 0.50);
    let p95_ms = percentile_ms(samples_ms, 0.95);
    let avg_ms = samples_ms.iter().sum::<f64>() / samples_ms.len() as f64;
    (p50_ms, p95_ms, avg_ms)
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
