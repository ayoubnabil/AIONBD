use std::env;
use std::fs;
use std::time::{Duration, Instant};

use aionbd_core::{dot_product, l2_distance, Collection, CollectionConfig};

const DEFAULT_DIMENSION: usize = 128;
const DEFAULT_DATASET_SIZE: usize = 10_000;
const DEFAULT_COLLECTION_POINTS: usize = 10_000;
const DEFAULT_WARMUP_RUNS: usize = 8;
const DEFAULT_MEASURED_RUNS: usize = 50;
const DEFAULT_COLLECTION_WARMUP_RUNS: usize = 3;
const DEFAULT_COLLECTION_MEASURED_RUNS: usize = 15;

#[derive(Clone, Copy)]
struct CoreBenchConfig {
    dimension: usize,
    dataset_size: usize,
    collection_points: usize,
    warmup_runs: usize,
    measured_runs: usize,
    collection_warmup_runs: usize,
    collection_measured_runs: usize,
}

fn load_config() -> CoreBenchConfig {
    CoreBenchConfig {
        dimension: read_usize_env_with_min("AIONBD_BENCH_DIMENSION", DEFAULT_DIMENSION, 1),
        dataset_size: read_usize_env_with_min("AIONBD_BENCH_DATASET_SIZE", DEFAULT_DATASET_SIZE, 1),
        collection_points: read_usize_env_with_min(
            "AIONBD_BENCH_COLLECTION_POINTS",
            DEFAULT_COLLECTION_POINTS,
            1,
        ),
        warmup_runs: read_usize_env_with_min("AIONBD_BENCH_WARMUP_RUNS", DEFAULT_WARMUP_RUNS, 0),
        measured_runs: read_usize_env_with_min(
            "AIONBD_BENCH_MEASURED_RUNS",
            DEFAULT_MEASURED_RUNS,
            1,
        ),
        collection_warmup_runs: read_usize_env_with_min(
            "AIONBD_BENCH_COLLECTION_WARMUP_RUNS",
            DEFAULT_COLLECTION_WARMUP_RUNS,
            0,
        ),
        collection_measured_runs: read_usize_env_with_min(
            "AIONBD_BENCH_COLLECTION_MEASURED_RUNS",
            DEFAULT_COLLECTION_MEASURED_RUNS,
            1,
        ),
    }
}

pub(crate) fn run_dot_bench(mode: &str) -> bool {
    let config = load_config();
    let query = deterministic_vector(42, config.dimension);
    let dataset = generate_dataset(config.dataset_size, config.dimension);

    for _ in 0..config.warmup_runs {
        if run_dot_once(&query, &dataset).is_none() {
            return false;
        }
    }

    let mut elapsed_samples = Vec::with_capacity(config.measured_runs);
    let mut total_duration = Duration::from_secs(0);
    let mut last_best_id = 0usize;
    let mut last_best_score = f32::MIN;

    for _ in 0..config.measured_runs {
        let Some((elapsed, best_id, best_score)) = run_dot_once(&query, &dataset) else {
            return false;
        };
        elapsed_samples.push(elapsed.as_secs_f64() * 1_000.0);
        total_duration += elapsed;
        last_best_id = best_id;
        last_best_score = best_score;
    }

    let (p50_ms, p95_ms, avg_ms) = summarize_ms(&elapsed_samples);
    let total_ops = (config.measured_runs * config.dataset_size) as f64;
    let qps = total_ops / total_duration.as_secs_f64();

    println!(
        "bench=core_dot mode={mode} dataset_size={} dimension={} warmup_runs={} measured_runs={} p50_ms={p50_ms:.6} p95_ms={p95_ms:.6} avg_ms={avg_ms:.6} qps={qps:.2} best_id={last_best_id} best_score={last_best_score:.6}",
        config.dataset_size,
        config.dimension,
        config.warmup_runs,
        config.measured_runs
    );

    true
}

pub(crate) fn run_l2_bench(mode: &str) -> bool {
    let config = load_config();
    let query = deterministic_vector(1337, config.dimension);
    let dataset = generate_dataset(config.dataset_size, config.dimension);

    for _ in 0..config.warmup_runs {
        if run_l2_once(&query, &dataset).is_none() {
            return false;
        }
    }

    let mut elapsed_samples = Vec::with_capacity(config.measured_runs);
    let mut total_duration = Duration::from_secs(0);
    let mut last_best_id = 0usize;
    let mut last_best_distance = f32::MAX;

    for _ in 0..config.measured_runs {
        let Some((elapsed, best_id, best_distance)) = run_l2_once(&query, &dataset) else {
            return false;
        };
        elapsed_samples.push(elapsed.as_secs_f64() * 1_000.0);
        total_duration += elapsed;
        last_best_id = best_id;
        last_best_distance = best_distance;
    }

    let (p50_ms, p95_ms, avg_ms) = summarize_ms(&elapsed_samples);
    let total_ops = (config.measured_runs * config.dataset_size) as f64;
    let qps = total_ops / total_duration.as_secs_f64();

    println!(
        "bench=core_l2 mode={mode} dataset_size={} dimension={} warmup_runs={} measured_runs={} p50_ms={p50_ms:.6} p95_ms={p95_ms:.6} avg_ms={avg_ms:.6} qps={qps:.2} best_id={last_best_id} best_distance={last_best_distance:.6}",
        config.dataset_size,
        config.dimension,
        config.warmup_runs,
        config.measured_runs
    );

    true
}

pub(crate) fn run_collection_bench(mode: &str) -> bool {
    let config = load_config();

    for _ in 0..config.collection_warmup_runs {
        if run_collection_once(config.dimension, config.collection_points).is_none() {
            return false;
        }
    }

    let mut upsert_ms = Vec::with_capacity(config.collection_measured_runs);
    let mut get_ms = Vec::with_capacity(config.collection_measured_runs);
    let mut total_upsert = Duration::from_secs(0);
    let mut total_get = Duration::from_secs(0);
    let mut checksum = 0usize;
    let mut rss_delta_bytes_max = 0u64;

    for _ in 0..config.collection_measured_runs {
        let Some((upsert_elapsed, get_elapsed, sample_checksum, rss_delta_bytes)) =
            run_collection_once(config.dimension, config.collection_points)
        else {
            return false;
        };

        upsert_ms.push(upsert_elapsed.as_secs_f64() * 1_000.0);
        get_ms.push(get_elapsed.as_secs_f64() * 1_000.0);
        total_upsert += upsert_elapsed;
        total_get += get_elapsed;
        checksum = sample_checksum;
        rss_delta_bytes_max = rss_delta_bytes_max.max(rss_delta_bytes);
    }

    let (upsert_p50_ms, upsert_p95_ms, upsert_avg_ms) = summarize_ms(&upsert_ms);
    let (get_p50_ms, get_p95_ms, get_avg_ms) = summarize_ms(&get_ms);
    let ops = (config.collection_measured_runs * config.collection_points) as f64;
    let upsert_qps = ops / total_upsert.as_secs_f64();
    let get_qps = ops / total_get.as_secs_f64();
    let raw_vector_bytes = config
        .collection_points
        .saturating_mul(config.dimension)
        .saturating_mul(std::mem::size_of::<f32>());
    let rss_bytes_per_point = if config.collection_points == 0 {
        0.0
    } else {
        rss_delta_bytes_max as f64 / config.collection_points as f64
    };

    println!(
        "bench=collection_memory_crud mode={mode} points={} dimension={} warmup_runs={} measured_runs={} upsert_p50_ms={upsert_p50_ms:.6} upsert_p95_ms={upsert_p95_ms:.6} upsert_avg_ms={upsert_avg_ms:.6} upsert_qps={upsert_qps:.2} get_p50_ms={get_p50_ms:.6} get_p95_ms={get_p95_ms:.6} get_avg_ms={get_avg_ms:.6} get_qps={get_qps:.2} raw_vector_bytes={} raw_vector_mb={:.3} rss_delta_bytes_max={} rss_bytes_per_point={rss_bytes_per_point:.2} checksum={checksum}",
        config.collection_points,
        config.dimension,
        config.collection_warmup_runs,
        config.collection_measured_runs,
        raw_vector_bytes,
        raw_vector_bytes as f64 / (1024.0 * 1024.0),
        rss_delta_bytes_max
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

fn run_collection_once(
    dimension: usize,
    collection_points: usize,
) -> Option<(Duration, Duration, usize, u64)> {
    let config = match CollectionConfig::new(dimension, true) {
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
    let rss_before = process_rss_bytes();

    let upsert_started = Instant::now();
    for id in 0..collection_points {
        let values = deterministic_vector(id as u64, dimension);
        if let Err(error) = collection.upsert_point(id as u64, values) {
            eprintln!("error=collection_upsert_failed id={id} detail=\"{error}\"");
            return None;
        }
    }
    let upsert_elapsed = upsert_started.elapsed();
    let rss_after_upsert = process_rss_bytes();
    let rss_delta_bytes = rss_after_upsert.saturating_sub(rss_before);

    let get_started = Instant::now();
    let mut checksum = 0usize;
    for id in 0..collection_points {
        let Some(values) = collection.get_point(id as u64) else {
            eprintln!("error=collection_get_missing id={id}");
            return None;
        };
        checksum = checksum.wrapping_add(values.len());
    }
    let get_elapsed = get_started.elapsed();

    Some((upsert_elapsed, get_elapsed, checksum, rss_delta_bytes))
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

fn read_usize_env_with_min(key: &str, default: usize, min: usize) -> usize {
    let Ok(raw) = env::var(key) else {
        return default;
    };
    let Ok(parsed) = raw.parse::<usize>() else {
        return default;
    };
    if parsed < min {
        default
    } else {
        parsed
    }
}

fn process_rss_bytes() -> u64 {
    let Ok(status) = fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    for line in status.lines() {
        if !line.starts_with("VmRSS:") {
            continue;
        }
        let mut parts = line.split_whitespace();
        let _ = parts.next();
        let Some(kb_raw) = parts.next() else {
            return 0;
        };
        let Ok(kb) = kb_raw.parse::<u64>() else {
            return 0;
        };
        return kb.saturating_mul(1024);
    }
    0
}
