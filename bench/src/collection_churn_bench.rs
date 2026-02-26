use std::env;
use std::time::{Duration, Instant};

use aionbd_core::{Collection, CollectionConfig};

const DEFAULT_DIMENSION: usize = 128;
const DEFAULT_COLLECTION_POINTS: usize = 10_000;
const DEFAULT_DELETE_RATIO_PERCENT: usize = 70;
const DEFAULT_WARMUP_RUNS: usize = 1;
const DEFAULT_MEASURED_RUNS: usize = 5;

#[derive(Clone, Copy)]
struct ChurnBenchConfig {
    dimension: usize,
    points: usize,
    delete_ratio_percent: usize,
    warmup_runs: usize,
    measured_runs: usize,
}

pub(crate) fn run_collection_churn_bench(mode: &str) -> bool {
    let config = load_config();

    for _ in 0..config.warmup_runs {
        if run_once(config).is_none() {
            return false;
        }
    }

    let mut delete_ms = Vec::with_capacity(config.measured_runs);
    let mut reinsert_ms = Vec::with_capacity(config.measured_runs);
    let mut total_delete = Duration::from_secs(0);
    let mut total_reinsert = Duration::from_secs(0);
    let mut slot_count_after_delete = 0usize;
    let mut live_count_after_delete = 0usize;
    let mut slot_count_after_reinsert = 0usize;
    let mut live_count_after_reinsert = 0usize;

    for _ in 0..config.measured_runs {
        let Some((
            delete_elapsed,
            reinsert_elapsed,
            slots_delete,
            live_delete,
            slots_reinsert,
            live_reinsert,
        )) = run_once(config)
        else {
            return false;
        };

        delete_ms.push(delete_elapsed.as_secs_f64() * 1_000.0);
        reinsert_ms.push(reinsert_elapsed.as_secs_f64() * 1_000.0);
        total_delete += delete_elapsed;
        total_reinsert += reinsert_elapsed;
        slot_count_after_delete = slots_delete;
        live_count_after_delete = live_delete;
        slot_count_after_reinsert = slots_reinsert;
        live_count_after_reinsert = live_reinsert;
    }

    let (delete_p50_ms, delete_p95_ms, delete_avg_ms) = summarize_ms(&delete_ms);
    let (reinsert_p50_ms, reinsert_p95_ms, reinsert_avg_ms) = summarize_ms(&reinsert_ms);
    let delete_count = config
        .points
        .saturating_mul(config.delete_ratio_percent)
        .saturating_div(100)
        .max(1)
        .min(config.points);
    let delete_ops = (config.measured_runs * delete_count) as f64;
    let reinsert_ops = (config.measured_runs * delete_count) as f64;
    let delete_qps = delete_ops / total_delete.as_secs_f64();
    let reinsert_qps = reinsert_ops / total_reinsert.as_secs_f64();
    let delete_slot_utilization = if slot_count_after_delete == 0 {
        1.0
    } else {
        live_count_after_delete as f64 / slot_count_after_delete as f64
    };
    let reinsert_slot_utilization = if slot_count_after_reinsert == 0 {
        1.0
    } else {
        live_count_after_reinsert as f64 / slot_count_after_reinsert as f64
    };

    println!(
        "bench=collection_churn mode={mode} points={} dimension={} delete_ratio_percent={} warmup_runs={} measured_runs={} delete_p50_ms={delete_p50_ms:.6} delete_p95_ms={delete_p95_ms:.6} delete_avg_ms={delete_avg_ms:.6} delete_qps={delete_qps:.2} reinsert_p50_ms={reinsert_p50_ms:.6} reinsert_p95_ms={reinsert_p95_ms:.6} reinsert_avg_ms={reinsert_avg_ms:.6} reinsert_qps={reinsert_qps:.2} slot_count_after_delete={} live_count_after_delete={} slot_utilization_after_delete={delete_slot_utilization:.4} slot_count_after_reinsert={} live_count_after_reinsert={} slot_utilization_after_reinsert={reinsert_slot_utilization:.4}",
        config.points,
        config.dimension,
        config.delete_ratio_percent,
        config.warmup_runs,
        config.measured_runs,
        slot_count_after_delete,
        live_count_after_delete,
        slot_count_after_reinsert,
        live_count_after_reinsert,
    );

    true
}

fn run_once(config: ChurnBenchConfig) -> Option<(Duration, Duration, usize, usize, usize, usize)> {
    let collection_config = match CollectionConfig::new(config.dimension, true) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("error=collection_config_failed detail=\"{error}\"");
            return None;
        }
    };
    let mut collection = match Collection::new("churn", collection_config) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("error=collection_create_failed detail=\"{error}\"");
            return None;
        }
    };

    for id in 0..config.points {
        let values = deterministic_vector(id as u64, config.dimension);
        if let Err(error) = collection.upsert_point(id as u64, values) {
            eprintln!("error=churn_insert_failed point_id={id} detail=\"{error}\"");
            return None;
        }
    }

    let delete_count = config
        .points
        .saturating_mul(config.delete_ratio_percent)
        .saturating_div(100)
        .max(1)
        .min(config.points);

    let delete_started_at = Instant::now();
    for id in 0..delete_count {
        if !collection.delete_point(id as u64) {
            eprintln!("error=churn_delete_missing point_id={id}");
            return None;
        }
    }
    let delete_elapsed = delete_started_at.elapsed();
    let slot_count_after_delete = collection.slot_count();
    let live_count_after_delete = collection.len();

    let reinsert_started_at = Instant::now();
    for offset in 0..delete_count {
        let id = config.points + offset;
        let values = deterministic_vector(id as u64, config.dimension);
        if let Err(error) = collection.upsert_point(id as u64, values) {
            eprintln!("error=churn_reinsert_failed point_id={id} detail=\"{error}\"");
            return None;
        }
    }
    let reinsert_elapsed = reinsert_started_at.elapsed();
    let slot_count_after_reinsert = collection.slot_count();
    let live_count_after_reinsert = collection.len();

    Some((
        delete_elapsed,
        reinsert_elapsed,
        slot_count_after_delete,
        live_count_after_delete,
        slot_count_after_reinsert,
        live_count_after_reinsert,
    ))
}

fn load_config() -> ChurnBenchConfig {
    ChurnBenchConfig {
        dimension: read_usize_env_with_min("AIONBD_BENCH_DIMENSION", DEFAULT_DIMENSION, 1),
        points: read_usize_env_with_min(
            "AIONBD_BENCH_COLLECTION_POINTS",
            DEFAULT_COLLECTION_POINTS,
            1,
        ),
        delete_ratio_percent: read_usize_env_with_range(
            "AIONBD_BENCH_CHURN_DELETE_RATIO_PERCENT",
            DEFAULT_DELETE_RATIO_PERCENT,
            1,
            99,
        ),
        warmup_runs: read_usize_env_with_min(
            "AIONBD_BENCH_CHURN_WARMUP_RUNS",
            DEFAULT_WARMUP_RUNS,
            0,
        ),
        measured_runs: read_usize_env_with_min(
            "AIONBD_BENCH_CHURN_MEASURED_RUNS",
            DEFAULT_MEASURED_RUNS,
            1,
        ),
    }
}

fn deterministic_vector(seed: u64, dimension: usize) -> Vec<f32> {
    (0..dimension)
        .map(|index| {
            let mixed = (seed as usize)
                .wrapping_mul(31)
                .wrapping_add(index.wrapping_mul(17))
                .wrapping_add(13);
            ((mixed % 10_000) as f32) / 10_000.0
        })
        .collect()
}

fn summarize_ms(samples_ms: &[f64]) -> (f64, f64, f64) {
    if samples_ms.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    let mut sorted = samples_ms.to_vec();
    sorted.sort_by(f64::total_cmp);
    let len = sorted.len();
    let p50 = sorted[len / 2];
    let p95_index = ((len as f64) * 0.95).ceil() as usize;
    let p95 = sorted[p95_index.saturating_sub(1).min(len - 1)];
    let avg = sorted.iter().copied().sum::<f64>() / len as f64;
    (p50, p95, avg)
}

fn read_usize_env_with_min(name: &str, default: usize, min: usize) -> usize {
    match env::var(name) {
        Ok(raw) => match raw.trim().parse::<usize>() {
            Ok(value) if value >= min => value,
            Ok(value) => {
                eprintln!(
                    "warn=invalid_env value={} env={} reason=\"must be >= {}\" using_default={}",
                    value, name, min, default
                );
                default
            }
            Err(_) => {
                eprintln!(
                    "warn=invalid_env value=\"{}\" env={} reason=\"parse_usize_failed\" using_default={}",
                    raw, name, default
                );
                default
            }
        },
        Err(_) => default,
    }
}

fn read_usize_env_with_range(name: &str, default: usize, min: usize, max: usize) -> usize {
    match env::var(name) {
        Ok(raw) => match raw.trim().parse::<usize>() {
            Ok(value) if (min..=max).contains(&value) => value,
            Ok(value) => {
                eprintln!(
                    "warn=invalid_env value={} env={} reason=\"must be in [{}, {}]\" using_default={}",
                    value, name, min, max, default
                );
                default
            }
            Err(_) => {
                eprintln!(
                    "warn=invalid_env value=\"{}\" env={} reason=\"parse_usize_failed\" using_default={}",
                    raw, name, default
                );
                default
            }
        },
        Err(_) => default,
    }
}
