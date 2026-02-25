use std::time::{Duration, Instant};

use aionbd_core::{Collection, CollectionConfig};

const LIST_POINTS: usize = 50_000;
const LIST_DIMENSION: usize = 32;
const LIST_PAGE_SIZE: usize = 256;
const LIST_WARMUP_RUNS: usize = 3;
const LIST_MEASURED_RUNS: usize = 20;

pub(crate) fn run_list_points_bench(mode: &str) -> bool {
    let Some(collection) = build_collection() else {
        return false;
    };

    for _ in 0..LIST_WARMUP_RUNS {
        if run_offset_pagination_once(&collection).is_none()
            || run_cursor_pagination_once(&collection).is_none()
        {
            return false;
        }
    }

    let mut offset_samples = Vec::with_capacity(LIST_MEASURED_RUNS);
    let mut cursor_samples = Vec::with_capacity(LIST_MEASURED_RUNS);
    let mut offset_total = Duration::ZERO;
    let mut cursor_total = Duration::ZERO;
    let mut checksum = 0u64;

    for _ in 0..LIST_MEASURED_RUNS {
        let Some((offset_elapsed, offset_checksum)) = run_offset_pagination_once(&collection)
        else {
            return false;
        };
        let Some((cursor_elapsed, cursor_checksum)) = run_cursor_pagination_once(&collection)
        else {
            return false;
        };

        if offset_checksum != cursor_checksum {
            eprintln!(
                "error=list_points_checksum_mismatch offset={offset_checksum} cursor={cursor_checksum}"
            );
            return false;
        }

        offset_samples.push(offset_elapsed.as_secs_f64() * 1_000.0);
        cursor_samples.push(cursor_elapsed.as_secs_f64() * 1_000.0);
        offset_total += offset_elapsed;
        cursor_total += cursor_elapsed;
        checksum = offset_checksum;
    }

    let (offset_p50_ms, offset_p95_ms, offset_avg_ms) = summarize_ms(&offset_samples);
    let (cursor_p50_ms, cursor_p95_ms, cursor_avg_ms) = summarize_ms(&cursor_samples);
    let offset_pages_per_sec =
        (LIST_MEASURED_RUNS as f64 * pages_per_run() as f64) / offset_total.as_secs_f64();
    let cursor_pages_per_sec =
        (LIST_MEASURED_RUNS as f64 * pages_per_run() as f64) / cursor_total.as_secs_f64();
    let speedup = if cursor_avg_ms > 0.0 {
        offset_avg_ms / cursor_avg_ms
    } else {
        f64::INFINITY
    };

    println!(
        "bench=collection_list_points mode={mode} points={LIST_POINTS} page_size={LIST_PAGE_SIZE} warmup_runs={LIST_WARMUP_RUNS} measured_runs={LIST_MEASURED_RUNS} offset_p50_ms={offset_p50_ms:.6} offset_p95_ms={offset_p95_ms:.6} offset_avg_ms={offset_avg_ms:.6} offset_pages_s={offset_pages_per_sec:.2} cursor_p50_ms={cursor_p50_ms:.6} cursor_p95_ms={cursor_p95_ms:.6} cursor_avg_ms={cursor_avg_ms:.6} cursor_pages_s={cursor_pages_per_sec:.2} speedup={speedup:.2} checksum={checksum}"
    );

    true
}

fn build_collection() -> Option<Collection> {
    let config = match CollectionConfig::new(LIST_DIMENSION, true) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("error=list_points_collection_config_failed detail=\"{error}\"");
            return None;
        }
    };

    let mut collection = match Collection::new("list_points_bench", config) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("error=list_points_collection_create_failed detail=\"{error}\"");
            return None;
        }
    };

    for id in 0..LIST_POINTS {
        let values = deterministic_vector(id as u64, LIST_DIMENSION);
        if let Err(error) = collection.upsert_point(id as u64, values) {
            eprintln!("error=list_points_collection_upsert_failed id={id} detail=\"{error}\"");
            return None;
        }
    }

    Some(collection)
}

fn run_offset_pagination_once(collection: &Collection) -> Option<(Duration, u64)> {
    let started_at = Instant::now();
    let mut offset = 0usize;
    let mut seen = 0usize;
    let mut checksum = 0u64;

    loop {
        let page = collection.point_ids_page(offset, LIST_PAGE_SIZE);
        if page.is_empty() {
            break;
        }
        seen += page.len();
        checksum = checksum.wrapping_add(page.iter().copied().sum::<u64>());
        offset = offset.saturating_add(page.len());
    }

    if seen != collection.len() {
        eprintln!(
            "error=list_points_offset_seen_mismatch seen={seen} expected={}",
            collection.len()
        );
        return None;
    }

    Some((started_at.elapsed(), checksum))
}

fn run_cursor_pagination_once(collection: &Collection) -> Option<(Duration, u64)> {
    let started_at = Instant::now();
    let mut cursor = None;
    let mut seen = 0usize;
    let mut checksum = 0u64;

    loop {
        let (page, next_cursor) = collection.point_ids_page_after(cursor, LIST_PAGE_SIZE);
        if page.is_empty() {
            break;
        }

        seen += page.len();
        checksum = checksum.wrapping_add(page.iter().copied().sum::<u64>());
        cursor = next_cursor;
        if cursor.is_none() {
            break;
        }
    }

    if seen != collection.len() {
        eprintln!(
            "error=list_points_cursor_seen_mismatch seen={seen} expected={}",
            collection.len()
        );
        return None;
    }

    Some((started_at.elapsed(), checksum))
}

fn pages_per_run() -> usize {
    LIST_POINTS.div_ceil(LIST_PAGE_SIZE)
}

fn summarize_ms(samples_ms: &[f64]) -> (f64, f64, f64) {
    let p50_ms = percentile_ms(samples_ms, 0.50);
    let p95_ms = percentile_ms(samples_ms, 0.95);
    let avg_ms = samples_ms.iter().sum::<f64>() / samples_ms.len() as f64;
    (p50_ms, p95_ms, avg_ms)
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
