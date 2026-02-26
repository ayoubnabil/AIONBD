use crate::persistence_write_utils::{
    average_ms, clean_root, deterministic_vector, percentile_ms, should_sync_batch,
    should_sync_this_write, temp_root, wal_path_for,
};
use aionbd_core::{append_wal_record_with_sync, append_wal_records_with_sync, WalRecord};
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

const WRITE_COUNT: usize = 4_096;
const DIMENSION: usize = 64;
const WARMUP_RUNS: usize = 1;
const MEASURED_RUNS: usize = 5;
const GROUP_BATCH_SIZE: usize = 16;
const SYNC_EVERY_N_WRITES: u64 = 32;

#[derive(Clone, Copy)]
enum Strategy {
    SingleSyncEachWrite,
    SingleSyncEveryN,
    GroupSyncEachBatch,
    GroupSyncEveryN,
}

impl Strategy {
    fn as_str(self) -> &'static str {
        match self {
            Self::SingleSyncEachWrite => "single_sync_each_write",
            Self::SingleSyncEveryN => "single_sync_every_n",
            Self::GroupSyncEachBatch => "group_sync_each_batch",
            Self::GroupSyncEveryN => "group_sync_every_n",
        }
    }
}

struct BenchRow {
    strategy: Strategy,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    avg_ms: f64,
    qps: f64,
    wal_bytes: u64,
}

struct RunStats {
    elapsed: Duration,
    latencies_ms: Vec<f64>,
    wal_bytes: u64,
}

pub(crate) fn run_persistence_write_bench(mode: &str) -> bool {
    let root = temp_root();
    if let Err(error) = fs::create_dir_all(&root) {
        eprintln!(
            "error=persistence_write_bench_setup_failed root={} detail=\"{}\"",
            root.display(),
            error
        );
        return false;
    }
    let records = build_records();
    println!(
        "bench=persistence_write mode={mode} writes={WRITE_COUNT} dimension={DIMENSION} warmup_runs={WARMUP_RUNS} measured_runs={MEASURED_RUNS} group_batch_size={GROUP_BATCH_SIZE} sync_every_n={SYNC_EVERY_N_WRITES}"
    );
    println!("| strategy | p50_ms | p95_ms | p99_ms | avg_ms | qps | wal_mb |");
    println!("|---|---:|---:|---:|---:|---:|---:|");

    for strategy in [
        Strategy::SingleSyncEachWrite,
        Strategy::SingleSyncEveryN,
        Strategy::GroupSyncEachBatch,
        Strategy::GroupSyncEveryN,
    ] {
        let Some(row) = run_strategy(&root, &records, strategy) else {
            clean_root(&root);
            return false;
        };
        println!(
            "| {} | {:.6} | {:.6} | {:.6} | {:.6} | {:.2} | {:.3} |",
            row.strategy.as_str(),
            row.p50_ms,
            row.p95_ms,
            row.p99_ms,
            row.avg_ms,
            row.qps,
            row.wal_bytes as f64 / (1024.0 * 1024.0)
        );
        println!(
            "bench=persistence_write_row strategy={} p50_ms={:.6} p95_ms={:.6} p99_ms={:.6} avg_ms={:.6} qps={:.2} wal_bytes={}",
            row.strategy.as_str(),
            row.p50_ms,
            row.p95_ms,
            row.p99_ms,
            row.avg_ms,
            row.qps,
            row.wal_bytes
        );
    }
    clean_root(&root);
    true
}

fn run_strategy(root: &Path, records: &[WalRecord], strategy: Strategy) -> Option<BenchRow> {
    for run in 0..WARMUP_RUNS {
        let wal_path = wal_path_for(root, strategy.as_str(), "warmup", run);
        let _ = run_once(&wal_path, records, strategy)?;
    }

    let mut latencies_ms = Vec::with_capacity(MEASURED_RUNS * WRITE_COUNT);
    let mut total_elapsed = Duration::ZERO;
    let mut total_wal_bytes = 0u64;
    for run in 0..MEASURED_RUNS {
        let wal_path = wal_path_for(root, strategy.as_str(), "measured", run);
        let stats = run_once(&wal_path, records, strategy)?;
        total_elapsed += stats.elapsed;
        total_wal_bytes = total_wal_bytes.saturating_add(stats.wal_bytes);
        latencies_ms.extend(stats.latencies_ms);
    }

    let writes = (MEASURED_RUNS * WRITE_COUNT) as f64;
    let qps = if total_elapsed.as_secs_f64() > 0.0 {
        writes / total_elapsed.as_secs_f64()
    } else {
        0.0
    };
    let wal_bytes = total_wal_bytes / MEASURED_RUNS as u64;
    Some(BenchRow {
        strategy,
        p50_ms: percentile_ms(&latencies_ms, 0.50),
        p95_ms: percentile_ms(&latencies_ms, 0.95),
        p99_ms: percentile_ms(&latencies_ms, 0.99),
        avg_ms: average_ms(&latencies_ms),
        qps,
        wal_bytes,
    })
}

fn run_once(path: &Path, records: &[WalRecord], strategy: Strategy) -> Option<RunStats> {
    if path.exists() && fs::remove_file(path).is_err() {
        eprintln!(
            "error=persistence_write_bench_cleanup_failed path={}",
            path.display()
        );
        return None;
    }
    let stats = match strategy {
        Strategy::SingleSyncEachWrite | Strategy::SingleSyncEveryN => {
            run_single(path, records, strategy)
        }
        Strategy::GroupSyncEachBatch | Strategy::GroupSyncEveryN => {
            run_grouped(path, records, strategy)
        }
    }?;
    let wal_bytes = match fs::metadata(path) {
        Ok(metadata) => metadata.len(),
        Err(error) => {
            eprintln!(
                "error=persistence_write_bench_metadata_failed path={} detail=\"{}\"",
                path.display(),
                error
            );
            return None;
        }
    };
    Some(RunStats {
        elapsed: stats.elapsed,
        latencies_ms: stats.latencies_ms,
        wal_bytes,
    })
}

fn run_single(path: &Path, records: &[WalRecord], strategy: Strategy) -> Option<RunStats> {
    let mut latencies_ms = Vec::with_capacity(records.len());
    let started = Instant::now();
    for (idx, record) in records.iter().enumerate() {
        let write_seq = idx as u64 + 1;
        let should_sync = match strategy {
            Strategy::SingleSyncEachWrite => true,
            Strategy::SingleSyncEveryN => should_sync_this_write(write_seq, SYNC_EVERY_N_WRITES),
            _ => return None,
        };
        let write_started = Instant::now();
        if let Err(error) = append_wal_record_with_sync(path, record, should_sync) {
            eprintln!(
                "error=persistence_write_bench_append_failed strategy={} seq={} detail=\"{}\"",
                strategy.as_str(),
                write_seq,
                error
            );
            return None;
        }
        latencies_ms.push(write_started.elapsed().as_secs_f64() * 1_000.0);
    }
    Some(RunStats {
        elapsed: started.elapsed(),
        latencies_ms,
        wal_bytes: 0,
    })
}

fn run_grouped(path: &Path, records: &[WalRecord], strategy: Strategy) -> Option<RunStats> {
    let mut latencies_ms = Vec::with_capacity(records.len());
    let mut pending_starts = Vec::with_capacity(GROUP_BATCH_SIZE);
    let mut batch_start = 0usize;
    let mut seq_start = 1u64;
    let started = Instant::now();

    for idx in 0..records.len() {
        pending_starts.push(Instant::now());
        if pending_starts.len() < GROUP_BATCH_SIZE {
            continue;
        }
        let batch_end = idx + 1;
        let batch_len = pending_starts.len() as u64;
        flush_group(
            path,
            &records[batch_start..batch_end],
            &mut pending_starts,
            strategy,
            seq_start,
            &mut latencies_ms,
        )?;
        seq_start = seq_start.saturating_add(batch_len);
        batch_start = batch_end;
    }

    if !pending_starts.is_empty() {
        flush_group(
            path,
            &records[batch_start..records.len()],
            &mut pending_starts,
            strategy,
            seq_start,
            &mut latencies_ms,
        )?;
    }
    Some(RunStats {
        elapsed: started.elapsed(),
        latencies_ms,
        wal_bytes: 0,
    })
}

fn flush_group(
    path: &Path,
    records: &[WalRecord],
    pending_starts: &mut Vec<Instant>,
    strategy: Strategy,
    seq_start: u64,
    latencies_ms: &mut Vec<f64>,
) -> Option<()> {
    let should_sync = match strategy {
        Strategy::GroupSyncEachBatch => true,
        Strategy::GroupSyncEveryN => {
            should_sync_batch(seq_start, records.len() as u64, SYNC_EVERY_N_WRITES)
        }
        _ => return None,
    };
    if let Err(error) = append_wal_records_with_sync(path, records, should_sync) {
        eprintln!(
            "error=persistence_write_bench_batch_append_failed strategy={} size={} detail=\"{}\"",
            strategy.as_str(),
            records.len(),
            error
        );
        return None;
    }
    let finished = Instant::now();
    latencies_ms.extend(
        pending_starts
            .drain(..)
            .map(|enqueue| finished.duration_since(enqueue).as_secs_f64() * 1_000.0),
    );
    Some(())
}

fn build_records() -> Vec<WalRecord> {
    (0..WRITE_COUNT)
        .map(|id| WalRecord::UpsertPoint {
            collection: "bench".to_string(),
            id: id as u64,
            values: deterministic_vector(id as u64, DIMENSION),
            payload: None,
        })
        .collect()
}
