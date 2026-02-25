use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn should_sync_this_write(write_seq: u64, sync_every_n_writes: u64) -> bool {
    if sync_every_n_writes == 0 {
        return false;
    }
    write_seq.checked_rem(sync_every_n_writes) == Some(0)
}

pub(crate) fn should_sync_batch(seq_start: u64, batch_size: u64, sync_every_n_writes: u64) -> bool {
    if sync_every_n_writes == 0 || batch_size == 0 {
        return false;
    }
    let seq_end = seq_start.saturating_add(batch_size.saturating_sub(1));
    let remainder = seq_start.checked_rem(sync_every_n_writes).unwrap_or(0);
    let first_multiple = if remainder == 0 {
        seq_start
    } else {
        seq_start.saturating_add(sync_every_n_writes - remainder)
    };
    first_multiple <= seq_end
}

pub(crate) fn average_ms(samples_ms: &[f64]) -> f64 {
    if samples_ms.is_empty() {
        return 0.0;
    }
    samples_ms.iter().sum::<f64>() / samples_ms.len() as f64
}

pub(crate) fn percentile_ms(samples_ms: &[f64], quantile: f64) -> f64 {
    if samples_ms.is_empty() {
        return 0.0;
    }
    let mut sorted = samples_ms.to_vec();
    sorted.sort_by(f64::total_cmp);
    let last_index = sorted.len().saturating_sub(1);
    let position = (quantile.clamp(0.0, 1.0) * last_index as f64).round() as usize;
    sorted[position]
}

pub(crate) fn deterministic_vector(seed: u64, dimension: usize) -> Vec<f32> {
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

pub(crate) fn wal_path_for(root: &Path, strategy: &str, phase: &str, run: usize) -> PathBuf {
    root.join(format!("{strategy}_{phase}_{run}.jsonl"))
}

pub(crate) fn temp_root() -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("aionbd_persistence_write_bench_{stamp}"))
}

pub(crate) fn clean_root(root: &Path) {
    if let Err(error) = fs::remove_dir_all(root) {
        eprintln!(
            "warn=persistence_write_bench_cleanup_failed root={} detail=\"{}\"",
            root.display(),
            error
        );
    }
}
