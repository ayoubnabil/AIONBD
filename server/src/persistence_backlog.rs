use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use aionbd_core::incremental_snapshot_dir;

use crate::state::{AppState, PersistenceBacklogSnapshot};

const WAL_BINARY_MAGIC: &[u8; 8] = b"AIONWAL1";

pub(crate) fn initialize_cache(state: &AppState) {
    if !state.config.persistence_enabled {
        return;
    }
    refresh_full_scan(state);
}

pub(crate) fn snapshot(state: &AppState) -> PersistenceBacklogSnapshot {
    state.persistence_backlog_cache.snapshot()
}

pub(crate) fn apply_wal_state(state: &AppState, wal_size_bytes: u64, wal_tail_open: bool) {
    state
        .persistence_backlog_cache
        .update_wal(wal_size_bytes, wal_tail_open);
}

pub(crate) fn refresh_full_scan(state: &AppState) {
    if !state.config.persistence_enabled {
        return;
    }
    let snapshot = scan_persistence_backlog(state);
    state.persistence_backlog_cache.store_snapshot(snapshot);
}

fn scan_persistence_backlog(state: &AppState) -> PersistenceBacklogSnapshot {
    let (wal_size_bytes, wal_tail_open) = read_wal_state(&state.config.wal_path);
    let incremental_dir = incremental_snapshot_dir(&state.config.snapshot_path);
    let mut incremental_segments = 0u64;
    let mut incremental_size_bytes = 0u64;

    if let Ok(entries) = fs::read_dir(incremental_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path
                .extension()
                .is_none_or(|extension| extension != "jsonl")
            {
                continue;
            }
            incremental_segments = incremental_segments.saturating_add(1);
            if let Ok(metadata) = entry.metadata() {
                incremental_size_bytes = incremental_size_bytes.saturating_add(metadata.len());
            }
        }
    }

    PersistenceBacklogSnapshot {
        wal_size_bytes,
        wal_tail_open,
        incremental_segments,
        incremental_size_bytes,
    }
}

pub(crate) fn read_wal_state(path: &Path) -> (u64, bool) {
    let wal_size_bytes = fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let wal_tail_open = wal_tail_is_open(path, wal_size_bytes);
    (wal_size_bytes, wal_tail_open)
}

fn wal_tail_is_open(path: &Path, wal_size_bytes: u64) -> bool {
    if wal_size_bytes == 0 {
        return false;
    }
    let Ok(mut file) = fs::File::open(path) else {
        return false;
    };
    if wal_size_bytes >= WAL_BINARY_MAGIC.len() as u64 {
        let mut magic = [0u8; WAL_BINARY_MAGIC.len()];
        if file.read_exact(&mut magic).is_ok() && &magic == WAL_BINARY_MAGIC {
            return false;
        }
    }
    if file.seek(SeekFrom::End(-1)).is_err() {
        return false;
    }
    let mut last = [0u8; 1];
    if file.read_exact(&mut last).is_err() {
        return false;
    }
    last[0] != b'\n'
}
