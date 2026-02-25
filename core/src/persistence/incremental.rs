use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use crate::Collection;

use super::fsync::sync_parent_dir;
use super::{
    load_snapshot, replay_wal, truncate_wal, write_snapshot, PersistOutcome, PersistenceError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointPolicy {
    pub incremental_compact_after: usize,
}

impl Default for CheckpointPolicy {
    fn default() -> Self {
        Self {
            incremental_compact_after: 8,
        }
    }
}

pub(super) fn replay_incremental_snapshots(
    snapshot_path: &Path,
    collections: &mut BTreeMap<String, Collection>,
) -> Result<(), PersistenceError> {
    for path in list_incremental_snapshots(snapshot_path)? {
        replay_wal(&path, collections)?;
    }
    Ok(())
}

pub fn checkpoint_snapshot_with_policy(
    snapshot_path: &Path,
    wal_path: &Path,
    collections: &BTreeMap<String, Collection>,
    policy: CheckpointPolicy,
) -> Result<PersistOutcome, PersistenceError> {
    match checkpoint_wal_with_policy(snapshot_path, wal_path, policy)
        .and_then(|_| maybe_compact_incrementals(snapshot_path, collections, policy))
    {
        Ok(()) => Ok(PersistOutcome::Checkpointed),
        Err(error) => Ok(PersistOutcome::WalOnly {
            reason: error.to_string(),
        }),
    }
}

pub fn checkpoint_wal(
    snapshot_path: &Path,
    wal_path: &Path,
) -> Result<PersistOutcome, PersistenceError> {
    let policy = CheckpointPolicy::default();
    checkpoint_wal_with_policy(snapshot_path, wal_path, policy)
        .and_then(|_| maybe_compact_incrementals_from_persisted_state(snapshot_path, policy))
        .map(|_| PersistOutcome::Checkpointed)
        .or_else(|error| {
            Ok(PersistOutcome::WalOnly {
                reason: error.to_string(),
            })
        })
}

pub fn checkpoint_wal_with_policy(
    snapshot_path: &Path,
    wal_path: &Path,
    _policy: CheckpointPolicy,
) -> Result<(), PersistenceError> {
    let _ = rotate_wal_to_incremental(snapshot_path, wal_path)?;
    Ok(())
}

pub fn incremental_snapshot_dir(snapshot_path: &Path) -> PathBuf {
    snapshot_path.with_extension("incrementals")
}

fn maybe_compact_incrementals(
    snapshot_path: &Path,
    collections: &BTreeMap<String, Collection>,
    policy: CheckpointPolicy,
) -> Result<(), PersistenceError> {
    let incremental_paths = list_incremental_snapshots(snapshot_path)?;
    if incremental_paths.len() < policy.incremental_compact_after {
        return Ok(());
    }

    write_snapshot(snapshot_path, collections)?;
    clear_incremental_snapshots(snapshot_path)?;
    Ok(())
}

fn maybe_compact_incrementals_from_persisted_state(
    snapshot_path: &Path,
    policy: CheckpointPolicy,
) -> Result<(), PersistenceError> {
    let incremental_paths = list_incremental_snapshots(snapshot_path)?;
    if incremental_paths.len() < policy.incremental_compact_after {
        return Ok(());
    }

    let mut persisted = load_snapshot(snapshot_path)?;
    replay_incremental_snapshots(snapshot_path, &mut persisted)?;
    write_snapshot(snapshot_path, &persisted)?;
    clear_incremental_snapshots(snapshot_path)?;
    Ok(())
}

fn rotate_wal_to_incremental(
    snapshot_path: &Path,
    wal_path: &Path,
) -> Result<bool, PersistenceError> {
    if !wal_path.exists() {
        return Ok(false);
    }

    let wal_len = fs::metadata(wal_path)?.len();
    if wal_len == 0 {
        return Ok(false);
    }

    let incremental_dir = incremental_snapshot_dir(snapshot_path);
    fs::create_dir_all(&incremental_dir)?;
    sync_parent_dir(&incremental_dir)?;
    let seq = next_incremental_seq(&incremental_dir)?;
    let incremental_path = incremental_dir.join(format!("{seq:020}.jsonl"));
    move_wal_to_incremental(wal_path, &incremental_path)?;
    sync_parent_dir(&incremental_path)?;
    truncate_wal(wal_path)?;
    sync_parent_dir(wal_path)?;
    Ok(true)
}

fn move_wal_to_incremental(
    wal_path: &Path,
    incremental_path: &Path,
) -> Result<(), PersistenceError> {
    match fs::rename(wal_path, incremental_path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::CrossesDevices => {
            copy_wal_across_filesystems(wal_path, incremental_path)
        }
        Err(error) => Err(error.into()),
    }
}

fn copy_wal_across_filesystems(
    wal_path: &Path,
    incremental_path: &Path,
) -> Result<(), PersistenceError> {
    let temp_path = incremental_path.with_extension("tmp");
    fs::copy(wal_path, &temp_path)?;
    File::open(&temp_path)?.sync_all()?;
    fs::rename(&temp_path, incremental_path)?;
    sync_parent_dir(incremental_path)?;
    Ok(())
}

fn list_incremental_snapshots(snapshot_path: &Path) -> Result<Vec<PathBuf>, PersistenceError> {
    let dir = incremental_snapshot_dir(snapshot_path);
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut paths = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path
            .extension()
            .is_some_and(|extension| extension == "jsonl")
        {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

fn next_incremental_seq(dir: &Path) -> Result<u64, PersistenceError> {
    if !dir.exists() {
        return Ok(1);
    }

    let mut max_seq = 0u64;
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        let Some(raw_seq) = file_name.split('.').next() else {
            continue;
        };
        if let Ok(seq) = raw_seq.parse::<u64>() {
            max_seq = max_seq.max(seq);
        }
    }
    Ok(max_seq.saturating_add(1))
}

fn clear_incremental_snapshots(snapshot_path: &Path) -> Result<(), PersistenceError> {
    let dir = incremental_snapshot_dir(snapshot_path);
    if !dir.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path
            .extension()
            .is_some_and(|extension| extension == "jsonl")
        {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}
