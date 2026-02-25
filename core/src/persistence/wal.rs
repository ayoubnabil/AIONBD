use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use crate::Collection;

use super::fsync::{ensure_parent_dir, sync_parent_dir, truncate_file_fully_synced};
use super::{apply_wal_record, PersistenceError, WalRecord};

pub(super) fn append_wal(
    path: &Path,
    record: &WalRecord,
    sync_on_write: bool,
) -> Result<(), PersistenceError> {
    append_wal_batch(path, std::slice::from_ref(record), sync_on_write)
}

pub(super) fn append_wal_batch(
    path: &Path,
    records: &[WalRecord],
    sync_on_write: bool,
) -> Result<(), PersistenceError> {
    if records.is_empty() {
        return Ok(());
    }
    ensure_parent_dir(path)?;

    let existed = path.exists();
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    for record in records {
        let mut line = serde_json::to_vec(record)?;
        line.push(b'\n');
        file.write_all(&line)?;
    }
    if sync_on_write {
        file.flush()?;
        file.sync_data()?;
    }
    if !existed {
        sync_parent_dir(path)?;
    }
    Ok(())
}

pub(super) fn truncate_wal(path: &Path) -> Result<(), PersistenceError> {
    ensure_parent_dir(path)?;
    truncate_file_fully_synced(path)?;
    Ok(())
}

pub(super) fn replay_wal(
    path: &Path,
    collections: &mut BTreeMap<String, Collection>,
) -> Result<(), PersistenceError> {
    if !path.exists() {
        return Ok(());
    }

    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    let mut line_number = 0usize;

    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        line_number += 1;

        let has_trailing_newline = line.ends_with('\n');
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let record = match serde_json::from_str(trimmed) {
            Ok(record) => record,
            Err(error) => {
                let tolerate_tail = !has_trailing_newline
                    && matches!(error.classify(), serde_json::error::Category::Eof)
                    && reader
                        .fill_buf()
                        .map(|remaining| remaining.is_empty())
                        .unwrap_or(false);
                if tolerate_tail {
                    break;
                }
                return Err(PersistenceError::InvalidData(format!(
                    "invalid wal line {line_number}: {error}"
                )));
            }
        };

        apply_wal_record(collections, &record).map_err(|error| {
            PersistenceError::InvalidData(format!(
                "failed to apply wal line {line_number}: {error}"
            ))
        })?;
    }

    Ok(())
}
