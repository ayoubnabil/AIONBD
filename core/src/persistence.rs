use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{Collection, CollectionConfig, CollectionError, PointId};

mod snapshot;

use snapshot::{load_snapshot, write_snapshot};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WalRecord {
    CreateCollection {
        name: String,
        dimension: usize,
        strict_finite: bool,
    },
    DeleteCollection {
        name: String,
    },
    UpsertPoint {
        collection: String,
        id: PointId,
        values: Vec<f32>,
    },
    DeletePoint {
        collection: String,
        id: PointId,
    },
}

#[derive(Debug)]
pub enum PersistenceError {
    Io(std::io::Error),
    Serde(serde_json::Error),
    Collection(CollectionError),
    InvalidData(String),
}

impl fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "io error: {error}"),
            Self::Serde(error) => write!(f, "serialization error: {error}"),
            Self::Collection(error) => write!(f, "collection error: {error}"),
            Self::InvalidData(message) => write!(f, "invalid persistence data: {message}"),
        }
    }
}

impl Error for PersistenceError {}

impl From<std::io::Error> for PersistenceError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for PersistenceError {
    fn from(value: serde_json::Error) -> Self {
        Self::Serde(value)
    }
}

impl From<CollectionError> for PersistenceError {
    fn from(value: CollectionError) -> Self {
        Self::Collection(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersistOutcome {
    Checkpointed,
    WalOnly { reason: String },
}

pub fn load_collections(
    snapshot_path: &Path,
    wal_path: &Path,
) -> Result<BTreeMap<String, Collection>, PersistenceError> {
    let mut collections = load_snapshot(snapshot_path)?;
    replay_wal(wal_path, &mut collections)?;
    Ok(collections)
}

pub fn persist_change(
    snapshot_path: &Path,
    wal_path: &Path,
    collections: &BTreeMap<String, Collection>,
    record: &WalRecord,
) -> Result<PersistOutcome, PersistenceError> {
    append_wal(wal_path, record)?;

    match write_snapshot(snapshot_path, collections).and_then(|_| truncate_wal(wal_path)) {
        Ok(()) => Ok(PersistOutcome::Checkpointed),
        Err(error) => Ok(PersistOutcome::WalOnly {
            reason: error.to_string(),
        }),
    }
}

pub fn apply_wal_record(
    collections: &mut BTreeMap<String, Collection>,
    record: &WalRecord,
) -> Result<(), PersistenceError> {
    match record {
        WalRecord::CreateCollection {
            name,
            dimension,
            strict_finite,
        } => {
            if let Some(existing) = collections.get(name) {
                if existing.dimension() == *dimension && existing.strict_finite() == *strict_finite
                {
                    return Ok(());
                }
                return Err(PersistenceError::InvalidData(format!(
                    "collection '{name}' already exists with different config"
                )));
            }

            let config = CollectionConfig::new(*dimension, *strict_finite)?;
            let collection = Collection::new(name.clone(), config)?;
            collections.insert(name.clone(), collection);
            Ok(())
        }
        WalRecord::DeleteCollection { name } => {
            let _ = collections.remove(name);
            Ok(())
        }
        WalRecord::UpsertPoint {
            collection,
            id,
            values,
        } => {
            let target = collections.get_mut(collection).ok_or_else(|| {
                PersistenceError::InvalidData(format!("collection '{collection}' does not exist"))
            })?;
            target.upsert_point(*id, values.clone())?;
            Ok(())
        }
        WalRecord::DeletePoint { collection, id } => {
            let target = collections.get_mut(collection).ok_or_else(|| {
                PersistenceError::InvalidData(format!("collection '{collection}' does not exist"))
            })?;
            let _ = target.remove_point(*id);
            Ok(())
        }
    }
}

fn append_wal(path: &Path, record: &WalRecord) -> Result<(), PersistenceError> {
    ensure_parent_dir(path)?;

    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, record)?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
}

fn truncate_wal(path: &Path) -> Result<(), PersistenceError> {
    ensure_parent_dir(path)?;
    fs::write(path, b"")?;
    Ok(())
}

fn replay_wal(
    path: &Path,
    collections: &mut BTreeMap<String, Collection>,
) -> Result<(), PersistenceError> {
    if !path.exists() {
        return Ok(());
    }

    let file = File::open(path)?;
    for (index, line) in BufReader::new(file).lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let record: WalRecord = serde_json::from_str(&line).map_err(|error| {
            PersistenceError::InvalidData(format!("invalid wal line {}: {error}", index + 1))
        })?;

        apply_wal_record(collections, &record)?;
    }

    Ok(())
}

fn ensure_parent_dir(path: &Path) -> Result<(), PersistenceError> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
