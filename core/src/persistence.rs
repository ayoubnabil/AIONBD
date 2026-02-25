use crate::{Collection, CollectionConfig, CollectionError, MetadataPayload, PointId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::path::Path;
mod fsync;
mod incremental;
mod snapshot;
mod wal;
pub use incremental::{incremental_snapshot_dir, CheckpointPolicy};
use snapshot::{load_snapshot, write_snapshot};
use wal::{append_wal, replay_wal, truncate_wal};

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
        #[serde(default)]
        payload: Option<MetadataPayload>,
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
    incremental::replay_incremental_snapshots(snapshot_path, &mut collections)?;
    replay_wal(wal_path, &mut collections)?;
    Ok(collections)
}

pub fn persist_change(
    snapshot_path: &Path,
    wal_path: &Path,
    collections: &BTreeMap<String, Collection>,
    record: &WalRecord,
) -> Result<PersistOutcome, PersistenceError> {
    append_wal_record(wal_path, record)?;
    checkpoint_snapshot_legacy(snapshot_path, wal_path, collections)
}

pub fn append_wal_record(wal_path: &Path, record: &WalRecord) -> Result<(), PersistenceError> {
    append_wal_record_with_sync(wal_path, record, true)
}

pub fn append_wal_record_with_sync(
    wal_path: &Path,
    record: &WalRecord,
    sync_on_write: bool,
) -> Result<(), PersistenceError> {
    append_wal(wal_path, record, sync_on_write)
}

pub fn checkpoint_snapshot(
    snapshot_path: &Path,
    wal_path: &Path,
    collections: &BTreeMap<String, Collection>,
) -> Result<PersistOutcome, PersistenceError> {
    checkpoint_snapshot_with_policy(
        snapshot_path,
        wal_path,
        collections,
        CheckpointPolicy::default(),
    )
}

pub fn checkpoint_snapshot_with_policy(
    snapshot_path: &Path,
    wal_path: &Path,
    collections: &BTreeMap<String, Collection>,
    policy: CheckpointPolicy,
) -> Result<PersistOutcome, PersistenceError> {
    incremental::checkpoint_snapshot_with_policy(snapshot_path, wal_path, collections, policy)
}

pub fn checkpoint_wal(
    snapshot_path: &Path,
    wal_path: &Path,
) -> Result<PersistOutcome, PersistenceError> {
    incremental::checkpoint_wal(snapshot_path, wal_path)
}

pub fn checkpoint_wal_with_policy(
    snapshot_path: &Path,
    wal_path: &Path,
    policy: CheckpointPolicy,
) -> Result<(), PersistenceError> {
    incremental::checkpoint_wal_with_policy(snapshot_path, wal_path, policy)
}

fn checkpoint_snapshot_legacy(
    snapshot_path: &Path,
    wal_path: &Path,
    collections: &BTreeMap<String, Collection>,
) -> Result<PersistOutcome, PersistenceError> {
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

            let config = match CollectionConfig::new(*dimension, *strict_finite) {
                Ok(config) => config,
                Err(CollectionError::InvalidConfig(_)) => {
                    // Tolerate legacy poisoned WAL create records (e.g. dimension=0)
                    // so startup can still recover the rest of the state.
                    return Ok(());
                }
                Err(error) => return Err(error.into()),
            };
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
            payload,
        } => {
            let target = collections.get_mut(collection).ok_or_else(|| {
                PersistenceError::InvalidData(format!("collection '{collection}' does not exist"))
            })?;
            target.upsert_point_with_payload(
                *id,
                values.clone(),
                payload.clone().unwrap_or_default(),
            )?;
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

#[cfg(test)]
mod tests;
