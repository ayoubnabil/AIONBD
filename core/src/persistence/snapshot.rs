use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{Collection, CollectionConfig, PointId};

use super::PersistenceError;

const SNAPSHOT_VERSION: u32 = 1;

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotDocument {
    version: u32,
    collections: Vec<SnapshotCollection>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotCollection {
    name: String,
    dimension: usize,
    strict_finite: bool,
    points: Vec<SnapshotPoint>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotPoint {
    id: PointId,
    values: Vec<f32>,
}

pub(super) fn load_snapshot(path: &Path) -> Result<BTreeMap<String, Collection>, PersistenceError> {
    if !path.exists() {
        return Ok(BTreeMap::new());
    }

    let raw = fs::read_to_string(path)?;
    let snapshot: SnapshotDocument = serde_json::from_str(&raw)?;

    if snapshot.version != SNAPSHOT_VERSION {
        return Err(PersistenceError::InvalidData(format!(
            "unsupported snapshot version {}",
            snapshot.version
        )));
    }

    let mut collections = BTreeMap::new();
    for entry in snapshot.collections {
        if collections.contains_key(&entry.name) {
            return Err(PersistenceError::InvalidData(format!(
                "duplicate collection '{}' in snapshot",
                entry.name
            )));
        }

        let config = CollectionConfig::new(entry.dimension, entry.strict_finite)?;
        let mut collection = Collection::new(entry.name.clone(), config)?;
        for point in entry.points {
            collection.upsert_point(point.id, point.values)?;
        }
        collections.insert(entry.name, collection);
    }

    Ok(collections)
}

pub(super) fn write_snapshot(
    path: &Path,
    collections: &BTreeMap<String, Collection>,
) -> Result<(), PersistenceError> {
    ensure_parent_dir(path)?;

    let snapshot = SnapshotDocument {
        version: SNAPSHOT_VERSION,
        collections: collections.values().map(snapshot_collection_from).collect(),
    };

    let temp_path = path.with_extension("tmp");
    let bytes = serde_json::to_vec_pretty(&snapshot)?;
    fs::write(&temp_path, bytes)?;
    fs::rename(temp_path, path)?;
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

fn snapshot_collection_from(collection: &Collection) -> SnapshotCollection {
    let points = collection
        .point_ids()
        .into_iter()
        .filter_map(|id| {
            collection.get_point(id).map(|values| SnapshotPoint {
                id,
                values: values.to_vec(),
            })
        })
        .collect();

    SnapshotCollection {
        name: collection.name().to_string(),
        dimension: collection.dimension(),
        strict_finite: collection.strict_finite(),
        points,
    }
}
