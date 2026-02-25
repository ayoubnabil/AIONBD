use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{Collection, CollectionConfig, MetadataPayload, PointId};

use super::fsync::{ensure_parent_dir, sync_parent_dir};
use super::PersistenceError;

const LEGACY_SNAPSHOT_VERSION: u32 = 1;
const SNAPSHOT_VERSION: u32 = 2;

#[derive(Debug, Serialize, Deserialize)]
struct LegacySnapshotDocument {
    version: u32,
    collections: Vec<LegacySnapshotCollection>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LegacySnapshotCollection {
    name: String,
    dimension: usize,
    strict_finite: bool,
    points: Vec<LegacySnapshotPoint>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LegacySnapshotPoint {
    id: PointId,
    values: Vec<f32>,
    #[serde(default)]
    payload: MetadataPayload,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SnapshotRecordOwned {
    SnapshotHeader {
        version: u32,
    },
    CreateCollection {
        name: String,
        dimension: usize,
        strict_finite: bool,
    },
    UpsertPoint {
        collection: String,
        id: PointId,
        values: Vec<f32>,
        #[serde(default)]
        payload: MetadataPayload,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SnapshotRecordRef<'a> {
    SnapshotHeader {
        version: u32,
    },
    CreateCollection {
        name: &'a str,
        dimension: usize,
        strict_finite: bool,
    },
    UpsertPoint {
        collection: &'a str,
        id: PointId,
        values: &'a [f32],
        #[serde(skip_serializing_if = "Option::is_none")]
        payload: Option<&'a MetadataPayload>,
    },
}

pub(super) fn load_snapshot(path: &Path) -> Result<BTreeMap<String, Collection>, PersistenceError> {
    if !path.exists() {
        return Ok(BTreeMap::new());
    }

    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let Some(first_line) = read_first_non_empty_line(&mut reader)? else {
        return Ok(BTreeMap::new());
    };

    match serde_json::from_str::<SnapshotRecordOwned>(first_line.trim()) {
        Ok(SnapshotRecordOwned::SnapshotHeader { version }) => {
            if version != SNAPSHOT_VERSION {
                return Err(PersistenceError::InvalidData(format!(
                    "unsupported snapshot version {version}"
                )));
            }
            load_snapshot_v2_from_reader(reader)
        }
        Ok(_) => Err(PersistenceError::InvalidData(
            "snapshot header must be the first record".to_string(),
        )),
        Err(_) => load_snapshot_legacy(path),
    }
}

pub(super) fn write_snapshot(
    path: &Path,
    collections: &BTreeMap<String, Collection>,
) -> Result<(), PersistenceError> {
    ensure_parent_dir(path)?;

    let temp_path = path.with_extension("tmp");
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp_path)?;
    let mut writer = BufWriter::new(file);
    write_snapshot_record(
        &mut writer,
        &SnapshotRecordRef::SnapshotHeader {
            version: SNAPSHOT_VERSION,
        },
    )?;

    for collection in collections.values() {
        write_snapshot_record(
            &mut writer,
            &SnapshotRecordRef::CreateCollection {
                name: collection.name(),
                dimension: collection.dimension(),
                strict_finite: collection.strict_finite(),
            },
        )?;
        for (id, values, payload) in collection.iter_points_with_payload() {
            write_snapshot_record(
                &mut writer,
                &SnapshotRecordRef::UpsertPoint {
                    collection: collection.name(),
                    id,
                    values,
                    payload: (!payload.is_empty()).then_some(payload),
                },
            )?;
        }
    }

    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);
    fs::rename(&temp_path, path)?;
    sync_parent_dir(path)?;
    Ok(())
}

fn load_snapshot_legacy(path: &Path) -> Result<BTreeMap<String, Collection>, PersistenceError> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let snapshot: LegacySnapshotDocument = serde_json::from_reader(reader)?;

    if snapshot.version != LEGACY_SNAPSHOT_VERSION {
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
            collection.upsert_point_with_payload(point.id, point.values, point.payload)?;
        }
        collections.insert(entry.name, collection);
    }
    Ok(collections)
}

fn load_snapshot_v2_from_reader(
    mut reader: BufReader<File>,
) -> Result<BTreeMap<String, Collection>, PersistenceError> {
    let mut collections = BTreeMap::new();
    let mut line = String::new();
    let mut line_number = 1usize;

    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        line_number += 1;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let record: SnapshotRecordOwned = serde_json::from_str(trimmed).map_err(|error| {
            PersistenceError::InvalidData(format!(
                "invalid snapshot record at line {line_number}: {error}"
            ))
        })?;
        apply_snapshot_record(&mut collections, record, line_number)?;
    }

    Ok(collections)
}

fn apply_snapshot_record(
    collections: &mut BTreeMap<String, Collection>,
    record: SnapshotRecordOwned,
    line_number: usize,
) -> Result<(), PersistenceError> {
    match record {
        SnapshotRecordOwned::SnapshotHeader { .. } => Err(PersistenceError::InvalidData(format!(
            "unexpected snapshot header at line {line_number}"
        ))),
        SnapshotRecordOwned::CreateCollection {
            name,
            dimension,
            strict_finite,
        } => {
            if collections.contains_key(&name) {
                return Err(PersistenceError::InvalidData(format!(
                    "duplicate collection '{name}' in snapshot"
                )));
            }
            let config = CollectionConfig::new(dimension, strict_finite)?;
            let collection = Collection::new(name.clone(), config)?;
            collections.insert(name, collection);
            Ok(())
        }
        SnapshotRecordOwned::UpsertPoint {
            collection,
            id,
            values,
            payload,
        } => {
            let target = collections.get_mut(&collection).ok_or_else(|| {
                PersistenceError::InvalidData(format!(
                    "unknown collection '{collection}' in snapshot line {line_number}"
                ))
            })?;
            target.upsert_point_with_payload(id, values, payload)?;
            Ok(())
        }
    }
}

fn write_snapshot_record(
    writer: &mut BufWriter<File>,
    record: &SnapshotRecordRef<'_>,
) -> Result<(), PersistenceError> {
    serde_json::to_writer(&mut *writer, record)?;
    writer.write_all(b"\n")?;
    Ok(())
}

fn read_first_non_empty_line(
    reader: &mut BufReader<File>,
) -> Result<Option<String>, PersistenceError> {
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            return Ok(None);
        }
        if !line.trim().is_empty() {
            return Ok(Some(line));
        }
    }
}
