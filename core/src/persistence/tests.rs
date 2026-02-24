use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{Collection, CollectionConfig};

use super::{apply_wal_record, load_collections, persist_change, WalRecord};

fn test_paths(prefix: &str) -> (PathBuf, PathBuf, PathBuf) {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock must be monotonic")
        .as_nanos();

    let root = std::env::temp_dir().join(format!("aionbd_{prefix}_{timestamp}"));
    let snapshot_path = root.join("snapshot.json");
    let wal_path = root.join("wal.jsonl");
    (root, snapshot_path, wal_path)
}

fn cleanup(root: &Path) {
    if root.exists() {
        fs::remove_dir_all(root).expect("temp directory should be removable");
    }
}

#[test]
fn wal_records_apply_in_order() {
    let mut collections = BTreeMap::<String, Collection>::new();

    apply_wal_record(
        &mut collections,
        &WalRecord::CreateCollection {
            name: "demo".to_string(),
            dimension: 3,
            strict_finite: true,
        },
    )
    .expect("create must succeed");

    apply_wal_record(
        &mut collections,
        &WalRecord::UpsertPoint {
            collection: "demo".to_string(),
            id: 1,
            values: vec![1.0, 2.0, 3.0],
        },
    )
    .expect("upsert must succeed");

    apply_wal_record(
        &mut collections,
        &WalRecord::DeletePoint {
            collection: "demo".to_string(),
            id: 1,
        },
    )
    .expect("delete must succeed");

    let collection = collections.get("demo").expect("collection should exist");
    assert_eq!(collection.len(), 0);
}

#[test]
fn persist_change_roundtrip_restores_data() {
    let (root, snapshot_path, wal_path) = test_paths("roundtrip");

    let config = CollectionConfig::new(3, true).expect("config must be valid");
    let mut collection = Collection::new("demo", config).expect("collection must be valid");
    collection
        .upsert_point(7, vec![7.0, 8.0, 9.0])
        .expect("upsert must succeed");

    let mut collections = BTreeMap::new();
    collections.insert("demo".to_string(), collection);

    persist_change(
        &snapshot_path,
        &wal_path,
        &collections,
        &WalRecord::CreateCollection {
            name: "demo".to_string(),
            dimension: 3,
            strict_finite: true,
        },
    )
    .expect("persist change must succeed");

    let restored = load_collections(&snapshot_path, &wal_path).expect("restore should succeed");
    let restored_collection = restored.get("demo").expect("collection should exist");
    assert_eq!(restored_collection.dimension(), 3);
    assert_eq!(restored_collection.get_point(7), Some(&[7.0, 8.0, 9.0][..]));

    cleanup(&root);
}

#[test]
fn load_collections_replays_wal_when_snapshot_missing() {
    let (root, snapshot_path, wal_path) = test_paths("wal_only");

    fs::create_dir_all(root.join("data")).expect("data directory should be creatable");
    fs::write(
        &wal_path,
        [
            r#"{"type":"create_collection","name":"demo","dimension":2,"strict_finite":true}"#,
            r#"{"type":"upsert_point","collection":"demo","id":5,"values":[1.0,2.0]}"#,
            "",
        ]
        .join("\n"),
    )
    .expect("wal should be writable");

    let collections = load_collections(&snapshot_path, &wal_path).expect("load should succeed");
    let collection = collections.get("demo").expect("collection should exist");
    assert_eq!(collection.get_point(5), Some(&[1.0, 2.0][..]));

    cleanup(&root);
}
