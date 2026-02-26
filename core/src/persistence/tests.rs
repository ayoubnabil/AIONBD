use std::collections::BTreeMap;
use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{Collection, CollectionConfig};

use super::{
    append_wal_record, apply_wal_record, checkpoint_wal, incremental_snapshot_dir,
    load_collections, persist_change, WalRecord,
};

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
            payload: None,
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
fn delete_collection_wal_record_is_idempotent() {
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
        &WalRecord::DeleteCollection {
            name: "demo".to_string(),
        },
    )
    .expect("delete should succeed");
    apply_wal_record(
        &mut collections,
        &WalRecord::DeleteCollection {
            name: "demo".to_string(),
        },
    )
    .expect("delete replay should be idempotent");

    assert!(!collections.contains_key("demo"));
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

#[test]
fn load_collections_replays_binary_wal_when_snapshot_missing() {
    let (root, snapshot_path, wal_path) = test_paths("wal_binary_only");

    append_wal_record(
        &wal_path,
        &WalRecord::CreateCollection {
            name: "demo".to_string(),
            dimension: 2,
            strict_finite: true,
        },
    )
    .expect("binary wal create append should succeed");
    append_wal_record(
        &wal_path,
        &WalRecord::UpsertPoint {
            collection: "demo".to_string(),
            id: 9,
            values: vec![3.0, 4.0],
            payload: None,
        },
    )
    .expect("binary wal upsert append should succeed");

    let collections = load_collections(&snapshot_path, &wal_path).expect("load should succeed");
    let collection = collections.get("demo").expect("collection should exist");
    assert_eq!(collection.get_point(9), Some(&[3.0, 4.0][..]));

    cleanup(&root);
}

#[test]
fn load_collections_tolerates_truncated_last_wal_record() {
    let (root, snapshot_path, wal_path) = test_paths("wal_truncated_tail");
    fs::create_dir_all(root.join("data")).expect("data directory should be creatable");

    fs::write(
        &wal_path,
        [
            r#"{"type":"create_collection","name":"demo","dimension":2,"strict_finite":true}"#,
            r#"{"type":"upsert_point","collection":"demo","id":5,"values":[1.0,2.0]}"#,
            r#"{"type":"upsert_point","collection":"demo","id":6,"values":[9.0"#,
        ]
        .join("\n"),
    )
    .expect("wal should be writable");

    let collections = load_collections(&snapshot_path, &wal_path)
        .expect("load should tolerate truncated tail record");
    let collection = collections.get("demo").expect("collection should exist");
    assert_eq!(collection.get_point(5), Some(&[1.0, 2.0][..]));
    assert_eq!(collection.get_point(6), None);

    cleanup(&root);
}

#[test]
fn load_collections_tolerates_truncated_last_binary_wal_record() {
    let (root, snapshot_path, wal_path) = test_paths("wal_binary_truncated_tail");

    append_wal_record(
        &wal_path,
        &WalRecord::CreateCollection {
            name: "demo".to_string(),
            dimension: 2,
            strict_finite: true,
        },
    )
    .expect("binary wal create append should succeed");
    append_wal_record(
        &wal_path,
        &WalRecord::UpsertPoint {
            collection: "demo".to_string(),
            id: 5,
            values: vec![1.0, 2.0],
            payload: None,
        },
    )
    .expect("binary wal upsert append should succeed");

    let len = fs::metadata(&wal_path)
        .expect("binary wal should exist")
        .len();
    let file = fs::OpenOptions::new()
        .write(true)
        .open(&wal_path)
        .expect("binary wal should be writable");
    file.set_len(len.saturating_sub(5))
        .expect("truncate should succeed");

    let collections = load_collections(&snapshot_path, &wal_path)
        .expect("load should tolerate truncated binary tail");
    let collection = collections.get("demo").expect("collection should exist");
    assert_eq!(collection.get_point(5), None);

    cleanup(&root);
}

#[test]
fn load_collections_tolerates_legacy_json_tail_after_binary_records() {
    let (root, snapshot_path, wal_path) = test_paths("wal_binary_with_json_tail");

    append_wal_record(
        &wal_path,
        &WalRecord::CreateCollection {
            name: "demo".to_string(),
            dimension: 2,
            strict_finite: true,
        },
    )
    .expect("binary wal create append should succeed");
    append_wal_record(
        &wal_path,
        &WalRecord::UpsertPoint {
            collection: "demo".to_string(),
            id: 5,
            values: vec![1.0, 2.0],
            payload: None,
        },
    )
    .expect("binary wal upsert append should succeed");

    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .expect("binary wal should be appendable");
    file.write_all(br#"{"type":"upsert_point","collection":"demo","id":9,"values":[3.0"#)
        .expect("legacy truncated json tail should be writable");
    file.flush().expect("flush should succeed");

    let collections =
        load_collections(&snapshot_path, &wal_path).expect("load should tolerate legacy json tail");
    let collection = collections.get("demo").expect("collection should exist");
    assert_eq!(collection.get_point(5), Some(&[1.0, 2.0][..]));
    assert_eq!(collection.get_point(9), None);

    cleanup(&root);
}

#[test]
fn load_collections_rejects_binary_wal_checksum_mismatch() {
    let (root, snapshot_path, wal_path) = test_paths("wal_binary_checksum_mismatch");

    append_wal_record(
        &wal_path,
        &WalRecord::CreateCollection {
            name: "demo".to_string(),
            dimension: 2,
            strict_finite: true,
        },
    )
    .expect("binary wal create append should succeed");
    append_wal_record(
        &wal_path,
        &WalRecord::UpsertPoint {
            collection: "demo".to_string(),
            id: 5,
            values: vec![1.0, 2.0],
            payload: None,
        },
    )
    .expect("binary wal upsert append should succeed");

    let mut bytes = fs::read(&wal_path).expect("binary wal should be readable");
    let last = bytes.last_mut().expect("binary wal should not be empty");
    *last ^= 0x01;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&wal_path)
        .expect("binary wal should be writable");
    file.seek(SeekFrom::Start(0)).expect("seek should succeed");
    file.write_all(&bytes).expect("rewrite should succeed");
    file.flush().expect("flush should succeed");

    let error = load_collections(&snapshot_path, &wal_path)
        .expect_err("checksum mismatch should fail replay");
    assert!(error.to_string().contains("checksum mismatch"));

    cleanup(&root);
}

#[test]
fn load_collections_tolerates_replayed_create_after_snapshot() {
    let (root, snapshot_path, wal_path) = test_paths("idempotent_create");

    fs::create_dir_all(root.join("data")).expect("data directory should be creatable");
    fs::write(
        &snapshot_path,
        r#"{"version":1,"collections":[{"name":"demo","dimension":2,"strict_finite":true,"points":[]}]} "#,
    )
    .expect("snapshot should be writable");
    fs::write(
        &wal_path,
        r#"{"type":"create_collection","name":"demo","dimension":2,"strict_finite":true}"#,
    )
    .expect("wal should be writable");

    let collections = load_collections(&snapshot_path, &wal_path).expect("load should succeed");
    assert!(collections.contains_key("demo"));

    cleanup(&root);
}

#[test]
fn load_collections_tolerates_legacy_invalid_create_record() {
    let (root, snapshot_path, wal_path) = test_paths("invalid_create_record");
    fs::create_dir_all(root.join("data")).expect("data directory should be creatable");

    fs::write(
        &wal_path,
        [
            r#"{"type":"create_collection","name":"bad","dimension":0,"strict_finite":true}"#,
            r#"{"type":"create_collection","name":"demo","dimension":2,"strict_finite":true}"#,
            r#"{"type":"upsert_point","collection":"demo","id":7,"values":[1.0,2.0]}"#,
        ]
        .join("\n"),
    )
    .expect("wal should be writable");

    let collections = load_collections(&snapshot_path, &wal_path).expect("load should succeed");
    assert!(!collections.contains_key("bad"));
    let demo = collections
        .get("demo")
        .expect("demo collection should exist");
    assert_eq!(demo.get_point(7), Some(&[1.0, 2.0][..]));

    cleanup(&root);
}

#[test]
fn load_collections_replays_incremental_snapshot_segments() {
    let (root, snapshot_path, wal_path) = test_paths("incrementals");
    fs::create_dir_all(&root).expect("root directory should exist");

    fs::write(
        &snapshot_path,
        r#"{"version":1,"collections":[{"name":"demo","dimension":2,"strict_finite":true,"points":[]}]} "#,
    )
    .expect("snapshot should be writable");

    let incremental_dir = incremental_snapshot_dir(&snapshot_path);
    fs::create_dir_all(&incremental_dir).expect("incremental directory should exist");
    fs::write(
        incremental_dir.join("0000000000000001.jsonl"),
        r#"{"type":"upsert_point","collection":"demo","id":1,"values":[3.0,4.0]}"#,
    )
    .expect("incremental snapshot should be writable");

    let collections = load_collections(&snapshot_path, &wal_path).expect("load should succeed");
    let collection = collections.get("demo").expect("collection should exist");
    assert_eq!(collection.get_point(1), Some(&[3.0, 4.0][..]));

    cleanup(&root);
}

#[test]
fn checkpoint_wal_rotates_segments_and_compacts_after_threshold() {
    let (root, snapshot_path, wal_path) = test_paths("wal_compaction");

    for id in 0..8_u64 {
        append_wal_record(
            &wal_path,
            &WalRecord::CreateCollection {
                name: format!("demo_{id}"),
                dimension: 2,
                strict_finite: true,
            },
        )
        .expect("append wal should succeed");
        let outcome = checkpoint_wal(&snapshot_path, &wal_path).expect("checkpoint should succeed");
        assert!(matches!(outcome, super::PersistOutcome::Checkpointed));
    }

    let incremental_dir = incremental_snapshot_dir(&snapshot_path);
    let remaining_segments = fs::read_dir(&incremental_dir)
        .expect("incremental directory should be readable")
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "jsonl")
        })
        .count();
    assert_eq!(remaining_segments, 0);

    let restored = load_collections(&snapshot_path, &wal_path).expect("restore should succeed");
    assert_eq!(restored.len(), 8);

    cleanup(&root);
}
