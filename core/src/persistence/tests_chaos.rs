use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use super::load_collections;

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
fn load_collections_ignores_invalid_incremental_path_entry() {
    let (root, snapshot_path, wal_path) = test_paths("invalid_incrementals_entry");
    fs::create_dir_all(&root).expect("temp directory should be creatable");

    let incremental_marker = snapshot_path.with_extension("incrementals");
    fs::write(&incremental_marker, b"not-a-directory")
        .expect("incremental marker file should be writable");
    fs::write(
        &wal_path,
        [
            r#"{"type":"create_collection","name":"demo","dimension":2,"strict_finite":true}"#,
            r#"{"type":"upsert_point","collection":"demo","id":7,"values":[1.0,2.0]}"#,
            "",
        ]
        .join("\n"),
    )
    .expect("wal should be writable");

    let collections = load_collections(&snapshot_path, &wal_path)
        .expect("load should ignore invalid incremental path and still replay wal");
    let collection = collections
        .get("demo")
        .expect("demo collection should exist");
    assert_eq!(collection.get_point(7), Some(&[1.0_f32, 2.0_f32][..]));

    cleanup(&root);
}
