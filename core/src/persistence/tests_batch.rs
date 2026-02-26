use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::Collection;

use super::{append_wal_records_with_sync, replay_wal, WalRecord};

fn wal_path(prefix: &str) -> std::path::PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be monotonic")
        .as_nanos();
    std::env::temp_dir().join(format!("aionbd_{prefix}_{stamp}.jsonl"))
}

fn cleanup(path: &Path) {
    if path.exists() {
        fs::remove_file(path).expect("temp wal should be removable");
    }
}

#[test]
fn append_wal_records_with_sync_appends_all_records() {
    let wal = wal_path("batch_append");
    let records = vec![
        WalRecord::CreateCollection {
            name: "demo".to_string(),
            dimension: 2,
            strict_finite: true,
        },
        WalRecord::UpsertPoint {
            collection: "demo".to_string(),
            id: 7,
            values: vec![1.0, 2.0],
            payload: None,
        },
    ];

    append_wal_records_with_sync(&wal, &records, true).expect("batch append should succeed");

    let mut collections = std::collections::BTreeMap::<String, Collection>::new();
    replay_wal(&wal, &mut collections).expect("replay should succeed");
    let collection = collections.get("demo").expect("collection should exist");
    assert_eq!(collection.get_point(7), Some(&[1.0, 2.0][..]));

    cleanup(&wal);
}
