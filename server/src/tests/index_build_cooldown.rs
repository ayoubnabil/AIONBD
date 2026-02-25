use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use aionbd_core::{Collection, CollectionConfig};

use crate::config::AppConfig;
use crate::index_manager::schedule_l2_build_if_needed;
use crate::ivf_index::IvfIndex;
use crate::state::AppState;

fn test_state() -> AppState {
    let config = AppConfig {
        bind: "127.0.0.1:0".parse().expect("socket addr must parse"),
        max_dimension: 8,
        max_points_per_collection: 1_000_000,
        strict_finite: true,
        request_timeout_ms: 2_000,
        max_body_bytes: 1_048_576,
        max_concurrency: 256,
        max_page_limit: 1_000,
        max_topk_limit: 1_000,
        checkpoint_interval: 1,
        persistence_enabled: false,
        wal_sync_on_write: true,
        snapshot_path: std::path::PathBuf::from("unused_snapshot.json"),
        wal_path: std::path::PathBuf::from("unused_wal.jsonl"),
    };

    AppState::with_collections(config, std::collections::BTreeMap::new())
}

fn seeded_collection(name: &str) -> Collection {
    let mut collection = Collection::new(
        name.to_string(),
        CollectionConfig::new(2, true).expect("config should be valid"),
    )
    .expect("collection should be valid");
    for id in 0..IvfIndex::min_indexed_points() as u64 {
        collection
            .upsert_point(id, vec![id as f32, 0.0])
            .expect("upsert should succeed");
    }
    collection
}

#[test]
fn schedule_skips_when_cooldown_is_active() {
    let state = test_state();
    let collection = seeded_collection("cooldown_skip");
    state
        .l2_index_last_started_ms
        .lock()
        .expect("timestamp lock should be available")
        .insert("cooldown_skip".to_string(), u64::MAX);

    schedule_l2_build_if_needed(&state, "cooldown_skip", &collection);

    assert_eq!(
        state
            .metrics
            .l2_index_build_cooldown_skips
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        state
            .metrics
            .l2_index_build_requests
            .load(Ordering::Relaxed),
        0
    );
    assert!(!state
        .l2_index_building
        .read()
        .expect("build marker lock should be available")
        .contains("cooldown_skip"));
}

#[tokio::test]
async fn schedule_records_request_when_not_throttled() {
    let state = test_state();
    let name = "cooldown_allowed";
    let handle = Arc::new(RwLock::new(seeded_collection(name)));
    state
        .collections
        .write()
        .expect("collection registry lock should be available")
        .insert(name.to_string(), Arc::clone(&handle));

    {
        let collection = handle.read().expect("collection lock should be available");
        schedule_l2_build_if_needed(&state, name, &collection);
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        state
            .metrics
            .l2_index_build_requests
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        state
            .metrics
            .l2_index_build_cooldown_skips
            .load(Ordering::Relaxed),
        0
    );
}
