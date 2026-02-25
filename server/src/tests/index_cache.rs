use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use aionbd_core::{Collection, CollectionConfig};

use crate::build_app;
use crate::config::AppConfig;
use crate::index_manager::remove_l2_index_entry;
use crate::ivf_index::IvfIndex;
use crate::state::AppState;

fn test_state() -> AppState {
    let config = AppConfig {
        bind: "127.0.0.1:0".parse().expect("socket addr must parse"),
        max_dimension: 8,
        strict_finite: true,
        request_timeout_ms: 2_000,
        max_body_bytes: 1_048_576,
        max_concurrency: 256,
        max_page_limit: 1_000,
        max_topk_limit: 1_000,
        checkpoint_interval: 1,
        persistence_enabled: false,
        snapshot_path: std::path::PathBuf::from("unused_snapshot.json"),
        wal_path: std::path::PathBuf::from("unused_wal.jsonl"),
    };

    AppState::with_collections(config, std::collections::BTreeMap::new())
}

fn seed_cached_l2_index(state: &AppState, name: &str) {
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
    let index = IvfIndex::build(&collection).expect("index should be built");

    state
        .collections
        .write()
        .expect("collection registry lock should be available")
        .insert(
            name.to_string(),
            std::sync::Arc::new(std::sync::RwLock::new(collection)),
        );
    state
        .l2_indexes
        .write()
        .expect("l2 index cache lock should be available")
        .insert(name.to_string(), index);
}

#[tokio::test]
async fn upsert_existing_point_invalidates_cached_l2_index() {
    let state = test_state();
    seed_cached_l2_index(&state, "cache_upsert");
    let app = build_app(state.clone());

    assert!(state
        .l2_indexes
        .read()
        .expect("l2 index cache lock should be available")
        .contains_key("cache_upsert"));

    let upsert_req = Request::builder()
        .method("PUT")
        .uri("/collections/cache_upsert/points/1")
        .header("content-type", "application/json")
        .body(Body::from(json!({"values": [999.0, 0.0]}).to_string()))
        .expect("request must build");
    let upsert_resp = app.oneshot(upsert_req).await.expect("response expected");
    assert_eq!(upsert_resp.status(), StatusCode::OK);

    assert!(!state
        .l2_indexes
        .read()
        .expect("l2 index cache lock should be available")
        .contains_key("cache_upsert"));
}

#[tokio::test]
async fn upsert_new_point_invalidates_cached_l2_index() {
    let state = test_state();
    seed_cached_l2_index(&state, "cache_upsert_new");
    let app = build_app(state.clone());

    let upsert_req = Request::builder()
        .method("PUT")
        .uri("/collections/cache_upsert_new/points/999999")
        .header("content-type", "application/json")
        .body(Body::from(json!({"values": [999.0, 0.0]}).to_string()))
        .expect("request must build");
    let upsert_resp = app.oneshot(upsert_req).await.expect("response expected");
    assert_eq!(upsert_resp.status(), StatusCode::OK);

    assert!(!state
        .l2_indexes
        .read()
        .expect("l2 index cache lock should be available")
        .contains_key("cache_upsert_new"));
}

#[tokio::test]
async fn delete_collection_invalidates_cached_l2_index() {
    let state = test_state();
    seed_cached_l2_index(&state, "cache_delete");
    let app = build_app(state.clone());

    assert!(state
        .l2_indexes
        .read()
        .expect("l2 index cache lock should be available")
        .contains_key("cache_delete"));

    let delete_req = Request::builder()
        .method("DELETE")
        .uri("/collections/cache_delete")
        .body(Body::empty())
        .expect("request must build");
    let delete_resp = app.oneshot(delete_req).await.expect("response expected");
    assert_eq!(delete_resp.status(), StatusCode::OK);

    assert!(!state
        .l2_indexes
        .read()
        .expect("l2 index cache lock should be available")
        .contains_key("cache_delete"));
}

#[test]
fn invalidating_cache_does_not_cancel_in_flight_build_marker() {
    let state = test_state();
    seed_cached_l2_index(&state, "cache_keep_building");
    state
        .l2_index_building
        .write()
        .expect("l2 index building lock should be available")
        .insert("cache_keep_building".to_string());

    remove_l2_index_entry(&state, "cache_keep_building");

    assert!(!state
        .l2_indexes
        .read()
        .expect("l2 index cache lock should be available")
        .contains_key("cache_keep_building"));
    assert!(state
        .l2_index_building
        .read()
        .expect("l2 index building lock should be available")
        .contains("cache_keep_building"));
}
