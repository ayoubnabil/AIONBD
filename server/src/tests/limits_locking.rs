use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use crate::auth::AuthConfig;
use crate::build_app;
use crate::config::AppConfig;
use crate::handler_utils::remove_collection_write_lock;
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

#[tokio::test]
async fn unknown_collection_writes_do_not_grow_lock_map() {
    let state = test_state();
    let app = build_app(state.clone());

    let before = state.collection_write_locks.lock().await.len();

    for suffix in 0..24usize {
        let upsert_req = Request::builder()
            .method("PUT")
            .uri(format!("/collections/missing_{suffix}/points/1"))
            .header("content-type", "application/json")
            .body(Body::from(json!({"values": [1.0, 2.0]}).to_string()))
            .expect("request must build");
        let upsert_resp = app
            .clone()
            .oneshot(upsert_req)
            .await
            .expect("response expected");
        assert_eq!(upsert_resp.status(), StatusCode::NOT_FOUND);
    }

    for suffix in 0..24usize {
        let delete_req = Request::builder()
            .method("DELETE")
            .uri(format!("/collections/missing_{suffix}/points/1"))
            .body(Body::empty())
            .expect("request must build");
        let delete_resp = app
            .clone()
            .oneshot(delete_req)
            .await
            .expect("response expected");
        assert_eq!(delete_resp.status(), StatusCode::NOT_FOUND);
    }

    let after = state.collection_write_locks.lock().await.len();
    assert_eq!(before, after);
}

#[tokio::test]
async fn rejected_create_by_quota_does_not_grow_lock_map() {
    let base = test_state();
    let auth = AuthConfig {
        tenant_max_collections: 1,
        ..AuthConfig::default()
    };
    let state = AppState::with_collections_and_auth(
        base.config.as_ref().clone(),
        std::collections::BTreeMap::new(),
        auth,
    );
    let app = build_app(state.clone());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "locked", "dimension": 2}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    let before = state.collection_write_locks.lock().await.len();

    for suffix in 0..16usize {
        let req = Request::builder()
            .method("POST")
            .uri("/collections")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"name": format!("quota_{suffix}"), "dimension": 2}).to_string(),
            ))
            .expect("request must build");
        let resp = app.clone().oneshot(req).await.expect("response expected");
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    let after = state.collection_write_locks.lock().await.len();
    assert_eq!(before, after);
}

#[tokio::test]
async fn remove_collection_write_lock_keeps_shared_lock_and_removes_when_idle() {
    let state = test_state();
    let name = "demo";

    let shared_lock = {
        let mut locks = state.collection_write_locks.lock().await;
        let lock = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let cloned = lock.clone();
        locks.insert(name.to_string(), lock);
        cloned
    };

    remove_collection_write_lock(&state, name)
        .await
        .expect("lock removal should not fail");
    assert!(state.collection_write_locks.lock().await.contains_key(name));

    drop(shared_lock);
    remove_collection_write_lock(&state, name)
        .await
        .expect("lock removal should not fail");
    assert!(!state.collection_write_locks.lock().await.contains_key(name));
}

#[tokio::test]
async fn stale_write_lock_entry_is_pruned_after_missing_collection_write() {
    let state = test_state();
    let app = build_app(state.clone());

    {
        let mut locks = state.collection_write_locks.lock().await;
        locks.insert(
            "ghost".to_string(),
            std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
        );
    }

    let upsert_req = Request::builder()
        .method("PUT")
        .uri("/collections/ghost/points/1")
        .header("content-type", "application/json")
        .body(Body::from(json!({"values": [1.0, 2.0]}).to_string()))
        .expect("request must build");
    let upsert_resp = app
        .clone()
        .oneshot(upsert_req)
        .await
        .expect("response expected");
    assert_eq!(upsert_resp.status(), StatusCode::NOT_FOUND);
    assert!(!state
        .collection_write_locks
        .lock()
        .await
        .contains_key("ghost"));

    {
        let mut locks = state.collection_write_locks.lock().await;
        locks.insert(
            "ghost".to_string(),
            std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
        );
    }

    let delete_req = Request::builder()
        .method("DELETE")
        .uri("/collections/ghost/points/1")
        .body(Body::empty())
        .expect("request must build");
    let delete_resp = app
        .clone()
        .oneshot(delete_req)
        .await
        .expect("response expected");
    assert_eq!(delete_resp.status(), StatusCode::NOT_FOUND);
    assert!(!state
        .collection_write_locks
        .lock()
        .await
        .contains_key("ghost"));
}
