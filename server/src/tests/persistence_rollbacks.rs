use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use crate::build_app;
use crate::config::AppConfig;
use crate::state::AppState;

fn persistence_paths() -> (PathBuf, PathBuf, PathBuf) {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock must be monotonic")
        .as_nanos();
    let root = std::env::temp_dir().join(format!("aionbd_server_rollback_test_{stamp}"));
    let snapshot = root.join("snapshot.json");
    let wal = root.join("wal.jsonl");
    (root, snapshot, wal)
}

fn cleanup_dir(path: &Path) {
    if path.exists() {
        fs::remove_dir_all(path).expect("temp directory should be removable");
    }
}

fn poison_wal_path(path: &Path) {
    if path.exists() {
        if path.is_file() {
            fs::remove_file(path).expect("wal file should be removable");
        } else {
            fs::remove_dir_all(path).expect("wal directory should be removable");
        }
    }
    fs::create_dir_all(path).expect("wal path directory should be creatable");
}

fn test_state(snapshot_path: PathBuf, wal_path: PathBuf) -> AppState {
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
        persistence_enabled: true,
        wal_sync_on_write: true,
        snapshot_path,
        wal_path,
    };

    AppState::with_collections(config, BTreeMap::new())
}

#[tokio::test]
async fn failed_persist_rolls_back_upsert_mutation() {
    let (root, snapshot_path, wal_path) = persistence_paths();
    let app = build_app(test_state(snapshot_path, wal_path.clone()));

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "demo", "dimension": 3}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    poison_wal_path(&wal_path);

    let upsert_req = Request::builder()
        .method("PUT")
        .uri("/collections/demo/points/41")
        .header("content-type", "application/json")
        .body(Body::from(json!({"values": [1.0, 2.0, 3.0]}).to_string()))
        .expect("request must build");
    let upsert_resp = app
        .clone()
        .oneshot(upsert_req)
        .await
        .expect("response expected");
    assert_eq!(upsert_resp.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let get_req = Request::builder()
        .method("GET")
        .uri("/collections/demo/points/41")
        .body(Body::empty())
        .expect("request must build");
    let get_resp = app
        .clone()
        .oneshot(get_req)
        .await
        .expect("response expected");
    assert_eq!(get_resp.status(), StatusCode::NOT_FOUND);

    cleanup_dir(&root);
}

#[tokio::test]
async fn invalid_create_collection_does_not_write_wal_record() {
    let (root, snapshot_path, wal_path) = persistence_paths();
    let app = build_app(test_state(snapshot_path, wal_path.clone()));

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "bad_create", "dimension": 0}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::BAD_REQUEST);

    if wal_path.exists() {
        let wal_body = fs::read_to_string(&wal_path).expect("wal should be readable");
        assert!(
            wal_body.trim().is_empty(),
            "wal should not contain an invalid create record"
        );
    }

    cleanup_dir(&root);
}

#[tokio::test]
async fn failed_persist_rolls_back_delete_point_mutation() {
    let (root, snapshot_path, wal_path) = persistence_paths();
    let app = build_app(test_state(snapshot_path, wal_path.clone()));

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "demo", "dimension": 3}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    let upsert_req = Request::builder()
        .method("PUT")
        .uri("/collections/demo/points/7")
        .header("content-type", "application/json")
        .body(Body::from(json!({"values": [9.0, 8.0, 7.0]}).to_string()))
        .expect("request must build");
    let upsert_resp = app
        .clone()
        .oneshot(upsert_req)
        .await
        .expect("response expected");
    assert_eq!(upsert_resp.status(), StatusCode::OK);

    poison_wal_path(&wal_path);

    let delete_req = Request::builder()
        .method("DELETE")
        .uri("/collections/demo/points/7")
        .body(Body::empty())
        .expect("request must build");
    let delete_resp = app
        .clone()
        .oneshot(delete_req)
        .await
        .expect("response expected");
    assert_eq!(delete_resp.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let get_req = Request::builder()
        .method("GET")
        .uri("/collections/demo/points/7")
        .body(Body::empty())
        .expect("request must build");
    let get_resp = app
        .clone()
        .oneshot(get_req)
        .await
        .expect("response expected");
    assert_eq!(get_resp.status(), StatusCode::OK);

    cleanup_dir(&root);
}
