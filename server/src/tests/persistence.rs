use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use aionbd_core::load_collections;
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

    let root = std::env::temp_dir().join(format!("aionbd_server_test_{stamp}"));
    let snapshot = root.join("snapshot.json");
    let wal = root.join("wal.jsonl");
    (root, snapshot, wal)
}

fn cleanup_dir(path: &Path) {
    if path.exists() {
        fs::remove_dir_all(path).expect("temp directory should be removable");
    }
}

#[tokio::test]
async fn persistence_survives_restart_via_snapshot_and_wal() {
    let (root, snapshot_path, wal_path) = persistence_paths();

    let config = AppConfig {
        bind: "127.0.0.1:0".parse().expect("socket addr must parse"),
        max_dimension: 8,
        strict_finite: true,
        request_timeout_ms: 2_000,
        max_body_bytes: 1_048_576,
        max_concurrency: 256,
        persistence_enabled: true,
        snapshot_path: snapshot_path.clone(),
        wal_path: wal_path.clone(),
    };

    let app = build_app(AppState::with_collections(config.clone(), BTreeMap::new()));

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "persisted", "dimension": 3}).to_string(),
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
        .uri("/collections/persisted/points/9")
        .header("content-type", "application/json")
        .body(Body::from(json!({"values": [3.0, 4.0, 5.0]}).to_string()))
        .expect("request must build");
    let upsert_resp = app
        .clone()
        .oneshot(upsert_req)
        .await
        .expect("response expected");
    assert_eq!(upsert_resp.status(), StatusCode::OK);

    let restored = load_collections(&snapshot_path, &wal_path).expect("restore should succeed");
    let restart_app = build_app(AppState::with_collections(config, restored));

    let get_point_req = Request::builder()
        .method("GET")
        .uri("/collections/persisted/points/9")
        .body(Body::empty())
        .expect("request must build");
    let get_point_resp = restart_app
        .clone()
        .oneshot(get_point_req)
        .await
        .expect("response expected");
    assert_eq!(get_point_resp.status(), StatusCode::OK);

    cleanup_dir(&root);
}
