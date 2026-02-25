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
    let root = std::env::temp_dir().join(format!("aionbd_server_sync_mode_test_{stamp}"));
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
async fn persistence_accepts_writes_when_wal_sync_is_disabled() {
    let (root, snapshot_path, wal_path) = persistence_paths();
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
        checkpoint_interval: 8,
        persistence_enabled: true,
        wal_sync_on_write: false,
        snapshot_path,
        wal_path: wal_path.clone(),
    };

    let app = build_app(AppState::with_collections(config, BTreeMap::new()));
    let create = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name":"async_wal","dimension":3}).to_string(),
        ))
        .expect("request must build");
    assert_eq!(
        app.clone()
            .oneshot(create)
            .await
            .expect("response expected")
            .status(),
        StatusCode::OK
    );
    assert!(fs::metadata(&wal_path).map(|meta| meta.len()).unwrap_or(0) > 0);

    cleanup_dir(&root);
}
