use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use aionbd_core::incremental_snapshot_dir;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use crate::build_app;
use crate::config::AppConfig;
use crate::state::AppState;

fn persistence_paths() -> (PathBuf, PathBuf, PathBuf) {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock must be monotonic")
        .as_nanos();
    let root = std::env::temp_dir().join(format!("aionbd_server_metrics_test_{stamp}"));
    let snapshot = root.join("snapshot.json");
    let wal = root.join("wal.jsonl");
    (root, snapshot, wal)
}

fn cleanup_dir(path: &Path) {
    if path.exists() {
        fs::remove_dir_all(path).expect("temp directory should be removable");
    }
}

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
async fn metrics_report_persistence_backlog_sizes() {
    let (root, snapshot_path, wal_path) = persistence_paths();
    fs::create_dir_all(&root).expect("temp root should be creatable");
    fs::write(&wal_path, b"abcde").expect("wal file should be writable");
    let incremental_dir = incremental_snapshot_dir(&snapshot_path);
    fs::create_dir_all(&incremental_dir).expect("incremental dir should be creatable");
    fs::write(incremental_dir.join("0001.jsonl"), b"12").expect("segment should be writable");
    fs::write(incremental_dir.join("0002.jsonl"), b"345").expect("segment should be writable");
    fs::write(incremental_dir.join("ignore.tmp"), b"nope").expect("temp file should be writable");

    let mut state = test_state();
    {
        let config = std::sync::Arc::make_mut(&mut state.config);
        config.persistence_enabled = true;
        config.snapshot_path = snapshot_path;
        config.wal_path = wal_path;
    }
    let app = build_app(state);
    let metrics_resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .expect("response expected");
    assert_eq!(metrics_resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(metrics_resp.into_body(), usize::MAX)
        .await
        .expect("body should be readable");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("valid json response");
    assert_eq!(payload["persistence_wal_size_bytes"], 5);
    assert_eq!(payload["persistence_wal_tail_open"], true);
    assert_eq!(payload["persistence_incremental_segments"], 2);
    assert_eq!(payload["persistence_incremental_size_bytes"], 5);
    assert_eq!(payload["persistence_wal_sync_on_write"], true);
    assert_eq!(payload["max_points_per_collection"], 1_000_000);

    cleanup_dir(&root);
}
