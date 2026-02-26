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
    let root = std::env::temp_dir().join(format!("aionbd_server_degraded_test_{stamp}"));
    let snapshot = root.join("snapshot.json");
    let wal = root.join("wal.jsonl");
    (root, snapshot, wal)
}

fn cleanup_dir(path: &Path) {
    if path.exists() {
        fs::remove_dir_all(path).expect("temp directory should be removable");
    }
}

fn persistence_state(snapshot_path: PathBuf, wal_path: PathBuf) -> AppState {
    let config = AppConfig {
        bind: "127.0.0.1:0".parse().expect("socket addr must parse"),
        max_dimension: 8,
        max_points_per_collection: 1_000_000,
        memory_budget_bytes: 0,
        strict_finite: true,
        request_timeout_ms: 2_000,
        max_body_bytes: 1_048_576,
        max_concurrency: 256,
        max_page_limit: 1_000,
        max_topk_limit: 1_000,
        checkpoint_interval: 1,
        persistence_enabled: true,
        wal_sync_on_write: true,
        wal_sync_every_n_writes: 0,
        wal_sync_interval_seconds: 0,
        wal_group_commit_max_batch: 16,
        wal_group_commit_flush_delay_ms: 0,
        async_checkpoints: false,
        checkpoint_compact_after: 64,
        snapshot_path,
        wal_path,
    };
    AppState::with_collections(config, BTreeMap::new())
}

#[tokio::test]
async fn wal_only_checkpoint_marks_server_not_ready() {
    let (root, snapshot_path, wal_path) = persistence_paths();
    fs::create_dir_all(&root).expect("temp root should be creatable");

    let blocking_incremental_dir = snapshot_path.with_extension("incrementals");
    fs::write(&blocking_incremental_dir, b"not-a-directory")
        .expect("blocking file should be writable");

    let app = build_app(persistence_state(snapshot_path, wal_path));

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/collections")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"name":"degraded","dimension":3}).to_string(),
                ))
                .expect("request must build"),
        )
        .await
        .expect("response expected");
    assert_eq!(create.status(), StatusCode::OK);

    let ready = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/ready")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .expect("response expected");
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);

    let metrics = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .expect("response expected");
    assert_eq!(metrics.status(), StatusCode::OK);
    let metrics_json = super::json_body(metrics).await;
    assert_eq!(metrics_json["storage_available"], false);
    assert_eq!(metrics_json["ready"], false);
    assert_eq!(metrics_json["persistence_checkpoint_degraded_total"], 1);

    cleanup_dir(&root);
}

#[tokio::test]
async fn successful_checkpoint_restores_storage_readiness() {
    let (root, snapshot_path, wal_path) = persistence_paths();
    fs::create_dir_all(&root).expect("temp root should be creatable");
    let blocking_incremental_dir = snapshot_path.with_extension("incrementals");
    fs::write(&blocking_incremental_dir, b"not-a-directory")
        .expect("blocking file should be writable");

    let app = build_app(persistence_state(snapshot_path.clone(), wal_path));

    let create_degraded = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/collections")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"name":"degraded","dimension":3}).to_string(),
                ))
                .expect("request must build"),
        )
        .await
        .expect("response expected");
    assert_eq!(create_degraded.status(), StatusCode::OK);

    fs::remove_file(&blocking_incremental_dir).expect("blocking file should be removable");

    let create_recovered = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/collections")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"name":"recovered","dimension":3}).to_string(),
                ))
                .expect("request must build"),
        )
        .await
        .expect("response expected");
    assert_eq!(create_recovered.status(), StatusCode::OK);

    let ready = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/ready")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .expect("response expected");
    assert_eq!(ready.status(), StatusCode::OK);

    let metrics = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .expect("response expected");
    assert_eq!(metrics.status(), StatusCode::OK);
    let metrics_json = super::json_body(metrics).await;
    assert_eq!(metrics_json["storage_available"], true);
    assert_eq!(metrics_json["ready"], true);
    assert_eq!(metrics_json["persistence_checkpoint_degraded_total"], 1);

    cleanup_dir(&root);
}
