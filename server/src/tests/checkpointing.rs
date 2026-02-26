use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use tokio::time::{sleep, Duration};
use tower::ServiceExt;

use crate::build_app;
use crate::config::AppConfig;
use crate::state::AppState;

fn persistence_paths() -> (PathBuf, PathBuf, PathBuf) {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock must be monotonic")
        .as_nanos();
    let root = std::env::temp_dir().join(format!("aionbd_server_checkpoint_test_{stamp}"));
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
async fn checkpoint_interval_truncates_wal_periodically() {
    let (root, snapshot_path, wal_path) = persistence_paths();

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
        checkpoint_interval: 3,
        persistence_enabled: true,
        wal_sync_on_write: true,
        wal_sync_every_n_writes: 0,
        wal_sync_interval_seconds: 0,
        wal_group_commit_max_batch: 16,
        wal_group_commit_flush_delay_ms: 0,
        async_checkpoints: false,
        checkpoint_compact_after: 64,
        snapshot_path,
        wal_path: wal_path.clone(),
    };
    let app = build_app(AppState::with_collections(config, BTreeMap::new()));

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "batched", "dimension": 3}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);
    assert!(!fs::read_to_string(&wal_path)
        .expect("wal should be readable")
        .is_empty());

    for id in [1_u64, 2_u64] {
        let upsert_req = Request::builder()
            .method("PUT")
            .uri(format!("/collections/batched/points/{id}"))
            .header("content-type", "application/json")
            .body(Body::from(json!({"values": [1.0, 2.0, 3.0]}).to_string()))
            .expect("request must build");
        let upsert_resp = app
            .clone()
            .oneshot(upsert_req)
            .await
            .expect("response expected");
        assert_eq!(upsert_resp.status(), StatusCode::OK);
    }

    let mut wal_cleared = false;
    for _ in 0..50 {
        if fs::read_to_string(&wal_path)
            .expect("wal should be readable")
            .is_empty()
        {
            wal_cleared = true;
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }
    assert!(
        wal_cleared,
        "wal should be truncated after async checkpoint"
    );
    cleanup_dir(&root);
}
