use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use aionbd_core::{append_wal_record_with_sync, load_collections, WalRecord};
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

    let root = std::env::temp_dir().join(format!("aionbd_server_chaos_test_{stamp}"));
    let snapshot = root.join("snapshot.json");
    let wal = root.join("wal.jsonl");
    (root, snapshot, wal)
}

fn cleanup_dir(path: &Path) {
    if path.exists() {
        fs::remove_dir_all(path).expect("temp directory should be removable");
    }
}

fn persistence_config(
    snapshot_path: PathBuf,
    wal_path: PathBuf,
    checkpoint_interval: usize,
) -> AppConfig {
    AppConfig {
        bind: "127.0.0.1:0".parse().expect("socket addr must parse"),
        max_dimension: 8,
        strict_finite: true,
        request_timeout_ms: 2_000,
        max_body_bytes: 1_048_576,
        max_concurrency: 256,
        max_page_limit: 1_000,
        max_topk_limit: 1_000,
        checkpoint_interval,
        persistence_enabled: true,
        snapshot_path,
        wal_path,
    }
}

fn persistence_state(
    snapshot_path: PathBuf,
    wal_path: PathBuf,
    checkpoint_interval: usize,
) -> AppState {
    AppState::with_collections(
        persistence_config(snapshot_path, wal_path, checkpoint_interval),
        BTreeMap::new(),
    )
}

async fn create_collection(app: &axum::Router, name: &str, dimension: usize) -> StatusCode {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/collections")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"name": name, "dimension": dimension}).to_string(),
                ))
                .expect("request must build"),
        )
        .await
        .expect("response expected")
        .status()
}

async fn upsert_point(app: &axum::Router, collection: &str, id: u64, values: &[f32]) -> StatusCode {
    app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/collections/{collection}/points/{id}"))
                .header("content-type", "application/json")
                .body(Body::from(json!({ "values": values }).to_string()))
                .expect("request must build"),
        )
        .await
        .expect("response expected")
        .status()
}

#[tokio::test]
async fn restart_reconciles_incrementals_and_truncated_wal_tail() {
    let (root, snapshot_path, wal_path) = persistence_paths();
    fs::create_dir_all(&root).expect("temp root should be creatable");

    let app = build_app(persistence_state(
        snapshot_path.clone(),
        wal_path.clone(),
        1,
    ));

    assert_eq!(create_collection(&app, "demo", 3).await, StatusCode::OK);
    assert_eq!(
        upsert_point(&app, "demo", 1, &[1.0_f32, 2.0_f32, 3.0_f32]).await,
        StatusCode::OK
    );
    assert_eq!(
        upsert_point(&app, "demo", 2, &[4.0_f32, 5.0_f32, 6.0_f32]).await,
        StatusCode::OK
    );

    append_wal_record_with_sync(
        &wal_path,
        &WalRecord::UpsertPoint {
            collection: "demo".to_string(),
            id: 3,
            values: vec![7.0_f32, 8.0_f32, 9.0_f32],
            payload: None,
        },
        true,
    )
    .expect("manual wal append should succeed");
    let mut wal_file = OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .expect("wal file should be appendable");
    wal_file
        .write_all(br#"{"type":"upsert_point","collection":"demo","id":99,"values":[1.0"#)
        .expect("truncated tail should be writable");
    wal_file.sync_data().expect("wal tail should be synced");

    let restored = load_collections(&snapshot_path, &wal_path).expect("restore should succeed");
    let collection = restored.get("demo").expect("demo collection should exist");
    assert_eq!(
        collection.get_point(1),
        Some(&[1.0_f32, 2.0_f32, 3.0_f32][..])
    );
    assert_eq!(
        collection.get_point(2),
        Some(&[4.0_f32, 5.0_f32, 6.0_f32][..])
    );
    assert_eq!(
        collection.get_point(3),
        Some(&[7.0_f32, 8.0_f32, 9.0_f32][..])
    );
    assert_eq!(collection.get_point(99), None);

    let restart_config = persistence_config(snapshot_path, wal_path, 1);
    let restart_app = build_app(AppState::with_collections(restart_config, restored));
    let get_recovered = restart_app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/collections/demo/points/3")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .expect("response expected");
    assert_eq!(get_recovered.status(), StatusCode::OK);

    cleanup_dir(&root);
}

#[tokio::test]
async fn degraded_checkpoint_state_is_recovered_on_restart() {
    let (root, snapshot_path, wal_path) = persistence_paths();
    fs::create_dir_all(&root).expect("temp root should be creatable");

    let blocking_incremental_dir = snapshot_path.with_extension("incrementals");
    fs::write(&blocking_incremental_dir, b"not-a-directory")
        .expect("blocking file should be writable");

    let app = build_app(persistence_state(
        snapshot_path.clone(),
        wal_path.clone(),
        1,
    ));
    assert_eq!(create_collection(&app, "degraded", 3).await, StatusCode::OK);
    assert_eq!(
        upsert_point(&app, "degraded", 7, &[0.1_f32, 0.2_f32, 0.3_f32]).await,
        StatusCode::OK
    );

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

    let restored = load_collections(&snapshot_path, &wal_path).expect("restore should succeed");
    let collection = restored
        .get("degraded")
        .expect("degraded collection should exist");
    assert_eq!(
        collection.get_point(7),
        Some(&[0.1_f32, 0.2_f32, 0.3_f32][..])
    );

    let restart_config = persistence_config(snapshot_path, wal_path, 1);
    let restart_app = build_app(AppState::with_collections(restart_config, restored));
    let get_recovered = restart_app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/collections/degraded/points/7")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .expect("response expected");
    assert_eq!(get_recovered.status(), StatusCode::OK);

    cleanup_dir(&root);
}
