use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use crate::build_app;
use crate::config::AppConfig;
use crate::state::AppState;

fn test_state() -> AppState {
    let config = AppConfig {
        bind: "127.0.0.1:0".parse().expect("socket addr must parse"),
        max_dimension: 8,
        strict_finite: true,
        request_timeout_ms: 2_000,
        max_body_bytes: 1_048_576,
        max_concurrency: 256,
        checkpoint_interval: 1,
        persistence_enabled: false,
        snapshot_path: std::path::PathBuf::from("unused_snapshot.json"),
        wal_path: std::path::PathBuf::from("unused_wal.jsonl"),
    };

    AppState::with_collections(config, std::collections::BTreeMap::new())
}

#[tokio::test]
async fn delete_collection_removes_it_and_second_delete_is_not_found() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "to_delete", "dimension": 3}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    let delete_req = Request::builder()
        .method("DELETE")
        .uri("/collections/to_delete")
        .body(Body::empty())
        .expect("request must build");
    let delete_resp = app
        .clone()
        .oneshot(delete_req)
        .await
        .expect("response expected");
    assert_eq!(delete_resp.status(), StatusCode::OK);

    let get_req = Request::builder()
        .method("GET")
        .uri("/collections/to_delete")
        .body(Body::empty())
        .expect("request must build");
    let get_resp = app
        .clone()
        .oneshot(get_req)
        .await
        .expect("response expected");
    assert_eq!(get_resp.status(), StatusCode::NOT_FOUND);

    let delete_again_req = Request::builder()
        .method("DELETE")
        .uri("/collections/to_delete")
        .body(Body::empty())
        .expect("request must build");
    let delete_again_resp = app
        .clone()
        .oneshot(delete_again_req)
        .await
        .expect("response expected");
    assert_eq!(delete_again_resp.status(), StatusCode::NOT_FOUND);
}
