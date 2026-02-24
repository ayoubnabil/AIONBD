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
        persistence_enabled: false,
        snapshot_path: std::path::PathBuf::from("unused_snapshot.json"),
        wal_path: std::path::PathBuf::from("unused_wal.jsonl"),
    };

    AppState::with_collections(config, std::collections::BTreeMap::new())
}

#[tokio::test]
async fn list_points_supports_offset_and_limit() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "list_points", "dimension": 2}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    for (id, values) in [
        (1u64, json!([1.0, 0.0])),
        (2u64, json!([2.0, 0.0])),
        (3u64, json!([3.0, 0.0])),
    ] {
        let upsert_req = Request::builder()
            .method("PUT")
            .uri(format!("/collections/list_points/points/{id}"))
            .header("content-type", "application/json")
            .body(Body::from(json!({"values": values}).to_string()))
            .expect("request must build");
        let upsert_resp = app
            .clone()
            .oneshot(upsert_req)
            .await
            .expect("response expected");
        assert_eq!(upsert_resp.status(), StatusCode::OK);
    }

    let list_req = Request::builder()
        .method("GET")
        .uri("/collections/list_points/points?offset=1&limit=2")
        .body(Body::empty())
        .expect("request must build");
    let list_resp = app
        .clone()
        .oneshot(list_req)
        .await
        .expect("response expected");
    assert_eq!(list_resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
        .await
        .expect("body should be readable");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("valid json response");

    assert_eq!(payload["total"], 3);
    assert_eq!(payload["points"][0]["id"], 2);
    assert_eq!(payload["points"][1]["id"], 3);
    assert_eq!(payload["next_offset"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_points_rejects_zero_limit() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "list_points_limit", "dimension": 2}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    let list_req = Request::builder()
        .method("GET")
        .uri("/collections/list_points_limit/points?limit=0")
        .body(Body::empty())
        .expect("request must build");
    let list_resp = app
        .clone()
        .oneshot(list_req)
        .await
        .expect("response expected");

    assert_eq!(list_resp.status(), StatusCode::BAD_REQUEST);
}
