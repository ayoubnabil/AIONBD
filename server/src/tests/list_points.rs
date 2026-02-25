use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use crate::build_app;
use crate::config::AppConfig;
use crate::state::AppState;

fn test_state() -> AppState {
    test_state_with_max_page_limit(1_000)
}

fn test_state_with_max_page_limit(max_page_limit: usize) -> AppState {
    let config = AppConfig {
        bind: "127.0.0.1:0".parse().expect("socket addr must parse"),
        max_dimension: 8,
        max_points_per_collection: 1_000_000,
        strict_finite: true,
        request_timeout_ms: 2_000,
        max_body_bytes: 1_048_576,
        max_concurrency: 256,
        max_page_limit,
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
    assert_eq!(payload["next_after_id"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_points_supports_after_id_cursor_pagination() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "list_points_cursor", "dimension": 2}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    for (id, values) in [
        (10u64, json!([1.0, 0.0])),
        (20u64, json!([2.0, 0.0])),
        (30u64, json!([3.0, 0.0])),
        (40u64, json!([4.0, 0.0])),
        (50u64, json!([5.0, 0.0])),
    ] {
        let upsert_req = Request::builder()
            .method("PUT")
            .uri(format!("/collections/list_points_cursor/points/{id}"))
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

    let first_page_req = Request::builder()
        .method("GET")
        .uri("/collections/list_points_cursor/points?after_id=20&limit=2")
        .body(Body::empty())
        .expect("request must build");
    let first_page_resp = app
        .clone()
        .oneshot(first_page_req)
        .await
        .expect("response expected");
    assert_eq!(first_page_resp.status(), StatusCode::OK);

    let first_page_body = axum::body::to_bytes(first_page_resp.into_body(), usize::MAX)
        .await
        .expect("body should be readable");
    let first_payload: serde_json::Value =
        serde_json::from_slice(&first_page_body).expect("valid json response");

    assert_eq!(first_payload["total"], 5);
    assert_eq!(first_payload["points"][0]["id"], 30);
    assert_eq!(first_payload["points"][1]["id"], 40);
    assert_eq!(first_payload["next_offset"], serde_json::Value::Null);
    assert_eq!(first_payload["next_after_id"], 40);

    let second_page_req = Request::builder()
        .method("GET")
        .uri("/collections/list_points_cursor/points?after_id=40&limit=2")
        .body(Body::empty())
        .expect("request must build");
    let second_page_resp = app
        .clone()
        .oneshot(second_page_req)
        .await
        .expect("response expected");
    assert_eq!(second_page_resp.status(), StatusCode::OK);

    let second_page_body = axum::body::to_bytes(second_page_resp.into_body(), usize::MAX)
        .await
        .expect("body should be readable");
    let second_payload: serde_json::Value =
        serde_json::from_slice(&second_page_body).expect("valid json response");

    assert_eq!(second_payload["points"][0]["id"], 50);
    assert_eq!(second_payload["next_after_id"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_points_rejects_mixed_offset_and_after_id() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "list_points_mixed_params", "dimension": 2}).to_string(),
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
        .uri("/collections/list_points_mixed_params/points?offset=1&after_id=2&limit=2")
        .body(Body::empty())
        .expect("request must build");
    let list_resp = app
        .clone()
        .oneshot(list_req)
        .await
        .expect("response expected");

    assert_eq!(list_resp.status(), StatusCode::BAD_REQUEST);
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

#[tokio::test]
async fn list_points_rejects_limit_above_max_page_limit() {
    let app = build_app(test_state_with_max_page_limit(2));

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "list_points_max_limit", "dimension": 2}).to_string(),
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
        .uri("/collections/list_points_max_limit/points?limit=3")
        .body(Body::empty())
        .expect("request must build");
    let list_resp = app
        .clone()
        .oneshot(list_req)
        .await
        .expect("response expected");

    assert_eq!(list_resp.status(), StatusCode::BAD_REQUEST);
}
