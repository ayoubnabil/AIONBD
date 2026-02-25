use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use crate::build_app;
use crate::config::AppConfig;
use crate::state::AppState;

fn test_state(max_page_limit: usize, max_topk_limit: usize) -> AppState {
    test_state_with_max_dimension(8, max_page_limit, max_topk_limit)
}

fn test_state_with_max_points(max_points_per_collection: usize) -> AppState {
    let config = AppConfig {
        bind: "127.0.0.1:0".parse().expect("socket addr must parse"),
        max_dimension: 8,
        max_points_per_collection,
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

fn test_state_with_max_dimension(
    max_dimension: usize,
    max_page_limit: usize,
    max_topk_limit: usize,
) -> AppState {
    let config = AppConfig {
        bind: "127.0.0.1:0".parse().expect("socket addr must parse"),
        max_dimension,
        max_points_per_collection: 1_000_000,
        strict_finite: true,
        request_timeout_ms: 2_000,
        max_body_bytes: 1_048_576,
        max_concurrency: 256,
        max_page_limit,
        max_topk_limit,
        checkpoint_interval: 1,
        persistence_enabled: false,
        wal_sync_on_write: true,
        snapshot_path: std::path::PathBuf::from("unused_snapshot.json"),
        wal_path: std::path::PathBuf::from("unused_wal.jsonl"),
    };

    AppState::with_collections(config, std::collections::BTreeMap::new())
}

#[tokio::test]
async fn create_collection_rejects_dimension_above_configured_maximum() {
    let app = build_app(test_state_with_max_dimension(4, 1_000, 1_000));

    let request = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "too_wide", "dimension": 5}).to_string(),
        ))
        .expect("request must build");
    let response = app
        .oneshot(request)
        .await
        .expect("response should be available");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn upsert_rejects_new_point_when_collection_cap_is_reached() {
    let app = build_app(test_state_with_max_points(1));

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "cap_demo", "dimension": 2}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    let first_upsert = Request::builder()
        .method("PUT")
        .uri("/collections/cap_demo/points/1")
        .header("content-type", "application/json")
        .body(Body::from(json!({"values": [1.0, 0.0]}).to_string()))
        .expect("request must build");
    let first_resp = app
        .clone()
        .oneshot(first_upsert)
        .await
        .expect("response expected");
    assert_eq!(first_resp.status(), StatusCode::OK);

    let second_upsert = Request::builder()
        .method("PUT")
        .uri("/collections/cap_demo/points/2")
        .header("content-type", "application/json")
        .body(Body::from(json!({"values": [2.0, 0.0]}).to_string()))
        .expect("request must build");
    let second_resp = app
        .clone()
        .oneshot(second_upsert)
        .await
        .expect("response expected");
    assert_eq!(second_resp.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn list_points_uses_capped_default_limit_when_limit_is_omitted() {
    let app = build_app(test_state(2, 1_000));

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "points_default_cap", "dimension": 2}).to_string(),
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
            .uri(format!("/collections/points_default_cap/points/{id}"))
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
        .uri("/collections/points_default_cap/points")
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
    assert_eq!(payload["points"][0]["id"], 1);
    assert_eq!(payload["points"][1]["id"], 2);
    assert_eq!(payload["next_offset"], 2);
    assert_eq!(payload["next_after_id"], 2);
}

#[tokio::test]
async fn search_top_k_uses_capped_default_limit_when_limit_is_omitted() {
    let app = build_app(test_state(1_000, 1));

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "search_default_cap", "dimension": 2}).to_string(),
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
        (2u64, json!([0.9, 0.0])),
        (3u64, json!([0.1, 0.0])),
    ] {
        let upsert_req = Request::builder()
            .method("PUT")
            .uri(format!("/collections/search_default_cap/points/{id}"))
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

    let search_req = Request::builder()
        .method("POST")
        .uri("/collections/search_default_cap/search/topk")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"query": [1.0, 0.0], "metric": "dot"}).to_string(),
        ))
        .expect("request must build");
    let search_resp = app
        .clone()
        .oneshot(search_req)
        .await
        .expect("response expected");
    assert_eq!(search_resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(search_resp.into_body(), usize::MAX)
        .await
        .expect("body should be readable");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("valid json response");

    assert_eq!(
        payload["hits"]
            .as_array()
            .expect("hits should be array")
            .len(),
        1
    );
    assert_eq!(payload["hits"][0]["id"], 1);
}
