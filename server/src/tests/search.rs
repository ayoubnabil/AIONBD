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
async fn search_top_k_returns_sorted_hits() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "search_k", "dimension": 2}).to_string(),
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
            .uri(format!("/collections/search_k/points/{id}"))
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
        .uri("/collections/search_k/search/topk")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"query": [1.0, 0.0], "metric": "dot", "limit": 2}).to_string(),
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

    assert_eq!(payload["metric"], "dot");
    assert_eq!(payload["hits"][0]["id"], 1);
    assert_eq!(payload["hits"][1]["id"], 2);
}

#[tokio::test]
async fn search_top_k_rejects_zero_limit() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "search_limit", "dimension": 2}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    let search_req = Request::builder()
        .method("POST")
        .uri("/collections/search_limit/search/topk")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"query": [1.0, 0.0], "metric": "dot", "limit": 0}).to_string(),
        ))
        .expect("request must build");
    let search_resp = app
        .clone()
        .oneshot(search_req)
        .await
        .expect("response expected");

    assert_eq!(search_resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn search_top_k_uses_id_tiebreak_for_equal_scores() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "search_tie", "dimension": 2}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    for (id, values) in [
        (2u64, json!([1.0, 0.0])),
        (1u64, json!([1.0, 0.0])),
        (3u64, json!([0.0, 0.0])),
    ] {
        let upsert_req = Request::builder()
            .method("PUT")
            .uri(format!("/collections/search_tie/points/{id}"))
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
        .uri("/collections/search_tie/search/topk")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"query": [1.0, 0.0], "metric": "dot", "limit": 2}).to_string(),
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

    assert_eq!(payload["metric"], "dot");
    assert_eq!(payload["hits"][0]["id"], 1);
    assert_eq!(payload["hits"][1]["id"], 2);
}
