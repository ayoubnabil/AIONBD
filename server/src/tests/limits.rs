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
    test_state_with_max_points_and_memory(max_points_per_collection, 0)
}

fn test_state_with_max_points_and_memory(
    max_points_per_collection: usize,
    memory_budget_bytes: u64,
) -> AppState {
    let config = AppConfig {
        bind: "127.0.0.1:0".parse().expect("socket addr must parse"),
        max_dimension: 8,
        max_points_per_collection,
        memory_budget_bytes,
        strict_finite: true,
        request_timeout_ms: 2_000,
        max_body_bytes: 1_048_576,
        max_concurrency: 256,
        max_page_limit: 1_000,
        max_topk_limit: 1_000,
        checkpoint_interval: 1,
        persistence_enabled: false,
        wal_sync_on_write: true,
        wal_sync_every_n_writes: 0,
        wal_sync_interval_seconds: 0,
        wal_group_commit_max_batch: 16,
        wal_group_commit_flush_delay_ms: 0,
        async_checkpoints: false,
        checkpoint_compact_after: 64,
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
        memory_budget_bytes: 0,
        strict_finite: true,
        request_timeout_ms: 2_000,
        max_body_bytes: 1_048_576,
        max_concurrency: 256,
        max_page_limit,
        max_topk_limit,
        checkpoint_interval: 1,
        persistence_enabled: false,
        wal_sync_on_write: true,
        wal_sync_every_n_writes: 0,
        wal_sync_interval_seconds: 0,
        wal_group_commit_max_batch: 16,
        wal_group_commit_flush_delay_ms: 0,
        async_checkpoints: false,
        checkpoint_compact_after: 64,
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
async fn upsert_rejects_new_point_when_memory_budget_is_reached() {
    let budget_bytes = 8u64;
    let app = build_app(test_state_with_max_points_and_memory(
        1_000_000,
        budget_bytes,
    ));

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "ram_cap_demo", "dimension": 2}).to_string(),
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
        .uri("/collections/ram_cap_demo/points/1")
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
        .uri("/collections/ram_cap_demo/points/2")
        .header("content-type", "application/json")
        .body(Body::from(json!({"values": [2.0, 0.0]}).to_string()))
        .expect("request must build");
    let second_resp = app
        .clone()
        .oneshot(second_upsert)
        .await
        .expect("response expected");
    assert_eq!(second_resp.status(), StatusCode::TOO_MANY_REQUESTS);
    let second_json = super::json_body(second_resp).await;
    assert_eq!(second_json["code"], "resource_exhausted");
    assert!(second_json["message"]
        .as_str()
        .unwrap_or("")
        .contains("memory budget exceeded"));

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
    let metrics_json = super::json_body(metrics_resp).await;
    assert_eq!(metrics_json["memory_budget_bytes"], budget_bytes);
    assert_eq!(metrics_json["memory_used_bytes"], budget_bytes);
}

#[tokio::test]
async fn delete_point_releases_memory_budget_for_new_insert() {
    let budget_bytes = 8u64;
    let app = build_app(test_state_with_max_points_and_memory(
        1_000_000,
        budget_bytes,
    ));

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "ram_release_demo", "dimension": 2}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    let upsert_first = Request::builder()
        .method("PUT")
        .uri("/collections/ram_release_demo/points/1")
        .header("content-type", "application/json")
        .body(Body::from(json!({"values": [1.0, 0.0]}).to_string()))
        .expect("request must build");
    let upsert_first_resp = app
        .clone()
        .oneshot(upsert_first)
        .await
        .expect("response expected");
    assert_eq!(upsert_first_resp.status(), StatusCode::OK);

    let upsert_second_rejected = Request::builder()
        .method("PUT")
        .uri("/collections/ram_release_demo/points/2")
        .header("content-type", "application/json")
        .body(Body::from(json!({"values": [2.0, 0.0]}).to_string()))
        .expect("request must build");
    let upsert_second_rejected_resp = app
        .clone()
        .oneshot(upsert_second_rejected)
        .await
        .expect("response expected");
    assert_eq!(
        upsert_second_rejected_resp.status(),
        StatusCode::TOO_MANY_REQUESTS
    );

    let delete_first = Request::builder()
        .method("DELETE")
        .uri("/collections/ram_release_demo/points/1")
        .body(Body::empty())
        .expect("request must build");
    let delete_first_resp = app
        .clone()
        .oneshot(delete_first)
        .await
        .expect("response expected");
    assert_eq!(delete_first_resp.status(), StatusCode::OK);

    let upsert_second_after_release = Request::builder()
        .method("PUT")
        .uri("/collections/ram_release_demo/points/2")
        .header("content-type", "application/json")
        .body(Body::from(json!({"values": [2.0, 0.0]}).to_string()))
        .expect("request must build");
    let upsert_second_after_release_resp = app
        .clone()
        .oneshot(upsert_second_after_release)
        .await
        .expect("response expected");
    assert_eq!(upsert_second_after_release_resp.status(), StatusCode::OK);

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
    let metrics_json = super::json_body(metrics_resp).await;
    assert_eq!(metrics_json["memory_budget_bytes"], budget_bytes);
    assert_eq!(metrics_json["memory_used_bytes"], budget_bytes);
}

#[tokio::test]
async fn concurrent_upserts_do_not_bypass_collection_cap() {
    let cap = 4usize;
    let app = build_app(test_state_with_max_points(cap));

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "cap_concurrent", "dimension": 2}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    let mut tasks = Vec::new();
    for id in 1u64..=32u64 {
        let app = app.clone();
        tasks.push(tokio::spawn(async move {
            let upsert_req = Request::builder()
                .method("PUT")
                .uri(format!("/collections/cap_concurrent/points/{id}"))
                .header("content-type", "application/json")
                .body(Body::from(json!({"values": [id as f32, 0.0]}).to_string()))
                .expect("request must build");
            app.oneshot(upsert_req)
                .await
                .expect("response expected")
                .status()
        }));
    }

    let mut ok = 0usize;
    let mut rejected = 0usize;
    for task in tasks {
        match task.await.expect("task should join") {
            StatusCode::OK => ok += 1,
            StatusCode::TOO_MANY_REQUESTS => rejected += 1,
            other => panic!("unexpected status for concurrent upsert: {other}"),
        }
    }
    assert_eq!(ok, cap);
    assert_eq!(rejected, 32 - cap);

    let get_req = Request::builder()
        .method("GET")
        .uri("/collections/cap_concurrent")
        .body(Body::empty())
        .expect("request must build");
    let get_resp = app
        .clone()
        .oneshot(get_req)
        .await
        .expect("response expected");
    assert_eq!(get_resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(get_resp.into_body(), usize::MAX)
        .await
        .expect("body should be readable");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("valid json response");
    assert_eq!(payload["point_count"], cap);
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
