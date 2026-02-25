use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use std::sync::atomic::Ordering;
use tower::ServiceExt;

use crate::build_app;
use crate::config::AppConfig;
use crate::state::AppState;

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
async fn metrics_reports_collection_and_point_counts() {
    let app = build_app(test_state());

    let create_collection = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "metrics_demo", "dimension": 2}).to_string(),
        ))
        .expect("request must build");
    let create_response = app
        .clone()
        .oneshot(create_collection)
        .await
        .expect("response expected");
    assert_eq!(create_response.status(), StatusCode::OK);

    for id in [10u64, 20u64] {
        let upsert = Request::builder()
            .method("PUT")
            .uri(format!("/collections/metrics_demo/points/{id}"))
            .header("content-type", "application/json")
            .body(Body::from(json!({"values": [1.0, 2.0]}).to_string()))
            .expect("request must build");
        let response = app
            .clone()
            .oneshot(upsert)
            .await
            .expect("response expected");
        assert_eq!(response.status(), StatusCode::OK);
    }

    let metrics_req = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(Body::empty())
        .expect("request must build");
    let metrics_resp = app
        .clone()
        .oneshot(metrics_req)
        .await
        .expect("response expected");
    assert_eq!(metrics_resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(metrics_resp.into_body(), usize::MAX)
        .await
        .expect("body should be readable");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("valid json response");

    assert_eq!(payload["ready"], true);
    assert_eq!(payload["engine_loaded"], true);
    assert_eq!(payload["storage_available"], true);
    assert!(payload["http_requests_total"].as_u64().unwrap_or(0) >= 4);
    assert_eq!(payload["http_requests_in_flight"], 1);
    assert!(payload["http_responses_2xx_total"].as_u64().unwrap_or(0) >= 3);
    assert_eq!(payload["http_responses_4xx_total"], 0);
    assert_eq!(payload["http_requests_5xx_total"], 0);
    assert!(
        payload["http_request_duration_us_total"]
            .as_u64()
            .unwrap_or(0)
            >= payload["http_request_duration_us_max"]
                .as_u64()
                .unwrap_or(0)
    );
    assert!(
        payload["http_request_duration_us_avg"]
            .as_f64()
            .unwrap_or(-1.0)
            >= 0.0
    );
    assert_eq!(payload["collections"], 1);
    assert_eq!(payload["points"], 2);
    assert_eq!(payload["l2_indexes"], 0);
    assert_eq!(payload["l2_index_build_cooldown_ms"], 1_000);
    assert_eq!(payload["l2_index_warmup_on_boot"], true);
    assert_eq!(payload["persistence_enabled"], false);
    assert_eq!(payload["persistence_wal_sync_on_write"], true);
    assert_eq!(payload["persistence_wal_sync_every_n_writes"], 0);
    assert_eq!(payload["persistence_async_checkpoints"], false);
    assert_eq!(payload["persistence_checkpoint_compact_after"], 64);
    assert_eq!(payload["persistence_writes"], 0);
    assert_eq!(payload["persistence_checkpoint_in_flight"], false);
    assert_eq!(payload["persistence_wal_size_bytes"], 0);
    assert_eq!(payload["persistence_wal_tail_open"], false);
    assert_eq!(payload["persistence_incremental_segments"], 0);
    assert_eq!(payload["persistence_incremental_size_bytes"], 0);
    assert_eq!(payload["auth_failures_total"], 0);
    assert_eq!(payload["rate_limit_rejections_total"], 0);
    assert!(payload["audit_events_total"].as_u64().unwrap_or(0) >= 1);
    assert_eq!(payload["tenant_quota_collection_rejections_total"], 0);
    assert_eq!(payload["tenant_quota_point_rejections_total"], 0);
    assert!(payload["search_queries_total"].as_u64().is_some());
    assert!(payload["search_ivf_queries_total"].as_u64().is_some());
    assert!(payload["search_ivf_fallback_exact_total"]
        .as_u64()
        .is_some());
    assert_eq!(payload["max_points_per_collection"], 1_000_000);
    assert!(payload["uptime_ms"].as_u64().is_some());
}

#[tokio::test]
async fn metrics_reflect_runtime_flags_and_write_counter() {
    let state = test_state();
    state.engine_loaded.store(false, Ordering::Relaxed);
    state.storage_available.store(false, Ordering::Relaxed);
    state
        .metrics
        .http_requests_total
        .store(5, Ordering::Relaxed);
    state
        .metrics
        .http_requests_in_flight
        .store(1, Ordering::Relaxed);
    state
        .metrics
        .http_responses_2xx_total
        .store(4, Ordering::Relaxed);
    state
        .metrics
        .http_responses_4xx_total
        .store(2, Ordering::Relaxed);
    state
        .metrics
        .http_requests_5xx_total
        .store(2, Ordering::Relaxed);
    state
        .metrics
        .http_request_duration_us_total
        .store(1_500, Ordering::Relaxed);
    state
        .metrics
        .http_request_duration_us_max
        .store(500, Ordering::Relaxed);
    state
        .metrics
        .auth_failures_total
        .store(11, Ordering::Relaxed);
    state
        .metrics
        .rate_limit_rejections_total
        .store(12, Ordering::Relaxed);
    state
        .metrics
        .audit_events_total
        .store(13, Ordering::Relaxed);
    state.metrics.persistence_writes.store(9, Ordering::Relaxed);
    state
        .persistence_checkpoint_in_flight
        .store(true, Ordering::Relaxed);
    state
        .metrics
        .search_queries_total
        .store(21, Ordering::Relaxed);
    state
        .metrics
        .search_ivf_queries_total
        .store(8, Ordering::Relaxed);
    state
        .metrics
        .search_ivf_fallback_exact_total
        .store(3, Ordering::Relaxed);
    let app = build_app(state);

    let metrics_req = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(Body::empty())
        .expect("request must build");
    let metrics_resp = app.oneshot(metrics_req).await.expect("response expected");
    assert_eq!(metrics_resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(metrics_resp.into_body(), usize::MAX)
        .await
        .expect("body should be readable");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("valid json response");

    assert_eq!(payload["ready"], false);
    assert_eq!(payload["engine_loaded"], false);
    assert_eq!(payload["storage_available"], false);
    assert_eq!(payload["http_requests_total"], 6);
    assert_eq!(payload["http_requests_in_flight"], 2);
    assert_eq!(payload["http_responses_2xx_total"], 4);
    assert_eq!(payload["http_responses_4xx_total"], 2);
    assert_eq!(payload["http_requests_5xx_total"], 2);
    assert!(
        payload["http_request_duration_us_total"]
            .as_u64()
            .unwrap_or(0)
            >= 1_500
    );
    assert!(
        payload["http_request_duration_us_max"]
            .as_u64()
            .unwrap_or(0)
            >= 500
    );
    assert!(
        payload["http_request_duration_us_avg"]
            .as_f64()
            .unwrap_or(-1.0)
            >= 0.0
    );
    assert_eq!(payload["auth_failures_total"], 11);
    assert_eq!(payload["rate_limit_rejections_total"], 12);
    assert_eq!(payload["audit_events_total"], 13);
    assert_eq!(payload["persistence_writes"], 9);
    assert_eq!(payload["persistence_checkpoint_in_flight"], true);
    assert_eq!(payload["persistence_wal_sync_on_write"], true);
    assert_eq!(payload["persistence_wal_sync_every_n_writes"], 0);
    assert_eq!(payload["persistence_async_checkpoints"], false);
    assert_eq!(payload["persistence_checkpoint_compact_after"], 64);
    assert_eq!(payload["persistence_wal_size_bytes"], 0);
    assert_eq!(payload["persistence_wal_tail_open"], false);
    assert_eq!(payload["persistence_incremental_segments"], 0);
    assert_eq!(payload["persistence_incremental_size_bytes"], 0);
    assert_eq!(payload["search_queries_total"], 21);
    assert_eq!(payload["search_ivf_queries_total"], 8);
    assert_eq!(payload["search_ivf_fallback_exact_total"], 3);
    assert_eq!(payload["l2_index_build_cooldown_ms"], 1_000);
    assert_eq!(payload["l2_index_warmup_on_boot"], true);
    assert_eq!(payload["max_points_per_collection"], 1_000_000);
}
