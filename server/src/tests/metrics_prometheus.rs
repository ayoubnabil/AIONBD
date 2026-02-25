use std::sync::atomic::Ordering;

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
async fn metrics_prometheus_reports_text_metrics() {
    let app = build_app(test_state());

    let create_collection = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "metrics_prom", "dimension": 2}).to_string(),
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
            .uri(format!("/collections/metrics_prom/points/{id}"))
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
        .uri("/metrics/prometheus")
        .body(Body::empty())
        .expect("request must build");
    let metrics_resp = app
        .clone()
        .oneshot(metrics_req)
        .await
        .expect("response expected");
    assert_eq!(metrics_resp.status(), StatusCode::OK);

    let content_type = metrics_resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    assert!(content_type.starts_with("text/plain"));

    let body = axum::body::to_bytes(metrics_resp.into_body(), usize::MAX)
        .await
        .expect("body should be readable");
    let payload = String::from_utf8(body.to_vec()).expect("response should be utf8");

    assert!(payload.contains("aionbd_collections 1"));
    assert!(payload.contains("aionbd_points 2"));
    assert!(payload.contains("aionbd_l2_indexes 0"));
    assert!(payload.contains("aionbd_l2_index_build_cooldown_ms 1000"));
    assert!(payload.contains("aionbd_http_requests_total"));
    assert!(payload.contains("aionbd_http_requests_in_flight 1"));
    assert!(payload.contains("aionbd_http_responses_2xx_total 3"));
    assert!(payload.contains("aionbd_http_responses_4xx_total 0"));
    assert!(payload.contains("aionbd_http_requests_5xx_total 0"));
    assert!(payload.contains("aionbd_http_request_duration_us_total"));
    assert!(payload.contains("aionbd_http_request_duration_us_max"));
    assert!(payload.contains("aionbd_http_request_duration_us_avg"));
    assert!(payload.contains("aionbd_ready 1"));
    assert!(payload.contains("aionbd_engine_loaded 1"));
    assert!(payload.contains("aionbd_storage_available 1"));
    assert!(payload.contains("aionbd_persistence_enabled 0"));
    assert!(payload.contains("aionbd_persistence_wal_sync_on_write 1"));
    assert!(payload.contains("aionbd_persistence_writes 0"));
    assert!(payload.contains("aionbd_persistence_wal_size_bytes 0"));
    assert!(payload.contains("aionbd_persistence_wal_tail_open 0"));
    assert!(payload.contains("aionbd_persistence_incremental_segments 0"));
    assert!(payload.contains("aionbd_persistence_incremental_size_bytes 0"));
    assert!(payload.contains("aionbd_auth_failures_total 0"));
    assert!(payload.contains("aionbd_rate_limit_rejections_total 0"));
    assert!(payload.contains("aionbd_audit_events_total "));
    assert!(payload.contains("aionbd_tenant_quota_collection_rejections_total 0"));
    assert!(payload.contains("aionbd_tenant_quota_point_rejections_total 0"));
    assert!(payload.contains("aionbd_search_queries_total "));
    assert!(payload.contains("aionbd_search_ivf_queries_total "));
    assert!(payload.contains("aionbd_search_ivf_fallback_exact_total "));
    assert!(payload.contains("aionbd_max_points_per_collection 1000000"));
    assert!(payload.contains("# TYPE aionbd_persistence_writes counter"));
}

#[tokio::test]
async fn metrics_prometheus_reflects_runtime_flags() {
    let mut state = test_state();
    {
        let config = std::sync::Arc::make_mut(&mut state.config);
        config.wal_sync_on_write = false;
    }
    state.engine_loaded.store(false, Ordering::Relaxed);
    state.storage_available.store(false, Ordering::Relaxed);
    state
        .metrics
        .http_requests_total
        .store(8, Ordering::Relaxed);
    state
        .metrics
        .http_requests_in_flight
        .store(3, Ordering::Relaxed);
    state
        .metrics
        .http_responses_2xx_total
        .store(6, Ordering::Relaxed);
    state
        .metrics
        .http_responses_4xx_total
        .store(2, Ordering::Relaxed);
    state
        .metrics
        .http_requests_5xx_total
        .store(1, Ordering::Relaxed);
    state
        .metrics
        .http_request_duration_us_total
        .store(1_234_567, Ordering::Relaxed);
    state
        .metrics
        .http_request_duration_us_max
        .store(999_999, Ordering::Relaxed);
    state
        .metrics
        .auth_failures_total
        .store(3, Ordering::Relaxed);
    state
        .metrics
        .rate_limit_rejections_total
        .store(4, Ordering::Relaxed);
    state.metrics.audit_events_total.store(5, Ordering::Relaxed);
    state
        .metrics
        .persistence_writes
        .store(12, Ordering::Relaxed);
    let app = build_app(state);

    let metrics_req = Request::builder()
        .method("GET")
        .uri("/metrics/prometheus")
        .body(Body::empty())
        .expect("request must build");
    let metrics_resp = app.oneshot(metrics_req).await.expect("response expected");
    assert_eq!(metrics_resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(metrics_resp.into_body(), usize::MAX)
        .await
        .expect("body should be readable");
    let payload = String::from_utf8(body.to_vec()).expect("response should be utf8");

    assert!(payload.contains("aionbd_http_requests_total 9"));
    assert!(payload.contains("aionbd_http_requests_in_flight 4"));
    assert!(payload.contains("aionbd_http_responses_2xx_total 6"));
    assert!(payload.contains("aionbd_http_responses_4xx_total 2"));
    assert!(payload.contains("aionbd_http_requests_5xx_total 1"));
    assert!(payload.contains("aionbd_http_request_duration_us_total "));
    assert!(payload.contains("aionbd_http_request_duration_us_max 999999"));
    assert!(payload.contains("aionbd_http_request_duration_us_avg "));
    assert!(payload.contains("aionbd_l2_index_build_cooldown_ms 1000"));
    assert!(payload.contains("aionbd_ready 0"));
    assert!(payload.contains("aionbd_engine_loaded 0"));
    assert!(payload.contains("aionbd_storage_available 0"));
    assert!(payload.contains("aionbd_persistence_wal_sync_on_write 0"));
    assert!(payload.contains("aionbd_persistence_writes 12"));
    assert!(payload.contains("aionbd_persistence_wal_size_bytes 0"));
    assert!(payload.contains("aionbd_persistence_wal_tail_open 0"));
    assert!(payload.contains("aionbd_persistence_incremental_segments 0"));
    assert!(payload.contains("aionbd_persistence_incremental_size_bytes 0"));
    assert!(payload.contains("aionbd_auth_failures_total 3"));
    assert!(payload.contains("aionbd_rate_limit_rejections_total 4"));
    assert!(payload.contains("aionbd_audit_events_total 5"));
    assert!(payload.contains("aionbd_search_queries_total "));
    assert!(payload.contains("aionbd_search_ivf_queries_total "));
    assert!(payload.contains("aionbd_search_ivf_fallback_exact_total "));
    assert!(payload.contains("aionbd_max_points_per_collection 1000000"));
}
