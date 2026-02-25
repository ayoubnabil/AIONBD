use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tokio::sync::Semaphore;
use tower::ServiceExt;

use crate::build_app;
use crate::state::TenantRateWindow;

#[tokio::test]
async fn metrics_report_runtime_map_cardinality() {
    let state = super::test_state();
    state
        .collection_write_locks
        .lock()
        .await
        .insert("alpha".to_string(), Arc::new(Semaphore::new(1)));
    state.tenant_rate_windows.lock().await.insert(
        "tenant-a".to_string(),
        TenantRateWindow {
            minute: 123,
            count: 2,
        },
    );
    state
        .tenant_quota_locks
        .lock()
        .await
        .insert("tenant-a".to_string(), Arc::new(Semaphore::new(1)));

    let app = build_app(state);
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .expect("response expected");
    assert_eq!(response.status(), StatusCode::OK);

    let payload = super::json_body(response).await;
    assert_eq!(payload["collection_write_lock_entries"], 1);
    assert_eq!(payload["tenant_rate_window_entries"], 1);
    assert_eq!(payload["tenant_quota_lock_entries"], 1);
}

#[tokio::test]
async fn metrics_prometheus_report_runtime_map_cardinality() {
    let state = super::test_state();
    state
        .collection_write_locks
        .lock()
        .await
        .insert("beta".to_string(), Arc::new(Semaphore::new(1)));
    state.tenant_rate_windows.lock().await.insert(
        "tenant-b".to_string(),
        TenantRateWindow {
            minute: 456,
            count: 3,
        },
    );
    state
        .tenant_quota_locks
        .lock()
        .await
        .insert("tenant-b".to_string(), Arc::new(Semaphore::new(1)));

    let app = build_app(state);
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics/prometheus")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .expect("response expected");
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should be readable");
    let payload = String::from_utf8(body.to_vec()).expect("response should be utf8");
    assert!(payload.contains("aionbd_collection_write_lock_entries 1"));
    assert!(payload.contains("aionbd_tenant_rate_window_entries 1"));
    assert!(payload.contains("aionbd_tenant_quota_lock_entries 1"));
}
