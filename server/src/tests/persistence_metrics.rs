use std::sync::atomic::Ordering;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use crate::build_app;

#[tokio::test]
async fn metrics_reports_checkpoint_counters() {
    let state = super::test_state();
    state
        .metrics
        .persistence_checkpoint_degraded_total
        .store(4, Ordering::Relaxed);
    state
        .metrics
        .persistence_checkpoint_success_total
        .store(7, Ordering::Relaxed);
    state
        .metrics
        .persistence_checkpoint_error_total
        .store(2, Ordering::Relaxed);
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
    assert_eq!(payload["persistence_checkpoint_degraded_total"], 4);
    assert_eq!(payload["persistence_checkpoint_success_total"], 7);
    assert_eq!(payload["persistence_checkpoint_error_total"], 2);
}

#[tokio::test]
async fn metrics_prometheus_reports_checkpoint_counters() {
    let state = super::test_state();
    state
        .metrics
        .persistence_checkpoint_degraded_total
        .store(5, Ordering::Relaxed);
    state
        .metrics
        .persistence_checkpoint_success_total
        .store(9, Ordering::Relaxed);
    state
        .metrics
        .persistence_checkpoint_error_total
        .store(3, Ordering::Relaxed);
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
    assert!(payload.contains("aionbd_persistence_checkpoint_degraded_total 5"));
    assert!(payload.contains("aionbd_persistence_checkpoint_success_total 9"));
    assert!(payload.contains("aionbd_persistence_checkpoint_error_total 3"));
}
