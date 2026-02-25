use std::sync::atomic::Ordering;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use crate::build_app;

#[tokio::test]
async fn collection_routes_are_blocked_when_engine_is_not_ready() {
    let state = super::test_state();
    state.engine_loaded.store(false, Ordering::Relaxed);
    let app = build_app(state);

    let list_collections = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/collections")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .expect("response expected");
    assert_eq!(list_collections.status(), StatusCode::SERVICE_UNAVAILABLE);

    let create_collection = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/collections")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"name": "blocked", "dimension": 3}).to_string(),
                ))
                .expect("request must build"),
        )
        .await
        .expect("response expected");
    assert_eq!(create_collection.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn health_and_metrics_routes_remain_available_when_engine_is_not_ready() {
    let state = super::test_state();
    state.engine_loaded.store(false, Ordering::Relaxed);
    let app = build_app(state);

    let metrics = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .expect("response expected");
    assert_eq!(metrics.status(), StatusCode::OK);

    let distance = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/distance")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "left": [1.0, 0.0],
                        "right": [0.0, 1.0],
                        "metric": "l2"
                    })
                    .to_string(),
                ))
                .expect("request must build"),
        )
        .await
        .expect("response expected");
    assert_eq!(distance.status(), StatusCode::OK);
}
