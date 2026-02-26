use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use crate::build_app;

use super::test_state;

#[tokio::test]
async fn filter_clause_rejects_unknown_fields_in_untagged_input() {
    let app = build_app(test_state());

    let request = Request::builder()
        .method("POST")
        .uri("/collections/any/search/topk")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "query": [1.0, 0.0],
                "metric": "dot",
                "limit": 1,
                "filter": {
                    "must": [{"field": "score", "value": 42, "gt": 1.0}]
                }
            })
            .to_string(),
        ))
        .expect("request must build");
    let response = app.oneshot(request).await.expect("response expected");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn metadata_match_treats_integer_and_float_numbers_as_equal() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "numeric_filter", "dimension": 2}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    let upsert_req = Request::builder()
        .method("PUT")
        .uri("/collections/numeric_filter/points/1")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"values": [1.0, 0.0], "payload": {"score": 42.0}}).to_string(),
        ))
        .expect("request must build");
    let upsert_resp = app
        .clone()
        .oneshot(upsert_req)
        .await
        .expect("response expected");
    assert_eq!(upsert_resp.status(), StatusCode::OK);

    let search_req = Request::builder()
        .method("POST")
        .uri("/collections/numeric_filter/search/topk")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "query": [1.0, 0.0],
                "metric": "dot",
                "mode": "exact",
                "limit": 5,
                "filter": {
                    "must": [{"field": "score", "value": 42}]
                }
            })
            .to_string(),
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
    assert_eq!(payload["hits"][0]["id"], 1);
}

#[tokio::test]
async fn metadata_match_uses_tolerant_float_comparison() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "float_filter", "dimension": 2}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    let value = 0.1_f64 + 0.2_f64;
    let upsert_req = Request::builder()
        .method("PUT")
        .uri("/collections/float_filter/points/1")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"values": [1.0, 0.0], "payload": {"score": value}}).to_string(),
        ))
        .expect("request must build");
    let upsert_resp = app
        .clone()
        .oneshot(upsert_req)
        .await
        .expect("response expected");
    assert_eq!(upsert_resp.status(), StatusCode::OK);

    let search_req = Request::builder()
        .method("POST")
        .uri("/collections/float_filter/search/topk")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "query": [1.0, 0.0],
                "metric": "dot",
                "mode": "exact",
                "limit": 5,
                "filter": {
                    "must": [{"field": "score", "value": 0.3}]
                }
            })
            .to_string(),
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
    assert_eq!(payload["hits"][0]["id"], 1);
}

#[cfg(feature = "exp_filter_must_not")]
#[tokio::test]
async fn filter_must_not_excludes_matching_points() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "must_not_filter", "dimension": 2}).to_string(),
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
        .uri("/collections/must_not_filter/points/1")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"values": [1.0, 0.0], "payload": {"tier": "gold"}}).to_string(),
        ))
        .expect("request must build");
    let first_resp = app
        .clone()
        .oneshot(upsert_first)
        .await
        .expect("response expected");
    assert_eq!(first_resp.status(), StatusCode::OK);

    let upsert_second = Request::builder()
        .method("PUT")
        .uri("/collections/must_not_filter/points/2")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"values": [0.9, 0.0], "payload": {"tier": "silver"}}).to_string(),
        ))
        .expect("request must build");
    let second_resp = app
        .clone()
        .oneshot(upsert_second)
        .await
        .expect("response expected");
    assert_eq!(second_resp.status(), StatusCode::OK);

    let search_req = Request::builder()
        .method("POST")
        .uri("/collections/must_not_filter/search/topk")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "query": [1.0, 0.0],
                "metric": "dot",
                "mode": "exact",
                "limit": 5,
                "filter": {
                    "must_not": [{"field": "tier", "value": "gold"}]
                }
            })
            .to_string(),
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
    assert_eq!(payload["hits"][0]["id"], 2);
}

#[cfg(not(feature = "exp_filter_must_not"))]
#[tokio::test]
async fn filter_must_not_requires_feature_flag() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "must_not_flag", "dimension": 2}).to_string(),
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
        .uri("/collections/must_not_flag/search/topk")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "query": [1.0, 0.0],
                "metric": "dot",
                "mode": "exact",
                "limit": 5,
                "filter": {
                    "must_not": [{"field": "tier", "value": "gold"}]
                }
            })
            .to_string(),
        ))
        .expect("request must build");
    let search_resp = app
        .clone()
        .oneshot(search_req)
        .await
        .expect("response expected");
    assert_eq!(search_resp.status(), StatusCode::BAD_REQUEST);
}
