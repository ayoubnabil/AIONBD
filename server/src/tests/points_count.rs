use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use crate::build_app;

use super::{json_body, test_state};

#[tokio::test]
async fn points_count_returns_total_without_filter() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "count_total", "dimension": 2}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    for id in [1_u64, 2_u64, 3_u64] {
        let upsert_req = Request::builder()
            .method("PUT")
            .uri(format!("/collections/count_total/points/{id}"))
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"values": [id as f32, 0.0], "payload": {"group": "all"}}).to_string(),
            ))
            .expect("request must build");
        let upsert_resp = app
            .clone()
            .oneshot(upsert_req)
            .await
            .expect("response expected");
        assert_eq!(upsert_resp.status(), StatusCode::OK);
    }

    let count_req = Request::builder()
        .method("POST")
        .uri("/collections/count_total/points/count")
        .header("content-type", "application/json")
        .body(Body::from(json!({}).to_string()))
        .expect("request must build");
    let count_resp = app
        .clone()
        .oneshot(count_req)
        .await
        .expect("response expected");
    assert_eq!(count_resp.status(), StatusCode::OK);

    let payload = json_body(count_resp).await;
    assert_eq!(payload["count"], 3);
}

#[tokio::test]
async fn points_count_applies_filter() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "count_filtered", "dimension": 2}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    let points = [
        (1_u64, "gold", [1.0_f32, 0.0_f32]),
        (2_u64, "silver", [0.5_f32, 0.0_f32]),
        (3_u64, "gold", [0.2_f32, 0.0_f32]),
    ];
    for (id, tier, values) in points {
        let upsert_req = Request::builder()
            .method("PUT")
            .uri(format!("/collections/count_filtered/points/{id}"))
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"values": values, "payload": {"tier": tier}}).to_string(),
            ))
            .expect("request must build");
        let upsert_resp = app
            .clone()
            .oneshot(upsert_req)
            .await
            .expect("response expected");
        assert_eq!(upsert_resp.status(), StatusCode::OK);
    }

    let count_req = Request::builder()
        .method("POST")
        .uri("/collections/count_filtered/points/count")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"filter": {"must": [{"field": "tier", "value": "gold"}]}}).to_string(),
        ))
        .expect("request must build");
    let count_resp = app
        .clone()
        .oneshot(count_req)
        .await
        .expect("response expected");
    assert_eq!(count_resp.status(), StatusCode::OK);

    let payload = json_body(count_resp).await;
    assert_eq!(payload["count"], 2);
}
