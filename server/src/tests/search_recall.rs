use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use crate::build_app;
use crate::ivf_index::IvfIndex;

#[tokio::test]
async fn search_recall_target_on_ivf_falls_back_to_exact_mode() {
    let app = build_app(super::test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "search_recall", "dimension": 2}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    for id in 0..IvfIndex::min_indexed_points() as u64 {
        let upsert_req = Request::builder()
            .method("PUT")
            .uri(format!("/collections/search_recall/points/{id}"))
            .header("content-type", "application/json")
            .body(Body::from(json!({"values": [id as f32, 0.0]}).to_string()))
            .expect("request must build");
        let upsert_resp = app
            .clone()
            .oneshot(upsert_req)
            .await
            .expect("response expected");
        assert_eq!(upsert_resp.status(), StatusCode::OK);
    }

    let ivf_req = Request::builder()
        .method("POST")
        .uri("/collections/search_recall/search/topk")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "query": [33.0, 12.0],
                "metric": "l2",
                "mode": "ivf",
                "target_recall": 1.0,
                "limit": 20
            })
            .to_string(),
        ))
        .expect("request must build");
    let ivf_resp = app
        .clone()
        .oneshot(ivf_req)
        .await
        .expect("response expected");
    assert_eq!(ivf_resp.status(), StatusCode::OK);
    let ivf_body = axum::body::to_bytes(ivf_resp.into_body(), usize::MAX)
        .await
        .expect("body should be readable");
    let ivf_payload: serde_json::Value =
        serde_json::from_slice(&ivf_body).expect("valid json response");

    assert_eq!(ivf_payload["mode"], "exact");
    assert_eq!(ivf_payload["recall_at_k"], 1.0);
}
