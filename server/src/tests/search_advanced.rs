use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use aionbd_core::{Collection, CollectionConfig};

use crate::build_app;
use crate::ivf_index::IvfIndex;

use super::test_state;

#[tokio::test]
async fn search_supports_filtering_with_metadata_payload() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "search_filter", "dimension": 2}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    for (id, values, payload) in [
        (
            1u64,
            json!([1.0, 0.0]),
            json!({"category":"a","score":0.7,"active":false}),
        ),
        (
            2u64,
            json!([0.9, 0.0]),
            json!({"category":"a","score":0.2,"active":true}),
        ),
        (
            3u64,
            json!([0.8, 0.0]),
            json!({"category":"a","score":0.1,"active":false}),
        ),
        (
            4u64,
            json!([0.95, 0.0]),
            json!({"category":"b","score":0.9,"active":true}),
        ),
    ] {
        let upsert_req = Request::builder()
            .method("PUT")
            .uri(format!("/collections/search_filter/points/{id}"))
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"values": values, "payload": payload}).to_string(),
            ))
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
        .uri("/collections/search_filter/search/topk")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "query": [1.0, 0.0],
                "metric": "dot",
                "mode": "exact",
                "limit": 5,
                "filter": {
                    "must": [{"field": "category", "value": "a"}],
                    "should": [
                        {"field": "score", "gte": 0.6},
                        {"field": "active", "value": true}
                    ],
                    "minimum_should_match": 1
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

    assert_eq!(payload["mode"], "exact");
    assert_eq!(payload["recall_at_k"], 1.0);
    assert_eq!(payload["hits"][0]["id"], 1);
    assert_eq!(payload["hits"][1]["id"], 2);
    assert_eq!(payload["hits"].as_array().map(|items| items.len()), Some(2));
    assert_eq!(payload["hits"][0]["payload"]["category"], "a");
}

#[tokio::test]
async fn search_mode_ivf_rejects_non_l2_metric() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "search_mode_err", "dimension": 2}).to_string(),
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
        .uri("/collections/search_mode_err/search/topk")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"query": [1.0, 0.0], "metric": "dot", "mode": "ivf", "limit": 1}).to_string(),
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
async fn search_recall_target_rejects_ivf_mode() {
    let app = build_app(test_state());

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
    assert_eq!(ivf_resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn search_mode_ivf_without_target_recall_avoids_quality_full_scan() {
    let state = test_state();
    let mut collection = Collection::new(
        "search_ivf_fast".to_string(),
        CollectionConfig::new(2, true).expect("config should be valid"),
    )
    .expect("collection should be valid");
    for id in 0..IvfIndex::min_indexed_points() as u64 {
        collection
            .upsert_point(id, vec![id as f32, (id % 7) as f32])
            .expect("upsert should succeed");
    }
    let index = IvfIndex::build(&collection).expect("index should be built");
    state
        .collections
        .write()
        .expect("collection registry lock should be available")
        .insert(
            "search_ivf_fast".to_string(),
            std::sync::Arc::new(std::sync::RwLock::new(collection)),
        );
    state
        .l2_indexes
        .write()
        .expect("l2 index cache lock should be available")
        .insert("search_ivf_fast".to_string(), index);
    let app = build_app(state);

    let ivf_req = Request::builder()
        .method("POST")
        .uri("/collections/search_ivf_fast/search/topk")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"query": [33.0, 1.0], "metric": "l2", "mode": "ivf", "limit": 20}).to_string(),
        ))
        .expect("request must build");
    let ivf_resp = app.oneshot(ivf_req).await.expect("response expected");
    assert_eq!(ivf_resp.status(), StatusCode::OK);
    let ivf_body = axum::body::to_bytes(ivf_resp.into_body(), usize::MAX)
        .await
        .expect("body should be readable");
    let ivf_payload: serde_json::Value =
        serde_json::from_slice(&ivf_body).expect("valid json response");

    assert_eq!(ivf_payload["mode"], "ivf");
    assert!(ivf_payload.get("recall_at_k").is_none());
}

#[tokio::test]
async fn search_cosine_skips_zero_norm_points_instead_of_failing() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "search_zero_norm", "dimension": 2, "strict_finite": false}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    for (id, values) in [(1_u64, json!([1.0, 0.0])), (2_u64, json!([0.0, 0.0]))] {
        let upsert_req = Request::builder()
            .method("PUT")
            .uri(format!("/collections/search_zero_norm/points/{id}"))
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
        .uri("/collections/search_zero_norm/search/topk")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"query": [1.0, 0.0], "metric": "cosine", "mode": "exact", "limit": 10})
                .to_string(),
        ))
        .expect("request must build");
    let search_resp = app.oneshot(search_req).await.expect("response expected");
    assert_eq!(search_resp.status(), StatusCode::OK);
}
