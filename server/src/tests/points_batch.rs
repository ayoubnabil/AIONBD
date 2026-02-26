use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use super::{json_body, test_state};
use crate::build_app;

#[tokio::test]
async fn upsert_points_batch_creates_and_updates_points() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "batch_demo", "dimension": 3, "strict_finite": true}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    let first_batch_req = Request::builder()
        .method("POST")
        .uri("/collections/batch_demo/points")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "points": [
                    {"id": 1, "values": [1.0, 2.0, 3.0], "payload": {"tenant": "edge"}},
                    {"id": 2, "values": [4.0, 5.0, 6.0]}
                ]
            })
            .to_string(),
        ))
        .expect("request must build");
    let first_batch_resp = app
        .clone()
        .oneshot(first_batch_req)
        .await
        .expect("response expected");
    assert_eq!(first_batch_resp.status(), StatusCode::OK);
    let first_batch_json = json_body(first_batch_resp).await;
    assert_eq!(first_batch_json["created"], 2);
    assert_eq!(first_batch_json["updated"], 0);
    assert_eq!(first_batch_json["results"][0]["id"], 1);
    assert_eq!(first_batch_json["results"][0]["created"], true);
    assert_eq!(first_batch_json["results"][1]["id"], 2);
    assert_eq!(first_batch_json["results"][1]["created"], true);

    let second_batch_req = Request::builder()
        .method("POST")
        .uri("/collections/batch_demo/points")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "points": [
                    {"id": 2, "values": [40.0, 50.0, 60.0]},
                    {"id": 3, "values": [7.0, 8.0, 9.0], "payload": {"region": "eu"}}
                ]
            })
            .to_string(),
        ))
        .expect("request must build");
    let second_batch_resp = app
        .clone()
        .oneshot(second_batch_req)
        .await
        .expect("response expected");
    assert_eq!(second_batch_resp.status(), StatusCode::OK);
    let second_batch_json = json_body(second_batch_resp).await;
    assert_eq!(second_batch_json["created"], 1);
    assert_eq!(second_batch_json["updated"], 1);
    assert_eq!(second_batch_json["results"][0]["id"], 2);
    assert_eq!(second_batch_json["results"][0]["created"], false);
    assert_eq!(second_batch_json["results"][1]["id"], 3);
    assert_eq!(second_batch_json["results"][1]["created"], true);

    let get_collection_req = Request::builder()
        .method("GET")
        .uri("/collections/batch_demo")
        .body(Body::empty())
        .expect("request must build");
    let get_collection_resp = app
        .clone()
        .oneshot(get_collection_req)
        .await
        .expect("response expected");
    assert_eq!(get_collection_resp.status(), StatusCode::OK);
    let collection_json = json_body(get_collection_resp).await;
    assert_eq!(collection_json["point_count"], 3);

    let get_point_req = Request::builder()
        .method("GET")
        .uri("/collections/batch_demo/points/2")
        .body(Body::empty())
        .expect("request must build");
    let get_point_resp = app
        .clone()
        .oneshot(get_point_req)
        .await
        .expect("response expected");
    assert_eq!(get_point_resp.status(), StatusCode::OK);
    let point_json = json_body(get_point_resp).await;
    assert_eq!(point_json["values"], json!([40.0, 50.0, 60.0]));
}

#[tokio::test]
async fn upsert_points_batch_rejects_empty_points() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "batch_empty", "dimension": 3, "strict_finite": true}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    let batch_req = Request::builder()
        .method("POST")
        .uri("/collections/batch_empty/points")
        .header("content-type", "application/json")
        .body(Body::from(json!({"points": []}).to_string()))
        .expect("request must build");
    let batch_resp = app
        .clone()
        .oneshot(batch_req)
        .await
        .expect("response expected");
    assert_eq!(batch_resp.status(), StatusCode::BAD_REQUEST);
}
