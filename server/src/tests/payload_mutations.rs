use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use crate::build_app;

use super::{json_body, test_state};

#[tokio::test]
async fn set_payload_merges_and_overwrites_fields() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name":"payload_set","dimension":3}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    for (id, tier) in [(1_u64, "free"), (2_u64, "basic")] {
        let upsert_req = Request::builder()
            .method("PUT")
            .uri(format!("/collections/payload_set/points/{id}"))
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"values":[1.0,2.0,3.0], "payload":{"tier":tier,"count":1}}).to_string(),
            ))
            .expect("request must build");
        let upsert_resp = app
            .clone()
            .oneshot(upsert_req)
            .await
            .expect("response expected");
        assert_eq!(upsert_resp.status(), StatusCode::OK);
    }

    let set_payload_req = Request::builder()
        .method("POST")
        .uri("/collections/payload_set/points/payload/set")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "points": [1, 2, 2],
                "payload": {"tier":"pro","region":"eu"}
            })
            .to_string(),
        ))
        .expect("request must build");
    let set_payload_resp = app
        .clone()
        .oneshot(set_payload_req)
        .await
        .expect("response expected");
    assert_eq!(set_payload_resp.status(), StatusCode::OK);
    let set_payload_json = json_body(set_payload_resp).await;
    assert_eq!(set_payload_json["updated"], 2);

    let get_point_req = Request::builder()
        .method("GET")
        .uri("/collections/payload_set/points/1")
        .body(Body::empty())
        .expect("request must build");
    let get_point_resp = app
        .clone()
        .oneshot(get_point_req)
        .await
        .expect("response expected");
    assert_eq!(get_point_resp.status(), StatusCode::OK);
    let get_point_json = json_body(get_point_resp).await;
    assert_eq!(get_point_json["payload"]["tier"], "pro");
    assert_eq!(get_point_json["payload"]["region"], "eu");
    assert_eq!(get_point_json["payload"]["count"], 1);
}

#[tokio::test]
async fn delete_payload_keys_removes_requested_fields_only() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name":"payload_delete","dimension":3}).to_string(),
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
        .uri("/collections/payload_delete/points/1")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "values":[1.0,2.0,3.0],
                "payload":{"tier":"pro","region":"eu","keep":42}
            })
            .to_string(),
        ))
        .expect("request must build");
    let upsert_resp = app
        .clone()
        .oneshot(upsert_req)
        .await
        .expect("response expected");
    assert_eq!(upsert_resp.status(), StatusCode::OK);

    let delete_payload_req = Request::builder()
        .method("POST")
        .uri("/collections/payload_delete/points/payload/delete")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"points":[1], "keys":["region","tier","tier"]}).to_string(),
        ))
        .expect("request must build");
    let delete_payload_resp = app
        .clone()
        .oneshot(delete_payload_req)
        .await
        .expect("response expected");
    assert_eq!(delete_payload_resp.status(), StatusCode::OK);
    let delete_payload_json = json_body(delete_payload_resp).await;
    assert_eq!(delete_payload_json["updated"], 1);

    let get_point_req = Request::builder()
        .method("GET")
        .uri("/collections/payload_delete/points/1")
        .body(Body::empty())
        .expect("request must build");
    let get_point_resp = app
        .clone()
        .oneshot(get_point_req)
        .await
        .expect("response expected");
    assert_eq!(get_point_resp.status(), StatusCode::OK);
    let get_point_json = json_body(get_point_resp).await;
    assert_eq!(get_point_json["payload"]["keep"], 42);
    assert!(get_point_json["payload"].get("region").is_none());
    assert!(get_point_json["payload"].get("tier").is_none());
}

#[tokio::test]
async fn payload_mutation_rejects_empty_inputs() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name":"payload_invalid","dimension":3}).to_string(),
        ))
        .expect("request must build");
    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    let set_payload_req = Request::builder()
        .method("POST")
        .uri("/collections/payload_invalid/points/payload/set")
        .header("content-type", "application/json")
        .body(Body::from(json!({"points":[1], "payload":{}}).to_string()))
        .expect("request must build");
    let set_payload_resp = app
        .clone()
        .oneshot(set_payload_req)
        .await
        .expect("response expected");
    assert_eq!(set_payload_resp.status(), StatusCode::BAD_REQUEST);

    let delete_payload_req = Request::builder()
        .method("POST")
        .uri("/collections/payload_invalid/points/payload/delete")
        .header("content-type", "application/json")
        .body(Body::from(json!({"points":[1], "keys":[]}).to_string()))
        .expect("request must build");
    let delete_payload_resp = app
        .clone()
        .oneshot(delete_payload_req)
        .await
        .expect("response expected");
    assert_eq!(delete_payload_resp.status(), StatusCode::BAD_REQUEST);
}
