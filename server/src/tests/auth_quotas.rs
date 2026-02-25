use std::collections::BTreeMap;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use crate::auth::{AuthConfig, AuthMode};
use crate::build_app;
use crate::config::AppConfig;
use crate::state::AppState;

fn quota_state(max_collections: u64, max_points: u64) -> AppState {
    let config = AppConfig {
        bind: "127.0.0.1:0".parse().expect("socket addr must parse"),
        max_dimension: 8,
        strict_finite: true,
        request_timeout_ms: 2_000,
        max_body_bytes: 1_048_576,
        max_concurrency: 256,
        max_page_limit: 1_000,
        max_topk_limit: 1_000,
        checkpoint_interval: 1,
        persistence_enabled: false,
        snapshot_path: std::path::PathBuf::from("unused_snapshot.json"),
        wal_path: std::path::PathBuf::from("unused_wal.jsonl"),
    };
    let auth_config = AuthConfig {
        mode: AuthMode::ApiKey,
        api_key_to_tenant: BTreeMap::from([
            ("key-a".to_string(), "tenant_a".to_string()),
            ("key-b".to_string(), "tenant_b".to_string()),
        ]),
        bearer_token_to_tenant: BTreeMap::new(),
        rate_limit_per_minute: 0,
        tenant_max_collections: max_collections,
        tenant_max_points: max_points,
    };
    AppState::with_collections_and_auth(config, BTreeMap::new(), auth_config)
}

fn request_with_api_key(
    method: &str,
    uri: &str,
    api_key: &str,
    body: Option<serde_json::Value>,
) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("x-api-key", api_key);
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    match body {
        Some(body) => builder
            .body(Body::from(body.to_string()))
            .expect("request must build"),
        None => builder.body(Body::empty()).expect("request must build"),
    }
}

#[tokio::test]
async fn tenant_collection_quota_is_enforced() {
    let app = build_app(quota_state(1, 0));

    let create_first = app
        .clone()
        .oneshot(request_with_api_key(
            "POST",
            "/collections",
            "key-a",
            Some(json!({"name":"c1","dimension":3})),
        ))
        .await
        .expect("response expected");
    assert_eq!(create_first.status(), StatusCode::OK);

    let create_second = app
        .clone()
        .oneshot(request_with_api_key(
            "POST",
            "/collections",
            "key-a",
            Some(json!({"name":"c2","dimension":3})),
        ))
        .await
        .expect("response expected");
    assert_eq!(create_second.status(), StatusCode::TOO_MANY_REQUESTS);
    let create_second_json = super::json_body(create_second).await;
    assert_eq!(create_second_json["code"], "resource_exhausted");
    let metrics_after_quota = app
        .clone()
        .oneshot(request_with_api_key("GET", "/metrics", "key-a", None))
        .await
        .expect("response expected");
    assert_eq!(metrics_after_quota.status(), StatusCode::OK);
    let metrics_after_quota_json = super::json_body(metrics_after_quota).await;
    assert_eq!(
        metrics_after_quota_json["tenant_quota_collection_rejections_total"],
        1
    );
    assert_eq!(
        metrics_after_quota_json["tenant_quota_point_rejections_total"],
        0
    );

    let create_other_tenant = app
        .oneshot(request_with_api_key(
            "POST",
            "/collections",
            "key-b",
            Some(json!({"name":"c2","dimension":3})),
        ))
        .await
        .expect("response expected");
    assert_eq!(create_other_tenant.status(), StatusCode::OK);
}

#[tokio::test]
async fn tenant_point_quota_is_enforced_across_collections() {
    let app = build_app(quota_state(8, 2));

    for collection in ["a", "b"] {
        let create = app
            .clone()
            .oneshot(request_with_api_key(
                "POST",
                "/collections",
                "key-a",
                Some(json!({"name": collection, "dimension": 3})),
            ))
            .await
            .expect("response expected");
        assert_eq!(create.status(), StatusCode::OK);
    }

    let upsert_a1 = app
        .clone()
        .oneshot(request_with_api_key(
            "PUT",
            "/collections/a/points/1",
            "key-a",
            Some(json!({"values":[1.0,2.0,3.0]})),
        ))
        .await
        .expect("response expected");
    assert_eq!(upsert_a1.status(), StatusCode::OK);

    let upsert_b2 = app
        .clone()
        .oneshot(request_with_api_key(
            "PUT",
            "/collections/b/points/2",
            "key-a",
            Some(json!({"values":[3.0,2.0,1.0]})),
        ))
        .await
        .expect("response expected");
    assert_eq!(upsert_b2.status(), StatusCode::OK);

    let update_existing = app
        .clone()
        .oneshot(request_with_api_key(
            "PUT",
            "/collections/a/points/1",
            "key-a",
            Some(json!({"values":[9.0,9.0,9.0]})),
        ))
        .await
        .expect("response expected");
    assert_eq!(update_existing.status(), StatusCode::OK);

    let upsert_third = app
        .clone()
        .oneshot(request_with_api_key(
            "PUT",
            "/collections/a/points/3",
            "key-a",
            Some(json!({"values":[0.0,0.0,1.0]})),
        ))
        .await
        .expect("response expected");
    assert_eq!(upsert_third.status(), StatusCode::TOO_MANY_REQUESTS);
    let metrics_after_point_quota = app
        .clone()
        .oneshot(request_with_api_key("GET", "/metrics", "key-a", None))
        .await
        .expect("response expected");
    assert_eq!(metrics_after_point_quota.status(), StatusCode::OK);
    let metrics_after_point_quota_json = super::json_body(metrics_after_point_quota).await;
    assert_eq!(
        metrics_after_point_quota_json["tenant_quota_point_rejections_total"],
        1
    );

    let create_other_tenant = app
        .clone()
        .oneshot(request_with_api_key(
            "POST",
            "/collections",
            "key-b",
            Some(json!({"name":"c","dimension":3})),
        ))
        .await
        .expect("response expected");
    assert_eq!(create_other_tenant.status(), StatusCode::OK);

    let upsert_other_tenant = app
        .oneshot(request_with_api_key(
            "PUT",
            "/collections/c/points/7",
            "key-b",
            Some(json!({"values":[1.0,0.0,0.0]})),
        ))
        .await
        .expect("response expected");
    assert_eq!(upsert_other_tenant.status(), StatusCode::OK);
}
