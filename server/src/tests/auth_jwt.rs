use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::Serialize;
use serde_json::json;
use tower::ServiceExt;

use crate::auth::jwt::JwtConfig;
use crate::auth::{AuthConfig, AuthMode};
use crate::build_app;
use crate::config::AppConfig;
use crate::state::AppState;

const TEST_JWT_SECRET: &str = "test-jwt-secret-for-aionbd";

fn jwt_state() -> AppState {
    let config = AppConfig {
        bind: "127.0.0.1:0".parse().expect("socket addr must parse"),
        max_dimension: 8,
        max_points_per_collection: 1_000_000,
        memory_budget_bytes: 0,
        strict_finite: true,
        request_timeout_ms: 2_000,
        max_body_bytes: 1_048_576,
        max_concurrency: 256,
        max_page_limit: 1_000,
        max_topk_limit: 1_000,
        checkpoint_interval: 1,
        persistence_enabled: false,
        wal_sync_on_write: true,
        wal_sync_every_n_writes: 0,
        wal_sync_interval_seconds: 0,
        wal_group_commit_max_batch: 16,
        wal_group_commit_flush_delay_ms: 0,
        async_checkpoints: false,
        checkpoint_compact_after: 64,
        snapshot_path: std::path::PathBuf::from("unused_snapshot.json"),
        wal_path: std::path::PathBuf::from("unused_wal.jsonl"),
    };
    let auth_config = AuthConfig {
        mode: AuthMode::Jwt,
        api_key_to_tenant: BTreeMap::new(),
        api_key_scopes: BTreeMap::new(),
        bearer_token_to_tenant: BTreeMap::new(),
        jwt: Some(JwtConfig::for_tests(TEST_JWT_SECRET)),
        rate_limit_per_minute: 0,
        rate_window_retention_minutes: 60,
        tenant_max_collections: 0,
        tenant_max_points: 0,
    };
    AppState::with_collections_and_auth(config, BTreeMap::new(), auth_config)
}

fn request_with_token(
    method: &str,
    uri: &str,
    bearer_token: Option<&str>,
    body: Option<serde_json::Value>,
) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = bearer_token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    match body {
        Some(body) => builder
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .expect("request must build"),
        None => builder.body(Body::empty()).expect("request must build"),
    }
}

#[derive(Serialize)]
struct TestClaims<'a> {
    sub: &'a str,
    tenant: &'a str,
    exp: u64,
}

fn jwt_for_tenant(tenant: &str, principal: &str, exp_offset_seconds: i64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock must be monotonic")
        .as_secs() as i64;
    let exp = now.saturating_add(exp_offset_seconds).max(0) as u64;
    let claims = TestClaims {
        sub: principal,
        tenant,
        exp,
    };
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(TEST_JWT_SECRET.as_bytes()),
    )
    .expect("jwt should be signed")
}

#[tokio::test]
async fn jwt_mode_requires_valid_bearer_token() {
    let app = build_app(jwt_state());

    let missing = app
        .clone()
        .oneshot(request_with_token("GET", "/collections", None, None))
        .await
        .expect("response expected");
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);

    let wrong_sig_claims = TestClaims {
        sub: "tenant_a_user",
        tenant: "tenant_a",
        exp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock must be monotonic")
            .as_secs()
            + 3600,
    };
    let wrong_sig_token = encode(
        &Header::new(Algorithm::HS256),
        &wrong_sig_claims,
        &EncodingKey::from_secret("wrong-secret".as_bytes()),
    )
    .expect("jwt should be signed");
    let invalid = app
        .clone()
        .oneshot(request_with_token(
            "GET",
            "/collections",
            Some(&wrong_sig_token),
            None,
        ))
        .await
        .expect("response expected");
    assert_eq!(invalid.status(), StatusCode::UNAUTHORIZED);

    let valid = app
        .clone()
        .oneshot(request_with_token(
            "GET",
            "/collections",
            Some(&jwt_for_tenant("tenant_a", "tenant_a_user", 3600)),
            None,
        ))
        .await
        .expect("response expected");
    assert_eq!(valid.status(), StatusCode::OK);
}

#[tokio::test]
async fn jwt_mode_rejects_expired_token() {
    let app = build_app(jwt_state());
    let response = app
        .oneshot(request_with_token(
            "GET",
            "/collections",
            Some(&jwt_for_tenant("tenant_a", "tenant_a_user", -120)),
            None,
        ))
        .await
        .expect("response expected");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn jwt_tenants_are_scoped_per_resource_name() {
    let app = build_app(jwt_state());
    let tenant_a = jwt_for_tenant("tenant_a", "alice", 3600);
    let tenant_b = jwt_for_tenant("tenant_b", "bob", 3600);

    for token in [&tenant_a, &tenant_b] {
        let response = app
            .clone()
            .oneshot(request_with_token(
                "POST",
                "/collections",
                Some(token),
                Some(json!({"name":"demo","dimension":3})),
            ))
            .await
            .expect("response expected");
        assert_eq!(response.status(), StatusCode::OK);
    }

    let upsert = app
        .clone()
        .oneshot(request_with_token(
            "PUT",
            "/collections/demo/points/1",
            Some(&tenant_a),
            Some(json!({"values":[1.0,2.0,3.0]})),
        ))
        .await
        .expect("response expected");
    assert_eq!(upsert.status(), StatusCode::OK);

    let get_a = app
        .clone()
        .oneshot(request_with_token(
            "GET",
            "/collections/demo/points/1",
            Some(&tenant_a),
            None,
        ))
        .await
        .expect("response expected");
    assert_eq!(get_a.status(), StatusCode::OK);

    let get_b = app
        .oneshot(request_with_token(
            "GET",
            "/collections/demo/points/1",
            Some(&tenant_b),
            None,
        ))
        .await
        .expect("response expected");
    assert_eq!(get_b.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn jwt_tenant_cannot_access_other_tenant_collection_routes() {
    let app = build_app(jwt_state());
    let tenant_a = jwt_for_tenant("tenant_a", "alice", 3600);
    let tenant_b = jwt_for_tenant("tenant_b", "bob", 3600);

    let create = app
        .clone()
        .oneshot(request_with_token(
            "POST",
            "/collections",
            Some(&tenant_a),
            Some(json!({"name":"private","dimension":3})),
        ))
        .await
        .expect("response expected");
    assert_eq!(create.status(), StatusCode::OK);

    let upsert = app
        .clone()
        .oneshot(request_with_token(
            "PUT",
            "/collections/private/points/1",
            Some(&tenant_a),
            Some(json!({"values":[1.0,2.0,3.0]})),
        ))
        .await
        .expect("response expected");
    assert_eq!(upsert.status(), StatusCode::OK);

    let list_points = app
        .clone()
        .oneshot(request_with_token(
            "GET",
            "/collections/private/points",
            Some(&tenant_b),
            None,
        ))
        .await
        .expect("response expected");
    assert_eq!(list_points.status(), StatusCode::NOT_FOUND);

    let search = app
        .clone()
        .oneshot(request_with_token(
            "POST",
            "/collections/private/search/topk",
            Some(&tenant_b),
            Some(json!({"query":[1.0,2.0,3.0],"metric":"l2","limit":1})),
        ))
        .await
        .expect("response expected");
    assert_eq!(search.status(), StatusCode::NOT_FOUND);

    let get = app
        .oneshot(request_with_token(
            "GET",
            "/collections/private",
            Some(&tenant_b),
            None,
        ))
        .await
        .expect("response expected");
    assert_eq!(get.status(), StatusCode::NOT_FOUND);
}
