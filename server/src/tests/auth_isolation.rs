use std::collections::BTreeMap;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

#[cfg(feature = "exp_auth_api_key_scopes")]
use crate::auth::AccessScope;
use crate::auth::{AuthConfig, AuthMode};
use crate::build_app;
use crate::config::AppConfig;
use crate::state::{AppState, TenantRateWindow};

fn auth_state() -> AppState {
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
        mode: AuthMode::ApiKey,
        api_key_to_tenant: BTreeMap::from([
            ("key-a".to_string(), "tenant_a".to_string()),
            ("key-b".to_string(), "tenant_b".to_string()),
        ]),
        #[cfg(feature = "exp_auth_api_key_scopes")]
        api_key_scopes: BTreeMap::new(),
        bearer_token_to_tenant: BTreeMap::new(),
        jwt: None,
        rate_limit_per_minute: 0,
        rate_window_retention_minutes: 60,
        tenant_max_collections: 0,
        tenant_max_points: 0,
    };
    AppState::with_collections_and_auth(config, BTreeMap::new(), auth_config)
}

fn request_with_api_key(
    method: &str,
    uri: &str,
    api_key: Option<&str>,
    body: Option<serde_json::Value>,
) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(api_key) = api_key {
        builder = builder.header("x-api-key", api_key);
    }
    match body {
        Some(body) => builder
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .expect("request must build"),
        None => builder.body(Body::empty()).expect("request must build"),
    }
}

#[tokio::test]
async fn auth_mode_requires_credentials() {
    let app = build_app(auth_state());

    let response = app
        .clone()
        .oneshot(request_with_api_key("GET", "/collections", None, None))
        .await
        .expect("response expected");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[cfg(feature = "exp_auth_api_key_scopes")]
#[tokio::test]
async fn read_only_api_key_scope_blocks_write_routes() {
    let mut state = auth_state();
    let mut auth_config = (*state.auth_config).clone();
    auth_config.api_key_to_tenant = BTreeMap::from([
        ("key-read".to_string(), "tenant_a".to_string()),
        ("key-write".to_string(), "tenant_a".to_string()),
    ]);
    auth_config.api_key_scopes = BTreeMap::from([
        ("key-read".to_string(), AccessScope::Read),
        ("key-write".to_string(), AccessScope::Write),
    ]);
    state = AppState::with_collections_and_auth(
        (*state.config).clone(),
        std::collections::BTreeMap::new(),
        auth_config,
    );
    let app = build_app(state);

    let create = app
        .clone()
        .oneshot(request_with_api_key(
            "POST",
            "/collections",
            Some("key-write"),
            Some(json!({"name":"scoped","dimension":3})),
        ))
        .await
        .expect("response expected");
    assert_eq!(create.status(), StatusCode::OK);

    let seed = app
        .clone()
        .oneshot(request_with_api_key(
            "PUT",
            "/collections/scoped/points/1",
            Some("key-write"),
            Some(json!({"values":[1.0,2.0,3.0]})),
        ))
        .await
        .expect("response expected");
    assert_eq!(seed.status(), StatusCode::OK);

    let read_ok = app
        .clone()
        .oneshot(request_with_api_key(
            "GET",
            "/collections/scoped/points/1",
            Some("key-read"),
            None,
        ))
        .await
        .expect("response expected");
    assert_eq!(read_ok.status(), StatusCode::OK);

    let write_rejected = app
        .clone()
        .oneshot(request_with_api_key(
            "PUT",
            "/collections/scoped/points/1",
            Some("key-read"),
            Some(json!({"values":[9.0,9.0,9.0]})),
        ))
        .await
        .expect("response expected");
    assert_eq!(write_rejected.status(), StatusCode::FORBIDDEN);
    let write_rejected_json = super::json_body(write_rejected).await;
    assert_eq!(write_rejected_json["code"], "forbidden");

    let create_rejected = app
        .clone()
        .oneshot(request_with_api_key(
            "POST",
            "/collections",
            Some("key-read"),
            Some(json!({"name":"blocked","dimension":3})),
        ))
        .await
        .expect("response expected");
    assert_eq!(create_rejected.status(), StatusCode::FORBIDDEN);

    let list = app
        .oneshot(request_with_api_key(
            "GET",
            "/collections",
            Some("key-read"),
            None,
        ))
        .await
        .expect("response expected");
    assert_eq!(list.status(), StatusCode::OK);
}

#[cfg(not(feature = "exp_auth_api_key_scopes"))]
#[tokio::test]
async fn api_key_scope_configuration_is_ignored_when_feature_is_disabled() {
    let mut state = auth_state();
    let mut auth_config = (*state.auth_config).clone();
    auth_config.api_key_to_tenant =
        BTreeMap::from([("key-read".to_string(), "tenant_a".to_string())]);
    state = AppState::with_collections_and_auth(
        (*state.config).clone(),
        std::collections::BTreeMap::new(),
        auth_config,
    );
    let app = build_app(state);

    let create = app
        .clone()
        .oneshot(request_with_api_key(
            "POST",
            "/collections",
            Some("key-read"),
            Some(json!({"name":"scoped","dimension":3})),
        ))
        .await
        .expect("response expected");
    assert_eq!(create.status(), StatusCode::OK);

    let upsert = app
        .oneshot(request_with_api_key(
            "PUT",
            "/collections/scoped/points/1",
            Some("key-read"),
            Some(json!({"values":[1.0,2.0,3.0]})),
        ))
        .await
        .expect("response expected");
    assert_eq!(upsert.status(), StatusCode::OK);
}

#[tokio::test]
async fn tenants_are_scoped_per_resource_name() {
    let state = auth_state();
    let inspect_state = state.clone();
    let app = build_app(state);

    for (key, expected_status) in [("key-a", StatusCode::OK), ("key-b", StatusCode::OK)] {
        let response = app
            .clone()
            .oneshot(request_with_api_key(
                "POST",
                "/collections",
                Some(key),
                Some(json!({"name":"demo","dimension":3})),
            ))
            .await
            .expect("response expected");
        assert_eq!(response.status(), expected_status);
    }

    let upsert = app
        .clone()
        .oneshot(request_with_api_key(
            "PUT",
            "/collections/demo/points/7",
            Some("key-a"),
            Some(json!({"values":[1.0,2.0,3.0]})),
        ))
        .await
        .expect("response expected");
    assert_eq!(upsert.status(), StatusCode::OK);

    let get_a = app
        .clone()
        .oneshot(request_with_api_key(
            "GET",
            "/collections/demo/points/7",
            Some("key-a"),
            None,
        ))
        .await
        .expect("response expected");
    assert_eq!(get_a.status(), StatusCode::OK);

    let get_b = app
        .clone()
        .oneshot(request_with_api_key(
            "GET",
            "/collections/demo/points/7",
            Some("key-b"),
            None,
        ))
        .await
        .expect("response expected");
    assert_eq!(get_b.status(), StatusCode::NOT_FOUND);

    let list_a = app
        .clone()
        .oneshot(request_with_api_key(
            "GET",
            "/collections",
            Some("key-a"),
            None,
        ))
        .await
        .expect("response expected");
    assert_eq!(list_a.status(), StatusCode::OK);
    let list_a_json = super::json_body(list_a).await;
    assert_eq!(list_a_json["collections"][0]["name"], "demo");
    assert_eq!(list_a_json["collections"][0]["point_count"], 1);

    let list_b = app
        .clone()
        .oneshot(request_with_api_key(
            "GET",
            "/collections",
            Some("key-b"),
            None,
        ))
        .await
        .expect("response expected");
    assert_eq!(list_b.status(), StatusCode::OK);
    let list_b_json = super::json_body(list_b).await;
    assert_eq!(list_b_json["collections"][0]["name"], "demo");
    assert_eq!(list_b_json["collections"][0]["point_count"], 0);

    let registry = inspect_state
        .collections
        .read()
        .expect("collection registry lock must be readable");
    assert!(registry.contains_key("tenant_a::demo"));
    assert!(registry.contains_key("tenant_b::demo"));
    assert!(!registry.contains_key("demo"));
}

#[tokio::test]
async fn tenant_cannot_access_other_tenant_collection_routes() {
    let app = build_app(auth_state());

    let create = app
        .clone()
        .oneshot(request_with_api_key(
            "POST",
            "/collections",
            Some("key-a"),
            Some(json!({"name":"private","dimension":3})),
        ))
        .await
        .expect("response expected");
    assert_eq!(create.status(), StatusCode::OK);

    let upsert = app
        .clone()
        .oneshot(request_with_api_key(
            "PUT",
            "/collections/private/points/1",
            Some("key-a"),
            Some(json!({"values":[1.0,2.0,3.0]})),
        ))
        .await
        .expect("response expected");
    assert_eq!(upsert.status(), StatusCode::OK);

    let list_points = app
        .clone()
        .oneshot(request_with_api_key(
            "GET",
            "/collections/private/points",
            Some("key-b"),
            None,
        ))
        .await
        .expect("response expected");
    assert_eq!(list_points.status(), StatusCode::NOT_FOUND);

    let search = app
        .clone()
        .oneshot(request_with_api_key(
            "POST",
            "/collections/private/search/topk",
            Some("key-b"),
            Some(json!({"query":[1.0,2.0,3.0],"metric":"l2","limit":1})),
        ))
        .await
        .expect("response expected");
    assert_eq!(search.status(), StatusCode::NOT_FOUND);

    let get = app
        .oneshot(request_with_api_key(
            "GET",
            "/collections/private",
            Some("key-b"),
            None,
        ))
        .await
        .expect("response expected");
    assert_eq!(get.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn rate_limit_windows_are_pruned() {
    let mut state = auth_state();
    let mut auth_config = (*state.auth_config).clone();
    auth_config.rate_limit_per_minute = 10;
    state = AppState::with_collections_and_auth(
        (*state.config).clone(),
        std::collections::BTreeMap::new(),
        auth_config,
    );

    state.tenant_rate_windows.insert(
        "stale".to_string(),
        TenantRateWindow {
            minute: 0,
            count: 1,
        },
    );

    let app = build_app(state.clone());
    let response = app
        .oneshot(request_with_api_key(
            "GET",
            "/collections",
            Some("key-a"),
            None,
        ))
        .await
        .expect("response expected");
    assert_eq!(response.status(), StatusCode::OK);

    assert!(!state.tenant_rate_windows.contains_key("stale"));
    assert!(state.tenant_rate_windows.contains_key("tenant_a"));
}
