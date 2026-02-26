use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt;

use crate::build_app;
use crate::config::AppConfig;
use crate::state::AppState;
mod auth_isolation;
mod auth_jwt;
mod auth_quotas;
mod checkpointing;
mod collection_deletion;
mod engine_guard;
mod filtering;
mod http_metrics;
mod index_build_cooldown;
mod index_cache;
mod limits;
mod limits_locking;
mod list_points;
mod list_points_offset;
mod metrics;
mod metrics_backlog;
mod metrics_prometheus;
#[cfg(feature = "exp_payload_mutation_api")]
mod payload_mutations;
mod persistence;
mod persistence_chaos;
mod persistence_degraded;
mod persistence_metrics;
mod persistence_rollbacks;
mod persistence_sync_mode;
mod points_batch;
#[cfg(feature = "exp_points_count")]
mod points_count;
mod runtime_maps_metrics;
mod search;
mod search_advanced;
mod search_recall;

fn test_state() -> AppState {
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

    AppState::with_collections(config, std::collections::BTreeMap::new())
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body must be readable");
    serde_json::from_slice(&bytes).expect("response body must be valid json")
}

#[tokio::test]
async fn collection_point_crud_flow_works() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "demo", "dimension": 3, "strict_finite": true}).to_string(),
        ))
        .expect("request must build");

    let create_resp = app
        .clone()
        .oneshot(create_req)
        .await
        .expect("response expected");
    assert_eq!(create_resp.status(), StatusCode::OK);

    let create_json = json_body(create_resp).await;
    assert_eq!(create_json["name"], "demo");
    assert_eq!(create_json["dimension"], 3);
    assert_eq!(create_json["strict_finite"], true);
    assert_eq!(create_json["point_count"], 0);

    let upsert_req = Request::builder()
        .method("PUT")
        .uri("/collections/demo/points/42")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"values": [1.0, 2.0, 3.0], "payload": {"tenant": "edge", "version": 2}})
                .to_string(),
        ))
        .expect("request must build");

    let upsert_resp = app
        .clone()
        .oneshot(upsert_req)
        .await
        .expect("response expected");
    assert_eq!(upsert_resp.status(), StatusCode::OK);

    let upsert_json = json_body(upsert_resp).await;
    assert_eq!(upsert_json["id"], 42);
    assert_eq!(upsert_json["created"], true);

    let get_collection_req = Request::builder()
        .method("GET")
        .uri("/collections/demo")
        .body(Body::empty())
        .expect("request must build");

    let get_collection_resp = app
        .clone()
        .oneshot(get_collection_req)
        .await
        .expect("response expected");
    assert_eq!(get_collection_resp.status(), StatusCode::OK);

    let collection_json = json_body(get_collection_resp).await;
    assert_eq!(collection_json["point_count"], 1);

    let get_point_req = Request::builder()
        .method("GET")
        .uri("/collections/demo/points/42")
        .body(Body::empty())
        .expect("request must build");

    let get_point_resp = app
        .clone()
        .oneshot(get_point_req)
        .await
        .expect("response expected");
    assert_eq!(get_point_resp.status(), StatusCode::OK);

    let point_json = json_body(get_point_resp).await;
    assert_eq!(point_json["id"], 42);
    assert_eq!(point_json["values"], json!([1.0, 2.0, 3.0]));
    assert_eq!(point_json["payload"]["tenant"], "edge");
    assert_eq!(point_json["payload"]["version"], 2);

    let delete_req = Request::builder()
        .method("DELETE")
        .uri("/collections/demo/points/42")
        .body(Body::empty())
        .expect("request must build");

    let delete_resp = app
        .clone()
        .oneshot(delete_req)
        .await
        .expect("response expected");
    assert_eq!(delete_resp.status(), StatusCode::OK);

    let delete_json = json_body(delete_resp).await;
    assert_eq!(delete_json["id"], 42);
    assert_eq!(delete_json["deleted"], true);
}

#[tokio::test]
async fn creating_existing_collection_returns_conflict() {
    let app = build_app(test_state());

    let body = json!({"name": "demo", "dimension": 3}).to_string();
    let first_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(body.clone()))
        .expect("request must build");

    let first_resp = app
        .clone()
        .oneshot(first_req)
        .await
        .expect("response expected");
    assert_eq!(first_resp.status(), StatusCode::OK);

    let second_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .expect("request must build");

    let second_resp = app
        .clone()
        .oneshot(second_req)
        .await
        .expect("response expected");
    assert_eq!(second_resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn upsert_point_rejects_dimension_mismatch() {
    let app = build_app(test_state());

    let create_req = Request::builder()
        .method("POST")
        .uri("/collections")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"name": "demo", "dimension": 3}).to_string(),
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
        .uri("/collections/demo/points/7")
        .header("content-type", "application/json")
        .body(Body::from(json!({"values": [1.0, 2.0]}).to_string()))
        .expect("request must build");

    let upsert_resp = app
        .clone()
        .oneshot(upsert_req)
        .await
        .expect("response expected");
    assert_eq!(upsert_resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn list_collections_returns_sorted_names() {
    let app = build_app(test_state());

    for name in ["zeta", "alpha"] {
        let create_req = Request::builder()
            .method("POST")
            .uri("/collections")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"name": name, "dimension": 3, "strict_finite": true}).to_string(),
            ))
            .expect("request must build");

        let create_resp = app
            .clone()
            .oneshot(create_req)
            .await
            .expect("response expected");
        assert_eq!(create_resp.status(), StatusCode::OK);
    }

    let list_req = Request::builder()
        .method("GET")
        .uri("/collections")
        .body(Body::empty())
        .expect("request must build");

    let list_resp = app
        .clone()
        .oneshot(list_req)
        .await
        .expect("response expected");
    assert_eq!(list_resp.status(), StatusCode::OK);

    let list_json = json_body(list_resp).await;
    assert_eq!(list_json["collections"][0]["name"], "alpha");
    assert_eq!(list_json["collections"][1]["name"], "zeta");
}
