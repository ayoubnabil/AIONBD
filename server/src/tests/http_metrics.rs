use std::sync::atomic::Ordering;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use crate::build_app;
use crate::config::AppConfig;
use crate::state::AppState;

fn test_state() -> AppState {
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

    AppState::with_collections(config, std::collections::BTreeMap::new())
}

#[tokio::test]
async fn middleware_tracks_total_and_in_flight_requests() {
    let state = test_state();
    let app = build_app(state.clone());

    assert_eq!(state.metrics.http_requests_total.load(Ordering::Relaxed), 0);
    assert_eq!(
        state
            .metrics
            .http_requests_in_flight
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        state
            .metrics
            .http_responses_2xx_total
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        state
            .metrics
            .http_responses_4xx_total
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        state
            .metrics
            .http_requests_5xx_total
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        state
            .metrics
            .http_request_duration_us_total
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        state
            .metrics
            .http_request_duration_us_max
            .load(Ordering::Relaxed),
        0
    );

    for uri in ["/live", "/ready", "/missing"] {
        let req = Request::builder()
            .method("GET")
            .uri(uri)
            .body(Body::empty())
            .expect("request must build");
        let resp = app.clone().oneshot(req).await.expect("response expected");
        if uri == "/missing" {
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        } else {
            assert_eq!(resp.status(), StatusCode::OK);
        }
    }

    assert_eq!(state.metrics.http_requests_total.load(Ordering::Relaxed), 3);
    assert_eq!(
        state
            .metrics
            .http_requests_in_flight
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        state
            .metrics
            .http_responses_2xx_total
            .load(Ordering::Relaxed),
        2
    );
    assert_eq!(
        state
            .metrics
            .http_responses_4xx_total
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        state
            .metrics
            .http_requests_5xx_total
            .load(Ordering::Relaxed),
        0
    );
    assert!(
        state
            .metrics
            .http_request_duration_us_total
            .load(Ordering::Relaxed)
            >= state
                .metrics
                .http_request_duration_us_max
                .load(Ordering::Relaxed)
    );
}

#[tokio::test]
async fn middleware_tracks_5xx_for_internal_errors() {
    let state = test_state();
    let collections = state.collections.clone();

    let poison_result = std::thread::spawn(move || {
        let _guard = collections
            .write()
            .expect("collection registry lock should be writable");
        panic!("poison collection lock")
    })
    .join();
    assert!(poison_result.is_err());

    let app = build_app(state.clone());
    let req = Request::builder()
        .method("GET")
        .uri("/collections")
        .body(Body::empty())
        .expect("request must build");
    let resp = app.oneshot(req).await.expect("response expected");

    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(state.metrics.http_requests_total.load(Ordering::Relaxed), 1);
    assert_eq!(
        state
            .metrics
            .http_requests_in_flight
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        state
            .metrics
            .http_responses_2xx_total
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        state
            .metrics
            .http_responses_4xx_total
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        state
            .metrics
            .http_requests_5xx_total
            .load(Ordering::Relaxed),
        1
    );
    assert!(
        state
            .metrics
            .http_request_duration_us_total
            .load(Ordering::Relaxed)
            >= state
                .metrics
                .http_request_duration_us_max
                .load(Ordering::Relaxed)
    );
}
