mod common;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use common::*;
use providarr::{api, config::ApiAuthConfig};
use tower::ServiceExt;

async fn authed_app() -> axum::Router {
    let state = app_state(|config| {
        config.api_auth = ApiAuthConfig {
            enabled: true,
            api_key: "sekret".to_string(),
            header: "x-api-key".to_string(),
        };
    })
    .await;
    api::router(state)
}

#[tokio::test]
async fn rejects_missing_or_wrong_key_but_allows_health() {
    let app = authed_app().await;

    let unauth = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/ratelimits")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);

    let wrong = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/ratelimits")
                .header("x-api-key", "nope")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);

    let authed = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/ratelimits")
                .header("x-api-key", "sekret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(authed.status(), StatusCode::OK);

    // Liveness stays open for orchestrators.
    let health = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);
}

#[tokio::test]
async fn policy_is_public_and_only_discloses_bypass_count() {
    let state = app_state(|config| {
        config.api_auth = ApiAuthConfig {
            enabled: true,
            api_key: "sekret".to_string(),
            header: "x-api-key".to_string(),
        };
        config.inbound.bypass = vec!["127.0.0.1".to_string(), "10.0.0.0/8".to_string()];
    })
    .await;
    let app = api::router(state);

    // No API key: Theoriarr fetches this unauthenticated for its warning banner.
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/policy")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let inbound = &value["rate_limits"]["inbound"];
    assert!(
        inbound.get("bypass").is_none(),
        "bypass list must not be public"
    );
    assert_eq!(inbound["bypass_count"], 2);
}

#[tokio::test]
async fn rate_limit_history_endpoint_responds() {
    let app = api::router(app_state(|_| {}).await);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/ratelimits/history?limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}
