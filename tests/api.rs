mod common;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use common::*;
use providarr::{
    api,
    config::{AppConfig, AuthConfig, ProviderConfig},
    state::AppState,
};
use tower::ServiceExt;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

async fn build_state(base_url: &str) -> (AppState, String) {
    let mut config = AppConfig::default();
    config.database.url = test_database_url().await;
    config.cache.enabled = true;
    config.replay.enabled = false;
    config.inbound.enabled = false;
    config.providers.clear();

    let mut provider = ProviderConfig::tmdb_default();
    provider.base_url = base_url.trim_end_matches('/').to_string();
    provider.auth = AuthConfig::None;
    provider.requests_per_second = 1000.0;
    provider.burst = 1000;

    let name = unique("api");
    config.providers.insert(name.clone(), provider);
    let state = AppState::build(config).await.expect("build app state");
    (state, name)
}

#[tokio::test]
async fn health_reports_ok_and_providers() {
    let (state, name) = build_state("http://127.0.0.1:1").await;
    let app = api::router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["status"], "ok");
    assert!(value["providers"][&name].is_boolean());
}

#[tokio::test]
async fn proxy_returns_cache_headers_and_ratelimit_report() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/movie/550"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_string("{\"id\":550}"),
        )
        .mount(&server)
        .await;

    let (state, name) = build_state(&server.uri()).await;
    let app = api::router(state);
    let uri = format!("/v1/{name}/movie/550");

    let first = app
        .clone()
        .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(
        first
            .headers()
            .get("x-providarr-cache")
            .unwrap()
            .to_str()
            .unwrap(),
        "miss"
    );

    let second = app
        .clone()
        .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(
        second
            .headers()
            .get("x-providarr-cache")
            .unwrap()
            .to_str()
            .unwrap(),
        "hit"
    );

    let report = app
        .oneshot(
            Request::builder()
                .uri("/v1/ratelimits")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(report.status(), StatusCode::OK);
    let bytes = to_bytes(report.into_body(), usize::MAX).await.unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let endpoints = value["endpoints"].as_array().unwrap();
    assert!(
        endpoints
            .iter()
            .any(|entry| entry["provider"] == name && entry["requests_total"] == 1),
        "ratelimit report should include the provider transaction"
    );
}

#[tokio::test]
async fn unknown_provider_is_404() {
    let (state, _) = build_state("http://127.0.0.1:1").await;
    let app = api::router(state);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/nope/movie/1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn sonarr_services_time_and_ping_are_local() {
    let (state, _) = build_state("http://127.0.0.1:1").await;
    let app = api::router(state);

    let time = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/sonarr/services/time")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(time.status(), StatusCode::OK);
    let bytes = to_bytes(time.into_body(), usize::MAX).await.unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(value["dateTimeUtc"].as_str().unwrap().contains('T'));

    let ping = app
        .oneshot(
            Request::builder()
                .uri("/sonarr/services/ping")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ping.status(), StatusCode::OK);
}

#[tokio::test]
async fn policy_discloses_cache_and_rate_limits() {
    let (state, name) = build_state("http://127.0.0.1:1").await;
    let app = api::router(state);

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

    assert!(value["cache"]["default_ttl_seconds"].as_u64().unwrap() > 0);
    assert!(
        value["rate_limits"]["providers"][&name]["requests_per_second"]
            .as_f64()
            .unwrap()
            > 0.0
    );
    assert!(value["rate_limits"]["inbound"]["burst"].as_u64().unwrap() > 0);
    assert!(
        value["disclosure"]["recommendation"]
            .as_str()
            .unwrap()
            .contains("Host your own")
    );
}
