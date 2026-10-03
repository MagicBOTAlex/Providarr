mod common;

use std::net::{IpAddr, SocketAddr};

use axum::{
    body::Body,
    extract::ConnectInfo,
    http::{Request, StatusCode},
};
use common::*;
use providarr::{api, config::InboundLimiterConfig};
use tower::ServiceExt;

fn limiter_config() -> InboundLimiterConfig {
    InboundLimiterConfig {
        enabled: true,
        requests_per_second: 1.0,
        burst: 2,
        global_requests_per_second: 0.0,
        global_burst: 1,
        max_concurrent: 0,
        // Trust the TCP peer, never the client-supplied header.
        trust_forwarded_for: false,
        bypass: Vec::new(),
    }
}

async fn app() -> axum::Router {
    let state = app_state(|config| {
        config.inbound = limiter_config();
    })
    .await;
    api::router(state)
}

/// Builds a request whose TCP peer is `ip`, as a real connection would.
fn request_from(ip: &str) -> Request<Body> {
    request_uri_from(ip, "/health")
}

fn request_uri_from(ip: &str, uri: &str) -> Request<Body> {
    let addr = SocketAddr::new(ip.parse::<IpAddr>().unwrap(), 40_000);
    let mut request = Request::builder().uri(uri).body(Body::empty()).unwrap();
    request.extensions_mut().insert(ConnectInfo(addr));
    request
}

fn with_xff(mut request: Request<Body>, value: &str) -> Request<Body> {
    request
        .headers_mut()
        .insert("x-forwarded-for", value.parse().unwrap());
    request
}

#[tokio::test]
async fn rejects_after_burst_per_ip() {
    let app = app().await;

    for _ in 0..2 {
        let response = app
            .clone()
            .oneshot(request_from("203.0.113.5"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    let limited = app
        .clone()
        .oneshot(request_from("203.0.113.5"))
        .await
        .unwrap();
    assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(limited.headers().contains_key("retry-after"));

    // A different client IP is unaffected.
    let other = app.oneshot(request_from("203.0.113.6")).await.unwrap();
    assert_eq!(other.status(), StatusCode::OK);
}

#[tokio::test]
async fn ignores_spoofed_forwarded_for_when_peer_is_untrusted() {
    let app = app().await;

    // Rotating X-Forwarded-For must not mint new per-IP buckets; the peer is key.
    for i in 0..2 {
        let response = app
            .clone()
            .oneshot(with_xff(request_from("192.0.2.10"), &format!("10.0.0.{i}")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    let limited = app
        .oneshot(with_xff(request_from("192.0.2.10"), "10.0.0.99"))
        .await
        .unwrap();
    assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn aggregates_ipv6_peers_by_slash_64() {
    let app = app().await;

    for ip in ["2001:db8:1:2::1", "2001:db8:1:2::2"] {
        let response = app.clone().oneshot(request_from(ip)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    // A different interface identifier in the same /64 shares the bucket.
    let limited = app
        .clone()
        .oneshot(request_from("2001:db8:1:2::3"))
        .await
        .unwrap();
    assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);

    // A different /64 is independent.
    let other = app.oneshot(request_from("2001:db8:1:3::1")).await.unwrap();
    assert_eq!(other.status(), StatusCode::OK);
}

#[tokio::test]
async fn bypasses_configured_ip_and_cidr() {
    let state = app_state(|config| {
        let mut inbound = limiter_config();
        inbound.bypass = vec!["198.51.100.0/24".to_string(), "203.0.113.9".to_string()];
        config.inbound = inbound;
    })
    .await;
    let app = api::router(state);

    // Way past the burst, but bypassed by CIDR / host rule.
    for _ in 0..10 {
        let response = app
            .clone()
            .oneshot(request_from("198.51.100.42"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    for _ in 0..10 {
        let response = app
            .clone()
            .oneshot(request_from("203.0.113.9"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    // An address outside the bypass ranges is still limited.
    for _ in 0..2 {
        let response = app
            .clone()
            .oneshot(request_from("203.0.113.10"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    let limited = app.oneshot(request_from("203.0.113.10")).await.unwrap();
    assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn rejects_globally_across_distinct_ips() {
    let state = app_state(|config| {
        let mut inbound = limiter_config();
        // Per-IP buckets are effectively unlimited; only the global cap applies.
        inbound.requests_per_second = 1000.0;
        inbound.burst = 1000;
        inbound.global_requests_per_second = 1.0;
        inbound.global_burst = 2;
        config.inbound = inbound;
    })
    .await;
    let app = api::router(state);

    for i in 1..=2 {
        let response = app
            .clone()
            .oneshot(request_from(&format!("203.0.113.{i}")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    // A brand-new IP is still rejected once the server-wide budget is spent.
    let limited = app.oneshot(request_from("203.0.113.3")).await.unwrap();
    assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(limited.headers().contains_key("retry-after"));
}

#[tokio::test]
async fn exposes_inbound_allowed_and_limited_metrics() {
    let state = app_state(|config| {
        let mut inbound = limiter_config();
        inbound.bypass = vec!["198.51.100.0/24".to_string()];
        config.inbound = inbound;
    })
    .await;
    let app = api::router(state);

    // Two allowed, then one rejected, all from the same client IP.
    for _ in 0..2 {
        let response = app
            .clone()
            .oneshot(request_from("198.18.0.1"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    let limited = app
        .clone()
        .oneshot(request_from("198.18.0.1"))
        .await
        .unwrap();
    assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);

    // Deterministically populate the bypassed counter for this test process.
    let bypassed = app
        .clone()
        .oneshot(request_from("198.51.100.7"))
        .await
        .unwrap();
    assert_eq!(bypassed.status(), StatusCode::OK);

    // Query metrics from a fresh IP so the scrape itself is not throttled.
    let metrics = app
        .oneshot(request_uri_from("198.18.9.9", "/metrics"))
        .await
        .unwrap();

    assert_eq!(metrics.status(), StatusCode::OK);

    let body = axum::body::to_bytes(metrics.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();

    assert!(text.contains("providarr_inbound_allowed_total"));
    assert!(text.contains("providarr_inbound_limited_total"));
    assert!(text.contains("providarr_inbound_bypassed_total"));
}

#[tokio::test]
async fn sonarr_search_accepts_trailing_slash() {
    let app = app().await;

    // The handler may fail upstream (replay off), but the route must exist: a
    // missing route would hit the fallback and return 404.
    let response = app
        .oneshot(
            Request::builder()
                .uri("/sonarr/v1/tvdb/search/eng/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(response.status(), StatusCode::NOT_FOUND);
}
