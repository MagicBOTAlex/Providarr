mod common;

use std::time::Duration;

use common::*;
use providarr::{config::BackoffConfig, error::AppError};
use sqlx::Row;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[tokio::test]
async fn caches_upstream_and_counts_a_single_transaction() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/movie/550"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_string("{\"id\":550}"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let t = registry_with_provider(&server.uri(), true).await;

    let first = t
        .registry
        .fetch(&t.provider_name, "GET", "/movie/550?language=en")
        .await
        .unwrap();
    assert_eq!(first.status, 200);
    assert!(!first.cached);
    assert_eq!(&first.body[..], b"{\"id\":550}");

    let second = t
        .registry
        .fetch(&t.provider_name, "GET", "/movie/550?language=en")
        .await
        .unwrap();
    assert!(second.cached, "second call must be served from cache");
    assert!(!second.stale);

    let snaps = t.stats.snapshot();
    let snap = snapshot_for(&snaps, &t.provider_name);
    assert_eq!(snap.endpoint, "movie/{id}");
    assert_eq!(snap.requests_total, 1, "only one upstream transaction");
    assert_eq!(snap.requests_since_limit, 1);
    assert_eq!(snap.successes, 1);
    // wiremock's expect(1) is verified when the server drops.
}

#[tokio::test]
async fn caches_a_404_response() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/movie/999"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;

    let t = registry_with_provider(&server.uri(), true).await;

    let first = t
        .registry
        .fetch(&t.provider_name, "GET", "/movie/999")
        .await
        .unwrap();
    assert_eq!(first.status, 404);
    assert!(!first.cached);

    let second = t
        .registry
        .fetch(&t.provider_name, "GET", "/movie/999")
        .await
        .unwrap();
    assert_eq!(second.status, 404);
    assert!(second.cached, "a 404 must be served from cache");

    let snaps = t.stats.snapshot();
    let snap = snapshot_for(&snaps, &t.provider_name);
    assert_eq!(
        snap.requests_total, 1,
        "only one upstream transaction for a cached 404"
    );
}

#[tokio::test]
async fn records_live_responses_for_replay() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/movie/1"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_string("{\"id\":1}"),
        )
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let record_dir = dir.path().to_path_buf();

    let t = registry_custom(&server.uri(), true, None, move |config| {
        config.replay.record = true;
        config.replay.dir = record_dir.to_string_lossy().into_owned();
    })
    .await;

    let response = t
        .registry
        .fetch(&t.provider_name, "GET", "/movie/1")
        .await
        .unwrap();
    assert_eq!(response.status, 200);

    // 2xx responses are also cached; drop the cache lookup so the recorded disk
    // copy is what a fresh (offline) store replays.
    let recorded = std::fs::read_dir(dir.path().join("recorded"))
        .expect("recorded dir created")
        .count();
    assert_eq!(recorded, 1, "one recorded response file");

    let store = providarr::replay::ReplayStore::load(dir.path(), false);
    let hit = store
        .upstream(&t.provider_name, "GET", "/movie/1?language=en")
        .expect("recorded response replays");
    assert_eq!(hit.status, 200);
    assert_eq!(&hit.body[..], b"{\"id\":1}");
}

#[tokio::test]
async fn rate_limit_records_how_many_transactions_were_sent() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/movie/1"))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "1"))
        .mount(&server)
        .await;

    let t = registry_with_provider(&server.uri(), false).await;

    let err = t
        .registry
        .fetch(&t.provider_name, "GET", "/movie/1")
        .await
        .unwrap_err();
    match &err {
        AppError::RateLimited { retry_after_ms, .. } => {
            assert!(
                *retry_after_ms >= 800,
                "upstream Retry-After must be honoured (got {retry_after_ms}ms)"
            );
        }
        other => panic!("expected RateLimited, got {other:?}"),
    }

    let snaps = t.stats.snapshot();
    let snap = snapshot_for(&snaps, &t.provider_name);
    assert_eq!(snap.rate_limit_hits, 1);
    assert_eq!(snap.observed_limit_threshold, Some(1));
    assert_eq!(snap.requests_since_limit, 0, "resets after a limit");
    assert_eq!(snap.last_status, Some(429));

    let row =
        sqlx::query("SELECT requests_since_last_limit FROM rate_limit_events WHERE provider = $1")
            .bind(&t.provider_name)
            .fetch_one(&t.pool)
            .await
            .unwrap();
    assert_eq!(row.get::<i64, _>("requests_since_last_limit"), 1);
}

#[tokio::test]
async fn progressive_backoff_drops_requests_beyond_budget() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/movie/9"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;

    let backoff = BackoffConfig {
        base_delay: Duration::from_secs(60),
        factor: 2.0,
        max_delay: Duration::from_secs(300),
        jitter: 0.0,
        max_consecutive_failures: 8,
        drop_after_wait: Duration::from_millis(50),
        max_retries: 0,
        max_request_timeout: Duration::from_secs(120),
    };
    let t = registry_custom(&server.uri(), false, Some(backoff), |_| {}).await;

    // First failure pushes the provider into a 60s backoff.
    let first = t.registry.fetch(&t.provider_name, "GET", "/movie/9").await;
    assert!(first.is_err());

    // The next request cannot wait 60s and must be dropped quickly.
    let start = std::time::Instant::now();
    let err = t
        .registry
        .fetch(&t.provider_name, "GET", "/movie/9")
        .await
        .unwrap_err();
    assert!(matches!(err, AppError::Dropped(_)), "got {err:?}");
    assert!(start.elapsed() < Duration::from_secs(5));

    let snaps = t.stats.snapshot();
    let snap = snapshot_for(&snaps, &t.provider_name);
    assert_eq!(snap.dropped, 1);
    assert_eq!(snap.failures, 1);
    assert_eq!(
        snap.requests_total, 1,
        "an attempt dropped by the limiter must not count as a transaction"
    );
}

#[tokio::test]
async fn coalesces_concurrent_identical_requests() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/movie/42"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(300))
                .set_body_string("{\"id\":42}"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let t = registry_with_provider(&server.uri(), true).await;

    let (first, second) = tokio::join!(
        t.registry.fetch(&t.provider_name, "GET", "/movie/42"),
        t.registry.fetch(&t.provider_name, "GET", "/movie/42"),
    );

    assert!(first.is_ok() && second.is_ok());
    // `expect(1)` verifies only a single upstream call despite two concurrent requests.
}

#[tokio::test]
async fn serves_stale_and_revalidates_in_background() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/movie/77"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("cache-control", "max-age=1")
                .set_body_string("{\"v\":1}"),
        )
        .expect(2)
        .mount(&server)
        .await;

    let t = registry_with_provider(&server.uri(), true).await;

    let first = t
        .registry
        .fetch(&t.provider_name, "GET", "/movie/77")
        .await
        .unwrap();
    assert!(!first.stale);

    // Expire the entry (TTL 1s) but stay within the stale window.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let stale = t
        .registry
        .fetch(&t.provider_name, "GET", "/movie/77")
        .await
        .unwrap();
    assert!(stale.stale, "expired entry should be served stale");
    assert_eq!(&stale.body[..], b"{\"v\":1}");

    // Give the background revalidation time to hit upstream; `.expect(2)` verifies it.
    tokio::time::sleep(Duration::from_millis(800)).await;
}

#[tokio::test]
async fn serves_stale_on_upstream_error_when_available() {
    let server = MockServer::start().await;

    // First: a healthy response with a 1ms TTL so it expires quickly.
    Mock::given(method("GET"))
        .and(path("/movie/7"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("cache-control", "max-age=1")
                .set_body_string("{\"id\":7}"),
        )
        .mount(&server)
        .await;

    let t = registry_with_provider(&server.uri(), true).await;
    let fresh = t
        .registry
        .fetch(&t.provider_name, "GET", "/movie/7")
        .await
        .unwrap();
    assert_eq!(fresh.status, 200);

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Replace the mock with a 503 so the stale entry must be served.
    server.reset().await;
    Mock::given(method("GET"))
        .and(path("/movie/7"))
        .respond_with(ResponseTemplate::new(503).set_body_string("down"))
        .mount(&server)
        .await;

    let stale = t
        .registry
        .fetch(&t.provider_name, "GET", "/movie/7")
        .await
        .unwrap();
    assert_eq!(stale.status, 200);
    assert!(stale.stale, "must be marked stale");
    assert_eq!(&stale.body[..], b"{\"id\":7}");
}

#[tokio::test]
async fn retries_transient_upstream_failures_then_succeeds() {
    let server = MockServer::start().await;

    // Fail twice, then succeed. The retry loop should ride through both 503s.
    Mock::given(method("GET"))
        .and(path("/movie/retry"))
        .respond_with(ResponseTemplate::new(503).set_body_string("down"))
        .up_to_n_times(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/movie/retry"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{\"ok\":true}"))
        .mount(&server)
        .await;

    let backoff = BackoffConfig {
        base_delay: Duration::from_millis(1),
        factor: 2.0,
        max_delay: Duration::from_millis(50),
        jitter: 0.0,
        max_consecutive_failures: 8,
        drop_after_wait: Duration::from_secs(10),
        max_retries: 3,
        max_request_timeout: Duration::from_secs(120),
    };
    let t = registry_custom(&server.uri(), false, Some(backoff), |_| {}).await;

    let response = t
        .registry
        .fetch(&t.provider_name, "GET", "/movie/retry")
        .await
        .expect("retry should eventually succeed");
    assert_eq!(response.status, 200);
    assert_eq!(&response.body[..], b"{\"ok\":true}");
}

#[tokio::test]
async fn stale_burst_spawns_a_single_revalidation() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/movie/88"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("cache-control", "max-age=1")
                .set_delay(Duration::from_millis(400))
                .set_body_string("{\"v\":1}"),
        )
        .expect(2)
        .mount(&server)
        .await;

    let t = registry_with_provider(&server.uri(), true).await;

    let first = t
        .registry
        .fetch(&t.provider_name, "GET", "/movie/88")
        .await
        .unwrap();
    assert!(!first.stale);

    tokio::time::sleep(Duration::from_secs(2)).await;

    // A burst of concurrent stale requests must spawn at most ONE background
    // refresh; `.expect(2)` (initial + single refresh) verifies it.
    let mut set = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let registry = t.registry.clone();
        let name = t.provider_name.clone();
        set.spawn(async move { registry.fetch(&name, "GET", "/movie/88").await });
    }
    while let Some(joined) = set.join_next().await {
        let response = joined.unwrap().unwrap();
        assert_eq!(response.status, 200);
        // Late observers may see the completed refresh instead of the stale
        // value; the single-flight guarantee is verified by `.expect(2)`.
    }

    tokio::time::sleep(Duration::from_millis(1_200)).await;
}

#[tokio::test]
async fn no_store_invalidates_an_existing_entry() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/movie/99"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("cache-control", "max-age=1")
                .set_body_string("{\"v\":1}"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let t = registry_custom(&server.uri(), true, None, |config| {
        config.cache.stale_while_revalidate = false;
    })
    .await;

    let first = t
        .registry
        .fetch(&t.provider_name, "GET", "/movie/99")
        .await
        .unwrap();
    assert_eq!(&first.body[..], b"{\"v\":1}");

    tokio::time::sleep(Duration::from_secs(2)).await;

    server.reset().await;
    Mock::given(method("GET"))
        .and(path("/movie/99"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("cache-control", "no-store")
                .set_body_string("{\"v\":2}"),
        )
        .expect(2)
        .mount(&server)
        .await;

    // The expired entry is refreshed upstream, which answers no-store: the old
    // row must be dropped, so the following call misses the cache too.
    let second = t
        .registry
        .fetch(&t.provider_name, "GET", "/movie/99")
        .await
        .unwrap();
    assert_eq!(&second.body[..], b"{\"v\":2}");

    let third = t
        .registry
        .fetch(&t.provider_name, "GET", "/movie/99")
        .await
        .unwrap();
    assert_eq!(&third.body[..], b"{\"v\":2}");
    assert!(!third.cached, "no-store must have invalidated the row");
}
