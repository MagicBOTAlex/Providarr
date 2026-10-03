mod common;

use std::time::Duration;

use common::*;
use providarr::cache::CacheStore;

/// Hit accounting is applied by a detached write, so tests must wait for it to
/// settle rather than reading the counter immediately after `get`.
async fn wait_for_hits(store: &CacheStore, key: &str, expected: i64) {
    let mut last = i64::MIN;
    for _ in 0..250 {
        let row = sqlx::query("SELECT hits FROM cache_entries WHERE cache_key = $1")
            .bind(key)
            .fetch_one(store.pool())
            .await
            .expect("read hits");
        last = sqlx::Row::get::<i64, _>(&row, "hits");
        if last == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("hits did not settle to {expected} (last = {last})");
}

#[tokio::test]
async fn cache_put_get_and_hits() {
    let pool = test_pool().await;
    let name = unique("cache");
    let store = CacheStore::new(pool, providarr::config::CacheConfig::default());

    let key = CacheStore::key(&name, "GET", "/movie/1?language=en");
    assert!(store.get(&key).await.unwrap().is_none(), "starts empty");

    store
        .put(
            &key,
            &name,
            "movie/{id}",
            "/movie/1?language=en",
            200,
            Some("application/json"),
            b"{\"id\":1}",
            &serde_json::json!({ "etag": "abc" }),
            Duration::from_secs(60),
        )
        .await
        .unwrap();

    let entry = store.get(&key).await.unwrap().expect("entry present");
    assert_eq!(entry.status_code, 200);
    assert_eq!(entry.content_type.as_deref(), Some("application/json"));
    assert_eq!(&entry.body[..], b"{\"id\":1}");
    assert!(entry.is_fresh(chrono::Utc::now()));

    // second read bumps hit counter (applied asynchronously)
    let _ = store.get(&key).await.unwrap().unwrap();
    wait_for_hits(&store, &key, 2).await;

    let stats = store.stats().await.unwrap();
    assert!(stats.entries >= 1);
    assert!(stats.total_hits >= 2, "total_hits: {}", stats.total_hits);
    assert!(
        stats.approx_bytes >= 7,
        "approx_bytes: {}",
        stats.approx_bytes
    );
}

#[tokio::test]
async fn expired_entries_are_not_fresh_but_in_stale_window() {
    let pool = test_pool().await;
    let name = unique("cache");
    let config = providarr::config::CacheConfig::default();
    let store = CacheStore::new(pool, config.clone());

    let key = CacheStore::key(&name, "GET", "/tv/1");
    store
        .put(
            &key,
            &name,
            "tv/{id}",
            "/tv/1",
            200,
            Some("application/json"),
            b"{}",
            &serde_json::json!({}),
            Duration::from_millis(1),
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;
    let entry = store.get(&key).await.unwrap().unwrap();
    let now = chrono::Utc::now();
    assert!(!entry.is_fresh(now), "expired entry should not be fresh");
    assert!(
        entry.is_within_stale_window(now, &config),
        "expired entry still usable as stale"
    );
}

#[tokio::test]
async fn hits_accumulate_across_refresh() {
    let pool = test_pool().await;
    let name = unique("cache");
    let store = CacheStore::new(pool, providarr::config::CacheConfig::default());
    let key = CacheStore::key(&name, "GET", "/movie/1");
    let headers = serde_json::json!({});

    store
        .put(
            &key,
            &name,
            "movie/{id}",
            "/movie/1",
            200,
            Some("application/json"),
            b"{}",
            &headers,
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    let _ = store.get(&key).await.unwrap().unwrap();
    store
        .put(
            &key,
            &name,
            "movie/{id}",
            "/movie/1",
            200,
            Some("application/json"),
            b"{}",
            &headers,
            Duration::from_secs(60),
        )
        .await
        .unwrap(); // refresh while still valid
    let _ = store.get(&key).await.unwrap().unwrap();

    wait_for_hits(&store, &key, 2).await;
}

#[tokio::test]
async fn negative_entries_use_a_short_stale_window() {
    let pool = test_pool().await;
    let name = unique("cache");
    let config = providarr::config::CacheConfig::default();
    let store = CacheStore::new(pool, config.clone());

    let key = CacheStore::key(&name, "GET", "/missing/1");
    store
        .put(
            &key,
            &name,
            "missing/{id}",
            "/missing/1",
            404,
            None,
            b"{}",
            &serde_json::json!({}),
            Duration::from_millis(1),
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;
    let entry = store.get(&key).await.unwrap().unwrap();
    let now = chrono::Utc::now();
    assert!(entry.is_within_stale_window(now, &config));

    // Negative entries must not inherit the 14-day positive stale window.
    let later = now + chrono::Duration::days(2);
    assert!(
        !entry.is_within_stale_window(later, &config),
        "negative entries must use a short stale window"
    );
}

#[tokio::test]
async fn purge_removes_entries_past_stale_window() {
    let pool = test_pool().await;
    let name = unique("cache");
    let store = CacheStore::new(pool, providarr::config::CacheConfig::default());

    let key = CacheStore::key(&name, "GET", "/old");
    store
        .put(
            &key,
            &name,
            "old",
            "/old",
            200,
            None,
            b"{}",
            &serde_json::json!({}),
            Duration::from_millis(1),
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;
    // keep_stale_for = 0 means anything already expired is purgeable.
    store.purge_expired(Duration::from_secs(0)).await.unwrap();
    assert!(
        store.get(&key).await.unwrap().is_none(),
        "purged entry is gone"
    );
}
