mod common;

use common::*;
use providarr::db;

#[tokio::test]
async fn provider_backoff_persists_and_clears() {
    let pool = test_pool().await;

    db::upsert_provider_backoff(&pool, "tmdb", 3, 5_000)
        .await
        .unwrap();

    let rows = db::load_provider_backoff(&pool).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].provider, "tmdb");
    assert_eq!(rows[0].consecutive_failures, 3);
    assert_eq!(rows[0].remaining_ms, 5_000);

    db::clear_provider_backoff(&pool, "tmdb").await.unwrap();
    assert!(db::load_provider_backoff(&pool).await.unwrap().is_empty());
}
