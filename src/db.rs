use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{PgPool, Row, postgres::PgPoolOptions};

use crate::{config::DatabaseConfig, error::AppError, ratelimit::stats::EndpointStatSnapshot};

pub async fn connect(cfg: &DatabaseConfig) -> Result<PgPool, AppError> {
    let pool = PgPoolOptions::new()
        .max_connections(cfg.max_connections)
        .min_connections(cfg.min_connections)
        .acquire_timeout(cfg.acquire_timeout)
        .connect(&cfg.url)
        .await?;
    Ok(pool)
}

pub async fn migrate(pool: &PgPool) -> Result<(), AppError> {
    sqlx::migrate!("./migrations").run(pool).await?;
    Ok(())
}

pub async fn load_endpoint_stats(pool: &PgPool) -> Result<Vec<EndpointStatSnapshot>, AppError> {
    let rows = sqlx::query(
        "SELECT provider, endpoint, requests_total, requests_since_limit, rate_limit_hits, \
         failures, successes, dropped, last_status, last_request_at, last_rate_limit_at, \
         observed_limit_threshold, noted_limit FROM endpoint_stats",
    )
    .fetch_all(pool)
    .await?;

    let snaps = rows
        .into_iter()
        .map(|row| EndpointStatSnapshot {
            provider: row.get::<String, _>("provider"),
            endpoint: row.get::<String, _>("endpoint"),
            requests_total: row.get::<i64, _>("requests_total").max(0) as u64,
            requests_since_limit: row.get::<i64, _>("requests_since_limit").max(0) as u64,
            rate_limit_hits: row.get::<i64, _>("rate_limit_hits").max(0) as u64,
            failures: row.get::<i64, _>("failures").max(0) as u64,
            successes: row.get::<i64, _>("successes").max(0) as u64,
            dropped: row.get::<i64, _>("dropped").max(0) as u64,
            last_status: row.get::<Option<i32>, _>("last_status").map(|s| s as u16),
            last_request_at: row.get::<Option<DateTime<Utc>>, _>("last_request_at"),
            last_rate_limit_at: row.get::<Option<DateTime<Utc>>, _>("last_rate_limit_at"),
            observed_limit_threshold: row
                .get::<Option<i64>, _>("observed_limit_threshold")
                .map(|v| v.max(0) as u64),
            noted_limit: row.get::<Option<String>, _>("noted_limit"),
        })
        .collect();

    Ok(snaps)
}

pub async fn upsert_endpoint_stats(
    pool: &PgPool,
    snaps: &[EndpointStatSnapshot],
) -> Result<(), AppError> {
    let mut tx = pool.begin().await?;
    for s in snaps {
        sqlx::query(
            "INSERT INTO endpoint_stats (provider, endpoint, requests_total, requests_since_limit, \
             rate_limit_hits, failures, successes, dropped, last_status, last_request_at, \
             last_rate_limit_at, observed_limit_threshold, noted_limit, updated_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13, now()) \
             ON CONFLICT (provider, endpoint) DO UPDATE SET \
               requests_total = EXCLUDED.requests_total, \
               requests_since_limit = EXCLUDED.requests_since_limit, \
               rate_limit_hits = EXCLUDED.rate_limit_hits, \
               failures = EXCLUDED.failures, \
               successes = EXCLUDED.successes, \
               dropped = EXCLUDED.dropped, \
               last_status = EXCLUDED.last_status, \
               last_request_at = EXCLUDED.last_request_at, \
               last_rate_limit_at = EXCLUDED.last_rate_limit_at, \
               observed_limit_threshold = EXCLUDED.observed_limit_threshold, \
               noted_limit = EXCLUDED.noted_limit, \
               updated_at = now()",
        )
        .bind(&s.provider)
        .bind(&s.endpoint)
        .bind(s.requests_total as i64)
        .bind(s.requests_since_limit as i64)
        .bind(s.rate_limit_hits as i64)
        .bind(s.failures as i64)
        .bind(s.successes as i64)
        .bind(s.dropped as i64)
        .bind(s.last_status.map(i32::from))
        .bind(s.last_request_at)
        .bind(s.last_rate_limit_at)
        .bind(s.observed_limit_threshold.map(|v| v as i64))
        .bind(&s.noted_limit)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub async fn insert_rate_limit_event(
    pool: &PgPool,
    provider: &str,
    endpoint: &str,
    requests_since_last_limit: u64,
    status_code: Option<u16>,
    detail: Option<&str>,
) -> Result<(), AppError> {
    sqlx::query(
        "INSERT INTO rate_limit_events (provider, endpoint, requests_since_last_limit, status_code, detail) \
         VALUES ($1,$2,$3,$4,$5)",
    )
    .bind(provider)
    .bind(endpoint)
    .bind(requests_since_last_limit as i64)
    .bind(status_code.map(i32::from))
    .bind(detail)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn ping(pool: &PgPool) -> Result<(), AppError> {
    sqlx::query("SELECT 1").execute(pool).await?;
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
pub struct RateLimitEventRow {
    pub provider: String,
    pub endpoint: String,
    pub occurred_at: DateTime<Utc>,
    pub requests_since_last_limit: i64,
    pub status_code: Option<i32>,
    pub detail: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ProviderBackoffRow {
    pub provider: String,
    pub consecutive_failures: i32,
    pub remaining_ms: i64,
    pub updated_at: DateTime<Utc>,
}

pub async fn upsert_provider_backoff(
    pool: &PgPool,
    provider: &str,
    consecutive_failures: i32,
    remaining_ms: i64,
) -> Result<(), AppError> {
    sqlx::query(
        "INSERT INTO provider_backoff (provider, consecutive_failures, remaining_ms, updated_at) \
         VALUES ($1, $2, $3, now()) \
         ON CONFLICT (provider) DO UPDATE SET \
           consecutive_failures = EXCLUDED.consecutive_failures, \
           remaining_ms = EXCLUDED.remaining_ms, \
           updated_at = now()",
    )
    .bind(provider)
    .bind(consecutive_failures)
    .bind(remaining_ms)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn clear_provider_backoff(pool: &PgPool, provider: &str) -> Result<(), AppError> {
    sqlx::query("DELETE FROM provider_backoff WHERE provider = $1")
        .bind(provider)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn load_provider_backoff(pool: &PgPool) -> Result<Vec<ProviderBackoffRow>, AppError> {
    let rows = sqlx::query(
        "SELECT provider, consecutive_failures, remaining_ms, updated_at FROM provider_backoff",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| ProviderBackoffRow {
            provider: row.get::<String, _>("provider"),
            consecutive_failures: row.get::<i32, _>("consecutive_failures"),
            remaining_ms: row.get::<i64, _>("remaining_ms"),
            updated_at: row.get::<DateTime<Utc>, _>("updated_at"),
        })
        .collect())
}

pub async fn load_rate_limit_events(
    pool: &PgPool,
    provider: Option<&str>,
    limit: i64,
) -> Result<Vec<RateLimitEventRow>, AppError> {
    let rows = sqlx::query(
        "SELECT provider, endpoint, occurred_at, requests_since_last_limit, status_code, detail \
         FROM rate_limit_events \
         WHERE ($1::text IS NULL OR provider = $1) \
         ORDER BY occurred_at DESC LIMIT $2",
    )
    .bind(provider)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RateLimitEventRow {
            provider: row.get::<String, _>("provider"),
            endpoint: row.get::<String, _>("endpoint"),
            occurred_at: row.get::<DateTime<Utc>, _>("occurred_at"),
            requests_since_last_limit: row.get::<i64, _>("requests_since_last_limit"),
            status_code: row.get::<Option<i32>, _>("status_code"),
            detail: row.get::<Option<String>, _>("detail"),
        })
        .collect())
}
