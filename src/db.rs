use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{PgPool, Row, postgres::PgPoolOptions};

use crate::{config::DatabaseConfig, error::AppError, ratelimit::stats::EndpointStatSnapshot};

/// Upper bound on the number of endpoint-stat rows loaded into memory at
/// startup. Rows are ordered by `updated_at` (indexed by
/// `endpoint_stats_updated_at_idx`) so the most recently active endpoints win
/// and startup memory stays bounded even if the table has grown large.
const STARTUP_ENDPOINT_STATS_LIMIT: i64 = 50_000;

pub async fn connect(cfg: &DatabaseConfig) -> Result<PgPool, AppError> {
    let url = resolve_connection_url(cfg)?;
    let pool = PgPoolOptions::new()
        .max_connections(cfg.max_connections)
        .min_connections(cfg.min_connections)
        .acquire_timeout(cfg.acquire_timeout)
        .connect(&url)
        .await?;
    Ok(pool)
}

/// Applies the TLS policy from `DatabaseConfig` to the connection URL.
///
/// When `require_tls` is set, `sslmode=require` is appended unless the URL
/// already specifies an `sslmode`/`ssl-mode`. Otherwise a non-loopback host
/// without an explicit sslmode gets a warning recommending TLS.
fn resolve_connection_url(cfg: &DatabaseConfig) -> Result<String, AppError> {
    let has_sslmode = cfg.url.contains("sslmode=") || cfg.url.contains("ssl-mode=");
    if cfg.require_tls {
        if has_sslmode {
            return Ok(cfg.url.clone());
        }
        let separator = if cfg.url.contains('?') { '&' } else { '?' };
        let url = format!("{}{separator}sslmode=require", cfg.url);
        tracing::info!(
            "database.require_tls is enabled; appending sslmode=require to the connection URL"
        );
        return Ok(url);
    }

    if !has_sslmode
        && let Some(host) = database_host(&cfg.url)
        && !is_loopback_host(&host)
    {
        tracing::warn!(
            host = %host,
            "database.url does not set sslmode and the host is not loopback; set database.require_tls=true or add sslmode=require"
        );
    }
    Ok(cfg.url.clone())
}

fn database_host(url: &str) -> Option<String> {
    url::Url::parse(url).ok()?.host_str().map(str::to_string)
}

fn is_loopback_host(host: &str) -> bool {
    host == "localhost"
        || host
            .parse::<std::net::IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false)
}

pub async fn migrate(pool: &PgPool) -> Result<(), AppError> {
    sqlx::migrate!("./migrations").run(pool).await?;
    Ok(())
}

pub async fn load_endpoint_stats(pool: &PgPool) -> Result<Vec<EndpointStatSnapshot>, AppError> {
    let rows = sqlx::query(
        "SELECT provider, endpoint, requests_total, requests_since_limit, rate_limit_hits, \
         failures, successes, dropped, last_status, last_request_at, last_rate_limit_at, \
         observed_limit_threshold, noted_limit FROM endpoint_stats \
         ORDER BY updated_at DESC LIMIT $1",
    )
    .bind(STARTUP_ENDPOINT_STATS_LIMIT)
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

/// Delete endpoint-stat rows that have not been updated within `older_than`.
/// Returns the number of rows removed. The cutoff is computed in Postgres and
/// passed as a bound interval parameter; the predicate is sargable against
/// `endpoint_stats_updated_at_idx`.
pub async fn prune_endpoint_stats(pool: &PgPool, older_than: Duration) -> Result<u64, AppError> {
    let result = sqlx::query(
        "DELETE FROM endpoint_stats WHERE updated_at < now() - make_interval(secs => $1)",
    )
    .bind(older_than.as_secs_f64())
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// Delete rate-limit audit rows older than `older_than`. Returns the number of
/// rows removed. Backed by `rate_limit_events_occurred_at_idx`.
pub async fn prune_rate_limit_events(pool: &PgPool, older_than: Duration) -> Result<u64, AppError> {
    let result = sqlx::query(
        "DELETE FROM rate_limit_events WHERE occurred_at < now() - make_interval(secs => $1)",
    )
    .bind(older_than.as_secs_f64())
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
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
    // Keep the two branches separate so each uses an index: the unfiltered
    // branch is backed by `rate_limit_events_occurred_at_idx`, while the
    // provider-filtered branch uses
    // `rate_limit_events_provider_idx (provider, occurred_at DESC)`. A single
    // `($1 IS NULL OR provider = $1)` predicate is not sargable and would force
    // a full scan plus sort.
    let rows = match provider {
        Some(provider) => {
            sqlx::query(
                "SELECT provider, endpoint, occurred_at, requests_since_last_limit, status_code, detail \
                 FROM rate_limit_events \
                 WHERE provider = $1 ORDER BY occurred_at DESC LIMIT $2",
            )
            .bind(provider)
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
        None => {
            sqlx::query(
                "SELECT provider, endpoint, occurred_at, requests_since_last_limit, status_code, detail \
                 FROM rate_limit_events \
                 ORDER BY occurred_at DESC LIMIT $1",
            )
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
    };

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn require_tls_appends_sslmode() {
        let cfg = DatabaseConfig {
            url: "postgres://user:pw@db.example.com:5432/providarr".into(),
            require_tls: true,
            ..DatabaseConfig::default()
        };
        let url = resolve_connection_url(&cfg).unwrap();
        assert!(url.ends_with("sslmode=require"), "{url}");
        assert!(url.contains("?sslmode=require"), "{url}");
    }

    #[test]
    fn require_tls_respects_existing_sslmode() {
        for raw in [
            "postgres://user:pw@db.example.com:5432/providarr?sslmode=verify-full",
            "postgres://user:pw@db.example.com:5432/providarr?ssl-mode=require",
        ] {
            let cfg = DatabaseConfig {
                url: raw.into(),
                require_tls: true,
                ..DatabaseConfig::default()
            };
            assert_eq!(resolve_connection_url(&cfg).unwrap(), raw);
        }
    }

    #[test]
    fn require_tls_appends_with_ampersand_when_query_present() {
        let cfg = DatabaseConfig {
            url: "postgres://user:pw@db.example.com:5432/providarr?application_name=providarr"
                .into(),
            require_tls: true,
            ..DatabaseConfig::default()
        };
        let url = resolve_connection_url(&cfg).unwrap();
        assert!(url.ends_with("&sslmode=require"), "{url}");
    }

    #[test]
    fn plain_url_is_unchanged_without_require_tls() {
        let cfg = DatabaseConfig::default();
        assert_eq!(resolve_connection_url(&cfg).unwrap(), cfg.url);
    }

    #[test]
    fn loopback_hosts_are_detected() {
        assert!(is_loopback_host("localhost"));
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("::1"));
        assert!(!is_loopback_host("db.example.com"));
        assert!(!is_loopback_host("10.0.0.5"));
    }
}
