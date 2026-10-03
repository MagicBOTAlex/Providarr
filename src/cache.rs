use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};

use crate::{config::CacheConfig, error::AppError};

#[derive(Debug, Clone)]
pub struct CachedEntry {
    pub status_code: u16,
    pub content_type: Option<String>,
    pub body: Bytes,
    pub headers: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl CachedEntry {
    pub fn is_fresh(&self, now: DateTime<Utc>) -> bool {
        self.expires_at > now
    }

    pub fn is_within_stale_window(&self, now: DateTime<Utc>, cfg: &CacheConfig) -> bool {
        // Negative (4xx) entries get their own, deliberately short stale window
        // so a transient error is not pinned for the full stale_if_error span.
        let window = if self.status_code >= 400 {
            cfg.negative_stale_if_error
        } else {
            cfg.stale_if_error
        };
        let stale = ChronoDuration::from_std(window)
            .unwrap_or_else(|_| ChronoDuration::seconds(window.as_secs() as i64));
        self.expires_at + stale > now
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CacheStats {
    pub entries: i64,
    pub expired: i64,
    pub total_hits: i64,
    pub approx_bytes: i64,
}

/// Running totals maintained as the cache is written. Keeps `/v1/cache/stats`
/// off the hot path: no per-request full-table scan (or `octet_length` over
/// TOASTed bodies).
#[derive(Debug, Default)]
struct CacheCounters {
    entries: AtomicI64,
    total_hits: AtomicI64,
    approx_bytes: AtomicI64,
}

/// Heavy Postgres-backed cache for upstream responses.
#[derive(Clone)]
pub struct CacheStore {
    pool: PgPool,
    config: CacheConfig,
    counters: Arc<CacheCounters>,
}

impl CacheStore {
    pub fn new(pool: PgPool, config: CacheConfig) -> Self {
        Self {
            pool,
            config,
            counters: Arc::new(CacheCounters::default()),
        }
    }

    pub fn config(&self) -> &CacheConfig {
        &self.config
    }

    pub fn enabled(&self) -> bool {
        self.config.enabled
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Stable cache key over provider + method + path/query.
    pub fn key(provider: &str, method: &str, path_and_query: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(provider.as_bytes());
        hasher.update(b"\n");
        hasher.update(method.to_ascii_uppercase().as_bytes());
        hasher.update(b"\n");
        hasher.update(path_and_query.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// Returns the entry regardless of freshness (caller decides fresh/stale).
    /// Fresh lookups bump the hit counter.
    pub async fn get(&self, key: &str) -> Result<Option<CachedEntry>, AppError> {
        let row = sqlx::query(
            "UPDATE cache_entries SET hits = hits + 1, last_accessed_at = now() \
             WHERE cache_key = $1 \
             RETURNING status_code, content_type, body, headers, created_at, expires_at",
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        if row.is_some() {
            self.counters.total_hits.fetch_add(1, Ordering::Relaxed);
        }

        Ok(row.map(|row| CachedEntry {
            status_code: row.get::<i32, _>("status_code").max(0) as u16,
            content_type: row.get::<Option<String>, _>("content_type"),
            body: Bytes::from(row.get::<Vec<u8>, _>("body")),
            headers: row.get::<serde_json::Value, _>("headers"),
            created_at: row.get::<DateTime<Utc>, _>("created_at"),
            expires_at: row.get::<DateTime<Utc>, _>("expires_at"),
        }))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn put(
        &self,
        key: &str,
        provider: &str,
        endpoint: &str,
        resource: &str,
        status_code: u16,
        content_type: Option<&str>,
        body: &[u8],
        headers: &serde_json::Value,
        ttl: Duration,
    ) -> Result<(), AppError> {
        if !self.config.enabled {
            return Ok(());
        }
        if body.len() > self.config.max_body_bytes {
            tracing::warn!(
                provider,
                endpoint,
                bytes = body.len(),
                max_body_bytes = self.config.max_body_bytes,
                "cache body exceeds max_body_bytes; not storing"
            );
            crate::metrics::cache_oversized(provider, endpoint);
            return Ok(());
        }

        // Track the previous body size so the running byte counter stays exact
        // when an existing row is overwritten.
        let previous_len: Option<i64> = sqlx::query_scalar(
            "SELECT octet_length(body)::bigint FROM cache_entries WHERE cache_key = $1",
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        let ttl = ttl.min(self.config.max_ttl);
        let expires_at = Utc::now()
            + ChronoDuration::from_std(ttl)
                .unwrap_or_else(|_| ChronoDuration::seconds(ttl.as_secs() as i64));

        sqlx::query(
            "INSERT INTO cache_entries \
             (cache_key, provider, endpoint, resource, status_code, content_type, body, headers, expires_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) \
             ON CONFLICT (cache_key) DO UPDATE SET \
               provider = EXCLUDED.provider, \
               endpoint = EXCLUDED.endpoint, \
               resource = EXCLUDED.resource, \
               status_code = EXCLUDED.status_code, \
               content_type = EXCLUDED.content_type, \
               body = EXCLUDED.body, \
               headers = EXCLUDED.headers, \
               expires_at = EXCLUDED.expires_at, \
               created_at = now()",
        )
        .bind(key)
        .bind(provider)
        .bind(endpoint)
        .bind(resource)
        .bind(status_code as i32)
        .bind(content_type)
        .bind(body)
        .bind(headers)
        .bind(expires_at)
        .execute(&self.pool)
        .await?;

        let new_len = body.len() as i64;
        match previous_len {
            None => {
                self.counters.entries.fetch_add(1, Ordering::Relaxed);
                self.counters
                    .approx_bytes
                    .fetch_add(new_len, Ordering::Relaxed);
            }
            Some(old_len) => {
                self.counters
                    .approx_bytes
                    .fetch_add(new_len - old_len, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    /// Removes a single entry (e.g. after an explicit `no-store`/`no-cache`).
    pub async fn invalidate(&self, key: &str) -> Result<(), AppError> {
        let removed: Option<i64> = sqlx::query_scalar(
            "DELETE FROM cache_entries WHERE cache_key = $1 RETURNING octet_length(body)::bigint",
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(len) = removed {
            self.counters.entries.fetch_sub(1, Ordering::Relaxed);
            self.counters.approx_bytes.fetch_sub(len, Ordering::Relaxed);
        }
        Ok(())
    }

    pub async fn purge_expired(&self, keep_stale_for: Duration) -> Result<u64, AppError> {
        let cutoff = Utc::now()
            - ChronoDuration::from_std(keep_stale_for)
                .unwrap_or_else(|_| ChronoDuration::seconds(keep_stale_for.as_secs() as i64));
        let removed: Vec<i64> = sqlx::query_scalar(
            "DELETE FROM cache_entries WHERE expires_at < $1 RETURNING octet_length(body)::bigint",
        )
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await?;

        let rows = removed.len() as i64;
        if rows > 0 {
            let bytes: i64 = removed.iter().sum();
            self.counters.entries.fetch_sub(rows, Ordering::Relaxed);
            self.counters
                .approx_bytes
                .fetch_sub(bytes, Ordering::Relaxed);
        }
        Ok(removed.len() as u64)
    }

    /// Seeds the in-memory counters from the current table state. Called once at
    /// startup so `/v1/cache/stats` never has to touch the whole table again.
    pub async fn seed_stats(&self) -> Result<(), AppError> {
        let row = sqlx::query(
            "SELECT COUNT(*) AS entries, \
             COALESCE(SUM(hits), 0)::bigint AS total_hits, \
             COALESCE(SUM(octet_length(body)), 0)::bigint AS approx_bytes \
             FROM cache_entries",
        )
        .fetch_one(&self.pool)
        .await?;
        self.counters
            .entries
            .store(row.get::<i64, _>("entries"), Ordering::Relaxed);
        self.counters
            .total_hits
            .store(row.get::<i64, _>("total_hits"), Ordering::Relaxed);
        self.counters
            .approx_bytes
            .store(row.get::<i64, _>("approx_bytes"), Ordering::Relaxed);
        Ok(())
    }

    /// Cheap stats derived from running counters. Only the expired count is
    /// queried, and it uses the `expires_at` index without reading bodies.
    pub async fn stats(&self) -> Result<CacheStats, AppError> {
        let expired: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM cache_entries WHERE expires_at <= now()")
                .fetch_one(&self.pool)
                .await?;
        Ok(CacheStats {
            entries: self.counters.entries.load(Ordering::Relaxed),
            expired,
            total_hits: self.counters.total_hits.load(Ordering::Relaxed),
            approx_bytes: self.counters.approx_bytes.load(Ordering::Relaxed),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_key_is_stable_and_sensitive() {
        let a = CacheStore::key("tmdb", "GET", "/movie/550?language=en");
        let b = CacheStore::key("tmdb", "get", "/movie/550?language=en");
        let c = CacheStore::key("tmdb", "GET", "/movie/551?language=en");
        let d = CacheStore::key("tvdb", "GET", "/movie/550?language=en");
        assert_eq!(a, b, "method casing must not matter");
        assert_ne!(a, c);
        assert_ne!(a, d);
    }
}
