use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};

use crate::{config::CacheConfig, error::AppError};

/// Hard upper bound on the number of rows kept in `cache_entries`. TTLs can be
/// up to six months and the hourly purge only removes rows past their stale
/// window, so an explicit budget is required to keep the table from growing
/// without limit. Eviction is enforced on the write path (see `put`).
const MAX_ENTRIES: i64 = 200_000;

/// Hard upper bound on the total body bytes kept in `cache_entries`.
const MAX_TOTAL_BYTES: i64 = 8 * 1024 * 1024 * 1024;

/// Percentage of each cap that a sweep evicts down to. Draining to a low-water
/// mark gives hysteresis: after a sweep the table must grow by ~15% of the cap
/// before the next sweep, so eviction cost is amortized over many writes rather
/// than paid on every `put` once the cap is reached.
const EVICTION_TARGET_PERCENT: i64 = 85;

/// Maximum rows removed by a single indexed, `LIMIT`-bounded delete. Bounds the
/// work, locking and `RETURNING octet_length(body)` detoast of one statement.
const EVICTION_BATCH_ROWS: i64 = 5_000;

/// Upper bound on delete statements per sweep. Prevents an unbounded loop when
/// counters and the table disagree; counters are reconciled afterwards.
const EVICTION_MAX_BATCHES: usize = 8;

/// Maximum detached hit-counter writes allowed in flight at once. When this is
/// saturated, further hits only update the in-memory total so a hot key cannot
/// spawn unbounded tasks or monopolise the connection pool.
const MAX_INFLIGHT_HIT_WRITES: i64 = 128;

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

/// Releases the "eviction in progress" latch even if the sweep returns early.
struct EvictingGuard<'a>(&'a AtomicBool);

impl Drop for EvictingGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Low-water mark for a cap, in the same units as the cap.
const fn low_water(cap: i64) -> i64 {
    cap / 100 * EVICTION_TARGET_PERCENT
}

/// Number of rows a single eviction statement should target given the current
/// counters. Sized to whichever budget is under more pressure, but always
/// bounded by `EVICTION_BATCH_ROWS`.
fn eviction_batch_size(entries: i64, bytes: i64, target_entries: i64, target_bytes: i64) -> i64 {
    let over_entries = (entries - target_entries).max(0);
    let bytes_over = (bytes - target_bytes).max(0);
    // Average row size is a good enough estimate to translate byte pressure
    // into a row count; the surrounding loop re-checks after every batch.
    let avg_row = (bytes / entries.max(1)).max(1);
    let rows_for_bytes = bytes_over / avg_row + 1;
    over_entries
        .max(rows_for_bytes)
        .clamp(1, EVICTION_BATCH_ROWS)
}

/// Heavy Postgres-backed cache for upstream responses.
#[derive(Clone)]
pub struct CacheStore {
    pool: PgPool,
    config: CacheConfig,
    counters: Arc<CacheCounters>,
    /// Number of detached hit-accounting writes currently in flight.
    hit_writes: Arc<AtomicI64>,
    /// Latch so only one task runs a budget sweep at a time.
    evicting: Arc<AtomicBool>,
}

impl CacheStore {
    pub fn new(pool: PgPool, config: CacheConfig) -> Self {
        Self {
            pool,
            config,
            counters: Arc::new(CacheCounters::default()),
            hit_writes: Arc::new(AtomicI64::new(0)),
            evicting: Arc::new(AtomicBool::new(false)),
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
        // Plain read: takes no row write lock and returns the pool connection as
        // soon as the row is fetched. Oversized (pre-existing) rows are skipped
        // so a body above `max_body_bytes` can never be returned.
        let row = sqlx::query(
            "SELECT status_code, content_type, body, headers, created_at, expires_at \
             FROM cache_entries \
             WHERE cache_key = $1 AND octet_length(body) <= $2",
        )
        .bind(key)
        .bind(self.config.max_body_bytes as i64)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            // Either the key is absent, or a legacy row is larger than the read
            // guard allows. Best-effort drop such a row so it cannot pin disk
            // forever; failures are never surfaced to the caller.
            if let Err(e) = self.delete_oversized(key).await {
                tracing::debug!(cache_key = key, error = %e, "failed to drop oversized cache row");
            }
            return Ok(None);
        };

        // Hit accounting is best-effort and detached from the read so a hot key
        // does not serialise the pool behind per-hit row locks. The read
        // connection was already released above.
        self.record_hit(key);

        Ok(Some(CachedEntry {
            status_code: row.get::<i32, _>("status_code").max(0) as u16,
            content_type: row.get::<Option<String>, _>("content_type"),
            body: Bytes::from(row.get::<Vec<u8>, _>("body")),
            headers: row.get::<serde_json::Value, _>("headers"),
            created_at: row.get::<DateTime<Utc>, _>("created_at"),
            expires_at: row.get::<DateTime<Utc>, _>("expires_at"),
        }))
    }

    /// Fire-and-forget `hits`/`last_accessed_at` update. The in-memory total is
    /// always bumped; the database write is dropped when too many are already in
    /// flight so a hot loop cannot exhaust the pool or spawn unbounded tasks.
    fn record_hit(&self, key: &str) {
        self.counters.total_hits.fetch_add(1, Ordering::Relaxed);

        if self.hit_writes.load(Ordering::Relaxed) >= MAX_INFLIGHT_HIT_WRITES {
            return;
        }
        self.hit_writes.fetch_add(1, Ordering::Relaxed);

        let pool = self.pool.clone();
        let key = key.to_owned();
        let in_flight = self.hit_writes.clone();
        tokio::spawn(async move {
            if let Err(e) = sqlx::query(
                "UPDATE cache_entries SET hits = hits + 1, last_accessed_at = now() \
                 WHERE cache_key = $1",
            )
            .bind(&key)
            .execute(&pool)
            .await
            {
                tracing::debug!(cache_key = %key, error = %e, "failed to record cache hit");
            }
            in_flight.fetch_sub(1, Ordering::Relaxed);
        });
    }

    /// Drops a single oversized legacy row. Returns the bytes removed so the
    /// caller can keep the counters exact.
    async fn delete_oversized(&self, key: &str) -> Result<(), sqlx::Error> {
        let removed: Option<i64> = sqlx::query_scalar(
            "DELETE FROM cache_entries \
             WHERE cache_key = $1 AND octet_length(body) > $2 \
             RETURNING octet_length(body)::bigint",
        )
        .bind(key)
        .bind(self.config.max_body_bytes as i64)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(len) = removed {
            self.counters.entries.fetch_sub(1, Ordering::Relaxed);
            self.counters.approx_bytes.fetch_sub(len, Ordering::Relaxed);
        }
        Ok(())
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

        let ttl = ttl.min(self.config.max_ttl);
        let expires_at = Utc::now()
            + ChronoDuration::from_std(ttl)
                .unwrap_or_else(|_| ChronoDuration::seconds(ttl.as_secs() as i64));

        // Upsert and read the previous body length in one statement. The
        // `previous` CTE sees the pre-statement snapshot, so its value is the
        // row we are replacing (NULL for an insert). Doing this atomically stops
        // a concurrent overwrite from racing the byte accounting.
        //
        // `last_accessed_at` is set on both insert and conflict so a freshly
        // written row is not treated as "never accessed".
        let previous_len: Option<i64> = sqlx::query_scalar(
            "WITH input AS ( \
                 SELECT $1::text AS cache_key, $2::text AS provider, $3::text AS endpoint, \
                        $4::text AS resource, $5::integer AS status_code, \
                        $6::text AS content_type, $7::bytea AS body, \
                        $8::jsonb AS headers, $9::timestamptz AS expires_at \
             ), \
             previous AS ( \
                 SELECT octet_length(c.body)::bigint AS old_len \
                 FROM cache_entries c JOIN input i ON c.cache_key = i.cache_key \
             ), \
             upsert AS ( \
                 INSERT INTO cache_entries \
                     (cache_key, provider, endpoint, resource, status_code, content_type, \
                      body, headers, expires_at, last_accessed_at) \
                 SELECT cache_key, provider, endpoint, resource, status_code, content_type, \
                        body, headers, expires_at, now() \
                 FROM input \
                 ON CONFLICT (cache_key) DO UPDATE SET \
                     provider = EXCLUDED.provider, \
                     endpoint = EXCLUDED.endpoint, \
                     resource = EXCLUDED.resource, \
                     status_code = EXCLUDED.status_code, \
                     content_type = EXCLUDED.content_type, \
                     body = EXCLUDED.body, \
                     headers = EXCLUDED.headers, \
                     expires_at = EXCLUDED.expires_at, \
                     created_at = now(), \
                     last_accessed_at = now() \
                 RETURNING 1 \
             ) \
             SELECT (SELECT old_len FROM previous) \
             FROM (SELECT COUNT(*) FROM upsert) s",
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
        .fetch_one(&self.pool)
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

        // Best-effort: never fail a request because a budget sweep errored.
        if let Err(e) = self.enforce_budget().await {
            tracing::warn!(
                provider,
                endpoint,
                error = %e,
                "cache budget enforcement failed; table may exceed caps until next write"
            );
        }
        Ok(())
    }

    fn counters_exceed_cap(&self) -> bool {
        self.counters.entries.load(Ordering::Relaxed) > MAX_ENTRIES
            || self.counters.approx_bytes.load(Ordering::Relaxed) > MAX_TOTAL_BYTES
    }

    /// Evicts least-valuable rows when the running counters exceed the hard
    /// caps, draining to a low-water mark so sweeps are amortized across many
    /// writes. Each statement is an indexed, `LIMIT`-bounded delete ordered by
    /// the `expires_at` index; no full-table sort or window function is used.
    ///
    /// The `expires_at` index is the only ordered index the schema provides.
    /// Ordering by `last_accessed_at` (true global LRU) would need its own index
    /// owned by the migrations workstream; until then, soonest-to-expire is the
    /// closest indexed proxy, with `last_accessed_at` as a tie-break.
    async fn enforce_budget(&self) -> Result<(), sqlx::Error> {
        if !self.counters_exceed_cap() {
            return Ok(());
        }
        // Only one sweep at a time; a concurrent write will be covered by it.
        if self.evicting.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let _guard = EvictingGuard(&self.evicting);

        let target_entries = low_water(MAX_ENTRIES);
        let target_bytes = low_water(MAX_TOTAL_BYTES);
        let mut evicted_entries = 0i64;
        let mut evicted_bytes = 0i64;

        for _ in 0..EVICTION_MAX_BATCHES {
            let entries = self.counters.entries.load(Ordering::Relaxed);
            let bytes = self.counters.approx_bytes.load(Ordering::Relaxed);
            if entries <= target_entries && bytes <= target_bytes {
                break;
            }

            let batch = eviction_batch_size(entries, bytes, target_entries, target_bytes);
            let removed: Vec<i64> = sqlx::query_scalar(
                "DELETE FROM cache_entries \
                 WHERE ctid IN ( \
                     SELECT ctid FROM cache_entries \
                     ORDER BY expires_at ASC, last_accessed_at ASC NULLS FIRST \
                     LIMIT $1 \
                 ) \
                 RETURNING octet_length(body)::bigint",
            )
            .bind(batch)
            .fetch_all(&self.pool)
            .await?;

            if removed.is_empty() {
                // Nothing left to delete, so the counters are ahead of the table
                // (e.g. another instance already purged). Re-sync from the table
                // truth so we do not sweep on every subsequent write.
                self.reconcile_counters().await?;
                break;
            }

            let rows = removed.len() as i64;
            let bytes_removed: i64 = removed.iter().sum();
            self.counters.entries.fetch_sub(rows, Ordering::Relaxed);
            self.counters
                .approx_bytes
                .fetch_sub(bytes_removed, Ordering::Relaxed);
            evicted_entries += rows;
            evicted_bytes += bytes_removed;
        }

        // If the bounded sweep could not reach the low-water marks (a large
        // backlog, or drifted counters), re-sync from the table so the counters
        // never stay permanently above a cap the table is actually under.
        if self.counters_exceed_cap() {
            self.reconcile_counters().await?;
        }

        if evicted_entries > 0 {
            tracing::info!(
                evicted_entries,
                evicted_bytes,
                "cache budget exceeded; evicted least-valuable entries"
            );
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

        // The hourly purge is the natural reconciliation point: another instance
        // may have written or evicted rows behind these counters. Best-effort so
        // a reconcile failure does not report the purge itself as failed.
        if let Err(e) = self.reconcile_counters().await {
            tracing::warn!(error = %e, "failed to reconcile cache counters after purge");
        }
        Ok(removed.len() as u64)
    }

    /// Recomputes the in-memory counters from the table. This is a full scan and
    /// is only used at startup and after a sweep that cannot reconcile itself
    /// from the removed rows, never on the steady-state write path.
    async fn reconcile_counters(&self) -> Result<(), sqlx::Error> {
        let row = sqlx::query(
            "SELECT COUNT(*)::bigint AS entries, \
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
            .approx_bytes
            .store(row.get::<i64, _>("approx_bytes"), Ordering::Relaxed);
        // In-memory hits are bumped on every lookup, including hits whose
        // detached database write was dropped under load, so never move the
        // running hit total backwards when reconciling.
        self.counters
            .total_hits
            .fetch_max(row.get::<i64, _>("total_hits"), Ordering::Relaxed);
        Ok(())
    }

    /// Seeds the in-memory counters from the current table state. Called once at
    /// startup so `/v1/cache/stats` never has to touch the whole table again.
    pub async fn seed_stats(&self) -> Result<(), AppError> {
        self.reconcile_counters().await?;
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

    #[test]
    fn low_water_leaves_hysteresis_headroom() {
        assert!(low_water(MAX_ENTRIES) < MAX_ENTRIES);
        assert_eq!(
            low_water(MAX_ENTRIES),
            MAX_ENTRIES / 100 * EVICTION_TARGET_PERCENT
        );
    }

    #[test]
    fn eviction_batch_is_bounded_and_scales_to_pressure() {
        let target_entries = low_water(MAX_ENTRIES);
        let target_bytes = low_water(MAX_TOTAL_BYTES);

        // Entry pressure far above the cap: full batch, never more.
        assert_eq!(
            eviction_batch_size(MAX_ENTRIES + 100_000, 0, target_entries, target_bytes),
            EVICTION_BATCH_ROWS
        );

        // Byte pressure with few, large rows: batch sized to the byte overage
        // rather than wiping a small cache.
        let entries = 8_000;
        let bytes = MAX_TOTAL_BYTES + 1_000_000_000;
        let batch = eviction_batch_size(entries, bytes, target_entries, target_bytes);
        assert!(batch > 0 && batch < EVICTION_BATCH_ROWS, "batch = {batch}");

        // A single row over a cap still makes progress.
        assert!(eviction_batch_size(MAX_ENTRIES + 1, 0, target_entries, target_bytes) >= 1);
    }
}
