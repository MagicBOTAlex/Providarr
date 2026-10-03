use std::{cell::RefCell, future::Future};

use chrono::{DateTime, Utc};
use serde_json::json;
use tokio::task_local;

/// Aggregates the cache outcome of every upstream fetch made while mapping a
/// single Theoriarr-facing response.
#[derive(Debug, Default, Clone)]
pub struct CacheReport {
    hits: u32,
    stale: u32,
    replay: u32,
    misses: u32,
    oldest_cached_at: Option<DateTime<Utc>>,
    min_ttl_seconds: Option<i64>,
}

impl CacheReport {
    pub fn record(
        &mut self,
        cached: bool,
        stale: bool,
        replayed: bool,
        created_at: Option<DateTime<Utc>>,
        expires_at: Option<DateTime<Utc>>,
    ) {
        if replayed {
            self.replay += 1;
            return;
        }

        if stale {
            self.stale += 1;
        } else if cached {
            self.hits += 1;
        } else {
            self.misses += 1;
        }

        if let Some(created) = created_at
            && self.oldest_cached_at.is_none_or(|old| created < old)
        {
            self.oldest_cached_at = Some(created);
        }

        if let Some(expires) = expires_at {
            let now = Utc::now();
            let ttl = (expires - now).num_seconds().max(0);
            self.min_ttl_seconds = Some(self.min_ttl_seconds.map_or(ttl, |t| t.min(ttl)));
        }
    }

    pub fn status(&self) -> &'static str {
        if self.stale > 0 {
            "stale"
        } else if self.hits > 0 && self.misses > 0 {
            "partial"
        } else if self.hits > 0 {
            "hit"
        } else if self.replay > 0 && self.misses == 0 {
            "replay"
        } else {
            "miss"
        }
    }

    pub fn is_empty(&self) -> bool {
        self.hits == 0 && self.stale == 0 && self.misses == 0 && self.replay == 0
    }

    /// Cache metadata inserted into object responses under `_providarr`.
    pub fn to_json(&self) -> serde_json::Value {
        let age_seconds = self
            .oldest_cached_at
            .map(|created| (Utc::now() - created).num_seconds().max(0));

        json!({
            "cache": self.status(),
            "cacheHits": self.hits,
            "staleHits": self.stale,
            "upstreamRequests": self.misses,
            "replayed": self.replay,
            "cachedAt": self.oldest_cached_at,
            "ageSeconds": age_seconds,
            "ttlSeconds": self.min_ttl_seconds,
        })
    }
}

task_local! {
    static CACHE_REPORT: RefCell<CacheReport>;
}

/// Runs `future` with a fresh cache report in scope and returns both the future's
/// output and the report. Other tasks are unaffected.
pub async fn scope<F, T>(future: F) -> (T, CacheReport)
where
    F: Future<Output = T>,
{
    CACHE_REPORT
        .scope(RefCell::new(CacheReport::default()), async move {
            let value = future.await;
            let report = CACHE_REPORT.with(|cell| cell.borrow().clone());
            (value, report)
        })
        .await
}

/// Records one upstream result into the in-scope report (no-op outside a scope).
pub fn record(
    cached: bool,
    stale: bool,
    replayed: bool,
    created_at: Option<DateTime<Utc>>,
    expires_at: Option<DateTime<Utc>>,
) {
    let _ = CACHE_REPORT.try_with(|cell| {
        cell.borrow_mut()
            .record(cached, stale, replayed, created_at, expires_at)
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn summarises_mixed_cache_outcomes() {
        let mut report = CacheReport::default();
        let created = Utc::now() - Duration::seconds(120);
        let expires = Utc::now() + Duration::seconds(600);

        report.record(true, false, false, Some(created), Some(expires));
        report.record(false, false, false, None, None);

        assert_eq!(report.status(), "partial");
        let json = report.to_json();
        assert_eq!(json["cache"], "partial");
        assert_eq!(json["cacheHits"], 1);
        assert_eq!(json["upstreamRequests"], 1);
        assert!(json["ageSeconds"].as_i64().unwrap() >= 120);
        assert!(json["ttlSeconds"].as_i64().unwrap() <= 600);
    }

    #[test]
    fn all_misses_report_a_miss() {
        let mut report = CacheReport::default();
        report.record(false, false, false, None, None);
        assert_eq!(report.status(), "miss");
    }

    #[test]
    fn all_hits_report_a_hit() {
        let mut report = CacheReport::default();
        report.record(true, false, false, Some(Utc::now()), Some(Utc::now()));
        assert_eq!(report.status(), "hit");
    }
}
