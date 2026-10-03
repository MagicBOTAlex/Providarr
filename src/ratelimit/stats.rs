use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use serde::Serialize;

type Key = (String, String);

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Debug, Default)]
struct Counters {
    requests_total: AtomicU64,
    requests_since_limit: AtomicU64,
    rate_limit_hits: AtomicU64,
    failures: AtomicU64,
    successes: AtomicU64,
    dropped: AtomicU64,
    last_status: AtomicU64,     // 0 = none
    last_request_at: AtomicU64, // unix millis, 0 = none
}

#[derive(Debug)]
struct EndpointEntry {
    counters: Counters,
    last_rate_limit_at: Mutex<Option<DateTime<Utc>>>,
    observed_limit_threshold: AtomicU64, // 0 = none
    noted_limit: RwLock<Option<String>>,
}

impl EndpointEntry {
    fn new() -> Self {
        Self {
            counters: Counters::default(),
            last_rate_limit_at: Mutex::new(None),
            observed_limit_threshold: AtomicU64::new(0),
            noted_limit: RwLock::new(None),
        }
    }
}

/// Serializable view of an endpoint's accounting.
#[derive(Debug, Clone, Serialize)]
pub struct EndpointStatSnapshot {
    pub provider: String,
    pub endpoint: String,
    pub requests_total: u64,
    pub requests_since_limit: u64,
    pub rate_limit_hits: u64,
    pub failures: u64,
    pub successes: u64,
    pub dropped: u64,
    pub last_status: Option<u16>,
    pub last_request_at: Option<DateTime<Utc>>,
    pub last_rate_limit_at: Option<DateTime<Utc>>,
    pub observed_limit_threshold: Option<u64>,
    pub noted_limit: Option<String>,
}

/// In-memory, lock-free-ish accounting for every (provider, endpoint) pair.
///
/// `requests_since_limit` only resets when an upstream actually returns a rate
/// limit response; Providarr never probes for the limit on purpose.
#[derive(Debug, Default)]
pub struct StatsRegistry {
    map: DashMap<Key, EndpointEntry>,
}

impl StatsRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    fn key(provider: &str, endpoint: &str) -> Key {
        (provider.to_string(), endpoint.to_string())
    }

    pub fn record_request(&self, provider: &str, endpoint: &str, noted_limit: Option<&str>) {
        let entry = self
            .map
            .entry(Self::key(provider, endpoint))
            .or_insert_with(EndpointEntry::new);
        entry
            .counters
            .requests_total
            .fetch_add(1, Ordering::Relaxed);
        entry
            .counters
            .requests_since_limit
            .fetch_add(1, Ordering::Relaxed);
        entry
            .counters
            .last_request_at
            .store(now_millis(), Ordering::Relaxed);
        if let Some(note) = noted_limit {
            let mut guard = entry.noted_limit.write();
            if guard.is_none() {
                *guard = Some(note.to_string());
            }
        }
    }

    pub fn record_success(&self, provider: &str, endpoint: &str, status: u16) {
        let entry = self
            .map
            .entry(Self::key(provider, endpoint))
            .or_insert_with(EndpointEntry::new);
        entry.counters.successes.fetch_add(1, Ordering::Relaxed);
        entry
            .counters
            .last_status
            .store(status as u64, Ordering::Relaxed);
    }

    pub fn record_failure(&self, provider: &str, endpoint: &str, status: Option<u16>) {
        let entry = self
            .map
            .entry(Self::key(provider, endpoint))
            .or_insert_with(EndpointEntry::new);
        entry.counters.failures.fetch_add(1, Ordering::Relaxed);
        if let Some(s) = status {
            entry
                .counters
                .last_status
                .store(s as u64, Ordering::Relaxed);
        }
    }

    pub fn record_dropped(&self, provider: &str, endpoint: &str) {
        let entry = self
            .map
            .entry(Self::key(provider, endpoint))
            .or_insert_with(EndpointEntry::new);
        entry.counters.dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a rate-limit response and returns how many requests had been
    /// sent to this endpoint since the previous limit was recorded.
    pub fn record_rate_limit(&self, provider: &str, endpoint: &str, status: u16) -> u64 {
        let entry = self
            .map
            .entry(Self::key(provider, endpoint))
            .or_insert_with(EndpointEntry::new);
        entry
            .counters
            .rate_limit_hits
            .fetch_add(1, Ordering::Relaxed);
        entry
            .counters
            .last_status
            .store(status as u64, Ordering::Relaxed);
        *entry.last_rate_limit_at.lock() = Some(Utc::now());

        let since = entry
            .counters
            .requests_since_limit
            .swap(0, Ordering::SeqCst);

        if entry.observed_limit_threshold.load(Ordering::Relaxed) == 0 {
            entry
                .observed_limit_threshold
                .store(since.max(1), Ordering::Relaxed);
        }
        since
    }

    /// Seeds counters from persisted state at startup (does not double count).
    pub fn seed(&self, snap: &EndpointStatSnapshot) {
        let entry = self
            .map
            .entry(Self::key(&snap.provider, &snap.endpoint))
            .or_insert_with(EndpointEntry::new);
        entry
            .counters
            .requests_total
            .store(snap.requests_total, Ordering::Relaxed);
        entry
            .counters
            .requests_since_limit
            .store(snap.requests_since_limit, Ordering::Relaxed);
        entry
            .counters
            .rate_limit_hits
            .store(snap.rate_limit_hits, Ordering::Relaxed);
        entry
            .counters
            .failures
            .store(snap.failures, Ordering::Relaxed);
        entry
            .counters
            .successes
            .store(snap.successes, Ordering::Relaxed);
        entry
            .counters
            .dropped
            .store(snap.dropped, Ordering::Relaxed);
        entry.counters.last_status.store(
            snap.last_status.map(u64::from).unwrap_or(0),
            Ordering::Relaxed,
        );
        entry.counters.last_request_at.store(
            snap.last_request_at
                .map(|dt| dt.timestamp_millis().max(0) as u64)
                .unwrap_or(0),
            Ordering::Relaxed,
        );
        *entry.last_rate_limit_at.lock() = snap.last_rate_limit_at;
        entry.observed_limit_threshold.store(
            snap.observed_limit_threshold.unwrap_or(0),
            Ordering::Relaxed,
        );
        if let Some(note) = &snap.noted_limit {
            *entry.noted_limit.write() = Some(note.clone());
        }
    }

    pub fn snapshot(&self) -> Vec<EndpointStatSnapshot> {
        self.map
            .iter()
            .map(|entry| {
                let (provider, endpoint) = entry.key().clone();
                let c = &entry.value().counters;
                let last_status = match c.last_status.load(Ordering::Relaxed) {
                    0 => None,
                    s => Some(s as u16),
                };
                let last_request_ms = c.last_request_at.load(Ordering::Relaxed);
                EndpointStatSnapshot {
                    provider,
                    endpoint,
                    requests_total: c.requests_total.load(Ordering::Relaxed),
                    requests_since_limit: c.requests_since_limit.load(Ordering::Relaxed),
                    rate_limit_hits: c.rate_limit_hits.load(Ordering::Relaxed),
                    failures: c.failures.load(Ordering::Relaxed),
                    successes: c.successes.load(Ordering::Relaxed),
                    dropped: c.dropped.load(Ordering::Relaxed),
                    last_status,
                    last_request_at: millis_to_dt(last_request_ms),
                    last_rate_limit_at: *entry.value().last_rate_limit_at.lock(),
                    observed_limit_threshold: match entry
                        .value()
                        .observed_limit_threshold
                        .load(Ordering::Relaxed)
                    {
                        0 => None,
                        v => Some(v),
                    },
                    noted_limit: entry.value().noted_limit.read().clone(),
                }
            })
            .collect()
    }
}

fn millis_to_dt(ms: u64) -> Option<DateTime<Utc>> {
    if ms == 0 {
        None
    } else {
        DateTime::<Utc>::from_timestamp_millis(ms as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_requests_and_resets_since_limit() {
        let stats = StatsRegistry::new();
        stats.record_request("tmdb", "movie/{id}", Some("~50/s"));
        stats.record_request("tmdb", "movie/{id}", None);
        stats.record_request("tmdb", "movie/{id}", None);
        stats.record_success("tmdb", "movie/{id}", 200);

        let snaps = stats.snapshot();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].requests_total, 3);
        assert_eq!(snaps[0].requests_since_limit, 3);
        assert_eq!(snaps[0].successes, 1);
        assert_eq!(snaps[0].noted_limit.as_deref(), Some("~50/s"));

        let since = stats.record_rate_limit("tmdb", "movie/{id}", 429);
        assert_eq!(since, 3);

        stats.record_request("tmdb", "movie/{id}", None);
        let snaps = stats.snapshot();
        assert_eq!(snaps[0].requests_total, 4);
        assert_eq!(snaps[0].requests_since_limit, 1);
        assert_eq!(snaps[0].rate_limit_hits, 1);
        assert_eq!(snaps[0].observed_limit_threshold, Some(3));
        assert_eq!(snaps[0].last_status, Some(429));
    }

    #[test]
    fn seed_restores_state() {
        let stats = StatsRegistry::new();
        stats.seed(&EndpointStatSnapshot {
            provider: "tvdb".into(),
            endpoint: "series/{id}".into(),
            requests_total: 42,
            requests_since_limit: 5,
            rate_limit_hits: 1,
            failures: 2,
            successes: 40,
            dropped: 0,
            last_status: Some(200),
            last_request_at: None,
            last_rate_limit_at: None,
            observed_limit_threshold: Some(100),
            noted_limit: Some("note".into()),
        });
        let snaps = stats.snapshot();
        assert_eq!(snaps[0].requests_total, 42);
        assert_eq!(snaps[0].successes, 40);
        assert_eq!(snaps[0].observed_limit_threshold, Some(100));
    }
}
