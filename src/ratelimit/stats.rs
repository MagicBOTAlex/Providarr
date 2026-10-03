use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use serde::Serialize;

type Key = (String, String);

/// Hard cap on distinct `(provider, endpoint)` entries. Endpoints are mostly
/// bounded by the provider's route templates, but a caller can pass arbitrary
/// strings, so this prevents unbounded memory growth (cardinality DoS).
const MAX_ENTRIES: usize = 10_000;

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
    /// Serialises the "make room then insert" path so the `MAX_ENTRIES` bound
    /// holds under concurrency. A plain DashMap `len()` check is racy: many
    /// threads can all observe `len() < MAX` and overshoot. Lock ordering is
    /// always `capacity_lock` -> DashMap shard -> inner locks; `snapshot` never
    /// takes `capacity_lock`, so there is no cycle.
    capacity_lock: Mutex<()>,
}

impl StatsRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    fn key(provider: &str, endpoint: &str) -> Key {
        (provider.to_string(), endpoint.to_string())
    }

    /// Runs `f` against the entry for `(provider, endpoint)`, inserting it first
    /// if absent. Insertion is bounded by `MAX_ENTRIES` and serialised by
    /// `capacity_lock` so the cap cannot be overshot; existing keys are updated
    /// on the lock-free fast path.
    fn with_entry<R>(
        &self,
        provider: &str,
        endpoint: &str,
        f: impl FnOnce(&EndpointEntry) -> R,
    ) -> R {
        let key = Self::key(provider, endpoint);
        if let Some(entry) = self.map.get(&key) {
            return f(entry.value());
        }

        let _guard = self.capacity_lock.lock();
        // Re-check under the lock: another thread may have inserted it while we
        // were waiting.
        if let Some(entry) = self.map.get(&key) {
            return f(entry.value());
        }
        self.evict_for_insert_locked();
        let entry = self.map.entry(key).or_insert_with(EndpointEntry::new);
        f(entry.value())
    }

    /// Called with `capacity_lock` held and the target key absent. Removes
    /// enough entries to guarantee the following insert cannot exceed the cap.
    /// Keys are collected before removal so no DashMap references are held
    /// across mutation.
    fn evict_for_insert_locked(&self) {
        if self.map.len() < MAX_ENTRIES {
            return;
        }
        let to_remove = self.map.len() - MAX_ENTRIES + 1;
        let keys: Vec<Key> = self
            .map
            .iter()
            .take(to_remove)
            .map(|entry| entry.key().clone())
            .collect();
        for key in keys {
            self.map.remove(&key);
        }
    }

    pub fn record_request(&self, provider: &str, endpoint: &str, noted_limit: Option<&str>) {
        self.with_entry(provider, endpoint, |entry| {
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
        });
    }

    pub fn record_success(&self, provider: &str, endpoint: &str, status: u16) {
        self.with_entry(provider, endpoint, |entry| {
            entry.counters.successes.fetch_add(1, Ordering::Relaxed);
            entry
                .counters
                .last_status
                .store(status as u64, Ordering::Relaxed);
        });
    }

    pub fn record_failure(&self, provider: &str, endpoint: &str, status: Option<u16>) {
        self.with_entry(provider, endpoint, |entry| {
            entry.counters.failures.fetch_add(1, Ordering::Relaxed);
            if let Some(s) = status {
                entry
                    .counters
                    .last_status
                    .store(s as u64, Ordering::Relaxed);
            }
        });
    }

    pub fn record_dropped(&self, provider: &str, endpoint: &str) {
        self.with_entry(provider, endpoint, |entry| {
            entry.counters.dropped.fetch_add(1, Ordering::Relaxed);
        });
    }

    /// Records a rate-limit response and returns how many requests had been
    /// sent to this endpoint since the previous limit was recorded.
    pub fn record_rate_limit(&self, provider: &str, endpoint: &str, status: u16) -> u64 {
        self.with_entry(provider, endpoint, |entry| {
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
        })
    }

    /// Seeds counters from persisted state at startup (does not double count).
    /// Uses the same bounded insertion path as the `record_*` methods so a
    /// hostile/large persisted set cannot exceed `MAX_ENTRIES`.
    pub fn seed(&self, snap: &EndpointStatSnapshot) {
        self.with_entry(&snap.provider, &snap.endpoint, |entry| {
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
        });
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

    #[test]
    fn map_is_bounded_by_max_entries() {
        let stats = StatsRegistry::new();
        for i in 0..(MAX_ENTRIES + 100) {
            stats.record_request("provider", &format!("endpoint/{i}"), None);
        }
        assert!(
            stats.map.len() <= MAX_ENTRIES,
            "map grew to {} entries",
            stats.map.len()
        );
        assert!(stats.snapshot().len() <= MAX_ENTRIES);
    }

    #[test]
    fn map_bound_holds_under_concurrency() {
        use std::sync::Arc;
        let stats = Arc::new(StatsRegistry::new());
        let mut handles = Vec::new();
        for t in 0..8 {
            let stats = stats.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..(MAX_ENTRIES / 4) {
                    stats.record_request("provider", &format!("t{t}/e{i}"), None);
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        assert!(
            stats.map.len() <= MAX_ENTRIES,
            "map grew to {} entries under concurrency",
            stats.map.len()
        );
        assert!(stats.snapshot().len() <= MAX_ENTRIES);
    }

    fn snap(provider: &str, endpoint: &str) -> EndpointStatSnapshot {
        EndpointStatSnapshot {
            provider: provider.into(),
            endpoint: endpoint.into(),
            requests_total: 1,
            requests_since_limit: 1,
            rate_limit_hits: 0,
            failures: 0,
            successes: 1,
            dropped: 0,
            last_status: Some(200),
            last_request_at: None,
            last_rate_limit_at: None,
            observed_limit_threshold: None,
            noted_limit: None,
        }
    }

    #[test]
    fn seed_is_bounded_by_max_entries() {
        let stats = StatsRegistry::new();
        for i in 0..(MAX_ENTRIES + 100) {
            stats.seed(&snap("provider", &format!("endpoint/{i}")));
        }
        assert!(
            stats.map.len() <= MAX_ENTRIES,
            "seed grew map to {} entries",
            stats.map.len()
        );
    }

    #[test]
    fn existing_key_is_not_evicted_when_full() {
        let stats = StatsRegistry::new();
        for i in 0..MAX_ENTRIES {
            stats.record_request("provider", &format!("endpoint/{i}"), None);
        }
        // Updating a key already present must not drop that key's counters.
        stats.record_success("provider", "endpoint/0", 200);
        let snaps = stats.snapshot();
        let entry = snaps
            .iter()
            .find(|s| s.endpoint == "endpoint/0")
            .expect("existing endpoint was evicted");
        assert_eq!(entry.successes, 1);
    }
}
