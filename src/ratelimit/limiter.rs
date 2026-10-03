use std::{
    collections::HashMap,
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, Instant},
};

use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::{BackoffConfig, ProviderConfig};
use crate::error::AppError;
use crate::ratelimit::{backoff::BackoffState, stats::StatsRegistry};

#[derive(Debug, thiserror::Error)]
pub enum LimitError {
    #[error("dropped after waiting {wait_ms}ms: {reason}")]
    Dropped { wait_ms: u64, reason: String },
    #[error("rate limiter closed")]
    Closed,
}

/// Everything needed to talk to one upstream provider safely:
/// a steady-state rate limiter, a progressive backoff gate, a concurrency
/// semaphore and shared accounting.
pub struct ProviderRuntime {
    name: String,
    config: ProviderConfig,
    backoff_config: BackoffConfig,
    limiter: DefaultDirectRateLimiter,
    endpoint_limiters: HashMap<String, DefaultDirectRateLimiter>,
    backoff: Mutex<BackoffState>,
    semaphore: Arc<Semaphore>,
    http: reqwest::Client,
    stats: Arc<StatsRegistry>,
}

impl ProviderRuntime {
    pub fn new(
        name: impl Into<String>,
        config: ProviderConfig,
        backoff_config: BackoffConfig,
        stats: Arc<StatsRegistry>,
    ) -> Result<Self, AppError> {
        let limiter = RateLimiter::direct(quota_for(&config));
        let endpoint_limiters = config
            .endpoint_rps
            .iter()
            .map(|(segment, rps)| {
                (
                    segment.clone(),
                    RateLimiter::direct(quota_for_rps(*rps, endpoint_burst(*rps, config.burst))),
                )
            })
            .collect();
        let semaphore = Arc::new(Semaphore::new(config.max_concurrency.max(1)));
        let http = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .pool_idle_timeout(Duration::from_secs(30))
            .user_agent(concat!("Providarr/", env!("CARGO_PKG_VERSION")))
            // Never follow redirects: an upstream (or an attacker who controls
            // one) must not be able to bounce us to internal hosts (SSRF).
            .redirect(reqwest::redirect::Policy::none())
            // Ignore ambient proxy env vars so they cannot redirect egress
            // without an explicit, reviewed configuration change.
            .no_proxy()
            .build()
            .map_err(AppError::Http)?;

        Ok(Self {
            name: name.into(),
            config,
            backoff_config,
            limiter,
            endpoint_limiters,
            backoff: Mutex::new(BackoffState::new()),
            semaphore,
            http,
            stats,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn config(&self) -> &ProviderConfig {
        &self.config
    }

    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }

    pub fn stats(&self) -> &Arc<StatsRegistry> {
        &self.stats
    }

    pub fn effective_timeout(&self) -> Duration {
        self.backoff
            .lock()
            .effective_timeout(self.config.request_timeout, &self.backoff_config)
    }

    pub fn backoff_remaining(&self) -> Duration {
        self.backoff.lock().remaining_wait(Instant::now())
    }

    pub fn consecutive_failures(&self) -> u32 {
        self.backoff.lock().consecutive_failures
    }

    /// Maximum in-request retries after the first attempt when upstream fails.
    pub fn max_retries(&self) -> u32 {
        self.backoff_config.max_retries
    }

    /// (consecutive failures, remaining backoff wait) for persistence.
    pub fn backoff_snapshot(&self) -> (u32, Duration) {
        self.backoff.lock().snapshot(Instant::now())
    }

    /// Restore persisted backoff state (e.g. after a restart).
    pub fn restore_backoff(&self, failures: u32, remaining: Duration) {
        self.backoff
            .lock()
            .restore(failures, remaining, Instant::now());
    }

    /// Per-endpoint limiter (first path segment) if configured, else the default.
    fn limiter_for(&self, endpoint: &str) -> &DefaultDirectRateLimiter {
        let segment = endpoint.split('/').next().unwrap_or(endpoint);
        self.endpoint_limiters.get(segment).unwrap_or(&self.limiter)
    }

    /// Waits for a rate-limit permit, the backoff window and a concurrency slot.
    ///
    /// A single deadline (`drop_after_wait`) spans all three stages, so the total
    /// time spent waiting can never exceed the budget even if each stage fits
    /// individually.
    pub async fn acquire(&self, endpoint: &str) -> Result<OwnedSemaphorePermit, LimitError> {
        let budget = self.backoff_config.drop_after_wait;
        let deadline = tokio::time::Instant::now() + budget;

        let wait = self.backoff_remaining();
        if wait > budget {
            self.stats.record_dropped(&self.name, endpoint);
            return Err(LimitError::Dropped {
                wait_ms: wait.as_millis() as u64,
                reason: "provider is in backoff".to_string(),
            });
        }
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }

        let limiter = self.limiter_for(endpoint);
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero()
            || tokio::time::timeout(remaining, limiter.until_ready())
                .await
                .is_err()
        {
            self.stats.record_dropped(&self.name, endpoint);
            return Err(LimitError::Dropped {
                wait_ms: budget.as_millis() as u64,
                reason: "rate limiter queue wait exceeded total budget".to_string(),
            });
        }

        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            self.stats.record_dropped(&self.name, endpoint);
            return Err(LimitError::Dropped {
                wait_ms: budget.as_millis() as u64,
                reason: "concurrency slot wait exceeded total budget".to_string(),
            });
        }

        match tokio::time::timeout(remaining, self.semaphore.clone().acquire_owned()).await {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(_)) => Err(LimitError::Closed),
            Err(_) => {
                self.stats.record_dropped(&self.name, endpoint);
                Err(LimitError::Dropped {
                    wait_ms: budget.as_millis() as u64,
                    reason: "concurrency slot wait exceeded total budget".to_string(),
                })
            }
        }
    }

    /// Extends the provider backoff so the next attempt waits at least the
    /// upstream-provided `Retry-After` window.
    pub fn enforce_retry_after(&self, wait: Duration) {
        self.backoff
            .lock()
            .enforce_minimum_wait(wait, Instant::now());
    }

    pub fn on_success(&self, endpoint: &str, status: u16) {
        self.backoff.lock().record_success();
        self.stats.record_success(&self.name, endpoint, status);
    }

    pub fn on_failure(&self, endpoint: &str, status: Option<u16>) {
        self.backoff
            .lock()
            .record_failure(&self.backoff_config, Instant::now());
        self.stats.record_failure(&self.name, endpoint, status);
    }

    /// Returns how many requests were sent to this endpoint since the last
    /// recorded rate limit.
    pub fn on_rate_limited(&self, endpoint: &str, status: u16) -> u64 {
        self.backoff
            .lock()
            .record_failure(&self.backoff_config, Instant::now());
        self.stats.record_rate_limit(&self.name, endpoint, status)
    }
}

fn quota_for(config: &ProviderConfig) -> Quota {
    quota_for_rps(config.requests_per_second, config.burst)
}

/// Burst for a per-endpoint limiter. Inheriting the provider-wide burst would
/// defeat a low `endpoint_rps` override (e.g. 2 req/s with a burst of 40), so
/// cap it at the endpoint's per-second rate.
fn endpoint_burst(requests_per_second: f64, provider_burst: u32) -> u32 {
    let from_rps = requests_per_second.ceil().max(1.0) as u32;
    provider_burst.min(from_rps).max(1)
}

fn quota_for_rps(requests_per_second: f64, burst: u32) -> Quota {
    let burst = NonZeroU32::new(burst.max(1)).unwrap_or(NonZeroU32::new(1).unwrap());
    if requests_per_second <= 0.0 {
        // Effectively unlimited; still bounded by the concurrency semaphore.
        return Quota::with_period(Duration::from_nanos(1))
            .expect("non-zero period")
            .allow_burst(NonZeroU32::new(u32::MAX).unwrap());
    }
    // `1 / rps` can overflow a `Duration` (or be non-finite) for very small
    // positive rates, and `Duration::from_secs_f64` would panic. Clamp to a
    // sane range and fall back to a one-second period if conversion fails; we
    // must never panic on user-supplied config.
    let secs = (1.0 / requests_per_second).clamp(1e-9, 86_400.0);
    let period = Duration::try_from_secs_f64(secs).unwrap_or(Duration::from_secs(1));
    Quota::with_period(period)
        .expect("period is non-zero")
        .allow_burst(burst)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;

    fn test_config(rps: f64, burst: u32) -> ProviderConfig {
        let mut c = ProviderConfig::tmdb_default();
        c.requests_per_second = rps;
        c.burst = burst;
        c.max_concurrency = 2;
        c.request_timeout = Duration::from_millis(200);
        c
    }

    #[test]
    fn endpoint_burst_is_capped_by_rps() {
        assert_eq!(endpoint_burst(2.0, 40), 2);
        assert_eq!(endpoint_burst(0.5, 40), 1);
        assert_eq!(endpoint_burst(10.0, 40), 10);
        assert_eq!(endpoint_burst(100.0, 40), 40);
    }

    #[test]
    fn quota_for_rps_never_panics_for_extreme_rates() {
        // Very small positive rates previously overflowed `Duration::from_secs_f64`.
        let _ = quota_for_rps(f64::MIN_POSITIVE, 1);
        let _ = quota_for_rps(1e-300, 5);
        // Very large rates and non-finite inputs must also be handled.
        let _ = quota_for_rps(f64::MAX, 1);
        let _ = quota_for_rps(f64::INFINITY, 1);
        let _ = quota_for_rps(f64::NAN, 1);
        // `<= 0.0` stays on the effectively-unlimited path.
        let _ = quota_for_rps(0.0, 1);
        let _ = quota_for_rps(-1.0, 1);
    }

    #[tokio::test]
    async fn acquire_returns_permits_and_success_resets_backoff() {
        let stats = Arc::new(StatsRegistry::new());
        let rt = ProviderRuntime::new(
            "tmdb",
            test_config(1000.0, 100),
            AppConfig::default().backoff,
            stats.clone(),
        )
        .unwrap();

        let permit = rt.acquire("movie/{id}").await.unwrap();
        drop(permit);
        rt.on_success("movie/{id}", 200);
        assert_eq!(rt.consecutive_failures(), 0);
    }

    #[tokio::test]
    async fn backoff_drops_requests_beyond_budget() {
        let stats = Arc::new(StatsRegistry::new());
        let mut cfg = AppConfig::default();
        cfg.backoff.base_delay = Duration::from_secs(5);
        cfg.backoff.jitter = 0.0;
        cfg.backoff.drop_after_wait = Duration::from_millis(50);
        let rt = ProviderRuntime::new("tmdb", test_config(1000.0, 100), cfg.backoff, stats.clone())
            .unwrap();

        rt.on_failure("movie/{id}", Some(500));

        let err = rt.acquire("movie/{id}").await.unwrap_err();
        assert!(matches!(err, LimitError::Dropped { .. }));
        let snaps = stats.snapshot();
        assert_eq!(snaps[0].dropped, 1);
    }

    #[tokio::test]
    async fn rate_limit_increments_and_lengthens_backoff() {
        let stats = Arc::new(StatsRegistry::new());
        let mut cfg = AppConfig::default();
        cfg.backoff.base_delay = Duration::from_millis(10);
        cfg.backoff.jitter = 0.0;
        let rt = ProviderRuntime::new("tvdb", test_config(1000.0, 100), cfg.backoff, stats.clone())
            .unwrap();

        rt.stats().record_request("tvdb", "series/{id}", None);
        let since = rt.on_rate_limited("series/{id}", 429);
        assert_eq!(since, 1);
        assert_eq!(rt.consecutive_failures(), 1);
        assert!(rt.effective_timeout() >= Duration::from_millis(200));
    }
}
