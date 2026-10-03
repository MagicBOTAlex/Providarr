use std::time::{Duration, Instant};

use rand::RngExt;

use crate::config::BackoffConfig;

/// Progressive backoff state for a single upstream provider.
///
/// The delay grows exponentially with consecutive failures (including rate
/// limit responses) and is used both to gate outgoing requests and to lengthen
/// the per-request timeout. Any success resets it.
#[derive(Debug, Clone, Default)]
pub struct BackoffState {
    pub consecutive_failures: u32,
    pub delay: Duration,
    pub next_allowed_at: Option<Instant>,
}

impl BackoffState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Deterministic delay for a 1-based failure count, before jitter.
    pub fn delay_for(failures: u32, cfg: &BackoffConfig) -> Duration {
        if failures == 0 {
            return Duration::ZERO;
        }
        let exp = cfg.factor.max(1.0).powi((failures - 1) as i32);
        let secs = (cfg.base_delay.as_secs_f64() * exp).min(cfg.max_delay.as_secs_f64());
        Duration::from_secs_f64(secs.max(0.0))
    }

    pub fn record_failure(&mut self, cfg: &BackoffConfig, now: Instant) {
        self.consecutive_failures = self
            .consecutive_failures
            .saturating_add(1)
            .min(cfg.max_consecutive_failures.max(1));
        let base = Self::delay_for(self.consecutive_failures, cfg);
        // Jitter is applied to the already-capped delay, then clamped so the
        // randomised value can never exceed `max_delay`.
        self.delay = apply_jitter(base, cfg.jitter).min(cfg.max_delay);
        self.next_allowed_at = Some(now + self.delay);
    }

    /// Extends the current backoff window so it is at least `wait` from `now`.
    /// Used to honour an upstream `Retry-After` that is longer than our own
    /// computed backoff.
    pub fn enforce_minimum_wait(&mut self, wait: Duration, now: Instant) {
        if wait.is_zero() {
            return;
        }
        let target = now + wait;
        if self.next_allowed_at.is_none_or(|at| at < target) {
            self.next_allowed_at = Some(target);
        }
        if self.delay < wait {
            self.delay = wait;
        }
    }

    pub fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.delay = Duration::ZERO;
        self.next_allowed_at = None;
    }

    pub fn remaining_wait(&self, now: Instant) -> Duration {
        match self.next_allowed_at {
            Some(at) if at > now => at - now,
            _ => Duration::ZERO,
        }
    }

    pub fn is_in_backoff(&self, now: Instant) -> bool {
        self.remaining_wait(now) > Duration::ZERO
    }

    /// Snapshot for persistence: (consecutive failures, remaining wait).
    pub fn snapshot(&self, now: Instant) -> (u32, Duration) {
        (self.consecutive_failures, self.remaining_wait(now))
    }

    /// Restore from persistence (e.g. after a restart).
    pub fn restore(&mut self, failures: u32, remaining: Duration, now: Instant) {
        self.consecutive_failures = failures;
        self.delay = remaining;
        self.next_allowed_at = if remaining.is_zero() {
            None
        } else {
            Some(now + remaining)
        };
    }

    /// Timeout grows with the failure count so a struggling upstream gets more
    /// room before we give up on it.
    pub fn effective_timeout(&self, base: Duration, cfg: &BackoffConfig) -> Duration {
        if self.consecutive_failures == 0 {
            return base;
        }
        let mult = cfg
            .factor
            .max(1.0)
            .powi(self.consecutive_failures as i32 - 1);
        // Cap with `max_request_timeout`, but never below the provider's base
        // timeout (a misconfigured cap must not shrink the request window).
        let cap = cfg
            .max_request_timeout
            .as_secs_f64()
            .max(base.as_secs_f64());
        let secs = (base.as_secs_f64() * mult).max(base.as_secs_f64()).min(cap);
        Duration::from_secs_f64(secs)
    }
}

/// Applies multiplicative jitter in `[1 - jitter, 1 + jitter]`.
pub fn apply_jitter(delay: Duration, jitter: f64) -> Duration {
    if jitter <= 0.0 || delay.is_zero() {
        return delay;
    }
    let mut rng = rand::rng();
    let factor = 1.0 + jitter * rng.random_range(-1.0_f64..1.0_f64);
    Duration::from_secs_f64((delay.as_secs_f64() * factor).max(0.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> BackoffConfig {
        BackoffConfig {
            base_delay: Duration::from_millis(500),
            factor: 2.0,
            max_delay: Duration::from_secs(60),
            jitter: 0.0,
            max_consecutive_failures: 8,
            drop_after_wait: Duration::from_secs(10),
            max_retries: 0,
            max_request_timeout: Duration::from_secs(120),
        }
    }

    #[test]
    fn delay_grows_exponentially_and_caps() {
        let c = cfg();
        assert_eq!(BackoffState::delay_for(0, &c), Duration::ZERO);
        assert_eq!(BackoffState::delay_for(1, &c), Duration::from_millis(500));
        assert_eq!(BackoffState::delay_for(2, &c), Duration::from_secs(1));
        assert_eq!(BackoffState::delay_for(3, &c), Duration::from_secs(2));
        assert_eq!(BackoffState::delay_for(4, &c), Duration::from_secs(4));
        // capped at max_delay
        assert_eq!(BackoffState::delay_for(20, &c), Duration::from_secs(60));
    }

    #[test]
    fn failure_sets_next_allowed_and_success_resets() {
        let c = cfg();
        let start = Instant::now();
        let mut state = BackoffState::new();

        state.record_failure(&c, start);
        assert_eq!(state.consecutive_failures, 1);
        assert_eq!(state.delay, Duration::from_millis(500));
        assert_eq!(state.remaining_wait(start), Duration::from_millis(500));
        assert!(state.is_in_backoff(start));

        // second failure doubles the delay
        state.record_failure(&c, start);
        assert_eq!(state.delay, Duration::from_secs(1));

        state.record_success();
        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.remaining_wait(start), Duration::ZERO);
        assert!(!state.is_in_backoff(start));
    }

    #[test]
    fn failure_count_is_capped() {
        let c = cfg();
        let now = Instant::now();
        let mut state = BackoffState::new();
        for _ in 0..100 {
            state.record_failure(&c, now);
        }
        assert_eq!(state.consecutive_failures, c.max_consecutive_failures);
    }

    #[test]
    fn timeout_lengthens_with_failures() {
        let c = cfg();
        let base = Duration::from_secs(1);
        let now = Instant::now();
        let mut state = BackoffState::new();
        assert_eq!(state.effective_timeout(base, &c), base);
        state.record_failure(&c, now);
        assert_eq!(state.effective_timeout(base, &c), Duration::from_secs(1));
        state.record_failure(&c, now);
        assert_eq!(state.effective_timeout(base, &c), Duration::from_secs(2));
        state.record_failure(&c, now);
        assert_eq!(state.effective_timeout(base, &c), Duration::from_secs(4));
    }

    #[test]
    fn snapshot_and_restore_round_trip() {
        let c = cfg();
        let start = Instant::now();
        let mut state = BackoffState::new();
        state.record_failure(&c, start);
        state.record_failure(&c, start);

        let (failures, remaining) = state.snapshot(start);
        assert_eq!(failures, 2);
        assert_eq!(remaining, Duration::from_secs(1));

        let mut restored = BackoffState::new();
        let later = start + Duration::from_millis(400);
        restored.restore(failures, remaining, later);
        assert_eq!(restored.consecutive_failures, 2);
        assert_eq!(
            restored.remaining_wait(later),
            Duration::from_secs(1),
            "restores the full remaining wait from the restore point"
        );
    }

    #[test]
    fn jitter_stays_bounded() {
        for _ in 0..50 {
            let d = apply_jitter(Duration::from_secs(10), 0.2);
            assert!(d >= Duration::from_secs(8) && d <= Duration::from_secs(12));
        }
    }

    #[test]
    fn jitter_is_clamped_after_the_max_delay_cap() {
        let mut c = cfg();
        c.base_delay = Duration::from_secs(60);
        c.max_delay = Duration::from_secs(60);
        c.jitter = 1.0;
        let now = Instant::now();
        let mut state = BackoffState::new();
        for _ in 0..20 {
            state.record_failure(&c, now);
            assert!(
                state.delay <= c.max_delay,
                "jittered delay {} exceeded cap {}",
                state.delay.as_secs_f64(),
                c.max_delay.as_secs_f64()
            );
        }
    }

    #[test]
    fn timeout_never_shrinks_below_base() {
        let mut c = cfg();
        // A cap smaller than the base timeout must not shrink it.
        c.max_request_timeout = Duration::from_millis(500);
        let base = Duration::from_secs(10);
        let now = Instant::now();
        let mut state = BackoffState::new();
        state.record_failure(&c, now);
        assert_eq!(state.effective_timeout(base, &c), base);
        state.record_failure(&c, now);
        assert_eq!(state.effective_timeout(base, &c), base);
    }

    #[test]
    fn retry_after_extends_the_window() {
        let c = cfg();
        let now = Instant::now();
        let mut state = BackoffState::new();
        state.record_failure(&c, now);
        let before = state.remaining_wait(now);
        state.enforce_minimum_wait(Duration::from_secs(5), now);
        assert!(state.remaining_wait(now) >= before);
        assert_eq!(state.remaining_wait(now), Duration::from_secs(5));
        // A shorter Retry-After never shortens the existing window.
        state.enforce_minimum_wait(Duration::from_secs(1), now);
        assert_eq!(state.remaining_wait(now), Duration::from_secs(5));
    }
}
