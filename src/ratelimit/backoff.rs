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
    /// Earliest time the next recovery probe may be sent while the backoff
    /// window is longer than the caller's wait budget. `None` means no probe
    /// has been armed since the last success/restore.
    pub next_probe_at: Option<Instant>,
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
        // `Instant + Duration` panics on overflow; fall back to `now` so a
        // pathological config can never crash the process.
        self.next_allowed_at = Some(now.checked_add(self.delay).unwrap_or(now));
    }

    /// Extends the current backoff window so it is at least `wait` from `now`.
    /// Used to honour an upstream `Retry-After` that is longer than our own
    /// computed backoff.
    pub fn enforce_minimum_wait(&mut self, wait: Duration, now: Instant) {
        if wait.is_zero() {
            return;
        }
        let target = now.checked_add(wait).unwrap_or(now);
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
        self.next_probe_at = None;
    }

    /// Atomically decides whether a single recovery probe may be sent while the
    /// provider is in a backoff longer than the caller's wait budget.
    ///
    /// The first call after entering deep backoff only *arms* the probe timer
    /// (and returns `false`); a probe is allowed once per `interval` thereafter.
    /// This lets a success reset the backoff without letting a flood of callers
    /// all bypass the window at once.
    pub fn try_begin_probe(&mut self, now: Instant, interval: Duration) -> bool {
        match self.next_probe_at {
            None => {
                // Arm the timer; the current caller is still dropped.
                // `checked_add` overflow leaves it unarmed (next call re-arms
                // and drops) so a pathological interval can never cause a
                // probe storm.
                self.next_probe_at = now.checked_add(interval);
                false
            }
            Some(at) if at > now => false,
            Some(_) => {
                self.next_probe_at = now.checked_add(interval);
                true
            }
        }
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
            Some(now.checked_add(remaining).unwrap_or(now))
        };
        self.next_probe_at = None;
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
    fn probe_is_armed_then_limited_to_an_interval() {
        let now = Instant::now();
        let interval = Duration::from_secs(10);
        let mut state = BackoffState::new();

        // First deep-backoff caller only arms the timer and is rejected.
        assert!(!state.try_begin_probe(now, interval));
        // Still inside the interval: rejected.
        assert!(!state.try_begin_probe(now + Duration::from_secs(9), interval));
        // Once the interval elapses a single probe is allowed.
        assert!(state.try_begin_probe(now + Duration::from_secs(10), interval));
        // And the next one is again gated by the interval.
        assert!(!state.try_begin_probe(now + Duration::from_secs(19), interval));
        assert!(state.try_begin_probe(now + Duration::from_secs(20), interval));
    }

    #[test]
    fn success_resets_armed_probe() {
        let now = Instant::now();
        let mut state = BackoffState::new();
        assert!(!state.try_begin_probe(now, Duration::from_secs(10)));
        state.record_success();
        assert!(state.next_probe_at.is_none());
        // After a reset the probe must be re-armed (rejected once) rather than
        // being immediately available.
        assert!(!state.try_begin_probe(now, Duration::from_secs(10)));
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
    fn record_failure_never_panics_on_instant_overflow() {
        let mut c = cfg();
        // Large enough to overflow `Instant::checked_add` while still a valid
        // `Duration` (so `delay_for` itself does not panic).
        let huge = Duration::from_secs(i64::MAX as u64);
        c.base_delay = huge;
        c.max_delay = huge;
        c.factor = 2.0;
        c.jitter = 0.0;
        let now = Instant::now();
        let mut state = BackoffState::new();
        state.record_failure(&c, now);
        assert!(state.next_allowed_at.is_some());
        // A subsequent failure must not panic either.
        state.record_failure(&c, now);
        assert!(state.next_allowed_at.is_some());
    }

    #[test]
    fn enforce_minimum_wait_never_panics_on_instant_overflow() {
        let now = Instant::now();
        let mut state = BackoffState::new();
        state.enforce_minimum_wait(Duration::MAX, now);
        assert!(state.next_allowed_at.is_some());
        // The existing window must be preserved rather than moved backwards.
        state.enforce_minimum_wait(Duration::from_secs(5), now);
        assert_eq!(state.remaining_wait(now), Duration::from_secs(5));
    }

    #[test]
    fn restore_never_panics_on_instant_overflow() {
        let now = Instant::now();
        let mut state = BackoffState::new();
        state.restore(3, Duration::MAX, now);
        assert!(state.next_allowed_at.is_some());
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
