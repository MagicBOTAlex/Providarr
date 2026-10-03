use std::{collections::HashMap, path::Path, time::Duration};

use serde::{Deserialize, Serialize};

use crate::error::AppError;

/// Upper bound on `backoff.max_retries` so a misconfiguration cannot make a
/// single inbound request hold a connection for an unbounded number of tries.
const MAX_RETRIES_CAP: u32 = 20;

/// Upper bound on `inbound.max_concurrent`.
const MAX_CONCURRENT_CAP: u32 = 65_535;

/// Upper bound on `search.hydrate_limit` so a misconfiguration cannot make a
/// single search hydrate the whole provider result set.
const MAX_HYDRATE_LIMIT: usize = 100;

/// Upper bound on `backoff.max_delay` so a hostile config cannot overflow
/// `Instant` arithmetic when scheduling the next retry.
const MAX_BACKOFF_MAX_DELAY: Duration = Duration::from_secs(3_600);

/// Upper bound on `backoff.max_request_timeout` (10 minutes).
const MAX_BACKOFF_MAX_REQUEST_TIMEOUT: Duration = Duration::from_secs(600);

/// Upper bound on `backoff.max_consecutive_failures` so a hostile config cannot
/// pin a provider in an effectively unbounded lockout.
const MAX_CONSECUTIVE_FAILURES_CAP: u32 = 100;

/// Upper bound on `cache.max_ttl` (365 days).
const MAX_CACHE_TTL: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// Upper bound on `cache.max_body_bytes` (64 MiB).
const MAX_CACHE_BODY_BYTES: usize = 64 * 1024 * 1024;

/// Smallest non-zero requests/second value. Periods are the reciprocal, so
/// anything smaller risks overflowing the rate-limiter's `Instant`.
const MIN_REQUESTS_PER_SECOND: f64 = 1e-6;

/// Rejects rates that are non-finite, negative, or too small to invert safely.
/// `0.0` is allowed and means "disabled".
fn invalid_requests_per_second(value: f64) -> bool {
    !value.is_finite() || value < 0.0 || (value > 0.0 && value < MIN_REQUESTS_PER_SECOND)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub cache: CacheConfig,
    pub search: SearchConfig,
    pub backoff: BackoffConfig,
    pub replay: ReplayConfig,
    pub inbound: InboundLimiterConfig,
    pub api_auth: ApiAuthConfig,
    pub logging: LoggingConfig,
    pub providers: HashMap<String, ProviderConfig>,
}

impl AppConfig {
    /// Load config from `PROVIDARR_CONFIG` (default `config/config.json`).
    ///
    /// A missing file is not fatal: built-in defaults are used. Environment
    /// variables override the values that matter for deployment.
    pub fn load() -> Result<Self, AppError> {
        let path =
            std::env::var("PROVIDARR_CONFIG").unwrap_or_else(|_| "config/config.json".to_string());

        let mut config = if Path::new(&path).exists() {
            let raw = std::fs::read_to_string(&path)?;
            serde_json::from_str(&raw)
                .map_err(|e| AppError::Config(format!("failed to parse {path}: {e}")))?
        } else {
            tracing::warn!(path, "config file not found, using defaults");
            AppConfig::default()
        };

        if let Ok(url) = std::env::var("DATABASE_URL") {
            config.database.url = url;
        }

        apply_inbound_env_overrides(&mut config)?;
        apply_logging_env_overrides(&mut config)?;
        apply_api_auth_env_overrides(&mut config)?;

        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), AppError> {
        if self.database.url.trim().is_empty() {
            return Err(AppError::Config(
                "database.url is empty (set DATABASE_URL or config.database.url)".into(),
            ));
        }
        if self.database.url.contains("providarr:providarr") {
            tracing::warn!(
                "database.url uses the default providarr:providarr credentials; rotate them for any non-local deployment"
            );
        }

        if self.search.hydrate_limit > MAX_HYDRATE_LIMIT {
            return Err(AppError::Config(format!(
                "search.hydrate_limit must be <= {MAX_HYDRATE_LIMIT} (got {})",
                self.search.hydrate_limit
            )));
        }

        let backoff = &self.backoff;
        if backoff.drop_after_wait.is_zero() {
            return Err(AppError::Config(
                "backoff.drop_after_wait must be > 0".into(),
            ));
        }
        if !backoff.factor.is_finite() || backoff.factor < 1.0 {
            return Err(AppError::Config(
                "backoff.factor must be a finite number >= 1".into(),
            ));
        }
        if !backoff.jitter.is_finite() || !(0.0..=1.0).contains(&backoff.jitter) {
            return Err(AppError::Config(
                "backoff.jitter must be between 0 and 1".into(),
            ));
        }
        if backoff.max_retries > MAX_RETRIES_CAP {
            return Err(AppError::Config(format!(
                "backoff.max_retries must be <= {MAX_RETRIES_CAP} (got {})",
                backoff.max_retries
            )));
        }
        if backoff.max_consecutive_failures == 0 {
            return Err(AppError::Config(
                "backoff.max_consecutive_failures must be > 0".into(),
            ));
        }
        if backoff.max_consecutive_failures > MAX_CONSECUTIVE_FAILURES_CAP {
            return Err(AppError::Config(format!(
                "backoff.max_consecutive_failures must be <= {MAX_CONSECUTIVE_FAILURES_CAP} (got {})",
                backoff.max_consecutive_failures
            )));
        }
        if backoff.max_delay > MAX_BACKOFF_MAX_DELAY {
            return Err(AppError::Config(format!(
                "backoff.max_delay must be <= {}s (got {}s)",
                MAX_BACKOFF_MAX_DELAY.as_secs(),
                backoff.max_delay.as_secs()
            )));
        }
        if backoff.max_request_timeout > MAX_BACKOFF_MAX_REQUEST_TIMEOUT {
            return Err(AppError::Config(format!(
                "backoff.max_request_timeout must be <= {}s (got {}s)",
                MAX_BACKOFF_MAX_REQUEST_TIMEOUT.as_secs(),
                backoff.max_request_timeout.as_secs()
            )));
        }

        for (name, provider) in &self.providers {
            if provider.base_url.trim().is_empty() {
                return Err(AppError::Config(format!(
                    "providers.{name}.base_url is empty"
                )));
            }
            if invalid_requests_per_second(provider.requests_per_second) {
                return Err(AppError::Config(format!(
                    "providers.{name}.requests_per_second must be exactly 0 or a finite number >= {MIN_REQUESTS_PER_SECOND}"
                )));
            }
            if provider.burst == 0 {
                return Err(AppError::Config(format!(
                    "providers.{name}.burst must be > 0"
                )));
            }
            if provider.max_concurrency == 0 {
                return Err(AppError::Config(format!(
                    "providers.{name}.max_concurrency must be > 0"
                )));
            }
            for (segment, rps) in &provider.endpoint_rps {
                if invalid_requests_per_second(*rps) {
                    return Err(AppError::Config(format!(
                        "providers.{name}.endpoint_rps.{segment} must be exactly 0 or a finite number >= {MIN_REQUESTS_PER_SECOND}"
                    )));
                }
            }
        }

        let inbound = &self.inbound;
        if !inbound.enabled {
            tracing::warn!(
                "inbound rate limiting is disabled; requests are not throttled (set PROVIDARR_INBOUND_ENABLED=true to enable)"
            );
        }
        if !inbound.requests_per_second.is_finite() || inbound.requests_per_second < 0.0 {
            return Err(AppError::Config(
                "inbound.requests_per_second must be a finite number >= 0".into(),
            ));
        }
        if !inbound.global_requests_per_second.is_finite()
            || inbound.global_requests_per_second < 0.0
        {
            return Err(AppError::Config(
                "inbound.global_requests_per_second must be a finite number >= 0".into(),
            ));
        }
        if inbound.global_requests_per_second > 0.0 && inbound.global_burst == 0 {
            return Err(AppError::Config(
                "inbound.global_burst must be > 0 when the global limiter is enabled".into(),
            ));
        }
        if inbound.max_concurrent > MAX_CONCURRENT_CAP {
            return Err(AppError::Config(format!(
                "inbound.max_concurrent must be <= {MAX_CONCURRENT_CAP} (got {})",
                inbound.max_concurrent
            )));
        }

        if self.cache.max_body_bytes == 0 {
            return Err(AppError::Config("cache.max_body_bytes must be > 0".into()));
        }
        if self.cache.max_body_bytes > MAX_CACHE_BODY_BYTES {
            return Err(AppError::Config(format!(
                "cache.max_body_bytes must be <= {MAX_CACHE_BODY_BYTES} (got {})",
                self.cache.max_body_bytes
            )));
        }
        if self.cache.max_ttl > MAX_CACHE_TTL {
            return Err(AppError::Config(format!(
                "cache.max_ttl must be <= {}s (got {}s)",
                MAX_CACHE_TTL.as_secs(),
                self.cache.max_ttl.as_secs()
            )));
        }
        if self.cache.default_ttl > self.cache.max_ttl {
            return Err(AppError::Config(
                "cache.default_ttl must be <= cache.max_ttl".into(),
            ));
        }
        if self.cache.stale_if_error > self.cache.max_ttl {
            return Err(AppError::Config(
                "cache.stale_if_error must be <= cache.max_ttl".into(),
            ));
        }

        if self.api_auth.enabled && self.api_auth.api_key.trim().is_empty() {
            return Err(AppError::Config(
                "api_auth.enabled is true but api_auth.api_key is empty; set PROVIDARR_API_AUTH_KEY or disable api_auth"
                    .into(),
            ));
        }

        Ok(())
    }

    pub fn provider(&self, name: &str) -> Option<&ProviderConfig> {
        self.providers.get(name)
    }
}

/// Applies `PROVIDARR_INBOUND_*` environment overrides on top of the JSON/default config.
///
/// An environment variable that is present but unparseable is a hard error so a
/// typo cannot silently fall back to the default value.
fn apply_inbound_env_overrides(config: &mut AppConfig) -> Result<(), AppError> {
    if let Some(value) = env_parse::<bool>("PROVIDARR_INBOUND_ENABLED", parse_bool)? {
        config.inbound.enabled = value;
    }
    if let Some(value) = env_parse::<f64>("PROVIDARR_INBOUND_RPS", |v| v.trim().parse().ok())? {
        config.inbound.requests_per_second = value;
    }
    if let Some(value) = env_parse::<u32>("PROVIDARR_INBOUND_BURST", |v| v.trim().parse().ok())? {
        config.inbound.burst = value;
    }
    if let Some(value) =
        env_parse::<f64>("PROVIDARR_INBOUND_GLOBAL_RPS", |v| v.trim().parse().ok())?
    {
        config.inbound.global_requests_per_second = value;
    }
    if let Some(value) =
        env_parse::<u32>("PROVIDARR_INBOUND_GLOBAL_BURST", |v| v.trim().parse().ok())?
    {
        config.inbound.global_burst = value;
    }
    if let Some(value) = env_parse::<u32>("PROVIDARR_INBOUND_MAX_CONCURRENT", |v| {
        v.trim().parse().ok()
    })? {
        config.inbound.max_concurrent = value;
    }
    if let Some(value) = env_parse::<bool>("PROVIDARR_INBOUND_TRUST_FORWARDED_FOR", parse_bool)? {
        config.inbound.trust_forwarded_for = value;
    }
    if let Ok(raw) = std::env::var("PROVIDARR_INBOUND_BYPASS") {
        config.inbound.bypass = raw
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(str::to_string)
            .collect();
    }
    Ok(())
}

/// Reads an environment variable and parses it, returning `Ok(None)` when it is
/// unset and an error when it is set but cannot be parsed.
fn env_parse<T>(name: &str, parse: impl Fn(&str) -> Option<T>) -> Result<Option<T>, AppError> {
    match std::env::var(name) {
        Ok(raw) => parse(&raw).map(Some).ok_or_else(|| {
            AppError::Config(format!("{name} is set but not a valid value: {raw:?}"))
        }),
        Err(_) => Ok(None),
    }
}

fn parse_bool(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" | "" => Some(false),
        _ => None,
    }
}

/// Applies `PROVIDARR_LOG_*` environment overrides on top of the JSON/default config.
fn apply_logging_env_overrides(config: &mut AppConfig) -> Result<(), AppError> {
    if let Some(value) = env_parse::<bool>("PROVIDARR_LOG_ENABLED", parse_bool)? {
        config.logging.enabled = value;
    }
    if let Ok(value) = std::env::var("PROVIDARR_LOG_DIR")
        && !value.trim().is_empty()
    {
        config.logging.dir = value;
    }
    if let Some(rotation) = env_parse::<LogRotation>("PROVIDARR_LOG_ROTATION", parse_rotation)? {
        config.logging.rotation = rotation;
    }
    if let Some(value) = env_parse::<usize>("PROVIDARR_LOG_MAX_FILES", |v| v.trim().parse().ok())? {
        config.logging.max_files = value;
    }
    if let Some(value) = env_parse::<bool>("PROVIDARR_LOG_STDOUT", parse_bool)? {
        config.logging.stdout = value;
    }
    Ok(())
}

/// Applies `PROVIDARR_API_AUTH_*` environment overrides on top of the JSON/default config.
///
/// This lets deployments inject the shared secret without committing it to
/// `config/config.json`.
fn apply_api_auth_env_overrides(config: &mut AppConfig) -> Result<(), AppError> {
    if let Some(value) = env_parse::<bool>("PROVIDARR_API_AUTH_ENABLED", parse_bool)? {
        config.api_auth.enabled = value;
    }
    if let Ok(value) = std::env::var("PROVIDARR_API_AUTH_KEY")
        && !value.trim().is_empty()
    {
        config.api_auth.api_key = value;
    }
    if let Ok(value) = std::env::var("PROVIDARR_API_AUTH_HEADER")
        && !value.trim().is_empty()
    {
        config.api_auth.header = value;
    }
    Ok(())
}

fn parse_rotation(raw: &str) -> Option<LogRotation> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "never" | "none" | "off" => Some(LogRotation::Never),
        "minutely" | "minute" => Some(LogRotation::Minutely),
        "hourly" | "hour" => Some(LogRotation::Hourly),
        "daily" | "day" => Some(LogRotation::Daily),
        _ => None,
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        let mut providers = HashMap::new();
        providers.insert("tmdb".to_string(), ProviderConfig::tmdb_default());
        providers.insert("tvdb".to_string(), ProviderConfig::tvdb_default());
        Self {
            server: ServerConfig::default(),
            database: DatabaseConfig::default(),
            cache: CacheConfig::default(),
            search: SearchConfig::default(),
            backoff: BackoffConfig::default(),
            replay: ReplayConfig::default(),
            inbound: InboundLimiterConfig::default(),
            api_auth: ApiAuthConfig::default(),
            logging: LoggingConfig::default(),
            providers,
        }
    }
}

/// Search/list behaviour.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SearchConfig {
    /// How many search/list results are hydrated with full details. Lower means
    /// fewer upstream calls; the first few results are usually what the caller uses.
    pub hydrate_limit: usize,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self { hydrate_limit: 5 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    #[serde(with = "humantime_serde")]
    pub request_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 4155,
            request_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
    pub min_connections: u32,
    #[serde(with = "humantime_serde")]
    pub acquire_timeout: Duration,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: "postgres://providarr:providarr@127.0.0.1:5432/providarr".to_string(),
            max_connections: 10,
            min_connections: 1,
            acquire_timeout: Duration::from_secs(10),
        }
    }
}

/// A `Duration` that (de)serializes as a human string like `"7d"` in JSON.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HumanDuration(#[serde(with = "humantime_serde")] pub Duration);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    pub enabled: bool,
    #[serde(with = "humantime_serde")]
    pub default_ttl: Duration,
    #[serde(with = "humantime_serde")]
    pub max_ttl: Duration,
    /// Floor applied when an upstream `Cache-Control: max-age` is shorter than this
    /// (only when `honor_cache_control` is on). Aggressive caching avoids letting a
    /// short upstream max-age shrink our own cache.
    #[serde(with = "humantime_serde")]
    pub min_ttl: Duration,
    #[serde(with = "humantime_serde")]
    pub negative_ttl: Duration,
    /// How long a `404 Not Found` is cached. Missing resources (specials seasons,
    /// unreleased episodes, bad ids) are stable, so cache them longer than the
    /// generic 4xx `negative_ttl` to avoid re-querying the provider.
    #[serde(with = "humantime_serde")]
    pub not_found_ttl: Duration,
    #[serde(with = "humantime_serde")]
    pub stale_if_error: Duration,
    /// How long a *negative* (4xx) entry may be served stale after it expires.
    /// Kept deliberately short so a transient 4xx does not pin an error for the
    /// full `stale_if_error` window.
    #[serde(with = "humantime_serde")]
    pub negative_stale_if_error: Duration,
    /// Respect upstream `Cache-Control`. Off by default: our per-endpoint policy wins.
    pub honor_cache_control: bool,
    /// Deduplicate concurrent misses for the same key (single-flight).
    pub request_coalescing: bool,
    /// On an expired-but-still-stale entry, serve it immediately and refresh in the
    /// background instead of blocking.
    pub stale_while_revalidate: bool,
    /// Per-resource TTL overrides keyed by one or more leading path segments
    /// (e.g. `movie`, `movie/popular`, `configuration`, `search`). The longest
    /// matching prefix wins, so sub-endpoints can have their own TTL.
    pub endpoint_ttl: HashMap<String, HumanDuration>,
    pub max_body_bytes: usize,
}

impl CacheConfig {
    /// TTL for a request path, matching the longest configured leading-segment
    /// prefix. This lets `movie/popular` or `tv/changes` override the broader
    /// `movie`/`tv` policy instead of being shadowed by the first segment.
    pub fn ttl_for_path(&self, path: &str) -> Duration {
        let path = path.split('?').next().unwrap_or(path);
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

        for len in (1..=segments.len()).rev() {
            let key = segments[..len].join("/");
            if let Some(ttl) = self.endpoint_ttl.get(&key) {
                return ttl.0.min(self.max_ttl);
            }
        }

        self.default_ttl.min(self.max_ttl)
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        let day: u64 = 24 * 60 * 60;
        let month: u64 = 30 * day;
        // Metadata and static reference data are cached for ~6 months.
        let long: u64 = 6 * month;
        let mut endpoint_ttl = HashMap::new();
        for (key, secs) in [
            ("movie", long),
            ("movies", long),
            ("tv", long),
            ("series", long),
            ("collection", long),
            ("find", long),
            ("people", long),
            ("person", long),
            ("companies", long),
            ("networks", long),
            ("search", day),
            ("list", day),
            ("trending", day),
            ("popular", day),
            ("discover", 12 * 60 * 60),
            ("changes", 60 * 60),
            ("changed", 60 * 60),
            ("updates", 60 * 60),
            ("configuration", long),
            ("languages", long),
            ("genres", long),
            ("countries", long),
            ("certifications", long),
            ("artwork", long),
            ("awards", long),
            ("genders", long),
            ("movie-statuses", long),
            ("series-statuses", long),
            ("content-ratings", long),
            ("source-types", long),
            ("timezones", long),
            // Sub-endpoints that would otherwise inherit the broad first-segment
            // TTL (e.g. grabbing the 7-day `movie` TTL).
            ("movie/changes", 60 * 60),
            ("movie/popular", day),
            ("tv/changes", 60 * 60),
            ("tv/popular", day),
            ("person/changes", 60 * 60),
            ("person/popular", day),
            ("trending/all", day),
            ("discover/movie", 12 * 60 * 60),
            ("discover/tv", 12 * 60 * 60),
        ] {
            endpoint_ttl.insert(key.to_string(), HumanDuration(Duration::from_secs(secs)));
        }

        Self {
            enabled: true,
            default_ttl: Duration::from_secs(3 * month),
            max_ttl: Duration::from_secs(long),
            min_ttl: Duration::from_secs(60 * 60),
            negative_ttl: Duration::from_secs(5 * 60),
            not_found_ttl: Duration::from_secs(60 * 60),
            stale_if_error: Duration::from_secs(30 * day),
            negative_stale_if_error: Duration::from_secs(10 * 60),
            honor_cache_control: false,
            request_coalescing: true,
            stale_while_revalidate: true,
            endpoint_ttl,
            max_body_bytes: 10 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BackoffConfig {
    #[serde(with = "humantime_serde")]
    pub base_delay: Duration,
    pub factor: f64,
    #[serde(with = "humantime_serde")]
    pub max_delay: Duration,
    pub jitter: f64,
    pub max_consecutive_failures: u32,
    #[serde(with = "humantime_serde")]
    pub drop_after_wait: Duration,
    /// Additional attempts within a single inbound request when an upstream call
    /// fails (offline/timeout, 429, or 5xx). Each retry waits the progressively
    /// growing backoff window; if that exceeds `drop_after_wait` the request is
    /// dropped instead of retried. `3` means up to 4 total attempts.
    pub max_retries: u32,
    /// Hard ceiling for the per-request timeout once backoff growth is applied.
    /// Unlike `max_delay` this never shrinks the timeout below the provider's
    /// configured `request_timeout`.
    #[serde(with = "humantime_serde")]
    pub max_request_timeout: Duration,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(500),
            factor: 2.0,
            max_delay: Duration::from_secs(60),
            jitter: 0.2,
            max_consecutive_failures: 8,
            drop_after_wait: Duration::from_secs(10),
            max_retries: 3,
            max_request_timeout: Duration::from_secs(120),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReplayConfig {
    /// Serve canned fixtures instead of calling upstream providers.
    pub enabled: bool,
    /// Directory containing `manifest.json` and the fixture files.
    pub dir: String,
    /// When a fixture is missing, fall through to the real upstream instead of erroring.
    pub fallback_to_upstream: bool,
    /// Save every successful live upstream response under `<dir>/recorded/` so a
    /// later run can replay it offline. Off by default.
    pub record: bool,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            // Off by default: the mappers serve live TMDb/TVDB. Enable replay to serve
            // canned fixtures (used by the test suite / offline development).
            enabled: false,
            dir: "fixtures".to_string(),
            fallback_to_upstream: false,
            record: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogRotation {
    Never,
    Minutely,
    Hourly,
    #[default]
    Daily,
}

/// Rotating file logging (in addition to stdout). Uses `tracing-appender`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LoggingConfig {
    /// Write log files at all. When false, logs only go to stdout.
    pub enabled: bool,
    /// Directory for log files (created if missing).
    pub dir: String,
    /// Prefix for the rolled file names, e.g. `providarr.2026-10-02.log`.
    pub filename_prefix: String,
    /// Rotation cadence.
    pub rotation: LogRotation,
    /// Keep at most this many rolled files (0 = keep all).
    pub max_files: usize,
    /// Also emit logs to stdout.
    pub stdout: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            dir: "logs".to_string(),
            filename_prefix: "providarr".to_string(),
            rotation: LogRotation::Daily,
            max_files: 7,
            stdout: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct InboundLimiterConfig {
    /// Rate-limit requests coming *into* Providarr, keyed per client IP.
    pub enabled: bool,
    pub requests_per_second: f64,
    pub burst: u32,
    /// Server-wide ceiling across *all* client IPs, so a flood from many distinct
    /// IPs cannot overwhelm the instance. `0` disables the global limiter.
    pub global_requests_per_second: f64,
    pub global_burst: u32,
    /// Maximum requests being handled at once. `0` disables the cap.
    pub max_concurrent: u32,
    /// Honour `X-Forwarded-For` / `X-Real-IP` (enable only behind a trusted proxy).
    pub trust_forwarded_for: bool,
    /// Client IPs/CIDRs exempt from the limiter, e.g. `127.0.0.1`, `10.0.0.0/8`,
    /// `2001:db8::/32`. Also settable via `PROVIDARR_INBOUND_BYPASS` (comma separated).
    pub bypass: Vec<String>,
}

impl Default for InboundLimiterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            requests_per_second: 2.0,
            burst: 5,
            global_requests_per_second: 100.0,
            global_burst: 200,
            max_concurrent: 128,
            // Off by default: forwarding headers are only honored when the immediate peer is a
            // configured trusted proxy (see inbound.rs), otherwise they are spoofable.
            trust_forwarded_for: false,
            bypass: Vec::new(),
        }
    }
}

/// Optional shared-secret auth for Providarr's own HTTP API.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ApiAuthConfig {
    pub enabled: bool,
    /// The accepted key. Auth is only enforced when this is non-empty.
    pub api_key: String,
    /// Header to read the key from (case-insensitive).
    pub header: String,
}

impl Default for ApiAuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            api_key: String::new(),
            header: "x-api-key".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProviderConfig {
    pub base_url: String,
    pub requests_per_second: f64,
    pub burst: u32,
    pub max_concurrency: usize,
    #[serde(with = "humantime_serde")]
    pub request_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    pub documented_rate_limit: String,
    pub auth: AuthConfig,
    /// Per-endpoint (first path segment) requests/second overrides, e.g. `{"search": 2.0}`.
    #[serde(default)]
    pub endpoint_rps: HashMap<String, f64>,
}

impl ProviderConfig {
    pub fn tmdb_default() -> Self {
        Self {
            base_url: "https://api.themoviedb.org/3".to_string(),
            requests_per_second: 40.0,
            burst: 40,
            max_concurrency: 8,
            request_timeout: Duration::from_secs(15),
            connect_timeout: Duration::from_secs(5),
            documented_rate_limit: "~50 requests/second per API key (documented; not probed)"
                .to_string(),
            auth: AuthConfig::Bearer {
                env_var: "TMDB_API_TOKEN".to_string(),
            },
            endpoint_rps: HashMap::new(),
        }
    }

    pub fn tvdb_default() -> Self {
        Self {
            base_url: "https://api4.thetvdb.com/v4".to_string(),
            requests_per_second: 1.0,
            burst: 5,
            max_concurrency: 2,
            request_timeout: Duration::from_secs(15),
            connect_timeout: Duration::from_secs(5),
            documented_rate_limit: "conservative default pending review of TVDB terms (not probed)"
                .to_string(),
            auth: AuthConfig::TvdbLogin {
                env_var: "TVDB_API_KEY".to_string(),
            },
            endpoint_rps: HashMap::new(),
        }
    }
}

impl Default for ProviderConfig {
    fn default() -> Self {
        Self::tmdb_default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "style", rename_all = "snake_case")]
pub enum AuthConfig {
    None,
    Bearer { env_var: String },
    TvdbLogin { env_var: String },
    Query { env_var: String, param: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_ttl_policy_is_long_for_metadata_and_huge_for_static() {
        let config = CacheConfig::default();
        let day = Duration::from_secs(24 * 60 * 60);
        let half_year = 6 * 30 * day;
        let hour = Duration::from_secs(60 * 60);

        assert_eq!(config.ttl_for_path("/movie/535167"), half_year);
        assert_eq!(config.ttl_for_path("/series/239951/extended"), half_year);
        assert_eq!(config.ttl_for_path("/tv/1399"), half_year);
        assert_eq!(config.ttl_for_path("/search/movie"), day);
        assert_eq!(config.ttl_for_path("/configuration"), config.max_ttl);

        // Sub-endpoints override the broad first-segment policy.
        assert_eq!(config.ttl_for_path("/movie/changes"), hour);
        assert_eq!(config.ttl_for_path("/movie/popular"), day);
        assert_eq!(config.ttl_for_path("/tv/changes"), hour);
        assert_eq!(config.ttl_for_path("/discover/movie"), 12 * hour);

        // Unknown segments fall back to the default, and everything is capped.
        assert_eq!(config.ttl_for_path("/something/else"), config.default_ttl);
        assert!(config.ttl_for_path("/configuration") <= config.max_ttl);
    }

    #[test]
    fn validation_rejects_insane_backoff_and_limits() {
        let mut config = AppConfig::default();
        assert!(config.validate().is_ok());

        config.backoff.drop_after_wait = Duration::ZERO;
        assert!(config.validate().is_err());

        let mut config = AppConfig::default();
        config.backoff.jitter = 1.5;
        assert!(config.validate().is_err());

        let mut config = AppConfig::default();
        config.backoff.factor = 0.5;
        assert!(config.validate().is_err());

        let mut config = AppConfig::default();
        config.backoff.max_retries = u32::MAX;
        assert!(config.validate().is_err());

        let mut config = AppConfig::default();
        config
            .providers
            .get_mut("tmdb")
            .unwrap()
            .endpoint_rps
            .insert("search".into(), f64::NAN);
        assert!(config.validate().is_err());

        let mut config = AppConfig::default();
        config.inbound.max_concurrent = u32::MAX;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_unknown_config_fields() {
        let raw = r#"{"cache": {"enabled": true, "not_a_real_field": 1}}"#;
        assert!(serde_json::from_str::<AppConfig>(raw).is_err());
    }

    #[test]
    fn api_auth_is_fail_closed_when_enabled_without_key() {
        let mut config = AppConfig::default();
        config.api_auth.enabled = true;
        config.api_auth.api_key = String::new();
        assert!(config.validate().is_err());

        config.api_auth.enabled = true;
        config.api_auth.api_key = "sekret".into();
        assert!(config.validate().is_ok());

        config.api_auth.enabled = false;
        config.api_auth.api_key = String::new();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validation_caps_hostile_timers_and_lockouts() {
        let mut config = AppConfig::default();
        config.backoff.max_delay = Duration::from_secs(MAX_BACKOFF_MAX_DELAY.as_secs() + 1);
        assert!(config.validate().is_err());

        let mut config = AppConfig::default();
        config.backoff.max_request_timeout =
            Duration::from_secs(MAX_BACKOFF_MAX_REQUEST_TIMEOUT.as_secs() + 1);
        assert!(config.validate().is_err());

        let mut config = AppConfig::default();
        config.backoff.max_consecutive_failures = MAX_CONSECUTIVE_FAILURES_CAP + 1;
        assert!(config.validate().is_err());

        let mut config = AppConfig::default();
        config.cache.max_ttl = MAX_CACHE_TTL + Duration::from_secs(1);
        assert!(config.validate().is_err());

        let mut config = AppConfig::default();
        config.cache.stale_if_error = config.cache.max_ttl + Duration::from_secs(1);
        assert!(config.validate().is_err());

        let mut config = AppConfig::default();
        config.cache.max_body_bytes = MAX_CACHE_BODY_BYTES + 1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn validation_rejects_uninvertible_rate_limits() {
        let mut config = AppConfig::default();
        config
            .providers
            .get_mut("tmdb")
            .unwrap()
            .requests_per_second = MIN_REQUESTS_PER_SECOND / 2.0;
        assert!(config.validate().is_err());

        let mut config = AppConfig::default();
        config
            .providers
            .get_mut("tmdb")
            .unwrap()
            .endpoint_rps
            .insert("search".into(), MIN_REQUESTS_PER_SECOND / 2.0);
        assert!(config.validate().is_err());

        // Exactly zero and the minimum accepted value are both allowed.
        let mut config = AppConfig::default();
        config
            .providers
            .get_mut("tmdb")
            .unwrap()
            .requests_per_second = 0.0;
        config
            .providers
            .get_mut("tmdb")
            .unwrap()
            .endpoint_rps
            .insert("search".into(), MIN_REQUESTS_PER_SECOND);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn checked_in_config_parses_and_validates() {
        let raw = include_str!("../config/config.json");
        let config: AppConfig =
            serde_json::from_str(raw).expect("parse config/config.json under deny_unknown_fields");
        config.validate().expect("validate config/config.json");
    }

    #[test]
    fn search_defaults_and_cap() {
        let config = AppConfig::default();
        assert_eq!(config.search.hydrate_limit, 5);

        let mut over = AppConfig::default();
        over.search.hydrate_limit = MAX_HYDRATE_LIMIT + 1;
        assert!(over.validate().is_err());
    }

    #[test]
    fn logging_defaults_are_sane() {
        let config = LoggingConfig::default();
        assert!(config.enabled);
        assert_eq!(config.dir, "logs");
        assert_eq!(config.filename_prefix, "providarr");
        assert_eq!(config.rotation, LogRotation::Daily);
        assert_eq!(config.max_files, 7);
        assert!(config.stdout);
    }

    #[test]
    fn parses_log_rotation_names() {
        assert_eq!(parse_rotation("daily"), Some(LogRotation::Daily));
        assert_eq!(parse_rotation("Hourly"), Some(LogRotation::Hourly));
        assert_eq!(parse_rotation("minutely"), Some(LogRotation::Minutely));
        assert_eq!(parse_rotation("never"), Some(LogRotation::Never));
        assert_eq!(parse_rotation("weekly"), None);
    }
}
