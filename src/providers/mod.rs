use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use sqlx::PgPool;

use crate::{
    cache::{CacheStore, CachedEntry},
    config::{AppConfig, AuthConfig, CacheConfig, ProviderConfig},
    db,
    error::AppError,
    metrics,
    ratelimit::{limiter::ProviderRuntime, stats::StatsRegistry},
    replay::ReplayStore,
};

pub mod auth;

use auth::{AuthManager, ResolvedAuth};

#[derive(Debug, Clone)]
pub struct ProviderResponse {
    pub status: u16,
    pub content_type: Option<String>,
    pub body: Bytes,
    pub cached: bool,
    pub stale: bool,
    pub replayed: bool,
    /// When the served cache entry was stored (only for cache hits/stale).
    pub cache_created_at: Option<DateTime<Utc>>,
    /// When the served cache entry expires (only for cache hits/stale).
    pub cache_expires_at: Option<DateTime<Utc>>,
}

pub struct Provider {
    pub name: String,
    pub config: ProviderConfig,
    pub runtime: ProviderRuntime,
    pub auth: AuthManager,
}

#[derive(Clone)]
pub struct ProviderRegistry {
    providers: HashMap<String, Arc<Provider>>,
    cache: Arc<CacheStore>,
    pool: PgPool,
    stats: Arc<StatsRegistry>,
    replay: Arc<ReplayStore>,
    replay_enabled: bool,
    replay_fallback: bool,
    coalescing_enabled: bool,
    /// Upper bound applied to any upstream `Retry-After`, matching the backoff
    /// ceiling so an attacker-controlled header cannot pin a provider offline.
    backoff_max_delay: Duration,
    /// Per-key single-flight locks. A burst of identical misses shares one lock
    /// so only the first performs the upstream fetch; unrelated keys never
    /// contend. Entries are removed once no request holds the lock.
    coalesce: Arc<DashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Number of entries currently in `coalesce`, tracked with an atomic so it
    /// can be read while a DashMap shard guard is held (taking `DashMap::len`
    /// there could self-deadlock).
    coalesce_len: Arc<AtomicUsize>,
    /// Latches once `coalesce` reaches `MAX_COALESCE_ENTRIES`. From then on new
    /// keys use the fallback shards, so a key can never race between a per-key
    /// lock and a shard lock.
    coalesce_saturated: Arc<AtomicBool>,
    /// Fixed pool of sharded fallback locks. When `coalesce` is saturated,
    /// identical keys still hash to the same shard and therefore serialise.
    coalesce_shards: Arc<[Arc<tokio::sync::Mutex<()>>]>,
    /// Keys with an in-flight background revalidation. Prevents a burst of stale
    /// requests for the same key from spawning one upstream refresh each.
    revalidating: Arc<DashMap<String, ()>>,
    /// Caps concurrent background revalidations so stale requests for many
    /// distinct keys cannot spawn unbounded upstream work.
    revalidation_permits: Arc<tokio::sync::Semaphore>,
}

/// Hard ceiling on tracked single-flight keys, guarding against unbounded map
/// growth under a flood of distinct keys. Once exceeded, new keys fall back to
/// the shard locks instead of giving up coalescing, so identical keys still
/// serialise.
const MAX_COALESCE_ENTRIES: usize = 10_000;

/// Number of fallback lock shards. Identical keys always map to the same shard,
/// so the single-flight invariant holds even once the per-key map is saturated.
const COALESCE_SHARDS: usize = 256;

/// Bounds on how long a request waits for an in-flight identical fetch. The
/// holder can run `max_retries + 1` attempts, so allow for all of them, but
/// never queue behind a struggling upstream without limit.
const COALESCE_WAIT_FLOOR: Duration = Duration::from_secs(5);
const COALESCE_WAIT_CEILING: Duration = Duration::from_secs(120);

/// Maximum number of background revalidations allowed to run at once. Kept
/// small so a burst of stale keys cannot occupy a large share of a provider's
/// foreground concurrency slots.
const MAX_CONCURRENT_REVALIDATIONS: usize = 4;

impl ProviderRegistry {
    pub fn build(
        config: &AppConfig,
        pool: PgPool,
        cache: Arc<CacheStore>,
        stats: Arc<StatsRegistry>,
        replay: Arc<ReplayStore>,
    ) -> Result<Self, AppError> {
        let mut providers = HashMap::new();
        for (name, provider_config) in &config.providers {
            let runtime = ProviderRuntime::new(
                name.clone(),
                provider_config.clone(),
                config.backoff.clone(),
                stats.clone(),
            )?;
            let auth = AuthManager::new(provider_config.auth.clone());
            if !auth.has_credentials() && provider_config.auth != AuthConfig::None {
                tracing::warn!(provider = name, "provider has no credentials configured");
            }
            providers.insert(
                name.clone(),
                Arc::new(Provider {
                    name: name.clone(),
                    config: provider_config.clone(),
                    runtime,
                    auth,
                }),
            );
        }
        Ok(Self {
            providers,
            cache,
            pool,
            stats,
            replay_enabled: config.replay.enabled,
            replay_fallback: config.replay.fallback_to_upstream,
            replay,
            coalescing_enabled: config.cache.request_coalescing,
            backoff_max_delay: config.backoff.max_delay,
            coalesce: Arc::new(DashMap::new()),
            coalesce_len: Arc::new(AtomicUsize::new(0)),
            coalesce_saturated: Arc::new(AtomicBool::new(false)),
            coalesce_shards: (0..COALESCE_SHARDS)
                .map(|_| Arc::new(tokio::sync::Mutex::new(())))
                .collect::<Vec<_>>()
                .into(),
            revalidating: Arc::new(DashMap::new()),
            revalidation_permits: Arc::new(tokio::sync::Semaphore::new(
                MAX_CONCURRENT_REVALIDATIONS,
            )),
        })
    }

    pub fn provider(&self, name: &str) -> Option<Arc<Provider>> {
        self.providers.get(name).cloned()
    }

    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.providers.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn stats(&self) -> &Arc<StatsRegistry> {
        &self.stats
    }

    pub fn cache(&self) -> &Arc<CacheStore> {
        &self.cache
    }

    pub fn replay(&self) -> &Arc<ReplayStore> {
        &self.replay
    }

    pub fn replay_enabled(&self) -> bool {
        self.replay_enabled
    }

    /// Restore persisted backoff for a provider (called at startup).
    pub fn restore_backoff(&self, provider: &str, failures: u32, remaining: Duration) {
        if let Some(provider) = self.provider(provider) {
            provider.runtime.restore_backoff(failures, remaining);
        }
    }

    async fn persist_backoff(&self, provider_name: &str, runtime: &ProviderRuntime) {
        let (failures, remaining) = runtime.backoff_snapshot();
        let result = if failures == 0 {
            db::clear_provider_backoff(&self.pool, provider_name).await
        } else {
            db::upsert_provider_backoff(
                &self.pool,
                provider_name,
                failures as i32,
                remaining.as_millis() as i64,
            )
            .await
        };

        if let Err(err) = result {
            tracing::warn!(error = %err, provider = provider_name, "failed to persist provider backoff");
        }
    }

    /// Aggressively cached, rate-limited proxy of a provider GET/HEAD request.
    pub async fn fetch(
        self: &Arc<Self>,
        provider_name: &str,
        method: &str,
        path_and_query: &str,
    ) -> Result<ProviderResponse, AppError> {
        let provider = self
            .provider(provider_name)
            .ok_or_else(|| AppError::UnknownProvider(provider_name.to_string()))?;

        let method_upper = method.to_ascii_uppercase();
        if method_upper != "GET" && method_upper != "HEAD" {
            return Err(AppError::Internal(format!(
                "method {method} is not supported by the proxy"
            )));
        }

        let endpoint = normalize_endpoint(path_and_query);
        let path = path_and_query.split('?').next().unwrap_or(path_and_query);

        if self.replay_enabled {
            if let Some(fixture) =
                self.replay
                    .upstream(provider_name, &method_upper, path_and_query)
            {
                metrics::replayed(provider_name, &endpoint);
                return Ok(ProviderResponse {
                    status: fixture.status,
                    content_type: fixture.content_type,
                    body: fixture.body,
                    cached: false,
                    stale: false,
                    replayed: true,
                    cache_created_at: None,
                    cache_expires_at: None,
                });
            }
            if !self.replay_fallback {
                // Never reach out to the real provider while replaying.
                return Err(AppError::ReplayMiss(format!(
                    "{provider_name} {path_and_query}"
                )));
            }
        }

        let cache_key = CacheStore::key(provider_name, &method_upper, path_and_query);
        let now = Utc::now();

        // Cache hits and serve-stale-immediately do not need the single-flight
        // lock; only an actual upstream fetch does. This keeps the lock held
        // for the fetch alone, never across the cache lookup.
        let mut stale: Option<CachedEntry> = None;
        if self.cache.enabled() {
            match self.cache.get(&cache_key).await {
                Ok(Some(entry)) if entry.is_fresh(now) => {
                    metrics::cache_hit(provider_name, &endpoint);
                    return Ok(entry_to_response(entry, false));
                }
                Ok(Some(entry))
                    if self.cache.config().stale_while_revalidate
                        && entry.is_within_stale_window(now, self.cache.config()) =>
                {
                    // Serve stale immediately, refresh in the background. The
                    // per-key registry guarantees only the first observer spawns
                    // a refresh; the rest serve stale without spawning.
                    metrics::cache_stale(provider_name, &endpoint);
                    self.spawn_revalidation(
                        cache_key.clone(),
                        provider_name.to_string(),
                        method_upper.clone(),
                        path_and_query.to_string(),
                        endpoint.clone(),
                        path.to_string(),
                    );
                    return Ok(entry_to_response(entry, true));
                }
                Ok(Some(entry)) if entry.is_within_stale_window(now, self.cache.config()) => {
                    stale = Some(entry);
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        key = %cache_key,
                        "cache lookup failed; treating as a miss"
                    );
                }
            }
            metrics::cache_miss(provider_name, &endpoint);
        }

        // Single-flight: a per-key lock so a burst of identical misses triggers
        // at most one upstream call, without unrelated keys blocking each other.
        // Existing/hot keys are always coalesced, even once the registry is
        // saturated. The wait is bounded: rather than queue behind a struggling
        // upstream forever, serve stale or surface an error.
        let coalesce_guard = if self.cache.enabled() && self.coalescing_enabled {
            let wait = coalesce_wait_bound(&provider);
            match self.acquire_coalesce(&cache_key, wait).await {
                Ok(guard) => Some(guard),
                Err(err) => {
                    return match stale {
                        Some(entry) => {
                            metrics::cache_stale(provider_name, &endpoint);
                            Ok(entry_to_response(entry, true))
                        }
                        None => Err(err),
                    };
                }
            }
        } else {
            None
        };

        // While we waited for the lock another request may have populated the
        // entry; re-check before doing upstream work.
        if coalesce_guard.is_some() {
            match self.cache.get(&cache_key).await {
                Ok(Some(entry)) if entry.is_fresh(Utc::now()) => {
                    metrics::cache_hit(provider_name, &endpoint);
                    return Ok(entry_to_response(entry, false));
                }
                Ok(Some(entry)) if entry.is_within_stale_window(now, self.cache.config()) => {
                    stale = Some(entry);
                }
                _ => {}
            }
        }

        match self
            .fetch_upstream(
                provider_name,
                &method_upper,
                path_and_query,
                endpoint.clone(),
                path.to_string(),
            )
            .await
        {
            Ok(response) => Ok(response),
            Err(err) => match stale {
                Some(entry) => {
                    metrics::cache_stale(provider_name, &endpoint);
                    Ok(entry_to_response(entry, true))
                }
                None => Err(err),
            },
        }
    }

    /// Acquires the single-flight lock for `key`, inserting a fresh per-key lock
    /// on first use.
    ///
    /// Once the per-key map is saturated the key is routed to a fixed shard
    /// lock instead of being left uncoalesced; identical keys therefore always
    /// serialise regardless of map pressure. The decision is made while holding
    /// the key's map-shard guard so a key can never straddle both mechanisms.
    async fn acquire_coalesce(
        self: &Arc<Self>,
        key: &str,
        wait: Duration,
    ) -> Result<CoalesceGuard, AppError> {
        use dashmap::mapref::entry::Entry;

        let lock = match self.coalesce.entry(key.to_string()) {
            Entry::Occupied(entry) => Arc::clone(entry.get()),
            Entry::Vacant(entry) => {
                let saturated = self.coalesce_saturated.load(Ordering::Acquire);
                let has_room = self.coalesce_len.load(Ordering::Acquire) < MAX_COALESCE_ENTRIES;
                if saturated || !has_room {
                    // Latch saturation so a later removal cannot let this key
                    // race between a per-key lock and a shard lock. The entry
                    // guard is dropped before the await in `acquire_shard`.
                    self.coalesce_saturated.store(true, Ordering::Release);
                    drop(entry);
                    return self.acquire_coalesce_shard(key, wait).await;
                }
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                entry.insert(Arc::clone(&lock));
                self.coalesce_len.fetch_add(1, Ordering::AcqRel);
                lock
            }
        };
        // The map entry guard is dropped before the await below: never hold a
        // `DashMap` reference across `.await`.
        let guard = lock_with_timeout(&lock, wait).await?;
        Ok(CoalesceGuard {
            registry: Some(Arc::clone(&self.coalesce)),
            len: Some(Arc::clone(&self.coalesce_len)),
            key: key.to_string(),
            guard: Some(guard),
        })
    }

    /// Fallback single-flight lock used once the per-key map is saturated.
    async fn acquire_coalesce_shard(
        &self,
        key: &str,
        wait: Duration,
    ) -> Result<CoalesceGuard, AppError> {
        let index = shard_index(key, self.coalesce_shards.len());
        let lock = Arc::clone(&self.coalesce_shards[index]);
        let guard = lock_with_timeout(&lock, wait).await?;
        Ok(CoalesceGuard {
            registry: None,
            len: None,
            key: String::new(),
            guard: Some(guard),
        })
    }

    /// Spawns a background refresh for `cache_key` unless one is already running.
    /// Others observing the same key serve stale and return immediately. If the
    /// concurrent-revalidation budget is exhausted, the refresh is skipped
    /// rather than blocking the caller.
    fn spawn_revalidation(
        self: &Arc<Self>,
        cache_key: String,
        provider_name: String,
        method_upper: String,
        path_and_query: String,
        endpoint: String,
        path: String,
    ) {
        use dashmap::mapref::entry::Entry;

        let permit = match self.revalidation_permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => return,
        };

        match self.revalidating.entry(cache_key.clone()) {
            Entry::Occupied(_) => return,
            Entry::Vacant(vacant) => {
                vacant.insert(());
            }
        }

        // Build the guard in the caller and move it into the task: if the task
        // is dropped before its first poll it still drops the guard, so the
        // `revalidating` marker cannot leak.
        let guard = RevalidationGuard {
            map: Arc::clone(&self.revalidating),
            key: cache_key,
        };

        let this = Arc::clone(self);
        tokio::spawn(async move {
            let _permit = permit;
            let _guard = guard;
            if this
                .fetch_upstream(
                    &provider_name,
                    &method_upper,
                    &path_and_query,
                    endpoint,
                    path,
                )
                .await
                .is_err()
            {
                tracing::debug!(
                    provider = %provider_name,
                    method = %method_upper,
                    "background revalidation failed"
                );
            }
        });
    }

    #[allow(clippy::too_many_arguments)]
    async fn fetch_upstream(
        &self,
        provider_name: &str,
        method_upper: &str,
        path_and_query: &str,
        endpoint: String,
        path: String,
    ) -> Result<ProviderResponse, AppError> {
        let provider = self
            .provider(provider_name)
            .ok_or_else(|| AppError::UnknownProvider(provider_name.to_string()))?;

        let auth = provider.auth.resolve(&provider.runtime).await?;
        let mut url = build_url(&provider.config.base_url, path_and_query)?;
        if let ResolvedAuth::Query { param, value } = &auth {
            url = append_query(url, param, value);
        }

        let max_body_bytes = self.cache.config().max_body_bytes;

        let request_method = if method_upper == "HEAD" {
            reqwest::Method::HEAD
        } else {
            reqwest::Method::GET
        };

        // Progressive retry: each attempt re-enters `acquire`, which waits the
        // provider's (growing) backoff window before sending. Once the window
        // exceeds `drop_after_wait` the request is dropped rather than retried.
        let max_attempts = provider.runtime.max_retries().saturating_add(1).max(1);
        let mut attempt: u32 = 0;

        loop {
            attempt += 1;

            let _permit = match provider.runtime.acquire(&endpoint).await {
                Ok(permit) => permit,
                Err(err) => {
                    metrics::dropped(provider_name, &endpoint);
                    return Err(err.into());
                }
            };

            // Counted only after a permit is held, so attempts dropped by the
            // limiter/backoff are not recorded as upstream transactions.
            self.stats.record_request(
                provider_name,
                &endpoint,
                Some(provider.config.documented_rate_limit.as_str()),
            );

            let mut request = provider
                .runtime
                .http()
                .request(request_method.clone(), &url)
                .timeout(provider.runtime.effective_timeout())
                .header(reqwest::header::ACCEPT, "application/json");
            if let ResolvedAuth::Bearer(token) = &auth {
                request = request.bearer_auth(token);
            }

            let response = match request.send().await {
                Ok(response) => response,
                Err(err) => {
                    provider.runtime.on_failure(&endpoint, None);
                    metrics::upstream_error(provider_name, &endpoint);
                    self.persist_backoff(provider_name, &provider.runtime).await;

                    if (err.is_timeout() || err.is_connect()) && attempt < max_attempts {
                        tracing::warn!(
                            provider = provider_name,
                            endpoint = %endpoint,
                            attempt,
                            max_attempts,
                            timeout = err.is_timeout(),
                            "upstream offline; retrying after backoff"
                        );
                        continue;
                    }

                    return if err.is_timeout() {
                        Err(AppError::Timeout)
                    } else {
                        Err(AppError::Http(err))
                    };
                }
            };

            let status = response.status();
            let status_code = status.as_u16();
            metrics::request(provider_name, &endpoint, status_code);
            let content_type = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let cache_control = response
                .headers()
                .get(reqwest::header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(parse_retry_after);

            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                let since = provider.runtime.on_rate_limited(&endpoint, status_code);
                if let Some(retry_after) = retry_after {
                    // Honour upstream's Retry-After when it is longer than our
                    // own computed backoff, but never beyond our own ceiling:
                    // an oversized value must not pin the provider offline.
                    provider
                        .runtime
                        .enforce_retry_after(retry_after.min(self.backoff_max_delay));
                }
                metrics::rate_limited(provider_name, &endpoint);
                self.record_rate_limit_event(provider_name, &endpoint, since, status_code)
                    .await;
                self.persist_backoff(provider_name, &provider.runtime).await;

                if attempt < max_attempts {
                    tracing::warn!(
                        provider = provider_name,
                        endpoint = %endpoint,
                        attempt,
                        max_attempts,
                        "upstream rate limited; retrying after backoff"
                    );
                    continue;
                }

                return Err(AppError::RateLimited {
                    provider: provider_name.to_string(),
                    retry_after_ms: provider.runtime.backoff_remaining().as_millis() as u64,
                });
            }

            let body = match read_body_capped(response, max_body_bytes).await {
                Ok(body) => body,
                Err(err) => {
                    // A partial/failed body read is a transport failure just
                    // like a failed send: account for it and retry if allowed.
                    // A body that exceeded the cap is deterministic, so skip
                    // the retry and surface it immediately.
                    provider.runtime.on_failure(&endpoint, None);
                    metrics::upstream_error(provider_name, &endpoint);
                    self.persist_backoff(provider_name, &provider.runtime).await;

                    if attempt < max_attempts && !matches!(err, AppError::Upstream { .. }) {
                        tracing::warn!(
                            provider = provider_name,
                            endpoint = %endpoint,
                            attempt,
                            max_attempts,
                            "failed to read upstream body; retrying after backoff"
                        );
                        continue;
                    }

                    return Err(err);
                }
            };

            if status.is_server_error() {
                provider.runtime.on_failure(&endpoint, Some(status_code));
                metrics::upstream_error(provider_name, &endpoint);
                self.persist_backoff(provider_name, &provider.runtime).await;

                if attempt < max_attempts {
                    tracing::warn!(
                        provider = provider_name,
                        endpoint = %endpoint,
                        attempt,
                        max_attempts,
                        status = status_code,
                        "upstream error; retrying after backoff"
                    );
                    continue;
                }

                return Err(AppError::Upstream {
                    status: status_code,
                    body: String::from_utf8_lossy(&body).chars().take(500).collect(),
                });
            }

            let policy = if status.is_success() {
                let had_backoff = provider.runtime.consecutive_failures() > 0;
                provider.runtime.on_success(&endpoint, status_code);
                if had_backoff {
                    self.persist_backoff(provider_name, &provider.runtime).await;
                }

                // Capture the raw response for later offline replay when enabled.
                self.replay.record(
                    provider_name,
                    method_upper,
                    path_and_query,
                    status_code,
                    content_type.clone(),
                    &body,
                );

                cache_policy_from(cache_control.as_deref(), self.cache.config(), &path)
            } else if status.is_redirection() {
                // Redirects are not followed. Never pin a 3xx as a cached
                // error: surface it as-is and drop any existing entry.
                CachePolicy::NoStore
            } else {
                // 404s are stable ("does not exist"), so cache them longer than a
                // generic 4xx; neither pushes the provider into backoff.
                let ttl = if status == reqwest::StatusCode::NOT_FOUND {
                    self.cache.config().not_found_ttl
                } else {
                    self.cache.config().negative_ttl
                };

                CachePolicy::Store(ttl)
            };

            let cache_key = CacheStore::key(provider_name, method_upper, path_and_query);
            match policy {
                CachePolicy::Store(ttl) if self.cache.enabled() && !ttl.is_zero() => {
                    let headers = serde_json::json!({
                        "content-type": content_type,
                        "cache-control": cache_control,
                    });
                    if let Err(err) = self
                        .cache
                        .put(
                            &cache_key,
                            provider_name,
                            &endpoint,
                            path_and_query,
                            status_code,
                            content_type.as_deref(),
                            &body,
                            &headers,
                            ttl,
                        )
                        .await
                    {
                        // Cache writes are best-effort: never fail the request
                        // because the cache is unavailable.
                        tracing::warn!(
                            error = %err,
                            key = %cache_key,
                            "failed to store cache entry; serving upstream response"
                        );
                    }
                }
                CachePolicy::NoStore if self.cache.enabled() => {
                    // Explicit no-store/no-cache: drop any existing row instead
                    // of silently continuing to serve the old value.
                    if let Err(err) = self.cache.invalidate(&cache_key).await {
                        tracing::warn!(
                            error = %err,
                            key = %cache_key,
                            "failed to invalidate cache entry for no-store response"
                        );
                    }
                }
                _ => {}
            }

            return Ok(ProviderResponse {
                status: status_code,
                content_type,
                body,
                cached: false,
                stale: false,
                replayed: false,
                cache_created_at: None,
                cache_expires_at: None,
            });
        }
    }

    async fn record_rate_limit_event(
        &self,
        provider: &str,
        endpoint: &str,
        requests_since_last_limit: u64,
        status_code: u16,
    ) {
        if let Err(err) = db::insert_rate_limit_event(
            &self.pool,
            provider,
            endpoint,
            requests_since_last_limit,
            Some(status_code),
            Some("upstream returned a rate-limit response"),
        )
        .await
        {
            tracing::warn!(error = %err, "failed to persist rate-limit event");
        }
    }
}

/// RAII holder for a single-flight lock. Dropping it releases the lock and, for
/// per-key guards, removes the map entry if no other request still holds it.
struct CoalesceGuard {
    /// Per-key registry to prune on release; `None` for shard fallback guards.
    registry: Option<Arc<DashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    /// Mirrors the number of live entries in `registry`.
    len: Option<Arc<AtomicUsize>>,
    key: String,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for CoalesceGuard {
    fn drop(&mut self) {
        // Release the mutex first so our own `OwnedMutexGuard` Arc no longer
        // counts; the map plus any waiting request are all that can remain.
        self.guard.take();
        if let (Some(registry), Some(len)) = (&self.registry, &self.len)
            && registry
                .remove_if(&self.key, |_, lock| Arc::strong_count(lock) == 1)
                .is_some()
        {
            len.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// Acquires an owned mutex guard, giving up after `wait` so a request can never
/// queue behind an in-flight identical fetch indefinitely.
async fn lock_with_timeout(
    lock: &Arc<tokio::sync::Mutex<()>>,
    wait: Duration,
) -> Result<tokio::sync::OwnedMutexGuard<()>, AppError> {
    match tokio::time::timeout(wait, Arc::clone(lock).lock_owned()).await {
        Ok(guard) => Ok(guard),
        Err(_) => Err(AppError::Dropped(
            "timed out waiting for an in-flight identical request".to_string(),
        )),
    }
}

/// Maps a cache key onto one of `shards` buckets. The FNV-1a hash keeps the
/// mapping stable for the process lifetime without a hasher dependency.
fn shard_index(key: &str, shards: usize) -> usize {
    debug_assert!(shards > 0);
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in key.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash as usize) % shards
}

/// Upper bound on how long a request waits for an in-flight identical fetch.
/// The holder may run `max_retries + 1` attempts, each bounded by the current
/// effective request timeout; clamp the total to a sane range.
fn coalesce_wait_bound(provider: &Provider) -> Duration {
    let attempts = provider.runtime.max_retries().saturating_add(1);
    provider
        .runtime
        .effective_timeout()
        .saturating_mul(attempts)
        .max(COALESCE_WAIT_FLOOR)
        .min(COALESCE_WAIT_CEILING)
}

/// Removes the `revalidating` marker on drop, so a panicked or aborted
/// background task cannot leak the key and permanently block revalidation.
struct RevalidationGuard {
    map: Arc<DashMap<String, ()>>,
    key: String,
}

impl Drop for RevalidationGuard {
    fn drop(&mut self) {
        self.map.remove(&self.key);
    }
}

fn entry_to_response(entry: CachedEntry, stale: bool) -> ProviderResponse {
    ProviderResponse {
        status: entry.status_code,
        content_type: entry.content_type,
        body: entry.body,
        cached: true,
        stale,
        replayed: false,
        cache_created_at: Some(entry.created_at),
        cache_expires_at: Some(entry.expires_at),
    }
}

/// Reads an upstream response body, refusing to buffer more than `max` bytes.
///
/// The `Content-Length` header is checked first (when present) to reject an
/// oversized body before any of it is read; the streamed body is then counted
/// chunk by chunk so a missing/lying length or a decompression bomb is still
/// capped. Excessively large bodies surface as `AppError::Upstream`.
async fn read_body_capped(mut response: reqwest::Response, max: usize) -> Result<Bytes, AppError> {
    let status = response.status().as_u16();
    if let Some(len) = response.content_length()
        && len > max as u64
    {
        return Err(AppError::Upstream {
            status,
            body: format!("upstream body of {len} bytes exceeds the {max} byte limit"),
        });
    }

    let mut body = BytesMut::new();
    loop {
        let chunk = match response.chunk().await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => break,
            Err(err) if err.is_timeout() => return Err(AppError::Timeout),
            Err(err) => return Err(AppError::Http(err)),
        };
        if body.len().saturating_add(chunk.len()) > max {
            return Err(AppError::Upstream {
                status,
                body: format!("upstream body exceeds the {max} byte limit"),
            });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

fn build_url(base_url: &str, path_and_query: &str) -> Result<String, AppError> {
    if path_and_query.contains('\\') || path_and_query.chars().any(char::is_control) {
        return Err(AppError::Internal(
            "request path contained illegal characters".to_string(),
        ));
    }
    if base_url.contains('\\') || base_url.chars().any(char::is_control) {
        return Err(AppError::Config(
            "provider base url contained illegal characters".to_string(),
        ));
    }

    let mut base = url::Url::parse(base_url)
        .map_err(|err| AppError::Config(format!("invalid provider base url: {err}")))?;

    // Treat the configured base path as a directory so relative request paths
    // are appended to it instead of replacing it.
    if !base.path().ends_with('/') {
        let path = format!("{}/", base.path());
        base.set_path(&path);
    }
    let base_path = base.path().to_string();

    let joined = base
        .join(path_and_query.trim_start_matches('/'))
        .map_err(|err| AppError::Internal(format!("invalid request path: {err}")))?;

    // `Url` normalises `..` and absolute/network-path references, so a crafted
    // path could otherwise escape the configured base path while still carrying
    // the provider API key. Reject anything that leaves the base origin/path.
    if joined.scheme() != base.scheme()
        || joined.host_str() != base.host_str()
        || joined.port_or_known_default() != base.port_or_known_default()
        || !joined.path().starts_with(&base_path)
    {
        return Err(AppError::Internal(
            "request path escapes the configured provider base url".to_string(),
        ));
    }

    // Credentials or a fragment in a joined path would be forwarded upstream
    // (and could smuggle authority); reject them outright.
    if !joined.username().is_empty() || joined.password().is_some() || joined.fragment().is_some() {
        return Err(AppError::Internal(
            "request path introduced userinfo or a fragment".to_string(),
        ));
    }

    // A percent-encoded dot or slash can survive URL normalisation and then be
    // decoded by the upstream, escaping the configured base path (double
    // encoding). No legitimate provider path needs them.
    let joined_path = joined.path().to_ascii_lowercase();
    if joined_path.contains("%2e") || joined_path.contains("%2f") {
        return Err(AppError::Internal(
            "request path contained a percent-encoded path segment or separator".to_string(),
        ));
    }

    Ok(joined.into())
}

fn append_query(mut url: String, param: &str, value: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    let encoded: String = url::form_urlencoded::byte_serialize(value.as_bytes()).collect();
    url.push(separator);
    url.push_str(param);
    url.push('=');
    url.push_str(&encoded);
    url
}

/// What to do with the response once it has been read.
enum CachePolicy {
    Store(Duration),
    /// Upstream explicitly asked us not to store (or to revalidate) this
    /// response, so any existing entry must be dropped.
    NoStore,
}

fn cache_policy_from(cache_control: Option<&str>, config: &CacheConfig, path: &str) -> CachePolicy {
    let base = config.ttl_for_path(path);

    if config.honor_cache_control
        && let Some(header) = cache_control
    {
        let lower = header.to_ascii_lowercase();
        if lower.contains("no-store") || lower.contains("no-cache") {
            return CachePolicy::NoStore;
        }
        if let Some(max_age) = parse_max_age(&lower) {
            // Aggressive: never below min_ttl, never above max_ttl.
            return CachePolicy::Store(
                Duration::from_secs(max_age)
                    .max(config.min_ttl)
                    .min(config.max_ttl),
            );
        }
    }

    CachePolicy::Store(base)
}

/// Parses an RFC 7231 `Retry-After` value: either delta-seconds or an HTTP-date.
/// The result is unbounded; callers must clamp it to the backoff ceiling.
fn parse_retry_after(value: &str) -> Option<Duration> {
    let value = value.trim();
    if let Ok(secs) = value.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    if let Ok(date) = DateTime::parse_from_rfc2822(value) {
        let secs = (date.with_timezone(&Utc) - Utc::now()).num_seconds().max(0) as u64;
        return Some(Duration::from_secs(secs));
    }
    None
}

fn parse_max_age(header: &str) -> Option<u64> {
    let idx = header.find("max-age=")?;
    let rest = &header[idx + "max-age=".len()..];
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// Static path words that are safe to keep verbatim in an endpoint label. Any
/// other non-identifier segment is collapsed to `{other}` so an attacker cannot
/// explode metric cardinality with arbitrary path segments.
const STATIC_SEGMENTS: &[&str] = &[
    "movie",
    "movies",
    "tv",
    "series",
    "season",
    "seasons",
    "episode",
    "episodes",
    "collection",
    "find",
    "people",
    "person",
    "company",
    "companies",
    "network",
    "networks",
    "search",
    "list",
    "trending",
    "popular",
    "discover",
    "changes",
    "changed",
    "updates",
    "configuration",
    "languages",
    "language",
    "genres",
    "genre",
    "countries",
    "certifications",
    "artwork",
    "awards",
    "genders",
    "credits",
    "credit",
    "images",
    "image",
    "videos",
    "video",
    "external_ids",
    "keywords",
    "keyword",
    "reviews",
    "review",
    "similar",
    "recommendations",
    "translations",
    "watch",
    "providers",
    "movie-statuses",
    "series-statuses",
    "content-ratings",
    "source-types",
    "timezones",
];

/// Maximum number of path segments retained in an endpoint label. Anything
/// beyond this is folded into a single trailing `{other}`.
const MAX_ENDPOINT_SEGMENTS: usize = 6;

/// Collapses opaque identifiers so accounting is grouped by resource shape,
/// e.g. `/movie/550` and `/movie/551` both become `movie/{id}`. Unknown static
/// words and excessive path depth become `{other}` to bound cardinality.
pub fn normalize_endpoint(path_and_query: &str) -> String {
    let path = path_and_query.split('?').next().unwrap_or("");
    let segments: Vec<&str> = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    if segments.is_empty() {
        return "root".to_string();
    }

    let truncated = segments.len() > MAX_ENDPOINT_SEGMENTS;
    let mut parts: Vec<String> = segments
        .iter()
        .take(MAX_ENDPOINT_SEGMENTS)
        .map(|segment| bucket_segment(segment))
        .collect();
    if truncated {
        parts.push("{other}".to_string());
    }
    parts.join("/")
}

fn bucket_segment(segment: &str) -> String {
    if is_identifier(segment) {
        return "{id}".to_string();
    }
    let lower = segment.to_ascii_lowercase();
    if STATIC_SEGMENTS.contains(&lower.as_str()) {
        lower
    } else {
        "{other}".to_string()
    }
}

fn is_identifier(segment: &str) -> bool {
    if segment.is_empty() {
        return false;
    }
    if segment.chars().all(|c| c.is_ascii_digit()) {
        return true;
    }
    // IMDb-style ids, e.g. tt0944947
    if let Some(rest) = segment.strip_prefix("tt")
        && !rest.is_empty()
        && rest.chars().all(|c| c.is_ascii_digit())
    {
        return true;
    }
    // UUID-ish (e.g. TVDB keys)
    let uuid_like = segment.len() >= 16
        && segment.contains('-')
        && segment.chars().all(|c| c.is_ascii_hexdigit() || c == '-');
    uuid_like || is_hex_hash(segment)
}

fn is_hex_hash(segment: &str) -> bool {
    segment.len() >= 24 && segment.chars().all(|c| c.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_identifiers() {
        assert_eq!(normalize_endpoint("/movie/550"), "movie/{id}");
        assert_eq!(normalize_endpoint("/movie/550?language=en"), "movie/{id}");
        assert_eq!(
            normalize_endpoint("/tv/1399/season/1"),
            "tv/{id}/season/{id}"
        );
        assert_eq!(normalize_endpoint("/search/movie"), "search/movie");
        assert_eq!(normalize_endpoint("/find/tt0944947"), "find/{id}");
        assert_eq!(normalize_endpoint("/"), "root");
        assert_eq!(normalize_endpoint(""), "root");
    }

    #[test]
    fn buckets_unknown_segments_and_caps_depth() {
        assert_eq!(normalize_endpoint("/foo/bar"), "{other}/{other}");
        assert_eq!(
            normalize_endpoint("/movie/secret-looking-slug"),
            "movie/{other}"
        );
        // Known words are lowercased; unknown words collapse.
        assert_eq!(normalize_endpoint("/Movie/Popular"), "movie/popular");
        // Deeply nested paths are truncated to a bounded label.
        assert_eq!(
            normalize_endpoint("/movie/1/tv/2/person/3/foo/bar/baz"),
            "movie/{id}/tv/{id}/person/{id}/{other}"
        );
    }

    #[test]
    fn build_url_preserves_base_and_query() {
        assert_eq!(
            build_url("https://api.example.com/3", "/movie/550?language=en").unwrap(),
            "https://api.example.com/3/movie/550?language=en"
        );
        assert_eq!(
            build_url("https://api.example.com/3", "movie/550").unwrap(),
            "https://api.example.com/3/movie/550"
        );
    }

    #[test]
    fn build_url_rejects_base_path_escape() {
        for path in [
            "/../admin",
            "movie/../../admin",
            "/movie/%2e%2e/%2e%2e/admin",
            "https://evil.example.com/x",
            "/movie\\..\\admin",
            "/movie/\u{0000}",
        ] {
            assert!(
                build_url("https://api.example.com/3", path).is_err(),
                "expected rejection for {path:?}"
            );
        }
        // A protocol-relative reference is treated as a relative path, so it
        // stays on the configured origin instead of escaping.
        assert_eq!(
            build_url("https://api.example.com/3", "//evil.example.com/x").unwrap(),
            "https://api.example.com/3/evil.example.com/x"
        );
    }

    #[test]
    fn build_url_rejects_userinfo_fragment_and_encoded_separators() {
        for path in [
            "https://user:pass@api.example.com/3/movie/550",
            "https://api.example.com/3/movie/550#frag",
            "/movie/%2Fadmin",
            "/movie/%2fadmin",
            "/movie/%2Enpm",
            "/movie/%2e%2e/%2e%2e/secret",
        ] {
            assert!(
                build_url("https://api.example.com/3", path).is_err(),
                "expected rejection for {path:?}"
            );
        }
        // A legitimate encoded query value still passes.
        assert_eq!(
            build_url("https://api.example.com/3", "/search?query=a%20b").unwrap(),
            "https://api.example.com/3/search?query=a%20b"
        );
    }

    #[test]
    fn shard_index_is_deterministic_and_bounded() {
        for key in ["a", "b", "some-long-cache-key"] {
            let index = shard_index(key, COALESCE_SHARDS);
            assert!(index < COALESCE_SHARDS);
            assert_eq!(index, shard_index(key, COALESCE_SHARDS));
        }
    }

    #[test]
    fn parses_max_age() {
        assert_eq!(parse_max_age("public, max-age=3600"), Some(3600));
        assert_eq!(parse_max_age("no-cache"), None);
        assert_eq!(parse_max_age("max-age=60, must-revalidate"), Some(60));
    }

    #[test]
    fn parses_retry_after_delta_and_http_date() {
        assert_eq!(parse_retry_after("120"), Some(Duration::from_secs(120)));
        assert_eq!(parse_retry_after(" 30 "), Some(Duration::from_secs(30)));
        // An HTTP-date in the past clamps to zero rather than being ignored.
        let past = parse_retry_after("Sun, 06 Nov 1994 08:49:37 GMT").unwrap();
        assert_eq!(past, Duration::ZERO);
        assert_eq!(parse_retry_after("not-a-date"), None);
    }

    #[test]
    fn no_store_policy_invalidates() {
        let config = CacheConfig {
            honor_cache_control: true,
            ..CacheConfig::default()
        };
        assert!(matches!(
            cache_policy_from(Some("no-store"), &config, "/movie/550"),
            CachePolicy::NoStore
        ));
        assert!(matches!(
            cache_policy_from(Some("no-cache"), &config, "/movie/550"),
            CachePolicy::NoStore
        ));
        assert!(matches!(
            cache_policy_from(Some("max-age=120"), &config, "/movie/550"),
            CachePolicy::Store(_)
        ));
    }

    #[test]
    fn appends_query_encoded() {
        let url = append_query("https://x/3/movie/550".to_string(), "api_key", "a b&c");
        assert_eq!(url, "https://x/3/movie/550?api_key=a+b%26c");
        let url = append_query("https://x/3/movie/550?language=en".to_string(), "k", "v");
        assert_eq!(url, "https://x/3/movie/550?language=en&k=v");
    }
}
