use std::{
    collections::{BTreeMap, HashMap},
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{Path, Query, RawQuery, State},
    http::{Method, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use bytes::Bytes;
use serde_json::json;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tower_http::{limit::RequestBodyLimitLayer, timeout::TimeoutLayer, trace::TraceLayer};

use crate::{error::AppError, inbound::InboundLimiter, state::AppState};

pub fn router(state: AppState) -> Router {
    let request_timeout = state.config.server.request_timeout;
    let max_body = state.config.cache.max_body_bytes;

    Router::new()
        .route("/health", get(health))
        .route("/v1/policy", get(policy))
        .route("/metrics", get(metrics_handler))
        .route("/v1/ratelimits", get(ratelimits))
        .route("/v1/ratelimits/history", get(ratelimits_history))
        .route("/v1/cache/stats", get(cache_stats))
        .route("/v1/{provider}/{*path}", get(proxy))
        // Theoriarr-facing metadata API (Radarr/SkyHook shapes).
        .route(
            "/radarr/v1/movie/{id}",
            get(radarr_movie).post(radarr_movie_bulk),
        )
        .route(
            "/radarr/v1/movie/collection/{id}",
            get(radarr_movie_collection),
        )
        .route("/radarr/v1/movie/imdb/{id}", get(radarr_movie_imdb))
        .route("/radarr/v1/search", get(radarr_search))
        .route("/radarr/v1/list/tmdb/{kind}", get(radarr_list))
        .route(
            "/sonarr/v1/tvdb/shows/{language}/{id}",
            get(metadata_series),
        )
        .route("/sonarr/v1/tvdb/search/{language}", get(sonarr_search))
        // Theoriarr emits the trailing slash; axum treats it as a distinct path.
        .route("/sonarr/v1/tvdb/search/{language}/", get(sonarr_search))
        // Sonarr ancillary health checks served locally so Theoriarr never has to
        // talk to services.sonarr.tv. Scene mapping and daily series are handled
        // independently (TheXEM in Theoriarr; TVDB air days in the series mapper).
        .route("/sonarr/services/time", get(services_time))
        .route("/sonarr/services/ping", get(services_ping))
        .fallback(not_found)
        // Layers are applied inner-to-outer, so the last `.layer` added is the
        // outermost. Inbound rate limiting must run before auth, otherwise an
        // unauthenticated flood is rejected (cheaply, but without consuming any
        // per-IP budget) before the limiter ever sees it.
        .layer(middleware::from_fn_with_state(
            state.clone(),
            api_auth_middleware,
        ))
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::with_status_code(
            StatusCode::GATEWAY_TIMEOUT,
            request_timeout,
        ))
        .layer(RequestBodyLimitLayer::new(max_body))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            inbound_middleware,
        ))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Result<Json<serde_json::Value>, AppError> {
    crate::db::ping(&state.pool).await?;
    let providers = state
        .registry
        .names()
        .into_iter()
        .map(|name| {
            let has_credentials = state
                .registry
                .provider(&name)
                .map(|provider| provider.auth.has_credentials())
                .unwrap_or(false);
            (name, has_credentials)
        })
        .collect::<BTreeMap<_, _>>();

    Ok(Json(json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_secs": state.started_at.elapsed().as_secs(),
        "cache_enabled": state.cache.enabled(),
        "replay_enabled": state.registry.replay_enabled(),
        "inbound_limiter_enabled": state.inbound.enabled(),
        "providers": providers,
    })))
}

/// Public disclosure of the effective cache policy and rate limits, so clients
/// (e.g. Theoriarr) can show the server's TTLs and advise self-hosting.
async fn policy(State(state): State<AppState>) -> Json<serde_json::Value> {
    let config = &state.config;
    let cache = &config.cache;

    let endpoint_ttl = cache
        .endpoint_ttl
        .iter()
        .map(|(name, ttl)| (name.clone(), ttl.0.as_secs()))
        .collect::<BTreeMap<_, _>>();

    let providers = config
        .providers
        .iter()
        .map(|(name, provider)| {
            (
                name.clone(),
                json!({
                    "requests_per_second": provider.requests_per_second,
                    "burst": provider.burst,
                    "documented_rate_limit": provider.documented_rate_limit,
                    "endpoint_rps": provider.endpoint_rps,
                }),
            )
        })
        .collect::<BTreeMap<_, _>>();

    Json(json!({
        "cache": {
            "enabled": cache.enabled,
            "default_ttl_seconds": cache.default_ttl.as_secs(),
            "max_ttl_seconds": cache.max_ttl.as_secs(),
            "stale_if_error_seconds": cache.stale_if_error.as_secs(),
            "endpoint_ttl_seconds": endpoint_ttl,
        },
        "rate_limits": {
            "inbound": {
                "enabled": config.inbound.enabled,
                "requests_per_second": config.inbound.requests_per_second,
                "burst": config.inbound.burst,
                "global": {
                    "enabled": config.inbound.global_requests_per_second > 0.0,
                    "requests_per_second": config.inbound.global_requests_per_second,
                    "burst": config.inbound.global_burst,
                },
                "max_concurrent": config.inbound.max_concurrent,
                // Only the size of the bypass set is public: the individual
                // addresses would let an attacker aim traffic away from the rules.
                "bypass_count": state.inbound.bypass_count(),
            },
            "backoff": {
                "base_delay_ms": config.backoff.base_delay.as_millis() as u64,
                "max_delay_ms": config.backoff.max_delay.as_millis() as u64,
                "max_retries": config.backoff.max_retries,
                "drop_after_wait_ms": config.backoff.drop_after_wait.as_millis() as u64,
            },
            "providers": providers,
        },
        "disclosure": {
            "caching": "Aggressive by default: 3-day default TTL, 7-day movie/series metadata, up to 30-day static reference data, 14-day stale-if-error.",
            "inbound_rate_limit": "Per-IP inbound limiter plus a server-wide ceiling and in-flight concurrency cap are enabled by default (see rate_limits.inbound).",
            "recommendation": "Host your own Providarr instance instead of relying on the shared public one (providarr.deprived.dev).",
        },
    }))
}

async fn metrics_handler(State(state): State<AppState>) -> Response {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.metrics.render(),
    )
        .into_response()
}

async fn cache_stats(State(state): State<AppState>) -> Result<Json<serde_json::Value>, AppError> {
    let stats = state.cache.stats().await?;
    Ok(Json(json!(stats)))
}

async fn ratelimits(State(state): State<AppState>) -> Result<Json<serde_json::Value>, AppError> {
    let mut endpoints = Vec::new();
    for snapshot in state.stats.snapshot() {
        let mut value = serde_json::to_value(&snapshot)?;
        let provider = state.registry.provider(&snapshot.provider);
        let backoff_ms = provider
            .as_ref()
            .map(|p| p.runtime.backoff_remaining().as_millis() as u64)
            .unwrap_or(0);
        let failures = provider
            .as_ref()
            .map(|p| p.runtime.consecutive_failures())
            .unwrap_or(0);

        crate::metrics::backoff_seconds(
            &snapshot.provider,
            &snapshot.endpoint,
            backoff_ms as f64 / 1000.0,
        );

        if let Some(object) = value.as_object_mut() {
            object.insert("backoff_remaining_ms".into(), json!(backoff_ms));
            object.insert("consecutive_failures".into(), json!(failures));
            if let Some(provider) = &provider {
                object.insert(
                    "documented_rate_limit".into(),
                    json!(provider.config.documented_rate_limit),
                );
                object.insert(
                    "requests_per_second".into(),
                    json!(provider.config.requests_per_second),
                );
                object.insert("burst".into(), json!(provider.config.burst));
            }
        }
        endpoints.push(value);
    }

    Ok(Json(json!({
        "generated_at": chrono::Utc::now(),
        "endpoints": endpoints,
    })))
}

async fn ratelimits_history(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, AppError> {
    let provider = params.get("provider").map(String::as_str);
    let limit = params
        .get("limit")
        .and_then(|l| l.parse::<i64>().ok())
        .unwrap_or(100)
        .clamp(1, 1000);

    let events = crate::db::load_rate_limit_events(&state.pool, provider, limit).await?;
    Ok(Json(json!({ "events": events })))
}

async fn proxy(
    State(state): State<AppState>,
    Path((provider, path)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    method: Method,
) -> Result<Response, AppError> {
    if method != Method::GET && method != Method::HEAD {
        return Err(AppError::Internal(format!(
            "method {method} is not supported"
        )));
    }

    let mut path_and_query = format!("/{}", path.trim_start_matches('/'));
    if let Some(query) = query
        && !query.is_empty()
    {
        path_and_query.push('?');
        path_and_query.push_str(&query);
    }

    let response = state
        .registry
        .fetch(&provider, method.as_str(), &path_and_query)
        .await?;

    let cache_state = if response.stale {
        "stale"
    } else if response.cached {
        "hit"
    } else {
        "miss"
    };

    let mut builder = Response::builder()
        .status(StatusCode::from_u16(response.status).unwrap_or(StatusCode::BAD_GATEWAY))
        .header("x-providarr-cache", cache_state)
        .header("x-providarr-provider", provider);

    if response.replayed {
        builder = builder.header("x-providarr-replay", "true");
    }

    if let Some(content_type) = response.content_type {
        builder = builder.header(header::CONTENT_TYPE, content_type);
    }

    builder
        .body(axum::body::Body::from(response.body))
        .map_err(|err| AppError::Internal(err.to_string()))
}

async fn radarr_movie(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response, AppError> {
    if id == "changed" {
        if state.config.replay.enabled {
            return json_value(&Vec::<i64>::new());
        }
        let ids =
            crate::mapper::changed_movies(&state, params.get("since").map(String::as_str)).await?;
        return json_value(&ids);
    }

    if state.config.replay.enabled {
        let body = state.replay.movie(&id);
        return metadata_fixture(&state, "movie", body);
    }

    let tmdb_id: i64 = id.parse().map_err(|_| AppError::NotFound)?;
    let (movie, report) = crate::cache_report::scope(crate::mapper::movie(&state, tmdb_id)).await;
    json_value_report(&movie?, &report)
}

async fn radarr_movie_bulk(
    State(state): State<AppState>,
    Json(ids): Json<Vec<i64>>,
) -> Result<Response, AppError> {
    if state.config.replay.enabled {
        return metadata_fixture(&state, "movie", None);
    }

    let movies = crate::mapper::bulk(&state, &ids).await?;
    json_value(&movies)
}

async fn radarr_movie_collection(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, AppError> {
    if state.config.replay.enabled {
        return metadata_fixture(&state, "collection", None);
    }

    let tmdb_id: i64 = id.parse().map_err(|_| AppError::NotFound)?;
    let (collection, report) =
        crate::cache_report::scope(crate::mapper::collection(&state, tmdb_id)).await;
    json_value_report(&collection?, &report)
}

async fn radarr_movie_imdb(
    State(state): State<AppState>,
    Path(imdb_id): Path<String>,
) -> Result<Response, AppError> {
    if state.config.replay.enabled {
        return metadata_fixture(&state, "imdb", None);
    }

    let movies = crate::mapper::movie_by_imdb(&state, &imdb_id).await?;
    json_value(&movies)
}

async fn radarr_search(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response, AppError> {
    if state.config.replay.enabled {
        return metadata_fixture(&state, "search", None);
    }

    let query = params.get("q").cloned().unwrap_or_default();
    let movies =
        crate::mapper::search(&state, &query, params.get("year").map(String::as_str)).await?;
    json_value(&movies)
}

async fn radarr_list(
    State(state): State<AppState>,
    Path(kind): Path<String>,
) -> Result<Response, AppError> {
    if state.config.replay.enabled {
        return metadata_fixture(&state, "list", None);
    }

    let movies = crate::mapper::list(&state, &kind).await?;
    json_value(&movies)
}

async fn metadata_series(
    State(state): State<AppState>,
    Path((language, id)): Path<(String, String)>,
) -> Result<Response, AppError> {
    if state.config.replay.enabled {
        let body = state.replay.series(&id);
        return metadata_fixture(&state, "series", body);
    }

    let tvdb_id: i64 = id.parse().map_err(|_| AppError::NotFound)?;
    let (show, report) =
        crate::cache_report::scope(crate::mapper::series::show(&state, tvdb_id, &language)).await;
    json_value_report(&show?, &report)
}

async fn sonarr_search(
    State(state): State<AppState>,
    Path(language): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response, AppError> {
    if state.config.replay.enabled {
        return metadata_fixture(&state, "search", None);
    }

    let term = params.get("term").cloned().unwrap_or_default();
    let shows = crate::mapper::series::search(&state, &term, &language).await?;
    json_value(&shows)
}

async fn services_time() -> Result<Response, AppError> {
    json_value(&json!({ "dateTimeUtc": chrono::Utc::now().to_rfc3339() }))
}

async fn services_ping() -> StatusCode {
    StatusCode::OK
}

fn metadata_fixture(
    state: &AppState,
    kind: &str,
    body: Option<Bytes>,
) -> Result<Response, AppError> {
    if !state.config.replay.enabled {
        return Err(AppError::NotImplemented(
            "provider metadata mapping is not implemented; enable replay fixtures".to_string(),
        ));
    }

    match body {
        Some(body) => {
            crate::metrics::replayed("metadata", kind);
            Ok(json_bytes(body))
        }
        None => {
            crate::metrics::replay_miss(kind);
            Err(AppError::NotFound)
        }
    }
}

fn json_bytes(body: Bytes) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-providarr-source", "fixture")
        .body(axum::body::Body::from(body))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

fn json_value<T: serde::Serialize>(value: &T) -> Result<Response, AppError> {
    json_response(serde_json::to_value(value)?)
}

/// Like [`json_value`], but attaches the per-request cache report under `_providarr`.
fn json_value_report<T: serde::Serialize>(
    value: &T,
    report: &crate::cache_report::CacheReport,
) -> Result<Response, AppError> {
    let mut json = serde_json::to_value(value)?;

    if !report.is_empty()
        && let Some(object) = json.as_object_mut()
    {
        object.insert("_providarr".to_string(), report.to_json());
    }

    json_response(json)
}

fn json_response(json: serde_json::Value) -> Result<Response, AppError> {
    let body = serde_json::to_vec(&json)?;
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-providarr-source", "provider")
        .body(axum::body::Body::from(body))
        .map_err(|err| AppError::Internal(err.to_string()))
}

async fn api_auth_middleware(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let auth = &state.config.api_auth;
    let path = request.uri().path();
    if auth.enabled && !auth.api_key.is_empty() && path != "/health" && path != "/v1/policy" {
        let provided = request
            .headers()
            .get(auth.header.as_str())
            .and_then(|value| value.to_str().ok());
        if !provided.is_some_and(|provided| constant_time_eq(provided, &auth.api_key)) {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "unauthorized" })),
            )
                .into_response();
        }
    }

    next.run(request).await
}

async fn inbound_middleware(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    if !state.inbound.enabled() {
        return next.run(request).await;
    }

    let method = request.method().clone();
    let path = request.uri().path().to_string();

    let Some(ip) = client_ip(&request, &state.inbound) else {
        tracing::debug!(method = %method, path = %path, "inbound request had no resolvable client IP; limiter skipped");
        return next.run(request).await;
    };

    if state.inbound.is_bypassed(ip) {
        crate::metrics::inbound_bypassed();
        tracing::debug!(client_ip = %ip, method = %method, path = %path, "inbound request exempt via bypass rule");
        return next.run(request).await;
    }

    // The server-wide ceiling runs *before* the per-IP bucket is allocated or
    // updated, so a flood from many distinct IPs cannot first grow the keyed
    // state and only then get rejected.
    if let Err(wait) = state.inbound.check_global() {
        crate::metrics::inbound_global_limited();
        let retry_after_secs = retry_after_secs(wait);

        tracing::warn!(
            client_ip = %ip,
            method = %method,
            path = %path,
            retry_after_secs,
            "inbound global rate limit hit; request rejected with 429"
        );

        return too_many_requests(retry_after_secs);
    }

    if let Err(wait) = state.inbound.check(ip) {
        crate::metrics::inbound_limited();
        let retry_after_secs = retry_after_secs(wait);

        tracing::warn!(
            client_ip = %ip,
            method = %method,
            path = %path,
            retry_after_secs,
            "inbound per-IP rate limit hit; request rejected with 429"
        );

        return too_many_requests(retry_after_secs);
    }

    // Hold the permit for the whole request so the cap is honoured during upstream work.
    let _permit = match state.inbound.acquire_concurrency() {
        Ok(permit) => permit,
        Err(wait) => {
            crate::metrics::inbound_concurrency_limited();
            let retry_after_secs = retry_after_secs(wait);

            tracing::warn!(
                client_ip = %ip,
                method = %method,
                path = %path,
                "inbound concurrency limit hit; request rejected with 429"
            );

            return too_many_requests(retry_after_secs);
        }
    };

    crate::metrics::inbound_allowed();
    tracing::debug!(client_ip = %ip, method = %method, path = %path, "inbound request allowed");

    next.run(request).await
}

fn too_many_requests(retry_after_secs: u64) -> Response {
    let mut response = (
        StatusCode::TOO_MANY_REQUESTS,
        Json(json!({
            "error": "rate limit exceeded",
            "retry_after_secs": retry_after_secs,
        })),
    )
        .into_response();

    if let Ok(value) = retry_after_secs.to_string().parse() {
        response.headers_mut().insert("retry-after", value);
    }

    response
}

/// Whole seconds to advertise in `Retry-After`, rounded up with a floor of one.
fn retry_after_secs(wait: Duration) -> u64 {
    (wait.as_secs_f64().ceil() as u64).max(1)
}

/// Constant-time comparison over SHA-256 digests, so neither the comparison time
/// nor the key length leaks. Hashing both sides first keeps the compared slices
/// the same length.
fn constant_time_eq(provided: &str, expected: &str) -> bool {
    let provided = Sha256::digest(provided.as_bytes());
    let expected = Sha256::digest(expected.as_bytes());
    provided.ct_eq(&expected).into()
}

/// Resolves the client IP, delegating trust decisions to the limiter.
///
/// Without a `ConnectInfo` peer (e.g. a test harness) no IP can be resolved and
/// the limiter is skipped, so callers must supply one in production.
fn client_ip(request: &axum::extract::Request, inbound: &InboundLimiter) -> Option<IpAddr> {
    let peer = request
        .extensions()
        .get::<axum::extract::ConnectInfo<SocketAddr>>()
        .map(|connect_info| connect_info.0.ip())?;

    let forwarded_for = request
        .headers()
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok());
    let real_ip = request
        .headers()
        .get("x-real-ip")
        .and_then(|value| value.to_str().ok());

    Some(inbound.resolve_client_ip(peer, forwarded_for, real_ip))
}

async fn not_found() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" })))
}
