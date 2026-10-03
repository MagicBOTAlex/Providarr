use std::time::Duration;

use providarr::{api, config::AppConfig, state::AppState, telemetry};

/// Retention window for the append-only accounting tables. Rows older than this
/// are pruned by the hourly maintenance task. Kept as a constant rather than a
/// config field so the bound cannot be accidentally disabled in deployment.
const RETENTION_WINDOW: Duration = Duration::from_secs(60 * 60 * 24 * 30);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let config = AppConfig::load()?;
    let _log_guard = telemetry::init(&config.logging);

    let address = format!("{}:{}", config.server.host, config.server.port);
    let state = AppState::build(config).await?;

    let inbound = &state.config.inbound;
    if inbound.enabled {
        tracing::info!(
            requests_per_second = inbound.requests_per_second,
            burst = inbound.burst,
            global_requests_per_second = inbound.global_requests_per_second,
            global_burst = inbound.global_burst,
            max_concurrent = inbound.max_concurrent,
            trust_forwarded_for = inbound.trust_forwarded_for,
            trusted_proxies = state.inbound.trusted_proxy_count(),
            bypass = ?inbound.bypass,
            "inbound rate limiter active"
        );
    } else {
        tracing::warn!("inbound per-IP rate limiter is DISABLED (PROVIDARR_INBOUND_ENABLED=false)");
    }

    let api_auth = &state.config.api_auth;
    if api_auth.enabled && api_auth.api_key.is_empty() {
        tracing::warn!(
            "API authentication is ENABLED but no api_key is configured; \
             authenticated routes are inaccessible and unauthenticated routes are \
             rejected with 401"
        );
    }

    spawn_background_tasks(state.clone());

    let app = api::router(state);
    let listener = tokio::net::TcpListener::bind(&address).await?;
    tracing::info!(%address, "Providarr listening");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    Ok(())
}

fn spawn_background_tasks(state: AppState) {
    let flush_state = state.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(30));
        loop {
            tick.tick().await;
            if let Err(err) = flush_state.flush_stats().await {
                tracing::warn!(error = %err, "failed to flush endpoint stats");
            }
        }
    });

    let purge_state = state.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(3600));
        loop {
            tick.tick().await;
            let keep_stale = purge_state.config.cache.stale_if_error;
            match purge_state.cache.purge_expired(keep_stale).await {
                Ok(purged) if purged > 0 => {
                    tracing::info!(purged, "purged expired cache entries");
                }
                Ok(_) => {}
                Err(err) => tracing::warn!(error = %err, "failed to purge cache"),
            }
        }
    });

    // Bound append-only accounting tables so `endpoint_stats` and
    // `rate_limit_events` cannot grow without limit.
    let retention_state = state.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(3600));
        loop {
            tick.tick().await;
            match providarr::db::prune_endpoint_stats(&retention_state.pool, RETENTION_WINDOW).await
            {
                Ok(pruned) if pruned > 0 => {
                    tracing::info!(pruned, "pruned stale endpoint_stats rows");
                }
                Ok(_) => {}
                Err(err) => tracing::warn!(error = %err, "failed to prune endpoint_stats"),
            }
            match providarr::db::prune_rate_limit_events(&retention_state.pool, RETENTION_WINDOW)
                .await
            {
                Ok(pruned) if pruned > 0 => {
                    tracing::info!(pruned, "pruned stale rate_limit_events rows");
                }
                Ok(_) => {}
                Err(err) => tracing::warn!(error = %err, "failed to prune rate_limit_events"),
            }
        }
    });

    // Bound the per-IP limiter's keyed state so a large, rotating set of client
    // IPs cannot grow memory without limit. The limiter also enforces a hard cap
    // at request time; this periodic sweep keeps idle buckets from lingering.
    let inbound_state = state;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(60));
        loop {
            tick.tick().await;
            inbound_state.inbound.retain_recent();
            tracing::debug!(
                tracked_keys = inbound_state.inbound.tracked_keys(),
                "evicted stale inbound limiter state"
            );
        }
    });
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.ok();
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("shutdown signal received");
}
