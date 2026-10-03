use std::{net::SocketAddr, sync::Arc, time::Duration};

use hyper::server::conn::http1;
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    service::TowerToHyperService,
};
use providarr::{api, config::AppConfig, state::AppState, telemetry};
use tokio::sync::{Semaphore, watch};
use tower::{Service as _, ServiceExt as _};

/// Retention window for the append-only accounting tables. Rows older than this
/// are pruned by the hourly maintenance task. Kept as a constant rather than a
/// config field so the bound cannot be accidentally disabled in deployment.
const RETENTION_WINDOW: Duration = Duration::from_secs(60 * 60 * 24 * 30);

/// Maximum time a client may take to transmit its request headers. Bounds
/// slowloris-style clients that trickle headers to hold connections open. axum's
/// `serve` is intentionally unconfigurable, so the server runs a custom hyper
/// accept loop that can set this on the connection builder.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Upper bound on concurrently served connections. A permit is acquired before
/// a connection task is spawned and released when it ends, so a connection
/// flood cannot exhaust file descriptors, memory, or spawned tasks. Excess
/// clients wait in the OS accept backlog rather than in our process.
const MAX_CONCURRENT_CONNECTIONS: usize = 1024;

/// How long to pause after an accept failure caused by file-descriptor
/// exhaustion (`EMFILE`/`ENFILE`), mirroring hyper's historical behavior.
const ACCEPT_BACKOFF: Duration = Duration::from_secs(1);

/// Hard deadline for draining in-flight connections after a shutdown signal.
/// Once it elapses `serve` returns so the process can exit even if a connection
/// refuses to finish.
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(10);

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

    spawn_background_tasks(state.clone());

    let app = api::router(state);
    let listener = tokio::net::TcpListener::bind(&address).await?;
    tracing::info!(%address, "Providarr listening");

    serve(listener, app, HEADER_READ_TIMEOUT).await?;

    Ok(())
}

/// Runs a custom hyper accept loop so connection-level knobs (notably the
/// HTTP/1 header-read timeout) are configurable, which `axum::serve` does not
/// allow.
///
/// Only HTTP/1 is served: deployments sit behind a reverse proxy that speaks
/// HTTP/1.1 to the backend, and hyper's HTTP/1 builder arms the header-read
/// timeout against the very first read (whereas the auto builder sniffs for the
/// HTTP/2 preface first and HTTP/2 has no header/keepalive deadline).
///
/// `ConnectInfo<SocketAddr>` is preserved by routing every accepted connection
/// through `IntoMakeServiceWithConnectInfo` (which implements `Service<SocketAddr>`)
/// before wrapping the resulting tower service for hyper. Graceful shutdown is
/// preserved with the same `watch`-channel pattern `axum::serve` uses, plus a
/// hard drain deadline.
async fn serve(
    listener: tokio::net::TcpListener,
    app: axum::Router,
    header_read_timeout: Duration,
) -> anyhow::Result<()> {
    let mut make_service = app.into_make_service_with_connect_info::<SocketAddr>();

    // Dropping the receiver makes every `Sender::closed()` resolve, which the
    // accept loop and each in-flight connection observe as the shutdown signal.
    let (signal_tx, signal_rx) = watch::channel(());
    tokio::spawn(async move {
        // Wait for the first signal, then trigger graceful shutdown.
        shutdown_signal().await;
        drop(signal_rx);

        // A second signal forces immediate exit rather than waiting out the
        // drain deadline.
        shutdown_signal().await;
        tracing::warn!("second shutdown signal received; forcing immediate exit");
        std::process::exit(0);
    });

    // Held open until every connection task has finished, so we can wait for
    // in-flight requests after the listener stops accepting.
    let (close_tx, close_rx) = watch::channel(());

    // Bounds connections/tasks spawned by the accept loop.
    let connections = Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTIONS));

    loop {
        let (stream, remote_addr) = tokio::select! {
            conn = listener.accept() => match conn {
                Ok(pair) => pair,
                Err(err) if is_connection_error(&err) => {
                    // The peer went away before we accepted it. Ignore and keep
                    // serving; one bad connection must never stop the server.
                    tracing::debug!(error = %err, "ignoring transient accept error");
                    continue;
                }
                Err(err) if is_fd_exhaustion(&err) => {
                    // Too many open files: log, back off, and retry instead of
                    // aborting the process.
                    tracing::error!(error = %err, "accept failed (fd exhaustion); backing off");
                    tokio::time::sleep(ACCEPT_BACKOFF).await;
                    continue;
                }
                Err(err) => return Err(err.into()),
            },
            _ = signal_tx.closed() => break,
        };

        // Acquire a slot before spawning; released when the task ends. If the
        // semaphore were ever closed this would be fatal, but we never close it.
        // The shutdown branch prevents waiting on a full semaphore from
        // deadlocking graceful shutdown.
        let permit = tokio::select! {
            permit = Arc::clone(&connections).acquire_owned() => {
                permit.expect("connection semaphore is never closed")
            }
            _ = signal_tx.closed() => break,
        };

        let io = TokioIo::new(stream);

        let tower_service = make_service
            .call(remote_addr)
            .await
            .map(|service| {
                service.map_request(|request: axum::extract::Request<hyper::body::Incoming>| {
                    request.map(axum::body::Body::new)
                })
            })
            .unwrap_or_else(|err| match err {});

        let hyper_service = TowerToHyperService::new(tower_service);
        let signal_tx = signal_tx.clone();
        let close_rx = close_rx.clone();

        tokio::spawn(async move {
            let mut builder = http1::Builder::new();
            builder
                .timer(TokioTimer::new())
                .header_read_timeout(header_read_timeout);

            let mut conn = std::pin::pin!(builder.serve_connection(io, hyper_service));
            let mut signal_closed = std::pin::pin!(signal_tx.closed());
            let mut shutting_down = false;

            loop {
                tokio::select! {
                    result = conn.as_mut() => {
                        if let Err(err) = result {
                            tracing::debug!(error = %err, "connection closed with error");
                        }
                        break;
                    }
                    _ = &mut signal_closed, if !shutting_down => {
                        shutting_down = true;
                        conn.as_mut().graceful_shutdown();
                    }
                }
            }

            drop(close_rx);
            drop(permit);
        });
    }

    drop(close_rx);
    drop(listener);

    // Wait for all connection tasks to observe the shutdown and drain, but not
    // forever: a stuck connection must not prevent process exit.
    if tokio::time::timeout(SHUTDOWN_DEADLINE, close_tx.closed())
        .await
        .is_err()
    {
        tracing::warn!(
            deadline = ?SHUTDOWN_DEADLINE,
            "shutdown deadline elapsed with connections still open; exiting anyway"
        );
    }

    Ok(())
}

/// Returns `true` for accept errors that merely mean the pending connection
/// vanished before it could be accepted.
fn is_connection_error(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
    )
}

/// Returns `true` when an accept failed because the process ran out of file
/// descriptors (`EMFILE`) or the system-wide limit was hit (`ENFILE`).
fn is_fd_exhaustion(err: &std::io::Error) -> bool {
    #[cfg(unix)]
    {
        matches!(err.raw_os_error(), Some(libc::EMFILE) | Some(libc::ENFILE))
    }
    #[cfg(not(unix))]
    {
        let _ = err;
        false
    }
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

/// Resolves on the next SIGINT (Ctrl-C) or, on Unix, SIGTERM. Safe to call
/// repeatedly; each call waits for a fresh signal.
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
