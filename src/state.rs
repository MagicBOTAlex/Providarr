use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use metrics_exporter_prometheus::PrometheusHandle;
use sqlx::PgPool;

use crate::{
    cache::CacheStore, config::AppConfig, db, error::AppError, inbound::InboundLimiter,
    providers::ProviderRegistry, ratelimit::stats::StatsRegistry, replay::ReplayStore,
};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<AppConfig>,
    pub pool: PgPool,
    pub cache: Arc<CacheStore>,
    pub stats: Arc<StatsRegistry>,
    pub registry: Arc<ProviderRegistry>,
    pub replay: Arc<ReplayStore>,
    pub inbound: Arc<InboundLimiter>,
    pub metrics: PrometheusHandle,
    pub started_at: Instant,
}

impl AppState {
    pub async fn build(config: AppConfig) -> Result<Self, AppError> {
        let pool = db::connect(&config.database).await?;
        db::migrate(&pool).await?;

        let stats = Arc::new(StatsRegistry::new());
        for snapshot in db::load_endpoint_stats(&pool).await? {
            stats.seed(&snapshot);
        }

        let cache = Arc::new(CacheStore::new(pool.clone(), config.cache.clone()));
        cache.seed_stats().await?;
        let replay = ReplayStore::load_arc(&config.replay.dir, config.replay.record);
        let inbound = Arc::new(InboundLimiter::new(&config.inbound));
        let registry = Arc::new(ProviderRegistry::build(
            &config,
            pool.clone(),
            cache.clone(),
            stats.clone(),
            replay.clone(),
        )?);

        // Resume any backoff window that was still active before a restart.
        let now = chrono::Utc::now();
        for row in db::load_provider_backoff(&pool).await? {
            let elapsed_ms = (now - row.updated_at).num_milliseconds().max(0) as u64;
            let remaining = Duration::from_millis(row.remaining_ms.max(0) as u64)
                .saturating_sub(Duration::from_millis(elapsed_ms));
            if row.consecutive_failures > 0 && !remaining.is_zero() {
                registry.restore_backoff(&row.provider, row.consecutive_failures as u32, remaining);
            } else {
                // Consumed/expired window: delete the row instead of leaving it
                // around forever.
                if let Err(err) = db::clear_provider_backoff(&pool, &row.provider).await {
                    tracing::warn!(error = %err, provider = %row.provider, "failed to clear expired provider backoff");
                }
            }
        }

        Ok(Self {
            config: Arc::new(config),
            pool,
            cache,
            stats,
            registry,
            replay,
            inbound,
            metrics: crate::metrics::install(),
            started_at: Instant::now(),
        })
    }

    pub async fn flush_stats(&self) -> Result<(), AppError> {
        db::upsert_endpoint_stats(&self.pool, &self.stats.snapshot()).await
    }
}
