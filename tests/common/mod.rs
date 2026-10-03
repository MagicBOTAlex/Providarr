#![allow(dead_code)]

use std::{sync::Arc, time::Duration};

use providarr::{
    cache::CacheStore,
    config::{AppConfig, AuthConfig, BackoffConfig, ProviderConfig},
    db,
    providers::ProviderRegistry,
    ratelimit::stats::StatsRegistry,
    replay::ReplayStore,
    state::AppState,
};
use sqlx::{
    AssertSqlSafe, Connection, Executor, PgConnection, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    raw_sql,
};
use tokio::sync::OnceCell;

/// A self-contained PostgreSQL server started once per test process. A `static`
/// is never dropped, so the server stays up for the whole run.
static EMBEDDED_POSTGRES: OnceCell<postgresql_embedded::PostgreSQL> = OnceCell::const_new();

async fn embedded_postgres() -> &'static postgresql_embedded::PostgreSQL {
    EMBEDDED_POSTGRES
        .get_or_init(|| async {
            use postgresql_embedded::{PostgreSQL, SettingsBuilder, V16};

            let settings = SettingsBuilder::new()
                .version((*V16).clone())
                .host("127.0.0.1")
                .port(0)
                .username("providarr")
                .password("providarr")
                .temporary(true)
                .build();

            let mut postgresql = PostgreSQL::new(settings);
            postgresql.setup().await.expect(
                "set up embedded postgres (first run downloads binaries; it cannot run as root)",
            );
            postgresql.start().await.expect("start embedded postgres");
            postgresql
                .create_database("providarr_test")
                .await
                .expect("create providarr_test database");

            postgresql
        })
        .await
}

/// The test database URL: `TEST_DATABASE_URL` when set (e.g. CI or a local
/// server), otherwise an on-demand embedded PostgreSQL.
pub async fn test_database_url() -> String {
    if let Ok(url) = std::env::var("TEST_DATABASE_URL") {
        return url;
    }

    embedded_postgres().await.settings().url("providarr_test")
}

/// Each call gets its own freshly-migrated schema so tests are fully isolated
/// (global operations like `purge_expired` cannot affect a sibling test).
pub async fn test_pool() -> PgPool {
    let url = test_database_url().await;
    let base: PgConnectOptions = url.parse().expect("parse test database url");
    let schema = format!("test_{}", uuid::Uuid::new_v4().simple());

    // Create the schema on a single connection before any pool connection opens.
    let mut conn = PgConnection::connect_with(&base)
        .await
        .expect("connect to test database (is Postgres running?)");
    let create_sql = format!("CREATE SCHEMA \"{schema}\"");
    conn.execute(raw_sql(AssertSqlSafe(create_sql)))
        .await
        .expect("create test schema");
    drop(conn);

    let options = base.options([("search_path", schema)]);
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await
        .expect("connect to test schema");
    db::migrate(&pool).await.expect("run migrations");
    pool
}

/// Unique name so parallel tests never share accounting rows or cache keys.
pub fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4().simple())
}

/// Builds an `AppState` against the test database. Providers are pointed at an
/// unreachable address and replay is off by default, so tests must opt in to
/// whatever behaviour they exercise.
pub async fn app_state<F>(mutate: F) -> AppState
where
    F: FnOnce(&mut AppConfig),
{
    let mut config = AppConfig::default();
    config.database.url = test_database_url().await;
    config.cache.enabled = false;
    config.replay.enabled = false;
    config.inbound.enabled = false;

    for provider in config.providers.values_mut() {
        provider.base_url = "http://127.0.0.1:1".to_string();
        provider.auth = AuthConfig::None;
    }

    mutate(&mut config);
    AppState::build(config).await.expect("build app state")
}

pub struct TestRegistry {
    pub registry: Arc<ProviderRegistry>,
    pub cache: Arc<CacheStore>,
    pub stats: Arc<StatsRegistry>,
    pub pool: PgPool,
    pub provider_name: String,
}

pub async fn registry_with_provider(base_url: &str, cache_enabled: bool) -> TestRegistry {
    registry_custom(base_url, cache_enabled, None, |_| {}).await
}

pub async fn registry_custom<F>(
    base_url: &str,
    cache_enabled: bool,
    backoff: Option<BackoffConfig>,
    mutate: F,
) -> TestRegistry
where
    F: FnOnce(&mut AppConfig),
{
    let pool = test_pool().await;
    let mut config = AppConfig::default();
    config.cache.enabled = cache_enabled;
    config.cache.default_ttl = Duration::from_secs(60);
    config.replay.enabled = false;
    config.inbound.enabled = false;
    // Keep the legacy test behaviour: honour Cache-Control and don't apply the
    // per-endpoint policy/floor, so tests can control TTLs explicitly.
    config.cache.honor_cache_control = true;
    config.cache.min_ttl = Duration::from_secs(0);
    config.cache.endpoint_ttl.clear();
    // Existing tests assert single-attempt behaviour; opt in to retries explicitly.
    config.backoff.max_retries = 0;
    if let Some(backoff) = backoff {
        config.backoff = backoff;
    }
    config.providers.clear();

    let mut provider = ProviderConfig::tmdb_default();
    provider.base_url = base_url.trim_end_matches('/').to_string();
    provider.auth = AuthConfig::None;
    provider.requests_per_second = 1000.0;
    provider.burst = 1000;
    provider.max_concurrency = 4;

    let provider_name = unique("test");
    config.providers.insert(provider_name.clone(), provider);
    mutate(&mut config);

    let stats = Arc::new(StatsRegistry::new());
    let cache = Arc::new(CacheStore::new(pool.clone(), config.cache.clone()));
    let replay = ReplayStore::load_arc(&config.replay.dir, config.replay.record);
    let registry = Arc::new(
        ProviderRegistry::build(&config, pool.clone(), cache.clone(), stats.clone(), replay)
            .expect("build provider registry"),
    );

    TestRegistry {
        registry,
        cache,
        stats,
        pool,
        provider_name,
    }
}

pub fn snapshot_for<'a>(
    snaps: &'a [providarr::ratelimit::stats::EndpointStatSnapshot],
    provider: &str,
) -> &'a providarr::ratelimit::stats::EndpointStatSnapshot {
    snaps
        .iter()
        .find(|snap| snap.provider == provider)
        .expect("snapshot for provider")
}
