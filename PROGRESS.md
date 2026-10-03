# Providarr Progress

## HANDOFF — state as of 2026-10-02 (resume here next month)

### TL;DR
Providarr is a standalone Rust service that **replaces both upstream metadata services** for
Theoriarr: Radarr's `api.radarr.video` (movies) and Sonarr's `skyhook.sonarr.tv` (series). It maps
**live TMDb/TVDB** into the Radarr `MovieResource` / SkyHook `ShowResource` JSON shapes, behind:
per-provider rate limiting + progressive backoff, an **aggressive Postgres cache**
(per-resource TTLs, single-flight coalescing, stale-while-revalidate), optional per-IP inbound
limiting (per-IP plus a server-wide ceiling and in-flight concurrency cap) with request logging, and
optional API-key auth. Movie and series add/lookup/refresh are **confirmed end-to-end against
Theoriarr** with live APIs, and Theoriarr's only metadata backend is now Providarr (the upstream
source enum/URLs were removed). **106 tests pass**, clippy clean, Docker images verified.

### Repos, remotes, branches
| Repo | Path | Remote | Branch | Last commit (at handoff) |
|---|---|---|---|---|
| Providarr | `/root/projects/Providarr` | `git@github.com:MagicBOTAlex/Providarr.git` | `master` | `846a663` |
| Theoriarr | `/root/projects/Theoriarr` | `git@github.com:MagicBOTAlex/Theoriarr.git` | `main` | `025514a` |

Commit identity used in this sandbox: Providarr `Providarr Bot <providarr@localhost>`, Theoriarr
`Theoriarr Bot <theoriarr@localhost>`.

### Fresh sandbox setup (do this first next time)
```bash
# Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal
~/.cargo/bin/rustup component add rustfmt clippy
# System deps (linker + aws-lc-sys build) and Postgres
apt-get install -y build-essential pkg-config cmake clang libssl-dev postgresql postgresql-contrib libpq-dev
chmod 1777 /tmp            # sandbox quirk: /tmp was 0755, breaks apt
pg_ctlcluster 16 main start
su postgres -c "psql -c \"CREATE ROLE providarr LOGIN PASSWORD 'providarr' CREATEDB;\""
su postgres -c "createdb -O providarr providarr"
su postgres -c "createdb -O providarr providarr_test"
```
`.env` (gitignored) needs: `TMDB_API_TOKEN` (TMDb v4 read token), `TVDB_API_KEY`, `DATABASE_URL`,
`PROVIDARR_CONFIG=config/config.json`, `RUST_LOG`. **Keys belong here, never in the repo.**

### Build / run / test
```bash
cd /root/projects/Providarr
cargo run                                   # listens 0.0.0.0:4155
cargo test                                  # auto-starts embedded PostgreSQL (TEST_DATABASE_URL overrides)
cargo clippy --all-targets && cargo fmt
```
- Live config is `config/config.json` (`replay.enabled=false`, mappers on).
- `PROVIDARR_CONFIG=/tmp/opencode/providarr-live.json` was used for live E2E (a copy with replay off).
- Docker: `docker build -t providarr:embedded -f Dockerfile.embedded .`; embedded runs Postgres in
  the same container. Compose: `docker-compose.yml` (external PG), `docker-compose.embedded.yml`.

### Theoriarr build / E2E
```bash
cd /root/projects/Theoriarr
SKIP_FRONTEND=1 ./build.sh
SKIP_BUILD=1 ./test-parallel.sh             # Core 6250, Common 706, Api 200, Host 18, Http 51, Update 17, Libraries 2
# runtime (unified host; uses SQLite under -data):
cd src/Theoriarr.Series/_output/net10.0
THEORIARR__METADATA__PROVIDARRBASEURL=http://127.0.0.1:4155 \
setsid ./Sonarr -data=<tmpdir> -nobrowser -noinstancecheck &   # port 6868
```
**Critical gotcha:** the unified host selects Series vs Movies endpoints from the **API key** in
`<tmpdir>/config.xml` — `X-Api-Key: <ApiKey>` for series, `X-Api-Key: <MovieApiKey>` for movies.
  Wrong key → 404. The metadata address is read from `THEORIARR__METADATA__PROVIDARRBASEURL`
  (double underscore = `:`) or from **Settings > Metadata Source** (applied on restart; env wins).

### What is implemented and verified

**Movie mapper** (`src/mapper/mod.rs`): TMDb → Radarr `MovieResource`. Routes (live when replay off):
`GET /radarr/v1/movie/{tmdbId}`, `GET /radarr/v1/movie/collection/{id}`,
`GET /radarr/v1/movie/imdb/{imdbId}`, `POST /radarr/v1/movie/bulk` (body `[ids]`),
`GET /radarr/v1/movie/changed?since=` (TMDb `/movie/changes`, 14-day clamp),
`GET /radarr/v1/search?q=&year=`, `GET /radarr/v1/list/tmdb/{trending,popular}`.
Live-verified against the live TMDb API (real ids resolve to real movies; titles are not
reproduced here).

**Series mapper** (`src/mapper/series.rs`): TVDB → SkyHook `ShowResource`. Routes:
`GET /sonarr/v1/tvdb/shows/{language}/{id}`, `GET /sonarr/v1/tvdb/search/{language}?term=`.
Uses 3 TVDB calls for a show (`/series/{id}/extended`, `/series/{id}/translations/{lang}`,
`/series/{id}/episodes/default` paginated) + 1 TMDb call for the rating; search is a **single**
TVDB `/search` call. Live-verified against the live TVDB API (real ids resolve to real series;
titles are not reproduced here).
TVDB ignores `Accept-Language`; English name/overview come from `/translations/eng`.

**Generic proxy**: `GET /v1/{provider}/{*path}` → cached, rate-limited, authenticated forward of
TMDb/TVDB (used for arbitrary provider calls and tests).

**Caching** (`src/cache.rs`): Postgres; per-resource TTL policy (`cache.endpoint_ttl`, e.g. 7d
metadata, 30d static, 1h changes), `min_ttl`/`max_ttl` clamp, `honor_cache_control=false` (our policy
wins), 14d `stale_if_error`, **stale-while-revalidate**, **single-flight coalescing** (64 shards),
negative caching, cumulative hits. Confirmed live: 5 concurrent cold requests → 1 upstream call;
TTL expiry re-fetches.

**Rate limiting** (`src/ratelimit/`): `governor` GCRA per provider (`requests_per_second`/`burst`,
plus per-endpoint overrides `provider.endpoint_rps`), progressive exponential backoff with timeout
growth and drop-after-wait, per-endpoint accounting + `rate_limit_events` audit, `/v1/ratelimits`
and `/v1/ratelimits/history`.

**Inbound** (`src/inbound.rs`): optional per-IP GCRA limiter plus a server-wide ceiling
(`global_requests_per_second`/`global_burst`) and an in-flight concurrency cap (`max_concurrent`),
with per-IP state eviction, `X-Forwarded-For`/`X-Real-IP` aware, and allowed/bypassed/rejected logging
plus `providarr_inbound_*` metrics. **Auth** (`api_auth.*`): optional shared secret, `/health` exempt.

**Other endpoints**: `GET /health`, `GET /metrics` (Prometheus), `GET /v1/cache/stats`.

**Replay** (`src/replay.rs`, `tests/fixtures/manifest.json`): off by default; when
`replay.enabled=true` serves canned fixtures and **never** calls upstream. Kept for offline
development + tests. The bundled fixtures are synthetic (fictional titles/ids and `example.com`
image URLs); operators can point `replay.dir` at their own directory. Set `replay.record=true` to
write every successful live response under `<replay.dir>/recorded/` (gitignored) for a later
`replay.enabled=true` run.

### Config reference (`config/config.json`)
- `server` (host/port/request_timeout), `database`, `cache` (see above), `backoff`,
  `replay` (`enabled`, `dir`, `fallback_to_upstream`, `record`), `inbound`, `api_auth`,
  `providers.{tmdb,tvdb}` (`base_url`, `requests_per_second`, `burst`, `max_concurrency`,
  `request_timeout`, `connect_timeout`, `documented_rate_limit`, `auth`, `endpoint_rps`).
- Secrets come from `.env`; `DATABASE_URL` env overrides the config.

### Tests (106) — where they live
- Unit: `src/ratelimit/backoff.rs`, `src/ratelimit/limiter.rs`, `src/ratelimit/stats.rs`,
  `src/cache.rs`, `src/inbound.rs`, `src/config.rs`, `src/replay.rs`, `src/mapper/mod.rs`,
  `src/mapper/series.rs`.
- Integration (`tests/`): `api.rs`, `auth.rs`, `cache_pg.rs`, `replay.rs`, `inbound.rs`, `proxy.rs`
  (wiremock cache/coalescing/SWR/backoff), `mapper_http.rs` (mocked TMDb/TVDB end-to-end),
  `persistence.rs` (provider backoff across restarts).
- Integration tests isolate via a per-test Postgres schema (`tests/common/mod.rs`); the server is
  an auto-started embedded PostgreSQL (or `TEST_DATABASE_URL`), and `app_state(...)` points providers
  at wiremock/loopback; replay off unless opted in.

### Theoriarr-side metadata integration (done)
`NzbDrone.Common/Options/MetadataOptions.cs` (Providarr-only; the source enum was deleted);
`RadarrCloudRequestBuilder` and `SonarrCloudRequestBuilder` inject `IOptions<MetadataOptions>` and
always resolve `{ProvidarrBaseUrl}/radarr/v1/{route}` and
`{ProvidarrBaseUrl}/sonarr/v1/tvdb/{route}/{language}/`. Bound in `Bootstrap.ConfigureOptions` under
`Theoriarr:Metadata`, with a `PostConfigure<IConfigService>` that applies the DB value on restart.
Runtime config: `IConfigService.ProvidarrBaseUrl` exposed at `settings/metadatasource` and editable in
**Settings > Metadata Source** (env/JSON still win). The `SkyHook` namespace/classes were renamed to
`MetadataSource/Provider`; `api.radarr.video`/`skyhook.sonarr.tv`/`services.sonarr.tv` are gone.

### Known issues / accepted limitations
See `PROBLEMS.md`. Highlights: prod hardening (in-process limiters/backoff, multi-instance not
safe), cache ignores upstream `Cache-Control` by default, `tvRageId` stays null (accepted),
`airDateUtc` is assumed UTC (`{airDate}T00:00:00Z`), and search/list hydrate full details per movie
result (cached).

### Next steps (prioritized; see `TODO.md`)
1. ~~Theoriarr runtime metadata-source setting~~ — **done**: Providarr-only plus
   **Settings > Metadata Source** (`settings/metadatasource`), applied on restart.
2. ~~Decouple `services.sonarr.tv`~~ — **done** (cycle 10).
3. **Series enrichment**: MAL/AniList/AniDB ids are done; timezone-accurate `airDateUtc` was
   **accepted as UTC** (`{airDate}T00:00:00Z`), and `tvRageId` stays null (TVRage is defunct) —
   no active work here.
4. **Scale-out**: shared (Postgres/Redis) rate limiter for multiple instances.
5. **Cache warming** (deferred — nothing currently consumes TVDB static reference endpoints).

### Session gotchas / lessons
- Sandbox `/tmp` perms broke apt → `chmod 1777 /tmp`.
- `cargo add` run in parallel clobbers `Cargo.toml` — add sequentially.
- reqwest 0.13 renamed `rustls-tls` → `rustls`; rand 0.10 needs `use rand::RngExt`.
- `sqlx` 0.9: dynamic SQL for pool/connection needs `raw_sql(AssertSqlSafe(String))`; `SUM(bigint)`
  returns `numeric` (cast to `::bigint`); `sqlx::migrate!` needs the `macros` feature.
- Postgres does not auto-start in the sandbox (`invoke-rc.d` denied) → start manually.
- Theoriarr unified API picks Movies vs Series by API key (see above).
- TMDb ships an empty `en` translation for original-language titles; Theoriarr would blank the
  title, so the mapper drops empty-title translations.
- `pgrep -f 'target/debug/providarr'` matches your own shell command — don't use it for cleanup.

---

## Cycle 1 — 2026-10-02

**Goal:** stand up the service and implement rate limiting, backoff, accounting, and Postgres
caching around basic TMDb/TheTVDB API requests. No metadata reshaping, no Theoriarr integration.

### Environment

- Installed Rust (stable 1.99.0) via rustup, and Postgres 16 via apt.
- Started the cluster (`pg_ctlcluster 16 main start`) and created:
  ```sql
  CREATE ROLE providarr LOGIN PASSWORD 'providarr' CREATEDB;
  CREATE DATABASE providarr OWNER providarr;
  CREATE DATABASE providarr_test OWNER providarr;
  ```
- Installed `build-essential pkg-config cmake clang libssl-dev` (needed for linking and for the
  `aws-lc-sys`/rustls build).

### Built

- `src/ratelimit/limiter.rs` — `ProviderRuntime`: governor limiter + backoff gate + concurrency
  semaphore. `acquire()` waits for a permit, the backoff window, and a slot, dropping any request
  whose wait exceeds `drop_after_wait`.
- `src/ratelimit/backoff.rs` — deterministic exponential delay/timeout growth with optional jitter;
  reset on success.
- `src/ratelimit/stats.rs` — lock-light per-endpoint counters; records requests-since-last-limit and
  observed limit threshold on a real 429 only.
- `src/cache.rs` — Postgres cache with TTL, `Cache-Control`, negative cache, stale-if-error, purge.
- `src/providers/{mod,auth}.rs` — provider registry, TMDb bearer auth, TVDB `/login` + cached token,
  and the fetch pipeline (cache → limiter → backoff → auth → request → account → cache).
- `src/api/mod.rs` — axum router: `/health`, `/metrics`, `/v1/ratelimits`, `/v1/cache/stats`,
  `/v1/{provider}/{*path}`.
- `src/{config,db,state,metrics,telemetry,error}.rs` and `src/main.rs` with background stats-flush
  (30s) and cache-purge (1h) tasks.
- `migrations/0001_init.sql`.

### Tests

- 14 unit tests (backoff growth/cap/reset, timeout growth, jitter bounds, limiter permits/drop,
  stats counting, cache key, endpoint normalization, max-age parsing, query encoding).
- 3 API tests, 3 Postgres cache tests, 4 proxy tests (wiremock): caching, 429 accounting,
  progressive drop, stale-if-error. **24 tests passing.**
- Integration tests isolate themselves with a per-test Postgres schema.

### Live validation (single request each, no rate-limit probing)

- `GET /v1/tmdb/authentication` → `{"success":true}` (TMDb key valid).
- `GET /v1/tvdb/series/{id}` → mapped series (TVDB key + login flow valid).
- `GET /v1/tmdb/movie/550` twice → first `x-providarr-cache: miss`, second `hit`; one upstream
  transaction recorded.
- `/v1/ratelimits` shows `requests_total: 1`, `requests_since_limit: 1`, documented limit noted.

### Fixed during the cycle

- `SUM(bigint)` returns `numeric`; cast to `::bigint` in cache stats (was panicking a worker).

### Decisions

- Keep the service generic (raw proxy) for now; typed endpoints and the metadata layer are future
  cycles. See `TODO.md`.

## Cycle 2 — 2026-10-02 (Theoriarr integration stubs, replay-only)

**Goal:** start integrating with Theoriarr without calling TMDb/TVDB. Serve recorded fixtures for the
metadata routes Theoriarr expects, and add per-IP inbound limiting.

### Built

- `src/replay.rs` — loads a caller-supplied `manifest.json`, serves canned upstream responses (for the
  proxy) and target-shape resources (for the metadata routes). Replay is **on by default** and
  `fallback_to_upstream=false`, so a missing fixture errors instead of reaching a real API.
- `tests/fixtures/` — synthetic raw TMDb/TVDB responses and target RadarrAPI/SkyHook resources used
  by the test suite (fictional titles, ids and `example.com` image URLs; no captured provider data).
- Theoriarr-facing routes in `src/api/mod.rs`:
  - `GET /radarr/v1/movie/{id}`
  - `GET /radarr/v1/movie/collection/{id}`
  - `GET /sonarr/v1/tvdb/shows/{language}/{id}`
- `src/inbound.rs` — per-IP GCRA limiter for incoming requests (config `inbound.*`), honouring
  `X-Forwarded-For` / `X-Real-IP` when `trust_forwarded_for` is set; 429 + `Retry-After` when limited.
- Proxy responses now expose `x-providarr-replay: true`; metadata responses `x-providarr-source: fixture`.
- `AppState`/`ProviderRegistry` wired with the replay store and inbound limiter; server serves with
  `ConnectInfo<SocketAddr>` so real client IPs are available.
- Docker images ship config only; no fixture data is bundled.

### Tests

- Unit: inbound limiter (per-IP burst + independence), replay matching (updated to allowlisted IDs),
  plus the existing 17.
- Integration: `tests/replay.rs` (movie/series/proxy fixtures; missing fixture 404s without upstream)
  and `tests/inbound.rs` (per-IP 429 after burst; other IPs unaffected). **32 tests, 0 failed.**

### Live smoke (replay on)

- `/health` → `replay_enabled:true`, `inbound_limiter_enabled:true`.
- `/radarr/v1/movie/1` → *Synthetic Feature*, `x-providarr-source: fixture`.
- `/sonarr/v1/tvdb/shows/en/1` → *Synthetic Show*.
- `/v1/tmdb/movie/1` → raw fixture, `x-providarr-replay: true`.
- Unknown fixture → `404`; **no upstream calls in the log**.

### Theoriarr side

- Metadata source is DI-selectable via `MetadataOptions` (previous cycle):
  `THEORIARR__METADATA__SOURCE=Providarr`, `THEORIARR__METADATA__PROVIDARRBASEURL=…`.
  Theoriarr requests `{base}/radarr/v1/movie/{tmdbId}` and
  `{base}/sonarr/v1/tvdb/shows/{language}/{tvdbId}`, matching the routes above.

### End-to-end (confirmed)

Ran the unified Theoriarr host against a replay-only Providarr:
`THEORIARR__METADATA__SOURCE=Providarr THEORIARR__METADATA__PROVIDARRBASEURL=http://127.0.0.1:4155`.

- Movie lookup by TMDb id resolves (200).
- Series lookup by TVDB id resolves (1 result).
- Misses prove the routing (no upstream fallback): an uncaptured movie id -> 500; an uncaptured
  series id -> 0 results.
- Request routing note: the unified host selects Series/Movies endpoints from the API key
  (`ApiKey` vs `MovieApiKey` in `config.xml`).

### Next

- Implement the remaining Theoriarr routes (`movie/bulk`, `movie/imdb`, `movie/changed`,
  `list/tmdb/*`, `search`) and the real TMDb/TVDB → resource mapper. See `TODO.md`.

## Cycle 3 — 2026-10-02 (aggressive caching)

**Goal:** make caching aggressive (few upstream calls, resilient) and confirm it end-to-end against
the real API.

### Built

- **Per-resource TTL policy** (`cache.endpoint_ttl`): 7d for `movie`/`tv`/`series`/`collection`/…,
  1d for `search`/`list`/trending, 1h for `changes`/`updates`, 30d for static reference data
  (`configuration`, `languages`, `genres`, …). Unknown segments use `default_ttl`.
- **`min_ttl` floor** + `max_ttl` cap; **`honor_cache_control` now off by default** (our policy
  wins; when on, upstream `max-age` is clamped to `[min_ttl, max_ttl]`; `no-store`/`no-cache` still
  bypass).
- **`stale_if_error` = 14d** so an upstream outage is served from stale cache.
- **Request coalescing (single-flight)**: concurrent identical misses serialize on one of 64 shards
  and make a single upstream call.
- **Cumulative hits**: refreshing an entry no longer resets its hit counter.
- Better `cache_stale` metric; config default TTLs updated in `config/config.json`.

### Confirmed live (real TMDb, temporary replay-off config)

- **5 concurrent cold requests → `providarr_requests_total = 1`** (1 miss, 4 coalesced hits).
- Immediate repeat → `x-providarr-cache: hit`, byte-identical body (2199 B), so the full
  status/content-type/body is stored.
- DB row `expires_at - created_at = 5s`; after TTL+stale lapsed → `miss`, `requests_total` 1→2.

### Tests

- Unit: `endpoint_ttl_policy` (config). Integration: `coalesces_concurrent_identical_requests`
  (wiremock `expect(1)`), `hits_accumulate_across_refresh`. Existing cache/proxy tests updated to
  drive `Cache-Control` explicitly. **33 tests, 0 failed.**

### Deferred

- **Stale-while-revalidate** (serve stale immediately + background refresh) — not done yet; long
  TTLs + coalescing + 14d stale-on-error cover most of the benefit. Tracked in `TODO.md`.

## Cycle 4 — 2026-10-02 (movie mapper: TMDb → Radarr MovieResource)

**Goal:** serve the Radarr-shaped movie endpoints from live TMDb so Providarr can replace
`api.radarr.video` without Theoriarr changes.

### Built

- `src/mapper/mod.rs`: TMDb → Radarr `MovieResource` mapper with output structs matching Theoriarr's
  `Resource/*.cs` (PascalCase JSON). Covers title/overview/ids, runtime, popularity, year,
  premier/in-cinema/digital/physical dates, images, genres, keywords, studios, certifications,
  alternative titles, translations, cast/crew (with headshots), the multi-provider ratings bag,
  recommendations, collection, and the YouTube trailer.
- Routes wired in `src/api/mod.rs` (live when `replay.enabled=false`):
  `movie/{id}`, `movie/collection/{id}`, `movie/imdb/{id}`, `movie/bulk` (POST),
  `movie/changed` (currently `[]`), `search?q=&year=`, `list/tmdb/{trending,popular}`.
- Search/list hydrate full details per result (cached) so `MapMovie` has everything it needs.
- Fixed: drop TMDb translations with an empty title, which Theoriarr otherwise used to blank the
  movie title (TMDb ships an empty `en` translation for original-language titles).

### Confirmed live (Theoriarr → Providarr → real TMDb, replay off)

- Real TMDb ids resolve to released movies (real titles not reproduced here).
- Uncaptured ids resolve too, proving real mapping (not just fixtures).
  `providarr_requests_total{provider="tmdb",endpoint="movie/{id}"}` increments.

### Tests

- Mapper unit tests (`maps_core_movie_fields`, `encodes_with_pascal_case_keys`) against the synthetic
  raw TMDb fixture, asserting non-null lists and PascalCase keys. **37 tests, 0 failed.**

### Next

- Series mapper (TVDB → SkyHook `ShowResource`) and `search`; then flip the default to real API.

## Cycle 5 — 2026-10-02 (series mapper: TVDB → SkyHook ShowResource)

**Goal:** serve the series endpoints from live TVDB so Providarr fully replaces `skyhook.sonarr.tv`.

### Built

- `src/mapper/series.rs`: TVDB → SkyHook `ShowResource` (camelCase JSON). Uses three TVDB calls per
  series: `/series/{id}/extended` (name/slug/dates/ids/genres/content ratings/aliases/characters/
  artworks/seasons), `/series/{id}/translations/{lang}` (English name + overview — TVDB ignores
  `Accept-Language`), and `/series/{id}/episodes/default` (paginated).
- Maps actors, images (artwork types → Poster/Fanart/Banner/Clearlogo), seasons (derived from
  episodes with season images), episodes, external IDs (IMDb/TMDb), content rating, genres,
  alternative titles, and time-of-day.
- Routes live when replay off: `shows/{language}/{id}` and `search/{language}?term=`.
- **Flipped `replay.enabled` default to `false`** — mappers now serve live TMDb/TVDB by default;
  fixtures remain for the test suite / offline development.

### Confirmed live (Theoriarr → Providarr → real TVDB, replay off)

- Real TVDB ids resolve to series (titles not reproduced here).
- Uncaptured ids resolve too, proving real mapping (not just fixtures).
- Metrics: 3 TVDB calls per series (`extended`, `translations/eng`, `episodes/default`).

### Tests

- `mapper::series` unit tests against the synthetic extended/episodes fixtures, plus camelCase key
  serialization. **39 tests, 0 failed.**

### Next

- Real `movie/changed`; optional series enrichment (tvMaze/tvRage/MAL/AniList, ratings) and accurate
  `airDateUtc`; stale-while-revalidate; auth on Providarr's own API.

## Cycle 6 — 2026-10-02 (completeness + hardening)

**Goal:** close the remaining stub/gap items.

### Built

- **Real `movie/changed`**: `GET /radarr/v1/movie/changed?since=` now queries TMDb
  `/movie/changes`, clamped to its 14-day window, and returns the changed ids (`src/mapper/mod.rs`).
- **Stale-while-revalidate**: an expired-but-still-stale entry is served immediately and refreshed in
  the background (`fetch` now takes `Arc<Self>` and spawns the refresh); config
  `cache.stale_while_revalidate` (on).
- **Optional API auth**: `config.api_auth` (`enabled`, `api_key`, `header`) guards every route except
  `/health` with a shared secret.
- **`/v1/ratelimits/history`**: recent `rate_limit_events` (`?provider=&limit=`), backed by a new
  `db::load_rate_limit_events`.

### Tests

- SWR (`serves_stale_and_revalidates_in_background`, wiremock `expect(2)`), auth
  (missing/wrong key → 401, valid → 200, `/health` open), history endpoint, `parse_since` formats.
  **43 tests, 0 failed.**

### Next

- Series enrichment (tvMaze/tvRage/MAL/AniList, ratings, timezone-accurate `airDateUtc`); per-endpoint
  rps overrides; persist backoff; cache warming.

## Cycle 7 — 2026-10-02 (series search + enrichment)

- **Series search is now a single TVDB call**: `/search?query=&type=series` results carry
  name/overview/image/year/network/remote ids, so `ShowResource` is built directly instead of
  fetching full details (3 calls) per hit.
- **TV Maze id** (`tvMazeId`) mapped from TVDB `remoteIds` (`"TV Maze"`), alongside IMDb/TMDb.
- **Series rating** pulled from TMDb `/tv/{tmdbId}` (TVDB has no ratings) and mapped to the SkyHook
  `Rating` shape.
- Confirmed live: `/search` returns results in one call; show lookup maps `tvMazeId`/`tmdbId` and
  episodes (real titles not reproduced here).
- **44 tests, 0 failed.**

## Cycle 8 — 2026-10-02 (per-endpoint limits + mapper tests)

- **Per-endpoint rps overrides**: `providers.<name>.endpoint_rps` (first path segment) selects a
  dedicated limiter, falling back to the provider-wide one.
- **HTTP-level mapper tests** (`tests/mapper_http.rs`) with a mocked TMDb/TVDB: exercise the real
  routes end-to-end without touching the network.
- **46 tests, 0 failed.**
- Cache warming deferred: no static reference endpoints are consumed yet, so there is nothing
  meaningful to warm.
