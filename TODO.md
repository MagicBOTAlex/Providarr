# Providarr TODO

> **Resuming?** Read the detailed handoff at the top of [`PROGRESS.md`](PROGRESS.md) first — it has
> environment setup, the Theoriarr E2E recipe, architecture map, gotchas, and the prioritized next
> steps. This file is the short backlog.

Legend: `[ ]` open · `[~]` in progress · `[x]` done.

## Scope guard (current cycle)

**Live mappers, no longer replay-only.** Providarr maps live TMDb → Radarr `MovieResource` and
TVDB → SkyHook `ShowResource`, with aggressive caching, rate limiting, SWR and optional auth.
Replay is off by default (synthetic fixtures under `tests/fixtures/` remain for tests/offline;
`replay.record=true` captures live responses under `<replay.dir>/recorded/` for later offline
replay). The
core Theoriarr metadata surface is implemented and verified end-to-end; remaining work is runtime
settings, `services.sonarr.tv` decoupling, enrichment, and scale-out (see `Near term`).

## Done — cycle 1

- [x] Rust scaffold, JSON config + `.env`.
- [x] Postgres schema + migrations (`cache_entries`, `endpoint_stats`, `rate_limit_events`, `request_windows`).
- [x] Per-provider rate limiting (`governor`), progressive backoff, per-endpoint accounting.
- [x] Heavy Postgres cache: TTL, `Cache-Control`, negative cache, stale-if-error.
- [x] Provider auth (TMDb bearer; TVDB `/login` + cached token).
- [x] Generic proxy + `/health`, `/metrics`, `/v1/ratelimits`, `/v1/cache/stats`.

## Done — cycle 2 (integration stubs)

- [x] Replay fixture layer (`src/replay.rs`, `tests/fixtures/manifest.json`) — **on by default**, no
      upstream fallback, so nothing reaches TMDb/TVDB automatically.
- [x] Theoriarr-facing routes serving target fixtures:
      `GET /radarr/v1/movie/{id}`, `GET /radarr/v1/movie/collection/{id}`,
      `GET /sonarr/v1/tvdb/shows/{language}/{id}`.
- [x] Per-IP inbound rate limiting (`src/inbound.rs`) + config + metrics + tests.
- [x] Replay/inbound integration tests; live smoke served fixtures with **zero upstream calls**.
- [x] **Theoriarr ↔ Providarr end-to-end confirmed** via replay fixtures; non-fixture IDs fail or
      return empty, proving no upstream fallback.
- [x] Docker images copy config only; fixture data is not bundled.

## Done — cycle 3 (aggressive caching)

- [x] Per-resource TTL policy (`endpoint_ttl`) with `min_ttl` floor / `max_ttl` cap.
- [x] `honor_cache_control` off by default (our TTL policy wins).
- [x] `stale_if_error` 14d; cumulative hits across refresh; `cache_stale` metric.
- [x] Request coalescing (single-flight) — 64 shards, one upstream call per burst.
- [x] Unit + integration tests; live confirmation against real TMDb (5 concurrent → 1 upstream).

## Done — cycle 4 (movie mapper)

- [x] TMDb → Radarr `MovieResource` mapper (`src/mapper/mod.rs`).
- [x] Movie routes live when replay off: `movie/{id}`, `movie/collection/{id}`, `movie/imdb/{id}`,
      `movie/bulk`, `movie/changed` (`[]`), `search`, `list/tmdb/{trending,popular}`.
- [x] Confirmed end-to-end with Theoriarr against live TMDb (535167, 550, 27205).

## Done — cycle 5 (series mapper)

- [x] TVDB → SkyHook `ShowResource` mapper (`src/mapper/series.rs`).
- [x] Series routes live when replay off: `shows/{language}/{id}` and `search/{language}`.
- [x] Flipped `replay.enabled` default to `false` (synthetic fixtures still used by tests).
- [x] Confirmed end-to-end with Theoriarr against live TVDB (239951, 327417, 121361).

## Done — cycle 6 (completeness + hardening)

- [x] Real `movie/changed` (TMDb `/movie/changes`).
- [x] Stale-while-revalidate (serve stale, refresh in background).
- [x] Optional API-key auth (`config.api_auth`, `/health` exempt).
- [x] `/v1/ratelimits/history` backed by `rate_limit_events`.

## Done — cycle 7 (series search + enrichment)

- [x] Single-call series search mapping.
- [x] `tvMazeId` from TVDB `remoteIds`.
- [x] Series rating from TMDb `/tv/{tmdbId}`.
- [x] MAL/AniList/AniDB ids from TVDB `remoteIds` (comma/space separated aware).

## Done — cycle 8 (per-endpoint rps + mapper tests)

- [x] `providers.<name>.endpoint_rps` per-endpoint overrides.
- [x] HTTP-level mapper integration tests (`tests/mapper_http.rs`, mocked TMDb/TVDB).

## Done — cycle 9 (backoff persistence)

- [x] `provider_backoff` table + migration `0002`.
- [x] Persist/resume provider backoff across restarts (expired windows ignored).
- [x] DB round-trip + `BackoffState` snapshot/restore unit tests.

## Done — cycle 10 (drop services.sonarr.tv)

- [x] `GET /sonarr/services/{time,ping}` served locally for Sonarr's health checks.
- [x] Series `ShowResource.daily` derived from TVDB `airsDays` (≥5 days/week), so Theoriarr no
      longer needs the upstream `/dailyseries` list.
- [x] **No `services.sonarr.tv` dependency at all.** Theoriarr's scene mapping comes from its own
      independent TheXEM integration; daily status from TVDB; health checks from Providarr. Removed
      the interim `sonarr_services` proxy provider and `/scenemapping` + `/dailyseries` routes.
- [x] Theoriarr `MetadataOptions.ServicesUrl` + Providarr-derived default in
      `SonarrCloudRequestBuilder`.

## Done — cycle 11 (strict inbound limiting + bypass)

- [x] Inbound limiter defaults lowered to **2 req/s, burst 5** (`config/config.json`).
- [x] `inbound.bypass` allowlist of IPs/CIDRs — IPv4 and IPv6 (`10.0.0.0/8`, `2001:db8::/32`),
      IPv4-mapped IPv6 normalised.
- [x] Env overrides: `PROVIDARR_INBOUND_{ENABLED,RPS,BURST,TRUST_FORWARDED_FOR,BYPASS}`.
- [x] `providarr_inbound_bypassed_total` metric; `429` + `Retry-After` on limit.

## Done — cycle 12 (in-request retry with progressive backoff)

- [x] `fetch_upstream` retries offline/timeout, 429 and 5xx responses in-request, re-acquiring the
      limiter each attempt so the growing backoff window is enforced per retry.
- [x] `backoff.max_retries` (default 3); retries stop when the wait exceeds `drop_after_wait`.
- [x] Offline/429/5xx retried with exponential delay + jitter before finally failing.
- [x] Integration test covering fail-twice-then-succeed; failure tests pin `max_retries = 0`.

## Done — cycle 13 (rotating file logging)

- [x] `tracing-appender` rotating file logs (never/minutely/hourly/daily) alongside stdout.
- [x] `logging` config section + `PROVIDARR_LOG_{ENABLED,DIR,ROTATION,MAX_FILES,STDOUT}` env
      overrides; old files pruned to `max_files` (default 7, daily).
- [x] Docker images create/declare `/app/logs` and both compose files mount a logs volume.
- [x] `logs/` gitignored; `build_appender` test + config tests; verified live (file written).

## Done — cycle 14 (policy disclosure + per-response cache info)

- [x] Public `GET /v1/policy` exposing effective cache TTLs, inbound limiter, provider budgets and
      backoff, plus a recommendation to self-host.
- [x] Object metadata responses carry a `_providarr` block with `cache`, `cacheHits`,
      `upstreamRequests`, `cachedAt`, `ageSeconds` and `ttlSeconds` (via task-local cache report).
- [x] README warning documents the aggressive default (3-day TTL) and the rate limits.

## Done — cycle 15 (global + concurrency inbound limits, request logging)

- [x] Server-wide ceiling (`inbound.global_requests_per_second` / `global_burst`) alongside the
      per-IP limiter; `0` disables either.
- [x] In-flight concurrency cap (`inbound.max_concurrent`).
- [x] Per-IP limiter state eviction (`retain_recent`, 300s) so idle IPs don't accumulate.
- [x] Allowed/bypassed logged at `DEBUG`, per-IP/global/concurrency rejections at `WARN`;
      `providarr_inbound_allowed_total` metric; startup log of the effective limiter config.
- [x] `/v1/policy` exposes `rate_limits.inbound.global` and `max_concurrent`; README/`.env.example`.

## Near term

- [x] Series enrichment: MAL/AniList/AniDB ids (from TVDB `remoteIds`, comma-separated aware).
- [x] Per-endpoint rps overrides (`providers.<name>.endpoint_rps`).
- [x] Persist provider backoff across restarts (`provider_backoff` table).
- [ ] Cache warming for the static TVDB reference endpoints — deferred: the mappers don't
      consume static reference endpoints yet, so there is nothing useful to warm.
- [x] Per-endpoint TTL/rps overrides for TVDB static data (`endpoint_ttl` + `endpoint_rps`).

## Later cycles

- [ ] TVDB `/updates` delta sync + a local mirror database.
- [ ] Multi-instance safe rate limiting (shared Postgres/Redis limiter).
- [ ] Retention/pruning job for `rate_limit_events` history.
- [ ] Self-hosted OAuth broker integration (Trakt/Simkl/AniList).
- [ ] Admin UI/dashboard; cache-latency and per-endpoint p95 metrics.
- [ ] Fully retire the fixture path once the real mapper + cutover land.

## Notes / decisions

- Replay is **off by default** (`replay.enabled=false`, `fallback_to_upstream=false`); synthetic
  fixtures under `tests/fixtures/` remain for tests/offline. The live mappers hit TMDb/TVDB. Set
  `replay.record=true` to capture live responses under `<replay.dir>/recorded/` (gitignored) for
  offline replay.
- `airDateUtc` is **assumed UTC** (`{airDate}T00:00:00Z`); timezone-corrected derivation from
  `airsTime` + network/country is accepted-not-done.
- `tvRageId` stays null (TVRage is defunct); accepted.
- The bundled test fixtures are synthetic (fictional titles, ids, synopses, and `example.com` image
  URLs); no captured provider data is committed.
- TMDb uses the v4 read access token (bearer); TVDB logs in and caches the token. Limits are never
  probed.
- Config is JSON; secrets via `.env` (gitignored).
