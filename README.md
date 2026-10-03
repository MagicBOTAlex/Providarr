
# Disclaimer, absolute AI LLM
This project was a trial of LLM agentic programming capabilities. \
I spent 300CNY on deepseek-4.1-flash and 10 days, supervising it (cucking). \
I let it run, while I did Uni assignments. \
I have not touched a single line of code.

I placed the model in a docker container Ubuntu sandbox and let it run with all permissions.

I am not responsible if anything breaks, or if anything happens when using this software. \
But if you find problems, please submit them as issues or make a pull request. \
I have no idea what is going on with the code tho, so expect me letting the AI fix any problems.

Am I proud? No \
Does it work? Yeah (At least for me)

You have been warned!

I actively use this and I find it better than Sonarr and Radarr. \
But, this would not have been possible without Sonarr's and Radarr's open source. \
Any donations, please support them and not me. \
This project is really just their code, but regurgitated.
Expect tons of bugs!

Everything under here is AI written.

# Providarr

A small Rust service that proxies metadata requests to **TMDb** and **TheTVDB**, with
per-provider rate limiting, progressive backoff, and heavy Postgres caching. It serves the
metadata APIs used by [Theoriarr](https://github.com/MagicBOTAlex/Theoriarr).

> **Aggressive caching and strict rate limits by default.** Cache TTL is 3 months by default
> (6 months for movie/series metadata and static reference data) with a 30-day
> stale-if-error window and stale-while-revalidate. Inbound requests are limited per client IP
> (2 req/s, burst 5) with a server-wide ceiling (100 req/s, burst 200) and a 128 concurrent-request
> cap. Tune `cache.*` / `inbound.*` / `providers.*` in `config/config.json`; the effective policy is
> published at `GET /v1/policy`. **Host your own instance** rather than relying on the shared public
> `providarr.deprived.dev`.

## Features

- **Metadata mappers** — Radarr `MovieResource` and Sonarr SkyHook `ShowResource` shapes from live TMDb/TheTVDB.
- **Rate limiting** — a GCRA limiter per provider plus a strict per-IP inbound limiter, a server-wide ceiling, and a concurrency cap. Budgets are conservative and never probed.
- **Retry + backoff** — offline/timeout/429/5xx upstream calls are retried with exponential backoff and jitter; each attempt re-enters the limiter.
- **Postgres cache** — long per-resource TTLs, negative caching, cumulative hit counts, and request coalescing (a burst of identical misses makes one upstream call).
- **Replay fixtures** — optional user-supplied canned responses for offline development; the test suite uses a synthetic set under `tests/fixtures/`.
- **Observability** — `/health`, Prometheus `/metrics`, `/v1/ratelimits`, `/v1/cache/stats`.
- **Optional auth** — shared-secret API key compared in constant time; inbound limiting runs before auth.

## Quick start

```bash
cp .env.example .env      # fill in TMDB_API_TOKEN and TVDB_API_KEY
# Postgres must be running; see PROGRESS.md for the role/database SQL.
cargo run                 # listens on 0.0.0.0:4155
```

Config lives in [`config/config.json`](config/config.json); secrets and `DATABASE_URL` come from `.env`.

### Docker

| Dockerfile | Postgres | Command |
|---|---|---|
| `Dockerfile` | external | `docker compose up -d --build` |
| `Dockerfile.embedded` | embedded (PG 16) | `docker compose -f docker-compose.embedded.yml up -d --build` |

Both are multi-stage builds that run as a non-root user; secrets are supplied at runtime and never
baked into the image.

## Nix

The [flake](flake.nix) provides the toolchain, the package and a NixOS module:

```bash
nix develop     # dev shell (Rust, cargo tooling, Postgres 16)
nix build       # ./result/bin/providarr
nix run         # build and start on 0.0.0.0:4155
```

```nix
services.providarr = {
  enable = true;
  openFirewall = true;
  environmentFile = "/run/secrets/providarr.env"; # DATABASE_URL, TMDB_API_TOKEN, TVDB_API_KEY
  settings.inbound.burst = 10;                    # any config.json section
};
```

The module runs the service as the `providarr` user with `/var/lib/providarr` as its state
directory, and generates the `server`/`logging` sections of `config.json` (extendable through
`services.providarr.settings`).

## Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Liveness + per-provider credential status |
| GET | `/v1/policy` | Effective cache TTLs, rate limits and disclosure (public) |
| GET | `/metrics` | Prometheus metrics |
| GET | `/v1/ratelimits` | Per-endpoint counters, backoff, documented limits |
| GET | `/v1/cache/stats` | Cache entry/hit/byte counts |
| GET | `/v1/{provider}/{*path}` | Cached, rate-limited, authenticated proxy |
| GET | `/radarr/v1/movie/{id}` | Movie metadata (`MovieResource`) |
| GET | `/radarr/v1/movie/collection/{id}` | Movie collection resource |
| GET | `/radarr/v1/movie/imdb/{imdbId}` | Movie(s) by IMDb id |
| POST | `/radarr/v1/movie/bulk` | Bulk movie detail (body `[tmdbId, …]`) |
| GET | `/radarr/v1/movie/changed?since=` | Changed movie ids (14-day window) |
| GET | `/radarr/v1/search?q=&year=` | Movie search |
| GET | `/radarr/v1/list/tmdb/{trending,popular}` | Movie lists |
| GET | `/sonarr/v1/tvdb/shows/{language}/{id}` | Series metadata (`ShowResource`) |
| GET | `/sonarr/v1/tvdb/search/{language}?term=` | Series search |
| GET | `/sonarr/services/{time,ping}` | Local UTC clock / liveness |

The proxy adds `x-providarr-cache: miss|hit|stale`, `x-providarr-provider` and `x-providarr-replay`
headers. Replay is **off** by default; set `replay.enabled=true` to serve canned responses from the
`replay.dir` directory (default `fixtures/`, which the operator supplies) instead of calling
upstream. The repository contains no captured provider data: the fixtures shipped for the test
suite under [`tests/fixtures/`](tests/fixtures) are synthetic and fictional. Set
`replay.record=true` to capture every successful live response under `<replay.dir>/recorded/` for a
later `replay.enabled=true` run (that directory is gitignored).

## Configuration

All settings live in `config/config.json`, overridable with `PROVIDARR_*` environment variables
(see [`.env.example`](.env.example)). The main groups:

- `cache` — `default_ttl` (90d), per-resource `endpoint_ttl` (up to 180d for movie/series/static), `min_ttl`/`max_ttl` (180d), `negative_ttl` (5m), `not_found_ttl` (1h), `stale_if_error` (30d), `stale_while_revalidate`, `honor_cache_control`, `request_coalescing`.
- `inbound` — `requests_per_second` (2), `burst` (5), `global_requests_per_second` (100), `global_burst` (200), `max_concurrent` (128), `trust_forwarded_for` (false), `trusted_proxies`, `bypass`. Forwarding headers are only honoured from trusted peers, and IPv6 clients are keyed by `/64` prefix.
- `search` — `hydrate_limit` (5): how many search/list results are hydrated with full details.
- `providers` — per-provider outbound budget and timeout.
- `api_auth` — optional shared-secret API key (disabled by default).
- `logging` — stdout plus rotating files (`logs/providarr.<date>.log`, 7 kept by default).
- `replay` — `enabled` (false), `dir` (`fixtures`), `fallback_to_upstream`, `record` (false: capture live responses under `<dir>/recorded/` for later offline replay).

Providers: `tmdb` (`https://api.themoviedb.org/3`, `TMDB_API_TOKEN`) and `tvdb`
(`https://api4.thetvdb.com/v4`, `TVDB_API_KEY`, token cached 24h).

## Security

- **Enable API auth in production.** Turn on the shared-secret API key with
  `PROVIDARR_API_AUTH_ENABLED=true`, provide the secret via `PROVIDARR_API_AUTH_KEY`, and set the
  header name with `PROVIDARR_API_AUTH_HEADER` (default `x-api-key`). Auth is fail-closed: enabling
  it without a key is a startup error, so the secret can stay in the environment rather than
  `config/config.json`.
- **Limit network exposure.** The service binds to `0.0.0.0` by default. Bind it to loopback or a
  private network and terminate TLS at a reverse proxy in front of it.
- **Inbound rate limiting is enabled by default.** A per-client-IP limiter runs alongside a
  server-wide ceiling and a concurrency cap, so a single instance is not overwhelmed by a flood.
- **Keep secrets out of the repo.** Provider credentials (`TMDB_API_TOKEN`, `TVDB_API_KEY`) and
  `DATABASE_URL` are read from environment variables. Do not commit them; `.env` is gitignored and
  the Docker images are built without secrets.

## Theoriarr integration

Point Theoriarr at Providarr (a restart is required):

```bash
THEORIARR__METADATA__PROVIDARRBASEURL=http://providarr:4155
```

Theoriarr then calls `/radarr/v1/movie/{tmdbId}`, `/sonarr/v1/tvdb/shows/{language}/{tvdbId}` and
`/sonarr/services/*`. Scene mapping is handled by Theoriarr's own TheXEM integration, and
daily-series status comes from the air days in the `ShowResource`.

## Development

```bash
cargo fmt
cargo clippy --all-targets
cargo test          # starts an embedded PostgreSQL; each test uses an isolated schema
```

Tests self-provision a temporary PostgreSQL on first run (no Docker/root needed; the binaries are
downloaded and cached once). Set `TEST_DATABASE_URL` to use your own server instead — required in
root environments, where embedded PostgreSQL refuses to run. See [`PROGRESS.md`](PROGRESS.md),
[`TODO.md`](TODO.md) and [`PROBLEMS.md`](PROBLEMS.md) for deeper notes.

## Attribution

Providarr uses the TMDb and TheTVDB APIs. This product uses the TMDB API but is not endorsed or
certified by TMDB. TheTVDB terms of use apply.

## License

Providarr is licensed under the MIT License. See [LICENSE](LICENSE).
