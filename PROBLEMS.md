# Providarr Problems / Known Issues

## Open
*(none)*


## Deferred
- **Test Postgres is self-provisioned but not root-friendly.** `cargo test` starts an embedded
  PostgreSQL unless `TEST_DATABASE_URL` is set. Embedded PostgreSQL cannot run as root (a PostgreSQL
  restriction), so root CI must set `TEST_DATABASE_URL`; and against an external server the per-test
  schemas are not dropped (they accumulate in `providarr_test`).
- **Cache ignores upstream `Cache-Control` by default** (`honor_cache_control = false`), so we cache
  per our own policy even if a provider asks otherwise — including responses marked
  `no-cache`/`no-store` (e.g. TMDb auth/validate), which are stored under the path TTL. Intentional
  for aggressive caching; set `honor_cache_control = true` if a provider's directives must be obeyed.
- **Some provider requests cannot be cached.** Every successful `GET`/`HEAD` is cached (and `404`s
  now for `not_found_ttl`, default 1h), so the only requests that always reach the provider are
  `429`/`5xx` responses (they drive backoff and may serve a stale copy within `stale_if_error`),
  non-`GET` methods (rejected by the proxy), and the first request for any new query value (a search
  term, a new `since` day, a new page). The TVDB login handshake is cached in memory for 24h.
- **Rate limiting is per-instance.** Per-IP inbound limiting and the per-provider GCRA budget are
  in-process, so multiple Providarr instances each get their own budget and the aggregate could
  exceed provider limits. Needs a shared limiter before horizontal scaling. (Provider *backoff* is
  persisted, but the rate-limit budget is not shared.)
- **`tvRageId` is always null.** TVRage shut down in 2015 and TVDB rarely carries a TVRage remote id,
  so Theoriarr's `tvRageId` stays null. A `remote("TVRage")` lookup is not worth adding; accepted.
- **TVDB login token is cached in memory for 24h.** On restart it re-logs in. Fine for now; consider
  persisting or honouring the token's real expiry.
