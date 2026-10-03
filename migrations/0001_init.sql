-- Providarr initial schema: heavy Postgres cache + rate-limit accounting.

CREATE TABLE IF NOT EXISTS cache_entries (
    cache_key        TEXT PRIMARY KEY,
    provider         TEXT NOT NULL,
    endpoint         TEXT NOT NULL,
    resource         TEXT NOT NULL,
    status_code      INTEGER NOT NULL,
    content_type     TEXT,
    body             BYTEA NOT NULL,
    headers          JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at       TIMESTAMPTZ NOT NULL,
    hits             BIGINT NOT NULL DEFAULT 0,
    last_accessed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS cache_entries_expires_at_idx ON cache_entries (expires_at);
CREATE INDEX IF NOT EXISTS cache_entries_provider_endpoint_idx ON cache_entries (provider, endpoint);

-- One row per (provider, endpoint). Mirrors the in-memory counters so counts
-- survive restarts. `observed_limit_threshold` is only ever written when an
-- upstream actually returned a rate-limit response; Providarr never probes.
CREATE TABLE IF NOT EXISTS endpoint_stats (
    provider                 TEXT NOT NULL,
    endpoint                 TEXT NOT NULL,
    requests_total           BIGINT NOT NULL DEFAULT 0,
    requests_since_limit     BIGINT NOT NULL DEFAULT 0,
    rate_limit_hits          BIGINT NOT NULL DEFAULT 0,
    failures                 BIGINT NOT NULL DEFAULT 0,
    successes                BIGINT NOT NULL DEFAULT 0,
    dropped                  BIGINT NOT NULL DEFAULT 0,
    last_status              INTEGER,
    last_request_at          TIMESTAMPTZ,
    last_rate_limit_at       TIMESTAMPTZ,
    observed_limit_threshold BIGINT,
    noted_limit              TEXT,
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (provider, endpoint)
);

-- Audit trail of every rate-limit hit, recording how many transactions had been
-- sent to that endpoint since the previous limit before it tripped.
CREATE TABLE IF NOT EXISTS rate_limit_events (
    id                         BIGSERIAL PRIMARY KEY,
    provider                   TEXT NOT NULL,
    endpoint                   TEXT NOT NULL,
    occurred_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    requests_since_last_limit  BIGINT NOT NULL,
    status_code                INTEGER,
    detail                     TEXT
);

CREATE INDEX IF NOT EXISTS rate_limit_events_provider_idx ON rate_limit_events (provider, occurred_at DESC);

-- Fixed-window request counters, kept in Postgres so budgets can be inspected
-- and enforced across restarts.
CREATE TABLE IF NOT EXISTS request_windows (
    provider    TEXT NOT NULL,
    window_kind TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    requests    BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (provider, window_kind, window_start)
);
