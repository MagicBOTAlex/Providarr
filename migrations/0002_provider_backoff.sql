-- Persist per-provider backoff so a restart resumes an active backoff window
-- instead of immediately hammering a struggling upstream.

CREATE TABLE IF NOT EXISTS provider_backoff (
    provider              TEXT PRIMARY KEY,
    consecutive_failures  INTEGER NOT NULL DEFAULT 0,
    remaining_ms          BIGINT NOT NULL DEFAULT 0,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
