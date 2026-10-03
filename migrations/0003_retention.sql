-- Retention support: bound the growth of endpoint_stats and rate_limit_events
-- by making time-based pruning index-backed and cheap. Non-destructive.

-- Pruning endpoint_stats deletes rows whose updated_at is older than the
-- retention window; this index keeps that range delete sargable. It also backs
-- the bounded, most-recent-first startup load.
CREATE INDEX IF NOT EXISTS endpoint_stats_updated_at_idx
    ON endpoint_stats (updated_at);

-- rate_limit_events already has (provider, occurred_at DESC) for the
-- provider-filtered history query. Add a global occurred_at index so the
-- unfiltered "recent events" query and retention pruning avoid a full sort or
-- scan.
CREATE INDEX IF NOT EXISTS rate_limit_events_occurred_at_idx
    ON rate_limit_events (occurred_at);
