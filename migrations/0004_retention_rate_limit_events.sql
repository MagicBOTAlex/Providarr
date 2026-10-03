-- no-transaction
-- R3-M11: global occurred_at index backing the unfiltered "recent events" query
-- and retention pruning of rate_limit_events. This lives in its own migration
-- because `CREATE INDEX CONCURRENTLY` must be the only statement in its simple
-- query (a multi-statement file is executed as one implicit transaction), and it
-- is built CONCURRENTLY to avoid an ACCESS EXCLUSIVE startup lock on a
-- possibly-huge, already-populated table.
--
-- `rate_limit_events` already has (provider, occurred_at DESC) for the
-- provider-filtered history query.
--
-- Operational note: if interrupted, CONCURRENTLY can leave an INVALID index that
-- `IF NOT EXISTS` will not rebuild; an operator must
-- `DROP INDEX CONCURRENTLY rate_limit_events_occurred_at_idx;` and re-run.
CREATE INDEX CONCURRENTLY IF NOT EXISTS rate_limit_events_occurred_at_idx
    ON rate_limit_events (occurred_at);
