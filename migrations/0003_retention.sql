-- no-transaction
-- Retention support: bound the growth of endpoint_stats and rate_limit_events
-- by making time-based pruning index-backed and cheap. Non-destructive.
--
-- R3-M11: this migration is flagged `-- no-transaction` (sqlx runs it outside a
-- transaction) so that `CREATE INDEX CONCURRENTLY` is permitted. A plain
-- `CREATE INDEX` takes an ACCESS EXCLUSIVE lock on the target table and can stall
-- startup for a long time against a large, already-populated database. Each
-- CONCURRENTLY index must live in its own migration file because a multi-statement
-- file is executed as a single implicit transaction (see 0004 for the other one).
--
-- Operational note: if a CONCURRENTLY build is interrupted it can leave an INVALID
-- index. `IF NOT EXISTS` will then skip it on the next run, so an operator must
-- `DROP INDEX CONCURRENTLY endpoint_stats_updated_at_idx;` and re-run instead.

-- Pruning endpoint_stats deletes rows whose updated_at is older than the
-- retention window; this index keeps that range delete sargable. It also backs
-- the bounded, most-recent-first startup load.
CREATE INDEX CONCURRENTLY IF NOT EXISTS endpoint_stats_updated_at_idx
    ON endpoint_stats (updated_at);
