-- Keyset pagination for GET /subscriptions (ORDER BY created_at DESC, id DESC):
CREATE INDEX idx_sub_created_id ON subscriptions (created_at DESC, id DESC);

-- Status endpoint orders all attempts of a webhook by (replay_number, attempt_number) (§0).
-- The column order matches that sort so the query needs no extra in-memory ordering.
-- Intentionally NON-UNIQUE: a replay restarts the attempt chain, so the tuple repeats.
CREATE INDEX idx_dl_webhook_attempt ON delivery_logs (webhook_id, replay_number, attempt_number);

CREATE INDEX idx_dl_subscription_id ON delivery_logs (subscription_id);
CREATE INDEX idx_dl_status          ON delivery_logs (status);
CREATE INDEX idx_dl_next_retry_at   ON delivery_logs (next_retry_at) WHERE next_retry_at IS NOT NULL;

-- Orphan-recovery / lease scan: due pending rows (next_retry_at <= now()).
-- Partial index keeps it tiny — only pending rows are ever scanned.
CREATE INDEX idx_dl_pending_recovery ON delivery_logs (next_retry_at) WHERE status = 'pending';

CREATE INDEX idx_dl_created_at ON delivery_logs (created_at);

-- Cleanup prunes ingest_idempotency rows past IDEMPOTENCY_TTL by age.
CREATE INDEX idx_idem_created_at ON ingest_idempotency (created_at);
