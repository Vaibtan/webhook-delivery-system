-- Initial consolidated schema for the Go webhook delivery system.
-- The unused Python `webhooks` table is dropped: webhook IDs are minted
-- dynamically at ingestion. gen_random_uuid() is built into Postgres core (>= 13).

CREATE TYPE delivery_status AS ENUM (
    'pending', 'success', 'failed_attempt', 'final_failure'
);

CREATE TABLE subscriptions (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    target_url           VARCHAR(2048) NOT NULL,                  -- widened from Python's VARCHAR(255)
    secret_key           VARCHAR(255)  NOT NULL,
    previous_secret_key  VARCHAR(255),                            -- NEW: dual-secret rotation key
    secret_rotated_at    TIMESTAMPTZ,                             -- NEW: grace-period start
    event_types          TEXT[]        NOT NULL DEFAULT '{}',
    is_active            BOOLEAN       NOT NULL DEFAULT TRUE,     -- NEW (Python has no is_active)
    consecutive_failures INT           NOT NULL DEFAULT 0,        -- NEW: drives auto-disable
    created_at           TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE TABLE delivery_logs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),  -- X-Webhook-Delivery-ID (unique per attempt)
    webhook_id      UUID            NOT NULL,                     -- X-Webhook-ID (stable across attempts)
    subscription_id UUID            NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    target_url      VARCHAR(2048)   NOT NULL,
    payload         JSONB           NOT NULL,
    event_type      VARCHAR(100),
    attempt_number  INT             NOT NULL DEFAULT 1,           -- per-chain (resets to 1 on replay)
    replay_number   INT             NOT NULL DEFAULT 0,           -- 0 = original chain; +1 per replay
    status          delivery_status NOT NULL DEFAULT 'pending',
    http_status     INT,
    error_details   TEXT,
    next_retry_at   TIMESTAMPTZ,                                  -- retry due time AND visibility-lease deadline
    in_dlq          BOOLEAN         NOT NULL DEFAULT FALSE,       -- TRUE between final_failure and DLQ ack
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Idempotency authority for the OPTIONAL X-Idempotency-Key (Postgres, not Redis).
-- Written in the SAME transaction as the initial pending delivery_logs row, so a
-- crash can never acknowledge a key whose webhook row was not durably inserted.
CREATE TABLE ingest_idempotency (
    subscription_id UUID         NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    idempotency_key VARCHAR(128) NOT NULL,                        -- charset excludes the '.' signing delimiter
    webhook_id      UUID         NOT NULL,                        -- the X-Webhook-ID minted for the first accepted request
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (subscription_id, idempotency_key)                -- the atomic dedupe constraint
);
