package domain

import (
	"encoding/json"
	"time"
)

// DeliveryStatus is the lifecycle state of a single delivery attempt row.
type DeliveryStatus string

const (
	// StatusPending — created, not yet finalized. Undelivered work; the
	// retention cleanup must never reap a pending row (§ Advanced Features 1).
	StatusPending DeliveryStatus = "pending"
	// StatusSuccess — the target returned 2xx.
	StatusSuccess DeliveryStatus = "success"
	// StatusFailedAttempt — this attempt failed and a successor pending row
	// was created in the same transaction (more attempts remain).
	StatusFailedAttempt DeliveryStatus = "failed_attempt"
	// StatusFinalFailure — the last attempt failed; the row enters the DLQ.
	StatusFinalFailure DeliveryStatus = "final_failure"
)

// DeliveryLog is one immutable delivery attempt.
//
//   - ID         = X-Webhook-Delivery-ID, unique per attempt.
//   - WebhookID   = X-Webhook-ID, stable across all attempts of one ingested event.
//   - AttemptNumber is per-chain and resets to 1 on replay.
//   - ReplayNumber identifies the chain: 0 = original, +1 per replay. The current
//     outcome of a webhook is the chain with MAX(replay_number).
//
// (WebhookID, AttemptNumber) is intentionally non-unique — a replay restarts the
// attempt chain. Duplicate successors are prevented by the status CAS, not a
// unique index.
type DeliveryLog struct {
	ID             string
	WebhookID      string
	SubscriptionID string
	TargetURL      string
	Payload        json.RawMessage
	EventType      string // "" when the event carried no type
	AttemptNumber  int
	ReplayNumber   int
	Status         DeliveryStatus
	HTTPStatus     *int       // nil until an HTTP response (or terminal non-HTTP failure)
	ErrorDetails   string     // truncated to 512 chars by the deliverer; "" on success
	NextRetryAt    *time.Time // retry due time AND visibility-lease deadline for pending rows
	InDLQ          bool
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// IngestParams is the input to the atomic ingest transaction: optionally record
// the idempotency key and insert the initial pending delivery row.
type IngestParams struct {
	SubscriptionID string
	IdempotencyKey string // "" when the optional header is absent (no dedupe)
	WebhookID      string // caller-minted X-Webhook-ID for a first-time request
	TargetURL      string
	Payload        json.RawMessage
	EventType      string // "" when absent
}

// IngestResult reports the outcome of the ingest transaction. On a duplicate
// idempotency key, WebhookID is the STORED id and DeliveryLogID is empty (no new
// row, no enqueue).
type IngestResult struct {
	WebhookID     string
	DeliveryLogID string
	Duplicate     bool
}

// DuePending is a recovery-scan row ordered by (DueAt, ID).
type DuePending struct {
	ID    string
	DueAt time.Time
}

// RecoveryCursor is the keyset position for a single draining recovery scan.
type RecoveryCursor struct {
	DueAt time.Time
	ID    string
}

// StatusCounts aggregates delivery_logs rows by status over a window.
type StatusCounts map[DeliveryStatus]int
