package domain

import "errors"

// Sentinel errors shared across layers. Adapters wrap these (fmt.Errorf with %w)
// so the API layer can map domain failures to HTTP status codes via errors.Is.
var (
	// ErrNotFound — the requested entity does not exist (→ 404).
	ErrNotFound = errors.New("not found")
	// ErrConflict — a uniqueness / state precondition was violated (→ 409).
	ErrConflict = errors.New("conflict")
	// ErrInvalidInput — request payload failed validation (→ 400).
	ErrInvalidInput = errors.New("invalid input")

	// ErrInactiveSubscription — ingest targeted a deactivated subscription (→ 403/410).
	ErrInactiveSubscription = errors.New("subscription is inactive")

	// ErrSignatureInvalid — HMAC verification failed (→ 401).
	ErrSignatureInvalid = errors.New("signature verification failed")
	// ErrTimestampOutOfWindow — Webhook-Timestamp outside the allowed drift (→ 400).
	ErrTimestampOutOfWindow = errors.New("timestamp outside the allowed drift window")

	// ErrQueueEmpty — a blocking dequeue timed out with no task available.
	ErrQueueEmpty = errors.New("queue empty")
)
