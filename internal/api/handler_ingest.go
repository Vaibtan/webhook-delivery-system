package api

import (
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/idgen"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/signature"
)

const defaultIngestMaxBytes = 64 << 10 // 64 KiB

// ingestResponse is the body returned by POST /ingest/{id}.
type ingestResponse struct {
	WebhookID  string `json:"webhook_id"`
	DeliveryID string `json:"delivery_id,omitempty"`
	Status     string `json:"status"`
	Duplicate  bool   `json:"duplicate,omitempty"`
}

// handleIngest authenticates a webhook sender via HMAC, enforces the timestamp
// drift window and optional idempotency key, applies the event-type filter, and
// inserts the initial pending delivery row (atomically with idempotency dedupe).
// Public route — the per-subscription HMAC IS the authentication.
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	subID := pathID(r)

	sub, err := s.opts.Subscriptions.GetByID(r.Context(), subID)
	if err != nil {
		writeDomainError(w, r, err)
		return
	}
	if !sub.IsActive {
		writeError(w, http.StatusForbidden, "subscription is inactive")
		return
	}

	// Required signing headers.
	sig := r.Header.Get("X-Hub-Signature-256")
	timestamp := r.Header.Get("Webhook-Timestamp")
	if sig == "" || timestamp == "" {
		writeError(w, http.StatusBadRequest, "missing X-Hub-Signature-256 or Webhook-Timestamp header")
		return
	}
	idempotencyKey := r.Header.Get("X-Idempotency-Key")
	if !signature.ValidIdempotencyKey(idempotencyKey) {
		writeError(w, http.StatusBadRequest, "invalid X-Idempotency-Key (must match [A-Za-z0-9_-]{1,128})")
		return
	}

	// Read the raw body under the strict 64KB ingest cap.
	r.Body = http.MaxBytesReader(w, r.Body, defaultIngestMaxBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "payload exceeds 64KB limit")
			return
		}
		writeError(w, http.StatusBadRequest, "could not read request body")
		return
	}

	// Verify timestamp drift + HMAC over the RAW received body (with the key).
	if err := s.verifyIngest(sub, body, timestamp, idempotencyKey, sig); err != nil {
		switch {
		case errors.Is(err, domain.ErrTimestampOutOfWindow):
			writeError(w, http.StatusBadRequest, "timestamp outside the allowed drift window")
		default:
			writeError(w, http.StatusUnauthorized, "signature verification failed")
		}
		return
	}

	// Charge the subscription's quota only after authentication. An attacker who
	// merely learns a subscription UUID must not be able to consume its bucket.
	if !s.opts.RateLimitAllow(sub.ID) {
		writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
		return
	}

	// JSONB storage requires exactly one valid JSON value. Reject malformed
	// payloads at the trust boundary instead of surfacing a PostgreSQL 500.
	if !json.Valid(body) {
		writeError(w, http.StatusBadRequest, "payload must be valid JSON")
		return
	}

	// An empty subscription filter accepts all event types.
	eventType := r.URL.Query().Get("event_type")
	if err := domain.ValidateEventType(eventType); err != nil {
		writeDomainError(w, r, err)
		return
	}
	if !sub.AcceptsEvent(eventType) {
		writeJSON(w, http.StatusAccepted, ingestResponse{Status: "ignored"})
		return
	}

	webhookID, err := idgen.NewUUIDv4()
	if err != nil {
		writeDomainError(w, r, err)
		return
	}

	result, err := s.opts.DeliveryIngest.IngestPending(r.Context(), domain.IngestParams{
		SubscriptionID: sub.ID,
		IdempotencyKey: idempotencyKey,
		WebhookID:      webhookID,
		TargetURL:      sub.TargetURL,
		Payload:        body,
		EventType:      eventType,
	})
	if err != nil {
		writeDomainError(w, r, err)
		return
	}
	if result.Duplicate {
		writeJSON(w, http.StatusOK, ingestResponse{WebhookID: result.WebhookID, Status: "duplicate", Duplicate: true})
		return
	}

	// Sync delivery (dev): deliver in-request and report the persisted outcome.
	if s.opts.SyncDelivery {
		if err := s.opts.Deliverer.Process(r.Context(), result.DeliveryLogID); err != nil {
			loggerFrom(r.Context()).Error("sync delivery failed", "error", err, "delivery_id", result.DeliveryLogID)
			writeError(w, http.StatusInternalServerError, "delivery error")
			return
		}
		status := "delivered"
		if d, derr := s.opts.DeliveryIngest.GetByID(r.Context(), result.DeliveryLogID); derr == nil {
			status = string(d.Status)
		}
		writeJSON(w, http.StatusOK, ingestResponse{WebhookID: result.WebhookID, DeliveryID: result.DeliveryLogID, Status: status})
		return
	}

	// Async: enqueue and 202. If the enqueue fails the row is still durable —
	// the recovery scan picks it up.
	if err := s.opts.TaskQueue.Enqueue(r.Context(), result.DeliveryLogID); err != nil {
		loggerFrom(r.Context()).Error("enqueue failed", "error", err, "delivery_id", result.DeliveryLogID)
	}
	writeJSON(w, http.StatusAccepted, ingestResponse{WebhookID: result.WebhookID, DeliveryID: result.DeliveryLogID, Status: "accepted"})
}

// verifyIngest enforces the drift window then HMAC-verifies the raw body against
// the current secret, falling back to the previous secret within the grace
// window. Returns ErrTimestampOutOfWindow or ErrSignatureInvalid.
func (s *Server) verifyIngest(sub *domain.Subscription, body []byte, timestamp, idempotencyKey, sig string) error {
	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return domain.ErrTimestampOutOfWindow
	}
	// The drift/grace windows are config-owned and validated > 0 / >= 0 at startup,
	// so verifyIngest trusts the injected values rather than re-defaulting them.
	if math.Abs(float64(time.Now().Unix()-ts)) > s.opts.SignatureDriftWindow.Seconds() {
		return domain.ErrTimestampOutOfWindow
	}

	message := signature.BuildMessage(timestamp, idempotencyKey, body)
	if signature.Verify(message, sub.SecretKey, sig) {
		return nil
	}
	if sub.PreviousSecretKey != "" && sub.SecretRotatedAt != nil && time.Since(*sub.SecretRotatedAt) < s.opts.SecretGraceWindow {
		if signature.Verify(message, sub.PreviousSecretKey, sig) {
			return nil
		}
	}
	return domain.ErrSignatureInvalid
}
