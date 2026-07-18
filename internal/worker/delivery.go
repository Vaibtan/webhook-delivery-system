// Package worker contains the background delivery engine.
package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/signature"
)

const (
	maxErrorDetailLen  = 512
	userAgent          = "webhook-delivery-service/1.0"
	concurrencyBackoff = 3 * time.Second
)

// DelivererConfig holds tunables for the deliverer.
type DelivererConfig struct {
	// VisibilityTimeout is the lease stamped on a row when it is claimed.
	VisibilityTimeout time.Duration
	// MaxRetryAttempts is the total attempt budget (1 initial + N-1 retries).
	MaxRetryAttempts int
	// RetryBaseDelay / RetryMaxDelay parameterise the ×3 backoff curve.
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	// BreakerResetTimeout is how far ahead a breaker-denied row is rescheduled
	// (so it becomes due ~when the breaker admits its next probe).
	BreakerResetTimeout time.Duration
	// AutoDisableThreshold is the consecutive-final-failure count that disables a
	// subscription (is_active=FALSE).
	AutoDisableThreshold int
}

// Breaker is the per-URL circuit breaker the deliverer consults (satisfied by
// *breaker.CircuitBreaker).
type Breaker interface {
	Allow() (bool, int64)
	RecordSuccess(token int64)
	RecordFailure(token int64)
}

// Sem is a per-subscription concurrency permit (satisfied by *semaphore.Semaphore).
type Sem interface {
	TryAcquire() bool
	Release()
}

// MetricsRecorder records per-delivery latency and outcome (satisfied by
// *metrics.Collector).
type MetricsRecorder interface {
	RecordDelivery(latency time.Duration, success bool)
}

// deliveryStore is the subset of the delivery-log persistence port the Deliverer
// drives through the CAS lifecycle. The concrete *store.DeliveryLogRepo satisfies
// it; narrowing keeps the Deliverer's test surface to the methods it uses.
type deliveryStore interface {
	GetByID(ctx context.Context, id string) (*domain.DeliveryLog, error)
	ClaimPending(ctx context.Context, id string, lease time.Duration) (bool, error)
	MarkDropped(ctx context.Context, id string, errorDetails string) (bool, error)
	FailAndScheduleRetry(ctx context.Context, prevID string, httpStatus *int, errorDetails string, nextRetryAt time.Time) (successorID string, won bool, err error)
	RescheduleSamePending(ctx context.Context, id string, at time.Time) (bool, error)
}

// subscriptionState owns subscription reads and the cross-table consequences
// of terminal delivery transitions.
type subscriptionState interface {
	GetByID(ctx context.Context, id string) (*domain.Subscription, error)
	RecordDeliverySuccess(ctx context.Context, id, subscriptionID string, httpStatus int) (won bool, err error)
	RecordFinalFailure(ctx context.Context, id, subscriptionID string, httpStatus *int, errorDetails string, disableThreshold int) (won, disabled bool, err error)
}

type deliveryScheduler interface {
	Schedule(ctx context.Context, deliveryLogID string, at time.Time) error
}

type deadLetterWriter interface {
	Push(ctx context.Context, deliveryLogID string) error
}

// DeliveryControls are the required runtime controls around an HTTP attempt.
type DeliveryControls struct {
	BreakerFor   func(targetURL string) Breaker
	SemaphoreFor func(subscriptionID string) Sem
	Metrics      MetricsRecorder
}

// Deliverer performs one delivery attempt against a delivery-log row and applies
// the CAS-guarded status transition. It satisfies domain.Deliverer.
type Deliverer struct {
	logs   deliveryStore
	subs   subscriptionState
	sched  deliveryScheduler
	dlq    deadLetterWriter
	client *http.Client
	cfg    DelivererConfig

	breakerFor func(targetURL string) Breaker
	semFor     func(subscriptionID string) Sem
	metrics    MetricsRecorder
}

// NewDeliverer constructs an immutable Deliverer from validated configuration
// and explicit runtime controls.
func NewDeliverer(logs deliveryStore, subs subscriptionState, sched deliveryScheduler, dlq deadLetterWriter, client *http.Client, cfg DelivererConfig, controls DeliveryControls) *Deliverer {
	return &Deliverer{
		logs: logs, subs: subs, sched: sched, dlq: dlq, client: client, cfg: cfg,
		breakerFor: controls.BreakerFor,
		semFor:     controls.SemaphoreFor,
		metrics:    controls.Metrics,
	}
}

var _ domain.Deliverer = (*Deliverer)(nil)

// Process delivers the attempt identified by deliveryLogID. It returns a
// non-nil error only for unexpected infrastructure failures; ordinary delivery
// failures are persisted and reported as nil so they never tear down the pool.
func (d *Deliverer) Process(ctx context.Context, deliveryLogID string) error {
	log, err := d.logs.GetByID(ctx, deliveryLogID)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil // nothing to deliver
		}
		return fmt.Errorf("deliver: load log %s: %w", deliveryLogID, err)
	}
	if log.Status != domain.StatusPending {
		return nil // already finalized by someone else
	}

	// The visibility-lease CAS is the mutual-
	// exclusion point. 0 rows ⇒ not due / lease live / already handled ⇒ this is
	// a duplicate dequeue, so skip without delivering.
	claimed, err := d.logs.ClaimPending(ctx, log.ID, d.cfg.VisibilityTimeout)
	if err != nil {
		return fmt.Errorf("deliver: claim %s: %w", log.ID, err)
	}
	if !claimed {
		// Self-heal: if the row is still pending with a future due time (e.g. the
		// poller moved it slightly early under clock skew), reschedule it so it is
		// not stuck until the periodic recovery scan. A handled/lease-live row is
		// either re-armed harmlessly or dropped on its next, post-finalization check.
		d.rescheduleIfNotDue(ctx, log.ID)
		return nil
	}

	sub, err := d.subs.GetByID(ctx, log.SubscriptionID)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil // subscription deleted (cascade will clean the row)
		}
		return fmt.Errorf("deliver: load subscription %s: %w", log.SubscriptionID, err)
	}

	// Re-check is_active at delivery time (the sub may have been deactivated
	// between ingest and delivery). Intentional drop → final_failure, no DLQ.
	if !sub.IsActive {
		if _, err := d.logs.MarkDropped(ctx, log.ID, "subscription deactivated"); err != nil {
			return fmt.Errorf("deliver: mark deactivated: %w", err)
		}
		return nil
	}

	// Per-subscription concurrency (non-blocking). At the limit, requeue with a
	// small back-off WITHOUT consuming an attempt — never park the worker (avoids
	// head-of-line starvation). Acquired BEFORE the breaker so a half-open probe
	// token is never taken and then abandoned.
	sem := d.semFor(sub.ID)
	if !sem.TryAcquire() {
		d.rescheduleNoAttempt(ctx, log.ID, time.Now().Add(concurrencyBackoff), "per-sub concurrency limit")
		return nil
	}
	defer sem.Release()

	// Circuit breaker (per target URL). On denial: no HTTP call, no attempt
	// consumed — reschedule the same pending row so it retries ~when the breaker
	// admits its next probe. When allowed, the token MUST be reported back.
	cb := d.breakerFor(log.TargetURL)
	allowed, token := cb.Allow()
	if !allowed {
		d.rescheduleNoAttempt(ctx, log.ID, time.Now().Add(d.cfg.BreakerResetTimeout), "circuit breaker open")
		return nil
	}

	httpStatus, errDetail, latency := d.attempt(ctx, log, sub)
	success := httpStatus != nil && *httpStatus >= 200 && *httpStatus < 300
	d.metrics.RecordDelivery(latency, success)
	if success {
		cb.RecordSuccess(token)
	} else {
		cb.RecordFailure(token)
	}

	if success {
		_, err := d.subs.RecordDeliverySuccess(ctx, log.ID, sub.ID, *httpStatus)
		if err != nil {
			return fmt.Errorf("deliver: mark success: %w", err)
		}
		return nil
	}
	return d.handleFailure(ctx, log, httpStatus, errDetail)
}

// rescheduleNoAttempt pushes a pending row's due time forward WITHOUT consuming
// an attempt (breaker denial / concurrency requeue), then ZADDs post-commit.
func (d *Deliverer) rescheduleNoAttempt(ctx context.Context, id string, at time.Time, reason string) {
	won, err := d.logs.RescheduleSamePending(ctx, id, at)
	if err != nil {
		slog.Error("deliver: reschedule (no attempt) failed", "id", id, "reason", reason, "error", err)
		return
	}
	if !won {
		return // row finalized elsewhere
	}
	if err := d.sched.Schedule(ctx, id, at); err != nil {
		slog.Warn("deliver: reschedule ZADD failed (recovery will heal)", "id", id, "reason", reason, "error", err)
	}
}

// rescheduleIfNotDue re-arms the retry schedule for a row that the claim
// rejected because it is not yet due (pending with a future next_retry_at). This
// keeps an early-moved row responsive instead of waiting for the recovery scan.
func (d *Deliverer) rescheduleIfNotDue(ctx context.Context, id string) {
	cur, err := d.logs.GetByID(ctx, id)
	if err != nil || cur.Status != domain.StatusPending || cur.NextRetryAt == nil {
		return
	}
	if cur.NextRetryAt.After(time.Now()) {
		if err := d.sched.Schedule(ctx, id, *cur.NextRetryAt); err != nil {
			slog.Warn("deliver: reschedule not-due row failed", "id", id, "error", err)
		}
	}
}

// handleFailure schedules a successor while attempts remain, otherwise it
// atomically marks the row terminal and updates subscription failure state.
func (d *Deliverer) handleFailure(ctx context.Context, log *domain.DeliveryLog, httpStatus *int, errDetail string) error {
	if log.AttemptNumber < d.cfg.MaxRetryAttempts {
		nextAt := time.Now().Add(retryDelay(log.AttemptNumber, d.cfg.RetryBaseDelay, d.cfg.RetryMaxDelay))
		successorID, won, err := d.logs.FailAndScheduleRetry(ctx, log.ID, httpStatus, errDetail, nextAt)
		if err != nil {
			return fmt.Errorf("deliver: schedule retry: %w", err)
		}
		if !won {
			return nil // lost the CAS — another worker handled it
		}
		// Post-commit ZADD; best-effort — recovery heals a lost schedule because
		// the successor row is already durable and due-pending.
		if err := d.sched.Schedule(ctx, successorID, nextAt); err != nil {
			slog.Warn("deliver: schedule retry zadd failed (recovery will heal)",
				"successor_id", successorID, "error", err)
		}
		return nil
	}

	// Attempt budget exhausted → terminal. final_failure + in_dlq AND the
	// consecutive_failures bump (auto-disable) commit in one tx; the LPUSH and
	// cache eviction are post-commit best-effort.
	won, disabled, err := d.subs.RecordFinalFailure(
		ctx, log.ID, log.SubscriptionID, httpStatus, errDetail, d.cfg.AutoDisableThreshold,
	)
	if err != nil {
		return fmt.Errorf("deliver: finalize failure: %w", err)
	}
	if won {
		if err := d.dlq.Push(ctx, log.ID); err != nil {
			slog.Warn("deliver: dlq push failed (reconciler will heal)", "id", log.ID, "error", err)
		}
	}
	if disabled {
		slog.Warn("deliver: subscription auto-disabled after consecutive failures", "subscription_id", log.SubscriptionID)
	}
	return nil
}

// attempt performs the HTTP POST and returns (httpStatus, errorDetail, latency).
// A nil httpStatus means the request never produced a response (network/dial
// error); latency covers the HTTP round trip.
func (d *Deliverer) attempt(ctx context.Context, log *domain.DeliveryLog, sub *domain.Subscription) (*int, string, time.Duration) {
	logger := slog.Default().With("delivery_id", log.ID, "webhook_id", log.WebhookID)

	// Deterministic outbound body, signed with the canonical grammar (empty key).
	canonical, err := signature.CanonicalJSON(log.Payload)
	if err != nil {
		logger.Error("deliver: canonicalize payload", "error", err)
		return nil, truncate("invalid stored payload: "+err.Error(), maxErrorDetailLen), 0
	}
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	sig := signature.Sign(signature.BuildMessage(ts, "", canonical), sub.SecretKey)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, log.TargetURL, bytes.NewReader(canonical))
	if err != nil {
		return nil, truncate("build request: "+err.Error(), maxErrorDetailLen), 0
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("X-Webhook-ID", log.WebhookID)
	req.Header.Set("X-Webhook-Delivery-ID", log.ID)
	req.Header.Set("X-Delivery-Attempt", strconv.Itoa(log.AttemptNumber))
	req.Header.Set("Webhook-Timestamp", ts)
	req.Header.Set("X-Hub-Signature-256", sig)

	start := time.Now()
	resp, err := d.client.Do(req)
	latency := time.Since(start)
	if err != nil {
		logger.Warn("deliver: request failed", "error", err)
		return nil, truncate(err.Error(), maxErrorDetailLen), latency
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body) // drain for connection reuse
		_ = resp.Body.Close()
	}()

	status := resp.StatusCode
	if status >= 200 && status < 300 {
		return &status, "", latency
	}
	// Capture a bounded snippet of the error body.
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorDetailLen))
	detail := fmt.Sprintf("HTTP %d: %s", status, string(snippet))
	return &status, truncate(detail, maxErrorDetailLen), latency
}

// truncate returns valid UTF-8 capped at n bytes for PostgreSQL text fields.
func truncate(s string, n int) string {
	s = strings.ToValidUTF8(s, "�")
	if len(s) <= n {
		return s
	}
	s = s[:n]
	for !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}
