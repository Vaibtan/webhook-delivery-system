package worker

import (
	"context"
	"log/slog"
	"math"
	"math/rand/v2"
	"time"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
)

// retryDelay returns the delay before the next attempt, given the attempt number
// that JUST failed (1..). Geometric ×3 from base, capped at max, with ±20%
// jitter so a thundering herd of simultaneous failures spreads out (plan §4b).
// math/rand/v2 top-level funcs are safe for concurrent use.
func retryDelay(attempt int, base, max time.Duration) time.Duration {
	if base <= 0 {
		base = 10 * time.Second
	}
	delay := time.Duration(float64(base) * math.Pow(3, float64(attempt-1)))
	if max > 0 && delay > max {
		delay = max
	}
	jitter := delay / 5
	if jitter <= 0 {
		return delay
	}
	return delay - jitter + time.Duration(rand.Int64N(int64(2*jitter)))
}

// RetrySchedulerWorker polls the retry sorted set on an interval and atomically
// moves due entries into the work queue via the scheduler's Lua script.
type RetrySchedulerWorker struct {
	sched    domain.RetryScheduler
	interval time.Duration
	now      func() time.Time
}

// NewRetrySchedulerWorker constructs the poller. A poll interval of ~1s keeps
// retry latency tight without hammering Redis.
func NewRetrySchedulerWorker(sched domain.RetryScheduler, interval time.Duration) *RetrySchedulerWorker {
	if interval <= 0 {
		interval = time.Second
	}
	return &RetrySchedulerWorker{sched: sched, interval: interval, now: time.Now}
}

// Run polls until ctx is cancelled.
func (w *RetrySchedulerWorker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			n, err := w.sched.ClaimDue(ctx, w.now())
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				slog.Error("retry scheduler: claim due failed", "error", err)
				continue
			}
			if n > 0 {
				slog.Debug("retry scheduler: re-enqueued due retries", "count", n)
			}
		}
	}
}

// dueFinder is the recovery worker's view of the delivery-log repository.
type dueFinder interface {
	FindDuePending(ctx context.Context, limit int) ([]string, error)
}

// RecoveryWorker re-enqueues orphaned pending rows: those genuinely due whose
// enqueue was lost, or whose worker crashed and the visibility lease expired
// (plan §0/§1). In-flight rows are hidden by their lease, so they are not
// re-enqueued. Duplicate enqueues are harmless — the claim CAS admits only one.
type RecoveryWorker struct {
	logs     dueFinder
	queue    domain.TaskQueue
	interval time.Duration
	batch    int
}

// NewRecoveryWorker constructs the recovery scanner.
func NewRecoveryWorker(logs dueFinder, queue domain.TaskQueue, interval time.Duration, batch int) *RecoveryWorker {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	if batch <= 0 {
		batch = 100
	}
	return &RecoveryWorker{logs: logs, queue: queue, interval: interval, batch: batch}
}

// Run scans at startup and then on the interval until ctx is cancelled.
func (w *RecoveryWorker) Run(ctx context.Context) error {
	w.scan(ctx) // startup scan reclaims work lost across a restart
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			w.scan(ctx)
		}
	}
}

func (w *RecoveryWorker) scan(ctx context.Context) {
	ids, err := w.logs.FindDuePending(ctx, w.batch)
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("recovery: scan failed", "error", err)
		}
		return
	}
	requeued := 0
	for _, id := range ids {
		if err := w.queue.Enqueue(ctx, id); err != nil {
			slog.Error("recovery: re-enqueue failed", "delivery_id", id, "error", err)
			continue
		}
		requeued++
	}
	if requeued > 0 {
		slog.Info("recovery: re-enqueued orphaned pending rows", "count", requeued)
	}
}
