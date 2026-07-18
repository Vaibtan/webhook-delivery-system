package worker

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
)

// retryDelay applies capped ×3 backoff with ±20% jitter.
func retryDelay(attempt int, base, max time.Duration) time.Duration {
	delay := base
	for i := 1; i < attempt && delay < max; i++ {
		if delay > max/3 {
			delay = max
			break
		}
		delay *= 3
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
	sched    retryClaimer
	interval time.Duration
	now      func() time.Time
}

type retryClaimer interface {
	ClaimDue(ctx context.Context, now time.Time) (int, error)
}

// NewRetrySchedulerWorker constructs the poller. A poll interval of ~1s keeps
// retry latency tight without hammering Redis.
func NewRetrySchedulerWorker(sched retryClaimer, interval time.Duration) *RetrySchedulerWorker {
	return &RetrySchedulerWorker{sched: sched, interval: interval, now: time.Now}
}

// Run polls until ctx is cancelled.
func (w *RetrySchedulerWorker) Run(ctx context.Context) error {
	return runPeriodic(ctx, w.interval, false, func(ctx context.Context) {
		n, err := w.sched.ClaimDue(ctx, w.now())
		if err != nil {
			if ctx.Err() == nil {
				slog.Error("retry scheduler: claim due failed", "error", err)
			}
			return
		}
		if n > 0 {
			slog.Debug("retry scheduler: re-enqueued due retries", "count", n)
		}
	})
}

// dueFinder is the recovery worker's view of the delivery-log repository.
type dueFinder interface {
	FindDuePending(ctx context.Context, limit int, after *domain.RecoveryCursor) ([]domain.DuePending, error)
	RearmLeaseLessPending(ctx context.Context, limit int, orphanThreshold time.Duration) ([]string, error)
}

type taskEnqueuer interface {
	Enqueue(ctx context.Context, deliveryLogID string) error
}

// RecoveryWorker re-enqueues work whose enqueue was lost or whose visibility
// lease expired. Live leases are excluded; the claim CAS tolerates duplicates.
type RecoveryWorker struct {
	logs            dueFinder
	queue           taskEnqueuer
	interval        time.Duration
	batch           int
	orphanThreshold time.Duration
}

// NewRecoveryWorker constructs the recovery scanner.
func NewRecoveryWorker(logs dueFinder, queue taskEnqueuer, interval time.Duration, batch int, orphanThreshold time.Duration) *RecoveryWorker {
	return &RecoveryWorker{
		logs: logs, queue: queue, interval: interval, batch: batch,
		orphanThreshold: orphanThreshold,
	}
}

// Run scans at startup and then on the interval until ctx is cancelled.
func (w *RecoveryWorker) Run(ctx context.Context) error {
	return runPeriodic(ctx, w.interval, true, w.scan)
}

func (w *RecoveryWorker) scan(ctx context.Context) {
	requeued := 0
	var after *domain.RecoveryCursor
	for {
		items, err := w.logs.FindDuePending(ctx, w.batch, after)
		if err != nil {
			if ctx.Err() == nil {
				slog.Error("recovery: scan failed", "error", err)
			}
			return
		}
		for _, item := range items {
			requeued += w.enqueue(ctx, item.ID)
		}
		if len(items) < w.batch {
			break
		}
		last := items[len(items)-1]
		after = &domain.RecoveryCursor{DueAt: last.DueAt, ID: last.ID}
	}

	for {
		ids, err := w.logs.RearmLeaseLessPending(ctx, w.batch, w.orphanThreshold)
		if err != nil {
			if ctx.Err() == nil {
				slog.Error("recovery: rearm lease-less rows failed", "error", err)
			}
			return
		}
		for _, id := range ids {
			requeued += w.enqueue(ctx, id)
		}
		if len(ids) < w.batch {
			break
		}
	}
	if requeued > 0 {
		slog.Info("recovery: re-enqueued orphaned pending rows", "count", requeued)
	}
}

func (w *RecoveryWorker) enqueue(ctx context.Context, id string) int {
	if err := w.queue.Enqueue(ctx, id); err != nil {
		if ctx.Err() == nil {
			slog.Error("recovery: re-enqueue failed", "delivery_id", id, "error", err)
		}
		return 0
	}
	return 1
}
