package worker

import (
	"context"
	"log/slog"
	"time"
)

// cleanupRepo is the cleanup worker's view of the repository.
type cleanupRepo interface {
	DeleteExpired(ctx context.Context, retention time.Duration) (int64, error)
	PruneIdempotency(ctx context.Context, ttl time.Duration) (int64, error)
}

// CleanupWorker is the sole retention-driven row deleter. It never deletes
// pending or unacknowledged DLQ rows.
type CleanupWorker struct {
	repo           cleanupRepo
	retention      time.Duration
	idempotencyTTL time.Duration
	interval       time.Duration
}

// NewCleanupWorker constructs the retention worker.
func NewCleanupWorker(repo cleanupRepo, retention, idempotencyTTL, interval time.Duration) *CleanupWorker {
	return &CleanupWorker{repo: repo, retention: retention, idempotencyTTL: idempotencyTTL, interval: interval}
}

// Run cleans at startup then on the interval until ctx is cancelled.
func (w *CleanupWorker) Run(ctx context.Context) error {
	return runPeriodic(ctx, w.interval, true, w.cleanup)
}

func (w *CleanupWorker) cleanup(ctx context.Context) {
	deleted, err := w.repo.DeleteExpired(ctx, w.retention)
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("cleanup: delete expired failed", "error", err)
		}
		return
	}
	pruned, err := w.repo.PruneIdempotency(ctx, w.idempotencyTTL)
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("cleanup: prune idempotency failed", "error", err)
		}
		return
	}
	if deleted > 0 || pruned > 0 {
		slog.Info("cleanup: reaped rows", "delivery_logs", deleted, "ingest_idempotency", pruned)
	}
}

// reconcilerRepo is the DLQ reconciler's view of the repository.
type reconcilerRepo interface {
	ListInDLQ(ctx context.Context) ([]string, error)
	IsInDLQ(ctx context.Context, id string) (bool, error)
	ForceAckOldDLQ(ctx context.Context, maxAge time.Duration) ([]string, error)
}

type deadLetterIndex interface {
	Push(ctx context.Context, deliveryLogID string) error
	Remove(ctx context.Context, deliveryLogID string) error
	List(ctx context.Context) ([]string, error)
}

// DLQReconciler repairs the Redis DLQ index from PostgreSQL and force-acks aged
// entries. It never deletes delivery rows.
type DLQReconciler struct {
	repo     reconcilerRepo
	dlq      deadLetterIndex
	maxAge   time.Duration
	interval time.Duration
}

// NewDLQReconciler constructs the reconciler.
func NewDLQReconciler(repo reconcilerRepo, dlq deadLetterIndex, maxAge, interval time.Duration) *DLQReconciler {
	return &DLQReconciler{repo: repo, dlq: dlq, maxAge: maxAge, interval: interval}
}

// Run reconciles at startup then on the interval until ctx is cancelled.
func (w *DLQReconciler) Run(ctx context.Context) error {
	return runPeriodic(ctx, w.interval, true, w.reconcile)
}

func (w *DLQReconciler) reconcile(ctx context.Context) {
	// DLQ_MAX_AGE safety valve first: force-ack aged entries (flag only).
	forced, err := w.repo.ForceAckOldDLQ(ctx, w.maxAge)
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("dlq reconciler: force-ack failed", "error", err)
		}
		return
	}
	for _, id := range forced {
		if err := w.dlq.Remove(ctx, id); err != nil {
			slog.Warn("dlq reconciler: LREM after force-ack failed", "id", id, "error", err)
		}
		slog.Warn("dlq reconciler: force-acked aged DLQ entry", "id", id, "max_age", w.maxAge)
	}

	flagged, err := w.repo.ListInDLQ(ctx)
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("dlq reconciler: list flagged failed", "error", err)
		}
		return
	}
	listed, err := w.dlq.List(ctx)
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("dlq reconciler: list redis failed", "error", err)
		}
		return
	}

	listedSet := make(map[string]struct{}, len(listed))
	for _, id := range listed {
		listedSet[id] = struct{}{}
	}
	flaggedSet := make(map[string]struct{}, len(flagged))
	for _, id := range flagged {
		flaggedSet[id] = struct{}{}
	}

	// flag→list: a flagged row missing from the LIST (lost LPUSH) is re-pushed.
	// Re-validate against the DB first — symmetric with the list→flag branch. The
	// `flagged` snapshot (DB) was taken BEFORE the `listed` snapshot (Redis), so a
	// concurrent ack that cleared in_dlq and then LREM'd in that window could
	// otherwise be wrongly re-pushed (transient, but avoidable with one point read).
	for _, id := range flagged {
		if _, ok := listedSet[id]; ok {
			continue
		}
		if stillFlagged, err := w.repo.IsInDLQ(ctx, id); err != nil || !stillFlagged {
			continue
		}
		if err := w.dlq.Push(ctx, id); err != nil {
			slog.Warn("dlq reconciler: re-push failed", "id", id, "error", err)
		}
	}
	// list→flag: a LIST id whose row is gone OR already acked (in_dlq=FALSE) is LREM'd.
	for _, id := range listed {
		if _, ok := flaggedSet[id]; ok {
			continue
		}
		// Double-check against the DB (the flagged snapshot may be slightly stale).
		stillFlagged, err := w.repo.IsInDLQ(ctx, id)
		if err != nil || stillFlagged {
			continue
		}
		if err := w.dlq.Remove(ctx, id); err != nil {
			slog.Warn("dlq reconciler: LREM stale entry failed", "id", id, "error", err)
		}
	}
}
