package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
)

// cleanupRepo is the cleanup worker's view of the repository.
type cleanupRepo interface {
	DeleteExpired(ctx context.Context, retention time.Duration) (int64, error)
	PruneIdempotency(ctx context.Context, ttl time.Duration) (int64, error)
}

// CleanupWorker is the SOLE retention-driven row deleter (plan Adv §1). It
// deletes terminal, non-DLQ rows past the retention horizon and prunes the
// idempotency table — never pending or in_dlq rows.
type CleanupWorker struct {
	repo           cleanupRepo
	retention      time.Duration
	idempotencyTTL time.Duration
	interval       time.Duration
}

// NewCleanupWorker constructs the retention worker.
func NewCleanupWorker(repo cleanupRepo, retention, idempotencyTTL, interval time.Duration) *CleanupWorker {
	if interval <= 0 {
		interval = time.Hour
	}
	return &CleanupWorker{repo: repo, retention: retention, idempotencyTTL: idempotencyTTL, interval: interval}
}

// Run cleans at startup then on the interval until ctx is cancelled.
func (w *CleanupWorker) Run(ctx context.Context) error {
	w.cleanup(ctx)
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			w.cleanup(ctx)
		}
	}
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

// DLQReconciler keeps the Redis DLQ LIST in sync with the authoritative in_dlq
// flag and force-acks aged entries (plan Adv §1). It mutates flags only — it
// never deletes rows, keeping CleanupWorker the sole deleter.
type DLQReconciler struct {
	repo     reconcilerRepo
	dlq      domain.DeadLetterQueue
	maxAge   time.Duration
	interval time.Duration
}

// NewDLQReconciler constructs the reconciler.
func NewDLQReconciler(repo reconcilerRepo, dlq domain.DeadLetterQueue, maxAge, interval time.Duration) *DLQReconciler {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	return &DLQReconciler{repo: repo, dlq: dlq, maxAge: maxAge, interval: interval}
}

// Run reconciles at startup then on the interval until ctx is cancelled.
func (w *DLQReconciler) Run(ctx context.Context) error {
	w.reconcile(ctx)
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			w.reconcile(ctx)
		}
	}
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
