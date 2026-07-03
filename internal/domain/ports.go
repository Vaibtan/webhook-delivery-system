package domain

import (
	"context"
	"time"
)

// Cursor is an opaque keyset-pagination position for listing subscriptions,
// ordered by (created_at DESC, id DESC). A nil cursor means "from the start".
type Cursor struct {
	CreatedAt time.Time
	ID        string
}

// Page is one page of a keyset-paginated listing. NextCursor is nil when the
// last page has been reached.
type Page[T any] struct {
	Items      []T
	NextCursor *Cursor
}

// SubscriptionRepository is the persistence port for subscriptions. The store
// adapter (internal/store) implements it in raw SQL. Methods return ErrNotFound
// where a row is expected but absent.
//
// The interface grows with later slices (rotation, auto-disable counters); each
// addition is implemented in the slice that introduces it.
type SubscriptionRepository interface {
	Create(ctx context.Context, s *Subscription) error
	GetByID(ctx context.Context, id string) (*Subscription, error)
	List(ctx context.Context, limit int, after *Cursor) (Page[*Subscription], error)
	Update(ctx context.Context, s *Subscription) error
	Delete(ctx context.Context, id string) error
	// RotateSecret installs newSecret, demotes the current to previous, and starts
	// the grace window. Returns the updated subscription (plan §5).
	RotateSecret(ctx context.Context, id, newSecret string) (*Subscription, error)
}

// DeliveryLogRepository is the persistence port for delivery attempt rows. The
// CAS / single-transaction lifecycle methods (claim, fail→successor, terminal)
// are added in the slices that implement the worker.
type DeliveryLogRepository interface {
	Create(ctx context.Context, d *DeliveryLog) error
	GetByID(ctx context.Context, id string) (*DeliveryLog, error)
	ListByWebhookID(ctx context.Context, webhookID string) ([]*DeliveryLog, error)
	ListBySubscription(ctx context.Context, subscriptionID string, limit int) ([]*DeliveryLog, error)

	// IngestPending runs the atomic ingest transaction: optional idempotency
	// dedupe + initial pending-row insert (plan §5).
	IngestPending(ctx context.Context, p IngestParams) (IngestResult, error)

	// CountByStatusSince counts attempt rows by status created since `since`.
	CountByStatusSince(ctx context.Context, since time.Time) (StatusCounts, error)

	// MarkSuccess CAS-transitions a pending row to success and resets the
	// subscription's consecutive_failures (one tx). Returns true if THIS caller
	// won the CAS.
	MarkSuccess(ctx context.Context, id, subscriptionID string, httpStatus int) (bool, error)

	// MarkFinalFailure CAS-transitions a pending row to final_failure + in_dlq
	// WITHOUT touching the subscription counter — used for the intentional
	// "subscription deactivated" drop (not a delivery failure).
	MarkFinalFailure(ctx context.Context, id string, httpStatus *int, errorDetails string) (bool, error)

	// FinalizeFailure is the terminal delivery-failure tx: final_failure + in_dlq
	// AND consecutive_failures++ (auto-disable at threshold). Returns won (CAS
	// succeeded) and disabled (sub just crossed the threshold → evict its cache).
	FinalizeFailure(ctx context.Context, id, subscriptionID string, httpStatus *int, errorDetails string, disableThreshold int) (won bool, disabled bool, err error)

	// ClaimPending applies the visibility lease: the conditional CAS
	//   next_retry_at = now()+lease WHERE id=$1 AND status='pending' AND next_retry_at <= now()
	// The `next_retry_at <= now()` predicate makes the claim the mutual-exclusion
	// point. Returns true only if THIS worker claimed the row (1 row affected);
	// false means not due / lease live / already handled — a duplicate dequeue.
	ClaimPending(ctx context.Context, id string, lease time.Duration) (bool, error)

	// FailAndScheduleRetry is the failed→successor transaction (plan §0/§4b): in
	// ONE tx, CAS the prev row to failed_attempt (0 rows ⇒ won=false, abort) and
	// INSERT a successor pending row (attempt_number+1, same chain, next_retry_at).
	// The caller ZADDs the successor id post-commit.
	FailAndScheduleRetry(ctx context.Context, prevID string, httpStatus *int, errorDetails string, nextRetryAt time.Time) (successorID string, won bool, err error)

	// FindDuePending returns ids of pending rows that are due (next_retry_at <=
	// now()) — lost enqueues or rows whose worker crashed (lease expired). Backs
	// the orphan-recovery scan.
	FindDuePending(ctx context.Context, limit int) ([]string, error)

	// RescheduleSamePending pushes a pending row's next_retry_at without
	// consuming an attempt (breaker denial / concurrency requeue, plan §3/Adv §3).
	RescheduleSamePending(ctx context.Context, id string, at time.Time) (bool, error)

	// AckDLQ atomically clears the in_dlq flag for the (subscription, webhook)
	// row (the ack itself). Returns the claimed row id and ok=true if THIS caller
	// won the claim; ok=false ⇒ already reaped/acked (→ 404).
	AckDLQ(ctx context.Context, subscriptionID, webhookID string) (claimedID string, ok bool, err error)

	// ReplayDLQ atomically claims the DLQ row and, in the same tx, inserts a fresh
	// pending row starting a NEW chain (replay_number+1, attempt_number=1). Returns
	// the claimed (old) id and the new chain's id; ok=false ⇒ already reaped (→ 404).
	ReplayDLQ(ctx context.Context, subscriptionID, webhookID string) (claimedID, newID string, ok bool, err error)

	// DeleteExpired is the sole retention-driven deleter: removes terminal,
	// non-DLQ rows older than retention. NEVER deletes pending or in_dlq rows.
	DeleteExpired(ctx context.Context, retention time.Duration) (int64, error)

	// PruneIdempotency removes ingest_idempotency rows older than ttl.
	PruneIdempotency(ctx context.Context, ttl time.Duration) (int64, error)

	// ListInDLQ returns ids of all rows with in_dlq=TRUE (reconciler: flag→list).
	ListInDLQ(ctx context.Context) ([]string, error)

	// IsInDLQ reports whether a given row is currently flagged in_dlq=TRUE
	// (reconciler: list→flag — a LIST id failing this is LREM'd).
	IsInDLQ(ctx context.Context, id string) (bool, error)

	// ForceAckOldDLQ clears in_dlq for entries older than maxAge that no operator
	// acked (the DLQ_MAX_AGE safety valve), returning the affected ids.
	ForceAckOldDLQ(ctx context.Context, maxAge time.Duration) ([]string, error)
}

// DeadLetterQueue is the Redis LIST index of permanently-failed attempt ids.
// Postgres' in_dlq flag is the source of truth; the reconciler repairs the LIST.
type DeadLetterQueue interface {
	Push(ctx context.Context, deliveryLogID string) error
	Remove(ctx context.Context, deliveryLogID string) error
	List(ctx context.Context) ([]string, error)
	Depth(ctx context.Context) (int64, error)
}

// TaskQueue is the immediate-delivery work queue (Redis LIST). Enqueue happens
// after the DB commit (Postgres is the source of truth); Dequeue returns
// ErrQueueEmpty on timeout.
type TaskQueue interface {
	Enqueue(ctx context.Context, deliveryLogID string) error
	Dequeue(ctx context.Context, timeout time.Duration) (string, error)
	Depth(ctx context.Context) (int64, error)
}

// RetryScheduler is the durable retry sorted-set (Redis ZSET, score = due unix
// time). Schedule adds an entry post-commit; ClaimDue atomically moves due
// entries to the work queue via a Lua script.
type RetryScheduler interface {
	Schedule(ctx context.Context, deliveryLogID string, at time.Time) error
	ClaimDue(ctx context.Context, now time.Time) (int, error)
}

// Deliverer performs a single delivery attempt for a delivery-log row, applying
// the CAS-guarded status transition. Implemented by internal/worker.
type Deliverer interface {
	Process(ctx context.Context, deliveryLogID string) error
}
