package queue

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
)

// KeyDLQ is the Redis key for the dead-letter LIST. It is a fast operator-facing
// index; the durable signal is the in_dlq flag in Postgres (the reconciler
// repairs any divergence).
const KeyDLQ = "webhook:dlq"

// DLQ implements domain.DeadLetterQueue over a Redis LIST.
type DLQ struct {
	rdb *redis.Client
	key string
}

// NewDLQ constructs a DLQ on the default webhook:dlq key.
func NewDLQ(rdb *redis.Client) *DLQ {
	return &DLQ{rdb: rdb, key: KeyDLQ}
}

var _ domain.DeadLetterQueue = (*DLQ)(nil)

// Push LPUSHes a final_failure row id onto the DLQ (best-effort, post-commit).
func (d *DLQ) Push(ctx context.Context, deliveryLogID string) error {
	if err := d.rdb.LPush(ctx, d.key, deliveryLogID).Err(); err != nil {
		return fmt.Errorf("dlq: push: %w", err)
	}
	return nil
}

// Remove LREMs all occurrences of id from the DLQ (on ack/replay/reconcile).
func (d *DLQ) Remove(ctx context.Context, deliveryLogID string) error {
	if err := d.rdb.LRem(ctx, d.key, 0, deliveryLogID).Err(); err != nil {
		return fmt.Errorf("dlq: remove: %w", err)
	}
	return nil
}

// List returns every id currently in the DLQ LIST (for the reconciler).
func (d *DLQ) List(ctx context.Context) ([]string, error) {
	ids, err := d.rdb.LRange(ctx, d.key, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("dlq: list: %w", err)
	}
	return ids, nil
}

// Depth returns the DLQ length (for /monitor).
func (d *DLQ) Depth(ctx context.Context) (int64, error) {
	n, err := d.rdb.LLen(ctx, d.key).Result()
	if err != nil {
		return 0, fmt.Errorf("dlq: depth: %w", err)
	}
	return n, nil
}
