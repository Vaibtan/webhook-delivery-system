// Package queue is the Redis LIST adapter for the immediate-delivery work queue
// (webhook:queue). LPUSH on enqueue, BRPOP on dequeue. It is a derived index:
// Postgres remains the source of truth, and recovery heals any lost enqueue.
package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
)

// KeyQueue is the Redis key for the immediate-delivery LIST.
const KeyQueue = "webhook:queue"

// Queue stores immediate delivery work in a Redis LIST.
type Queue struct {
	rdb *redis.Client
	key string
}

// New constructs a Queue on the default webhook:queue key.
func New(rdb *redis.Client) *Queue {
	return &Queue{rdb: rdb, key: KeyQueue}
}

// Enqueue LPUSHes a delivery-log id onto the queue (called post-commit).
func (q *Queue) Enqueue(ctx context.Context, deliveryLogID string) error {
	if err := q.rdb.LPush(ctx, q.key, deliveryLogID).Err(); err != nil {
		return fmt.Errorf("queue: enqueue: %w", err)
	}
	return nil
}

// Dequeue BRPOPs the next id, blocking up to timeout. Returns domain.ErrQueueEmpty
// when the timeout elapses with nothing available.
func (q *Queue) Dequeue(ctx context.Context, timeout time.Duration) (string, error) {
	res, err := q.rdb.BRPop(ctx, timeout, q.key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", domain.ErrQueueEmpty
		}
		return "", fmt.Errorf("queue: dequeue: %w", err)
	}
	// BRPOP returns [key, value].
	if len(res) != 2 {
		return "", domain.ErrQueueEmpty
	}
	return res[1], nil
}

// Depth returns the current queue length (for /monitor).
func (q *Queue) Depth(ctx context.Context) (int64, error) {
	n, err := q.rdb.LLen(ctx, q.key).Result()
	if err != nil {
		return 0, fmt.Errorf("queue: depth: %w", err)
	}
	return n, nil
}
