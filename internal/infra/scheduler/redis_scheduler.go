// Package scheduler is the Redis sorted-set adapter for durable retry
// scheduling. Each member is a pending delivery-log id; the score is its due
// unix time. A Lua script atomically moves due entries to webhook:queue,
// eliminating the multi-replica race of separate ZRANGEBYSCORE + ZREM + LPUSH.
package scheduler

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Vaibtan/webhook-delivery-system/internal/infra/queue"
)

// KeySchedule is the Redis key for the retry sorted set.
const KeySchedule = "webhook:retry:schedule"

// claimDueScript atomically claims up to 100 due entries (score <= now) and
// moves them from the schedule ZSET (KEYS[1]) to the work LIST (KEYS[2]).
var claimDueScript = redis.NewScript(`
	local tasks = redis.call('ZRANGEBYSCORE', KEYS[1], '0', ARGV[1], 'LIMIT', 0, 100)
	for _, task in ipairs(tasks) do
		redis.call('ZREM', KEYS[1], task)
		redis.call('LPUSH', KEYS[2], task)
	end
	return #tasks
`)

// Scheduler stores durable retry due times in Redis.
type Scheduler struct {
	rdb         *redis.Client
	scheduleKey string
	queueKey    string
}

// New constructs a Scheduler that feeds the default webhook:queue.
func New(rdb *redis.Client) *Scheduler {
	return &Scheduler{rdb: rdb, scheduleKey: KeySchedule, queueKey: queue.KeyQueue}
}

// unixScore renders a time as fractional unix SECONDS with millisecond
// precision (UnixMilli fits exactly in float64). Using sub-second scores avoids
// the integer-truncation bug where at.Unix() floors the due time, letting the
// poller move a row up to ~1s EARLY — before its full-precision next_retry_at —
// so the visibility-lease claim (next_retry_at <= NOW()) would then reject it.
func unixScore(t time.Time) float64 { return float64(t.UnixMilli()) / 1000.0 }

// Schedule ZADDs the id with a sub-second due score (called post-commit).
func (s *Scheduler) Schedule(ctx context.Context, deliveryLogID string, at time.Time) error {
	if err := s.rdb.ZAdd(ctx, s.scheduleKey, redis.Z{Score: unixScore(at), Member: deliveryLogID}).Err(); err != nil {
		return fmt.Errorf("scheduler: zadd: %w", err)
	}
	return nil
}

// ClaimDue moves all entries due at `now` into the work queue, returning the
// count moved.
func (s *Scheduler) ClaimDue(ctx context.Context, now time.Time) (int, error) {
	keys := []string{s.scheduleKey, s.queueKey}
	nowArg := strconv.FormatFloat(unixScore(now), 'f', 3, 64)
	n, err := claimDueScript.Run(ctx, s.rdb, keys, nowArg).Int()
	if err != nil {
		return 0, fmt.Errorf("scheduler: claim due: %w", err)
	}
	return n, nil
}
