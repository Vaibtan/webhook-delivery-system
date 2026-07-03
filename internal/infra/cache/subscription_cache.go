// Package cache provides a read-through Redis cache for subscriptions. It
// decorates a domain.SubscriptionRepository: GetByID is served from Redis (JSON,
// CACHE_TTL) on a hit and back-filled on a miss; every other method delegates to
// the underlying repository. Cache coherency is delete-on-write — callers invoke
// Evict on every subscription write path (plan Adv §5).
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
)

const keyPrefix = "subscription:"

// SubscriptionCache is a read-through cache decorator.
type SubscriptionCache struct {
	rdb  *redis.Client
	repo domain.SubscriptionRepository
	ttl  time.Duration
}

// NewSubscriptionCache wraps repo with a Redis read-through cache.
func NewSubscriptionCache(rdb *redis.Client, repo domain.SubscriptionRepository, ttl time.Duration) *SubscriptionCache {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &SubscriptionCache{rdb: rdb, repo: repo, ttl: ttl}
}

var _ domain.SubscriptionRepository = (*SubscriptionCache)(nil)

func key(id string) string { return keyPrefix + id }

// GetByID serves from cache on a hit, otherwise reads through to the repo and
// back-fills. Cache/Redis errors degrade gracefully to the repo.
func (c *SubscriptionCache) GetByID(ctx context.Context, id string) (*domain.Subscription, error) {
	if raw, err := c.rdb.Get(ctx, key(id)).Bytes(); err == nil {
		var s domain.Subscription
		if json.Unmarshal(raw, &s) == nil {
			return &s, nil
		}
		// Corrupt entry — fall through to the repo.
	} else if !errors.Is(err, redis.Nil) {
		// Redis unavailable — degrade to the repo rather than failing the read.
		return c.repo.GetByID(ctx, id)
	}

	s, err := c.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if raw, mErr := json.Marshal(s); mErr == nil {
		_ = c.rdb.Set(ctx, key(id), raw, c.ttl).Err() // best-effort back-fill
	}
	return s, nil
}

// Evict deletes the cached entry (delete-on-write). Best-effort.
func (c *SubscriptionCache) Evict(ctx context.Context, id string) error {
	if err := c.rdb.Del(ctx, key(id)).Err(); err != nil {
		return fmt.Errorf("cache: evict %s: %w", id, err)
	}
	return nil
}

// --- delegated writes/reads (callers Evict explicitly on write paths) --------

func (c *SubscriptionCache) Create(ctx context.Context, s *domain.Subscription) error {
	return c.repo.Create(ctx, s)
}

func (c *SubscriptionCache) List(ctx context.Context, limit int, after *domain.Cursor) (domain.Page[*domain.Subscription], error) {
	return c.repo.List(ctx, limit, after)
}

func (c *SubscriptionCache) Update(ctx context.Context, s *domain.Subscription) error {
	return c.repo.Update(ctx, s)
}

func (c *SubscriptionCache) Delete(ctx context.Context, id string) error {
	return c.repo.Delete(ctx, id)
}

func (c *SubscriptionCache) RotateSecret(ctx context.Context, id, newSecret string) (*domain.Subscription, error) {
	return c.repo.RotateSecret(ctx, id, newSecret)
}
