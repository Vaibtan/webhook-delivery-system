// Package subscription owns the live state of subscriptions: durable CRUD,
// Redis read-through caching, delivery-outcome consequences, and derived
// rate/concurrency controls.
package subscription

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/maphash"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/ratelimit"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/registry"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/semaphore"
)

const (
	keyPrefix        = "subscription:"
	cacheLockStripes = 64
)

type repository interface {
	Create(ctx context.Context, sub *domain.Subscription) error
	GetByID(ctx context.Context, id string) (*domain.Subscription, error)
	List(ctx context.Context, limit int, after *domain.Cursor) (domain.Page[*domain.Subscription], error)
	Update(ctx context.Context, sub *domain.Subscription) error
	Delete(ctx context.Context, id string) error
	RotateSecret(ctx context.Context, id, newSecret string) (*domain.Subscription, error)
}

type deliveryOutcomes interface {
	MarkSuccess(ctx context.Context, id, subscriptionID string, httpStatus int) (won, subscriptionChanged bool, err error)
	FinalizeFailure(ctx context.Context, id, subscriptionID string, httpStatus *int, errorDetails string, disableThreshold int) (won, disabled bool, err error)
}

// Config contains the validated policy for cached and derived subscription
// state.
type Config struct {
	CacheTTL         time.Duration
	RegistryIdleTTL  time.Duration
	RateLimitPerSec  int
	RateLimitBurst   int
	ConcurrencyLimit int
}

// State is the single mutation boundary for durable and derived subscription
// state.
type State struct {
	rdb      *redis.Client
	repo     repository
	outcomes deliveryOutcomes
	ttl      time.Duration

	buckets    *registry.Registry[*ratelimit.TokenBucket]
	semaphores *registry.Registry[*semaphore.Semaphore]

	// Stripes close the commit-to-invalidation window without allocating one
	// lock per subscription. Dirty keys bypass Redis after an invalidation
	// failure until the stale entry expires or a later repair succeeds.
	stripes  [cacheLockStripes]sync.Mutex
	hashSeed maphash.Seed
	dirtyMu  sync.Mutex
	dirty    map[string]time.Time
}

// New constructs the subscription state boundary.
func New(rdb *redis.Client, repo repository, outcomes deliveryOutcomes, cfg Config) *State {
	return &State{
		rdb: rdb, repo: repo, outcomes: outcomes, ttl: cfg.CacheTTL,
		buckets: registry.New(func(string) *ratelimit.TokenBucket {
			return ratelimit.NewTokenBucket(cfg.RateLimitPerSec, cfg.RateLimitBurst)
		}, cfg.RegistryIdleTTL),
		semaphores: registry.New(func(string) *semaphore.Semaphore {
			return semaphore.New(cfg.ConcurrencyLimit)
		}, cfg.RegistryIdleTTL),
		hashSeed: maphash.MakeSeed(),
		dirty:    make(map[string]time.Time),
	}
}

func key(id string) string { return keyPrefix + id }

func (s *State) stripe(id string) *sync.Mutex {
	return &s.stripes[maphash.String(s.hashSeed, id)%cacheLockStripes]
}

func (s *State) isDirty(id string) bool {
	s.dirtyMu.Lock()
	defer s.dirtyMu.Unlock()
	until, ok := s.dirty[id]
	if ok && time.Now().After(until) {
		delete(s.dirty, id)
		return false
	}
	return ok
}

func (s *State) markDirty(id string) {
	s.dirtyMu.Lock()
	s.dirty[id] = time.Now().Add(s.ttl)
	s.dirtyMu.Unlock()
}

func (s *State) clearDirty(id string) {
	s.dirtyMu.Lock()
	delete(s.dirty, id)
	s.dirtyMu.Unlock()
}

// GetByID reads through Redis and falls back to the durable repository whenever
// the cache is missing, corrupt, dirty, or unavailable.
func (s *State) GetByID(ctx context.Context, id string) (*domain.Subscription, error) {
	mu := s.stripe(id)
	mu.Lock()
	defer mu.Unlock()

	if !s.isDirty(id) {
		if raw, err := s.rdb.Get(ctx, key(id)).Bytes(); err == nil {
			var sub domain.Subscription
			if json.Unmarshal(raw, &sub) == nil {
				return &sub, nil
			}
		} else if !errors.Is(err, redis.Nil) {
			return s.repo.GetByID(ctx, id)
		}
	}

	sub, err := s.repo.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			if delErr := s.rdb.Del(ctx, key(id)).Err(); delErr == nil {
				s.clearDirty(id)
			}
		}
		return nil, err
	}
	if raw, marshalErr := json.Marshal(sub); marshalErr == nil {
		if setErr := s.rdb.Set(ctx, key(id), raw, s.ttl).Err(); setErr == nil {
			s.clearDirty(id)
		} else {
			s.markDirty(id)
		}
	}
	return sub, nil
}

func (s *State) evict(ctx context.Context, id string) error {
	mu := s.stripe(id)
	mu.Lock()
	defer mu.Unlock()
	return s.evictLocked(ctx, id)
}

func (s *State) evictLocked(ctx context.Context, id string) error {
	if err := s.rdb.Del(ctx, key(id)).Err(); err != nil {
		s.markDirty(id)
		return fmt.Errorf("subscription: evict %s: %w", id, err)
	}
	s.clearDirty(id)
	return nil
}

func (s *State) resetRuntime(id string) {
	s.buckets.Remove(id)
	s.semaphores.Remove(id)
}

// Allow consumes one admission token for a subscription.
func (s *State) Allow(id string) bool {
	return s.buckets.Get(id).Allow()
}

// Semaphore returns the concurrency control for a subscription.
func (s *State) Semaphore(id string) *semaphore.Semaphore {
	return s.semaphores.Get(id)
}

// Sweep evicts idle derived state and returns the number of entries removed.
func (s *State) Sweep() int {
	return s.buckets.Sweep() + s.semaphores.Sweep()
}

func (s *State) Create(ctx context.Context, sub *domain.Subscription) error {
	if err := s.repo.Create(ctx, sub); err != nil {
		return err
	}
	_ = s.evict(ctx, sub.ID)
	return nil
}

func (s *State) List(ctx context.Context, limit int, after *domain.Cursor) (domain.Page[*domain.Subscription], error) {
	return s.repo.List(ctx, limit, after)
}

func (s *State) Update(ctx context.Context, sub *domain.Subscription) error {
	mu := s.stripe(sub.ID)
	mu.Lock()
	defer mu.Unlock()
	if err := s.repo.Update(ctx, sub); err != nil {
		return err
	}
	_ = s.evictLocked(ctx, sub.ID)
	return nil
}

func (s *State) Delete(ctx context.Context, id string) error {
	mu := s.stripe(id)
	mu.Lock()
	defer mu.Unlock()
	if err := s.repo.Delete(ctx, id); err != nil {
		return err
	}
	_ = s.evictLocked(ctx, id)
	s.resetRuntime(id)
	return nil
}

func (s *State) RotateSecret(ctx context.Context, id, newSecret string) (*domain.Subscription, error) {
	mu := s.stripe(id)
	mu.Lock()
	defer mu.Unlock()
	sub, err := s.repo.RotateSecret(ctx, id, newSecret)
	if err != nil {
		return nil, err
	}
	_ = s.evictLocked(ctx, id)
	return sub, nil
}

// RecordDeliverySuccess commits the delivery transition and invalidates cached
// subscription state only when the transition reset its failure counter.
func (s *State) RecordDeliverySuccess(ctx context.Context, id, subscriptionID string, httpStatus int) (bool, error) {
	mu := s.stripe(subscriptionID)
	mu.Lock()
	defer mu.Unlock()
	won, changed, err := s.outcomes.MarkSuccess(ctx, id, subscriptionID, httpStatus)
	if err != nil {
		return false, err
	}
	if changed {
		_ = s.evictLocked(ctx, subscriptionID)
	}
	return won, nil
}

// RecordFinalFailure commits terminal delivery state with the subscription
// failure counter. Auto-disablement also resets derived policy state.
func (s *State) RecordFinalFailure(ctx context.Context, id, subscriptionID string, httpStatus *int, errorDetails string, disableThreshold int) (bool, bool, error) {
	mu := s.stripe(subscriptionID)
	mu.Lock()
	defer mu.Unlock()
	won, disabled, err := s.outcomes.FinalizeFailure(
		ctx, id, subscriptionID, httpStatus, errorDetails, disableThreshold,
	)
	if err != nil {
		return false, false, err
	}
	if won {
		_ = s.evictLocked(ctx, subscriptionID)
	}
	if disabled {
		s.resetRuntime(subscriptionID)
	}
	return won, disabled, nil
}
