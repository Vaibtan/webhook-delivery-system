// Package registry is a generic, bounded, concurrency-safe map of per-key
// resources (token buckets keyed by subscription, semaphores by subscription,
// circuit breakers by target URL). All three share one eviction discipline:
// a periodic idle sweep plus explicit removal on subscription policy changes.
package registry

import (
	"sync"
	"time"
)

// Registry holds per-key values of type T, created lazily on first Get.
type Registry[T any] struct {
	mu      sync.Mutex
	items   map[string]*slot[T]
	create  func(key string) T
	idleTTL time.Duration
	now     func() time.Time // injectable clock (tests)
}

type slot[T any] struct {
	val      T
	lastUsed time.Time
}

// New constructs a Registry whose entries are built by create and evicted after
// idleTTL of inactivity.
func New[T any](create func(key string) T, idleTTL time.Duration) *Registry[T] {
	return &Registry[T]{
		items:   make(map[string]*slot[T]),
		create:  create,
		idleTTL: idleTTL,
		now:     time.Now,
	}
}

// Get returns the value for key, creating it on first use, and stamps it as
// recently used.
func (r *Registry[T]) Get(key string) T {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.items[key]
	if !ok {
		s = &slot[T]{val: r.create(key)}
		r.items[key] = s
	}
	s.lastUsed = r.now()
	return s.val
}

// Remove evicts a key immediately (e.g. on subscription delete).
func (r *Registry[T]) Remove(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.items, key)
}

// Sweep evicts entries idle longer than idleTTL, returning the count removed.
func (r *Registry[T]) Sweep() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := r.now().Add(-r.idleTTL)
	n := 0
	for k, s := range r.items {
		if s.lastUsed.Before(cutoff) {
			delete(r.items, k)
			n++
		}
	}
	return n
}

// Snapshot returns a copy of the current key→value map (e.g. to report circuit
// breaker states on /monitor).
func (r *Registry[T]) Snapshot() map[string]T {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]T, len(r.items))
	for k, s := range r.items {
		out[k] = s.val
	}
	return out
}
